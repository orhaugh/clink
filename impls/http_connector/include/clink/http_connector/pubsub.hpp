#pragma once

// Google Cloud Pub/Sub connector over the REST API (v1), built ON the shared
// keep-alive HttpRequest + the bulk-post retry core, with NO new dependency:
//
//   pubsub_sink   - POST projects/<p>/topics/<t>:publish with a
//                   {"messages":[{"data":"<base64>"}]} body. At-least-once,
//                   batched via the shared BatchedHttpBulkSink
//                   (BulkFraming::PubSubMessages).
//   pubsub_source - synchronous Pull + Acknowledge: POST :pull, emit each
//                   message's base64-decoded data, hold the ackIds, and POST
//                   :acknowledge on the checkpoint (the ack IS the offset commit,
//                   same shape as the Redis Streams source). At-least-once.
//
// AUTH: this build does not mint OAuth2 tokens. Supply a bearer token via the
// `auth_token` option (-> "Authorization: Bearer <token>") or arbitrary
// `headers`; against the Pub/Sub emulator no auth is needed. A static token is
// valid only for its lifetime (~1h on GCP); metadata-server / service-account
// token refresh is a documented follow-up.
//
// PULL semantics: the REST synchronous Pull is used with returnImmediately so
// produce() returns promptly (bounded cancel latency) and the source sleeps the
// poll interval when a pull is empty. returnImmediately is deprecated-but-
// functional on GCP and fully supported by the emulator; gRPC StreamingPull is
// the future upgrade. Unacked messages are redelivered by the server after the
// subscription ackDeadline, which is ALSO the crash-recovery mechanism (no local
// data cursor needed) - so a checkpoint interval longer than the ackDeadline
// yields duplicates (at-least-once).
//
// CAVEAT (honest, same class as the Redis Streams source): :acknowledge is an
// external side effect not transactional with the global checkpoint. If a
// checkpoint's snapshot_offset acks a batch and the GLOBAL checkpoint then fails,
// those messages will not redeliver - at-most-once for that one batch. Strict
// exactly-once would need a source on_commit hook (deferred). One further
// at-most-once exception: a message whose `data` is not valid base64 is acked +
// dropped (with an error metric) rather than redelivered forever in a crash-loop.

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/http_connector/base64.hpp"
#include "clink/http_connector/batched_http_bulk_sink.hpp"
#include "clink/http_connector/http_bulk_post.hpp"  // classify_http_status, backoff_delay
#include "clink/http_connector/http_request.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/state_backend.hpp"

namespace clink::http_connector {

// --- Resource-id validation + REST path builders (pure, unit-tested) ---

// A Pub/Sub project / topic / subscription id is a single path segment, inserted
// verbatim (NOT percent-encoded, so a legal '+'/'%' is not mangled). Enforce the
// GCP-legal id charset as an ALLOWLIST - letters, digits and '-._~%+' - which
// both excludes the path/verb-breaking characters ('/', ':', '?', '#') and
// rejects odd-but-not-injecting bytes ('"', '\\', '{', space, control) up front
// with a clear client-side error instead of an opaque server 4xx.
inline void pubsub_check_id(const std::string& kind, const std::string& id) {
    if (id.empty()) {
        throw std::runtime_error("pubsub: '" + kind + "' is required");
    }
    for (char c : id) {
        // ASCII allowlist; a high-bit byte (negative char) matches none and is
        // rejected, so no unsigned conversion is needed.
        const bool legal = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                           (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' || c == '~' ||
                           c == '%' || c == '+';
        if (!legal) {
            throw std::runtime_error("pubsub: invalid " + kind + " '" + id + "'");
        }
    }
}

inline std::string pubsub_publish_path(const std::string& project, const std::string& topic) {
    return "/v1/projects/" + project + "/topics/" + topic + ":publish";
}
inline std::string pubsub_pull_path(const std::string& project, const std::string& subscription) {
    return "/v1/projects/" + project + "/subscriptions/" + subscription + ":pull";
}
inline std::string pubsub_ack_path(const std::string& project, const std::string& subscription) {
    return "/v1/projects/" + project + "/subscriptions/" + subscription + ":acknowledge";
}

// --- Request/response bodies (pure, unit-tested) ---

// One PubsubMessage JSON object for the publish body. The payload is base64'd
// into "data"; base64 is JSON-safe (no chars needing escaping) so this is a
// direct concatenation.
inline std::string pubsub_message_fragment(const std::string& payload) {
    return "{\"data\":\"" + base64_encode(payload) + "\"}";
}

inline std::string pubsub_pull_body(int max_messages, bool return_immediately) {
    return "{\"maxMessages\":" + std::to_string(max_messages) +
           ",\"returnImmediately\":" + (return_immediately ? "true" : "false") + "}";
}

// AcknowledgeRequest body. AckIds are server-opaque strings; serialise each
// through JsonValue so any escaping is correct.
inline std::string pubsub_ack_body(const std::vector<std::string>& ack_ids) {
    std::string b = "{\"ackIds\":[";
    bool first = true;
    for (const auto& id : ack_ids) {
        if (!first) {
            b.push_back(',');
        }
        first = false;
        b += clink::config::JsonValue{id}.serialize(0);
    }
    b += "]}";
    return b;
}

struct PulledMessage {
    std::string ack_id;
    std::string payload;  // base64-decoded message data (empty if absent/undecodable)
    std::string message_id;
    bool data_ok{true};  // false when data was present but failed base64 decode
};

// Parse a PullResponse body into the received messages. Throws on invalid JSON
// (a transport-level corruption the caller should treat as a failed pull). A
// message with no ackId is skipped (it cannot be acked). receivedMessages absent
// or empty -> empty vector (an idle pull).
inline std::vector<PulledMessage> pubsub_parse_pull_response(const std::string& body) {
    clink::config::JsonValue root;
    try {
        root = clink::config::parse(body);
    } catch (...) {
        throw std::runtime_error("pubsub: pull response is not valid JSON");
    }
    std::vector<PulledMessage> out;
    if (!root.is_object()) {
        return out;
    }
    const auto& obj = root.as_object();
    auto rm = obj.find("receivedMessages");
    if (rm == obj.end() || !rm->second.is_array()) {
        return out;
    }
    for (const auto& elem : rm->second.as_array()) {
        if (!elem.is_object()) {
            continue;
        }
        const auto& e = elem.as_object();
        PulledMessage m;
        if (auto a = e.find("ackId"); a != e.end() && a->second.is_string()) {
            m.ack_id = a->second.as_string();
        }
        if (m.ack_id.empty()) {
            continue;  // unackable; drop
        }
        if (auto msg = e.find("message"); msg != e.end() && msg->second.is_object()) {
            const auto& mo = msg->second.as_object();
            if (auto id = mo.find("messageId"); id != mo.end() && id->second.is_string()) {
                m.message_id = id->second.as_string();
            }
            if (auto d = mo.find("data"); d != mo.end() && d->second.is_string()) {
                if (auto decoded = base64_decode(d->second.as_string())) {
                    m.payload = std::move(*decoded);
                } else {
                    m.data_ok = false;
                }
            }
        }
        out.push_back(std::move(m));
    }
    return out;
}

// --- Publish sink (reuses BatchedHttpBulkSink) ---

struct PubSubSinkOptions {
    HttpRequest::Options http;  // base_url + headers (auth) + timeouts + verify_tls
    std::string project;
    std::string topic;
    std::size_t batch_records{1000};           // Pub/Sub caps a publish at 1000 messages
    std::size_t batch_bytes{9 * 1024 * 1024};  // ... and ~10MB; leave JSON headroom
    int max_retries{4};
    std::chrono::milliseconds linger{0};
    DlqPolicy dlq_policy{DlqPolicy::Fail};
    std::string name{"pubsub_sink"};
};

inline std::shared_ptr<Sink<std::string>> make_pubsub_publish_sink(PubSubSinkOptions o) {
    pubsub_check_id("project", o.project);
    pubsub_check_id("topic", o.topic);
    if (o.batch_records < 1) {
        o.batch_records = 1;
    } else if (o.batch_records > 1000) {
        o.batch_records = 1000;  // hard Pub/Sub publish limit
    }

    BatchedHttpBulkSink<std::string>::Options bo;
    bo.http = std::move(o.http);
    bo.path = pubsub_publish_path(o.project, o.topic);
    bo.content_type = "application/json";
    bo.framing = BulkFraming::PubSubMessages;
    bo.max_records = o.batch_records;
    bo.max_bytes = o.batch_bytes;
    bo.max_retries = o.max_retries;
    bo.max_age = o.linger;
    bo.dlq_policy = o.dlq_policy;
    bo.name = o.name;
    // A successful publish is any 2xx; the body carries messageIds. The default
    // whole-batch 2xx validator is correct (publish is all-or-nothing - there is
    // no per-message partial-success report to parse).
    auto render = [](std::string& out, const std::string& rec) {
        out += pubsub_message_fragment(rec);
    };
    return std::make_shared<BatchedHttpBulkSink<std::string>>(std::move(bo), std::move(render));
}

// --- Pull source ---

struct PubSubSourceOptions {
    HttpRequest::Options http;
    std::string project;
    std::string subscription;
    int max_messages{1000};                        // Pull maxMessages per request
    bool return_immediately{true};                 // bounded cancel latency; see header note
    std::chrono::milliseconds poll_interval{500};  // sleep after an empty pull
    int max_retries{4};                            // transient-pull retries within one produce()
    std::chrono::milliseconds retry_base_backoff{200};
    std::string name{"pubsub_source"};
};

class PubSubPullSource : public Source<std::string> {
public:
    explicit PubSubPullSource(PubSubSourceOptions opts) : opts_(std::move(opts)) {
        pubsub_check_id("project", opts_.project);
        pubsub_check_id("subscription", opts_.subscription);
        if (!HttpRequest::tls_supported() && opts_.http.base_url.rfind("https://", 0) == 0) {
            throw std::runtime_error(opts_.name +
                                     ": https endpoint but this build has no TLS support (rebuild "
                                     "the http_connector module with OpenSSL)");
        }
        if (opts_.max_messages < 1) {
            opts_.max_messages = 1;
        } else if (opts_.max_messages > 1000) {
            opts_.max_messages = 1000;  // Pull maxMessages ceiling
        }
        if (opts_.poll_interval.count() < 0) {
            opts_.poll_interval = std::chrono::milliseconds{0};
        }
        if (opts_.max_retries < 0) {
            opts_.max_retries = 0;
        } else if (opts_.max_retries > 20) {
            opts_.max_retries = 20;
        }
    }

    void open() override {
        client_ = std::make_unique<HttpRequest>(opts_.http);
        pull_path_ = pubsub_pull_path(opts_.project, opts_.subscription);
        ack_path_ = pubsub_ack_path(opts_.project, opts_.subscription);
        unacked_ids_.clear();
    }

    bool produce(Emitter<std::string>& out) override {
        if (this->cancelled() || client_ == nullptr) {
            return false;
        }
        const std::string body = pubsub_pull_body(opts_.max_messages, opts_.return_immediately);
        // Each iteration either breaks on a 2xx or throws (transient + last
        // attempt, or any permanent status), so control always leaves the loop
        // with a 2xx in `res`.
        HttpResponse res;
        for (int attempt = 0; attempt <= opts_.max_retries; ++attempt) {
            if (attempt > 0) {
                std::this_thread::sleep_for(backoff_delay(attempt, opts_.retry_base_backoff));
            }
            res = client_->post(pull_path_, body, "application/json");
            if (res.status >= 200 && res.status < 300) {
                break;
            }
            const bool transient = classify_http_status(res.status) == HttpFailureClass::Transient;
            if (!transient || attempt == opts_.max_retries) {
                clink::metrics::connector::error_inc("pubsub", "source");
                const std::string detail =
                    res.status == 0 ? res.error : "HTTP " + std::to_string(res.status);
                throw std::runtime_error(opts_.name + ": POST " + opts_.http.base_url + pull_path_ +
                                         " failed (" + detail + ")");
            }
        }

        std::vector<PulledMessage> msgs = pubsub_parse_pull_response(res.body);
        Batch<std::string> batch;
        std::uint64_t bytes = 0;
        std::size_t undecodable = 0;
        for (auto& m : msgs) {
            unacked_ids_.push_back(std::move(m.ack_id));
            if (!m.data_ok) {
                // Poison message (data present but not valid base64): the ackId
                // stays in unacked_ids_ so it is acked + DROPPED on the next
                // checkpoint, rather than redelivered forever in a crash-loop.
                // This is the one at-most-once exception (see the header note).
                ++undecodable;
                continue;
            }
            bytes += m.payload.size();
            batch.emplace(std::move(m.payload));
        }
        if (undecodable > 0) {
            clink::metrics::connector::error_inc("pubsub", "source");
        }
        if (!batch.empty()) {
            const auto n = batch.size();
            clink::metrics::connector::records_in_inc("pubsub", n);
            clink::metrics::connector::bytes_in_inc("pubsub", bytes);
            out.emit_data(std::move(batch));
        } else if (opts_.poll_interval.count() > 0) {
            // Empty pull (returnImmediately): sleep so we do not hot-loop. The
            // sleep bounds cancel latency to one poll interval.
            std::this_thread::sleep_for(opts_.poll_interval);
        }
        return !this->cancelled();
    }

    void cancel() override { Source<std::string>::cancel(); }

    void close() override { client_.reset(); }

    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    std::string name() const override { return opts_.name; }

    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId /*ckpt*/) override {
        // Acknowledge everything delivered since the last checkpoint: the offset
        // commit. Chunk to stay under the AcknowledgeRequest size limit. On any
        // failure KEEP the unacked ids (the server redelivers after the ack
        // deadline) so at-least-once holds.
        if (client_ != nullptr && !unacked_ids_.empty()) {
            constexpr std::size_t kAckChunk = 1000;
            bool all_ok = true;
            for (std::size_t off = 0; off < unacked_ids_.size(); off += kAckChunk) {
                const std::size_t end = std::min(off + kAckChunk, unacked_ids_.size());
                std::vector<std::string> chunk(unacked_ids_.begin() + static_cast<long>(off),
                                               unacked_ids_.begin() + static_cast<long>(end));
                HttpResponse r =
                    client_->post(ack_path_, pubsub_ack_body(chunk), "application/json");
                if (!(r.status >= 200 && r.status < 300)) {
                    all_ok = false;
                    clink::metrics::connector::error_inc("pubsub", "source");
                    break;
                }
            }
            if (all_ok) {
                acked_total_ += unacked_ids_.size();
                unacked_ids_.clear();
            }
        }
        // Persist a progress marker so restore_offset can report whether a prior
        // checkpoint ran for this subscription. The ack deadline (server side) is
        // the real recovery mechanism, not a stored data cursor.
        const std::string key = std::string(kMarkerPrefix) + opts_.subscription;
        const std::string val = std::to_string(acked_total_);
        backend.put_operator_state(op_id, key, StateBackend::ValueView{val.data(), val.size()});
    }

    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        bool found = false;
        const std::string_view prefix{kMarkerPrefix};
        backend.scan_operator_state(
            op_id, [&](StateBackend::KeyView key, StateBackend::ValueView /*value*/) {
                if (key.size() > prefix.size() && key.substr(0, prefix.size()) == prefix) {
                    found = true;
                }
            });
        return found;
    }

    // Test/observability accessors.
    [[nodiscard]] std::size_t unacked_count() const noexcept { return unacked_ids_.size(); }
    [[nodiscard]] std::uint64_t acked_total() const noexcept { return acked_total_; }

private:
    static constexpr const char* kMarkerPrefix = "__pubsub_acked__:";

    PubSubSourceOptions opts_;
    std::unique_ptr<HttpRequest> client_;
    std::string pull_path_;
    std::string ack_path_;
    std::vector<std::string> unacked_ids_;  // delivered since the last checkpoint
    std::uint64_t acked_total_{0};
};

}  // namespace clink::http_connector
