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
//                   :acknowledge once the checkpoint that covers them has
//                   completed (the ack IS the offset commit). At-least-once.
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
// the future upgrade. A message the source holds is leased: unacked, the server
// delivers it again once its ack deadline passes, which is ALSO the
// crash-recovery mechanism (no local data cursor needed).
//
// ACKNOWLEDGEMENT waits for CHECKPOINT COMPLETION. snapshot_offset() seals the
// messages pulled since the last barrier as that checkpoint's, and
// notify_checkpoint_complete() makes every interval at or below the completed
// id ready; the next produce() turn (or close()) posts :acknowledge for them, so
// every request stays on the source thread. An aborted or never-completed
// checkpoint leaves its messages unacknowledged, and the server delivers them
// again. While a message waits, the source holds its lease: it sets the
// deadline to ack_deadline on receipt and extends it at half that period, for
// at most max_lease; past that the server delivers the message again under a
// new ackId (a duplicate, acknowledged with its own checkpoint). Duplicates also
// follow whenever a checkpoint completes and the process dies before its
// acknowledgement lands. The completion is not the commit confirmation: a job
// whose restore selects an older checkpoint than one this source acknowledged
// does not see those messages again.
//
// BOUND: at most max_outstanding messages are held unacknowledged; at the bound
// the source stops pulling until a checkpoint completes, so a job that never
// completes one reads max_outstanding messages and waits.
//
// DATA THAT IS NOT BASE64 (on_decode_error): 'fail' (the default) stops the
// subtask without acknowledging the message, so it is delivered again on every
// restart until the subscription's dead-letter policy moves it aside or it is
// removed; 'drop' acknowledges it with its checkpoint, emits nothing and counts
// it in dropped_events_total, so it is lost by choice. A received message with
// no ackId cannot be acknowledged and is skipped; the server delivers it again.

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/connectors/checkpoint_ack_ledger.hpp"
#include "clink/http_connector/base64.hpp"
#include "clink/http_connector/batched_http_bulk_sink.hpp"
#include "clink/http_connector/http_bulk_post.hpp"  // classify_http_status, backoff_delay
#include "clink/http_connector/http_request.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/log_buffer.hpp"
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
inline std::string pubsub_modack_path(const std::string& project, const std::string& subscription) {
    return "/v1/projects/" + project + "/subscriptions/" + subscription + ":modifyAckDeadline";
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
        clink::config::JsonValue{id}.serialize_into(b);
    }
    b += "]}";
    return b;
}

// ModifyAckDeadlineRequest body: the ackIds and the new deadline in seconds
// (0 hands the messages back for immediate redelivery).
inline std::string pubsub_modack_body(const std::vector<std::string>& ack_ids, int seconds) {
    std::string b = "{\"ackIds\":[";
    bool first = true;
    for (const auto& id : ack_ids) {
        if (!first) {
            b.push_back(',');
        }
        first = false;
        clink::config::JsonValue{id}.serialize_into(b);
    }
    b += "],\"ackDeadlineSeconds\":" + std::to_string(seconds) + "}";
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
    int max_retries{4};  // transient-failure retries of a pull or an acknowledge
    std::chrono::milliseconds retry_base_backoff{200};
    // The lease the source takes on each message it holds, sent as
    // ackDeadlineSeconds (rounded up to whole seconds, at most 600) on receipt and
    // again at half this period.
    std::chrono::milliseconds ack_deadline{std::chrono::seconds{60}};
    // How long the source keeps extending one message's lease. Past it the
    // server delivers the message again under a new ackId.
    std::chrono::milliseconds max_lease{std::chrono::hours{1}};
    // Most messages held unacknowledged; at the bound pulls pause until a
    // checkpoint completes.
    std::size_t max_outstanding{10000};
    // A message whose data is not valid base64 (see the header note).
    enum class OnDecodeError { Fail, Drop };
    OnDecodeError on_decode_error{OnDecodeError::Fail};
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
        if (opts_.ack_deadline < std::chrono::milliseconds{2}) {
            opts_.ack_deadline = std::chrono::milliseconds{2};
        } else if (opts_.ack_deadline > std::chrono::seconds{600}) {
            opts_.ack_deadline = std::chrono::seconds{600};  // ModifyAckDeadline ceiling
        }
        if (opts_.max_outstanding < 1) {
            opts_.max_outstanding = 1;
        }
    }

    void open() override {
        client_ = std::make_unique<HttpRequest>(opts_.http);
        pull_path_ = pubsub_pull_path(opts_.project, opts_.subscription);
        ack_path_ = pubsub_ack_path(opts_.project, opts_.subscription);
        modack_path_ = pubsub_modack_path(opts_.project, opts_.subscription);
    }

    bool produce(Emitter<std::string>& out) override {
        if (this->cancelled() || client_ == nullptr) {
            return false;
        }
        acknowledge_completed_();
        extend_leases_();
        const std::size_t held = ledger_.outstanding();
        if (held >= opts_.max_outstanding) {
            if (!paused_) {
                paused_ = true;
                clink::log::warn("pubsub.source",
                                 opts_.name + ": " + std::to_string(held) +
                                     " messages wait for a completed checkpoint before they are "
                                     "acknowledged; pulls paused until one completes "
                                     "(max_outstanding " +
                                     std::to_string(opts_.max_outstanding) + ")");
            }
            std::this_thread::sleep_for(
                std::min(opts_.poll_interval, std::chrono::milliseconds{50}));
            return !this->cancelled();
        }
        paused_ = false;
        const int room = static_cast<int>(std::min<std::size_t>(
            opts_.max_outstanding - held, static_cast<std::size_t>(opts_.max_messages)));
        HttpResponse res = post_with_retries_(
            pull_path_, pubsub_pull_body(room, opts_.return_immediately), "pull");

        std::vector<PulledMessage> msgs = pubsub_parse_pull_response(res.body);
        if (opts_.on_decode_error == PubSubSourceOptions::OnDecodeError::Fail) {
            for (const auto& m : msgs) {
                if (m.data_ok) {
                    continue;
                }
                clink::metrics::connector::error_inc("pubsub", "source");
                // Nothing of this pull was emitted or held: hand all of it back,
                // so the restart sees it at once rather than when the leases run out.
                std::vector<std::string> pulled;
                for (const auto& each : msgs) {
                    pulled.push_back(each.ack_id);
                }
                nack_(pulled);
                throw std::runtime_error(
                    opts_.name + ": message " + m.message_id +
                    " has data that is not valid base64. on_decode_error='fail' stops here "
                    "without acknowledging it, so it is delivered again until it is removed or "
                    "the subscription's dead-letter policy moves it aside; "
                    "on_decode_error='drop' acknowledges and counts such messages instead");
            }
        }
        Batch<std::string> batch;
        std::uint64_t bytes = 0;
        std::size_t dropped = 0;
        const auto now = std::chrono::steady_clock::now();
        for (auto& m : msgs) {
            leases_[m.ack_id] = Lease{now, now};
            ledger_.delivered(m.ack_id);
            if (!m.data_ok) {
                ++dropped;  // acknowledged with its checkpoint, never emitted
                continue;
            }
            bytes += m.payload.size();
            batch.emplace(std::move(m.payload));
        }
        if (dropped > 0) {
            clink::metrics::connector::error_inc("pubsub", "source");
            clink::metrics::connector::dropped_events_inc("pubsub", dropped);
        }
        // Take the lease on what just arrived: the subscription's own deadline
        // may be shorter than the wait for a checkpoint.
        extend_leases_();
        if (!batch.empty()) {
            const auto n = batch.size();
            clink::metrics::connector::records_in_inc("pubsub", n);
            clink::metrics::connector::bytes_in_inc("pubsub", bytes);
            out.emit_data(std::move(batch));
        } else if (msgs.empty() && opts_.poll_interval.count() > 0) {
            // Empty pull (returnImmediately): sleep so we do not hot-loop. The
            // sleep bounds cancel latency to one poll interval.
            std::this_thread::sleep_for(opts_.poll_interval);
        }
        return !this->cancelled();
    }

    void cancel() override { Source<std::string>::cancel(); }

    // Acknowledges what completed checkpoints cover and hands every other message
    // whose lease may still run - held, or of an aborted checkpoint - back for
    // immediate redelivery (ackDeadlineSeconds 0), whether the source is closing
    // at a cancel, a failure or the end of a stop. Best effort: what fails here
    // is delivered again when its lease runs out.
    void close() override {
        if (client_ != nullptr) {
            try {
                acknowledge_completed_();
            } catch (const std::exception& e) {
                clink::log::warn("pubsub.source",
                                 opts_.name +
                                     ": messages a completed checkpoint covers were not "
                                     "acknowledged at close, and are delivered again: " +
                                     e.what());
            }
            std::vector<std::string> held;
            held.reserve(leases_.size());
            for (const auto& [id, _] : leases_) {
                held.push_back(id);
            }
            nack_(held);
            leases_.clear();
        }
        client_.reset();
    }

    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    std::string name() const override { return opts_.name; }

    // Seals the messages pulled since the last barrier as this checkpoint's;
    // none is acknowledged until the checkpoint completes.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt) override {
        ledger_.seal(ckpt.value());
        // A progress marker, so restore_offset can report whether a prior
        // checkpoint ran for this subscription. The server's redelivery is the
        // recovery mechanism, not a stored data cursor.
        const std::string key = std::string(kMarkerPrefix) + opts_.subscription;
        const std::string val = std::to_string(acked_total_);
        backend.put_operator_state(op_id, key, StateBackend::ValueView{val.data(), val.size()});
    }

    // Commit-dispatch thread: only moves ackIds between the ledger's lists. The
    // requests run on the source thread.
    void notify_checkpoint_complete(CheckpointId ckpt) override { ledger_.complete(ckpt.value()); }
    void notify_checkpoint_aborted(CheckpointId ckpt) override { ledger_.abort(ckpt.value()); }

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
    [[nodiscard]] std::size_t unacked_count() const { return ledger_.outstanding(); }
    [[nodiscard]] std::uint64_t acked_total() const noexcept { return acked_total_; }
    // Leases the source still extends.
    [[nodiscard]] std::size_t leased_count() const noexcept {
        return static_cast<std::size_t>(std::count_if(
            leases_.begin(), leases_.end(), [](const auto& l) { return !l.second.released; }));
    }

private:
    static constexpr const char* kMarkerPrefix = "__pubsub_acked__:";
    static constexpr std::size_t kAckChunk = 1000;  // ackIds per request

    struct Lease {
        std::chrono::steady_clock::time_point received;
        std::chrono::steady_clock::time_point extend_at;  // next ModifyAckDeadline due
        // Its checkpoint aborted: no longer extended, and forgotten once the
        // lease has run out, but handed back at close until then.
        bool released{false};
    };

    // A pull or an acknowledge: transient failures are retried with backoff up
    // to max_retries; a permanent one, or the last transient one, throws. An
    // acknowledge that throws leaves its messages unacknowledged (or acknowledged,
    // when only the reply was lost), and either way the server delivers again only
    // what no completed checkpoint acknowledged.
    HttpResponse post_with_retries_(const std::string& path,
                                    const std::string& body,
                                    const char* what) {
        HttpResponse res;
        for (int attempt = 0; attempt <= opts_.max_retries; ++attempt) {
            if (attempt > 0) {
                std::this_thread::sleep_for(backoff_delay(attempt, opts_.retry_base_backoff));
            }
            res = client_->post(path, body, "application/json");
            if (res.status >= 200 && res.status < 300) {
                return res;
            }
            const bool transient = classify_http_status(res.status) == HttpFailureClass::Transient;
            if (!transient || attempt == opts_.max_retries) {
                clink::metrics::connector::error_inc("pubsub", "source");
                const std::string detail =
                    res.status == 0 ? res.error : "HTTP " + std::to_string(res.status);
                throw std::runtime_error(opts_.name + ": " + what + " (POST " +
                                         opts_.http.base_url + path + ") failed (" + detail + ")");
            }
        }
        return res;
    }

    // Acknowledge what completed checkpoints cover; stop extending the leases of
    // an aborted checkpoint's messages, which the server then delivers again.
    void acknowledge_completed_() {
        auto handover = ledger_.take();
        for (const auto& id : handover.released) {
            if (auto l = leases_.find(id); l != leases_.end()) {
                l->second.released = true;
            }
        }
        const auto& ids = handover.ready;
        for (std::size_t off = 0; off < ids.size(); off += kAckChunk) {
            const std::size_t end = std::min(off + kAckChunk, ids.size());
            const std::vector<std::string> chunk(ids.begin() + static_cast<long>(off),
                                                 ids.begin() + static_cast<long>(end));
            for (const auto& id : chunk) {
                leases_.erase(id);
            }
            (void)post_with_retries_(ack_path_, pubsub_ack_body(chunk), "acknowledge");
            acked_total_ += chunk.size();
        }
    }

    // Extend every lease that is due, up to max_lease from receipt. A failed
    // extension is retried next turn, and the cost of one that never lands is a
    // duplicate, not a loss, so it does not fail the subtask.
    void extend_leases_() {
        const auto now = std::chrono::steady_clock::now();
        std::vector<std::string> due;
        std::size_t expired = 0;
        for (auto it = leases_.begin(); it != leases_.end();) {
            if (it->second.released) {
                // The lease last extended at extend_at - ack_deadline/2 has run
                // out by extend_at + ack_deadline/2.
                if (now >= it->second.extend_at + opts_.ack_deadline / 2) {
                    it = leases_.erase(it);
                } else {
                    ++it;
                }
                continue;
            }
            if (now < it->second.extend_at) {
                ++it;
                continue;
            }
            if (now - it->second.received >= opts_.max_lease) {
                ++expired;
                it = leases_.erase(it);
                continue;
            }
            due.push_back(it->first);
            it->second.extend_at = now + opts_.ack_deadline / 2;
            ++it;
        }
        if (expired > 0) {
            clink::log::warn("pubsub.source",
                             opts_.name + ": " + std::to_string(expired) +
                                 " messages held past max_lease are no longer extended; the "
                                 "server delivers them again under new ackIds");
        }
        const int seconds = static_cast<int>(
            std::min<std::int64_t>(600, (opts_.ack_deadline.count() + 999) / 1000));
        for (std::size_t off = 0; off < due.size(); off += kAckChunk) {
            const std::size_t end = std::min(off + kAckChunk, due.size());
            const std::vector<std::string> chunk(due.begin() + static_cast<long>(off),
                                                 due.begin() + static_cast<long>(end));
            HttpResponse r =
                client_->post(modack_path_, pubsub_modack_body(chunk, seconds), "application/json");
            if (!(r.status >= 200 && r.status < 300)) {
                clink::metrics::connector::error_inc("pubsub", "source");
                const auto retry_at = now + opts_.retry_base_backoff;
                for (const auto& id : chunk) {
                    if (auto l = leases_.find(id); l != leases_.end()) {
                        l->second.extend_at = std::min(l->second.extend_at, retry_at);
                    }
                }
            }
        }
    }

    // Hand messages back for immediate redelivery. Best effort: one attempt.
    void nack_(const std::vector<std::string>& ids) {
        for (std::size_t off = 0; off < ids.size(); off += kAckChunk) {
            const std::size_t end = std::min(off + kAckChunk, ids.size());
            const std::vector<std::string> chunk(ids.begin() + static_cast<long>(off),
                                                 ids.begin() + static_cast<long>(end));
            (void)client_->post(modack_path_, pubsub_modack_body(chunk, 0), "application/json");
        }
    }

    PubSubSourceOptions opts_;
    std::unique_ptr<HttpRequest> client_;
    std::string pull_path_;
    std::string ack_path_;
    std::string modack_path_;
    clink::connectors::CheckpointAckLedger<std::string> ledger_;  // ackIds awaiting :acknowledge
    std::map<std::string, Lease> leases_;  // every held ackId (source thread only)
    std::uint64_t acked_total_{0};
    bool paused_{false};  // at max_outstanding, waiting for a checkpoint to complete
};

}  // namespace clink::http_connector
