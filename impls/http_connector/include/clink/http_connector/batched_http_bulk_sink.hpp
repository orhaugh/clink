#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include "clink/fault/fault_injection.hpp"
#include "clink/http_connector/http_bulk_post.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::http_connector {

// Thrown by a renderer for a record it cannot write the way the sink promises,
// such as an Elasticsearch record without the field document_id names, which
// would be indexed under a generated _id that a replay then duplicates. The
// record would be refused the same way on every replay, so it is poison: under
// dlq=drop it is dead-lettered like a record the endpoint refused, and
// otherwise the sink fails.
struct UnrenderableRecord : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// How a batch of rendered records is framed into one request body.
enum class BulkFraming {
    JsonArray,       // [rec0,rec1,...]  (records are JSON values)
    Ndjson,          // rec0\nrec1\n...  (newline-delimited; records may be multi-line,
                     // e.g. the Elasticsearch _bulk action+doc pair)
    PubSubMessages,  // {"messages":[rec0,rec1,...]}  (each record is a Pub/Sub
                     // PubsubMessage JSON object, e.g. {"data":"<base64>"})
};

// Reusable base for at-least-once HTTP bulk sinks. A subclass (or a factory)
// supplies a per-record renderer; the base owns buffering, framing, flush
// triggers, the keep-alive HTTP client, and bounded retry+backoff.
//
// Delivery: AT-LEAST-ONCE. flush() runs on every checkpoint barrier (before the
// sink acks, so data is delivered no later than the checkpoint that covers it)
// and at end-of-stream, plus whenever the buffered count/bytes thresholds trip
// (latency + memory bounding). On permanent failure (retries exhausted) it
// THROWS rather than dropping, so the runner fails the subtask and the job
// replays from the last checkpoint - no silent loss. Duplicates on replay are
// the expected at-least-once trade-off; idempotent writes (a stable doc id) are
// how a subclass upgrades toward effectively-once.
//
// Records the endpoint applies by key (an Elasticsearch document id) name that
// key through a KeyedRenderer. A retry then resends a failed record together
// with every later record of the flush that names the same key, in order, so
// the resend cannot leave a document at an older record's value. Records with
// no key keep the plain rule: only the failed ones are resent.
//
// The pause before a retry ends early when the task is cancelled, and the
// flush then throws with its batch still pending. Fault points
// "<name>.before_write" (an attempt's body is built and not yet sent) and
// "<name>.after_write" (the endpoint confirmed every record of the flush, which
// has not yet returned) let recovery tests stop a task at either side of a write.
template <typename In>
class BatchedHttpBulkSink : public Sink<In> {
public:
    // Render ONE record into `out` (no framing separators - the base adds
    // those). Must append, not overwrite.
    using Renderer = std::function<void(std::string& out, const In& rec)>;
    // As Renderer, and also set `key` to what the endpoint applies the record by
    // (an Elasticsearch _id), or leave it empty for a record it does not.
    using KeyedRenderer = std::function<void(std::string& out, std::string& key, const In& rec)>;

    // Decides whether a response counts as a successful delivery (shared alias;
    // see http_bulk_post.hpp). Default (empty) = any 2xx.
    using ResponseValidator = clink::http_connector::ResponseValidator;

    struct Options {
        HttpRequest::Options http;  // base_url + headers + timeouts + verify_tls
        std::string path{"/"};      // request path
        std::string content_type{"application/json"};
        BulkFraming framing{BulkFraming::JsonArray};
        std::size_t max_records{500};  // flush when this many buffered
        std::size_t max_bytes{4 * 1024 * 1024};
        int max_retries{4};  // attempts beyond the first
        std::chrono::milliseconds retry_base_backoff{200};
        // Linger: flush a partial batch once its OLDEST record is this old, even
        // below max_records/max_bytes - bounds latency on a sparse stream that
        // would otherwise wait for the checkpoint barrier. 0 = disabled. Only
        // evaluated when a record arrives (no timer thread), so a fully-idle
        // buffer still waits for the barrier.
        std::chrono::milliseconds max_age{0};
        bool flush_on_barrier{true};      // align delivery to checkpoints (at-least-once)
        ResponseValidator response_ok{};  // empty -> 2xx is success (whole-batch)
        // Per-item response handler (e.g. Elasticsearch _bulk items[] parsing).
        // When set it takes precedence over response_ok: a partial failure resends
        // ONLY the failed-transient records and DLQs/throws the failed-permanent
        // ones, instead of re-sending the whole batch (which re-indexes the
        // already-written records). Empty -> whole-batch via response_ok.
        ResponseHandler response_handler{};
        DlqPolicy dlq_policy{DlqPolicy::Fail};  // permanent (4xx) failure: throw vs drop
        std::string name{"http_bulk_sink"};
    };

    // Upper bound on retry attempts (see RetryPolicy::kMaxRetries).
    static constexpr int kMaxRetries = RetryPolicy::kMaxRetries;

    BatchedHttpBulkSink(Options opts, Renderer render)
        : BatchedHttpBulkSink(
              std::move(opts),
              KeyedRenderer{[r = std::move(render)](std::string& out,
                                                    std::string& /*key*/,
                                                    const In& rec) { r(out, rec); }},
              /*keyed=*/false) {}

    BatchedHttpBulkSink(Options opts, KeyedRenderer render)
        : BatchedHttpBulkSink(std::move(opts), std::move(render), /*keyed=*/true) {}

private:
    BatchedHttpBulkSink(Options opts, KeyedRenderer render, bool keyed)
        : opts_(std::move(opts)),
          render_(std::move(render)),
          keyed_(keyed),
          before_write_point_(opts_.name + ".before_write"),
          after_write_point_(opts_.name + ".after_write") {
        // Clamp user-supplied max_retries to a sane range: negatives would skip
        // the POST loop entirely (throw without ever sending); large values
        // would overflow the backoff shift / schedule a multi-year sleep.
        if (opts_.max_retries < 0) {
            opts_.max_retries = 0;
        } else if (opts_.max_retries > kMaxRetries) {
            opts_.max_retries = kMaxRetries;
        }
        if (!HttpRequest::tls_supported() && opts_.http.base_url.rfind("https://", 0) == 0) {
            throw std::runtime_error(opts_.name +
                                     ": https URL but this build has no TLS support (rebuild the "
                                     "http_connector module with OpenSSL)");
        }
    }

public:
    void open() override {
        client_ = std::make_unique<HttpRequest>(opts_.http);
        fragments_.clear();
        keys_.clear();
        pending_bytes_ = 0;
    }

    void on_data(const Batch<In>& batch) override {
        for (const auto& rec : batch) {
            append_(rec.value());
            if (fragments_.size() >= opts_.max_records || pending_bytes_ >= opts_.max_bytes ||
                linger_elapsed_()) {
                flush();
            }
        }
    }

    void on_barrier(CheckpointBarrier /*b*/) override {
        if (opts_.flush_on_barrier) {
            flush();
        }
    }
    // With the barrier flush on, as every factory sets it, the barrier writes
    // out everything received before it, which is the at-least-once guarantee
    // only while the sink owns its chain's checkpoint, so Dag::add_sink refuses
    // another sink beside it.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override {
        return opts_.flush_on_barrier;
    }

    void flush() override {
        if (fragments_.empty()) {
            return;
        }
        const std::size_t n = fragments_.size();
        // Indices into fragments_ still to deliver this flush. Each POST sends the
        // active set; the response handler reports which of THOSE records failed,
        // and only the transient failures stay active for the next attempt - the
        // succeeded records are never re-sent.
        std::vector<std::size_t> active(n);
        for (std::size_t i = 0; i < n; ++i) {
            active[i] = i;
        }
        std::size_t dropped = 0;
        std::size_t dropped_bytes = 0;
        const auto t0 = std::chrono::steady_clock::now();
        HttpResponse last;
        std::string why;  // the latest verdict's own account of the response
        for (int attempt = 0; attempt <= opts_.max_retries; ++attempt) {
            if (attempt > 0) {
                wait_before_retry_(attempt, last, why);
            }
            (void)CLINK_FAULT_POINT(before_write_point_);
            last = client_->post(opts_.path, build_body_(active), opts_.content_type);
            BulkResult r = opts_.response_handler
                               ? opts_.response_handler(last, active.size())
                               : whole_batch_result(last, active.size(), opts_.response_ok);
            why = std::move(r.detail);
            if (r.ok) {
                (void)CLINK_FAULT_POINT(after_write_point_);
                finish_success_(n, dropped, dropped_bytes, t0);
                return;
            }
            // Permanent (poison) records: DLQ under Drop, throw under Fail. The
            // rel < active.size() guards defend the public response_handler seam:
            // the in-tree handlers only ever return in-range indices, but a
            // misbehaving custom handler must not index out of bounds (UB).
            if (!r.failed_permanent.empty()) {
                if (opts_.dlq_policy == DlqPolicy::Drop) {
                    std::size_t valid = 0;
                    auto* rt = this->runtime();
                    for (std::size_t rel : r.failed_permanent) {
                        if (rel >= active.size()) {
                            continue;
                        }
                        const std::string& frag = fragments_[active[rel]];
                        dropped_bytes += frag.size();
                        ++valid;
                        // Route the poison record to the DLQ before it is dropped, so
                        // it is surfaced (logged by default) rather than vanishing.
                        if (rt != nullptr) {
                            rt->report_bad_record(clink::BadRecord{
                                .payload = frag,
                                .error = "permanently rejected by " + opts_.http.base_url +
                                         opts_.path + " (" + delivery_detail_(last, why) + ")",
                                .connector = opts_.name,
                                .direction = "sink",
                                .location = opts_.http.base_url + opts_.path});
                        }
                    }
                    dropped += valid;
                    if (valid > 0) {
                        clink::metrics::connector::dropped_records_inc(opts_.name, valid);
                        clink::metrics::connector::permanent_failures_inc(opts_.name);
                    }
                } else {
                    clink::metrics::connector::error_inc(opts_.name, "sink");
                    throw HttpDeliveryError(
                        opts_.name + ": " + std::to_string(r.failed_permanent.size()) +
                            " record(s) permanently rejected by " + opts_.http.base_url +
                            opts_.path + " (" + delivery_detail_(last, why) + ")",
                        last);
                }
            }
            // Everything else this round either succeeded or was dropped; the
            // transient failures get resent, with the later records of their keys.
            std::vector<std::size_t> next = resend_set_(active, r);
            if (next.empty()) {
                finish_success_(n, dropped, dropped_bytes, t0);
                return;
            }
            active = std::move(next);
        }
        // Retries exhausted with transient survivors: an outage -> throw so the
        // job replays from the last checkpoint (at-least-once, never silent drop).
        clink::metrics::connector::error_inc(opts_.name, "sink");
        throw HttpDeliveryError(opts_.name + ": POST " + opts_.http.base_url + opts_.path +
                                    " failed after " + std::to_string(opts_.max_retries + 1) +
                                    " attempts (" + delivery_detail_(last, why) + ")",
                                last);
    }

    std::string name() const override { return opts_.name; }

private:
    // How often the pause before a retry looks at the task's cancel signal.
    static constexpr std::chrono::milliseconds kCancelPoll{50};

    // The pause before retry `attempt`, cut short when the task is cancelled (a
    // cancelled job, or one restarting after another task failed), which then
    // throws instead of sleeping through the rest of the retries. The batch
    // stays pending: nothing of it counts as written.
    void wait_before_retry_(int attempt, const HttpResponse& last, const std::string& why) const {
        const auto until =
            std::chrono::steady_clock::now() + backoff_delay(attempt, opts_.retry_base_backoff);
        const auto* rt = this->runtime();
        for (;;) {
            if (rt != nullptr && rt->cancel_requested()) {
                throw HttpDeliveryError(opts_.name + ": cancelled while retrying a POST to " +
                                            opts_.http.base_url + opts_.path + " (" +
                                            delivery_detail_(last, why) + ")",
                                        last);
            }
            const auto now = std::chrono::steady_clock::now();
            if (now >= until) {
                return;
            }
            std::this_thread::sleep_for(
                std::min<std::chrono::steady_clock::duration>(kCancelPoll, until - now));
        }
    }

    // The records to send again after an attempt, as positions in fragments_ in
    // batch order: those the attempt failed transiently and, when records carry
    // keys, every later record of `active` naming the same key as one of them,
    // even one this attempt wrote. Resent alone, the earlier record would land
    // after the later ones and leave the key at an older record's value. A
    // record refused for good is not resent.
    std::vector<std::size_t> resend_set_(const std::vector<std::size_t>& active,
                                         const BulkResult& r) const {
        std::vector<char> again(active.size(), 0);
        for (std::size_t rel : r.failed_transient) {
            if (rel < active.size()) {
                again[rel] = 1;
            }
        }
        if (keyed_) {
            std::vector<char> refused(active.size(), 0);
            for (std::size_t rel : r.failed_permanent) {
                if (rel < active.size()) {
                    refused[rel] = 1;
                }
            }
            std::unordered_set<std::string_view> resent_keys;
            for (std::size_t rel = 0; rel < active.size(); ++rel) {
                const std::string& key = keys_[active[rel]];
                if (key.empty()) {
                    continue;
                }
                if (again[rel] != 0) {
                    resent_keys.insert(key);
                } else if (refused[rel] == 0 && resent_keys.count(key) != 0) {
                    again[rel] = 1;
                }
            }
        }
        std::vector<std::size_t> next;
        for (std::size_t rel = 0; rel < active.size(); ++rel) {
            if (again[rel] != 0) {
                next.push_back(active[rel]);
            }
        }
        return next;
    }

    void append_(const In& rec) {
        if (fragments_.empty()) {
            first_buffered_at_ = std::chrono::steady_clock::now();  // age clock for linger
        }
        std::string frag;
        std::string key;
        try {
            render_(frag, key, rec);
        } catch (const UnrenderableRecord& e) {
            clink::metrics::connector::error_inc(opts_.name, "sink");
            if (opts_.dlq_policy != DlqPolicy::Drop) {
                throw;
            }
            if (auto* rt = this->runtime(); rt != nullptr) {
                std::string payload;
                if constexpr (std::is_same_v<In, std::string>) {
                    payload = rec;
                }
                rt->report_bad_record(
                    clink::BadRecord{.payload = std::move(payload),
                                     .error = e.what(),
                                     .connector = opts_.name,
                                     .direction = "sink",
                                     .location = opts_.http.base_url + opts_.path});
            }
            clink::metrics::connector::dropped_records_inc(opts_.name, 1);
            return;
        }
        pending_bytes_ += frag.size();
        fragments_.push_back(std::move(frag));
        if (keyed_) {
            keys_.push_back(std::move(key));
        }
    }

    bool linger_elapsed_() const {
        return opts_.max_age.count() > 0 && !fragments_.empty() &&
               std::chrono::steady_clock::now() - first_buffered_at_ >= opts_.max_age;
    }

    // Frame the active fragments into one request body.
    std::string build_body_(const std::vector<std::size_t>& active) const {
        std::string body;
        if (opts_.framing == BulkFraming::JsonArray) {
            body.push_back('[');
            bool first = true;
            for (std::size_t idx : active) {
                if (!first) {
                    body.push_back(',');
                }
                first = false;
                body += fragments_[idx];
            }
            body.push_back(']');
        } else if (opts_.framing == BulkFraming::PubSubMessages) {
            body += "{\"messages\":[";
            bool first = true;
            for (std::size_t idx : active) {
                if (!first) {
                    body.push_back(',');
                }
                first = false;
                body += fragments_[idx];
            }
            body += "]}";
        } else {  // Ndjson: every record (incl. the last) is newline-terminated.
            for (std::size_t idx : active) {
                body += fragments_[idx];
                body.push_back('\n');
            }
        }
        return body;
    }

    void finish_success_(std::size_t n,
                         std::size_t dropped,
                         std::size_t dropped_bytes,
                         std::chrono::steady_clock::time_point t0) {
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        const std::size_t delivered = n - dropped;
        if (delivered > 0) {
            clink::metrics::connector::records_out_inc(opts_.name, delivered);
            clink::metrics::connector::bytes_out_inc(opts_.name, pending_bytes_ - dropped_bytes);
            clink::metrics::connector::commit_latency_observe(opts_.name,
                                                              static_cast<std::uint64_t>(dt));
        }
        fragments_.clear();
        keys_.clear();
        pending_bytes_ = 0;
    }

    // The HTTP status (or transport error), a 2xx body's opening, and what the
    // response handler made of the response.
    static std::string delivery_detail_(const HttpResponse& res, const std::string& why) {
        std::string detail = res.status == 0 ? res.error : "HTTP " + std::to_string(res.status);
        if (res.status >= 200 && res.status < 300 && !res.body.empty()) {
            detail += " body: " + res.body.substr(0, 256);
        }
        if (!why.empty()) {
            detail += "; " + why;
        }
        return detail;
    }

    Options opts_;
    KeyedRenderer render_;
    bool keyed_{false};               // the renderer names a key per record
    std::string before_write_point_;  // fault point: an attempt about to be sent
    std::string after_write_point_;   // fault point: the flush confirmed, not returned
    std::unique_ptr<HttpRequest> client_;
    std::vector<std::string> fragments_;  // per-record rendered bodies (for subset resend)
    std::vector<std::string> keys_;       // per-record keys when keyed_, else empty
    std::size_t pending_bytes_{0};        // sum of fragment sizes (flush trigger + bytes metric)
    std::chrono::steady_clock::time_point
        first_buffered_at_{};  // age of the oldest buffered record
};

}  // namespace clink::http_connector
