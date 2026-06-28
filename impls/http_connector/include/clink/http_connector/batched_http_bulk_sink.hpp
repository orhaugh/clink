#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/http_connector/http_bulk_post.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::http_connector {

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
template <typename In>
class BatchedHttpBulkSink : public Sink<In> {
public:
    // Render ONE record into `out` (no framing separators - the base adds
    // those). Must append, not overwrite.
    using Renderer = std::function<void(std::string& out, const In& rec)>;

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
        : opts_(std::move(opts)), render_(std::move(render)) {
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

    void open() override {
        client_ = std::make_unique<HttpRequest>(opts_.http);
        fragments_.clear();
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
        for (int attempt = 0; attempt <= opts_.max_retries; ++attempt) {
            if (attempt > 0) {
                std::this_thread::sleep_for(backoff_delay(attempt, opts_.retry_base_backoff));
            }
            last = client_->post(opts_.path, build_body_(active), opts_.content_type);
            BulkResult r = opts_.response_handler
                               ? opts_.response_handler(last, active.size())
                               : whole_batch_result(last, active.size(), opts_.response_ok);
            if (r.ok) {
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
                                         opts_.path + " (" + delivery_detail_(last) + ")",
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
                            opts_.path + " (" + delivery_detail_(last) + ")",
                        last);
                }
            }
            // Everything else this round either succeeded or was dropped; only the
            // transient failures get resent.
            std::vector<std::size_t> next;
            next.reserve(r.failed_transient.size());
            for (std::size_t rel : r.failed_transient) {
                if (rel < active.size()) {
                    next.push_back(active[rel]);
                }
            }
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
                                    " attempts (" + delivery_detail_(last) + ")",
                                last);
    }

    std::string name() const override { return opts_.name; }

private:
    void append_(const In& rec) {
        if (fragments_.empty()) {
            first_buffered_at_ = std::chrono::steady_clock::now();  // age clock for linger
        }
        std::string frag;
        render_(frag, rec);
        pending_bytes_ += frag.size();
        fragments_.push_back(std::move(frag));
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
        pending_bytes_ = 0;
    }

    static std::string delivery_detail_(const HttpResponse& res) {
        std::string detail = res.status == 0 ? res.error : "HTTP " + std::to_string(res.status);
        if (res.status >= 200 && res.status < 300 && !res.body.empty()) {
            detail += " body: " + res.body.substr(0, 256);
        }
        return detail;
    }

    Options opts_;
    Renderer render_;
    std::unique_ptr<HttpRequest> client_;
    std::vector<std::string> fragments_;  // per-record rendered bodies (for subset resend)
    std::size_t pending_bytes_{0};        // sum of fragment sizes (flush trigger + bytes metric)
    std::chrono::steady_clock::time_point
        first_buffered_at_{};  // age of the oldest buffered record
};

}  // namespace clink::http_connector
