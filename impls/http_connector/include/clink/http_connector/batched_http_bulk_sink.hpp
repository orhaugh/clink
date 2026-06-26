#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <map>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "clink/http_connector/http_bulk_post.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::http_connector {

// How a batch of rendered records is framed into one request body.
enum class BulkFraming {
    JsonArray,  // [rec0,rec1,...]  (records are JSON values)
    Ndjson,     // rec0\nrec1\n...  (newline-delimited; records may be multi-line,
                // e.g. the Elasticsearch _bulk action+doc pair)
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
        bool flush_on_barrier{true};      // align delivery to checkpoints (at-least-once)
        ResponseValidator response_ok{};  // empty -> 2xx is success
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
        buffer_.clear();
        count_ = 0;
    }

    void on_data(const Batch<In>& batch) override {
        for (const auto& rec : batch) {
            append_(rec.value());
            if (count_ >= opts_.max_records || buffer_.size() >= opts_.max_bytes) {
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
        if (count_ == 0) {
            return;
        }
        std::string body = opts_.framing == BulkFraming::JsonArray ? "[" + buffer_ + "]" : buffer_;
        post_with_retry_(body);
        buffer_.clear();
        count_ = 0;
    }

    std::string name() const override { return opts_.name; }

private:
    void append_(const In& rec) {
        if (opts_.framing == BulkFraming::JsonArray) {
            if (count_ > 0) {
                buffer_.push_back(',');
            }
            render_(buffer_, rec);
        } else {  // Ndjson
            render_(buffer_, rec);
            buffer_.push_back('\n');
        }
        ++count_;
    }

    void post_with_retry_(const std::string& body) {
        // opts_.max_retries was clamped in the ctor; the shared helper owns the
        // backoff + throw-on-exhaustion (at-least-once) semantics.
        RetryPolicy policy{opts_.max_retries, opts_.retry_base_backoff};
        clink::http_connector::post_with_retry(*client_,
                                               opts_.path,
                                               body,
                                               opts_.content_type,
                                               policy,
                                               opts_.response_ok,
                                               opts_.name,
                                               opts_.http.base_url);
    }

    Options opts_;
    Renderer render_;
    std::unique_ptr<HttpRequest> client_;
    std::string buffer_;
    std::size_t count_{0};
};

}  // namespace clink::http_connector
