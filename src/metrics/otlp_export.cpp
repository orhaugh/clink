#include "clink/metrics/otlp_export.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <random>
#include <stdexcept>
#include <thread>

#ifdef CLINK_HAS_HTTP
#include "clink/http/http_client.hpp"
#endif

namespace clink::metrics {

namespace {

// Protobuf-JSON escaping for the strings we emit (metric names, span names,
// attribute values). Same minimal set the rest of the control plane escapes.
std::string jq(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 2);
    out += '"';
    for (const char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof buf, "\\u%04x", c);
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    out += '"';
    return out;
}

// 64-bit integers travel as JSON strings in the protobuf-JSON mapping
// (asInt, count, timeUnixNano, bucketCounts); doubles as plain numbers.
std::string ju64(std::uint64_t v) {
    return "\"" + std::to_string(v) + "\"";
}

std::string jdouble(double v) {
    char buf[64];
    std::snprintf(buf, sizeof buf, "%.17g", v);
    return buf;
}

std::string resource_json(std::string_view service_name) {
    return R"({"attributes":[{"key":"service.name","value":{"stringValue":)" + jq(service_name) +
           "}}]}";
}

// Random hex of the requested width. Span identity does not need
// cryptographic strength, only non-collision at lifecycle-span volume.
std::string random_hex(std::size_t chars) {
    static std::mutex mu;
    static std::mt19937_64 rng{std::random_device{}()};
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out(chars, '0');
    std::lock_guard lock(mu);
    for (auto& c : out) {
        c = kHex[rng() & 0xF];
    }
    return out;
}

}  // namespace

SpanBuffer& SpanBuffer::global() {
    static SpanBuffer instance;
    return instance;
}

void SpanBuffer::set_enabled(bool on) noexcept {
    std::lock_guard lock(mu_);
    enabled_ = on;
    if (!on) {
        spans_.clear();
    }
}

bool SpanBuffer::enabled() const noexcept {
    std::lock_guard lock(mu_);
    return enabled_;
}

void SpanBuffer::record(OtlpSpan span) {
    std::lock_guard lock(mu_);
    if (!enabled_) {
        return;
    }
    if (span.trace_id.empty()) {
        span.trace_id = random_hex(32);
    }
    if (span.span_id.empty()) {
        span.span_id = random_hex(16);
    }
    if (spans_.size() >= kCapacity) {
        spans_.pop_front();
        MetricsRegistry::global().counter("clink_otlp_spans_dropped_total").increment();
    }
    spans_.push_back(std::move(span));
}

std::vector<OtlpSpan> SpanBuffer::drain() {
    std::lock_guard lock(mu_);
    std::vector<OtlpSpan> out(std::make_move_iterator(spans_.begin()),
                              std::make_move_iterator(spans_.end()));
    spans_.clear();
    return out;
}

std::uint64_t otlp_now_unix_nano() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          std::chrono::system_clock::now().time_since_epoch())
                                          .count());
}

std::string otlp_metrics_json(const MetricsRegistry::Snapshot& snap,
                              std::string_view service_name,
                              std::uint64_t time_unix_nano) {
    const auto t = ju64(time_unix_nano);
    std::string metrics;
    bool first = true;
    const auto sep = [&] {
        if (!first) {
            metrics += ',';
        }
        first = false;
    };
    for (const auto& [name, value] : snap.counters) {
        sep();
        metrics += "{\"name\":" + jq(name) + ",\"sum\":{\"dataPoints\":[{\"asInt\":" + ju64(value) +
                   ",\"timeUnixNano\":" + t +
                   "}],\"aggregationTemporality\":2,\"isMonotonic\":true}}";
    }
    for (const auto& [name, value] : snap.gauges) {
        sep();
        metrics += "{\"name\":" + jq(name) + ",\"gauge\":{\"dataPoints\":[{\"asInt\":\"" +
                   std::to_string(value) + "\",\"timeUnixNano\":" + t + "}]}}";
    }
    for (const auto& h : snap.histograms) {
        sep();
        std::string bounds;
        for (std::size_t i = 0; i < h.data.upper_bounds.size(); ++i) {
            if (i > 0) {
                bounds += ',';
            }
            bounds += jdouble(h.data.upper_bounds[i]);
        }
        std::string buckets;
        for (std::size_t i = 0; i < h.data.bucket_counts.size(); ++i) {
            if (i > 0) {
                buckets += ',';
            }
            buckets += ju64(h.data.bucket_counts[i]);
        }
        metrics += "{\"name\":" + jq(h.name) +
                   ",\"histogram\":{\"dataPoints\":[{\"count\":" + ju64(h.data.count) +
                   ",\"sum\":" + jdouble(h.data.sum) + ",\"bucketCounts\":[" + buckets +
                   "],\"explicitBounds\":[" + bounds + "],\"timeUnixNano\":" + t +
                   "}],\"aggregationTemporality\":2}}";
    }
    return "{\"resourceMetrics\":[{\"resource\":" + resource_json(service_name) +
           ",\"scopeMetrics\":[{\"scope\":{\"name\":\"clink\"},\"metrics\":[" + metrics + "]}]}]}";
}

std::string otlp_spans_json(const std::vector<OtlpSpan>& spans, std::string_view service_name) {
    std::string body;
    for (std::size_t i = 0; i < spans.size(); ++i) {
        const auto& s = spans[i];
        if (i > 0) {
            body += ',';
        }
        std::string attrs;
        for (std::size_t a = 0; a < s.attributes.size(); ++a) {
            if (a > 0) {
                attrs += ',';
            }
            attrs += "{\"key\":" + jq(s.attributes[a].first) +
                     ",\"value\":{\"stringValue\":" + jq(s.attributes[a].second) + "}}";
        }
        body += "{\"traceId\":" + jq(s.trace_id) + ",\"spanId\":" + jq(s.span_id) +
                ",\"name\":" + jq(s.name) +
                ",\"kind\":1,\"startTimeUnixNano\":" + ju64(s.start_unix_nano) +
                ",\"endTimeUnixNano\":" + ju64(s.end_unix_nano) + ",\"attributes\":[" + attrs +
                "],\"status\":{\"code\":" + (s.ok ? "1" : "2") + "}}";
    }
    return "{\"resourceSpans\":[{\"resource\":" + resource_json(service_name) +
           ",\"scopeSpans\":[{\"scope\":{\"name\":\"clink\"},\"spans\":[" + body + "]}]}]}";
}

#ifdef CLINK_HAS_HTTP

struct OtlpHttpExporter::Impl {
    Config cfg;
    std::mutex mu;
    std::condition_variable cv;
    bool stop{false};
    std::thread loop;

    void run() {
        std::unique_lock lock(mu);
        while (!stop) {
            cv.wait_for(lock, std::chrono::milliseconds(cfg.interval_ms), [this] { return stop; });
            if (stop) {
                return;
            }
            lock.unlock();
            do_export();
            lock.lock();
        }
    }

    void do_export() {
        http::HttpClient client(cfg.host, cfg.port);
        bool ok = true;
        {
            const auto body = otlp_metrics_json(
                MetricsRegistry::global().snapshot(), cfg.service_name, otlp_now_unix_nano());
            const auto resp = client.post("/v1/metrics", body);
            ok = ok && resp.status >= 200 && resp.status < 300;
        }
        if (auto spans = SpanBuffer::global().drain(); !spans.empty()) {
            const auto body = otlp_spans_json(spans, cfg.service_name);
            const auto resp = client.post("/v1/traces", body);
            ok = ok && resp.status >= 200 && resp.status < 300;
        }
        MetricsRegistry::global()
            .counter(ok ? "clink_otlp_exports_total" : "clink_otlp_export_failures_total")
            .increment();
    }
};

OtlpHttpExporter::OtlpHttpExporter(Config cfg) : impl_(std::make_unique<Impl>()) {
    impl_->cfg = std::move(cfg);
    SpanBuffer::global().set_enabled(true);
    impl_->loop = std::thread([this] { impl_->run(); });
}

OtlpHttpExporter::~OtlpHttpExporter() {
    {
        std::lock_guard lock(impl_->mu);
        impl_->stop = true;
    }
    impl_->cv.notify_all();
    if (impl_->loop.joinable()) {
        impl_->loop.join();
    }
    SpanBuffer::global().set_enabled(false);
}

void OtlpHttpExporter::export_once() {
    impl_->do_export();
}

#else  // !CLINK_HAS_HTTP

// No HTTP subsystem in this build: constructing an exporter is a caller
// error and says so, rather than accepting a config it cannot honour.
struct OtlpHttpExporter::Impl {};

OtlpHttpExporter::OtlpHttpExporter(Config) {
    throw std::runtime_error(
        "OtlpHttpExporter: this build has no HTTP subsystem; rebuild with HTTP enabled");
}

OtlpHttpExporter::~OtlpHttpExporter() = default;

void OtlpHttpExporter::export_once() {}

#endif  // CLINK_HAS_HTTP

}  // namespace clink::metrics
