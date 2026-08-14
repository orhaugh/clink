#pragma once

// OTLP/HTTP export: the real implementation behind OtelBoundary.
//
// Ships MetricsRegistry snapshots and finished spans to any OpenTelemetry
// collector's OTLP/HTTP JSON endpoints (/v1/metrics, /v1/traces). The wire
// format is the OTLP protobuf-JSON mapping, hand-encoded here the same way
// the rest of the control plane writes JSON - no opentelemetry-cpp, no
// Protobuf, no gRPC. Those dependencies fight this tree's Arrow pin for
// Abseil/Protobuf versions and buy nothing at this volume: a metrics
// snapshot every few seconds and a handful of lifecycle spans.
//
// Deliberate scope:
//   * OFF unless an endpoint is configured. No exporter object, no thread,
//     and SpanBuffer::record() is a no-op, so a build or deployment that
//     never asks for OTel pays nothing.
//   * Lifecycle spans, never per-record spans. The span sites are coarse
//     engine transitions (checkpoint completion, job submission); tracing
//     records would melt any collector and time nothing useful.
//   * Plain HTTP. A TLS hop belongs to a collector sidecar next to the
//     process, which is the standard OTel deployment shape anyway.
//   * Encoders are pure functions of their inputs so tests can pin the
//     wire shape without a collector; the exporter thread is just
//     clock + HttpClient + encoders.

#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/metrics/metrics_registry.hpp"

namespace clink::metrics {

// One finished span. Times are wall-clock nanoseconds since the Unix epoch
// (OTLP's representation). trace_id (32 hex chars) and span_id (16 hex
// chars) are filled by SpanBuffer::record when left empty; callers that
// relate spans set them explicitly.
struct OtlpSpan {
    std::string name;
    std::uint64_t start_unix_nano{0};
    std::uint64_t end_unix_nano{0};
    std::vector<std::pair<std::string, std::string>> attributes;
    bool ok{true};
    std::string trace_id;
    std::string span_id;
};

// Bounded buffer the engine's span sites record into. Disabled by default:
// record() drops immediately until an exporter enables it, so span sites
// cost one atomic load when nobody is exporting. When enabled and full, the
// OLDEST span is dropped and counted (clink_otlp_spans_dropped_total) - a
// slow collector must never grow engine memory.
class SpanBuffer {
public:
    static SpanBuffer& global();

    void set_enabled(bool on) noexcept;
    [[nodiscard]] bool enabled() const noexcept;

    void record(OtlpSpan span);
    [[nodiscard]] std::vector<OtlpSpan> drain();

    static constexpr std::size_t kCapacity = 1024;

private:
    mutable std::mutex mu_;
    std::deque<OtlpSpan> spans_;
    bool enabled_{false};
};

// Convenience for the engine's span sites: wall-clock now in OTLP nanos,
// and "now minus a measured duration" for sites that time a phase and
// record the span at its end.
[[nodiscard]] std::uint64_t otlp_now_unix_nano();

// --- OTLP protobuf-JSON encoders (pure) -------------------------------------

// ExportMetricsServiceRequest. Counters become monotonic cumulative sums,
// gauges become gauges, histograms become cumulative OTLP histograms
// (explicit bounds; the +Inf overflow is the last bucket count, matching
// Histogram::Snapshot's layout). `time_unix_nano` stamps every data point.
[[nodiscard]] std::string otlp_metrics_json(const MetricsRegistry::Snapshot& snap,
                                            std::string_view service_name,
                                            std::uint64_t time_unix_nano);

// ExportTraceServiceRequest over already-finished spans.
[[nodiscard]] std::string otlp_spans_json(const std::vector<OtlpSpan>& spans,
                                          std::string_view service_name);

// Periodic pusher: every `interval_ms`, snapshot the registry and POST it to
// http://host:port/v1/metrics, and drain SpanBuffer::global() to /v1/traces.
// Constructing it enables the span buffer; destroying it disables the buffer
// and joins the thread. Failures increment clink_otlp_export_failures_total
// and are otherwise silent - an export path must never take the engine down
// with it. In a build without the HTTP subsystem the constructor throws
// rather than silently not exporting; callers gate on their own build flag.
class OtlpHttpExporter {
public:
    struct Config {
        std::string host{"127.0.0.1"};
        std::uint16_t port{4318};
        std::string service_name{"clink"};
        std::uint32_t interval_ms{10000};
    };

    explicit OtlpHttpExporter(Config cfg);
    ~OtlpHttpExporter();

    OtlpHttpExporter(const OtlpHttpExporter&) = delete;
    OtlpHttpExporter& operator=(const OtlpHttpExporter&) = delete;

    // One synchronous export pass (also what the loop calls). Exposed so
    // tests drive it deterministically instead of waiting on the clock.
    void export_once();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::metrics
