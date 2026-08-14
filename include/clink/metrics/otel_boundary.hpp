#pragma once

#include <chrono>
#include <functional>
#include <string>

#include "clink/metrics/metrics_registry.hpp"

namespace clink {

// Test/tooling boundary over MetricsRegistry snapshots.
//
// The REAL OpenTelemetry exporter is otlp_export.hpp (OtlpHttpExporter):
// OTLP/HTTP JSON to a collector, hand-encoded, no opentelemetry-cpp - its
// heavy Protobuf/gRPC pins would fight this tree's Arrow pin for nothing at
// this volume. This boundary stays for what it was always used for: letting
// a test or tool substitute its own sink for a registry snapshot without
// caring about any wire format.
class OtelBoundary {
public:
    using ExportFn = std::function<void(const MetricsRegistry::Snapshot&)>;

    explicit OtelBoundary(MetricsRegistry& registry) : registry_(registry) {}

    void set_endpoint(std::string endpoint) { endpoint_ = std::move(endpoint); }
    const std::string& endpoint() const noexcept { return endpoint_; }

    // Test/tooling hook: install a custom sink. With no sink installed the
    // boundary is a no-op.
    void set_export_fn(ExportFn fn) { export_ = std::move(fn); }

    // Push a single snapshot synchronously. The continuous-export loop will
    // be added with the real OTel integration.
    void export_once() const {
        if (export_) {
            export_(registry_.snapshot());
        }
    }

    std::chrono::milliseconds export_interval() const noexcept { return export_interval_; }
    void set_export_interval(std::chrono::milliseconds v) noexcept { export_interval_ = v; }

private:
    MetricsRegistry& registry_;
    std::string endpoint_{};
    ExportFn export_{};
    std::chrono::milliseconds export_interval_{std::chrono::seconds{10}};
};

}  // namespace clink
