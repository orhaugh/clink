#pragma once

#include <chrono>
#include <functional>
#include <string>

#include "clink/metrics/metrics_registry.hpp"

namespace clink {

// Boundary class for the future OpenTelemetry exporter.
//
// We do not pull in opentelemetry-cpp yet - it has heavy build dependencies
// (Protobuf, gRPC) and the right time to add it is when we have a real
// production deployment to feed. For now, the boundary type exposes the shape
// of the eventual exporter so tests and tooling can use it without needing
// the OTel libraries:
//
//   * register_otlp_endpoint(...)  - configure where metrics will be shipped
//   * export_loop(...)             - poll the registry and call the exporter
//   * sink callback                - test harness can substitute its own sink
//
// When the real OTel integration lands, only the implementation moves; this
// header stays.
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
