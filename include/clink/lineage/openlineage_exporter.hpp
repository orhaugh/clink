#pragma once

// Built-in lineage exporter that ships clink lineage to an OpenLineage
// HTTP receiver (Marquez, or anything that speaks the OpenLineage run-
// event API). OpenLineage is the lingua franca that DataHub, Apache
// Atlas and others also ingest, so this one exporter covers the common
// targets; bespoke backends implement LineageListener directly.
//
// Mapping:
//   * JobStarted  -> a START run event carrying the source datasets as
//                    inputs and the sink datasets as outputs.
//   * JobCompleted -> a COMPLETE / FAIL / ABORT run event (status
//                    ok / failed / cancelled), correlated to the START by
//                    a job-derived runId. Inputs/outputs were established
//                    by the START event, so these carry none.
//
// Delivery is asynchronous: on_event serialises the run event and pushes
// it onto a bounded outbox; a worker thread POSTs from the outbox so the
// EventBus publish thread is never blocked on network I/O. Overflow drops
// the oldest queued event and counts the drop.
//
// Requires the HTTP client (CLINK_HAS_HTTP). Config keys:
//   endpoint   - receiver base URL, e.g. "http://localhost:5000" (http
//                only; required).
//   namespace  - OpenLineage job namespace (default "clink").
//   path       - POST path on the receiver (default "/api/v1/lineage").
//   producer   - producer URI recorded on each event.
//   max_queue  - outbox capacity before drop-oldest (default 1024).

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "clink/lineage/lineage_listener.hpp"

namespace clink::http {
class HttpClient;
}

namespace clink::lineage {

struct OpenLineageConfig {
    std::string host;                     // parsed from endpoint
    std::uint16_t port{80};               // parsed from endpoint
    std::string path{"/api/v1/lineage"};  // POST path on the receiver
    std::string job_namespace{"clink"};   // OpenLineage job namespace
    std::string producer{"https://github.com/clink/clink"};
    std::size_t max_queue{1024};
};

class OpenLineageExporter : public LineageListener {
public:
    explicit OpenLineageExporter(OpenLineageConfig cfg);
    ~OpenLineageExporter() override;

    OpenLineageExporter(const OpenLineageExporter&) = delete;
    OpenLineageExporter& operator=(const OpenLineageExporter&) = delete;

    void on_event(const LineageEvent& ev) override;

    // Observability for tests / ops: events successfully POSTed and
    // events dropped due to a full outbox.
    std::uint64_t sent() const { return sent_.load(); }
    std::uint64_t dropped() const { return dropped_.load(); }

private:
    void run();  // worker loop

    OpenLineageConfig cfg_;
    std::unique_ptr<http::HttpClient> client_;

    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<std::string> outbox_;  // serialised OpenLineage run events
    bool stop_{false};
    std::thread worker_;

    std::atomic<std::uint64_t> sent_{0};
    std::atomic<std::uint64_t> dropped_{0};
};

// Build an OpenLineageConfig from the generic listener config map.
// Returns false when no usable endpoint is configured.
bool parse_openlineage_config(const LineageListenerConfig& cfg, OpenLineageConfig& out);

// Register the "openlineage" factory into the registry. Called by
// register_builtin_lineage_listeners() when HTTP is available.
void register_openlineage_listener(LineageListenerRegistry& registry);

}  // namespace clink::lineage
