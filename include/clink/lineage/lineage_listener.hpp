#pragma once

// Pluggable lineage export.
//
// Capture (extract_lineage, the LineageGraph) is engine-internal and
// vendor-neutral. Export is pluggable: a LineageListener translates the
// graph plus run-state into whatever an external lineage system wants
// (OpenLineage, a catalog, a metadata store). The two are connected by
// the in-process EventBus: the coordinator emits "coordinator.job_lineage" at submit and
// "coordinator.job_completed" at termination; a LineageDispatcher subscribes,
// reconstructs the structured event, and fans it out to the listeners.
//
// Listeners are HOST-side components. They must be constructed inside the
// clink_node process (not a job .so), because a .so links its own private
// EventBus singleton and would never see the host's events.

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "clink/lineage/lineage_graph.hpp"
#include "clink/runtime/event_bus.hpp"

namespace clink::lineage {

// Canonical EventBus types the dispatcher consumes. Shared so the coordinator
// emit-site and the dispatcher agree on the strings.
inline constexpr const char* kEventJobLineage = "coordinator.job_lineage";
inline constexpr const char* kEventJobCompleted = "coordinator.job_completed";

// A run-level lineage event delivered to listeners.
struct LineageEvent {
    enum class Kind { JobStarted, JobCompleted };
    Kind kind{Kind::JobStarted};
    std::int64_t ts_ms{0};
    std::uint64_t job_id{0};
    // Human-readable job name from the submitted spec; empty when the job
    // was submitted without one (consumers fall back to the id).
    std::string job_name;
    // JobStarted only: the lineage graph. Empty for JobCompleted.
    LineageGraph graph;
    // JobCompleted only: "ok" | "failed" | "cancelled". Empty otherwise.
    std::string status;
    std::string error;  // optional failure detail
};

// A consumer of lineage events. Implement this to ship lineage to an
// external system. on_event is called on the dispatcher's delivery path,
// which runs on the EventBus publish thread; an implementation that does
// network I/O MUST NOT block there - queue and return (see
// OpenLineageExporter for the reference outbox shape).
class LineageListener {
public:
    virtual ~LineageListener() = default;
    virtual void on_event(const LineageEvent& ev) = 0;
};

using LineageListenerConfig = std::map<std::string, std::string>;

// Registry of named listener factories. Mirrors the engine's other
// *Registry singletons: built-ins register at startup; the host process
// (clink_node) looks them up by name from config and constructs them.
class LineageListenerRegistry {
public:
    using Factory = std::function<std::unique_ptr<LineageListener>(const LineageListenerConfig&)>;

    void register_factory(std::string name, Factory factory);
    bool contains(const std::string& name) const;
    std::vector<std::string> names() const;
    // Returns nullptr if no factory is registered under `name`.
    std::unique_ptr<LineageListener> create(const std::string& name,
                                            const LineageListenerConfig& cfg) const;

    static LineageListenerRegistry& global();

private:
    std::map<std::string, Factory> factories_;
};

// Register all built-in lineage listeners into the registry. Currently the
// "openlineage" HTTP exporter, present only when the build includes the
// HTTP client. Call once on the host before constructing a dispatcher.
void register_builtin_lineage_listeners(
    LineageListenerRegistry& registry = LineageListenerRegistry::global());

// Bridges the EventBus to a set of listeners. Subscribes on construction
// and unsubscribes on destruction (RAII). Construct once on the host and
// keep it alive for as long as lineage export should run. The `bus`
// parameter exists for testing; production uses EventBus::global().
class LineageDispatcher {
public:
    explicit LineageDispatcher(std::vector<std::unique_ptr<LineageListener>> listeners,
                               EventBus& bus = EventBus::global());

    LineageDispatcher(const LineageDispatcher&) = delete;
    LineageDispatcher& operator=(const LineageDispatcher&) = delete;

    std::size_t listener_count() const { return listeners_.size(); }

private:
    void on_bus_event(const Event& e);

    std::vector<std::unique_ptr<LineageListener>> listeners_;
    Subscription sub_;
};

}  // namespace clink::lineage
