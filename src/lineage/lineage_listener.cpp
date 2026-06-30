#include "clink/lineage/lineage_listener.hpp"

#include "clink/config/json.hpp"
#include "clink/runtime/log_buffer.hpp"

#ifdef CLINK_HAS_HTTP
#include "clink/lineage/openlineage_exporter.hpp"
#endif

namespace clink::lineage {

void LineageListenerRegistry::register_factory(std::string name, Factory factory) {
    factories_[std::move(name)] = std::move(factory);
}

bool LineageListenerRegistry::contains(const std::string& name) const {
    return factories_.find(name) != factories_.end();
}

std::vector<std::string> LineageListenerRegistry::names() const {
    std::vector<std::string> out;
    out.reserve(factories_.size());
    for (const auto& [name, _] : factories_) {
        out.push_back(name);
    }
    return out;
}

std::unique_ptr<LineageListener> LineageListenerRegistry::create(
    const std::string& name, const LineageListenerConfig& cfg) const {
    auto it = factories_.find(name);
    if (it == factories_.end()) {
        return nullptr;
    }
    return it->second(cfg);
}

LineageListenerRegistry& LineageListenerRegistry::global() {
    static LineageListenerRegistry registry;
    return registry;
}

void register_builtin_lineage_listeners(LineageListenerRegistry& registry) {
#ifdef CLINK_HAS_HTTP
    register_openlineage_listener(registry);
#else
    (void)registry;
#endif
}

LineageDispatcher::LineageDispatcher(std::vector<std::unique_ptr<LineageListener>> listeners,
                                     EventBus& bus)
    : listeners_(std::move(listeners)) {
    const auto id = bus.subscribe_raw([this](const Event& e) { on_bus_event(e); });
    sub_ = Subscription(bus, id);
}

void LineageDispatcher::on_bus_event(const Event& e) {
    // Keep this lightweight: it runs under the EventBus mutex on the
    // publishing thread. Parsing a lineage payload (a handful of nodes) is
    // cheap; each listener is responsible for not blocking here.
    if (listeners_.empty()) {
        return;
    }
    LineageEvent ev;
    ev.ts_ms = e.ts_ms;
    try {
        if (e.type == kEventJobLineage) {
            const auto root = config::parse(e.payload);
            ev.kind = LineageEvent::Kind::JobStarted;
            ev.job_id = static_cast<std::uint64_t>(root.int_or("job_id", 0));
            if (root.contains("lineage")) {
                ev.graph = LineageGraph::from_json(root.at("lineage").serialize());
            }
        } else if (e.type == kEventJobCompleted) {
            const auto root = config::parse(e.payload);
            ev.kind = LineageEvent::Kind::JobCompleted;
            ev.job_id = static_cast<std::uint64_t>(root.int_or("job_id", 0));
            ev.status = root.string_or("status", "");
            // The completed event carries an "errors" array; surface the
            // first as the failure detail.
            if (root.contains("errors") && root.at("errors").is_array() &&
                !root.at("errors").as_array().empty()) {
                const auto& first = root.at("errors").as_array().front();
                if (first.is_string()) {
                    ev.error = first.as_string();
                }
            }
        } else {
            return;  // not a lineage-bearing event
        }
    } catch (const std::exception& ex) {
        log::warn("lineage.dispatch", std::string("failed to parse event payload: ") + ex.what());
        return;
    }

    for (const auto& listener : listeners_) {
        try {
            listener->on_event(ev);
        } catch (const std::exception& ex) {
            log::warn("lineage.dispatch", std::string("listener threw: ") + ex.what());
        }
    }
}

}  // namespace clink::lineage
