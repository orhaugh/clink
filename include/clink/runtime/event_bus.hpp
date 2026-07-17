#pragma once

// In-process pub-sub for cluster lifecycle events.
//
// Backs the HTTP-5 /api/v1/events SSE endpoint: coordinator/worker lifecycle code calls
// `clink::events::publish(...)` at the same hooks where it already emits
// log lines and metrics, and any number of subscribers (currently: SSE
// streams) fan-out from a single mutex-guarded vector.
//
// Delivery is best-effort and synchronous: publish() walks subscribers under
// the bus mutex and calls each callback. Subscribers MUST be lightweight -
// typically just "queue the event for the SSE writer thread". Heavy work in
// a callback would back up every other subscriber.
//
// Lifetime: Subscription is an RAII handle. Destructor calls unsubscribe.
// Callers store the handle for as long as they want events; dropping it
// stops delivery. Safe to destroy a Subscription from a different thread
// than the one that called subscribe().

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace clink {

struct Event {
    std::int64_t ts_ms{};  // milliseconds since unix epoch
    std::string type;      // e.g. "coordinator.worker_registered", "coordinator.job_submitted"
    std::string payload;   // JSON; consumer-defined per type
};

class EventBus {
public:
    using Callback = std::function<void(const Event&)>;
    using SubscriberId = std::uint64_t;

    SubscriberId subscribe_raw(Callback cb);
    void unsubscribe(SubscriberId id);
    void publish(Event e);

    static EventBus& global();

private:
    mutable std::mutex mu_;
    SubscriberId next_id_{1};
    std::vector<std::pair<SubscriberId, Callback>> subs_;
};

// RAII wrapper around EventBus subscription. Holding the handle keeps
// the subscriber active; destroying it (or calling reset()) detaches.
class Subscription {
public:
    Subscription() = default;
    Subscription(EventBus& bus, EventBus::SubscriberId id) noexcept : bus_(&bus), id_(id) {}
    ~Subscription() { reset(); }

    Subscription(const Subscription&) = delete;
    Subscription& operator=(const Subscription&) = delete;
    Subscription(Subscription&& o) noexcept : bus_(o.bus_), id_(o.id_) {
        o.bus_ = nullptr;
        o.id_ = 0;
    }
    Subscription& operator=(Subscription&& o) noexcept {
        if (this != &o) {
            reset();
            bus_ = o.bus_;
            id_ = o.id_;
            o.bus_ = nullptr;
            o.id_ = 0;
        }
        return *this;
    }

    void reset() {
        if (bus_ != nullptr && id_ != 0) {
            bus_->unsubscribe(id_);
        }
        bus_ = nullptr;
        id_ = 0;
    }

    bool active() const noexcept { return bus_ != nullptr && id_ != 0; }

private:
    EventBus* bus_{nullptr};
    EventBus::SubscriberId id_{0};
};

namespace events {

// Convenience: stamp ts_ms = now, then publish to EventBus::global().
void publish(std::string type, std::string payload);

// Convenience: subscribe to EventBus::global() and return the RAII handle.
[[nodiscard]] Subscription subscribe(EventBus::Callback cb);

}  // namespace events

}  // namespace clink
