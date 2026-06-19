#include "clink/runtime/event_bus.hpp"

#include <algorithm>
#include <chrono>
#include <utility>

namespace clink {

EventBus::SubscriberId EventBus::subscribe_raw(Callback cb) {
    std::lock_guard lock(mu_);
    const auto id = next_id_++;
    subs_.emplace_back(id, std::move(cb));
    return id;
}

void EventBus::unsubscribe(SubscriberId id) {
    std::lock_guard lock(mu_);
    subs_.erase(
        std::remove_if(subs_.begin(), subs_.end(), [id](const auto& p) { return p.first == id; }),
        subs_.end());
}

void EventBus::publish(Event e) {
    // Snapshot the callback list under the lock, then dispatch without
    // holding the bus mutex. Subscribers that take their own locks could
    // otherwise deadlock against threads currently inside subscribe()/
    // unsubscribe().
    std::vector<Callback> callbacks;
    {
        std::lock_guard lock(mu_);
        callbacks.reserve(subs_.size());
        for (const auto& [_, cb] : subs_) {
            callbacks.push_back(cb);
        }
    }
    for (auto& cb : callbacks) {
        try {
            cb(e);
        } catch (...) {
            // Subscribers must not propagate exceptions back through the
            // bus; swallow and continue so one bad subscriber can't
            // starve the rest.
        }
    }
}

EventBus& EventBus::global() {
    static EventBus instance;
    return instance;
}

namespace events {

namespace {

std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

void publish(std::string type, std::string payload) {
    Event e;
    e.ts_ms = now_ms();
    e.type = std::move(type);
    e.payload = std::move(payload);
    EventBus::global().publish(std::move(e));
}

Subscription subscribe(EventBus::Callback cb) {
    auto& bus = EventBus::global();
    const auto id = bus.subscribe_raw(std::move(cb));
    return Subscription(bus, id);
}

}  // namespace events

}  // namespace clink
