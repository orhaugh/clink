#pragma once

#include <compare>

#include "clink/time/event_time.hpp"

namespace clink {

// A watermark asserts that no further records with event_time < t will arrive
// on this stream. It is the engine's lower bound on event-time progress and is
// what allows windows to fire and event-time triggers to advance.
//
// Operators must propagate watermarks monotonically; a watermark that travels
// backwards is a bug, and downstream operators are entitled to ignore it.
//
// Idleness:
//
// A watermark may also be marked "idle" via `Watermark::idle(...)` /
// `is_idle()`. An idle watermark is the assigner's way of saying "I have
// no records to emit and shouldn't hold back time at downstream joins."
// MultiInputAlignment treats idle inputs as excluded from the running
// min, so a single quiet partition doesn't stall global watermark
// progress. The timestamp on an idle watermark is informational (it's
// the last observed event-time); the operator side checks `is_idle()`
// to decide alignment.
class Watermark {
public:
    constexpr Watermark() = default;
    constexpr explicit Watermark(EventTime t) noexcept : timestamp_(t) {}
    constexpr Watermark(EventTime t, bool idle) noexcept : timestamp_(t), idle_(idle) {}

    constexpr EventTime timestamp() const noexcept { return timestamp_; }

    // True iff this watermark signals an idle stream - i.e., the source
    // has produced no records for the configured idleness duration.
    // Downstream multi-input alignment skips idle inputs when computing
    // the min watermark.
    constexpr bool is_idle() const noexcept { return idle_; }

    // Factory for an idle marker. The timestamp records "where the
    // assigner was when it went idle"; downstream consumers use it for
    // logging / diagnostics but not for alignment math (which simply
    // skips idle inputs).
    static constexpr Watermark idle(EventTime t = EventTime::min()) noexcept {
        return Watermark{t, true};
    }

    static constexpr Watermark min() noexcept { return Watermark{EventTime::min()}; }
    static constexpr Watermark max() noexcept { return Watermark{EventTime::max()}; }

    constexpr auto operator<=>(const Watermark&) const = default;

private:
    EventTime timestamp_{EventTime::min()};
    bool idle_{false};
};

}  // namespace clink
