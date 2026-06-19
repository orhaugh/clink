#pragma once

#include <chrono>
#include <compare>
#include <cstdint>
#include <limits>

namespace clink {

// EventTime is the time at which an event actually occurred in the source
// system, distinct from processing time (when the engine sees it).
//
// We model it as a 64-bit millisecond count since the Unix epoch. This is the
// same convention used by  and Beam and is wide enough to be safe.
class EventTime {
public:
    using rep = std::int64_t;

    constexpr EventTime() = default;
    constexpr explicit EventTime(rep ms_since_epoch) noexcept : ms_(ms_since_epoch) {}

    static constexpr EventTime from_millis(rep ms) noexcept { return EventTime{ms}; }

    static EventTime from_system_clock(std::chrono::system_clock::time_point tp) noexcept {
        const auto ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
        return EventTime{static_cast<rep>(ms)};
    }

    static constexpr EventTime min() noexcept { return EventTime{std::numeric_limits<rep>::min()}; }
    static constexpr EventTime max() noexcept { return EventTime{std::numeric_limits<rep>::max()}; }

    constexpr rep millis() const noexcept { return ms_; }

    constexpr auto operator<=>(const EventTime&) const = default;

private:
    rep ms_{0};
};

}  // namespace clink
