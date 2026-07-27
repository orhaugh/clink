#pragma once

// The arithmetic that maps an event time onto the windows containing it.
//
// This exists as one header because the same two lines were written eight times
// across the SQL window operators, the DataStream window operators, the async
// variants and the fluent API - and seven of the eight used C++ integer division,
// which truncates toward zero rather than flooring. The two agree for every
// non-negative timestamp, which is why the divergence went unnoticed: it appears
// only for event times before the Unix epoch.
//
// For a 10s tumbling window, a record at ts = -15000 belongs in [-20000, -10000).
// Truncating division computes -15000 / 10000 = -1 and places it in [-10000, 0),
// a window that does not contain it. Every pre-epoch record was shifted one window
// toward zero, and a hopping window - which also clamped its first pane to zero -
// discarded them entirely.
//
// Pre-epoch event time is ordinary data: a historical backfill, or a source whose
// clock is wrong. Whatever a window does with it, every window kind must do the
// same thing, and a record must land in a window that contains it.

#include <cstdint>
#include <limits>
#include <vector>

// Namespace matches the sibling time headers (event_time.hpp, watermark.hpp),
// which all sit directly in clink rather than a nested namespace.
namespace clink {

namespace window_detail {

// Every intermediate below is computed in 128 bits and clamped once, because the
// obvious 64-bit forms are not total. A timestamp of INT64_MAX gives a window_end of
// start + size that exceeds INT64_MAX; a timestamp of INT64_MIN makes the floored
// start itself go below INT64_MIN, and makes a hop's `ts - size + slide` underflow.
// All three are signed overflow, which is undefined behaviour rather than a wrong
// answer, and INT64_MIN and INT64_MAX are exactly what an uninitialised or
// sentinel-valued timestamp column carries - so this is reachable from bad input
// rather than only from absurd input.
using Wide = __int128;

[[nodiscard]] constexpr std::int64_t clamp_to_int64(Wide v) noexcept {
    constexpr Wide lo = static_cast<Wide>(std::numeric_limits<std::int64_t>::min());
    constexpr Wide hi = static_cast<Wide>(std::numeric_limits<std::int64_t>::max());
    if (v < lo) {
        return std::numeric_limits<std::int64_t>::min();
    }
    if (v > hi) {
        return std::numeric_limits<std::int64_t>::max();
    }
    return static_cast<std::int64_t>(v);
}

[[nodiscard]] constexpr Wide floor_div_wide(Wide a, Wide b) noexcept {
    const Wide q = a / b;
    const Wide r = a % b;
    return (r != 0 && ((r < 0) != (b < 0))) ? q - 1 : q;
}

}  // namespace window_detail

// Floor division: the quotient rounded toward negative infinity, unlike C++'s `/`
// which rounds toward zero. The two differ only when exactly one operand is
// negative.
[[nodiscard]] constexpr std::int64_t floor_div(std::int64_t a, std::int64_t b) noexcept {
    return window_detail::clamp_to_int64(window_detail::floor_div_wide(
        static_cast<window_detail::Wide>(a), static_cast<window_detail::Wide>(b)));
}

// The start of the fixed-size window of width `size` containing `ts`. Windows are
// anchored at the epoch, so this is the largest multiple of `size` not exceeding
// `ts`. Saturates rather than overflowing at the extremes of int64.
[[nodiscard]] constexpr std::int64_t window_start_for(std::int64_t ts, std::int64_t size) noexcept {
    const auto w = static_cast<window_detail::Wide>(size);
    return window_detail::clamp_to_int64(
        window_detail::floor_div_wide(static_cast<window_detail::Wide>(ts), w) * w);
}

// The exclusive end of a window starting at `start` and `size` wide, saturating at
// INT64_MAX.
//
// Saturation means the very topmost window is effectively closed at INT64_MAX rather
// than half-open: a record at exactly INT64_MAX has no representable exclusive upper
// bound above it. That single timestamp is the one point where `ts < window_end` does
// not hold, and keeping the record in a saturated window is preferable to dropping it
// or to invoking undefined behaviour.
[[nodiscard]] constexpr std::int64_t window_end_for(std::int64_t start,
                                                    std::int64_t size) noexcept {
    return window_detail::clamp_to_int64(static_cast<window_detail::Wide>(start) +
                                         static_cast<window_detail::Wide>(size));
}

// Saturating add and subtract, for the time comparisons a window makes around its
// own bounds: a session extent plus its gap, a watermark minus a grace band. Each of
// those is `timestamp +/- duration`, and each overflows for a timestamp at the
// extremes of int64 - which is what a corrupt or sentinel timestamp column carries.
[[nodiscard]] constexpr std::int64_t sat_add(std::int64_t a, std::int64_t b) noexcept {
    return window_detail::clamp_to_int64(static_cast<window_detail::Wide>(a) +
                                         static_cast<window_detail::Wide>(b));
}

[[nodiscard]] constexpr std::int64_t sat_sub(std::int64_t a, std::int64_t b) noexcept {
    return window_detail::clamp_to_int64(static_cast<window_detail::Wide>(a) -
                                         static_cast<window_detail::Wide>(b));
}

// Every start of a `size`-wide window that slides by `slide` and contains `ts`,
// in ascending order. A record belongs to ceil(size / slide) panes.
//
// No pane is clamped to the epoch. Clamping was what made a hopping window
// disagree with a tumbling window of the same width: it dropped the panes with
// negative starts rather than emitting them, so a record near or before the epoch
// contributed to fewer panes than it belongs to.
[[nodiscard]] inline std::vector<std::int64_t> hop_window_starts_for(std::int64_t ts,
                                                                     std::int64_t size,
                                                                     std::int64_t slide) {
    using window_detail::Wide;
    const auto wts = static_cast<Wide>(ts);
    const auto wsize = static_cast<Wide>(size);
    const auto wslide = static_cast<Wide>(slide);
    // Wide throughout: `ts - size + slide` underflows int64 for a timestamp near
    // INT64_MIN, and the floored first pane can sit below INT64_MIN even when every
    // pane that CONTAINS ts is representable.
    const Wide last = window_detail::floor_div_wide(wts, wslide) * wslide;
    const Wide first = window_detail::floor_div_wide(wts - wsize + wslide, wslide) * wslide;
    std::vector<std::int64_t> out;
    out.reserve(static_cast<std::size_t>(((last - first) / wslide) + 1));
    for (Wide s = first; s <= last; s += wslide) {
        // The guard is not redundant: `first` can precede the earliest pane that
        // actually contains `ts` when size is not a multiple of slide. It also drops
        // any pane whose start is not representable, since such a pane cannot
        // contain a representable ts.
        if (wts >= s && wts < s + wsize) {
            out.push_back(window_detail::clamp_to_int64(s));
        }
    }
    return out;
}

}  // namespace clink
