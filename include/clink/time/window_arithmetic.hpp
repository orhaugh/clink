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
#include <vector>

// Namespace matches the sibling time headers (event_time.hpp, watermark.hpp),
// which all sit directly in clink rather than a nested namespace.
namespace clink {

// Floor division: the quotient rounded toward negative infinity, unlike C++'s `/`
// which rounds toward zero. The two differ only when exactly one operand is
// negative.
[[nodiscard]] constexpr std::int64_t floor_div(std::int64_t a, std::int64_t b) noexcept {
    const std::int64_t q = a / b;
    const std::int64_t r = a % b;
    return (r != 0 && ((r < 0) != (b < 0))) ? q - 1 : q;
}

// The start of the fixed-size window of width `size` containing `ts`. Windows are
// anchored at the epoch, so this is the largest multiple of `size` not exceeding
// `ts`.
[[nodiscard]] constexpr std::int64_t window_start_for(std::int64_t ts, std::int64_t size) noexcept {
    return floor_div(ts, size) * size;
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
    const std::int64_t last = floor_div(ts, slide) * slide;
    const std::int64_t first = floor_div(ts - size + slide, slide) * slide;
    std::vector<std::int64_t> out;
    out.reserve(static_cast<std::size_t>(((last - first) / slide) + 1));
    for (std::int64_t s = first; s <= last; s += slide) {
        // The guard is not redundant: `first` can precede the earliest pane that
        // actually contains `ts` when size is not a multiple of slide.
        if (ts >= s && ts < s + size) {
            out.push_back(s);
        }
    }
    return out;
}

}  // namespace clink
