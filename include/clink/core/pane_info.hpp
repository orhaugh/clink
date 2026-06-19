#pragma once

#include <compare>
#include <cstdint>

namespace clink {

// PaneInfo describes one window emission ("pane") relative to the
// window's lifecycle. Mirrors PaneInfo.
//
//   * timing:
//       - Early    - fired before watermark reached window_end (a count
//                    trigger, processing-time trigger, or custom trigger
//                    fired the window early).
//       - OnTime   - fired by EventTimeTrigger when watermark first
//                    crossed window_end.
//       - Late     - fired by a late-arriving record after window has
//                    already had its on-time fire (within
//                    allowed_lateness).
//
//   * pane_index    - monotonic counter for emissions of this window.
//                     0 is the first pane.
//
//   * is_first      - true iff this is the first emission for the
//                     window. Useful for downstream consumers that want
//                     to distinguish initial vs subsequent panes.
//   * is_last       - true iff this is the final emission (the window
//                     will be purged immediately after this pane).
//
// The struct is small and trivially copyable so it threads through the
// operator chain on every record at minimal cost.
struct PaneInfo {
    enum class Timing : std::uint8_t {
        Early = 0,
        OnTime = 1,
        Late = 2,
    };

    Timing timing{Timing::OnTime};
    std::int64_t pane_index{0};
    bool is_first{true};
    bool is_last{false};

    constexpr auto operator<=>(const PaneInfo&) const noexcept = default;
};

}  // namespace clink
