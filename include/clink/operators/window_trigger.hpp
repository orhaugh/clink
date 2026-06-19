#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <utility>

namespace clink {

// Decision returned by a Trigger after observing an element or a timer
// event for a given window. Mirrors TriggerResult enum.
//
//   * Continue       - do nothing yet.
//   * Fire           - emit the window's pane; keep the window state.
//   * Purge          - discard window state without emitting.
//   * FireAndPurge   - emit and discard.
enum class TriggerResult : std::uint8_t {
    Continue = 0,
    Fire = 1,
    Purge = 2,
    FireAndPurge = 3,
};

constexpr bool should_fire(TriggerResult r) noexcept {
    return r == TriggerResult::Fire || r == TriggerResult::FireAndPurge;
}
constexpr bool should_purge(TriggerResult r) noexcept {
    return r == TriggerResult::Purge || r == TriggerResult::FireAndPurge;
}

// TimeWindow corresponds to TimeWindow: a half-open interval
// [start, end) on the event-time axis. Used by tumbling and sliding
// windows; session windows have their own dynamic-bound representation.
struct TimeWindow {
    std::int64_t start{};
    std::int64_t end{};

    constexpr std::int64_t max_timestamp() const noexcept { return end - 1; }
    constexpr auto operator<=>(const TimeWindow&) const noexcept = default;
};

// TriggerContext gives a Trigger access to the engine's time signals
// and (eventually) a per-window timer service. The MVP doesn't expose
// timer registration - operators iterate pending windows on every
// watermark advance and dispatch on_event_time directly. Triggers that
// only need current-time signals (EventTimeTrigger, ProcessingTimeTrigger,
// CountTrigger) work fine without explicit timers.
template <typename Window>
class TriggerContext {
public:
    TriggerContext() = default;
    virtual ~TriggerContext() = default;
    TriggerContext(const TriggerContext&) = delete;
    TriggerContext& operator=(const TriggerContext&) = delete;
    TriggerContext(TriggerContext&&) = delete;
    TriggerContext& operator=(TriggerContext&&) = delete;

    // The latest watermark timestamp observed by the operator. Triggers
    // use this to detect "already late" elements.
    virtual std::int64_t current_watermark() const noexcept = 0;

    // Wall-clock time, milliseconds since the Unix epoch. Used by
    // ProcessingTimeTrigger.
    virtual std::int64_t current_processing_time() const noexcept = 0;
};

// A Trigger decides when a window emits. Stateless triggers (e.g.
// EventTimeTrigger) override only on_element / on_event_time; stateful
// triggers (CountTrigger) keep per-window state internally and clear
// it via clear() when the window is purged.
template <typename T, typename Window>
class Trigger {
public:
    Trigger() = default;
    virtual ~Trigger() = default;
    Trigger(const Trigger&) = delete;
    Trigger& operator=(const Trigger&) = delete;
    Trigger(Trigger&&) = delete;
    Trigger& operator=(Trigger&&) = delete;

    // Called when a record is assigned to `window`.
    virtual TriggerResult on_element(const T& value,
                                     std::int64_t timestamp,
                                     const Window& window,
                                     TriggerContext<Window>& ctx) = 0;

    // Called when the watermark crosses an event-time boundary the
    // trigger registered (or the operator's window_end, in this MVP).
    virtual TriggerResult on_event_time(std::int64_t time,
                                        const Window& window,
                                        TriggerContext<Window>& ctx) = 0;

    // Called when wall-clock time passes a registered processing-time
    // boundary. Default implementation: do nothing.
    virtual TriggerResult on_processing_time(std::int64_t /*time*/,
                                             const Window& /*window*/,
                                             TriggerContext<Window>& /*ctx*/) {
        return TriggerResult::Continue;
    }

    // Called when the operator purges the window. Triggers that hold
    // per-window state (CountTrigger, etc.) override to release it.
    virtual void clear(const Window& /*window*/, TriggerContext<Window>& /*ctx*/) {}

    // --- Durability of trigger state across checkpoint/restore ---
    //
    // Stateless triggers (EventTimeTrigger, ProcessingTimeTrigger) derive
    // their fire decisions purely from the watermark / wall-clock and hold no
    // per-window state, so they need none of this - the defaults are no-ops and
    // cost nothing. Stateful triggers (CountTrigger and user triggers that
    // accumulate per-window state) MUST override these so a partially-counted
    // window survives a restart: without it, restore resets the count to zero
    // and the window fires at the wrong record, breaking exactly-once.
    //
    // The window operator persists snapshot_state() as one blob at each
    // checkpoint barrier and feeds it back via restore_state() on open(). The
    // blob is opaque to the operator; encode the whole trigger's per-window
    // state in it.
    [[nodiscard]] virtual bool is_stateful() const noexcept { return false; }
    [[nodiscard]] virtual std::string snapshot_state() const { return {}; }
    virtual void restore_state(std::string_view /*blob*/) {}
};

// ----- Built-in triggers (specialised on TimeWindow) -----

// EventTimeTrigger fires when watermark crosses window.end. Returns
// Fire (NOT FireAndPurge) so the operator's allowed-lateness logic can
// decide when to actually purge the window - matches
// EventTimeTrigger semantics where cleanup is the operator's separate
// concern via a cleanup timer at window_end + allowed_lateness.
template <typename T>
class EventTimeTrigger final : public Trigger<T, TimeWindow> {
public:
    TriggerResult on_element(const T&,
                             std::int64_t /*timestamp*/,
                             const TimeWindow& window,
                             TriggerContext<TimeWindow>& ctx) override {
        // If the record is already late (window has already closed),
        // fire immediately so the operator can record the late pane.
        if (ctx.current_watermark() >= window.end) {
            return TriggerResult::Fire;
        }
        return TriggerResult::Continue;
    }
    TriggerResult on_event_time(std::int64_t time,
                                const TimeWindow& window,
                                TriggerContext<TimeWindow>&) override {
        return time >= window.end ? TriggerResult::Fire : TriggerResult::Continue;
    }
};

// ProcessingTimeTrigger fires when wall-clock time crosses window.end.
// Useful for "best-effort" near-real-time aggregations where strict
// event-time ordering isn't possible.
template <typename T>
class ProcessingTimeTrigger final : public Trigger<T, TimeWindow> {
public:
    TriggerResult on_element(const T&,
                             std::int64_t,
                             const TimeWindow& window,
                             TriggerContext<TimeWindow>& ctx) override {
        if (ctx.current_processing_time() >= window.end) {
            return TriggerResult::FireAndPurge;
        }
        return TriggerResult::Continue;
    }
    TriggerResult on_event_time(std::int64_t,
                                const TimeWindow&,
                                TriggerContext<TimeWindow>&) override {
        return TriggerResult::Continue;
    }
    TriggerResult on_processing_time(std::int64_t time,
                                     const TimeWindow& window,
                                     TriggerContext<TimeWindow>&) override {
        return time >= window.end ? TriggerResult::FireAndPurge : TriggerResult::Continue;
    }
};

// CountTrigger fires after `max_count` records have been added to the
// window. Watermark progress is ignored - the trigger purges its own
// state on each fire so the next batch of records starts fresh.
template <typename T>
class CountTrigger final : public Trigger<T, TimeWindow> {
public:
    explicit CountTrigger(std::int64_t max_count) : max_(max_count) {}

    TriggerResult on_element(const T&,
                             std::int64_t,
                             const TimeWindow& window,
                             TriggerContext<TimeWindow>&) override {
        const auto key = std::make_pair(window.start, window.end);
        auto& n = counts_[key];
        ++n;
        if (n >= max_) {
            counts_.erase(key);
            return TriggerResult::FireAndPurge;
        }
        return TriggerResult::Continue;
    }
    TriggerResult on_event_time(std::int64_t,
                                const TimeWindow&,
                                TriggerContext<TimeWindow>&) override {
        return TriggerResult::Continue;
    }
    void clear(const TimeWindow& window, TriggerContext<TimeWindow>&) override {
        counts_.erase(std::make_pair(window.start, window.end));
    }

    // CountTrigger is stateful: counts_ is the per-window progress toward
    // max_. It must survive checkpoint/restore so a partially-counted window
    // resumes from its real count instead of restarting at zero.
    [[nodiscard]] bool is_stateful() const noexcept override { return true; }

    // Wire: [u8 version=1][u32 n BE][ n x (i64 start BE)(i64 end BE)(i64 count BE) ].
    [[nodiscard]] std::string snapshot_state() const override {
        std::string out;
        out.reserve(1 + 4 + counts_.size() * 24);
        out.push_back(static_cast<char>(1));  // version
        put_be32_(out, static_cast<std::uint32_t>(counts_.size()));
        for (const auto& [win, n] : counts_) {
            put_be64_(out, static_cast<std::uint64_t>(win.first));
            put_be64_(out, static_cast<std::uint64_t>(win.second));
            put_be64_(out, static_cast<std::uint64_t>(n));
        }
        return out;
    }

    void restore_state(std::string_view blob) override {
        counts_.clear();
        if (blob.size() < 5 || static_cast<unsigned char>(blob[0]) != 1) {
            return;
        }
        const auto n = get_be32_(blob, 1);
        std::size_t off = 5;
        for (std::uint32_t i = 0; i < n; ++i) {
            if (off + 24 > blob.size()) {
                return;  // truncated; keep what parsed
            }
            const auto start = static_cast<std::int64_t>(get_be64_(blob, off));
            const auto end = static_cast<std::int64_t>(get_be64_(blob, off + 8));
            const auto cnt = static_cast<std::int64_t>(get_be64_(blob, off + 16));
            counts_[std::make_pair(start, end)] = cnt;
            off += 24;
        }
    }

private:
    static void put_be32_(std::string& out, std::uint32_t v) {
        for (int i = 3; i >= 0; --i) {
            out.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
        }
    }
    static void put_be64_(std::string& out, std::uint64_t v) {
        for (int i = 7; i >= 0; --i) {
            out.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
        }
    }
    static std::uint32_t get_be32_(std::string_view b, std::size_t off) {
        std::uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            v = (v << 8) | static_cast<unsigned char>(b[off + static_cast<std::size_t>(i)]);
        }
        return v;
    }
    static std::uint64_t get_be64_(std::string_view b, std::size_t off) {
        std::uint64_t v = 0;
        for (int i = 0; i < 8; ++i) {
            v = (v << 8) | static_cast<unsigned char>(b[off + static_cast<std::size_t>(i)]);
        }
        return v;
    }

    std::int64_t max_;
    std::map<std::pair<std::int64_t, std::int64_t>, std::int64_t> counts_;
};

}  // namespace clink
