#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/metrics/checkpoint_metrics.hpp"
#include "clink/time/watermark.hpp"

namespace clink {

// Bookkeeping for an operator with N inputs. Tracks per-input watermark and
// barrier state so the operator code can stay focused on its actual job
// (merging data, joining, etc.).
//
// The operator drives this state machine by calling on_watermark / on_barrier
// / on_input_closed. After each call, it reads the returned struct to decide
// whether to forward a watermark, forward a barrier, and which inputs are
// currently paused (i.e. should not be polled because they've passed a
// barrier that other inputs haven't reached yet).
class MultiInputAlignment {
public:
    // Alias to CheckpointBarrier::Mode. The aligner formerly
    // owned this enum and chose the alignment policy at startup; that
    // captured-at-startup model has been replaced by a per-barrier
    // mode (the barrier carries its own Mode and the aligner honours
    // it). The alias is kept so existing call sites that name
    // `MultiInputAlignment::Mode::Unaligned` continue to compile.
    using Mode = CheckpointBarrier::Mode;

    explicit MultiInputAlignment(std::size_t input_count)
        : input_wm_(input_count, Watermark::min()),
          paused_(input_count, false),
          closed_(input_count, false),
          idle_(input_count, false) {}

    // Stamp the aligner with the OperatorId.value() of the operator
    // it belongs to so per-op barrier alignment metrics route to the
    // right counter. Zero (the default) disables metric emission so
    // unit tests of the aligner itself don't fabricate counter
    // entries.
    void set_operator_id(std::uint64_t op_id) noexcept { op_id_for_metrics_ = op_id; }

    struct WatermarkAdvance {
        bool forward{false};
        Watermark watermark{Watermark::min()};
    };

    struct BarrierAdvance {
        bool forward{false};
        CheckpointBarrier barrier{};
        // True iff this advance was triggered by the FIRST input to
        // deliver this barrier (unaligned mode). The runner uses this
        // flag to know when to capture the in-flight records on the
        // other channels into the snapshot. Always false in aligned
        // mode - by the time we forward, every input has delivered.
        bool unaligned_first{false};
    };

    // Update input i's watermark to wm. Returns whether the operator should
    // emit a downstream watermark, and if so the value (the running min of
    // all inputs).
    //
    // Idleness handling:
    //   * An idle watermark (wm.is_idle()) marks input i as idle -
    //     input_wm_[i] is set to Watermark::max() so it no longer
    //     constrains the running min. A quiet partition can't stall
    //     downstream time.
    //   * An active watermark from a previously-idle input transitions
    //     back. To prevent the global watermark from regressing, the
    //     re-joining input's effective wm is clamped to at least the
    //     currently-emitted global watermark (matches the documented semantics).
    //   * The forward-comparison is timestamp-only - operator<=> on
    //     Watermark includes the idle flag, so a raw `wm > input_wm_[i]`
    //     comparison would order based on idleness too. Use timestamp().
    WatermarkAdvance on_watermark(std::size_t i, Watermark wm) {
        if (wm.is_idle()) {
            idle_[i] = true;
            input_wm_[i] = Watermark::max();
            return recompute_watermark_();
        }
        // Active watermark: transition out of idle if needed.
        if (idle_[i]) {
            idle_[i] = false;
            // Clamp to current global to avoid regression. The
            // re-joining input "catches up" to the global watermark;
            // records below it would be late under the existing
            // contract.
            const auto clamped =
                wm.timestamp() > emitted_wm_.timestamp() ? wm : Watermark{emitted_wm_.timestamp()};
            input_wm_[i] = clamped;
        } else if (wm.timestamp() > input_wm_[i].timestamp()) {
            input_wm_[i] = wm;
        }
        return recompute_watermark_();
    }

    // Record that input i delivered a barrier. Returns whether the barrier
    // is now aligned across all alive inputs and should be forwarded
    // downstream.
    //
    // The barrier's stamped Mode decides behaviour per-barrier:
    //   Aligned: paused_[i] = true; further delivery of the same
    //     barrier on other inputs may complete alignment and forward.
    //   Unaligned: first delivery forwards immediately and never
    //     pauses; subsequent deliveries of the same id are absorbed
    //     silently. The first-delivery advance carries
    //     `unaligned_first=true` so the operator runner knows to
    //     capture the still-pending inputs' in-flight records into
    //     the snapshot.
    //
    // Modes can change across checkpoints; the first delivery of a
    // given checkpoint id pins the mode for that checkpoint at this
    // aligner.
    BarrierAdvance on_barrier(std::size_t i, CheckpointBarrier b) {
        auto& seen = seen_barriers_[b.id().value()];
        if (seen.empty()) {
            seen.assign(input_wm_.size(), false);
            // First delivery for this id wins the mode decision.
            // Same-id deliveries with a different mode will keep the
            // first-seen mode (Chandy-Lamport requires per-checkpoint
            // mode agreement; a mismatch is a stamping bug upstream).
            seen_mode_[b.id().value()] = b.mode();
            first_seen_time_[b.id().value()] = std::chrono::steady_clock::now();
        }
        const bool first_for_this_barrier = !any_true_(seen);
        seen[i] = true;
        const Mode effective_mode = seen_mode_[b.id().value()];

        if (effective_mode == Mode::Unaligned) {
            if (!first_for_this_barrier) {
                // Subsequent deliveries: harmless. Don't re-forward.
                // GC the bookkeeping once every alive input has been
                // accounted for so memory stays bounded.
                maybe_drop_seen_(b.id().value());
                return {};
            }
            // First delivery -> forward immediately, no pausing.
            BarrierAdvance adv;
            adv.forward = true;
            adv.barrier = b;
            adv.unaligned_first = true;
            return adv;
        }

        paused_[i] = true;
        return check_alignment_(b.id().value());
    }

    // Mark input i as closed. Closed inputs are excluded from alignment
    // checks (they implicitly satisfy any in-flight barrier and contribute
    // EventTime::max() to the watermark min).
    BarrierAdvance on_input_closed(std::size_t i) {
        if (closed_[i]) {
            return {};
        }
        closed_[i] = true;
        input_wm_[i] = Watermark::max();  // closed inputs no longer hold back time
        // A close may complete alignment for a pending barrier; check each.
        for (auto& [id, _] : seen_barriers_) {
            if (auto adv = check_alignment_(id); adv.forward) {
                return adv;
            }
        }
        // Even if no barrier completed, the watermark min may have moved.
        // Caller is expected to call recompute_watermark_via_close() too, or
        // we expose it inline here:
        return {};
    }

    // Run a watermark recompute (useful after on_input_closed which doesn't
    // emit a watermark advance directly).
    WatermarkAdvance refresh_watermark() { return recompute_watermark_(); }

    // True if input i should currently be skipped (paused at barrier or
    // closed).
    bool input_paused(std::size_t i) const noexcept { return paused_[i] || closed_[i]; }
    bool input_closed(std::size_t i) const noexcept { return closed_[i]; }

    bool all_closed() const noexcept {
        return std::all_of(closed_.begin(), closed_.end(), [](bool c) { return c; });
    }

    std::size_t input_count() const noexcept { return input_wm_.size(); }

    // The most recent watermark this aligner has forwarded downstream
    // (i.e. the running min of all input watermarks at last advance). Useful
    // for operators that need to make decisions based on "what time has the
    // engine guaranteed past us" - e.g. dropping late-arriving records.
    Watermark current_watermark() const noexcept { return emitted_wm_; }

    // Enumerate inputs that have NOT yet delivered the
    // barrier `ck_id`. Returned indices skip closed inputs (closed
    // inputs implicitly satisfy any barrier and have nothing to
    // drain). Empty result means "every alive input has delivered;
    // there's no in-flight to capture."
    //
    // Stateful multi-input operators consult this on the
    // `adv.unaligned_first` advance to know which channels they
    // should drain into snapshot state before the barrier moves
    // downstream. The aligner records same-id deliveries via
    // on_barrier; this accessor reads that bitmap. Calling it for an
    // unknown ck_id (one that no input has delivered yet) returns
    // every alive input.
    std::vector<std::size_t> pending_inputs_for(CheckpointId ck_id) const {
        std::vector<std::size_t> pending;
        auto it = seen_barriers_.find(ck_id.value());
        if (it == seen_barriers_.end()) {
            for (std::size_t i = 0; i < input_wm_.size(); ++i) {
                if (!closed_[i]) {
                    pending.push_back(i);
                }
            }
            return pending;
        }
        const auto& flags = it->second;
        for (std::size_t i = 0; i < flags.size(); ++i) {
            if (!flags[i] && !closed_[i]) {
                pending.push_back(i);
            }
        }
        return pending;
    }

private:
    static bool any_true_(const std::vector<bool>& v) {
        for (bool x : v) {
            if (x) {
                return true;
            }
        }
        return false;
    }

    // In unaligned mode we no longer need the bitmap once every alive
    // input has been seen - it just tracks "have we forwarded for this
    // id". GC it eagerly to keep the map small.
    void maybe_drop_seen_(std::uint64_t ck_id) {
        auto it = seen_barriers_.find(ck_id);
        if (it == seen_barriers_.end()) {
            return;
        }
        for (std::size_t j = 0; j < it->second.size(); ++j) {
            if (!closed_[j] && !it->second[j]) {
                return;
            }
        }
        seen_barriers_.erase(it);
        seen_mode_.erase(ck_id);
    }

    BarrierAdvance check_alignment_(std::uint64_t ck_id) {
        auto it = seen_barriers_.find(ck_id);
        if (it == seen_barriers_.end()) {
            return {};
        }
        const auto& flags = it->second;
        for (std::size_t j = 0; j < flags.size(); ++j) {
            if (!closed_[j] && !flags[j]) {
                return {};  // not yet aligned
            }
        }
        // All alive inputs delivered this barrier - release. Preserve
        // the stamped mode on the forwarded barrier so downstream
        // operators see the same policy.
        Mode m = Mode::Aligned;
        if (auto mit = seen_mode_.find(ck_id); mit != seen_mode_.end()) {
            m = mit->second;
        }
        BarrierAdvance adv;
        adv.forward = true;
        adv.barrier = CheckpointBarrier{CheckpointId{ck_id}, /*terminal=*/false, m};
        seen_barriers_.erase(it);
        seen_mode_.erase(ck_id);
        if (op_id_for_metrics_ != 0) {
            if (auto tit = first_seen_time_.find(ck_id); tit != first_seen_time_.end()) {
                const auto wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                         std::chrono::steady_clock::now() - tit->second)
                                         .count();
                clink::metrics::ckpt::barrier_aligned(op_id_for_metrics_,
                                                      static_cast<std::uint64_t>(wait_ns));
            } else {
                clink::metrics::ckpt::barrier_aligned(op_id_for_metrics_, 0);
            }
        }
        first_seen_time_.erase(ck_id);
        // Unpause every input (they may pause again on a future barrier).
        for (std::size_t j = 0; j < paused_.size(); ++j) {
            if (!closed_[j]) {
                paused_[j] = false;
            }
        }
        return adv;
    }

    WatermarkAdvance recompute_watermark_() {
        // Three cases:
        //   1. No alive inputs left (all closed): use the historic
        //      min path. Closed inputs are already at Watermark::max(),
        //      so min() = max() and we naturally emit end-of-time -
        //      matching pre-idleness behavior.
        //   2. Some inputs alive but all alive ones idle: emit a single
        //      idle marker. The downstream operator (a join's
        //      MultiInputAlignment) skips this branch in its own
        //      alignment so global time can still advance from
        //      another active branch.
        //   3. At least one alive + active input: compute min over all
        //      input_wm_ entries (closed and idle ones are at max(),
        //      so they don't pull the min down).
        const bool any_alive = anyAlive_();
        const bool any_alive_active = anyAliveActive_();
        if (any_alive && !any_alive_active) {
            // Case 2: all alive inputs are idle. Emit one idle marker
            // per transition; don't spam downstream with repeats.
            if (!last_emitted_idle_) {
                last_emitted_idle_ = true;
                return WatermarkAdvance{
                    .forward = true,
                    .watermark = Watermark::idle(emitted_wm_.timestamp()),
                };
            }
            return WatermarkAdvance{};
        }
        // Cases 1 and 3: at least one active input OR everyone closed.
        Watermark current = *std::min_element(input_wm_.begin(), input_wm_.end());
        if (last_emitted_idle_) {
            // Coming back from all-idle: force-emit even if the
            // numeric watermark didn't advance, so downstream knows
            // we're active again. We send the current min (which is
            // at least emitted_wm_ thanks to the idle→active clamp
            // in on_watermark).
            last_emitted_idle_ = false;
            emitted_wm_ = current;
            return WatermarkAdvance{.forward = true, .watermark = current};
        }
        if (current.timestamp() > emitted_wm_.timestamp()) {
            emitted_wm_ = current;
            return WatermarkAdvance{.forward = true, .watermark = current};
        }
        return WatermarkAdvance{};
    }

    bool anyAlive_() const noexcept {
        for (std::size_t i = 0; i < input_wm_.size(); ++i) {
            if (!closed_[i]) {
                return true;
            }
        }
        return false;
    }

    bool anyAliveActive_() const noexcept {
        for (std::size_t i = 0; i < input_wm_.size(); ++i) {
            if (!closed_[i] && !idle_[i]) {
                return true;
            }
        }
        return false;
    }

    // Per-checkpoint mode pinned on first delivery for that id. The
    // CheckpointCoordinator stamps the mode on the barrier itself; we
    // remember the first-seen mode and apply it to every same-id
    // delivery so aligned and unaligned semantics never mix mid-flight
    // for one checkpoint. Entries are GC'd alongside seen_barriers_.
    std::unordered_map<std::uint64_t, Mode> seen_mode_;
    std::vector<Watermark> input_wm_;
    std::vector<bool> paused_;
    std::vector<bool> closed_;
    std::vector<bool> idle_;
    std::unordered_map<std::uint64_t, std::vector<bool>> seen_barriers_;
    // First-input-delivery time per in-flight checkpoint id. Stamped
    // on first delivery, consumed at alignment to feed
    // barrier_align_wait_ns_sum. Empty when no aligned barrier is
    // in flight.
    std::unordered_map<std::uint64_t, std::chrono::steady_clock::time_point> first_seen_time_;
    // OperatorId.value() of the owning operator. Set by the dag.hpp
    // runner that constructs the aligner; 0 means "don't emit
    // metrics" (aligner-only UTs leave this default).
    std::uint64_t op_id_for_metrics_{0};
    Watermark emitted_wm_{Watermark::min()};
    bool last_emitted_idle_{false};
};

}  // namespace clink
