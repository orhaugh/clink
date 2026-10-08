#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <unordered_map>
#include <utility>
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
//
// Every checkpoint is a consistent cut across all inputs. A barrier is
// forwarded only once every alive input has delivered it, whatever mode it
// is stamped with, unless the operator captures in-flight rows (see the
// constructor): forwarding an Unaligned barrier on its first delivery leaves
// the rows still queued on the other inputs to reach the operator after it,
// outside the checkpoint their sources' offsets already put them in, and only
// a capture of those rows into the checkpoint makes that sound.
class MultiInputAlignment {
public:
    // Alias to CheckpointBarrier::Mode. The aligner formerly
    // owned this enum and chose the alignment policy at startup; that
    // captured-at-startup model has been replaced by a per-barrier
    // mode (the barrier carries its own Mode and the aligner honours
    // it). The alias is kept so existing call sites that name
    // `MultiInputAlignment::Mode::Unaligned` continue to compile.
    using Mode = CheckpointBarrier::Mode;

    // `captures_in_flight` says whether the owning operator, when a barrier
    // stamped Unaligned arrives, captures the rows still queued on its other
    // inputs into the checkpoint. Only then may the aligner forward that
    // barrier on its first delivery. No operator does today, so every runner
    // leaves it false, and an Unaligned barrier is aligned exactly as an
    // Aligned one and forwarded marked Aligned: unaligned and adaptive
    // checkpoints align at every fan-in. The flag keeps the
    // forward-on-first-delivery path for a capture that is correct.
    explicit MultiInputAlignment(std::size_t input_count, bool captures_in_flight = false)
        : input_wm_(input_count, Watermark::min()),
          paused_(input_count, false),
          closed_(input_count, false),
          cancelled_close_(input_count, false),
          drained_(input_count, false),
          idle_(input_count, false),
          captures_in_flight_(captures_in_flight) {}

    // Stamp the aligner with the OperatorId.value() of the operator
    // it belongs to so per-op barrier alignment metrics route to the
    // right counter. Zero (the default) disables metric emission so
    // unit tests of the aligner itself don't fabricate counter
    // entries.
    // `metrics` is the registry they go to: the RuntimeContext's, so that on
    // the plugin path they reach the host's registry and not the job module's
    // copy of the global one (nullptr: the global registry).
    void set_operator_id(std::uint64_t op_id, MetricsRegistry* metrics = nullptr) noexcept {
        op_id_for_metrics_ = op_id;
        metrics_ = metrics;
    }

    [[nodiscard]] bool captures_in_flight() const noexcept { return captures_in_flight_; }

    // Called once, on the first forced alignment: a barrier stamped
    // Unaligned that this aligner aligned across more than one input because
    // the operator does not capture in-flight rows. The runner routes it to
    // its log; the count of every one is forced_alignments() and, with an
    // operator id set, clink_op_barrier_forced_alignments_total.
    void set_forced_alignment_notice(std::function<void(CheckpointBarrier)> notice) {
        forced_alignment_notice_ = std::move(notice);
    }

    [[nodiscard]] std::uint64_t forced_alignments() const noexcept { return forced_alignments_; }

    struct WatermarkAdvance {
        bool forward{false};
        Watermark watermark{Watermark::min()};
    };

    struct BarrierAdvance {
        bool forward{false};
        CheckpointBarrier barrier{};
        // True iff this advance forwards an Unaligned barrier on its FIRST
        // delivery, which only an aligner built with captures_in_flight does:
        // the runner then captures the rows still queued on the other inputs
        // (pending_inputs_for) into the checkpoint. Always false otherwise,
        // since by the time the barrier forwards every input has delivered it.
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
    // Input i pauses (paused_[i] = true) and the barrier forwards once every
    // alive input has delivered the same id, whatever its stamped Mode. An
    // Unaligned stamp changes that only for an aligner built with
    // captures_in_flight: its first delivery forwards immediately and never
    // pauses, subsequent deliveries of the same id are absorbed silently, and
    // the first-delivery advance carries `unaligned_first=true` so the runner
    // captures the still-pending inputs' in-flight rows into the snapshot.
    // Without that flag an Unaligned barrier is aligned like any other and
    // forwarded marked Aligned, since that is the cut it now describes.
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
            seen_terminal_[b.id().value()] = b.is_terminal();
            first_seen_time_[b.id().value()] = std::chrono::steady_clock::now();
        } else if (!b.is_terminal()) {
            seen_terminal_[b.id().value()] = false;
        }
        const bool first_for_this_barrier = !any_true_(seen);
        seen[i] = true;

        if (forwards_on_first_delivery_(b.id().value())) {
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
    // EventTime::max() to the watermark min, so survivors' time is never
    // held back by a gone input). `cancelled` records WHY it closed
    // (followups item 79): a FINISHED close is genuine end-of-input, but a
    // CANCELLED close is teardown, and once every input is gone the
    // recompute must not read the wreckage as end-of-time - that fired
    // every open window into a still-live sink during cancel, appending a
    // nondeterministic partial tail (QUAL-07's qcum, 11,532 rows).
    //
    // An input that delivered a drain marker first (see on_drain) closes as a
    // handoff, which counts the same way as a cancel here.
    BarrierAdvance on_input_closed(std::size_t i, bool cancelled = false) {
        if (closed_[i]) {
            return {};
        }
        closed_[i] = true;
        cancelled_close_[i] = cancelled || drained_[i];
        input_wm_[i] = Watermark::max();  // closed inputs no longer hold back time
        // A close may complete alignment for a pending barrier; check each.
        // A barrier forwarded on its first delivery is not pending: the
        // close only lets its bookkeeping go, and it must not forward again.
        std::vector<std::uint64_t> ids;
        ids.reserve(seen_barriers_.size());
        for (const auto& [id, _] : seen_barriers_) {
            ids.push_back(id);
        }
        for (const auto id : ids) {
            if (forwards_on_first_delivery_(id)) {
                maybe_drop_seen_(id);
                continue;
            }
            if (auto adv = check_alignment_(id); adv.forward) {
                return adv;
            }
        }
        // Even if no barrier completed, the watermark min may have moved.
        // Caller is expected to call recompute_watermark_via_close() too, or
        // we expose it inline here:
        return {};
    }

    // Record that input i delivered a rescale drain marker: its subtask is
    // handing its key groups to a successor that restores from the cutover
    // checkpoint and carries on, so the close that follows is not end of
    // input. That close still frees the survivors' time, but it can never be
    // what turns an all-closed set into an end-of-time watermark, so no
    // end-of-time goes downstream and the event-time timers do not all fire
    // at the handoff. This governs the watermark only: a runner whose inputs
    // all close Finished still calls flush() at exit, and a window operator's
    // flush() fires its open windows there.
    void on_drain(std::size_t i) { drained_[i] = true; }

    // Run a watermark recompute (useful after on_input_closed which doesn't
    // emit a watermark advance directly).
    WatermarkAdvance refresh_watermark() { return recompute_watermark_(); }

    // Admit a new input mid-run (hot rescale downstream rebind, design
    // record 008). Legal only while NO barrier is in flight: the per-
    // barrier delivery bitmaps were sized to the membership at first
    // delivery, and growing the membership mid-alignment would let a
    // barrier complete against a set that never included the newcomer.
    // The cutover choreography guarantees the window (the checkpoint
    // clock pauses between the cutover checkpoint and Complete); the
    // refusal turns a violated guarantee into a visible failure instead
    // of a mis-aligned snapshot. Completed-but-lingering unaligned
    // entries (their GC only runs on subsequent deliveries) are swept
    // before judging, so a quiet single-input history cannot wedge the
    // add.
    //
    // The joining input's watermark starts clamped to the current
    // emitted global - the idle-reactivation rule. Note what this does
    // NOT buy: the monotone emit guard already prevents regression, and
    // an advance still waits for the newcomer's first watermark either
    // way (the min includes it) - which the upstream's post-swap
    // re-broadcast delivers immediately. The clamp keeps the rules
    // uniform and the newcomer's recorded position from reading as
    // pre-history. Returns the new input's index, or nullopt when
    // refused.
    std::optional<std::size_t> add_input() {
        std::vector<std::uint64_t> ids;
        ids.reserve(seen_barriers_.size());
        for (const auto& [id, _] : seen_barriers_) {
            ids.push_back(id);
        }
        for (const auto id : ids) {
            maybe_drop_seen_(id);
        }
        if (!seen_barriers_.empty()) {
            return std::nullopt;
        }
        input_wm_.push_back(Watermark{emitted_wm_.timestamp()});
        paused_.push_back(false);
        closed_.push_back(false);
        cancelled_close_.push_back(false);
        drained_.push_back(false);
        idle_.push_back(false);
        return input_wm_.size() - 1;
    }

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
    // An operator that captures in-flight rows consults this on the
    // `adv.unaligned_first` advance to know which channels it must
    // capture before the barrier moves downstream; no runner does
    // today. The aligner records same-id deliveries via
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

    // Whether checkpoint `ck_id` was forwarded on its first delivery: it is
    // stamped Unaligned and the operator captures in-flight rows.
    bool forwards_on_first_delivery_(std::uint64_t ck_id) const {
        if (!captures_in_flight_) {
            return false;
        }
        const auto it = seen_mode_.find(ck_id);
        return it != seen_mode_.end() && it->second == Mode::Unaligned;
    }

    // A barrier forwarded on its first delivery needs its bitmap only to
    // absorb the later deliveries of its id. GC it once every alive input
    // has been seen, to keep the map small.
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
        seen_terminal_.erase(ck_id);
        first_seen_time_.erase(ck_id);
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
        // All alive inputs delivered this barrier, so release it, carrying
        // the stamped mode on so downstream operators see the same policy.
        // An Unaligned stamp reaches here only when the operator does not
        // capture in-flight rows, and was aligned like any other: it goes on
        // marked Aligned, which is the cut it now describes.
        Mode m = Mode::Aligned;
        bool forced = false;
        if (auto mit = seen_mode_.find(ck_id); mit != seen_mode_.end()) {
            forced = mit->second == Mode::Unaligned && !captures_in_flight_;
            m = forced ? Mode::Aligned : mit->second;
        }
        // A single input has nothing to wait for, so only an alignment
        // across several inputs counts as forced.
        const bool across_inputs = flags.size() > 1;
        // A terminal barrier (a bounded source's local end-of-stream commit)
        // stays terminal once every input has delivered it or finished, so
        // the sink behind the fan-in commits the tail as it would behind one
        // input. Not when an input closed by cancellation or for a rescale
        // handoff: that is not end of input, and the tail must not be
        // published.
        bool terminal = false;
        if (auto tit = seen_terminal_.find(ck_id); tit != seen_terminal_.end()) {
            terminal = tit->second && std::none_of(cancelled_close_.begin(),
                                                   cancelled_close_.end(),
                                                   [](bool c) { return c; });
        }
        BarrierAdvance adv;
        adv.forward = true;
        adv.barrier = CheckpointBarrier{CheckpointId{ck_id}, terminal, m};
        seen_barriers_.erase(it);
        seen_mode_.erase(ck_id);
        seen_terminal_.erase(ck_id);
        if (op_id_for_metrics_ != 0) {
            if (auto tit = first_seen_time_.find(ck_id); tit != first_seen_time_.end()) {
                const auto wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                         std::chrono::steady_clock::now() - tit->second)
                                         .count();
                clink::metrics::ckpt::barrier_aligned(
                    metrics_, op_id_for_metrics_, static_cast<std::uint64_t>(wait_ns));
            } else {
                clink::metrics::ckpt::barrier_aligned(metrics_, op_id_for_metrics_, 0);
            }
        }
        first_seen_time_.erase(ck_id);
        // Unpause every input (they may pause again on a future barrier).
        for (std::size_t j = 0; j < paused_.size(); ++j) {
            if (!closed_[j]) {
                paused_[j] = false;
            }
        }
        if (forced && across_inputs) {
            note_forced_alignment_(adv.barrier);
        }
        return adv;
    }

    // Count a forced alignment, and hand the first one to the runner's
    // notice so it is logged once per operator rather than per barrier.
    void note_forced_alignment_(const CheckpointBarrier& b) {
        ++forced_alignments_;
        if (op_id_for_metrics_ != 0) {
            clink::metrics::ckpt::barrier_forced_aligned(metrics_, op_id_for_metrics_);
        }
        if (forced_alignments_ == 1 && forced_alignment_notice_) {
            forced_alignment_notice_(b);
        }
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
        //
        // Everyone-closed carries a caveat (item 79): closed inputs sit at
        // Watermark::max(), so the min over an all-closed set is
        // end-of-time - correct when every input FINISHED (a bounded
        // pipeline's genuine end of input), but a set that includes a
        // CANCELLED close is teardown wreckage, and emitting end-of-time
        // from it fires every open event-time window into whatever is
        // still attached downstream. Cancel is not end-of-input: hold the
        // watermark where it was and let the runner exit.
        if (!any_alive) {
            for (std::size_t i = 0; i < cancelled_close_.size(); ++i) {
                if (cancelled_close_[i]) {
                    return WatermarkAdvance{};
                }
            }
        }
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
    // Whether every delivery of a pending checkpoint id so far was a
    // terminal barrier. GC'd alongside seen_barriers_.
    std::unordered_map<std::uint64_t, bool> seen_terminal_;
    std::vector<Watermark> input_wm_;
    std::vector<bool> paused_;
    std::vector<bool> closed_;
    // Whether each closed input closed by CANCELLATION rather than clean
    // end-of-input. Only consulted once every input is closed: an
    // all-closed set that includes a cancelled close is teardown, and the
    // recompute must not turn it into an end-of-time watermark (item 79).
    std::vector<bool> cancelled_close_;
    // Whether each input delivered a drain marker (see on_drain); its close
    // is then recorded in cancelled_close_ as not end of input.
    std::vector<bool> drained_;
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
    MetricsRegistry* metrics_{nullptr};
    Watermark emitted_wm_{Watermark::min()};
    bool last_emitted_idle_{false};
    // See the constructor. False everywhere today.
    bool captures_in_flight_{false};
    // Barriers stamped Unaligned that were aligned across several inputs
    // because the operator does not capture in-flight rows, and the notice
    // the first of them is handed to.
    std::uint64_t forced_alignments_{0};
    std::function<void(CheckpointBarrier)> forced_alignment_notice_;
};

}  // namespace clink
