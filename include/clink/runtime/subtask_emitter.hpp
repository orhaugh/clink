#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "clink/core/stream_element.hpp"
#include "clink/runtime/bounded_channel.hpp"

namespace clink {

// SubtaskEmitter is the parallel-aware analogue of Emitter<Out>. It owns a
// vector of downstream channels - one per parallel subtask of the next stage
// - and routes each emitted item across them according to the routing mode.
//
// Three routing modes:
//
//   * **Forward** (no partitioner, single output): every record goes to the
//     single output channel. Equivalent to the original 1:1 Emitter.
//
//   * **Hash partition**: data records run through a user-supplied
//     partitioner (typically `hash(key) % N`); each record is enqueued on
//     exactly one downstream channel. Watermarks and barriers are
//     broadcast to ALL downstream channels - they're stream-wide signals,
//     not per-key.
//
//   * **Fan-in**: a degenerate case of hash partition with N=1. All
//     records, watermarks, barriers go to the single channel.
//
// The Emitter API (`emit_data`, `emit_watermark`, `emit_barrier`) matches
// the existing single-channel Emitter so operator code is unchanged.
template <typename Out>
class SubtaskEmitter {
public:
    using Channel = BoundedChannel<StreamElement<Out>>;
    // Returns the index of the downstream subtask that should receive this
    // record. Index is taken modulo the number of channels.
    using Partitioner = std::function<std::size_t(const Out&)>;

    SubtaskEmitter() = default;

    explicit SubtaskEmitter(std::vector<std::shared_ptr<Channel>> outputs,
                            Partitioner partitioner = {})
        : outputs_(std::move(outputs)), partitioner_(std::move(partitioner)) {}

    // Single-output convenience (forward).
    explicit SubtaskEmitter(std::shared_ptr<Channel> output) : outputs_{std::move(output)} {}

    bool emit_data(Batch<Out> batch) {
        if (outputs_.empty()) {
            return true;
        }
        if (outputs_.size() == 1 || !partitioner_) {
            return outputs_[0]->push(StreamElement<Out>::data(std::move(batch)));
        }
        // Partition into per-output sub-batches.
        std::vector<Batch<Out>> sub(outputs_.size());
        for (auto& rec : batch) {
            const std::size_t target = partitioner_(rec.value()) % outputs_.size();
            sub[target].push(std::move(rec));
        }
        bool ok = true;
        for (std::size_t i = 0; i < outputs_.size(); ++i) {
            if (!sub[i].empty()) {
                ok &= outputs_[i]->push(StreamElement<Out>::data(std::move(sub[i])));
            }
        }
        return ok;
    }

    bool emit_watermark(Watermark wm) {
        bool ok = true;
        for (auto& ch : outputs_) {
            ok &= ch->push(StreamElement<Out>::watermark(wm));
        }
        return ok;
    }

    bool emit_barrier(CheckpointBarrier b) {
        bool ok = true;
        for (auto& ch : outputs_) {
            ok &= ch->push(StreamElement<Out>::barrier(b));
        }
        return ok;
    }

    // Phase 29d-3: drain marker. Broadcast to every downstream channel
    // so the entire fan-out gets the "this upstream subtask is winding
    // down" signal before the channel closes behind it.
    bool emit_drain(DrainMarker d) {
        bool ok = true;
        for (auto& ch : outputs_) {
            ok &= ch->push(StreamElement<Out>::drain(d));
        }
        return ok;
    }

    std::size_t output_count() const noexcept { return outputs_.size(); }

    // Late binding - used by Dag to wire output channels after the emitter's
    // owner subtask has been declared but before any thread starts. Not
    // thread-safe vs concurrent emit calls; the DAG must call this only
    // before LocalExecutor::start() is invoked.
    void attach(std::vector<std::shared_ptr<Channel>> outputs, Partitioner partitioner) {
        outputs_ = std::move(outputs);
        partitioner_ = std::move(partitioner);
    }

    // Close every owned output channel. Used by source/operator runner
    // cancel hooks so downstream consumers drain and exit promptly.
    void close_all() {
        for (auto& ch : outputs_) {
            ch->close();
        }
    }

private:
    std::vector<std::shared_ptr<Channel>> outputs_;
    Partitioner partitioner_;
};

}  // namespace clink
