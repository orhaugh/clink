#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#ifdef CLINK_HAS_ARROW
#include <arrow/api.h>
#include <arrow/compute/api.h>
#endif

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
            // Forward: a columnar batch rides through unchanged (sidecar kept).
            return outputs_[0]->push(StreamElement<Out>::data(std::move(batch)));
        }
#ifdef CLINK_HAS_ARROW
        // Columnar hash partition: keep each per-subtask sub-batch columnar
        // (Arrow gather via the registered "filter" kernel) so the shuffle does
        // not de-columnarize the stream. Routing is IDENTICAL to the row path -
        // the SAME partitioner runs on the same per-row values - so state
        // ownership / rescale agreement is preserved exactly; only the carrier
        // (columnar vs row sub-batch) differs. Falls back to the row split on
        // any Arrow surprise (decided before any push, so no double-emit).
        if (batch.is_columnar() && batch.arrow()) {
            if (auto subs = partition_columnar_(batch); subs.has_value()) {
                bool ok = true;
                for (std::size_t i = 0; i < outputs_.size(); ++i) {
                    if ((*subs)[i].size() > 0) {
                        ok &= outputs_[i]->push(StreamElement<Out>::data(std::move((*subs)[i])));
                    }
                }
                return ok;
            }
        }
#endif
        // Partition into per-output sub-batches (row path).
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

    // Drain marker. Broadcast to every downstream channel
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
#ifdef CLINK_HAS_ARROW
    // Gather a columnar batch into one columnar sub-batch per output subtask.
    // Targets are computed with the SAME partitioner on the same per-row values
    // (so routing is byte-identical to the row split), then each subtask's rows
    // are gathered with Arrow's "filter" kernel into a sub-RecordBatch wrapped
    // as a columnar Batch<Out> reusing the parent's materialize closure. Returns
    // nullopt on any Arrow failure so the caller falls back to the row split.
    std::optional<std::vector<Batch<Out>>> partition_columnar_(const Batch<Out>& batch) {
        const auto& rb = batch.arrow();
        const std::int64_t n = rb->num_rows();
        const std::size_t num = outputs_.size();

        // One typed decode to obtain the per-row values the partitioner needs.
        const auto& recs = batch.records();
        if (static_cast<std::int64_t>(recs.size()) != n) {
            return std::nullopt;
        }
        std::vector<arrow::BooleanBuilder> masks(num);
        for (auto& m : masks) {
            if (!m.Reserve(n).ok()) {
                return std::nullopt;
            }
        }
        for (std::int64_t i = 0; i < n; ++i) {
            const std::size_t target =
                partitioner_(recs[static_cast<std::size_t>(i)].value()) % num;
            for (std::size_t t = 0; t < num; ++t) {
                masks[t].UnsafeAppend(t == target);
            }
        }
        std::vector<Batch<Out>> out(num);
        for (std::size_t t = 0; t < num; ++t) {
            std::shared_ptr<arrow::Array> mask;
            if (!masks[t].Finish(&mask).ok()) {
                return std::nullopt;
            }
            auto filtered = arrow::compute::Filter(arrow::Datum(rb), arrow::Datum(mask));
            if (!filtered.ok() || filtered->kind() != arrow::Datum::RECORD_BATCH) {
                return std::nullopt;
            }
            auto sub_rb = filtered->record_batch();
            if (sub_rb->num_rows() > 0) {
                out[t] = batch.with_arrow(sub_rb, static_cast<std::size_t>(sub_rb->num_rows()));
            }
        }
        return out;
    }
#endif

    std::vector<std::shared_ptr<Channel>> outputs_;
    Partitioner partitioner_;
};

}  // namespace clink
