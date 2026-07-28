#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

// In-memory source that emits COLUMNAR string-keyed batches: a 3-column Arrow
// RecordBatch {event_time(null), key:utf8, value:int64} built directly from its
// (key, value) pairs (no Record objects). A columnar-aware downstream (e.g.
// ColumnarKeyedStringAggregateOperator) groups+aggregates straight off the Arrow
// buffers, allocating a std::string only per DISTINCT key (heterogeneous map
// lookup), not per record; a row-only downstream lazily decodes the sidecar (one
// std::string per row). The string-keyed counterpart to ColumnarKeyedVectorSource.
class ColumnarStringKeyedVectorSource final : public Source<std::pair<std::string, std::int64_t>> {
public:
    using KV = std::pair<std::string, std::int64_t>;

    explicit ColumnarStringKeyedVectorSource(std::vector<KV> data, std::size_t batch_size = 4096)
        : data_(std::move(data)), batch_size_(batch_size == 0 ? 1 : batch_size) {
        batcher_ = string_keyed_arrow_batcher();
        auto parse = batcher_.parse;
        materialize_ =
            [parse = std::move(parse)](const arrow::RecordBatch& rb) -> std::vector<Record<KV>> {
            auto b = parse(rb);
            return b ? b->take_records() : std::vector<Record<KV>>{};
        };
    }

    bool produce(Emitter<KV>& out) override {
        if (this->cancelled() || pos_ >= data_.size()) {
            return false;
        }
        const std::size_t end = std::min(pos_ + batch_size_, data_.size());
        const auto n = static_cast<std::int64_t>(end - pos_);

        arrow::Int64Builder ts_b;
        arrow::StringBuilder k_b;
        arrow::Int64Builder v_b;
        if (!ts_b.AppendNulls(n).ok() || !k_b.Reserve(n).ok() || !v_b.Reserve(n).ok()) {
            return false;
        }
        for (std::size_t i = pos_; i < end; ++i) {
            if (!k_b.Append(data_[i].first).ok() || !v_b.Append(data_[i].second).ok()) {
                return false;
            }
        }
        std::shared_ptr<arrow::Array> ts_arr;
        std::shared_ptr<arrow::Array> k_arr;
        std::shared_ptr<arrow::Array> v_arr;
        if (!ts_b.Finish(&ts_arr).ok() || !k_b.Finish(&k_arr).ok() || !v_b.Finish(&v_arr).ok()) {
            return false;
        }
        // KNOWN FLAKY TSAN RACE, seen 2026-07-28. The three shared_ptr<arrow::Array>
        // locals below release their references when this frame exits, which is AFTER
        // emit_data has handed the batch downstream. TSan intermittently reports a
        // data race between the last-use `operator delete` on this thread and a read
        // of the same control block on the consumer thread, via
        // ColumnarKeyedStringAggregate.EndToEndColumnarPipelineDecodesZeroRowsOnIngest.
        //
        // Attribution, so the next TSan failure here is not re-investigated: the SAME
        // commit both failed and passed a full CI TSan run, so it is load-dependent and
        // not tied to any recent change. Running the single test in isolation under
        // TSan with tsan-suppressions.txt does NOT reproduce it (0 races over 3 runs at
        // two commits); running it WITHOUT the suppressions file reports it every time,
        // which is a measurement artefact rather than a second finding.
        //
        // Not diagnosed. A refcount is atomic, so a plain concurrent inc/dec would not
        // race - which means either the handoff lacks a happens-before edge TSan can
        // see, or the reported stack is not the whole story. Worth pinning before
        // anyone trusts a green TSan run completely: an intermittently-red gate is only
        // marginally better than a red one.
        auto rb = arrow::RecordBatch::Make(batcher_.schema(), n, {ts_arr, k_arr, v_arr});

        out.emit_data(Batch<KV>{std::move(rb), static_cast<std::size_t>(n), materialize_});
        pos_ = end;
        return pos_ < data_.size();
    }

    std::string name() const override { return "columnar_string_keyed_vector_source"; }

private:
    std::vector<KV> data_;
    std::size_t batch_size_;
    std::size_t pos_{0};
    ArrowBatcher<KV> batcher_;
    Batch<KV>::MaterializeFn materialize_;
};

}  // namespace clink
