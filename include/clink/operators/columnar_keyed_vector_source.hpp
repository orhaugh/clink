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

// In-memory source that emits COLUMNAR keyed batches: it builds a 3-column
// Arrow RecordBatch {event_time(null), key, value} directly from its
// (key, value) pairs (no Record objects) and emits a Batch<pair<int64,int64>>
// carrying that RecordBatch as a sidecar. A columnar-aware downstream (e.g.
// ColumnarKeyedAggregateOperator) groups+aggregates straight off the Arrow buffers
// with zero row materialization; a row-only downstream lazily decodes the
// sidecar on first row access. The keyed counterpart to ColumnarVectorSource,
// for the columnar keyed-aggregation path. Event-time is left null.
class ColumnarKeyedVectorSource final : public Source<std::pair<std::int64_t, std::int64_t>> {
public:
    using KV = std::pair<std::int64_t, std::int64_t>;

    // event_times_ms: optional per-record event time (parallel to data). Empty
    // => all event_time null (processing-time semantics, the increment-2
    // default). Non-empty (same length as data) => the event_time column is
    // populated, so a downstream window operator can assign windows by event
    // time off the columnar buffer.
    explicit ColumnarKeyedVectorSource(std::vector<KV> data,
                                       std::size_t batch_size = 4096,
                                       std::vector<std::int64_t> event_times_ms = {})
        : data_(std::move(data)),
          event_times_(std::move(event_times_ms)),
          batch_size_(batch_size == 0 ? 1 : batch_size) {
        batcher_ = int64_keyed_arrow_batcher();
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
        arrow::Int64Builder k_b;
        arrow::Int64Builder v_b;
        const bool have_ts = event_times_.size() == data_.size();
        if (!ts_b.Reserve(n).ok() || !k_b.Reserve(n).ok() || !v_b.Reserve(n).ok()) {
            return false;
        }
        for (std::size_t i = pos_; i < end; ++i) {
            // event_time: the supplied stamp, or null when none was given.
            const bool ts_ok = have_ts ? ts_b.Append(event_times_[i]).ok() : ts_b.AppendNull().ok();
            if (!ts_ok || !k_b.Append(data_[i].first).ok() || !v_b.Append(data_[i].second).ok()) {
                return false;
            }
        }
        std::shared_ptr<arrow::Array> ts_arr;
        std::shared_ptr<arrow::Array> k_arr;
        std::shared_ptr<arrow::Array> v_arr;
        if (!ts_b.Finish(&ts_arr).ok() || !k_b.Finish(&k_arr).ok() || !v_b.Finish(&v_arr).ok()) {
            return false;
        }
        auto rb = arrow::RecordBatch::Make(batcher_.schema(), n, {ts_arr, k_arr, v_arr});

        out.emit_data(Batch<KV>{std::move(rb), static_cast<std::size_t>(n), materialize_});
        pos_ = end;
        return pos_ < data_.size();
    }

    std::string name() const override { return "columnar_keyed_vector_source"; }

private:
    std::vector<KV> data_;
    std::vector<std::int64_t> event_times_;
    std::size_t batch_size_;
    std::size_t pos_{0};
    ArrowBatcher<KV> batcher_;
    Batch<KV>::MaterializeFn materialize_;
};

}  // namespace clink
