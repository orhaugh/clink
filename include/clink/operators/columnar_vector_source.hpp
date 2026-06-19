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

// In-memory source that emits COLUMNAR int64 batches: it builds an Arrow
// RecordBatch directly from its values (no Record objects) and emits a
// Batch<int64_t> carrying that RecordBatch as a sidecar, so a columnar-aware
// downstream operator processes columns with zero row materialization. A
// row-only downstream still works - it lazily decodes the sidecar on first
// row access. The counterpart to the row VectorSource, for the columnar
// execution path. Event-time is left null (processing-time semantics).
class ColumnarVectorSource final : public Source<std::int64_t> {
public:
    explicit ColumnarVectorSource(std::vector<std::int64_t> data, std::size_t batch_size = 4096)
        : data_(std::move(data)), batch_size_(batch_size == 0 ? 1 : batch_size) {
        batcher_ = int64_arrow_batcher();
        auto parse = batcher_.parse;
        materialize_ = [parse = std::move(parse)](
                           const arrow::RecordBatch& rb) -> std::vector<Record<std::int64_t>> {
            auto b = parse(rb);
            return b ? b->take_records() : std::vector<Record<std::int64_t>>{};
        };
    }

    bool produce(Emitter<std::int64_t>& out) override {
        if (this->cancelled() || pos_ >= data_.size()) {
            return false;
        }
        const std::size_t end = std::min(pos_ + batch_size_, data_.size());
        const auto n = static_cast<std::int64_t>(end - pos_);

        arrow::Int64Builder ts_b;
        arrow::Int64Builder v_b;
        // event_time: all null (no event time). value: the slice.
        if (!ts_b.AppendNulls(n).ok() || !v_b.Reserve(n).ok()) {
            return false;
        }
        for (std::size_t i = pos_; i < end; ++i) {
            if (!v_b.Append(data_[i]).ok()) {
                return false;
            }
        }
        std::shared_ptr<arrow::Array> ts_arr;
        std::shared_ptr<arrow::Array> v_arr;
        if (!ts_b.Finish(&ts_arr).ok() || !v_b.Finish(&v_arr).ok()) {
            return false;
        }
        auto rb = arrow::RecordBatch::Make(batcher_.schema(), n, {ts_arr, v_arr});

        out.emit_data(
            Batch<std::int64_t>{std::move(rb), static_cast<std::size_t>(n), materialize_});
        pos_ = end;
        return pos_ < data_.size();
    }

    std::string name() const override { return "columnar_vector_source"; }

private:
    std::vector<std::int64_t> data_;
    std::size_t batch_size_;
    std::size_t pos_{0};
    ArrowBatcher<std::int64_t> batcher_;
    Batch<std::int64_t>::MaterializeFn materialize_;
};

}  // namespace clink
