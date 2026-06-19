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

// In-memory source that emits COLUMNAR string batches: it builds an Arrow
// RecordBatch {event_time(null), value:utf8} directly from its strings (no
// Record objects) and emits a Batch<std::string> carrying that RecordBatch as a
// sidecar. A columnar-aware downstream (e.g. ColumnarStringFilterOperator) scans
// the Arrow StringArray with zero std::string construction; a row-only
// downstream lazily decodes the sidecar (one std::string alloc per row) on first
// row access. The string counterpart to ColumnarVectorSource - the columnar win
// is larger here because materialization means a heap alloc per record.
class ColumnarStringVectorSource final : public Source<std::string> {
public:
    explicit ColumnarStringVectorSource(std::vector<std::string> data,
                                        std::size_t batch_size = 4096)
        : data_(std::move(data)), batch_size_(batch_size == 0 ? 1 : batch_size) {
        batcher_ = string_arrow_batcher();
        auto parse = batcher_.parse;
        materialize_ = [parse = std::move(parse)](
                           const arrow::RecordBatch& rb) -> std::vector<Record<std::string>> {
            auto b = parse(rb);
            return b ? b->take_records() : std::vector<Record<std::string>>{};
        };
    }

    bool produce(Emitter<std::string>& out) override {
        if (this->cancelled() || pos_ >= data_.size()) {
            return false;
        }
        const std::size_t end = std::min(pos_ + batch_size_, data_.size());
        const auto n = static_cast<std::int64_t>(end - pos_);

        arrow::Int64Builder ts_b;
        arrow::StringBuilder v_b;
        // event_time: all null. value: the slice.
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

        out.emit_data(Batch<std::string>{std::move(rb), static_cast<std::size_t>(n), materialize_});
        pos_ = end;
        return pos_ < data_.size();
    }

    std::string name() const override { return "columnar_string_vector_source"; }

private:
    std::vector<std::string> data_;
    std::size_t batch_size_;
    std::size_t pos_{0};
    ArrowBatcher<std::string> batcher_;
    Batch<std::string>::MaterializeFn materialize_;
};

}  // namespace clink
