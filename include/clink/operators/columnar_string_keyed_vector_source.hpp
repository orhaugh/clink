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
        // The arrays are handed over to the batch wholly - moved into an explicit
        // vector (an initializer list would silently copy: its elements are const) -
        // so after emit_data this thread holds no reference to any Arrow object in
        // the emitted batch, and the final release always runs on the thread that
        // read the data.
        //
        // That discipline exists because of a diagnosed TSan false positive (fired
        // once in CI on 2026-07-28; same commit also passed, so load-dependent).
        // When these locals outlived the emit, two release orders were possible.
        // Normal order: this thread dropped its refs first (a visible atomic RMW on
        // the control block), the consumer's element did the last release, and the
        // frees landed on the thread that read - nothing to report. Flake order
        // (this thread preempted between emit_data and scope exit): the consumer
        // dropped FIRST, but that decrement runs inside ~SimpleRecordBatch in
        // libarrow.so, which is not built with TSan (zero __tsan_* imports), so TSan
        // recorded nothing; the last release then ran HERE, visibly, deleting the
        // make_shared co-allocated array block whose bytes the consumer had just
        // read via Value(i). Read on one thread, free on another, and the only
        // ordering edge routed through an uninstrumented library: reported as a
        // race. The refcount's acq_rel ordering makes the free correct on real
        // hardware; the report was a tool blind spot, not a bug.
        //
        // Established by experiment, not just by fit: a 500us sleep between
        // emit_data and scope exit (forcing the flake order every batch) made the
        // report fire 20/20 runs with the CI stack pair; with this handover in
        // place and the sleep still forcing the order, 0/20. Residual class, never
        // observed and not closable from clink code: two threads co-owning Arrow
        // objects where the reader's release runs inside libarrow's destructor
        // chain (an input element whose columns pass through to an emitted batch,
        // or fan-out siblings sharing one batch) can in principle still produce
        // this shape; closing it outright needs an Arrow built with TSan.
        std::vector<std::shared_ptr<arrow::Array>> cols;
        cols.reserve(3);
        cols.push_back(std::move(ts_arr));
        cols.push_back(std::move(k_arr));
        cols.push_back(std::move(v_arr));
        auto rb = arrow::RecordBatch::Make(batcher_.schema(), n, std::move(cols));

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
