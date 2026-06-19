#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

// Columnar-native filter on the int64 value column: keep rows where
// value >= threshold. When the input batch carries an Arrow RecordBatch
// sidecar it builds a boolean mask over the value column and runs Arrow's
// SIMD-optimized "filter" kernel to gather the passing rows of BOTH columns
// into a new RecordBatch, so event-time (column 0) rides along and ZERO rows
// are materialized. When the input is row-only (or the schema is unexpected)
// it falls back to the identical row predicate, so it is a drop-in for
// FilterOperator<int64_t>.
//
// Arrow kernel availability (this build): the "filter" SELECTION kernel is
// registered and used here for the gather. The arithmetic/comparison kernels
// (greater_equal, etc.) are NOT registered - the Arrow package ships them
// compiled-in but the default registry holds only ~13 core functions and the
// public arrow::compute::Initialize() that would register the rest is not an
// exported symbol in this package (verified against Arrow::arrow_shared). So
// the comparison mask is hand-rolled (a dense autovectorizable scan); a true
// greater_equal-kernel mask is unblocked only by a Docker Arrow build that
// exports Initialize() / auto-registers the kernels.
//
// Scope: int64 only, a single >= comparison. Generic columnar map / other
// types / keyed operators are out of scope.
class ColumnarFilterOperator final : public Operator<std::int64_t, std::int64_t> {
public:
    explicit ColumnarFilterOperator(std::int64_t threshold, std::string name = "columnar_filter")
        : threshold_(threshold), name_(std::move(name)) {
        // Reuse the int64 batcher's parse for lazy row materialization of the
        // columnar batches this operator emits (so a downstream row consumer
        // still works). Wrap it to return the row vector directly.
        auto parse = int64_arrow_batcher().parse;
        materialize_ = [parse = std::move(parse)](
                           const arrow::RecordBatch& rb) -> std::vector<Record<std::int64_t>> {
            auto b = parse(rb);
            return b ? b->take_records() : std::vector<Record<std::int64_t>>{};
        };
    }

    [[nodiscard]] bool supports_columnar() const noexcept override { return true; }

    // Vectorized fast path. Returns false (fall back to process()) on any
    // schema/compute surprise so a mismatched batch degrades cleanly.
    bool process_columnar(const StreamElement<std::int64_t>& element,
                          Emitter<std::int64_t>& out) override {
        if (!element.is_data() || !element.as_data().is_columnar()) {
            return false;
        }
        const auto& rb = element.as_data().arrow();
        if (!rb || rb->num_columns() < 2) {
            return false;
        }
        // Schema is {event_time(0), value(1)}; both int64. Anything else
        // falls back to the row path.
        const auto* ts = dynamic_cast<const arrow::Int64Array*>(rb->column(0).get());
        const auto* val = dynamic_cast<const arrow::Int64Array*>(rb->column(1).get());
        if (ts == nullptr || val == nullptr) {
            return false;
        }
        const std::int64_t n = rb->num_rows();
        (void)ts;  // selected by Filter below, not read directly here
        // Build the boolean selection mask (value >= threshold) with a dense,
        // autovectorizable scan over the value buffer. The comparison is
        // hand-rolled because the greater_equal compute kernel is not
        // registered in this Arrow package (see header note); the SELECTION
        // below uses Arrow's registered, SIMD-optimized "filter" kernel.
        arrow::BooleanBuilder mask_b;
        if (!mask_b.Reserve(n).ok()) {
            return false;
        }
        for (std::int64_t i = 0; i < n; ++i) {
            mask_b.UnsafeAppend(val->Value(i) >= threshold_);
        }
        std::shared_ptr<arrow::Array> mask;
        if (!mask_b.Finish(&mask).ok()) {
            return false;
        }
        // Vectorized multi-column gather via the Filter kernel: event_time
        // (column 0) rides along, so per-record event time is preserved.
        auto filtered = arrow::compute::Filter(arrow::Datum(rb), arrow::Datum(mask));
        if (!filtered.ok() || filtered->kind() != arrow::Datum::RECORD_BATCH) {
            return false;
        }
        auto out_rb = filtered->record_batch();
        const auto out_n = static_cast<std::size_t>(out_rb->num_rows());
        if (out_n == 0) {
            return true;  // nothing passes; emit nothing (matches row filter)
        }
        out.emit_data(Batch<std::int64_t>{std::move(out_rb), out_n, materialize_});
        return true;
    }

    // Row path: the fallback (row-only upstream) and the watermark/barrier
    // forwarder. Identical predicate to the columnar path.
    void process(const StreamElement<std::int64_t>& element, Emitter<std::int64_t>& out) override {
        if (element.is_data()) {
            const Batch<std::int64_t>& in_batch = element.as_data();
            Batch<std::int64_t> out_batch;
            for (const auto& record : in_batch) {
                if (record.value() >= threshold_) {
                    out_batch.push(record);
                }
            }
            if (!out_batch.empty()) {
                out.emit_data(std::move(out_batch));
            }
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    std::string name() const override { return name_; }

private:
    std::int64_t threshold_;
    std::string name_;
    Batch<std::int64_t>::MaterializeFn materialize_;
};

}  // namespace clink
