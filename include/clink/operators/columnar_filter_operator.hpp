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
// Arrow kernel availability: CORRECTED 2026-07-26. This comment previously said the
// arithmetic/comparison kernels could not be reached because
// arrow::compute::Initialize() "is not an exported symbol in this package (verified
// against Arrow::arrow_shared)". That verification looked in the wrong library. From
// Arrow 15 the kernel set lives in libarrow_compute, a SEPARATE library, and
// Initialize() is exported from there. Linking it and calling Initialize() takes the
// function registry from 13 functions to 305, and greater_equal then answers correctly -
// measured against both the Homebrew and the pinned Arrow 24 prefixes.
//
// The engine now links it when present and exposes clink::arrow_compute_available()
// (include/clink/core/arrow_compute.hpp).
//
// The hand-rolled mask below is KEPT, deliberately, and is not dead code:
//   * a build against an Arrow without the compute library is still supported;
//   * kernel null-handling and type promotion are not automatically identical to this
//     scan, so swapping carriers is a behavioural change that needs its own equivalence
//     test, not a drop-in;
//   * it is not where the time goes. clink's own per-stage attribution puts the
//     arithmetic in a Kafka JSON pipeline at ~4% of worker CPU, so this is a capability
//     unblock rather than a throughput one, and it should be swapped only behind a
//     measurement on a filter-heavy shape.
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
