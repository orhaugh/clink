#pragma once

// ColumnarRowFilterOperator - columnar-native WHERE on the SQL Row channel.
//
// When the input Batch<Row> carries an Arrow RecordBatch sidecar (a columnar
// source like Parquet, an upstream columnar operator, or the sidecar-preserving
// Row wire) this evaluates the json_predicate against the Arrow columns: each
// referenced column's cell is read straight from the typed buffer with NO
// Row/JSON materialization, a boolean selection mask is built, and Arrow's
// SIMD "filter" kernel gathers the passing rows of EVERY column into a new
// RecordBatch. The output stays columnar (sidecar set, rows lazily decoded) so
// the rest of the chain keeps riding the fast path. Row-only input (or any
// Arrow surprise before emit) falls back to the identical row predicate, so it
// is a drop-in for FilterOperator<Row> + json_predicate with byte-identical
// results.

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#ifdef CLINK_HAS_ARROW

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/metrics/operator_metrics.hpp"
#include "clink/operators/json_predicate.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace clink {

class ColumnarRowFilterOperator final : public Operator<sql::Row, sql::Row> {
public:
    explicit ColumnarRowFilterOperator(std::shared_ptr<clink::config::JsonValue> predicate,
                                       std::string name = "filter_row_predicate")
        : predicate_(std::move(predicate)), name_(std::move(name)) {}

    [[nodiscard]] bool supports_columnar() const noexcept override { return true; }

    // Vectorized fast path. Returns false (fall back to process()) on any
    // Arrow surprise BEFORE emitting, so a mismatched batch degrades cleanly.
    bool process_columnar(const StreamElement<sql::Row>& element, Emitter<sql::Row>& out) override {
        if (!element.is_data() || !element.as_data().is_columnar()) {
            return false;
        }
        const auto& rb = element.as_data().arrow();
        if (!rb) {
            return false;
        }
        const std::int64_t n = rb->num_rows();

        // Build the boolean selection mask by evaluating the predicate per row
        // against the Arrow cells. read_cell yields the exact same JsonValue the
        // row path sees, so the predicate semantics are identical - we just skip
        // materializing the Row map and re-parsing JSON. The gather below is the
        // vectorized part (Arrow's registered "filter" kernel).
        arrow::BooleanBuilder mask_b;
        if (!mask_b.Reserve(n).ok()) {
            return false;
        }
        std::int64_t kept = 0;
        for (std::int64_t i = 0; i < n; ++i) {
            auto resolve = [&](const std::string& nm) -> clink::config::JsonValue {
                const int idx = rb->schema()->GetFieldIndex(nm);
                if (idx < 0) {
                    return clink::config::JsonValue{nullptr};
                }
                return sql::row_columnar_detail::read_cell(
                    rb->schema()->field(idx)->type(), *rb->column(idx), i);
            };
            const bool keep = clink::operators::evaluate_json_predicate(*predicate_, resolve);
            mask_b.UnsafeAppend(keep);
            kept += keep ? 1 : 0;
        }
        std::shared_ptr<arrow::Array> mask;
        if (!mask_b.Finish(&mask).ok()) {
            return false;
        }

        const std::int64_t dropped = n - kept;
        auto count_dropped = [&] {
            if (dropped > 0) {
                clink::metrics::op::records_dropped_inc(
                    this->runtime() ? this->runtime()->metrics() : nullptr,
                    this->id().value(),
                    static_cast<std::size_t>(dropped));
            }
        };
        if (kept == 0) {
            count_dropped();  // committed to the columnar path: count once here
            return true;      // nothing passes; emit nothing (matches the row filter)
        }
        auto filtered = arrow::compute::Filter(arrow::Datum(rb), arrow::Datum(mask));
        if (!filtered.ok() || filtered->kind() != arrow::Datum::RECORD_BATCH) {
            // Not emitted yet: fall back to the row path, which is the SINGLE
            // accounting point for this batch (so dropped is not double-counted).
            return false;
        }
        count_dropped();
        auto out_rb = filtered->record_batch();
        out.emit_data(Batch<sql::Row>{
            out_rb, static_cast<std::size_t>(out_rb->num_rows()), sql::row_materialize_fn()});
        return true;
    }

    // Row path: the fallback (row-only upstream) and the watermark/barrier
    // forwarder. Identical predicate to the columnar path.
    void process(const StreamElement<sql::Row>& element, Emitter<sql::Row>& out) override {
        if (element.is_data()) {
            const Batch<sql::Row>& in_batch = element.as_data();
            Batch<sql::Row> out_batch;
            for (const auto& record : in_batch) {
                auto resolve = [&](const std::string& nm) -> clink::config::JsonValue {
                    auto it = record.value().values.find(nm);
                    if (it == record.value().values.end()) {
                        return clink::config::JsonValue{nullptr};
                    }
                    return it->second;
                };
                if (clink::operators::evaluate_json_predicate(*predicate_, resolve)) {
                    out_batch.push(record);
                }
            }
            if (const auto dropped = in_batch.size() - out_batch.size(); dropped > 0) {
                clink::metrics::op::records_dropped_inc(
                    this->runtime() ? this->runtime()->metrics() : nullptr,
                    this->id().value(),
                    dropped);
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
    std::shared_ptr<clink::config::JsonValue> predicate_;
    std::string name_;
};

}  // namespace clink

#endif  // CLINK_HAS_ARROW
