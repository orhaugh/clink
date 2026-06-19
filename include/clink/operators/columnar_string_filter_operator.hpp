#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

// Columnar-native string filter: keep rows whose utf8 value starts with a fixed
// prefix. When the input batch carries an Arrow RecordBatch sidecar it scans the
// StringArray with std::string_view (zero std::string construction), builds a
// boolean mask, and runs Arrow's "filter" kernel to gather the passing rows
// (event-time rides along) into a new RecordBatch - ZERO rows materialized. When
// the input is row-only (or the schema is unexpected) it falls back to the
// identical row predicate.
//
// The string win is larger than the int64 filter's: the row path materializes a
// std::string for EVERY record (a heap alloc each), while the columnar path
// touches the Arrow value buffer through string_view and allocates nothing for
// the rows it drops. Same Arrow-kernel posture as ColumnarFilterOperator: the
// hand-rolled prefix scan builds the mask (no comparison kernel registered), the
// registered "filter" selection kernel does the multi-column gather.
class ColumnarStringFilterOperator final : public Operator<std::string, std::string> {
public:
    explicit ColumnarStringFilterOperator(std::string prefix,
                                          std::string name = "columnar_string_filter")
        : prefix_(std::move(prefix)), name_(std::move(name)) {
        auto parse = string_arrow_batcher().parse;
        materialize_ = [parse = std::move(parse)](
                           const arrow::RecordBatch& rb) -> std::vector<Record<std::string>> {
            auto b = parse(rb);
            return b ? b->take_records() : std::vector<Record<std::string>>{};
        };
    }

    [[nodiscard]] bool supports_columnar() const noexcept override { return true; }

    bool process_columnar(const StreamElement<std::string>& element,
                          Emitter<std::string>& out) override {
        if (!element.is_data() || !element.as_data().is_columnar()) {
            return false;
        }
        const auto& rb = element.as_data().arrow();
        if (!rb || rb->num_columns() < 2) {
            return false;
        }
        // Schema {event_time(0), value(1):utf8}.
        const auto* val = dynamic_cast<const arrow::StringArray*>(rb->column(1).get());
        if (val == nullptr) {
            return false;
        }
        const std::int64_t n = rb->num_rows();
        arrow::BooleanBuilder mask_b;
        if (!mask_b.Reserve(n).ok()) {
            return false;
        }
        // Dense scan over the StringArray via string_view - no std::string
        // allocations. starts_with(prefix) is the mask.
        for (std::int64_t i = 0; i < n; ++i) {
            const std::string_view v = val->GetView(i);
            mask_b.UnsafeAppend(starts_with_(v));
        }
        std::shared_ptr<arrow::Array> mask;
        if (!mask_b.Finish(&mask).ok()) {
            return false;
        }
        auto filtered = arrow::compute::Filter(arrow::Datum(rb), arrow::Datum(mask));
        if (!filtered.ok() || filtered->kind() != arrow::Datum::RECORD_BATCH) {
            return false;
        }
        auto out_rb = filtered->record_batch();
        const auto out_n = static_cast<std::size_t>(out_rb->num_rows());
        if (out_n == 0) {
            return true;  // nothing passes; emit nothing (matches the row filter)
        }
        out.emit_data(Batch<std::string>{std::move(out_rb), out_n, materialize_});
        return true;
    }

    void process(const StreamElement<std::string>& element, Emitter<std::string>& out) override {
        if (element.is_data()) {
            const Batch<std::string>& in_batch = element.as_data();
            Batch<std::string> out_batch;
            for (const auto& record : in_batch) {
                if (starts_with_(record.value())) {
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
    [[nodiscard]] bool starts_with_(std::string_view v) const noexcept {
        return v.starts_with(prefix_);
    }

    std::string prefix_;
    std::string name_;
    Batch<std::string>::MaterializeFn materialize_;
};

}  // namespace clink
