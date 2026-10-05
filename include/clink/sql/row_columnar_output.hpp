#pragma once

// RowColumnarOutput - born-columnar operator OUTPUT for a declared schema.
//
// The columnar path so far has been about ingest: a source or bridge attaches an
// Arrow sidecar, and operators that understand it read columns instead of rows
// (see docs/internals/columnar-execution.md). Their OUTPUT was still row form,
// so an operator that produced rows paid for a name-keyed
// FlatMap<string, JsonValue> per emitted row: one string key copy per column per
// row, a pair-vector allocation per row, and the malloc churn both imply.
//
// This builder is the other half. An operator appends its output values straight
// into one typed Arrow builder per declared output column and emits the finished
// RecordBatch as a columnar Batch<Row>. The column names live in the schema,
// once, instead of in every row. No Row is constructed at all.
//
// Two things make it safe to use:
//
//   * Exact or bail. The sidecar layout (effective_type) keeps a handful of
//     types as typed columns and stores the rest as text, so not every value
//     comes back out of it as it went in: a SMALLINT or DATE number, a
//     TIMESTAMP number where the layout carries TIMESTAMP as text, or an ARRAY,
//     MAP or ROW value, would come back as a string, and where it carries
//     TIMESTAMP as epoch milliseconds a TIMESTAMP that is not an integer would
//     not come back at all. An operator therefore appends a row through
//     try_append_row, which checks every cell with
//     row_columnar_detail::cell_is_exact before appending any, and on false
//     takes the row path for that emission. What does go in is converted by
//     append_json_cell, the function the row-batch converter (build_column)
//     uses, and read back by read_cell, so a row consumer sees the row the
//     operator would have built.
//   * The emitted batch carries row_materialize_fn(), so a row consumer
//     downstream decodes it lazily exactly once and sees the rows it would have
//     received anyway. Correctness never depends on the consumer being columnar.
//
// Whether emitting columnar is FASTER, however, does depend on the consumer: a
// row consumer pays the Arrow build and then the materialise. So the decision to
// use this builder belongs to the planner, which knows the chain, and not to the
// operator, which does not. See the columnar_output param in physical_plan.cpp.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#ifdef CLINK_HAS_ARROW

#include <arrow/api.h>

#include "clink/config/json.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace clink::detail {
// Process-wide count of born-columnar bails: each increment is one emission an
// operator began on the columnar carrier and finished in row form, because a
// row could not ride it (a changelog marker, a cell try_append_row refuses, an
// absent group value). The materialisation counter cannot tell this apart from
// a consumer decoding a batch that stayed columnar, since a bail decodes what
// it had accumulated too. Tests read it to prove an emission never bailed.
// Incremented only on a bail, so it is not a hot-path cost.
inline std::atomic<std::uint64_t>& columnar_output_bail_counter() noexcept {
    static std::atomic<std::uint64_t> counter{0};
    return counter;
}
}  // namespace clink::detail

namespace clink::sql {

// Minimum rows in one emission for the columnar carrier to be worth building.
//
// Building a RecordBatch has a fixed cost per BATCH (a builder and an array per
// column, the schema, the RecordBatch itself) that row form does not pay, against
// a saving per ROW (no name-keyed FlatMap, no per-column key copy). Below the
// crossover the fixed cost wins and emitting columnar is slower.
//
// Measured on the 4M-row embedded shapes (docs/d4-typed-row-plan.md): an equi-join
// emits thousands of rows per input batch and gains ~13% CPU; a windowed fire
// emits a handful of panes per watermark and LOST ~12% when it built a batch for
// them. 64 sits between the two, and the same rule then covers any future emission
// site without another measurement round. Deliberately not a knob: a threshold
// nobody tunes is one fewer configuration to get wrong.
inline constexpr std::size_t kColumnarOutputMinRows = 64;

// Learns, per operator, whether its emissions are big enough for the columnar
// carrier to pay - because the row count is only known once a batch has been
// built, so the choice for THIS batch has to be made from the last ones.
//
// Same shape as the source-side decode damper (json_string_to_row_columnar): back
// off after a short run of unprofitable batches, then re-probe periodically so an
// operator whose batches grow later is not stuck on the slow path forever. Wrong
// guesses cost a little speed and nothing else, since both carriers emit the same
// rows.
class ColumnarOutputDamper {
public:
    [[nodiscard]] bool active() const noexcept { return active_; }

    // Call once per emission with the number of rows emitted.
    void observed(std::size_t rows) noexcept {
        if (active_) {
            small_run_ = rows < kColumnarOutputMinRows ? small_run_ + 1 : 0;
            if (small_run_ >= kSmallRunBeforeBackoff) {
                active_ = false;
                since_probe_ = 0;
            }
            return;
        }
        if (++since_probe_ >= kBatchesBetweenProbes) {
            active_ = true;
            small_run_ = 0;
        }
    }

private:
    static constexpr int kSmallRunBeforeBackoff = 4;
    static constexpr int kBatchesBetweenProbes = 64;

    bool active_ = true;
    int small_run_ = 0;
    int since_probe_ = 0;
};

class RowColumnarOutput {
public:
    explicit RowColumnarOutput(std::vector<RowColumn> columns) : columns_(std::move(columns)) {
        eff_.reserve(columns_.size());
        builders_.reserve(columns_.size());
        // Column 0 is the event time, nullable, exactly as every other columnar
        // Row producer emits it. This is not decoration: rows_from_record_batch
        // reads column 0 as the event time and takes value columns from index 1,
        // so a sidecar without it would have its first value column silently
        // consumed as a timestamp and dropped from the materialised rows.
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(columns_.size() + 1);
        fields.push_back(arrow_event_time_field());
        for (const auto& c : columns_) {
            auto eff = row_columnar_detail::effective_type(c.type);
            fields.push_back(arrow::field(c.name, eff, /*nullable=*/true));
            builders_.push_back(row_columnar_detail::make_cell_builder(eff));
            eff_.push_back(std::move(eff));
        }
        schema_ = arrow::schema(std::move(fields));
    }

    [[nodiscard]] std::size_t width() const noexcept { return columns_.size(); }
    [[nodiscard]] const std::vector<RowColumn>& columns() const noexcept { return columns_; }
    [[nodiscard]] std::size_t rows() const noexcept { return rows_; }
    [[nodiscard]] bool empty() const noexcept { return rows_ == 0; }

    // Append one cell of the row under construction. `v == nullptr` appends a
    // null, which is how an absent column and a JSON-null column both arrive -
    // matching the row path exactly. No exactness check: an operator emitting
    // its output appends through try_append_row instead.
    void append(std::size_t col, const clink::config::JsonValue* v) {
        row_columnar_detail::append_json_cell(*builders_[col], *eff_[col], v);
    }

    // Append one whole output row, `cells[i]` being column i's value (nullptr
    // for a null), if every cell comes back out of its column unchanged
    // (row_columnar_detail::cell_is_exact). Otherwise appends nothing and returns
    // false, and the caller emits this row, and the rest of its emission, in row
    // form. Checked before anything is appended because an Arrow builder cannot
    // take back a partial row.
    [[nodiscard]] bool try_append_row(const std::vector<const clink::config::JsonValue*>& cells,
                                      const std::optional<EventTime>& t = std::nullopt) {
        if (cells.size() != builders_.size()) {
            return false;
        }
        for (std::size_t i = 0; i < cells.size(); ++i) {
            if (!row_columnar_detail::cell_is_exact(*eff_[i], cells[i])) {
                return false;
            }
        }
        for (std::size_t i = 0; i < cells.size(); ++i) {
            append(i, cells[i]);
        }
        end_row(t);
        return true;
    }

    // Append every cell of one output row from a source Row, reading each
    // declared column by name. Unchecked, like append(): it gives exactly what
    // build_column gives for the same row, which is what the converter parity
    // tests compare.
    void append_row_projection(const Row& src, const std::optional<EventTime>& t = std::nullopt) {
        for (std::size_t i = 0; i < columns_.size(); ++i) {
            append(i, row_columnar_detail::field(src, columns_[i].name));
        }
        end_row(t);
    }

    // Close the current row, with its event time (none by default, which is what
    // a join's pair carries; a window pane carries window_end - 1). Every value column must have
    // had exactly one append since the previous end_row(): a caller that skipped one shears the
    // columns apart, which finish() detects by comparing lengths and reports by returning nullptr
    // rather than emitting a malformed batch.
    void end_row(const std::optional<EventTime>& t = std::nullopt) {
        (void)clink::detail::append_event_time(time_builder_, t);
        ++rows_;
    }

    // Hint the expected row count so each column allocates once.
    void reserve(std::int64_t n) {
        (void)time_builder_.Reserve(n);
        for (std::size_t i = 0; i < builders_.size(); ++i) {
            // A list builder's capacity belongs to its child, so reserving the
            // parent's offsets is not the useful axis; skip it (build_column
            // makes the same call and the same exception).
            if (eff_[i]->id() != arrow::Type::LIST) {
                (void)builders_[i]->Reserve(n);
            }
        }
    }

    // Finish the accumulated rows into a RecordBatch and reset for reuse.
    // Returns nullptr when nothing was appended, or when any column came out at
    // a different length from the others (which would mean a caller bug; better
    // to fall back to the row path than to emit a sheared batch).
    std::shared_ptr<arrow::RecordBatch> finish() {
        if (rows_ == 0) {
            return nullptr;
        }
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        arrays.reserve(builders_.size() + 1);
        std::shared_ptr<arrow::Array> t_arr;
        if (!time_builder_.Finish(&t_arr).ok() || t_arr == nullptr ||
            t_arr->length() != static_cast<std::int64_t>(rows_)) {
            reset_builders_();
            return nullptr;
        }
        arrays.push_back(std::move(t_arr));
        for (auto& b : builders_) {
            std::shared_ptr<arrow::Array> arr;
            if (!b->Finish(&arr).ok() || arr == nullptr ||
                arr->length() != static_cast<std::int64_t>(rows_)) {
                reset_builders_();
                return nullptr;
            }
            arrays.push_back(std::move(arr));
        }
        auto out =
            arrow::RecordBatch::Make(schema_, static_cast<std::int64_t>(rows_), std::move(arrays));
        rows_ = 0;
        return out;
    }

private:
    // Finish() drains a builder even when it fails, so on the failure path
    // rebuild them rather than leaving half-drained state behind.
    void reset_builders_() {
        time_builder_.Reset();
        for (std::size_t i = 0; i < builders_.size(); ++i) {
            builders_[i] = row_columnar_detail::make_cell_builder(eff_[i]);
        }
        rows_ = 0;
    }

    std::vector<RowColumn> columns_;
    std::vector<std::shared_ptr<arrow::DataType>> eff_;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders_;
    arrow::Int64Builder time_builder_;
    std::shared_ptr<arrow::Schema> schema_;
    std::size_t rows_ = 0;
};

// Wrap a finished RecordBatch as a columnar Batch<Row>: sidecar set, rows
// unmaterialised, decoded lazily by the self-describing reader if a row consumer
// asks for them.
inline Batch<Row> columnar_row_batch(const std::shared_ptr<arrow::RecordBatch>& rb) {
    return Batch<Row>{rb, static_cast<std::size_t>(rb->num_rows()), row_materialize_fn()};
}

}  // namespace clink::sql

#endif  // CLINK_HAS_ARROW
