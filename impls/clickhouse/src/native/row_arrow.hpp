#pragma once

#include <memory>
#include <vector>

#include <arrow/api.h>

#include "clink/core/record.hpp"
#include "clink/sql/row.hpp"

#include "native/intake.hpp"
#include "native/types.hpp"

namespace clink::clickhouse::native {

// One typed Arrow column per declared input column, in declared order, typed
// by arrow_type_for. The scalar types the shared batcher supports (BIGINT,
// INTEGER, REAL, DOUBLE, BOOLEAN, VARCHAR and DECIMAL) go through
// clink::sql::row_columnar_detail::append_json_cell, so the Row form and a
// later columnar form agree by construction: at the top level a cell of the
// wrong JSON kind, or outside the type's range, becomes NULL, which is the
// shared rule. The rest (SMALLINT, TINYINT, TIMESTAMP, DATE, ARRAY, MAP, ROW,
// and every element inside a composite, whatever its type) are built here,
// and a cell of the wrong kind or out of range throws
// ConversionError(column, row, reason) instead. Inside a composite a number
// with a fraction is refused for every integer type rather than truncated,
// and a map fails when two of its keys convert to the same value, which
// would otherwise land as one key held twice. A JSON null, an absent column
// and an absent ROW field are NULL for every type.
class RowArrowBuilder {
public:
    // Throws NativeSinkError(column_plan) for a declared type with no Arrow
    // layout (TIME, BYTEA, an unsupported spelling), which the column plan
    // refuses at open before a builder is made.
    explicit RowArrowBuilder(std::vector<SqlColumn> columns);
    ~RowArrowBuilder();
    RowArrowBuilder(RowArrowBuilder&&) noexcept;
    RowArrowBuilder& operator=(RowArrowBuilder&&) noexcept;

    // One RecordBatch with one row per record of `batch`. A columnar batch is
    // read through its row accessors, and so materialised. Throws
    // ConversionError naming the declared column and the row within `batch`;
    // a reason never quotes text, only its length.
    [[nodiscard]] std::shared_ptr<arrow::RecordBatch> build(const Batch<sql::Row>& batch) const;
    // The same chunk from a columnar batch's sidecar, with no Row built: each
    // cell is read as the self-describing reader reads it into a Row value
    // (row_columnar_detail::read_cell) and goes through the per-column loop
    // build() runs. So it equals build() over the rows `batch` materialises
    // to, by sql::row_materialize_fn(), and throws the same ConversionError
    // for the same cell. `plan` is compile_intake's for `batch`'s schema.
    [[nodiscard]] std::shared_ptr<arrow::RecordBatch> build_columnar(
        const arrow::RecordBatch& batch, const IntakePlan& plan) const;
    [[nodiscard]] const std::shared_ptr<arrow::Schema>& schema() const noexcept;

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::clickhouse::native
