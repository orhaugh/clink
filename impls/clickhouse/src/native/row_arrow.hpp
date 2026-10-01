#pragma once

#include <memory>
#include <vector>

#include <arrow/api.h>

#include "clink/core/record.hpp"
#include "clink/sql/row.hpp"

#include "native/types.hpp"

namespace clink::clickhouse::native {

// One typed Arrow column per declared input column, in declared order. The
// eight types the shared batcher supports go through
// clink::sql::row_columnar_detail::append_json_cell, so the Row form and a
// later columnar form agree by construction; the rest are built here. Never
// throws for a value: a cell of the wrong JSON kind, or outside its declared
// type's range, becomes NULL, which is the shared rule.
class RowArrowBuilder {
public:
    explicit RowArrowBuilder(std::vector<SqlColumn> columns);
    ~RowArrowBuilder();
    RowArrowBuilder(RowArrowBuilder&&) noexcept;
    RowArrowBuilder& operator=(RowArrowBuilder&&) noexcept;

    [[nodiscard]] std::shared_ptr<arrow::RecordBatch> build(const Batch<sql::Row>& batch) const;
    [[nodiscard]] const std::shared_ptr<arrow::Schema>& schema() const noexcept;

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::clickhouse::native
