#include "native/row_arrow.hpp"

#include <exception>

#include "native/errors.hpp"

namespace clink::clickhouse::native {

struct RowArrowBuilder::Impl {};

RowArrowBuilder::RowArrowBuilder(std::vector<SqlColumn> /*columns*/) {
    not_implemented("RowArrowBuilder");
}

RowArrowBuilder::~RowArrowBuilder() = default;
RowArrowBuilder::RowArrowBuilder(RowArrowBuilder&&) noexcept = default;
RowArrowBuilder& RowArrowBuilder::operator=(RowArrowBuilder&&) noexcept = default;

std::shared_ptr<arrow::RecordBatch> RowArrowBuilder::build(const Batch<sql::Row>& /*batch*/) const {
    not_implemented("RowArrowBuilder::build");
}

const std::shared_ptr<arrow::Schema>& RowArrowBuilder::schema() const noexcept {
    std::terminate();
}

}  // namespace clink::clickhouse::native
