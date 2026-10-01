#include "native/arrow_to_block.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native {

struct BlockBuilder::Impl {};

BlockBuilder::BlockBuilder(const ColumnPlan& /*plan*/) {
    not_implemented("BlockBuilder");
}

BlockBuilder::~BlockBuilder() = default;
BlockBuilder::BlockBuilder(BlockBuilder&&) noexcept = default;
BlockBuilder& BlockBuilder::operator=(BlockBuilder&&) noexcept = default;

void BlockBuilder::append(const arrow::RecordBatch& /*chunk*/,
                          std::int64_t /*offset*/,
                          std::int64_t /*length*/) {
    not_implemented("BlockBuilder::append");
}

std::size_t BlockBuilder::rows() const noexcept {
    return 0;
}

std::size_t BlockBuilder::payload_bytes() const noexcept {
    return 0;
}

std::size_t BlockBuilder::owned_bytes() const noexcept {
    return 0;
}

::clickhouse::Block BlockBuilder::take() {
    not_implemented("BlockBuilder::take");
}

void BlockBuilder::reset() {
    not_implemented("BlockBuilder::reset");
}

double BlockBuilder::bytes_per_row(const arrow::RecordBatch& /*chunk*/) const {
    not_implemented("BlockBuilder::bytes_per_row");
}

std::size_t chunk_bytes(const arrow::RecordBatch& /*chunk*/) {
    not_implemented("chunk_bytes");
}

std::string redacted_row(const ColumnPlan& /*plan*/,
                         const arrow::RecordBatch& /*chunk*/,
                         std::int64_t /*row*/,
                         const std::string& /*offending_column*/) {
    not_implemented("redacted_row");
}

}  // namespace clink::clickhouse::native
