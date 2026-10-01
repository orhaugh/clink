#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include <arrow/api.h>
#include <clickhouse/block.h>

#include "native/column_plan.hpp"

namespace clink::clickhouse::native {

inline constexpr std::size_t kMaxBlockBytes = 16ULL << 20;

// Accumulates converted rows into the next outgoing Native block. One per
// writer; not thread-safe.
class BlockBuilder {
public:
    explicit BlockBuilder(const ColumnPlan& plan);
    ~BlockBuilder();
    BlockBuilder(BlockBuilder&&) noexcept;
    BlockBuilder& operator=(BlockBuilder&&) noexcept;

    // Appends rows [offset, offset + length) of `chunk`, honouring every
    // array's own offset(). Throws ConversionError naming the column and the
    // chunk row; the builder is then unusable until reset().
    void append(const arrow::RecordBatch& chunk, std::int64_t offset, std::int64_t length);
    [[nodiscard]] std::size_t rows() const noexcept;
    // The batch_bytes and kMaxBlockBytes measure: fixed-width bytes plus
    // string bytes plus one byte per string, recursively.
    [[nodiscard]] std::size_t payload_bytes() const noexcept;
    // Bytes the block owns (fixed-width copies), for the memory charge.
    [[nodiscard]] std::size_t owned_bytes() const noexcept;
    // Moves the columns out as a Block with RefreshRowCount() applied and
    // resets. Precondition: rows() > 0 (a 0-row block is never sent).
    [[nodiscard]] ::clickhouse::Block take();
    void reset();
    // Estimated payload bytes per row of `chunk` under this plan, so the
    // writer can slice a chunk to fit the rest of a block.
    [[nodiscard]] double bytes_per_row(const arrow::RecordBatch& chunk) const;

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

// The sum of the Arrow buffer sizes of `chunk`: what the queue and the memory
// charge count.
[[nodiscard]] std::size_t chunk_bytes(const arrow::RecordBatch& chunk);

// The offending row for a permanent-failure log. Every column shows its target
// type; only the offending column's value is shown, and only when it is a
// number, a Decimal, a Date or a DateTime. String, FixedString and composite
// values show their length alone ("email=String(len 23)"): row data may be
// confidential, and the type and length are enough to find the defect.
[[nodiscard]] std::string redacted_row(const ColumnPlan& plan,
                                       const arrow::RecordBatch& chunk,
                                       std::int64_t row,
                                       const std::string& offending_column);

}  // namespace clink::clickhouse::native
