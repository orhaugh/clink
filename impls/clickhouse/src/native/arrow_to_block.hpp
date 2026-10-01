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
// writer; not thread-safe. The builder keeps its own copy of the plan.
class BlockBuilder {
public:
    explicit BlockBuilder(const ColumnPlan& plan);
    ~BlockBuilder();
    BlockBuilder(BlockBuilder&&) noexcept;
    BlockBuilder& operator=(BlockBuilder&&) noexcept;

    // Appends rows [offset, offset + length) of `chunk`, honouring every
    // array's own offset(). Throws ConversionError naming the column and the
    // chunk row; the builder is then unusable until reset(). A String column
    // keeps views into the chunk's buffers, so the chunk must outlive the
    // block (ColumnPlan::retains_chunks).
    void append(const arrow::RecordBatch& chunk, std::int64_t offset, std::int64_t length);
    [[nodiscard]] std::size_t rows() const noexcept;
    // The batch_bytes and kMaxBlockBytes measure: fixed-width bytes plus
    // string bytes plus one byte per string, recursively.
    [[nodiscard]] std::size_t payload_bytes() const noexcept;
    // Bytes the block owns, for the memory charge: fixed-width copies, plus
    // sizeof(std::string_view) per reserved value of every zero-copy String
    // column at any depth, plus the storage-block table the client reserves
    // beside those views. The client's vectors are private, so the builder
    // reserves every column itself, growing by half again, and counts the
    // capacity it asked for: growth slack is never uncounted.
    [[nodiscard]] std::size_t owned_bytes() const noexcept;
    // Moves the columns out as a Block with RefreshRowCount() applied and
    // resets. Read rows(), payload_bytes() and owned_bytes() first: they
    // describe the block only until take() returns. Throws std::logic_error
    // when rows() is 0, because a 0-row block is never sent.
    [[nodiscard]] ::clickhouse::Block take();
    void reset();
    // Estimated payload bytes per row of `chunk` under this plan, so the
    // writer can slice a chunk to fit the rest of a block.
    [[nodiscard]] double bytes_per_row(const arrow::RecordBatch& chunk) const;

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

// One part of a held block divided for a resource split. Column::Slice copies
// every String value into storage the part owns, so the part no longer needs
// the chunk, and owned_bytes counts those copies for the memory charge.
struct BlockSlice {
    ::clickhouse::Block block;
    std::size_t rows{0};
    std::size_t payload_bytes{0};  // the BlockBuilder::payload_bytes measure
    std::size_t owned_bytes{0};
};

// Rows [begin, begin + length) of a block that BlockBuilder::take() made, by
// Column::Slice, with RefreshRowCount() applied. Throws std::out_of_range
// for an empty or out-of-bounds range, since a 0-row block is never sent.
[[nodiscard]] BlockSlice slice_block(const ::clickhouse::Block& block,
                                     std::size_t begin,
                                     std::size_t length);

// The sum of the Arrow buffer sizes of `chunk`, each shared buffer once: what
// the queue and the memory charge count.
[[nodiscard]] std::size_t chunk_bytes(const arrow::RecordBatch& chunk);

// The offending row for a permanent-failure log. Every column shows its target
// type; only the offending column's value is shown, and only when it is a
// number, a Decimal, a Date or a DateTime. String, FixedString and composite
// values show their length alone ("email=String(len 23)"): row data may be
// confidential, and the type and length are enough to find the defect.
// `offending_column` may name an element (ConversionError names
// "tags.element"), which points at its top-level column.
[[nodiscard]] std::string redacted_row(const ColumnPlan& plan,
                                       const arrow::RecordBatch& chunk,
                                       std::int64_t row,
                                       const std::string& offending_column);

}  // namespace clink::clickhouse::native
