// Round-trip + contract tests for ArrowBatcher<T>.
//
// The implicit contract for any ArrowBatcher<T>:
//   1.  build(batch).schema() == schema()
//   2.  parse(build(batch)) yields a Batch<T> whose records compare
//       equal to the input batch (values + event_times).
//   3.  schema() returns the same Schema object on every call (a
//       reasonable user expectation; not strictly required but locks
//       in the convention).
//
// This file runs the contract across:
//   * int64 columnar batcher
//   * string columnar batcher
//   * int32 / uint32 / uint64 columnar batchers (the generic
//     primitive_arrow_batcher path)
//   * binary-fallback batcher with Codec<int64> (smoke-test that
//     the value_bytes:binary wrapping round-trips correctly)
//
// A future user-supplied batcher that violates the contract would
// also fail this test if added here.

#ifndef CLINK_HAS_ARROW
#error "test_arrow_batcher requires Arrow"
#endif

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/record.hpp"

using namespace clink;

namespace {

template <typename T>
bool batch_equal(const Batch<T>& a, const Batch<T>& b) {
    if (a.size() != b.size()) {
        return false;
    }
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (a[i].value() != b[i].value())
            return false;
        const auto at = a[i].event_time();
        const auto bt = b[i].event_time();
        if (at.has_value() != bt.has_value())
            return false;
        if (at.has_value() && at->millis() != bt->millis())
            return false;
    }
    return true;
}

// Generic round-trip + schema-stability harness. The caller supplies
// the batcher under test and a non-empty input batch.
template <typename T>
void assert_contract(const ArrowBatcher<T>& batcher, Batch<T> input) {
    ASSERT_TRUE(static_cast<bool>(batcher.schema));
    ASSERT_TRUE(static_cast<bool>(batcher.build));
    ASSERT_TRUE(static_cast<bool>(batcher.parse));

    auto s1 = batcher.schema();
    auto s2 = batcher.schema();
    ASSERT_NE(s1, nullptr);
    EXPECT_TRUE(s1->Equals(*s2, /*check_metadata=*/false))
        << "schema() must be stable across calls";

    auto record_batch = batcher.build(input);
    ASSERT_NE(record_batch, nullptr) << "build() returned null";
    EXPECT_TRUE(record_batch->schema()->Equals(*s1, /*check_metadata=*/false))
        << "build()'s output schema must equal schema()";

    auto parsed = batcher.parse(*record_batch);
    ASSERT_TRUE(parsed.has_value()) << "parse() rejected its own batcher's output";
    EXPECT_TRUE(batch_equal(*parsed, input))
        << "parse(build(batch)) must equal batch (record-by-record)";
}

}  // namespace

TEST(ArrowBatcherContract, Int64Columnar) {
    Batch<std::int64_t> b;
    b.emplace(10, EventTime{100});
    b.emplace(-7, EventTime{200});
    b.emplace(0);  // no event-time
    b.emplace(std::numeric_limits<std::int64_t>::max(), EventTime{1});
    b.emplace(std::numeric_limits<std::int64_t>::min());
    assert_contract<std::int64_t>(int64_arrow_batcher(), std::move(b));
}

TEST(ArrowBatcherContract, StringColumnar) {
    Batch<std::string> b;
    b.emplace(std::string{"alpha"}, EventTime{1});
    b.emplace(std::string{""}, EventTime{2});  // empty string
    b.emplace(std::string{"with\nnewline\tand\0null", 21});
    b.emplace(std::string{"no-ts"});
    assert_contract<std::string>(string_arrow_batcher(), std::move(b));
}

TEST(ArrowBatcherContract, Int32Columnar) {
    Batch<std::int32_t> b;
    b.emplace(42, EventTime{100});
    b.emplace(-1);
    b.emplace(std::numeric_limits<std::int32_t>::max(), EventTime{200});
    assert_contract<std::int32_t>(int32_arrow_batcher(), std::move(b));
}

TEST(ArrowBatcherContract, UInt32Columnar) {
    Batch<std::uint32_t> b;
    b.emplace(0u, EventTime{1});
    b.emplace(123u);
    b.emplace(std::numeric_limits<std::uint32_t>::max(), EventTime{2});
    assert_contract<std::uint32_t>(uint32_arrow_batcher(), std::move(b));
}

TEST(ArrowBatcherContract, UInt64Columnar) {
    Batch<std::uint64_t> b;
    b.emplace(0u, EventTime{1});
    b.emplace(123u);
    b.emplace(std::numeric_limits<std::uint64_t>::max(), EventTime{2});
    assert_contract<std::uint64_t>(uint64_arrow_batcher(), std::move(b));
}

TEST(ArrowBatcherContract, BinaryFallbackOverInt64Codec) {
    // The fallback wraps any Codec<T> in a value_bytes:binary column.
    // Verifies that the per-record codec encode/decode round-trips
    // correctly when channelled through the Arrow IPC layer.
    Batch<std::int64_t> b;
    b.emplace(10, EventTime{100});
    b.emplace(-7);
    b.emplace(std::numeric_limits<std::int64_t>::max(), EventTime{1});
    assert_contract<std::int64_t>(make_default_arrow_batcher<std::int64_t>(int64_codec()),
                                  std::move(b));
}

TEST(ArrowBatcherContract, BinaryFallbackOverStringCodec) {
    Batch<std::string> b;
    b.emplace(std::string{"hello"}, EventTime{1});
    b.emplace(std::string{""});
    b.emplace(std::string{"world"}, EventTime{2});
    assert_contract<std::string>(make_default_arrow_batcher<std::string>(string_codec()),
                                 std::move(b));
}

TEST(ArrowBatcherContract, EmptyBatchRoundTripsToEmpty) {
    // Edge case: empty batch must round-trip cleanly. Some operator
    // boundaries can produce empty batches; the wire must handle them.
    Batch<std::int64_t> empty;
    auto batcher = int64_arrow_batcher();
    auto rb = batcher.build(empty);
    ASSERT_NE(rb, nullptr);
    EXPECT_EQ(rb->num_rows(), 0);
    auto parsed = batcher.parse(*rb);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_TRUE(parsed->empty());
}
