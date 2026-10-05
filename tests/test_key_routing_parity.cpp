// The row and columnar carriers of a keyed shuffle edge must route a key to the SAME
// subtask.
//
// This is a correctness requirement, not an optimisation, and it was stated only in a
// comment until this test existed. A stream mixes carriers by design: the columnar JSON
// bridge falls back to row form per batch when a record is not faithfully representable,
// and once damped it emits one columnar probe every 64 row batches. So the same key
// routinely arrives on both carriers within one job.
//
// It was broken. The row extractor read __key with `static_cast<int64_t>(as_number())`,
// and as_number() widens int64 to double, discarding everything below the 53-bit mantissa
// - while the columnar extractor read the Int64Array cell exactly. __key holds a full
// 64-bit FNV fold, so measured over 100,000 keys, 99.4% changed value on the round trip
// and 74.5% landed on a different subtask at parallelism 4. A key arriving on both
// carriers split its group state across two subtasks and produced a silently wrong
// aggregate: no crash, no error, just the wrong number.
//
// These tests compare the two extractors directly, which is the only way to catch it - an
// all-row or an all-columnar stream is self-consistent (the rounding is deterministic per
// key), so an end-to-end output test on either carrier alone passes either way.

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace {

using clink::Batch;
using clink::sql::Row;

constexpr const char* kRowKeyField = "__key";

// The subtask a key lands on, by the engine's own routing maths.
std::uint32_t subtask_of(std::int64_t key, std::uint32_t parallelism) {
    const auto bytes =
        std::span<const std::byte>{reinterpret_cast<const std::byte*>(&key), sizeof(key)};
    return clink::subtask_for_key_group(clink::key_group_for_key(bytes), parallelism);
}

// Keys that exercise the full 64-bit range, which is what a FNV fold produces. Small
// values below 2^53 survive a double round trip, so a test using only those would pass
// against the bug.
std::vector<std::int64_t> wide_keys() {
    std::vector<std::int64_t> out;
    std::uint64_t h = 1469598103934665603ULL;
    for (int i = 0; i < 500; ++i) {
        // The same FNV-1a fold the engine uses to build __key from a key column.
        const auto* b = reinterpret_cast<const unsigned char*>(&i);
        for (std::size_t k = 0; k < sizeof(i); ++k) {
            h ^= b[k];
            h *= 1099511628211ULL;
        }
        out.push_back(static_cast<std::int64_t>(h & 0x7fffffffffffffffLL));
    }
    return out;
}

}  // namespace

// The core invariant: both extractors must agree on every key.
TEST(KeyRoutingParity, RowAndColumnarExtractorsAgreeOnEveryKey) {
    clink::cluster::ensure_built_ins_registered();
    clink::plugin::PluginRegistry reg;
    clink::sql::install(reg);

    auto row_fn =
        clink::cluster::KeyExtractorRegistry::default_instance().find<Row>("row", "row_key");
    ASSERT_TRUE(row_fn) << "row_key extractor not registered";
    auto col_fn = clink::cluster::KeyExtractorRegistry::default_instance().find_columnar<Row>(
        "row", "row_key");
    ASSERT_TRUE(col_fn) << "columnar row_key extractor not registered";

    const auto keys = wide_keys();

    // Columnar side: a batch whose __key column carries the exact int64s.
    arrow::Int64Builder ts_b, key_b;
    for (const auto k : keys) {
        EXPECT_TRUE(ts_b.Append(1'700'000'000'000).ok());
        EXPECT_TRUE(key_b.Append(k).ok());
    }
    std::shared_ptr<arrow::Array> ts_a, key_a;
    ASSERT_TRUE(ts_b.Finish(&ts_a).ok());
    ASSERT_TRUE(key_b.Finish(&key_a).ok());
    auto rb =
        arrow::RecordBatch::Make(arrow::schema({arrow::field("event_time", arrow::int64(), true),
                                                arrow::field(kRowKeyField, arrow::int64(), true)}),
                                 static_cast<std::int64_t>(keys.size()),
                                 {ts_a, key_a});
    Batch<Row> columnar{rb, keys.size(), clink::sql::row_materialize_fn()};

    auto col_keys = col_fn(columnar);
    ASSERT_TRUE(col_keys.has_value()) << "the columnar extractor declined a __key batch";
    ASSERT_EQ(col_keys->size(), keys.size());

    for (std::uint32_t par : {2U, 4U, 8U}) {
        std::size_t value_mismatch = 0;
        std::size_t route_mismatch = 0;
        for (std::size_t i = 0; i < keys.size(); ++i) {
            // Row side: the same key as the row carrier presents it.
            Row r;
            r.values.emplace(kRowKeyField, clink::config::JsonValue{keys[i]});
            const std::int64_t row_key = row_fn(r);
            const std::int64_t col_key = (*col_keys)[i];
            if (row_key != col_key) {
                ++value_mismatch;
            }
            if (subtask_of(row_key, par) != subtask_of(col_key, par)) {
                ++route_mismatch;
            }
        }
        EXPECT_EQ(value_mismatch, 0u)
            << "at parallelism " << par << ", " << value_mismatch << " of " << keys.size()
            << " keys read differently by the two carriers (as_number() widens int64 to "
               "double; use as_int())";
        EXPECT_EQ(route_mismatch, 0u)
            << "at parallelism " << par << ", " << route_mismatch << " of " << keys.size()
            << " keys route to DIFFERENT subtasks depending on the carrier, so their group "
               "state splits and the aggregate is silently wrong";
    }
}

// A key above 2^53 is where the bug lives; a key below it round-trips through a double
// unharmed. Pin the boundary explicitly so a future "optimisation" back to as_number()
// cannot pass by testing only small keys.
TEST(KeyRoutingParity, KeysAboveTheDoubleMantissaAreReadExactly) {
    clink::cluster::ensure_built_ins_registered();
    clink::plugin::PluginRegistry reg;
    clink::sql::install(reg);
    auto row_fn =
        clink::cluster::KeyExtractorRegistry::default_instance().find<Row>("row", "row_key");
    ASSERT_TRUE(row_fn);

    // 2^53 + 1 is the smallest positive integer a double cannot represent.
    for (const std::int64_t k : {(std::int64_t{1} << 53) + 1,
                                 (std::int64_t{1} << 62) + 12345,
                                 std::int64_t{9007199254740993LL},
                                 std::numeric_limits<std::int64_t>::max() - 1}) {
        Row r;
        r.values.emplace(kRowKeyField, clink::config::JsonValue{k});
        EXPECT_EQ(row_fn(r), k) << "key " << k << " was not read exactly";
    }
}

// A missing __key must still be the well-defined 0, on both carriers.
TEST(KeyRoutingParity, MissingKeyFieldIsZeroOnBothCarriers) {
    clink::cluster::ensure_built_ins_registered();
    clink::plugin::PluginRegistry reg;
    clink::sql::install(reg);
    auto row_fn =
        clink::cluster::KeyExtractorRegistry::default_instance().find<Row>("row", "row_key");
    ASSERT_TRUE(row_fn);
    Row r;
    r.values.emplace("something_else", clink::config::JsonValue{std::int64_t{7}});
    EXPECT_EQ(row_fn(r), 0);
}

// A TIMESTAMP grouping key. Under the second Row layout the columnar carriers hold
// TIMESTAMP and TIMESTAMPTZ as timestamp(ms[, "UTC"]) columns, where the row path
// holds the same epoch milliseconds as a JSON integer. row_compute_key folds the
// key columns into __key on both carriers, and both must give every row the same
// __key, or a key whose batches arrive on both carriers splits its group state
// across subtasks, and keys move between subtasks across a restore.
TEST(KeyRoutingParity, ATimestampKeyFoldsToTheSameKeyOnBothCarriers) {
    clink::cluster::ensure_built_ins_registered();
    clink::plugin::PluginRegistry reg;
    clink::sql::install(reg);
    const auto* factory = clink::cluster::OperatorRegistry::default_instance().find_operator(
        "row_compute_key", "row", "row");
    ASSERT_NE(factory, nullptr) << "row_compute_key is not registered";

    // Epoch milliseconds over a wide range: negative epochs, values past 2^53,
    // and a null. (hash_json_value reads a number through a double, so a value at
    // the very ends of int64 would cast out of range on both carriers alike.)
    std::vector<std::optional<std::int64_t>> stamps = {std::int64_t{1'700'000'000'000},
                                                       std::int64_t{-1},
                                                       std::int64_t{0},
                                                       std::int64_t{-2'208'988'800'000},
                                                       (std::int64_t{1} << 53) + 1,
                                                       (std::int64_t{1} << 62) + 12345,
                                                       -(std::int64_t{1} << 62),
                                                       std::nullopt};
    for (const auto k : wide_keys()) {
        stamps.emplace_back(k % 20'000'000'000'000);
    }
    const auto n = static_cast<std::int64_t>(stamps.size());

    for (const bool zoned : {false, true}) {
        SCOPED_TRACE(zoned ? "TIMESTAMPTZ" : "TIMESTAMP");
        const auto ts_type = zoned ? arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")
                                   : arrow::timestamp(arrow::TimeUnit::MILLI);
        // On its own and beside a BIGINT key, since the fold runs over every
        // key column in order.
        for (const std::string columns : {"ts", "id,ts"}) {
            SCOPED_TRACE(columns);
            clink::cluster::OperatorBuildContext ctx;
            ctx.params["columns"] = columns;
            auto op = std::static_pointer_cast<clink::Operator<Row, Row>>(factory->build(ctx));
            ASSERT_TRUE(op->supports_columnar());

            arrow::Int64Builder et_b;
            arrow::Int64Builder id_b;
            arrow::TimestampBuilder ts_b(ts_type, arrow::default_memory_pool());
            Batch<Row> rows;
            for (std::int64_t i = 0; i < n; ++i) {
                ASSERT_TRUE(et_b.AppendNull().ok());
                ASSERT_TRUE(id_b.Append(i * 7).ok());
                Row r;
                r.values.emplace("id", clink::config::JsonValue{i * 7});
                const auto& s = stamps[static_cast<std::size_t>(i)];
                if (s.has_value()) {
                    ASSERT_TRUE(ts_b.Append(*s).ok());
                    r.values.emplace("ts", clink::config::JsonValue{*s});
                } else {
                    ASSERT_TRUE(ts_b.AppendNull().ok());
                    r.values.emplace("ts", clink::config::JsonValue{});
                }
                rows.emplace(std::move(r));
            }
            auto rb = arrow::RecordBatch::Make(
                arrow::schema({arrow::field("event_time", arrow::int64(), true),
                               arrow::field("id", arrow::int64(), true),
                               arrow::field("ts", ts_type, true)}),
                n,
                {et_b.Finish().ValueOrDie(),
                 id_b.Finish().ValueOrDie(),
                 ts_b.Finish().ValueOrDie()});
            Batch<Row> columnar{rb, static_cast<std::size_t>(n), clink::sql::row_materialize_fn()};

            clink::BoundedChannel<clink::StreamElement<Row>> ch(8);
            clink::Emitter<Row> out(&ch);
            ASSERT_TRUE(
                op->process_columnar(clink::StreamElement<Row>::data(std::move(columnar)), out))
                << "row_compute_key declined a batch with a timestamp key column";
            op->process(clink::StreamElement<Row>::data(std::move(rows)), out);
            auto col_el = ch.try_pop();
            auto row_el = ch.try_pop();
            ASSERT_TRUE(col_el.has_value() && row_el.has_value());
            ASSERT_TRUE(col_el->as_data().is_columnar());
            const auto& sidecar = *col_el->as_data().arrow();
            const auto key_col = sidecar.GetColumnByName(kRowKeyField);
            ASSERT_NE(key_col, nullptr);
            const auto& col_keys = static_cast<const arrow::Int64Array&>(*key_col);
            ASSERT_EQ(row_el->as_data().size(), static_cast<std::size_t>(n));

            std::size_t value_mismatch = 0;
            std::size_t route_mismatch = 0;
            std::size_t i = 0;
            for (const auto& rec : row_el->as_data()) {
                const std::int64_t row_key = rec.value().values.at(kRowKeyField).as_int();
                const std::int64_t col_key = col_keys.Value(static_cast<std::int64_t>(i));
                value_mismatch += row_key != col_key ? 1 : 0;
                for (std::uint32_t par : {2U, 4U, 8U}) {
                    route_mismatch += subtask_of(row_key, par) != subtask_of(col_key, par) ? 1 : 0;
                }
                ++i;
            }
            EXPECT_EQ(value_mismatch, 0U)
                << value_mismatch << " of " << n << " rows folded to a different __key";
            EXPECT_EQ(route_mismatch, 0U);
        }
    }
}
