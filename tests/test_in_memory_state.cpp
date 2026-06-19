#include <cstring>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/state/in_memory_state_backend.hpp"

#ifdef CLINK_HAS_ARROW
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#endif

using namespace clink;

namespace {

std::string_view sv(const std::string& s) {
    return std::string_view{s};
}

std::string to_string(const StateBackend::Value& v) {
    std::string out(v.size(), '\0');
    if (!v.empty()) {
        std::memcpy(out.data(), v.data(), v.size());
    }
    return out;
}

}  // namespace

// Construction-path symmetry: InMemory stays on the synchronous snapshot
// path (no off-thread durable write to hide; a frozen view would still
// need a full copy under the lock). The runner keys off this flag to
// decide whether to spin up a snapshot worker, so a wrong answer here
// would silently route InMemory through the async path.
TEST(InMemoryStateBackend, DoesNotSupportAsyncPersist) {
    InMemoryStateBackend backend;
    EXPECT_FALSE(backend.supports_async_persist());
}

TEST(InMemoryStateBackend, PutGetErase) {
    InMemoryStateBackend backend;
    OperatorId op{42};

    backend.put(op, sv(std::string{"k1"}), sv(std::string{"v1"}));
    backend.put(op, sv(std::string{"k2"}), sv(std::string{"v2"}));

    auto v1 = backend.get(op, sv(std::string{"k1"}));
    ASSERT_TRUE(v1.has_value());
    EXPECT_EQ(to_string(*v1), "v1");

    backend.erase(op, sv(std::string{"k1"}));
    EXPECT_FALSE(backend.get(op, sv(std::string{"k1"})).has_value());
    EXPECT_TRUE(backend.get(op, sv(std::string{"k2"})).has_value());
}

TEST(InMemoryStateBackend, KeysAreScopedPerOperator) {
    InMemoryStateBackend backend;
    OperatorId a{1};
    OperatorId b{2};

    backend.put(a, sv(std::string{"x"}), sv(std::string{"alpha"}));
    backend.put(b, sv(std::string{"x"}), sv(std::string{"beta"}));

    auto va = backend.get(a, sv(std::string{"x"}));
    auto vb = backend.get(b, sv(std::string{"x"}));
    ASSERT_TRUE(va.has_value());
    ASSERT_TRUE(vb.has_value());
    EXPECT_EQ(to_string(*va), "alpha");
    EXPECT_EQ(to_string(*vb), "beta");
}

TEST(InMemoryStateBackend, SnapshotRestoreRoundTrip) {
    InMemoryStateBackend backend;
    OperatorId op{7};
    backend.put(op, sv(std::string{"foo"}), sv(std::string{"bar"}));
    backend.put(op, sv(std::string{"baz"}), sv(std::string{"quux"}));

    Snapshot snap = backend.snapshot(CheckpointId{1});

    InMemoryStateBackend recovered;
    recovered.restore(snap);

    auto v1 = recovered.get(op, sv(std::string{"foo"}));
    auto v2 = recovered.get(op, sv(std::string{"baz"}));
    ASSERT_TRUE(v1.has_value());
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(to_string(*v1), "bar");
    EXPECT_EQ(to_string(*v2), "quux");
}

// Verify the key-group filter: a new subtask post-rescale must only load
// keys whose leading kg-byte falls in its assigned range. Synthesizes a
// snapshot directly (no KeyedState dependency) so the test exercises the
// backend's filter logic in isolation.
TEST(InMemoryStateBackend, RestoreFiltersByKeyGroupRange) {
    InMemoryStateBackend src;
    OperatorId op{1};
    // Hand-crafted keys: first byte is the kg. We pick kg values that
    // span four sub-ranges of [0, 128) so any half-open filter slices
    // them cleanly.
    const auto put_kg = [&](std::uint8_t kg, const std::string& tail, const std::string& v) {
        std::string key;
        key.push_back(static_cast<char>(kg));
        key.append(tail);
        src.put(op, sv(key), sv(v));
    };
    put_kg(5, "alpha", "A");    // kg 5
    put_kg(30, "beta", "B");    // kg 30
    put_kg(80, "gamma", "C");   // kg 80
    put_kg(120, "delta", "D");  // kg 120

    Snapshot snap = src.snapshot(CheckpointId{1});

    // Subtask owning [0, 64): only kg 5 and 30 survive.
    {
        InMemoryStateBackend lower;
        lower.restore(snap, KeyGroupRange{KeyGroup{0}, KeyGroup{64}});
        std::string k5;
        k5.push_back(static_cast<char>(5));
        k5.append("alpha");
        std::string k30;
        k30.push_back(static_cast<char>(30));
        k30.append("beta");
        std::string k80;
        k80.push_back(static_cast<char>(80));
        k80.append("gamma");
        EXPECT_TRUE(lower.get(op, sv(k5)).has_value());
        EXPECT_TRUE(lower.get(op, sv(k30)).has_value());
        EXPECT_FALSE(lower.get(op, sv(k80)).has_value());
    }
    // Subtask owning [64, 128): only kg 80 and 120 survive.
    {
        InMemoryStateBackend upper;
        upper.restore(snap, KeyGroupRange{KeyGroup{64}, KeyGroup{128}});
        std::string k5;
        k5.push_back(static_cast<char>(5));
        k5.append("alpha");
        std::string k80;
        k80.push_back(static_cast<char>(80));
        k80.append("gamma");
        std::string k120;
        k120.push_back(static_cast<char>(120));
        k120.append("delta");
        EXPECT_FALSE(upper.get(op, sv(k5)).has_value());
        EXPECT_TRUE(upper.get(op, sv(k80)).has_value());
        EXPECT_TRUE(upper.get(op, sv(k120)).has_value());
    }
    // Default (covers_all) loads every key - the back-compat path for
    // restore-on-resubmit without rescale.
    {
        InMemoryStateBackend full;
        full.restore(snap);
        std::size_t count = 0;
        full.scan(op, [&](StateBackend::KeyView, StateBackend::ValueView) { ++count; });
        EXPECT_EQ(count, 4u);
    }
}

// Operator-state rows (source offsets, broadcast slots) must survive a
// rescale restore on EVERY subtask - they have no key group, so the
// key-group filter must never narrow them. Before the fix, the offset key
// "__src_offsets__" (first byte '_' = 95, an in-range key group) was
// silently dropped on every subtask whose range excluded 95. This is the
// regression guard for that silent-data-loss bug (#54 Gap A).
TEST(InMemoryStateBackend, OperatorStateSurvivesRescaleOnEverySubtask) {
    InMemoryStateBackend src;
    const OperatorId op{7};
    // A keyed row at key group 95 (so it collides with the offset key's
    // accidental group) plus the operator-state offset row.
    std::string keyed95;
    keyed95.push_back(static_cast<char>(95));
    keyed95.append("keyed");
    src.put(op, sv(keyed95), sv(std::string{"KEYED95"}));
    src.put_operator_state(op, sv(std::string{"__src_offsets__"}), sv(std::string{"OFFSETS"}));

    auto snap = src.snapshot(CheckpointId{1});

    // Two disjoint subtask ranges, NEITHER overlapping the other; the
    // keyed-95 row lands in exactly one, but the operator-state row must
    // appear in BOTH.
    for (auto range :
         {KeyGroupRange{KeyGroup{0}, KeyGroup{64}}, KeyGroupRange{KeyGroup{64}, KeyGroup{128}}}) {
        InMemoryStateBackend sub;
        sub.restore(snap, range);
        auto off = sub.get_operator_state(op, sv(std::string{"__src_offsets__"}));
        ASSERT_TRUE(off.has_value())
            << "operator state dropped for range [" << range.first << "," << range.last << ")";
        EXPECT_EQ(to_string(*off), "OFFSETS");
    }

    // The keyed row is still narrowed normally: present only in the range
    // that owns group 95 ([64,128)), absent in [0,64).
    {
        InMemoryStateBackend lower;
        lower.restore(snap, KeyGroupRange{KeyGroup{0}, KeyGroup{64}});
        EXPECT_FALSE(lower.get(op, sv(keyed95)).has_value());
        InMemoryStateBackend upper;
        upper.restore(snap, KeyGroupRange{KeyGroup{64}, KeyGroup{128}});
        EXPECT_TRUE(upper.get(op, sv(keyed95)).has_value());
    }
}

// A checkpoint taken before operator-state prefixing stored the raw key.
// get_operator_state must fall back to it so pre-existing checkpoints still
// restore (same-parallelism resubmit).
TEST(InMemoryStateBackend, OperatorStateFallsBackToLegacyRawKey) {
    InMemoryStateBackend src;
    const OperatorId op{3};
    // Simulate a legacy snapshot: the raw, unprefixed key.
    src.put(op, sv(std::string{"__src_offsets__"}), sv(std::string{"LEGACY"}));
    auto snap = src.snapshot(CheckpointId{1});

    InMemoryStateBackend full;
    full.restore(snap);  // covers_all: same-parallelism resubmit
    auto off = full.get_operator_state(op, sv(std::string{"__src_offsets__"}));
    ASSERT_TRUE(off.has_value());
    EXPECT_EQ(to_string(*off), "LEGACY");
}

// #54 Gap B: operator state spread across DISTINCT keys (one row per Kafka
// partition) unions across a scale-down merge instead of colliding. Two
// parents each snapshot their own partitions' rows; the merged snapshot
// must contain ALL of them. (A single whole-map key would last-writer-win.)
TEST(InMemoryStateBackend, OperatorStateRowsUnionAcrossMerge) {
    const OperatorId op{9};
    InMemoryStateBackend parent_a;
    parent_a.put_operator_state(op, sv(std::string{"off:0"}), sv(std::string{"A0"}));
    parent_a.put_operator_state(op, sv(std::string{"off:1"}), sv(std::string{"A1"}));
    auto snap_a = parent_a.snapshot(CheckpointId{1});

    InMemoryStateBackend parent_b;
    parent_b.put_operator_state(op, sv(std::string{"off:2"}), sv(std::string{"B2"}));
    auto snap_b = parent_b.snapshot(CheckpointId{1});

    std::vector<std::vector<std::byte>> parts{snap_a.bytes, snap_b.bytes};
    auto merged_bytes = InMemoryStateBackend::merge_snapshot_bytes(parts);

    InMemoryStateBackend merged;
    merged.restore(Snapshot{CheckpointId{1}, merged_bytes});
    EXPECT_EQ(to_string(*merged.get_operator_state(op, sv(std::string{"off:0"}))), "A0");
    EXPECT_EQ(to_string(*merged.get_operator_state(op, sv(std::string{"off:1"}))), "A1");
    EXPECT_EQ(to_string(*merged.get_operator_state(op, sv(std::string{"off:2"}))), "B2");

    // And scan_operator_state sees exactly the three logical keys.
    std::set<std::string> seen;
    merged.scan_operator_state(
        op, [&](StateBackend::KeyView k, StateBackend::ValueView) { seen.emplace(k); });
    EXPECT_EQ(seen, (std::set<std::string>{"off:0", "off:1", "off:2"}));
}

// #54 Gap B D1: when the SAME operator-state key (a partition offset) appears
// in two merged parents with DIFFERENT 8-byte i64-LE values - a partition
// that migrated between subtasks, or a stale row a non-owner re-persisted -
// restore must keep the GREATER (offsets are monotonic) so the union never
// rewinds, INDEPENDENT of merge/iteration order. Last-writer-wins could
// rewind and re-deliver.
TEST(InMemoryStateBackend, OperatorOffsetCollisionKeepsMaxOrderIndependent) {
    const OperatorId op{4};
    auto le8 = [](std::int64_t v) {
        std::string s(8, '\0');
        const auto u = static_cast<std::uint64_t>(v);
        for (int i = 0; i < 8; ++i) {
            s[static_cast<std::size_t>(i)] = static_cast<char>((u >> (i * 8)) & 0xFF);
        }
        return s;
    };
    auto decode = [](const StateBackend::Value& v) {
        std::uint64_t u = 0;
        for (int i = 0; i < 8; ++i) {
            u |= static_cast<std::uint64_t>(
                     std::to_integer<std::uint8_t>(v[static_cast<std::size_t>(i)]))
                 << (i * 8);
        }
        return static_cast<std::int64_t>(u);
    };
    auto snap_with = [&](std::int64_t off) {
        InMemoryStateBackend b;
        b.put_operator_state(op, sv(std::string{"off:0"}), sv(le8(off)));
        return b.snapshot(CheckpointId{1}).bytes;
    };
    auto hi = snap_with(100);
    auto lo = snap_with(50);

    // Either merge order must resolve to the max (100), never the stale 50.
    for (auto parts : {std::vector<std::vector<std::byte>>{hi, lo},
                       std::vector<std::vector<std::byte>>{lo, hi}}) {
        InMemoryStateBackend merged;
        merged.restore(
            Snapshot{CheckpointId{1}, InMemoryStateBackend::merge_snapshot_bytes(parts)});
        auto v = merged.get_operator_state(op, sv(std::string{"off:0"}));
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(decode(*v), 100) << "merge must keep the greater offset (no rewind)";
    }
}

#ifdef CLINK_HAS_ARROW
// Locks in the on-disk contract: snapshot bytes are a valid Arrow IPC
// stream with the documented schema, openable by any standard Arrow
// reader (pyarrow, duckdb, polars, ...). Detects regressions like
// "someone reintroduced custom framing" or "schema changed silently".
TEST(InMemoryStateBackend, SnapshotBytesParseAsArrowIPC) {
    InMemoryStateBackend backend;
    OperatorId op{17};
    backend.put(op, sv(std::string{"alpha"}), sv(std::string{"A"}));
    backend.put(op, sv(std::string{"beta"}), sv(std::string{"BB"}));

    auto snap = backend.snapshot(CheckpointId{42});
    ASSERT_FALSE(snap.bytes.empty());

    auto buffer =
        std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(snap.bytes.data()),
                                        static_cast<int64_t>(snap.bytes.size()));
    auto input = std::make_shared<arrow::io::BufferReader>(buffer);
    auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
    ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();
    auto reader = *reader_result;

    auto schema = reader->schema();
    ASSERT_EQ(schema->num_fields(), 3);
    EXPECT_EQ(schema->field(0)->name(), "op_id");
    EXPECT_EQ(schema->field(0)->type()->id(), arrow::Type::UINT64);
    EXPECT_EQ(schema->field(1)->name(), "key_bytes");
    EXPECT_EQ(schema->field(1)->type()->id(), arrow::Type::BINARY);
    EXPECT_EQ(schema->field(2)->name(), "value_bytes");
    EXPECT_EQ(schema->field(2)->type()->id(), arrow::Type::BINARY);

    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_TRUE(reader->ReadNext(&batch).ok());
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->num_rows(), 2);
}
#endif
