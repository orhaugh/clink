// Unit tests for state schema versioning + migration registry.
//
// The registry is the foundation for state schema evolution:
// users declare a SchemaVersionTrait<T> bump on a breaking shape
// change, register a migration function from the old version to the
// new one, and the engine composes single-step migrations into chains
// at restore time.
//
// These tests cover the registry's correctness primitives:
//   - Per-type compile-time version trait + specialisation.
//   - Single-edge migrate round-trip.
//   - Multi-edge chain composition (v1 -> v2 -> v3).
//   - has_path matches migrate's reachability decision.
//   - No-path conditions surface a descriptive error.
//   - Replacing a registered edge replaces (does not duplicate) the fn.
//   - Idempotent / no-op migration when from == to.
//   - Invalid registrations (same version, null fn) reject up front.

#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/schema_version.hpp"
#include "clink/state/state_migration_on_restore.hpp"

namespace {

// Helpers to round-trip a uint32 payload through the migrations. The
// migration fn convention used here: read a uint32, transform it, write
// it back. Real migrations would re-shape Arrow-encoded or codec-encoded
// bytes; the registry doesn't care about the inner structure.
std::vector<std::byte> u32_to_bytes(std::uint32_t v) {
    std::vector<std::byte> out(sizeof(std::uint32_t));
    std::memcpy(out.data(), &v, sizeof(v));
    return out;
}

std::uint32_t bytes_to_u32(std::span<const std::byte> bytes) {
    std::uint32_t v = 0;
    if (bytes.size() < sizeof(v))
        return v;
    std::memcpy(&v, bytes.data(), sizeof(v));
    return v;
}

// Default + specialised type for the trait test.
struct DefaultVersionedType {};

struct UpgradedType {};

}  // namespace

namespace clink {
template <>
struct SchemaVersionTrait<UpgradedType> {
    static constexpr std::uint32_t value = 7;
};
}  // namespace clink

namespace {

TEST(StateSchemaVersion, TraitDefaultsToOne) {
    EXPECT_EQ(clink::schema_version_v<DefaultVersionedType>, 1u);
}

TEST(StateSchemaVersion, TraitSpecialisationWins) {
    EXPECT_EQ(clink::schema_version_v<UpgradedType>, 7u);
}

TEST(StateSchemaVersion, SingleEdgeMigrate) {
    clink::StateMigrationRegistry reg;
    reg.register_migration("Counter", 1, 2, [](std::span<const std::byte> in) {
        return u32_to_bytes(bytes_to_u32(in) + 100);
    });

    auto out = reg.migrate("Counter", 1, 2, u32_to_bytes(42));
    EXPECT_EQ(bytes_to_u32(out), 142u);
    EXPECT_TRUE(reg.has_path("Counter", 1, 2));
}

TEST(StateSchemaVersion, NoOpOnSameVersion) {
    clink::StateMigrationRegistry reg;
    // No registrations needed; same-version migrate is a copy.
    auto input = u32_to_bytes(99);
    auto out = reg.migrate("Counter", 3, 3, input);
    EXPECT_EQ(out, input);
    EXPECT_TRUE(reg.has_path("Counter", 3, 3));
}

TEST(StateSchemaVersion, MultiEdgeChainComposes) {
    clink::StateMigrationRegistry reg;
    // v1 -> v2 adds 10. v2 -> v3 doubles. So v1 -> v3 should compute
    // (x + 10) * 2 on the encoded uint32.
    reg.register_migration("Counter", 1, 2, [](std::span<const std::byte> in) {
        return u32_to_bytes(bytes_to_u32(in) + 10);
    });
    reg.register_migration("Counter", 2, 3, [](std::span<const std::byte> in) {
        return u32_to_bytes(bytes_to_u32(in) * 2);
    });

    auto out = reg.migrate("Counter", 1, 3, u32_to_bytes(5));
    EXPECT_EQ(bytes_to_u32(out), (5u + 10) * 2);
    EXPECT_TRUE(reg.has_path("Counter", 1, 3));
}

TEST(StateSchemaVersion, BfsPicksShortestPath) {
    clink::StateMigrationRegistry reg;
    // Two paths from v1 to v3: long (v1 -> v2 -> v3 with side effects)
    // and short (direct v1 -> v3). BFS must pick the direct edge.
    reg.register_migration("Box", 1, 2, [](std::span<const std::byte>) {
        return u32_to_bytes(9999);  // would corrupt
    });
    reg.register_migration("Box", 2, 3, [](std::span<const std::byte>) {
        return u32_to_bytes(8888);  // would corrupt
    });
    reg.register_migration("Box", 1, 3, [](std::span<const std::byte> in) {
        return u32_to_bytes(bytes_to_u32(in) + 1);
    });

    auto out = reg.migrate("Box", 1, 3, u32_to_bytes(42));
    EXPECT_EQ(bytes_to_u32(out), 43u);
}

TEST(StateSchemaVersion, MissingPathThrows) {
    clink::StateMigrationRegistry reg;
    reg.register_migration(
        "Counter", 1, 2, [](std::span<const std::byte>) { return std::vector<std::byte>{}; });

    EXPECT_FALSE(reg.has_path("Counter", 1, 5));
    EXPECT_THROW(reg.migrate("Counter", 1, 5, u32_to_bytes(0)), std::runtime_error);
}

TEST(StateSchemaVersion, MissingStateTypeThrows) {
    clink::StateMigrationRegistry reg;
    reg.register_migration(
        "KnownType", 1, 2, [](std::span<const std::byte>) { return std::vector<std::byte>{}; });

    EXPECT_FALSE(reg.has_path("UnknownType", 1, 2));
    EXPECT_THROW(reg.migrate("UnknownType", 1, 2, u32_to_bytes(0)), std::runtime_error);
}

TEST(StateSchemaVersion, ReregisterReplacesFn) {
    clink::StateMigrationRegistry reg;
    reg.register_migration("Counter", 1, 2, [](std::span<const std::byte> in) {
        return u32_to_bytes(bytes_to_u32(in) + 1);
    });
    // Second registration overrides the first.
    reg.register_migration("Counter", 1, 2, [](std::span<const std::byte> in) {
        return u32_to_bytes(bytes_to_u32(in) + 100);
    });

    auto out = reg.migrate("Counter", 1, 2, u32_to_bytes(10));
    EXPECT_EQ(bytes_to_u32(out), 110u);

    // Only one edge should be recorded.
    auto edges = reg.edges_for("Counter");
    ASSERT_EQ(edges.size(), 1u);
    EXPECT_EQ(edges[0].from_version, 1u);
    EXPECT_EQ(edges[0].to_version, 2u);
}

TEST(StateSchemaVersion, RejectsSameVersionRegistration) {
    clink::StateMigrationRegistry reg;
    EXPECT_THROW(
        reg.register_migration(
            "Counter", 4, 4, [](std::span<const std::byte>) { return std::vector<std::byte>{}; }),
        std::invalid_argument);
}

TEST(StateSchemaVersion, RejectsNullFn) {
    clink::StateMigrationRegistry reg;
    EXPECT_THROW(reg.register_migration("Counter", 1, 2, {}), std::invalid_argument);
}

TEST(StateSchemaVersion, EdgesForReportsAllRegistered) {
    clink::StateMigrationRegistry reg;
    reg.register_migration(
        "T", 1, 2, [](std::span<const std::byte>) { return std::vector<std::byte>{}; });
    reg.register_migration(
        "T", 2, 3, [](std::span<const std::byte>) { return std::vector<std::byte>{}; });
    reg.register_migration(
        "T", 2, 4, [](std::span<const std::byte>) { return std::vector<std::byte>{}; });

    auto edges = reg.edges_for("T");
    EXPECT_EQ(edges.size(), 3u);

    // Validate the set of (from, to) pairs without depending on order.
    bool saw_1_2 = false, saw_2_3 = false, saw_2_4 = false;
    for (auto& e : edges) {
        if (e.from_version == 1 && e.to_version == 2)
            saw_1_2 = true;
        if (e.from_version == 2 && e.to_version == 3)
            saw_2_3 = true;
        if (e.from_version == 2 && e.to_version == 4)
            saw_2_4 = true;
    }
    EXPECT_TRUE(saw_1_2);
    EXPECT_TRUE(saw_2_3);
    EXPECT_TRUE(saw_2_4);
}

TEST(StateSchemaVersion, GlobalRegistryIsSingleton) {
    auto& a = clink::StateMigrationRegistry::global();
    auto& b = clink::StateMigrationRegistry::global();
    EXPECT_EQ(&a, &b);
}

// --- StateVersionMap pack/unpack + getters --------------------------

TEST(StateVersionMap, RoundTripPackUnpack) {
    clink::StateVersionMap m;
    m.set(clink::OperatorId{7}, "Counter", 2);
    m.set(clink::OperatorId{42}, "JoinState", 5);

    auto packed = m.pack();
    auto roundtripped = clink::StateVersionMap::unpack(packed);
    EXPECT_EQ(roundtripped.size(), 2u);
    EXPECT_EQ(roundtripped.get(clink::OperatorId{7}, "Counter"), std::optional<std::uint32_t>{2});
    EXPECT_EQ(roundtripped.get(clink::OperatorId{42}, "JoinState"),
              std::optional<std::uint32_t>{5});
}

TEST(StateVersionMap, EmptyMapRoundTrips) {
    clink::StateVersionMap empty;
    EXPECT_TRUE(empty.empty());
    EXPECT_EQ(empty.pack(), "");
    auto unpacked = clink::StateVersionMap::unpack("");
    EXPECT_TRUE(unpacked.empty());
}

TEST(StateVersionMap, SlotRoundTripsAndSlotlessStaysThreeField) {
    clink::StateVersionMap m;
    m.set(clink::OperatorId{7}, "TypeA", 2, "slot_a");  // 4-field
    m.set(clink::OperatorId{7}, "TypeB", 3);            // slotless -> 3-field

    const auto packed = m.pack();
    // The slotless entry must pack as the historic 3-field form (no
    // trailing '|'); the slotted entry carries the 4th field.
    EXPECT_NE(packed.find("7|TypeA|2|slot_a"), std::string::npos) << packed;
    EXPECT_NE(packed.find("7|TypeB|3"), std::string::npos) << packed;
    EXPECT_EQ(packed.find("7|TypeB|3|"), std::string::npos) << packed;

    const auto entries = clink::StateVersionMap::unpack(packed).entries();
    ASSERT_EQ(entries.size(), 2u);
    for (const auto& e : entries) {
        if (e.state_type == "TypeA") {
            EXPECT_EQ(e.version, 2u);
            EXPECT_EQ(e.slot, "slot_a");
        } else {
            EXPECT_EQ(e.state_type, "TypeB");
            EXPECT_EQ(e.version, 3u);
            EXPECT_TRUE(e.slot.empty());
        }
    }
}

TEST(StateVersionMap, LegacyThreeFieldUnpacksWithEmptySlot) {
    // A snapshot written before slots existed (3-field lines) must parse
    // with an empty slot, not throw.
    const auto m = clink::StateVersionMap::unpack("7|Counter|2\n42|JoinState|5");
    const auto entries = m.entries();
    ASSERT_EQ(entries.size(), 2u);
    for (const auto& e : entries) {
        EXPECT_TRUE(e.slot.empty()) << e.state_type;
    }
    EXPECT_EQ(m.get(clink::OperatorId{7}, "Counter"), std::optional<std::uint32_t>{2});
}

TEST(StateVersionMap, RejectsDelimitersInSlot) {
    clink::StateVersionMap m;
    EXPECT_THROW(m.set(clink::OperatorId{1}, "T", 1, "has|pipe"), std::invalid_argument);
    EXPECT_THROW(m.set(clink::OperatorId{1}, "T", 1, "has\nnl"), std::invalid_argument);
}

TEST(StateVersionMap, RejectsRebindingStateTypeToADifferentSlot) {
    // The migrator keys 'from' on (op, state_type) and filters by slot, so
    // the same state_type must map to one slot. Re-binding it to another
    // non-empty slot would silently drop the first - fail fast instead.
    clink::StateVersionMap m;
    m.set(clink::OperatorId{1}, "T", 2, "slot_a");
    EXPECT_THROW(m.set(clink::OperatorId{1}, "T", 3, "slot_b"), std::invalid_argument);
    // A pure version bump keeping the same slot is allowed (SetReplaces).
    EXPECT_NO_THROW(m.set(clink::OperatorId{1}, "T", 5, "slot_a"));
    EXPECT_EQ(m.get(clink::OperatorId{1}, "T"), std::optional<std::uint32_t>{5});
    // Distinct state_types on the same op may carry distinct slots.
    EXPECT_NO_THROW(m.set(clink::OperatorId{1}, "U", 1, "slot_b"));
}

TEST(StateVersionMap, UnpackThrowsRuntimeErrorOnTooManyFields) {
    // A corrupt savepoint line with a 5th field must surface as the
    // documented std::runtime_error (a parse failure), NOT as the
    // std::invalid_argument that set()'s slot-delimiter check would raise.
    EXPECT_THROW((void)clink::StateVersionMap::unpack("1|T|2|a|b"), std::runtime_error);
    EXPECT_THROW((void)clink::StateVersionMap::unpack("1|T|2|a|b"), std::exception);
}

TEST(StateVersionMap, SetReplaces) {
    clink::StateVersionMap m;
    m.set(clink::OperatorId{1}, "T", 1);
    m.set(clink::OperatorId{1}, "T", 4);
    EXPECT_EQ(m.size(), 1u);
    EXPECT_EQ(m.get(clink::OperatorId{1}, "T"), std::optional<std::uint32_t>{4});
}

TEST(StateVersionMap, GetMissingReturnsNullopt) {
    clink::StateVersionMap m;
    m.set(clink::OperatorId{1}, "X", 1);
    EXPECT_EQ(m.get(clink::OperatorId{1}, "Y"), std::nullopt);
    EXPECT_EQ(m.get(clink::OperatorId{2}, "X"), std::nullopt);
}

TEST(StateVersionMap, RejectsDelimitersInStateType) {
    clink::StateVersionMap m;
    EXPECT_THROW(m.set(clink::OperatorId{1}, "has|pipe", 1), std::invalid_argument);
    EXPECT_THROW(m.set(clink::OperatorId{1}, "has\nnewline", 1), std::invalid_argument);
}

TEST(StateVersionMap, UnpackRejectsCorrupt) {
    EXPECT_THROW(clink::StateVersionMap::unpack("no_delimiters_here"), std::runtime_error);
    EXPECT_THROW(clink::StateVersionMap::unpack("1|onlyone"), std::runtime_error);
    EXPECT_THROW(clink::StateVersionMap::unpack("notanumber|T|1"), std::runtime_error);
    EXPECT_THROW(clink::StateVersionMap::unpack("1|T|notanumber"), std::runtime_error);
}

// --- Snapshot integration: versions ride the Arrow IPC metadata -----

TEST(StateVersionMap, InMemoryBackendSnapshotRoundTripsVersions) {
    auto original = std::make_shared<clink::InMemoryStateBackend>();

    // Write some state plus a version map describing the codec
    // versions in play for two operators.
    original->put(clink::OperatorId{1}, "k1", std::string_view{"v1"});
    original->put(clink::OperatorId{2}, "k2", std::string_view{"v2"});

    clink::StateVersionMap versions;
    versions.set(clink::OperatorId{1}, "Counter", 3);
    versions.set(clink::OperatorId{2}, "WindowState", 7);
    original->set_state_versions(versions);

    auto snap = original->snapshot(clink::CheckpointId{1});

    // Restore into a fresh backend; both the keyed state and the
    // version map should survive the round-trip.
    auto fresh = std::make_shared<clink::InMemoryStateBackend>();
    fresh->restore(snap);

    EXPECT_TRUE(fresh->get(clink::OperatorId{1}, "k1").has_value());
    EXPECT_TRUE(fresh->get(clink::OperatorId{2}, "k2").has_value());

    auto restored = fresh->restored_state_versions();
    EXPECT_EQ(restored.size(), 2u);
    EXPECT_EQ(restored.get(clink::OperatorId{1}, "Counter"), std::optional<std::uint32_t>{3});
    EXPECT_EQ(restored.get(clink::OperatorId{2}, "WindowState"), std::optional<std::uint32_t>{7});
}

TEST(StateVersionMap, RestoreFromUnversionedSnapshotLeavesMapEmpty) {
    // Default (no versions stamped) - simulates an unversioned
    // snapshot. Restore should succeed and leave the version map empty
    // so the caller falls back to the SchemaVersionTrait default.
    auto original = std::make_shared<clink::InMemoryStateBackend>();
    original->put(clink::OperatorId{3}, "k", std::string_view{"v"});
    auto snap = original->snapshot(clink::CheckpointId{1});

    auto fresh = std::make_shared<clink::InMemoryStateBackend>();
    fresh->restore(snap);
    EXPECT_TRUE(fresh->restored_state_versions().empty());
}

TEST(StateVersionMap, EmptyVersionMapDoesNotAddMetadata) {
    // Set a clearly-empty map; snapshot should still produce a valid
    // (zero-metadata) Arrow stream and restore-on-fresh-backend should
    // not surface any phantom entries.
    auto original = std::make_shared<clink::InMemoryStateBackend>();
    original->set_state_versions(clink::StateVersionMap{});
    auto snap = original->snapshot(clink::CheckpointId{1});

    auto fresh = std::make_shared<clink::InMemoryStateBackend>();
    fresh->restore(snap);
    EXPECT_TRUE(fresh->restored_state_versions().empty());
}

TEST(StateVersionMap, EntriesAreSorted) {
    clink::StateVersionMap m;
    m.set(clink::OperatorId{5}, "B", 1);
    m.set(clink::OperatorId{1}, "B", 1);
    m.set(clink::OperatorId{5}, "A", 1);

    auto entries = m.entries();
    ASSERT_EQ(entries.size(), 3u);
    EXPECT_EQ(entries[0].op_id.value(), 1u);
    EXPECT_EQ(entries[1].op_id.value(), 5u);
    EXPECT_EQ(entries[1].state_type, "A");
    EXPECT_EQ(entries[2].op_id.value(), 5u);
    EXPECT_EQ(entries[2].state_type, "B");
}

// --- Arrow auto-migration -------------------------------------------

namespace {

// Helper: encode a one-row RecordBatch with the given schema + column
// arrays into an Arrow IPC stream byte blob. Mirrors the wire shape
// the auto-migration consumes.
std::vector<std::byte> encode_arrow_batch(std::shared_ptr<arrow::Schema> schema,
                                          std::vector<std::shared_ptr<arrow::Array>> columns,
                                          int64_t num_rows) {
    auto batch = arrow::RecordBatch::Make(schema, num_rows, columns);
    auto sink_result = arrow::io::BufferOutputStream::Create();
    auto sink = *sink_result;
    auto writer_result = arrow::ipc::MakeStreamWriter(sink, schema);
    auto writer = *writer_result;
    (void)writer->WriteRecordBatch(*batch);
    (void)writer->Close();
    auto buf = *sink->Finish();
    std::vector<std::byte> bytes(static_cast<std::size_t>(buf->size()));
    if (buf->size() > 0) {
        std::memcpy(bytes.data(), buf->data(), static_cast<std::size_t>(buf->size()));
    }
    return bytes;
}

// Helper: decode an Arrow IPC blob to a RecordBatch.
std::shared_ptr<arrow::RecordBatch> decode_arrow_batch(std::span<const std::byte> bytes) {
    auto buf = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(bytes.data()),
                                               static_cast<int64_t>(bytes.size()));
    auto input = std::make_shared<arrow::io::BufferReader>(buf);
    auto reader = *arrow::ipc::RecordBatchStreamReader::Open(input);
    std::shared_ptr<arrow::RecordBatch> batch;
    (void)reader->ReadNext(&batch);
    return batch;
}

}  // namespace

TEST(StateSchemaArrowMigration, RegisterAndLookupSchema) {
    clink::StateMigrationRegistry reg;
    auto schema = arrow::schema({arrow::field("user_id", arrow::int64(), /*nullable=*/false)});
    reg.register_arrow_schema("Counter", 1, schema);

    auto fetched = reg.arrow_schema_for("Counter", 1);
    ASSERT_NE(fetched, nullptr);
    EXPECT_TRUE(fetched->Equals(*schema));

    EXPECT_EQ(reg.arrow_schema_for("Counter", 2), nullptr);
    EXPECT_EQ(reg.arrow_schema_for("Unknown", 1), nullptr);
}

TEST(StateSchemaArrowMigration, RejectsNullSchema) {
    clink::StateMigrationRegistry reg;
    EXPECT_THROW(reg.register_arrow_schema("X", 1, nullptr), std::invalid_argument);
}

TEST(StateSchemaArrowMigration, AddingNullableColumnIsAdditive) {
    clink::StateMigrationRegistry reg;
    auto v1 = arrow::schema({arrow::field("count", arrow::int64(), /*nullable=*/false)});
    auto v2 = arrow::schema({arrow::field("count", arrow::int64(), /*nullable=*/false),
                             arrow::field("last_seen_ms", arrow::int64(), /*nullable=*/true)});
    reg.register_arrow_schema("UserCounter", 1, v1);
    reg.register_arrow_schema("UserCounter", 2, v2);

    EXPECT_TRUE(reg.can_auto_arrow_migrate("UserCounter", 1, 2));
    EXPECT_TRUE(reg.has_path("UserCounter", 1, 2));
}

TEST(StateSchemaArrowMigration, AddingNonNullableColumnIsNotAdditive) {
    clink::StateMigrationRegistry reg;
    auto v1 = arrow::schema({arrow::field("count", arrow::int64(), false)});
    auto v2 = arrow::schema({arrow::field("count", arrow::int64(), false),
                             arrow::field("required_field", arrow::int64(), /*nullable=*/false)});
    reg.register_arrow_schema("UserCounter", 1, v1);
    reg.register_arrow_schema("UserCounter", 2, v2);

    EXPECT_FALSE(reg.can_auto_arrow_migrate("UserCounter", 1, 2));
    EXPECT_FALSE(reg.has_path("UserCounter", 1, 2));
}

TEST(StateSchemaArrowMigration, RemovingColumnIsNotAdditive) {
    clink::StateMigrationRegistry reg;
    auto v1 = arrow::schema(
        {arrow::field("a", arrow::int64(), false), arrow::field("b", arrow::int64(), false)});
    auto v2 = arrow::schema({arrow::field("a", arrow::int64(), false)});
    reg.register_arrow_schema("Two", 1, v1);
    reg.register_arrow_schema("Two", 2, v2);

    EXPECT_FALSE(reg.can_auto_arrow_migrate("Two", 1, 2));
}

TEST(StateSchemaArrowMigration, WideningIntegerIsAdditive) {
    clink::StateMigrationRegistry reg;
    auto v1 = arrow::schema({arrow::field("n", arrow::int32(), false)});
    auto v2 = arrow::schema({arrow::field("n", arrow::int64(), false)});
    reg.register_arrow_schema("N", 1, v1);
    reg.register_arrow_schema("N", 2, v2);

    EXPECT_TRUE(reg.can_auto_arrow_migrate("N", 1, 2));
}

TEST(StateSchemaArrowMigration, NarrowingIntegerIsNotAdditive) {
    clink::StateMigrationRegistry reg;
    auto v1 = arrow::schema({arrow::field("n", arrow::int64(), false)});
    auto v2 = arrow::schema({arrow::field("n", arrow::int32(), false)});
    reg.register_arrow_schema("N", 1, v1);
    reg.register_arrow_schema("N", 2, v2);

    EXPECT_FALSE(reg.can_auto_arrow_migrate("N", 1, 2));
}

TEST(StateSchemaArrowMigration, MigrateAddsNullColumnForNewField) {
    clink::StateMigrationRegistry reg;
    auto v1 = arrow::schema({arrow::field("count", arrow::int64(), false)});
    auto v2 = arrow::schema({arrow::field("count", arrow::int64(), false),
                             arrow::field("ts_ms", arrow::int64(), /*nullable=*/true)});
    reg.register_arrow_schema("UserCounter", 1, v1);
    reg.register_arrow_schema("UserCounter", 2, v2);

    // Encode a one-row v1 batch with count=42.
    arrow::Int64Builder count_b;
    (void)count_b.Append(42);
    std::shared_ptr<arrow::Array> count_arr;
    (void)count_b.Finish(&count_arr);
    auto v1_bytes = encode_arrow_batch(v1, {count_arr}, 1);

    // Auto-migrate to v2.
    auto v2_bytes = reg.migrate("UserCounter", 1, 2, v1_bytes);

    auto v2_batch = decode_arrow_batch(v2_bytes);
    ASSERT_NE(v2_batch, nullptr);
    EXPECT_EQ(v2_batch->num_columns(), 2);
    EXPECT_EQ(v2_batch->num_rows(), 1);
    EXPECT_TRUE(v2_batch->schema()->Equals(*v2));

    const auto* count_col = static_cast<const arrow::Int64Array*>(v2_batch->column(0).get());
    EXPECT_EQ(count_col->Value(0), 42);

    // New ts_ms column is filled with nulls.
    const auto* ts_col = static_cast<const arrow::Int64Array*>(v2_batch->column(1).get());
    EXPECT_TRUE(ts_col->IsNull(0));
}

TEST(StateSchemaArrowMigration, MigrateWidensIntegerColumn) {
    clink::StateMigrationRegistry reg;
    auto v1 = arrow::schema({arrow::field("n", arrow::int32(), false)});
    auto v2 = arrow::schema({arrow::field("n", arrow::int64(), false)});
    reg.register_arrow_schema("N", 1, v1);
    reg.register_arrow_schema("N", 2, v2);

    arrow::Int32Builder b;
    (void)b.Append(123456);
    std::shared_ptr<arrow::Array> arr;
    (void)b.Finish(&arr);
    auto v1_bytes = encode_arrow_batch(v1, {arr}, 1);

    auto v2_bytes = reg.migrate("N", 1, 2, v1_bytes);
    auto batch = decode_arrow_batch(v2_bytes);
    ASSERT_NE(batch, nullptr);
    EXPECT_TRUE(batch->schema()->Equals(*v2));
    const auto* n_col = static_cast<const arrow::Int64Array*>(batch->column(0).get());
    EXPECT_EQ(n_col->Value(0), 123456);
}

TEST(StateSchemaArrowMigration, IncompatibleSchemasFailMigration) {
    clink::StateMigrationRegistry reg;
    auto v1 = arrow::schema({arrow::field("a", arrow::utf8(), false)});
    auto v2 = arrow::schema({arrow::field("a", arrow::int64(), false)});
    reg.register_arrow_schema("X", 1, v1);
    reg.register_arrow_schema("X", 2, v2);

    EXPECT_FALSE(reg.can_auto_arrow_migrate("X", 1, 2));
    EXPECT_THROW(reg.migrate("X", 1, 2, std::vector<std::byte>{}), std::runtime_error);
}

TEST(StateSchemaArrowMigration, ExplicitMigrationWinsOverAutoMigration) {
    // When the user registers an explicit migration fn AND the
    // schemas are also auto-compatible, the explicit fn takes
    // precedence. Lets users override the default projection
    // (e.g., compute a default value for the new column rather
    // than leaving it null).
    clink::StateMigrationRegistry reg;
    auto v1 = arrow::schema({arrow::field("count", arrow::int64(), false)});
    auto v2 = arrow::schema(
        {arrow::field("count", arrow::int64(), false), arrow::field("flag", arrow::int64(), true)});
    reg.register_arrow_schema("Box", 1, v1);
    reg.register_arrow_schema("Box", 2, v2);

    bool explicit_called = false;
    reg.register_migration("Box", 1, 2, [&](std::span<const std::byte> input) {
        explicit_called = true;
        return std::vector<std::byte>(input.begin(), input.end());
    });

    auto out = reg.migrate("Box", 1, 2, std::vector<std::byte>{});
    EXPECT_TRUE(explicit_called) << "explicit fn must be preferred over auto-migration";
}

// --- Integration: stamp drives a real value migration --------------
//
// The pieces above are tested in isolation (registry migrate; version
// stamps round-tripping a snapshot). This stitches them into the flow
// a restore coordinator performs end to end: state written at schema
// v1 is snapshotted with its version stamp, restored into a fresh
// backend, and because the live operator now expects v2, the recovered
// stamp drives a registry migration of the stored value bytes through
// the backend's raw get/put - after which a v2 reader sees the
// migrated value. Guards the whole stamp -> detect -> migrate path,
// not just its halves.
struct UserCounterV2 {
    std::uint64_t count{};
    std::uint64_t bonus{};
};

TEST(StateSchemaVersionIntegration, StampedV1StateMigratesToV2OnRestore) {
    // v1 value layout: [count:u64]; v2 layout: [count:u64][bonus:u64].
    const auto enc_u64 = [](std::uint64_t v) {
        std::string s(8, '\0');
        for (std::size_t i = 0; i < 8; ++i) {
            s[i] = static_cast<char>((v >> (i * 8)) & 0xFF);
        }
        return s;
    };
    const auto dec_u64 = [](std::span<const std::byte> b, std::size_t off) {
        std::uint64_t v = 0;
        for (std::size_t i = 0; i < 8; ++i) {
            v |= static_cast<std::uint64_t>(static_cast<unsigned char>(b[off + i])) << (i * 8);
        }
        return v;
    };

    const clink::OperatorId op{42};
    const std::string key = "user:1";
    const std::string state_type = "UserCounter";

    // Original run: write v1 state, stamp version 1, snapshot.
    auto original = std::make_shared<clink::InMemoryStateBackend>();
    original->put(op, key, enc_u64(7));  // count = 7
    clink::StateVersionMap versions;
    versions.set(op, state_type, 1);
    original->set_state_versions(versions);
    auto snap = original->snapshot(clink::CheckpointId{1});

    // New run: restore into a fresh backend; the live operator's codec
    // is now UserCounterV2 (expected schema version 2).
    auto fresh = std::make_shared<clink::InMemoryStateBackend>();
    fresh->restore(snap);

    const auto restored_v = fresh->restored_state_versions().get(op, state_type);
    ASSERT_EQ(restored_v, std::optional<std::uint32_t>{1})
        << "the v1 stamp must survive the snapshot/restore round-trip";
    const std::uint32_t expected_v = 2;

    // The stored value is the raw v1 form (8 bytes). A v2 reader would
    // mis-read it - which is exactly what migration prevents.
    auto raw = fresh->get(op, key);
    ASSERT_TRUE(raw.has_value());
    ASSERT_EQ(raw->size(), 8u);

    // Restore-coordinator step: stamp says v1, operator wants v2, so
    // migrate the stored bytes through the registry and write them back.
    clink::StateMigrationRegistry reg;
    reg.register_migration(state_type, 1, 2, [](std::span<const std::byte> in) {
        std::vector<std::byte> migrated(in.begin(), in.end());
        migrated.resize(16, std::byte{0});  // preserve count, default bonus
        return migrated;
    });
    ASSERT_TRUE(reg.has_path(state_type, *restored_v, expected_v));
    const auto migrated = reg.migrate(state_type, *restored_v, expected_v, *raw);
    fresh->put(
        op, key, std::string_view{reinterpret_cast<const char*>(migrated.data()), migrated.size()});

    // v2 reader now sees the migrated value: count preserved, bonus
    // default-filled.
    auto v2raw = fresh->get(op, key);
    ASSERT_TRUE(v2raw.has_value());
    ASSERT_EQ(v2raw->size(), 16u);
    const UserCounterV2 got{dec_u64(*v2raw, 0), dec_u64(*v2raw, 8)};
    EXPECT_EQ(got.count, 7u) << "original count must be preserved across migration";
    EXPECT_EQ(got.bonus, 0u) << "new v2 field must be default-filled";
}

// --- Schema-evo foundation: check_restore_compatibility ------------

TEST(StateMigrationOnRestore, CheckFlagsOnlyUnbridgeableEntries) {
    const clink::OperatorId op_a{1};
    const clink::OperatorId op_b{2};
    clink::StateMigrationRegistry reg;
    reg.register_migration("A", 1, 2, [](std::span<const std::byte> in) {
        return std::vector<std::byte>(in.begin(), in.end());
    });

    clink::StateVersionMap stored;
    stored.set(op_a, "A", 1);
    // op_b "B" intentionally NOT stamped -> treated as version 1.

    clink::StateVersionMap expected;
    expected.set(op_a, "A", 2);  // bridgeable (1->2 registered)
    expected.set(op_b, "B", 2);  // unbridgeable (no migration), from defaults to 1

    auto incompat = clink::check_restore_compatibility(stored, expected, reg);
    ASSERT_EQ(incompat.size(), 1u);
    EXPECT_EQ(incompat[0].op_id, op_b);
    EXPECT_EQ(incompat[0].state_type, "B");
    EXPECT_EQ(incompat[0].from_version, 1u);  // value_or(1) for the unstamped entry
    EXPECT_EQ(incompat[0].to_version, 2u);
}

TEST(StateMigrationOnRestore, CheckPassesWhenVersionsMatchOrPathExists) {
    const clink::OperatorId op{1};
    clink::StateMigrationRegistry reg;
    reg.register_migration("T", 1, 2, [](std::span<const std::byte> in) {
        return std::vector<std::byte>(in.begin(), in.end());
    });
    reg.register_migration("T", 2, 3, [](std::span<const std::byte> in) {
        return std::vector<std::byte>(in.begin(), in.end());
    });
    clink::StateVersionMap stored;
    stored.set(op, "T", 1);
    // Equal version -> no work; multi-step chain 1->3 reachable.
    clink::StateVersionMap same;
    same.set(op, "T", 1);
    EXPECT_TRUE(clink::check_restore_compatibility(stored, same, reg).empty());
    clink::StateVersionMap up;
    up.set(op, "T", 3);
    EXPECT_TRUE(clink::check_restore_compatibility(stored, up, reg).empty());
}

// --- Schema-evo D: incompatibility pack/unpack (.so-ABI boundary) --

TEST(StateMigrationOnRestore, IncompatibilitiesRoundTripThroughPack) {
    std::vector<clink::StateIncompatibility> in{
        clink::StateIncompatibility{clink::OperatorId{7}, "i64_sum", 1, 3},
        clink::StateIncompatibility{clink::OperatorId{42}, "win_state", 2, 5},
    };
    const std::string packed = clink::pack_incompatibilities(in);
    const auto out = clink::unpack_incompatibilities(packed);
    ASSERT_EQ(out.size(), in.size());
    for (std::size_t i = 0; i < in.size(); ++i) {
        EXPECT_EQ(out[i].op_id, in[i].op_id);
        EXPECT_EQ(out[i].state_type, in[i].state_type);
        EXPECT_EQ(out[i].from_version, in[i].from_version);
        EXPECT_EQ(out[i].to_version, in[i].to_version);
    }
}

TEST(StateMigrationOnRestore, EmptyIncompatibilityListMeansCompatible) {
    EXPECT_TRUE(clink::pack_incompatibilities({}).empty());
    EXPECT_TRUE(clink::unpack_incompatibilities("").empty());
}

TEST(StateMigrationOnRestore, UnpackRejectsMalformedLine) {
    // Only two '|' separators where four fields (three separators) are
    // required.
    EXPECT_THROW((void)clink::unpack_incompatibilities("7|i64_sum|3"), std::runtime_error);
    EXPECT_THROW((void)clink::unpack_incompatibilities("notanum|t|1|2"), std::runtime_error);
}

// --- Schema-evo foundation: migrate_restored_state ----------------

TEST(StateMigrationOnRestore, MigratesEveryValueUnderTheOpAndRestamps) {
    const auto enc_u64 = [](std::uint64_t v) {
        std::string s(8, '\0');
        for (std::size_t i = 0; i < 8; ++i)
            s[i] = static_cast<char>((v >> (i * 8)) & 0xFF);
        return s;
    };
    const clink::OperatorId op{42};
    const std::string state_type = "UserCounter";

    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    backend->put(op, "user:1", enc_u64(7));
    backend->put(op, "user:2", enc_u64(99));
    clink::StateVersionMap v1;
    v1.set(op, state_type, 1);
    backend->set_state_versions(v1);
    // Round-trip through a snapshot so restored_state_versions() reports v1.
    auto snap = backend->snapshot(clink::CheckpointId{1});
    auto fresh = std::make_shared<clink::InMemoryStateBackend>();
    fresh->restore(snap);

    clink::StateMigrationRegistry reg;
    reg.register_migration(state_type, 1, 2, [](std::span<const std::byte> in) {
        std::vector<std::byte> out(in.begin(), in.end());
        out.resize(16, std::byte{0});  // v2 appends an 8-byte bonus field
        return out;
    });
    clink::StateVersionMap expected;
    expected.set(op, state_type, 2);

    clink::migrate_restored_state(*fresh, expected, reg);

    // Both keys migrated (8 -> 16 bytes), original prefix preserved.
    for (const auto& [key, want] : {std::pair{"user:1", 7u}, std::pair{"user:2", 99u}}) {
        auto v = fresh->get(op, key);
        ASSERT_TRUE(v.has_value());
        ASSERT_EQ(v->size(), 16u) << key;
        std::uint64_t count = 0;
        for (std::size_t i = 0; i < 8; ++i) {
            count |= static_cast<std::uint64_t>(static_cast<unsigned char>((*v)[i])) << (i * 8);
        }
        EXPECT_EQ(count, want) << key;
    }
    // Re-stamped to v2 so the next snapshot records the migrated version.
    EXPECT_EQ(fresh->restored_state_versions().get(op, state_type),
              std::optional<std::uint32_t>{2});
}

TEST(StateMigrationOnRestore, MigrateThrowsOnMissingPath) {
    const clink::OperatorId op{1};
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    backend->put(op, "k", std::string(8, '\0'));
    clink::StateVersionMap v1;
    v1.set(op, "T", 1);
    backend->set_state_versions(v1);
    auto snap = backend->snapshot(clink::CheckpointId{1});
    auto fresh = std::make_shared<clink::InMemoryStateBackend>();
    fresh->restore(snap);

    clink::StateMigrationRegistry reg;  // no migration registered
    clink::StateVersionMap expected;
    expected.set(op, "T", 2);
    EXPECT_THROW(clink::migrate_restored_state(*fresh, expected, reg), std::runtime_error);
}

TEST(StateMigrationOnRestore, MigrateIsNoOpWhenVersionsAlreadyMatch) {
    const clink::OperatorId op{1};
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    backend->put(op, "k", std::string(8, '\x05'));
    clink::StateVersionMap v2;
    v2.set(op, "T", 2);
    backend->set_state_versions(v2);
    auto snap = backend->snapshot(clink::CheckpointId{1});
    auto fresh = std::make_shared<clink::InMemoryStateBackend>();
    fresh->restore(snap);

    clink::StateMigrationRegistry reg;  // none needed
    clink::StateVersionMap expected;
    expected.set(op, "T", 2);
    clink::migrate_restored_state(*fresh, expected, reg);  // must not throw
    auto v = fresh->get(op, "k");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(v->size(), 8u);  // unchanged
}

// --- Schema-evo: slot-aware migration (multi-slot operators) ------

namespace {
// Build a backend key in the KeyedState layout: [kg_byte][slot]['|'][user].
// The kg byte is irrelevant to the slot filter, so any value works.
std::string slot_key(std::string_view slot, std::string_view user) {
    std::string k;
    k.push_back('\x00');
    k.append(slot);
    k.push_back('|');
    k.append(user);
    return k;
}
// A growing migration: v2 appends 8 zero bytes to the value.
clink::StateMigrationRegistry::MigrationFn grow8() {
    return [](std::span<const std::byte> in) {
        std::vector<std::byte> out(in.begin(), in.end());
        out.resize(in.size() + 8, std::byte{0});
        return out;
    };
}
}  // namespace

TEST(StateMigrationOnRestore, MigratesOnlyTheNamedSlotLeavingSiblingsByteIdentical) {
    const clink::OperatorId op{7};
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    backend->put(op, slot_key("slot_a", "k1"), std::string(8, '\x01'));
    backend->put(op, slot_key("slot_a", "k2"), std::string(8, '\x02'));
    backend->put(op, slot_key("slot_b", "k1"), std::string(8, '\x41'));  // 'A'
    backend->put(op, slot_key("slot_b", "k2"), std::string(8, '\x42'));  // 'B'

    clink::StateVersionMap stored;
    stored.set(op, "TypeA", 1, "slot_a");
    stored.set(op, "TypeB", 1, "slot_b");
    backend->set_state_versions(stored);
    auto snap = backend->snapshot(clink::CheckpointId{1});
    auto fresh = std::make_shared<clink::InMemoryStateBackend>();
    fresh->restore(snap);

    clink::StateMigrationRegistry reg;
    reg.register_migration("TypeA", 1, 2, grow8());

    // Bump only slot_a's version; slot_b stays v1 (a path that does not
    // even exist for TypeB - proving slot_b is never touched).
    clink::StateVersionMap expected;
    expected.set(op, "TypeA", 2, "slot_a");
    expected.set(op, "TypeB", 1, "slot_b");

    clink::migrate_restored_state(*fresh, expected, reg);

    // slot_a migrated: 8 -> 16 bytes.
    EXPECT_EQ(fresh->get(op, slot_key("slot_a", "k1"))->size(), 16u);
    EXPECT_EQ(fresh->get(op, slot_key("slot_a", "k2"))->size(), 16u);
    // slot_b byte-identical: still 8 bytes, original content.
    for (const auto& [user, byte] : {std::pair{"k1", '\x41'}, std::pair{"k2", '\x42'}}) {
        auto v = fresh->get(op, slot_key("slot_b", user));
        ASSERT_TRUE(v.has_value());
        ASSERT_EQ(v->size(), 8u) << user;
        for (auto b : *v) {
            EXPECT_EQ(static_cast<char>(b), byte) << user;
        }
    }
}

TEST(StateMigrationOnRestore, SlotPrefixMatchIsExactNotAStringPrefix) {
    // Slots "buf" and "buf2": migrating "buf" must NOT touch "buf2". The
    // '|' sentinel after the slot name is what distinguishes them.
    const clink::OperatorId op{3};
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    backend->put(op, slot_key("buf", "k"), std::string(8, '\x01'));
    backend->put(op, slot_key("buf2", "k"), std::string(8, '\x09'));

    clink::StateVersionMap stored;
    stored.set(op, "Buf", 1, "buf");
    stored.set(op, "Buf2", 1, "buf2");
    backend->set_state_versions(stored);
    auto snap = backend->snapshot(clink::CheckpointId{1});
    auto fresh = std::make_shared<clink::InMemoryStateBackend>();
    fresh->restore(snap);

    clink::StateMigrationRegistry reg;
    reg.register_migration("Buf", 1, 2, grow8());
    clink::StateVersionMap expected;
    expected.set(op, "Buf", 2, "buf");
    expected.set(op, "Buf2", 1, "buf2");

    clink::migrate_restored_state(*fresh, expected, reg);

    EXPECT_EQ(fresh->get(op, slot_key("buf", "k"))->size(), 16u);  // migrated
    EXPECT_EQ(fresh->get(op, slot_key("buf2", "k"))->size(), 8u);  // untouched
}

TEST(StateMigrationOnRestore, SlotAbsentFromExpectedMapIsLeftUntouched) {
    // Only slot_a is declared+bumped; slot_b has no expected entry at all,
    // so its values must survive unchanged.
    const clink::OperatorId op{5};
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    backend->put(op, slot_key("slot_a", "k"), std::string(8, '\x01'));
    backend->put(op, slot_key("slot_b", "k"), std::string(8, '\x07'));
    clink::StateVersionMap stored;
    stored.set(op, "TypeA", 1, "slot_a");
    backend->set_state_versions(stored);
    auto snap = backend->snapshot(clink::CheckpointId{1});
    auto fresh = std::make_shared<clink::InMemoryStateBackend>();
    fresh->restore(snap);

    clink::StateMigrationRegistry reg;
    reg.register_migration("TypeA", 1, 2, grow8());
    clink::StateVersionMap expected;
    expected.set(op, "TypeA", 2, "slot_a");  // slot_b intentionally absent

    clink::migrate_restored_state(*fresh, expected, reg);

    EXPECT_EQ(fresh->get(op, slot_key("slot_a", "k"))->size(), 16u);  // migrated
    EXPECT_EQ(fresh->get(op, slot_key("slot_b", "k"))->size(), 8u);   // untouched
}

TEST(StateMigrationOnRestore, EmptySlotMigratesEveryValueUnderOp) {
    // Legacy / single-slot contract: an expected entry with an empty slot
    // migrates ALL values under the op, even slot-prefixed ones.
    const clink::OperatorId op{9};
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    backend->put(op, slot_key("slot_a", "k"), std::string(8, '\x01'));
    backend->put(op, slot_key("slot_b", "k"), std::string(8, '\x02'));
    clink::StateVersionMap stored;
    stored.set(op, "T", 1);  // no slot
    backend->set_state_versions(stored);
    auto snap = backend->snapshot(clink::CheckpointId{1});
    auto fresh = std::make_shared<clink::InMemoryStateBackend>();
    fresh->restore(snap);

    clink::StateMigrationRegistry reg;
    reg.register_migration("T", 1, 2, grow8());
    clink::StateVersionMap expected;
    expected.set(op, "T", 2);  // empty slot -> whole-op

    clink::migrate_restored_state(*fresh, expected, reg);

    EXPECT_EQ(fresh->get(op, slot_key("slot_a", "k"))->size(), 16u);
    EXPECT_EQ(fresh->get(op, slot_key("slot_b", "k"))->size(), 16u);
}

TEST(StateMigrationOnRestore, RealKeyedStateKeysFilterBySlotEndToEnd) {
    // Use the REAL KeyedState encoder (not hand-built keys) to prove the
    // migrator's slot filter matches what operators actually write.
    const clink::OperatorId op{11};
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    {
        clink::KeyedState<std::string, std::int64_t> a(
            *backend, op, "slot_a", clink::string_codec(), clink::int64_codec());
        clink::KeyedState<std::string, std::int64_t> b(
            *backend, op, "slot_b", clink::string_codec(), clink::int64_codec());
        a.put("x", 1);
        a.put("y", 2);
        b.put("x", 3);
    }
    clink::StateVersionMap stored;
    stored.set(op, "TypeA", 1, "slot_a");
    stored.set(op, "TypeB", 1, "slot_b");
    backend->set_state_versions(stored);
    auto snap = backend->snapshot(clink::CheckpointId{1});
    auto fresh = std::make_shared<clink::InMemoryStateBackend>();
    fresh->restore(snap);

    clink::StateMigrationRegistry reg;
    reg.register_migration("TypeA", 1, 2, grow8());
    clink::StateVersionMap expected;
    expected.set(op, "TypeA", 2, "slot_a");
    expected.set(op, "TypeB", 1, "slot_b");
    clink::migrate_restored_state(*fresh, expected, reg);

    // int64_codec encodes a fixed 8 bytes. slot_a migrated -> 16, slot_b
    // untouched -> 8. Verify against the REAL encoded keys via raw scan.
    auto slot_of = [](clink::StateBackend::KeyView k, std::string_view slot) {
        return k.size() >= 1 + slot.size() + 1 && k.compare(1, slot.size(), slot) == 0 &&
               k[1 + slot.size()] == '|';
    };
    std::size_t a_count = 0;
    std::size_t b_count = 0;
    fresh->scan(op, [&](clink::StateBackend::KeyView k, clink::StateBackend::ValueView v) {
        if (slot_of(k, "slot_a")) {
            ++a_count;
            EXPECT_EQ(v.size(), 16u);
        } else if (slot_of(k, "slot_b")) {
            ++b_count;
            EXPECT_EQ(v.size(), 8u);
        }
    });
    EXPECT_EQ(a_count, 2u);
    EXPECT_EQ(b_count, 1u);
}

TEST(StateMigrationOnRestore, RestampMergesSoUndeclaredSiblingStampsSurvive) {
    // Multi-generation: gen-2 bumps only slot_a and declares only slot_a.
    // The re-stamp must MERGE (not replace) so slot_b's stored stamp
    // survives - otherwise gen-3 would read slot_b as v1 and wrongly
    // re-migrate already-current data.
    const clink::OperatorId op{13};
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    backend->put(op, slot_key("slot_a", "k"), std::string(8, '\x01'));
    backend->put(op, slot_key("slot_b", "k"), std::string(8, '\x02'));
    clink::StateVersionMap gen1;
    gen1.set(op, "TypeA", 1, "slot_a");
    gen1.set(op, "TypeB", 2, "slot_b");  // slot_b already at v2
    backend->set_state_versions(gen1);
    auto snap = backend->snapshot(clink::CheckpointId{1});
    auto fresh = std::make_shared<clink::InMemoryStateBackend>();
    fresh->restore(snap);

    clink::StateMigrationRegistry reg;
    reg.register_migration("TypeA", 1, 2, grow8());
    // Gen-2 declares ONLY slot_a (the one it bumped).
    clink::StateVersionMap gen2_expected;
    gen2_expected.set(op, "TypeA", 2, "slot_a");

    clink::migrate_restored_state(*fresh, gen2_expected, reg);

    // slot_b's stamp must survive the re-stamp (merge), not be dropped.
    EXPECT_EQ(fresh->restored_state_versions().get(op, "TypeB"), std::optional<std::uint32_t>{2});
    EXPECT_EQ(fresh->restored_state_versions().get(op, "TypeA"), std::optional<std::uint32_t>{2});
}

// End-to-end: the LocalExecutor restore hook migrates restored state up
// to the job's expected versions BEFORE any operator runs. The migrate
// fires synchronously inside start() (ahead of operator threads), so we
// can inspect the backend right after the job completes.
TEST(StateMigrationOnRestore, LocalExecutorMigratesRestoredStateBeforeOperatorsRun) {
    const auto enc_u64 = [](std::uint64_t v) {
        std::string s(8, '\0');
        for (std::size_t i = 0; i < 8; ++i)
            s[i] = static_cast<char>((v >> (i * 8)) & 0xFF);
        return s;
    };
    const clink::OperatorId stateful_op{42};
    // Unique state_type so registering in the GLOBAL registry (which the
    // restore hook uses) does not bleed into other tests.
    const std::string state_type = "MigrateOnRestoreIT";
    clink::StateMigrationRegistry::global().register_migration(
        state_type, 1, 2, [](std::span<const std::byte> in) {
            std::vector<std::byte> out(in.begin(), in.end());
            out.resize(16, std::byte{0});  // v2 appends an 8-byte field
            return out;
        });

    // Seed v1 state, stamp v1, snapshot.
    auto original = std::make_shared<clink::InMemoryStateBackend>();
    original->put(stateful_op, "k", enc_u64(7));
    clink::StateVersionMap v1;
    v1.set(stateful_op, state_type, 1);
    original->set_state_versions(v1);
    auto snap = original->snapshot(clink::CheckpointId{1});

    // A trivial job that restores into a fresh backend, expecting v2.
    auto fresh = std::make_shared<clink::InMemoryStateBackend>();
    clink::Dag dag;
    auto src = std::make_shared<clink::VectorSource<std::int64_t>>(
        std::vector<clink::Record<std::int64_t>>{});  // empty: completes immediately
    auto h = dag.add_source<std::int64_t>(src);
    auto sink = std::make_shared<clink::CollectingSink<std::int64_t>>();
    dag.add_sink<std::int64_t>(h, sink);

    clink::JobConfig cfg;
    cfg.state_backend = fresh;
    cfg.restore_from = std::move(snap);
    clink::StateVersionMap expected;
    expected.set(stateful_op, state_type, 2);
    cfg.expected_state_versions = expected;

    clink::LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    // Restored v1 value migrated to v2 (8 -> 16 bytes), count preserved,
    // and the backend re-stamped to v2.
    auto val = fresh->get(stateful_op, "k");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val->size(), 16u);
    std::uint64_t count = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        count |= static_cast<std::uint64_t>(static_cast<unsigned char>((*val)[i])) << (i * 8);
    }
    EXPECT_EQ(count, 7u);
    EXPECT_EQ(fresh->restored_state_versions().get(stateful_op, state_type),
              std::optional<std::uint32_t>{2});
}

}  // namespace
