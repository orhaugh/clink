// The shape-fingerprint gate (design record 009, increment 3): a described
// type's field list changing WITHOUT a declared schema-version bump must
// refuse the restore at bind time, because reading on would misinterpret
// every value; the declared bump + migration path must pass, because the
// migration clears the stamp; and anything without a stamp (an older
// snapshot, an undescribed type, a backend that stores none) gates
// nothing.

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/core/derived_codec.hpp"
#include "clink/core/fields.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/schema_version.hpp"
#include "clink/state/state_migration_on_restore.hpp"

// Described types at namespace scope (the macro specialises
// clink::ArrowFields<T>), Fpg-prefixed against cross-TU collisions.
struct FpgTradeV1 {
    std::int64_t id{};
    std::string sym;
    bool operator==(const FpgTradeV1&) const = default;
};
CLINK_ARROW_FIELDS(FpgTradeV1, id, sym);

// The same two fields, declared in the other order: the derived codec
// would write different bytes, so the fingerprint MUST differ.
struct FpgTradeReordered {
    std::string sym;
    std::int64_t id{};
    bool operator==(const FpgTradeReordered&) const = default;
};
CLINK_ARROW_FIELDS(FpgTradeReordered, sym, id);

// The declared-evolution target: one field added.
struct FpgTradeV2 {
    std::int64_t id{};
    std::string sym;
    std::int64_t qty{};
    bool operator==(const FpgTradeV2&) const = default;
};
CLINK_ARROW_FIELDS(FpgTradeV2, id, sym, qty);

struct FpgOptWrapped {
    std::optional<std::int64_t> v;
};
CLINK_ARROW_FIELDS(FpgOptWrapped, v);

struct FpgPlain {
    std::int64_t v{};
};
CLINK_ARROW_FIELDS(FpgPlain, v);

// Compile-time pins: the relational properties the gate depends on. The
// ABSOLUTE values are frozen by the state-fingerprints fixture in
// tests/test_format_fixtures.cpp, so the kind-tag table cannot drift
// silently between builds.
static_assert(clink::fields_fingerprint_v<FpgTradeV1> !=
                  clink::fields_fingerprint_v<FpgTradeReordered>,
              "field ORDER must be part of the shape");
static_assert(clink::fields_fingerprint_v<FpgTradeV1> != clink::fields_fingerprint_v<FpgTradeV2>,
              "an added field must change the shape");
static_assert(clink::fields_fingerprint_v<FpgOptWrapped> != clink::fields_fingerprint_v<FpgPlain>,
              "optional<T> and T must not share a shape even under one field name");

namespace {

using clink::CheckpointId;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::RuntimeContext;

constexpr auto kOp = OperatorId{7};
const std::string kSlot = "fpg_slot";

template <typename V>
clink::KeyedState<std::int64_t, V> bind(RuntimeContext& ctx) {
    return ctx.template keyed_state<std::int64_t, V>(
        kSlot, clink::int64_codec(), clink::derived_codec<V>());
}

TEST(StateFingerprintGate, SameShapeRestoresAndRestamps) {
    InMemoryStateBackend writer_backend;
    RuntimeContext write_ctx(kOp, "fpg", &writer_backend, nullptr);
    auto st = bind<FpgTradeV1>(write_ctx);
    st.put(1, FpgTradeV1{.id = 1, .sym = "a"});
    const auto snap = writer_backend.snapshot(CheckpointId{1});

    InMemoryStateBackend restored;
    restored.restore(snap);
    RuntimeContext read_ctx(kOp, "fpg", &restored, nullptr);
    auto st2 = bind<FpgTradeV1>(read_ctx);  // must not throw
    const auto got = st2.get(1);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, (FpgTradeV1{.id = 1, .sym = "a"}));
}

TEST(StateFingerprintGate, AReorderedFieldListWithoutABumpIsRefused) {
    // The headline: same slot, same fields, different declared order, no
    // version bump, no migration. Every byte would be misread.
    InMemoryStateBackend writer_backend;
    RuntimeContext write_ctx(kOp, "fpg", &writer_backend, nullptr);
    auto st = bind<FpgTradeV1>(write_ctx);
    st.put(1, FpgTradeV1{.id = 1, .sym = "a"});
    const auto snap = writer_backend.snapshot(CheckpointId{1});

    InMemoryStateBackend restored;
    restored.restore(snap);
    RuntimeContext read_ctx(kOp, "fpg", &restored, nullptr);
    try {
        auto st2 = bind<FpgTradeReordered>(read_ctx);
        FAIL() << "a reordered shape bound against the old bytes";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string{e.what()}.find("shape"), std::string::npos) << e.what();
        EXPECT_NE(std::string{e.what()}.find("SchemaVersionTrait"), std::string::npos)
            << "the refusal must name the remedy: " << e.what();
    }
}

TEST(StateFingerprintGate, ADeclaredBumpWithAMigrationPasses) {
    // v1 -> v2: the migration rewrites the values AND clears the stamp,
    // so binding the new shape afterwards is legitimate and passes.
    const std::string state_type = "fpg.trade.migration_clears_stamp";
    clink::StateMigrationRegistry::global().register_migration(
        state_type, 1, 2, [](std::span<const std::byte> in) {
            const auto v1 = clink::derived_codec<FpgTradeV1>().decode(in);
            if (!v1.has_value()) {
                throw std::runtime_error("fpg migration: v1 bytes did not decode");
            }
            return clink::derived_codec<FpgTradeV2>().encode(
                FpgTradeV2{.id = v1->id, .sym = v1->sym, .qty = 0});
        });

    InMemoryStateBackend writer_backend;
    RuntimeContext write_ctx(kOp, "fpg", &writer_backend, nullptr);
    auto st = bind<FpgTradeV1>(write_ctx);
    st.put(1, FpgTradeV1{.id = 5, .sym = "m"});
    clink::StateVersionMap v1_stamp;
    v1_stamp.set(kOp, state_type, 1, kSlot);
    writer_backend.set_state_versions(v1_stamp);
    const auto snap = writer_backend.snapshot(CheckpointId{1});

    InMemoryStateBackend restored;
    restored.restore(snap);
    clink::StateVersionMap expected;
    expected.set(kOp, state_type, 2, kSlot);
    clink::migrate_restored_state(restored, expected);

    RuntimeContext read_ctx(kOp, "fpg", &restored, nullptr);
    auto st2 = bind<FpgTradeV2>(read_ctx);  // must not throw: stamp cleared
    const auto got = st2.get(1);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, (FpgTradeV2{.id = 5, .sym = "m", .qty = 0}));
}

TEST(StateFingerprintGate, AnUnstampedSnapshotGatesNothing) {
    // The pre-fingerprint world: values exist, no stamp rides the
    // snapshot. Binding ANY shape must behave exactly as before the gate
    // existed. (Simulated by clearing the stamp before the snapshot.)
    InMemoryStateBackend writer_backend;
    RuntimeContext write_ctx(kOp, "fpg", &writer_backend, nullptr);
    auto st = bind<FpgTradeV1>(write_ctx);
    st.put(1, FpgTradeV1{.id = 1, .sym = "a"});
    writer_backend.clear_state_fingerprint(kOp, kSlot);
    const auto snap = writer_backend.snapshot(CheckpointId{1});

    InMemoryStateBackend restored;
    restored.restore(snap);
    RuntimeContext read_ctx(kOp, "fpg", &restored, nullptr);
    auto st2 = bind<FpgTradeReordered>(read_ctx);  // absent stamp: no gate
    (void)st2;
}

TEST(StateFingerprintGate, ClearForAnEmptySlotClearsEveryStampUnderTheOperator) {
    clink::StateFingerprintMap m;
    m.set(kOp, "a", 1);
    m.set(kOp, "b", 2);
    m.set(OperatorId{8}, "a", 3);
    m.clear_for(kOp, "");
    EXPECT_FALSE(m.get(kOp, "a").has_value());
    EXPECT_FALSE(m.get(kOp, "b").has_value());
    EXPECT_TRUE(m.get(OperatorId{8}, "a").has_value());
}

// ----- the pre-deploy twin of the bind gate -----

TEST(FingerprintCompatCheck, MatchingFingerprintsPass) {
    clink::StateFingerprintMap stored;
    stored.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeV1>);
    clink::StateFingerprintMap declared;
    declared.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeV1>);
    EXPECT_TRUE(clink::check_fingerprint_compatibility(stored, declared, {}, {}).empty());
}

TEST(FingerprintCompatCheck, MismatchWithNoBumpIsReported) {
    clink::StateFingerprintMap stored;
    stored.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeV1>);
    clink::StateFingerprintMap declared;
    declared.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeReordered>);
    const auto out = clink::check_fingerprint_compatibility(stored, declared, {}, {});
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0].op_id, kOp);
    EXPECT_EQ(out[0].slot, kSlot);
    EXPECT_EQ(out[0].stored_fingerprint, clink::fields_fingerprint_v<FpgTradeV1>);
    EXPECT_EQ(out[0].declared_fingerprint, clink::fields_fingerprint_v<FpgTradeReordered>);
}

TEST(FingerprintCompatCheck, MismatchWithSlotMatchedBumpPasses) {
    clink::StateFingerprintMap stored;
    stored.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeV1>);
    clink::StateFingerprintMap declared;
    declared.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeV2>);
    clink::StateVersionMap stored_versions;
    stored_versions.set(kOp, "fpg.trade", 1, kSlot);
    clink::StateVersionMap expected;
    expected.set(kOp, "fpg.trade", 2, kSlot);
    EXPECT_TRUE(
        clink::check_fingerprint_compatibility(stored, declared, stored_versions, expected).empty())
        << "a declared bump covering the slot is migration intent - the migrator clears the stamp";
}

TEST(FingerprintCompatCheck, MismatchWithOpWideBumpPasses) {
    clink::StateFingerprintMap stored;
    stored.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeV1>);
    clink::StateFingerprintMap declared;
    declared.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeV2>);
    clink::StateVersionMap expected;
    expected.set(kOp, "fpg.trade", 2);  // empty slot = whole operator
    EXPECT_TRUE(clink::check_fingerprint_compatibility(stored, declared, {}, expected).empty())
        << "stored version defaults to 1, expected 2: op-wide intent covers the slot";
}

TEST(FingerprintCompatCheck, MismatchWithBumpOnDifferentSlotIsReported) {
    clink::StateFingerprintMap stored;
    stored.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeV1>);
    clink::StateFingerprintMap declared;
    declared.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeV2>);
    clink::StateVersionMap expected;
    expected.set(kOp, "fpg.other", 2, "some_other_slot");
    const auto out = clink::check_fingerprint_compatibility(stored, declared, {}, expected);
    EXPECT_EQ(out.size(), 1u) << "a bump scoped to a different slot is not intent for this one";
}

TEST(FingerprintCompatCheck, MismatchWithBumpAlreadyAtStoredVersionIsReported) {
    // The expected version EQUALS the stored one: no migration will run, no
    // stamp will clear - the shape change is undeclared. This is exactly the
    // double-declaration trap the version consultation exists to catch.
    clink::StateFingerprintMap stored;
    stored.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeV1>);
    clink::StateFingerprintMap declared;
    declared.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeReordered>);
    clink::StateVersionMap stored_versions;
    stored_versions.set(kOp, "fpg.trade", 2, kSlot);
    clink::StateVersionMap expected;
    expected.set(kOp, "fpg.trade", 2, kSlot);
    EXPECT_EQ(
        clink::check_fingerprint_compatibility(stored, declared, stored_versions, expected).size(),
        1u);
}

TEST(FingerprintCompatCheck, AbsentStoredGatesNothing) {
    clink::StateFingerprintMap declared;
    declared.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeV1>);
    EXPECT_TRUE(clink::check_fingerprint_compatibility({}, declared, {}, {}).empty());
}

TEST(FingerprintCompatCheck, AbsentDeclaredGatesNothing) {
    clink::StateFingerprintMap stored;
    stored.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeV1>);
    EXPECT_TRUE(clink::check_fingerprint_compatibility(stored, {}, {}, {}).empty())
        << "a job that did not opt in is never blocked; the bind gate stays the backstop";
}

TEST(FingerprintCompatCheck, PackUnpackRoundTripsAndRejectsMalformedLines) {
    std::vector<clink::StateFingerprintMismatch> in;
    in.push_back(clink::StateFingerprintMismatch{
        .op_id = kOp, .slot = kSlot, .stored_fingerprint = 0x1, .declared_fingerprint = 0x2});
    in.push_back(clink::StateFingerprintMismatch{.op_id = OperatorId{9},
                                                 .slot = "other",
                                                 .stored_fingerprint = 0xabcdef0123456789ULL,
                                                 .declared_fingerprint = 0xffffffffffffffffULL});
    const auto packed = clink::pack_fingerprint_mismatches(in);
    const auto out = clink::unpack_fingerprint_mismatches(packed);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0].op_id, kOp);
    EXPECT_EQ(out[0].slot, kSlot);
    EXPECT_EQ(out[0].stored_fingerprint, 0x1u);
    EXPECT_EQ(out[1].declared_fingerprint, 0xffffffffffffffffULL);

    EXPECT_TRUE(clink::unpack_fingerprint_mismatches("").empty());
    EXPECT_THROW((void)clink::unpack_fingerprint_mismatches("7|slot|abc"), std::runtime_error);
    EXPECT_THROW((void)clink::unpack_fingerprint_mismatches("x|slot|1|2"), std::runtime_error);
    EXPECT_THROW((void)clink::unpack_fingerprint_mismatches("7|slot|zz|1"), std::runtime_error);
}

// The parity pin: the pre-deploy rule and the actual bind must agree over the
// same snapshot, in both directions - refusing where the bind refuses, and
// passing where the migration path makes the bind pass.
TEST(FingerprintCompatCheck, PreDeployVerdictMatchesBindGate) {
    const std::string state_type = "fpg.trade.predeploy_parity";

    // Build one snapshot: v1-shaped value, v1 version stamp, v1 fingerprint.
    InMemoryStateBackend writer_backend;
    RuntimeContext write_ctx(kOp, "fpg", &writer_backend, nullptr);
    auto st = bind<FpgTradeV1>(write_ctx);
    st.put(1, FpgTradeV1{.id = 3, .sym = "p"});
    clink::StateVersionMap v1_stamp;
    v1_stamp.set(kOp, state_type, 1, kSlot);
    writer_backend.set_state_versions(v1_stamp);
    const auto snap = writer_backend.snapshot(CheckpointId{1});

    // Quadrant A: undeclared reorder. Pre-deploy reports a mismatch AND the
    // bind refuses.
    {
        clink::StateFingerprintMap declared;
        declared.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeReordered>);
        InMemoryStateBackend restored;
        restored.restore(snap);
        const auto verdict =
            clink::check_fingerprint_compatibility(restored.restored_state_fingerprints(),
                                                   declared,
                                                   restored.restored_state_versions(),
                                                   v1_stamp);
        EXPECT_EQ(verdict.size(), 1u);
        RuntimeContext read_ctx(kOp, "fpg", &restored, nullptr);
        EXPECT_THROW((void)bind<FpgTradeReordered>(read_ctx), std::runtime_error);
    }

    // Quadrant B: declared bump + registered migration. Pre-deploy passes AND
    // the bind (after the migrator ran) passes.
    {
        clink::StateMigrationRegistry::global().register_migration(
            state_type, 1, 2, [](std::span<const std::byte> in) {
                const auto v1 = clink::derived_codec<FpgTradeV1>().decode(in);
                if (!v1.has_value()) {
                    throw std::runtime_error("parity migration: v1 bytes did not decode");
                }
                return clink::derived_codec<FpgTradeV2>().encode(
                    FpgTradeV2{.id = v1->id, .sym = v1->sym, .qty = 0});
            });
        clink::StateFingerprintMap declared;
        declared.set(kOp, kSlot, clink::fields_fingerprint_v<FpgTradeV2>);
        clink::StateVersionMap expected;
        expected.set(kOp, state_type, 2, kSlot);
        InMemoryStateBackend restored;
        restored.restore(snap);
        const auto verdict =
            clink::check_fingerprint_compatibility(restored.restored_state_fingerprints(),
                                                   declared,
                                                   restored.restored_state_versions(),
                                                   expected);
        EXPECT_TRUE(verdict.empty());
        clink::migrate_restored_state(restored, expected);
        RuntimeContext read_ctx(kOp, "fpg", &restored, nullptr);
        auto st2 = bind<FpgTradeV2>(read_ctx);
        const auto got = st2.get(1);
        ASSERT_TRUE(got.has_value());
        EXPECT_EQ(*got, (FpgTradeV2{.id = 3, .sym = "p", .qty = 0}));
    }
}

TEST(StateFingerprintGate, EntriesEnumeratesDeterministically) {
    // entries() feeds whole-map consumers (the sharded redistribute, the
    // pre-deploy check); its order is the same (op, slot) order pack()
    // emits, independent of insertion order.
    clink::StateFingerprintMap a;
    a.set(OperatorId{9}, "zz", 1);
    a.set(OperatorId{7}, "bb", 2);
    a.set(OperatorId{7}, "aa", 3);
    const auto entries = a.entries();
    ASSERT_EQ(entries.size(), 3u);
    EXPECT_EQ(entries[0].op_id, OperatorId{7});
    EXPECT_EQ(entries[0].slot, "aa");
    EXPECT_EQ(entries[0].fingerprint, 3u);
    EXPECT_EQ(entries[1].op_id, OperatorId{7});
    EXPECT_EQ(entries[1].slot, "bb");
    EXPECT_EQ(entries[2].op_id, OperatorId{9});
    EXPECT_EQ(entries[2].slot, "zz");
}

TEST(StateFingerprintGate, PackUnpackRoundTripsAndRejectsMalformedLines) {
    clink::StateFingerprintMap m;
    m.set(kOp, "s1", 0xDEADBEEFCAFE1234ULL);
    m.set(OperatorId{9}, "s2", 1);
    const auto packed = m.pack();
    const auto back = clink::StateFingerprintMap::unpack(packed);
    EXPECT_EQ(back.get(kOp, "s1"), std::optional<std::uint64_t>{0xDEADBEEFCAFE1234ULL});
    EXPECT_EQ(back.get(OperatorId{9}, "s2"), std::optional<std::uint64_t>{1});
    EXPECT_EQ(back.pack(), packed) << "packing must be deterministic";

    for (const auto* bad : {"7|slot",                       // two fields
                            "7|slot|aa|bb",                 // four fields
                            "7|slot|nothex",                // bad hex
                            "x|slot|aa",                    // bad op
                            "7|slot|aaaaaaaaaaaaaaaaa"}) {  // 17 hex digits
        EXPECT_THROW((void)clink::StateFingerprintMap::unpack(bad), std::runtime_error) << bad;
    }
    EXPECT_THROW(m.set(kOp, "a|b", 1), std::invalid_argument);
}

}  // namespace
