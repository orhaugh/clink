// The backend expiry-compaction hook.
//
// TTL'd state has to be physically reclaimed, not merely hidden. The
// portable route is KeyedState::cleanup_batch, which scans. On an LSM
// backend that is the wrong shape twice: the scan competes with the write
// path, and the backend is ALREADY rewriting every live SST during
// compaction, so it can drop expired entries for free while it is there.
//
// These tests cover the seam itself with a fake backend (so the contract
// is testable without RocksDB), and the fallback behaviour that keeps
// backends without the hook correct. The RocksDB implementation has its
// own test beside the backend, where a real compaction can be forced.

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"

namespace {

using namespace clink;
using namespace std::chrono_literals;

constexpr OperatorId kOp{41};

// An in-memory backend that ALSO implements the compaction hook, so the
// seam can be exercised without an LSM. compact_expired() applies the
// installed predicate to everything and reports the count - the shape a
// backend that can count its drops would use.
// InMemoryStateBackend is final, so the fake DELEGATES rather than
// derives - which is arguably the more honest shape anyway: it proves the
// hook is usable by any backend, not only one in the existing hierarchy.
class CompactingBackend final : public StateBackend {
public:
    void put(OperatorId op, KeyView key, ValueView value) override { inner_.put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return inner_.get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { inner_.erase(op, key); }
    void scan(OperatorId op, const ScanVisitor& visit) const override { inner_.scan(op, visit); }
    Snapshot snapshot(CheckpointId id) override { return inner_.snapshot(id); }
    void restore(const Snapshot& s, const KeyGroupRange& kg = {}) override {
        inner_.restore(s, kg);
    }
    std::string description() const override { return "compacting test backend"; }

    [[nodiscard]] bool supports_expiry_compaction() const noexcept override { return true; }

    void set_expiry_filter(ExpiryPredicate pred) override {
        pred_ = std::move(pred);
        ++installs_;
    }

    std::optional<std::size_t> compact_expired(OperatorId op) override {
        ++compactions_;
        if (!pred_) {
            return std::size_t{0};
        }
        std::vector<std::string> doomed;
        inner_.scan(op, [&](KeyView k, ValueView v) {
            if (pred_(op, k, v)) {
                doomed.emplace_back(k);
            }
        });
        for (const auto& k : doomed) {
            inner_.erase(op, k);
        }
        return doomed.size();
    }

    [[nodiscard]] int installs() const noexcept { return installs_; }
    [[nodiscard]] int compactions() const noexcept { return compactions_; }

private:
    InMemoryStateBackend inner_;
    ExpiryPredicate pred_;
    int installs_{0};
    int compactions_{0};
};

TtlConfig event_ttl(std::chrono::milliseconds ttl) {
    return TtlConfig{.ttl = ttl, .refresh_on_write = true, .domain = TtlTimeDomain::EventTime};
}

std::size_t resident(StateBackend& b) {
    std::size_t n = 0;
    b.scan(kOp, [&](std::string_view, std::string_view) { ++n; });
    return n;
}

// --- the default -------------------------------------------------------------

TEST(ExpiryCompaction, ABackendWithoutTheHookReportsSoAndIsUnaffected) {
    InMemoryStateBackend b;
    EXPECT_FALSE(b.supports_expiry_compaction());
    EXPECT_FALSE(b.compact_expired(kOp).has_value());
    // Installing a filter on a backend that has none must be a harmless
    // no-op, so a caller can install unconditionally.
    EXPECT_NO_THROW(
        b.set_expiry_filter([](OperatorId, std::string_view, std::string_view) { return true; }));
}

TEST(ExpiryCompaction, WithoutTheHookCleanupStillScansAndReleases) {
    // The portable fallback must keep working: a backend with no
    // compaction path is correct, just slower to give memory back.
    InMemoryStateBackend b;
    KeyedState<std::int64_t, std::string> s(
        b, kOp, "slot", int64_codec(), string_codec(), event_ttl(10ms));
    s.advance_watermark(1'000);
    for (int i = 0; i < 20; ++i) {
        s.put(i, "v");
    }
    s.advance_watermark(100'000);
    ASSERT_EQ(resident(b), 20U);
    s.cleanup_all();
    EXPECT_EQ(resident(b), 0U);
}

// --- the hook ----------------------------------------------------------------

TEST(ExpiryCompaction, CleanupDelegatesToTheBackendWhenItCan) {
    CompactingBackend b;
    KeyedState<std::int64_t, std::string> s(
        b, kOp, "slot", int64_codec(), string_codec(), event_ttl(1000ms));
    s.advance_watermark(10'000);
    for (int i = 0; i < 15; ++i) {
        s.put(i, "v");
    }
    s.advance_watermark(20'000);

    const auto reclaimed = s.cleanup_batch();
    EXPECT_EQ(b.compactions(), 1) << "cleanup did not route through the backend hook";
    EXPECT_EQ(reclaimed, 15U);
    EXPECT_EQ(resident(b), 0U);
}

TEST(ExpiryCompaction, TheFilterIsInstalledOncePerSlotNotPerSweep) {
    CompactingBackend b;
    KeyedState<std::int64_t, std::string> s(
        b, kOp, "slot", int64_codec(), string_codec(), event_ttl(1000ms));
    s.advance_watermark(10'000);
    s.put(1, "v");
    s.advance_watermark(20'000);
    s.cleanup_batch();
    s.cleanup_batch();
    s.cleanup_batch();
    EXPECT_EQ(b.installs(), 1) << "the predicate was reinstalled on every sweep";
}

TEST(ExpiryCompaction, TheFilterKeepsLiveEntries) {
    CompactingBackend b;
    KeyedState<std::int64_t, std::string> s(
        b, kOp, "slot", int64_codec(), string_codec(), event_ttl(1000ms));
    s.advance_watermark(10'000);
    s.put(1, "old");
    s.advance_watermark(10'500);
    s.put(2, "new");
    s.advance_watermark(11'200);  // key 1 dead, key 2 live

    const auto reclaimed = s.cleanup_batch();
    EXPECT_EQ(reclaimed, 1U) << "the filter dropped a live entry, or missed a dead one";
    EXPECT_FALSE(s.get(1).has_value());
    EXPECT_EQ(s.get(2).value_or(""), "new");
}

TEST(ExpiryCompaction, TheFilterIgnoresOtherOperatorsAndOtherSlots) {
    // The filter is installed on the BACKEND, which may hold state for
    // several slots and (for a shared backend) several operators. A slot
    // that is not TTL-stamped must not have its first eight bytes read as
    // a deadline - that would drop live state belonging to someone else.
    CompactingBackend b;
    KeyedState<std::int64_t, std::string> ttl_slot(
        b, kOp, "ttl", int64_codec(), string_codec(), event_ttl(1000ms));
    KeyedState<std::int64_t, std::string> plain_slot(
        b, kOp, "plain", int64_codec(), string_codec());

    ttl_slot.advance_watermark(10'000);
    ttl_slot.put(1, "doomed");
    // A value long enough to look stamped, in a slot with no TTL.
    plain_slot.put(1, "aaaaaaaaaaaaaaaaaaaa");

    ttl_slot.advance_watermark(20'000);
    ttl_slot.cleanup_batch();

    EXPECT_FALSE(ttl_slot.get(1).has_value()) << "the TTL slot's entry was not reclaimed";
    EXPECT_EQ(plain_slot.get(1).value_or(""), "aaaaaaaaaaaaaaaaaaaa")
        << "the filter reached into a slot with no TTL and dropped live state";
}

TEST(ExpiryCompaction, NoCompactionRunsBeforeEventTimeExists) {
    CompactingBackend b;
    KeyedState<std::int64_t, std::string> s(
        b, kOp, "slot", int64_codec(), string_codec(), event_ttl(1ms));
    s.put(1, "v");
    EXPECT_EQ(s.cleanup_batch(), 0U);
    EXPECT_EQ(b.compactions(), 0) << "a compaction ran against a clock that had not started";
    EXPECT_EQ(resident(b), 1U);
}

TEST(ExpiryCompaction, ASlotWithNoTtlNeitherInstallsNorCompacts) {
    CompactingBackend b;
    KeyedState<std::int64_t, std::string> s(b, kOp, "slot", int64_codec(), string_codec());
    s.put(1, "v");
    EXPECT_EQ(s.cleanup_batch(), 0U);
    EXPECT_EQ(b.installs(), 0);
    EXPECT_EQ(b.compactions(), 0);
    EXPECT_EQ(resident(b), 1U);
}

TEST(ExpiryCompaction, ProcessingTimeFilterUsesTheWallClock) {
    CompactingBackend b;
    KeyedState<std::int64_t, std::string> s(
        b, kOp, "slot", int64_codec(), string_codec(), TtlConfig{.ttl = 10ms});
    s.put(1, "v");
    std::this_thread::sleep_for(40ms);
    EXPECT_EQ(s.cleanup_batch(), 1U);
    EXPECT_EQ(resident(b), 0U);
}

}  // namespace
