// Unit tests for the generic exactly-once sink base, CommittingSink<In, C>.
//
// These exercise the framework choreography (prepare -> persist -> commit /
// abort / recover) directly against an InMemoryStateBackend, with a fake
// connector whose "external system" is an in-memory event log the test
// inspects. No cluster, no real I/O - just the protocol the base owns:
//
//   1. PrepareThenCommit          - barrier persists a handle; commit finalises
//                                    it and clears the state key.
//   2. PrepareThenAbort           - abort rolls back and clears the key.
//   3. CommitIsIdempotent         - a second commit (and an unknown-id commit)
//                                    is a no-op.
//   4. AbortThenCommitIsNoOp      - after abort, commit for the same id no-ops.
//   5. NulloptPrepareIsNoOp       - prepare returning nullopt persists nothing.
//   6. CrashBeforeCommitRecovers  - a fresh sink instance sharing the backend
//                                    finalises a handle left pending by a
//                                    crashed instance, at open().
//   7. OnOpenRunsBeforeRecovery   - resources are initialised before recovery.
//   8. RecoverOverrideIsHonoured  - a custom recover() is used, not commit().
//   9. CodecRoundTripsCrossInstance - serialize on one instance, deserialize on
//                                    another (the producer/consumer are never
//                                    the same object across a crash).
//  10. CommitGroupIsObservable    - the base keeps the Sink commit-group API.

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/committing_sink.hpp"
#include "clink/core/record.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

// A committable is a checkpoint id plus a payload. The codec is a trivial
// "<ckpt>:<payload>" so an asymmetric-instance round-trip is easy to assert.
struct FakeCommittable {
    std::uint64_t ckpt{};
    std::string payload;
};

// Shared, test-owned "world": every commit / abort / open / recover appends an
// event, so the test can assert both content and ORDER.
struct World {
    std::vector<std::string> events;

    std::vector<std::string> with_prefix(std::string_view pfx) const {
        std::vector<std::string> out;
        for (const auto& e : events) {
            if (e.rfind(pfx, 0) == 0)
                out.push_back(e.substr(pfx.size()));
        }
        return out;
    }
    std::vector<std::string> committed() const { return with_prefix("commit:"); }
    std::vector<std::string> aborted() const { return with_prefix("abort:"); }
    std::vector<std::string> recovered() const { return with_prefix("recover:"); }
};

class FakeCommittingSink : public CommittingSink<std::string, FakeCommittable> {
public:
    explicit FakeCommittingSink(World* world, std::uint32_t sub = 0)
        : CommittingSink(sub), world_(world) {}

    // Test knobs.
    bool custom_recover = false;  // override recover() instead of defaulting to commit()

    void on_open() override { world_->events.emplace_back("open"); }

    void write(const Batch<std::string>& batch) override {
        for (const auto& r : batch)
            buffer_.push_back(r.value());
    }

    std::optional<FakeCommittable> prepare_commit(std::uint64_t ckpt) override {
        if (buffer_.empty())
            return std::nullopt;  // nothing to commit this checkpoint
        std::string joined;
        for (const auto& s : buffer_)
            joined += s;
        buffer_.clear();
        return FakeCommittable{ckpt, joined};
    }

    bool commit(const FakeCommittable& c) override {
        world_->events.push_back("commit:" + c.payload);
        return true;
    }

    void abort(const FakeCommittable& c) override {
        world_->events.push_back("abort:" + c.payload);
    }

    void recover(const FakeCommittable& c) override {
        if (custom_recover) {
            world_->events.push_back("recover:" + c.payload);
            return;  // deliberately does NOT commit
        }
        CommittingSink::recover(c);  // default -> commit()
    }

    std::string serialize(const FakeCommittable& c) const override {
        return std::to_string(c.ckpt) + ":" + c.payload;
    }

    FakeCommittable deserialize(std::string_view s) const override {
        const auto pos = s.find(':');
        FakeCommittable c;
        c.ckpt = std::stoull(std::string(s.substr(0, pos)));
        c.payload = std::string(s.substr(pos + 1));
        return c;
    }

    // Expose the protected reconciliation helper for testing.
    std::vector<FakeCommittable> peek_pending() const { return this->pending_committables(); }

private:
    World* world_;
    std::vector<std::string> buffer_;
};

constexpr OperatorId kOp{42};

std::shared_ptr<FakeCommittingSink> make_sink(World& world,
                                              RuntimeContext& rctx,
                                              std::uint32_t sub = 0) {
    auto sink = std::make_shared<FakeCommittingSink>(&world, sub);
    sink->set_id(kOp);
    sink->attach_runtime(&rctx);
    return sink;
}

Batch<std::string> batch_of(const std::vector<std::string>& xs) {
    Batch<std::string> b;
    for (const auto& s : xs)
        b.emplace(s);
    return b;
}

// The logical operator-state key the base persists a handle under.
std::string pending_key(std::uint32_t sub, std::uint64_t ckpt) {
    return "_xo_pending_sub" + std::to_string(sub) + "_" + std::to_string(ckpt);
}

// Read a persisted handle back through the operator-state accessor (which the
// base uses), so the test sees exactly what recovery would.
bool has_pending(InMemoryStateBackend& state, std::uint32_t sub, std::uint64_t ckpt) {
    return state.get_operator_state(kOp, pending_key(sub, ckpt)).has_value();
}

}  // namespace

TEST(CommittingSink, PrepareThenCommit) {
    World world;
    InMemoryStateBackend state;
    RuntimeContext rctx(kOp, "fake", &state, /*metrics=*/nullptr);
    auto sink = make_sink(world, rctx);

    sink->open();
    sink->on_data(batch_of({"a", "b", "c"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    EXPECT_TRUE(has_pending(state, 0, 1)) << "barrier should persist the handle";

    sink->on_commit(1);
    EXPECT_EQ(world.committed(), (std::vector<std::string>{"abc"}));
    EXPECT_FALSE(has_pending(state, 0, 1)) << "commit should clear the state key";
}

TEST(CommittingSink, PrepareThenAbort) {
    World world;
    InMemoryStateBackend state;
    RuntimeContext rctx(kOp, "fake", &state, nullptr);
    auto sink = make_sink(world, rctx);

    sink->open();
    sink->on_data(batch_of({"x", "y"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{2}});
    sink->on_abort(2);

    EXPECT_EQ(world.aborted(), (std::vector<std::string>{"xy"}));
    EXPECT_TRUE(world.committed().empty());
    EXPECT_FALSE(has_pending(state, 0, 2));
}

TEST(CommittingSink, CommitIsIdempotent) {
    World world;
    InMemoryStateBackend state;
    RuntimeContext rctx(kOp, "fake", &state, nullptr);
    auto sink = make_sink(world, rctx);

    sink->open();
    sink->on_data(batch_of({"k"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{5}});
    sink->on_commit(5);
    EXPECT_NO_THROW(sink->on_commit(5));    // second commit: no-op
    EXPECT_NO_THROW(sink->on_commit(999));  // unknown id: no-op
    EXPECT_EQ(world.committed(), (std::vector<std::string>{"k"}))
        << "commit must fire exactly once";
}

TEST(CommittingSink, AbortThenCommitIsNoOp) {
    World world;
    InMemoryStateBackend state;
    RuntimeContext rctx(kOp, "fake", &state, nullptr);
    auto sink = make_sink(world, rctx);

    sink->open();
    sink->on_data(batch_of({"z"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{4}});
    sink->on_abort(4);
    EXPECT_NO_THROW(sink->on_commit(4));  // key already gone
    EXPECT_TRUE(world.committed().empty());
}

TEST(CommittingSink, NulloptPrepareIsNoOp) {
    World world;
    InMemoryStateBackend state;
    RuntimeContext rctx(kOp, "fake", &state, nullptr);
    auto sink = make_sink(world, rctx);

    sink->open();
    // No on_data -> buffer empty -> prepare_commit returns nullopt.
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    EXPECT_FALSE(has_pending(state, 0, 1)) << "nullopt prepare must persist nothing";
    sink->on_commit(1);
    EXPECT_TRUE(world.committed().empty());
}

TEST(CommittingSink, CrashBeforeCommitRecoversAtOpen) {
    // A first sink instance prepares checkpoint 7 but the process dies before
    // on_commit. A fresh instance sharing the same backend + operator id
    // finalises the pending handle during its open() recovery scan.
    World world;
    InMemoryStateBackend state;
    RuntimeContext rctx(kOp, "fake", &state, nullptr);

    {
        auto crashed = make_sink(world, rctx);
        crashed->open();
        crashed->on_data(batch_of({"p", "q"}));
        crashed->on_barrier(CheckpointBarrier{CheckpointId{7}});
        // No on_commit - simulate a crash. Handle is durable in state.
        ASSERT_TRUE(has_pending(state, 0, 7));
    }

    World fresh_world;
    auto restarted = make_sink(fresh_world, rctx);
    restarted->open();  // recover_all_() promotes the pending handle

    EXPECT_EQ(fresh_world.committed(), (std::vector<std::string>{"pq"}))
        << "recovery should finalise the pending handle";
    EXPECT_FALSE(has_pending(state, 0, 7)) << "recovery should clear the key";
}

TEST(CommittingSink, OnOpenRunsBeforeRecovery) {
    // Seed a pending handle, then open a fresh sink. The "open" event must
    // precede the recovery "commit" - resources are initialised first.
    World seed_world;
    InMemoryStateBackend state;
    RuntimeContext rctx(kOp, "fake", &state, nullptr);
    {
        auto seeder = make_sink(seed_world, rctx);
        seeder->open();
        seeder->on_data(batch_of({"r"}));
        seeder->on_barrier(CheckpointBarrier{CheckpointId{8}});
    }

    World world;
    auto sink = make_sink(world, rctx);
    sink->open();

    ASSERT_EQ(world.events.size(), 2u);
    EXPECT_EQ(world.events[0], "open");
    EXPECT_EQ(world.events[1], "commit:r");
}

TEST(CommittingSink, RecoverOverrideIsHonoured) {
    World seed_world;
    InMemoryStateBackend state;
    RuntimeContext rctx(kOp, "fake", &state, nullptr);
    {
        auto seeder = make_sink(seed_world, rctx);
        seeder->open();
        seeder->on_data(batch_of({"m"}));
        seeder->on_barrier(CheckpointBarrier{CheckpointId{9}});
    }

    World world;
    auto sink = make_sink(world, rctx);
    sink->custom_recover = true;
    sink->open();

    EXPECT_EQ(world.recovered(), (std::vector<std::string>{"m"}));
    EXPECT_TRUE(world.committed().empty()) << "custom recover must not fall back to commit";
    EXPECT_FALSE(has_pending(state, 0, 9)) << "recovery still clears the key";
}

TEST(CommittingSink, CodecRoundTripsCrossInstance) {
    // The producer and the recoverer are never the same object, so the codec
    // must not depend on instance state. Serialize on one, deserialize on
    // another, and confirm the fields survive.
    World w1, w2;
    FakeCommittingSink producer(&w1);
    FakeCommittingSink consumer(&w2);

    const FakeCommittable original{123, "hello:world"};  // payload contains the delimiter
    const std::string blob = producer.serialize(original);
    const FakeCommittable back = consumer.deserialize(blob);

    EXPECT_EQ(back.ckpt, original.ckpt);
    EXPECT_EQ(back.payload, original.payload);
}

TEST(CommittingSink, PendingCommittablesReflectsPreparedSet) {
    // pending_committables() returns the persisted-but-unfinalised handles, for a
    // connector reconciling an external registry at open. Empty initially; holds
    // the prepared handle after a barrier; empty again after commit.
    World world;
    InMemoryStateBackend state;
    RuntimeContext rctx(kOp, "fake", &state, nullptr);
    auto sink = make_sink(world, rctx);

    sink->open();
    EXPECT_TRUE(sink->peek_pending().empty());

    sink->on_data(batch_of({"a", "b"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->on_data(batch_of({"c"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{2}});

    auto pending = sink->peek_pending();
    std::vector<std::string> payloads;
    for (const auto& c : pending)
        payloads.push_back(c.payload);
    std::sort(payloads.begin(), payloads.end());
    EXPECT_EQ(payloads, (std::vector<std::string>{"ab", "c"}));

    sink->on_commit(1);
    sink->on_commit(2);
    EXPECT_TRUE(sink->peek_pending().empty());
}

TEST(CommittingSink, CommitGroupIsObservable) {
    World world;
    FakeCommittingSink sink(&world);
    EXPECT_FALSE(sink.has_commit_group());
    sink.set_commit_group("atomic-group");
    EXPECT_TRUE(sink.has_commit_group());
    EXPECT_EQ(sink.commit_group(), "atomic-group");
}

// --- Rescale: every restored handle has exactly one owner ---------------------

namespace {

// Stage one handle per old subtask, payload "p<N>", into `state` under each old
// subtask's own key, the way a backend that hands every new subtask every old
// subtask's operator state restores them.
void stage_old_handles(World& world, InMemoryStateBackend& state, std::uint32_t old_parallelism) {
    for (std::uint32_t old = 0; old < old_parallelism; ++old) {
        RuntimeContext rctx(kOp, "fake", &state, nullptr);
        auto sink = make_sink(world, rctx, old);
        sink->on_data(batch_of({"p" + std::to_string(old)}));
        sink->on_barrier(CheckpointBarrier{CheckpointId{7}});
    }
}

std::size_t pending_keys(const InMemoryStateBackend& state) {
    std::size_t n = 0;
    state.scan_operator_state(kOp, [&](StateBackend::KeyView k, StateBackend::ValueView) {
        if (std::string_view{k}.rfind("_xo_pending_", 0) == 0)
            ++n;
    });
    return n;
}

// Open new subtask `sub` over its own copy of every old handle, succeeding the
// old subtasks [first, first + count).
void open_successor(World& world,
                    std::uint32_t old_parallelism,
                    std::uint32_t sub,
                    std::uint32_t first,
                    std::uint32_t count) {
    InMemoryStateBackend state;
    World staging;
    stage_old_handles(staging, state, old_parallelism);
    RuntimeContext rctx(kOp, "fake", &state, nullptr);
    RestoreSuccession succession;
    succession.first = first;
    succession.count = count;
    rctx.set_restore_succession(succession);
    auto sink = make_sink(world, rctx, sub);
    sink->open();
    EXPECT_EQ(pending_keys(state), 0U)
        << "new subtask " << sub << " kept a handle it neither finalised nor erased";
}

std::vector<std::string> sorted(std::vector<std::string> v) {
    std::sort(v.begin(), v.end());
    return v;
}

}  // namespace

TEST(CommittingSink, AScaleDownCommitsEveryOldSubtasksHandleExactlyOnce) {
    // 4 -> 2: new subtask 0 succeeds old 0 and 1, new subtask 1 old 2 and 3.
    // Each sees all four handles, yet each handle commits once, by its owner;
    // keyed by own index alone, old 2 and 3 were never committed and new 1
    // committed old 1's handle as its own.
    World world;
    open_successor(world, 4, 0, 0, 2);
    open_successor(world, 4, 1, 2, 2);
    EXPECT_EQ(sorted(world.committed()), (std::vector<std::string>{"p0", "p1", "p2", "p3"}));
}

TEST(CommittingSink, AScaleUpCommitsEachParentsHandleOnlyThroughItsFirstChild) {
    // 2 -> 4: children 0 and 1 share parent 0, children 2 and 3 parent 1. Only
    // the first child of each parent succeeds it.
    World world;
    open_successor(world, 2, 0, 0, 1);
    open_successor(world, 2, 1, 0, 0);
    open_successor(world, 2, 2, 1, 1);
    open_successor(world, 2, 3, 1, 0);
    EXPECT_EQ(sorted(world.committed()), (std::vector<std::string>{"p0", "p1"}));
}

TEST(CommittingSink, PendingCommittablesAreTheOnesThisSubtaskSucceeds) {
    // A connector reconciling an external registry at open (Postgres rolls back
    // what is not in this set) must see the handles it owns, not its own index's.
    World world;
    InMemoryStateBackend state;
    stage_old_handles(world, state, 4);
    RuntimeContext rctx(kOp, "fake", &state, nullptr);
    RestoreSuccession succession;
    succession.first = 2;
    succession.count = 2;
    rctx.set_restore_succession(succession);
    auto sink = make_sink(world, rctx, 1);
    std::vector<std::string> payloads;
    for (const auto& c : sink->peek_pending())
        payloads.push_back(c.payload);
    EXPECT_EQ(sorted(payloads), (std::vector<std::string>{"p2", "p3"}));
}

TEST(CommittingSink, WithoutASuccessionASiblingsHandlesAreLeftAlone) {
    // In-process subtasks may share one backend, so a handle under another
    // index is a live sibling's, not a restored copy: recovery finalises only
    // its own and erases nothing else.
    World world;
    InMemoryStateBackend state;
    stage_old_handles(world, state, 2);
    RuntimeContext rctx(kOp, "fake", &state, nullptr);
    auto sink = make_sink(world, rctx, 0);
    sink->open();
    EXPECT_EQ(world.committed(), (std::vector<std::string>{"p0"}));
    EXPECT_TRUE(state.get_operator_state(kOp, "_xo_pending_sub1_7").has_value());
}
