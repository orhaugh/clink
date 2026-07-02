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

TEST(CommittingSink, CommitGroupIsObservable) {
    World world;
    FakeCommittingSink sink(&world);
    EXPECT_FALSE(sink.has_commit_group());
    sink.set_commit_group("atomic-group");
    EXPECT_TRUE(sink.has_commit_group());
    EXPECT_EQ(sink.commit_group(), "atomic-group");
}
