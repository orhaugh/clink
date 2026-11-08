// Source::barrier_ready: a source whose records come in units no checkpoint
// may split (a database transaction, an initial snapshot) holds the barriers
// it is given until the unit ends.
//
// The scripted source emits records in units of three and is ready only
// between units. The test injects a barrier while a unit is half emitted;
// the barrier must reach the sink after the unit's last record, never inside
// it, and the offset the source snapshots at that barrier must cover the
// whole unit. A graceful stop requested inside a unit finishes the unit
// before the end-of-input tail. Each of the two source runners (single and
// parallel) has its own copy of the drain, so each gets its own test.
//
// Everything is scripted with latches; a step happens when its gate opens,
// never after a sleep.

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/network/local_data_plane.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

struct UnitGate {
    std::mutex mu;
    std::condition_variable cv;
    bool open{false};

    void signal() {
        {
            std::lock_guard lock(mu);
            open = true;
        }
        cv.notify_all();
    }
    [[nodiscard]] bool await(std::chrono::seconds bound = std::chrono::seconds{30}) {
        std::unique_lock lock(mu);
        return cv.wait_for(lock, bound, [&] { return open; });
    }
};

// Emits records in units of three; ready only when no unit is open. After its
// first record (a unit open) it parks once: it signals `mid_unit` and waits
// for `resume`, which the test opens after injecting a barrier. With a
// `limit`, it ends cleanly after that many records; without one, it runs until
// stopped or cancelled.
class UnitScriptedSource final : public Source<int> {
public:
    UnitScriptedSource(UnitGate& mid_unit, UnitGate& resume, int limit)
        : mid_unit_(mid_unit), resume_(resume), limit_(limit) {}

    bool produce(Emitter<int>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (emitted_ == 1 && !parked_) {
            parked_ = true;
            mid_unit_.signal();
            (void)resume_.await();
            return true;
        }
        if (limit_ > 0 && emitted_ >= limit_) {
            return false;
        }
        Batch<int> b;
        b.emplace(emitted_++);
        out.emit_data(std::move(b));
        return true;
    }

    [[nodiscard]] bool barrier_ready() const override { return emitted_ % 3 == 0; }

    void snapshot_offset(StateBackend& /*backend*/, OperatorId /*op*/, CheckpointId ckpt) override {
        std::lock_guard lock(mu_);
        offset_at_snapshot_[ckpt.value()] = emitted_;
    }

    [[nodiscard]] bool is_bounded() const noexcept override { return limit_ > 0; }

    [[nodiscard]] int offset_at(std::uint64_t ckpt) const {
        std::lock_guard lock(mu_);
        auto it = offset_at_snapshot_.find(ckpt);
        return it == offset_at_snapshot_.end() ? -1 : it->second;
    }

    std::string name() const override { return name_; }
    void set_name(std::string n) { name_ = std::move(n); }

private:
    UnitGate& mid_unit_;
    UnitGate& resume_;
    int limit_;
    std::atomic<int> emitted_{0};
    bool parked_{false};
    mutable std::mutex mu_;
    std::map<std::uint64_t, int> offset_at_snapshot_;
    std::string name_{"barrier_ready.source"};
};

class UnitCaptureSink final : public Sink<int> {
public:
    struct Event {
        bool barrier{false};
        bool terminal{false};
        std::int64_t value{0};
    };

    void on_data(const Batch<int>& batch) override {
        std::lock_guard lock(mu_);
        for (const auto& rec : batch) {
            events_.push_back({false, false, rec.value()});
            if (rec.value() == awaited_record_.load()) {
                record_seen_.signal();
            }
        }
    }
    void on_barrier(CheckpointBarrier b) override {
        std::lock_guard lock(mu_);
        events_.push_back({true, b.is_terminal(), static_cast<std::int64_t>(b.id().value())});
    }

    void await_record(int v) { awaited_record_.store(v); }
    [[nodiscard]] bool record_arrived() { return record_seen_.await(); }

    [[nodiscard]] std::vector<Event> events() const {
        std::lock_guard lock(mu_);
        return events_;
    }

    std::string name() const override { return name_; }
    void set_name(std::string n) { name_ = std::move(n); }

private:
    mutable std::mutex mu_;
    std::vector<Event> events_;
    std::atomic<int> awaited_record_{-1};
    UnitGate record_seen_;
    std::string name_{"barrier_ready.sink"};
};

std::string barrier_ready_tag() {
    static std::atomic<unsigned> seq{0};
    return "run" + std::to_string(seq.fetch_add(1));
}

// Where barrier `id` stands in the sink's stream: the number of records
// before it, or -1 when it never arrived.
int records_before_barrier(const std::vector<UnitCaptureSink::Event>& events, std::int64_t id) {
    int records = 0;
    for (const auto& e : events) {
        if (e.barrier && !e.terminal && e.value == id) {
            return records;
        }
        if (!e.barrier) {
            ++records;
        }
    }
    return -1;
}

}  // namespace

TEST(SourceBarrierReady, ABarrierWaitsForTheEndOfTheUnitInTheSingleRunner) {
    network::LocalDataPlane::instance().clear_for_testing();
    UnitGate mid_unit;
    UnitGate resume;
    const auto tag = barrier_ready_tag();
    auto src = std::make_shared<UnitScriptedSource>(mid_unit, resume, 6);
    src->set_name("barrier_ready.source." + tag);
    auto sink = std::make_shared<UnitCaptureSink>();
    sink->set_name("barrier_ready.sink." + tag);

    Dag dag;
    auto h0 = dag.add_source<int>(src);
    dag.add_sink<int>(h0, sink);
    auto injectors = dag.source_injectors();
    ASSERT_EQ(injectors.size(), 1u);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), cfg);
    exec.start();

    // One record of the first unit is out; the barrier arrives inside the unit.
    ASSERT_TRUE(mid_unit.await());
    injectors.front()(CheckpointBarrier{CheckpointId{1}});
    resume.signal();
    exec.await_termination();

    const auto events = sink->events();
    EXPECT_EQ(records_before_barrier(events, 1), 3)
        << "the barrier cut the unit: it must follow the unit's third record, not precede it";
    EXPECT_EQ(src->offset_at(1), 3)
        << "the offset snapshotted at the barrier must cover the whole unit";
    int records = 0;
    for (const auto& e : events) {
        records += e.barrier ? 0 : 1;
    }
    EXPECT_EQ(records, 6);
}

TEST(SourceBarrierReady, TheParallelRunnerHoldsTheBarrierTheSameWay) {
    network::LocalDataPlane::instance().clear_for_testing();
    UnitGate mid_unit;
    UnitGate resume;
    const auto tag = barrier_ready_tag();
    auto src = std::make_shared<UnitScriptedSource>(mid_unit, resume, 6);
    src->set_name("barrier_ready.psource." + tag);
    auto sink = std::make_shared<UnitCaptureSink>();
    sink->set_name("barrier_ready.psink." + tag);

    Dag dag;
    auto h0 = dag.add_parallel_source<int>([&](std::size_t) { return src; }, 1);
    dag.add_parallel_sink<int>(h0, [&](std::size_t) { return sink; }, 1);
    auto injectors = dag.source_injectors();
    ASSERT_EQ(injectors.size(), 1u);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), cfg);
    exec.start();

    ASSERT_TRUE(mid_unit.await());
    injectors.front()(CheckpointBarrier{CheckpointId{1}});
    resume.signal();
    exec.await_termination();

    EXPECT_EQ(records_before_barrier(sink->events(), 1), 3)
        << "the parallel runner's copy of the drain let the barrier cut the unit";
    EXPECT_EQ(src->offset_at(1), 3);
}

// A graceful stop asked for inside a unit: the runner keeps calling produce()
// until the unit ends, then runs the end-of-input tail, so the final
// checkpoint (here the in-process terminal barrier) follows the whole unit.
TEST(SourceBarrierReady, AGracefulStopFinishesTheUnitBeforeTheFinalCheckpoint) {
    network::LocalDataPlane::instance().clear_for_testing();
    UnitGate mid_unit;
    UnitGate resume;
    const auto tag = barrier_ready_tag();
    auto src = std::make_shared<UnitScriptedSource>(mid_unit, resume, 0);
    src->set_name("barrier_ready.stop_source." + tag);
    auto sink = std::make_shared<UnitCaptureSink>();
    sink->set_name("barrier_ready.stop_sink." + tag);

    Dag dag;
    auto h0 = dag.add_source<int>(src);
    dag.add_sink<int>(h0, sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    cfg.stop_requested = std::make_shared<std::atomic<bool>>(false);
    LocalExecutor exec(std::move(dag), cfg);
    exec.start();

    ASSERT_TRUE(mid_unit.await());
    cfg.stop_requested->store(true);
    resume.signal();
    exec.await_termination();

    const auto events = sink->events();
    std::vector<std::int64_t> records;
    bool terminal = false;
    for (const auto& e : events) {
        if (!e.barrier) {
            EXPECT_FALSE(terminal) << "a record followed the final barrier";
            records.push_back(e.value);
        } else if (e.terminal) {
            terminal = true;
        }
    }
    ASSERT_TRUE(terminal) << "the graceful stop never reached its end-of-input tail";
    ASSERT_FALSE(records.empty());
    EXPECT_EQ(records.size() % 3, 0u)
        << "the stop ended the stream inside a unit: " << records.size() << " records";
    EXPECT_EQ(records.size(), 3u) << "the stop let the source run past the unit it was in";
}
