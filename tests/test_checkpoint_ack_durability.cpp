// The ack-after-durable invariant, stated directly at the runner contract.
//
// A SubtaskCheckpointed ack is the coordinator's only evidence that a
// subtask's state for checkpoint N is durable: the pending set drains on
// acks, COMPLETED-N publishes when it empties, and a restore trusts any
// published checkpoint. So an ok-ack for N must imply N's snapshot file is
// already on disk - an ack that precedes the persist opens a window where a
// SIGKILL leaves a COMPLETED marker naming a participant whose snapshot does
// not exist. That is not hypothetical: a worker killed in that window left
// checkpoint-5.snap.part.4 (a partial temp file, never renamed) next to a
// COMPLETED-5 marker, and the restore refused - production-hardening item 51,
// reproduced by the Kafka exactly-once suite.
//
// The defect this pins: the SOURCE runner acked after snapshot_offset (an
// in-memory put) plus barrier emission, with no capture or persist call at
// all - durability was delegated to the subtask's terminal runner while the
// ack was not.

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_coordinator.hpp"
#include "clink/core/types.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/file_backed_state_backend.hpp"

namespace {

using namespace clink;
using namespace std::chrono_literals;

// Bounded ticking source with a checkpointable offset, so the persisted
// snapshot carries real source state - the thing a restore actually needs.
class ClinkAckDurabilityTickSource final : public Source<int> {
public:
    explicit ClinkAckDurabilityTickSource(int total) : total_(total) {}

    bool produce(Emitter<int>& out) override {
        if (counter_ >= total_) {
            return false;
        }
        Batch<int> b;
        b.emplace(counter_++);
        if (!out.emit_data(std::move(b))) {
            return false;
        }
        std::this_thread::sleep_for(1ms);
        return counter_ < total_;
    }

    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId /*ckpt*/) override {
        const std::string v = std::to_string(counter_);
        backend.put_operator_state(
            op_id, StateBackend::KeyView{"offset", 6}, StateBackend::ValueView{v.data(), v.size()});
    }

    std::string name() const override { return "ack_durability_tick_source"; }

private:
    int total_;
    int counter_{0};
};

class ClinkAckDurabilityNullSink final : public Sink<int> {
public:
    void on_data(const Batch<int>&) override {}
    std::string name() const override { return "ack_durability_null_sink"; }
};

}  // namespace

TEST(CheckpointAckDurability, AnOkAckImpliesTheCheckpointSnapshotIsAlreadyOnDisk) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ack_durability_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    auto backend = std::make_shared<FileBackedStateBackend>(dir);

    Dag dag;
    auto src = std::make_shared<ClinkAckDurabilityTickSource>(120);
    auto h0 = dag.add_source<int>(src);
    dag.add_sink<int>(h0, std::make_shared<ClinkAckDurabilityNullSink>());

    CheckpointCoordinator::Config ccfg;
    ccfg.interval = 10ms;
    CheckpointCoordinator coord(backend, ccfg);
    for (const auto& r : dag.runners()) {
        coord.register_operator(r.id);
    }
    coord.set_source_injectors(dag.source_injectors());

    // The probe: at every ok-ack, the acked checkpoint's snapshot file must
    // already exist. Recorded, not asserted inline - the callback runs on
    // runner threads.
    struct Violation {
        std::uint64_t ckpt;
    };
    std::mutex mu;
    std::vector<Violation> violations;
    std::atomic<int> ok_acks{0};

    JobConfig cfg;
    cfg.state_backend = backend;
    cfg.on_checkpoint_ack = [&](CheckpointId id, bool ok, std::string /*error*/) {
        if (!ok) {
            return;
        }
        ok_acks.fetch_add(1, std::memory_order_relaxed);
        const auto snap = dir / ("checkpoint-" + std::to_string(id.value()) + ".snap");
        if (!std::filesystem::exists(snap)) {
            std::lock_guard lk(mu);
            violations.push_back({id.value()});
        }
    };

    LocalExecutor exec(std::move(dag), cfg);
    exec.start();
    coord.start_periodic_trigger();
    exec.await_termination();
    coord.stop_periodic_trigger();

    // Vacuity guard: the run must have acked checkpoints at all.
    ASSERT_GT(ok_acks.load(), 0) << "no checkpoint was ever acked; the probe never ran";

    std::lock_guard lk(mu);
    std::string detail;
    for (const auto& v : violations) {
        detail += " ckpt-" + std::to_string(v.ckpt);
    }
    EXPECT_TRUE(violations.empty())
        << violations.size()
        << " ok-ack(s) fired before the acked checkpoint's snapshot existed on disk:" << detail
        << ". An ack is the coordinator's evidence of durability; a kill inside this window "
           "publishes a COMPLETED marker for a checkpoint whose participant snapshot is not "
           "there.";

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}
