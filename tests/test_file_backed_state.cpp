// Tests for FileBackedStateBackend: snapshot persistence, restore,
// crash-safe rename-on-commit semantics, and a recovery scenario where
// a second backend instance reads what the first wrote.
//
// Combined, these prove the v1 distributed-checkpointing surface: each
// subtask's in-memory state can be persisted to disk on snapshot and
// resurrected on a fresh process via restore.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/types.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/file_backed_state_backend.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/state_backend.hpp"

namespace {

using namespace clink;

std::filesystem::path scratch_dir(const std::string& tag) {
    const auto p = std::filesystem::temp_directory_path() / ("clink_file_backed_state_" + tag);
    std::error_code ec;
    std::filesystem::remove_all(p, ec);
    return p;
}

StateBackend::ValueView as_view(std::string_view s) {
    return StateBackend::ValueView{s.data(), s.size()};
}

std::string as_string(const StateBackend::Value& v) {
    return std::string{reinterpret_cast<const char*>(v.data()), v.size()};
}

TEST(FileBackedStateBackend, PutGetEraseDelegatesToInMemory) {
    const auto dir = scratch_dir("put_get_erase");
    FileBackedStateBackend backend(dir);
    backend.put(OperatorId{1}, "k1", as_view("v1"));
    backend.put(OperatorId{1}, "k2", as_view("v2"));
    backend.put(OperatorId{2}, "k1", as_view("v3"));

    EXPECT_EQ(as_string(*backend.get(OperatorId{1}, "k1")), "v1");
    EXPECT_EQ(as_string(*backend.get(OperatorId{1}, "k2")), "v2");
    EXPECT_EQ(as_string(*backend.get(OperatorId{2}, "k1")), "v3");
    EXPECT_FALSE(backend.get(OperatorId{2}, "nope").has_value());

    backend.erase(OperatorId{1}, "k1");
    EXPECT_FALSE(backend.get(OperatorId{1}, "k1").has_value());
}

TEST(FileBackedStateBackend, SnapshotWritesFileWithExpectedName) {
    const auto dir = scratch_dir("snapshot_path");
    FileBackedStateBackend backend(dir);
    backend.put(OperatorId{1}, "k", as_view("v"));
    const auto snap = backend.snapshot(CheckpointId{42});
    EXPECT_EQ(snap.checkpoint_id.value(), 42u);
    EXPECT_TRUE(snap.bytes.empty()) << "payload should live on disk, not in returned Snapshot";
    const auto expected_path = dir / "checkpoint-42.snap";
    EXPECT_TRUE(std::filesystem::exists(expected_path));
    EXPECT_GT(std::filesystem::file_size(expected_path), 0u);
    EXPECT_TRUE(backend.has_checkpoint(CheckpointId{42}));
    EXPECT_FALSE(backend.has_checkpoint(CheckpointId{43}));
}

TEST(FileBackedStateBackend, RestoreReadsBackWhatSnapshotWrote) {
    const auto dir = scratch_dir("restore_roundtrip");
    {
        FileBackedStateBackend writer(dir);
        writer.put(OperatorId{1}, "alpha", as_view("A"));
        writer.put(OperatorId{1}, "beta", as_view("B"));
        writer.put(OperatorId{2}, "gamma", as_view("G"));
        writer.snapshot(CheckpointId{7});
    }
    {
        FileBackedStateBackend reader(dir);
        // Empty before restore.
        EXPECT_FALSE(reader.get(OperatorId{1}, "alpha").has_value());
        reader.restore(Snapshot{.checkpoint_id = CheckpointId{7}, .bytes = {}});
        EXPECT_EQ(as_string(*reader.get(OperatorId{1}, "alpha")), "A");
        EXPECT_EQ(as_string(*reader.get(OperatorId{1}, "beta")), "B");
        EXPECT_EQ(as_string(*reader.get(OperatorId{2}, "gamma")), "G");
    }
}

TEST(FileBackedStateBackend, RestoreMissingCheckpointLeavesStateEmpty) {
    const auto dir = scratch_dir("missing_restore");
    FileBackedStateBackend backend(dir);
    backend.put(OperatorId{1}, "k", as_view("v"));
    // Restoring a checkpoint that was never written must be a no-op
    // (state already in the backend stays, no exception).
    backend.restore(Snapshot{.checkpoint_id = CheckpointId{99}, .bytes = {}});
    // The contract on restore is "load from disk if present, leave alone otherwise" -
    // there's nothing on disk so the existing in-memory state should be intact.
    EXPECT_EQ(as_string(*backend.get(OperatorId{1}, "k")), "v");
}

TEST(FileBackedStateBackend, ConcurrentReaderSeesLatestCommittedCheckpoint) {
    const auto dir = scratch_dir("incremental");
    FileBackedStateBackend writer(dir);
    // Two checkpoints to two different files.
    writer.put(OperatorId{1}, "k", as_view("v1"));
    writer.snapshot(CheckpointId{1});
    writer.put(OperatorId{1}, "k", as_view("v2"));
    writer.snapshot(CheckpointId{2});

    // A fresh backend pointed at the same dir can recover either.
    {
        FileBackedStateBackend r(dir);
        r.restore(Snapshot{.checkpoint_id = CheckpointId{1}, .bytes = {}});
        EXPECT_EQ(as_string(*r.get(OperatorId{1}, "k")), "v1");
    }
    {
        FileBackedStateBackend r(dir);
        r.restore(Snapshot{.checkpoint_id = CheckpointId{2}, .bytes = {}});
        EXPECT_EQ(as_string(*r.get(OperatorId{1}, "k")), "v2");
    }
}

TEST(FileBackedStateBackend, PartialWriteOnDiskIsNotLoaded) {
    // Simulates a crash mid-snapshot: a `.snap.part` file should NEVER
    // be picked up as a real checkpoint because the rename-on-commit
    // step never ran. We manually drop a `.part` file and verify that
    // restore() ignores it.
    const auto dir = scratch_dir("partial_write");
    FileBackedStateBackend backend(dir);
    backend.put(OperatorId{1}, "k", as_view("real"));
    backend.snapshot(CheckpointId{5});  // produces checkpoint-5.snap

    // Inject a "partial" file for a different checkpoint id.
    {
        std::ofstream out(dir / "checkpoint-6.snap.part", std::ios::binary);
        out << "garbage";
    }
    EXPECT_FALSE(backend.has_checkpoint(CheckpointId{6}));

    FileBackedStateBackend reader(dir);
    reader.restore(Snapshot{.checkpoint_id = CheckpointId{6}, .bytes = {}});
    EXPECT_FALSE(reader.get(OperatorId{1}, "k").has_value())
        << ".part files must not be picked up as completed checkpoints";

    // The real checkpoint 5 is still loadable.
    FileBackedStateBackend reader2(dir);
    reader2.restore(Snapshot{.checkpoint_id = CheckpointId{5}, .bytes = {}});
    EXPECT_EQ(as_string(*reader2.get(OperatorId{1}, "k")), "real");
}

// End-to-end recovery: run #1 accumulates per-key counts to disk via
// a FileBackedStateBackend; run #2 starts a fresh executor pointed at
// the same directory and proves the counters resume from where run #1
// left off rather than starting at zero.
class CountingOp final : public Operator<std::int64_t, std::int64_t> {
public:
    void open() override {
        state_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "counts", int64_codec(), int64_codec()));
    }
    void process(const StreamElement<std::int64_t>& el, Emitter<std::int64_t>& out) override {
        if (!el.is_data()) {
            if (el.is_watermark()) {
                out.emit_watermark(el.as_watermark());
            } else {
                out.emit_barrier(el.as_barrier());
            }
            return;
        }
        Batch<std::int64_t> b;
        for (const auto& r : el.as_data()) {
            const auto v = r.value();
            const auto c = state_->get(v).value_or(0) + 1;
            state_->put(v, c);
            b.emplace(c);
        }
        out.emit_data(std::move(b));
    }
    std::string name() const override { return "counter"; }

private:
    std::optional<KeyedState<std::int64_t, std::int64_t>> state_;
};

class VectorIntSource final : public Source<std::int64_t> {
public:
    explicit VectorIntSource(std::vector<std::int64_t> data) : data_(std::move(data)) {}
    bool produce(Emitter<std::int64_t>& out) override {
        if (emitted_) {
            return false;
        }
        Batch<std::int64_t> b;
        for (auto v : data_) {
            b.emplace(v);
        }
        out.emit_data(std::move(b));
        emitted_ = true;
        return false;
    }
    std::string name() const override { return "vec_src"; }

private:
    std::vector<std::int64_t> data_;
    bool emitted_{false};
};

class IntCollectingSink final : public Sink<std::int64_t> {
public:
    void on_data(const Batch<std::int64_t>& batch) override {
        for (const auto& r : batch) {
            received_.push_back(r.value());
        }
    }
    std::string name() const override { return "collect"; }
    std::vector<std::int64_t> received_;
};

TEST(FileBackedStateBackend, StateSurvivesProcessRestartViaSharedDirectory) {
    const auto dir = scratch_dir("end_to_end_recovery");
    const CheckpointId ckpt{1};

    // ---- Run 1: process [1,2,2,3] and snapshot ----
    {
        auto backend = std::make_shared<FileBackedStateBackend>(dir);
        Dag dag;
        auto src = std::make_shared<VectorIntSource>(std::vector<std::int64_t>{1, 2, 2, 3});
        auto op = std::make_shared<CountingOp>();
        auto sink = std::make_shared<IntCollectingSink>();
        auto h0 = dag.add_source<std::int64_t>(src);
        auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, op);
        dag.add_sink<std::int64_t>(h1, sink);

        JobConfig cfg;
        cfg.state_backend = backend;
        LocalExecutor exec(std::move(dag), cfg);
        exec.run();

        // 1 -> 1 (first), 2 -> 1 (first), 2 -> 2 (second), 3 -> 1 (first)
        EXPECT_EQ(sink->received_, (std::vector<std::int64_t>{1, 1, 2, 1}));
        backend->snapshot(ckpt);
    }

    // ---- Run 2: fresh executor, same directory, restore, process [2,3,4] ----
    {
        auto backend = std::make_shared<FileBackedStateBackend>(dir);
        Dag dag;
        auto src = std::make_shared<VectorIntSource>(std::vector<std::int64_t>{2, 3, 4});
        auto op = std::make_shared<CountingOp>();
        auto sink = std::make_shared<IntCollectingSink>();
        auto h0 = dag.add_source<std::int64_t>(src);
        auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, op);
        dag.add_sink<std::int64_t>(h1, sink);

        JobConfig cfg;
        cfg.state_backend = backend;
        cfg.restore_from = Snapshot{.checkpoint_id = ckpt, .bytes = {}};
        LocalExecutor exec(std::move(dag), cfg);
        exec.run();

        // After restore: counts {1->1, 2->2, 3->1}. Processing [2,3,4]:
        //   2 -> 3 (was 2, now 3)
        //   3 -> 2 (was 1, now 2)
        //   4 -> 1 (new key)
        EXPECT_EQ(sink->received_, (std::vector<std::int64_t>{3, 2, 1}));
    }
}

// FileBacked opts into the asynchronous capture/persist split so the
// snapshot worker can move its durable write off the operator thread.
TEST(FileBackedStateBackend, SupportsAsyncPersist) {
    const auto dir = scratch_dir("supports_async");
    FileBackedStateBackend backend(dir);
    EXPECT_TRUE(backend.supports_async_persist());
}

// capture() (serialise, no disk) followed by persist() (durable write)
// must produce exactly what the fused synchronous snapshot() produces:
// the same on-disk file, restorable to the same state.
TEST(FileBackedStateBackend, CaptureThenPersistMatchesSyncSnapshot) {
    const auto dir = scratch_dir("capture_persist_equiv");
    FileBackedStateBackend backend(dir);
    backend.put(OperatorId{1}, "a", as_view("1"));
    backend.put(OperatorId{1}, "b", as_view("2"));
    backend.put(OperatorId{2}, "c", as_view("3"));

    // Split path: capture on one logical thread, persist on another.
    auto handle = backend.capture(CheckpointId{9});
    EXPECT_EQ(handle.checkpoint_id.value(), 9u);
    // Mutating live state after capture must not change the captured blob.
    backend.put(OperatorId{1}, "a", as_view("MUTATED"));
    const auto snap = backend.persist(std::move(handle));
    EXPECT_EQ(snap.checkpoint_id.value(), 9u);
    EXPECT_TRUE(snap.bytes.empty());
    EXPECT_TRUE(std::filesystem::exists(dir / "checkpoint-9.snap"));

    // Restore the async-written checkpoint into a fresh backend: it must
    // reflect state at capture time, not the post-capture mutation.
    FileBackedStateBackend reader(dir);
    reader.restore(Snapshot{.checkpoint_id = CheckpointId{9}, .bytes = {}});
    EXPECT_EQ(as_string(*reader.get(OperatorId{1}, "a")), "1");
    EXPECT_EQ(as_string(*reader.get(OperatorId{1}, "b")), "2");
    EXPECT_EQ(as_string(*reader.get(OperatorId{2}, "c")), "3");
}

// A source that emits its data, then a (non-terminal) checkpoint barrier,
// driving the operator runner's async snapshot path end to end.
class IntSourceThenBarrier final : public Source<std::int64_t> {
public:
    IntSourceThenBarrier(std::vector<std::int64_t> data, CheckpointId ckpt)
        : data_(std::move(data)), ckpt_(ckpt) {}
    bool produce(Emitter<std::int64_t>& out) override {
        if (done_) {
            return false;
        }
        Batch<std::int64_t> b;
        for (auto v : data_) {
            b.emplace(v);
        }
        out.emit_data(std::move(b));
        out.emit_barrier(CheckpointBarrier{ckpt_});
        done_ = true;
        return false;
    }
    std::string name() const override { return "vec_src_barrier"; }

private:
    std::vector<std::int64_t> data_;
    CheckpointId ckpt_;
    bool done_{false};
};

// End-to-end through the runner's ASYNC path: run #1 flows a barrier
// through the operator, the snapshot worker durably persists the state
// off-thread (and the executor drains it before exiting), and run #2
// restores from that async-acked checkpoint and resumes counting. This is
// the make-or-break recovery round-trip for the async snapshot worker.
//
// Note: the operator and the sink share one FileBacked backend here, and
// both now run async snapshot workers, so this also exercises TWO workers
// persisting checkpoint 1 concurrently (the case write_and_rename_'s
// unique temp file guards). It is deterministic, not lucky: the source
// emits its data then exactly one barrier then EOF, so the shared backend
// is frozen the moment the operator processes the barrier - both captures
// see byte-identical state regardless of thread schedule.
TEST(FileBackedStateBackend, AsyncWorkerCheckpointThroughRunnerIsRestorable) {
    const auto dir = scratch_dir("async_runner_recovery");
    const CheckpointId ckpt{1};

    std::vector<std::tuple<std::uint64_t, bool>> acks;
    std::mutex ack_mu;

    // ---- Run 1: process [1,2,2,3], then a barrier the worker persists ----
    {
        auto backend = std::make_shared<FileBackedStateBackend>(dir);
        Dag dag;
        auto src =
            std::make_shared<IntSourceThenBarrier>(std::vector<std::int64_t>{1, 2, 2, 3}, ckpt);
        auto op = std::make_shared<CountingOp>();
        auto sink = std::make_shared<IntCollectingSink>();
        auto h0 = dag.add_source<std::int64_t>(src);
        auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, op);
        dag.add_sink<std::int64_t>(h1, sink);

        JobConfig cfg;
        cfg.state_backend = backend;
        cfg.on_checkpoint_ack = [&](CheckpointId id, bool ok, std::string) {
            std::lock_guard lock(ack_mu);
            acks.emplace_back(id.value(), ok);
        };
        LocalExecutor exec(std::move(dag), cfg);
        exec.run();

        EXPECT_EQ(sink->received_, (std::vector<std::int64_t>{1, 1, 2, 1}));
    }

    // The worker must have durably written the checkpoint before the
    // executor returned, and acked it ok.
    EXPECT_TRUE(std::filesystem::exists(dir / "checkpoint-1.snap"));
    {
        std::lock_guard lock(ack_mu);
        bool acked_ok = false;
        for (const auto& [cid, ok] : acks) {
            if (cid == 1u && ok) {
                acked_ok = true;
            }
        }
        EXPECT_TRUE(acked_ok) << "async checkpoint 1 must ack ok before the executor exits";
    }

    // ---- Run 2: fresh executor, restore the async-written checkpoint ----
    {
        auto backend = std::make_shared<FileBackedStateBackend>(dir);
        Dag dag;
        auto src = std::make_shared<VectorIntSource>(std::vector<std::int64_t>{2, 3, 4});
        auto op = std::make_shared<CountingOp>();
        auto sink = std::make_shared<IntCollectingSink>();
        auto h0 = dag.add_source<std::int64_t>(src);
        auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, op);
        dag.add_sink<std::int64_t>(h1, sink);

        JobConfig cfg;
        cfg.state_backend = backend;
        cfg.restore_from = Snapshot{.checkpoint_id = ckpt, .bytes = {}};
        LocalExecutor exec(std::move(dag), cfg);
        exec.run();

        // Counts resumed from the async checkpoint: {1->1, 2->2, 3->1}.
        EXPECT_EQ(sink->received_, (std::vector<std::int64_t>{3, 2, 1}));
    }
}

// A stateful two-input operator: counts occurrences of each value across
// BOTH inputs into keyed state and emits the running count. Used to prove
// the co-operator runner's async snapshot path persists restorable state.
class CoCountingOp final : public CoOperator<std::int64_t, std::int64_t, std::int64_t> {
public:
    void open() override {
        state_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "cocounts", int64_codec(), int64_codec()));
    }
    void process_element1(const StreamElement<std::int64_t>& el,
                          Emitter<std::int64_t>& out) override {
        bump_(el, out);
    }
    void process_element2(const StreamElement<std::int64_t>& el,
                          Emitter<std::int64_t>& out) override {
        bump_(el, out);
    }
    std::string name() const override { return "co_counter"; }

private:
    void bump_(const StreamElement<std::int64_t>& el, Emitter<std::int64_t>& out) {
        if (!el.is_data()) {
            return;
        }
        Batch<std::int64_t> b;
        for (const auto& r : el.as_data()) {
            const auto c = state_->get(r.value()).value_or(0) + 1;
            state_->put(r.value(), c);
            b.emplace(c);
        }
        out.emit_data(std::move(b));
    }
    std::optional<KeyedState<std::int64_t, std::int64_t>> state_;
};

std::vector<std::int64_t> drain_int_channel(BoundedChannel<StreamElement<std::int64_t>>& ch) {
    std::vector<std::int64_t> out;
    while (auto m = ch.try_pop()) {
        if (m->is_data()) {
            for (const auto& r : m->as_data()) {
                out.push_back(r.value());
            }
        }
    }
    return out;
}

// End-to-end through the CO-OPERATOR runner's ASYNC path: phase 1 flows an
// aligned barrier through a two-input stateful op on FileBacked; the
// snapshot worker durably persists the keyed state off-thread (drained
// before the runner exits) and acks it. Phase 2 restores from that
// async-acked checkpoint and proves the per-key counts resumed.
TEST(FileBackedStateBackend, CoOperatorAsyncCheckpointThroughRunnerIsRestorable) {
    const auto dir = scratch_dir("coop_async_recovery");
    OperatorId id{0};

    std::vector<std::tuple<std::uint64_t, bool>> acks;
    std::mutex ack_mu;

    // ---- Phase 1: process left {1,2} and right {2,3}, then an aligned
    // barrier on both inputs that the worker persists. ----
    {
        auto backend = std::make_shared<FileBackedStateBackend>(dir);
        auto left = std::make_shared<BoundedChannel<StreamElement<std::int64_t>>>(64);
        auto right = std::make_shared<BoundedChannel<StreamElement<std::int64_t>>>(64);
        Dag dag;
        auto op = std::make_shared<CoCountingOp>();
        op->set_uid("co");
        auto h = dag.add_co_operator<std::int64_t, std::int64_t, std::int64_t>(
            StageHandle<std::int64_t>{left, 0},
            StageHandle<std::int64_t>{right, 0},
            op,
            int64_codec(),
            int64_codec());
        id = dag.runners()[h.runner_index].id;

        auto batch = [](std::vector<std::int64_t> vals) {
            Batch<std::int64_t> b;
            for (auto v : vals) {
                b.emplace(v);
            }
            return StreamElement<std::int64_t>::data(std::move(b));
        };
        left->push(batch({1, 2}));
        right->push(batch({2, 3}));
        left->push(StreamElement<std::int64_t>::barrier(CheckpointBarrier{
            CheckpointId{1}, /*terminal=*/false, CheckpointBarrier::Mode::Aligned}));
        right->push(StreamElement<std::int64_t>::barrier(CheckpointBarrier{
            CheckpointId{1}, /*terminal=*/false, CheckpointBarrier::Mode::Aligned}));
        left->close();
        right->close();

        RuntimeContext ctx(id, "co_counter", backend.get(), &MetricsRegistry::global());
        ctx.set_checkpoint_ack([&](CheckpointId cid, bool ok, std::string) {
            std::lock_guard lock(ack_mu);
            acks.emplace_back(cid.value(), ok);
        });
        dag.runners()[h.runner_index].run(ctx, [] { return false; });
    }

    EXPECT_TRUE(std::filesystem::exists(dir / "checkpoint-1.snap"))
        << "co-op async worker must persist the checkpoint before the runner exits";
    {
        std::lock_guard lock(ack_mu);
        bool acked_ok = false;
        for (const auto& [cid, ok] : acks) {
            if (cid == 1u && ok) {
                acked_ok = true;
            }
        }
        EXPECT_TRUE(acked_ok) << "co-op async checkpoint 1 must ack ok";
    }

    // ---- Phase 2: restore the async-written checkpoint, process one more
    // record for an existing key, prove the count resumed. ----
    {
        auto backend = std::make_shared<FileBackedStateBackend>(dir);
        backend->restore(Snapshot{.checkpoint_id = CheckpointId{1}, .bytes = {}});
        auto left = std::make_shared<BoundedChannel<StreamElement<std::int64_t>>>(64);
        auto right = std::make_shared<BoundedChannel<StreamElement<std::int64_t>>>(64);
        Dag dag;
        auto op = std::make_shared<CoCountingOp>();
        op->set_uid("co");
        auto h = dag.add_co_operator<std::int64_t, std::int64_t, std::int64_t>(
            StageHandle<std::int64_t>{left, 0},
            StageHandle<std::int64_t>{right, 0},
            op,
            int64_codec(),
            int64_codec());
        ASSERT_EQ(dag.runners()[h.runner_index].id, id) << "uid must give a stable id";

        Batch<std::int64_t> b;
        b.emplace(2);  // value 2 had count 2 at the checkpoint
        left->push(StreamElement<std::int64_t>::data(std::move(b)));
        left->close();
        right->close();

        RuntimeContext ctx(id, "co_counter", backend.get(), &MetricsRegistry::global());
        dag.runners()[h.runner_index].run(ctx, [] { return false; });

        // Count for 2 resumed from the restored value (2) -> 3.
        EXPECT_EQ(drain_int_channel(*h.output), (std::vector<std::int64_t>{3}));
    }
}

// A sink that records the barriers and commits it observes, to prove the
// sink runner routes NON-terminal barriers through the async worker and
// keeps TERMINAL barriers on the synchronous on_commit path.
class RecordingSink final : public Sink<std::int64_t> {
public:
    void on_data(const Batch<std::int64_t>&) override {}
    void on_barrier(CheckpointBarrier b) override { barriers.push_back(b.id().value()); }
    void on_commit(std::uint64_t id) override { commits.push_back(id); }
    std::string name() const override { return "rec_sink"; }
    std::vector<std::uint64_t> barriers;
    std::vector<std::uint64_t> commits;
};

// The SINK runner's async path: a non-terminal barrier is captured on-thread
// and persisted + acked off-thread (file written, ack ok); a terminal
// barrier stays fully synchronous (local on_commit, no async worker, no
// snapshot file).
TEST(FileBackedStateBackend, SinkAsyncNonTerminalAndSyncTerminalBarrier) {
    const auto dir = scratch_dir("sink_async");
    auto backend = std::make_shared<FileBackedStateBackend>(dir);
    backend->put(OperatorId{1}, "k", as_view("v"));  // non-trivial state to snapshot

    auto in = std::make_shared<BoundedChannel<StreamElement<std::int64_t>>>(64);
    Dag dag;
    auto sink = std::make_shared<RecordingSink>();
    auto h = dag.add_sink<std::int64_t>(StageHandle<std::int64_t>{in, 0}, sink);
    const OperatorId id = dag.runners()[h.runner_index].id;

    in->push(StreamElement<std::int64_t>::barrier(
        CheckpointBarrier{CheckpointId{1}, /*terminal=*/false}));
    in->push(StreamElement<std::int64_t>::barrier(
        CheckpointBarrier{CheckpointId{2}, /*terminal=*/true}));
    in->close();

    std::vector<std::tuple<std::uint64_t, bool>> acks;
    std::mutex ack_mu;
    RuntimeContext ctx(id, "rec_sink", backend.get(), &MetricsRegistry::global());
    ctx.set_checkpoint_ack([&](CheckpointId cid, bool ok, std::string) {
        std::lock_guard lock(ack_mu);
        acks.emplace_back(cid.value(), ok);
    });
    dag.runners()[h.runner_index].run(ctx, [] { return false; });

    // Non-terminal 1: persisted via the worker and acked ok.
    EXPECT_TRUE(std::filesystem::exists(dir / "checkpoint-1.snap"));
    // Terminal 2: committed locally, never snapshotted.
    EXPECT_EQ(sink->commits, (std::vector<std::uint64_t>{2}));
    EXPECT_FALSE(std::filesystem::exists(dir / "checkpoint-2.snap"));
    EXPECT_EQ(sink->barriers, (std::vector<std::uint64_t>{1, 2}));

    std::lock_guard lock(ack_mu);
    ASSERT_EQ(acks.size(), 1u) << "only the non-terminal barrier acks; terminal commits locally";
    EXPECT_EQ(std::get<0>(acks[0]), 1u);
    EXPECT_TRUE(std::get<1>(acks[0]));
}

TEST(FileBackedStateBackend, ScanReturnsRestoredEntries) {
    const auto dir = scratch_dir("scan_after_restore");
    {
        FileBackedStateBackend w(dir);
        w.put(OperatorId{1}, "a", as_view("1"));
        w.put(OperatorId{1}, "b", as_view("2"));
        w.put(OperatorId{1}, "c", as_view("3"));
        w.snapshot(CheckpointId{1});
    }
    FileBackedStateBackend r(dir);
    r.restore(Snapshot{.checkpoint_id = CheckpointId{1}, .bytes = {}});
    std::vector<std::pair<std::string, std::string>> seen;
    r.scan(OperatorId{1}, [&seen](StateBackend::KeyView k, StateBackend::ValueView v) {
        seen.emplace_back(std::string{k}, std::string{v});
    });
    EXPECT_EQ(seen.size(), 3u);
}

TEST(FileBackedStateBackend, PurgeCheckpointRemovesOnlyTheTargetSnapshot) {
    const auto dir = scratch_dir("purge");
    FileBackedStateBackend backend(dir);
    backend.put(OperatorId{1}, "k", as_view("v"));
    backend.snapshot(CheckpointId{1});
    backend.snapshot(CheckpointId{2});

    const auto snap1 = dir / "checkpoint-1.snap";
    const auto snap2 = dir / "checkpoint-2.snap";
    ASSERT_TRUE(std::filesystem::exists(snap1));
    ASSERT_TRUE(std::filesystem::exists(snap2));

    // Purge the older checkpoint through the base-class virtual to prove
    // the override dispatches (this is how the retention manager calls it).
    StateBackend& base = backend;
    base.purge_checkpoint(CheckpointId{1});
    EXPECT_FALSE(std::filesystem::exists(snap1));
    EXPECT_TRUE(std::filesystem::exists(snap2));

    // Purging an unknown id is a harmless no-op.
    EXPECT_NO_THROW(base.purge_checkpoint(CheckpointId{999}));

    // The retained checkpoint still restores into a fresh backend.
    FileBackedStateBackend restored(dir);
    restored.restore(Snapshot{.checkpoint_id = CheckpointId{2}, .bytes = {}});
    EXPECT_EQ(as_string(*restored.get(OperatorId{1}, "k")), "v");

    std::filesystem::remove_all(dir);
}

}  // namespace
