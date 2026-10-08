// A checkpoint is a consistent cut across every input of every operator,
// whatever alignment mode the job asked for. A fan-in, such as a union in
// front of a sink or a keyed operator (which is how the cluster merges several
// upstream subtasks), aligns a barrier stamped Unaligned exactly as an Aligned
// one: the input that delivered it pauses until every input has, and the
// barrier goes on marked Aligned. A row a slow input sent before
// its barrier is therefore inside the checkpoint, and a row the fast input sent
// after its barrier is outside it.
//
// The union used to forward the first unaligned barrier at once and the slow
// input's earlier rows behind it, expecting a consumer downstream to capture
// them. None could: downstream sees one channel, in order. A sink or keyed
// operator behind the union put those rows in the next checkpoint while their
// source's offset in this one was already past them, so a restart from this
// checkpoint lost them.
//
// The runners are driven directly over pre-loaded, pre-closed channels, so the
// order of every element is fixed and nothing depends on timing. Each of those
// cases loads the same shape: the fast input holds barrier 1 and then a row of
// the next interval, and the slow input holds a row of this interval and then
// barrier 1. The last case runs a whole bounded job, for the terminal barrier
// that ends it.

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/connectors/committing_sink.hpp"
#include "clink/connectors/file_2pc_sink.hpp"
#include "clink/connectors/text_format.hpp"
#include "clink/core/codec.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

using V = std::int64_t;
using Channel = BoundedChannel<StreamElement<V>>;

constexpr std::uint64_t kCut = 1;
// Sent by the slow input before its barrier: inside checkpoint kCut.
constexpr V kBeforeCut = 7;
// Sent by the fast input after its barrier: outside checkpoint kCut.
constexpr V kAfterCut = 9;

StreamElement<V> row(V v) {
    Batch<V> b;
    b.emplace(v);
    return StreamElement<V>::data(std::move(b));
}

StreamElement<V> unaligned_barrier() {
    return StreamElement<V>::barrier(CheckpointBarrier{
        CheckpointId{kCut}, /*terminal=*/false, CheckpointBarrier::Mode::Unaligned});
}

struct FanIn {
    std::shared_ptr<Channel> fast = std::make_shared<Channel>(64);
    std::shared_ptr<Channel> slow = std::make_shared<Channel>(64);
};

// `reason` closes both inputs. Cancelled is what a kill looks like to the
// runners downstream: they exit without flushing or finalising anything.
FanIn loaded_fan_in(ChannelCloseReason reason) {
    FanIn in;
    in.fast->push(unaligned_barrier());
    in.fast->push(row(kAfterCut));
    in.slow->push(row(kBeforeCut));
    in.slow->push(unaligned_barrier());
    in.fast->close(reason);
    in.slow->close(reason);
    return in;
}

StageHandle<V> union_of(Dag& dag, const FanIn& in) {
    return dag.union_streams<V>({StageHandle<V>{in.fast, 0}, StageHandle<V>{in.slow, 0}});
}

void run_runner(const Dag& dag, std::size_t runner_index, StateBackend* backend) {
    const auto& runner = dag.runners()[runner_index];
    RuntimeContext ctx(runner.id, runner.name, backend, &MetricsRegistry::global());
    runner.run(ctx, [] { return false; });
}

// Runs a runner that acknowledges checkpoints, and answers the
// acknowledgement as the coordinator does once every subtask has reported:
// the checkpoint is now a restore point (its snapshot is taken here, on the
// runner's thread, right after the runner's own) and is then committed.
std::optional<Snapshot> run_acking_runner(const Dag& dag,
                                          std::size_t runner_index,
                                          InMemoryStateBackend& backend,
                                          const std::function<void(std::uint64_t)>& commit) {
    const auto& runner = dag.runners()[runner_index];
    RuntimeContext ctx(runner.id, runner.name, &backend, &MetricsRegistry::global());
    std::optional<Snapshot> cut;
    ctx.set_checkpoint_ack([&](CheckpointId id, bool ok, std::string error) {
        EXPECT_TRUE(ok) << error;
        EXPECT_EQ(id.value(), kCut);
        cut = backend.snapshot(id);
        if (commit) {
            commit(id.value());
        }
    });
    runner.run(ctx, [] { return false; });
    return cut;
}

// What left a stage, in order: rows by value, barriers by id and mode.
// Watermarks are left out; the closes at the end free time, which is not what
// these tests are about.
std::vector<std::string> trace(Channel& out) {
    std::vector<std::string> seen;
    while (auto e = out.try_pop()) {
        if (e->is_data()) {
            for (const auto& r : e->as_data()) {
                seen.push_back("row " + std::to_string(r.value()));
            }
        } else if (e->is_barrier()) {
            const auto b = e->as_barrier();
            seen.push_back(
                "barrier " + std::to_string(b.id().value()) +
                (b.mode() == CheckpointBarrier::Mode::Aligned ? " aligned" : " unaligned"));
        }
    }
    return seen;
}

std::filesystem::path scratch_dir(const std::string& tag) {
    auto dir = std::filesystem::temp_directory_path() /
               ("clink_fan_in_cut_" + tag + "_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

TextFormat<V> int_lines() {
    return TextFormat<V>{.decode = [](std::string_view s) -> std::optional<V> {
                             return static_cast<V>(std::stoll(std::string{s}));
                         },
                         .encode = [](const V& v) { return std::to_string(v); }};
}

std::vector<std::string> committed_lines(const std::filesystem::path& dir) {
    std::vector<std::string> lines;
    for (const auto& entry : std::filesystem::directory_iterator(dir / "committed")) {
        std::ifstream file(entry.path());
        std::string line;
        while (std::getline(file, line)) {
            lines.push_back(line);
        }
    }
    return lines;
}

// The external system of the committer family (Postgres, S3, the upsert
// sinks), reduced to what exactly-once needs from it: rows a transaction
// stages stay invisible until it commits, and committing a handle twice is
// a no-op.
struct TransactionalStore {
    std::mutex mu;
    std::map<std::string, std::vector<V>> prepared;
    std::vector<V> committed;
};

class StoreCommittingSink final : public CommittingSink<V, std::string> {
public:
    explicit StoreCommittingSink(std::shared_ptr<TransactionalStore> store)
        : store_(std::move(store)) {}

    void write(const Batch<V>& batch) override {
        for (const auto& r : batch) {
            open_txn_.push_back(r.value());
        }
    }
    std::optional<std::string> prepare_commit(std::uint64_t checkpoint_id) override {
        const std::string handle = "txn-" + std::to_string(checkpoint_id);
        std::lock_guard lock(store_->mu);
        store_->prepared[handle] = std::exchange(open_txn_, {});
        return handle;
    }
    bool commit(const std::string& handle) override {
        std::lock_guard lock(store_->mu);
        if (auto it = store_->prepared.find(handle); it != store_->prepared.end()) {
            store_->committed.insert(store_->committed.end(), it->second.begin(), it->second.end());
            store_->prepared.erase(it);
        }
        return true;
    }
    void abort(const std::string& handle) override {
        std::lock_guard lock(store_->mu);
        store_->prepared.erase(handle);
    }
    std::string serialize(const std::string& handle) const override { return handle; }
    std::string deserialize(std::string_view bytes) const override { return std::string{bytes}; }
    std::string name() const override { return "store_committing_sink"; }

private:
    std::shared_ptr<TransactionalStore> store_;
    std::vector<V> open_txn_;
};

// Counts rows per value in keyed state, which is what a checkpoint holds.
class KeyedRowCounter final : public Operator<V, V> {
public:
    void open() override {
        counts_.emplace(
            this->runtime()->template keyed_state<V, V>("counts", int64_codec(), int64_codec()));
    }
    void process(const StreamElement<V>& el, Emitter<V>& out) override {
        if (el.is_data()) {
            for (const auto& r : el.as_data()) {
                const V n = counts_->get(r.value()).value_or(0) + 1;
                counts_->put(r.value(), n);
            }
        } else if (el.is_watermark()) {
            out.emit_watermark(el.as_watermark());
        } else if (el.is_barrier()) {
            out.emit_barrier(el.as_barrier());
        }
    }
    std::string name() const override { return "keyed_row_counter"; }

private:
    std::optional<KeyedState<V, V>> counts_;
};

}  // namespace

TEST(FanInCheckpointCut, AUnionHoldsAnUnalignedBarrierUntilTheSlowInputDeliversIt) {
    auto in = loaded_fan_in(ChannelCloseReason::Finished);
    Dag dag;
    auto merged = union_of(dag, in);

    run_runner(dag, merged.runner_index, nullptr);

    EXPECT_EQ(trace(*merged.output),
              (std::vector<std::string>{"row 7", "barrier 1 aligned", "row 9"}))
        << "the union let barrier 1 overtake the slow input's row 7, so a consumer downstream "
           "files row 7 under checkpoint 2 although its source's offset in checkpoint 1 is "
           "already past it";
}

TEST(FanInCheckpointCut, AFileSink2PCBehindAUnionCommitsTheSlowInputsRowWithTheCheckpoint) {
    const auto dir = scratch_dir("file_2pc");
    auto in = loaded_fan_in(ChannelCloseReason::Cancelled);
    Dag dag;
    auto merged = union_of(dag, in);
    auto sink = std::make_shared<FileSink2PC<V>>(dir, int_lines(), /*subtask_idx=*/0);
    const auto sink_stage = dag.add_sink<V>(merged, sink);
    const OperatorId sink_id = dag.runners()[sink_stage.runner_index].id;

    // The live run, ending in a kill: checkpoint 1 completes and commits, then
    // the inputs close as cancelled and nothing after it is finalised.
    run_runner(dag, merged.runner_index, nullptr);
    InMemoryStateBackend live;
    const auto cut = run_acking_runner(
        dag, sink_stage.runner_index, live, [&](std::uint64_t id) { sink->on_commit(id); });
    ASSERT_TRUE(cut.has_value()) << "checkpoint 1 never reached the sink";

    // The restart from checkpoint 1: a fresh sink on the same directory, over
    // the restored state, finalises whatever the checkpoint holds.
    InMemoryStateBackend restored;
    restored.restore(*cut);
    auto successor = std::make_shared<FileSink2PC<V>>(dir, int_lines(), /*subtask_idx=*/0);
    successor->set_id(sink_id);
    RuntimeContext successor_ctx(sink_id, successor->name(), &restored, nullptr);
    successor->attach_runtime(&successor_ctx);
    successor->open();
    successor->close();
    successor->attach_runtime(nullptr);

    EXPECT_EQ(committed_lines(dir), (std::vector<std::string>{"7"}))
        << "row 7 preceded barrier 1 on its input, so the restart replays its source from past "
           "it: it is in the committed output now or it is lost";
    std::filesystem::remove_all(dir);
}

TEST(FanInCheckpointCut, ACommittingSinkBehindAUnionCommitsTheSlowInputsRowWithTheCheckpoint) {
    auto store = std::make_shared<TransactionalStore>();
    auto in = loaded_fan_in(ChannelCloseReason::Cancelled);
    Dag dag;
    auto merged = union_of(dag, in);
    auto sink = std::make_shared<StoreCommittingSink>(store);
    const auto sink_stage = dag.add_sink<V>(merged, sink);
    const OperatorId sink_id = dag.runners()[sink_stage.runner_index].id;

    run_runner(dag, merged.runner_index, nullptr);
    InMemoryStateBackend live;
    const auto cut = run_acking_runner(
        dag, sink_stage.runner_index, live, [&](std::uint64_t id) { sink->on_commit(id); });
    ASSERT_TRUE(cut.has_value()) << "checkpoint 1 never reached the sink";

    InMemoryStateBackend restored;
    restored.restore(*cut);
    auto successor = std::make_shared<StoreCommittingSink>(store);
    successor->set_id(sink_id);
    RuntimeContext successor_ctx(sink_id, successor->name(), &restored, nullptr);
    successor->attach_runtime(&successor_ctx);
    successor->open();
    successor->close();
    successor->attach_runtime(nullptr);

    std::lock_guard lock(store->mu);
    EXPECT_EQ(store->committed, (std::vector<V>{kBeforeCut}))
        << "row 7 preceded barrier 1 on its input, so the restart replays its source from past "
           "it: it is committed with checkpoint 1 or it is lost";
}

TEST(FanInCheckpointCut, AKeyedCounterBehindAUnionCountsTheSlowInputsRowInTheCheckpoint) {
    auto in = loaded_fan_in(ChannelCloseReason::Cancelled);
    Dag dag;
    auto merged = union_of(dag, in);
    auto counter = std::make_shared<KeyedRowCounter>();
    counter->set_uid("fan_in_cut_counter");
    const auto counted = dag.add_operator<V, V>(merged, counter);
    const OperatorId counter_id = dag.runners()[counted.runner_index].id;

    run_runner(dag, merged.runner_index, nullptr);
    InMemoryStateBackend live;
    const auto cut = run_acking_runner(dag, counted.runner_index, live, nullptr);
    ASSERT_TRUE(cut.has_value()) << "checkpoint 1 never reached the counter";

    InMemoryStateBackend restored;
    restored.restore(*cut);
    RuntimeContext restored_ctx(counter_id, "keyed_row_counter", &restored, nullptr);
    auto counts = restored_ctx.keyed_state<V, V>("counts", int64_codec(), int64_codec());
    EXPECT_EQ(counts.get(kBeforeCut).value_or(0), 1)
        << "checkpoint 1 does not count row 7, which preceded barrier 1 on its input: a restore "
           "from it never counts row 7 at all";
    EXPECT_EQ(counts.get(kAfterCut).value_or(0), 0)
        << "checkpoint 1 counts row 9, which followed barrier 1 on its input: a restore from it "
           "counts row 9 twice";
}

TEST(FanInCheckpointCut, ABoundedUnionCommitsItsTailLocallyInEveryAlignmentMode) {
    // In process, with no coordinator to run a final checkpoint, each bounded
    // source ends with a terminal barrier, and a 2PC sink commits the tail on
    // it. Behind a union that barrier has to stay terminal once every input
    // has delivered it: passed on as an ordinary barrier, the sink prepared
    // the tail, waited 30 s for a commit nothing would send, and failed.
    for (const bool unaligned : {false, true}) {
        SCOPED_TRACE(unaligned ? "unaligned" : "aligned");
        const auto dir = scratch_dir(unaligned ? "tail_unaligned" : "tail_aligned");
        Dag dag;
        auto a = std::make_shared<VectorSource<V>>(std::vector<Record<V>>{Record<V>{1}}, "a");
        auto b = std::make_shared<VectorSource<V>>(std::vector<Record<V>>{Record<V>{2}}, "b");
        auto from_a = dag.add_source<V>(a);
        auto from_b = dag.add_source<V>(b);
        auto merged = dag.union_streams<V>({from_a, from_b});
        dag.add_sink<V>(merged,
                        std::make_shared<FileSink2PC<V>>(dir, int_lines(), /*subtask_idx=*/0));
        JobConfig cfg;
        cfg.state_backend = std::make_shared<InMemoryStateBackend>();
        cfg.unaligned_checkpoints = unaligned;
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();

        const auto errors = exec.operator_errors();
        EXPECT_TRUE(errors.empty()) << errors.front().first << ": " << errors.front().second;
        auto lines = committed_lines(dir);
        std::sort(lines.begin(), lines.end());
        EXPECT_EQ(lines, (std::vector<std::string>{"1", "2"}));
        std::filesystem::remove_all(dir);
    }
}
