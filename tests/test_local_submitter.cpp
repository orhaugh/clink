// LocalSubmitter end-to-end test.
//
// Drives the fluent `StreamExecutionEnvironment` topology in-process -
// no JobManager, no TaskManager, no network bridges. Same fluent API
// surface the cluster path uses; the only difference is the terminal
// step: `LocalSubmitter::submit(env)` instead of
// `env.execute(name, JobSubmitter)`.

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/stream_execution_environment.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_submitter.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"

using namespace clink;
using namespace clink::api;

namespace {

std::vector<std::string> read_lines(const std::filesystem::path& path) {
    std::ifstream in(path);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        lines.push_back(line);
    }
    return lines;
}

}  // namespace

TEST(LocalSubmitter, SourceMapSinkRunsInProcess) {
    // Source emits {1..5}; map doubles; sink writes one line per record.
    cluster::ensure_built_ins_registered();
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_local_submitter_source_map_sink.txt";
    std::filesystem::remove(out_path);

    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(5).start(1).build());
    src.map<std::int64_t>([](const std::int64_t& v) { return v * 2; })
        .sink(FileInt64Sink::builder().path(out_path.string()).build());

    cluster::LocalSubmitter::submit(env);

    auto lines = read_lines(out_path);
    std::sort(lines.begin(), lines.end());
    ASSERT_EQ(lines.size(), 5u);
    EXPECT_EQ(lines[0], "10");
    EXPECT_EQ(lines[1], "2");
    EXPECT_EQ(lines[2], "4");
    EXPECT_EQ(lines[3], "6");
    EXPECT_EQ(lines[4], "8");

    std::filesystem::remove(out_path);
}

TEST(LocalSubmitter, FilterChainCompiles) {
    // Compose multiple inline ops to confirm chained DagBuilder lookups
    // (mint_inline_op_type → register_operator → DagBuilder side
    // effect → walker) work for >1 mid-chain op.
    cluster::ensure_built_ins_registered();
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_local_submitter_chain.txt";
    std::filesystem::remove(out_path);

    auto env = StreamExecutionEnvironment::create();
    env.source<std::int64_t>(IntRangeSource::builder().count(10).start(1).build())
        .filter([](const std::int64_t& v) { return v % 2 == 0; })
        .map<std::int64_t>([](const std::int64_t& v) { return v + 100; })
        .sink(FileInt64Sink::builder().path(out_path.string()).build());

    cluster::LocalSubmitter::submit(env);

    auto lines = read_lines(out_path);
    std::sort(lines.begin(), lines.end());
    ASSERT_EQ(lines.size(), 5u);  // 2,4,6,8,10 -> 102,104,106,108,110
    EXPECT_EQ(lines[0], "102");
    EXPECT_EQ(lines[1], "104");
    EXPECT_EQ(lines[2], "106");
    EXPECT_EQ(lines[3], "108");
    EXPECT_EQ(lines[4], "110");

    std::filesystem::remove(out_path);
}

TEST(LocalSubmitter, EmptyGraphRejected) {
    auto env = StreamExecutionEnvironment::create();
    EXPECT_THROW(cluster::LocalSubmitter::submit(env), std::runtime_error);
}

namespace {

// Splits each input int into main (evens) + side (odds-as-string)
// outputs. The test wires the side output through a second sink to
// confirm the LocalSubmitter handles the "<producer>::<tag>" resolution.
class SplitEvensOddsProcess : public clink::ProcessFunction<std::int64_t, std::int64_t> {
public:
    static const clink::OutputTag<std::string>& odd_tag() {
        static const clink::OutputTag<std::string> t{"odds"};
        return t;
    }
    void process_element(const std::int64_t& v,
                         clink::ProcessFunctionContext<std::int64_t>& ctx,
                         clink::Collector<std::int64_t>& out) override {
        if (v % 2 == 0) {
            out.collect(v);
        } else {
            ctx.template side_output<std::string>(odd_tag()).emit_data(
                clink::Batch<std::string>{std::vector<clink::Record<std::string>>{
                    clink::Record<std::string>{std::to_string(v)}}});
        }
    }
    std::string name() const override { return "split_evens_odds"; }
};

}  // namespace

TEST(LocalSubmitter, SideOutputFlowsToSeparateSink) {
    clink::cluster::ensure_built_ins_registered();
    const auto evens_path =
        std::filesystem::temp_directory_path() / "clink_local_submitter_evens.txt";
    const auto odds_path =
        std::filesystem::temp_directory_path() / "clink_local_submitter_odds.txt";
    std::filesystem::remove(evens_path);
    std::filesystem::remove(odds_path);

    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(6).start(1).build());
    auto split = src.process<std::int64_t>(std::make_shared<SplitEvensOddsProcess>());

    split.sink(FileInt64Sink::builder().path(evens_path.string()).build());
    split.side_output<std::string>(SplitEvensOddsProcess::odd_tag())
        .sink(FileTextSink::builder().path(odds_path.string()).build());

    clink::cluster::LocalSubmitter::submit(env);

    auto evens = read_lines(evens_path);
    std::sort(evens.begin(), evens.end());
    ASSERT_EQ(evens.size(), 3u);  // 2, 4, 6
    EXPECT_EQ(evens[0], "2");
    EXPECT_EQ(evens[1], "4");
    EXPECT_EQ(evens[2], "6");

    auto odds = read_lines(odds_path);
    std::sort(odds.begin(), odds.end());
    ASSERT_EQ(odds.size(), 3u);  // 1, 3, 5
    EXPECT_EQ(odds[0], "1");
    EXPECT_EQ(odds[1], "3");
    EXPECT_EQ(odds[2], "5");

    std::filesystem::remove(evens_path);
    std::filesystem::remove(odds_path);
}

TEST(LocalSubmitter, ParallelUniformChainSubmitsWithoutThrowing) {
    // Smoke check: a uniformly-parallel source→map→sink chain
    // (parallelism = 2 on every op) submits and returns without
    // throwing. This exercises the parallel branch of every
    // DagBuilder closure (source / operator / sink) plus the walker's
    // matching-parallelism check.
    //
    // Real output assertions need partition-aware connectors so each
    // subtask writes to its own path; built-in IntRangeSource doesn't
    // partition `count` across subtasks and FileInt64Sink doesn't
    // include subtask_idx in its path, so a runtime assertion would
    // race on the shared file. The LocalSubmitter's contract is "wire
    // parallelism through to the right add_parallel_* call"; partition
    // correctness is the connector's responsibility.
    cluster::ensure_built_ins_registered();
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_local_submitter_parallel_smoke.txt";
    std::filesystem::remove(out_path);

    auto env = StreamExecutionEnvironment::create();
    env.source<std::int64_t>(IntRangeSource::builder().count(2).start(1).build())
        .map<std::int64_t>([](const std::int64_t& v) { return v * 10; })
        .sink(FileInt64Sink::builder().path(out_path.string()).build());

    auto& mutable_graph = const_cast<cluster::JobGraphSpec&>(env.graph());  // NOLINT(*-const-cast)
    for (auto& op : mutable_graph.ops) {
        op.parallelism = 2;
    }
    EXPECT_NO_THROW(cluster::LocalSubmitter::submit(env));
    std::filesystem::remove(out_path);
}

TEST(LocalSubmitter, MixedParallelismRejected) {
    // Uniform parallelism is supported; mixing values across an edge
    // requires hash-shuffle or rebalance which are follow-ups.
    cluster::ensure_built_ins_registered();
    auto env = StreamExecutionEnvironment::create();
    env.source<std::int64_t>(IntRangeSource::builder().count(1).start(0).build())
        .map<std::int64_t>([](const std::int64_t& v) { return v; })
        .sink(FileInt64Sink::builder().path("/tmp/_unused.txt").build());
    auto& g = const_cast<cluster::JobGraphSpec&>(env.graph());  // NOLINT(*-const-cast)
    ASSERT_GE(g.ops.size(), 2u);
    g.ops[0].parallelism = 2;  // source @ 2
    g.ops[1].parallelism = 3;  // map @ 3 - mismatched
    EXPECT_THROW(cluster::LocalSubmitter::submit(env), std::runtime_error);
}

TEST(LocalSubmitter, UnregisteredOpTypeRejected) {
    // Hand-build a JobGraphSpec referencing a fake op_type. v1
    // LocalSubmitter must surface that as a precise runtime_error
    // rather than silently dropping the op.
    cluster::ensure_built_ins_registered();
    auto env = StreamExecutionEnvironment::create();
    // Use the existing source builder so the env has one valid op,
    // then mutate the graph's op_type to something fake via a fresh
    // env -- simulating a plugin .so that didn't load.
    // (Direct graph mutation is the simplest way to exercise this
    // path without writing a fake plugin.)
    env.source<std::int64_t>(IntRangeSource::builder().count(1).start(0).build());
    // Cast away const-ness to mutate the spec for the test only.
    auto& mutable_graph = const_cast<cluster::JobGraphSpec&>(env.graph());  // NOLINT(*-const-cast)
    ASSERT_FALSE(mutable_graph.ops.empty());
    mutable_graph.ops.front().type = "nonexistent_op_type_for_test";

    EXPECT_THROW(cluster::LocalSubmitter::submit(env), std::runtime_error);
}

namespace {

// KeyedProcessFunction that maintains a running per-key sum in keyed
// state. The function only works if RuntimeContext was constructed
// with a state_backend - without one, `ctx.keyed_state(...)` throws.
// Pairs with the JobConfig overload test below.
class RunningSumFn final
    : public clink::KeyedProcessFunction<std::int64_t, std::int64_t, std::int64_t> {
public:
    void open(clink::RuntimeContext& ctx) override {
        state_ = std::make_unique<clink::KeyedState<std::int64_t, std::int64_t>>(
            ctx.keyed_state<std::int64_t, std::int64_t>(
                "sum", clink::int64_codec(), clink::int64_codec()));
    }
    void process_element(const std::int64_t& v,
                         clink::ProcessFunctionContext<std::int64_t>& /*ctx*/,
                         clink::Collector<std::int64_t>& out) override {
        const auto prev = state_->get(current_key()).value_or(0);
        const auto next = prev + v;
        state_->put(current_key(), next);
        out.collect(next);
    }
    std::string name() const override { return "running_sum"; }

private:
    std::unique_ptr<clink::KeyedState<std::int64_t, std::int64_t>> state_;
};

}  // namespace

// KeyedCoProcessFunction that emits "left:<key>:<v>" and
// "right:<key>:<v>" lines, exercising current_key()'s typed-K access
// on both dispatch sides. Used by the connect_process fluent test
// below.
class TypedKKeyedCoFn final
    : public KeyedCoProcessFunction<std::string, std::int64_t, std::int64_t, std::string> {
public:
    void process_element1(const std::int64_t& v,
                          ProcessFunctionContext<std::string>& /*ctx*/,
                          Collector<std::string>& out) override {
        out.collect("left:" + current_key() + ":" + std::to_string(v));
    }
    void process_element2(const std::int64_t& v,
                          ProcessFunctionContext<std::string>& /*ctx*/,
                          Collector<std::string>& out) override {
        out.collect("right:" + current_key() + ":" + std::to_string(v));
    }
};

TEST(LocalSubmitter, KeyedConnectProcessExposesTypedKey) {
    // Two int64 sources, both keyed by the same name. The fluent
    // .connect_process(...) supplies per-side key extractors that
    // map int64 -> string (typed-K). The KeyedCoProcessFunction's
    // current_key() must see the string key on each dispatch side.
    cluster::ensure_built_ins_registered();
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_keyed_connect_process.txt";
    std::filesystem::remove(out_path);

    auto env = StreamExecutionEnvironment::create();
    // Pre-register a shared int64 key extractor so both sides of the
    // connect agree on the partitioning name. connect_process checks
    // the names match (so subtask routing is consistent across the
    // two upstreams), then layers the typed-K extractors on top.
    env.registry().register_key_extractor<std::int64_t>(
        "v_mod_2", [](const std::int64_t& v) { return v % 2; });

    auto left = env.source<std::int64_t>(IntRangeSource::builder().count(2).start(1).build())
                    .key_by(std::string{"v_mod_2"});
    auto right = env.source<std::int64_t>(IntRangeSource::builder().count(2).start(10).build())
                     .key_by(std::string{"v_mod_2"});

    left.connect_process<std::int64_t, std::string, std::string>(
            right,
            std::make_shared<TypedKKeyedCoFn>(),
            [](const std::int64_t& v) { return std::string{"L-"} + std::to_string(v); },
            [](const std::int64_t& v) { return std::string{"R-"} + std::to_string(v); })
        .uid("keyed-co-process")
        .sink(FileTextSink::builder().path(out_path.string()).build());

    cluster::LocalSubmitter::submit(env);

    auto lines = read_lines(out_path);
    std::sort(lines.begin(), lines.end());
    ASSERT_EQ(lines.size(), 4u);
    // Left records: 1, 2. Right records: 10, 11.
    // Left key_fn -> "L-1", "L-2"; right key_fn -> "R-10", "R-11".
    EXPECT_EQ(lines[0], "left:L-1:1");
    EXPECT_EQ(lines[1], "left:L-2:2");
    EXPECT_EQ(lines[2], "right:R-10:10");
    EXPECT_EQ(lines[3], "right:R-11:11");

    std::filesystem::remove(out_path);
}

TEST(LocalSubmitter, JobConfigOverloadProvidesStateBackendForKeyedState) {
    // Pipeline: 1..5 -> key_by(v%2) -> running per-key sum -> sink.
    // The keyed `RunningSumFn` opens its state slot in open(), which
    // calls `ctx.keyed_state(...)` - that throws unless a state_backend
    // is configured. So this test exercises that the `submit(env,
    // JobConfig)` overload actually threads the backend through to
    // LocalExecutor (the bare `submit(env)` overload constructs a
    // default JobConfig with no backend, which would fail here).
    //
    // Per-key sum sequence given source order {1,2,3,4,5}:
    //   key=1: 1, 1+3=4, 4+5=9
    //   key=0: 2, 2+4=6
    // Output stream interleaved in source order: 1, 2, 4, 6, 9.
    cluster::ensure_built_ins_registered();
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_local_submitter_keyed_state.txt";
    std::filesystem::remove(out_path);

    auto env = StreamExecutionEnvironment::create();
    env.source<std::int64_t>(IntRangeSource::builder().count(5).start(1).build())
        .key_by([](const std::int64_t& v) { return v % 2; })
        .process<std::int64_t>(std::make_shared<RunningSumFn>())
        .uid("running-sum")  // stateful op - pin OperatorId for restore.
        .sink(FileInt64Sink::builder().path(out_path.string()).build());

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    cluster::LocalSubmitter::submit(env, std::move(cfg));

    auto lines = read_lines(out_path);
    std::sort(lines.begin(), lines.end());
    ASSERT_EQ(lines.size(), 5u);
    EXPECT_EQ(lines[0], "1");
    EXPECT_EQ(lines[1], "2");
    EXPECT_EQ(lines[2], "4");
    EXPECT_EQ(lines[3], "6");
    EXPECT_EQ(lines[4], "9");

    std::filesystem::remove(out_path);
}

TEST(LocalSubmitter, KeyedStateWithoutJobConfigBackendRejected) {
    // Symmetric negative case: the same keyed pipeline without a
    // configured state_backend must fail (not silently produce
    // garbage). This guards against a regression where the
    // `submit(env)` zero-config overload defaults a backend in -
    // which would mask user-mistakes that the JobConfig overload
    // is the explicit fix for.
    cluster::ensure_built_ins_registered();
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_local_submitter_keyed_state_no_backend.txt";
    std::filesystem::remove(out_path);

    auto env = StreamExecutionEnvironment::create();
    env.source<std::int64_t>(IntRangeSource::builder().count(5).start(1).build())
        .key_by([](const std::int64_t& v) { return v % 2; })
        .process<std::int64_t>(std::make_shared<RunningSumFn>())
        .uid("running-sum-nobackend")
        .sink(FileInt64Sink::builder().path(out_path.string()).build());

    EXPECT_THROW(cluster::LocalSubmitter::submit(env), std::runtime_error);

    std::filesystem::remove(out_path);
}

TEST(LocalSubmitter, MultiConsumerFanOutBothConsumersReceiveAllRecords) {
    // Asymmetric fan-out: a single source feeds two downstream branches
    // - one direct sink, one map-then-sink. Without forking the
    // producer's BoundedChannel, both consumers race on a single-reader
    // queue and each record goes to exactly one branch. The TypeRegistry
    // make_fork_handles path must give each branch a complete copy of
    // the stream.
    //
    // The asymmetry (one chain length 0, the other length 1) matters:
    // a symmetric `src.sink(a); src.sink(b);` test could pass on a
    // future "both sinks share a single MPMC channel" implementation
    // that wouldn't actually copy. The map in branch B forces a
    // distinct downstream operator, so anything cheaper than a real
    // fork would show up as missing records in one file.
    cluster::ensure_built_ins_registered();
    const auto direct_path =
        std::filesystem::temp_directory_path() / "clink_local_submitter_fanout_direct.txt";
    const auto mapped_path =
        std::filesystem::temp_directory_path() / "clink_local_submitter_fanout_mapped.txt";
    std::filesystem::remove(direct_path);
    std::filesystem::remove(mapped_path);

    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(5).start(1).build());
    src.sink(FileInt64Sink::builder().path(direct_path.string()).build());
    src.map<std::int64_t>([](const std::int64_t& v) { return v + 100; })
        .sink(FileInt64Sink::builder().path(mapped_path.string()).build());

    cluster::LocalSubmitter::submit(env);

    auto direct = read_lines(direct_path);
    std::sort(direct.begin(), direct.end());
    ASSERT_EQ(direct.size(), 5u);
    EXPECT_EQ(direct[0], "1");
    EXPECT_EQ(direct[1], "2");
    EXPECT_EQ(direct[2], "3");
    EXPECT_EQ(direct[3], "4");
    EXPECT_EQ(direct[4], "5");

    auto mapped = read_lines(mapped_path);
    std::sort(mapped.begin(), mapped.end());
    ASSERT_EQ(mapped.size(), 5u);
    EXPECT_EQ(mapped[0], "101");
    EXPECT_EQ(mapped[1], "102");
    EXPECT_EQ(mapped[2], "103");
    EXPECT_EQ(mapped[3], "104");
    EXPECT_EQ(mapped[4], "105");

    std::filesystem::remove(direct_path);
    std::filesystem::remove(mapped_path);
}

TEST(LocalSubmitter, MultiConsumerFanOutAtParallelismGreaterThanOneSubmitsWithoutThrowing) {
    // Smoke check: multi-consumer fan-out from a producer with
    // parallelism > 1 is wired through `Dag::fork_parallel<T>` and
    // exercises the per-subtask broadcast path in TypeRegistry's
    // make_fork_handles closure. Content assertions need partition-aware
    // sinks (FileInt64Sink writes a shared path so subtasks race) - that
    // contract is the connector's responsibility. The DAG-level
    // `DagForkParallel.BroadcastsToAllBranchesAtParallelism` covers
    // record-level correctness with deterministic VectorSource + CollectingSink.
    cluster::ensure_built_ins_registered();
    const auto path_a =
        std::filesystem::temp_directory_path() / "clink_local_submitter_fanout_parallel_a.txt";
    const auto path_b =
        std::filesystem::temp_directory_path() / "clink_local_submitter_fanout_parallel_b.txt";
    std::filesystem::remove(path_a);
    std::filesystem::remove(path_b);

    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(1).start(0).build());
    src.sink(FileInt64Sink::builder().path(path_a.string()).build());
    src.sink(FileInt64Sink::builder().path(path_b.string()).build());

    auto& g = const_cast<cluster::JobGraphSpec&>(env.graph());  // NOLINT(*-const-cast)
    ASSERT_GE(g.ops.size(), 3u);                                // source + 2 sinks
    for (auto& op : g.ops) {
        op.parallelism = 2;
    }

    EXPECT_NO_THROW(cluster::LocalSubmitter::submit(env));
    std::filesystem::remove(path_a);
    std::filesystem::remove(path_b);
}
