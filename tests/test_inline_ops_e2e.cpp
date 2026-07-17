// Integration test for the inline-lambda DataStream<T>::map() /
// .flat_map() sugar. The pipeline is submitted through a real
// JobSubmitter against an in-process coordinator + worker pair so the round-trip
// touches: fluent-API graph construction, JSON serialisation, coordinator
// planning, deployment to the worker, and the worker's lookup of the minted
// op-type in the process-wide RunnerRegistry singleton.
//
// In-process is the only configuration where inline lambdas work -
// the lambda lives in this test's RunnerRegistry singleton and a
// remote worker (different process) wouldn't see it. The contract is
// documented on DataStream<T>::map().

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/application/job_submitter.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/worker.hpp"

using namespace clink;
using namespace clink::api;
using namespace std::chrono_literals;

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

TEST(InlineOpsE2E, InlineMapRunsEndToEndAgainstInProcessCluster) {
    cluster::Coordinator coordinator;
    const auto coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-inline"});

    cluster::Worker::Config worker_cfg;
    worker_cfg.slot_count = 4;  // source + op + sink need 3 slots
    cluster::Worker worker("worker-inline", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    std::this_thread::sleep_for(100ms);

    const auto out_path = std::filesystem::temp_directory_path() / "clink_inline_map_e2e.txt";
    std::filesystem::remove(out_path);

    auto env = Pipeline::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(5).start(10).build());
    src.map<std::int64_t>([](const std::int64_t& v) { return v * 3; })
        .sink(FileInt64Sink::builder().path(out_path.string()).build());

    application::JobSubmitter submitter("127.0.0.1", coordinator_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 10s;
    const auto result = env.execute("inline-map", submitter, opts);

    worker.stop();
    coordinator.stop();

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // 10, 11, 12, 13, 14 × 3 = 30, 33, 36, 39, 42
    EXPECT_EQ(read_lines(out_path), (std::vector<std::string>{"30", "33", "36", "39", "42"}));
    std::filesystem::remove(out_path);
}

TEST(InlineOpsE2E, InlineFilterDropsRecordsAgainstInProcessCluster) {
    cluster::Coordinator coordinator;
    const auto coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-inline-filter"});

    cluster::Worker::Config worker_cfg;
    worker_cfg.slot_count = 4;
    cluster::Worker worker("worker-inline-filter", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    std::this_thread::sleep_for(100ms);

    const auto out_path = std::filesystem::temp_directory_path() / "clink_inline_filter_e2e.txt";
    std::filesystem::remove(out_path);

    auto env = Pipeline::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(10).start(1).build());
    src.filter([](const std::int64_t& v) { return v % 2 == 0; })
        .sink(FileInt64Sink::builder().path(out_path.string()).build());

    application::JobSubmitter submitter("127.0.0.1", coordinator_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 10s;
    const auto result = env.execute("inline-filter", submitter, opts);

    worker.stop();
    coordinator.stop();

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // 1..10 filtered to evens -> 2, 4, 6, 8, 10
    EXPECT_EQ(read_lines(out_path), (std::vector<std::string>{"2", "4", "6", "8", "10"}));
    std::filesystem::remove(out_path);
}

TEST(InlineOpsE2E, InlineKeyByThenReduceAccumulatesPerKey) {
    cluster::Coordinator coordinator;
    const auto coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-inline-reduce"});

    cluster::Worker::Config worker_cfg;
    worker_cfg.slot_count = 8;  // 1 src + 2 reduce + 2 sink = 5
    cluster::Worker worker("worker-inline-reduce", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    std::this_thread::sleep_for(100ms);

    const auto base_path = std::filesystem::temp_directory_path() / "clink_inline_reduce_e2e.txt";
    std::filesystem::remove(std::filesystem::path{base_path.string() + ".0"});
    std::filesystem::remove(std::filesystem::path{base_path.string() + ".1"});

    auto env = Pipeline::create();
    // Source (par=1) -> keyed reduce (par=2 via key_by hash routing) ->
    // sink (par=2 produces path.0 and path.1). Key = v % 2 routes evens
    // to one subtask, odds to the other; each subtask accumulates
    // independently.
    //
    // Records 1..6:
    //   parity-1 subtask sees: 1 (sum 1), 3 (sum 4), 5 (sum 9)
    //   parity-0 subtask sees: 2 (sum 2), 4 (sum 6), 6 (sum 12)
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(6).start(1).build());
    src.key_by([](const std::int64_t& v) { return v % 2; })
        .reduce([](const std::int64_t& a, const std::int64_t& b) { return a + b; })
        .sink(FileInt64Sink::builder().path(base_path.string()).parallelism(2).build());

    application::JobSubmitter submitter("127.0.0.1", coordinator_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 10s;
    const auto result = env.execute("inline-reduce", submitter, opts);

    worker.stop();
    coordinator.stop();

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Hash partitioning by id parity routes (1, 3, 5) to one subtask and
    // (2, 4, 6) to the other. Which subtask hosts which parity depends on
    // the hash seed - assert by content irrespective of which file
    // received which parity.
    auto a = read_lines(base_path.string() + ".0");
    auto b = read_lines(base_path.string() + ".1");
    if (!a.empty() && a.front() == "2") {
        std::swap(a, b);
    }
    EXPECT_EQ(a, (std::vector<std::string>{"1", "4", "9"}));
    EXPECT_EQ(b, (std::vector<std::string>{"2", "6", "12"}));
    std::filesystem::remove(std::filesystem::path{base_path.string() + ".0"});
    std::filesystem::remove(std::filesystem::path{base_path.string() + ".1"});
}

// Simple e2e for .assign_timestamps_monotonic(): source -> assigner ->
// sink at parallelism 1. Verifies the inline assigner runs as a regular
// non-keyed transform in the cluster (records flow through unchanged;
// the assigner stamps event-time + emits watermarks but the test only
// checks the data path here since the sink discards watermarks).
//
// A full canonical  pipeline (source -> map -> filter -> assigner ->
// key_by -> sliding_window -> aggregate -> sink) does not yet run
// end-to-end in this configuration - there's a separate cluster
// routing bug where chained source+assigner -> keyed-downstream hangs.
// The aggregate operator's correctness is covered by the operator-level
// tests in test_stream_env.cpp (SlidingWindowAggregateOperator suite).
// Canonical pipeline: source -> map -> filter ->
// assign_timestamps -> key_by -> sliding_window -> aggregate -> sink.
// Verifies the full fluent API stack runs end-to-end through an
// in-process Coordinator + Worker.
TEST(InlineOpsE2E, CanonicalPatternRunsEndToEnd) {
    cluster::Coordinator coordinator;
    const auto coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-canonical"});

    cluster::Worker::Config worker_cfg;
    worker_cfg.slot_count = 16;
    cluster::Worker worker("worker-canonical", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    std::this_thread::sleep_for(100ms);

    const auto base_path = std::filesystem::temp_directory_path() / "clink_canonical_pipeline.txt";
    for (int i = 0; i < 2; ++i) {
        std::filesystem::remove(
            std::filesystem::path{base_path.string() + "." + std::to_string(i)});
    }

    auto env = Pipeline::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(10).start(1).build());
    src.map<std::int64_t>([](const std::int64_t& v) { return v * 10; })  // 10..100
        .filter([](const std::int64_t& v) { return v > 30; })            // 40..100
        .assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v}; })
        .key_by([](const std::int64_t& v) { return (v / 10) % 2; })
        .sliding_window(30ms, 30ms)
        .parallelism(2)
        .aggregate<std::int64_t>([]() { return std::int64_t{0}; },
                                 [](const std::int64_t& a, const std::int64_t& b) { return a + b; })
        .sink(FileInt64Sink::builder().path(base_path.string()).parallelism(2).build());

    application::JobSubmitter submitter("127.0.0.1", coordinator_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    const auto result = env.execute("-canonical", submitter, opts);

    worker.stop();
    coordinator.stop();

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Tumbling windows of 30ms (size == slide). With event-times 40..100:
    //   [30, 60):  40, 50           (parity 0: 40, parity 1: 50)
    //   [60, 90):  60, 70, 80       (parity 0: 60+80=140, parity 1: 70)
    //   [90, 120): 90, 100          (parity 0: 100, parity 1: 90)
    //
    // Parity-0 sums: 40, 140, 100  -> sorted strings: {"100", "140", "40"}
    // Parity-1 sums: 50, 70, 90    -> sorted strings: {"50", "70", "90"}
    auto a = read_lines(base_path.string() + ".0");
    auto b = read_lines(base_path.string() + ".1");
    std::sort(a.begin(), a.end());
    std::sort(b.begin(), b.end());
    const std::vector<std::string> parity_0{"100", "140", "40"};
    const std::vector<std::string> parity_1{"50", "70", "90"};
    if (a == parity_1) {
        std::swap(a, b);
    }
    EXPECT_EQ(a, parity_0);
    EXPECT_EQ(b, parity_1);

    for (int i = 0; i < 2; ++i) {
        std::filesystem::remove(
            std::filesystem::path{base_path.string() + "." + std::to_string(i)});
    }
}

// Event-time + windowing the -idiomatic way: from_elements gives
// explicit values, assign_timestamps_monotonic uses each value as its
// own event-time, key_by partitions, sliding_window groups by time
// range, aggregate folds per (key, window). Cleaner than synthesising
// timestamps off IntRangeSource indices, and the data → window
// mapping reads directly off the values list.
TEST(InlineOpsE2E, FromElementsEventTimeSlidingWindowAggregate) {
    cluster::Coordinator coordinator;
    const auto coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-from-elements-window"});

    cluster::Worker::Config worker_cfg;
    worker_cfg.slot_count = 12;
    cluster::Worker worker("worker-from-elements-window", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    std::this_thread::sleep_for(100ms);

    const auto base_path =
        std::filesystem::temp_directory_path() / "clink_from_elements_window_e2e.txt";
    for (int i = 0; i < 2; ++i) {
        std::filesystem::remove(
            std::filesystem::path{base_path.string() + "." + std::to_string(i)});
    }

    auto env = Pipeline::create();
    // Events at event-time = value * 100 ms: 100ms, 200ms, ..., 500ms.
    env.from_elements<std::int64_t>({1, 2, 3, 4, 5})
        .assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v * 100}; })
        .key_by([](const std::int64_t& v) { return v % 2; })  // parity
        .sliding_window(200ms, 200ms)                         // tumbling
        .parallelism(2)
        .aggregate<std::int64_t>([]() { return std::int64_t{0}; },
                                 [](const std::int64_t& a, const std::int64_t& b) { return a + b; })
        .sink(FileInt64Sink::builder().path(base_path.string()).parallelism(2).build());

    application::JobSubmitter submitter("127.0.0.1", coordinator_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    const auto result = env.execute("from-elements-window", submitter, opts);

    worker.stop();
    coordinator.stop();

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Tumbling windows of 200ms with events at 100, 200, 300, 400, 500:
    //   [0,   200): value 1 (parity 1, t=100)            -> parity 1 sum 1
    //   [200, 400): values 2 (parity 0, t=200),
    //               3 (parity 1, t=300)                  -> p0 sum 2, p1 sum 3
    //   [400, 600): values 4 (parity 0, t=400),
    //               5 (parity 1, t=500)                  -> p0 sum 4, p1 sum 5
    //
    // Parity 0 emissions (sorted): {"2", "4"}
    // Parity 1 emissions (sorted): {"1", "3", "5"}
    auto a = read_lines(base_path.string() + ".0");
    auto b = read_lines(base_path.string() + ".1");
    std::sort(a.begin(), a.end());
    std::sort(b.begin(), b.end());
    const std::vector<std::string> parity_0{"2", "4"};
    const std::vector<std::string> parity_1{"1", "3", "5"};
    if (a == parity_1) {
        std::swap(a, b);
    }
    EXPECT_EQ(a, parity_0);
    EXPECT_EQ(b, parity_1);

    for (int i = 0; i < 2; ++i) {
        std::filesystem::remove(
            std::filesystem::path{base_path.string() + "." + std::to_string(i)});
    }
}

TEST(InlineOpsE2E, FromElementsEmitsLiteralValuesThroughCluster) {
    cluster::Coordinator coordinator;
    const auto coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-from-elements"});

    cluster::Worker::Config worker_cfg;
    worker_cfg.slot_count = 4;
    cluster::Worker worker("worker-from-elements", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    std::this_thread::sleep_for(100ms);

    const auto out_path = std::filesystem::temp_directory_path() / "clink_from_elements_e2e.txt";
    std::filesystem::remove(out_path);

    auto env = Pipeline::create();
    env.from_elements<std::string>({"alpha", "beta", "gamma", "delta"})
        .map<std::string>([](const std::string& s) { return s + "!"; })
        .sink(FileTextSink::builder().path(out_path.string()).build());

    application::JobSubmitter submitter("127.0.0.1", coordinator_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 10s;
    const auto result = env.execute("from-elements", submitter, opts);

    worker.stop();
    coordinator.stop();

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    EXPECT_EQ(read_lines(out_path),
              (std::vector<std::string>{"alpha!", "beta!", "gamma!", "delta!"}));
    std::filesystem::remove(out_path);
}

TEST(InlineOpsE2E, InlineAssignTimestampsRunsThroughCluster) {
    cluster::Coordinator coordinator;
    const auto coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-inline-ts"});

    cluster::Worker::Config worker_cfg;
    worker_cfg.slot_count = 4;
    cluster::Worker worker("worker-inline-ts", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    std::this_thread::sleep_for(100ms);

    const auto out_path = std::filesystem::temp_directory_path() / "clink_inline_ts_e2e.txt";
    std::filesystem::remove(out_path);

    auto env = Pipeline::create();
    env.source<std::int64_t>(IntRangeSource::builder().count(5).start(1).build())
        .assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v * 100}; })
        .sink(FileInt64Sink::builder().path(out_path.string()).build());

    application::JobSubmitter submitter("127.0.0.1", coordinator_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 10s;
    const auto result = env.execute("inline-ts", submitter, opts);

    worker.stop();
    coordinator.stop();

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Records pass through the assigner unchanged (it only mutates
    // event_time, not value). 1..5.
    EXPECT_EQ(read_lines(out_path), (std::vector<std::string>{"1", "2", "3", "4", "5"}));
    std::filesystem::remove(out_path);
}

TEST(InlineOpsE2E, InlineFlatMapExpandsAndDropsRecords) {
    cluster::Coordinator coordinator;
    const auto coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-inline-flat"});

    cluster::Worker::Config worker_cfg;
    worker_cfg.slot_count = 4;
    cluster::Worker worker("worker-inline-flat", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    std::this_thread::sleep_for(100ms);

    const auto out_path = std::filesystem::temp_directory_path() / "clink_inline_flatmap_e2e.txt";
    std::filesystem::remove(out_path);

    auto env = Pipeline::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(4).start(1).build());

    // For each input n: emit "n" once if n is odd, twice if even.
    // Input 1, 2, 3, 4 → 1, 2, 2, 3, 4, 4
    src.flat_map<std::string>([](const std::int64_t& v) {
           std::vector<std::string> out{std::to_string(v)};
           if (v % 2 == 0) {
               out.push_back(std::to_string(v));
           }
           return out;
       })
        .sink(FileTextSink::builder().path(out_path.string()).build());

    application::JobSubmitter submitter("127.0.0.1", coordinator_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 10s;
    const auto result = env.execute("inline-flat-map", submitter, opts);

    worker.stop();
    coordinator.stop();

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    EXPECT_EQ(read_lines(out_path), (std::vector<std::string>{"1", "2", "2", "3", "4", "4"}));
    std::filesystem::remove(out_path);
}
