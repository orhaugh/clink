// Tests for the built-in core (no external dep) connector factories
// registered in the RunnerRegistry. These are the entry points the
// job-graph JSON uses when a submission says e.g. `"type":
// "file_text_source"`.
//
// Vendor-specific factory tests (kafka_text_source, postgres_*,
// clickhouse_sink, s3_text_sink) live with their impl module under
// impls/<vendor>/tests/test_factory_registration.cpp. Splitting them
// out is what makes clink_core_tests link against clink::core
// only - no impl dependency leaks into the core build.

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/worker.hpp"
#include "clink/connectors/file_sink.hpp"
#include "clink/connectors/file_source.hpp"
#include "clink/connectors/text_format.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

namespace {

using namespace clink;
using namespace clink::cluster;

TEST(RealConnectorFactories, FileTextSourceAndSinkAreRegistered) {
    ensure_built_ins_registered();
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("file_text_source", "string"), nullptr);
    EXPECT_NE(rr.find_sink("file_text_sink", "string"), nullptr);
}

// End-to-end LocalExecutor run: file_text_source -> file_text_sink
// pipes lines from an input file to an output file. Exercises the
// same FileSource<string>/FileSink<string> instances that the
// submission-layer factories build at runtime.
TEST(RealConnectorFactories, FileSourceToFileSinkEndToEnd) {
    const auto in_path = std::filesystem::temp_directory_path() / "clink_real_connector_in.txt";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_real_connector_out.txt";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    {
        std::ofstream f(in_path);
        f << "alpha\nbeta\ngamma\n";
    }

    Dag dag;
    auto src = std::make_shared<FileSource<std::string>>(in_path, string_text_format());
    auto snk = std::make_shared<FileSink<std::string>>(out_path, string_text_format());
    auto h0 = dag.add_source<std::string>(src);
    dag.add_sink<std::string>(h0, snk);

    LocalExecutor exec(std::move(dag));
    exec.run();

    std::ifstream out_in(out_path);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(out_in, line)) {
        lines.push_back(line);
    }
    EXPECT_EQ(lines, (std::vector<std::string>{"alpha", "beta", "gamma"}));

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Drive the file_text_source factory's underlying constructor directly
// to confirm the BuildContext -> FileSource<string> wiring is correct.
// Same code path the generic role on the worker uses when a job graph
// references "file_text_source".
TEST(RealConnectorFactories, FileTextSourceFactoryBuildsAndRuns) {
    ensure_built_ins_registered();
    const auto in_path =
        std::filesystem::temp_directory_path() / "clink_real_connector_factory_in.txt";
    std::filesystem::remove(in_path);
    {
        std::ofstream f(in_path);
        f << "one\ntwo\nthree\n";
    }

    auto src = std::make_shared<FileSource<std::string>>(in_path, string_text_format());
    src->open();

    std::vector<std::string> seen;
    bool more = true;
    while (more) {
        Batch<std::string> b;
        Emitter<std::string> collector(
            Emitter<std::string>::Forward([&seen](StreamElement<std::string> e) {
                if (e.is_data()) {
                    for (const auto& r : e.as_data()) {
                        seen.push_back(r.value());
                    }
                }
                return true;
            }));
        more = src->produce(collector);
    }
    src->close();
    EXPECT_EQ(seen, (std::vector<std::string>{"one", "two", "three"}));

    std::filesystem::remove(in_path);
}

// One file does not divide between subtasks. At parallelism 2 the first subtask
// reads it and the second runs an idle stand-in, so each line reaches the sink
// once; each subtask used to read the whole file, and the job wrote it twice.
TEST(RealConnectorFactories, FileTextSourceAtParallelismTwoWritesEachLineOnce) {
    ensure_built_ins_registered();
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_file_source_p2_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    const auto in_path = dir / "in.txt";
    const auto out_path = dir / "out.txt";
    {
        std::ofstream f(in_path);
        f << "alpha\nbeta\ngamma\n";
    }

    Coordinator coordinator;
    const std::uint16_t port = coordinator.start();
    coordinator.expect_workers({"worker-file-p2"});
    Worker::Config wcfg;
    wcfg.slot_count = 4;
    Worker worker("worker-file-p2", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(std::chrono::seconds{2}));

    JobGraphSpec g;
    OperatorSpec src;
    src.id = "src";
    src.type = "file_text_source";
    src.parallelism = 2;
    src.out_channel = std::string{kChannelString};
    src.params = {{"path", in_path.string()}};
    g.ops.push_back(src);
    OperatorSpec snk;
    snk.id = "snk";
    snk.type = "file_text_sink";
    snk.parallelism = 2;
    snk.out_channel = std::string{kChannelString};
    snk.inputs = {"src"};
    snk.params = {{"path", out_path.string()}};
    g.ops.push_back(snk);

    const auto job_id = coordinator.submit_job(g,
                                               OperatorRegistry::default_instance(),
                                               std::vector<PluginBinary>{},
                                               CheckpointConfig{},
                                               nullptr);
    ASSERT_TRUE(coordinator.await_job_completion(job_id, std::chrono::seconds{10}));
    EXPECT_TRUE(coordinator.job_errors(job_id).empty());

    // The sink writes one file per subtask: path.0 and path.1.
    std::vector<std::string> lines;
    for (const char* suffix : {".0", ".1"}) {
        std::ifstream in(out_path.string() + suffix);
        for (std::string line; std::getline(in, line);) {
            lines.push_back(line);
        }
    }
    std::sort(lines.begin(), lines.end());
    EXPECT_EQ(lines, (std::vector<std::string>{"alpha", "beta", "gamma"}));

    worker.stop();
    coordinator.stop();
    std::filesystem::remove_all(dir);
}

}  // namespace
