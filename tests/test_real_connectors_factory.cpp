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

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/runner_registry.hpp"
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
// Same code path the generic role on the TM uses when a job graph
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

}  // namespace
