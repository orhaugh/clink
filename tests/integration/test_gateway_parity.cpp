// Gateway pipeline parity end-to-end test.
//
// Exercises a realistic multi-stage shape (FragmentReassembly →
// EnrichmentJoin → main + liveness side outputs) on the cluster
// runtime to prove the engine handles that topology end-to-end with
// no substitutions.
//
// Spawns:
//   * 1 JM
//   * 6 TMs (1 fragment source + 1 enrichment source + 2 reassemblers
//     + 2 joins + 2 main sinks + 1 liveness sink = 9 subtasks. Slot
//     count is 2 per TM by default so 6 TMs cover 12 slots cleanly.)
//
// Asserts:
//   * The main sink files together contain one enriched record per
//     reassembled group (sorted)
//   * The liveness sink contains one liveness event per emitted
//     enriched record (sorted)

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <signal.h>
#include <spawn.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>
#include <sys/wait.h>

#include "clink/application/job_submitter.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/core/codec.hpp"
#include "clink/runtime/network/network_channel.hpp"

extern char** environ;

namespace {

using namespace clink;
using namespace clink::network;
using namespace std::chrono_literals;

std::filesystem::path node_binary_path() {
#ifdef CLINK_NODE_BINARY
    return std::filesystem::path{CLINK_NODE_BINARY};
#else
    return {};
#endif
}

std::filesystem::path gateway_plugin_path() {
#ifdef CLINK_GATEWAY_PLUGIN_PATH
    return std::filesystem::path{CLINK_GATEWAY_PLUGIN_PATH};
#else
    return {};
#endif
}

pid_t spawn_node(const std::vector<std::string>& argv, const std::filesystem::path& binary_path) {
    std::vector<char*> raw_argv;
    raw_argv.reserve(argv.size() + 1);
    for (const auto& s : argv) {
        raw_argv.push_back(const_cast<char*>(s.c_str()));
    }
    raw_argv.push_back(nullptr);
    pid_t pid = -1;
    const auto rc =
        posix_spawn(&pid, binary_path.c_str(), nullptr, nullptr, raw_argv.data(), environ);
    return rc == 0 ? pid : -1;
}

void kill_quietly(pid_t pid) {
    if (pid > 0) {
        kill(pid, SIGKILL);
        int s = 0;
        ::waitpid(pid, &s, 0);
    }
}

std::uint16_t probe_free_port() {
    NetworkChannelSource<std::int64_t> probe(0, int64_codec());
    return probe.listen();
}

std::vector<std::string> read_all_lines(const std::filesystem::path& p) {
    std::vector<std::string> out;
    std::ifstream in(p);
    std::string line;
    while (std::getline(in, line)) {
        out.push_back(line);
    }
    return out;
}

}  // namespace

// Exercises the full gateway-style pipeline:
//
//   FragmentSource ──hash─▶ Reassembler(par=2) ──hash─▶ ┐
//                                                       ├── EnrichmentJoin(par=2) ─▶
//                                                       EnrichedFileSink(par=2)
//   EnrichmentSource ────────────────────hash──────────▶ ┘             │
//                                                                       └─side─▶
//                                                                       file_text_sink(par=1)
//
// Reassembler input fragments are designed to cover both reassembly
// (groups G1, G3) and pass-through (G2 with count=1) paths, and to
// arrive in interleaved order so the per-group ListState is exercised
// across multiple records. EnrichmentSource is given a delay that
// causes some fragments to arrive at the join before their enrichment
// (forcing the join's pending buffer) and some after (forcing the
// processElement1 fast path).
TEST(GatewayParity, ReassemblyJoinAndLivenessSideOutputCrossWire) {
    const auto binary = node_binary_path();
    if (!std::filesystem::exists(binary)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto plugin = gateway_plugin_path();
    if (!std::filesystem::exists(plugin)) {
        GTEST_SKIP() << "gateway_plugin not built";
    }

    const auto jm_port = probe_free_port();
    const auto main_sink_path =
        std::filesystem::temp_directory_path() / "clink_gateway_parity_main.txt";
    const auto liveness_path =
        std::filesystem::temp_directory_path() / "clink_gateway_parity_liveness.txt";
    // Sinks at par=2 use suffixes .0/.1; clean them all up beforehand.
    for (const auto& base : {main_sink_path}) {
        std::filesystem::remove(base);
        std::filesystem::remove(std::filesystem::path{base.string() + ".0"});
        std::filesystem::remove(std::filesystem::path{base.string() + ".1"});
    }
    std::filesystem::remove(liveness_path);

    clink::cluster::JobGraphSpec graph;
    {
        // FragmentSource: 5 fragments, 3 distinct groups.
        // G1 + G3 are fragmented (count=2); G2 passes through.
        clink::cluster::OperatorSpec op;
        op.id = "fragments";
        op.type = "gateway.FragmentSource";
        op.out_channel = "gateway.Fragment";
        op.params = {
            {"script", "G1:0:2:10|G2:0:1:42|G1:1:2:20|G3:0:2:5|G3:1:2:7"},
            {"delay_ms", "60"},
        };
        op.parallelism = 1;
        graph.ops.push_back(std::move(op));
    }
    {
        clink::cluster::OperatorSpec op;
        op.id = "enrichments";
        op.type = "gateway.EnrichmentSource";
        op.out_channel = "gateway.Enrichment";
        op.params = {
            {"script", "G1:profileA|G2:profileB|G3:profileC"},
            {"delay_ms", "120"},  // intentionally slower than fragments so
                                  // some pending-buffer paths are exercised
        };
        op.parallelism = 1;
        graph.ops.push_back(std::move(op));
    }
    {
        clink::cluster::OperatorSpec op;
        op.id = "reassembler";
        op.type = "gateway.Reassembler";
        op.out_channel = "gateway.Fragment";
        op.inputs = {"fragments"};
        op.parallelism = 2;
        op.key_by = "gateway.by_key";
        graph.ops.push_back(std::move(op));
    }
    {
        clink::cluster::OperatorSpec op;
        op.id = "join";
        op.type = "gateway.EnrichmentJoin";
        op.out_channel = "gateway.Enriched";
        op.inputs = {"reassembler", "enrichments"};
        op.parallelism = 2;
        op.key_by = "gateway.by_key";
        op.side_outputs.push_back(
            clink::cluster::SideOutputDecl{.tag = "gateway.liveness", .channel_type = "string"});
        graph.ops.push_back(std::move(op));
    }
    {
        clink::cluster::OperatorSpec op;
        op.id = "main_sink";
        op.type = "gateway.EnrichedFileSink";
        op.out_channel = "gateway.Enriched";
        op.inputs = {"join"};
        op.parallelism = 2;
        op.params = {{"path", main_sink_path.string()}};
        graph.ops.push_back(std::move(op));
    }
    {
        // Side sink consumes the liveness tag via the "id::tag" input
        // syntax and writes plain strings to a single file.
        clink::cluster::OperatorSpec op;
        op.id = "liveness_sink";
        op.type = "file_text_sink";
        op.out_channel = "string";
        op.inputs = {"join::gateway.liveness"};
        op.parallelism = 1;
        op.params = {{"path", liveness_path.string()}};
        graph.ops.push_back(std::move(op));
    }

    const pid_t jm_pid =
        spawn_node({"clink_node", "--role=jm", "--port=" + std::to_string(jm_port)}, binary);
    ASSERT_GT(jm_pid, 0);
    std::this_thread::sleep_for(200ms);

    // 1 + 1 + 2 + 2 + 2 + 1 = 9 subtasks. The TM default slot count is
    // 1, so spawn 9 TMs for clean placement.
    std::vector<pid_t> tms;
    for (int i = 1; i <= 9; ++i) {
        tms.push_back(spawn_node({"clink_node",
                                  "--role=tm",
                                  "--id=tm-gw-" + std::to_string(i),
                                  "--jm-host=127.0.0.1",
                                  "--jm-port=" + std::to_string(jm_port)},
                                 binary));
        ASSERT_GT(tms.back(), 0);
    }
    std::this_thread::sleep_for(400ms);

    clink::application::JobSubmitter submitter("127.0.0.1", jm_port);
    clink::application::SubmitOptions opts;
    opts.wait_timeout = std::chrono::seconds(30);
    const auto result = submitter.submit(graph.to_json(), {plugin.string()}, opts);

    kill_quietly(jm_pid);
    for (auto tm : tms) {
        kill_quietly(tm);
    }

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Collect both per-subtask main sink files and sort by key.
    std::vector<std::string> main_lines;
    for (const auto& suffix : {".0", ".1"}) {
        auto p = std::filesystem::path{main_sink_path.string() + suffix};
        auto ls = read_all_lines(p);
        for (auto& l : ls) {
            main_lines.push_back(std::move(l));
        }
    }
    std::sort(main_lines.begin(), main_lines.end());

    // Expected: one Enriched per group, value = sum of fragment values
    // for that group (G1=10+20=30, G2=42 pass-through, G3=5+7=12).
    EXPECT_EQ(main_lines,
              (std::vector<std::string>{
                  "G1:profileA:30",
                  "G2:profileB:42",
                  "G3:profileC:12",
              }));

    // Liveness side output: one event per emitted Enriched record,
    // routed across the wire to the dedicated string sink.
    auto liveness_lines = read_all_lines(liveness_path);
    std::sort(liveness_lines.begin(), liveness_lines.end());
    EXPECT_EQ(liveness_lines,
              (std::vector<std::string>{
                  "liveness:G1",
                  "liveness:G2",
                  "liveness:G3",
              }));

    for (const auto& suffix : {".0", ".1"}) {
        std::filesystem::remove(std::filesystem::path{main_sink_path.string() + suffix});
    }
    std::filesystem::remove(liveness_path);
}
