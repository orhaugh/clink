// End-to-end plugin-submission integration test.
//
// Spawns:
//   * 1x JM       (clink_node --role=jm --port=...)
//   * 2x TMs      (clink_node --role=tm ...)
//   * 1x client   (clink_node --role=client --graph=... --plugin=hello.so)
//
// The job graph references types and ops defined ONLY in the hello
// plugin: hello.Greeting (channel type), hello.GreetingSource,
// hello.GreetingFileSink. The cluster has zero of those compiled in;
// they only exist in the .so the client ships.
//
// Confirms: client packs the .so + content hash, JM loads it locally
// + ships it in Deploy, TMs dlopen it from their own caches, and the
// user-defined source/sink run across the cluster's runner-registry
// dispatch path with no recompile of clink_node.

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
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

std::filesystem::path hello_plugin_path() {
#ifdef CLINK_HELLO_PLUGIN_PATH
    return std::filesystem::path{CLINK_HELLO_PLUGIN_PATH};
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

[[maybe_unused]] bool wait_for(pid_t pid, std::chrono::milliseconds timeout, int& exit_code) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        int status = 0;
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) {
            if (WIFEXITED(status)) {
                exit_code = WEXITSTATUS(status);
                return true;
            }
            if (WIFSIGNALED(status)) {
                exit_code = 128 + WTERMSIG(status);
                return true;
            }
        }
        std::this_thread::sleep_for(50ms);
    }
    return false;
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

}  // namespace

TEST(PluginSubmission, ClientShipsPluginAndClusterRunsUserTypes) {
    const auto binary = node_binary_path();
    if (!std::filesystem::exists(binary)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto plugin = hello_plugin_path();
    if (!std::filesystem::exists(plugin)) {
        GTEST_SKIP() << "hello_plugin not built";
    }

    const auto jm_port = probe_free_port();
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_plugin_submission_test.txt";
    std::filesystem::remove(out_path);

    // The graph references types ONLY in the hello plugin. The cluster
    // has nothing compiled in for "hello.Greeting" et al. Built
    // programmatically via the C++ API; no JSON on disk.
    clink::cluster::JobGraphSpec graph;
    clink::cluster::OperatorSpec src;
    src.id = "src";
    src.type = "hello.GreetingSource";
    src.out_channel = "hello.Greeting";
    src.params = {{"count", "4"}, {"start", "200"}};
    graph.ops.push_back(std::move(src));
    clink::cluster::OperatorSpec snk;
    snk.id = "snk";
    snk.type = "hello.GreetingFileSink";
    snk.out_channel = "hello.Greeting";
    snk.inputs = {"src"};
    snk.params = {{"path", out_path.string()}};
    graph.ops.push_back(std::move(snk));

    const pid_t jm_pid =
        spawn_node({"clink_node", "--role=jm", "--port=" + std::to_string(jm_port)}, binary);
    ASSERT_GT(jm_pid, 0);
    std::this_thread::sleep_for(200ms);

    const pid_t tm_a_pid = spawn_node({"clink_node",
                                       "--role=tm",
                                       "--id=tm-a",
                                       "--jm-host=127.0.0.1",
                                       "--jm-port=" + std::to_string(jm_port)},
                                      binary);
    const pid_t tm_b_pid = spawn_node({"clink_node",
                                       "--role=tm",
                                       "--id=tm-b",
                                       "--jm-host=127.0.0.1",
                                       "--jm-port=" + std::to_string(jm_port)},
                                      binary);
    ASSERT_GT(tm_a_pid, 0);
    ASSERT_GT(tm_b_pid, 0);
    std::this_thread::sleep_for(300ms);

    clink::application::JobSubmitter submitter("127.0.0.1", jm_port);
    clink::application::SubmitOptions opts;
    opts.wait_timeout = std::chrono::seconds(15);
    const auto result = submitter.submit(graph.to_json(), {plugin.string()}, opts);

    kill_quietly(jm_pid);
    kill_quietly(tm_a_pid);
    kill_quietly(tm_b_pid);

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // The plugin's sink wrote one "<id>:<message>" per line.
    std::ifstream in(out_path);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        lines.push_back(line);
    }
    EXPECT_EQ(lines,
              (std::vector<std::string>{
                  "200:hello-200", "201:hello-201", "202:hello-202", "203:hello-203"}));
    std::filesystem::remove(out_path);
}

// Keyed-state smoke: a stateful operator in the plugin uses
// ValueState (via runtime()->keyed_state<>()) to count records per
// parity bucket. State survives across process() invocations within
// the subtask. The output proves the count incremented correctly per
// bucket - which only happens if the cluster wired up the
// InMemoryStateBackend before LocalExecutor ran.
TEST(PluginSubmission, KeyedStateCountersAccumulateInPluginOperator) {
    const auto binary = node_binary_path();
    if (!std::filesystem::exists(binary)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto plugin = hello_plugin_path();
    if (!std::filesystem::exists(plugin)) {
        GTEST_SKIP() << "hello_plugin not built";
    }

    const auto jm_port = probe_free_port();
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_plugin_keyed_state_test.txt";
    std::filesystem::remove(out_path);

    clink::cluster::JobGraphSpec graph;
    clink::cluster::OperatorSpec src;
    src.id = "src";
    src.type = "hello.GreetingSource";
    src.out_channel = "hello.Greeting";
    src.params = {{"count", "6"}, {"start", "1"}};
    graph.ops.push_back(std::move(src));
    clink::cluster::OperatorSpec counter;
    counter.id = "counter";
    counter.type = "hello.ParityCounter";
    counter.out_channel = "hello.Greeting";
    counter.inputs = {"src"};
    graph.ops.push_back(std::move(counter));
    clink::cluster::OperatorSpec snk;
    snk.id = "snk";
    snk.type = "hello.GreetingFileSink";
    snk.out_channel = "hello.Greeting";
    snk.inputs = {"counter"};
    snk.params = {{"path", out_path.string()}};
    graph.ops.push_back(std::move(snk));

    const pid_t jm_pid =
        spawn_node({"clink_node", "--role=jm", "--port=" + std::to_string(jm_port)}, binary);
    ASSERT_GT(jm_pid, 0);
    std::this_thread::sleep_for(200ms);

    const pid_t tm_pid = spawn_node({"clink_node",
                                     "--role=tm",
                                     "--id=tm-1",
                                     "--jm-host=127.0.0.1",
                                     "--jm-port=" + std::to_string(jm_port)},
                                    binary);
    const pid_t tm2_pid = spawn_node({"clink_node",
                                      "--role=tm",
                                      "--id=tm-2",
                                      "--jm-host=127.0.0.1",
                                      "--jm-port=" + std::to_string(jm_port)},
                                     binary);
    const pid_t tm3_pid = spawn_node({"clink_node",
                                      "--role=tm",
                                      "--id=tm-3",
                                      "--jm-host=127.0.0.1",
                                      "--jm-port=" + std::to_string(jm_port)},
                                     binary);
    ASSERT_GT(tm_pid, 0);
    ASSERT_GT(tm2_pid, 0);
    ASSERT_GT(tm3_pid, 0);
    std::this_thread::sleep_for(300ms);

    clink::application::JobSubmitter submitter("127.0.0.1", jm_port);
    clink::application::SubmitOptions opts;
    opts.wait_timeout = std::chrono::seconds(15);
    const auto result = submitter.submit(graph.to_json(), {plugin.string()}, opts);

    kill_quietly(jm_pid);
    kill_quietly(tm_pid);
    kill_quietly(tm2_pid);
    kill_quietly(tm3_pid);

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Source emits ids 1..6. Counter buckets by parity, accumulates
    // counts per bucket. Sink writes "<id>:<bucket>:<count>".
    std::ifstream in(out_path);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        lines.push_back(line);
    }
    EXPECT_EQ(lines,
              (std::vector<std::string>{"1:1:1", "2:0:1", "3:1:2", "4:0:2", "5:1:3", "6:0:3"}));
    std::filesystem::remove(out_path);
}

// Plugin-defined CoOperator (Greeting x int64 -> Greeting) running in
// the cluster. Both input streams share the same subtask via
// hello.BucketTally; keyed state on each side is independent, so the
// per-record outputs are deterministic regardless of how the runner
// interleaves the two inputs.
TEST(PluginSubmission, CoOperatorRunsTwoHeterogeneousInputs) {
    const auto binary = node_binary_path();
    if (!std::filesystem::exists(binary)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto plugin = hello_plugin_path();
    if (!std::filesystem::exists(plugin)) {
        GTEST_SKIP() << "hello_plugin not built";
    }

    const auto jm_port = probe_free_port();
    const auto out_path = std::filesystem::temp_directory_path() / "clink_plugin_coop_test.txt";
    std::filesystem::remove(out_path);

    clink::cluster::JobGraphSpec graph;
    {
        clink::cluster::OperatorSpec op;
        op.id = "g_src";
        op.type = "hello.GreetingSource";
        op.out_channel = "hello.Greeting";
        op.params = {{"count", "6"}, {"start", "1"}};
        graph.ops.push_back(std::move(op));
    }
    {
        clink::cluster::OperatorSpec op;
        op.id = "i_src";
        op.type = "int64_range_source";
        op.out_channel = "int64";
        op.params = {{"count", "3"}, {"start", "10"}};
        graph.ops.push_back(std::move(op));
    }
    {
        clink::cluster::OperatorSpec op;
        op.id = "coop";
        op.type = "hello.BucketTally";
        op.out_channel = "hello.Greeting";
        op.inputs = {"g_src", "i_src"};
        graph.ops.push_back(std::move(op));
    }
    {
        clink::cluster::OperatorSpec op;
        op.id = "snk";
        op.type = "hello.GreetingFileSink";
        op.out_channel = "hello.Greeting";
        op.inputs = {"coop"};
        op.params = {{"path", out_path.string()}};
        graph.ops.push_back(std::move(op));
    }

    const pid_t jm_pid =
        spawn_node({"clink_node", "--role=jm", "--port=" + std::to_string(jm_port)}, binary);
    ASSERT_GT(jm_pid, 0);
    std::this_thread::sleep_for(200ms);

    const pid_t tm1 = spawn_node({"clink_node",
                                  "--role=tm",
                                  "--id=tm-1",
                                  "--jm-host=127.0.0.1",
                                  "--jm-port=" + std::to_string(jm_port)},
                                 binary);
    const pid_t tm2 = spawn_node({"clink_node",
                                  "--role=tm",
                                  "--id=tm-2",
                                  "--jm-host=127.0.0.1",
                                  "--jm-port=" + std::to_string(jm_port)},
                                 binary);
    const pid_t tm3 = spawn_node({"clink_node",
                                  "--role=tm",
                                  "--id=tm-3",
                                  "--jm-host=127.0.0.1",
                                  "--jm-port=" + std::to_string(jm_port)},
                                 binary);
    const pid_t tm4 = spawn_node({"clink_node",
                                  "--role=tm",
                                  "--id=tm-4",
                                  "--jm-host=127.0.0.1",
                                  "--jm-port=" + std::to_string(jm_port)},
                                 binary);
    ASSERT_GT(tm1, 0);
    ASSERT_GT(tm2, 0);
    ASSERT_GT(tm3, 0);
    ASSERT_GT(tm4, 0);
    std::this_thread::sleep_for(300ms);

    clink::application::JobSubmitter submitter("127.0.0.1", jm_port);
    clink::application::SubmitOptions opts;
    opts.wait_timeout = std::chrono::seconds(15);
    const auto result = submitter.submit(graph.to_json(), {plugin.string()}, opts);

    kill_quietly(jm_pid);
    kill_quietly(tm1);
    kill_quietly(tm2);
    kill_quietly(tm3);
    kill_quietly(tm4);

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    std::ifstream in(out_path);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        lines.push_back(line);
    }
    // Per-stream counters are independent, so each record's output value
    // is deterministic regardless of left/right interleaving. We sort by
    // the leading numeric id for stable comparison.
    std::sort(lines.begin(), lines.end(), [](const std::string& a, const std::string& b) {
        return std::stoll(a) < std::stoll(b);
    });
    EXPECT_EQ(lines,
              (std::vector<std::string>{"1:G:1:1",
                                        "2:G:0:1",
                                        "3:G:1:2",
                                        "4:G:0:2",
                                        "5:G:1:3",
                                        "6:G:0:3",
                                        "10:I:10",
                                        "11:I:21",
                                        "12:I:33"}));
    std::filesystem::remove(out_path);
}

// Side outputs over the wire: a plugin operator emits even-id Greetings
// on its main output and odd-id ones to a named side output (channel
// "string"). The graph wires the side output to a separate text sink.
// Test asserts both files contain the expected, disjoint records.
TEST(PluginSubmission, SideOutputCrossesTheClusterWire) {
    const auto binary = node_binary_path();
    if (!std::filesystem::exists(binary)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto plugin = hello_plugin_path();
    if (!std::filesystem::exists(plugin)) {
        GTEST_SKIP() << "hello_plugin not built";
    }

    const auto jm_port = probe_free_port();
    const auto main_out = std::filesystem::temp_directory_path() / "clink_side_main.txt";
    const auto side_out = std::filesystem::temp_directory_path() / "clink_side_extra.txt";
    std::filesystem::remove(main_out);
    std::filesystem::remove(side_out);

    clink::cluster::JobGraphSpec graph;
    {
        clink::cluster::OperatorSpec op;
        op.id = "src";
        op.type = "hello.GreetingSource";
        op.out_channel = "hello.Greeting";
        op.params = {{"count", "6"}, {"start", "1"}};
        graph.ops.push_back(std::move(op));
    }
    {
        clink::cluster::OperatorSpec op;
        op.id = "part";
        op.type = "hello.GreetingPartitioner";
        op.out_channel = "hello.Greeting";
        op.inputs = {"src"};
        op.side_outputs.push_back(
            clink::cluster::SideOutputDecl{.tag = "hello.odd_text", .channel_type = "string"});
        graph.ops.push_back(std::move(op));
    }
    {
        // Main sink: receives even-id Greetings.
        clink::cluster::OperatorSpec op;
        op.id = "snk_main";
        op.type = "hello.GreetingFileSink";
        op.out_channel = "hello.Greeting";
        op.inputs = {"part"};
        op.params = {{"path", main_out.string()}};
        graph.ops.push_back(std::move(op));
    }
    {
        // Side sink: receives odd-id text records via the "hello.odd_text" tag.
        clink::cluster::OperatorSpec op;
        op.id = "snk_side";
        op.type = "file_text_sink";
        op.out_channel = "string";
        op.inputs = {"part::hello.odd_text"};
        op.params = {{"path", side_out.string()}};
        graph.ops.push_back(std::move(op));
    }

    const pid_t jm_pid =
        spawn_node({"clink_node", "--role=jm", "--port=" + std::to_string(jm_port)}, binary);
    ASSERT_GT(jm_pid, 0);
    std::this_thread::sleep_for(200ms);
    std::vector<pid_t> tms;
    for (int i = 1; i <= 4; ++i) {
        tms.push_back(spawn_node({"clink_node",
                                  "--role=tm",
                                  "--id=tm-side-" + std::to_string(i),
                                  "--jm-host=127.0.0.1",
                                  "--jm-port=" + std::to_string(jm_port)},
                                 binary));
        ASSERT_GT(tms.back(), 0);
    }
    std::this_thread::sleep_for(300ms);

    clink::application::JobSubmitter submitter("127.0.0.1", jm_port);
    clink::application::SubmitOptions opts;
    opts.wait_timeout = std::chrono::seconds(20);
    const auto result = submitter.submit(graph.to_json(), {plugin.string()}, opts);

    kill_quietly(jm_pid);
    for (auto tm : tms) {
        kill_quietly(tm);
    }

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto read_lines = [](const std::filesystem::path& p) {
        std::vector<std::string> out;
        std::ifstream in(p);
        std::string line;
        while (std::getline(in, line)) {
            out.push_back(line);
        }
        return out;
    };
    auto main_lines = read_lines(main_out);
    auto side_lines = read_lines(side_out);
    // Main contains greetings with even ids: 2, 4, 6.
    EXPECT_EQ(main_lines, (std::vector<std::string>{"2:hello-2", "4:hello-4", "6:hello-6"}));
    // Side contains text rows for odd ids.
    EXPECT_EQ(side_lines, (std::vector<std::string>{"odd:1", "odd:3", "odd:5"}));

    std::filesystem::remove(main_out);
    std::filesystem::remove(side_out);
}

// Hash-routing parity test: a keyed plugin operator running at
// parallelism=2 must see only records whose key hashes to its slot.
// We use the existing hello.ParityCounter (keyed state by bucket =
// id%2) and a key extractor that returns the same parity. With Hash
// routing wired correctly, subtask 0 sees only one parity and subtask
// 1 sees only the other; per-bucket counts in each subtask's output
// file match the single-subtask baseline (1, 2, 3 within the bucket).
// If Hash routing is broken, records scatter and the per-subtask
// counts will not be 1, 2, 3.
TEST(PluginSubmission, KeyByPartitionsRecordsAcrossParallelSubtasks) {
    const auto binary = node_binary_path();
    if (!std::filesystem::exists(binary)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto plugin = hello_plugin_path();
    if (!std::filesystem::exists(plugin)) {
        GTEST_SKIP() << "hello_plugin not built";
    }

    const auto jm_port = probe_free_port();
    const auto base_path = std::filesystem::temp_directory_path() / "clink_plugin_keyby_test.txt";
    // Sinks at par=2 produce path.0 and path.1.
    std::filesystem::remove(std::filesystem::path{base_path.string() + ".0"});
    std::filesystem::remove(std::filesystem::path{base_path.string() + ".1"});

    clink::cluster::JobGraphSpec graph;
    {
        clink::cluster::OperatorSpec op;
        op.id = "src";
        op.type = "hello.GreetingSource";
        op.out_channel = "hello.Greeting";
        op.params = {{"count", "6"}, {"start", "1"}};
        op.parallelism = 1;
        graph.ops.push_back(std::move(op));
    }
    {
        clink::cluster::OperatorSpec op;
        op.id = "counter";
        op.type = "hello.ParityCounter";
        op.out_channel = "hello.Greeting";
        op.inputs = {"src"};
        op.parallelism = 2;
        op.key_by = "hello.by_parity";  // hash by id parity -> 0 or 1
        graph.ops.push_back(std::move(op));
    }
    {
        clink::cluster::OperatorSpec op;
        op.id = "snk";
        op.type = "hello.GreetingFileSink";
        op.out_channel = "hello.Greeting";
        op.inputs = {"counter"};
        op.parallelism = 2;
        op.params = {{"path", base_path.string()}};
        graph.ops.push_back(std::move(op));
    }

    const pid_t jm_pid =
        spawn_node({"clink_node", "--role=jm", "--port=" + std::to_string(jm_port)}, binary);
    ASSERT_GT(jm_pid, 0);
    std::this_thread::sleep_for(200ms);

    // Need 1 source + 2 counter + 2 sink = 5 subtask slots. Spawn 5 TMs.
    std::vector<pid_t> tms;
    for (int i = 1; i <= 5; ++i) {
        tms.push_back(spawn_node({"clink_node",
                                  "--role=tm",
                                  "--id=tm-" + std::to_string(i),
                                  "--jm-host=127.0.0.1",
                                  "--jm-port=" + std::to_string(jm_port)},
                                 binary));
        ASSERT_GT(tms.back(), 0);
    }
    std::this_thread::sleep_for(300ms);

    clink::application::JobSubmitter submitter("127.0.0.1", jm_port);
    clink::application::SubmitOptions opts;
    opts.wait_timeout = std::chrono::seconds(20);
    const auto result = submitter.submit(graph.to_json(), {plugin.string()}, opts);

    kill_quietly(jm_pid);
    for (auto tm : tms) {
        kill_quietly(tm);
    }

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Read both per-subtask sink files.
    auto read_lines = [](const std::filesystem::path& p) {
        std::vector<std::string> out;
        std::ifstream in(p);
        std::string line;
        while (std::getline(in, line)) {
            out.push_back(line);
        }
        return out;
    };
    const auto file0 = std::filesystem::path{base_path.string() + ".0"};
    const auto file1 = std::filesystem::path{base_path.string() + ".1"};
    auto lines0 = read_lines(file0);
    auto lines1 = read_lines(file1);

    // Each line is "<id>:<bucket>:<count>". Sort each file by id for a
    // stable assertion (the source emits 1..6 in order, but hash-routed
    // delivery to a subtask preserves order so this sort is a safety
    // net rather than a real reordering).
    auto sort_by_id = [](std::vector<std::string>& xs) {
        std::sort(xs.begin(), xs.end(), [](const std::string& a, const std::string& b) {
            return std::stoll(a) < std::stoll(b);
        });
    };
    sort_by_id(lines0);
    sort_by_id(lines1);

    // Each subtask's records must ALL share the same bucket value.
    // Within a bucket, count must increment 1, 2, 3 across the 3 records.
    auto check_subtask_file =
        [](const std::vector<std::string>& lines) -> std::pair<int, std::vector<int>> {
        // Returns (bucket, counts) so caller can verify the partitioning.
        std::vector<int> ids;
        std::vector<int> buckets;
        std::vector<int> counts;
        for (const auto& l : lines) {
            // parse "<id>:<bucket>:<count>"
            auto p1 = l.find(':');
            auto p2 = l.find(':', p1 + 1);
            ids.push_back(std::stoi(l.substr(0, p1)));
            buckets.push_back(std::stoi(l.substr(p1 + 1, p2 - p1 - 1)));
            counts.push_back(std::stoi(l.substr(p2 + 1)));
        }
        // All buckets in this file must match.
        for (auto b : buckets) {
            EXPECT_EQ(b, buckets[0]) << "subtask saw records from multiple buckets - "
                                        "Hash routing did not partition by key";
        }
        return {buckets.empty() ? -1 : buckets[0], counts};
    };

    auto [b0, c0] = check_subtask_file(lines0);
    auto [b1, c1] = check_subtask_file(lines1);

    // Each subtask got exactly 3 records.
    EXPECT_EQ(c0.size(), 3u);
    EXPECT_EQ(c1.size(), 3u);
    // The two subtasks must have seen DIFFERENT buckets.
    EXPECT_NE(b0, b1) << "both subtasks saw the same bucket - keys did not split";
    // Per-bucket counts must be 1, 2, 3 (state survived across records
    // within a subtask, no cross-subtask leakage).
    EXPECT_EQ(c0, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(c1, (std::vector<int>{1, 2, 3}));

    std::filesystem::remove(file0);
    std::filesystem::remove(file1);
}

// Distributed-checkpointing end-to-end: a stateful plugin operator
// runs at parallelism=1, the JM periodically triggers checkpoints, the
// subtask snapshots its keyed state to disk on barrier. We then
// re-submit the same job with restore_from_dir set; the counter
// resumes its per-parity counts from the saved state and the new
// outputs reflect the accumulated history.
TEST(PluginSubmission, CheckpointAndRestoreAcrossJobRuns) {
    const auto binary = node_binary_path();
    if (!std::filesystem::exists(binary)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto plugin = hello_plugin_path();
    if (!std::filesystem::exists(plugin)) {
        GTEST_SKIP() << "hello_plugin not built";
    }

    const auto ckpt_dir =
        std::filesystem::temp_directory_path() /
        ("clink_ckpt_e2e_" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(ckpt_dir);

    auto build_graph = [](const std::filesystem::path& out,
                          std::int64_t start,
                          std::int64_t count,
                          std::int64_t delay_ms) {
        clink::cluster::JobGraphSpec graph;
        {
            clink::cluster::OperatorSpec op;
            op.id = "src";
            op.type = "hello.GreetingSource";
            op.out_channel = "hello.Greeting";
            op.params = {{"count", std::to_string(count)},
                         {"start", std::to_string(start)},
                         {"delay_ms", std::to_string(delay_ms)}};
            graph.ops.push_back(std::move(op));
        }
        {
            clink::cluster::OperatorSpec op;
            op.id = "counter";
            op.type = "hello.ParityCounter";
            op.out_channel = "hello.Greeting";
            op.inputs = {"src"};
            graph.ops.push_back(std::move(op));
        }
        {
            clink::cluster::OperatorSpec op;
            op.id = "snk";
            op.type = "hello.GreetingFileSink";
            op.out_channel = "hello.Greeting";
            op.inputs = {"counter"};
            op.params = {{"path", out.string()}};
            graph.ops.push_back(std::move(op));
        }
        return graph;
    };

    auto spawn_cluster = [&]() {
        struct Cluster {
            std::uint16_t jm_port;
            pid_t jm_pid;
            std::vector<pid_t> tms;
        };
        Cluster c;
        c.jm_port = probe_free_port();
        c.jm_pid =
            spawn_node({"clink_node", "--role=jm", "--port=" + std::to_string(c.jm_port)}, binary);
        std::this_thread::sleep_for(200ms);
        for (int i = 1; i <= 3; ++i) {
            c.tms.push_back(spawn_node({"clink_node",
                                        "--role=tm",
                                        "--id=tm-ckpt-" + std::to_string(i),
                                        "--jm-host=127.0.0.1",
                                        "--jm-port=" + std::to_string(c.jm_port)},
                                       binary));
        }
        std::this_thread::sleep_for(300ms);
        return c;
    };
    auto teardown = [](auto& cluster) {
        kill_quietly(cluster.jm_pid);
        for (auto pid : cluster.tms) {
            kill_quietly(pid);
        }
    };

    // ---- Run 1: process ids 1..4, periodic checkpoint snaps state ----
    const auto run1_out = std::filesystem::temp_directory_path() / "clink_ckpt_run1.out";
    std::filesystem::remove(run1_out);
    auto cluster = spawn_cluster();
    ASSERT_GT(cluster.jm_pid, 0);

    clink::application::JobSubmitter submitter1("127.0.0.1", cluster.jm_port);
    clink::application::SubmitOptions opts1;
    opts1.wait_timeout = std::chrono::seconds(20);
    opts1.checkpoint.checkpoint_dir = ckpt_dir.string();
    opts1.checkpoint.interval_ms = 50;  // fire often so we get acks before job ends
    const auto r1 = submitter1.submit(
        build_graph(run1_out, 1, 4, /*delay_ms=*/100).to_json(), {plugin.string()}, opts1);
    teardown(cluster);

    ASSERT_TRUE(r1.completed) << "run 1 reject: " << r1.reject_message;
    EXPECT_TRUE(r1.ok) << "run 1 errors: " << (r1.errors.empty() ? "(none)" : r1.errors[0]);

    // Find the latest COMPLETED-<id> marker the JM wrote.
    std::uint64_t completed_id = 0;
    if (std::filesystem::exists(ckpt_dir)) {
        for (const auto& entry : std::filesystem::directory_iterator(ckpt_dir)) {
            const auto fname = entry.path().filename().string();
            if (fname.rfind("COMPLETED-", 0) == 0) {
                try {
                    const auto id = std::stoull(fname.substr(10));
                    completed_id = std::max<std::uint64_t>(completed_id, id);
                } catch (...) {
                }
            }
        }
    }
    ASSERT_GT(completed_id, 0U) << "no COMPLETED marker in " << ckpt_dir
                                << " - JM coordinator did not finish any checkpoint";

    // Verify run-1 output as a sanity check (sources emit ids 1..4 -> bucket 1/0
    // for odd/even, counts 1..2 per bucket).
    {
        std::ifstream in(run1_out);
        std::vector<std::string> lines;
        std::string line;
        while (std::getline(in, line)) {
            lines.push_back(line);
        }
        EXPECT_EQ(lines, (std::vector<std::string>{"1:1:1", "2:0:1", "3:1:2", "4:0:2"}));
    }

    // ---- Run 2: process ids 5..8, restore state -> counts continue ----
    const auto run2_out = std::filesystem::temp_directory_path() / "clink_ckpt_run2.out";
    std::filesystem::remove(run2_out);
    const auto restore_dir =
        std::filesystem::temp_directory_path() /
        ("clink_ckpt_e2e_run2_" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(restore_dir);

    auto cluster2 = spawn_cluster();
    ASSERT_GT(cluster2.jm_pid, 0);
    clink::application::JobSubmitter submitter2("127.0.0.1", cluster2.jm_port);
    clink::application::SubmitOptions opts2;
    opts2.wait_timeout = std::chrono::seconds(20);
    opts2.checkpoint.checkpoint_dir = restore_dir.string();
    opts2.checkpoint.interval_ms = 0;  // no periodic; only restore
    opts2.checkpoint.restore_from_dir = ckpt_dir.string();
    opts2.checkpoint.restore_from_checkpoint_id = completed_id;
    const auto r2 = submitter2.submit(
        build_graph(run2_out, 5, 4, /*delay_ms=*/0).to_json(), {plugin.string()}, opts2);
    teardown(cluster2);

    ASSERT_TRUE(r2.completed) << "run 2 reject: " << r2.reject_message;
    EXPECT_TRUE(r2.ok) << "run 2 errors: " << (r2.errors.empty() ? "(none)" : r2.errors[0]);

    // After restore: parity counts continue from run 1.
    //   ids 1..4 set: even = {2,4} (count=2), odd = {1,3} (count=2).
    // Run 2 ids 5..8:
    //   5 -> bucket 1, count = 2 + 1 = 3
    //   6 -> bucket 0, count = 2 + 1 = 3
    //   7 -> bucket 1, count = 4
    //   8 -> bucket 0, count = 4
    {
        std::ifstream in(run2_out);
        std::vector<std::string> lines;
        std::string line;
        while (std::getline(in, line)) {
            lines.push_back(line);
        }
        EXPECT_EQ(lines, (std::vector<std::string>{"5:1:3", "6:0:3", "7:1:4", "8:0:4"}))
            << "state did not restore - counter is starting from zero again";
    }

    std::filesystem::remove_all(ckpt_dir);
    std::filesystem::remove_all(restore_dir);
    std::filesystem::remove(run1_out);
    std::filesystem::remove(run2_out);
}
