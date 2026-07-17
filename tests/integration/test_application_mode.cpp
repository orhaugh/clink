// End-to-end test for the Application-Mode JobSubmitter API.
//
// Spawns a coordinator + worker and uses clink::application::JobSubmitter to push
// a job graph (built from a JobGraphSpec via the C++ API, no JSON file
// on disk) at the cluster, waits for completion, and asserts the sink
// wrote what we expected. This replaces the `clink_node --role=client
// --graph=...` flow for users who'd rather link clink directly.

#include <array>
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
using namespace clink::cluster;
using namespace clink::network;
using namespace std::chrono_literals;

std::filesystem::path node_binary_path() {
#ifdef CLINK_NODE_BINARY
    return std::filesystem::path{CLINK_NODE_BINARY};
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

}  // namespace

TEST(ApplicationMode, JobSubmitterPushesAndWaitsForCompletion) {
    const auto binary = node_binary_path();
    if (!std::filesystem::exists(binary)) {
        GTEST_SKIP() << "clink_node not built";
    }

    const auto coordinator_port = probe_free_port();
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_application_mode_test.txt";
    std::filesystem::remove(out_path);

    const pid_t coordinator_pid = spawn_node(
        {"clink_node", "--role=coordinator", "--port=" + std::to_string(coordinator_port)}, binary);
    ASSERT_GT(coordinator_pid, 0);
    std::this_thread::sleep_for(200ms);

    const pid_t worker_pid = spawn_node({"clink_node",
                                         "--role=worker",
                                         "--id=worker-1",
                                         "--coordinator-host=127.0.0.1",
                                         "--coordinator-port=" + std::to_string(coordinator_port)},
                                        binary);
    ASSERT_GT(worker_pid, 0);
    std::this_thread::sleep_for(300ms);

    // Build the job graph programmatically rather than reading JSON
    // from disk. This is the whole point of the API: applications
    // construct their graph at runtime.
    JobGraphSpec graph;
    OperatorSpec src;
    src.id = "src";
    src.type = "int64_range_source";
    src.out_channel = "int64";
    src.params["count"] = "5";
    src.params["start"] = "100";
    graph.ops.push_back(std::move(src));
    OperatorSpec snk;
    snk.id = "snk";
    snk.type = "file_int64_sink";
    snk.inputs = {"src"};
    snk.out_channel = "int64";
    snk.params["path"] = out_path.string();
    graph.ops.push_back(std::move(snk));

    application::JobSubmitter submitter("127.0.0.1", coordinator_port);
    application::SubmitOptions opts;
    opts.wait_timeout = std::chrono::seconds(15);
    const auto result = submitter.submit(graph.to_json(), {}, opts);

    kill_quietly(coordinator_pid);
    kill_quietly(worker_pid);

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    EXPECT_NE(result.job_id, 0u);

    std::ifstream in(out_path);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        lines.push_back(line);
    }
    EXPECT_EQ(lines, (std::vector<std::string>{"100", "101", "102", "103", "104"}));
    std::filesystem::remove(out_path);
}

// Reject path: submit with no coordinator listening. Verifies that the API
// returns a SubmitResult with reject_message instead of throwing,
// which is what applications need for clean error reporting.
TEST(ApplicationMode, JobSubmitterReportsConnectFailureCleanly) {
    application::JobSubmitter submitter("127.0.0.1", 1);  // port 1 = unreachable
    const auto result = submitter.submit("{\"ops\":[]}", {}, {});
    EXPECT_FALSE(result.ok);
    EXPECT_FALSE(result.completed);
    EXPECT_FALSE(result.reject_message.empty());
}
