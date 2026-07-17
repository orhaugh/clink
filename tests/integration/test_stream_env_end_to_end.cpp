// End-to-end test for the fluent Pipeline API.
//
// Spawns a coordinator + worker, then drives a pipeline entirely via the typed
// builder API (no JSON file on disk, no hand-written JobGraphSpec).
// Asserts the sink received the expected records.

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

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/application/job_submitter.hpp"
#include "clink/core/codec.hpp"
#include "clink/runtime/network/network_channel.hpp"

extern char** environ;

namespace {

using namespace clink;
using namespace clink::api;
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

TEST(StreamEnvEndToEnd, FluentApiSubmitsAndRunsAgainstCluster) {
    const auto binary = node_binary_path();
    if (!std::filesystem::exists(binary)) {
        GTEST_SKIP() << "clink_node not built";
    }

    const auto coordinator_port = probe_free_port();
    const auto out_path = std::filesystem::temp_directory_path() / "clink_stream_env_e2e.txt";
    std::filesystem::remove(out_path);

    const pid_t coordinator_pid = spawn_node(
        {"clink_node", "--role=coordinator", "--port=" + std::to_string(coordinator_port)}, binary);
    ASSERT_GT(coordinator_pid, 0);
    std::this_thread::sleep_for(200ms);

    const pid_t worker_pid = spawn_node({"clink_node",
                                         "--role=worker",
                                         "--id=worker-e2e",
                                         "--coordinator-host=127.0.0.1",
                                         "--coordinator-port=" + std::to_string(coordinator_port)},
                                        binary);
    ASSERT_GT(worker_pid, 0);
    std::this_thread::sleep_for(300ms);

    // Build the pipeline using only the fluent API. No JSON anywhere.
    auto env = Pipeline::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(4).start(50).build());
    src.transform<std::int64_t>("multiply_int64", {{"factor", "2"}})
        .sink(FileInt64Sink::builder().path(out_path.string()).build());

    application::JobSubmitter submitter("127.0.0.1", coordinator_port);
    application::SubmitOptions opts;
    opts.wait_timeout = std::chrono::seconds(15);
    const auto result = env.execute("e2e", submitter, opts);

    kill_quietly(coordinator_pid);
    kill_quietly(worker_pid);

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    std::ifstream in(out_path);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        lines.push_back(line);
    }
    // 50,51,52,53 each times 2 -> 100, 102, 104, 106
    EXPECT_EQ(lines, (std::vector<std::string>{"100", "102", "104", "106"}));
    std::filesystem::remove(out_path);
}
