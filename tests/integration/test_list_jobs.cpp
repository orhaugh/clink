// End-to-end test for JobSubmitter::list_jobs() against a live coordinator.
//
// Spawns coordinator + worker, submits a small job, and after it completes calls
// list_jobs() to confirm the coordinator is tracking the job and the snapshot
// reflects the completion state.

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

TEST(ListJobs, SubmitterCanEnumerateRunningAndCompletedJobs) {
    const auto binary = node_binary_path();
    if (!std::filesystem::exists(binary)) {
        GTEST_SKIP() << "clink_node not built";
    }

    const auto coordinator_port = probe_free_port();
    const auto out_path = std::filesystem::temp_directory_path() / "clink_list_jobs_test.txt";
    std::filesystem::remove(out_path);

    const pid_t coordinator_pid = spawn_node(
        {"clink_node", "--role=coordinator", "--port=" + std::to_string(coordinator_port)}, binary);
    ASSERT_GT(coordinator_pid, 0);
    std::this_thread::sleep_for(200ms);

    const pid_t worker_pid = spawn_node({"clink_node",
                                         "--role=worker",
                                         "--id=worker-list-jobs",
                                         "--coordinator-host=127.0.0.1",
                                         "--coordinator-port=" + std::to_string(coordinator_port)},
                                        binary);
    ASSERT_GT(worker_pid, 0);
    std::this_thread::sleep_for(300ms);

    application::JobSubmitter submitter("127.0.0.1", coordinator_port);

    // Initially: no jobs.
    {
        const auto listing = submitter.list_jobs();
        ASSERT_TRUE(listing.ok) << listing.error;
        EXPECT_TRUE(listing.jobs.empty());
    }

    // Submit a small job and wait for completion.
    JobGraphSpec graph;
    OperatorSpec src;
    src.id = "src";
    src.type = "int64_range_source";
    src.out_channel = "int64";
    src.params["count"] = "3";
    src.params["start"] = "1";
    graph.ops.push_back(std::move(src));
    OperatorSpec snk;
    snk.id = "snk";
    snk.type = "file_int64_sink";
    snk.inputs = {"src"};
    snk.out_channel = "int64";
    snk.params["path"] = out_path.string();
    graph.ops.push_back(std::move(snk));

    application::SubmitOptions opts;
    opts.wait_timeout = std::chrono::seconds(15);
    const auto submit_res = submitter.submit(graph.to_json(), {}, opts);
    ASSERT_TRUE(submit_res.completed) << submit_res.reject_message;
    EXPECT_TRUE(submit_res.ok);

    // After completion, list_jobs should show the job with
    // completion_signalled = true. The coordinator doesn't prune the record
    // immediately.
    {
        const auto listing = submitter.list_jobs();
        ASSERT_TRUE(listing.ok) << listing.error;
        ASSERT_EQ(listing.jobs.size(), 1u);
        EXPECT_EQ(listing.jobs[0].job_id, submit_res.job_id);
        EXPECT_TRUE(listing.jobs[0].completion_signalled);
        EXPECT_EQ(listing.jobs[0].completed_subtasks, listing.jobs[0].total_subtasks);
        EXPECT_GT(listing.jobs[0].total_subtasks, 0u);
    }

    kill_quietly(coordinator_pid);
    kill_quietly(worker_pid);
    std::filesystem::remove(out_path);
}

TEST(ListJobs, FailsCleanlyWhenCoordinatorUnreachable) {
    application::JobSubmitter submitter("127.0.0.1", 1);  // unreachable
    const auto listing = submitter.list_jobs();
    EXPECT_FALSE(listing.ok);
    EXPECT_FALSE(listing.error.empty());
    EXPECT_TRUE(listing.jobs.empty());
}
