// Heavy-duty integration test: 1 coordinator + 3 workers (separate processes,
// real networking) running a CLINK_REGISTER_JOB pipeline with
// custom typed channels.
//
// Pipeline (defined in examples/heavy_pipeline_job.cpp):
//   from_elements<Customer>(99) -> map<Order> -> key_by(region)
//     -> reduce(per-region sum + count) -> map<string> -> sink(par=3)
//
// The sink runs at parallelism=3, so the final mapped strings are
// scattered across <out>.0, <out>.1, <out>.2 via Rebalance routing
// (cross-worker wire). Reduce emits the running accumulator on every
// input, so each region has a sequence of partial sums in the
// outputs; the row with the highest count per region carries the
// final per-region total.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <signal.h>
#include <spawn.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>
#include <sys/wait.h>

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

std::filesystem::path submit_binary_path() {
#ifdef CLINK_SUBMIT_BINARY
    return std::filesystem::path{CLINK_SUBMIT_BINARY};
#else
    return {};
#endif
}

std::filesystem::path heavy_job_path() {
#ifdef CLINK_HEAVY_JOB_PATH
    return std::filesystem::path{CLINK_HEAVY_JOB_PATH};
#else
    return {};
#endif
}

pid_t spawn_proc(const std::vector<std::string>& argv, const std::filesystem::path& binary_path) {
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

bool wait_for(pid_t pid, std::chrono::milliseconds timeout, int& exit_code) {
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

struct RegionTotal {
    std::int64_t total_amount{0};
    std::int64_t count{0};
};

// Parse one "<region>|<total>|<count>" line emitted by the pipeline's
// final map. Returns nullopt on malformed lines so the test can
// surface them with a clear error rather than silently dropping.
std::optional<std::pair<std::string, RegionTotal>> parse_line(const std::string& line) {
    const auto p1 = line.find('|');
    if (p1 == std::string::npos) {
        return std::nullopt;
    }
    const auto p2 = line.find('|', p1 + 1);
    if (p2 == std::string::npos) {
        return std::nullopt;
    }
    RegionTotal rt;
    try {
        rt.total_amount = std::stoll(line.substr(p1 + 1, p2 - p1 - 1));
        rt.count = std::stoll(line.substr(p2 + 1));
    } catch (...) {
        return std::nullopt;
    }
    return std::make_pair(line.substr(0, p1), rt);
}

}  // namespace

TEST(HeavyPipelineE2E, KeyByReduceSinkAcrossThreeWorkers) {
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto submit = submit_binary_path();
    if (!std::filesystem::exists(submit)) {
        GTEST_SKIP() << "clink_submit_job not built";
    }
    const auto job_so = heavy_job_path();
    if (!std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "heavy_pipeline_job.so not built";
    }

    const auto tmpdir = std::filesystem::temp_directory_path();
    const auto out_base = tmpdir / "clink_heavy_e2e_out";
    for (int i = 0; i < 3; ++i) {
        std::filesystem::remove(std::filesystem::path{out_base.string() + "." + std::to_string(i)});
    }
    // The job .so reads CLINK_HEAVY_OUT_BASE at build_fn time
    // (under std::call_once). Set BEFORE spawning the cluster so the
    // coordinator + workers all inherit it through their environments.
    ::setenv("CLINK_HEAVY_OUT_BASE", out_base.c_str(), 1);

    const auto coordinator_port = probe_free_port();
    const pid_t coordinator_pid = spawn_proc(
        {"clink_node", "--role=coordinator", "--port=" + std::to_string(coordinator_port)}, node);
    ASSERT_GT(coordinator_pid, 0);
    std::this_thread::sleep_for(200ms);

    // 3 workers, each with --slots=3. Pipeline needs 1+1+1+1+3 = 7 slots;
    // 9 across the cluster means the deploy spreads work across all
    // three workers (placement is greedy first-fit, slot_capacity=3 forces
    // the planner to pick a new worker after the third subtask).
    std::vector<pid_t> workers;
    for (int i = 1; i <= 3; ++i) {
        workers.push_back(spawn_proc({"clink_node",
                                      "--role=worker",
                                      "--id=worker-heavy-" + std::to_string(i),
                                      "--slots=3",
                                      "--coordinator-host=127.0.0.1",
                                      "--coordinator-port=" + std::to_string(coordinator_port)},
                                     node));
        ASSERT_GT(workers.back(), 0);
    }
    std::this_thread::sleep_for(400ms);

    const pid_t submit_pid = spawn_proc({"clink_submit_job",
                                         "--job=" + job_so.string(),
                                         "--coordinator-host=127.0.0.1",
                                         "--coordinator-port=" + std::to_string(coordinator_port),
                                         "--wait-timeout-s=45",
                                         "--name=heavy-pipeline"},
                                        submit);
    ASSERT_GT(submit_pid, 0);

    int submit_exit = -1;
    const bool exited = wait_for(submit_pid, 60s, submit_exit);

    kill_quietly(coordinator_pid);
    for (auto pid : workers) {
        kill_quietly(pid);
    }

    ASSERT_TRUE(exited) << "clink_submit_job did not exit within 60s";
    EXPECT_EQ(submit_exit, 0) << "clink_submit_job exited non-zero (" << submit_exit << ")";

    // Collect every row from all three sink subtask files.
    std::vector<std::string> all_lines;
    for (int i = 0; i < 3; ++i) {
        const auto p = std::filesystem::path{out_base.string() + "." + std::to_string(i)};
        std::ifstream in(p);
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty()) {
                all_lines.push_back(std::move(line));
            }
        }
    }

    // Reduce emits the running accumulator on every input record, so
    // the sink sees 99 rows total (one per customer). Verify no
    // records were dropped on the wire.
    ASSERT_EQ(all_lines.size(), 99u)
        << "expected 99 sink rows (one per input record); got " << all_lines.size();

    // Group by region, keeping the row with the highest count seen
    // per region - that's the final running total for that region.
    std::unordered_map<std::string, RegionTotal> final_per_region;
    for (const auto& line : all_lines) {
        auto parsed = parse_line(line);
        ASSERT_TRUE(parsed.has_value()) << "malformed sink row: '" << line << "'";
        const auto& [region, rt] = *parsed;
        auto it = final_per_region.find(region);
        if (it == final_per_region.end() || it->second.count < rt.count) {
            final_per_region[region] = rt;
        }
    }

    // Expected per-region totals (kept in lockstep with make_customers()
    // in examples/heavy_pipeline_job.cpp):
    //   NA   : ids 0..29   amount = id*10         -> sum 4350, count 30
    //   EU   : ids 30..59  amount = (id-30)*20    -> sum 8700, count 30
    //   ASIA : ids 60..84  amount = (id-60)*5+100 -> sum 4000, count 25
    //   SA   : ids 85..98  amount = (id-85)*7+50  -> sum 1337, count 14
    ASSERT_EQ(final_per_region.size(), 4u)
        << "expected 4 distinct regions in output; got " << final_per_region.size();
    EXPECT_EQ(final_per_region["NA"].total_amount, 4350);
    EXPECT_EQ(final_per_region["NA"].count, 30);
    EXPECT_EQ(final_per_region["EU"].total_amount, 8700);
    EXPECT_EQ(final_per_region["EU"].count, 30);
    EXPECT_EQ(final_per_region["ASIA"].total_amount, 4000);
    EXPECT_EQ(final_per_region["ASIA"].count, 25);
    EXPECT_EQ(final_per_region["SA"].total_amount, 1337);
    EXPECT_EQ(final_per_region["SA"].count, 14);

    for (int i = 0; i < 3; ++i) {
        std::filesystem::remove(std::filesystem::path{out_base.string() + "." + std::to_string(i)});
    }
}
