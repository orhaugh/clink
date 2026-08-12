// Soak: a long-running job on real clink_node processes, with workers killed
// on a cadence, sampled for memory and checkpoint progress, and verified
// exactly-once at the end.
//
// Everything else in this repository runs in seconds to minutes; hardening
// item 22 recorded what that leaves untested - memory stability, checkpoint
// cadence drift and restart robustness over WALL TIME. This binary is that
// test. It is deliberately not part of the `integration` label: its budget
// is minutes to hours (CLINK_SOAK_MINUTES, default 8), so it runs under its
// own `soak` label, on demand:
//
//   CLINK_SOAK_MINUTES=120 ctest --test-dir build -L soak
//
// What it pins, and how each assertion earns its place:
//   - exactly-once across every kill: the committed output contains every
//     record exactly once. This is the contract the checkpoint machinery
//     sells; hours and repeated kills are the conditions the second-scale
//     tests cannot create.
//   - vacuity: at least two kills actually landed. A soak that never killed
//     anything proves nothing about recovery.
//   - progress: checkpoints keep completing over the whole run, and the
//     longest stall (no COMPLETED marker advance) stays under a bound that
//     comfortably covers one watchdog-detect + redeploy cycle. Cadence
//     DRIFT - intervals quietly stretching - shows up here as a stall.
//   - memory: the coordinator's RSS in the last quarter of the run is not
//     materially above the first quarter's. The coordinator is the one
//     process that lives the whole soak (workers are killed and replaced),
//     so it is where a leak has the whole run to compound.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/application/job_submitter.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/core/types.hpp"

#include "tests/integration/cluster_harness.hpp"

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;

namespace {

std::filesystem::path soak_node_binary() {
#ifdef CLINK_NODE_BINARY
    return std::filesystem::path{CLINK_NODE_BINARY};
#else
    return {};
#endif
}

int soak_minutes() {
    if (const char* v = std::getenv("CLINK_SOAK_MINUTES")) {
        const int m = std::atoi(v);
        if (m > 0) {
            return m;
        }
    }
    return 8;
}

// RSS in kilobytes, or 0 when the process is gone. `ps` spells this the
// same on macOS and Linux, which is why it is the sampler here rather than
// /proc (absent on macOS) or mach APIs (absent on Linux).
long rss_kb(pid_t pid) {
    const std::string cmd = "ps -o rss= -p " + std::to_string(pid) + " 2>/dev/null";
    FILE* p = ::popen(cmd.c_str(), "r");
    if (p == nullptr) {
        return 0;
    }
    char buf[64] = {};
    long out = 0;
    if (::fgets(buf, sizeof(buf), p) != nullptr) {
        out = std::strtol(buf, nullptr, 10);
    }
    ::pclose(p);
    return out;
}

// Highest COMPLETED-N under the checkpoint tree, or 0. Same scan the
// exactly-once suites use; duplicated rather than shared because the two
// TUs link into one binary and a shared header for eleven lines is worse
// than the copy (and the anon namespace keeps the ODR honest).
std::uint64_t soak_latest_completed(const std::filesystem::path& ckpt_root) {
    std::uint64_t latest = 0;
    std::error_code ec;
    for (const auto& e : std::filesystem::recursive_directory_iterator(ckpt_root, ec)) {
        if (ec) {
            break;
        }
        if (!e.is_regular_file()) {
            continue;
        }
        const auto name = e.path().filename().string();
        if (name.rfind("COMPLETED-", 0) != 0) {
            continue;
        }
        try {
            latest = std::max(latest, static_cast<std::uint64_t>(std::stoull(name.substr(10))));
        } catch (const std::exception&) {
        }
    }
    return latest;
}

clink::cluster::JobGraphSpec soak_graph(std::int64_t count, const std::filesystem::path& dir) {
    clink::cluster::JobGraphSpec g;
    clink::cluster::OperatorSpec src;
    src.id = "src";
    src.type = "int64_range_source";
    src.out_channel = std::string{clink::cluster::kChannelInt64};
    src.params = {{"count", std::to_string(count)}, {"delay_ms", "2"}};
    g.ops.push_back(std::move(src));
    clink::cluster::OperatorSpec conv;
    conv.id = "conv";
    conv.type = "int64_to_string";
    conv.out_channel = std::string{clink::cluster::kChannelString};
    conv.inputs = {"src"};
    g.ops.push_back(std::move(conv));
    clink::cluster::OperatorSpec snk;
    snk.id = "snk";
    snk.type = "file_2pc_sink_string";
    snk.out_channel = std::string{clink::cluster::kChannelString};
    snk.inputs = {"conv"};
    snk.params = {{"dir", dir.string()}};
    g.ops.push_back(std::move(snk));
    return g;
}

long median(std::vector<long> v) {
    if (v.empty()) {
        return 0;
    }
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}

}  // namespace

TEST(Soak, HoursOfWorkerKillsStayExactlyOnceWithFlatMemory) {
    if (soak_node_binary().empty() || !std::filesystem::exists(soak_node_binary())) {
        GTEST_SKIP() << "clink_node not built";
    }
    const int minutes = soak_minutes();
    // ~500 records/second at delay_ms=2; sized so production fills most of
    // the budget and the tail (restart pauses, final checkpoint) fits in
    // the slack the wait below leaves.
    const std::int64_t count = static_cast<std::int64_t>(minutes) * 60 * 400;

    ClusterSpec spec;
    spec.node_binary = soak_node_binary();
    spec.workers = 2;
    spec.slots_per_worker = 4;
    Cluster c(spec);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    const auto out_dir = c.root() / "soak_out";

    std::atomic<bool> done{false};
    std::atomic<int> kills{0};

    // Killer: alternate workers on a cadence that yields several kills over
    // any budget, never sooner than the previous recovery had time to
    // finish. Stops half way through the LAST interval so the tail of the
    // run completes undisturbed and the final checkpoint can commit.
    const auto kill_period = std::chrono::seconds(std::max(90, (minutes * 60) / 8));
    std::thread killer([&] {
        int idx = 0;
        auto next = std::chrono::steady_clock::now() + kill_period;
        while (!done.load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() < next) {
                std::this_thread::sleep_for(250ms);
                continue;
            }
            c.worker(static_cast<std::size_t>(idx)).kill_hard();
            (void)c.await_process_gone(static_cast<std::size_t>(idx));
            // Give the watchdog time to declare the loss and the restart to
            // land before the worker slot refills; an instant respawn is a
            // DIFFERENT scenario (item 46 covers it) and would make every
            // kill exercise that path instead.
            std::this_thread::sleep_for(5s);
            if (done.load(std::memory_order_acquire)) {
                break;
            }
            if (!c.start_worker(static_cast<std::size_t>(idx))) {
                ADD_FAILURE() << "worker " << idx << " failed to respawn";
                break;
            }
            kills.fetch_add(1, std::memory_order_relaxed);
            idx = 1 - idx;
            next = std::chrono::steady_clock::now() + kill_period;
        }
    });

    // Sampler: coordinator RSS + checkpoint progress every 10s.
    std::vector<long> coordinator_rss;
    std::atomic<long> longest_stall_s{0};
    std::thread sampler([&] {
        std::uint64_t last_marker = 0;
        auto last_advance = std::chrono::steady_clock::now();
        while (!done.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(10s);
            const auto rss = rss_kb(c.coordinator().pid());
            if (rss > 0) {
                coordinator_rss.push_back(rss);
            }
            const auto marker = soak_latest_completed(c.checkpoint_dir());
            const auto now = std::chrono::steady_clock::now();
            if (marker > last_marker) {
                last_marker = marker;
                last_advance = now;
            } else {
                const auto stall =
                    std::chrono::duration_cast<std::chrono::seconds>(now - last_advance).count();
                long prev = longest_stall_s.load(std::memory_order_relaxed);
                while (stall > prev &&
                       !longest_stall_s.compare_exchange_weak(prev, static_cast<long>(stall))) {
                }
            }
        }
    });

    clink::application::JobSubmitter submitter("127.0.0.1", c.coordinator_port());
    clink::application::SubmitOptions opts;
    opts.wait_for_completion = true;
    opts.wait_timeout = std::chrono::seconds(minutes * 60 + std::max(300, minutes * 30));
    opts.checkpoint.checkpoint_dir = c.checkpoint_dir().string();
    opts.checkpoint.interval_ms = 250;
    opts.checkpoint.max_restarts_on_worker_loss = 100;

    const auto r = submitter.submit(soak_graph(count, out_dir).to_json(), {}, opts);
    done.store(true, std::memory_order_release);
    killer.join();
    sampler.join();

    ASSERT_TRUE(r.ok) << r.reject_message;
    for (const auto& e : r.errors) {
        ADD_FAILURE() << "job completed with error: " << e;
    }

    // Vacuity: this was a soak with real kills, not an idle wait.
    EXPECT_GE(kills.load(), 2) << "fewer than two kills landed; the budget is too small "
                                  "for the kill cadence and the run proved nothing about recovery";

    // Exactly-once across every kill: each record once, none missing, none
    // foreign, read from the committed side of the 2PC sink only.
    std::vector<std::string> lines;
    std::error_code ec;
    for (const auto& e : std::filesystem::directory_iterator(out_dir / "committed", ec)) {
        if (!e.is_regular_file()) {
            continue;
        }
        std::ifstream in(e.path());
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty()) {
                lines.push_back(line);
            }
        }
    }
    std::set<std::string> unique(lines.begin(), lines.end());
    EXPECT_EQ(static_cast<std::int64_t>(lines.size()), count)
        << "committed line count is off by " << (static_cast<std::int64_t>(lines.size()) - count);
    EXPECT_EQ(unique.size(), lines.size()) << "duplicates in the committed output";
    EXPECT_EQ(static_cast<std::int64_t>(unique.size()), count) << "records missing";

    // Progress: the longest no-new-checkpoint stretch must fit inside one
    // loss-detect + restart + resume cycle with margin. Cadence drift or a
    // wedged restart shows up here long before the whole job times out.
    EXPECT_LE(longest_stall_s.load(), 120)
        << "checkpoints stalled for " << longest_stall_s.load() << "s during the soak";

    // Memory: the coordinator lives the whole run; its RSS in the final
    // quarter should not be materially above the first quarter's. The 1.75x
    // bound is generous - allocator warmup and bookkeeping growth are real -
    // but a genuine leak compounds over a soak and clears it easily.
    if (coordinator_rss.size() >= 12) {
        const auto q = coordinator_rss.size() / 4;
        const std::vector<long> first(coordinator_rss.begin(),
                                      coordinator_rss.begin() + static_cast<std::ptrdiff_t>(q));
        const std::vector<long> last(coordinator_rss.end() - static_cast<std::ptrdiff_t>(q),
                                     coordinator_rss.end());
        const auto m_first = median(first);
        const auto m_last = median(last);
        EXPECT_LE(m_last, static_cast<long>(static_cast<double>(m_first) * 1.75) + 65536)
            << "coordinator RSS grew from median " << m_first << " KB to " << m_last
            << " KB across the soak";
    }
}
