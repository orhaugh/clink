// Stopping a running job gracefully, and the difference from cancelling it.
//
// Follow-up item 6. Shutdown was cancellation: SIGTERM and `clink cancel` stop
// the subtasks where they are, so every record processed since the last completed
// checkpoint is discarded and replays when the job is resubmitted. Consistent
// with the delivery guarantee, but not a drain - and an operator upgrading a job
// had no "stop cleanly here, resume from there" path.
//
// `clink stop` tells every source to stop producing and then run its
// end-of-input path: flush, take a coordinator-coordinated final checkpoint, and
// block until the sinks have committed it. Only then does the subtask report
// finished, so the job ends as a SUCCESS with its tail durable.
//
// The assertion that matters is not "the job stopped" - a cancel does that too.
// It is that the committed output after a stop is a PREFIX of the input with
// nothing missing from it, and that the reported checkpoint id is real. A stop
// that quietly dropped the in-flight records would still look like a clean stop
// from the outside.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "tests/integration/await_port.hpp"
#include "tests/integration/cluster_harness.hpp"

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::Process;
using clink::itest::ScopedDiagnostics;

namespace {

std::filesystem::path node_binary() {
#ifdef CLINK_NODE_BINARY
    return std::filesystem::path{CLINK_NODE_BINARY};
#else
    return {};
#endif
}

std::filesystem::path submit_binary() {
#ifdef CLINK_SUBMIT_BINARY
    return std::filesystem::path{CLINK_SUBMIT_BINARY};
#else
    return {};
#endif
}

std::filesystem::path cli_binary() {
#ifdef CLINK_CLI_BINARY
    return std::filesystem::path{CLINK_CLI_BINARY};
#else
    return {};
#endif
}

std::filesystem::path stop_job_so() {
#ifdef CLINK_RESCALE_XO_JOB_PATH
    return std::filesystem::path{CLINK_RESCALE_XO_JOB_PATH};
#else
    return {};
#endif
}

// The rescale job is reused deliberately: it has a replayable source that
// checkpoints its offset, a keyed operator that reports state mismatches as
// visible output, and a 2PC sink whose committed files can be compared as a
// multiset. That is exactly the instrumentation a stop needs, and duplicating it
// would mean two jobs to keep in step.
constexpr int kTotalRecords = 400;
constexpr int kTickMs = 40;
constexpr int kKeys = 8;
// Stop once this much has been published, so there is real accumulated state and
// a real in-flight tail rather than an empty job.
constexpr unsigned kMinCommittedBeforeStop = 40;

std::vector<std::string> committed_records(const std::filesystem::path& out_dir) {
    std::vector<std::string> lines;
    const auto dir = out_dir / "committed";
    std::error_code ec;
    if (!std::filesystem::exists(dir, ec)) {
        return lines;
    }
    for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        std::ifstream in(entry.path());
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty()) {
                lines.push_back(line);
            }
        }
    }
    return lines;
}

// What a stopped job's output must look like: every record it committed is one
// the source emitted, none twice, and the set has no HOLES - if record-N is
// present then so is every record below it. A stop is allowed to publish less
// than the whole input (that is the point: it stopped), but it is not allowed to
// skip.
struct PrefixCheck {
    std::size_t count{};
    std::vector<std::string> duplicated;
    std::vector<std::string> unexpected;
    std::vector<int> holes;
    int highest{-1};
};

PrefixCheck check_prefix(const std::filesystem::path& out_dir) {
    PrefixCheck v;
    const auto lines = committed_records(out_dir);
    v.count = lines.size();
    std::set<int> seen;
    for (const auto& line : lines) {
        const auto dash = line.rfind('-');
        int idx = -1;
        if (dash != std::string::npos) {
            try {
                idx = std::stoi(line.substr(dash + 1));
            } catch (const std::exception&) {
                idx = -1;
            }
        }
        if (!line.starts_with("record-") || idx < 0 || idx >= kTotalRecords) {
            v.unexpected.push_back(line);
            continue;
        }
        if (!seen.insert(idx).second) {
            v.duplicated.push_back(line);
        }
        v.highest = std::max(v.highest, idx);
    }
    for (int i = 0; i <= v.highest; ++i) {
        if (seen.count(i) == 0) {
            v.holes.push_back(i);
        }
    }
    return v;
}

std::string describe(const PrefixCheck& v) {
    std::ostringstream os;
    os << v.count << " committed, highest record-" << v.highest;
    if (!v.duplicated.empty()) {
        os << "; " << v.duplicated.size() << " DUPLICATED (" << v.duplicated.front() << " ...)";
    }
    if (!v.unexpected.empty()) {
        os << "; " << v.unexpected.size() << " UNEXPECTED (" << v.unexpected.front() << " ...)";
    }
    if (!v.holes.empty()) {
        os << "; " << v.holes.size() << " HOLES (record-" << v.holes.front() << " ...)";
    }
    return os.str();
}

class GracefulStopTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(submit_binary()) ||
            !std::filesystem::exists(stop_job_so()) || !std::filesystem::exists(cli_binary())) {
            GTEST_SKIP() << "node / submitter / job .so / cli not built";
        }
        base_ = std::filesystem::temp_directory_path() /
                ("clink_stop_" + std::to_string(::getpid()) + "_" +
                 ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(base_);
        out_dir_ = base_ / "out";
        std::filesystem::create_directories(out_dir_);
        ::setenv("CLINK_RXO_OUT", out_dir_.c_str(), 1);
        ::setenv("CLINK_RXO_TOTAL", std::to_string(kTotalRecords).c_str(), 1);
        ::setenv("CLINK_RXO_TICK_MS", std::to_string(kTickMs).c_str(), 1);
        ::setenv("CLINK_RXO_KEYS", std::to_string(kKeys).c_str(), 1);
        ::setenv("CLINK_RXO_MAX_PAR", "4", 1);
        ::setenv("CLINK_RXO_PAR", "1", 1);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(base_, ec);
    }

    static ClusterSpec spec() {
        ClusterSpec s;
        s.node_binary = node_binary();
        s.workers = 2;
        s.slots_per_worker = 4;
        return s;
    }

    std::unique_ptr<Process> submit(Cluster& c) {
        auto p = std::make_unique<Process>();
        std::vector<std::string> argv{submit_binary().string(),
                                      "--job=" + stop_job_so().string(),
                                      "--coordinator-host=127.0.0.1",
                                      "--coordinator-port=" + std::to_string(c.coordinator_port()),
                                      "--wait-timeout-s=180",
                                      "--checkpoint-dir=" + c.checkpoint_dir().string(),
                                      "--checkpoint-interval-ms=200"};
        const bool ok = p->spawn("submit", submit_binary(), std::move(argv), c.log_dir());
        return ok ? std::move(p) : nullptr;
    }

    int run_cli(Cluster& c, const std::string& subcommand, std::string* out_text) {
        std::ostringstream cmd;
        cmd << cli_binary().string() << " " << subcommand << " --job-id=1"
            << " --coordinator-host=127.0.0.1"
            << " --coordinator-port=" << c.coordinator_port() << " 2>&1";
        FILE* pipe = ::popen(cmd.str().c_str(), "r");
        if (pipe == nullptr) {
            return -1;
        }
        std::string text;
        std::array<char, 512> buf{};
        while (::fgets(buf.data(), static_cast<int>(buf.size()), pipe) != nullptr) {
            text += buf.data();
        }
        const int status = ::pclose(pipe);
        if (out_text != nullptr) {
            *out_text = text;
        }
        return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    }

    [[nodiscard]] bool await_committed(unsigned n) {
        return clink::itest::await_condition([&] { return check_prefix(out_dir_).count >= n; },
                                             std::chrono::seconds(90));
    }

    std::filesystem::path base_;
    std::filesystem::path out_dir_;
};

}  // namespace

TEST_F(GracefulStopTest, StoppingAJobCommitsItsTailAndReportsACheckpointToResumeFrom) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(await_committed(kMinCommittedBeforeStop))
        << "the job never published enough to make a stop meaningful";
    const auto before = check_prefix(out_dir_);

    std::string stop_out;
    const int rc = run_cli(c, "stop", &stop_out);
    std::cerr << "STOP-OUTCOME rc=" << rc << ": " << stop_out;

    // The submitter must exit CLEANLY. A cancel makes it exit non-zero; the whole
    // point of a stop is that the job ends as a success.
    const auto exit_code = sub->await_exit(std::chrono::seconds(180));
    const auto after = check_prefix(out_dir_);
    const std::string context = "stop rc=" + std::to_string(rc) + " reply=" + stop_out +
                                " | before: " + describe(before) + " | after: " + describe(after);

    ASSERT_EQ(rc, 0) << "the stop was refused. " << context;
    EXPECT_NE(stop_out.find("ok=1"), std::string::npos) << context;
    ASSERT_TRUE(exit_code.has_value()) << "submitter never exited. " << context;
    EXPECT_EQ(*exit_code, 0)
        << "the submitter reported failure after a GRACEFUL stop. A stop must finish the job as a "
           "success - that is what distinguishes it from a cancel. "
        << context;

    // The coordinator's own account of it.
    EXPECT_TRUE(c.coordinator().log_contains("graceful stop requested"))
        << "the coordinator did not log the stop. " << context;
    EXPECT_TRUE(c.coordinator().log_contains("stopped cleanly at checkpoint"))
        << "the coordinator did not report a clean stop. " << context;

    // The reported checkpoint id must be real and non-zero: it is what an
    // operator resubmits from, and a zero would silently mean "start over".
    const auto pos = stop_out.find("savepoint_checkpoint_id=");
    ASSERT_NE(pos, std::string::npos) << context;
    const auto id =
        std::stoull(stop_out.substr(pos + std::string("savepoint_checkpoint_id=").size()));
    EXPECT_GT(id, 0u) << "the stop reported checkpoint 0, which is not a point to resume from. "
                      << context;

    // THE ASSERTION. The output is a prefix of the input: no duplicates, no
    // records the source never emitted, and no HOLES. A stop that discarded its
    // in-flight tail instead of committing it would leave a hole, or would leave
    // the highest committed record below where the checkpoint says it got to.
    EXPECT_TRUE(after.duplicated.empty()) << "the stop duplicated committed records. " << context;
    EXPECT_TRUE(after.unexpected.empty())
        << "output contains records the source never emitted; a STATE-MISMATCH here is the keyed "
           "operator reporting its own per-key counter disagreed. "
        << context;
    EXPECT_TRUE(after.holes.empty())
        << "the committed output has holes, so the stop published some records and dropped others "
           "below them. "
        << context;

    // And it must have committed MORE than it had at the moment of the request -
    // the tail. Without this the test would pass on a stop that published
    // nothing further, which is a cancel.
    EXPECT_GT(after.count, before.count)
        << "the stop committed nothing beyond what was already published, so it discarded the "
           "in-flight tail rather than draining it. "
        << context;

    // And materially LESS than the whole input, or the job simply ran to
    // completion and the stop did nothing.
    //
    // This is the assertion that makes the test a test. The source is bounded, so
    // a stop that never reached the produce loop is indistinguishable from one
    // that worked - the job finishes either way, the submitter exits 0, the final
    // checkpoint is taken at end-of-input, and every other assertion here still
    // holds. Mutation-checked: removing the stop check from the source runner is
    // caught by this line and nothing else.
    EXPECT_LT(after.count, static_cast<std::size_t>(kTotalRecords))
        << "the job published its entire input, so it ran to completion and the stop had no "
           "effect - the assertions above would look identical either way. "
        << context;
}

// The contrast, asserted rather than described: a cancel does NOT drain, and the
// submitter reports failure. Kept next to the stop test because the two together
// are the reason both commands exist - and because if a future change made cancel
// drain too, one of these would fail and say so.
TEST_F(GracefulStopTest, CancellingTheSameJobIsAbruptAndReportsFailure) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(await_committed(kMinCommittedBeforeStop));

    std::string cancel_out;
    const int rc = run_cli(c, "cancel", &cancel_out);
    std::cerr << "CANCEL-OUTCOME rc=" << rc << ": " << cancel_out;

    const auto exit_code = sub->await_exit(std::chrono::seconds(180));
    const auto after = check_prefix(out_dir_);
    const std::string context =
        "cancel rc=" + std::to_string(rc) + " reply=" + cancel_out + " | " + describe(after);

    ASSERT_TRUE(exit_code.has_value()) << "submitter never exited. " << context;
    EXPECT_NE(*exit_code, 0)
        << "a CANCELLED job reported success. Cancel is the abrupt path and must stay "
           "distinguishable from stop, or an operator cannot tell which they got. "
        << context;

    // A cancel still must not corrupt what it had already published. It may
    // publish less than a stop; it may not publish something wrong.
    EXPECT_TRUE(after.duplicated.empty()) << "a cancel duplicated committed records. " << context;
    EXPECT_TRUE(after.unexpected.empty()) << context;
    EXPECT_TRUE(after.holes.empty()) << "a cancel left holes in the committed output. " << context;
}
