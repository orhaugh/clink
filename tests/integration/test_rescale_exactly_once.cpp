// Exactly-once output across a PER-OPERATOR rescale.
//
// Follow-up item 2. The method is the one that earned its keep on worker loss
// and coordinator failover: judge the job by its COMMITTED OUTPUT - the full
// record multiset, checked for duplicates, gaps and records the source never
// emitted - rather than by whether the rescale was accepted or the job ran
// again. F34 was found precisely because the output disagreed while every
// liveness assertion passed.
//
// Rescale is the scenario this had not covered, and it is the one that moves
// key-group state between subtasks - the arithmetic F38 showed was being
// computed wrongly, where a keyed operator was handed a fraction of the key
// space and silently discarded the rest at restore.
//
// `test_coordinator_rescale.cpp` records why this could not be asserted
// before, and names the two prerequisites: an op-level rescale path, and a
// replay-correct source. Both exist now, so this drives:
//
//   examples/rescale_exactly_once_job.cpp
//     replayable source (checkpointed offset)
//       -> keyed, self-checking counter, rescalable [1, max]
//       -> file 2PC sink (committed output is what gets compared)
//
// The operator checks its own per-key state and emits a STATE-MISMATCH record
// when the counter disagrees with what the record's index implies. Those are
// not in the expected multiset, so they surface here as `unexpected` output
// naming the key - state loss becomes an attributable record rather than a
// silent difference.
//
// What is deliberately NOT asserted: that the rescale is accepted. A refusal
// is reported, and the run still has to produce correct output either way -
// a rescale that is declined must not corrupt anything. Both outcomes are
// distinguished in the failure messages so a red test says which happened.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_planner.hpp"

#include "tests/integration/cluster_harness.hpp"

namespace {

using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::Process;
using clink::itest::ScopedDiagnostics;

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
std::filesystem::path rescale_job_so() {
#ifdef CLINK_RESCALE_XO_JOB_PATH
    return std::filesystem::path{CLINK_RESCALE_XO_JOB_PATH};
#else
    return {};
#endif
}

// Deliberately many keys relative to the record count, and the reasoning
// matters: a keyed test with FEW keys can miss a key-space partitioning defect
// entirely. Four keys occupy four key groups out of 128, and a defect that
// hands an operator only a slice of the space may happen to include all four -
// mutation-checked, and it did. Spreading keys widely makes a range-restricted
// restore drop some of them.
//
// Records per key still has to exceed one, or the operator's counter check is
// vacuous (expected_before is 0 for every record and a lost counter reads as
// correct). 96 records over 12 keys is 8 per key.
//
// The record count and tick together set how long the job LIVES, and that turned
// out to be the thing the whole test hinges on. A rescale is not synchronous
// with its request: the coordinator accepts, waits for the next checkpoint to
// complete, dispatches the drain, waits for every old subtask to finish, then
// deploys the new ones. At 96 records x 25ms the job ran for 2.4s and simply
// ended first, so the run reported an accepted rescale that never happened - and
// passed with the rescale path's key-group arithmetic deliberately broken. 240
// records at 125ms gives the job about 30 seconds, comfortably longer than the
// cutover takes, and 20 records per key.
constexpr int kTotalRecords = 240;
constexpr int kTickMs = 125;
constexpr int kKeys = 12;
constexpr int kMaxParallelism = 4;

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

struct OutputVerdict {
    std::vector<std::string> duplicated;
    std::vector<std::string> missing;
    std::vector<std::string> unexpected;  // includes the operator's STATE-MISMATCH markers
    std::size_t total_lines{0};
};

OutputVerdict verify_exactly_once(const std::filesystem::path& out_dir, int total) {
    OutputVerdict v;
    std::map<std::string, int> seen;
    for (const auto& line : committed_records(out_dir)) {
        ++seen[line];
        ++v.total_lines;
    }
    for (int i = 0; i < total; ++i) {
        const auto want = "record-" + std::to_string(i);
        const auto it = seen.find(want);
        if (it == seen.end()) {
            v.missing.push_back(want);
        } else if (it->second > 1) {
            v.duplicated.push_back(want + " x" + std::to_string(it->second));
        }
        seen.erase(want);
    }
    for (const auto& [line, count] : seen) {
        v.unexpected.push_back(line + " x" + std::to_string(count));
    }
    return v;
}

std::string describe(const OutputVerdict& v) {
    std::ostringstream os;
    os << v.total_lines << " committed lines";
    const auto list = [&os](const char* label, const std::vector<std::string>& xs) {
        if (xs.empty()) {
            return;
        }
        os << "; " << xs.size() << " " << label << ": ";
        for (std::size_t i = 0; i < xs.size() && i < 8; ++i) {
            os << (i ? ", " : "") << xs[i];
        }
        if (xs.size() > 8) {
            os << ", ... (+" << (xs.size() - 8) << ")";
        }
    };
    list("MISSING", v.missing);
    list("DUPLICATED", v.duplicated);
    list("UNEXPECTED", v.unexpected);
    return os.str();
}

class RescaleExactlyOnceTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(submit_binary()) ||
            !std::filesystem::exists(cli_binary()) || !std::filesystem::exists(rescale_job_so())) {
            GTEST_SKIP() << "cluster binaries or the rescale job are not built";
        }
        base_ = std::filesystem::temp_directory_path() /
                ("clink_rxo_" + std::to_string(::getpid()) + "_" +
                 ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(base_);
        out_dir_ = base_ / "out";
        std::filesystem::create_directories(out_dir_);
        ::setenv("CLINK_RXO_OUT", out_dir_.c_str(), 1);
        ::setenv("CLINK_RXO_TOTAL", std::to_string(kTotalRecords).c_str(), 1);
        ::setenv("CLINK_RXO_TICK_MS", std::to_string(kTickMs).c_str(), 1);
        ::setenv("CLINK_RXO_KEYS", std::to_string(kKeys).c_str(), 1);
        ::setenv("CLINK_RXO_MAX_PAR", std::to_string(kMaxParallelism).c_str(), 1);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(base_, ec);
    }

    static ClusterSpec spec() {
        ClusterSpec s;
        s.node_binary = node_binary();
        s.workers = 2;
        s.slots_per_worker = 4;  // room for the operator to grow to kMaxParallelism
        return s;
    }

    std::unique_ptr<Process> submit(Cluster& c) {
        auto p = std::make_unique<Process>();
        std::vector<std::string> argv{submit_binary().string(),
                                      "--job=" + rescale_job_so().string(),
                                      "--coordinator-host=127.0.0.1",
                                      "--coordinator-port=" + std::to_string(c.coordinator_port()),
                                      "--wait-timeout-s=120",
                                      "--checkpoint-dir=" + c.checkpoint_dir().string(),
                                      // Periodic checkpointing is REQUIRED for an operator rescale:
                                      // request_operator_rescale refuses without it, because the
                                      // rescale would wait in Preparing forever for a checkpoint
                                      // that never lands.
                                      "--checkpoint-interval-ms=200",
                                      "--max-restarts-on-worker-loss=2"};
        const bool ok = p->spawn("submit", submit_binary(), std::move(argv), c.log_dir());
        return ok ? std::move(p) : nullptr;
    }

    // Ask for the operator rescale over the real CLI. Returns the exit code
    // and captures the output, so a refusal can be reported with its reason
    // rather than as a bare number.
    int run_cli(Cluster& c, const std::string& subcommand_and_flags, std::string* out_text) {
        std::ostringstream cmd;
        cmd << cli_binary().string() << " " << subcommand_and_flags
            << " --job-id=1"  // first job on a fresh coordinator
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

    // Per-operator rescale: `clink rescale-op --op=<operator id>`.
    int rescale_operator(Cluster& c,
                         const std::string& op_id,
                         int parallelism,
                         std::string* out_text) {
        return run_cli(c,
                       "rescale-op --op=" + op_id + " --parallelism=" + std::to_string(parallelism),
                       out_text);
    }

    // Whole-role rescale: `clink rescale --role=__clink_subtask`. Every clink
    // subtask carries that one shared role (job_planner.cpp), so this resizes
    // the job's whole subtask population rather than one operator.
    int rescale_whole_role(Cluster& c, int parallelism, std::string* out_text) {
        return run_cli(c,
                       "rescale --role=" + std::string{clink::cluster::kGenericSubtaskRole} +
                           " --parallelism=" + std::to_string(parallelism),
                       out_text);
    }

    std::filesystem::path base_;
    std::filesystem::path out_dir_;
};

}  // namespace

// The premise. Without it a failure below could be the job, the keyed
// operator's self-check, or the 2PC sink rather than the rescale.
TEST_F(RescaleExactlyOnceTest, TheJobCommitsEveryRecordExactlyOnceWithNoRescale) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c);
    ASSERT_NE(sub, nullptr);
    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value()) << "submitter never exited";

    const auto v = verify_exactly_once(out_dir_, kTotalRecords);
    EXPECT_TRUE(v.missing.empty()) << "a CLEAN run did not commit every record: " << describe(v);
    EXPECT_TRUE(v.duplicated.empty()) << "a CLEAN run duplicated output: " << describe(v);
    EXPECT_TRUE(v.unexpected.empty())
        << "a CLEAN run produced records the source never emitted - a STATE-MISMATCH here means "
           "the operator's own per-key check failed without any rescale: "
        << describe(v);
}

// Per-operator rescale: `clink rescale-op --op=counter --parallelism=4`.
//
// This case was written to demonstrate exactly-once ACROSS a per-operator
// rescale. It cannot, because the rescale does not happen, and finding that out
// is what the test now pins.
//
// The coordinator used to accept the request (`ok=1 accepted_target=4`) and then
// do nothing: every step after the accept addresses the operator's subtasks by
// matching DeploymentTask::role against the operator id, while the planner gives
// every subtask the one shared role "__clink_subtask". BeginRescale went to
// zero workers, the operator sat in Draining until the job ended, and no log line
// said so. The first version of this test passed on that - it waited for output
// equality after an accepted request, and got it, because nothing had been
// resized. It also passed with the rescale path's key-group arithmetic
// deliberately broken, three separate ways.
//
// So the contract asserted here is the honest one: a request the coordinator
// cannot execute is REFUSED, with a reason that says why, and the running job is
// untouched. When per-operator addressing lands, this test should be replaced by
// one that drives a real rescale and asserts output equality across it.
TEST_F(RescaleExactlyOnceTest, PerOperatorRescaleIsRefusedRatherThanSilentlyAccepted) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c);
    ASSERT_NE(sub, nullptr);

    // Ask only once the job is genuinely running and has published output, so
    // the refusal is about addressability and not about an un-started job.
    ASSERT_TRUE(clink::itest::await(
        [&] { return verify_exactly_once(out_dir_, kTotalRecords).total_lines > 0; },
        std::chrono::seconds(60)))
        << "nothing was committed before the rescale request, so the job was not yet running";

    std::string rescale_out;
    const int rc = rescale_operator(c, "counter", kMaxParallelism, &rescale_out);
    std::cerr << "RESCALE-OP-OUTCOME rc=" << rc << ": " << rescale_out << "\n";

    const auto exit_code = sub->await_exit(std::chrono::seconds(120));
    const auto v = verify_exactly_once(out_dir_, kTotalRecords);
    const std::string context =
        "rescale-op rc=" + std::to_string(rc) + " reply=" + rescale_out + " | " + describe(v);

    EXPECT_NE(rc, 0) << "the coordinator accepted a per-operator rescale it cannot execute. An "
                        "accepted request that never runs is worse than a refusal: the caller "
                        "waits for a resize that will never happen. "
                     << context;
    EXPECT_NE(rescale_out.find("shared role"), std::string::npos)
        << "the refusal must say WHY - that every subtask carries one shared role, so a single "
           "operator's subtasks cannot be targeted - otherwise an operator cannot tell this "
           "apart from a bad argument. "
        << context;

    // The refusal must be a no-op on the running job, not a rescale that half
    // happened. Both halves matter: nothing dispatched, and the job unharmed.
    EXPECT_FALSE(c.coordinator().log_contains("begin rescale dispatched"))
        << "a refused request still dispatched a drain. " << context;
    ASSERT_TRUE(exit_code.has_value()) << "submitter never exited. " << context;
    EXPECT_EQ(*exit_code, 0) << "the job did not finish cleanly after a refused rescale. "
                             << context;
    EXPECT_TRUE(v.missing.empty()) << "records were lost after a refused rescale. " << context;
    EXPECT_TRUE(v.duplicated.empty())
        << "records were duplicated after a refused rescale. " << context;
    EXPECT_TRUE(v.unexpected.empty())
        << "output contains records the source never emitted. A STATE-MISMATCH marker here is "
           "the keyed operator reporting that its own per-key counter disagreed with the record "
           "index. "
        << context;
}

// Whole-job rescale: `clink rescale --role=__clink_subtask --parallelism=1`, the
// path that DOES execute.
//
// It must not run on a multi-operator job. Measured before the refusal was added,
// on exactly this job: the rescale was accepted ("rescale initiated; draining 2
// worker connection(s)"), the coordinator redeployed the job as ONE task cloned
// from a single operator chain, that task failed immediately with "missing
// resolved peer for edge" because the peers it referenced were no longer
// deployed, every restart attempt failed the same way, and the job finished
// FAILED having committed 3 of 240 records.
//
// So this asserts the refusal, and - the part that makes it sensitive - that the
// job is still intact afterwards. Reverting the refusal does not merely flip the
// exit code, it destroys the job, and the output assertions below fail with 237
// records missing.
TEST_F(RescaleExactlyOnceTest, WholeRoleRescaleOfAMultiOperatorJobIsRefused) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(clink::itest::await(
        [&] { return verify_exactly_once(out_dir_, kTotalRecords).total_lines > 0; },
        std::chrono::seconds(60)))
        << "nothing was committed before the rescale request, so the job was not yet running";

    std::string rescale_out;
    const int rc = rescale_whole_role(c, 1, &rescale_out);
    std::cerr << "RESCALE-WHOLE-ROLE-OUTCOME rc=" << rc << ": " << rescale_out << "\n";

    const auto exit_code = sub->await_exit(std::chrono::seconds(120));
    const auto v = verify_exactly_once(out_dir_, kTotalRecords);
    const std::string context =
        "rescale rc=" + std::to_string(rc) + " reply=" + rescale_out + " | " + describe(v);

    EXPECT_NE(rc, 0) << "a whole-role rescale of a multi-operator job was accepted. It cannot be "
                        "carried out: the redeploy drops every operator chain but one. "
                     << context;
    EXPECT_NE(rescale_out.find("covers all"), std::string::npos)
        << "the refusal must explain that the role spans every operator in the job. " << context;

    // The job must be untouched. This is the assertion that would have caught
    // the destructive behaviour: it fails with hundreds of records missing.
    ASSERT_TRUE(exit_code.has_value()) << "submitter never exited. " << context;
    EXPECT_EQ(*exit_code, 0) << "the job did not finish cleanly after a refused rescale. "
                             << context;
    EXPECT_TRUE(v.missing.empty()) << "records were lost after a refused rescale. " << context;
    EXPECT_TRUE(v.duplicated.empty())
        << "records were duplicated after a refused rescale. " << context;
    EXPECT_TRUE(v.unexpected.empty())
        << "output contains records the source never emitted. " << context;
}
