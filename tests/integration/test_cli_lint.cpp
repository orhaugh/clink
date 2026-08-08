// `clink lint` - the config gate, runnable without a cluster.
//
// W17 recorded the gap: the linter ran at submission and behind
// `--profile`, so checking a deployment's flags meant submitting a job with
// them. These cases drive the real binary and assert on exit codes, because
// the exit code is what a deploy pipeline gates on and the text is not.
//
// Writing them found F36: five of the linter's checks were unreachable
// through the CLI. `clink run` only populated a CheckpointConfig when a
// checkpoint dir had been resolved, so `--checkpoint-interval-ms=5000` with
// no dir - the example in config_lint.hpp's own header - was dropped before
// the linter saw it. The check was unit-tested and passing, and could not
// fire on the path an operator uses.
//
// The cases below therefore assert two different things: that the command
// reports each class of problem, and that the reachability regression cannot
// come back.

#include <array>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace {

std::filesystem::path cli_binary() {
#ifdef CLINK_CLI_BINARY
    return std::filesystem::path{CLINK_CLI_BINARY};
#else
    return {};
#endif
}

struct Run {
    int exit_code{-1};
    std::string output;

    [[nodiscard]] bool mentions(std::string_view needle) const {
        return output.find(needle) != std::string::npos;
    }
};

// Both streams, because an error can land on either and a test that watched
// only stdout would report a silent pass when the message moved.
Run run_lint(const std::string& args) {
    Run r;
    const std::string cmd = cli_binary().string() + " lint " + args + " 2>&1";
    FILE* pipe = ::popen(cmd.c_str(), "r");
    if (pipe == nullptr) {
        return r;
    }
    std::array<char, 512> buf{};
    while (::fgets(buf.data(), static_cast<int>(buf.size()), pipe) != nullptr) {
        r.output += buf.data();
    }
    const int status = ::pclose(pipe);
    r.exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    return r;
}

class CliLintTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::filesystem::exists(cli_binary())) {
            GTEST_SKIP() << "clink CLI is not built";
        }
    }
};

}  // namespace

TEST_F(CliLintTest, ACoherentConfigExitsZero) {
    // The premise. Without it, every assertion below could be satisfied by a
    // command that fails on everything.
    const auto r = run_lint(
        "--checkpoint-dir=/tmp/clink_lint_x --checkpoint-interval-ms=5000 "
        "--max-restarts-on-worker-loss=3");
    EXPECT_EQ(r.exit_code, 0) << r.output;
    EXPECT_TRUE(r.mentions("no problems found")) << r.output;
}

TEST_F(CliLintTest, AnIntervalWithNoCheckpointDirIsAnError) {
    // F36. The linter's own motivating example, and the case the CLI used to
    // drop before the linter could see it. A regression here means the
    // command reports a clean configuration that takes no checkpoints.
    const auto r = run_lint("--checkpoint-interval-ms=5000");
    EXPECT_EQ(r.exit_code, 1) << "an interval with no checkpoint_dir takes NO checkpoints and must "
                                 "not lint clean: "
                              << r.output;
    EXPECT_TRUE(r.mentions("NO checkpoint will ever be taken")) << r.output;
    EXPECT_TRUE(r.mentions("REFUSED at submission"))
        << "the command reported an error without saying it blocks a submission: " << r.output;
}

TEST_F(CliLintTest, EveryDirDependentCheckIsReachableThroughTheCli) {
    // The general form of F36, kept as one case because the defect was one
    // defect: the CLI dropping flags when no dir was resolved. Each of these
    // was unit-tested and unreachable.
    struct Case {
        const char* args;
        const char* expect;
    };
    const std::array<Case, 3> cases{{
        {"--checkpoint-interval-ms=5000", "checkpoint_interval_ms"},
        {"--max-restarts-on-worker-loss=4", "max_restarts_on_worker_loss"},
        {"--capture-records=100", "capture_records"},
    }};
    for (const auto& c : cases) {
        const auto r = run_lint(c.args);
        EXPECT_EQ(r.exit_code, 1) << "`clink lint " << c.args << "` linted clean: " << r.output;
        EXPECT_TRUE(r.mentions(c.expect))
            << "`clink lint " << c.args << "` did not report " << c.expect << ": " << r.output;
    }
}

TEST_F(CliLintTest, AHalfSetRestorePairIsAnError) {
    // A resume that will not happen is the worst case in the file: the job
    // starts from scratch and says nothing.
    const auto r = run_lint("--checkpoint-dir=/tmp/clink_lint_x --restore-from-checkpoint-id=7");
    EXPECT_EQ(r.exit_code, 1) << r.output;
    EXPECT_TRUE(r.mentions("nothing is restored")) << r.output;
}

TEST_F(CliLintTest, AProductionProfileWithNoDurableStoreIsAnError) {
    const auto r = run_lint("--profile=production --state-backend=memory");
    EXPECT_EQ(r.exit_code, 1) << "profile=production with a memory backend gives none of the "
                                 "guarantees the profile names: "
                              << r.output;
    EXPECT_TRUE(r.mentions("profile=production requires a checkpoint_dir")) << r.output;
}

TEST_F(CliLintTest, AnUnknownProfileIsUsageNotAFinding) {
    // Exit 2, not 1. A pipeline distinguishes "your config is bad" from "you
    // invoked me wrong", and conflating them makes a typo look like a
    // configuration verdict.
    const auto r = run_lint("--profile=nonsense");
    EXPECT_EQ(r.exit_code, 2) << r.output;
}

TEST_F(CliLintTest, AnUnknownStateBackendSchemeIsUsageNotAFinding) {
    const auto r = run_lint("--state-backend=cassandra");
    EXPECT_EQ(r.exit_code, 2) << r.output;
    EXPECT_TRUE(r.mentions("unknown --state-backend scheme")) << r.output;
}

TEST_F(CliLintTest, WarningsAloneDoNotFailTheCommand) {
    // The distinction the severity levels exist for. A dir with no interval
    // is legitimate for a bounded job, so it warns; exiting non-zero would
    // make the command unusable as a gate and it would be switched off.
    const auto r = run_lint("--checkpoint-dir=/tmp/clink_lint_x");
    EXPECT_EQ(r.exit_code, 0) << "a warning-only config failed the gate: " << r.output;
    EXPECT_TRUE(r.mentions("warning")) << r.output;
    EXPECT_TRUE(r.mentions("would not block a submission")) << r.output;
}

TEST_F(CliLintTest, LivenessIsCheckedOnlyWhenAskedAbout) {
    // Two properties in one place because they are two halves of one
    // decision. Coordinator liveness flags belong to a process this command
    // does not start, so linting their defaults would report findings about
    // a cluster nobody configured - and never linting them would leave the
    // check unreachable, which is F36 again.
    const auto absent =
        run_lint("--checkpoint-dir=/tmp/clink_lint_x --checkpoint-interval-ms=5000");
    EXPECT_FALSE(absent.mentions("heartbeat"))
        << "liveness was linted although no liveness flag was given: " << absent.output;

    const auto given = run_lint("--heartbeat-interval-ms=6000 --heartbeat-timeout-ms=5000");
    EXPECT_EQ(given.exit_code, 1) << "an interval at or above the timeout declares a HEALTHY "
                                     "worker lost and must be an error: "
                                  << given.output;
    EXPECT_TRUE(given.mentions("declared lost")) << given.output;
}

TEST_F(CliLintTest, TheNodeDefaultsAreWhatGetLinted) {
    // The one way this command could confidently report a clean cluster that
    // then misbehaves: filling unset liveness flags with defaults that are
    // not clink_node's, so the combination linted is not the combination
    // that runs. clink_node's timeout default is 5000 and its heartbeat
    // interval is a fixed 500, a ratio the linter is content with - so
    // supplying only the watchdog flag must come back clean.
    const auto r = run_lint("--watchdog-interval-ms=200");
    EXPECT_EQ(r.exit_code, 0) << "linting clink_node's own liveness defaults reported a problem, "
                                 "so either the defaults here drifted from clink_node.cpp or the "
                                 "shipped defaults are genuinely bad. Both matter: "
                              << r.output;
}

// --- guarantee cross-check (follow-up 24) -----------------------------------
//
// The config linter and the delivery-guarantee analyser answer adjacent
// questions and used to never compare notes. The linter asks "is this
// configuration self-consistent"; the analyser asks "what can this pipeline
// actually deliver". A configuration passes the first and still not support
// what it looks like it is asking for.

namespace {

// Write a graph JSON to a unique path. Keyed by pid and name because ctest runs
// each test as its own process and a fixed path would have concurrent tests
// reading each other's fixture.
std::filesystem::path write_graph(const std::string& name, const std::string& body) {
    const auto p = std::filesystem::temp_directory_path() /
                   ("clink_lint_graph_" + std::to_string(::getpid()) + "_" + name + ".json");
    std::ofstream out(p);
    out << body;
    return p;
}

constexpr const char* kNonReplayableSourceToFileSink = R"({"ops":[
 {"id":"src","type":"int64_range_source","out_channel":"int64","inputs":[],"params":{"start":"0","end":"10"}},
 {"id":"snk","type":"file_line_sink","out_channel":"int64","inputs":["src"],"params":{"path":"/tmp/clink_lint_sink"}}
]})";

constexpr const char* kSameGraphAskingForExactlyOnce = R"({"ops":[
 {"id":"src","type":"int64_range_source","out_channel":"int64","inputs":[],"params":{"start":"0","end":"10"}},
 {"id":"snk","type":"file_line_sink","out_channel":"int64","inputs":["src"],"params":{"path":"/tmp/clink_lint_sink","delivery_guarantee":"exactly_once"}}
]})";

}  // namespace

// A coherent configuration whose pipeline still cannot deliver much. This is the
// case the cross-check exists for: every flag is fine, so the config linter alone
// says "no problems found" and the operator learns nothing about the guarantee
// they are actually getting.
TEST(CliLint, ReportsTheDeliveryGuaranteeAlongsideACleanConfig) {
    if (cli_binary().empty() || !std::filesystem::exists(cli_binary())) {
        GTEST_SKIP() << "clink CLI not built";
    }
    const auto graph = write_graph("clean", kNonReplayableSourceToFileSink);
    const auto r = run_lint("--graph-json=" + graph.string() +
                            " --checkpoint-dir=/tmp/clink_lint_ck --checkpoint-interval-ms=1000");
    std::filesystem::remove(graph);

    EXPECT_EQ(r.exit_code, 0) << r.output;
    EXPECT_TRUE(r.mentions("delivery guarantee")) << "the guarantee was not reported at all:\n"
                                                  << r.output;
    // The source cannot replay, so nothing stronger is reachable however coherent
    // the checkpoint flags are. Naming the limiting factor is the useful part.
    EXPECT_TRUE(r.mentions("int64_range_source"))
        << "the report did not name what limits the guarantee:\n"
        << r.output;
}

// The same pipeline, now ASKING for exactly-once. The submission gate would refuse
// it, so the lint must too - a clean lint followed by a refused submission is
// exactly the disagreement this command exists to rule out.
TEST(CliLint, RefusesWhenThePipelineCannotProvideTheRequestedGuarantee) {
    if (cli_binary().empty() || !std::filesystem::exists(cli_binary())) {
        GTEST_SKIP() << "clink CLI not built";
    }
    const auto graph = write_graph("requested", kSameGraphAskingForExactlyOnce);
    const auto r = run_lint("--graph-json=" + graph.string() +
                            " --checkpoint-dir=/tmp/clink_lint_ck --checkpoint-interval-ms=1000");
    std::filesystem::remove(graph);

    EXPECT_EQ(r.exit_code, 1) << "a pipeline that would be REFUSED at submission linted clean:\n"
                              << r.output;
    EXPECT_TRUE(r.mentions("cannot provide the requested guarantee")) << r.output;
    EXPECT_TRUE(r.mentions("REFUSED at submission")) << r.output;
}
