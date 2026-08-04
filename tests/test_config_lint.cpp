// Configuration coherence, checked before a job runs.
//
// The value of a linter is entirely in its precision. One false positive
// and people switch it off, at which point the true positives are worth
// nothing - which is what happened when the table-option checker's `mode`
// domain was assembled from memory and rejected every CDC table in the
// suite.
//
// So every check here has BOTH tests: the configuration it must refuse,
// and the nearby configuration it must accept. The negatives are the
// expensive half and the reason this file is long.
//
// Each check also traces to a fact about the engine rather than a
// preference, and the tests name it, so a future reader can tell a rule
// from an opinion.

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/config_lint.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/worker.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/orchestration_metrics.hpp"

using namespace clink::cluster;

namespace {

bool has_error_for(const std::vector<ConfigProblem>& ps, std::string_view setting) {
    for (const auto& p : ps) {
        if (p.is_error() && p.setting == setting) {
            return true;
        }
    }
    return false;
}

bool has_warning_for(const std::vector<ConfigProblem>& ps, std::string_view setting) {
    for (const auto& p : ps) {
        if (!p.is_error() && p.setting == setting) {
            return true;
        }
    }
    return false;
}

bool has_any_error(const std::vector<ConfigProblem>& ps) {
    for (const auto& p : ps) {
        if (p.is_error()) {
            return true;
        }
    }
    return false;
}

// A configuration that is coherent, as the baseline every test perturbs by
// exactly one field. Without this the tests would each build their own and
// drift.
CheckpointConfig coherent() {
    CheckpointConfig c;
    c.checkpoint_dir = "/var/clink/ckpt";
    c.interval_ms = 5'000;
    return c;
}

}  // namespace

TEST(ConfigLint, TheBaselineIsClean) {
    // Establishes the premise for every negative below. If this ever
    // reports something, the tests that assert "no error" stop meaning
    // what they say.
    const auto ps = lint_checkpoint_config(coherent());
    EXPECT_FALSE(has_any_error(ps)) << render_problems(ps);
    EXPECT_TRUE(ps.empty()) << "the baseline should not even warn:\n" << render_problems(ps);
}

// --- settings accepted and then ignored ----------------------------------

TEST(ConfigLint, AnIntervalWithNoDirectoryIsRefused) {
    // checkpoint_trigger_loop_ skips any job whose dir is empty, so this is
    // a request for periodic checkpoints that produces none.
    CheckpointConfig c;
    c.interval_ms = 500;
    const auto ps = lint_checkpoint_config(c);
    ASSERT_TRUE(has_error_for(ps, "checkpoint_interval_ms")) << render_problems(ps);
    // The diagnostic has to say what will happen, not just that it is
    // wrong: "NO checkpoint will ever be taken" is the actionable part.
    EXPECT_NE(render_problems(ps).find("NO checkpoint"), std::string::npos) << render_problems(ps);
}

TEST(ConfigLint, NeitherSetIsNotAnError) {
    // A job with no checkpointing at all is a legitimate, common choice -
    // it is what every embedded run does. Refusing it would make the
    // linter useless.
    const auto ps = lint_checkpoint_config(CheckpointConfig{});
    EXPECT_FALSE(has_any_error(ps)) << render_problems(ps);
}

TEST(ConfigLint, RestartsWithoutADirectoryAreRefusedButOnlyWhenExplicit) {
    // CheckpointConfig says max_restarts "has no effect without
    // checkpoint_dir", so an explicit non-zero value is a request being
    // dropped.
    CheckpointConfig c;
    c.max_restarts_on_worker_loss = 3;
    EXPECT_TRUE(has_error_for(lint_checkpoint_config(c), "max_restarts_on_worker_loss"));

    // kRestartAuto is the UNSET sentinel. Flagging it would fire on every
    // job that never mentioned restarts, which is most of them - the
    // difference between a linter and a nuisance.
    CheckpointConfig unset;
    EXPECT_FALSE(has_error_for(lint_checkpoint_config(unset), "max_restarts_on_worker_loss"));

    // An explicit 0 means "fail fast", which is honoured with or without a
    // directory, so there is nothing being ignored.
    CheckpointConfig fail_fast;
    fail_fast.max_restarts_on_worker_loss = 0;
    EXPECT_FALSE(has_error_for(lint_checkpoint_config(fail_fast), "max_restarts_on_worker_loss"));

    // And with a directory, restarts work, so no complaint.
    auto with_dir = coherent();
    with_dir.max_restarts_on_worker_loss = 3;
    EXPECT_FALSE(has_error_for(lint_checkpoint_config(with_dir), "max_restarts_on_worker_loss"));
}

TEST(ConfigLint, HalfARestoreRequestIsRefusedFromEitherSide) {
    // The engine restores only when dir AND id are both set. Half of it is
    // a resume that silently becomes a cold start - the worst outcome for
    // someone deliberately resuming.
    {
        auto c = coherent();
        c.restore_from_checkpoint_id = 12;
        EXPECT_TRUE(has_error_for(lint_checkpoint_config(c), "restore_from_checkpoint_id"));
    }
    {
        auto c = coherent();
        c.restore_from_dir = "/var/clink/ckpt";
        EXPECT_TRUE(has_error_for(lint_checkpoint_config(c), "restore_from_dir"));
    }
    {
        // Both set: a real resume, no complaint.
        auto c = coherent();
        c.restore_from_dir = "/var/clink/ckpt";
        c.restore_from_checkpoint_id = 12;
        const auto ps = lint_checkpoint_config(c);
        EXPECT_FALSE(has_any_error(ps)) << render_problems(ps);
    }
    {
        // Neither set: the ordinary case.
        EXPECT_FALSE(has_any_error(lint_checkpoint_config(coherent())));
    }
}

TEST(ConfigLint, ACaptureBoundWithNoCaptureDirectoryIsRefused) {
    auto c = coherent();
    c.capture_records = 1000;
    EXPECT_TRUE(has_error_for(lint_checkpoint_config(c), "capture_records"));

    c.capture_dir = "/var/clink/cap";
    EXPECT_FALSE(has_any_error(lint_checkpoint_config(c)));
}

TEST(ConfigLint, UnalignedWithoutCheckpointingWarnsRatherThanRefuses) {
    // Harmless, but it means the setting was misunderstood. Not an error:
    // nothing breaks, and refusing it would block a job that merely has a
    // redundant flag.
    CheckpointConfig c;
    c.alignment = CheckpointAlignment::Unaligned;
    const auto ps = lint_checkpoint_config(c);
    EXPECT_TRUE(has_warning_for(ps, "unaligned_checkpoints")) << render_problems(ps);
    EXPECT_FALSE(has_any_error(ps));

    // With checkpointing actually running, it applies to something.
    auto ok = coherent();
    ok.alignment = CheckpointAlignment::Unaligned;
    EXPECT_FALSE(has_warning_for(lint_checkpoint_config(ok), "unaligned_checkpoints"));
}

// --- contradictions ------------------------------------------------------

TEST(ConfigLint, AMemoryBackendWithACheckpointDirectoryIsRefused) {
    // The durability illusion, and the most dangerous entry in the file:
    // COMPLETED-N markers get written, so the control plane believes
    // checkpoints are completing, while the state they describe dies with
    // the process. A restore finds markers and no state.
    auto c = coherent();
    c.state_backend_uri = "memory://";
    const auto ps = lint_checkpoint_config(c);
    ASSERT_TRUE(has_error_for(ps, "state_backend_uri")) << render_problems(ps);
    EXPECT_NE(render_problems(ps).find("restores nothing"), std::string::npos);

    // A memory backend with NO directory is coherent: it says plainly that
    // this job has no recovery, which is a legitimate thing to want.
    CheckpointConfig mem_only;
    mem_only.state_backend_uri = "memory://";
    EXPECT_FALSE(has_any_error(lint_checkpoint_config(mem_only)));

    // Durable backends alongside a directory are the intended combination
    // and must not be flagged. This is the list a false positive would
    // most likely hit.
    for (const char* uri : {"rocksdb:///var/clink/state",
                            "remote-read://bucket/prefix",
                            "forst:///var/clink/state",
                            "s3sst+forst://bucket/prefix",
                            "/var/clink/state"}) {
        auto durable = coherent();
        durable.state_backend_uri = uri;
        const auto problems = lint_checkpoint_config(durable);
        EXPECT_FALSE(has_any_error(problems)) << uri << " was refused:\n"
                                              << render_problems(problems);
    }
}

TEST(ConfigLint, ADirectoryWithNoIntervalWarns) {
    // Legitimate for a bounded job (it still gets its end-of-stream
    // checkpoint) and silent data-loss-on-restart for an unbounded one. The
    // config alone cannot say which, so this warns and explains both.
    CheckpointConfig c;
    c.checkpoint_dir = "/var/clink/ckpt";
    const auto ps = lint_checkpoint_config(c);
    EXPECT_TRUE(has_warning_for(ps, "checkpoint_interval_ms")) << render_problems(ps);
    EXPECT_FALSE(has_any_error(ps)) << "a bounded job configured this way is fine";
}

// --- liveness ------------------------------------------------------------

TEST(ConfigLint, AHeartbeatIntervalAtOrAboveTheTimeoutIsRefused) {
    // Coordinator::Config's own comment: the interval should be about a
    // third of the timeout "so a single missed message doesn't trigger a
    // false positive". At interval >= timeout, a HEALTHY worker is declared
    // lost on schedule.
    EXPECT_TRUE(has_error_for(lint_liveness_config(2000, 2000, 100), "heartbeat_timeout"));
    EXPECT_TRUE(has_error_for(lint_liveness_config(3000, 2000, 100), "heartbeat_timeout"));

    // The shipped defaults (500ms interval, 2000ms timeout, 100ms watchdog)
    // must be clean. A linter that flags its own product's defaults is
    // broken by definition, and this is the assertion that catches it.
    const auto defaults = lint_liveness_config(500, 2000, 100);
    EXPECT_FALSE(has_any_error(defaults)) << render_problems(defaults);
    EXPECT_TRUE(defaults.empty()) << "the shipped defaults should not even warn:\n"
                                  << render_problems(defaults);
}

TEST(ConfigLint, ATimeoutUnderTwiceTheIntervalWarns) {
    // One dropped heartbeat then restarts the subtasks. Not fatal, but not
    // what anyone intends.
    const auto ps = lint_liveness_config(1200, 2000, 100);
    EXPECT_TRUE(has_warning_for(ps, "heartbeat_timeout")) << render_problems(ps);
    EXPECT_FALSE(has_any_error(ps));
}

TEST(ConfigLint, AWatchdogSlowerThanTheTimeoutWarns) {
    // Detection cannot be faster than the watchdog's own period, so the
    // configured timeout is not the one in effect.
    const auto ps = lint_liveness_config(500, 2000, 5000);
    EXPECT_TRUE(has_warning_for(ps, "watchdog_interval")) << render_problems(ps);
}

TEST(ConfigLint, LivenessCheckesIgnoreUnsetValues) {
    // A caller that knows only some of the three must not be told its
    // zeros are misconfigured.
    EXPECT_TRUE(lint_liveness_config(0, 0, 0).empty());
    EXPECT_TRUE(lint_liveness_config(500, 0, 0).empty());
}

// --- the submission gate -------------------------------------------------

TEST(ConfigLint, CheckConfigRejectsOnErrorsAndPassesOnWarnings) {
    // The shape the coordinator uses: a non-empty string is a rejection.
    CheckpointConfig bad;
    bad.interval_ms = 500;  // no directory
    EXPECT_FALSE(check_config(bad).empty());

    // A warning alone must NOT reject. This is the difference between a
    // gate and an obstacle: a bounded job with a directory and no interval
    // is normal and has to submit.
    CheckpointConfig warns;
    warns.checkpoint_dir = "/var/clink/ckpt";
    std::vector<ConfigProblem> problems;
    EXPECT_TRUE(check_config(warns, &problems).empty());
    EXPECT_FALSE(problems.empty()) << "the warning should still be reported to the caller";

    EXPECT_TRUE(check_config(coherent()).empty());
}

TEST(ConfigLint, RenderPutsErrorsBeforeWarnings) {
    // A reader needs the thing blocking them before the thing surprising
    // them.
    CheckpointConfig c;
    c.interval_ms = 500;                           // error
    c.alignment = CheckpointAlignment::Unaligned;  // warning
    const auto text = render_problems(lint_checkpoint_config(c));
    const auto first_error = text.find("error:");
    const auto first_warning = text.find("warning:");
    ASSERT_NE(first_error, std::string::npos) << text;
    ASSERT_NE(first_warning, std::string::npos) << text;
    EXPECT_LT(first_error, first_warning) << text;
}

// --- profiles ------------------------------------------------------------

TEST(ConfigLint, ProfileNamesRoundTripIncludingTheShortForms) {
    EXPECT_EQ(profile_from_string("production"), ConfigProfile::Production);
    EXPECT_EQ(profile_from_string("prod"), ConfigProfile::Production);
    EXPECT_EQ(profile_from_string("development"), ConfigProfile::Development);
    EXPECT_EQ(profile_from_string("dev"), ConfigProfile::Development);
    EXPECT_FALSE(profile_from_string("prod-ish").has_value());
    EXPECT_FALSE(profile_from_string("").has_value());
    EXPECT_STREQ(to_string(ConfigProfile::Production), "production");
}

TEST(ConfigLint, TheProductionProfileFillsInAnIntervalButNotOverAnExplicitOne) {
    {
        CheckpointConfig c;
        c.checkpoint_dir = "/var/clink/ckpt";
        apply_profile(ConfigProfile::Production,
                      c,
                      /*explicit_checkpoint_dir=*/true,
                      /*explicit_interval=*/false);
        EXPECT_GT(c.interval_ms, 0) << "production must supply a checkpoint interval";
    }
    {
        // An explicit 0 means "no periodic checkpoints" and must survive.
        // Quietly rewriting a flag someone set is the same failure as
        // ignoring one - which is what the rest of this file is about.
        CheckpointConfig c;
        c.checkpoint_dir = "/var/clink/ckpt";
        c.interval_ms = 0;
        apply_profile(ConfigProfile::Production,
                      c,
                      /*explicit_checkpoint_dir=*/true,
                      /*explicit_interval=*/true);
        EXPECT_EQ(c.interval_ms, 0) << "the profile overrode an explicit interval";
    }
    {
        // And it does not touch a non-default one either.
        CheckpointConfig c;
        c.checkpoint_dir = "/var/clink/ckpt";
        c.interval_ms = 250;
        apply_profile(ConfigProfile::Production,
                      c,
                      /*explicit_checkpoint_dir=*/true,
                      /*explicit_interval=*/true);
        EXPECT_EQ(c.interval_ms, 250);
    }
}

TEST(ConfigLint, TheDevelopmentProfileChangesNothing) {
    // Development means no recovery, which is already the default. The
    // profile exists so a submission can SAY so rather than arrive there by
    // omission - a choice rather than an accident - and it must not smuggle
    // settings in.
    CheckpointConfig before;
    CheckpointConfig after;
    apply_profile(ConfigProfile::Development, after, false, false);
    EXPECT_EQ(after.checkpoint_dir, before.checkpoint_dir);
    EXPECT_EQ(after.interval_ms, before.interval_ms);
    EXPECT_EQ(after.max_restarts_on_worker_loss, before.max_restarts_on_worker_loss);
    EXPECT_TRUE(lint_profile(ConfigProfile::Development, after).empty());
}

TEST(ConfigLint, TheProductionProfileRefusesWhatItCannotDeliver) {
    // The name IS the request. A submission that asks for production and
    // cannot recover is refused rather than downgraded, because a silent
    // downgrade means the operator believes they have guarantees they do
    // not.
    {
        CheckpointConfig c;  // no directory
        EXPECT_TRUE(has_error_for(lint_profile(ConfigProfile::Production, c), "profile"));
    }
    {
        auto c = coherent();
        c.state_backend_uri = "memory://";
        EXPECT_TRUE(has_error_for(lint_profile(ConfigProfile::Production, c), "profile"));
    }
    {
        // A coherent production config passes.
        auto c = coherent();
        c.state_backend_uri = "rocksdb:///var/clink/state";
        const auto ps = lint_profile(ConfigProfile::Production, c);
        EXPECT_FALSE(has_any_error(ps)) << render_problems(ps);
    }
    {
        // Fail-fast in production is unusual but deliberate: warn, do not
        // refuse. Someone who wants a job to stop rather than replay is
        // entitled to that.
        auto c = coherent();
        c.max_restarts_on_worker_loss = 0;
        const auto ps = lint_profile(ConfigProfile::Production, c);
        EXPECT_TRUE(has_warning_for(ps, "max_restarts_on_worker_loss")) << render_problems(ps);
        EXPECT_FALSE(has_any_error(ps));
    }
}

TEST(ConfigLint, DevelopmentDoesNotInheritProductionsDemands) {
    // A dev job with no directory is the whole point of the profile.
    CheckpointConfig c;
    EXPECT_TRUE(lint_profile(ConfigProfile::Development, c).empty());
    c.state_backend_uri = "memory://";
    EXPECT_TRUE(lint_profile(ConfigProfile::Development, c).empty());
}

// --- the gate, wired ------------------------------------------------------

// A gate tested only through its predicate is not proven to be wired. The
// snapshot format version was written by the writer and read by nobody for
// exactly that reason, so this drives a real submission.

TEST(ConfigLint, ARealSubmissionIsRejectedForAnIncoherentConfig) {
    Coordinator coordinator;
    const auto port = coordinator.start();
    coordinator.expect_workers({"w"});

    Worker worker("w", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(std::chrono::seconds(2)));

    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "1"}};
    g.ops.push_back(src);

    // An interval with no directory: the coordinator's trigger loop would
    // skip this job entirely, so the request produces no checkpoints.
    CheckpointConfig bad;
    bad.interval_ms = 500;

    try {
        (void)coordinator.submit_job(g, OperatorRegistry::default_instance(), {}, bad);
        FAIL() << "the coordinator accepted a config whose checkpoint interval can never fire";
    } catch (const std::exception& e) {
        const std::string what = e.what();
        EXPECT_NE(what.find("checkpoint_interval_ms"), std::string::npos) << what;
        EXPECT_NE(what.find("NO checkpoint"), std::string::npos)
            << "the rejection does not say what would have happened: " << what;
    }

    worker.stop();
    coordinator.stop();
}

TEST(JobGraphLint, ARealSubmissionIsRejectedForAKeyedOperatorAboveTheKeyGroupCount) {
    // The gate, not just the predicate: the coordinator must refuse this at
    // submission. It is checked before planning, so it does not need a cluster
    // large enough to host 200 subtasks - which is the point of catching it here
    // rather than discovering it from state that quietly went missing.
    Coordinator coordinator;
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-kg"});

    Worker worker("w-kg", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(std::chrono::seconds(2)));

    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "1"}};
    g.ops.push_back(src);
    OperatorSpec agg;
    agg.type = "identity_int64";
    agg.id = "agg";
    agg.inputs = {"src"};
    agg.parallelism = 200;  // above kNumKeyGroups
    agg.key_by = "identity";
    agg.out_channel = std::string{kChannelInt64};
    g.ops.push_back(agg);

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = "/tmp/clink_lint_gate_ckpt";
    ckpt.interval_ms = 500;

    try {
        (void)coordinator.submit_job(g, OperatorRegistry::default_instance(), {}, ckpt);
        FAIL() << "the coordinator accepted a keyed operator whose surplus subtasks would own no "
                  "key groups";
    } catch (const std::exception& e) {
        const std::string what = e.what();
        EXPECT_NE(what.find("empty key-group range"), std::string::npos) << what;
        EXPECT_NE(what.find("agg"), std::string::npos)
            << "the rejection does not name the operator at fault: " << what;
    }

    worker.stop();
    coordinator.stop();
}

TEST(JobGraphLint, ASubmissionWithOnlyGraphWarningsStillSucceeds) {
    // The other half. Rescale bounds that the runtime cannot reach are a warning,
    // not an error: the job runs perfectly well, it just cannot be rescaled.
    // Rejecting it would break every job that declared bounds it never used - and
    // a gate that refuses working jobs is one people switch off.
    Coordinator coordinator;
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-warn"});

    Worker::Config warn_cfg;
    warn_cfg.slot_count = 4;
    Worker worker("w-warn", "127.0.0.1", warn_cfg);
    worker.register_role("noop", [](const DeploymentTask&) {});
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(std::chrono::seconds(2)));

    const auto out_path = std::filesystem::temp_directory_path() /
                          ("clink_lint_warn_" + std::to_string(::getpid()) + ".txt");
    std::filesystem::remove(out_path);

    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    // From parallelism 2, a target of 1 is reachable (a divisor) and 3 is not
    // (neither multiple nor divisor), so the range is PARTIALLY reachable - a
    // warning. The bounds also have to satisfy JobGraphSpec::validate, which
    // requires min <= parallelism <= max.
    src.parallelism = 2;
    src.min_parallelism = 1;
    src.max_parallelism = 3;
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "1"}};
    g.ops.push_back(src);
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"src"};
    snk.parallelism = 1;
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", out_path.string()}};
    g.ops.push_back(snk);

    EXPECT_NO_THROW((void)coordinator.submit_job(
        g, OperatorRegistry::default_instance(), {}, CheckpointConfig{}))
        << "a graph whose only findings are warnings was refused";

    worker.stop();
    coordinator.stop();
    std::filesystem::remove(out_path);
}

TEST(ConfigLint, ARealSubmissionWithAWarningStillSucceeds) {
    // The other half, and the one a too-eager gate breaks: a directory with
    // no interval warns and MUST still submit. A bounded job configured
    // that way is normal.
    Coordinator coordinator;
    const auto port = coordinator.start();
    coordinator.expect_workers({"w"});

    // Two slots: the job below is a source plus a sink, and submit_job
    // refuses for want of capacity before the config gate is reached.
    Worker::Config worker_cfg;
    worker_cfg.slot_count = 4;
    Worker worker("w", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(std::chrono::seconds(2)));

    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "1"}};
    g.ops.push_back(src);

    const auto dir =
        std::filesystem::temp_directory_path() / ("clink_lint_ok_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir);

    // A sink too: plan_job refuses a source with no consumers, and this
    // test has to get PAST planning to prove the config gate let it
    // through. The rejection test above needs none, because the config gate
    // fires before planning.
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"src"};
    snk.parallelism = 1;
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", (dir / "out.txt").string()}};
    g.ops.push_back(snk);

    CheckpointConfig warns;
    warns.checkpoint_dir = dir.string();  // no interval: warns, does not reject

    JobId job_id = 0;
    ASSERT_NO_THROW(job_id =
                        coordinator.submit_job(g, OperatorRegistry::default_instance(), {}, warns))
        << "a warning-level problem blocked a submission";
    EXPECT_GT(job_id, 0U);

    worker.stop();
    coordinator.stop();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

// --- restart visibility ---------------------------------------------------

// A job that keeps failing and being redeployed is the most informative
// unhealthy state there is, and until now it emitted nothing:
// clink_coordinator_jobs_failed_total moves only when the restart budget is
// finally exhausted, so the signal arrived after the recovery had already
// been spent.
//
// Driven through a REAL restart rather than by calling the helper, because
// the helper being correct says nothing about whether the restart path
// reaches it - the same reason the config gate needed a real submission.
TEST(ConfigLint, ASubtaskRedeployIsCounted) {
    const auto counter_now = [](const char* name) {
        auto snap = clink::MetricsRegistry::global().snapshot();
        for (const auto& [n, v] : snap.counters) {
            if (n == name) {
                return v;
            }
        }
        return std::uint64_t{0};
    };

    Coordinator::Config cfg;
    cfg.max_restarts = 3;
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w"});

    // A role that fails its first attempt and succeeds after, so the job
    // restarts exactly once and then completes - a restart, not a failure.
    std::atomic<int> attempts{0};
    Worker::Config worker_cfg;
    worker_cfg.slot_count = 4;
    Worker worker("w", "127.0.0.1", worker_cfg);
    worker.register_role("flaky", [&attempts](const DeploymentTask&) {
        if (attempts.fetch_add(1) == 0) {
            throw std::runtime_error("first attempt fails");
        }
    });
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(std::chrono::seconds(2)));

    // This job has NO checkpoint directory, so the coordinator retries the
    // failing subtask in place rather than rolling the whole job back -
    // the second of the two restart mechanisms, and the one that had no
    // metric even after the first was instrumented.
    const auto before = counter_now(clink::metrics::kSubtaskRedeploys);

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{.worker_id = "w",
                                     .role = "flaky",
                                     .subtask_idx = 0,
                                     .data_port = 0,
                                     .peer_refs = {},
                                     .extra_config = ""});
    coordinator.deploy(plan);
    ASSERT_TRUE(coordinator.await_completion(std::chrono::seconds(5)));
    ASSERT_GE(attempts.load(), 2) << "the job never restarted, so there is nothing to count";

    EXPECT_GT(counter_now(clink::metrics::kSubtaskRedeploys), before)
        << "a subtask was redeployed and clink_coordinator_subtask_redeploys_total did not move, "
           "so a retry-looping job with no checkpointing is invisible in metrics";

    worker.stop();
    coordinator.stop();
}

// --- lint_job_graph ---------------------------------------------------
//
// The graph-level checks. Each is a setting the engine accepts and then either
// cannot honour or silently ignores, which is the bar the rest of this file
// holds itself to.

namespace {

// src -> keyed op -> sink, with the middle operator's settings under test.
clink::cluster::JobGraphSpec graph_with_keyed_op(std::uint32_t parallelism,
                                                 std::uint32_t min_p = 0,
                                                 std::uint32_t max_p = 0) {
    using namespace clink::cluster;
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .inputs = {},
        .parallelism = 1,
        .out_channel = std::string{kChannelInt64},
        .params = {{"count", "10"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "identity_int64",
        .id = "agg",
        .inputs = {"src"},
        .parallelism = parallelism,
        .min_parallelism = min_p,
        .max_parallelism = max_p,
        .out_channel = std::string{kChannelInt64},
        .key_by = "identity",
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"agg"},
        .parallelism = 1,
        .out_channel = std::string{kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });
    return g;
}

clink::cluster::CheckpointConfig checkpointed() {
    clink::cluster::CheckpointConfig c;
    c.checkpoint_dir = "/tmp/ckpt";
    c.interval_ms = 500;
    return c;
}

bool has_error_mentioning(const std::vector<clink::cluster::ConfigProblem>& ps,
                          std::string_view needle) {
    return std::any_of(ps.begin(), ps.end(), [&](const clink::cluster::ConfigProblem& p) {
        return p.is_error() && p.message.find(needle) != std::string::npos;
    });
}

bool has_warning_mentioning(const std::vector<clink::cluster::ConfigProblem>& ps,
                            std::string_view needle) {
    return std::any_of(ps.begin(), ps.end(), [&](const clink::cluster::ConfigProblem& p) {
        return !p.is_error() && p.message.find(needle) != std::string::npos;
    });
}

}  // namespace

TEST(JobGraphLint, AKeyedOperatorAboveTheKeyGroupCountIsAnError) {
    // The F38 shape, checkable before a record moves. 128 key groups divided
    // among 200 subtasks leaves 72 owning an empty range: no records, a slot
    // each, and any keyed state written through them dropped at restore.
    const auto problems = clink::cluster::lint_job_graph(graph_with_keyed_op(200), checkpointed());
    EXPECT_TRUE(has_error_mentioning(problems, "empty key-group range"))
        << clink::cluster::render_problems(problems);
    EXPECT_TRUE(has_error_mentioning(problems, "72 subtask"))
        << "the message should name how many subtasks own nothing: "
        << clink::cluster::render_problems(problems);
}

TEST(JobGraphLint, AKeyedOperatorAtOrBelowTheKeyGroupCountIsFine) {
    // The boundary matters: exactly 128 gives every subtask one group, which is
    // legitimate. An off-by-one here would reject a valid job, and a linter that
    // rejects valid jobs gets switched off.
    for (const std::uint32_t p : {1u, 2u, 64u, 127u, 128u}) {
        const auto problems =
            clink::cluster::lint_job_graph(graph_with_keyed_op(p), checkpointed());
        EXPECT_FALSE(has_error_mentioning(problems, "empty key-group range"))
            << "parallelism " << p
            << " was rejected: " << clink::cluster::render_problems(problems);
    }
}

TEST(JobGraphLint, AnUnkeyedOperatorAboveTheKeyGroupCountIsNotFlagged) {
    // Key groups only partition KEYED state. A stateless operator at parallelism
    // 200 is a scheduling question, not a correctness one, and flagging it would
    // be the linter having an opinion rather than a reason.
    auto g = graph_with_keyed_op(200);
    for (auto& op : g.ops) {
        op.key_by.clear();
    }
    const auto problems = clink::cluster::lint_job_graph(g, checkpointed());
    EXPECT_FALSE(has_error_mentioning(problems, "empty key-group range"))
        << clink::cluster::render_problems(problems);
}

TEST(JobGraphLint, RescaleBoundsTheRuntimeCannotReachAreFlagged) {
    // A rescale target must be an integer multiple or divisor of the current
    // parallelism (rescale_parent_mapping), so bounds of [5, 7] on an operator at
    // 4 describe a range every request against which will be refused.
    const auto unreachable =
        clink::cluster::lint_job_graph(graph_with_keyed_op(4, 5, 7), checkpointed());
    EXPECT_TRUE(has_warning_mentioning(unreachable, "no parallelism this operator can actually be"))
        << clink::cluster::render_problems(unreachable);

    // [2, 6] on an operator at 4 IS partly reachable - 2 and 8 are factors, and 2
    // and 6 are the ends, so 6 is not. The warning should say what does work
    // rather than just that something does not.
    const auto partial =
        clink::cluster::lint_job_graph(graph_with_keyed_op(4, 2, 6), checkpointed());
    EXPECT_TRUE(has_warning_mentioning(partial, "Reachable values in this range: 2"))
        << clink::cluster::render_problems(partial);

    // [1, 8] on an operator at 4: both ends are factors, nothing to say.
    const auto clean = clink::cluster::lint_job_graph(graph_with_keyed_op(4, 1, 8), checkpointed());
    EXPECT_FALSE(has_warning_mentioning(clean, "not both reachable"))
        << "a fully reachable range was flagged: " << clink::cluster::render_problems(clean);
}

TEST(JobGraphLint, RescaleBoundsWithoutPeriodicCheckpointingAreFlagged) {
    // request_operator_rescale refuses without periodic checkpointing, because
    // the new subtasks restore from a completed checkpoint. So bounds on a job
    // that never checkpoints are a setting that cannot be acted on - including by
    // the autoscaler.
    clink::cluster::CheckpointConfig none;  // no dir, no interval
    const auto problems = clink::cluster::lint_job_graph(graph_with_keyed_op(2, 1, 4), none);
    EXPECT_TRUE(has_warning_mentioning(problems, "no periodic checkpointing"))
        << clink::cluster::render_problems(problems);

    // A Warning and not an Error on purpose: the job itself runs perfectly well,
    // it just cannot be rescaled. Rejecting the submission would break jobs that
    // never intended to rescale but declared bounds anyway.
    EXPECT_FALSE(has_error_mentioning(problems, "no periodic checkpointing"))
        << clink::cluster::render_problems(problems);
}

TEST(JobGraphLint, AGraphLargerThanTheClusterIsAnErrorWhenTheSlotCountIsKnown) {
    // src(1) + agg(8) + snk(1) = 10 subtasks.
    const auto g = graph_with_keyed_op(8);
    EXPECT_TRUE(has_error_mentioning(clink::cluster::lint_job_graph(g, checkpointed(), 4),
                                     "needs 10 subtask slots"))
        << "a graph twice the size of the cluster was not flagged";
    EXPECT_FALSE(has_error_mentioning(clink::cluster::lint_job_graph(g, checkpointed(), 10),
                                      "subtask slots"))
        << "a graph that exactly fits was flagged";
    // Unset means "no cluster to ask", as in the CLI - not "assume zero".
    EXPECT_FALSE(
        has_error_mentioning(clink::cluster::lint_job_graph(g, checkpointed()), "subtask slots"))
        << "an unknown slot count was treated as a failure";
}

TEST(JobGraphLint, ACleanGraphProducesNothing) {
    // The check that keeps the rest honest: a reasonable job must lint silent, or
    // the findings above are noise that trains people to ignore the output.
    const auto problems =
        clink::cluster::lint_job_graph(graph_with_keyed_op(4, 2, 8), checkpointed(), 16);
    EXPECT_TRUE(problems.empty()) << clink::cluster::render_problems(problems);
}
