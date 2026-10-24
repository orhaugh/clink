// Every source subtask of a job must emit the same checkpoint barriers, or the
// first operator that merges them waits for ever.
//
// A worker hands each TriggerCheckpoint to the barrier injectors its source
// subtasks have registered. A source registers them once its runner is built,
// after the PeerUpdate, and the coordinator starts triggering as soon as the
// PeerUpdate has gone out. A trigger that arrived between two sibling sources'
// registrations reached only the first, and one that arrived before either was
// queued and then replayed into whichever registered first. The other source
// never emitted that checkpoint's barrier. Downstream, every operator fed by
// both paused the inputs that had delivered it, waiting for one that never
// would, and then paused that input too on its next barrier: no input was read
// again, no checkpoint completed, and no output left the job, which still
// reported itself running. The broker-outage recovery gate stalled this way
// after a redeploy, with nothing in any log but channels that never received
// anything.
//
// The fault point holds the first source to reach its registration while the
// coordinator triggers every 50ms, so the first checkpoints are triggered while
// one source is registered and the other is not.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>

#include <gtest/gtest.h>

#include "clink/application/job_submitter.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/worker.hpp"
#include "clink/fault/fault_injection.hpp"

#include "tests/test_helpers/sanitizer_slack.hpp"

using namespace clink;
using namespace clink::cluster;
using namespace std::chrono_literals;

namespace {

std::filesystem::path handoff_hello_plugin_path() {
#ifdef CLINK_HELLO_PLUGIN_PATH
    return std::filesystem::path{CLINK_HELLO_PLUGIN_PATH};
#else
    return {};
#endif
}

// Two sources on one worker, both feeding one keyed counter: the counter's
// subtask merges them, so it aligns every barrier across the two. Each source
// emits `count` records, one every 25ms.
JobGraphSpec handoff_graph(const std::filesystem::path& out, int count = 40) {
    JobGraphSpec g;
    OperatorSpec src;
    src.id = "src";
    src.type = "hello.GreetingSource";
    src.out_channel = "hello.Greeting";
    src.parallelism = 2;
    src.params = {{"count", std::to_string(count)}, {"start", "1"}, {"delay_ms", "25"}};
    g.ops.push_back(std::move(src));
    OperatorSpec cnt;
    cnt.id = "counter";
    cnt.uid = "handoff-counter";
    cnt.key_by = "hello.by_parity";
    cnt.type = "hello.ParityCounter";
    cnt.out_channel = "hello.Greeting";
    cnt.inputs = {"src"};
    cnt.parallelism = 1;
    g.ops.push_back(std::move(cnt));
    OperatorSpec snk;
    snk.id = "snk";
    snk.type = "hello.GreetingFileSink";
    snk.out_channel = "hello.Greeting";
    snk.inputs = {"counter"};
    snk.parallelism = 1;
    snk.params = {{"path", out.string()}};
    g.ops.push_back(std::move(snk));
    return g;
}

// The ids of every COMPLETED-<id> marker job 1 wrote under `ckpt`.
std::set<std::uint64_t> handoff_completed_ids(const std::filesystem::path& ckpt) {
    std::set<std::uint64_t> ids;
    const auto dir = ckpt / "_jobs" / "1";
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
        const auto name = entry.path().filename().string();
        constexpr std::string_view kPrefix = "COMPLETED-";
        if (name.rfind(kPrefix, 0) != 0) {
            continue;
        }
        const auto digits = name.substr(kPrefix.size());
        if (digits.empty() || digits.find_first_not_of("0123456789") != std::string::npos) {
            continue;
        }
        ids.insert(std::stoull(digits));
    }
    return ids;
}

std::size_t handoff_line_count(const std::filesystem::path& p) {
    std::ifstream in(p);
    std::size_t n = 0;
    std::string line;
    while (std::getline(in, line)) {
        ++n;
    }
    return n;
}

}  // namespace

TEST(SourceTriggerHandoff, ASourceThatRegistersLateStillEmitsTheBarriersItsSiblingDid) {
    const auto plugin = handoff_hello_plugin_path();
    if (plugin.empty() || !std::filesystem::exists(plugin)) {
        GTEST_SKIP() << "hello_plugin not built";
    }
    clink::fault::Registry::instance().reset();
    struct RegistryReset {
        ~RegistryReset() { clink::fault::Registry::instance().reset(); }
    } reset_on_exit;

    Coordinator::Config cfg;
    cfg.max_restarts = 0;
    Coordinator coordinator(cfg);
    const std::uint16_t port = coordinator.start();
    coordinator.expect_workers({"worker-handoff"});
    Worker::Config wcfg;
    wcfg.slot_count = 4;
    Worker worker("worker-handoff", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto tag = std::to_string(::getpid());
    const auto tmp = std::filesystem::temp_directory_path();
    const auto ckpt = tmp / ("clink_handoff_ckpt_" + tag);
    const auto out = tmp / ("clink_handoff_out_" + tag);
    std::filesystem::remove_all(ckpt);
    std::filesystem::remove(out);

    // Ordinal 1: only the first source to reach its registration is held, so
    // its sibling registers at once and the triggers of the next 1.5s find one
    // source of the job registered on this worker and one not.
    const clink::fault::ScopedFault guard(clink::fault::Rule{
        .point = clink::fault::points::kWorkerBeforeSourceInjectors,
        .ordinal = 1,
        .action = clink::fault::Action::Delay,
        .arg = 1500,
    });

    clink::application::JobSubmitter submitter("127.0.0.1", port);
    clink::application::SubmitOptions opts;
    opts.wait_for_completion = true;
    opts.wait_timeout = clink::test_support::scale_slack(30s);
    opts.ack_timeout = clink::test_support::scale_slack(opts.ack_timeout);
    opts.checkpoint.checkpoint_dir = ckpt.string();
    opts.checkpoint.interval_ms = 50;
    const auto r = submitter.submit(handoff_graph(out).to_json(), {plugin.string()}, opts);

    // Gathered before the cluster stops, and asserted after, so a job that
    // never completed cannot hold the teardown.
    const auto hits =
        clink::fault::Registry::instance().hits(clink::fault::points::kWorkerBeforeSourceInjectors);
    const auto lines = handoff_line_count(out);
    const auto completed = handoff_completed_ids(ckpt);
    std::string errors;
    for (const auto& e : r.errors) {
        errors += "\n  " + e;
    }
    worker.stop();
    coordinator.stop();
    std::filesystem::remove_all(ckpt);
    std::filesystem::remove(out);

    // Vacuity: both sources passed the point, and the first was held there.
    EXPECT_GE(hits, 2U);
    ASSERT_TRUE(r.ok) << "the job did not complete: with one source missing a barrier its "
                         "sibling emitted, the counter that merges them waits for it for ever ("
                      << r.reject_message << ")" << errors;
    EXPECT_EQ(lines, 80U) << "two sources of 40 records each, counted once";

    // Checkpoint 1 is triggered while the held source has no injectors
    // registered. It completes only if that source still emits its barrier
    // once it registers; a source that skipped it would leave it pending for
    // good, and the job would carry on from a later one.
    ASSERT_FALSE(completed.empty()) << "the job completed no checkpoint; the probe is vacuous";
    EXPECT_EQ(completed.count(1), 1U)
        << "checkpoint 1, triggered while one source was still registering, never completed";
    for (std::uint64_t id = 1; id <= *completed.rbegin(); ++id) {
        EXPECT_EQ(completed.count(id), 1U)
            << "checkpoint " << id << " never completed although a later one did";
    }
}

// A savepoint is triggered as soon as the job is deployed: unlike the periodic
// loop it does not wait for every subtask to report itself listening, so its
// trigger can reach a worker before the job's sources there have parsed their
// chains, let alone registered their injectors. The worker held triggers for a
// source only from its chain parse on, and dropped one that came earlier: the
// savepoint never completed, and take_savepoint waited out its timeout (the
// commit-teardown fence test timed out in CI this way). Every subtask's thread
// is held before it parses its chain while the savepoint is taken, so the
// trigger arrives while no source knows yet that it is one. The sources run for
// far longer than the test: take_savepoint reports success once any checkpoint
// at or above the savepoint's id completes, and a bounded job's end-of-stream
// checkpoint would stand in for a savepoint that never happened.
TEST(SourceTriggerHandoff, ASavepointTakenBeforeTheSourcesParseTheirChainsCompletes) {
    const auto plugin = handoff_hello_plugin_path();
    if (plugin.empty() || !std::filesystem::exists(plugin)) {
        GTEST_SKIP() << "hello_plugin not built";
    }
    clink::fault::Registry::instance().reset();
    struct RegistryReset {
        ~RegistryReset() { clink::fault::Registry::instance().reset(); }
    } reset_on_exit;

    Coordinator::Config cfg;
    cfg.max_restarts = 0;
    Coordinator coordinator(cfg);
    const std::uint16_t port = coordinator.start();
    coordinator.expect_workers({"worker-handoff-savepoint"});
    Worker::Config wcfg;
    wcfg.slot_count = 4;
    Worker worker("worker-handoff-savepoint", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto tag = std::to_string(::getpid());
    const auto tmp = std::filesystem::temp_directory_path();
    const auto ckpt = tmp / ("clink_handoff_sp_ckpt_" + tag);
    const auto out = tmp / ("clink_handoff_sp_out_" + tag);
    std::filesystem::remove_all(ckpt);
    std::filesystem::remove(out);

    // Ordinal 0: every subtask of the job waits here, the two sources included.
    const clink::fault::ScopedFault guard(clink::fault::Rule{
        .point = clink::fault::points::kWorkerBeforeChainParse,
        .ordinal = 0,
        .action = clink::fault::Action::Delay,
        .arg = 1000,
    });

    clink::application::JobSubmitter submitter("127.0.0.1", port);
    clink::application::SubmitOptions opts;
    opts.wait_for_completion = false;
    opts.ack_timeout = clink::test_support::scale_slack(opts.ack_timeout);
    opts.checkpoint.checkpoint_dir = ckpt.string();
    // No periodic checkpoints: the savepoint is the only one under test.
    opts.checkpoint.interval_ms = 0;
    const auto r = submitter.submit(handoff_graph(out, 100'000).to_json(), {plugin.string()}, opts);
    ASSERT_TRUE(r.ok) << r.reject_message;

    // Its trigger is queued behind the job's Deploy, so the worker has the
    // Deploy before the trigger and the subtasks have not parsed their chains.
    const auto sp = coordinator.take_savepoint(r.job_id, clink::test_support::scale_slack(15s));
    const auto completed = handoff_completed_ids(ckpt);
    const bool cancelled = coordinator.cancel_job(r.job_id).ok;
    const bool finished =
        coordinator.await_job_completion(r.job_id, clink::test_support::scale_slack(30s));

    // Gathered before the cluster stops, and asserted after.
    const auto hits =
        clink::fault::Registry::instance().hits(clink::fault::points::kWorkerBeforeChainParse);
    worker.stop();
    coordinator.stop();
    std::filesystem::remove_all(ckpt);
    std::filesystem::remove(out);

    // Vacuity: the two sources, the counter and the sink all waited.
    EXPECT_GE(hits, 4U);
    ASSERT_TRUE(sp.ok) << "the savepoint triggered before the sources parsed their chains never "
                          "completed: "
                       << sp.message;
    EXPECT_EQ(completed.count(sp.checkpoint_id), 1U)
        << "the savepoint reported checkpoint " << sp.checkpoint_id
        << " but its COMPLETED marker is not there";
    EXPECT_TRUE(cancelled);
    EXPECT_TRUE(finished) << "the job did not finish after its cancel";
}
