// A source that acknowledges at checkpoint completion, run in an in-process
// cluster. The completions the coordinator broadcasts must reach it through the
// subtask's commit callbacks, and with par-1 fusion through the fused chain's;
// a record may be acknowledged only once a checkpoint at or above the one that
// sealed it has completed, judged from the COMPLETED markers the coordinator
// wrote, not from anything the source reports; a failed checkpoint's records
// are never acknowledged; and the notifications arrive off the source thread,
// while the acknowledgements are taken on it. The Redis Streams and Pub/Sub
// sources acknowledge through the same ledger as this probe.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/guarantee_gate.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/cluster/worker.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/connectors/checkpoint_ack_ledger.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/plugin/plugin.hpp"

#include "tests/test_helpers/sanitizer_slack.hpp"

namespace {

using namespace std::chrono_literals;
using clink::Batch;
using clink::CheckpointId;
using clink::Emitter;
using clink::OperatorId;
using clink::Source;
using clink::StateBackend;
using clink::cluster::CheckpointConfig;
using clink::cluster::Coordinator;
using clink::cluster::JobGraphSpec;
using clink::cluster::JobId;
using clink::cluster::OperatorRegistry;
using clink::cluster::OperatorSpec;
using clink::cluster::PluginBinary;
using clink::cluster::RunnerRegistry;
using clink::cluster::SelectorRegistry;
using clink::cluster::TypeRegistry;
using clink::cluster::Worker;

// What every probe incarnation did, for the test to judge afterwards.
struct ProbeLog {
    std::mutex mu;
    int incarnations{0};
    std::map<std::uint64_t, std::vector<std::string>> sealed;  // checkpoint -> records it sealed
    std::map<std::string, std::uint64_t> sealed_by;            // record -> checkpoint
    std::map<int, std::uint64_t> max_sealed;                   // incarnation -> highest id sealed
    std::vector<std::string> acked;
    std::vector<std::uint64_t> completions;
    std::set<std::thread::id> source_threads;
    std::set<std::thread::id> notify_threads;
    std::set<std::thread::id> ack_threads;

    void reset() {
        std::lock_guard lk(mu);
        incarnations = 0;
        sealed.clear();
        sealed_by.clear();
        max_sealed.clear();
        acked.clear();
        completions.clear();
        source_threads.clear();
        notify_threads.clear();
        ack_threads.clear();
    }
};
ProbeLog& probe() {
    static ProbeLog log;
    return log;
}

// Emits "<incarnation>:<n>" records, a few per millisecond, and acknowledges
// through the ledger exactly as the broker-backed sources do.
class AckProbeSource final : public Source<std::string> {
public:
    void open() override {
        std::lock_guard lk(probe().mu);
        incarnation_ = ++probe().incarnations;
    }

    bool produce(Emitter<std::string>& out) override {
        take_acks_();
        const std::string id = std::to_string(incarnation_) + ":" + std::to_string(next_++);
        {
            std::lock_guard lk(probe().mu);
            probe().source_threads.insert(std::this_thread::get_id());
        }
        since_barrier_.push_back(id);
        ledger_.delivered(id);
        Batch<std::string> b;
        b.emplace(id);
        out.emit_data(std::move(b));
        std::this_thread::sleep_for(1ms);
        return !this->cancelled();
    }

    void snapshot_offset(StateBackend&, OperatorId, CheckpointId ckpt) override {
        {
            std::lock_guard lk(probe().mu);
            auto& ids = probe().sealed[ckpt.value()];
            for (const auto& id : since_barrier_) {
                ids.push_back(id);
                probe().sealed_by[id] = ckpt.value();
            }
            auto& hi = probe().max_sealed[incarnation_];
            hi = std::max(hi, ckpt.value());
        }
        since_barrier_.clear();
        ledger_.seal(ckpt.value());
    }

    void notify_checkpoint_complete(CheckpointId ckpt) override {
        {
            std::lock_guard lk(probe().mu);
            probe().completions.push_back(ckpt.value());
            probe().notify_threads.insert(std::this_thread::get_id());
        }
        ledger_.complete(ckpt.value());
    }
    void notify_checkpoint_aborted(CheckpointId ckpt) override {
        {
            std::lock_guard lk(probe().mu);
            probe().notify_threads.insert(std::this_thread::get_id());
        }
        ledger_.abort(ckpt.value());
    }

    void close() override { take_acks_(); }
    [[nodiscard]] bool is_bounded() const noexcept override { return false; }
    std::string name() const override { return "ack_probe_source"; }

private:
    void take_acks_() {
        auto h = ledger_.take();
        if (h.ready.empty()) {
            return;
        }
        std::lock_guard lk(probe().mu);
        probe().acked.insert(probe().acked.end(), h.ready.begin(), h.ready.end());
        probe().ack_threads.insert(std::this_thread::get_id());
    }

    clink::connectors::CheckpointAckLedger<std::string> ledger_;
    std::vector<std::string> since_barrier_;
    int incarnation_{0};
    std::uint64_t next_{0};
};

void register_probe() {
    static std::once_flag once;
    std::call_once(once, [] {
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg(TypeRegistry::default_instance(),
                                          RunnerRegistry::default_instance(),
                                          SelectorRegistry::default_instance());
        reg.register_source<std::string>(
            "ack_probe_source",
            [](const clink::plugin::BuildContext&) -> std::shared_ptr<Source<std::string>> {
                return std::make_shared<AckProbeSource>();
            });
    });
}

// probe -> identity -> collecting sink, one subtask each. Under par-1 fusion the
// source and the sink fold into the identity operator's chain. With
// `two_phase_dir` the sink is the two-phase-commit file sink instead, whose
// prepare a test can fail.
JobGraphSpec probe_graph(const std::string& two_phase_dir = {}) {
    JobGraphSpec g;
    OperatorSpec src;
    src.id = "src";
    src.type = "ack_probe_source";
    src.parallelism = 1;
    src.out_channel = std::string{clink::cluster::kChannelString};
    g.ops.push_back(src);
    OperatorSpec mid;
    mid.id = "mid";
    mid.type = "identity_string";
    mid.inputs = {"src"};
    mid.parallelism = 1;
    mid.out_channel = std::string{clink::cluster::kChannelString};
    g.ops.push_back(mid);
    OperatorSpec snk;
    snk.id = "snk";
    snk.type = two_phase_dir.empty() ? "collecting_string_sink" : "file_2pc_sink_string";
    if (!two_phase_dir.empty()) {
        snk.params = {{"dir", two_phase_dir}};
    }
    snk.inputs = {"mid"};
    snk.parallelism = 1;
    snk.out_channel = std::string{clink::cluster::kChannelString};
    g.ops.push_back(snk);
    return g;
}

struct FusionEnv {
    explicit FusionEnv(bool fuse) {
        if (const char* v = std::getenv("CLINK_PLAN_FUSE_PAR1")) {
            previous = v;
        }
        if (fuse) {
            ::setenv("CLINK_PLAN_FUSE_PAR1", "1", 1);
        } else {
            ::unsetenv("CLINK_PLAN_FUSE_PAR1");
        }
    }
    ~FusionEnv() {
        if (previous) {
            ::setenv("CLINK_PLAN_FUSE_PAR1", previous->c_str(), 1);
        } else {
            ::unsetenv("CLINK_PLAN_FUSE_PAR1");
        }
    }
    FusionEnv(const FusionEnv&) = delete;
    FusionEnv& operator=(const FusionEnv&) = delete;
    std::optional<std::string> previous;
};

struct ProbeCluster {
    explicit ProbeCluster(std::uint32_t max_restarts) : coordinator(config(max_restarts)) {
        const auto port = coordinator.start();
        coordinator.expect_workers({"worker-ack-hooks"});
        Worker::Config wcfg;
        wcfg.slot_count = 4;
        worker = std::make_unique<Worker>("worker-ack-hooks", "127.0.0.1", wcfg);
        worker->connect_to_coordinator("127.0.0.1", port);
    }
    ~ProbeCluster() {
        clink::fault::Registry::instance().reset();
        worker->stop();
        coordinator.stop();
    }
    ProbeCluster(const ProbeCluster&) = delete;
    ProbeCluster& operator=(const ProbeCluster&) = delete;

    static Coordinator::Config config(std::uint32_t max_restarts) {
        Coordinator::Config cfg;
        cfg.max_restarts = max_restarts;
        return cfg;
    }

    Coordinator coordinator;
    std::unique_ptr<Worker> worker;
};

std::filesystem::path scratch(const std::string& tag) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ack_hooks_" + tag + "_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    return dir;
}

// The checkpoints the coordinator recorded as complete, from its markers.
std::set<std::uint64_t> completed_markers(const std::filesystem::path& ckpt_dir, JobId job) {
    std::set<std::uint64_t> ids;
    const auto dir = ckpt_dir / "_jobs" / std::to_string(job);
    std::error_code ec;
    for (const auto& e : std::filesystem::directory_iterator(dir, ec)) {
        const auto name = e.path().filename().string();
        if (name.rfind("COMPLETED-", 0) == 0 && name.find('.') == std::string::npos) {
            ids.insert(std::stoull(name.substr(std::string{"COMPLETED-"}.size())));
        }
    }
    return ids;
}

template <typename Pred>
bool await(Pred pred, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(10ms);
    }
    return pred();
}

std::size_t completions_seen() {
    std::lock_guard lk(probe().mu);
    return probe().completions.size();
}

// Every acknowledged record was sealed by a checkpoint that a completed one, in
// the same incarnation's id range, covers; none of a checkpoint that did not
// complete is acknowledged unless a later completed checkpoint of its
// incarnation covers it.
void expect_acks_covered_by_completed(const std::set<std::uint64_t>& completed) {
    std::lock_guard lk(probe().mu);
    ASSERT_FALSE(probe().acked.empty()) << "nothing was acknowledged at all";
    for (const auto& id : probe().acked) {
        const auto it = probe().sealed_by.find(id);
        ASSERT_NE(it, probe().sealed_by.end()) << id << " was acknowledged but never sealed";
        const int incarnation = std::stoi(id.substr(0, id.find(':')));
        const auto ceiling = probe().max_sealed[incarnation];
        const auto cover = completed.lower_bound(it->second);
        EXPECT_TRUE(cover != completed.end() && *cover <= ceiling)
            << id << " was sealed by checkpoint " << it->second
            << " and acknowledged, but no completed checkpoint of its incarnation covers it";
    }
    for (const auto c : probe().completions) {
        EXPECT_TRUE(completed.count(c) == 1)
            << "the source was told checkpoint " << c << " completed, and no marker says so";
    }
    for (const auto& t : probe().notify_threads) {
        EXPECT_EQ(probe().source_threads.count(t), 0u) << "a completion ran on the source thread";
    }
    for (const auto& t : probe().ack_threads) {
        EXPECT_EQ(probe().source_threads.count(t), 1u)
            << "acknowledgements were taken off the source thread";
    }
}

void run_completion_case(bool fuse) {
    register_probe();
    probe().reset();
    FusionEnv env(fuse);
    const auto root = scratch(fuse ? "fused" : "unfused");
    ProbeCluster c(/*max_restarts=*/0);
    ASSERT_TRUE(c.coordinator.await_registrations(clink::test_support::scale_slack(5s)));
    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = (root / "ckpt").string();
    ckpt.interval_ms = 100;
    const auto job = c.coordinator.submit_job(
        probe_graph(), OperatorRegistry::default_instance(), std::vector<PluginBinary>{}, ckpt);
    ASSERT_TRUE(await([] { return completions_seen() >= 4; },
                      clink::test_support::scale_slack(std::chrono::milliseconds{30000})))
        << "the source saw " << completions_seen() << " completions";
    ASSERT_TRUE(c.coordinator.cancel_job(job).ok);
    ASSERT_TRUE(c.coordinator.await_job_completion(job, clink::test_support::scale_slack(30s)));
    expect_acks_covered_by_completed(completed_markers(root / "ckpt", job));
    std::filesystem::remove_all(root);
}

TEST(SourceAckHooks, CompletionsReachASubtaskSourceAndCoverOnlyTheirCheckpoints) {
    run_completion_case(/*fuse=*/false);
}

TEST(SourceAckHooks, CompletionsReachASourceFusedIntoItsChain) {
    run_completion_case(/*fuse=*/true);
}

// The sink fails its prepare at a checkpoint the source has already sealed, and
// the job restarts from the last completed one. That checkpoint's records are
// never acknowledged: no checkpoint above it completes in that incarnation, and
// the next incarnation's completions never reach the old source.
TEST(SourceAckHooks, ACheckpointThatNeverCompletesHasNoRecordAcknowledged) {
    register_probe();
    probe().reset();
    FusionEnv env(false);
    const auto root = scratch("failed");
    ProbeCluster c(/*max_restarts=*/2);
    ASSERT_TRUE(c.coordinator.await_registrations(clink::test_support::scale_slack(5s)));
    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = (root / "ckpt").string();
    ckpt.interval_ms = 100;
    const auto job = c.coordinator.submit_job(probe_graph((root / "out").string()),
                                              OperatorRegistry::default_instance(),
                                              std::vector<PluginBinary>{},
                                              ckpt);
    ASSERT_TRUE(await([] { return completions_seen() >= 2; },
                      clink::test_support::scale_slack(std::chrono::milliseconds{30000})));
    auto& faults = clink::fault::Registry::instance();
    faults.arm({.point = clink::fault::points::kSinkBeforePrepare,
                .ordinal = 1,
                .action = clink::fault::Action::Throw});
    ASSERT_TRUE(await(
        [] {
            std::lock_guard lk(probe().mu);
            return probe().incarnations >= 2;
        },
        clink::test_support::scale_slack(std::chrono::milliseconds{30000})))
        << "the failed prepare did not restart the job";
    const auto before = completions_seen();
    ASSERT_TRUE(await([&] { return completions_seen() >= before + 2; },
                      clink::test_support::scale_slack(std::chrono::milliseconds{30000})))
        << "the restarted job completed no checkpoint";
    ASSERT_TRUE(c.coordinator.cancel_job(job).ok);
    ASSERT_TRUE(c.coordinator.await_job_completion(job, clink::test_support::scale_slack(30s)));
    faults.reset();
    const auto completed = completed_markers(root / "ckpt", job);
    expect_acks_covered_by_completed(completed);
    // The first incarnation sealed checkpoints that never completed, among them
    // the one whose prepare failed, and acknowledged none of their records.
    std::lock_guard lk(probe().mu);
    const auto first_ceiling = probe().max_sealed[1];
    std::size_t never_completed = 0;
    for (const auto& [id, records] : probe().sealed) {
        if (records.empty() || records.front().rfind("1:", 0) != 0) {
            continue;
        }
        if (completed.lower_bound(id) != completed.end() &&
            *completed.lower_bound(id) <= first_ceiling) {
            continue;  // covered by a completion of the first incarnation
        }
        ++never_completed;
        for (const auto& r : records) {
            EXPECT_EQ(std::count(probe().acked.begin(), probe().acked.end(), r), 0)
                << r << " of checkpoint " << id << ", which never completed, was acknowledged";
        }
    }
    EXPECT_GE(never_completed, 1u) << "the first incarnation sealed no checkpoint that failed";
    std::filesystem::remove_all(root);
}

// A source whose capability record refuses a scale-down (the Redis Streams
// source's does: a removed subtask's consumer keeps its pending entries) is
// refused a lower parallelism by the coordinator, with the record's reason,
// before anything drains. The record here is a stand-in, declared for the probe
// and removed again, so the test needs no Redis. It names the operator
// parameter the refusal depends on, as the MQTT source's names shared_group,
// and the operator sets it: the coordinator judges the operator's own
// parameters.
TEST(SourceAckHooks, ACoordinatorRefusesAScaleDownTheSourcesRecordRulesOut) {
    register_probe();
    probe().reset();
    FusionEnv env(false);
    clink::connectors::CapabilityRegistry::instance().declare(
        clink::connectors::ConnectorCapabilities{
            .name = "ack_probe",
            .version = "1",
            .is_source = true,
            .replayable = false,
            .offset_model = clink::connectors::OffsetModel::Acknowledgement,
            .checkpoint_integrated = true,
            .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
            .scale_down_refusal =
                "each subtask owns state on the broker that no other subtask reads",
            .scale_down_refusal_param = "owns_state",
        });
    struct Undeclare {
        ~Undeclare() { clink::connectors::CapabilityRegistry::instance().undeclare("ack_probe"); }
    } undeclare;
    const auto root = scratch("rescale");
    ProbeCluster c(/*max_restarts=*/0);
    ASSERT_TRUE(c.coordinator.await_registrations(clink::test_support::scale_slack(5s)));
    auto g = probe_graph();
    g.ops[0].parallelism = 2;
    g.ops[0].min_parallelism = 1;
    g.ops[0].max_parallelism = 4;
    g.ops[0].params["owns_state"] = "yes";
    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = (root / "ckpt").string();
    ckpt.interval_ms = 100;
    const auto job = c.coordinator.submit_job(
        g, OperatorRegistry::default_instance(), std::vector<PluginBinary>{}, ckpt);
    ASSERT_TRUE(await([] { return completions_seen() >= 2; },
                      clink::test_support::scale_slack(std::chrono::milliseconds{30000})));
    const auto r = c.coordinator.request_operator_rescale(job, "src", 1);
    EXPECT_FALSE(r.ok);
    EXPECT_NE(r.reason.find("cannot be rescaled below parallelism 2"), std::string::npos)
        << r.reason;
    EXPECT_NE(r.reason.find("each subtask owns state on the broker"), std::string::npos)
        << r.reason;
    {
        std::lock_guard lk(probe().mu);
        EXPECT_EQ(probe().incarnations, 2) << "a refused rescale redeployed the source";
    }
    ASSERT_TRUE(c.coordinator.cancel_job(job).ok);
    ASSERT_TRUE(c.coordinator.await_job_completion(job, clink::test_support::scale_slack(30s)));
    std::filesystem::remove_all(root);
}

// A refusal that names a parameter holds only where the operator gives that
// parameter a value; one that names none holds in every mode. A record that
// names a parameter but refuses nothing is a mistake in the record.
TEST(SourceAckHooks, ARefusalNamingAParameterHoldsOnlyWhereTheOperatorSetsIt) {
    auto& reg = clink::connectors::CapabilityRegistry::instance();
    clink::connectors::ConnectorCapabilities rec{
        .name = "ack_probe",
        .version = "1",
        .is_source = true,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::Acknowledgement,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .scale_down_refusal = "each subtask owns state on the broker that no other subtask reads",
        .scale_down_refusal_param = "owns_state",
    };
    EXPECT_TRUE(rec.self_check().empty());
    reg.declare(rec);
    struct Undeclare {
        ~Undeclare() { clink::connectors::CapabilityRegistry::instance().undeclare("ack_probe"); }
    } undeclare;
    using clink::cluster::source_scale_down_refusal;
    EXPECT_EQ(source_scale_down_refusal("ack_probe", {{"owns_state", "yes"}}),
              rec.scale_down_refusal);
    EXPECT_EQ(source_scale_down_refusal("ack_probe", {}), "");
    EXPECT_EQ(source_scale_down_refusal("ack_probe", {{"owns_state", ""}}), "");

    rec.scale_down_refusal_param.clear();
    reg.undeclare("ack_probe");
    reg.declare(rec);
    EXPECT_EQ(source_scale_down_refusal("ack_probe", {}), rec.scale_down_refusal);

    rec.scale_down_refusal.clear();
    rec.scale_down_refusal_param = "owns_state";
    EXPECT_FALSE(rec.self_check().empty())
        << "a record naming the parameter of a refusal it does not have passed its self-check";
}

}  // namespace
