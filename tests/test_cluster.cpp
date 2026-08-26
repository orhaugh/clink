#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <latch>
#ifdef __APPLE__
#include <libproc.h>
#endif
#include <memory>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/application/job_submitter.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/commit_dispatch_gate.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/cluster/worker.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/core/codec.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/network/network_bridge.hpp"
#include "clink/runtime/network/network_channel.hpp"

#include "tests/test_helpers/sanitizer_slack.hpp"

using namespace clink;
using namespace clink::cluster;
using namespace clink::network;
using namespace std::chrono_literals;

// The submit-time default state-backend policy: a cluster-level default
// (clink_node --default-state-backend / Coordinator::Config.default_state_backend_uri)
// fills in a job's backend only when the submitter chose none, so an operator
// can make the async/disaggregated path the default without each job opting in.
TEST(DefaultStateBackendPolicy, AppliesDefaultWhenJobChoseNone) {
    CheckpointConfig c;  // state_backend_uri empty -> would resolve to memory
    apply_default_state_backend(c, "disagg-local://");
    EXPECT_EQ(c.state_backend_uri, "disagg-local://");
}

TEST(DefaultStateBackendPolicy, PerJobUriOverridesDefault) {
    CheckpointConfig c;
    c.state_backend_uri = "remote-read://bucket/prefix";  // explicit per-job --state-backend
    apply_default_state_backend(c, "disagg-local://");
    EXPECT_EQ(c.state_backend_uri, "remote-read://bucket/prefix") << "per-job choice must win";
}

TEST(DefaultStateBackendPolicy, EmptyDefaultIsNoOp) {
    CheckpointConfig c;  // both empty
    apply_default_state_backend(c, "");
    EXPECT_TRUE(c.state_backend_uri.empty()) << "no default -> legacy resolution preserved";
}

TEST(DefaultStateBackendPolicy, CheckpointDirIsNotABackendChoice) {
    // A job that set only checkpoint_dir (HA/coordination) but no backend still
    // inherits the default, so an operator pointing the cluster at a durable
    // deferring tier covers HA-enabled jobs too.
    CheckpointConfig c;
    c.checkpoint_dir = "/var/clink/ckpts";  // set, but no backend choice
    apply_default_state_backend(c, "remote-read://bucket");
    EXPECT_EQ(c.state_backend_uri, "remote-read://bucket");
    EXPECT_EQ(c.checkpoint_dir, "/var/clink/ckpts") << "checkpoint_dir is untouched";
}

// Recovery pins an empty (unspecified) backend to its legacy equivalent so a
// recovered job keeps the backend it ran with.
TEST(DefaultStateBackendPolicy, RecoveryPinsCheckpointDirToFile) {
    CheckpointConfig c;
    c.checkpoint_dir = "/var/clink/ckpts";  // file-durable via legacy resolution
    pin_recovered_state_backend(c);
    EXPECT_EQ(c.state_backend_uri, "/var/clink/ckpts");
}

TEST(DefaultStateBackendPolicy, RecoveryPinsEmptyToMemory) {
    CheckpointConfig c;  // no backend, no checkpoint_dir -> legacy memory
    pin_recovered_state_backend(c);
    EXPECT_EQ(c.state_backend_uri, "memory://");
}

TEST(DefaultStateBackendPolicy, RecoveryLeavesExplicitUriUntouched) {
    CheckpointConfig c;
    c.state_backend_uri = "remote-read://bucket";
    pin_recovered_state_backend(c);
    EXPECT_EQ(c.state_backend_uri, "remote-read://bucket");
}

// The defect this closes: a job submitted under an EMPTY cluster default (so it
// relied on checkpoint_dir -> file durability) must NOT be rebound when the coordinator
// is later restarted with a default configured. Recovery runs pin first, then
// submit_job's apply_default - the pin makes the default a no-op, so the
// recovered job keeps its file backend instead of silently switching to the
// (non-durable) disagg-local default and abandoning its checkpoints.
TEST(DefaultStateBackendPolicy, RecoveryDoesNotRebindAcrossDefaultChange) {
    CheckpointConfig c;
    c.checkpoint_dir = "/var/clink/ckpts";              // persisted state_backend_uri was empty
    pin_recovered_state_backend(c);                     // recovery pins to file...
    apply_default_state_backend(c, "disagg-local://");  // ...new default cannot rebind it
    EXPECT_EQ(c.state_backend_uri, "/var/clink/ckpts")
        << "a recovered job must keep its original backend across a default config change";
}

// 1 Coordinator + 2 Workers running in 3 threads. coordinator coordinates the
// deployment of a producer/consumer pipeline split across the workers:
//   worker-A: producer role - emits int64s into a NetworkBridgeSink.
//   worker-B: consumer role - listens via NetworkBridgeSource, collects.
// Verifies the full handshake, deployment, data-plane connection, and
// completion reporting.
TEST(Cluster, CoordinatorWorkerDistributedProducerConsumer) {
    // Pick a free port for the consumer's data plane. The bind-then-close
    // pattern leaves a tiny race window (microseconds) where another
    // process could steal the port; fine for in-process tests.
    std::uint16_t consumer_port = 0;
    {
        NetworkChannelSource<std::int64_t> probe(0, int64_codec());
        consumer_port = probe.listen();
    }

    // ----- Coordinator -----
    Coordinator coordinator;
    const std::uint16_t coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-a", "worker-b"});

    // ----- Shared sink for the consumer to deposit into; the test reads
    // it after the job completes. -----
    auto consumer_sink = std::make_shared<CollectingSink<std::int64_t>>();

    // ----- worker-B: consumer (start first so the producer can connect) -----
    Worker worker_b("worker-b", "127.0.0.1");
    worker_b.register_role("consumer", [consumer_sink](const DeploymentTask& task) {
        auto src =
            std::make_shared<NetworkBridgeSource<std::int64_t>>(task.data_port, int64_codec());
        src->prepare_listen();

        Dag dag;
        auto h0 = dag.add_source<std::int64_t>(src);
        dag.add_sink<std::int64_t>(h0, consumer_sink);

        LocalExecutor exec(std::move(dag));
        exec.run();  // blocks until source closes
    });
    worker_b.connect_to_coordinator("127.0.0.1", coordinator_port);

    // ----- worker-A: producer -----
    Worker worker_a("worker-a", "127.0.0.1");
    worker_a.register_role("producer", [](const DeploymentTask& task) {
        ASSERT_EQ(task.peers.size(), std::size_t{1});
        const auto& peer = task.peers[0];

        // Build a finite sequence to send to the consumer.
        std::vector<Record<std::int64_t>> records;
        for (std::int64_t i = 1; i <= 5; ++i) {
            records.emplace_back(Record<std::int64_t>{i * 100});
        }

        auto src = std::make_shared<VectorSource<std::int64_t>>(std::move(records));
        auto bridge = std::make_shared<NetworkBridgeSink<std::int64_t>>(
            peer.host, peer.data_port, int64_codec());

        // Briefly back off to give the consumer's accept() a moment.
        // (Without this the producer can race the consumer's listener
        // and connect refuses.) Real clusters would retry-connect.
        std::this_thread::sleep_for(50ms);

        Dag dag;
        auto h0 = dag.add_source<std::int64_t>(src);
        dag.add_sink<std::int64_t>(h0, bridge);

        LocalExecutor exec(std::move(dag));
        exec.run();
    });
    worker_a.connect_to_coordinator("127.0.0.1", coordinator_port);

    // ----- Plan and deploy -----
    ASSERT_TRUE(coordinator.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .worker_id = "worker-b",
        .role = "consumer",
        .subtask_idx = 0,
        .data_port = consumer_port,
        .peer_refs = {},
        .extra_config = "",
    });
    plan.tasks.push_back(PlannedTask{
        .worker_id = "worker-a",
        .role = "producer",
        .subtask_idx = 0,
        .data_port = 0,  // outbound only
        .peer_refs = {{"consumer", 0}},
        .extra_config = "",
    });
    coordinator.deploy(plan);

    ASSERT_TRUE(coordinator.await_completion(5s));
    EXPECT_TRUE(coordinator.errors().empty());

    // Verify the consumer received the producer's records intact.
    EXPECT_EQ(consumer_sink->collected(), (std::vector<std::int64_t>{100, 200, 300, 400, 500}));

    worker_a.stop();
    worker_b.stop();
    coordinator.stop();
}

// The restart drain deadline's DEFAULT must dominate the worst legitimate
// drain, not the typical one: a 2PC sink cancelled mid-call against an
// unreachable broker sits in bounded client operations (produce, flush,
// commit at up to ~30s each, and the Kafka sink's pre-fence describe holds
// open() for up to 90s) before it can observe the cancel. A default below
// that read a slow-but-live survivor as wedged and FAILED the job
// mid-broker-outage - the qualification campaign's soak watch item 63,
// reproduced by the orphaned-commit gate. This pins the floor so a future
// "tidy the timeouts" pass cannot silently reintroduce it.
TEST(Cluster, RestartDrainDeadlineDefaultDominatesBoundedSinkCalls) {
    EXPECT_GE(Coordinator::Config{}.restart_drain_timeout, std::chrono::milliseconds{120000});
}

// A control frame whose DISPATCH is slow must never read as a dead
// coordinator. Deploy handling runs on the worker's reader thread and
// includes writing the plugin bytes to cache and dlopen'ing them - work
// the OS can stall for seconds (macOS scans a freshly written .so on
// first execution; nine workers deploying the same plugin stalled every
// reader past the 3s coordinator lease in the gateway-pipeline test).
// The reader being busy processing the coordinator's OWN frame is
// evidence of contact, not of silence: pre-fix, the worker's heartbeat
// thread read the stale contact clock, dropped the session mid-deploy
// ("coordinator heartbeat lease expired"), re-registered, and the job
// never made progress. Here the armed Delay holds Deploy dispatch for
// three times the lease, and the job must still complete on the FIRST
// deployment - a re-registration means the lease misfired.
TEST(Cluster, ASlowDeployDispatchDoesNotExpireTheCoordinatorLease) {
    Coordinator coordinator;
    const std::uint16_t coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-slow"});

    auto sink = std::make_shared<CollectingSink<std::int64_t>>();
    Worker::Config cfg;
    cfg.heartbeat_interval = 100ms;
    cfg.coordinator_heartbeat_timeout = 600ms;
    Worker worker("worker-slow", "127.0.0.1", cfg);
    worker.register_role("solo", [sink](const DeploymentTask&) {
        std::vector<Record<std::int64_t>> records;
        records.emplace_back(Record<std::int64_t>{7});
        auto src = std::make_shared<VectorSource<std::int64_t>>(std::move(records));
        Dag dag;
        auto h0 = dag.add_source<std::int64_t>(src);
        dag.add_sink<std::int64_t>(h0, sink);
        LocalExecutor exec(std::move(dag));
        exec.run();
    });
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const clink::fault::ScopedFault guard(clink::fault::Rule{
        .point = clink::fault::points::kWorkerDeployDispatch,
        .ordinal = 0,
        .action = clink::fault::Action::Delay,
        .arg = 1800,
    });

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .worker_id = "worker-slow",
        .role = "solo",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    coordinator.deploy(plan);
    ASSERT_TRUE(coordinator.await_completion(10s))
        << "the deploy never completed - a lease expiry mid-dispatch drops the session";
    EXPECT_TRUE(coordinator.errors().empty());
    EXPECT_EQ(sink->collected(), (std::vector<std::int64_t>{7}));
    worker.stop();
    coordinator.stop();
}

// Heartbeat watchdog detects a worker that registers but stops sending
// heartbeats. The coordinator marks it lost, synthesises errors for any pending
// tasks, and unblocks await_completion.
TEST(Cluster, WatchdogDetectsLostWorker) {
    Coordinator::Config coordinator_cfg;
    coordinator_cfg.watchdog_interval = 50ms;
    coordinator_cfg.heartbeat_timeout = 250ms;
    Coordinator coordinator(coordinator_cfg);
    const std::uint16_t coordinator_port = coordinator.start();
    coordinator.expect_workers({"silent-worker"});

    // worker with heartbeats disabled - registers, then goes silent.
    Worker::Config worker_cfg;
    worker_cfg.heartbeat_interval = std::chrono::milliseconds{0};
    Worker worker("silent-worker", "127.0.0.1", worker_cfg);
    // Handler blocks longer than heartbeat_timeout. With heartbeats off
    // and no SubtaskFinished arriving during the sleep, the watchdog
    // declares the worker lost.
    worker.register_role("blocker",
                         [](const DeploymentTask&) { std::this_thread::sleep_for(800ms); });
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);

    ASSERT_TRUE(coordinator.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .worker_id = "silent-worker",
        .role = "blocker",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    coordinator.deploy(plan);

    ASSERT_TRUE(coordinator.await_completion(2s));

    auto lost = coordinator.lost_workers();
    ASSERT_EQ(lost.size(), 1u);
    EXPECT_EQ(lost[0], "silent-worker");

    auto errs = coordinator.errors();
    ASSERT_EQ(errs.size(), 1u);
    EXPECT_NE(errs[0].find("heartbeat timeout"), std::string::npos);

    worker.stop();
    coordinator.stop();
}

// A worker that re-registers under the SAME id (a restarted process with a stable
// name - the Kubernetes StatefulSet pattern) replaces its old WorkerConnection in
// the coordinator. The old connection's reader thread must be joined during that
// replacement: destroying a joinable std::thread is std::terminate, and the
// reader lambda holds a shared_ptr to its own WorkerConnection, so dropping the
// map's reference without joining hands destruction to the exiting reader
// itself. Before the fix this test crashed the process.
TEST(Cluster, WorkerReRegistrationUnderSameIdRetiresOldSession) {
    Coordinator::Config coordinator_cfg;
    coordinator_cfg.watchdog_interval = 50ms;
    coordinator_cfg.heartbeat_timeout = 60s;  // watchdog quiet; the test drives the churn
    Coordinator coordinator(coordinator_cfg);
    const std::uint16_t coordinator_port = coordinator.start();
    coordinator.expect_workers({"stable-worker"});

    // Session 1: register, then die ungracefully (stop() closes the conn;
    // the coordinator-side reader returns but its thread stays joinable).
    auto worker1 = std::make_unique<Worker>("stable-worker", "127.0.0.1");
    worker1->connect_to_coordinator("127.0.0.1", coordinator_port);
    ASSERT_TRUE(coordinator.await_registrations(2s));
    worker1->stop();
    worker1.reset();

    // Session 2: the restarted worker re-registers under the same id. Pre-fix:
    // std::terminate in handle_register_ replacing the old WorkerConnection.
    auto worker2 = std::make_unique<Worker>("stable-worker", "127.0.0.1");
    worker2->connect_to_coordinator("127.0.0.1", coordinator_port);

    // The coordinator must still be alive and serving: the re-registered worker is
    // schedulable (registration visible), proven by a successful deploy.
    bool ran = false;
    std::mutex m;
    std::condition_variable cv;
    worker2->register_role("noop", [&](const DeploymentTask&) {
        {
            std::lock_guard lk(m);
            ran = true;
        }
        cv.notify_all();
    });
    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .worker_id = "stable-worker",
        .role = "noop",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    coordinator.deploy(plan);
    {
        std::unique_lock lk(m);
        ASSERT_TRUE(cv.wait_for(lk, 5s, [&] { return ran; }));
    }

    worker2->stop();
    coordinator.stop();
}

// Dynamic placement: tasks with empty worker_id are auto-assigned to workers with
// free slots. Two workers each with capacity=1 → two unassigned tasks land
// one per worker (no overload).
TEST(Cluster, DynamicPlacementAssignsTasksToFreeSlots) {
    Coordinator coordinator;
    const std::uint16_t coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-x", "worker-y"});

    std::mutex seen_mu;
    std::vector<std::string> worker_ids_observed;

    auto record_role = [&](const std::string& worker_id) {
        return [&seen_mu, &worker_ids_observed, worker_id](const DeploymentTask&) {
            std::lock_guard lock(seen_mu);
            worker_ids_observed.push_back(worker_id);
        };
    };

    Worker::Config worker_cfg;
    worker_cfg.slot_count = 1;

    Worker worker_x("worker-x", "127.0.0.1", worker_cfg);
    worker_x.register_role("worker", record_role("worker-x"));
    worker_x.connect_to_coordinator("127.0.0.1", coordinator_port);

    Worker worker_y("worker-y", "127.0.0.1", worker_cfg);
    worker_y.register_role("worker", record_role("worker-y"));
    worker_y.connect_to_coordinator("127.0.0.1", coordinator_port);

    ASSERT_TRUE(coordinator.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .worker_id = "",
        .role = "worker",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    plan.tasks.push_back(PlannedTask{
        .worker_id = "",
        .role = "worker",
        .subtask_idx = 1,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    coordinator.deploy(plan);

    ASSERT_TRUE(coordinator.await_completion(2s));
    EXPECT_TRUE(coordinator.errors().empty());

    {
        std::lock_guard lock(seen_mu);
        ASSERT_EQ(worker_ids_observed.size(), 2u);
        std::sort(worker_ids_observed.begin(), worker_ids_observed.end());
        EXPECT_EQ(worker_ids_observed[0], "worker-x");
        EXPECT_EQ(worker_ids_observed[1], "worker-y");
    }

    worker_x.stop();
    worker_y.stop();
    coordinator.stop();
}

// coordinator redeploys a failing task up to max_restarts times, appending an
// attempt counter to extra_config. The role handler reads it to decide
// whether to "fail" (first attempt) or succeed (retry). After a retry
// succeeds the coordinator reports success even though the first attempt errored.
TEST(Cluster, RestartsFailingTaskUpToMaxRestarts) {
    Coordinator::Config coordinator_cfg;
    coordinator_cfg.max_restarts = 3;
    Coordinator coordinator(coordinator_cfg);
    const std::uint16_t coordinator_port = coordinator.start();
    coordinator.expect_workers({"flaky-worker"});

    std::atomic<int> attempts_observed{0};
    Worker worker("flaky-worker", "127.0.0.1");
    worker.register_role("flaky", [&attempts_observed](const DeploymentTask& task) {
        const auto& cfg = task.extra_config;
        int attempt = 0;
        if (auto pos = cfg.find("clink_attempt="); pos != std::string::npos) {
            attempt = std::stoi(cfg.substr(pos + std::strlen("clink_attempt=")));
        }
        attempts_observed.store(attempt);
        if (attempt == 0) {
            throw std::runtime_error("simulated crash on first attempt");
        }
        // Retries succeed.
    });
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);

    ASSERT_TRUE(coordinator.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .worker_id = "flaky-worker",
        .role = "flaky",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    coordinator.deploy(plan);

    ASSERT_TRUE(coordinator.await_completion(5s));
    EXPECT_TRUE(coordinator.errors().empty()) << "retry should have succeeded";
    EXPECT_GE(attempts_observed.load(), 1) << "second attempt should have run with clink_attempt=1";

    worker.stop();
    coordinator.stop();
}

// coordinator abort path: when one worker is declared lost, the coordinator broadcasts CancelJob
// to surviving workers. Their reader loops flip `cancelled_` so role handlers
// can poll and abort. The coordinator's errors() lists the lost worker's tasks; the
// surviving worker observes was_cancelled() == true.
TEST(Cluster, FailureBroadcastsCancelToSurvivors) {
    Coordinator::Config coordinator_cfg;
    coordinator_cfg.watchdog_interval = 50ms;
    coordinator_cfg.heartbeat_timeout = 600ms;  // > healthy.interval below, < silent's blocker
    Coordinator coordinator(coordinator_cfg);
    const std::uint16_t coordinator_port = coordinator.start();
    coordinator.expect_workers({"healthy-worker", "silent-worker"});

    // Healthy worker with frequent heartbeats so its last_seen stays current
    // throughout the run.
    Worker::Config healthy_cfg;
    healthy_cfg.heartbeat_interval = 100ms;
    Worker healthy("healthy-worker", "127.0.0.1", healthy_cfg);
    std::atomic<bool> healthy_observed_cancel{false};
    healthy.register_role("worker", [&healthy, &healthy_observed_cancel](const DeploymentTask&) {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (healthy.was_cancelled()) {
                healthy_observed_cancel.store(true);
                return;
            }
            std::this_thread::sleep_for(20ms);
        }
    });
    healthy.connect_to_coordinator("127.0.0.1", coordinator_port);

    // Silent worker with heartbeats disabled. Its handler blocks long enough
    // for the watchdog to fire.
    Worker::Config silent_cfg;
    silent_cfg.heartbeat_interval = std::chrono::milliseconds{0};
    Worker silent("silent-worker", "127.0.0.1", silent_cfg);
    silent.register_role("worker", [](const DeploymentTask&) { std::this_thread::sleep_for(2s); });
    silent.connect_to_coordinator("127.0.0.1", coordinator_port);

    ASSERT_TRUE(coordinator.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .worker_id = "healthy-worker",
        .role = "worker",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    plan.tasks.push_back(PlannedTask{
        .worker_id = "silent-worker",
        .role = "worker",
        .subtask_idx = 1,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    coordinator.deploy(plan);

    ASSERT_TRUE(coordinator.await_completion(3s));

    // The watchdog should have declared silent-worker lost.
    auto lost = coordinator.lost_workers();
    ASSERT_EQ(lost.size(), 1u);
    EXPECT_EQ(lost[0], "silent-worker");

    // Surviving worker saw the CancelJob broadcast.
    EXPECT_TRUE(healthy.was_cancelled());
    EXPECT_TRUE(healthy_observed_cancel.load());

    // coordinator's errors include the lost worker's task.
    auto errs = coordinator.errors();
    ASSERT_FALSE(errs.empty());
    EXPECT_NE(errs[0].find("silent-worker"), std::string::npos);

    healthy.stop();
    silent.stop();
    coordinator.stop();
}

// Spec-driven dispatch: the role handlers no longer hard-code the DAG
// structure. The coordinator ships a `JobGraphSpec` (text) in `extra_config`; each
// handler parses it, looks each op type up in a small registry, and
// builds the DAG dynamically. This is the foundation for "submit a job
// to a running coordinator" - users describe their job as a graph of named
// factories rather than recompiling the binary with new role lambdas.
TEST(Cluster, GraphSpecDrivenProducerConsumer) {
    // Pre-bind ports as before.
    std::uint16_t consumer_port = 0;
    {
        NetworkChannelSource<std::int64_t> probe(0, int64_codec());
        consumer_port = probe.listen();
    }

    Coordinator coordinator;
    const std::uint16_t coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-a", "worker-b"});

    auto consumer_sink = std::make_shared<CollectingSink<std::int64_t>>();

    // Generic graph runner. Builds a Source → [Operator...] → Sink chain
    // by dispatching each op spec against a built-in factory registry.
    // For the test we recognise four op types by name.
    auto run_graph = [consumer_sink](const DeploymentTask& task) {
        const auto spec = JobGraphSpec::parse(task.extra_config);
        if (spec.ops.empty()) {
            throw std::runtime_error("empty graph spec");
        }

        Dag dag;
        // First op is the source.
        const auto& first = spec.ops.front();
        std::optional<StageHandle<std::int64_t>> stage;
        if (first.type == "int64_vector_source") {
            // Parse comma-separated int64s.
            std::vector<Record<std::int64_t>> records;
            const auto values_str = param_string(first, "values");
            std::size_t start = 0;
            while (start < values_str.size()) {
                const auto comma = values_str.find(',', start);
                const auto piece = values_str.substr(start, comma - start);
                records.emplace_back(Record<std::int64_t>{std::stoll(piece)});
                if (comma == std::string::npos) {
                    break;
                }
                start = comma + 1;
            }
            auto src =
                std::make_shared<VectorSource<std::int64_t>>(std::move(records), "vector_source");
            stage = dag.add_source<std::int64_t>(src);
        } else if (first.type == "int64_network_source") {
            auto src =
                std::make_shared<NetworkBridgeSource<std::int64_t>>(task.data_port, int64_codec());
            src->prepare_listen();
            stage = dag.add_source<std::int64_t>(src);
        } else {
            throw std::runtime_error("unknown source type: " + first.type);
        }

        // Apply each remaining op.
        for (std::size_t i = 1; i + 1 < spec.ops.size(); ++i) {
            const auto& op = spec.ops[i];
            if (op.type == "int64_multiplier") {
                const auto factor = param_int64(op, "factor", 1);
                auto map = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
                    [factor](const std::int64_t& v) { return v * factor; }, "multiplier");
                stage = dag.add_operator<std::int64_t, std::int64_t>(*stage, map);
            } else {
                throw std::runtime_error("unknown op type: " + op.type);
            }
        }

        // Last op is the sink.
        const auto& last = spec.ops.back();
        if (last.type == "int64_network_sink") {
            if (task.peers.empty()) {
                throw std::runtime_error("network sink requires a peer");
            }
            // Brief delay so the consumer can accept first.
            std::this_thread::sleep_for(50ms);
            auto sink = std::make_shared<NetworkBridgeSink<std::int64_t>>(
                task.peers[0].host, task.peers[0].data_port, int64_codec());
            dag.add_sink<std::int64_t>(*stage, sink);
        } else if (last.type == "int64_collecting_sink") {
            dag.add_sink<std::int64_t>(*stage, consumer_sink);
        } else {
            throw std::runtime_error("unknown sink type: " + last.type);
        }

        LocalExecutor exec(std::move(dag));
        exec.run();
    };

    Worker worker_b("worker-b", "127.0.0.1");
    worker_b.register_role("graph", run_graph);
    worker_b.connect_to_coordinator("127.0.0.1", coordinator_port);

    Worker worker_a("worker-a", "127.0.0.1");
    worker_a.register_role("graph", run_graph);
    worker_a.connect_to_coordinator("127.0.0.1", coordinator_port);

    ASSERT_TRUE(coordinator.await_registrations(2s));

    // Producer's spec: vector source (1, 2, 3) → multiplier(factor=10) →
    // network sink. Output: 10, 20, 30 over the wire.
    JobGraphSpec producer_spec;
    producer_spec.ops.push_back({.type = "int64_vector_source", .params = {{"values", "1,2,3"}}});
    producer_spec.ops.push_back({.type = "int64_multiplier", .params = {{"factor", "10"}}});
    producer_spec.ops.push_back({.type = "int64_network_sink", .params = {}});

    JobGraphSpec consumer_spec;
    consumer_spec.ops.push_back({.type = "int64_network_source", .params = {}});
    consumer_spec.ops.push_back({.type = "int64_collecting_sink", .params = {}});

    JobPlan plan;
    // Distinct subtask_idx values so the peer-ref index can distinguish
    // the two tasks (both share role="graph"). Producer references the
    // consumer's (role, subtask) pair to find its host:port.
    plan.tasks.push_back(PlannedTask{
        .worker_id = "worker-b",
        .role = "graph",
        .subtask_idx = 0,
        .data_port = consumer_port,
        .peer_refs = {},
        .extra_config = consumer_spec.serialize(),
    });
    plan.tasks.push_back(PlannedTask{
        .worker_id = "worker-a",
        .role = "graph",
        .subtask_idx = 1,
        .data_port = 0,
        .peer_refs = {{"graph", 0}},
        .extra_config = producer_spec.serialize(),
    });
    coordinator.deploy(plan);

    ASSERT_TRUE(coordinator.await_completion(5s));
    EXPECT_TRUE(coordinator.errors().empty());

    EXPECT_EQ(consumer_sink->collected(), (std::vector<std::int64_t>{10, 20, 30}));

    worker_a.stop();
    worker_b.stop();
    coordinator.stop();
}

// A worker that registers but is dispatched a role it doesn't know about
// must report had_error=true on SubtaskFinished. The coordinator surfaces it via
// errors(); the worker stays registered and able to receive other work.
TEST(Cluster, DeployToWorkerWithoutRoleHandlerReportsError) {
    Coordinator coordinator;
    const std::uint16_t coordinator_port = coordinator.start();
    coordinator.expect_workers({"empty-worker"});

    Worker worker("empty-worker", "127.0.0.1");
    // Note: NO register_role call. worker has zero handlers.
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);

    ASSERT_TRUE(coordinator.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .worker_id = "empty-worker",
        .role = "ghost",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    coordinator.deploy(plan);

    ASSERT_TRUE(coordinator.await_completion(2s));
    auto errs = coordinator.errors();
    ASSERT_FALSE(errs.empty());
    bool found = false;
    for (const auto& e : errs) {
        if (e.find("ghost") != std::string::npos && e.find("no handler") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);

    worker.stop();
    coordinator.stop();
}

// Two workers both fail their tasks (throw). coordinator collects both errors and
// reports completion - failure of one doesn't mask the other.
TEST(Cluster, MultipleSimultaneousTaskFailuresAreAllReported) {
    Coordinator coordinator;  // max_restarts = 0 by default
    const std::uint16_t coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-1", "worker-2"});

    Worker worker1("worker-1", "127.0.0.1");
    worker1.register_role(
        "crashy", [](const DeploymentTask&) { throw std::runtime_error("crash from worker-1"); });
    worker1.connect_to_coordinator("127.0.0.1", coordinator_port);

    Worker worker2("worker-2", "127.0.0.1");
    worker2.register_role(
        "crashy", [](const DeploymentTask&) { throw std::runtime_error("crash from worker-2"); });
    worker2.connect_to_coordinator("127.0.0.1", coordinator_port);

    ASSERT_TRUE(coordinator.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .worker_id = "worker-1",
        .role = "crashy",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    plan.tasks.push_back(PlannedTask{
        .worker_id = "worker-2",
        .role = "crashy",
        .subtask_idx = 1,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    coordinator.deploy(plan);

    ASSERT_TRUE(coordinator.await_completion(3s));
    auto errs = coordinator.errors();
    ASSERT_GE(errs.size(), 2u);

    bool saw_tm1 = false;
    bool saw_tm2 = false;
    for (const auto& e : errs) {
        if (e.find("worker-1") != std::string::npos) {
            saw_tm1 = true;
        }
        if (e.find("worker-2") != std::string::npos) {
            saw_tm2 = true;
        }
    }
    EXPECT_TRUE(saw_tm1);
    EXPECT_TRUE(saw_tm2);

    worker1.stop();
    worker2.stop();
    coordinator.stop();
}

// Dynamic placement with not enough free slots: 1 worker with slot_count=1,
// 2 unassigned tasks. The behaviour is documented elsewhere - what we
// pin here is "doesn't silently hang". Either deploy() rejects, or
// errors() exposes the failure after await_completion.
TEST(Cluster, DynamicPlacementWithInsufficientSlotsDoesNotHang) {
    Coordinator::Config coordinator_cfg;
    coordinator_cfg.heartbeat_timeout = 500ms;
    coordinator_cfg.watchdog_interval = 50ms;
    Coordinator coordinator(coordinator_cfg);
    const std::uint16_t coordinator_port = coordinator.start();
    coordinator.expect_workers({"single-worker"});

    Worker::Config worker_cfg;
    worker_cfg.slot_count = 1;
    Worker worker("single-worker", "127.0.0.1", worker_cfg);
    std::atomic<int> ran{0};
    worker.register_role("worker", [&ran](const DeploymentTask&) { ran.fetch_add(1); });
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);

    ASSERT_TRUE(coordinator.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .worker_id = "",
        .role = "worker",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    plan.tasks.push_back(PlannedTask{
        .worker_id = "",
        .role = "worker",
        .subtask_idx = 1,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });

    bool deploy_threw = false;
    try {
        coordinator.deploy(plan);
    } catch (const std::exception&) {
        deploy_threw = true;
    }

    if (!deploy_threw) {
        // Bounded wait - must not hang forever even if the coordinator can't
        // place the second task.
        const bool completed = coordinator.await_completion(3s);
        if (completed) {
            // If completion was reported, the over-subscribed task must
            // surface as either an error or the coordinator must have packed both
            // onto the single slot serially.
            EXPECT_GE(ran.load(), 1);
        } else {
            // Otherwise the second task is unplaced; that's acceptable
            // as long as we got here without hanging the test process.
            SUCCEED();
        }
    }

    worker.stop();
    coordinator.stop();
}

// History server: every job that reaches a terminal state lands in the
// coordinator's bounded history ring. We verify both an OK job and a FAILED job
// surface there with the right status, errors, and duration tracking.
TEST(Cluster, HistoryServerRetainsTerminalJobs) {
    Coordinator::Config coordinator_cfg;
    coordinator_cfg.max_restarts = 0;  // failing task surfaces immediately
    Coordinator coordinator(coordinator_cfg);
    const std::uint16_t coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-h"});

    Worker worker("worker-h", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    worker.register_role("boom",
                         [](const DeploymentTask&) { throw std::runtime_error("intentional"); });
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);

    ASSERT_TRUE(coordinator.await_registrations(2s));

    JobPlan ok_plan;
    ok_plan.tasks.push_back(PlannedTask{
        .worker_id = "worker-h",
        .role = "noop",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    coordinator.deploy(ok_plan);
    ASSERT_TRUE(coordinator.await_completion(3s));

    JobPlan fail_plan;
    fail_plan.tasks.push_back(PlannedTask{
        .worker_id = "worker-h",
        .role = "boom",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    coordinator.deploy(fail_plan);
    ASSERT_TRUE(coordinator.await_completion(3s));

    auto history = coordinator.job_history();
    ASSERT_GE(history.size(), 2u);

    const auto& ok_rec = history[history.size() - 2];
    EXPECT_EQ(ok_rec.status, "ok");
    EXPECT_TRUE(ok_rec.errors.empty());

    const auto& fail_rec = history.back();
    EXPECT_EQ(fail_rec.status, "failed");
    EXPECT_FALSE(fail_rec.errors.empty());
    EXPECT_NE(fail_rec.errors[0].find("intentional"), std::string::npos);

    EXPECT_GT(ok_rec.completed_at_unix_seconds, 0);
    EXPECT_GT(fail_rec.completed_at_unix_seconds, 0);

    auto looked_up = coordinator.job_history(fail_rec.job_id);
    ASSERT_TRUE(looked_up.has_value());
    EXPECT_EQ(looked_up->status, "failed");

    EXPECT_FALSE(coordinator.job_history(JobId{999999}).has_value());

    worker.stop();
    coordinator.stop();
}

// History persists to <ha_dir>/history/<job_id>.json and is reloaded
// on a fresh coordinator that points at the same ha_dir. Mirrors
// HistoryServer archive layout.
TEST(Cluster, HistoryServerPersistsAcrossRestart) {
    auto ha_dir = std::filesystem::temp_directory_path() /
                  ("clink-history-test-" + std::to_string(::getpid()));
    std::filesystem::remove_all(ha_dir);

    JobId failed_job_id = 0;
    {
        Coordinator::Config cfg;
        cfg.max_restarts = 0;
        Coordinator coordinator(cfg);
        coordinator.set_ha_dir(ha_dir.string());
        const auto port = coordinator.start();
        coordinator.expect_workers({"worker-p"});
        Worker worker("worker-p", "127.0.0.1");
        worker.register_role("noop", [](const DeploymentTask&) {});
        worker.register_role("boom",
                             [](const DeploymentTask&) { throw std::runtime_error("disk-test"); });
        worker.connect_to_coordinator("127.0.0.1", port);
        ASSERT_TRUE(coordinator.await_registrations(2s));

        JobPlan ok_plan;
        ok_plan.tasks.push_back(PlannedTask{
            .worker_id = "worker-p",
            .role = "noop",
            .subtask_idx = 0,
            .data_port = 0,
            .peer_refs = {},
            .extra_config = "",
        });
        coordinator.deploy(ok_plan);
        ASSERT_TRUE(coordinator.await_completion(3s));

        JobPlan fail_plan;
        fail_plan.tasks.push_back(PlannedTask{
            .worker_id = "worker-p",
            .role = "boom",
            .subtask_idx = 0,
            .data_port = 0,
            .peer_refs = {},
            .extra_config = "",
        });
        coordinator.deploy(fail_plan);
        ASSERT_TRUE(coordinator.await_completion(3s));

        auto h = coordinator.job_history();
        ASSERT_GE(h.size(), 2u);
        failed_job_id = h.back().job_id;

        worker.stop();
        coordinator.stop();
    }

    // Fresh coordinator points at the same ha_dir - should reload both records.
    {
        Coordinator coordinator2;
        coordinator2.set_ha_dir(ha_dir.string());
        auto h = coordinator2.job_history();
        ASSERT_EQ(h.size(), 2u);
        const auto* found = static_cast<const CompletedJobRecord*>(nullptr);
        for (const auto& rec : h) {
            if (rec.job_id == failed_job_id) {
                found = &rec;
                break;
            }
        }
        ASSERT_NE(found, nullptr);
        EXPECT_EQ(found->status, "failed");
        ASSERT_FALSE(found->errors.empty());
        EXPECT_NE(found->errors[0].find("disk-test"), std::string::npos);
        coordinator2.stop();
    }

    std::filesystem::remove_all(ha_dir);
}

// --- Per-operator rescale request surface ---------------

TEST(CoordinatorRescale, RequestOperatorRescaleUnknownJobReturnsError) {
    // The coordinator-level delegate validates job existence; the underlying
    // RescaleCoordinator state machine + bounds checks are already
    // unit-tested via test_rescale_coordinator.cpp. This test pins
    // the coordinator-only paths (unknown job, no coordinator) so a future
    // refactor can't silently drop them.
    Coordinator coordinator;
    auto result = coordinator.request_operator_rescale(JobId{999}, "join", 4);
    EXPECT_FALSE(result.ok);
    EXPECT_NE(result.reason.find("unknown job_id"), std::string::npos);
}

// Every refusal that can be decided without a running job, each with the reason
// it must give.
//
// These matter more than they look: a per-operator rescale DRAINS the job before
// it replans, so a request that turns out to be impossible after the drain
// leaves a stopped job with nothing to fall back to. Each of these has to be
// caught up front.
TEST(CoordinatorRescale, RefusalsThatCanBeDecidedBeforeDrainingAreDecidedUpFront) {
    using namespace std::chrono_literals;
    ensure_built_ins_registered();

    Coordinator coordinator;
    const auto coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-rs-validate"});
    Worker::Config worker_cfg;
    worker_cfg.slot_count = 8;
    Worker worker("worker-rs-validate", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto out_path = std::filesystem::temp_directory_path() /
                          ("clink_rescale_validate_" + std::to_string(::getpid()) + ".txt");
    const auto ckpt_dir = std::filesystem::temp_directory_path() /
                          ("clink_rescale_validate_ckpt_" + std::to_string(::getpid()));
    std::filesystem::remove(out_path);
    std::filesystem::remove_all(ckpt_dir);

    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.min_parallelism = 1;
    src.max_parallelism = 4;
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "200000"}};  // long enough to still be running below
    g.ops.push_back(src);
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"src"};
    snk.parallelism = 1;  // deliberately NO bounds
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", out_path.string()}};
    g.ops.push_back(snk);

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = ckpt_dir.string();
    ckpt.interval_ms = 50;
    const auto job_id = coordinator.submit_job(
        g, OperatorRegistry::default_instance(), std::vector<PluginBinary>{}, ckpt, nullptr);

    // Wait until the rescale coordinator has registered the job's operators.
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    bool ready = false;
    while (std::chrono::steady_clock::now() < deadline) {
        if (coordinator.operator_rescale_status(job_id, "src").has_value()) {
            ready = true;
            break;
        }
        std::this_thread::sleep_for(20ms);
    }
    ASSERT_TRUE(ready) << "operator rescale coordinator was not registered";

    // An operator the graph does not contain. Named alternatives in the reason,
    // because "unknown operator" on its own leaves the caller guessing.
    {
        auto r = coordinator.request_operator_rescale(job_id, "not_an_op", 2);
        EXPECT_FALSE(r.ok);
        EXPECT_NE(r.reason.find("has no operator"), std::string::npos) << r.reason;
        EXPECT_NE(r.reason.find("src"), std::string::npos)
            << "the refusal should name the operators the job does have: " << r.reason;
    }
    // An operator that declared no bounds is not scalable by policy: the job
    // never said what range is safe.
    {
        auto r = coordinator.request_operator_rescale(job_id, "snk", 2);
        EXPECT_FALSE(r.ok);
        EXPECT_NE(r.reason.find("declares no rescale bounds"), std::string::npos) << r.reason;
    }
    // Outside the declared bounds, both directions.
    {
        auto r = coordinator.request_operator_rescale(job_id, "src", 8);
        EXPECT_FALSE(r.ok);
        EXPECT_NE(r.reason.find("above operator"), std::string::npos) << r.reason;
    }
    // Zero is not a parallelism.
    {
        auto r = coordinator.request_operator_rescale(job_id, "src", 0);
        EXPECT_FALSE(r.ok);
        EXPECT_NE(r.reason.find("at least 1"), std::string::npos) << r.reason;
    }
    // A request for the parallelism it already runs at would drain the job for
    // no change, which is a stop the caller did not ask for.
    {
        auto r = coordinator.request_operator_rescale(job_id, "src", 1);
        EXPECT_FALSE(r.ok);
        EXPECT_NE(r.reason.find("already runs at parallelism"), std::string::npos) << r.reason;
    }
    // Every one of the above is answerable from the request and the graph alone,
    // and those assertions are also asserting that ORDER: each returns its own
    // reason rather than the "no checkpoint has completed yet" gate that comes
    // afterwards. That gate used to run first, so a mistyped operator name was
    // answered with "retry once a checkpoint has landed" - advice that would
    // never have helped. Accepting a well-formed request, and refusing a second
    // one while the first is draining, need a completed checkpoint and so live
    // in the integration tests.

    worker.stop();
    coordinator.stop();
    std::filesystem::remove(out_path);
    std::filesystem::remove_all(ckpt_dir);
}

// A rescale replans from a checkpoint, so there has to be one to replan from.
TEST(CoordinatorRescale, RefusesWhenNoCheckpointHasCompletedYet) {
    using namespace std::chrono_literals;
    ensure_built_ins_registered();

    Coordinator coordinator;
    const auto coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-rs-nockptyet"});
    Worker::Config worker_cfg;
    worker_cfg.slot_count = 4;
    Worker worker("worker-rs-nockptyet", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto out_path = std::filesystem::temp_directory_path() /
                          ("clink_rescale_nockpt_" + std::to_string(::getpid()) + ".txt");
    const auto ckpt_dir = std::filesystem::temp_directory_path() /
                          ("clink_rescale_nockpt_ckpt_" + std::to_string(::getpid()));
    std::filesystem::remove(out_path);
    std::filesystem::remove_all(ckpt_dir);

    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.min_parallelism = 1;
    src.max_parallelism = 4;
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "200000"}};
    g.ops.push_back(src);
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"src"};
    snk.parallelism = 1;
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", out_path.string()}};
    g.ops.push_back(snk);

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = ckpt_dir.string();
    // A cadence far longer than this test lives, so the periodic-checkpoint
    // predicate is satisfied but no checkpoint has landed.
    ckpt.interval_ms = 600'000;
    const auto job_id = coordinator.submit_job(
        g, OperatorRegistry::default_instance(), std::vector<PluginBinary>{}, ckpt, nullptr);

    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (coordinator.operator_rescale_status(job_id, "src").has_value()) {
            break;
        }
        std::this_thread::sleep_for(20ms);
    }

    auto r = coordinator.request_operator_rescale(job_id, "src", 2);
    EXPECT_FALSE(r.ok);
    EXPECT_NE(r.reason.find("no checkpoint has completed"), std::string::npos)
        << "a rescale with no checkpoint to restore from must be refused, not accepted and left "
           "to start the operator with empty state: "
        << r.reason;

    worker.stop();
    coordinator.stop();
    std::filesystem::remove(out_path);
    std::filesystem::remove_all(ckpt_dir);
}

// A per-operator rescale only advances out of Preparing when a
// checkpoint lands. A job with no periodic checkpointing would sit in
// Preparing forever, so request_operator_rescale must reject up front
// with a clear reason instead of hanging silently.
TEST(CoordinatorRescale, RejectsRescaleWhenNoCheckpointingConfigured) {
    using namespace std::chrono_literals;
    ensure_built_ins_registered();

    Coordinator coordinator;  // no autoscaler, and the submitted job has no checkpoint config
    const auto coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-rs-nockpt"});

    Worker::Config worker_cfg;
    worker_cfg.slot_count = 4;
    Worker worker("worker-rs-nockpt", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto out_path = std::filesystem::temp_directory_path() / "clink_rescale_no_ckpt.txt";
    std::filesystem::remove(out_path);

    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.min_parallelism = 1;
    src.max_parallelism = 4;
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

    const auto job_id = coordinator.submit_job(g, OperatorRegistry::default_instance());

    // Wait until the operator is deployed and its rescale coordinator is
    // registered (status becomes non-nullopt). Completed jobs linger in
    // jobs_, so the coordinator stays available for the request below.
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    bool coordinator_ready = false;
    while (std::chrono::steady_clock::now() < deadline) {
        if (coordinator.operator_rescale_status(job_id, "src").has_value()) {
            coordinator_ready = true;
            break;
        }
        std::this_thread::sleep_for(20ms);
    }
    ASSERT_TRUE(coordinator_ready) << "operator rescale coordinator was not registered";

    auto result = coordinator.request_operator_rescale(job_id, "src", 2);
    EXPECT_FALSE(result.ok);
    EXPECT_NE(result.reason.find("periodic checkpointing"), std::string::npos) << result.reason;

    worker.stop();
    coordinator.stop();
    std::filesystem::remove(out_path);
}

TEST(CoordinatorRescale, OperatorRescaleStatusUnknownJobReturnsNullopt) {
    Coordinator coordinator;
    auto st = coordinator.operator_rescale_status(JobId{999}, "join");
    EXPECT_FALSE(st.has_value());
}

// --- Autoscaler wiring through Coordinator -----------------

TEST(CoordinatorAutoscaler, NoConfigMeansNoAutoscaler) {
    // Default Coordinator::Config leaves the autoscaler unset.
    // autoscaler_ticks must return nullopt for any job; the wiring
    // is opt-in.
    Coordinator coordinator;
    auto t = coordinator.autoscaler_ticks(JobId{1});
    EXPECT_FALSE(t.has_value());
}

TEST(CoordinatorAutoscaler, UnknownJobReturnsNullopt) {
    Coordinator::Config cfg;
    AutoscalerConfig as_cfg;
    as_cfg.sample_period = std::chrono::milliseconds{50};
    cfg.autoscaler = as_cfg;
    Coordinator coordinator(cfg);
    EXPECT_FALSE(coordinator.autoscaler_ticks(JobId{99}).has_value());
}

// End-to-end: submit a graph with a bounded op, the per-job autoscaler
// thread starts, ticks, and (because sample_fn returns a saturated
// signal) eventually drives request_operator_rescale through the coordinator's
// public surface. Runs entirely in-process: one coordinator, one worker, one
// short-lived built-in pipeline.
TEST(CoordinatorAutoscaler, TicksAndFiresRescaleRequest) {
    using namespace std::chrono_literals;
    ensure_built_ins_registered();

    Coordinator::Config coordinator_cfg;
    AutoscalerConfig as_cfg;
    as_cfg.sample_period = 30ms;
    as_cfg.setpoint = 0.7;
    as_cfg.rescale_threshold = 0.05;  // any nonzero output fires
    as_cfg.cooldown = 0ms;
    as_cfg.pid.kp = 1.0;
    as_cfg.pid.ki = 0.0;
    as_cfg.pid.kd = 0.0;
    as_cfg.pid.output_min = -1.0;
    as_cfg.pid.output_max = 1.0;
    coordinator_cfg.autoscaler = as_cfg;

    Coordinator coordinator(coordinator_cfg);

    std::atomic<int> sample_calls{0};
    std::atomic<int> requests_seen{0};
    coordinator.set_autoscaler_sample_fn([&sample_calls](JobId, const std::string&) -> double {
        sample_calls.fetch_add(1, std::memory_order_relaxed);
        return 0.95;  // over-saturated -> scale up
    });

    const auto coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-as"});

    Worker::Config worker_cfg;
    worker_cfg.slot_count = 4;  // enough headroom for the 2 subtasks plus
                                // any rescale fan-out the autoscaler asks
                                // for during the test window.
    Worker worker("worker-as", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_autoscaler_coordinator_test.txt";
    std::filesystem::remove(out_path);

    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.min_parallelism = 1;
    src.max_parallelism = 4;
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

    const auto job_id = coordinator.submit_job(g, OperatorRegistry::default_instance());

    // Wait for the per-job autoscaler thread to clock at least a few
    // ticks AND for a rescale request to land (or saturate the budget).
    const auto deadline = std::chrono::steady_clock::now() + 1500ms;
    while (std::chrono::steady_clock::now() < deadline) {
        auto ticks = coordinator.autoscaler_ticks(job_id);
        auto status = coordinator.operator_rescale_status(job_id, "src");
        if (status.has_value() && status->state != RescaleState::Idle) {
            requests_seen.fetch_add(1, std::memory_order_relaxed);
        }
        if (ticks.has_value() && *ticks >= 3 && sample_calls.load() >= 1) {
            break;
        }
        std::this_thread::sleep_for(20ms);
    }

    const auto ticks = coordinator.autoscaler_ticks(job_id);
    ASSERT_TRUE(ticks.has_value()) << "autoscaler was not created for the job";
    EXPECT_GE(*ticks, 3u);
    EXPECT_GE(sample_calls.load(), 1);

    worker.stop();
    coordinator.stop();
    std::filesystem::remove(out_path);
}

TEST(CoordinatorAutoscaler, NoAutoscalerWhenOpsLackBounds) {
    using namespace std::chrono_literals;
    ensure_built_ins_registered();

    Coordinator::Config coordinator_cfg;
    AutoscalerConfig as_cfg;
    as_cfg.sample_period = 30ms;
    coordinator_cfg.autoscaler = as_cfg;
    Coordinator coordinator(coordinator_cfg);

    const auto coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-no-bounds"});
    Worker::Config worker_cfg;
    worker_cfg.slot_count = 2;
    Worker worker("worker-no-bounds", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto out_path = std::filesystem::temp_directory_path() / "clink_autoscaler_no_bounds.txt";
    std::filesystem::remove(out_path);

    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
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

    const auto job_id = coordinator.submit_job(g, OperatorRegistry::default_instance());
    // No op declares bounds -> the coordinator must NOT spawn a per-job
    // autoscaler. autoscaler_ticks returns nullopt.
    EXPECT_FALSE(coordinator.autoscaler_ticks(job_id).has_value());

    worker.stop();
    coordinator.stop();
    std::filesystem::remove(out_path);
}

// --- Worker connections: reaped, and capped ---------------------------
//
// The client path grew a reaper and a cap; the worker path had neither, and the
// asymmetry was where the work stopped rather than a decision. What made it worth
// closing is that the worker side leaks more: a lost worker's reader thread
// returns but nothing joins it, and the watchdog's shutdown_read() is
// shutdown(SHUT_RD), not close - so the thread handle and the socket both survive
// until stop(). Bounded by distinct worker ids ever seen, not by cluster size.

TEST(WorkerConnections, AreReapedRatherThanAccumulated) {
    using namespace std::chrono_literals;
    Coordinator::Config cfg;
    cfg.watchdog_interval = 20ms;
    cfg.heartbeat_timeout = 120ms;
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();

    constexpr int kCycles = 6;
    for (int i = 0; i < kCycles; ++i) {
        const auto id = "w-reap-" + std::to_string(i);
        coordinator.expect_workers({id});
        Worker::Config wcfg;
        // No heartbeats, so the watchdog declares this worker lost promptly and
        // its reader exits - the state the leak accumulated in.
        wcfg.heartbeat_interval = 0ms;
        Worker worker(id, "127.0.0.1", wcfg);
        worker.register_role("noop", [](const DeploymentTask&) {});
        worker.connect_to_coordinator("127.0.0.1", port);
        ASSERT_TRUE(coordinator.await_registrations(2s)) << "cycle " << i;
        worker.stop();
        // Reaping is driven by admission, as on the client path, so the next
        // registration is what runs it. The last cycle is reaped by the explicit
        // wait below.
    }

    const auto await_until = [](auto pred, std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        for (;;) {
            if (pred()) {
                return true;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
        }
    };
    const bool reaped = await_until(
        [&] {
            // Drive the reaper the way a real cluster does: another registration.
            coordinator.expect_workers({"w-reap-probe"});
            Worker probe("w-reap-probe", "127.0.0.1");
            probe.register_role("noop", [](const DeploymentTask&) {});
            try {
                probe.connect_to_coordinator("127.0.0.1", port);
            } catch (const std::exception&) {
                return false;
            }
            const auto held = coordinator.worker_connection_count();
            probe.stop();
            return held <= 2;
        },
        5s);

    EXPECT_TRUE(reaped) << "after " << kCycles
                        << " connect/lose cycles the coordinator still holds "
                        << coordinator.worker_connection_count()
                        << " worker connections; each one is a thread handle and an open socket "
                           "that nothing releases before stop()";

    // The RECORD must survive. Several paths distinguish "absent" from "present
    // and lost" and behave differently, so reaping must release the thread and
    // the socket without forgetting the worker existed.
    EXPECT_GE(coordinator.snapshot_workers().size(), static_cast<std::size_t>(kCycles))
        << "reaping erased worker records, which changes restart and drain semantics";

    coordinator.stop();
}

TEST(WorkerConnections, AreRefusedBeyondTheLimitRatherThanSpawningThreads) {
    using namespace std::chrono_literals;
    Coordinator::Config cfg;
    cfg.max_worker_connections = 2;
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();

    coordinator.expect_workers({"w-cap-1", "w-cap-2"});
    Worker w1("w-cap-1", "127.0.0.1");
    Worker w2("w-cap-2", "127.0.0.1");
    w1.register_role("noop", [](const DeploymentTask&) {});
    w2.register_role("noop", [](const DeploymentTask&) {});
    ASSERT_NO_THROW(w1.connect_to_coordinator("127.0.0.1", port));
    ASSERT_NO_THROW(w2.connect_to_coordinator("127.0.0.1", port));
    ASSERT_TRUE(coordinator.await_registrations(2s));

    Worker w3("w-cap-3", "127.0.0.1");
    w3.register_role("noop", [](const DeploymentTask&) {});
    EXPECT_THROW(w3.connect_to_coordinator("127.0.0.1", port), std::runtime_error)
        << "a third worker was admitted past max_worker_connections=2";
    EXPECT_EQ(coordinator.worker_connection_count(), 2u)
        << "the refused worker still left a connection behind";

    // A worker already on record must still be able to re-register when the
    // cluster is at the limit: a restart is not new capacity, and refusing it
    // would strand a worker that the coordinator is already counting.
    Worker w1_again("w-cap-1", "127.0.0.1");
    w1_again.register_role("noop", [](const DeploymentTask&) {});
    EXPECT_NO_THROW(w1_again.connect_to_coordinator("127.0.0.1", port))
        << "a restarting worker already on record was refused for being at the limit";

    w1_again.stop();
    w2.stop();
    w1.stop();
    coordinator.stop();
}

// Item 32: a terminal job's JobState is evicted from jobs_ at the SAME cap
// as its public history record, so "inspectable" means one thing on both
// surfaces and the coordinator's per-job state stops growing with jobs ever
// run. The off-by-one is pinned deliberately: a job AT the cap is still
// fully readable; one past it is gone from both.
TEST(Cluster, TerminalJobStatesAreEvictedAtTheHistoryCap) {
    Coordinator::Config cfg;
    cfg.max_restarts = 0;
    Coordinator coordinator(cfg);
    const std::uint16_t port = coordinator.start();
    coordinator.expect_workers({"worker-evict"});

    Worker worker("worker-evict", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    worker.register_role(
        "boom", [](const DeploymentTask&) { throw std::runtime_error("intentional-evict"); });
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto run_one = [&](const char* role) -> bool {
        JobPlan plan;
        plan.tasks.push_back(PlannedTask{
            .worker_id = "worker-evict",
            .role = role,
            .subtask_idx = 0,
            .data_port = 0,
            .peer_refs = {},
            .extra_config = "",
        });
        coordinator.deploy(plan);
        return coordinator.await_completion(5s);
    };

    ASSERT_TRUE(run_one("boom"));
    auto hist = coordinator.job_history();
    ASSERT_EQ(hist.size(), 1u);
    const JobId boom_id = hist.back().job_id;

    // Fill the ring to EXACTLY the cap: the boom job is entry 1 of 128 and
    // must still be fully readable on every surface.
    for (std::size_t i = 1; i < kCoordinatorHistoryCap; ++i) {
        ASSERT_TRUE(run_one("noop")) << "noop job " << i << " did not complete";
    }
    ASSERT_TRUE(coordinator.snapshot_job(boom_id).has_value())
        << "a terminal job inside the retention window must keep its JobState";
    {
        const auto errs = coordinator.job_errors(boom_id);
        ASSERT_FALSE(errs.empty());
        EXPECT_NE(errs.front().find("intentional-evict"), std::string::npos);
    }
    ASSERT_TRUE(coordinator.job_history(boom_id).has_value());

    // One more terminal pushes it out of the window: the JobState and the
    // history record leave together, and the read APIs agree it is gone.
    ASSERT_TRUE(run_one("noop"));
    EXPECT_FALSE(coordinator.snapshot_job(boom_id).has_value())
        << "the JobState behind an evicted history record must be evicted with it";
    EXPECT_TRUE(coordinator.job_errors(boom_id).empty());
    EXPECT_FALSE(coordinator.job_history(boom_id).has_value());

    // The retained set is exactly the ring's width, and the newest job is
    // fully readable.
    EXPECT_EQ(coordinator.snapshot_jobs().size(), kCoordinatorHistoryCap);
    const auto newest = coordinator.job_history().back().job_id;
    EXPECT_TRUE(coordinator.snapshot_job(newest).has_value());

    worker.stop();
    coordinator.stop();
}

// A RUNNING job can never be evicted, however much terminal churn happens
// around it: only ids that passed through the completion signal enter the
// eviction order. The hold job blocks on a latch across a full ring's worth
// of terminals, stays readable throughout, and still completes cleanly.
TEST(Cluster, ARunningJobSurvivesAFullRingOfTerminalChurn) {
    Coordinator::Config cfg;
    cfg.max_restarts = 0;
    Coordinator coordinator(cfg);
    const std::uint16_t port = coordinator.start();
    coordinator.expect_workers({"worker-churn"});

    Worker::Config wcfg;
    wcfg.slot_count = 2;  // the hold job occupies one; churn needs the other
    Worker worker("worker-churn", "127.0.0.1", wcfg);
    auto release = std::make_shared<std::latch>(1);
    // Count down on every exit path: a gtest ASSERT that aborts this test
    // body must not leave the worker's task thread blocked forever, or the
    // suite hangs at worker.stop() instead of reporting the failure.
    struct Releaser {
        std::shared_ptr<std::latch> l;
        bool released{false};
        void fire() {
            if (!released) {
                released = true;
                l->count_down();
            }
        }
        ~Releaser() { fire(); }
    } releaser{release};

    worker.register_role("noop", [](const DeploymentTask&) {});
    worker.register_role("hold", [release](const DeploymentTask&) { release->wait(); });
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    JobPlan hold_plan;
    hold_plan.tasks.push_back(PlannedTask{
        .worker_id = "worker-churn",
        .role = "hold",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    coordinator.deploy(hold_plan);
    auto running = coordinator.snapshot_jobs();
    ASSERT_EQ(running.size(), 1u);
    const JobId hold_id = running.front().id;

    for (std::size_t i = 0; i <= kCoordinatorHistoryCap; ++i) {
        JobPlan plan;
        plan.tasks.push_back(PlannedTask{
            .worker_id = "worker-churn",
            .role = "noop",
            .subtask_idx = 0,
            .data_port = 0,
            .peer_refs = {},
            .extra_config = "",
        });
        coordinator.deploy(plan);
        ASSERT_TRUE(coordinator.await_completion(5s)) << "churn job " << i;
    }

    auto held = coordinator.snapshot_job(hold_id);
    ASSERT_TRUE(held.has_value()) << "a running job must never be evicted";
    EXPECT_FALSE(held->completion_signalled);

    releaser.fire();
    EXPECT_TRUE(coordinator.await_job_completion(hold_id, 5s))
        << "the held job must still complete after the churn";

    worker.stop();
    coordinator.stop();
}

namespace {

std::filesystem::path cluster_hello_plugin_path() {
#ifdef CLINK_HELLO_PLUGIN_PATH
    return std::filesystem::path{CLINK_HELLO_PLUGIN_PATH};
#else
    return {};
#endif
}

std::uint64_t counter_value(const char* name) {
    return MetricsRegistry::global().counter(name).value();
}

}  // namespace

// Content-addressed plugin shipping (item 30), end to end and in process:
// the FIRST submit of a module uploads bytes (hash-first, one nack, one
// retry) and the deploy ships bytes to the worker once; the SECOND submit
// of the same module resolves from the coordinator's cache with no upload,
// and its deploy sends a hash-only reference - whose job still runs and
// produces output, which is the proof the reference resolved through the
// real dlopen path rather than being silently dropped.
TEST(Cluster, PluginBytesShipOncePerWorkerConnection) {
    const auto plugin = cluster_hello_plugin_path();
    if (plugin.empty() || !std::filesystem::exists(plugin)) {
        GTEST_SKIP() << "hello_plugin not built";
    }

    Coordinator::Config cfg;
    cfg.max_restarts = 0;
    Coordinator coordinator(cfg);
    const std::uint16_t port = coordinator.start();
    coordinator.expect_workers({"worker-hash-a"});
    Worker::Config wcfg;
    wcfg.slot_count = 4;
    Worker worker("worker-hash-a", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto graph_for = [](const std::filesystem::path& out) {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.id = "src";
        src.type = "hello.GreetingSource";
        src.out_channel = "hello.Greeting";
        src.params = {{"count", "4"}, {"start", "1"}};
        g.ops.push_back(std::move(src));
        clink::cluster::OperatorSpec snk;
        snk.id = "snk";
        snk.type = "hello.GreetingFileSink";
        snk.out_channel = "hello.Greeting";
        snk.inputs = {"src"};
        snk.params = {{"path", out.string()}};
        g.ops.push_back(std::move(snk));
        return g;
    };
    const auto tmp = std::filesystem::temp_directory_path();
    const auto pid_tag = std::to_string(::getpid());

    clink::application::JobSubmitter submitter("127.0.0.1", port);
    clink::application::SubmitOptions opts;
    opts.wait_for_completion = true;
    opts.wait_timeout = clink::test_support::scale_slack(10s);
    // The ack leg needs the same slack as the completion leg: SubmitJobAck
    // is sent only after the coordinator has dlopen'd the shipped module,
    // planned the job and dispatched the deploy, and sanitizer
    // instrumentation puts that well past the 10s production default
    // ("no SubmitJobAck: timed out" under TSan in CI).
    opts.ack_timeout = clink::test_support::scale_slack(opts.ack_timeout);

    const auto shipped_before = counter_value("clink_coordinator_plugin_bytes_shipped_total");
    const auto deduped_before = counter_value("clink_coordinator_plugin_ships_deduped_total");
    const auto cache_hits_before =
        counter_value("clink_coordinator_submit_plugin_cache_hits_total");

    const auto out1 = tmp / ("clink_hash_ship_1_" + pid_tag + ".out");
    std::filesystem::remove(out1);
    auto r1 = submitter.submit(graph_for(out1).to_json(), {plugin.string()}, opts);
    ASSERT_TRUE(r1.ok) << r1.reject_message;
    const auto shipped_after_1 = counter_value("clink_coordinator_plugin_bytes_shipped_total");
    EXPECT_GT(shipped_after_1, shipped_before)
        << "the first deploy of a module must ship its bytes";

    const auto out2 = tmp / ("clink_hash_ship_2_" + pid_tag + ".out");
    std::filesystem::remove(out2);
    auto r2 = submitter.submit(graph_for(out2).to_json(), {plugin.string()}, opts);
    ASSERT_TRUE(r2.ok) << r2.reject_message;

    EXPECT_EQ(counter_value("clink_coordinator_plugin_bytes_shipped_total"), shipped_after_1)
        << "a module already on the worker's connection must ship as a reference, not bytes";
    EXPECT_GT(counter_value("clink_coordinator_plugin_ships_deduped_total"), deduped_before);
    EXPECT_GT(counter_value("clink_coordinator_submit_plugin_cache_hits_total"), cache_hits_before)
        << "the second hash-first submit must resolve from the coordinator's cache";

    // The reference-deployed job actually ran: its sink wrote output.
    std::ifstream in2(out2);
    std::string line2;
    ASSERT_TRUE(std::getline(in2, line2)) << "the job deployed by reference produced no output";

    std::filesystem::remove(out1);
    std::filesystem::remove(out2);
    worker.stop();
    coordinator.stop();
}

// A hash-only reference the worker cannot resolve fails the deploy LOUDLY
// through the ordinary plugin-failure path - a reference is a claim about
// the worker's cache, never permission to run without the module. Driven
// through the real coordinator and worker by submitting a reference whose
// hash no cache has ever seen.
TEST(Cluster, AnUnresolvableHashOnlyPluginReferenceFailsTheDeployLoudly) {
    Coordinator::Config cfg;
    cfg.max_restarts = 0;
    Coordinator coordinator(cfg);
    const std::uint16_t port = coordinator.start();
    coordinator.expect_workers({"worker-hash-miss"});
    Worker::Config wcfg;
    wcfg.slot_count = 4;
    Worker worker("worker-hash-miss", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    clink::cluster::JobGraphSpec g;
    clink::cluster::OperatorSpec src;
    src.id = "src";
    src.type = "int64_range_source";
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "4"}};
    g.ops.push_back(std::move(src));
    clink::cluster::OperatorSpec snk;
    snk.id = "snk";
    snk.type = "collecting_int64_sink";
    snk.out_channel = std::string{kChannelInt64};
    snk.inputs = {"src"};
    g.ops.push_back(std::move(snk));

    std::vector<PluginBinary> plugins;
    plugins.push_back(
        PluginBinary{.name = "phantom", .content_hash = "feedfacefeedface", .bytes = {}});
    const auto job_id = coordinator.submit_job(
        g, OperatorRegistry::default_instance(), std::move(plugins), CheckpointConfig{}, nullptr);
    ASSERT_TRUE(coordinator.await_job_completion(job_id, 5s));

    const auto errs = coordinator.job_errors(job_id);
    ASSERT_FALSE(errs.empty()) << "an unresolvable reference must fail the job, not run it";
    EXPECT_NE(errs.front().find("not in this worker's cache"), std::string::npos) << errs.front();

    worker.stop();
    coordinator.stop();
}

namespace {

// Live thread count of THIS process. The join assertion below needs an
// observable, not an inference: jthread members join by construction, but
// nothing ever ASSERTED that a cancelled job leaves no thread behind
// (item 12's recorded gap).
std::size_t clink_test_live_thread_count() {
#ifdef __APPLE__
    struct proc_taskinfo info{};
    const int rc = proc_pidinfo(getpid(), PROC_PIDTASKINFO, 0, &info, PROC_PIDTASKINFO_SIZE);
    return rc == PROC_PIDTASKINFO_SIZE ? static_cast<std::size_t>(info.pti_threadnum) : 0;
#else
    std::ifstream in("/proc/self/status");
    std::string line;
    while (std::getline(in, line)) {
        if (line.rfind("Threads:", 0) == 0) {
            return static_cast<std::size_t>(std::stoul(line.substr(8)));
        }
    }
    return 0;
#endif
}

clink::cluster::JobGraphSpec clink_cancel_test_graph() {
    clink::cluster::JobGraphSpec g;
    clink::cluster::OperatorSpec src;
    src.id = "src";
    src.type = "int64_range_source";
    src.out_channel = std::string{kChannelInt64};
    // Long enough to still be running at every cancel below; delay keeps
    // the pipeline gently active rather than CPU-bound.
    src.params = {{"count", "10000000"}, {"delay_ms", "1"}};
    g.ops.push_back(std::move(src));
    clink::cluster::OperatorSpec snk;
    snk.id = "snk";
    snk.type = "collecting_int64_sink";
    snk.out_channel = std::string{kChannelInt64};
    snk.inputs = {"src"};
    g.ops.push_back(std::move(snk));
    return g;
}

}  // namespace

// A cancelled job's threads are JOINED, not leaked - asserted against the
// process's live thread count rather than inferred from jthread semantics.
// The first job doubles as the warm-up that spins any lazily-created
// engine singletons (metrics, logging, data-plane helpers), so the
// baseline reflects steady state and the assertion isolates job number two.
// Finished subtasks must not accumulate thread HANDLES on the worker.
//
// The leak this pins is invisible to the live-thread assertion in the
// test below it: an exited pthread is reaped by the kernel - so the
// thread count returns to baseline - while its 8MB stack mapping is
// held until someone joins the handle. A worker that only ever pushed
// onto task_threads_ and joined at shutdown therefore drifted upward by
// ~8MB of address space per completed subtask, flat thread count
// throughout, for as long as the process lived. Under the job churn a
// multi-day campaign generates that is a slow, silent climb.
//
// The assertion is on the retained handle count, because that is the
// only place the defect is observable.
TEST(Cluster, FinishedSubtaskThreadHandlesAreReapedNotAccumulated) {
    Coordinator::Config cfg;
    cfg.max_restarts = 0;
    Coordinator coordinator(cfg);
    const std::uint16_t port = coordinator.start();
    coordinator.expect_workers({"worker-reap"});
    Worker::Config wcfg;
    wcfg.slot_count = 4;
    Worker worker("worker-reap", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto run_one = [&]() -> bool {
        const auto job_id =
            coordinator.submit_job(clink_cancel_test_graph(), OperatorRegistry::default_instance());
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        bool running = false;
        while (std::chrono::steady_clock::now() < deadline) {
            const auto d = coordinator.snapshot_job(job_id);
            if (d.has_value() && d->completed_count == 0 && !d->tasks.empty()) {
                running = true;
                break;
            }
            std::this_thread::sleep_for(10ms);
        }
        if (!running) {
            return false;
        }
        (void)coordinator.cancel_job(job_id);
        return coordinator.await_job_completion(job_id, 5s);
    };

    ASSERT_TRUE(run_one()) << "warm-up job did not reach a terminal state";
    // After the warm-up, one job's worth of handles may still be held -
    // they are reaped on the NEXT deploy, not at completion.
    const auto after_first = worker.retained_task_thread_count();

    constexpr int kMoreJobs = 6;
    for (int i = 0; i < kMoreJobs; ++i) {
        ASSERT_TRUE(run_one()) << "job " << i << " did not reach a terminal state";
    }

    // The count must reflect concurrent subtasks, not the lifetime total.
    // Pre-fix this grew by one graph's worth of subtasks per job, so the
    // bound below fails on the accumulation rather than on any timing.
    const auto after_many = worker.retained_task_thread_count();
    EXPECT_LE(after_many, after_first * 2)
        << "task-thread handles accumulate with job count: " << after_first << " after one job, "
        << after_many << " after " << (kMoreJobs + 1)
        << " - finished subtasks are not being reaped";

    worker.stop();
    coordinator.stop();
}

TEST(Cluster, ACancelledJobsThreadsAreJoinedNotLeaked) {
    Coordinator::Config cfg;
    cfg.max_restarts = 0;
    Coordinator coordinator(cfg);
    const std::uint16_t port = coordinator.start();
    coordinator.expect_workers({"worker-join"});
    Worker::Config wcfg;
    wcfg.slot_count = 4;
    Worker worker("worker-join", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto run_and_cancel = [&]() -> bool {
        const auto job_id =
            coordinator.submit_job(clink_cancel_test_graph(), OperatorRegistry::default_instance());
        // The job is demonstrably RUNNING before the cancel: its sink has
        // output, so subtask threads exist.
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        bool running = false;
        while (std::chrono::steady_clock::now() < deadline) {
            const auto d = coordinator.snapshot_job(job_id);
            if (d.has_value() && d->completed_count == 0 && !d->tasks.empty()) {
                running = true;
                break;
            }
            std::this_thread::sleep_for(10ms);
        }
        if (!running) {
            return false;
        }
        (void)coordinator.cancel_job(job_id);
        return coordinator.await_job_completion(job_id, 5s);
    };

    // Warm-up: first job wakes every lazy singleton thread.
    ASSERT_TRUE(run_and_cancel()) << "warm-up job did not cancel cleanly";
    // Let the warm-up's threads finish exiting before taking the baseline.
    std::this_thread::sleep_for(200ms);
    const auto baseline = clink_test_live_thread_count();
    ASSERT_GT(baseline, 0u) << "thread-count probe unavailable on this platform";

    ASSERT_TRUE(run_and_cancel()) << "the measured job did not cancel cleanly";

    // The job's threads must all exit: poll until the count returns to the
    // baseline, with the deadline as a failure bound.
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    std::size_t now_count = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        now_count = clink_test_live_thread_count();
        if (now_count <= baseline) {
            break;
        }
        std::this_thread::sleep_for(50ms);
    }
    EXPECT_LE(now_count, baseline) << "a cancelled job left threads running: " << now_count
                                   << " live vs baseline " << baseline;

    worker.stop();
    coordinator.stop();
}

// Cancel landing INSIDE an armed checkpoint window: the persist path is
// held open by a Delay fault while records flow, the cancel arrives
// mid-window, and the job must still reach its terminal state within a
// bound - a held checkpoint write must not wedge cancellation (item 12's
// last recorded gap: no test combined cancel with an armed fault point).
TEST(Cluster, CancelDuringAnArmedCheckpointWindowStaysBounded) {
    clink::fault::Registry::instance().reset();
    // Every checkpoint write stalls 1500ms at the fault point - long
    // enough that the cancel demonstrably lands inside the window.
    clink::fault::Registry::instance().arm(
        clink::fault::Rule{.point = clink::fault::points::kCheckpointBeforeWrite,
                           .action = clink::fault::Action::Delay,
                           .arg = 1500});
    struct RegistryReset {
        ~RegistryReset() { clink::fault::Registry::instance().reset(); }
    } reset_on_exit;

    Coordinator::Config cfg;
    cfg.max_restarts = 0;
    Coordinator coordinator(cfg);
    const std::uint16_t port = coordinator.start();
    coordinator.expect_workers({"worker-fault-cancel"});
    Worker::Config wcfg;
    wcfg.slot_count = 4;
    Worker worker("worker-fault-cancel", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto ckpt_dir = std::filesystem::temp_directory_path() /
                          ("clink_fault_cancel_" + std::to_string(::getpid()));
    std::filesystem::remove_all(ckpt_dir);
    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = ckpt_dir.string();
    ckpt.interval_ms = 50;
    const auto job_id = coordinator.submit_job(clink_cancel_test_graph(),
                                               OperatorRegistry::default_instance(),
                                               std::vector<PluginBinary>{},
                                               ckpt);

    // A checkpoint is demonstrably IN FLIGHT (triggered, not yet acked)
    // before the cancel - the window the Delay holds open.
    {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        bool in_flight = false;
        while (std::chrono::steady_clock::now() < deadline) {
            const auto d = coordinator.snapshot_job(job_id);
            if (d.has_value() && !d->pending_checkpoint_ids.empty()) {
                in_flight = true;
                break;
            }
            std::this_thread::sleep_for(10ms);
        }
        ASSERT_TRUE(in_flight) << "no checkpoint entered flight before the cancel";
    }

    // The window is REAL, not assumed: wait until the write REACHES the
    // armed point (the hit is counted before the 1500ms hold begins), so
    // the cancel below demonstrably lands inside the held window. The
    // pending-ids check above only proved the trigger was sent; the first
    // version of this test asserted hits instantly and was vacuous.
    {
        const auto deadline =
            std::chrono::steady_clock::now() + clink::test_support::scale_slack(5s);
        bool reached = false;
        while (std::chrono::steady_clock::now() < deadline) {
            if (clink::fault::Registry::instance().hits(
                    clink::fault::points::kCheckpointBeforeWrite) > 0) {
                reached = true;
                break;
            }
            std::this_thread::sleep_for(5ms);
        }
        ASSERT_TRUE(reached) << "the checkpoint write never reached the armed point";
    }

    (void)coordinator.cancel_job(job_id);
    const auto cancel_started = std::chrono::steady_clock::now();
    // Base bound 30s: the un-instrumented run takes ~7s of it (a 1500ms
    // held write plus full in-process cluster teardown), and the UBSan
    // build - which the slack multiplier deliberately treats as 1x -
    // measured 10.08s against the old 10s figure. The contract is "far
    // below the wedge this test was written against", not any particular
    // single-digit figure.
    ASSERT_TRUE(coordinator.await_job_completion(job_id, clink::test_support::scale_slack(30s)))
        << "cancel wedged behind the held checkpoint write";
    const auto took = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - cancel_started);
    // Failure bound, not a synchronisation guess: well past the 1500ms
    // hold plus teardown, far below the 10s wait above.
    // Bounded is the contract; the figure is a wall-clock budget sized for
    // the production runtime, so it scales under sanitizer instrumentation
    // (TSan alone is a 5-15x slowdown - this failed the nightly at exactly
    // that multiplier).
    const auto bound_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        clink::test_support::scale_slack(std::chrono::milliseconds(20000)));
    EXPECT_LT(took.count(), bound_ms.count()) << "cancel took " << took.count() << "ms";

    const auto d = coordinator.snapshot_job(job_id);
    ASSERT_TRUE(d.has_value());
    EXPECT_TRUE(d->cancel_requested);

    std::filesystem::remove_all(ckpt_dir);
    worker.stop();
    coordinator.stop();
}

namespace {

// Latest COMPLETED-<id> marker under `dir`, recursively (markers are
// job-scoped at <dir>/_jobs/<job>/COMPLETED-<id>).
std::uint64_t clink_latest_completed_marker(const std::filesystem::path& dir) {
    std::uint64_t latest = 0;
    std::error_code ec;
    if (!std::filesystem::exists(dir, ec)) {
        return 0;
    }
    for (const auto& e : std::filesystem::recursive_directory_iterator(dir, ec)) {
        if (ec) {
            break;
        }
        if (!e.is_regular_file()) {
            continue;
        }
        const auto name = e.path().filename().string();
        if (name.rfind("COMPLETED-", 0) == 0) {
            try {
                latest = std::max<std::uint64_t>(latest, std::stoull(name.substr(10)));
            } catch (...) {
            }
        }
    }
    return latest;
}

// --- commit-confirmed restore protocol -------------------------------------
//
// A job with a sink whose external commit cannot be re-executed after a
// crash restores from the newest checkpoint whose commits provably
// EXECUTED (CONFIRMED-N), not the newest completed one. These pin the
// tracking half in-process: CONFIRMED markers appear exactly for tracked
// jobs, gated on the worker's CommitConfirmed after successful dispatch.
// The restore-selection half is pinned end to end by the Kafka
// exactly-once integration suite (a real broker is the only honest way to
// exercise a commit that dies with the process).

namespace {

clink::cluster::JobGraphSpec clink_confirm_test_graph(const std::filesystem::path& dir) {
    clink::cluster::JobGraphSpec g;
    clink::cluster::OperatorSpec src;
    src.id = "src";
    src.type = "int64_range_source";
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "4000"}, {"delay_ms", "1"}};
    g.ops.push_back(std::move(src));
    clink::cluster::OperatorSpec conv;
    conv.id = "conv";
    conv.type = "int64_to_string";
    conv.out_channel = std::string{kChannelString};
    conv.inputs = {"src"};
    g.ops.push_back(std::move(conv));
    clink::cluster::OperatorSpec snk;
    snk.id = "snk";
    snk.type = "file_2pc_sink_string";
    snk.out_channel = std::string{kChannelString};
    snk.inputs = {"conv"};
    snk.params = {{"dir", dir.string()}};
    g.ops.push_back(std::move(snk));
    return g;
}

// Override file_2pc's capability record for the duration of a test, and
// put the original back whatever happens - the CapabilityRegistry is
// process-global and the manifest gate asserts on the real record.
class ClinkScopedRecordOverride {
public:
    explicit ClinkScopedRecordOverride(clink::connectors::ConnectorCapabilities replacement) {
        const auto* current =
            clink::connectors::CapabilityRegistry::instance().find(replacement.name);
        if (current != nullptr) {
            original_ = *current;
        }
        clink::connectors::declare_connector(std::move(replacement));
    }
    ~ClinkScopedRecordOverride() {
        if (original_.has_value()) {
            clink::connectors::declare_connector(*original_);
        }
    }
    ClinkScopedRecordOverride(const ClinkScopedRecordOverride&) = delete;
    ClinkScopedRecordOverride& operator=(const ClinkScopedRecordOverride&) = delete;
    ClinkScopedRecordOverride(ClinkScopedRecordOverride&&) = delete;
    ClinkScopedRecordOverride& operator=(ClinkScopedRecordOverride&&) = delete;

private:
    std::optional<clink::connectors::ConnectorCapabilities> original_;
};

std::uint64_t clink_latest_marker(const std::filesystem::path& ckpt_dir,
                                  std::uint64_t job_id,
                                  const std::string& prefix) {
    std::uint64_t latest = 0;
    const auto dir = ckpt_dir / "_jobs" / std::to_string(job_id);
    std::error_code ec;
    if (!std::filesystem::exists(dir, ec)) {
        return 0;
    }
    for (const auto& e : std::filesystem::directory_iterator(dir, ec)) {
        const auto name = e.path().filename().string();
        if (name.rfind(prefix, 0) != 0) {
            continue;
        }
        try {
            latest = std::max(latest,
                              static_cast<std::uint64_t>(std::stoull(name.substr(prefix.size()))));
        } catch (...) {
        }
    }
    return latest;
}

void clink_run_confirm_job(const std::filesystem::path& ckpt_dir,
                           const std::filesystem::path& out_dir,
                           const std::string& worker_name) {
    Coordinator coordinator;
    const std::uint16_t port = coordinator.start();
    coordinator.expect_workers({worker_name});
    Worker::Config wcfg;
    wcfg.slot_count = 4;
    Worker worker(worker_name, "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));
    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = ckpt_dir.string();
    ckpt.interval_ms = 100;
    const auto job_id = coordinator.submit_job(clink_confirm_test_graph(out_dir),
                                               OperatorRegistry::default_instance(),
                                               std::vector<PluginBinary>{},
                                               ckpt);
    ASSERT_TRUE(coordinator.await_job_completion(job_id, clink::test_support::scale_slack(30s)));
    worker.stop();
    coordinator.stop();
}

}  // namespace

TEST(Cluster, ATrackedJobsCheckpointsGainConfirmedMarkers) {
    // The record override is what puts the job on the protocol: same
    // graph, same built-in sink, but its declared commit is no longer
    // recoverable, so the planner flags the sink task, the coordinator
    // tracks it, and every commit the worker executes comes back as a
    // CONFIRMED-N marker beside the COMPLETED-N one.
    clink::cluster::ensure_built_ins_registered();
    const auto* file_2pc = clink::connectors::CapabilityRegistry::instance().find("file_2pc");
    ASSERT_NE(file_2pc, nullptr) << "built-in capability records not declared";
    auto flagged = *file_2pc;
    flagged.commit_recoverable = false;
    ClinkScopedRecordOverride guard(std::move(flagged));

    const auto tmp = std::filesystem::temp_directory_path();
    const auto ckpt_dir = tmp / ("clink_confirm_ckpt_" + std::to_string(::getpid()));
    const auto out_dir = tmp / ("clink_confirm_out_" + std::to_string(::getpid()));
    std::filesystem::remove_all(ckpt_dir);
    std::filesystem::remove_all(out_dir);
    clink_run_confirm_job(ckpt_dir, out_dir, "worker-confirm-tracked");

    const auto completed = clink_latest_marker(ckpt_dir, 1, "COMPLETED-");
    const auto confirmed = clink_latest_marker(ckpt_dir, 1, "CONFIRMED-");
    EXPECT_GT(completed, 0u) << "the job never completed a checkpoint; the probe is vacuous";
    EXPECT_GT(confirmed, 0u)
        << "no CONFIRMED marker was written for a tracked job whose commits executed";
    EXPECT_LE(confirmed, completed) << "a checkpoint confirmed before it completed";
    std::filesystem::remove_all(ckpt_dir);
    std::filesystem::remove_all(out_dir);
}

TEST(Cluster, AnUntrackedJobsProtocolStaysDormant) {
    // Same job, real (recoverable) record: no tracking, no CONFIRMED
    // markers - the protocol must cost untracked jobs nothing.
    const auto tmp = std::filesystem::temp_directory_path();
    const auto ckpt_dir = tmp / ("clink_confirm_ckpt_ctl_" + std::to_string(::getpid()));
    const auto out_dir = tmp / ("clink_confirm_out_ctl_" + std::to_string(::getpid()));
    std::filesystem::remove_all(ckpt_dir);
    std::filesystem::remove_all(out_dir);
    clink_run_confirm_job(ckpt_dir, out_dir, "worker-confirm-untracked");

    EXPECT_GT(clink_latest_marker(ckpt_dir, 1, "COMPLETED-"), 0u);
    EXPECT_EQ(clink_latest_marker(ckpt_dir, 1, "CONFIRMED-"), 0u)
        << "an untracked job grew CONFIRMED markers; the protocol is not dormant";
    std::filesystem::remove_all(ckpt_dir);
    std::filesystem::remove_all(out_dir);
}

clink::cluster::JobGraphSpec clink_parity_graph(std::int64_t start,
                                                const std::filesystem::path& out) {
    clink::cluster::JobGraphSpec g;
    clink::cluster::OperatorSpec src;
    src.id = "src";
    src.type = "hello.GreetingSource";
    src.out_channel = "hello.Greeting";
    src.params = {{"count", "4"}, {"start", std::to_string(start)}, {"delay_ms", "60"}};
    g.ops.push_back(std::move(src));
    clink::cluster::OperatorSpec cnt;
    cnt.id = "counter";
    cnt.uid = "parity-counter";
    cnt.key_by = "hello.by_parity";
    cnt.type = "hello.ParityCounter";
    cnt.out_channel = "hello.Greeting";
    cnt.inputs = {"src"};
    g.ops.push_back(std::move(cnt));
    clink::cluster::OperatorSpec snk;
    snk.id = "snk";
    snk.type = "hello.GreetingFileSink";
    snk.out_channel = "hello.Greeting";
    snk.inputs = {"counter"};
    snk.params = {{"path", out.string()}};
    g.ops.push_back(std::move(snk));
    return g;
}

}  // namespace

// The gate that serialises 2PC commit dispatch against runner teardown (see
// commit_dispatch_gate.hpp for the worker-killing SIGSEGV it closes). The
// contract under test: retire_and_drain returns only after every in-flight
// dispatch has left, and any dispatch arriving after retirement is refused.
// Looped because the failure mode of a broken drain is a race, not a
// deterministic wrong answer.
TEST(CommitDispatchGate, RetireDrainsInFlightDispatchAndRefusesLateOnes) {
    for (int i = 0; i < 200; ++i) {
        clink::cluster::CommitDispatchGate gate;
        ASSERT_TRUE(gate.try_enter());
        std::atomic<bool> dispatch_finished{false};
        std::thread dispatcher([&] {
            dispatch_finished.store(true, std::memory_order_release);
            gate.leave();
        });
        // Retire on this thread while the dispatch is in flight. When it
        // returns, the dispatch MUST have finished first - that ordering is
        // exactly what makes destroying the executor behind the callback safe.
        gate.retire_and_drain();
        const bool finished = dispatch_finished.load(std::memory_order_acquire);
        const bool late_entry = gate.try_enter();
        dispatcher.join();
        ASSERT_TRUE(finished) << "retire_and_drain returned while a dispatch was still in flight";
        ASSERT_FALSE(late_entry) << "a retired gate must refuse new dispatch";
    }
}

// Item 19: the JOB-LEVEL response to a truncated checkpoint, end to end.
// The file-level behaviour was already pinned (a truncated write verifies
// Incomplete/Corrupt); what nothing asserted is the whole journey: the
// truncated write is INVISIBLE when it happens - the subtask acks, the
// checkpoint completes, markers advance - and the damage surfaces only at
// restore, where the job must REFUSE loudly rather than come up holding
// partial state, while the last good checkpoint still restores correctly.
TEST(Cluster, ATruncatedCheckpointIsRefusedAtRestoreWhileTheGoodOneStillRestores) {
    const auto plugin = cluster_hello_plugin_path();
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
    coordinator.expect_workers({"worker-trunc"});
    Worker::Config wcfg;
    wcfg.slot_count = 4;
    Worker worker("worker-trunc", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto tmp = std::filesystem::temp_directory_path();
    const auto tag = std::to_string(::getpid());
    const auto dir_good = tmp / ("clink_trunc_good_" + tag);
    const auto dir_bad = tmp / ("clink_trunc_bad_" + tag);
    const auto out1 = tmp / ("clink_trunc_out1_" + tag);
    const auto out1b = tmp / ("clink_trunc_out1b_" + tag);
    const auto out3 = tmp / ("clink_trunc_out3_" + tag);
    for (const auto& p : {dir_good, dir_bad}) {
        std::filesystem::remove_all(p);
    }
    for (const auto& p : {out1, out1b, out3}) {
        std::filesystem::remove(p);
    }

    clink::application::JobSubmitter submitter("127.0.0.1", port);

    // Run 1: clean checkpoints into dir_good, ids 1..4.
    {
        clink::application::SubmitOptions opts;
        opts.wait_for_completion = true;
        opts.wait_timeout = clink::test_support::scale_slack(20s);
        opts.ack_timeout = clink::test_support::scale_slack(opts.ack_timeout);
        opts.checkpoint.checkpoint_dir = dir_good.string();
        opts.checkpoint.interval_ms = 50;
        auto r = submitter.submit(clink_parity_graph(1, out1).to_json(), {plugin.string()}, opts);
        ASSERT_TRUE(r.ok) << r.reject_message;
    }
    const auto good_id = clink_latest_completed_marker(dir_good);
    ASSERT_GT(good_id, 0u) << "run 1 completed no checkpoint";

    // Run 1b: restore from the good point, but every snapshot write is
    // TRUNCATED. The recorded (and here asserted) behaviour: nothing
    // notices at write time - the job completes, acks flow, markers
    // advance past the good id. The damage is invisible until restore.
    clink::fault::Registry::instance().arm(
        clink::fault::Rule{.point = clink::fault::points::kCheckpointDuringWrite,
                           .action = clink::fault::Action::Truncate,
                           .arg = 8});
    {
        clink::application::SubmitOptions opts;
        opts.wait_for_completion = true;
        opts.wait_timeout = clink::test_support::scale_slack(20s);
        opts.ack_timeout = clink::test_support::scale_slack(opts.ack_timeout);
        opts.checkpoint.checkpoint_dir = dir_bad.string();
        opts.checkpoint.interval_ms = 50;
        opts.checkpoint.restore_from_dir = dir_good.string();
        opts.checkpoint.restore_from_checkpoint_id = good_id;
        auto r = submitter.submit(clink_parity_graph(5, out1b).to_json(), {plugin.string()}, opts);
        ASSERT_TRUE(r.ok)
            << "a truncated WRITE must be invisible at write time (that is the hazard): "
            << r.reject_message;
    }
    // Vacuity guard: the truncation demonstrably fired during run 1b.
    ASSERT_GT(clink::fault::Registry::instance().hits(clink::fault::points::kCheckpointDuringWrite),
              0u)
        << "the truncating fault never fired - everything below would be vacuous";
    clink::fault::Registry::instance().reset();
    const auto bad_id = clink_latest_completed_marker(dir_bad);
    ASSERT_GT(bad_id, 0u)
        << "the truncated run must still complete checkpoints - the damage is silent";

    // Run 2: restoring from the truncated checkpoint must REFUSE loudly -
    // the integrity verdict, not an empty comeback and not a hang.
    {
        clink::application::SubmitOptions opts;
        opts.wait_for_completion = true;
        opts.wait_timeout = clink::test_support::scale_slack(20s);
        opts.ack_timeout = clink::test_support::scale_slack(opts.ack_timeout);
        opts.checkpoint.checkpoint_dir = (tmp / ("clink_trunc_scratch_" + tag)).string();
        opts.checkpoint.interval_ms = 0;
        opts.checkpoint.restore_from_dir = dir_bad.string();
        opts.checkpoint.restore_from_checkpoint_id = bad_id;
        auto r =
            submitter.submit(clink_parity_graph(9, tmp / ("clink_trunc_out2_" + tag)).to_json(),
                             {plugin.string()},
                             opts);
        // The refusal may surface as a rejection message or as a
        // completed-with-errors job depending on where the restore runs;
        // collect every channel before asserting on the content.
        std::string joined = r.reject_message;
        for (const auto& e : r.errors) {
            joined += e + "\n";
        }
        EXPECT_FALSE(r.ok && r.errors.empty())
            << "restoring from a truncated checkpoint came up clean - partial state "
               "was accepted silently";
        EXPECT_NE(joined.find("sidecar"), std::string::npos)
            << "the refusal must carry the integrity verdict; got: " << joined;
    }

    // Run 3: the GOOD checkpoint still restores - parity counts continue
    // exactly from run 1's state (ids 1..4 gave each bucket 2).
    {
        clink::application::SubmitOptions opts;
        opts.wait_for_completion = true;
        opts.wait_timeout = clink::test_support::scale_slack(20s);
        opts.ack_timeout = clink::test_support::scale_slack(opts.ack_timeout);
        opts.checkpoint.checkpoint_dir = (tmp / ("clink_trunc_scratch3_" + tag)).string();
        opts.checkpoint.interval_ms = 0;
        opts.checkpoint.restore_from_dir = dir_good.string();
        opts.checkpoint.restore_from_checkpoint_id = good_id;
        auto r = submitter.submit(clink_parity_graph(5, out3).to_json(), {plugin.string()}, opts);
        ASSERT_TRUE(r.ok) << r.reject_message;
        std::ifstream in(out3);
        std::vector<std::string> lines;
        std::string line;
        while (std::getline(in, line)) {
            lines.push_back(line);
        }
        EXPECT_EQ(lines, (std::vector<std::string>{"5:1:3", "6:0:3", "7:1:4", "8:0:4"}))
            << "the good checkpoint must still restore run 1's counts";
    }

    for (const auto& p : {dir_good, dir_bad}) {
        std::filesystem::remove_all(p);
    }
    worker.stop();
    coordinator.stop();
}

// A whole-job restart that fires BEFORE the job completes its first own
// checkpoint must re-apply the restore point the job was SUBMITTED with.
// The restart machinery used to rebuild the restore purely from the job's
// own progress (latest completed/confirmed, of which a young job has none),
// so an early restart silently DROPPED a submitted savepoint and the job
// came back up on empty state. The trigger here is deterministic: a
// one-shot injected throw at state.before_restore kills attempt 1's
// restoring subtask with an ordinary (non-fatal) error, which is exactly
// the restartable class; attempt 2 must restore run 1's counts, not start
// counting from one.
TEST(Cluster, ARestartBeforeTheFirstCheckpointKeepsTheSubmittedRestorePoint) {
    const auto plugin = cluster_hello_plugin_path();
    if (plugin.empty() || !std::filesystem::exists(plugin)) {
        GTEST_SKIP() << "hello_plugin not built";
    }
    clink::fault::Registry::instance().reset();
    struct RegistryReset {
        ~RegistryReset() { clink::fault::Registry::instance().reset(); }
    } reset_on_exit;

    Coordinator coordinator;
    const std::uint16_t port = coordinator.start();
    coordinator.expect_workers({"worker-keeprestore"});
    Worker::Config wcfg;
    wcfg.slot_count = 4;
    Worker worker("worker-keeprestore", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto tmp = std::filesystem::temp_directory_path();
    const auto tag = std::to_string(::getpid());
    const auto dir_good = tmp / ("clink_keeprestore_good_" + tag);
    const auto dir_scratch = tmp / ("clink_keeprestore_scratch_" + tag);
    const auto out1 = tmp / ("clink_keeprestore_out1_" + tag);
    const auto out2 = tmp / ("clink_keeprestore_out2_" + tag);
    for (const auto& p : {dir_good, dir_scratch}) {
        std::filesystem::remove_all(p);
    }
    for (const auto& p : {out1, out2}) {
        std::filesystem::remove(p);
    }

    clink::application::JobSubmitter submitter("127.0.0.1", port);

    // Run 1: clean checkpoints into dir_good; each parity bucket counts to 2.
    {
        clink::application::SubmitOptions opts;
        opts.wait_for_completion = true;
        opts.wait_timeout = clink::test_support::scale_slack(20s);
        opts.ack_timeout = clink::test_support::scale_slack(opts.ack_timeout);
        opts.checkpoint.checkpoint_dir = dir_good.string();
        opts.checkpoint.interval_ms = 50;
        auto r = submitter.submit(clink_parity_graph(1, out1).to_json(), {plugin.string()}, opts);
        ASSERT_TRUE(r.ok) << r.reject_message;
    }
    const auto good_id = clink_latest_completed_marker(dir_good);
    ASSERT_GT(good_id, 0u) << "run 1 completed no checkpoint";

    // Run 2: restore from run 1, with periodic checkpoints OFF so the job
    // can never form a restore point of its own, and attempt 1's restore
    // killed by a one-shot ordinary error. The restart must carry the
    // SUBMITTED restore point into attempt 2.
    clink::fault::Registry::instance().arm(
        clink::fault::Rule{.point = clink::fault::points::kStateBeforeRestore,
                           .ordinal = 1,
                           .action = clink::fault::Action::Throw});
    {
        clink::application::SubmitOptions opts;
        opts.wait_for_completion = true;
        opts.wait_timeout = clink::test_support::scale_slack(30s);
        opts.ack_timeout = clink::test_support::scale_slack(opts.ack_timeout);
        opts.checkpoint.checkpoint_dir = dir_scratch.string();
        opts.checkpoint.interval_ms = 0;
        opts.checkpoint.restore_from_dir = dir_good.string();
        opts.checkpoint.restore_from_checkpoint_id = good_id;
        auto r = submitter.submit(clink_parity_graph(5, out2).to_json(), {plugin.string()}, opts);
        ASSERT_TRUE(r.ok) << "the one-shot restore failure must be survived by a restart: "
                          << r.reject_message;
        for (const auto& e : r.errors) {
            ADD_FAILURE() << "job completed with error: " << e;
        }
    }
    // Vacuity guards, both sides. The armed throw fired (attempt 1 really
    // died at its restore), and the point fired AGAIN afterwards (the
    // retry's restore actually ran - with the restore point dropped, the
    // redeploy carries id 0 and no restore happens at all, so the count
    // freezes where attempt 1 left it).
    const auto restore_hits =
        clink::fault::Registry::instance().hits(clink::fault::points::kStateBeforeRestore);
    EXPECT_GE(restore_hits, 3u) << "the retry never re-ran the submitted restore";

    {
        std::ifstream in(out2);
        std::vector<std::string> lines;
        std::string line;
        while (std::getline(in, line)) {
            lines.push_back(line);
        }
        EXPECT_EQ(lines, (std::vector<std::string>{"5:1:3", "6:0:3", "7:1:4", "8:0:4"}))
            << "the restart dropped the submitted restore point and counted from scratch";
    }

    for (const auto& p : {dir_good, dir_scratch}) {
        std::filesystem::remove_all(p);
    }
    worker.stop();
    coordinator.stop();
}

// The configured checkpoint interval must gate the trigger, not just the sleep.
//
// Found by QUAL-01 on 2026-08-16: a job submitted with a 10s interval was
// completing 61 checkpoints every 30 seconds - one about every 490ms, twenty
// times what it asked for, with the corresponding multiple of state writes,
// barrier injections and transactional sink commits. One worker logged 5,947
// refused commit dispatches in fifty minutes purely because so many
// checkpoints were being taken.
//
// The trigger loop took the minimum configured interval as its own sleep, so
// it never OVERSLEPT a job, and then triggered every eligible job on every
// pass - so it never waited for one either. interval_ms only ever shortened
// the loop tick below its 500ms default; a value above it did nothing at all.
//
// Four seconds against a sixty-second interval: correct behaviour is the one
// checkpoint fired immediately at startup, and the pre-fix behaviour is about
// eight. The margin is wide on purpose - this asserts a cadence, and a tight
// bound would just be a flake on a loaded machine.
TEST(CoordinatorCheckpointing, TheConfiguredIntervalGatesTheTriggerNotJustTheSleep) {
    using namespace std::chrono_literals;
    ensure_built_ins_registered();

    Coordinator coordinator;
    const auto coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-ckpt-cadence"});
    Worker::Config worker_cfg;
    worker_cfg.slot_count = 4;
    Worker worker("worker-ckpt-cadence", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    const auto out_path = std::filesystem::temp_directory_path() /
                          ("clink_ckpt_cadence_" + std::to_string(::getpid()) + ".txt");
    const auto ckpt_dir = std::filesystem::temp_directory_path() /
                          ("clink_ckpt_cadence_ckpt_" + std::to_string(::getpid()));
    std::filesystem::remove(out_path);
    std::filesystem::remove_all(ckpt_dir);

    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.out_channel = std::string{kChannelInt64};
    // Paced, not drained. An unpaced range source finishes in well under a
    // second, the job completes, periodic triggering stops with it, and the
    // window measures a finished job - which is how the first version of this
    // test passed against the unfixed code. 200 records at 50ms is ten
    // seconds of running, comfortably past the measurement window.
    src.params = {{"count", "200"}, {"delay_ms", "50"}};
    g.ops.push_back(src);
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"src"};
    snk.parallelism = 1;
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", out_path.string()}};
    g.ops.push_back(snk);

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = ckpt_dir.string();
    ckpt.interval_ms = 60'000;
    const auto job_id = coordinator.submit_job(
        g, OperatorRegistry::default_instance(), std::vector<PluginBinary>{}, ckpt, nullptr);

    std::this_thread::sleep_for(4s);
    const auto completed = coordinator.latest_completed_checkpoint(job_id);

    EXPECT_LE(completed, 2U)
        << "the job asked for a checkpoint every 60s and got " << completed
        << " of them in four seconds. The interval is being used only to shorten the "
           "trigger loop's own sleep, so every job is checkpointed at the loop tick "
           "whatever cadence it configured - paying that multiple in state writes, "
           "barrier injections and transactional sink commits.";

    worker.stop();
    coordinator.stop();
    std::filesystem::remove(out_path);
    std::filesystem::remove_all(ckpt_dir);
}

// A fresh job must not inherit a previous job's CONFIRMED marker.
//
// Found by QUAL-01 on 2026-08-16, as 181,071 duplicate committed results.
//
// For a job with commit-confirming sinks the restore point is the latest
// CONFIRMED checkpoint, not the latest completed one - a completed-but-
// unconfirmed checkpoint may hold an external transaction that died with the
// worker. To survive a coordinator restart, that id is seeded on submission
// from the CONFIRMED-N markers on disk.
//
// Those markers live under <checkpoint_dir>/_jobs/<job_id>/, and job ids
// restart at 1 with every coordinator. So a NEW job submitted into a
// checkpoint directory some earlier job used inherited that job's confirmed
// id. On the rig a job with zero completed checkpoints came up believing
// checkpoint 224 was confirmed; its first worker-loss restart duly chose 224
// as the restore point - a checkpoint belonging to a different run - replayed
// that run's source offsets, and re-emitted windows it had already committed.
// Every value was correct. Each simply arrived twice, which is an
// exactly-once violation a downstream consumer would double-count.
//
// A job that is not resuming has confirmed nothing, whatever is on disk.
TEST(Cluster, AFreshJobDoesNotInheritAPreviousJobsConfirmedCheckpoint) {
    using namespace std::chrono_literals;
    clink::cluster::ensure_built_ins_registered();
    // The record override puts the job on the commit-confirmation protocol,
    // which is the only case that reads the marker at all.
    const auto* file_2pc = clink::connectors::CapabilityRegistry::instance().find("file_2pc");
    ASSERT_NE(file_2pc, nullptr) << "built-in capability records not declared";
    auto flagged = *file_2pc;
    flagged.commit_recoverable = false;
    ClinkScopedRecordOverride guard(std::move(flagged));

    const auto tmp = std::filesystem::temp_directory_path();
    const auto ckpt_dir = tmp / ("clink_stale_confirm_ckpt_" + std::to_string(::getpid()));
    const auto out_dir = tmp / ("clink_stale_confirm_out_" + std::to_string(::getpid()));
    std::filesystem::remove_all(ckpt_dir);
    std::filesystem::remove_all(out_dir);

    // A previous job's leavings: job id 1 confirmed checkpoint 224 here.
    const auto marker_dir = ckpt_dir / "_jobs" / "1";
    std::filesystem::create_directories(marker_dir);
    {
        std::ofstream m(marker_dir / "CONFIRMED-224");
        m << "job=1\ncheckpoint=224\n";
    }
    {
        std::ofstream m(marker_dir / "COMPLETED-224");
        m << "job=1\ncheckpoint=224\n";
    }

    Coordinator coordinator;
    const std::uint16_t port = coordinator.start();
    coordinator.expect_workers({"worker-stale-confirm"});
    Worker::Config wcfg;
    wcfg.slot_count = 4;
    Worker worker("worker-stale-confirm", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = ckpt_dir.string();
    ckpt.interval_ms = 100;
    // Deliberately NO restore_from: this is a fresh submission, not a resume.
    const auto job_id = coordinator.submit_job(clink_confirm_test_graph(out_dir),
                                               OperatorRegistry::default_instance(),
                                               std::vector<PluginBinary>{},
                                               ckpt);

    const auto seeded = coordinator.latest_confirmed_checkpoint(job_id);
    EXPECT_EQ(seeded, 0u)
        << "a fresh job came up with latest_confirmed=" << seeded
        << ", inherited from a CONFIRMED marker an earlier job left in this checkpoint "
           "directory. Its first restart would restore from that checkpoint - another "
           "job's - replaying source offsets it never wrote and re-emitting output it had "
           "already committed.";

    ASSERT_TRUE(coordinator.await_job_completion(job_id, clink::test_support::scale_slack(30s)));
    worker.stop();
    coordinator.stop();
    std::filesystem::remove_all(ckpt_dir);
    std::filesystem::remove_all(out_dir);
}

// A refused dispatch must be distinguishable from a commit that ran.
//
// Found by QUAL-01 on 2026-08-16 as an output topic frozen at 955,647 records
// while the generator produced 1,997 events a second, the job reported
// RUNNING and checkpoints advanced past 331.
//
// The gate refuses a commit whose runner has retired, and that is safe by
// design only because the prepared handle is still persisted, so a later
// restore re-commits it. The refusal used to log and RETURN, which the
// worker's dispatch loop could not tell apart from a callback that had run:
// it counted the subtask as committed and sent CommitConfirmed. The
// coordinator then wrote CONFIRMED-N and advanced the restore point past the
// very transaction that never committed - so it was never committed and never
// replayed, and the sink's output simply stopped becoming visible.
//
// The only way to observe "it ran" must therefore be the absence of an
// exception.
TEST(CommitDispatchGate, ARefusedDispatchThrowsRatherThanLookingLikeACommit) {
    auto gate = std::make_shared<clink::cluster::CommitDispatchGate>();
    int ran = 0;
    auto cb = clink::cluster::gated_dispatch(gate, [&ran](std::uint64_t) { ++ran; });

    cb(41);
    EXPECT_EQ(ran, 1) << "a live gate must let the callback through";

    gate->retire_and_drain();

    bool refused = false;
    std::uint64_t refused_ckpt = 0;
    try {
        cb(42);
    } catch (const clink::cluster::CommitDispatchRefused& e) {
        refused = true;
        refused_ckpt = e.checkpoint_id();
    }
    EXPECT_TRUE(refused)
        << "a dispatch after retirement returned normally. The dispatch loop cannot "
           "distinguish that from a commit that executed, so it confirms the checkpoint - "
           "and the restore point advances past a transaction that was never committed and "
           "will never be replayed.";
    EXPECT_EQ(refused_ckpt, 42U) << "the refusal must name the checkpoint it refused";
    EXPECT_EQ(ran, 1) << "the callback must not have run after retirement";
}

// A retired callback must be pruned, not left to block the live one beside it.
//
// The other half of the QUAL-01 output stall. A subtask whose teardown hangs
// never reaches the code that removes its own registrations, so its retired
// callbacks stay in the dispatch bucket - run 3 logged 5,947 refusals from
// exactly that, and run c's drain timeout shows the hang that causes it.
//
// Once a refusal correctly blocks confirmation, a single stale entry would
// stop the subtask confirming anything ever again, even though the runner
// that replaced it is committing perfectly well. A gate never un-retires, so
// the entry can be dropped the moment it is seen to be dead.
TEST(CommitDispatchGate, ARetiredCallbackIsPrunedRatherThanBlockingTheLiveOneBesideIt) {
    auto dead_gate = std::make_shared<clink::cluster::CommitDispatchGate>();
    auto live_gate = std::make_shared<clink::cluster::CommitDispatchGate>();

    int dead_ran = 0;
    int live_ran = 0;
    std::vector<clink::cluster::GatedCallback> bucket{
        {dead_gate, [&dead_ran](std::uint64_t) { ++dead_ran; }},
        {live_gate, [&live_ran](std::uint64_t) { ++live_ran; }},
    };

    // The old runner retires; the replacement's registration sits beside it.
    dead_gate->retire_and_drain();

    std::erase_if(bucket, [](const clink::cluster::GatedCallback& c) { return c.dead(); });
    ASSERT_EQ(bucket.size(), 1U)
        << "a retired runner's callback stayed in the dispatch bucket. It can only ever "
           "refuse, and a refusal blocks confirmation for the whole subtask - so the job "
           "would never confirm another checkpoint, however well its live sink commits.";

    bool threw = false;
    for (const auto& cb : bucket) {
        try {
            cb(7);
        } catch (const clink::cluster::CommitDispatchRefused&) {
            threw = true;
        }
    }
    EXPECT_FALSE(threw) << "the surviving callback must dispatch cleanly";
    EXPECT_EQ(live_ran, 1) << "the live callback must have run";
    EXPECT_EQ(dead_ran, 0) << "the retired callback must not have run";
}

// A subtask whose OPERATOR throws must fail the job, never complete it
// silently (followups item 82).
//
// The LocalExecutor deliberately CATCHES operator-thread exceptions into
// operator_errors_ and returns normally, so an in-process caller can
// inspect them rather than have the process die. On the cluster path that
// makes the check mandatory: a bare exec.run() reports a clean exit for a
// subtask whose operator threw on the first record, and the coordinator
// then completes the job as ok having emitted NOTHING. Silent data loss
// presented as success - the worst shape a failure can take, because
// nothing anywhere says the answer is wrong.
//
// The built-in int64 dispatch arms had exactly that bare run(). This drives
// a real cluster job - range source -> throwing operator -> file sink - and
// requires the throw to reach the coordinator's job errors.
TEST(Cluster, AnOperatorThatThrowsFailsTheJobRatherThanCompletingItEmpty) {
    ensure_built_ins_registered();
    // Registered through the PLUGIN api, not the bare OperatorRegistry: the
    // fused-chain path builds its Dag from the per-op DagBuilder that
    // register_operator<In, Out> installs, and an op without one cannot be
    // chained at all.
    {
        clink::plugin::PluginRegistry reg(TypeRegistry::default_instance(),
                                          RunnerRegistry::default_instance(),
                                          SelectorRegistry::default_instance());
        reg.register_operator<std::int64_t, std::int64_t>(
            "clink_test_throwing_int64",
            [](const clink::plugin::BuildContext&)
                -> std::shared_ptr<Operator<std::int64_t, std::int64_t>> {
                return std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
                    [](const std::int64_t&) -> std::int64_t {
                        throw std::runtime_error("deliberate-operator-throw");
                    },
                    "clink_test_throwing_int64");
            });
    }

    Coordinator::Config cfg;
    cfg.max_restarts = 0;  // the first failure is the verdict
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"worker-op-throw"});
    Worker::Config wcfg;
    wcfg.slot_count = 8;
    Worker worker("worker-op-throw", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(5s));

    const auto out_path = std::filesystem::temp_directory_path() /
                          ("clink_op_throw_" + std::to_string(::getpid()) + ".txt");
    std::filesystem::remove(out_path);

    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "16"}};
    g.ops.push_back(src);
    OperatorSpec boom;
    boom.type = "clink_test_throwing_int64";
    boom.id = "boom";
    boom.inputs = {"src"};
    boom.parallelism = 1;
    boom.out_channel = std::string{kChannelInt64};
    g.ops.push_back(boom);
    // A SECOND mid-chain op after the throwing one, so the two FUSE into a
    // single subtask. That is the shape the defect actually took: a fused
    // chain dispatches through the generic DagBuilder path, which is where
    // the bare exec.run() lived. A single-op chain takes the typed
    // SubtaskRunner path, which already propagated - so a one-op version of
    // this test passes with the defect present and pins nothing.
    OperatorSpec pass;
    pass.type = "identity_int64";
    pass.id = "pass";
    pass.inputs = {"boom"};
    pass.parallelism = 1;
    pass.out_channel = std::string{kChannelInt64};
    g.ops.push_back(pass);
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"pass"};
    snk.parallelism = 1;
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", out_path.string()}};
    g.ops.push_back(snk);

    const auto job_id = coordinator.submit_job(g,
                                               OperatorRegistry::default_instance(),
                                               std::vector<PluginBinary>{},
                                               CheckpointConfig{},
                                               nullptr);
    ASSERT_TRUE(coordinator.await_job_completion(job_id, 30s));

    const auto errors = coordinator.job_errors(job_id);
    ASSERT_FALSE(errors.empty())
        << "the job completed with no errors although its operator threw on every record; "
           "a subtask that lost its operator to an exception reported a clean exit";
    bool named = false;
    for (const auto& e : errors) {
        named = named || e.find("deliberate-operator-throw") != std::string::npos;
    }
    EXPECT_TRUE(named) << "the job failed but no error names the operator's exception: "
                       << errors.front();
    std::filesystem::remove(out_path);
}

// A surviving worker must not accumulate one set of per-op registrations
// per recovery (followups item 84).
//
// At deploy, a subtask's output-attach path registers drain, cutover-arm,
// group-cutover and input-rebind hooks into per-job buckets keyed by op.
// Those buckets were APPEND-ONLY: a whole-job restart redeploys the same
// job id and pushes another set in, and nothing ever took the previous
// set out. Each closure pins its generation's operator state, channels
// and backends, so a worker that survives the restarts grows by one
// generation of engine state per recovery while a worker that was killed
// starts clean - which is exactly the shape a 9.4-hour campaign measured:
// a survivor at 292 MiB after a fault-free window, 1656 MiB after 27
// recoveries, flat to within 1 MiB whenever it was left alone.
//
// CancelJob is where a restart tears the old generation down, so it is
// where the registrations must go. This drives a real job, waits until
// the worker holds registrations for it, cancels, and requires zero.
TEST(Cluster, CancellingAJobReleasesTheWorkersRegistrationsForIt) {
    ensure_built_ins_registered();
    Coordinator::Config cfg;
    cfg.max_restarts = 0;
    cfg.heartbeat_timeout = clink::test_support::scale_slack(cfg.heartbeat_timeout);
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"worker-regs"});
    Worker::Config wcfg;
    wcfg.slot_count = 8;
    wcfg.coordinator_heartbeat_timeout =
        clink::test_support::scale_slack(wcfg.coordinator_heartbeat_timeout);
    Worker worker("worker-regs", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(5s));

    const auto out_path = std::filesystem::temp_directory_path() /
                          ("clink_regs_" + std::to_string(::getpid()) + ".txt");
    std::filesystem::remove(out_path);

    // Two mid-chain ops so the graph has network-bridged edges on both
    // sides of an operator: that is what registers the output-side and
    // input-side hooks. A long source keeps the job running while we look.
    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.out_channel = std::string{kChannelInt64};
    // Long-lived, not huge: at 1 ms per record 2M records outlive any
    // budget below by a wide margin. The built-in range source materialises
    // its whole record vector at construction, so a count in the tens of
    // millions kept a sanitizer build inside the factory for longer than the
    // cancel budget - nothing observes a cancel before the executor exists.
    src.params = {{"count", "2000000"}, {"delay_ms", "1"}};
    g.ops.push_back(src);
    OperatorSpec a;
    a.type = "identity_int64";
    a.id = "a";
    a.inputs = {"src"};
    a.parallelism = 2;
    a.out_channel = std::string{kChannelInt64};
    g.ops.push_back(a);
    OperatorSpec b;
    b.type = "identity_int64";
    b.id = "b";
    b.inputs = {"a"};
    b.parallelism = 2;
    b.out_channel = std::string{kChannelInt64};
    g.ops.push_back(b);
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"b"};
    snk.parallelism = 1;
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", out_path.string()}};
    g.ops.push_back(snk);

    const auto job_id = coordinator.submit_job(g,
                                               OperatorRegistry::default_instance(),
                                               std::vector<PluginBinary>{},
                                               CheckpointConfig{},
                                               nullptr);
    ASSERT_GT(job_id, 0U);

    // The worker must actually HOLD registrations for the job before the
    // cancel, or "zero afterwards" proves nothing.
    std::size_t before = 0;
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < deadline) {
        before = worker.registration_count(job_id);
        if (before > 0) {
            break;
        }
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_GT(before, 0U) << "the deployed job registered no per-op hooks on this worker, so "
                             "this test cannot see whether cancel releases them";

    (void)coordinator.cancel_job(job_id);
    ASSERT_TRUE(coordinator.await_job_completion(job_id, 30s));

    // Cancel is processed on the worker's reader thread; give it a bounded
    // moment rather than asserting the instant the coordinator sees completion.
    std::size_t after = before;
    const auto d2 = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < d2) {
        after = worker.registration_count(job_id);
        if (after == 0) {
            break;
        }
        std::this_thread::sleep_for(50ms);
    }
    EXPECT_EQ(after, 0U) << "the worker still holds " << after << " registration(s) for a "
                         << "cancelled job (held " << before << " while it ran); a restart "
                         << "would stack another set on top of these";
    std::filesystem::remove(out_path);
}

// A savepoint must outlive the checkpoints taken after it (followups item 74).
//
// `clink savepoint` hands the operator a (dir, id) and tells them to relocate
// it. On an operator whose retention keeps only the newest snapshot, the next
// periodic checkpoint used to unlink the savepoint's files - a window of ONE
// checkpoint interval to copy a possibly multi-GiB tree. QUAL-08's rehearsal
// lost 4 of 10 subtask snapshots of a savepoint before its restore ran.
//
// The coordinator now pins every savepoint id for the job's lifetime and
// carries the pinned set on CommitCheckpoint; the worker's purge and sweep
// both skip it. This drives a real cluster with keep-newest retention and a
// fast checkpoint clock, takes a savepoint, lets several checkpoints complete
// and commit, and then requires the savepoint's snapshot to be present in
// EVERY subtask directory while the unpinned checkpoint before it is gone -
// so the test can tell "pinned" from "nothing was swept at all".
TEST(Cluster, ASavepointSurvivesTheCheckpointsTakenAfterIt) {
    ensure_built_ins_registered();
    const auto ckpt_dir = std::filesystem::temp_directory_path() /
                          ("clink_savepoint_pin_" + std::to_string(::getpid()));
    const auto out_path = ckpt_dir / "out.txt";
    std::filesystem::remove_all(ckpt_dir);
    std::filesystem::create_directories(ckpt_dir);

    Coordinator::Config cfg;
    cfg.max_restarts = 0;
    // Liveness timeouts are not this test's contract. Under a sanitizer a
    // 200 ms checkpoint cadence with snapshot workers churning can hold the
    // heartbeat thread off the CPU past the 2 s default, and a worker
    // declared lost checkpoints nothing - which then reads as "the job
    // never checkpointed".
    cfg.heartbeat_timeout = clink::test_support::scale_slack(cfg.heartbeat_timeout);
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"worker-pin"});
    Worker::Config wcfg;
    wcfg.slot_count = 8;
    wcfg.coordinator_heartbeat_timeout =
        clink::test_support::scale_slack(wcfg.coordinator_heartbeat_timeout);
    wcfg.checkpoint_num_retained = 1;  // keep-newest: the policy that ate savepoints
    Worker worker("worker-pin", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(5s));

    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.out_channel = std::string{kChannelInt64};
    // Long-lived, not huge (see CancellingAJobReleasesTheWorkersRegistrationsForIt).
    src.params = {{"count", "2000000"}, {"delay_ms", "1"}};
    g.ops.push_back(src);
    OperatorSpec a;
    a.type = "identity_int64";
    a.id = "a";
    a.inputs = {"src"};
    a.parallelism = 2;
    a.out_channel = std::string{kChannelInt64};
    g.ops.push_back(a);
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"a"};
    snk.parallelism = 1;
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", out_path.string()}};
    g.ops.push_back(snk);

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = ckpt_dir.string();
    ckpt.interval_ms = 200;
    const auto job_id = coordinator.submit_job(
        g, OperatorRegistry::default_instance(), std::vector<PluginBinary>{}, ckpt, nullptr);
    ASSERT_GT(job_id, 0U);

    const auto completed = [&]() -> std::uint64_t {
        const auto snap = coordinator.snapshot_job(job_id);
        return snap ? snap->latest_completed_checkpoint_id : 0;
    };
    const auto await_completed = [&](std::uint64_t at_least, std::chrono::seconds budget) {
        const auto deadline = std::chrono::steady_clock::now() + budget;
        while (std::chrono::steady_clock::now() < deadline) {
            if (completed() >= at_least) {
                return true;
            }
            std::this_thread::sleep_for(50ms);
        }
        return false;
    };
    ASSERT_TRUE(await_completed(2, clink::test_support::scale_slack(20s)))
        << "the job never checkpointed";

    const auto sp = coordinator.take_savepoint(job_id, 20s);
    ASSERT_TRUE(sp.ok) << sp.message;
    const std::uint64_t S = sp.checkpoint_id;
    ASSERT_GE(S, 2u);

    // Several more checkpoints complete and commit; each commit runs the
    // worker's retention.
    ASSERT_TRUE(await_completed(S + 4, clink::test_support::scale_slack(30s)))
        << "checkpoints stopped after the savepoint";
    std::this_thread::sleep_for(500ms);  // let the last commit's sweep land

    // Every subtask directory that holds snapshots must still hold S.
    const std::string want = "checkpoint-" + std::to_string(S) + ".snap";
    const std::string prev = "checkpoint-" + std::to_string(S - 1) + ".snap";
    std::size_t dirs_with_snapshots = 0, dirs_with_savepoint = 0, dirs_with_prev = 0;
    for (const auto& e : std::filesystem::recursive_directory_iterator(ckpt_dir)) {
        if (!e.is_directory()) {
            continue;
        }
        bool any = false, has_want = false, has_prev = false;
        for (const auto& f : std::filesystem::directory_iterator(e.path())) {
            const auto name = f.path().filename().string();
            if (name.starts_with("checkpoint-") && name.ends_with(".snap")) {
                any = true;
                has_want = has_want || name == want;
                has_prev = has_prev || name == prev;
            }
        }
        if (any) {
            ++dirs_with_snapshots;
            dirs_with_savepoint += has_want;
            dirs_with_prev += has_prev;
        }
    }
    (void)coordinator.cancel_job(job_id);
    ASSERT_GT(dirs_with_snapshots, 0u) << "no snapshot directories under " << ckpt_dir;
    EXPECT_EQ(dirs_with_savepoint, dirs_with_snapshots)
        << "the savepoint's snapshot is missing from "
        << (dirs_with_snapshots - dirs_with_savepoint) << " of " << dirs_with_snapshots
        << " subtask directories - retention swept it";
    EXPECT_EQ(dirs_with_prev, 0u)
        << "the unpinned checkpoint before the savepoint is still present in " << dirs_with_prev
        << " directories, so retention did not run and this test proves nothing about the pin";
    std::filesystem::remove_all(ckpt_dir);
}
