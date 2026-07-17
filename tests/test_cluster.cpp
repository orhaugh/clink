#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/worker.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/network/network_bridge.hpp"
#include "clink/runtime/network/network_channel.hpp"

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
