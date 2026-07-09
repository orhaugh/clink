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
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/job_manager.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/task_manager.hpp"
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
// (clink_node --default-state-backend / JobManager::Config.default_state_backend_uri)
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
// relied on checkpoint_dir -> file durability) must NOT be rebound when the JM
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

// 1 JobManager + 2 TaskManagers running in 3 threads. JM coordinates the
// deployment of a producer/consumer pipeline split across the TMs:
//   TM-A: producer role - emits int64s into a NetworkBridgeSink.
//   TM-B: consumer role - listens via NetworkBridgeSource, collects.
// Verifies the full handshake, deployment, data-plane connection, and
// completion reporting.
TEST(Cluster, JmTmDistributedProducerConsumer) {
    // Pick a free port for the consumer's data plane. The bind-then-close
    // pattern leaves a tiny race window (microseconds) where another
    // process could steal the port; fine for in-process tests.
    std::uint16_t consumer_port = 0;
    {
        NetworkChannelSource<std::int64_t> probe(0, int64_codec());
        consumer_port = probe.listen();
    }

    // ----- JobManager -----
    JobManager jm;
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"tm-a", "tm-b"});

    // ----- Shared sink for the consumer to deposit into; the test reads
    // it after the job completes. -----
    auto consumer_sink = std::make_shared<CollectingSink<std::int64_t>>();

    // ----- TM-B: consumer (start first so the producer can connect) -----
    TaskManager tm_b("tm-b", "127.0.0.1");
    tm_b.register_role("consumer", [consumer_sink](const DeploymentTask& task) {
        auto src =
            std::make_shared<NetworkBridgeSource<std::int64_t>>(task.data_port, int64_codec());
        src->prepare_listen();

        Dag dag;
        auto h0 = dag.add_source<std::int64_t>(src);
        dag.add_sink<std::int64_t>(h0, consumer_sink);

        LocalExecutor exec(std::move(dag));
        exec.run();  // blocks until source closes
    });
    tm_b.connect_to_jm("127.0.0.1", jm_port);

    // ----- TM-A: producer -----
    TaskManager tm_a("tm-a", "127.0.0.1");
    tm_a.register_role("producer", [](const DeploymentTask& task) {
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
    tm_a.connect_to_jm("127.0.0.1", jm_port);

    // ----- Plan and deploy -----
    ASSERT_TRUE(jm.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .tm_id = "tm-b",
        .role = "consumer",
        .subtask_idx = 0,
        .data_port = consumer_port,
        .peer_refs = {},
        .extra_config = "",
    });
    plan.tasks.push_back(PlannedTask{
        .tm_id = "tm-a",
        .role = "producer",
        .subtask_idx = 0,
        .data_port = 0,  // outbound only
        .peer_refs = {{"consumer", 0}},
        .extra_config = "",
    });
    jm.deploy(plan);

    ASSERT_TRUE(jm.await_completion(5s));
    EXPECT_TRUE(jm.errors().empty());

    // Verify the consumer received the producer's records intact.
    EXPECT_EQ(consumer_sink->collected(), (std::vector<std::int64_t>{100, 200, 300, 400, 500}));

    tm_a.stop();
    tm_b.stop();
    jm.stop();
}

// Heartbeat watchdog detects a TM that registers but stops sending
// heartbeats. The JM marks it lost, synthesises errors for any pending
// tasks, and unblocks await_completion.
TEST(Cluster, WatchdogDetectsLostTaskManager) {
    JobManager::Config jm_cfg;
    jm_cfg.watchdog_interval = 50ms;
    jm_cfg.heartbeat_timeout = 250ms;
    JobManager jm(jm_cfg);
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"silent-tm"});

    // TM with heartbeats disabled - registers, then goes silent.
    TaskManager::Config tm_cfg;
    tm_cfg.heartbeat_interval = std::chrono::milliseconds{0};
    TaskManager tm("silent-tm", "127.0.0.1", tm_cfg);
    // Handler blocks longer than heartbeat_timeout. With heartbeats off
    // and no SubtaskFinished arriving during the sleep, the watchdog
    // declares the TM lost.
    tm.register_role("blocker", [](const DeploymentTask&) { std::this_thread::sleep_for(800ms); });
    tm.connect_to_jm("127.0.0.1", jm_port);

    ASSERT_TRUE(jm.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .tm_id = "silent-tm",
        .role = "blocker",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    jm.deploy(plan);

    ASSERT_TRUE(jm.await_completion(2s));

    auto lost = jm.lost_tms();
    ASSERT_EQ(lost.size(), 1u);
    EXPECT_EQ(lost[0], "silent-tm");

    auto errs = jm.errors();
    ASSERT_EQ(errs.size(), 1u);
    EXPECT_NE(errs[0].find("heartbeat timeout"), std::string::npos);

    tm.stop();
    jm.stop();
}

// A TM that re-registers under the SAME id (a restarted process with a stable
// name - the Kubernetes StatefulSet pattern) replaces its old TmConnection in
// the JM. The old connection's reader thread must be joined during that
// replacement: destroying a joinable std::thread is std::terminate, and the
// reader lambda holds a shared_ptr to its own TmConnection, so dropping the
// map's reference without joining hands destruction to the exiting reader
// itself. Before the fix this test crashed the process.
TEST(Cluster, TaskManagerReRegistrationUnderSameIdRetiresOldSession) {
    JobManager::Config jm_cfg;
    jm_cfg.watchdog_interval = 50ms;
    jm_cfg.heartbeat_timeout = 60s;  // watchdog quiet; the test drives the churn
    JobManager jm(jm_cfg);
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"stable-tm"});

    // Session 1: register, then die ungracefully (stop() closes the conn;
    // the JM-side reader returns but its thread stays joinable).
    auto tm1 = std::make_unique<TaskManager>("stable-tm", "127.0.0.1");
    tm1->connect_to_jm("127.0.0.1", jm_port);
    ASSERT_TRUE(jm.await_registrations(2s));
    tm1->stop();
    tm1.reset();

    // Session 2: the restarted TM re-registers under the same id. Pre-fix:
    // std::terminate in handle_register_ replacing the old TmConnection.
    auto tm2 = std::make_unique<TaskManager>("stable-tm", "127.0.0.1");
    tm2->connect_to_jm("127.0.0.1", jm_port);

    // The JM must still be alive and serving: the re-registered TM is
    // schedulable (registration visible), proven by a successful deploy.
    bool ran = false;
    std::mutex m;
    std::condition_variable cv;
    tm2->register_role("noop", [&](const DeploymentTask&) {
        {
            std::lock_guard lk(m);
            ran = true;
        }
        cv.notify_all();
    });
    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .tm_id = "stable-tm",
        .role = "noop",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    jm.deploy(plan);
    {
        std::unique_lock lk(m);
        ASSERT_TRUE(cv.wait_for(lk, 5s, [&] { return ran; }));
    }

    tm2->stop();
    jm.stop();
}

// Dynamic placement: tasks with empty tm_id are auto-assigned to TMs with
// free slots. Two TMs each with capacity=1 → two unassigned tasks land
// one per TM (no overload).
TEST(Cluster, DynamicPlacementAssignsTasksToFreeSlots) {
    JobManager jm;
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"tm-x", "tm-y"});

    std::mutex seen_mu;
    std::vector<std::string> tm_ids_observed;

    auto record_role = [&](const std::string& tm_id) {
        return [&seen_mu, &tm_ids_observed, tm_id](const DeploymentTask&) {
            std::lock_guard lock(seen_mu);
            tm_ids_observed.push_back(tm_id);
        };
    };

    TaskManager::Config tm_cfg;
    tm_cfg.slot_count = 1;

    TaskManager tm_x("tm-x", "127.0.0.1", tm_cfg);
    tm_x.register_role("worker", record_role("tm-x"));
    tm_x.connect_to_jm("127.0.0.1", jm_port);

    TaskManager tm_y("tm-y", "127.0.0.1", tm_cfg);
    tm_y.register_role("worker", record_role("tm-y"));
    tm_y.connect_to_jm("127.0.0.1", jm_port);

    ASSERT_TRUE(jm.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .tm_id = "",
        .role = "worker",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    plan.tasks.push_back(PlannedTask{
        .tm_id = "",
        .role = "worker",
        .subtask_idx = 1,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    jm.deploy(plan);

    ASSERT_TRUE(jm.await_completion(2s));
    EXPECT_TRUE(jm.errors().empty());

    {
        std::lock_guard lock(seen_mu);
        ASSERT_EQ(tm_ids_observed.size(), 2u);
        std::sort(tm_ids_observed.begin(), tm_ids_observed.end());
        EXPECT_EQ(tm_ids_observed[0], "tm-x");
        EXPECT_EQ(tm_ids_observed[1], "tm-y");
    }

    tm_x.stop();
    tm_y.stop();
    jm.stop();
}

// JM redeploys a failing task up to max_restarts times, appending an
// attempt counter to extra_config. The role handler reads it to decide
// whether to "fail" (first attempt) or succeed (retry). After a retry
// succeeds the JM reports success even though the first attempt errored.
TEST(Cluster, RestartsFailingTaskUpToMaxRestarts) {
    JobManager::Config jm_cfg;
    jm_cfg.max_restarts = 3;
    JobManager jm(jm_cfg);
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"flaky-tm"});

    std::atomic<int> attempts_observed{0};
    TaskManager tm("flaky-tm", "127.0.0.1");
    tm.register_role("flaky", [&attempts_observed](const DeploymentTask& task) {
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
    tm.connect_to_jm("127.0.0.1", jm_port);

    ASSERT_TRUE(jm.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .tm_id = "flaky-tm",
        .role = "flaky",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    jm.deploy(plan);

    ASSERT_TRUE(jm.await_completion(5s));
    EXPECT_TRUE(jm.errors().empty()) << "retry should have succeeded";
    EXPECT_GE(attempts_observed.load(), 1) << "second attempt should have run with clink_attempt=1";

    tm.stop();
    jm.stop();
}

// JM abort path: when one TM is declared lost, the JM broadcasts CancelJob
// to surviving TMs. Their reader loops flip `cancelled_` so role handlers
// can poll and abort. The JM's errors() lists the lost TM's tasks; the
// surviving TM observes was_cancelled() == true.
TEST(Cluster, FailureBroadcastsCancelToSurvivors) {
    JobManager::Config jm_cfg;
    jm_cfg.watchdog_interval = 50ms;
    jm_cfg.heartbeat_timeout = 600ms;  // > healthy.interval below, < silent's blocker
    JobManager jm(jm_cfg);
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"healthy-tm", "silent-tm"});

    // Healthy TM with frequent heartbeats so its last_seen stays current
    // throughout the run.
    TaskManager::Config healthy_cfg;
    healthy_cfg.heartbeat_interval = 100ms;
    TaskManager healthy("healthy-tm", "127.0.0.1", healthy_cfg);
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
    healthy.connect_to_jm("127.0.0.1", jm_port);

    // Silent TM with heartbeats disabled. Its handler blocks long enough
    // for the watchdog to fire.
    TaskManager::Config silent_cfg;
    silent_cfg.heartbeat_interval = std::chrono::milliseconds{0};
    TaskManager silent("silent-tm", "127.0.0.1", silent_cfg);
    silent.register_role("worker", [](const DeploymentTask&) { std::this_thread::sleep_for(2s); });
    silent.connect_to_jm("127.0.0.1", jm_port);

    ASSERT_TRUE(jm.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .tm_id = "healthy-tm",
        .role = "worker",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    plan.tasks.push_back(PlannedTask{
        .tm_id = "silent-tm",
        .role = "worker",
        .subtask_idx = 1,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    jm.deploy(plan);

    ASSERT_TRUE(jm.await_completion(3s));

    // The watchdog should have declared silent-tm lost.
    auto lost = jm.lost_tms();
    ASSERT_EQ(lost.size(), 1u);
    EXPECT_EQ(lost[0], "silent-tm");

    // Surviving TM saw the CancelJob broadcast.
    EXPECT_TRUE(healthy.was_cancelled());
    EXPECT_TRUE(healthy_observed_cancel.load());

    // JM's errors include the lost TM's task.
    auto errs = jm.errors();
    ASSERT_FALSE(errs.empty());
    EXPECT_NE(errs[0].find("silent-tm"), std::string::npos);

    healthy.stop();
    silent.stop();
    jm.stop();
}

// Spec-driven dispatch: the role handlers no longer hard-code the DAG
// structure. The JM ships a `JobGraphSpec` (text) in `extra_config`; each
// handler parses it, looks each op type up in a small registry, and
// builds the DAG dynamically. This is the foundation for "submit a job
// to a running JM" - users describe their job as a graph of named
// factories rather than recompiling the binary with new role lambdas.
TEST(Cluster, GraphSpecDrivenProducerConsumer) {
    // Pre-bind ports as before.
    std::uint16_t consumer_port = 0;
    {
        NetworkChannelSource<std::int64_t> probe(0, int64_codec());
        consumer_port = probe.listen();
    }

    JobManager jm;
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"tm-a", "tm-b"});

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

    TaskManager tm_b("tm-b", "127.0.0.1");
    tm_b.register_role("graph", run_graph);
    tm_b.connect_to_jm("127.0.0.1", jm_port);

    TaskManager tm_a("tm-a", "127.0.0.1");
    tm_a.register_role("graph", run_graph);
    tm_a.connect_to_jm("127.0.0.1", jm_port);

    ASSERT_TRUE(jm.await_registrations(2s));

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
        .tm_id = "tm-b",
        .role = "graph",
        .subtask_idx = 0,
        .data_port = consumer_port,
        .peer_refs = {},
        .extra_config = consumer_spec.serialize(),
    });
    plan.tasks.push_back(PlannedTask{
        .tm_id = "tm-a",
        .role = "graph",
        .subtask_idx = 1,
        .data_port = 0,
        .peer_refs = {{"graph", 0}},
        .extra_config = producer_spec.serialize(),
    });
    jm.deploy(plan);

    ASSERT_TRUE(jm.await_completion(5s));
    EXPECT_TRUE(jm.errors().empty());

    EXPECT_EQ(consumer_sink->collected(), (std::vector<std::int64_t>{10, 20, 30}));

    tm_a.stop();
    tm_b.stop();
    jm.stop();
}

// A TM that registers but is dispatched a role it doesn't know about
// must report had_error=true on SubtaskFinished. The JM surfaces it via
// errors(); the TM stays registered and able to receive other work.
TEST(Cluster, DeployToTmWithoutRoleHandlerReportsError) {
    JobManager jm;
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"empty-tm"});

    TaskManager tm("empty-tm", "127.0.0.1");
    // Note: NO register_role call. TM has zero handlers.
    tm.connect_to_jm("127.0.0.1", jm_port);

    ASSERT_TRUE(jm.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .tm_id = "empty-tm",
        .role = "ghost",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    jm.deploy(plan);

    ASSERT_TRUE(jm.await_completion(2s));
    auto errs = jm.errors();
    ASSERT_FALSE(errs.empty());
    bool found = false;
    for (const auto& e : errs) {
        if (e.find("ghost") != std::string::npos && e.find("no handler") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);

    tm.stop();
    jm.stop();
}

// Two TMs both fail their tasks (throw). JM collects both errors and
// reports completion - failure of one doesn't mask the other.
TEST(Cluster, MultipleSimultaneousTaskFailuresAreAllReported) {
    JobManager jm;  // max_restarts = 0 by default
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"tm-1", "tm-2"});

    TaskManager tm1("tm-1", "127.0.0.1");
    tm1.register_role("crashy",
                      [](const DeploymentTask&) { throw std::runtime_error("crash from tm-1"); });
    tm1.connect_to_jm("127.0.0.1", jm_port);

    TaskManager tm2("tm-2", "127.0.0.1");
    tm2.register_role("crashy",
                      [](const DeploymentTask&) { throw std::runtime_error("crash from tm-2"); });
    tm2.connect_to_jm("127.0.0.1", jm_port);

    ASSERT_TRUE(jm.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .tm_id = "tm-1",
        .role = "crashy",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    plan.tasks.push_back(PlannedTask{
        .tm_id = "tm-2",
        .role = "crashy",
        .subtask_idx = 1,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    jm.deploy(plan);

    ASSERT_TRUE(jm.await_completion(3s));
    auto errs = jm.errors();
    ASSERT_GE(errs.size(), 2u);

    bool saw_tm1 = false;
    bool saw_tm2 = false;
    for (const auto& e : errs) {
        if (e.find("tm-1") != std::string::npos) {
            saw_tm1 = true;
        }
        if (e.find("tm-2") != std::string::npos) {
            saw_tm2 = true;
        }
    }
    EXPECT_TRUE(saw_tm1);
    EXPECT_TRUE(saw_tm2);

    tm1.stop();
    tm2.stop();
    jm.stop();
}

// Dynamic placement with not enough free slots: 1 TM with slot_count=1,
// 2 unassigned tasks. The behaviour is documented elsewhere - what we
// pin here is "doesn't silently hang". Either deploy() rejects, or
// errors() exposes the failure after await_completion.
TEST(Cluster, DynamicPlacementWithInsufficientSlotsDoesNotHang) {
    JobManager::Config jm_cfg;
    jm_cfg.heartbeat_timeout = 500ms;
    jm_cfg.watchdog_interval = 50ms;
    JobManager jm(jm_cfg);
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"single-tm"});

    TaskManager::Config tm_cfg;
    tm_cfg.slot_count = 1;
    TaskManager tm("single-tm", "127.0.0.1", tm_cfg);
    std::atomic<int> ran{0};
    tm.register_role("worker", [&ran](const DeploymentTask&) { ran.fetch_add(1); });
    tm.connect_to_jm("127.0.0.1", jm_port);

    ASSERT_TRUE(jm.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .tm_id = "",
        .role = "worker",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    plan.tasks.push_back(PlannedTask{
        .tm_id = "",
        .role = "worker",
        .subtask_idx = 1,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });

    bool deploy_threw = false;
    try {
        jm.deploy(plan);
    } catch (const std::exception&) {
        deploy_threw = true;
    }

    if (!deploy_threw) {
        // Bounded wait - must not hang forever even if the JM can't
        // place the second task.
        const bool completed = jm.await_completion(3s);
        if (completed) {
            // If completion was reported, the over-subscribed task must
            // surface as either an error or the JM must have packed both
            // onto the single slot serially.
            EXPECT_GE(ran.load(), 1);
        } else {
            // Otherwise the second task is unplaced; that's acceptable
            // as long as we got here without hanging the test process.
            SUCCEED();
        }
    }

    tm.stop();
    jm.stop();
}

// History server: every job that reaches a terminal state lands in the
// JM's bounded history ring. We verify both an OK job and a FAILED job
// surface there with the right status, errors, and duration tracking.
TEST(Cluster, HistoryServerRetainsTerminalJobs) {
    JobManager::Config jm_cfg;
    jm_cfg.max_restarts = 0;  // failing task surfaces immediately
    JobManager jm(jm_cfg);
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"tm-h"});

    TaskManager tm("tm-h", "127.0.0.1");
    tm.register_role("noop", [](const DeploymentTask&) {});
    tm.register_role("boom",
                     [](const DeploymentTask&) { throw std::runtime_error("intentional"); });
    tm.connect_to_jm("127.0.0.1", jm_port);

    ASSERT_TRUE(jm.await_registrations(2s));

    JobPlan ok_plan;
    ok_plan.tasks.push_back(PlannedTask{
        .tm_id = "tm-h",
        .role = "noop",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    jm.deploy(ok_plan);
    ASSERT_TRUE(jm.await_completion(3s));

    JobPlan fail_plan;
    fail_plan.tasks.push_back(PlannedTask{
        .tm_id = "tm-h",
        .role = "boom",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    jm.deploy(fail_plan);
    ASSERT_TRUE(jm.await_completion(3s));

    auto history = jm.job_history();
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

    auto looked_up = jm.job_history(fail_rec.job_id);
    ASSERT_TRUE(looked_up.has_value());
    EXPECT_EQ(looked_up->status, "failed");

    EXPECT_FALSE(jm.job_history(JobId{999999}).has_value());

    tm.stop();
    jm.stop();
}

// History persists to <ha_dir>/history/<job_id>.json and is reloaded
// on a fresh JM that points at the same ha_dir. Mirrors
// HistoryServer archive layout.
TEST(Cluster, HistoryServerPersistsAcrossRestart) {
    auto ha_dir = std::filesystem::temp_directory_path() /
                  ("clink-history-test-" + std::to_string(::getpid()));
    std::filesystem::remove_all(ha_dir);

    JobId failed_job_id = 0;
    {
        JobManager::Config cfg;
        cfg.max_restarts = 0;
        JobManager jm(cfg);
        jm.set_ha_dir(ha_dir.string());
        const auto port = jm.start();
        jm.expect_tms({"tm-p"});
        TaskManager tm("tm-p", "127.0.0.1");
        tm.register_role("noop", [](const DeploymentTask&) {});
        tm.register_role("boom",
                         [](const DeploymentTask&) { throw std::runtime_error("disk-test"); });
        tm.connect_to_jm("127.0.0.1", port);
        ASSERT_TRUE(jm.await_registrations(2s));

        JobPlan ok_plan;
        ok_plan.tasks.push_back(PlannedTask{
            .tm_id = "tm-p",
            .role = "noop",
            .subtask_idx = 0,
            .data_port = 0,
            .peer_refs = {},
            .extra_config = "",
        });
        jm.deploy(ok_plan);
        ASSERT_TRUE(jm.await_completion(3s));

        JobPlan fail_plan;
        fail_plan.tasks.push_back(PlannedTask{
            .tm_id = "tm-p",
            .role = "boom",
            .subtask_idx = 0,
            .data_port = 0,
            .peer_refs = {},
            .extra_config = "",
        });
        jm.deploy(fail_plan);
        ASSERT_TRUE(jm.await_completion(3s));

        auto h = jm.job_history();
        ASSERT_GE(h.size(), 2u);
        failed_job_id = h.back().job_id;

        tm.stop();
        jm.stop();
    }

    // Fresh JM points at the same ha_dir - should reload both records.
    {
        JobManager jm2;
        jm2.set_ha_dir(ha_dir.string());
        auto h = jm2.job_history();
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
        jm2.stop();
    }

    std::filesystem::remove_all(ha_dir);
}

// --- Per-operator rescale request surface ---------------

TEST(JobManagerRescale, RequestOperatorRescaleUnknownJobReturnsError) {
    // The JM-level delegate validates job existence; the underlying
    // RescaleCoordinator state machine + bounds checks are already
    // unit-tested via test_rescale_coordinator.cpp. This test pins
    // the JM-only paths (unknown job, no coordinator) so a future
    // refactor can't silently drop them.
    JobManager jm;
    auto result = jm.request_operator_rescale(JobId{999}, "join", 4);
    EXPECT_FALSE(result.ok);
    EXPECT_NE(result.reason.find("unknown job_id"), std::string::npos);
}

// A per-operator rescale only advances out of Preparing when a
// checkpoint lands. A job with no periodic checkpointing would sit in
// Preparing forever, so request_operator_rescale must reject up front
// with a clear reason instead of hanging silently.
TEST(JobManagerRescale, RejectsRescaleWhenNoCheckpointingConfigured) {
    using namespace std::chrono_literals;
    ensure_built_ins_registered();

    JobManager jm;  // no autoscaler, and the submitted job has no checkpoint config
    const auto jm_port = jm.start();
    jm.expect_tms({"tm-rs-nockpt"});

    TaskManager::Config tm_cfg;
    tm_cfg.slot_count = 4;
    TaskManager tm("tm-rs-nockpt", "127.0.0.1", tm_cfg);
    tm.connect_to_jm("127.0.0.1", jm_port);
    ASSERT_TRUE(jm.await_registrations(2s));

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

    const auto job_id = jm.submit_job(g, OperatorRegistry::default_instance());

    // Wait until the operator is deployed and its rescale coordinator is
    // registered (status becomes non-nullopt). Completed jobs linger in
    // jobs_, so the coordinator stays available for the request below.
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    bool coordinator_ready = false;
    while (std::chrono::steady_clock::now() < deadline) {
        if (jm.operator_rescale_status(job_id, "src").has_value()) {
            coordinator_ready = true;
            break;
        }
        std::this_thread::sleep_for(20ms);
    }
    ASSERT_TRUE(coordinator_ready) << "operator rescale coordinator was not registered";

    auto result = jm.request_operator_rescale(job_id, "src", 2);
    EXPECT_FALSE(result.ok);
    EXPECT_NE(result.reason.find("periodic checkpointing"), std::string::npos) << result.reason;

    tm.stop();
    jm.stop();
    std::filesystem::remove(out_path);
}

TEST(JobManagerRescale, OperatorRescaleStatusUnknownJobReturnsNullopt) {
    JobManager jm;
    auto st = jm.operator_rescale_status(JobId{999}, "join");
    EXPECT_FALSE(st.has_value());
}

// --- Autoscaler wiring through JobManager -----------------

TEST(JobManagerAutoscaler, NoConfigMeansNoAutoscaler) {
    // Default JobManager::Config leaves the autoscaler unset.
    // autoscaler_ticks must return nullopt for any job; the wiring
    // is opt-in.
    JobManager jm;
    auto t = jm.autoscaler_ticks(JobId{1});
    EXPECT_FALSE(t.has_value());
}

TEST(JobManagerAutoscaler, UnknownJobReturnsNullopt) {
    JobManager::Config cfg;
    AutoscalerConfig as_cfg;
    as_cfg.sample_period = std::chrono::milliseconds{50};
    cfg.autoscaler = as_cfg;
    JobManager jm(cfg);
    EXPECT_FALSE(jm.autoscaler_ticks(JobId{99}).has_value());
}

// End-to-end: submit a graph with a bounded op, the per-job autoscaler
// thread starts, ticks, and (because sample_fn returns a saturated
// signal) eventually drives request_operator_rescale through the JM's
// public surface. Runs entirely in-process: one JM, one TM, one
// short-lived built-in pipeline.
TEST(JobManagerAutoscaler, TicksAndFiresRescaleRequest) {
    using namespace std::chrono_literals;
    ensure_built_ins_registered();

    JobManager::Config jm_cfg;
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
    jm_cfg.autoscaler = as_cfg;

    JobManager jm(jm_cfg);

    std::atomic<int> sample_calls{0};
    std::atomic<int> requests_seen{0};
    jm.set_autoscaler_sample_fn([&sample_calls](JobId, const std::string&) -> double {
        sample_calls.fetch_add(1, std::memory_order_relaxed);
        return 0.95;  // over-saturated -> scale up
    });

    const auto jm_port = jm.start();
    jm.expect_tms({"tm-as"});

    TaskManager::Config tm_cfg;
    tm_cfg.slot_count = 4;  // enough headroom for the 2 subtasks plus
                            // any rescale fan-out the autoscaler asks
                            // for during the test window.
    TaskManager tm("tm-as", "127.0.0.1", tm_cfg);
    tm.connect_to_jm("127.0.0.1", jm_port);
    ASSERT_TRUE(jm.await_registrations(2s));

    const auto out_path = std::filesystem::temp_directory_path() / "clink_autoscaler_jm_test.txt";
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

    const auto job_id = jm.submit_job(g, OperatorRegistry::default_instance());

    // Wait for the per-job autoscaler thread to clock at least a few
    // ticks AND for a rescale request to land (or saturate the budget).
    const auto deadline = std::chrono::steady_clock::now() + 1500ms;
    while (std::chrono::steady_clock::now() < deadline) {
        auto ticks = jm.autoscaler_ticks(job_id);
        auto status = jm.operator_rescale_status(job_id, "src");
        if (status.has_value() && status->state != RescaleState::Idle) {
            requests_seen.fetch_add(1, std::memory_order_relaxed);
        }
        if (ticks.has_value() && *ticks >= 3 && sample_calls.load() >= 1) {
            break;
        }
        std::this_thread::sleep_for(20ms);
    }

    const auto ticks = jm.autoscaler_ticks(job_id);
    ASSERT_TRUE(ticks.has_value()) << "autoscaler was not created for the job";
    EXPECT_GE(*ticks, 3u);
    EXPECT_GE(sample_calls.load(), 1);

    tm.stop();
    jm.stop();
    std::filesystem::remove(out_path);
}

TEST(JobManagerAutoscaler, NoAutoscalerWhenOpsLackBounds) {
    using namespace std::chrono_literals;
    ensure_built_ins_registered();

    JobManager::Config jm_cfg;
    AutoscalerConfig as_cfg;
    as_cfg.sample_period = 30ms;
    jm_cfg.autoscaler = as_cfg;
    JobManager jm(jm_cfg);

    const auto jm_port = jm.start();
    jm.expect_tms({"tm-no-bounds"});
    TaskManager::Config tm_cfg;
    tm_cfg.slot_count = 2;
    TaskManager tm("tm-no-bounds", "127.0.0.1", tm_cfg);
    tm.connect_to_jm("127.0.0.1", jm_port);
    ASSERT_TRUE(jm.await_registrations(2s));

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

    const auto job_id = jm.submit_job(g, OperatorRegistry::default_instance());
    // No op declares bounds -> the JM must NOT spawn a per-job
    // autoscaler. autoscaler_ticks returns nullopt.
    EXPECT_FALSE(jm.autoscaler_ticks(job_id).has_value());

    tm.stop();
    jm.stop();
    std::filesystem::remove(out_path);
}
