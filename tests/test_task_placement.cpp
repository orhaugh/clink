// Task placement must co-locate each parallel pipeline instance on one worker.
//
// Tasks sharing a subtask index are joined by FORWARD edges - subtask i of the source feeds
// subtask i of the projection - and the data plane hands a batch across a forward edge as a
// pointer when both ends are in one process, or serialises it over a socket when they are not.
// Placement therefore decides whether a forward edge costs a pointer copy or a network round
// trip, and nothing about the query or the transport can recover it afterwards.
//
// It was one-task-at-a-time greedy first-fit, which filled the first worker's slots before
// touching the next and so scattered each instance across hosts. Measured on a 3-worker rig at
// parallelism 12, nexmark q0 - four operators, forward edges only, NO shuffle - sent 16 of its
// 24 data-plane edges (67%) over TCP, and its CPU-per-event was 2.05x worse at parallelism 12
// than at 4 where the same sweep on a single host was flat to within 5%.
//
// These tests pin the contract on the placement function directly, so a regression shows up
// here rather than as an unexplained throughput loss on a multi-node cluster.

#include <algorithm>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/coordinator.hpp"

namespace {

using clink::cluster::assign_task_placement;
using clink::cluster::PlacementWorker;
using clink::cluster::PlannedTask;

// A plan shaped like a compiled SQL job: `ops` operators, each at `par`, so one pipeline
// instance is `ops` tasks sharing a subtask index.
std::vector<PlannedTask> make_plan(int ops, std::uint32_t par) {
    std::vector<PlannedTask> tasks;
    for (int o = 0; o < ops; ++o) {
        for (std::uint32_t i = 0; i < par; ++i) {
            PlannedTask t;
            t.role = "op_" + std::to_string(o);
            t.subtask_idx = i;
            tasks.push_back(std::move(t));
        }
    }
    return tasks;
}

std::vector<PlacementWorker> make_workers(int n, std::uint32_t slots) {
    std::vector<PlacementWorker> w;
    for (int i = 1; i <= n; ++i) {
        w.push_back(
            PlacementWorker{.worker_id = "worker-" + std::to_string(i), .free_slots = slots});
    }
    return w;
}

// Workers hosting each subtask index. Size 1 for every index means every instance is
// co-located, which is the property under test.
std::map<std::uint32_t, std::set<std::string>> workers_per_instance(
    const std::vector<PlannedTask>& tasks) {
    std::map<std::uint32_t, std::set<std::string>> out;
    for (const auto& t : tasks) {
        out[t.subtask_idx].insert(t.worker_id);
    }
    return out;
}

}  // namespace

// The core contract, at the exact shape that exposed the bug: 4 operators, parallelism 12,
// 3 workers of 16 slots. Greedy first-fit split 8 of the 12 instances across hosts.
TEST(TaskPlacement, CoLocatesEveryPipelineInstanceOnOneWorker) {
    auto tasks = make_plan(4, 12);
    auto workers = make_workers(3, 16);
    ASSERT_TRUE(assign_task_placement(tasks, workers));

    for (const auto& [idx, hosts] : workers_per_instance(tasks)) {
        EXPECT_EQ(hosts.size(), 1u)
            << "subtask " << idx << " is spread across " << hosts.size()
            << " workers, so its forward edges are network hops rather than pointer handoffs";
    }
    for (const auto& t : tasks) {
        EXPECT_FALSE(t.worker_id.empty());
    }
}

// Co-location must not come at the cost of balance: piling every instance onto one worker
// would make each edge local and the cluster useless.
TEST(TaskPlacement, SpreadsInstancesEvenlyAcrossWorkers) {
    auto tasks = make_plan(4, 12);
    auto workers = make_workers(3, 16);
    ASSERT_TRUE(assign_task_placement(tasks, workers));

    std::map<std::string, int> per_worker;
    for (const auto& t : tasks) {
        ++per_worker[t.worker_id];
    }
    ASSERT_EQ(per_worker.size(), 3u) << "all instances landed on a subset of the workers";
    for (const auto& [w, n] : per_worker) {
        EXPECT_EQ(n, 16) << w << " got " << n
                         << " tasks; 48 tasks over 3 workers should be 16 each";
    }
}

// Placement must be a function of the plan, not of a map iteration order. The coordinator's
// registry is an unordered_map, and an unstable order made placement - and any measurement of
// it - unrepeatable.
TEST(TaskPlacement, IsDeterministicRegardlessOfWorkerInputOrder) {
    auto tasks_a = make_plan(4, 12);
    auto workers_a = make_workers(3, 16);
    ASSERT_TRUE(assign_task_placement(tasks_a, workers_a));

    // Same workers, reversed input order.
    auto tasks_b = make_plan(4, 12);
    auto workers_b = make_workers(3, 16);
    std::reverse(workers_b.begin(), workers_b.end());
    ASSERT_TRUE(assign_task_placement(tasks_b, workers_b));

    ASSERT_EQ(tasks_a.size(), tasks_b.size());
    for (std::size_t i = 0; i < tasks_a.size(); ++i) {
        EXPECT_EQ(tasks_a[i].worker_id, tasks_b[i].worker_id)
            << "task " << i << " placed differently when the worker list arrived in another order";
    }
}

// A task the caller already pinned must be left alone, and must not consume the capacity
// bookkeeping twice.
TEST(TaskPlacement, LeavesPreAssignedTasksUntouched) {
    auto tasks = make_plan(2, 4);
    tasks[0].worker_id = "worker-3";  // pinned to a specific host
    auto workers = make_workers(3, 16);
    ASSERT_TRUE(assign_task_placement(tasks, workers));
    EXPECT_EQ(tasks[0].worker_id, "worker-3");
    for (const auto& t : tasks) {
        EXPECT_FALSE(t.worker_id.empty());
    }
}

// An instance larger than any single worker cannot be co-located. Splitting it is worse than
// co-locating and better than refusing the job, so it must still deploy.
TEST(TaskPlacement, SplitsAnInstanceTooLargeForAnyWorker) {
    auto tasks = make_plan(6, 2);       // instances of 6 tasks
    auto workers = make_workers(3, 4);  // no worker can hold 6
    ASSERT_TRUE(assign_task_placement(tasks, workers));
    for (const auto& t : tasks) {
        EXPECT_FALSE(t.worker_id.empty()) << "a splittable plan must still be fully placed";
    }
}

// Out of capacity is a reported failure, not a silent partial placement.
TEST(TaskPlacement, ReportsFailureWhenCapacityRunsOut) {
    auto tasks = make_plan(4, 12);      // 48 tasks
    auto workers = make_workers(1, 4);  // 4 slots
    EXPECT_FALSE(assign_task_placement(tasks, workers));
}

// No workers at all is a failure rather than a crash or a plan of empty assignments.
TEST(TaskPlacement, ReportsFailureWithNoWorkers) {
    auto tasks = make_plan(2, 2);
    std::vector<PlacementWorker> none;
    EXPECT_FALSE(assign_task_placement(tasks, none));
}

// A single worker is the degenerate case the split rig ran, where every edge is local anyway.
TEST(TaskPlacement, SingleWorkerTakesEverything) {
    auto tasks = make_plan(4, 4);
    auto workers = make_workers(1, 64);
    ASSERT_TRUE(assign_task_placement(tasks, workers));
    for (const auto& t : tasks) {
        EXPECT_EQ(t.worker_id, "worker-1");
    }
}

// Operators at DIFFERENT parallelism still group by subtask index: instance i is whatever
// tasks carry index i, and indices beyond a narrower operator's parallelism simply have fewer
// members.
TEST(TaskPlacement, HandlesOperatorsAtDifferentParallelism) {
    std::vector<PlannedTask> tasks;
    for (std::uint32_t i = 0; i < 2; ++i) {  // narrow source
        PlannedTask t;
        t.role = "src";
        t.subtask_idx = i;
        tasks.push_back(t);
    }
    for (std::uint32_t i = 0; i < 6; ++i) {  // wider downstream
        PlannedTask t;
        t.role = "agg";
        t.subtask_idx = i;
        tasks.push_back(t);
    }
    auto workers = make_workers(2, 8);
    ASSERT_TRUE(assign_task_placement(tasks, workers));
    for (const auto& [idx, hosts] : workers_per_instance(tasks)) {
        EXPECT_EQ(hosts.size(), 1u) << "subtask " << idx << " spread across workers";
    }
}
