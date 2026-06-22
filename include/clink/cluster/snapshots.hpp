// Plain-value-type snapshots of JM and TM internal state.
//
// Designed for the HTTP read API: handlers grab the mutex briefly,
// copy out into one of these structs, release, then serialize to
// JSON outside the critical section. No shared_ptrs, no references
// back into JM/TM internals - these are safe to keep around across
// async work.
//
// Snapshot fields are deliberately a subset of the full state: only
// the bits a dashboard / ops user needs. New endpoints that need
// additional fields should extend the corresponding struct here
// rather than reaching back into JM/TM internals.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "clink/cluster/protocol.hpp"

namespace clink::cluster {

// One registered TaskManager. lost=true means the JM watchdog
// declared the TM unreachable; its slots are excluded from
// slots_in_use but still counted in registered_tms (so the dashboard
// can show "5 TMs registered, 2 lost").
struct TmSummary {
    std::string tm_id;
    std::string data_host;
    std::uint32_t slot_capacity{0};
    std::uint32_t slots_in_use{0};
    bool lost{false};
    std::uint16_t http_port{0};  // 0 = TM not exposing HTTP; non-zero for federation
};

// One submitted job. Keeps just the surface fields; full per-subtask
// state lives in JobDetail.
struct JobSummary {
    JobId id{0};
    std::size_t expected_completion{0};
    std::size_t completed_count{0};
    bool completion_signalled{false};
    bool cancel_requested{false};
    std::size_t error_count{0};
};

struct JobTaskRecord {
    std::string role;
    std::uint32_t subtask_idx{0};
    std::string tm_id;
};

// A structured per-subtask failure, surfaced by GET /api/v1/jobs/:id alongside
// the flat `errors` list. The message carries the operator exception text and,
// where the toolchain supports std::stacktrace, a capture-site trace appended
// by the executor. `attempt` is the job restart generation at the time of the
// failure (0 = first run). This is built JM-side from the SubtaskFinished
// report, so it adds no new wire fields.
struct SubtaskErrorRecord {
    std::string role;
    std::uint32_t subtask_idx{0};
    std::string tm_id;
    std::uint32_t attempt{0};
    std::int64_t ts_ms{0};
    std::string message;
};

// Heavy per-job detail returned by GET /api/v1/jobs/:id.
struct JobDetail {
    JobId id{0};
    std::size_t expected_completion{0};
    std::size_t completed_count{0};
    bool completion_signalled{false};
    bool cancel_requested{false};
    std::vector<std::string> errors;
    std::vector<SubtaskErrorRecord> subtask_errors;
    std::vector<JobTaskRecord> tasks;
    std::uint64_t latest_completed_checkpoint_id{0};
    std::vector<std::uint64_t> pending_checkpoint_ids;
};

// --- Job DAG (GET /api/v1/jobs/:id/graph) ----------------------------------
// The logical operator graph plus physical subtask placement. Built from the
// retained JobGraphSpec (nodes + input edges) and the per-subtask
// OperatorChainSpec in tasks_by_tm (placement). Static topology; live metrics
// are layered in by the console from the separate /metrics scrape.

// One operator subtask's placement.
struct GraphSubtaskPlacement {
    std::uint32_t subtask_idx{0};
    std::string tm_id;
};

// One operator (node).
struct GraphNode {
    std::string id;
    std::string op_type;
    std::string display_name;
    std::string uid;
    std::string kind;  // "source" | "operator" | "sink"
    std::uint32_t parallelism{1};
    std::string out_channel;
    bool keyed{false};
    std::vector<GraphSubtaskPlacement> subtasks;
};

// One data-flow edge.
struct GraphEdge {
    std::string from;
    std::string to;
    std::string routing;  // "forward" | "hash" | "rebalance"
    std::string channel;  // element type on the wire
};

struct JobGraphDetail {
    JobId id{0};
    std::uint64_t topology_version{0};
    bool available{false};  // false when the job exists but no graph was retained
    std::vector<GraphNode> nodes;
    std::vector<GraphEdge> edges;
};

// Cluster-wide rollup served by GET /api/v1/cluster. Per-TM detail
// is also embedded so a single fetch fills the dashboard overview.
struct ClusterSnapshot {
    std::string bind_host;
    std::string advertise_host;
    std::uint16_t control_port{0};
    std::uint32_t total_slot_capacity{0};
    std::uint32_t slots_in_use{0};
    std::size_t jobs_total{0};
    std::size_t jobs_running{0};
    std::size_t jobs_completed{0};
    std::vector<TmSummary> task_managers;
};

// One running subtask on a TaskManager. Surfaced by GET /api/v1/tm/subtasks
// so the dashboard can show what each TM is currently executing.
struct SubtaskRecord {
    JobId job_id{0};
    std::string role;
    std::uint32_t subtask_idx{0};
    // "pending" - Deploy received, awaiting peer-update or running
    // "ready"   - peer-update received, runner thread launched
    // For v1 we only distinguish pending vs ready; finer states arrive
    // as the runner registers progress callbacks.
    std::string status;
};

// One TaskManager's local view of itself, returned by GET /api/v1/tm.
struct TmSnapshot {
    std::string tm_id;
    std::string data_host;
    std::uint32_t slot_capacity{0};
    std::uint32_t slots_in_use{0};
    std::string jm_host;
    std::uint16_t jm_port{0};
    std::size_t active_subtasks{0};
};

}  // namespace clink::cluster
