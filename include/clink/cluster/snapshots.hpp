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

// Heavy per-job detail returned by GET /api/v1/jobs/:id.
struct JobDetail {
    JobId id{0};
    std::size_t expected_completion{0};
    std::size_t completed_count{0};
    bool completion_signalled{false};
    bool cancel_requested{false};
    std::vector<std::string> errors;
    std::vector<JobTaskRecord> tasks;
    std::uint64_t latest_completed_checkpoint_id{0};
    std::vector<std::uint64_t> pending_checkpoint_ids;
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
