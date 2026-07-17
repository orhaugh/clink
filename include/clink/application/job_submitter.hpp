#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include "clink/cluster/protocol.hpp"

namespace clink::application {

// One entry returned by JobSubmitter::list_jobs().
struct JobListing {
    std::uint64_t job_id{};
    std::uint32_t total_subtasks{};
    std::uint32_t completed_subtasks{};
    bool completion_signalled{};
};

// Outcome of one JobSubmitter::submit() call. Mirrors what the cluster
// reports back via SubmitJobAck + JobCompleted.
struct SubmitResult {
    bool ok{false};
    std::uint64_t job_id{0};
    // Set by SubmitJobAck on rejection (validation failure, no slots, ...).
    std::string reject_message;
    // Per-operator error messages reported in JobCompleted. Empty on a
    // clean completion; populated even when ok=false to surface the
    // root cause across multiple subtasks.
    std::vector<std::string> errors;
    // True if the job ran to completion (whether or not it had per-
    // operator errors); false if the submission was rejected up-front
    // or the wait timed out.
    bool completed{false};
};

struct SubmitOptions {
    // How long to wait for the SubmitJobAck. Submission is meant to be
    // fast, so this is short by default.
    std::chrono::milliseconds ack_timeout{10000};
    // After successful submission, block waiting for JobCompleted up to
    // this long. Set to zero / negative to skip waiting (fire-and-
    // forget); the caller is responsible for follow-up.
    std::chrono::seconds wait_timeout{60};
    bool wait_for_completion{true};
    // Distributed-checkpointing config. Empty checkpoint_dir disables
    // checkpointing entirely; otherwise the coordinator triggers a periodic
    // barrier (interval_ms cadence) and every subtask snapshots its
    // keyed state to <checkpoint_dir>/<subtask>/checkpoint-<id>.snap.
    // To resume from a prior run, set restore_from_dir +
    // restore_from_checkpoint_id.
    clink::cluster::CheckpointConfig checkpoint;
};

// JobSubmitter: programmatic equivalent of `clink_node --role=client`.
//
// Construct one with the coordinator endpoint and call submit() with a graph
// JSON and zero-or-more local plugin .so paths. The submitter:
//   1. Reads each plugin file into memory + content-hashes it
//   2. Opens a TCP connection to the coordinator
//   3. Sends HelloClient + SubmitJob
//   4. Waits for SubmitJobAck (with ack_timeout)
//   5. If wait_for_completion, waits for JobCompleted (with wait_timeout)
//   6. Closes the connection and returns a SubmitResult
//
// Thread-safe in the sense that two threads can call submit() against
// the same instance concurrently - each call opens its own socket. The
// SubmitResult is plain data, no shared mutable state.
//
// Intended use: write a `main()` for your application that constructs
// a JobGraphSpec via the clink::cluster C++ API, serialises it to
// JSON, and calls JobSubmitter::submit(). Standalone "submit a job
// and exit" binaries replace JSON-on-disk + clink_node client.
class JobSubmitter {
public:
    JobSubmitter(std::string coordinator_host, std::uint16_t coordinator_port);

    SubmitResult submit(const std::string& graph_json,
                        const std::vector<std::string>& plugin_paths = {},
                        const SubmitOptions& opts = {}) const;

    // Query the coordinator for every job it currently tracks (running and
    // recently-completed; completion_signalled distinguishes them).
    // `ok` is false when the coordinator is unreachable; in that case `error`
    // holds the reason. Submission and list use the same TCP path.
    struct ListResult {
        bool ok{false};
        std::string error;
        std::vector<JobListing> jobs;
    };
    [[nodiscard]] ListResult list_jobs(
        std::chrono::milliseconds timeout = std::chrono::milliseconds{10000}) const;

private:
    std::string coordinator_host_;
    std::uint16_t coordinator_port_;
};

}  // namespace clink::application
