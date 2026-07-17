#pragma once

// HaCoordinator - distributed leader-election + leader-endpoint discovery.
//
// The interface is intentionally narrow so we can swap a file-based
// implementation (single-machine HA, default) for ZooKeeper / etcd /
// k8s ConfigMap-based ones later without touching the cluster code.
//
// Lifecycle for a coordinator:
//   1. Construct with a shared HA-dir path (and the endpoint we'd
//      advertise if we became leader).
//   2. Call start(); the coordinator spawns a poll thread that tries
//      to acquire leadership periodically.
//   3. Watch `is_leader()` - true means this process holds the lock.
//      Register on_become_leader_ callback to be notified.
//   4. On shutdown, stop() releases the lock + joins the thread.
//
// Lifecycle for a worker:
//   1. Construct (no endpoint to advertise).
//   2. Periodically (or on connect failure) call current_leader_endpoint
//      to discover where the active coordinator is.
//
// Epoch: each leadership acquisition bumps a monotonic epoch counter
// written to the leader-endpoint file. Fencing - workers / messages can be
// tagged with the epoch they were issued in; consumers ignore older
// epochs. v1 doesn't yet propagate epoch into the wire protocol, but
// the field is in place for when it does.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace clink::cluster {

struct LeaderEndpoint {
    std::string host;
    std::uint16_t port{0};
    std::uint64_t epoch{0};
    // Last-updated timestamp (steady-clock seconds since epoch). Lets
    // standbys detect a leader that lost its grip on the filesystem
    // without releasing the lock cleanly (the lock should be released
    // on process death, but pathological cases like NFS hangs can leak
    // stale ownership).
    std::int64_t updated_unix_ms{0};
};

class HaCoordinator {
public:
    virtual ~HaCoordinator() = default;

    // Begin trying to acquire leadership. Polls in the background.
    // Idempotent; second start() is a no-op.
    virtual void start() = 0;

    // Release leadership (if held) and join the background thread.
    virtual void stop() = 0;

    // True if this process currently holds the leader lock.
    virtual bool is_leader() const noexcept = 0;

    // Monotonic counter incremented each time this process acquires
    // leadership. 0 = never acquired.
    virtual std::uint64_t epoch() const noexcept = 0;

    // Read the current leader endpoint from shared storage. Returns
    // nullopt if no leader is currently registered or the file is
    // unreadable.
    virtual std::optional<LeaderEndpoint> current_leader_endpoint() = 0;

    // Callback invoked when this process transitions from standby to
    // leader. Runs on the poll thread; must be quick (it blocks
    // subsequent polls). Pass nullptr to clear.
    using LeaderCallback = std::function<void(std::uint64_t /*new_epoch*/)>;
    virtual void set_on_become_leader(LeaderCallback cb) = 0;
};

// File-based coordinator: fcntl(LOCK_EX) on <ha_dir>/leader.lock plus
// <ha_dir>/active-leader.json. The lock file is created if missing and
// auto-released by the OS when the holding process exits or crashes.
// `advertise_endpoint` is the host:port this coordinator would publish if it
// became leader (used by the worker's discovery). Pass empty for workers.
std::unique_ptr<HaCoordinator> make_file_ha_coordinator(
    std::string ha_dir,
    LeaderEndpoint advertise_endpoint = {},
    std::chrono::milliseconds poll_interval = std::chrono::milliseconds{200});

}  // namespace clink::cluster
