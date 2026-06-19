#pragma once

// EtcdHaCoordinator - multi-machine HA leader election via etcd v3.
//
// The factory function is always declared so call sites compile
// regardless of whether clink::etcd is linked. When the impl
// library is absent at link time the factory's definition is missing
// and the call fails at link, surfacing the configuration error
// loudly. clink_node guards the call with a build-time #ifdef so
// the binary degrades gracefully to FileHaCoordinator.
//
// Protocol (mirrors etcd-recipe leader election):
//   1. Grant a lease with TTL = lease_ttl_seconds.
//   2. Spawn a KeepAlive thread that refreshes the lease every
//      ~ttl/3 seconds - losing the lease drops leadership.
//   3. Atomic-Put the leader key with the lease; success = leader.
//   4. Standbys Watch the leader key and re-try on DELETE.
//   5. current_leader_endpoint() Gets the key and parses the value
//      payload (JSON-shaped, identical to FileHaCoordinator's
//      active-leader.json so consumers don't have to switch).
//
// Lease loss is the failure-detection primitive: a leader whose
// process or KeepAlive thread freezes lets the lease expire after
// at most lease_ttl_seconds, etcd deletes the key, every standby's
// watcher fires, and the next acquirer becomes leader.

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "clink/cluster/ha_coordinator.hpp"

namespace clink::cluster {

struct EtcdHaConfig {
    // Endpoints in any etcd-cpp-apiv3-accepted form, e.g.
    // "http://etcd-0:2379,http://etcd-1:2379" or a single URL.
    std::string endpoints;

    // Logical cluster name. Lets one etcd cluster host multiple
    // clink deployments - the leader key is prefixed with this so
    // /clink/jm/<cluster_name>/leader doesn't collide. Required.
    std::string cluster_name;

    // Lease TTL. A leader that fails to refresh within this window
    // loses the lock; defaults to 10s ( k8s HA uses 15s).
    std::chrono::seconds lease_ttl{10};

    // Reconnect / retry interval when etcd is unreachable.
    std::chrono::milliseconds retry_interval{500};
};

// Construct an etcd-backed HaCoordinator. advertise is the (host,
// port) this JM publishes when it becomes leader (zero-init for TMs
// that only read). The factory throws std::runtime_error on a
// fundamentally broken config (empty endpoints / cluster_name); the
// initial etcd connection happens lazily on start() so a temporarily
// unreachable etcd doesn't crash construction.
std::unique_ptr<HaCoordinator> make_etcd_ha_coordinator(EtcdHaConfig config,
                                                        LeaderEndpoint advertise = {});

}  // namespace clink::cluster
