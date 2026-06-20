#pragma once

// Disaggregated-state observability (OBS-3).
//
// Makes remote/disaggregated state observable: how often a read is served hot
// vs. fetched from the remote store and how long those fetches take
// (RemoteReadBackend), how well the on-host object cache is doing
// (LocalObjectCache), and how many objects/bytes each checkpoint pushes to the
// content-addressed store (S3CasSnapshotStore). These are the signals that tell
// an operator whether disaggregation is paying off (high cache hit-rate, low
// remote-read tail) or hurting (cold cache, fat remote tail).
//
// All metrics live under the clink_disagg_ prefix. Remote-read latency is a
// histogram (OBS-1) so the p99 fetch tail is visible, not just the mean.

#include <cstdint>

#include "clink/metrics/metrics_registry.hpp"

namespace clink::metrics {

inline constexpr const char* kRemoteHotHits = "clink_disagg_remote_hot_hits_total";
inline constexpr const char* kRemoteLoads = "clink_disagg_remote_loads_total";
inline constexpr const char* kRemoteLoadLatencyNs = "clink_disagg_remote_load_latency_ns";
inline constexpr const char* kHotEvictions = "clink_disagg_hot_evictions_total";
inline constexpr const char* kHotResidentBytes = "clink_disagg_hot_resident_bytes";
inline constexpr const char* kObjectCacheHits = "clink_disagg_object_cache_hits_total";
inline constexpr const char* kObjectCacheMisses = "clink_disagg_object_cache_misses_total";
inline constexpr const char* kObjectCacheEntries = "clink_disagg_object_cache_entries";
inline constexpr const char* kCheckpointObjects = "clink_disagg_checkpoint_objects";
inline constexpr const char* kCheckpointObjectBytes = "clink_disagg_checkpoint_object_bytes";
inline constexpr const char* kCasObjectsUploaded = "clink_disagg_cas_objects_uploaded_total";
inline constexpr const char* kCasObjectsDownloaded = "clink_disagg_cas_objects_downloaded_total";

namespace disagg {

// RemoteReadBackend: a read served from the hot tier with no remote fetch.
inline void remote_hot_hit() {
    MetricsRegistry::global().counter(kRemoteHotHits).increment();
}

// RemoteReadBackend: a blocking remote fetch completed in `latency_ns`. Bumps
// the load counter and records the latency distribution (p50/p95/p99 fetch tail).
inline void remote_load_observe(std::uint64_t latency_ns) {
    MetricsRegistry::global().counter(kRemoteLoads).increment();
    MetricsRegistry::global()
        .histogram(kRemoteLoadLatencyNs)
        .observe(static_cast<double>(latency_ns));
}

// RemoteReadBackend: a clean (durably-committed) key was evicted from the hot
// tier to honour the byte budget; a later read of it cold-fetches from the pool.
// This is the signal that working state genuinely exceeds the hot budget (true
// state-greater-than-RAM disaggregation), not just fast restore.
inline void hot_evicted() {
    MetricsRegistry::global().counter(kHotEvictions).increment();
}

// RemoteReadBackend: current resident hot-tier footprint in bytes (a gauge;
// publish on change-points like checkpoint, not per record).
inline void hot_resident_bytes_set(std::int64_t bytes) {
    MetricsRegistry::global().gauge(kHotResidentBytes).set(bytes);
}

// LocalObjectCache: a served-from-cache hit / a miss that fell through to S3.
inline void object_cache_hit() {
    MetricsRegistry::global().counter(kObjectCacheHits).increment();
}
inline void object_cache_miss() {
    MetricsRegistry::global().counter(kObjectCacheMisses).increment();
}

// LocalObjectCache: current resident entry count (a gauge; publish on change).
inline void object_cache_entries_set(std::int64_t n) {
    MetricsRegistry::global().gauge(kObjectCacheEntries).set(n);
}

// S3CasSnapshotStore: a checkpoint just wrote `object_count` objects totalling
// `object_bytes` to the content-addressed pool. Gauges reflect the most recent
// checkpoint, so a Grafana panel shows the per-checkpoint upload footprint
// (which content addressing keeps near O(changed bytes), not O(total state)).
inline void checkpoint_written(std::int64_t object_count, std::int64_t object_bytes) {
    MetricsRegistry::global().gauge(kCheckpointObjects).set(object_count);
    MetricsRegistry::global().gauge(kCheckpointObjectBytes).set(object_bytes);
}

// S3CasSnapshotStore: one content-addressed object actually uploaded (a shared
// object that a HEAD skipped does NOT count) / downloaded on restore.
inline void cas_object_uploaded() {
    MetricsRegistry::global().counter(kCasObjectsUploaded).increment();
}
inline void cas_object_downloaded() {
    MetricsRegistry::global().counter(kCasObjectsDownloaded).increment();
}

}  // namespace disagg

}  // namespace clink::metrics
