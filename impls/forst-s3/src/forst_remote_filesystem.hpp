#pragma once

// Live remote data files for the ForSt backend: a forstdb filesystem
// that routes the immutable data files (*.sst) under a local root to
// object storage via arrow::fs::S3FileSystem, while every other file
// (MANIFEST, CURRENT, WAL, LOCK, OPTIONS, LOG, ...) stays on the local
// filesystem. See forst_remote_filesystem.cpp for the mapping and
// semantics; this header is deliberately engine-free (opaque handles)
// so the scheme-registration TU never includes engine headers.

#include <memory>
#include <string>

#include "clink/state/forst_state_backend.hpp"  // clink::DataFileMirror

namespace clink::forst_s3 {

struct RemoteSstOptions {
    std::string bucket;
    // Objects live at <bucket>/<key_prefix>/<path-relative-to-local_root>.
    std::string key_prefix;
    // Paths under this root participate in the mapping (the working DB,
    // its sibling .cp-<id> checkpoint dirs, and restore working dirs all
    // live under it by construction of the scheme).
    std::string local_root;
    std::string endpoint;  // empty = real AWS; set for MinIO/localstack
    std::string region;
    bool anonymous{false};
    // Optional local read cache for the remote data files. Populated by
    // write-tee (an SST's bytes land in the cache as they upload, so
    // flush and compaction outputs - the bulk of steady-state reads -
    // are served locally), evicted LRU to the byte budget, dropped when
    // the engine deletes the SST. 0 bytes = disabled. The cache dir must
    // NOT sit under local_root (a *.sst there would route back to S3).
    // Empty dir with a non-zero budget = "<local_root>.sst-cache".
    std::string sst_cache_dir;
    std::uint64_t sst_cache_bytes{0};
};

// The engine Env whose filesystem implements the routing, in the opaque
// shape ForStStateBackend::Options carries (engine types stay out of
// public headers). holder owns the Env; env is the forstdb::Env*.
struct RemoteSstEnv {
    std::shared_ptr<void> holder;
    void* env{nullptr};
};

// Build the composite Env. Throws on S3 filesystem construction failure.
RemoteSstEnv make_remote_sst_env(const RemoteSstOptions& opts);

// The companion DataFileMirror over the SAME mapping: replicates /
// deletes / lists the remote data files that accompany a local
// checkpoint dir (restore re-home, purge, snapshot stats).
std::shared_ptr<clink::DataFileMirror> make_s3_data_file_mirror(const RemoteSstOptions& opts);

// SnapshotStore that uploads a checkpoint dir's (metadata-only) files to
// the same object prefix as its data files at persist time, and
// re-materialises them locally at fetch when the local dir is missing -
// this is what makes an s3sst checkpoint restorable on a fresh machine
// from the bucket alone (same URI, empty disk). defers_durable_write()
// is true, so the upload rides the snapshot worker via the backend's
// capture/persist split.
std::shared_ptr<clink::SnapshotStore> make_s3_metadata_store(const RemoteSstOptions& opts);

// Delete stale checkpoint STAGING prefixes ("<subtask>.cp-*.tmp") left
// on object storage by a checkpoint that crashed mid-stage: the engine
// removes the local staging dir on failure, but that never reaches the
// remote side. Called at backend construction (a fresh construction
// means no staging is in flight for this subtask, so everything matching
// is garbage). Returns the number of objects deleted.
std::size_t sweep_stale_staging(const RemoteSstOptions& opts, const std::string& subtask_dir);

}  // namespace clink::forst_s3
