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

}  // namespace clink::forst_s3
