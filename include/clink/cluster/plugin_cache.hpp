#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "clink/cluster/protocol.hpp"

namespace clink::cluster {

// FNV-1a 64-bit hash. Stable across builds and architectures. Used to
// identify plugin blobs in caches; not cryptographic.
std::uint64_t fnv1a_64(std::span<const std::byte> bytes) noexcept;

// Hex string of fnv1a_64; 16 chars lowercase.
std::string fnv1a_64_hex(std::span<const std::byte> bytes);

// Write a plugin binary to a per-process cache directory and return
// the on-disk path. Idempotent by content_hash; a second call for the
// same hash with the same bytes returns the cached path without
// re-writing.
//
// On disk, the path is `<base>/<content_hash>.dylib` (macOS) or `.so`
// (Linux). The base dir is created if it doesn't exist.
//
// `base_dir` defaults to a process-wide temp directory under
// $TMPDIR/clink-plugins/<pid>/. Pass an explicit path to share a
// cache across processes (e.g. integration tests with JM+TMs in
// separate processes).
std::string write_plugin_to_cache(const PluginBinary& blob, const std::string& base_dir = {});

// Compute content_hash for a fresh PluginBinary loaded from disk
// (used by the client when packaging a plugin for SubmitJob).
PluginBinary make_plugin_binary_from_file(const std::string& path, const std::string& name = {});

}  // namespace clink::cluster
