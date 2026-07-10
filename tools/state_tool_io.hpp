#pragma once

// Shared input handling for the state CLI verbs (state-cat, state-diff,
// state-export, state-query): resolve a --from path (a .snap/.arrows
// snapshot file, or a RocksDB checkpoint directory rendered through the
// Arrow export on RocksDB-linked builds) or a --dir/--id multi-subtask
// checkpoint into ONE canonical Arrow IPC snapshot byte stream.

#include <cstddef>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

#include "clink/state/in_memory_state_backend.hpp"

#ifdef CLINK_LINKED_ROCKSDB
#include "clink/state/rocksdb_state_backend.hpp"
#endif

namespace clink_tools {

// A RocksDB checkpoint dir is a complete RocksDB instance; CURRENT is
// its always-present manifest pointer, so it discriminates cleanly from
// a clink checkpoint dir of .snap files.
inline bool is_rocksdb_checkpoint_dir(const std::filesystem::path& p) {
    return std::filesystem::is_directory(p) && std::filesystem::exists(p / "CURRENT");
}

// Canonical snapshot bytes for a --from path: a .snap/.arrows file
// verbatim, or a RocksDB checkpoint dir rendered through the Arrow
// export (when the CLI is built with RocksDB linked).
inline std::vector<std::byte> canonical_bytes_for(const std::string& path) {
    if (is_rocksdb_checkpoint_dir(path)) {
#ifdef CLINK_LINKED_ROCKSDB
        return clink::rocksdb_checkpoint_to_arrow(path);
#else
        throw std::runtime_error(
            path +
            " is a RocksDB checkpoint dir, but this clink build has no RocksDB "
            "support; rebuild with the RocksDB impl linked");
#endif
    }
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("cannot open " + path);
    }
    std::vector<char> raw((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::vector<std::byte> bytes(raw.size());
    if (!raw.empty()) {
        std::memcpy(bytes.data(), raw.data(), raw.size());
    }
    return bytes;
}

// Collect the snapshot files that make up checkpoint `id` under `root`:
// every <root>/<subtask>/checkpoint-<id>.snap, plus <root>/checkpoint-
// <id>.snap when root itself is a subtask dir. Sorted for determinism.
inline std::vector<std::filesystem::path> checkpoint_files(const std::filesystem::path& root,
                                                           std::uint64_t id) {
    namespace fs = std::filesystem;
    const std::string name = "checkpoint-" + std::to_string(id) + ".snap";
    std::vector<fs::path> files;
    if (fs::exists(root / name)) {
        files.push_back(root / name);
    }
    if (fs::is_directory(root)) {
        for (const auto& entry : fs::directory_iterator(root)) {
            if (entry.is_directory() && fs::exists(entry.path() / name)) {
                files.push_back(entry.path() / name);
            }
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}

// Resolve the --from XOR --dir/--id input into canonical snapshot bytes
// plus a human-readable source label. The dir form merges every
// subtask's file via merge_snapshot_bytes - exactly the form a
// scale-down restore consumes (first non-empty file's version metadata
// preserved; the loader applies the operator-state collision policy).
struct ResolvedInput {
    std::vector<std::byte> bytes;
    std::string label;
};

inline ResolvedInput resolve_state_input(const std::string& from,
                                         const std::string& dir,
                                         const std::string& id_str) {
    ResolvedInput out;
    if (!from.empty()) {
        out.bytes = canonical_bytes_for(from);
        out.label = from;
        return out;
    }
    const auto id = std::stoull(id_str);
    const auto files = checkpoint_files(dir, id);
    if (files.empty()) {
        throw std::runtime_error("no checkpoint-" + std::to_string(id) + ".snap under " + dir +
                                 " (or its subtask subdirectories)");
    }
    std::vector<std::vector<std::byte>> parts;
    parts.reserve(files.size());
    for (const auto& f : files) {
        parts.push_back(canonical_bytes_for(f.string()));
    }
    out.bytes = clink::InMemoryStateBackend::merge_snapshot_bytes(parts);
    out.label = dir + " @ checkpoint " + std::to_string(id) + " (" + std::to_string(files.size()) +
                " subtask files)";
    return out;
}

}  // namespace clink_tools
