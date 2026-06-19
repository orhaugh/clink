#pragma once

// RocksDbMaterializationStore - RocksDB-backed implementation of
// clink::ExternalMaterializationStore. Used by ChangelogStateBackend
// when the user picks the changelog+rocksdb:// scheme via the
// StateBackendFactory, or directly by code that wants to keep large
// materializations in a key/value store off the snapshot wire.
//
// Layout:
//   * A single RocksDB instance at the path passed to the ctor.
//   * Each materialization is stored under key "mat:<checkpoint_id>"
//     (text-encoded). The handle string is the same key - the store
//     is its own resolver.
//
// Lifetime: the constructor opens the DB (create_if_missing=true);
// the destructor closes it. Concurrent writes/reads/erases against
// the same handle are serialised via RocksDB's own internal locking.

#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "clink/state/external_materialization_store.hpp"

namespace clink::rocksdb {

class RocksDbMaterializationStore final : public clink::ExternalMaterializationStore {
public:
    explicit RocksDbMaterializationStore(std::filesystem::path path);
    ~RocksDbMaterializationStore() override;

    RocksDbMaterializationStore(const RocksDbMaterializationStore&) = delete;
    RocksDbMaterializationStore& operator=(const RocksDbMaterializationStore&) = delete;

    std::string write(clink::CheckpointId id, std::span<const std::byte> bytes) override;
    std::vector<std::byte> read(const std::string& handle) override;
    void erase(const std::string& handle) override;
    [[nodiscard]] std::string description() const override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    std::filesystem::path path_;
};

}  // namespace clink::rocksdb
