#pragma once

// SnapshotArrowWriter - the canonical writer for the engine's Arrow IPC
// state-snapshot format:
//
//   op_id       : uint64   (OperatorId::value())
//   key_bytes   : binary   (full encoded key: kg byte + slot|user-key)
//   value_bytes : binary   (raw value bytes, opaque)
//
// plus an optional "clink.state_versions" schema-metadata entry holding
// the packed StateVersionMap. One row per (operator, key) entry; the
// output is a complete IPC stream (schema + one RecordBatch + EOS)
// directly readable by any Arrow consumer (pyarrow, DuckDB, Polars).
//
// Every producer of the format routes through this class so the bytes
// agree by construction: InMemoryStateBackend::snapshot() (and through
// it the sharded and file-backed backends), operator-state extraction,
// and the RocksDB Arrow export. Arrow types stay out of this header
// (pImpl) so linking a producer does not spread Arrow includes.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "clink/state/schema_version.hpp"

namespace clink {

// Schema-metadata key under which the packed StateVersionMap rides.
// Shared by the writer and every restore/inspection path that reads it.
inline constexpr char kStateVersionsMetadataKey[] = "clink.state_versions";

// Schema-metadata key carrying the snapshot FORMAT version, stamped on
// every stream this writer produces. Readers treat absence as version 1
// (streams written before the marker existed). The format contract and
// its evolution policy live in docs/internals/state-snapshot-format.md;
// the current version is 1.
inline constexpr char kSnapshotFormatVersionKey[] = "clink.format_version";
inline constexpr char kSnapshotFormatVersion[] = "1";

class SnapshotArrowWriter {
public:
    // `reserve_rows` pre-sizes the column builders (0 = grow on demand).
    explicit SnapshotArrowWriter(std::size_t reserve_rows = 0);
    ~SnapshotArrowWriter();

    SnapshotArrowWriter(const SnapshotArrowWriter&) = delete;
    SnapshotArrowWriter& operator=(const SnapshotArrowWriter&) = delete;
    SnapshotArrowWriter(SnapshotArrowWriter&&) = delete;
    SnapshotArrowWriter& operator=(SnapshotArrowWriter&&) = delete;

    void append(std::uint64_t op_id, std::string_view key_bytes, std::string_view value_bytes);

    // Build the complete IPC stream. A non-empty versions map is embedded
    // in the schema metadata; an empty one leaves the schema bare (the
    // reader treats absence as "no stamps recorded"). Call once.
    std::vector<std::byte> finish(const StateVersionMap& versions = {});

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
