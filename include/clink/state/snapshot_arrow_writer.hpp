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
//
// The one exception is arrow::KeyValueMetadata, needed by the format-version
// gate below: it is the type every reader already holds when it needs to
// check, and a pImpl around a version comparison would cost more clarity
// than the include costs compile time.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include <arrow/util/key_value_metadata.h>

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

// Highest format version this build can READ. Bump together with
// kSnapshotFormatVersion, and only for a change a reader of the previous
// version would MISREAD - never for one it would merely miss. Adding a
// metadata key or a derived projection is a compatible change and must
// not bump either.
inline constexpr std::uint32_t kMaxReadableSnapshotFormatVersion = 1;

// Enforce the format version on a snapshot about to be read.
//
// docs/internals/state-snapshot-format.md has always said readers MUST
// refuse a version above the highest they know rather than guess. Nothing
// did: the marker was written by the writer and read by nobody. Since a
// version bump means precisely "a change the previous reader MISREADS",
// and a bump does not have to change the column shape - a key-layout
// change would not - the existing schema check does not stand in for this.
// A future version-2 stream would have been restored as version 1 and
// produced wrong state with no error anywhere.
//
// Absence is version 1, permanently: streams written before the marker
// existed are valid and must stay readable.
//
// `context` names the caller, because "snapshot from a newer format" is
// only actionable if you know which snapshot.
inline void verify_snapshot_format_version(
    const std::shared_ptr<const arrow::KeyValueMetadata>& metadata, std::string_view context) {
    if (!metadata) {
        return;  // pre-marker stream: version 1
    }
    const auto idx = metadata->FindKey(kSnapshotFormatVersionKey);
    if (idx == -1) {
        return;  // pre-marker stream: version 1
    }
    const auto raw = metadata->value(idx);
    // Strict: every character a digit, nothing else. std::stoul alone is
    // too lenient for a gate - it skips leading whitespace and stops at
    // the first non-digit, so " 1" and "1.0" both parse as 1 and a stream
    // that was never written by clink reads as a version this build
    // happens to accept.
    const bool well_formed =
        !raw.empty() && raw.size() <= 9 &&
        std::all_of(raw.begin(), raw.end(), [](unsigned char c) { return c >= '0' && c <= '9'; });
    if (!well_formed) {
        throw std::runtime_error(std::string(context) +
                                 ": snapshot format version is not a plain decimal number ('" +
                                 raw + "'). The stream is corrupt, or was not written by clink.");
    }
    const auto version = static_cast<std::uint32_t>(std::stoul(raw));
    if (version == 0) {
        throw std::runtime_error(std::string(context) +
                                 ": snapshot declares format version 0, which does not exist.");
    }
    if (version > kMaxReadableSnapshotFormatVersion) {
        throw std::runtime_error(
            std::string(context) + ": snapshot is format version " + std::to_string(version) +
            " but this build reads at most version " +
            std::to_string(kMaxReadableSnapshotFormatVersion) +
            ". A newer format differs in a way this reader would misread rather than merely "
            "miss, so it is refused rather than guessed at. Restore with a build that "
            "understands it.");
    }
}

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
