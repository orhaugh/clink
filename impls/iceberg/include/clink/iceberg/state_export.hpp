#pragma once

// State-as-data: export a snapshot's keyed state as an Apache Iceberg
// table - the catalogued-lake form of the analytics projection. The
// table schema mirrors the Parquet export's decoded entry model:
//
//   op_id       : long     OperatorId::value(), bit-cast to signed
//                          (Iceberg has no unsigned types; ids above
//                          2^63 render negative but stay exact)
//   key_group   : int      leading key byte (>= 128 = operator state)
//   slot        : string   state-slot name ("<raw>" for unparsed keys)
//   user_key    : binary   raw user-key bytes after the '|' separator
//   value_bytes : binary   raw codec value bytes
//
// Each export commits ONE Iceberg snapshot (a FastAppend of one Parquet
// data file, tagged clink.state-export in the snapshot summary), so
// repeated exports into the same table accumulate as snapshots and the
// table's own history time-travels across them. The table is created
// when missing (unpartitioned) and appended to when present; appending
// requires the existing table's column names to match, otherwise the
// export throws rather than commit a mismatched file.
//
// Catalog/warehouse selection matches the Iceberg sink: a local
// filesystem or s3:// warehouse with the SQLite catalog (an s3://
// warehouse needs an explicit local catalog_uri), or an http(s)://
// catalog_uri for a REST catalog. StateVersionMap stamps are NOT
// carried in v1 (an Iceberg snapshot summary is string-typed; the
// packed map is binary) - use the Arrow/Parquet exports where version
// metadata matters.
//
// This header is driver-free (no iceberg-cpp includes); the
// implementation lives in iceberg_row_sink.cpp alongside the sink so
// the two share one catalog-open and write path.

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "clink/state_processor/state_diff.hpp"

namespace clink::iceberg {

struct IcebergStateExportOptions {
    std::string warehouse;                                 // required
    std::string table;                                     // required
    std::vector<std::string> namespace_levels{"default"};  // table namespace
    // Catalog selector: empty = <warehouse>/catalog.db (SQLite);
    // a local path = SQLite at that path; http(s):// = REST catalog.
    std::string catalog_uri;
    std::string rest_auth_token;
    // FileIO properties (s3.endpoint / s3.region / credentials /
    // s3.path-style-access) for an s3:// warehouse or a REST catalog.
    std::unordered_map<std::string, std::string> file_io_props;
};

struct IcebergStateExportResult {
    std::string table_location;  // the table's resolved location
    std::int64_t rows{0};        // entries written (one row each)
};

// Write every entry of the collected model as one Iceberg snapshot.
// Throws on catalog/write/commit failure or a schema mismatch with an
// existing table.
IcebergStateExportResult export_state_iceberg(const clink::state_processor::StateEntries& entries,
                                              IcebergStateExportOptions options);

}  // namespace clink::iceberg
