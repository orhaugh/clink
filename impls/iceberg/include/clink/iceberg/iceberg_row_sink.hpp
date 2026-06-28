#pragma once

// Apache Iceberg table-format SINK for the SQL Row channel, built on the native
// Apache iceberg-cpp library. Reuses the row columnar Arrow batcher (the same one
// parquet_row_sink / delta_row_sink use) for the typed Parquet DATA FILES, and uses
// iceberg-cpp for the manifests + table metadata + the atomic catalog commit, so the
// result is a real Iceberg table that Spark / Trino / PyIceberg / DuckDB read.
//
// CADENCE: one Iceberg snapshot (FastAppend commit) per checkpoint interval - write
// the interval's Rows into one Parquet data file, then on the barrier commit a
// snapshot whose `add` references it.
//
// DELIVERY = AT-LEAST-ONCE, append-only, SINGLE-WRITER (only subtask 0 active). NOT
// in v1: UPDATE/DELETE/MERGE, partitioning, exactly-once. v1 catalog = SQLite SQL
// catalog (server-less, offline-testable); REST catalog + S3 FileIO are follow-ons.
//
// This header is driver-free (no iceberg-cpp includes) so the factory registration
// and tests do not need the iceberg-cpp headers; the implementation is in the .cpp.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"

namespace clink::iceberg {

struct IcebergRowSinkOptions {
    std::string warehouse;  // catalog warehouse location (local path; s3:// is a follow-on)
    std::vector<std::string> namespace_levels{"default"};  // table namespace
    std::string table;                                     // table name (required)
    // SQLite catalog DB path; default "<warehouse>/catalog.db". (REST catalog is a
    // follow-on - a uri starting with http(s):// would select it.)
    std::string catalog_uri;
    ArrowBatcher<clink::sql::Row> batcher;  // schema-driven typed Arrow batcher
    std::uint32_t subtask_idx{0};           // only subtask 0 writes (single-writer table)
    std::string name{"iceberg_row_sink"};
};

// Build an Iceberg sink that writes Rows as Parquet data files + Iceberg snapshots.
std::shared_ptr<Sink<clink::sql::Row>> make_iceberg_row_sink(IcebergRowSinkOptions opts);

}  // namespace clink::iceberg
