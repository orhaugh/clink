#pragma once

// Apache Iceberg table-format SINK for the SQL Row channel, built on the native
// Apache iceberg-cpp library. Reuses the row columnar Arrow batcher (the same one
// parquet_row_sink / delta_row_sink use) for the typed Parquet DATA FILES, and uses
// iceberg-cpp for the manifests + table metadata + the atomic catalog commit, so the
// result is a real Iceberg table that Spark / Trino / PyIceberg / DuckDB read.
//
// CADENCE: one Iceberg snapshot per checkpoint interval - write the interval's Rows into
// one Parquet data file, then snapshot (FastAppend) it.
//
// DELIVERY = EXACTLY-ONCE when wired into the engine's two-phase commit (the data file is
// staged on the barrier and the snapshot is committed only after the checkpoint is
// globally durable - on_commit; the commit is idempotent, tagged with a clink.checkpoint-id
// snapshot summary property, so a redelivered commit or recovery replay never
// double-commits). Append-only, SINGLE-WRITER (only subtask 0 active). Falls back to
// AT-LEAST-ONCE in standalone use (no state backend / no JM): the barrier commits
// immediately. NOT in v1: UPDATE/DELETE/MERGE, partitioning. v1 catalog = SQLite SQL
// catalog (server-less, offline-testable); REST catalog is a follow-on; S3 FileIO is
// supported (s3:// warehouse).
//
// RESIDUALS: a data file staged before a crash but whose checkpoint never completed is an
// orphan until the engine's abort deletes it, or until Iceberg orphan-file maintenance
// reclaims it. SINGLE-WRITER is enforced within a job (subtask 0 only); the
// clink.checkpoint-id idempotency marker protects against THIS writer's own
// redelivery/replay - it is a precondition, NOT enforced, that no other job writes the
// same table concurrently (a foreign concurrent writer could defeat the idempotency scan).
//
// This header is driver-free (no iceberg-cpp includes) so the factory registration
// and tests do not need the iceberg-cpp headers; the implementation is in the .cpp.

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"

namespace clink::iceberg {

struct IcebergRowSinkOptions {
    // Catalog warehouse location. A local filesystem path (data + metadata + catalog all
    // local), OR an "s3://bucket/prefix" URI to write data + table metadata to S3/MinIO.
    std::string warehouse;
    std::vector<std::string> namespace_levels{"default"};  // table namespace
    std::string table;                                     // table name (required)
    // SQLite catalog DB path; default "<warehouse>/catalog.db" for a LOCAL warehouse.
    // REQUIRED (and must be a local path) for an s3:// warehouse - the SQLite catalog
    // file cannot live on S3. (REST catalog is a follow-on - an http(s):// uri selects it.)
    std::string catalog_uri;
    // FileIO properties for an s3:// warehouse (iceberg S3 keys: "s3.endpoint",
    // "s3.region", "s3.access-key-id", "s3.secret-access-key", "s3.path-style-access",
    // "s3.session-token"). Empty for a local warehouse. Missing creds fall back to the
    // standard AWS env/credential chain.
    std::unordered_map<std::string, std::string> file_io_props;
    ArrowBatcher<clink::sql::Row> batcher;  // schema-driven typed Arrow batcher
    std::uint32_t subtask_idx{0};           // only subtask 0 writes (single-writer table)
    std::string name{"iceberg_row_sink"};
};

// Build an Iceberg sink that writes Rows as Parquet data files + Iceberg snapshots.
std::shared_ptr<Sink<clink::sql::Row>> make_iceberg_row_sink(IcebergRowSinkOptions opts);

}  // namespace clink::iceberg
