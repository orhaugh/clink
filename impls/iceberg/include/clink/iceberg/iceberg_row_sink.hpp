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
// DUPLICATE WINDOW: the snapshot is committed when the barrier reaches this sink, which
// is BEFORE the job confirms the checkpoint is globally durable (there is no 2PC
// on_commit phase here). So duplicates arise not only from a crash between the data-file
// write and commit, but also when a checkpoint that already passed this sink is later
// ABORTED job-wide: failover replays the interval and commits a SECOND snapshot with the
// same rows. Both are valid at-least-once (no loss; readers may see duplicate rows). A
// data file written but not committed (commit failure / crash before commit) is orphaned;
// it is best-effort deleted on a commit failure, otherwise needs Iceberg orphan-file
// maintenance to reclaim. SINGLE-WRITER is enforced within a job (subtask 0 only); it is
// a precondition, NOT enforced, that no other job writes the same table concurrently.
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
