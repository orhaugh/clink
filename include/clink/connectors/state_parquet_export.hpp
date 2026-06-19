#pragma once

// export_state_to_parquet (DISAGG-9) - dump a state backend's rows to an open
// columnar Parquet file, directly queryable by DuckDB / pyarrow / polars off the
// file (or off object storage when the file is written through the ParquetS3
// seam). This is the externally-queryable-state property: the engine's state
// leaves the private SST/IPC formats as plain Parquet that any tool can read.
//
// Schema: op_id (uint64), key (binary), value (binary) - one row per stored
// entry across the requested operators. Values are raw bytes: a typed,
// column-per-field view needs the job's schema, which the SQL frontend carries
// and a typed exporter can layer on later; the generic export is schema-agnostic
// and lossless (the bytes round-trip exactly).
//
// Recipe (external, no engine dependency): `duckdb -c "SELECT op_id, count(*)
// FROM 'state.parquet' GROUP BY op_id"`, or pyarrow `pq.read_table('state.parquet')`.

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#ifndef CLINK_HAS_PARQUET
#error "state Parquet export requires CLINK_BUILD_ARROW=ON (Parquet ships alongside Arrow)."
#endif

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include "clink/core/types.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

struct StateExportStats {
    std::int64_t rows{0};
    std::size_t operators{0};
};

namespace detail {

inline void check_export_status(const arrow::Status& s, const char* what) {
    if (!s.ok()) {
        throw std::runtime_error(std::string("export_state_to_parquet: ") + what + ": " +
                                 s.ToString());
    }
}

// Scan `ops` into an Arrow table {op_id, key, value}; fills row/operator counts.
inline std::shared_ptr<arrow::Table> build_state_table(const StateBackend& backend,
                                                       const std::vector<OperatorId>& ops,
                                                       StateExportStats& stats) {
    arrow::UInt64Builder op_b;
    arrow::BinaryBuilder key_b;
    arrow::BinaryBuilder val_b;
    stats.operators = ops.size();
    for (const auto op : ops) {
        backend.scan(op, [&](StateBackend::KeyView k, StateBackend::ValueView v) {
            check_export_status(op_b.Append(op.value()), "op append");
            check_export_status(key_b.Append(reinterpret_cast<const std::uint8_t*>(k.data()),
                                             static_cast<std::int32_t>(k.size())),
                                "key append");
            check_export_status(val_b.Append(reinterpret_cast<const std::uint8_t*>(v.data()),
                                             static_cast<std::int32_t>(v.size())),
                                "value append");
            ++stats.rows;
        });
    }
    std::shared_ptr<arrow::Array> op_arr;
    std::shared_ptr<arrow::Array> key_arr;
    std::shared_ptr<arrow::Array> val_arr;
    check_export_status(op_b.Finish(&op_arr), "op finish");
    check_export_status(key_b.Finish(&key_arr), "key finish");
    check_export_status(val_b.Finish(&val_arr), "value finish");
    auto schema = arrow::schema({arrow::field("op_id", arrow::uint64()),
                                 arrow::field("key", arrow::binary()),
                                 arrow::field("value", arrow::binary())});
    return arrow::Table::Make(schema, {op_arr, key_arr, val_arr});
}

inline void write_table_parquet(const arrow::Table& table,
                                std::int64_t rows,
                                const std::shared_ptr<arrow::io::OutputStream>& out) {
    auto props =
        parquet::WriterProperties::Builder().compression(parquet::Compression::ZSTD)->build();
    auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
    const std::int64_t chunk = std::max<std::int64_t>(rows, 1);
    check_export_status(parquet::arrow::WriteTable(
                            table, arrow::default_memory_pool(), out, chunk, props, arrow_props),
                        "WriteTable");
}

}  // namespace detail

// Scan every (key, value) row of each operator in `ops` and write them as a
// ZSTD-compressed Parquet table to a caller-provided Arrow output stream (a
// local file, or an S3 stream for off-object-storage export). The caller owns
// and closes the stream. The schema is stored so external readers see the
// column names. Returns the row + operator counts.
inline StateExportStats export_state_to_parquet(
    const StateBackend& backend,
    const std::vector<OperatorId>& ops,
    const std::shared_ptr<arrow::io::OutputStream>& out) {
    StateExportStats stats;
    auto table = detail::build_state_table(backend, ops, stats);
    detail::write_table_parquet(*table, stats.rows, out);
    return stats;
}

// Convenience: export to a local Parquet file at out_path.
inline StateExportStats export_state_to_parquet(const StateBackend& backend,
                                                const std::vector<OperatorId>& ops,
                                                const std::filesystem::path& out_path) {
    auto out_result = arrow::io::FileOutputStream::Open(out_path.string());
    if (!out_result.ok()) {
        throw std::runtime_error("export_state_to_parquet: open " + out_path.string() + ": " +
                                 out_result.status().ToString());
    }
    auto stats = export_state_to_parquet(backend, ops, *out_result);
    detail::check_export_status((*out_result)->Close(), "close");
    return stats;
}

}  // namespace clink
