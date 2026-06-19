#pragma once

// export_state_to_parquet_s3 (ASYNC-11 / DISAGG-9) - write a state backend's
// rows to partitioned Parquet objects in S3, queryable directly off object
// storage. One object per operator at <bucket>/<prefix>/op_id=<id>/state.parquet
// (each file is self-describing: it keeps the op_id column). Reuses the generic
// exporter (clink/connectors/state_parquet_export.hpp) over an Arrow S3 output
// stream - the same ParquetS3 transport the connectors use.
//
// Recipe (external): `duckdb -c "SELECT op_id, count(*) FROM
// read_parquet('s3://bucket/prefix/op_id=*/state.parquet') GROUP BY op_id"`
// (read with a plain glob; op_id is a file column, so do not also enable
// hive_partitioning or the path op_id would clash with the file column).

#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <arrow/filesystem/s3fs.h>
#include <arrow/io/api.h>

#include "clink/connectors/parquet_s3_sink.hpp"  // clink::detail::ensure_arrow_s3_initialised
#include "clink/connectors/state_parquet_export.hpp"
#include "clink/core/types.hpp"
#include "clink/state/state_backend.hpp"

namespace clink::s3 {

struct S3ExportOptions {
    std::string bucket;  // required
    std::string prefix;  // key prefix; objects land at <prefix>/op_id=<id>/state.parquet
    std::optional<std::string> region;
    std::optional<std::string> endpoint_override;  // MinIO / localstack
    bool allow_anonymous{false};
};

// Export each operator's rows to its own Parquet object in S3 (hive-style
// op_id=<id> partition dirs). Returns the total row count + operator count.
inline StateExportStats export_state_to_parquet_s3(const StateBackend& backend,
                                                   const std::vector<OperatorId>& ops,
                                                   const S3ExportOptions& opts) {
    if (opts.bucket.empty()) {
        throw std::invalid_argument("export_state_to_parquet_s3: bucket is required");
    }
    clink::detail::ensure_arrow_s3_initialised();
    auto s3_opts = arrow::fs::S3Options::Defaults();
    if (opts.region) {
        s3_opts.region = *opts.region;
    }
    if (opts.endpoint_override) {
        s3_opts.endpoint_override = *opts.endpoint_override;
        s3_opts.scheme = "http";
    }
    if (opts.allow_anonymous) {
        s3_opts.ConfigureAnonymousCredentials();
    }
    auto fs_result = arrow::fs::S3FileSystem::Make(s3_opts);
    if (!fs_result.ok()) {
        throw std::runtime_error("export_state_to_parquet_s3: S3FileSystem::Make: " +
                                 fs_result.status().ToString());
    }
    auto fs = *fs_result;

    std::string base = opts.bucket;
    if (!opts.prefix.empty()) {
        base += "/" + opts.prefix;
    }
    StateExportStats total;
    total.operators = ops.size();
    for (const auto op : ops) {
        const std::string key = base + "/op_id=" + std::to_string(op.value()) + "/state.parquet";
        auto out_result = fs->OpenOutputStream(key);
        if (!out_result.ok()) {
            throw std::runtime_error("export_state_to_parquet_s3: OpenOutputStream(" + key +
                                     "): " + out_result.status().ToString());
        }
        const auto stats = export_state_to_parquet(backend, {op}, *out_result);
        if (auto c = (*out_result)->Close(); !c.ok()) {
            throw std::runtime_error("export_state_to_parquet_s3: close(" + key +
                                     "): " + c.ToString());
        }
        total.rows += stats.rows;
    }
    return total;
}

}  // namespace clink::s3
