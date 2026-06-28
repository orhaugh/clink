// Iceberg connector factory registration (iceberg_row_sink). Driver-free (the
// make_iceberg_row_sink factory that pulls iceberg-cpp is in the .cpp).

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "clink/iceberg/iceberg_row_sink.hpp"
#include "clink/iceberg/install.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace clink::iceberg {

namespace {

// Split a dotted namespace ("a.b.c") into levels; empty -> {"default"}.
std::vector<std::string> split_namespace(const std::string& s) {
    std::vector<std::string> out;
    std::size_t i = 0;
    while (i < s.size()) {
        std::size_t dot = s.find('.', i);
        if (dot == std::string::npos) {
            dot = s.size();
        }
        if (dot > i) {
            out.push_back(s.substr(i, dot - i));
        }
        i = dot + 1;
    }
    if (out.empty()) {
        out.push_back("default");
    }
    return out;
}

// Split a comma-separated list ("a,b,c") into trimmed non-empty items; empty -> {}.
std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::size_t i = 0;
    while (i < s.size()) {
        std::size_t comma = s.find(',', i);
        if (comma == std::string::npos) {
            comma = s.size();
        }
        std::size_t b = i;
        std::size_t e = comma;
        while (b < e && (s[b] == ' ' || s[b] == '\t')) {
            ++b;
        }
        while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t')) {
            --e;
        }
        if (e > b) {
            out.push_back(s.substr(b, e - b));
        }
        i = comma + 1;
    }
    return out;
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;
    using clink::sql::Row;

    // Self-register the SQL Row channel type so iceberg::install() works regardless of
    // whether clink::sql::install() has run yet (register_sink<Row> below needs the type
    // registered at call time). register_type is idempotent + last-write-wins, and this
    // is byte-for-byte the same (codec + wire batcher) registration clink::sql::install
    // performs, so the order the two installs run in does not change the final state -
    // it never clobbers SQL's columnar wire batcher with a codec-only registration.
    reg.register_type<Row>(std::string{clink::sql::kChannelRow},
                           clink::sql::row_json_codec(),
                           clink::sql::make_row_wire_batcher(clink::sql::row_json_codec()));

    // iceberg_row_sink: write Rows as an Apache Iceberg table (typed Parquet data
    // files + Iceberg snapshots) via iceberg-cpp + a SQLite SQL catalog. Append-only,
    // single-writer, at-least-once (see iceberg_row_sink.hpp). One snapshot per
    // checkpoint interval. Params:
    //   warehouse (required; the catalog warehouse location)
    //   table (required; the Iceberg table name)
    //   namespace (default "default"; dotted = multi-level)
    //   catalog_uri (default "<warehouse>/catalog.db" SQLite)
    //   schema_columns (required for the typed columns)
    reg.register_sink<Row>(
        "iceberg_row_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<Row>> {
            IcebergRowSinkOptions o;
            o.warehouse = ctx.param_or("warehouse", "");
            if (o.warehouse.empty()) {
                o.warehouse = ctx.param_or("path", "");  // accept 'path' as a warehouse alias
            }
            o.table = ctx.param_or("table", "");
            o.namespace_levels = split_namespace(ctx.param_or("namespace", "default"));
            o.catalog_uri = ctx.param_or("catalog_uri", "");
            // partition_by: comma-separated identity partition columns (empty = unpartitioned).
            o.partition_by = split_csv(ctx.param_or("partition_by", ""));
            // S3 FileIO config for an s3:// warehouse. clink-friendly param names mapped to
            // the iceberg S3 property keys; anything left unset falls back to the standard
            // AWS env/credential chain (incl. AWS_ENDPOINT_URL). path-style is required for
            // MinIO ('s3_path_style'='true').
            auto put_s3 = [&](const char* param, const char* ice_key) {
                std::string v = ctx.param_or(param, "");
                if (!v.empty()) {
                    o.file_io_props[ice_key] = std::move(v);
                }
            };
            put_s3("s3_endpoint", "s3.endpoint");
            put_s3("s3_region", "s3.region");
            put_s3("s3_access_key", "s3.access-key-id");
            put_s3("s3_secret_key", "s3.secret-access-key");
            put_s3("s3_session_token", "s3.session-token");
            put_s3("s3_path_style", "s3.path-style-access");
            o.batcher = clink::sql::make_row_columnar_arrow_batcher(
                clink::sql::parse_row_schema(ctx.param_or("schema_columns")));
            o.subtask_idx = ctx.subtask_idx;
            o.name = "iceberg_row_sink";
            return make_iceberg_row_sink(std::move(o));
        });
}

}  // namespace clink::iceberg
