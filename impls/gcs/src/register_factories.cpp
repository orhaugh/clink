// GCS Parquet factory registration (gcs_parquet_{int64,string}_{sink,source}).

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/connectors/parquet_gcs_sink.hpp"
#include "clink/connectors/parquet_gcs_source.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/gcs/install.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::gcs {

namespace {

// Apply the shared GCS auth/endpoint params onto an Options struct (sink or source share these).
template <typename Opts>
void apply_gcs_params(const clink::plugin::BuildContext& ctx, Opts& opts) {
    opts.bucket = ctx.param_or("bucket");
    opts.key = ctx.param_or("key");
    opts.anonymous = ctx.param_or("anonymous", "false") == "true";
    if (const auto t = ctx.param_or("access_token", ""); !t.empty()) {
        opts.access_token = t;
    }
    if (const auto e = ctx.param_or("endpoint_override", ""); !e.empty()) {
        opts.endpoint_override = e;
    }
    if (const auto s = ctx.param_or("scheme", ""); !s.empty()) {
        opts.scheme = s;
    }
    if (const auto p = ctx.param_or("project_id", ""); !p.empty()) {
        opts.project_id = p;
    }
    if (const auto r = ctx.param_int64_or("retry_limit_seconds", 0); r > 0) {
        opts.retry_limit_seconds = static_cast<double>(r);
    }
    if (opts.bucket.empty() || opts.key.empty()) {
        throw std::runtime_error("gcs_parquet: 'bucket' and 'key' are required");
    }
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // ---- Parquet over GCS (sink + source pairs, int64 + string channels) ----
    // Ride Arrow's GcsFileSystem. Auth: anonymous='true' (emulator / public bucket), an explicit
    // access_token, or Application Default Credentials (default). endpoint_override + scheme=http
    // target a fake-gcs-server emulator. Path = bucket + "/" + key.

    reg.register_sink<std::int64_t>(
        "gcs_parquet_int64_sink",
        [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::int64_t>> {
            ParquetGcsSink<std::int64_t>::Options opts;
            apply_gcs_params(ctx, opts);
            if (ctx.parallelism > 1) {
                opts.key += "." + std::to_string(ctx.subtask_idx) + ".parquet";
            }
            return std::make_shared<ParquetGcsSink<std::int64_t>>(std::move(opts),
                                                                  int64_arrow_batcher());
        });

    reg.register_sink<std::string>(
        "gcs_parquet_string_sink",
        [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            ParquetGcsSink<std::string>::Options opts;
            apply_gcs_params(ctx, opts);
            if (ctx.parallelism > 1) {
                opts.key += "." + std::to_string(ctx.subtask_idx) + ".parquet";
            }
            return std::make_shared<ParquetGcsSink<std::string>>(std::move(opts),
                                                                 string_arrow_batcher());
        });

    reg.register_source<std::int64_t>(
        "gcs_parquet_int64_source",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<std::int64_t>> {
            ParquetGcsSource<std::int64_t>::Options opts;
            apply_gcs_params(ctx, opts);
            return std::make_shared<ParquetGcsSource<std::int64_t>>(std::move(opts),
                                                                    int64_arrow_batcher());
        });

    reg.register_source<std::string>(
        "gcs_parquet_string_source",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            ParquetGcsSource<std::string>::Options opts;
            apply_gcs_params(ctx, opts);
            return std::make_shared<ParquetGcsSource<std::string>>(std::move(opts),
                                                                   string_arrow_batcher());
        });
}

}  // namespace clink::gcs
