// GCS Parquet factory registration (gcs_parquet_{int64,string}_{sink,source}).

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

#include <arrow/filesystem/filesystem.h>

#include "clink/connectors/multi_object_parquet_source.hpp"
#include "clink/connectors/parquet_fs_2pc_sink.hpp"
#include "clink/connectors/parquet_gcs_sink.hpp"
#include "clink/connectors/parquet_gcs_source.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/gcs/install.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::gcs {

namespace {

// Resolve a service-account key JSON for the auto-refreshing credential mode: `credentials_json`
// inline takes precedence, else `credentials_file` is read from disk. Returns nullopt when neither
// is set (the caller then falls back to a static access_token or Application Default Credentials).
inline std::optional<std::string> gcs_credentials_from(const clink::plugin::BuildContext& ctx) {
    if (const auto j = ctx.param_or("credentials_json", ""); !j.empty()) {
        return j;
    }
    if (const auto f = ctx.param_or("credentials_file", ""); !f.empty()) {
        std::ifstream in(f, std::ios::binary);
        if (!in) {
            throw std::runtime_error("gcs_parquet: cannot read credentials_file '" + f + "'");
        }
        std::ostringstream ss;
        ss << in.rdbuf();
        return ss.str();
    }
    return std::nullopt;
}

// Apply the shared GCS auth/endpoint params onto an Options struct (sink or source share these).
template <typename Opts>
void apply_gcs_params(const clink::plugin::BuildContext& ctx, Opts& opts) {
    opts.bucket = ctx.param_or("bucket");
    opts.key = ctx.param_or("key");
    opts.anonymous = ctx.param_or("anonymous", "false") == "true";
    if (const auto t = ctx.param_or("access_token", ""); !t.empty()) {
        opts.access_token = t;
    }
    opts.credentials_json = gcs_credentials_from(ctx);
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

// Register a GCS Parquet source: a single `key` reads one object; a `prefix` reads every
// matching object beneath it, sharded across subtasks via the shared MultiObjectParquetSource.
// The GcsFileSystem is built on the runner thread in open() via the captured options, reusing
// gcs_detail::make_gcs_options (the same call the single-object source uses).
template <typename T>
void register_gcs_parquet_source(clink::plugin::PluginRegistry& reg,
                                 std::string name,
                                 ArrowBatcher<T> batcher) {
    reg.register_source<T>(
        name,
        [name, batcher](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<Source<T>> {
            const auto bucket = ctx.param_or("bucket");
            if (bucket.empty()) {
                throw std::runtime_error(name + ": 'bucket' is required");
            }
            // Auth/endpoint params shared by the single- and multi-object paths.
            typename ParquetGcsSource<T>::Options opts;
            opts.anonymous = ctx.param_or("anonymous", "false") == "true";
            if (const auto t = ctx.param_or("access_token", ""); !t.empty()) {
                opts.access_token = t;
            }
            opts.credentials_json = gcs_credentials_from(ctx);
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

            // Multi-object: read every object under `prefix`, sharded by subtask.
            if (const auto prefix = ctx.param_or("prefix", ""); !prefix.empty()) {
                auto fs_factory = [opts, name]() -> std::shared_ptr<arrow::fs::FileSystem> {
                    auto gcs_opts = clink::gcs_detail::make_gcs_options(opts.anonymous,
                                                                        opts.access_token,
                                                                        opts.endpoint_override,
                                                                        opts.scheme,
                                                                        opts.project_id,
                                                                        opts.retry_limit_seconds,
                                                                        opts.credentials_json);
                    auto r = arrow::fs::GcsFileSystem::Make(gcs_opts);
                    if (!r.ok()) {
                        throw std::runtime_error(name +
                                                 ": GcsFileSystem::Make: " + r.status().ToString());
                    }
                    return *r;
                };
                typename MultiObjectParquetSource<T>::Options mo;
                mo.prefix = bucket + "/" + prefix;
                mo.subtask_idx = static_cast<int>(ctx.subtask_idx);
                mo.parallelism = static_cast<int>(ctx.parallelism);
                mo.recursive = ctx.param_or("recursive", "true") == "true";
                mo.suffix = ctx.param_or("suffix", ".parquet");
                return std::make_shared<MultiObjectParquetSource<T>>(
                    std::move(fs_factory), std::move(mo), batcher);
            }

            // Single object.
            opts.bucket = bucket;
            opts.key = ctx.param_or("key");
            if (opts.key.empty()) {
                throw std::runtime_error(name + ": 'key' or 'prefix' is required");
            }
            return std::make_shared<ParquetGcsSource<T>>(std::move(opts), batcher);
        });
}

// Register an exactly-once GCS Parquet sink (ParquetFsSink2PC): stages one file per checkpoint
// interval under <bucket>/<prefix>/staging and promotes it to <bucket>/<prefix>/committed on
// commit. The GcsFileSystem is built on the runner thread via gcs_detail::make_gcs_options (the
// same call the source uses). Read the result with gcs_parquet source on <prefix>/committed.
template <typename T>
void register_gcs_parquet_2pc_sink(clink::plugin::PluginRegistry& reg,
                                   std::string name,
                                   ArrowBatcher<T> batcher) {
    reg.register_sink<T>(
        name, [name, batcher](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<Sink<T>> {
            const auto bucket = ctx.param_or("bucket");
            const auto prefix = ctx.param_or("prefix");
            if (bucket.empty() || prefix.empty()) {
                throw std::runtime_error(name + ": 'bucket' and 'prefix' are required");
            }
            // Hold the auth/endpoint fields in the source Options so make_gcs_options gets the
            // exact field types the single-object source passes.
            typename ParquetGcsSource<T>::Options opts;
            opts.anonymous = ctx.param_or("anonymous", "false") == "true";
            if (const auto t = ctx.param_or("access_token", ""); !t.empty()) {
                opts.access_token = t;
            }
            opts.credentials_json = gcs_credentials_from(ctx);
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
            auto fs_factory = [opts, name]() -> std::shared_ptr<arrow::fs::FileSystem> {
                auto gcs_opts = clink::gcs_detail::make_gcs_options(opts.anonymous,
                                                                    opts.access_token,
                                                                    opts.endpoint_override,
                                                                    opts.scheme,
                                                                    opts.project_id,
                                                                    opts.retry_limit_seconds,
                                                                    opts.credentials_json);
                auto r = arrow::fs::GcsFileSystem::Make(gcs_opts);
                if (!r.ok()) {
                    throw std::runtime_error(name +
                                             ": GcsFileSystem::Make: " + r.status().ToString());
                }
                return *r;
            };
            typename ParquetFsSink2PC<T>::Options o;
            o.base = bucket + "/" + prefix;
            o.subtask_idx = static_cast<int>(ctx.subtask_idx);
            return std::make_shared<ParquetFsSink2PC<T>>(
                std::move(fs_factory), std::move(o), batcher);
        });
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

    register_gcs_parquet_source<std::int64_t>(
        reg, "gcs_parquet_int64_source", int64_arrow_batcher());
    register_gcs_parquet_source<std::string>(
        reg, "gcs_parquet_string_source", string_arrow_batcher());
    register_gcs_parquet_2pc_sink<std::int64_t>(
        reg, "gcs_parquet_2pc_int64_sink", int64_arrow_batcher());
    register_gcs_parquet_2pc_sink<std::string>(
        reg, "gcs_parquet_2pc_string_sink", string_arrow_batcher());
}

}  // namespace clink::gcs
