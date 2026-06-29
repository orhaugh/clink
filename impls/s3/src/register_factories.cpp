// S3 factory registration.

#include <cstddef>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>

#include "clink/connectors/multi_object_parquet_source.hpp"
#include "clink/connectors/parquet_fs_2pc_sink.hpp"
#include "clink/connectors/parquet_s3_sink.hpp"
#include "clink/connectors/parquet_s3_source.hpp"
#include "clink/connectors/s3_sink.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/s3/install.hpp"

namespace clink::s3 {

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // s3_text_sink: writes batches of string records to S3 objects under
    // bucket/key_prefix. Object rollover at rollover_bytes. params:
    //   bucket (required), key_prefix (default "")
    //   region (optional), endpoint_override (optional, for localstack)
    //   rollover_bytes (default 16 MiB)
    reg.register_sink<std::string>(
        "s3_text_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            S3Sink::Options opts;
            opts.bucket = ctx.param_or("bucket");
            opts.key_prefix = ctx.param_or("key_prefix", "");
            const auto region = ctx.param_or("region", "");
            if (!region.empty()) {
                opts.region = region;
            }
            const auto endpoint = ctx.param_or("endpoint_override", "");
            if (!endpoint.empty()) {
                opts.endpoint_override = endpoint;
            }
            opts.rollover_bytes =
                static_cast<std::size_t>(ctx.param_int64_or("rollover_bytes", 16 * 1024 * 1024));
            if (opts.bucket.empty()) {
                throw std::runtime_error("s3_text_sink: 'bucket' is required");
            }
            return std::make_shared<S3Sink>(std::move(opts));
        });

    // ---- Parquet over S3 (sink + source pair) ----
    //
    // These ride Arrow's S3FileSystem rather than the AWS-SDK-based
    // S3Sink, so they don't depend on the impls/s3 AWS-SDK glue.
    // Credentials resolve via the standard chain (env vars, instance
    // profile, ~/.aws/credentials). `endpoint_override` is for
    // localstack / MinIO testing. Path = `bucket` + `/` + `key`.
    auto parquet_s3_options = [](const BuildContext& ctx, const std::string& op_label) {
        const auto bucket = ctx.param_or("bucket");
        const auto key = ctx.param_or("key");
        if (bucket.empty() || key.empty()) {
            throw std::runtime_error(op_label + ": 'bucket' and 'key' are required");
        }
        return std::make_tuple(
            bucket, key, ctx.param_or("region", ""), ctx.param_or("endpoint_override", ""));
    };

    reg.register_sink<std::int64_t>(
        "s3_parquet_int64_sink",
        [parquet_s3_options](const BuildContext& ctx) -> std::shared_ptr<Sink<std::int64_t>> {
            auto [bucket, key, region, endpoint] = parquet_s3_options(ctx, "s3_parquet_int64_sink");
            ParquetS3Sink<std::int64_t>::Options opts;
            opts.bucket = bucket;
            opts.key = key;
            if (!region.empty())
                opts.region = region;
            if (!endpoint.empty())
                opts.endpoint_override = endpoint;
            if (ctx.parallelism > 1) {
                opts.key += "." + std::to_string(ctx.subtask_idx) + ".parquet";
            }
            return std::make_shared<ParquetS3Sink<std::int64_t>>(std::move(opts),
                                                                 int64_arrow_batcher());
        });

    reg.register_sink<std::string>(
        "s3_parquet_string_sink",
        [parquet_s3_options](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            auto [bucket, key, region, endpoint] =
                parquet_s3_options(ctx, "s3_parquet_string_sink");
            ParquetS3Sink<std::string>::Options opts;
            opts.bucket = bucket;
            opts.key = key;
            if (!region.empty())
                opts.region = region;
            if (!endpoint.empty())
                opts.endpoint_override = endpoint;
            if (ctx.parallelism > 1) {
                opts.key += "." + std::to_string(ctx.subtask_idx) + ".parquet";
            }
            return std::make_shared<ParquetS3Sink<std::string>>(std::move(opts),
                                                                string_arrow_batcher());
        });

    // Parquet S3 source. A single `key` reads one object; a `prefix` reads every
    // matching object under it, sharded across subtasks (MultiObjectParquetSource).
    // The S3FileSystem is built on the runner thread in open() via this factory.
    auto make_s3_fs_factory =
        [](std::string region,
           std::string endpoint,
           bool anon,
           std::string op_label) -> MultiObjectParquetSource<std::int64_t>::FileSystemFactory {
        return [region = std::move(region),
                endpoint = std::move(endpoint),
                anon,
                op_label = std::move(op_label)]() -> std::shared_ptr<arrow::fs::FileSystem> {
            clink::detail::ensure_arrow_s3_initialised();
            auto o = arrow::fs::S3Options::Defaults();
            if (!region.empty()) {
                o.region = region;
            }
            if (!endpoint.empty()) {
                o.endpoint_override = endpoint;
                o.scheme = "http";
            }
            if (anon) {
                o.ConfigureAnonymousCredentials();
            }
            auto r = arrow::fs::S3FileSystem::Make(o);
            if (!r.ok()) {
                throw std::runtime_error(op_label +
                                         ": S3FileSystem::Make: " + r.status().ToString());
            }
            return *r;
        };
    };

    auto register_parquet_source = [&reg, make_s3_fs_factory]<typename T>(
                                       const std::string& factory_name, ArrowBatcher<T> batcher) {
        reg.register_source<T>(
            factory_name,
            [factory_name, make_s3_fs_factory, batcher](
                const BuildContext& ctx) -> std::shared_ptr<Source<T>> {
                const auto bucket = ctx.param_or("bucket");
                if (bucket.empty()) {
                    throw std::runtime_error(factory_name + ": 'bucket' is required");
                }
                const auto region = ctx.param_or("region", "");
                const auto endpoint = ctx.param_or("endpoint_override", "");
                const bool anon = ctx.param_or("anonymous", "false") == "true";

                // Multi-object: read every object under `prefix`, sharded by subtask.
                if (const auto prefix = ctx.param_or("prefix", ""); !prefix.empty()) {
                    typename MultiObjectParquetSource<T>::Options mo;
                    mo.prefix = bucket + "/" + prefix;
                    mo.subtask_idx = static_cast<int>(ctx.subtask_idx);
                    mo.parallelism = static_cast<int>(ctx.parallelism);
                    mo.recursive = ctx.param_or("recursive", "true") == "true";
                    mo.suffix = ctx.param_or("suffix", ".parquet");
                    return std::make_shared<MultiObjectParquetSource<T>>(
                        make_s3_fs_factory(region, endpoint, anon, factory_name),
                        std::move(mo),
                        batcher);
                }

                // Single object.
                const auto key = ctx.param_or("key", "");
                if (key.empty()) {
                    throw std::runtime_error(factory_name + ": 'key' or 'prefix' is required");
                }
                typename ParquetS3Source<T>::Options opts;
                opts.bucket = bucket;
                opts.key = key;
                if (!region.empty()) {
                    opts.region = region;
                }
                if (!endpoint.empty()) {
                    opts.endpoint_override = endpoint;
                }
                opts.allow_anonymous = anon;
                return std::make_shared<ParquetS3Source<T>>(std::move(opts), batcher);
            });
    };

    register_parquet_source.template operator()<std::int64_t>("s3_parquet_int64_source",
                                                              int64_arrow_batcher());
    register_parquet_source.template operator()<std::string>("s3_parquet_string_source",
                                                             string_arrow_batcher());

    // Exactly-once Parquet sink over S3: stages one file per checkpoint interval under
    // <bucket>/<prefix>/staging and promotes it to <bucket>/<prefix>/committed on commit
    // (ParquetFsSink2PC). Read the result with s3_parquet source pointed at <prefix>/committed.
    auto register_parquet_2pc_sink = [&reg, make_s3_fs_factory]<typename T>(
                                         const std::string& factory_name, ArrowBatcher<T> batcher) {
        reg.register_sink<T>(
            factory_name,
            [factory_name, make_s3_fs_factory, batcher](
                const BuildContext& ctx) -> std::shared_ptr<Sink<T>> {
                const auto bucket = ctx.param_or("bucket");
                const auto prefix = ctx.param_or("prefix");
                if (bucket.empty() || prefix.empty()) {
                    throw std::runtime_error(factory_name + ": 'bucket' and 'prefix' are required");
                }
                typename ParquetFsSink2PC<T>::Options o;
                o.base = bucket + "/" + prefix;
                o.subtask_idx = static_cast<int>(ctx.subtask_idx);
                return std::make_shared<ParquetFsSink2PC<T>>(
                    make_s3_fs_factory(ctx.param_or("region", ""),
                                       ctx.param_or("endpoint_override", ""),
                                       ctx.param_or("anonymous", "false") == "true",
                                       factory_name),
                    std::move(o),
                    batcher);
            });
    };

    register_parquet_2pc_sink.template operator()<std::int64_t>("s3_parquet_2pc_int64_sink",
                                                                int64_arrow_batcher());
    register_parquet_2pc_sink.template operator()<std::string>("s3_parquet_2pc_string_sink",
                                                               string_arrow_batcher());
}

}  // namespace clink::s3
