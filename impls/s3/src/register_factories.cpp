// S3 factory registration.

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

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

    reg.register_source<std::int64_t>(
        "s3_parquet_int64_source",
        [parquet_s3_options](const BuildContext& ctx) -> std::shared_ptr<Source<std::int64_t>> {
            auto [bucket, key, region, endpoint] =
                parquet_s3_options(ctx, "s3_parquet_int64_source");
            ParquetS3Source<std::int64_t>::Options opts;
            opts.bucket = bucket;
            opts.key = key;
            if (!region.empty())
                opts.region = region;
            if (!endpoint.empty())
                opts.endpoint_override = endpoint;
            return std::make_shared<ParquetS3Source<std::int64_t>>(std::move(opts),
                                                                   int64_arrow_batcher());
        });

    reg.register_source<std::string>(
        "s3_parquet_string_source",
        [parquet_s3_options](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            auto [bucket, key, region, endpoint] =
                parquet_s3_options(ctx, "s3_parquet_string_source");
            ParquetS3Source<std::string>::Options opts;
            opts.bucket = bucket;
            opts.key = key;
            if (!region.empty())
                opts.region = region;
            if (!endpoint.empty())
                opts.endpoint_override = endpoint;
            return std::make_shared<ParquetS3Source<std::string>>(std::move(opts),
                                                                  string_arrow_batcher());
        });
}

}  // namespace clink::s3
