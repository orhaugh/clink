// Typed S3 Parquet sink helper. The existing `s3_text_sink` /
// `s3_parquet_int64_sink` / `s3_parquet_string_sink` factories cover
// the built-in channel types; this free function lets a user wire a
// `ParquetS3Sink<T>` for ANY registered channel type T by supplying a
// `Codec<T>` (the default Arrow batcher wraps codec bytes into a
// single `value_bytes:binary` column; users that want a typed Arrow
// schema can supply their own `ArrowBatcher<T>` instead).
//
// Usage:
//   clink::s3::parquet_sink<MyRecord>(
//       my_stream,
//       clink::s3::ParquetSinkOptions{
//           .bucket = "my-bucket",
//           .key = "2026/05/17/0001.parquet",
//           .endpoint_override = "http://minio:9000",
//       },
//       my_record_codec());  // Codec<MyRecord>, default binary framing
//
// Internally: registers a typed `ParquetS3Sink<T>` factory under a
// minted op_type via PluginRegistry::register_sink<T>, then appends a
// SinkDescriptor referencing that op_type on the stream's env. Same
// pattern as the M6 typed Kafka / ClickHouse helpers.

#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "clink/api/descriptors.hpp"
#include "clink/api/stream_execution_environment.hpp"
#include "clink/connectors/parquet_s3_sink.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/columnar_batcher.hpp"  // make_auto_arrow_batcher
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::s3 {

struct ParquetSinkOptions {
    std::string bucket;
    std::string key;                // e.g. "events/2026/05/17/0001.parquet"
    std::string region;             // empty = default chain
    std::string endpoint_override;  // empty = AWS public endpoint
};

namespace detail {

inline typename ParquetS3Sink<int>::Options make_parquet_options_template(
    const ParquetSinkOptions& src) {
    // Reused by all per-T instantiations - same option struct shape
    // for every T (ParquetS3Sink<T>::Options is just a typedef of the
    // bucket / key / region / endpoint trio).
    typename ParquetS3Sink<int>::Options dummy;
    dummy.bucket = src.bucket;
    dummy.key = src.key;
    if (!src.region.empty())
        dummy.region = src.region;
    if (!src.endpoint_override.empty())
        dummy.endpoint_override = src.endpoint_override;
    return dummy;
}

}  // namespace detail

template <typename T>
inline void parquet_sink(clink::api::DataStream<T> stream,
                         const ParquetSinkOptions& opts,
                         clink::Codec<T> codec,
                         std::string id = {},
                         std::function<std::string(const T&)> bucket_assigner = {}) {
    auto* env = stream.env();
    auto& reg = env->registry();
    const std::string op_type = env->mint_inline_op_type("s3_parquet_typed_sink");
    // Auto-select: a CLINK_ARROW_FIELDS-described T gets typed columnar Parquet
    // columns; anything else keeps the binary-fallback layout.
    auto batcher = clink::make_auto_arrow_batcher<T>(std::move(codec));
    reg.template register_sink<T>(
        op_type,
        [opts, batcher, bucket_assigner](
            const clink::plugin::BuildContext& ctx) -> std::shared_ptr<clink::Sink<T>> {
            typename clink::ParquetS3Sink<T>::Options o;
            o.bucket = opts.bucket;
            o.key = opts.key;
            if (!opts.region.empty()) {
                o.region = opts.region;
            }
            if (!opts.endpoint_override.empty()) {
                o.endpoint_override = opts.endpoint_override;
            }
            if (bucket_assigner) {
                // Wrap the user's per-record assigner so each subtask
                // (parallelism > 1) gets a distinct object even when
                // the assigner's output collides. Append the subtask
                // index before the trailing extension if present, else
                // suffix it.
                const std::uint32_t subtask_idx = ctx.subtask_idx;
                const std::uint32_t parallelism = ctx.parallelism;
                o.bucket_assigner = [assigner = bucket_assigner, subtask_idx, parallelism](
                                        const T& v) -> std::string {
                    auto k = assigner(v);
                    if (parallelism <= 1) {
                        return k;
                    }
                    const auto dot = k.find_last_of('.');
                    const auto slash = k.find_last_of('/');
                    if (dot != std::string::npos && (slash == std::string::npos || dot > slash)) {
                        return k.substr(0, dot) + "." + std::to_string(subtask_idx) + k.substr(dot);
                    }
                    return k + "." + std::to_string(subtask_idx);
                };
            } else if (ctx.parallelism > 1) {
                // Per-subtask key suffix when running parallelism > 1, so
                // each subtask writes to a distinct object. Mirrors the
                // `s3_parquet_int64_sink` / `s3_parquet_string_sink`
                // behaviour at register_factories.cpp.
                o.key += "." + std::to_string(ctx.subtask_idx) + ".parquet";
            }
            return std::make_shared<clink::ParquetS3Sink<T>>(std::move(o), batcher);
        });
    clink::api::SinkDescriptor desc;
    desc.op_type = op_type;
    desc.channel_type = clink::api::ChannelName<T>::get();
    stream.sink(std::move(desc), std::move(id));
}

}  // namespace clink::s3
