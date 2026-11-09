// S3 factory registration.

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>

#include "clink/connectors/capability.hpp"
#include "clink/connectors/multi_object_parquet_source.hpp"
#include "clink/connectors/parquet_fs_2pc_sink.hpp"
#include "clink/connectors/parquet_rolling_sink.hpp"
#include "clink/connectors/parquet_s3_sink.hpp"
#include "clink/connectors/parquet_s3_source.hpp"
#include "clink/connectors/s3_sink.hpp"
#include "clink/connectors/s3_sink_2pc.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/s3/install.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_channel.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace clink::s3 {

namespace {

// How a factory gets its Arrow batcher: one fixed batcher for the int64 and
// string channels, and for the Row channel one built for each table from its
// declared columns, so a SQL table's rows are written and read as typed
// Parquet columns, one per column, as the local parquet connector does. A scan
// the optimizer narrowed reads only its projected columns.
template <typename T>
using BatcherFor = std::function<ArrowBatcher<T>(const clink::plugin::BuildContext&)>;

template <typename T>
BatcherFor<T> fixed_batcher(ArrowBatcher<T> batcher) {
    return [batcher = std::move(batcher)](const clink::plugin::BuildContext&) { return batcher; };
}

ArrowBatcher<clink::sql::Row> row_batcher(const clink::plugin::BuildContext& ctx) {
    return clink::sql::make_row_columnar_arrow_batcher(clink::sql::projected_row_columns(
        clink::sql::parse_row_schema(ctx.param_or("schema_columns")),
        ctx.param_or("projected_columns")));
}

// A raw object sink's byte count: the whole value must be a positive integer.
// param_int64_or fell back to the default on garbage, so a typo passed
// unnoticed, and its result was cast to size_t, so rollover_bytes='0' put an
// object for every record. Any positive count is taken, as before.
std::size_t positive_bytes(const clink::plugin::BuildContext& ctx,
                           const std::string& who,
                           const std::string& key,
                           std::size_t fallback) {
    const auto it = ctx.params.find(key);
    if (it == ctx.params.end()) {
        return fallback;
    }
    const std::string value = clink::plugin::BuildContext::resolve_secret(it->second);
    std::uint64_t parsed = 0;
    const char* first = value.data();
    const char* last = first + value.size();
    const auto [end, ec] = std::from_chars(first, last, parsed);
    if (value.empty() || end != last || ec != std::errc{} || parsed == 0) {
        throw std::runtime_error(who + ": " + key + " must be a positive integer (got '" +
                                 it->second + "')");
    }
    return static_cast<std::size_t>(parsed);
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    // The raw text sink (s3_text_sink) and the C++ S3Source. The record before
    // this one still carried the Parquet modes' format, schema evolution and
    // partition discovery, which have records of their own.
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "s3",
        .version = "2",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {"aws-sdk-cpp", "arrow"},
        .runtime_dependencies = {"s3-compatible object store"},
        .formats = {"text"},
        .boundedness = clink::connectors::Boundedness::Bounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = false,
        .delivery = clink::connectors::DeliveryGuarantee::AtMostOnce,
        .transactional = false,
        .schema_evolution = false,
        .partition_discovery = false,
        .auth_methods = {"env-credentials", "instance-profile", "static-key"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {},
        .available_in_sql = true,
        .limitations = {"the text sink puts a part only at rollover_bytes and at the end of "
                        "input, not at a checkpoint: records buffered when a process dies are "
                        "lost, and parts put after the last checkpoint are put again after a "
                        "restore; use s3_2pc for output tied to checkpoint completion",
                        "the C++ source emits each object whole, as one batch held in memory, "
                        "and resumes a restore after the last object its checkpoint covered, by "
                        "key: an object added since with a key that sorts before that one is "
                        "not read"},
    });

    // The Parquet sinks over S3, which the "s3" record above (the at-most-once
    // text sink) used to describe as well, since their op types begin with
    // "s3": the rolling sink was reported at-most-once, and a job asking for
    // exactly-once through the 2PC sink was refused.
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "s3_parquet",
        .version = "1",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {"aws-sdk-cpp", "arrow (S3FileSystem)"},
        .runtime_dependencies = {"s3-compatible object store"},
        .formats = {"parquet"},
        .boundedness = clink::connectors::Boundedness::Bounded,
        .replayable = true,
        .offset_model = clink::connectors::OffsetModel::FileOffset,
        .checkpoint_integrated = true,
        // ParquetRollingSink: one complete part per checkpoint interval,
        // published when its upload completes.
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"env-credentials", "instance-profile", "static-key"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {},
        .available_in_sql = true,
        .limitations = {"plain sink is at-least-once; use s3_parquet_2pc for exactly-once",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "s3_parquet_2pc",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"aws-sdk-cpp", "arrow (S3FileSystem)"},
        .runtime_dependencies = {"s3-compatible object store"},
        .formats = {"parquet"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        // One staged file per checkpoint interval, promoted from
        // <prefix>/staging to <prefix>/committed on the commit broadcast.
        .delivery = clink::connectors::DeliveryGuarantee::ExactlyOnceAtomicPublish,
        .transactional = true,
        .auth_methods = {"env-credentials", "instance-profile", "static-key"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {},
        .available_in_sql = true,
        .limitations = {"a crash can leave orphaned staging objects; expire <prefix>/staging",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
        .required_options_for_exactly_once = {"bucket", "prefix"},
    });

    // s3_sink_2pc.hpp: parts are uploaded during the epoch, the multipart
    // handle is the committable, and the object does not exist until
    // CompleteMultipartUpload on the commit broadcast. Atomic publication
    // rather than a distributed transaction.
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "s3_2pc",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"aws-sdk-cpp"},
        .runtime_dependencies = {"s3-compatible object store with multipart upload"},
        .formats = {"bytes", "text", "parquet"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::ExactlyOnceAtomicPublish,
        .transactional = true,
        .schema_evolution = false,
        .partition_discovery = false,
        .auth_methods = {"env-credentials", "instance-profile", "static-key"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {},
        .available_in_sql = true,
        .limitations = {"an abandoned multipart upload keeps consuming storage until an "
                        "expiry lifecycle rule removes it - configure one",
                        "S3 requires every part except the last to be at least 5 MiB",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
        .required_options_for_exactly_once = {"bucket"},
    });

    using clink::plugin::BuildContext;

    // s3_text_sink: writes batches of string records to S3 objects under
    // bucket/key_prefix. Object rollover at rollover_bytes. params:
    //   bucket (required), key_prefix (default "")
    //   region (optional), endpoint_override (optional, for localstack)
    //   rollover_bytes (default 16 MiB, any positive integer)
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
                positive_bytes(ctx, "s3_text_sink", "rollover_bytes", 16 * 1024 * 1024);
            opts.subtask_idx = ctx.subtask_idx;
            opts.parallelism = ctx.parallelism == 0 ? 1 : ctx.parallelism;
            if (opts.bucket.empty()) {
                throw std::runtime_error("s3_text_sink: 'bucket' is required");
            }
            return std::make_shared<S3Sink>(std::move(opts));
        });

    // s3_2pc_string_sink: EXACTLY-ONCE raw-object S3 sink via multipart-upload-
    // complete-on-commit. Selected for delivery_guarantee='exactly_once'. One
    // object per (subtask, run, checkpoint) under <key_prefix>/sub<N>-<run>-<ckpt>.ndjson,
    // made visible atomically by CompleteMultipartUpload once the checkpoint is
    // globally durable. params: bucket (required), key_prefix, region,
    // endpoint_override (MinIO/LocalStack), part_size (default 5 MiB, any
    // positive integer; the sink raises a smaller one to S3's 5 MiB minimum).
    reg.register_sink<std::string>(
        "s3_2pc_string_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            S3Sink2PC::Options opts;
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
            opts.subtask_idx = ctx.subtask_idx;
            opts.part_size =
                positive_bytes(ctx, "s3_2pc_string_sink", "part_size", 5 * 1024 * 1024);
            if (opts.bucket.empty()) {
                throw std::runtime_error("s3_2pc_string_sink: 'bucket' is required");
            }
            auto sink = std::make_shared<S3Sink2PC>(std::move(opts));
            if (auto cg = ctx.param_or("commit_group", ""); !cg.empty()) {
                sink->set_commit_group(cg);
            }
            return sink;
        });

    // ---- Parquet over S3 (sink + source pair) ----
    //
    // These ride Arrow's S3FileSystem rather than the AWS-SDK-based
    // S3Sink, so they don't depend on the impls/s3 AWS-SDK glue.
    // Credentials resolve via the standard chain (env vars, instance
    // profile, ~/.aws/credentials). `endpoint_override` is for
    // localstack / MinIO testing. A sink writes a directory of parts under
    // `bucket` + `/` + `prefix` (or `key`); a source reads one object (`key`)
    // or every object under a `prefix`.

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
                                       const std::string& factory_name, BatcherFor<T> batcher_for) {
        reg.register_source<T>(
            factory_name,
            [factory_name, make_s3_fs_factory, batcher_for](
                const BuildContext& ctx) -> std::shared_ptr<Source<T>> {
                const auto batcher = batcher_for(ctx);
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
                // One object does not divide between subtasks (a prefix does).
                return clink::read_on_first_subtask<T>(
                    ctx.subtask_idx,
                    std::make_shared<ParquetS3Source<T>>(std::move(opts), batcher));
            });
    };

    register_parquet_source.template operator()<std::int64_t>("s3_parquet_int64_source",
                                                              fixed_batcher(int64_arrow_batcher()));
    register_parquet_source.template operator()<std::string>("s3_parquet_string_source",
                                                             fixed_batcher(string_arrow_batcher()));

    // Exactly-once Parquet sink over S3: stages one file per checkpoint interval under
    // <bucket>/<prefix>/staging and promotes it to <bucket>/<prefix>/committed on commit
    // (ParquetFsSink2PC). Read the result with s3_parquet source pointed at <prefix>/committed.
    auto register_parquet_2pc_sink = [&reg, make_s3_fs_factory]<typename T>(
                                         const std::string& factory_name,
                                         BatcherFor<T> batcher_for) {
        reg.register_sink<T>(
            factory_name,
            [factory_name, make_s3_fs_factory, batcher_for](
                const BuildContext& ctx) -> std::shared_ptr<Sink<T>> {
                const auto batcher = batcher_for(ctx);
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

    // At-least-once Parquet sink over S3: one complete object per subtask per checkpoint
    // interval under <bucket>/<prefix> (ParquetRollingSink). `key` is accepted as the prefix.
    // Read the result with s3_parquet source on the same prefix.
    auto register_parquet_rolling_sink = [&reg, make_s3_fs_factory]<typename T>(
                                             const std::string& factory_name,
                                             BatcherFor<T> batcher_for) {
        reg.register_sink<T>(
            factory_name,
            [factory_name, make_s3_fs_factory, batcher_for](
                const BuildContext& ctx) -> std::shared_ptr<Sink<T>> {
                const auto batcher = batcher_for(ctx);
                const auto bucket = ctx.param_or("bucket");
                auto prefix = ctx.param_or("prefix", "");
                if (prefix.empty()) {
                    prefix = ctx.param_or("key", "");
                }
                if (bucket.empty() || prefix.empty()) {
                    throw std::runtime_error(factory_name +
                                             ": 'bucket' and 'prefix' (or 'key') are required");
                }
                typename ParquetRollingSink<T>::Options o;
                o.dir = bucket + "/" + prefix;
                o.subtask_idx = ctx.subtask_idx;
                o.parallelism = ctx.parallelism;
                return std::make_shared<ParquetRollingSink<T>>(
                    make_s3_fs_factory(ctx.param_or("region", ""),
                                       ctx.param_or("endpoint_override", ""),
                                       ctx.param_or("anonymous", "false") == "true",
                                       factory_name),
                    std::move(o),
                    batcher,
                    factory_name);
            });
    };
    register_parquet_rolling_sink.template operator()<std::int64_t>(
        "s3_parquet_int64_sink", fixed_batcher(int64_arrow_batcher()));
    register_parquet_rolling_sink.template operator()<std::string>(
        "s3_parquet_string_sink", fixed_batcher(string_arrow_batcher()));

    register_parquet_2pc_sink.template operator()<std::int64_t>(
        "s3_parquet_2pc_int64_sink", fixed_batcher(int64_arrow_batcher()));
    register_parquet_2pc_sink.template operator()<std::string>(
        "s3_parquet_2pc_string_sink", fixed_batcher(string_arrow_batcher()));

    // The Row channel: a SQL table's declared columns as typed Parquet columns.
    // Its factories need the Row type registered first.
    clink::sql::register_row_channel(reg);
    register_parquet_source.template operator()<clink::sql::Row>(
        "s3_parquet_row_source", BatcherFor<clink::sql::Row>{row_batcher});
    register_parquet_rolling_sink.template operator()<clink::sql::Row>(
        "s3_parquet_row_sink", BatcherFor<clink::sql::Row>{row_batcher});
    register_parquet_2pc_sink.template operator()<clink::sql::Row>(
        "s3_parquet_2pc_row_sink", BatcherFor<clink::sql::Row>{row_batcher});
}

}  // namespace clink::s3
