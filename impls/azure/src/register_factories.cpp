// Azure Blob Parquet factory registration (azure_parquet_{int64,string}_{sink,source}).

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include <arrow/filesystem/filesystem.h>

#include "clink/azure/install.hpp"
#include "clink/connectors/multi_object_parquet_source.hpp"
#include "clink/connectors/parquet_azure_sink.hpp"
#include "clink/connectors/parquet_azure_source.hpp"
#include "clink/connectors/parquet_fs_2pc_sink.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::azure {

namespace {

// Apply the shared Azure auth/endpoint params onto an Options struct (sink + source share these).
template <typename Opts>
void apply_azure_params(const clink::plugin::BuildContext& ctx, Opts& opts) {
    opts.container = ctx.param_or("container");
    opts.key = ctx.param_or("key");
    opts.account_name = ctx.param_or("account_name");
    opts.anonymous = ctx.param_or("anonymous", "false") == "true";
    opts.use_default_credential = ctx.param_or("use_default_credential", "false") == "true";
    if (const auto k = ctx.param_or("account_key", ""); !k.empty()) {
        opts.account_key = k;
    }
    if (const auto s = ctx.param_or("sas_token", ""); !s.empty()) {
        opts.sas_token = s;
    }
    if (const auto a = ctx.param_or("blob_storage_authority", ""); !a.empty()) {
        opts.blob_storage_authority = a;
    }
    if (const auto s = ctx.param_or("blob_storage_scheme", ""); !s.empty()) {
        opts.blob_storage_scheme = s;
    }
    if (opts.container.empty() || opts.key.empty() || opts.account_name.empty()) {
        throw std::runtime_error(
            "azure_parquet: 'container', 'key' and 'account_name' are required");
    }
}

// Register an Azure Blob Parquet source: a single `key` reads one object; a `prefix` reads every
// matching object beneath it, sharded across subtasks via the shared MultiObjectParquetSource.
// The AzureFileSystem is built on the runner thread in open() via the captured options, reusing
// azure_detail::make_azure_options (the same call the single-object source uses).
template <typename T>
void register_azure_parquet_source(clink::plugin::PluginRegistry& reg,
                                   std::string name,
                                   ArrowBatcher<T> batcher) {
    reg.register_source<T>(
        name,
        [name, batcher](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<Source<T>> {
            const auto container = ctx.param_or("container");
            const auto account_name = ctx.param_or("account_name");
            if (container.empty() || account_name.empty()) {
                throw std::runtime_error(name + ": 'container' and 'account_name' are required");
            }
            // Auth/endpoint params shared by the single- and multi-object paths.
            typename ParquetAzureSource<T>::Options opts;
            opts.account_name = account_name;
            opts.anonymous = ctx.param_or("anonymous", "false") == "true";
            opts.use_default_credential = ctx.param_or("use_default_credential", "false") == "true";
            if (const auto k = ctx.param_or("account_key", ""); !k.empty()) {
                opts.account_key = k;
            }
            if (const auto s = ctx.param_or("sas_token", ""); !s.empty()) {
                opts.sas_token = s;
            }
            if (const auto a = ctx.param_or("blob_storage_authority", ""); !a.empty()) {
                opts.blob_storage_authority = a;
            }
            if (const auto s = ctx.param_or("blob_storage_scheme", ""); !s.empty()) {
                opts.blob_storage_scheme = s;
            }

            // Multi-object: read every object under `prefix`, sharded by subtask.
            if (const auto prefix = ctx.param_or("prefix", ""); !prefix.empty()) {
                auto fs_factory = [opts, name]() -> std::shared_ptr<arrow::fs::FileSystem> {
                    auto azure_opts =
                        clink::azure_detail::make_azure_options(opts.account_name,
                                                                opts.anonymous,
                                                                opts.account_key,
                                                                opts.sas_token,
                                                                opts.use_default_credential,
                                                                opts.blob_storage_authority,
                                                                opts.blob_storage_scheme);
                    auto r = arrow::fs::AzureFileSystem::Make(azure_opts);
                    if (!r.ok()) {
                        throw std::runtime_error(
                            name + ": AzureFileSystem::Make: " + r.status().ToString());
                    }
                    return *r;
                };
                typename MultiObjectParquetSource<T>::Options mo;
                mo.prefix = container + "/" + prefix;
                mo.subtask_idx = static_cast<int>(ctx.subtask_idx);
                mo.parallelism = static_cast<int>(ctx.parallelism);
                mo.recursive = ctx.param_or("recursive", "true") == "true";
                mo.suffix = ctx.param_or("suffix", ".parquet");
                return std::make_shared<MultiObjectParquetSource<T>>(
                    std::move(fs_factory), std::move(mo), batcher);
            }

            // Single object.
            opts.container = container;
            opts.key = ctx.param_or("key");
            if (opts.key.empty()) {
                throw std::runtime_error(name + ": 'key' or 'prefix' is required");
            }
            return std::make_shared<ParquetAzureSource<T>>(std::move(opts), batcher);
        });
}

// Register an exactly-once Azure Blob Parquet sink (ParquetFsSink2PC): stages one file per
// checkpoint interval under <container>/<prefix>/staging and promotes it to
// <container>/<prefix>/committed on commit. The AzureFileSystem is built on the runner thread via
// azure_detail::make_azure_options (the same call the source uses). Read with azure_parquet source
// on <prefix>/committed.
template <typename T>
void register_azure_parquet_2pc_sink(clink::plugin::PluginRegistry& reg,
                                     std::string name,
                                     ArrowBatcher<T> batcher) {
    reg.register_sink<T>(
        name, [name, batcher](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<Sink<T>> {
            const auto container = ctx.param_or("container");
            const auto account_name = ctx.param_or("account_name");
            const auto prefix = ctx.param_or("prefix");
            if (container.empty() || account_name.empty() || prefix.empty()) {
                throw std::runtime_error(name +
                                         ": 'container', 'account_name' and 'prefix' are required");
            }
            // Hold the auth/endpoint fields in the source Options so make_azure_options gets the
            // exact field types the single-object source passes.
            typename ParquetAzureSource<T>::Options opts;
            opts.account_name = account_name;
            opts.anonymous = ctx.param_or("anonymous", "false") == "true";
            opts.use_default_credential = ctx.param_or("use_default_credential", "false") == "true";
            if (const auto k = ctx.param_or("account_key", ""); !k.empty()) {
                opts.account_key = k;
            }
            if (const auto s = ctx.param_or("sas_token", ""); !s.empty()) {
                opts.sas_token = s;
            }
            if (const auto a = ctx.param_or("blob_storage_authority", ""); !a.empty()) {
                opts.blob_storage_authority = a;
            }
            if (const auto s = ctx.param_or("blob_storage_scheme", ""); !s.empty()) {
                opts.blob_storage_scheme = s;
            }
            auto fs_factory = [opts, name]() -> std::shared_ptr<arrow::fs::FileSystem> {
                auto azure_opts =
                    clink::azure_detail::make_azure_options(opts.account_name,
                                                            opts.anonymous,
                                                            opts.account_key,
                                                            opts.sas_token,
                                                            opts.use_default_credential,
                                                            opts.blob_storage_authority,
                                                            opts.blob_storage_scheme);
                auto r = arrow::fs::AzureFileSystem::Make(azure_opts);
                if (!r.ok()) {
                    throw std::runtime_error(name +
                                             ": AzureFileSystem::Make: " + r.status().ToString());
                }
                return *r;
            };
            typename ParquetFsSink2PC<T>::Options o;
            o.base = container + "/" + prefix;
            o.subtask_idx = static_cast<int>(ctx.subtask_idx);
            return std::make_shared<ParquetFsSink2PC<T>>(
                std::move(fs_factory), std::move(o), batcher);
        });
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // ---- Parquet over Azure Blob Storage (sink + source pairs, int64 + string channels) ----
    // Ride Arrow's AzureFileSystem. Auth: anonymous='true' (emulator / public container), an
    // account_key, a sas_token, use_default_credential='true' (managed-identity chain), or the
    // default credential chain. blob_storage_authority + blob_storage_scheme=http target an
    // Azurite emulator. Path = container + "/" + key.

    reg.register_sink<std::int64_t>(
        "azure_parquet_int64_sink",
        [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::int64_t>> {
            ParquetAzureSink<std::int64_t>::Options opts;
            apply_azure_params(ctx, opts);
            if (ctx.parallelism > 1) {
                opts.key += "." + std::to_string(ctx.subtask_idx) + ".parquet";
            }
            return std::make_shared<ParquetAzureSink<std::int64_t>>(std::move(opts),
                                                                    int64_arrow_batcher());
        });

    reg.register_sink<std::string>(
        "azure_parquet_string_sink",
        [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            ParquetAzureSink<std::string>::Options opts;
            apply_azure_params(ctx, opts);
            if (ctx.parallelism > 1) {
                opts.key += "." + std::to_string(ctx.subtask_idx) + ".parquet";
            }
            return std::make_shared<ParquetAzureSink<std::string>>(std::move(opts),
                                                                   string_arrow_batcher());
        });

    register_azure_parquet_source<std::int64_t>(
        reg, "azure_parquet_int64_source", int64_arrow_batcher());
    register_azure_parquet_source<std::string>(
        reg, "azure_parquet_string_source", string_arrow_batcher());
    register_azure_parquet_2pc_sink<std::int64_t>(
        reg, "azure_parquet_2pc_int64_sink", int64_arrow_batcher());
    register_azure_parquet_2pc_sink<std::string>(
        reg, "azure_parquet_2pc_string_sink", string_arrow_batcher());
}

}  // namespace clink::azure
