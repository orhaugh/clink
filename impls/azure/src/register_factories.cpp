// Azure Blob Parquet factory registration (azure_parquet_{int64,string}_{sink,source}).

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/azure/install.hpp"
#include "clink/connectors/parquet_azure_sink.hpp"
#include "clink/connectors/parquet_azure_source.hpp"
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

    reg.register_source<std::int64_t>(
        "azure_parquet_int64_source",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<std::int64_t>> {
            ParquetAzureSource<std::int64_t>::Options opts;
            apply_azure_params(ctx, opts);
            return std::make_shared<ParquetAzureSource<std::int64_t>>(std::move(opts),
                                                                      int64_arrow_batcher());
        });

    reg.register_source<std::string>(
        "azure_parquet_string_source",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            ParquetAzureSource<std::string>::Options opts;
            apply_azure_params(ctx, opts);
            return std::make_shared<ParquetAzureSource<std::string>>(std::move(opts),
                                                                     string_arrow_batcher());
        });
}

}  // namespace clink::azure
