// WebHDFS Parquet factory registration (webhdfs_parquet_{int64,string}_{sink,source}).

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/connectors/webhdfs_parquet_sink.hpp"
#include "clink/connectors/webhdfs_parquet_source.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/webhdfs/install.hpp"

namespace clink::webhdfs {

namespace {

// Apply the params common to the WebHDFS sink + source (endpoint, path, auth, transport).
template <typename Opts>
void apply_common_params(const clink::plugin::BuildContext& ctx, Opts& opts) {
    opts.base_url = ctx.param_or("base_url");
    opts.path = ctx.param_or("path");
    if (const auto u = ctx.param_or("user", ""); !u.empty()) {
        opts.user = u;
    }
    if (const auto d = ctx.param_or("delegation_token", ""); !d.empty()) {
        opts.delegation_token = d;
    }
    opts.verify_tls = ctx.param_or("verify_tls", "true") == "true";
    if (const auto c = ctx.param_int64_or("connect_timeout_ms", 0); c > 0) {
        opts.connect_timeout_ms = static_cast<int>(c);
    }
    if (const auto r = ctx.param_int64_or("rw_timeout_ms", 0); r > 0) {
        opts.rw_timeout_ms = static_cast<int>(r);
    }
    if (opts.base_url.empty() || opts.path.empty()) {
        throw std::runtime_error("webhdfs_parquet: 'base_url' and 'path' are required");
    }
}

template <typename T>
std::shared_ptr<Sink<T>> make_sink(const clink::plugin::BuildContext& ctx,
                                   ArrowBatcher<T> batcher) {
    typename WebHdfsParquetSink<T>::Options opts;
    apply_common_params(ctx, opts);
    opts.overwrite = ctx.param_or("overwrite", "true") == "true";
    if (const auto p = ctx.param_or("permission", ""); !p.empty()) {
        opts.permission = p;
    }
    if (ctx.parallelism > 1) {
        opts.path += "." + std::to_string(ctx.subtask_idx) + ".parquet";
    }
    return std::make_shared<WebHdfsParquetSink<T>>(std::move(opts), std::move(batcher));
}

template <typename T>
std::shared_ptr<Source<T>> make_source(const clink::plugin::BuildContext& ctx,
                                       ArrowBatcher<T> batcher) {
    typename WebHdfsParquetSource<T>::Options opts;
    apply_common_params(ctx, opts);
    return std::make_shared<WebHdfsParquetSource<T>>(std::move(opts), std::move(batcher));
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // ---- Parquet over WebHDFS / HttpFS (sink + source pairs, int64 + string channels) ----
    // Reuses clink's HTTP client (no JVM/libhdfs). base_url = the WebHDFS NameNode or an HttpFS
    // gateway root; path = the HDFS file path. Auth via user (user.name) or a delegation token.
    // Two-step REST write/read (CREATE/OPEN -> 307 -> datanode); see webhdfs_parquet_sink.hpp.

    reg.register_sink<std::int64_t>("webhdfs_parquet_int64_sink", [](const BuildContext& ctx) {
        return make_sink<std::int64_t>(ctx, int64_arrow_batcher());
    });

    reg.register_sink<std::string>("webhdfs_parquet_string_sink", [](const BuildContext& ctx) {
        return make_sink<std::string>(ctx, string_arrow_batcher());
    });

    reg.register_source<std::int64_t>("webhdfs_parquet_int64_source", [](const BuildContext& ctx) {
        return make_source<std::int64_t>(ctx, int64_arrow_batcher());
    });

    reg.register_source<std::string>("webhdfs_parquet_string_source", [](const BuildContext& ctx) {
        return make_source<std::string>(ctx, string_arrow_batcher());
    });
}

}  // namespace clink::webhdfs
