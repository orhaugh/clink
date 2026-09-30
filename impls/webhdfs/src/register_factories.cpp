// WebHDFS Parquet factory registration (webhdfs_parquet_{int64,string}_{sink,source}).

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/connectors/capability.hpp"
#include "clink/connectors/parquet_rolling_sink.hpp"
#include "clink/connectors/webhdfs_filesystem.hpp"
#include "clink/connectors/webhdfs_parquet_sink.hpp"
#include "clink/connectors/webhdfs_parquet_source.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/webhdfs/install.hpp"

namespace clink::webhdfs {

namespace {

// WebHDFS paths are absolute; RENAME takes its destination verbatim, so a relative
// directory would name different files on its two sides.
std::string absolute_hdfs_path(std::string path) {
    if (path.empty() || path.front() != '/') {
        path.insert(path.begin(), '/');
    }
    return path;
}

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

// The exactly-once sink: stages one file per checkpoint interval under <base>/staging and
// atomically RENAMEs it to <base>/committed on commit (WebHdfsParquetSink2PC).
template <typename T>
std::shared_ptr<Sink<T>> make_2pc_sink(const clink::plugin::BuildContext& ctx,
                                       const std::string& prefix,
                                       ArrowBatcher<T> batcher) {
    if (ctx.param_or("base_url").empty()) {
        throw std::runtime_error("webhdfs_parquet: 'base_url' is required");
    }
    typename WebHdfsParquetSink2PC<T>::Options o;
    o.base_url = ctx.param_or("base_url");
    o.base = prefix;
    if (const auto u = ctx.param_or("user", ""); !u.empty()) {
        o.user = u;
    }
    if (const auto d = ctx.param_or("delegation_token", ""); !d.empty()) {
        o.delegation_token = d;
    }
    if (const auto p = ctx.param_or("permission", ""); !p.empty()) {
        o.permission = p;
    }
    o.verify_tls = ctx.param_or("verify_tls", "true") == "true";
    if (const auto c = ctx.param_int64_or("connect_timeout_ms", 0); c > 0) {
        o.connect_timeout_ms = static_cast<int>(c);
    }
    if (const auto r = ctx.param_int64_or("rw_timeout_ms", 0); r > 0) {
        o.rw_timeout_ms = static_cast<int>(r);
    }
    o.subtask_idx = static_cast<int>(ctx.subtask_idx);
    return std::make_shared<WebHdfsParquetSink2PC<T>>(std::move(o), std::move(batcher));
}

template <typename T>
std::shared_ptr<Sink<T>> make_sink(const clink::plugin::BuildContext& ctx,
                                   ArrowBatcher<T> batcher) {
    // A `prefix` selects the exactly-once 2PC sink (stages under <prefix>/staging, atomically
    // RENAMEs to <prefix>/committed on commit); a `path` selects the at-least-once rolling sink
    // (a directory of part files). They are mutually exclusive.
    if (const auto prefix = ctx.param_or("prefix", ""); !prefix.empty()) {
        return make_2pc_sink<T>(ctx, prefix, std::move(batcher));
    }

    // `path` names an HDFS directory of part files, one per subtask per checkpoint interval
    // (ParquetRollingSink over WebHdfsFileSystem). Read it back with the source's `prefix`.
    const auto path = ctx.param_or("path");
    if (ctx.param_or("base_url").empty() || path.empty()) {
        throw std::runtime_error("webhdfs_parquet: 'base_url' and 'path' are required");
    }
    WebHdfsFileSystem::Options fo;
    fo.base_url = ctx.param_or("base_url");
    if (const auto u = ctx.param_or("user", ""); !u.empty()) {
        fo.user = u;
    }
    if (const auto d = ctx.param_or("delegation_token", ""); !d.empty()) {
        fo.delegation_token = d;
    }
    if (const auto p = ctx.param_or("permission", ""); !p.empty()) {
        fo.permission = p;
    }
    fo.verify_tls = ctx.param_or("verify_tls", "true") == "true";
    if (const auto c = ctx.param_int64_or("connect_timeout_ms", 0); c > 0) {
        fo.connect_timeout_ms = static_cast<int>(c);
    }
    if (const auto r = ctx.param_int64_or("rw_timeout_ms", 0); r > 0) {
        fo.rw_timeout_ms = static_cast<int>(r);
    }
    typename ParquetRollingSink<T>::Options o;
    o.dir = absolute_hdfs_path(path);
    o.subtask_idx = ctx.subtask_idx;
    o.parallelism = ctx.parallelism;
    return std::make_shared<ParquetRollingSink<T>>(
        [fo]() -> std::shared_ptr<arrow::fs::FileSystem> {
            return std::make_shared<WebHdfsFileSystem>(fo);
        },
        std::move(o),
        std::move(batcher),
        "webhdfs_parquet_sink");
}

template <typename T>
std::shared_ptr<Source<T>> make_source(const clink::plugin::BuildContext& ctx,
                                       ArrowBatcher<T> batcher) {
    // A `prefix` reads every matching Parquet object under that HDFS directory (via LISTSTATUS),
    // sharded across subtasks; a `path` reads one object, or a directory like `prefix`.
    // A `path` that names an HDFS directory (what the rolling sink writes under `path`) is read
    // the same way, so one table can be written and read back.
    std::string dir = ctx.param_or("prefix", "");
    if (dir.empty() && !ctx.param_or("path", "").empty() && !ctx.param_or("base_url").empty()) {
        WebHdfsFileSystem::Options fo;
        fo.base_url = ctx.param_or("base_url");
        if (const auto u = ctx.param_or("user", ""); !u.empty()) {
            fo.user = u;
        }
        if (const auto d = ctx.param_or("delegation_token", ""); !d.empty()) {
            fo.delegation_token = d;
        }
        fo.verify_tls = ctx.param_or("verify_tls", "true") == "true";
        const auto path = absolute_hdfs_path(ctx.param_or("path"));
        auto info = WebHdfsFileSystem(fo).GetFileInfo(path);
        if (info.ok() && info->type() == arrow::fs::FileType::Directory) {
            dir = path;
        }
    }
    if (!dir.empty()) {
        if (ctx.param_or("base_url").empty()) {
            throw std::runtime_error("webhdfs_parquet: 'base_url' is required");
        }
        typename WebHdfsMultiObjectParquetSource<T>::Options o;
        o.base_url = ctx.param_or("base_url");
        o.dir = dir;
        if (const auto u = ctx.param_or("user", ""); !u.empty()) {
            o.user = u;
        }
        if (const auto d = ctx.param_or("delegation_token", ""); !d.empty()) {
            o.delegation_token = d;
        }
        o.verify_tls = ctx.param_or("verify_tls", "true") == "true";
        if (const auto c = ctx.param_int64_or("connect_timeout_ms", 0); c > 0) {
            o.connect_timeout_ms = static_cast<int>(c);
        }
        if (const auto r = ctx.param_int64_or("rw_timeout_ms", 0); r > 0) {
            o.rw_timeout_ms = static_cast<int>(r);
        }
        o.suffix = ctx.param_or("suffix", ".parquet");
        o.subtask_idx = static_cast<int>(ctx.subtask_idx);
        o.parallelism = static_cast<int>(ctx.parallelism);
        return std::make_shared<WebHdfsMultiObjectParquetSource<T>>(std::move(o),
                                                                    std::move(batcher));
    }
    typename WebHdfsParquetSource<T>::Options opts;
    apply_common_params(ctx, opts);
    return std::make_shared<WebHdfsParquetSource<T>>(std::move(opts), std::move(batcher));
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "webhdfs_parquet",
        .version = "1",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {"clink http client", "arrow"},
        .runtime_dependencies = {"hadoop webhdfs endpoint"},
        .formats = {"parquet"},
        .boundedness = clink::connectors::Boundedness::Either,
        .replayable = true,
        .offset_model = clink::connectors::OffsetModel::FileOffset,
        .checkpoint_integrated = true,
        // With `prefix` the sink stages one file per checkpoint interval
        // and atomically RENAMEs it to <prefix>/committed on commit; with
        // `path` it writes a directory of part files, at-least-once.
        .delivery = clink::connectors::DeliveryGuarantee::ExactlyOnceAtomicPublish,
        .transactional = true,
        .auth_methods = {"none", "kerberos-proxied"},
        .tls = true,
        .backpressure = true,
        .retries = false,
        .timeout_options = {},
        .available_in_sql = true,
        .limitations =
            {"path= (a directory of part files) is at-least-once; exactly-once needs prefix=",
             "a crash can leave orphaned files under <prefix>/staging"},
        .required_options_for_exactly_once = {"prefix"},
    });

    using clink::plugin::BuildContext;

    // ---- Parquet over WebHDFS / HttpFS (sink + source pairs, int64 + string channels) ----
    // Reuses clink's HTTP client (no JVM/libhdfs). base_url = the WebHDFS NameNode or an HttpFS
    // gateway root; a sink's path = the HDFS directory it writes parts to, a source's path = one
    // HDFS file (prefix = a directory). Auth via user (user.name) or a delegation token.
    // Two-step REST write/read (CREATE/OPEN -> 307 -> datanode); see webhdfs_parquet_sink.hpp.

    reg.register_sink<std::int64_t>("webhdfs_parquet_int64_sink", [](const BuildContext& ctx) {
        return make_sink<std::int64_t>(ctx, int64_arrow_batcher());
    });

    reg.register_sink<std::string>("webhdfs_parquet_string_sink", [](const BuildContext& ctx) {
        return make_sink<std::string>(ctx, string_arrow_batcher());
    });

    // The names the SQL planner emits for delivery_guarantee='exactly_once'. Before these existed
    // an exactly-once webhdfs_parquet table planned to a factory nothing registered and failed at
    // deploy; the plain factory reached the 2PC sink only through `prefix`. The base directory is
    // `prefix`, or `path` when only that is given.
    const auto two_pc_base = [](const BuildContext& ctx) {
        auto base = ctx.param_or("prefix", "");
        if (base.empty()) {
            base = ctx.param_or("path", "");
        }
        if (base.empty()) {
            throw std::runtime_error(
                "webhdfs_parquet exactly-once sink: 'prefix' (or 'path') is required");
        }
        return base;
    };
    reg.register_sink<std::int64_t>(
        "webhdfs_parquet_2pc_int64_sink", [two_pc_base](const BuildContext& ctx) {
            return make_2pc_sink<std::int64_t>(ctx, two_pc_base(ctx), int64_arrow_batcher());
        });
    reg.register_sink<std::string>(
        "webhdfs_parquet_2pc_string_sink", [two_pc_base](const BuildContext& ctx) {
            return make_2pc_sink<std::string>(ctx, two_pc_base(ctx), string_arrow_batcher());
        });

    reg.register_source<std::int64_t>("webhdfs_parquet_int64_source", [](const BuildContext& ctx) {
        return make_source<std::int64_t>(ctx, int64_arrow_batcher());
    });

    reg.register_source<std::string>("webhdfs_parquet_string_source", [](const BuildContext& ctx) {
        return make_source<std::string>(ctx, string_arrow_batcher());
    });
}

}  // namespace clink::webhdfs
