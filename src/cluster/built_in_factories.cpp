#include "clink/cluster/built_in_factories.hpp"

#include <cstdint>
#include <fstream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/connectors/file_2pc_sink.hpp"
#include "clink/connectors/file_sink.hpp"
#include "clink/connectors/file_source.hpp"
#include "clink/connectors/multi_object_parquet_source.hpp"
#include "clink/connectors/parquet_sink.hpp"
#include "clink/connectors/parquet_source.hpp"
#include "clink/connectors/text_format.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/filter_operator.hpp"
#include "clink/operators/json_predicate.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::cluster {

namespace {

// File-line sinks. These move into core's own impl set; everything they
// need is already in the clink_core build, no external deps.

class FileLineSink final : public Sink<std::string> {
public:
    explicit FileLineSink(std::string path) : path_(std::move(path)) {}
    void open() override {
        out_.open(path_, std::ios::trunc);
        if (!out_.is_open()) {
            throw std::runtime_error("FileLineSink: failed to open " + path_);
        }
    }
    void on_data(const Batch<std::string>& batch) override {
        for (const auto& r : batch) {
            out_ << r.value() << "\n";
        }
    }
    void flush() override { out_.flush(); }
    void close() override { out_.close(); }
    std::string name() const override { return "file_line_sink"; }

private:
    std::string path_;
    std::ofstream out_;
};

class FileInt64Sink final : public Sink<std::int64_t> {
public:
    explicit FileInt64Sink(std::string path) : path_(std::move(path)) {}
    void open() override {
        out_.open(path_, std::ios::trunc);
        if (!out_.is_open()) {
            throw std::runtime_error("FileInt64Sink: failed to open " + path_);
        }
    }
    void on_data(const Batch<std::int64_t>& batch) override {
        for (const auto& r : batch) {
            out_ << r.value() << "\n";
        }
    }
    void flush() override { out_.flush(); }
    void close() override { out_.close(); }
    std::string name() const override { return "file_int64_sink"; }

private:
    std::string path_;
    std::ofstream out_;
};

void register_built_ins_via_plugin_api(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // ---- Channel types ----
    // Both built-in channel types get their columnar ArrowBatcher so
    // the wire format is `int64 → {event_time, value:int64}` and
    // `string → {event_time, value:utf8}`. User-registered types
    // without a custom batcher fall back to the binary-column path,
    // which still rides Arrow IPC framing but doesn't get the
    // columnar 5-9× win.
    reg.register_type<std::int64_t>(
        std::string{kChannelInt64}, int64_codec(), int64_arrow_batcher());
    reg.register_type<std::string>(
        std::string{kChannelString}, string_codec(), string_arrow_batcher());

    // ---- Sources ----
    reg.register_source<std::int64_t>(
        "int64_range_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::int64_t>> {
            const auto count = ctx.param_int64_or("count", 0);
            const auto start = ctx.param_int64_or("start", 1);
            const auto step = ctx.param_int64_or("step", 1);
            const auto par = static_cast<std::int64_t>(ctx.parallelism == 0 ? 1 : ctx.parallelism);
            const auto idx = static_cast<std::int64_t>(ctx.subtask_idx);
            std::vector<Record<std::int64_t>> records;
            for (std::int64_t k = idx; k < count; k += par) {
                records.emplace_back(Record<std::int64_t>{start + k * step});
            }
            return std::make_shared<VectorSource<std::int64_t>>(std::move(records),
                                                                "int64_range_source");
        });

    reg.register_source<std::string>(
        "string_lines_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            const auto raw = ctx.param_or("lines", "");
            std::vector<Record<std::string>> records;
            std::size_t start = 0;
            while (start <= raw.size()) {
                const auto pos = raw.find(',', start);
                const auto end = (pos == std::string::npos) ? raw.size() : pos;
                records.emplace_back(Record<std::string>{raw.substr(start, end - start)});
                if (pos == std::string::npos) {
                    break;
                }
                start = pos + 1;
            }
            return std::make_shared<VectorSource<std::string>>(std::move(records),
                                                               "string_lines_source");
        });

    // ---- Sinks ----
    reg.register_sink<std::int64_t>(
        "file_int64_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::int64_t>> {
            auto path = ctx.param_or("path");
            if (path.empty()) {
                throw std::runtime_error("file_int64_sink: 'path' param is required");
            }
            if (ctx.parallelism > 1) {
                path += "." + std::to_string(ctx.subtask_idx);
            }
            return std::make_shared<FileInt64Sink>(path);
        });

    reg.register_sink<std::string>(
        "file_line_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            auto path = ctx.param_or("path");
            if (path.empty()) {
                throw std::runtime_error("file_line_sink: 'path' param is required");
            }
            if (ctx.parallelism > 1) {
                path += "." + std::to_string(ctx.subtask_idx);
            }
            return std::make_shared<FileLineSink>(path);
        });

    // ---- Parquet sinks ----
    reg.register_sink<std::int64_t>(
        "parquet_int64_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::int64_t>> {
            auto path = ctx.param_or("path");
            if (path.empty()) {
                throw std::runtime_error("parquet_int64_sink: 'path' param is required");
            }
            if (ctx.parallelism > 1) {
                path += "." + std::to_string(ctx.subtask_idx) + ".parquet";
            }
            return std::make_shared<ParquetSink<std::int64_t>>(
                path, int64_arrow_batcher(), parquet::Compression::ZSTD, "parquet_int64_sink");
        });

    reg.register_sink<std::string>(
        "parquet_string_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            auto path = ctx.param_or("path");
            if (path.empty()) {
                throw std::runtime_error("parquet_string_sink: 'path' param is required");
            }
            if (ctx.parallelism > 1) {
                path += "." + std::to_string(ctx.subtask_idx) + ".parquet";
            }
            return std::make_shared<ParquetSink<std::string>>(
                path, string_arrow_batcher(), parquet::Compression::ZSTD, "parquet_string_sink");
        });

    // ---- Parquet sources ----
    // A single `path` reads one local file; a `prefix` (a directory) reads every matching Parquet
    // file beneath it, sharded round-robin across subtasks, via the shared MultiObjectParquetSource
    // over a LocalFileSystem (the same seam the S3/GCS/Azure sources use, with a local filesystem).
    auto register_local_parquet_source = [&reg]<typename T>(const std::string& factory_name,
                                                            ArrowBatcher<T> batcher) {
        reg.register_source<T>(
            factory_name,
            [factory_name, batcher](const BuildContext& ctx) -> std::shared_ptr<Source<T>> {
                if (const auto prefix = ctx.param_or("prefix", ""); !prefix.empty()) {
                    typename MultiObjectParquetSource<T>::Options o;
                    o.prefix = prefix;
                    o.subtask_idx = static_cast<int>(ctx.subtask_idx);
                    o.parallelism = static_cast<int>(ctx.parallelism);
                    o.recursive = ctx.param_or("recursive", "true") == "true";
                    o.suffix = ctx.param_or("suffix", ".parquet");
                    return std::make_shared<MultiObjectParquetSource<T>>(
                        []() -> std::shared_ptr<arrow::fs::FileSystem> {
                            return std::make_shared<arrow::fs::LocalFileSystem>();
                        },
                        std::move(o),
                        batcher,
                        factory_name);
                }
                auto path = ctx.param_or("path");
                if (path.empty()) {
                    throw std::runtime_error(factory_name +
                                             ": 'path' or 'prefix' param is required");
                }
                return std::make_shared<ParquetSource<T>>(path, batcher, factory_name);
            });
    };
    register_local_parquet_source.template operator()<std::int64_t>("parquet_int64_source",
                                                                    int64_arrow_batcher());
    register_local_parquet_source.template operator()<std::string>("parquet_string_source",
                                                                   string_arrow_batcher());

    reg.register_sink<std::int64_t>("collecting_int64_sink",
                                    [](const BuildContext&) -> std::shared_ptr<Sink<std::int64_t>> {
                                        return std::make_shared<CollectingSink<std::int64_t>>();
                                    });

    reg.register_sink<std::string>("collecting_string_sink",
                                   [](const BuildContext&) -> std::shared_ptr<Sink<std::string>> {
                                       return std::make_shared<CollectingSink<std::string>>();
                                   });

    // ---- Mid-chain operators ----
    reg.register_operator<std::int64_t, std::int64_t>(
        "identity_int64",
        [](const BuildContext&) -> std::shared_ptr<Operator<std::int64_t, std::int64_t>> {
            return std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
                [](const std::int64_t& v) { return v; }, "identity_int64");
        });

    reg.register_operator<std::string, std::string>(
        "identity_string",
        [](const BuildContext&) -> std::shared_ptr<Operator<std::string, std::string>> {
            return std::make_shared<MapOperator<std::string, std::string>>(
                [](const std::string& v) { return v; }, "identity_string");
        });

    // filter_string_predicate: emitted by the SQL planner for WHERE
    // clauses against single-string-column tables. The 'predicate'
    // param carries a JSON expression in the format defined by
    // include/clink/operators/json_predicate.hpp; the resolver hands
    // the input record back as the value of the (only) declared
    // column. Records where the predicate is false get dropped.
    reg.register_operator<std::string, std::string>(
        "filter_string_predicate",
        [](const BuildContext& ctx) -> std::shared_ptr<Operator<std::string, std::string>> {
            const auto pred_text = ctx.param_or("predicate", "");
            if (pred_text.empty()) {
                throw std::runtime_error("filter_string_predicate: 'predicate' param is required");
            }
            auto pred_json =
                std::make_shared<clink::config::JsonValue>(clink::config::parse(pred_text));
            return std::make_shared<clink::FilterOperator<std::string>>(
                [pred_json](const std::string& v) -> bool {
                    // Single-string-column path: every column lookup returns the
                    // input record as a JsonValue{string}. NULL doesn't apply here
                    // since clink's per-record string channel can't carry NULL.
                    auto resolve = [&](const std::string&) -> clink::config::JsonValue {
                        return clink::config::JsonValue{v};
                    };
                    return clink::operators::evaluate_json_predicate(*pred_json, resolve);
                },
                "filter_string_predicate");
        });

    reg.register_operator<std::int64_t, std::int64_t>(
        "multiply_int64",
        [](const BuildContext& ctx) -> std::shared_ptr<Operator<std::int64_t, std::int64_t>> {
            const auto factor = ctx.param_int64_or("factor", 1);
            return std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
                [factor](const std::int64_t& v) { return v * factor; }, "multiply_int64");
        });

    reg.register_operator<std::int64_t, std::string>(
        "int64_to_string",
        [](const BuildContext&) -> std::shared_ptr<Operator<std::int64_t, std::string>> {
            return std::make_shared<MapOperator<std::int64_t, std::string>>(
                [](const std::int64_t& v) { return std::to_string(v); }, "int64_to_string");
        });

    reg.register_operator<std::string, std::int64_t>(
        "string_to_int64",
        [](const BuildContext&) -> std::shared_ptr<Operator<std::string, std::int64_t>> {
            return std::make_shared<MapOperator<std::string, std::int64_t>>(
                [](const std::string& s) -> std::int64_t {
                    try {
                        return std::stoll(s);
                    } catch (...) {
                        return 0;
                    }
                },
                "string_to_int64");
        });

    reg.register_operator<std::int64_t, std::int64_t>(
        "even_filter_int64",
        [](const BuildContext&) -> std::shared_ptr<Operator<std::int64_t, std::int64_t>> {
            return std::make_shared<FilterOperator<std::int64_t>>(
                [](const std::int64_t& v) { return (v % 2) == 0; }, "even_filter_int64");
        });

    // ---- Real text-based connectors --------------------------------------
    //
    // file_text_source: reads one record per line from a path. params:
    //   path (required): filesystem path to read.
    //   batch_size (default 256): records per produce() batch.
    //
    // file_text_sink: writes one record per line. Per-subtask suffix
    // when parallelism > 1. params:
    //   path (required): filesystem path to write.
    //   append (default "false"): if "true", open in append mode.
    reg.register_source<std::string>(
        "file_text_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            const auto path = ctx.param_or("path");
            if (path.empty()) {
                throw std::runtime_error("file_text_source: 'path' param is required");
            }
            const auto batch_size = static_cast<std::size_t>(ctx.param_int64_or("batch_size", 256));
            return std::make_shared<FileSource<std::string>>(
                path, string_text_format(), batch_size, "file_text_source");
        });

    reg.register_sink<std::string>(
        "file_text_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            auto path = ctx.param_or("path");
            if (path.empty()) {
                throw std::runtime_error("file_text_sink: 'path' param is required");
            }
            if (ctx.parallelism > 1) {
                path += "." + std::to_string(ctx.subtask_idx);
            }
            const bool append = ctx.param_or("append", "false") == "true";
            return std::make_shared<FileSink<std::string>>(
                path, string_text_format(), append, "file_text_sink");
        });

    // 2PC version: same wire-up surface but the runtime writes into
    // <dir>/staging/ and only an atomic rename into <dir>/committed/
    // makes records visible. Survives JM crashes mid-checkpoint: on
    // restart-with-restore, any pre-committed staging file whose
    // checkpoint id is in restored state gets committed; uncommitted
    // staging files stay in place for operator inspection.
    reg.register_sink<std::string>(
        "file_2pc_sink_string", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            const auto dir = ctx.param_or("dir");
            if (dir.empty()) {
                throw std::runtime_error("file_2pc_sink_string: 'dir' param is required");
            }
            return std::make_shared<FileSink2PC<std::string>>(
                dir, string_text_format(), ctx.subtask_idx, "file_2pc_sink_string");
        });

    // Vendor connectors (Kafka, Postgres, ClickHouse, S3) live in
    // separate impl static libraries that the application/tests link
    // explicitly. Each impl exposes a clink::<x>::install(
    // PluginRegistry&) function - callers must invoke it for those
    // factories to become reachable through this registry.
}

}  // namespace

void ensure_built_ins_registered() {
    static std::once_flag flag;
    std::call_once(flag, []() {
        // Explicit registry references to avoid recursive call_once
        // through PluginRegistry's default constructor.
        auto& tr = TypeRegistry::default_instance();
        auto& rr = RunnerRegistry::default_instance();
        auto& sr = SelectorRegistry::default_instance();
        clink::plugin::PluginRegistry reg(tr, rr, sr);
        register_built_ins_via_plugin_api(reg);
        register_builtin_joins(rr);
    });
}

}  // namespace clink::cluster
