#pragma once

// Factory-string registration for custom-struct columnar Parquet
// connectors, so a CLINK_ARROW_FIELDS type is reachable from the
// job-spec path (op_type strings keyed in RunnerRegistry), not just the
// programmatic C++ API.
//
//     struct Trade { std::int64_t id; std::string symbol; double px; };
//     CLINK_ARROW_FIELDS(Trade, id, symbol, px)
//
//     // In a plugin's install(PluginRegistry&):
//     clink::plugin::register_columnar_parquet<Trade>(reg, "trade", trade_codec());
//
// registers, under the channel name "trade":
//     - register_type<Trade>      (columnar batcher)
//     - "trade_parquet_sink"      -> ParquetSink<Trade>      (plain, one file/subtask)
//     - "trade_parquet_source"    -> ParquetSource<Trade>
//     - "trade_parquet_2pc_sink"  -> ParquetSink2PC<Trade>   (transactional)
//
// The factories read the output path from the op-spec params ("path");
// the plain sink disambiguates per-subtask at parallelism>1, the 2PC
// sink uses its own sub<N> staging prefix under a shared output dir.
//
// The op_type names default off the channel name but are overridable.
// The Codec<T> is still required (register_type needs it for state and
// the network bridges); Parquet itself uses only the batcher.

#include <memory>
#include <stdexcept>
#include <string>

#include "clink/connectors/columnar_parquet.hpp"  // ParquetSink/Source + make_columnar_* helpers
#include "clink/core/codec.hpp"

#ifdef CLINK_HAS_PARQUET

#include "clink/connectors/parquet_2pc_sink.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::plugin {

// Reusable factory builders. Returned std::functions match the
// register_sink<T>/register_source<T> factory signatures, and can be
// invoked directly (with a BuildContext) to construct a connector.

template <clink::HasArrowFields T>
inline std::function<std::shared_ptr<clink::Sink<T>>(const BuildContext&)>
columnar_parquet_sink_factory(std::string op_name = "parquet_sink") {
    return
        [op_name = std::move(op_name)](const BuildContext& ctx) -> std::shared_ptr<clink::Sink<T>> {
            auto path = ctx.param_or("path");
            if (path.empty()) {
                throw std::runtime_error(op_name + ": 'path' param is required");
            }
            if (ctx.parallelism > 1) {
                path += "." + std::to_string(ctx.subtask_idx) + ".parquet";
            }
            return std::make_shared<clink::ParquetSink<T>>(
                path, clink::make_columnar_arrow_batcher<T>(), parquet::Compression::ZSTD, op_name);
        };
}

template <clink::HasArrowFields T>
inline std::function<std::shared_ptr<clink::Source<T>>(const BuildContext&)>
columnar_parquet_source_factory(std::string op_name = "parquet_source") {
    return [op_name =
                std::move(op_name)](const BuildContext& ctx) -> std::shared_ptr<clink::Source<T>> {
        auto path = ctx.param_or("path");
        if (path.empty()) {
            throw std::runtime_error(op_name + ": 'path' param is required");
        }
        return std::make_shared<clink::ParquetSource<T>>(
            path, clink::make_columnar_arrow_batcher<T>(), op_name);
    };
}

template <clink::HasArrowFields T>
inline std::function<std::shared_ptr<clink::Sink<T>>(const BuildContext&)>
columnar_parquet_2pc_sink_factory(std::string op_name = "parquet_2pc_sink") {
    return [op_name =
                std::move(op_name)](const BuildContext& ctx) -> std::shared_ptr<clink::Sink<T>> {
        auto dir = ctx.param_or("path");
        if (dir.empty()) {
            throw std::runtime_error(op_name + ": 'path' param is required");
        }
        return std::make_shared<clink::ParquetSink2PC<T>>(dir,
                                                          clink::make_columnar_arrow_batcher<T>(),
                                                          ctx.subtask_idx,
                                                          parquet::Compression::ZSTD,
                                                          op_name);
    };
}

// Register the type + all three Parquet connector factories under
// `channel`. op_type names default to "<channel>_parquet_{sink,source,2pc_sink}".
template <clink::HasArrowFields T>
inline void register_columnar_parquet(PluginRegistry& reg,
                                      const std::string& channel,
                                      clink::Codec<T> codec,
                                      std::string sink_op = {},
                                      std::string source_op = {},
                                      std::string sink_2pc_op = {}) {
    if (sink_op.empty()) {
        sink_op = channel + "_parquet_sink";
    }
    if (source_op.empty()) {
        source_op = channel + "_parquet_source";
    }
    if (sink_2pc_op.empty()) {
        sink_2pc_op = channel + "_parquet_2pc_sink";
    }
    reg.register_type<T>(channel, std::move(codec), clink::make_columnar_arrow_batcher<T>());
    reg.register_sink<T>(sink_op, columnar_parquet_sink_factory<T>(sink_op));
    reg.register_source<T>(source_op, columnar_parquet_source_factory<T>(source_op));
    reg.register_sink<T>(sink_2pc_op, columnar_parquet_2pc_sink_factory<T>(sink_2pc_op));
}

}  // namespace clink::plugin

#endif  // CLINK_HAS_PARQUET
