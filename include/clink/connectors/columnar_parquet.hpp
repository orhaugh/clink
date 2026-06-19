#pragma once

// Ergonomic columnar Parquet connectors for CLINK_ARROW_FIELDS types.
//
// ParquetSink<T> / ParquetSource<T> already ride any ArrowBatcher<T>.
// These helpers pair them with the generated columnar batcher so a
// described struct writes/reads externally-typed Parquet (one typed
// column per field, plus event_time) in one call:
//
//     struct Trade { std::int64_t id; std::string symbol; double px; };
//     CLINK_ARROW_FIELDS(Trade, id, symbol, px)
//
//     auto sink = clink::make_columnar_parquet_sink<Trade>("trades.parquet");
//     auto src  = clink::make_columnar_parquet_source<Trade>("trades.parquet");
//
// The file's stored Arrow schema is the columnar one, so pyarrow /
// duckdb / polars read each field as its own typed column rather than an
// opaque value_bytes blob. No Codec<T> is needed for Parquet (the
// batcher alone drives schema/build/parse); a codec is only required for
// state and the network bridges.

#include <filesystem>
#include <string>
#include <utility>

#include "clink/core/columnar_batcher.hpp"

#ifdef CLINK_HAS_PARQUET

#include "clink/connectors/parquet_sink.hpp"
#include "clink/connectors/parquet_source.hpp"

namespace clink {

template <HasArrowFields T>
inline ParquetSink<T> make_columnar_parquet_sink(
    std::filesystem::path path,
    parquet::Compression::type compression = parquet::Compression::ZSTD,
    std::string name = "parquet_sink") {
    return ParquetSink<T>(
        std::move(path), make_columnar_arrow_batcher<T>(), compression, std::move(name));
}

template <HasArrowFields T>
inline ParquetSource<T> make_columnar_parquet_source(std::filesystem::path path,
                                                     std::string name = "parquet_source") {
    return ParquetSource<T>(std::move(path), make_columnar_arrow_batcher<T>(), std::move(name));
}

}  // namespace clink

#endif  // CLINK_HAS_PARQUET
