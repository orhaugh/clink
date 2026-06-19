// 07 - Write Parquet, then read it back.
//
// Writes a few int64 records to a Parquet file, then reads them back
// through ParquetSource and prints them. The output is a normal Arrow
// IPC + Parquet stream, so the file is readable by pyarrow / duckdb /
// polars without any clink-specific glue.
//
// Pipeline (run twice in one process):
//   VectorSource<int64>   -> ParquetSink<int64>(out_path)
//   ParquetSource<int64>(out_path) -> FunctionSink(print)

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include <clink/connectors/parquet_sink.hpp>
#include <clink/connectors/parquet_source.hpp>
#include <clink/core/arrow_batcher.hpp>
#include <clink/operators/sink_operator.hpp>
#include <clink/operators/source_operator.hpp>
#include <clink/runtime/dag.hpp>
#include <clink/runtime/local_executor.hpp>

int main() {
    using namespace clink;
    namespace fs = std::filesystem;

    const fs::path path = fs::temp_directory_path() / "clink_consumer_07.parquet";

    // ---- 1. Write phase --------------------------------------------------
    {
        std::vector<Record<std::int64_t>> rows;
        for (std::int64_t i = 0; i < 8; ++i) {
            rows.emplace_back(Record<std::int64_t>{i * 10, EventTime{i}});
        }

        Dag dag;
        auto src  = std::make_shared<VectorSource<std::int64_t>>(std::move(rows));
        auto sink = std::make_shared<ParquetSink<std::int64_t>>(path, int64_arrow_batcher());

        auto h0 = dag.add_source<std::int64_t>(src);
        dag.add_sink<std::int64_t>(h0, sink);

        LocalExecutor(std::move(dag)).run();
        std::cout << "wrote " << path << '\n';
    }

    // ---- 2. Read-back phase ---------------------------------------------
    {
        Dag dag;
        auto src   = std::make_shared<ParquetSource<std::int64_t>>(path, int64_arrow_batcher());
        auto print = std::make_shared<FunctionSink<std::int64_t>>(
            [](const std::int64_t& v) { std::cout << "read: " << v << '\n'; });

        auto h0 = dag.add_source<std::int64_t>(src);
        dag.add_sink<std::int64_t>(h0, print);

        LocalExecutor(std::move(dag)).run();
    }
    return 0;
}
