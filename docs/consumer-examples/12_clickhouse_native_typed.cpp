// 12 - Write a typed struct to ClickHouse over the native protocol.
//
// A CLINK_FIELDS declaration gives the struct its Arrow schema, and
// make_clickhouse_native_sink<T> takes the ClickHouse column types from it:
// no SQL, no column list, no encoder. Unsigned fields land in unsigned
// columns, std::optional in Nullable ones, and the engine's event_time column
// is left out. The sink is at-least-once and must be the only sink on its
// chain.
//
// Pipeline:
//   VectorSource<Trade> -> make_clickhouse_native_sink<Trade>
//
// It needs a server, so it does nothing unless CLICKHOUSE_HOST names one
// (CLICKHOUSE_PORT, default 9000, for the native port). The sink checks the
// target at open and creates nothing, so create the table first:
//
//   CREATE TABLE default.clink_consumer_12_trades
//       (id Int64, qty UInt32, volume Nullable(UInt64), px Float64, venue String)
//   ENGINE = MergeTree ORDER BY id
//
// Links clink::clickhouse, which find_package(clink ... OPTIONAL_COMPONENTS
// clickhouse) provides when clink was built with the ClickHouse client.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <clink/clickhouse/native_sink.hpp>
#include <clink/core/fields.hpp>
#include <clink/operators/source_operator.hpp>
#include <clink/runtime/dag.hpp>
#include <clink/runtime/local_executor.hpp>

struct Trade {
    std::int64_t id;
    std::uint32_t qty;
    std::optional<std::uint64_t> volume;
    double px;
    std::string venue;
};
CLINK_FIELDS(Trade, id, qty, volume, px, venue);

int main() {
    using namespace clink;

    const char* host = std::getenv("CLICKHOUSE_HOST");
    if (host == nullptr || *host == '\0') {
        std::cout << "skipped: set CLICKHOUSE_HOST (and CLICKHOUSE_PORT) to write to a server\n";
        return 0;
    }
    const char* port = std::getenv("CLICKHOUSE_PORT");

    std::vector<Record<Trade>> trades;
    for (std::int64_t i = 0; i < 1000; ++i) {
        std::optional<std::uint64_t> volume;
        if (i % 10 != 0) {
            volume = (1ULL << 63) + static_cast<std::uint64_t>(i);  // past the signed range
        }
        trades.emplace_back(Trade{i,
                                  static_cast<std::uint32_t>(i * 3),
                                  volume,
                                  static_cast<double>(i) / 4.0,
                                  i % 2 == 0 ? "XLON" : "XPAR"});
    }

    // The options are the native sink's own (docs/connectors/clickhouse.md),
    // less sql_column_types: the struct supplies the types.
    const std::map<std::string, std::string> options = {
        {"host", host},
        {"port", port != nullptr && *port != '\0' ? port : "9000"},
        {"database", "default"},
        {"table", "clink_consumer_12_trades"},
    };

    Dag dag;
    auto source = dag.add_source<Trade>(std::make_shared<VectorSource<Trade>>(std::move(trades)));
    dag.add_sink<Trade>(source, clickhouse::make_clickhouse_native_sink<Trade>(options));

    LocalExecutor executor(std::move(dag));
    executor.run();
    const auto errors = executor.operator_errors();
    for (const auto& [op, message] : errors) {
        std::cerr << op << ": " << message << '\n';
    }
    if (!errors.empty()) {
        return 1;
    }
    std::cout << "wrote 1000 trades to default.clink_consumer_12_trades\n";
    return 0;
}
