// End-to-end Flight SQL: a real FlightSqlClient against the embedded
// engine's ClinkFlightSqlServer over loopback gRPC - updates run DDL,
// queries stream typed Arrow batches (changelog plans carry the leading
// row_kind column), GetTables serves the catalog.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/table.h>
#include <gtest/gtest.h>

#include "clink/embed/embedded_engine.hpp"
#include "clink/embed/flight_sql_server.hpp"

namespace fs = std::filesystem;
namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;

namespace {

struct FlightFixture {
    clink::embed::EmbeddedEngine engine;
    clink::embed::ClinkFlightSqlServer server;
    std::unique_ptr<flightsql::FlightSqlClient> client;

    explicit FlightFixture(clink::embed::EngineOptions opts)
        : engine(std::move(opts)), server(engine) {
        auto location = flight::Location::ForGrpcTcp("127.0.0.1", 0).ValueOrDie();
        flight::FlightServerOptions fopts{location};
        ASSERT_OK(server.Init(fopts));
        auto client_loc = flight::Location::ForGrpcTcp("127.0.0.1", server.port()).ValueOrDie();
        auto base = flight::FlightClient::Connect(client_loc).ValueOrDie();
        client = std::make_unique<flightsql::FlightSqlClient>(std::move(base));
    }

    static void ASSERT_OK(const arrow::Status& st) { ASSERT_TRUE(st.ok()) << st.ToString(); }

    // Run a query and drain every endpoint into one table.
    std::shared_ptr<arrow::Table> query(const std::string& sql) {
        flight::FlightCallOptions options;
        auto info = client->Execute(options, sql).ValueOrDie();
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        std::shared_ptr<arrow::Schema> schema;
        for (const auto& endpoint : info->endpoints()) {
            auto reader = client->DoGet(options, endpoint.ticket).ValueOrDie();
            schema = reader->GetSchema().ValueOrDie();
            while (true) {
                auto chunk = reader->Next().ValueOrDie();
                if (!chunk.data) {
                    break;
                }
                batches.push_back(chunk.data);
            }
        }
        return arrow::Table::FromRecordBatches(schema, batches).ValueOrDie();
    }
};

void write_orders(const fs::path& p) {
    std::ofstream out(p);
    out << R"({"user_id":1,"amount":10})" << "\n"
        << R"({"user_id":2,"amount":20})" << "\n"
        << R"({"user_id":1,"amount":30})" << "\n"
        << R"({"user_id":2,"amount":5})" << "\n"
        << R"({"user_id":1,"amount":7})" << "\n";
}

}  // namespace

TEST(FlightSql, UpdatesRunDdlAndQueriesStreamTypedBatches) {
    const auto in_path = fs::temp_directory_path() / "clink_flight_in.ndjson";
    fs::remove(in_path);
    write_orders(in_path);

    FlightFixture fx{clink::embed::EngineOptions{}};
    flight::FlightCallOptions options;

    // DDL via a statement update.
    auto update = fx.client->ExecuteUpdate(
        options,
        "CREATE TABLE orders (user_id BIGINT, amount BIGINT) WITH (connector='file', "
        "format='json', path='" +
            in_path.string() + "')");
    ASSERT_TRUE(update.ok()) << update.status().ToString();

    // GetTables serves the catalog.
    {
        auto info =
            fx.client->GetTables(options, nullptr, nullptr, nullptr, false, nullptr).ValueOrDie();
        auto reader = fx.client->DoGet(options, info->endpoints()[0].ticket).ValueOrDie();
        bool saw_orders = false;
        while (true) {
            auto chunk = reader->Next().ValueOrDie();
            if (!chunk.data) {
                break;
            }
            const auto& names =
                static_cast<const arrow::StringArray&>(*chunk.data->GetColumnByName("table_name"));
            for (std::int64_t i = 0; i < names.length(); ++i) {
                if (names.GetString(i) == "orders") {
                    saw_orders = true;
                }
            }
        }
        EXPECT_TRUE(saw_orders);
    }

    // An append query streams the rows.
    auto table = fx.query("SELECT user_id, amount FROM orders");
    EXPECT_EQ(table->num_rows(), 5);
    EXPECT_EQ(table->schema()->field(0)->name(), "user_id");

    // A retracting query streams the changelog with a leading row_kind
    // column; applying it reconstructs the final TOP-1 relation.
    auto topn = fx.query(
        "SELECT user_id, amount FROM ("
        "  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rn "
        "  FROM orders) ranked WHERE rn <= 1");
    ASSERT_EQ(topn->schema()->field(0)->name(), "row_kind");
    auto combined = topn->CombineChunks().ValueOrDie();
    const auto& kinds = static_cast<const arrow::StringArray&>(*combined->column(0)->chunk(0));
    const auto& users = static_cast<const arrow::Int64Array&>(*combined->column(1)->chunk(0));
    const auto& amounts = static_cast<const arrow::Int64Array&>(*combined->column(2)->chunk(0));
    std::map<std::pair<std::int64_t, std::int64_t>, int> relation;
    for (std::int64_t i = 0; i < combined->num_rows(); ++i) {
        const auto key = std::make_pair(users.Value(i), amounts.Value(i));
        const auto kind = kinds.GetString(i);
        if (kind == "insert" || kind == "update_after") {
            ++relation[key];
        } else if (--relation[key] == 0) {
            relation.erase(key);
        }
    }
    const std::map<std::pair<std::int64_t, std::int64_t>, int> expected{{{1, 30}, 1}, {{2, 20}, 1}};
    EXPECT_EQ(relation, expected);

    // Errors surface as statuses, not hangs.
    auto bad = fx.client->Execute(options, "SELECT nope FROM missing");
    EXPECT_FALSE(bad.ok());

    fs::remove(in_path);
}
