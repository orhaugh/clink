// Integration test: VectorSource → MapOperator → PostgresSink.
//
// Spins up Postgres via Docker, creates a destination table, runs a
// clink pipeline that converts a small typed input into parameter
// strings and INSERTs via the sink, then verifies the table contents
// match the input.
//
// Skipped when Docker is not available locally.

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/postgres_sink.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

#include "tests/integration/docker_postgres.hpp"

#ifdef CLINK_HAS_POSTGRES
#include <libpq-fe.h>
#endif

using namespace clink;

namespace {

struct WidgetRow {
    int id;
    std::string label;
    int qty;
};

}  // namespace

TEST(PostgresSinkIntegration, RoundTripsRowsThroughJdbcSinkAnalogue) {
    if (!test::DockerPostgres::docker_available()) {
        GTEST_SKIP() << "Docker not available; skipping Postgres sink integration test";
    }

    test::DockerPostgres pg;
    pg.exec(R"(
        CREATE TABLE widgets (
            id    INT PRIMARY KEY,
            label TEXT NOT NULL,
            qty   INT NOT NULL
        );
    )");

    std::vector<WidgetRow> widgets = {
        {1, "alpha", 10},
        {2, "beta", 20},
        {3, "gamma", 30},
    };
    std::vector<Record<WidgetRow>> records;
    for (auto& w : widgets) {
        records.emplace_back(Record<WidgetRow>{std::move(w)});
    }

    auto src = std::make_shared<VectorSource<WidgetRow>>(std::move(records));
    auto to_strings =
        std::make_shared<MapOperator<WidgetRow, std::vector<std::string>>>([](const WidgetRow& w) {
            return std::vector<std::string>{std::to_string(w.id), w.label, std::to_string(w.qty)};
        });
    PostgresSink::Options opts;
    opts.conninfo = pg.conninfo();
    opts.sql = "INSERT INTO widgets (id, label, qty) VALUES ($1, $2, $3)";
    opts.batch_rows = 2;  // force a mid-pipeline flush
    opts.batch_interval = std::chrono::milliseconds{50};
    auto sink = std::make_shared<PostgresSink>(std::move(opts));

    Dag dag;
    auto h0 = dag.add_source<WidgetRow>(src);
    auto h1 = dag.add_operator<WidgetRow, std::vector<std::string>>(h0, to_strings);
    dag.add_sink<std::vector<std::string>>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

#ifdef CLINK_HAS_POSTGRES
    PGconn* conn = PQconnectdb(pg.conninfo().c_str());
    ASSERT_EQ(PQstatus(conn), CONNECTION_OK) << PQerrorMessage(conn);
    PGresult* r = PQexec(conn, "SELECT id, label, qty FROM widgets ORDER BY id ASC");
    ASSERT_EQ(PQresultStatus(r), PGRES_TUPLES_OK) << PQerrorMessage(conn);
    ASSERT_EQ(PQntuples(r), 3);
    EXPECT_STREQ(PQgetvalue(r, 0, 0), "1");
    EXPECT_STREQ(PQgetvalue(r, 0, 1), "alpha");
    EXPECT_STREQ(PQgetvalue(r, 0, 2), "10");
    EXPECT_STREQ(PQgetvalue(r, 1, 1), "beta");
    EXPECT_STREQ(PQgetvalue(r, 2, 1), "gamma");
    PQclear(r);
    PQfinish(conn);
#endif
}

// SQL injection prophylactic: a parameter value containing a
// statement-terminator + DDL must NOT be interpreted as SQL. libpq's
// PQexecParams binds via the protocol's bind-message, so the payload
// is treated as a literal value regardless of contents.
TEST(PostgresSinkIntegration, ParameterValuesAreEscapedAgainstInjection) {
    if (!test::DockerPostgres::docker_available()) {
        GTEST_SKIP() << "Docker not available; skipping Postgres sink integration test";
    }

    test::DockerPostgres pg;
    pg.exec("CREATE TABLE notes (id INT, body TEXT);");

    std::vector<Record<std::vector<std::string>>> rows;
    rows.emplace_back(Record<std::vector<std::string>>{
        std::vector<std::string>{"1", "'); DROP TABLE notes; --"}});

    auto src = std::make_shared<VectorSource<std::vector<std::string>>>(std::move(rows));
    PostgresSink::Options opts;
    opts.conninfo = pg.conninfo();
    opts.sql = "INSERT INTO notes (id, body) VALUES ($1, $2)";
    auto sink = std::make_shared<PostgresSink>(std::move(opts));

    Dag dag;
    auto h0 = dag.add_source<std::vector<std::string>>(src);
    dag.add_sink<std::vector<std::string>>(h0, sink);
    LocalExecutor exec(std::move(dag));
    exec.run();

#ifdef CLINK_HAS_POSTGRES
    PGconn* conn = PQconnectdb(pg.conninfo().c_str());
    ASSERT_EQ(PQstatus(conn), CONNECTION_OK);
    PGresult* r = PQexec(conn, "SELECT body FROM notes WHERE id = 1");
    ASSERT_EQ(PQresultStatus(r), PGRES_TUPLES_OK);
    ASSERT_EQ(PQntuples(r), 1);
    // The literal payload made it through verbatim - the DROP statement
    // was not executed (table still exists for the SELECT to succeed).
    EXPECT_STREQ(PQgetvalue(r, 0, 0), "'); DROP TABLE notes; --");
    PQclear(r);
    PQfinish(conn);
#endif
}
