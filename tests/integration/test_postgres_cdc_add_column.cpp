// A column added to a captured PostgreSQL table, the documented way
// (docs/connectors/postgres.md, "Adding a column"): the job is stopped with
// `clink stop`, the column is added to the table, to the target and to the
// job's declaration, and the job is resubmitted restoring from the checkpoint
// the stop printed. With keep_rows='true' the restored source reads the table
// again for the column and resumes its stream, the kept rows completing the
// replayed changes that predate the column, and the target ends equal to the
// table in every column, the new one included on rows no change touched since
// it was added.
//
// Starts its own PostgreSQL container and a coordinator and Worker; skipped
// without Docker or the binaries.

#include <chrono>
#include <filesystem>
#include <libpq-fe.h>
#include <optional>
#include <regex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "tests/integration/cluster_harness.hpp"
#include "tests/integration/docker_postgres.hpp"

namespace {

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::Process;
using clink::test::DockerPostgres;
using Clock = std::chrono::steady_clock;

std::filesystem::path node_binary() {
#ifdef CLINK_NODE_BINARY
    return std::filesystem::path{CLINK_NODE_BINARY};
#else
    return {};
#endif
}

std::filesystem::path sql_binary() {
#ifdef CLINK_SUBMIT_SQL_BINARY
    return std::filesystem::path{CLINK_SUBMIT_SQL_BINARY};
#else
    return {};
#endif
}

std::filesystem::path cli_binary() {
#ifdef CLINK_CLI_BINARY
    return std::filesystem::path{CLINK_CLI_BINARY};
#else
    return {};
#endif
}

// The first column of the first row, or empty.
std::string query(const std::string& conninfo, const std::string& sql) {
    PGconn* c = PQconnectdb(conninfo.c_str());
    if (PQstatus(c) != CONNECTION_OK) {
        const std::string err = PQerrorMessage(c);
        PQfinish(c);
        throw std::runtime_error("connect: " + err);
    }
    PGresult* r = PQexec(c, sql.c_str());
    const auto st = PQresultStatus(r);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(c);
        PQclear(r);
        PQfinish(c);
        throw std::runtime_error(sql.substr(0, 120) + " -> " + err);
    }
    std::string out;
    if (st == PGRES_TUPLES_OK && PQntuples(r) > 0 && PQgetisnull(r, 0, 0) == 0) {
        out = PQgetvalue(r, 0, 0);
    }
    PQclear(r);
    PQfinish(c);
    return out;
}

std::string job_sql(const std::string& conninfo, bool with_note) {
    const std::string note = with_note ? ", note VARCHAR" : "";
    const std::string columns =
        "(id BIGINT PRIMARY KEY, status VARCHAR, amount DECIMAL(12, 2)" + note + ")";
    return "CREATE TABLE orders_cdc " + columns +
           " WITH (connector='postgres', format='json', mode='cdc', changelog_mode='upsert', "
           "keep_rows='true', conninfo='" +
           conninfo +
           "', slot_name='addcol_slot', publication_names='addcol_pub', "
           "\"table\"='public.orders'); "
           "CREATE TABLE replica " +
           columns + " WITH (connector='postgres', format='json', mode='upsert', conninfo='" +
           conninfo +
           "', \"table\"='public.replica'); "
           "INSERT INTO replica SELECT * FROM orders_cdc;";
}

void submit(Cluster& cluster, const std::string& sql, const std::vector<std::string>& extra) {
    std::vector<std::string> argv{sql_binary().string(),
                                  "-e",
                                  sql,
                                  "--coordinator-host",
                                  "127.0.0.1",
                                  "--coordinator-port",
                                  std::to_string(cluster.http_port()),
                                  "--checkpoint-dir",
                                  cluster.checkpoint_dir().string(),
                                  "--checkpoint-interval-ms",
                                  "300"};
    argv.insert(argv.end(), extra.begin(), extra.end());
    Process p;
    ASSERT_TRUE(p.spawn("submit-sql-" + std::to_string(extra.size()),
                        sql_binary(),
                        std::move(argv),
                        cluster.log_dir()));
    const auto code = p.await_exit(60s);
    ASSERT_TRUE(code.has_value());
    ASSERT_EQ(*code, 0) << p.read_log();
}

}  // namespace

TEST(PostgresCdcAddColumn, AColumnAddedWhileTheJobIsStoppedIsBackfilledOnRestore) {
    if (node_binary().empty() || sql_binary().empty() || cli_binary().empty()) {
        GTEST_SKIP() << "built without the node, submit or CLI binaries";
    }
    if (!DockerPostgres::docker_available()) {
        GTEST_SKIP() << "Docker not available";
    }
    clink::test::DockerPostgresOptions po;
    po.postgres_args = {"-c", "wal_level=logical"};
    DockerPostgres pg(po);
    const std::string conn = pg.conninfo();
    query(conn,
          "CREATE TABLE orders (id bigint PRIMARY KEY, status text, amount numeric(12, 2)); "
          "CREATE TABLE replica (id bigint PRIMARY KEY, status text, amount numeric(12, 2)); "
          "CREATE PUBLICATION addcol_pub FOR TABLE orders; "
          "INSERT INTO orders SELECT g, 'new', g * 1.5 FROM generate_series(1, 500) g");
    const std::string differ =
        "SELECT (SELECT count(*) FROM (SELECT * FROM orders EXCEPT SELECT * FROM replica) a) + "
        "(SELECT count(*) FROM (SELECT * FROM replica EXCEPT SELECT * FROM orders) b)";
    const auto await_equal = [&](const char* what) {
        const auto deadline = Clock::now() + 90s;
        while (query(conn, differ) != "0") {
            ASSERT_LT(Clock::now(), deadline) << what << ": the replica never matched the table";
            std::this_thread::sleep_for(200ms);
        }
    };

    ClusterSpec spec;
    spec.node_binary = node_binary();
    spec.workers = 1;
    spec.http = true;
    Cluster cluster(spec);
    clink::itest::ScopedDiagnostics diagnostics(cluster);
    ASSERT_TRUE(cluster.start_coordinator());
    ASSERT_TRUE(cluster.start_worker(0));
    ASSERT_TRUE(cluster.await_workers_registered(1));

    submit(cluster, job_sql(conn, /*with_note=*/false), {});
    query(conn, "UPDATE orders SET status = 'paid' WHERE id % 2 = 0");
    await_equal("the first run");

    // `clink stop`: a final checkpoint, then the job finishes.
    Process stop;
    ASSERT_TRUE(stop.spawn("stop",
                           cli_binary(),
                           {cli_binary().string(),
                            "stop",
                            "--job-id=1",
                            "--coordinator-host=127.0.0.1",
                            "--coordinator-port=" + std::to_string(cluster.coordinator_port())},
                           cluster.log_dir()));
    const auto stopped = stop.await_exit(90s);
    ASSERT_TRUE(stopped.has_value());
    ASSERT_EQ(*stopped, 0) << stop.read_log();
    std::smatch m;
    const std::string stop_out = stop.read_log();
    ASSERT_TRUE(std::regex_search(stop_out, m, std::regex{"savepoint_checkpoint_id=([0-9]+)"}))
        << stop_out;
    const std::string checkpoint = m[1].str();

    // While nothing runs: a change before the column, the column, changes
    // after it, and deletes.
    query(conn, "UPDATE orders SET amount = amount + 1 WHERE id % 3 = 0");
    query(conn, "ALTER TABLE orders ADD COLUMN note text DEFAULT 'none'");
    query(conn, "ALTER TABLE replica ADD COLUMN note text");
    query(conn, "UPDATE orders SET note = 'n' || id WHERE id % 4 = 0");
    query(conn, "DELETE FROM orders WHERE id % 7 = 0");
    query(conn,
          "INSERT INTO orders SELECT g, 'late', g * 2.0, 'late' FROM generate_series(600, 620) g");

    submit(cluster,
           job_sql(conn, /*with_note=*/true),
           {"--restore-from-dir",
            cluster.checkpoint_dir().string(),
            "--restore-from-checkpoint-id",
            checkpoint});
    await_equal("the restored run");
    // Rows no change touched since the column was added hold its default.
    EXPECT_EQ(query(conn, "SELECT count(*) FROM replica WHERE note = 'none'"),
              query(conn, "SELECT count(*) FROM orders WHERE note = 'none'"));
    EXPECT_NE(query(conn, "SELECT count(*) FROM replica WHERE note = 'none'"), "0");
    EXPECT_TRUE(
        cluster.worker(0).log_contains("declared since the checkpoint, so the table is "
                                       "read again"))
        << "the restored source did not read the table again for the new column";
    // Changes after the restore still apply.
    query(conn, "UPDATE orders SET note = 'after' WHERE id % 5 = 0");
    await_equal("after the restore");
}
