// Live instantiations of the public contract suites against a real
// PostgreSQL server: the 2PC sink through SinkContractSuite (its crash
// windows land on genuine PREPARE TRANSACTION state), and the upsert sink
// through UpsertContractSuite (idempotency-key collapse on a real primary
// key). Self-skip without CLINK_POSTGRES_CDC_TEST_DSN, same convention as
// the other live suites in this directory; the 2PC arm additionally needs
// max_prepared_transactions > 0.

#include <cstdlib>
#include <filesystem>
#include <libpq-fe.h>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/postgres_json_sink_2pc.hpp"
#include "clink/connectors/postgres_json_upsert_sink.hpp"
#include "clink/test/sink_contract.hpp"
#include "clink/test/upsert_contract.hpp"

namespace {

using clink::test::SinkContractFixture;
using clink::test::UpsertContractFixture;
using clink::test::UpsertState;

bool pg_configured() {
    return std::getenv("CLINK_POSTGRES_CDC_TEST_DSN") != nullptr;
}
std::string pg_dsn() {
    const char* v = std::getenv("CLINK_POSTGRES_CDC_TEST_DSN");
    return v == nullptr ? std::string{} : std::string{v};
}

std::string contract_scalar(const std::string& sql) {
    PGconn* c = PQconnectdb(pg_dsn().c_str());
    if (PQstatus(c) != CONNECTION_OK) {
        PQfinish(c);
        return {};
    }
    PGresult* r = PQexec(c, sql.c_str());
    std::string out;
    if (PQresultStatus(r) == PGRES_TUPLES_OK && PQntuples(r) > 0 && !PQgetisnull(r, 0, 0)) {
        out = PQgetvalue(r, 0, 0);
    }
    PQclear(r);
    PQfinish(c);
    return out;
}

void contract_sql(const std::string& sql) {
    PGconn* c = PQconnectdb(pg_dsn().c_str());
    ASSERT_EQ(PQstatus(c), CONNECTION_OK) << PQerrorMessage(c);
    PGresult* r = PQexec(c, sql.c_str());
    const auto st = PQresultStatus(r);
    EXPECT_TRUE(st == PGRES_COMMAND_OK || st == PGRES_TUPLES_OK)
        << sql << " -> " << PQerrorMessage(c);
    PQclear(r);
    PQfinish(c);
}

// (id, val) rows of `table`, any order.
UpsertState table_rows(const std::string& table) {
    UpsertState rows;
    PGconn* c = PQconnectdb(pg_dsn().c_str());
    EXPECT_EQ(PQstatus(c), CONNECTION_OK) << PQerrorMessage(c);
    PGresult* r = PQexec(c, ("SELECT id, val FROM " + table).c_str());
    if (PQresultStatus(r) == PGRES_TUPLES_OK) {
        for (int i = 0; i < PQntuples(r); ++i) {
            rows.emplace_back(PQgetvalue(r, i, 0), PQgetvalue(r, i, 1));
        }
    }
    PQclear(r);
    PQfinish(c);
    return rows;
}

// Table name derived from the suite's per-test scratch dir, so each test
// owns one table and re-running the same test recreates it.
std::string table_for(const std::filesystem::path& dir, const std::string& prefix) {
    std::string name = prefix + dir.filename().string();
    for (auto& ch : name) {
        // Lowercase-fold as well as sanitise: the fixture CREATEs the table
        // UNQUOTED (Postgres folds it to lowercase) while the sink quotes
        // the name case-preserved, and those two spellings must be the same
        // relation.
        if (ch >= 'A' && ch <= 'Z') {
            ch = static_cast<char>(ch - 'A' + 'a');
        } else if ((ch < 'a' || ch > 'z') && (ch < '0' || ch > '9')) {
            ch = '_';
        }
    }
    // Postgres identifiers truncate at 63 bytes; truncate explicitly so two
    // long test names cannot silently collide post-truncation mid-run.
    if (name.size() > 60) {
        name.resize(60);
    }
    return name;
}

std::string row_json(int id, const std::string& val) {
    return "{\"id\":" + std::to_string(id) + ",\"val\":\"" + val + "\"}";
}

// --- 2PC: SinkContractSuite ----------------------------------------------------

struct PostgresSink2PCContract {
    using Value = std::string;
    static constexpr std::string_view kCapabilityName = "postgres_2pc";

    static bool available() {
        // Skip (never fail) when the server is absent or PREPARE TRANSACTION
        // is disabled (max_prepared_transactions=0, the Postgres default).
        return pg_configured() && contract_scalar("SHOW max_prepared_transactions") != "0";
    }

    static SinkContractFixture<std::string> make(const std::filesystem::path& dir) {
        const auto table = table_for(dir, "clink_2pc_contract_");
        contract_sql("DROP TABLE IF EXISTS " + table);
        contract_sql("CREATE TABLE " + table + " (id int primary key, val text)");

        SinkContractFixture<std::string> fx;
        fx.records = {row_json(1, "a"), row_json(2, "b"), row_json(3, "c"), row_json(4, "d")};
        fx.fresh = [table] {
            clink::PostgresJsonSink2PCOptions o;
            o.conninfo = pg_dsn();
            o.table = table;
            o.columns = {"id", "val"};
            return std::make_shared<clink::PostgresJsonSink2PC>(o);
        };
        // Committed = what a plain reader sees; rows re-rendered in the same
        // spelling the records use so equality is byte-exact.
        fx.committed = [table] {
            std::vector<std::string> out;
            for (const auto& [id, val] : table_rows(table)) {
                out.push_back("{\"id\":" + id + ",\"val\":\"" + val + "\"}");
            }
            return out;
        };
        return fx;
    }
};

// --- upsert: UpsertContractSuite ------------------------------------------------

struct PostgresUpsertContract {
    using Value = std::string;
    static constexpr std::string_view kCapabilityName = "postgres_upsert";

    static bool available() { return pg_configured(); }

    static UpsertContractFixture<std::string> make(const std::filesystem::path& dir) {
        const auto table = table_for(dir, "clink_ups_contract_");
        contract_sql("DROP TABLE IF EXISTS " + table);
        contract_sql("CREATE TABLE " + table + " (id int primary key, val text)");

        UpsertContractFixture<std::string> fx;
        fx.records = {row_json(1, "a"), row_json(2, "b"), row_json(3, "c")};
        fx.updated_records = {row_json(1, "a2"), row_json(2, "b2"), row_json(3, "c2")};
        fx.expected = {{"1", "a"}, {"2", "b"}, {"3", "c"}};
        fx.expected_updated = {{"1", "a2"}, {"2", "b2"}, {"3", "c2"}};
        fx.fresh = [table] {
            clink::PostgresJsonUpsertSinkOptions o;
            o.conninfo = pg_dsn();
            o.table = table;
            o.columns = {"id", "val"};
            o.key_columns = {"id"};
            return std::make_shared<clink::PostgresJsonUpsertSink>(o);
        };
        fx.state = [table] { return table_rows(table); };
        return fx;
    }
};

}  // namespace

namespace clink::test {

INSTANTIATE_TYPED_TEST_SUITE_P(PostgresSink2PC, SinkContractSuite, PostgresSink2PCContract);
INSTANTIATE_TYPED_TEST_SUITE_P(PostgresUpsert, UpsertContractSuite, PostgresUpsertContract);

}  // namespace clink::test
