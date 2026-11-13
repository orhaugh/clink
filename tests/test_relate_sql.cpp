// The Relate sink through the SQL frontend: what the planner puts on the sink
// op (the declared column types, and no key the sink would refuse), the plans
// it refuses, and whole SQL pipelines run by the embedded engine into real
// stores under the temporary directory, read back through Relate's C library.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/embed/embedded_engine.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/relate/install.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/script_runner.hpp"

#include "store_reader.hpp"

using clink::relate::testing::count;
using clink::relate::testing::query;
using clink::relate::testing::TempDir;

namespace {

void ensure_installed() {
    static const bool done = [] {
        {
            clink::embed::EngineOptions opts;
            std::ostringstream sink;
            opts.out = &sink;
            opts.err = &sink;
            const clink::embed::EmbeddedEngine first{std::move(opts)};
        }
        clink::plugin::PluginRegistry reg;
        clink::relate::install(reg);
        return true;
    }();
    (void)done;
}

std::vector<clink::cluster::JobGraphSpec> compile_script(const std::string& sql,
                                                         std::uint32_t parallelism = 1) {
    ensure_installed();
    clink::sql::Catalog catalog;
    clink::sql::ScriptRunOptions opts;
    opts.parallelism = parallelism;
    std::ostringstream out;
    std::ostringstream err;
    const clink::sql::ScriptIO io{&out, &err};
    std::vector<clink::cluster::JobGraphSpec> specs;
    const auto submit = [&specs](const clink::cluster::JobGraphSpec& spec, const std::string&) {
        specs.push_back(spec);
        return 0;
    };
    if (clink::sql::run_script(sql, catalog, opts, io, submit) != 0) {
        throw std::runtime_error(err.str());
    }
    return specs;
}

std::vector<const clink::cluster::OperatorSpec*> relate_ops(
    const clink::cluster::JobGraphSpec& spec) {
    std::vector<const clink::cluster::OperatorSpec*> out;
    for (const auto& op : spec.ops) {
        if (op.type == "relate_sink") {
            out.push_back(&op);
        }
    }
    return out;
}

// Transfers between seven accounts, one a second, as JSON lines.
void write_transfers(const std::string& path, int n) {
    std::ofstream f(path);
    for (int i = 1; i <= n; ++i) {
        f << "{\"event_id\":" << i << ",\"sender\":\"acct-" << (i % 7) << "\",\"recipient\":\"acct-"
          << ((i + 3) % 7) << "\",\"amount\":" << (i * 125)
          << ",\"at\":" << (1700000000000LL + i * 1000) << "}\n";
    }
}

constexpr char kColumns[] =
    "(event_id BIGINT, sender VARCHAR, recipient VARCHAR, amount BIGINT, at BIGINT)";

std::string source_ddl(const std::string& file) {
    return std::string("CREATE TABLE transfers ") + kColumns +
           " WITH (connector='file', format='json', path='" + file +
           "', event_time_column='at', watermark_lag_ms='1000');";
}

constexpr char kSchema[] =
    "CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE; "
    "CREATE CONSTRAINT transfer_id IF NOT EXISTS FOR (t:Transfer) REQUIRE t.id IS UNIQUE";

// The connector page's example statement: one row per event, every node by its
// id under a constraint, and the relationships created once, guarded by a mark
// on the transfer's node, whichever table creates the node first.
constexpr char kStatement[] =
    "UNWIND $rows AS row "
    "WITH row.event_id AS id, collect(row)[0] AS row "
    "MERGE (s:Account {id: row.sender}) MERGE (r:Account {id: row.recipient}) "
    "MERGE (t:Transfer {id: id}) "
    "WITH s, r, t, row, t.linked IS NULL AS first "
    "SET t.amount = row.amount, t.at = row.at, t.linked = true "
    "WITH s, r, t, first "
    "CALL (s, r, t, first) { WITH s, r, t, first WHERE first "
    "CREATE (s)-[:SENT]->(t) CREATE (t)-[:TO]->(r) }";

std::string sink_ddl(const std::string& store,
                     const std::string& statement = kStatement,
                     const std::string& extra = "") {
    return std::string("CREATE TABLE graph ") + kColumns + " WITH (connector='relate', path='" +
           store + "', schema='" + kSchema + "', statement='" + statement + "'" + extra + ");";
}

// Runs a script in an embedded engine to the end of its jobs.
void run(const std::string& script) {
    ensure_installed();
    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    opts.out = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    ASSERT_EQ(engine.execute_script(script), 0) << err.str();
    ASSERT_TRUE(engine.await_all()) << err.str();
}

}  // namespace

TEST(RelateSql, TheSinkOpCarriesTheDeclaredTypesAndNoKeyTheSinkRefuses) {
    const TempDir dir;
    const auto file = dir.path("transfers.json");
    write_transfers(file, 3);
    const std::vector<std::pair<std::string, std::string>> scripts = {
        {"projection",
         source_ddl(file) + sink_ddl(dir.path("a.rl")) +
             "INSERT INTO graph SELECT event_id, sender, recipient, amount, at FROM transfers;"},
        {"filter and batching options",
         source_ddl(file) +
             sink_ddl(dir.path("b.rl"), kStatement, ", batch_size='50', retry_window_ms='5000'") +
             "INSERT INTO graph SELECT * FROM transfers WHERE amount > 200;"},
    };
    ensure_installed();
    const auto* factory =
        clink::cluster::OperatorRegistry::default_instance().find_sink("relate_sink", "row");
    ASSERT_NE(factory, nullptr);
    for (const auto& [name, sql] : scripts) {
        for (const std::uint32_t parallelism : {1U, 4U}) {
            SCOPED_TRACE(name + ", parallelism " + std::to_string(parallelism));
            const auto specs = compile_script(sql, parallelism);
            std::size_t found = 0;
            for (const auto& spec : specs) {
                for (const auto* op : relate_ops(spec)) {
                    ++found;
                    EXPECT_EQ(op->params.at("sql_column_types"),
                              "event_id:BIGINT;sender:VARCHAR;recipient:VARCHAR;amount:BIGINT;"
                              "at:BIGINT");
                    for (std::uint32_t subtask = 0; subtask < op->parallelism; ++subtask) {
                        clink::cluster::OperatorBuildContext ctx;
                        ctx.params = op->params;
                        ctx.subtask_idx = subtask;
                        ctx.parallelism = op->parallelism;
                        EXPECT_NO_THROW((void)factory->build(ctx))
                            << "the sink refused what the planner put on its op";
                    }
                }
            }
            EXPECT_EQ(found, 1U);
        }
    }
}

TEST(RelateSql, ASingleColumnTableStillGetsTypedRows) {
    const TempDir dir;
    const auto file = dir.path("transfers.json");
    write_transfers(file, 3);
    const auto specs = compile_script(
        source_ddl(file) + "CREATE TABLE ids (event_id BIGINT) WITH (connector='relate', path='" +
        dir.path("ids.rl") +
        "', statement='UNWIND $rows AS row MERGE (:Seen {id: row.event_id})'); "
        "INSERT INTO ids SELECT event_id FROM transfers;");
    ASSERT_EQ(specs.size(), 1U);
    const auto ops = relate_ops(specs.front());
    ASSERT_EQ(ops.size(), 1U);
    EXPECT_EQ(ops.front()->params.at("sql_column_types"), "event_id:BIGINT");
}

TEST(RelateSql, PlansAgainstTheDeliveryContractAreRefused) {
    const TempDir dir;
    const auto file = dir.path("transfers.json");
    write_transfers(file, 3);
    const std::string insert = "INSERT INTO graph SELECT * FROM transfers;";
    for (const auto& [extra, why] : std::vector<std::pair<std::string, std::string>>{
             {", delivery_guarantee='exactly_once'", "exactly_once"},
             {", changelog='true'", "changelog"},
             {", mode='upsert', primary_key='event_id'", "upsert"},
         }) {
        SCOPED_TRACE(extra);
        try {
            (void)compile_script(source_ddl(file) +
                                 sink_ddl(dir.path("refused.rl"), kStatement, extra) + insert);
            ADD_FAILURE() << "the plan was accepted";
        } catch (const std::runtime_error& e) {
            EXPECT_NE(std::string(e.what()).find(why), std::string::npos) << e.what();
        }
    }
    // A query whose result retracts rows (an outer join revises a row once its
    // match arrives) cannot reach a sink that only appends.
    try {
        (void)compile_script(
            source_ddl(file) +
            "CREATE TABLE pairs (a BIGINT, b BIGINT) WITH (connector='relate', path='" +
            dir.path("pairs.rl") +
            "', statement='UNWIND $rows AS row MERGE (:T {id: row.a})'); "
            "INSERT INTO pairs SELECT x.event_id, y.event_id FROM transfers x LEFT JOIN transfers "
            "y ON x.sender = y.recipient;");
        ADD_FAILURE() << "a changelog plan into the relate sink was accepted";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("append-only"), std::string::npos) << e.what();
    }
    EXPECT_FALSE(std::filesystem::exists(dir.path("refused.rl"))) << "planning opens no store";
}

TEST(RelateSql, AnEmbeddedPipelineWritesEveryRowIntoTheStore) {
    const TempDir dir;
    const auto file = dir.path("transfers.json");
    const auto store = dir.path("graph.rl");
    write_transfers(file, 25);
    run(source_ddl(file) + sink_ddl(store, kStatement, ", batch_size='10'") +
        "INSERT INTO graph SELECT * FROM transfers;");
    EXPECT_EQ(count(store, "MATCH (t:Transfer) RETURN count(t)"), 25);
    EXPECT_EQ(count(store, "MATCH (a:Account) RETURN count(a)"), 7);
    EXPECT_EQ(count(store,
                    "MATCH (:Account)-[:SENT]->(:Transfer)-[:TO]->(:Account) "
                    "RETURN count(*)"),
              25);
    EXPECT_EQ(
        query(store,
              "MATCH (s:Account)-[:SENT]->(t:Transfer {id: 9})-[:TO]->(r:Account) "
              "RETURN s.id, r.id, t.amount, t.at"),
        (std::vector<std::vector<std::string>>{{"acct-2", "acct-5", "1125", "1700000009000"}}));
    // The same pipeline again, into the same store: a replay of every row.
    run(source_ddl(file) + sink_ddl(store, kStatement, ", batch_size='10'") +
        "INSERT INTO graph SELECT * FROM transfers;");
    EXPECT_EQ(count(store, "MATCH (n) RETURN count(n)"), 32);
    EXPECT_EQ(count(store, "MATCH ()-[r]->() RETURN count(r)"), 50);
}

// Three jobs in one engine write one store at once: the events, an indicator
// from an event-time window that points at its events, and links from an
// event-time interval join. No job waits for another, so a link or an indicator
// can reach the store before the transfers it names.
TEST(RelateSql, SeveralJobsWriteOneStoreAndTheGraphConverges) {
    const TempDir dir;
    const auto file = dir.path("transfers.json");
    const auto store = dir.path("graph.rl");
    write_transfers(file, 60);
    const std::string keys =
        "CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE; "
        "CREATE CONSTRAINT transfer_id IF NOT EXISTS FOR (t:Transfer) REQUIRE t.id IS UNIQUE; "
        "CREATE CONSTRAINT busy_id IF NOT EXISTS FOR (b:Busy) REQUIRE b.id IS UNIQUE";
    run(source_ddl(file) + sink_ddl(store, kStatement, ", batch_size='7'") +
        "CREATE TABLE busy (id VARCHAR, account VARCHAR, n BIGINT, events BIGINT ARRAY) WITH ("
        "connector='relate', path='" +
        store + "', schema='" + keys +
        "', statement='UNWIND $rows AS row MERGE (a:Account {id: row.account}) "
        "MERGE (b:Busy {id: row.id}) SET b.n = row.n MERGE (b)-[:ABOUT]->(a) "
        "WITH b, row UNWIND row.events AS e "
        "MERGE (t:Transfer {id: e}) MERGE (b)-[:COUNTS]->(t)');"
        "CREATE TABLE onward (credit_id BIGINT, debit_id BIGINT) WITH (connector='relate', path='" +
        store + "', schema='" + keys +
        "', batch_size='5', statement='UNWIND $rows AS row "
        "MERGE (c:Transfer {id: row.credit_id}) MERGE (d:Transfer {id: row.debit_id}) "
        "MERGE (c)-[:PASSED_ON]->(d)');"
        "INSERT INTO graph SELECT * FROM transfers;"
        "INSERT INTO busy SELECT recipient || ':' || CAST(window_start AS VARCHAR) AS id, "
        "recipient AS account, COUNT(*) AS n, ARRAY_AGG(event_id) AS events "
        "FROM transfers GROUP BY recipient, TUMBLE(at, INTERVAL '20' SECOND);"
        "INSERT INTO onward SELECT c.event_id AS credit_id, d.event_id AS debit_id FROM transfers "
        "d "
        "JOIN transfers c ON d.sender = c.recipient "
        "AND d.at BETWEEN c.at + INTERVAL '1' SECOND AND c.at + INTERVAL '4' SECOND;");

    EXPECT_EQ(count(store, "MATCH (t:Transfer) RETURN count(t)"), 60);
    EXPECT_EQ(count(store, "MATCH (a:Account) RETURN count(a)"), 7);
    // Every transfer has its amount and both ends, whichever job made its node.
    EXPECT_EQ(count(store,
                    "MATCH (:Account)-[:SENT]->(t:Transfer)-[:TO]->(:Account) "
                    "WHERE t.amount IS NOT NULL RETURN count(t)"),
              60);
    // Each window counts the transfers it is linked to, and every transfer is in one window.
    EXPECT_EQ(count(store,
                    "MATCH (b:Busy)-[:COUNTS]->(t:Transfer) WITH b, count(t) AS linked "
                    "WHERE linked <> b.n RETURN count(b)"),
              0);
    EXPECT_EQ(count(store, "MATCH (:Busy)-[c:COUNTS]->(:Transfer) RETURN count(c)"), 60);
    // Transfer i is passed on by transfer i + 3 (its recipient sends again three
    // seconds later), and by no other inside the band.
    EXPECT_EQ(count(store, "MATCH (:Transfer)-[p:PASSED_ON]->(:Transfer) RETURN count(p)"), 57);
    EXPECT_EQ(count(store,
                    "MATCH (c:Transfer)-[:PASSED_ON]->(d:Transfer) WHERE d.id <> c.id + 3 "
                    "RETURN count(*)"),
              0);
    // Following the links from the first transfer walks the whole chain.
    EXPECT_EQ(count(store,
                    "MATCH p = (:Transfer {id: 1})-[:PASSED_ON*]->(last:Transfer) "
                    "WHERE NOT (last)-[:PASSED_ON]->() RETURN length(p)"),
              19);
}

TEST(RelateSql, AStatementTheSinkRefusesFailsTheJobAndNamesTheReason) {
    const TempDir dir;
    const auto file = dir.path("transfers.json");
    write_transfers(file, 5);
    ensure_installed();
    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    opts.out = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    const int rc = engine.execute_script(
        source_ddl(file) +
        sink_ddl(dir.path("refused.rl"), "UNWIND $rows AS row MATCH (a:Account) RETURN a.id") +
        "INSERT INTO graph SELECT * FROM transfers;");
    const bool ok = rc == 0 && engine.await_all();
    EXPECT_FALSE(ok) << "a statement that only reads ran as a sink";
    EXPECT_NE(err.str().find("relate sink"), std::string::npos) << err.str();
    EXPECT_NE(err.str().find("it only reads"), std::string::npos) << err.str();
}
