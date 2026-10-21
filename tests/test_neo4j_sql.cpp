// The Neo4j sink and lookup through the SQL frontend: what the planner puts on
// the sink op (the declared column types, and no key the sink would refuse)
// and on the lookup join op (the key, the lookup table's columns and options),
// the plans it refuses, and whole SQL pipelines run by the embedded engine
// against the in-process Query API stand-ins, and against the pinned server
// when CLINK_NEO4J_TEST_ENDPOINT names it (scripts/neo4j-live.sh).

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <httplib.h>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/embed/embedded_engine.hpp"
#include "clink/neo4j/install.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/script_runner.hpp"

#include "fake_lookup_api.hpp"
#include "fake_query_api.hpp"
#include "options.hpp"

using clink::config::JsonValue;
using clink::neo4j::testing::FakeLookupApi;
using clink::neo4j::testing::FakeQueryApi;
using clink::neo4j::testing::LookupAnswer;
using clink::neo4j::testing::typed_int;
using clink::neo4j::testing::typed_str;

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
        clink::neo4j::install(reg);
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

std::vector<const clink::cluster::OperatorSpec*> neo4j_ops(
    const clink::cluster::JobGraphSpec& spec) {
    std::vector<const clink::cluster::OperatorSpec*> out;
    for (const auto& op : spec.ops) {
        if (op.type == "neo4j_sink") {
            out.push_back(&op);
        }
    }
    return out;
}

// A JSON-lines file of transfers, removed with the object.
class TransfersFile {
public:
    explicit TransfersFile(int n) {
        path_ = std::filesystem::temp_directory_path() /
                ("clink_neo4j_sql_" + std::to_string(::getpid()) + "_" + std::to_string(++seq_) +
                 ".json");
        std::ofstream f(path_);
        for (int i = 1; i <= n; ++i) {
            f << "{\"event_id\":" << i << ",\"sender\":\"acct-" << (i % 7)
              << "\",\"recipient\":\"acct-" << ((i + 3) % 7) << "\",\"amount\":" << (i * 1.25)
              << ",\"at\":" << (1700000000000LL + i * 1000) << "}\n";
        }
    }
    ~TransfersFile() { std::filesystem::remove(path_); }
    [[nodiscard]] std::string path() const { return path_.string(); }

private:
    std::filesystem::path path_;
    static inline int seq_ = 0;
};

const char* kTransferColumns =
    "(event_id BIGINT, sender VARCHAR, recipient VARCHAR, amount DOUBLE, at TIMESTAMP(3))";

std::string source_ddl(const TransfersFile& f) {
    return std::string("CREATE TABLE transfers ") + kTransferColumns +
           " WITH (connector='file', format='json', path='" + f.path() + "');";
}

std::string sink_ddl(const std::string& endpoint,
                     const std::string& statement,
                     const std::string& extra = "") {
    return std::string("CREATE TABLE graph ") + kTransferColumns +
           " WITH (connector='neo4j', endpoint='" + endpoint +
           "', username='neo4j', password='env://CLINK_NEO4J_SQL_TEST_PW', statement='" +
           statement + "'" + extra + ");";
}

// A JSON-lines file of fan-in signals (event_id, account), and the path the
// lookup's output goes to, both removed with the object.
class SignalsFiles {
public:
    explicit SignalsFiles(const std::vector<std::string>& lines) {
        const auto stem =
            std::filesystem::temp_directory_path() /
            ("clink_neo4j_lookup_sql_" + std::to_string(::getpid()) + "_" + std::to_string(++seq_));
        in_ = stem.string() + "_in.json";
        out_ = stem.string() + "_out.json";
        std::ofstream f(in_);
        for (const auto& l : lines) {
            f << l << "\n";
        }
    }
    ~SignalsFiles() {
        std::filesystem::remove(in_);
        std::filesystem::remove(out_);
    }
    [[nodiscard]] const std::string& in() const { return in_; }
    [[nodiscard]] const std::string& out() const { return out_; }
    [[nodiscard]] std::vector<JsonValue> output() const {
        std::vector<JsonValue> rows;
        std::ifstream f(out_);
        std::string line;
        while (std::getline(f, line)) {
            if (!line.empty()) {
                rows.push_back(clink::config::parse(line));
            }
        }
        return rows;
    }

private:
    std::string in_;
    std::string out_;
    static inline int seq_ = 0;
};

constexpr char kLookupStatement[] =
    "MATCH (a:Account {id: $key}) RETURN a.shared AS shared, a.evidence AS evidence";

std::string lookup_ddl(const SignalsFiles& files,
                       const std::string& endpoint,
                       const std::string& key_type = "BIGINT",
                       const std::string& extra = "",
                       const std::string& username = "reader") {
    return "CREATE TABLE signals (event_id BIGINT, account " + key_type +
           ") WITH (connector='file', format='json', path='" + files.in() +
           "'); "
           "CREATE TABLE ctx (account BIGINT, shared BIGINT, evidence VARCHAR, matched BOOLEAN, "
           "observed_at TIMESTAMP(3)) WITH (connector='lookup', function='neo4j', endpoint='" +
           endpoint + "', username='" + username +
           "', password='env://CLINK_NEO4J_SQL_TEST_PW', statement='" + kLookupStatement +
           "', matched_column='matched', observed_at_column='observed_at'" + extra +
           "); "
           "CREATE TABLE enriched (event_id BIGINT, account " +
           key_type +
           ", shared BIGINT, evidence VARCHAR, matched BOOLEAN, observed_at TIMESTAMP(3)) WITH "
           "(connector='file', format='json', path='" +
           files.out() + "');";
}

std::string lookup_insert(const char* join) {
    return std::string(
               "INSERT INTO enriched SELECT s.event_id, s.account, d.shared, d.evidence, "
               "d.matched, d.observed_at FROM signals s ") +
           join + " ctx d ON s.account = d.account;";
}

constexpr char kStatement[] =
    "UNWIND $rows AS row MERGE (s:SqlAccount {id: row.sender}) "
    "MERGE (r:SqlAccount {id: row.recipient}) "
    "MERGE (t:SqlTransfer {event_id: row.event_id}) ON CREATE SET t.amount = row.amount, t.at = "
    "row.at "
    "MERGE (s)-[:SENT]->(t) MERGE (t)-[:TO]->(r)";

}  // namespace

TEST(Neo4jSql, TheSinkOpCarriesTheDeclaredTypesAndNoKeyTheSinkRefuses) {
    ::setenv("CLINK_NEO4J_SQL_TEST_PW", "pw", 1);
    ensure_installed();
    TransfersFile file(3);
    const std::vector<std::pair<std::string, std::string>> scripts = {
        {"projection",
         source_ddl(file) + sink_ddl("http://127.0.0.1:7474", kStatement) +
             "INSERT INTO graph SELECT event_id, sender, recipient, amount, at FROM transfers;"},
        {"filter and batching options",
         source_ddl(file) +
             sink_ddl(
                 "http://127.0.0.1:7474", kStatement, ", batch_size='50', retry_window_ms='5000'") +
             "INSERT INTO graph SELECT * FROM transfers WHERE amount > 2;"},
    };
    const auto* factory =
        clink::cluster::OperatorRegistry::default_instance().find_sink("neo4j_sink", "row");
    ASSERT_NE(factory, nullptr);
    for (const auto& [name, sql] : scripts) {
        for (const std::uint32_t parallelism : {1U, 4U}) {
            SCOPED_TRACE(name + ", parallelism " + std::to_string(parallelism));
            const auto specs = compile_script(sql, parallelism);
            std::size_t found = 0;
            for (const auto& spec : specs) {
                for (const auto* op : neo4j_ops(spec)) {
                    ++found;
                    EXPECT_EQ(op->params.at("sql_column_types"),
                              "event_id:BIGINT;sender:VARCHAR;recipient:VARCHAR;amount:DOUBLE;"
                              "at:TIMESTAMP(3)");
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

TEST(Neo4jSql, ASingleColumnTableStillGetsTypedRows) {
    ::setenv("CLINK_NEO4J_SQL_TEST_PW", "pw", 1);
    TransfersFile file(3);
    const auto specs = compile_script(
        source_ddl(file) +
        "CREATE TABLE ids (event_id BIGINT) WITH (connector='neo4j', endpoint='http://h:7474', "
        "username='neo4j', password='env://CLINK_NEO4J_SQL_TEST_PW', statement='UNWIND $rows AS "
        "row "
        "MERGE (:Seen {id: row.event_id})'); INSERT INTO ids SELECT event_id FROM transfers;");
    ASSERT_EQ(specs.size(), 1U);
    const auto ops = neo4j_ops(specs.front());
    ASSERT_EQ(ops.size(), 1U);
    EXPECT_EQ(ops.front()->params.at("sql_column_types"), "event_id:BIGINT");
}

TEST(Neo4jSql, PlansAgainstTheDeliveryContractAreRefused) {
    TransfersFile file(3);
    const std::string insert = "INSERT INTO graph SELECT * FROM transfers;";
    for (const auto& [extra, why] : std::vector<std::pair<std::string, std::string>>{
             {", delivery_guarantee='exactly_once'", "exactly_once"},
             {", changelog='true'", "changelog"},
         }) {
        SCOPED_TRACE(extra);
        try {
            (void)compile_script(source_ddl(file) + sink_ddl("http://h:7474", kStatement, extra) +
                                 insert);
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
            "CREATE TABLE pairs (a BIGINT, b BIGINT) WITH (connector='neo4j', "
            "endpoint='http://h:7474', statement='UNWIND $rows AS row MERGE (:T {id: row.a})'); "
            "INSERT INTO pairs SELECT x.event_id, y.event_id FROM transfers x LEFT JOIN transfers "
            "y "
            "ON x.sender = y.recipient;");
        ADD_FAILURE() << "a changelog plan into the neo4j sink was accepted";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("append-only"), std::string::npos) << e.what();
    }
}

TEST(Neo4jSql, AnEmbeddedPipelineSendsEveryRowAsTypedParameters) {
    ::setenv("CLINK_NEO4J_SQL_TEST_PW", "pw", 1);
    ensure_installed();
    FakeQueryApi api;
    TransfersFile file(25);
    {
        clink::embed::EngineOptions opts;
        std::ostringstream err;
        opts.err = &err;
        opts.out = &err;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        ASSERT_EQ(engine.execute_script(source_ddl(file) +
                                        sink_ddl(api.url(), kStatement, ", batch_size='10'")),
                  0)
            << err.str();
        ASSERT_EQ(engine.execute_script("INSERT INTO graph SELECT * FROM transfers;"), 0)
            << err.str();
        ASSERT_TRUE(engine.await_all()) << err.str();
    }
    ASSERT_EQ(api.explains().size(), 1U);
    std::vector<std::int64_t> ids;
    for (const auto& b : api.batches()) {
        EXPECT_LE(b.ids.size(), 10U);
        EXPECT_EQ(b.authorization, "Basic bmVvNGo6cHc=");  // neo4j:pw, read from env://
        ids.insert(ids.end(), b.ids.begin(), b.ids.end());
    }
    std::sort(ids.begin(), ids.end());
    std::vector<std::int64_t> want;
    for (std::int64_t i = 1; i <= 25; ++i) {
        want.push_back(i);
    }
    EXPECT_EQ(ids, want) << "every row, once";
}

TEST(Neo4jSqlLive, AnEmbeddedSqlPipelineLandsInTheGraph) {
    const char* endpoint = std::getenv("CLINK_NEO4J_TEST_ENDPOINT");
    const char* password = std::getenv("CLINK_NEO4J_TEST_PASSWORD");
    if (endpoint == nullptr || password == nullptr) {
        GTEST_SKIP() << "set CLINK_NEO4J_TEST_ENDPOINT and CLINK_NEO4J_TEST_PASSWORD "
                        "(scripts/neo4j-live.sh does)";
    }
    ::setenv("CLINK_NEO4J_SQL_TEST_PW", password, 1);
    ensure_installed();
    httplib::Client oracle(endpoint);
    oracle.set_basic_auth("neo4j", password);
    const auto run = [&](const std::string& statement) {
        std::string body = "{\"statement\":";
        JsonValue{statement}.serialize_into(body);
        body += "}";
        const auto res = oracle.Post("/db/neo4j/query/v2", body, "application/json");
        if (!res || res->body.find("\"errors\"") != std::string::npos) {
            throw std::runtime_error(res ? res->body : httplib::to_string(res.error()));
        }
        return clink::config::parse(res->body);
    };
    run("MATCH (n) WHERE n:SqlAccount OR n:SqlTransfer DETACH DELETE n");
    TransfersFile file(40);
    {
        clink::embed::EngineOptions opts;
        std::ostringstream err;
        opts.err = &err;
        opts.out = &err;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        ASSERT_EQ(engine.execute_script(source_ddl(file) +
                                        sink_ddl(endpoint, kStatement, ", batch_size='16'")),
                  0)
            << err.str();
        ASSERT_EQ(engine.execute_script("INSERT INTO graph SELECT * FROM transfers;"), 0)
            << err.str();
        ASSERT_TRUE(engine.await_all()) << err.str();
    }
    // Every transfer, with its amount and time, sent by and to the right
    // accounts, read back by the case's own client.
    const auto got =
        run("MATCH (s:SqlAccount)-[:SENT]->(t:SqlTransfer)-[:TO]->(r:SqlAccount) "
            "RETURN t.event_id, s.id, r.id, t.amount, toString(t.at) ORDER BY t.event_id");
    const auto& values = got.at("data").at("values").as_array();
    ASSERT_EQ(values.size(), 40U);
    for (std::size_t i = 0; i < values.size(); ++i) {
        const auto& row = values[i].as_array();
        const auto id = static_cast<std::int64_t>(i + 1);
        EXPECT_EQ(row[0].as_int(), id);
        EXPECT_EQ(row[1].as_string(), "acct-" + std::to_string(id % 7));
        EXPECT_EQ(row[2].as_string(), "acct-" + std::to_string((id + 3) % 7));
        EXPECT_DOUBLE_EQ(row[3].as_number(), static_cast<double>(id) * 1.25);
        // at = 2023-11-14T22:13:20 UTC plus `id` seconds, stored as a LocalDateTime.
        const std::int64_t secs = 20 + id;
        char want_at[32];
        std::snprintf(want_at,
                      sizeof(want_at),
                      "2023-11-14T22:%02lld:%02lld",
                      static_cast<long long>(13 + secs / 60),
                      static_cast<long long>(secs % 60));
        EXPECT_EQ(row[4].as_string().substr(0, 19), want_at) << "event " << id;
    }
    const auto accounts = run("MATCH (a:SqlAccount) RETURN count(a)");
    EXPECT_EQ(accounts.at("data").at("values").as_array()[0].as_array()[0].as_int(), 7);
    run("MATCH (n) WHERE n:SqlAccount OR n:SqlTransfer DETACH DELETE n");
}

TEST(Neo4jSqlLookup, ThePlannerLowersANeo4jLookupWithItsKeyColumnsAndOptions) {
    SignalsFiles files({});
    const auto specs = compile_script(lookup_ddl(files, "http://graph.internal:7474") +
                                      lookup_insert("LEFT JOIN"));
    ASSERT_EQ(specs.size(), 1U);
    const clink::cluster::OperatorSpec* lookup = nullptr;
    for (const auto& op : specs[0].ops) {
        EXPECT_NE(op.type, "async_lookup_join_row") << "the generic lookup op was planned";
        if (op.type == "neo4j_lookup_join_row") {
            lookup = &op;
        }
    }
    ASSERT_NE(lookup, nullptr);
    const auto& p = lookup->params;
    EXPECT_EQ(p.at("join_type"), "left_outer");
    EXPECT_EQ(p.at("probe_alias"), "s");
    EXPECT_EQ(p.at("dim_alias"), "d");
    EXPECT_EQ(p.at("probe_key_column"), "account");
    EXPECT_EQ(p.at("probe_key_type"), "BIGINT");
    EXPECT_EQ(p.at("dim_key_column"), "account");
    EXPECT_EQ(p.at("dim_column_types"),
              "account:BIGINT;shared:BIGINT;evidence:VARCHAR;matched:BOOLEAN;"
              "observed_at:TIMESTAMP(3)");
    EXPECT_EQ(p.at("neo4j.endpoint"), "http://graph.internal:7474");
    EXPECT_EQ(p.at("neo4j.statement"), kLookupStatement);
    EXPECT_EQ(p.at("neo4j.password"), "env://CLINK_NEO4J_SQL_TEST_PW");
    EXPECT_EQ(p.count("neo4j.connector"), 0U);
    EXPECT_EQ(p.count("neo4j.function"), 0U);
    // What the planner emits is what the operator parses.
    ::setenv("CLINK_NEO4J_SQL_TEST_PW", "pw", 1);
    const auto o = clink::neo4j::parse_lookup_options(p, 0, 1);
    EXPECT_TRUE(o.outer);
    EXPECT_EQ(o.result_columns.size(), 2U);

    // INNER drops the no-matches after the lookup; the key may sit on either
    // side of the equality.
    const auto inner = compile_script(
        lookup_ddl(files, "http://graph.internal:7474") +
        "INSERT INTO enriched SELECT s.event_id, s.account, d.shared, d.evidence, d.matched, "
        "d.observed_at FROM signals s JOIN ctx d ON d.account = s.account;");
    bool filtered = false;
    bool seen_lookup = false;
    for (const auto& op : inner[0].ops) {
        if (op.type == "neo4j_lookup_join_row") {
            seen_lookup = true;
            EXPECT_EQ(op.params.at("join_type"), "inner");
            EXPECT_EQ(op.params.at("probe_key_column"), "account");
        }
        if (seen_lookup && op.type == "filter_row_predicate") {
            filtered = op.params.at("predicate").find("d_account") != std::string::npos;
        }
    }
    EXPECT_TRUE(filtered);
}

TEST(Neo4jSqlLookup, JoinShapesOutsideTheContractAreRefusedBeforeAnyRequest) {
    SignalsFiles files({});
    const std::string ddl = lookup_ddl(files, "http://graph.internal:7474");
    const std::pair<std::string, const char*> refused[] = {
        {"INSERT INTO enriched SELECT s.event_id, s.account, d.shared, d.evidence, d.matched, "
         "d.observed_at FROM signals s RIGHT JOIN ctx d ON s.account = d.account;",
         "only INNER and LEFT"},
        {"INSERT INTO enriched SELECT s.event_id, s.account, d.shared, d.evidence, d.matched, "
         "d.observed_at FROM ctx d JOIN signals s ON s.account = d.account;",
         "must be on the right"},
        {"INSERT INTO enriched SELECT s.event_id, s.account, d.shared, d.evidence, d.matched, "
         "d.observed_at FROM signals s JOIN ctx d ON s.account > d.account;",
         "JOIN ON must be"},
        {"INSERT INTO enriched SELECT account, account, shared, evidence, matched, observed_at "
         "FROM ctx;",
         "not a stream"},
    };
    for (const auto& [sql, why] : refused) {
        try {
            (void)compile_script(ddl + sql);
            ADD_FAILURE() << "accepted: " << sql;
        } catch (const std::runtime_error& e) {
            EXPECT_NE(std::string(e.what()).find(why), std::string::npos) << e.what();
        }
    }
    // A key whose type cannot match the lookup's is refused when the job is
    // built, before a request is sent.
    ::setenv("CLINK_NEO4J_SQL_TEST_PW", "pw", 1);
    ensure_installed();
    FakeLookupApi api({"shared", "evidence"});
    SignalsFiles typed_files({R"({"event_id":1,"account":"acct-1"})"});
    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    opts.out = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    const int rc = engine.execute_script(lookup_ddl(typed_files, api.url(), "VARCHAR") +
                                         lookup_insert("LEFT JOIN"));
    const bool ok = rc == 0 && engine.await_all();
    EXPECT_FALSE(ok);
    EXPECT_NE(err.str().find("declare both as integers or both as VARCHAR"), std::string::npos)
        << err.str();
    EXPECT_EQ(api.explains(), 0U);
    EXPECT_TRUE(api.reads().empty());
}

TEST(Neo4jSqlLookup, AnEmbeddedPipelineEnrichesEveryRowAndKeepsTheNoMatches) {
    ::setenv("CLINK_NEO4J_SQL_TEST_PW", "pw", 1);
    ensure_installed();
    FakeLookupApi api({"shared", "evidence"});
    LookupAnswer seven;
    seven.rows = {{typed_int(3), typed_str("acct-02,acct-03,acct-05")}};
    api.answer("7", seven);
    LookupAnswer nineteen;
    nineteen.rows = {{typed_int(0), typed_str("")}};
    api.answer("19", nineteen);
    const std::vector<std::string> input = {
        R"({"event_id":1,"account":7})",
        R"({"event_id":2,"account":33})",
        R"({"event_id":3,"account":null})",
        R"({"event_id":4,"account":19})",
    };
    for (const char* join : {"LEFT JOIN", "JOIN"}) {
        SignalsFiles files(input);
        clink::embed::EngineOptions opts;
        std::ostringstream err;
        opts.err = &err;
        opts.out = &err;
        {
            clink::embed::EmbeddedEngine engine{std::move(opts)};
            ASSERT_EQ(engine.execute_script(lookup_ddl(files, api.url()) + lookup_insert(join)), 0)
                << err.str();
            ASSERT_TRUE(engine.await_all()) << err.str();
        }
        std::map<std::int64_t, JsonValue> by_event;
        for (const auto& row : files.output()) {
            by_event[row.at("event_id").as_int()] = row;
        }
        const bool left = std::string(join) == "LEFT JOIN";
        ASSERT_EQ(by_event.size(), left ? 4U : 2U) << join;
        const auto& seven_row = by_event.at(1);
        EXPECT_EQ(seven_row.at("shared").as_int(), 3);
        EXPECT_EQ(seven_row.at("evidence").as_string(), "acct-02,acct-03,acct-05");
        EXPECT_TRUE(seven_row.at("matched").as_bool());
        EXPECT_FALSE(seven_row.at("observed_at").is_null());
        // A match with nothing shared is still a match.
        EXPECT_EQ(by_event.at(4).at("shared").as_int(), 0);
        EXPECT_TRUE(by_event.at(4).at("matched").as_bool());
        if (left) {
            EXPECT_FALSE(by_event.at(2).at("matched").as_bool());
            EXPECT_TRUE(by_event.at(2).at("shared").is_null());
            EXPECT_FALSE(by_event.at(2).at("observed_at").is_null()) << "the no-match was read";
            EXPECT_FALSE(by_event.at(3).at("matched").as_bool());
            EXPECT_TRUE(by_event.at(3).at("observed_at").is_null()) << "a NULL key is not read";
        }
    }
    for (const auto& read : api.reads()) {
        EXPECT_EQ(read.access_mode, "READ");
        EXPECT_EQ(read.statement, kLookupStatement);
    }
}

TEST(Neo4jSqlLookupLive, AnEmbeddedSqlPipelineEnrichesFromTheGraph) {
    const char* endpoint = std::getenv("CLINK_NEO4J_TEST_ENDPOINT");
    const char* password = std::getenv("CLINK_NEO4J_TEST_PASSWORD");
    if (endpoint == nullptr || password == nullptr) {
        GTEST_SKIP() << "set CLINK_NEO4J_TEST_ENDPOINT and CLINK_NEO4J_TEST_PASSWORD "
                        "(scripts/neo4j-live.sh does)";
    }
    ::setenv("CLINK_NEO4J_SQL_TEST_PW", password, 1);
    ensure_installed();
    httplib::Client oracle(endpoint);
    oracle.set_basic_auth("neo4j", password);
    const auto run = [&](const std::string& statement) {
        std::string body = "{\"statement\":";
        JsonValue{statement}.serialize_into(body);
        body += "}";
        const auto res = oracle.Post("/db/neo4j/query/v2", body, "application/json");
        if (!res || res->body.find("\"errors\"") != std::string::npos) {
            throw std::runtime_error(res ? res->body : httplib::to_string(res.error()));
        }
    };
    run("MATCH (n:Account) WHERE n.id IN [7, 19] DETACH DELETE n");
    run("CREATE (:Account {id: 7, shared: 3, evidence: 'acct-02,acct-03,acct-05'}), "
        "(:Account {id: 19, shared: 0, evidence: ''})");
    SignalsFiles files({R"({"event_id":1,"account":7})",
                        R"({"event_id":2,"account":33})",
                        R"({"event_id":3,"account":19})"});
    {
        clink::embed::EngineOptions opts;
        std::ostringstream err;
        opts.err = &err;
        opts.out = &err;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        ASSERT_EQ(engine.execute_script(lookup_ddl(files, endpoint, "BIGINT", "", "neo4j") +
                                        lookup_insert("LEFT JOIN")),
                  0)
            << err.str();
        ASSERT_TRUE(engine.await_all()) << err.str();
    }
    run("MATCH (n:Account) WHERE n.id IN [7, 19] DETACH DELETE n");
    std::map<std::int64_t, JsonValue> by_event;
    for (const auto& row : files.output()) {
        by_event[row.at("event_id").as_int()] = row;
    }
    ASSERT_EQ(by_event.size(), 3U);
    EXPECT_EQ(by_event.at(1).at("shared").as_int(), 3);
    EXPECT_EQ(by_event.at(1).at("evidence").as_string(), "acct-02,acct-03,acct-05");
    EXPECT_TRUE(by_event.at(1).at("matched").as_bool());
    EXPECT_FALSE(by_event.at(2).at("matched").as_bool());
    EXPECT_TRUE(by_event.at(2).at("evidence").is_null());
    EXPECT_TRUE(by_event.at(3).at("matched").as_bool());
    EXPECT_EQ(by_event.at(3).at("shared").as_int(), 0);
    std::printf("[neo4j-sql-lookup-live] a matched row as written: %s\n",
                by_event.at(1).serialize(0).c_str());
}
