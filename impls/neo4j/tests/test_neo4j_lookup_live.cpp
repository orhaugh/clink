// The lookup against a real server: the pinned build in
// docker/integration-services.yml, started by scripts/neo4j-live.sh. Every case
// skips unless CLINK_NEO4J_TEST_ENDPOINT and CLINK_NEO4J_TEST_PASSWORD name the
// server, and the gate script fails on any skip.
//
// The cases pin what the lookup's contract rests on: values read back exactly,
// no match and more than one row told apart from a match, the open check
// refusing statements that may not only read, the server itself refusing a
// write sent in read mode with the graph unchanged, the server's own timeout,
// cancellation, and the reply bound. Each case works under labels of its own
// and removes them when it ends; what it asserts about the graph it reads
// through its own client (Oracle), not through the lookup.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/row.hpp"

#include "live_oracle.hpp"
#include "lookup.hpp"
#include "options.hpp"

using namespace std::chrono_literals;
using clink::Batch;
using clink::Emitter;
using clink::StreamElement;
using clink::config::JsonValue;
using clink::neo4j::LookupCall;
using clink::neo4j::LookupPool;
using clink::neo4j::Neo4jError;
using clink::neo4j::Neo4jLookupJoin;
using clink::neo4j::parse_lookup_options;
using clink::neo4j::testing::env;
using clink::neo4j::testing::Oracle;
using clink::neo4j::testing::text;
using clink::sql::Row;
namespace code = clink::neo4j::code;

namespace {

using Clock = std::chrono::steady_clock;

// A lookup join on a VARCHAR key, run the way its runner runs it, with every
// output row kept.
struct LiveLookup {
    LiveLookup(const std::map<std::string, std::string>& params) {
        op = std::make_unique<Neo4jLookupJoin>(parse_lookup_options(params, 0, 1));
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{8100}, "neo4j_lookup_join_row", nullptr, &metrics);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        op->attach_runtime(ctx.get());
    }
    ~LiveLookup() {
        if (op) {
            op->close();
            op->attach_runtime(nullptr);
        }
    }
    // The output rows for these keys, in input order.
    std::vector<Row> run(const std::vector<std::optional<std::string>>& keys) {
        Batch<Row> b;
        std::int64_t event = 0;
        for (const auto& k : keys) {
            Row r;
            r.values["event_id"] = JsonValue{++event};
            r.values["key"] = k ? JsonValue{*k} : JsonValue{};
            b.emplace(std::move(r));
        }
        rows.clear();
        op->process(StreamElement<Row>::data(std::move(b)), out);
        op->flush(out);
        return rows;
    }

    clink::MetricsRegistry metrics;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
    std::unique_ptr<Neo4jLookupJoin> op;
    std::vector<Row> rows;
    Emitter<Row> out{[this](StreamElement<Row> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                rows.push_back(r.value());
            }
        }
        return true;
    }};
};

std::pair<std::string, std::string> failure(const std::function<void()>& f) {
    try {
        f();
    } catch (const Neo4jError& e) {
        return {e.code(), e.what()};
    }
    return {};
}

class Neo4jLookupLive : public ::testing::Test {
protected:
    void SetUp() override {
        endpoint_ = env("CLINK_NEO4J_TEST_ENDPOINT");
        password_ = env("CLINK_NEO4J_TEST_PASSWORD");
        if (endpoint_.empty() || password_.empty()) {
            GTEST_SKIP() << "set CLINK_NEO4J_TEST_ENDPOINT and CLINK_NEO4J_TEST_PASSWORD "
                            "(scripts/neo4j-live.sh does)";
        }
        oracle_ = std::make_unique<Oracle>(endpoint_, password_);
        static int counter = 0;
        label_ = "LookupProbe_" + std::to_string(::getpid()) + "_" + std::to_string(++counter);
        constraint_ = "c_" + label_;
        oracle_->run("CREATE CONSTRAINT " + constraint_ + " IF NOT EXISTS FOR (n:" + label_ +
                     ") REQUIRE n.id IS UNIQUE");
    }

    void TearDown() override {
        if (!oracle_) {
            return;
        }
        try {
            oracle_->run("MATCH (n) WHERE n:" + label_ + " OR n:" + label_ +
                         "_Dup DETACH DELETE n");
            oracle_->run("DROP CONSTRAINT " + constraint_ + " IF EXISTS");
        } catch (const std::exception& e) {
            ADD_FAILURE() << "cleaning up failed: " << e.what();
        }
    }

    // The planner's description of a LEFT JOIN on a VARCHAR key, with the
    // lookup table's options.
    std::map<std::string, std::string> params(const std::string& statement,
                                              const std::string& result_types,
                                              std::map<std::string, std::string> extra = {}) const {
        std::string dim_columns = "key";
        std::string dim_types = "key:VARCHAR";
        std::size_t start = 0;
        while (start < result_types.size()) {
            const auto end = std::min(result_types.find(';', start), result_types.size());
            const std::string entry = result_types.substr(start, end - start);
            dim_columns += "," + entry.substr(0, entry.find(':'));
            dim_types += ";" + entry;
            start = end + 1;
        }
        dim_columns += ",matched,observed_at,bookmark";
        dim_types += ";matched:BOOLEAN;observed_at:TIMESTAMP(3);bookmark:VARCHAR";
        std::map<std::string, std::string> p = {
            {"probe_alias", "s"},
            {"dim_alias", "d"},
            {"probe_columns", "event_id,key"},
            {"dim_columns", dim_columns},
            {"join_type", "left_outer"},
            {"probe_key_column", "key"},
            {"probe_key_type", "VARCHAR"},
            {"dim_key_column", "key"},
            {"dim_column_types", dim_types},
            {"neo4j.endpoint", endpoint_},
            {"neo4j.username", "neo4j"},
            {"neo4j.password", password_},
            {"neo4j.statement", statement},
            {"neo4j.matched_column", "matched"},
            {"neo4j.observed_at_column", "observed_at"},
            {"neo4j.bookmark_column", "bookmark"},
        };
        for (auto& [k, v] : extra) {
            p["neo4j." + k] = v;
        }
        return p;
    }

    std::int64_t nodes() const {
        return oracle_->count("MATCH (n) WHERE n:" + label_ + " RETURN count(n)");
    }

    std::string endpoint_;
    std::string password_;
    std::string label_;
    std::string constraint_;
    std::unique_ptr<Oracle> oracle_;
};

}  // namespace

TEST_F(Neo4jLookupLive, ValuesAreReadBackExactlyAndANoMatchIsExplicit) {
    oracle_->run("CREATE (:" + label_ +
                     " {id: 'k1', big: 9223372036854775807, small: -9223372036854775808, "
                     "ratio: 0.1, flag: true, name: $name, tiny: 127})",
                 R"({"name":"été – \"x\""})");
    LiveLookup l(params("MATCH (n:" + label_ +
                            " {id: $key}) RETURN n.big AS big, n.small AS small, n.ratio AS "
                            "ratio, n.flag AS flag, n.name AS name, n.tiny AS tiny, n.absent AS "
                            "absent",
                        "big:BIGINT;small:BIGINT;ratio:DOUBLE;flag:BOOLEAN;name:VARCHAR;tiny:"
                        "TINYINT;absent:VARCHAR"));
    l.op->open();
    const auto rows = l.run({"k1", "k-none", std::nullopt});
    ASSERT_EQ(rows.size(), 3U);
    const auto& hit = rows[0].values;
    EXPECT_TRUE(hit.at("d_matched").as_bool());
    EXPECT_EQ(hit.at("d_key").as_string(), "k1");
    EXPECT_EQ(hit.at("d_big").as_int(), 9223372036854775807LL);
    EXPECT_EQ(hit.at("d_small").as_int(), -9223372036854775807LL - 1);
    EXPECT_EQ(hit.at("d_ratio").as_number(), 0.1);
    EXPECT_TRUE(hit.at("d_flag").as_bool());
    EXPECT_EQ(hit.at("d_name").as_string(), "été – \"x\"");
    EXPECT_EQ(hit.at("d_tiny").as_int(), 127);
    EXPECT_TRUE(hit.at("d_absent").is_null()) << "a missing property is NULL in a match";
    EXPECT_FALSE(hit.at("d_bookmark").as_string().empty());
    const auto& miss = rows[1].values;
    EXPECT_FALSE(miss.at("d_matched").as_bool());
    EXPECT_TRUE(miss.at("d_key").is_null());
    EXPECT_TRUE(miss.at("d_big").is_null());
    EXPECT_FALSE(miss.at("d_observed_at").is_null());
    EXPECT_FALSE(rows[2].values.at("d_matched").as_bool());
    EXPECT_TRUE(rows[2].values.at("d_observed_at").is_null());

    // The bookmark names the database's committed position, not a time: a
    // write moves it, a read does not.
    const auto first = hit.at("d_bookmark").as_string();
    const auto again = l.run({"k1"});
    oracle_->run("MATCH (n:" + label_ + " {id: 'k1'}) SET n.tiny = 1");
    const auto after = l.run({"k1"});
    ASSERT_EQ(after.size(), 1U);
    EXPECT_EQ(after[0].values.at("d_tiny").as_int(), 1)
        << "the lookup reads the graph as it is now";
    EXPECT_NE(after[0].values.at("d_bookmark").as_string(), first);
    EXPECT_EQ(again[0].values.at("d_bookmark").as_string(), first);
}

TEST_F(Neo4jLookupLive, AValueOutsideTheDeclaredTypeAndTwoRowsFailTheLookup) {
    oracle_->run("CREATE (:" + label_ + " {id: 'k1', tiny: 128, n: 'secret-text'})");
    oracle_->run("CREATE (:" + label_ + "_Dup {group: 'g'}), (:" + label_ + "_Dup {group: 'g'})");
    {
        LiveLookup l(
            params("MATCH (n:" + label_ + " {id: $key}) RETURN n.tiny AS tiny", "tiny:TINYINT"));
        l.op->open();
        const auto [c, m] = failure([&] { (void)l.run({"k1"}); });
        EXPECT_EQ(c, code::kValueInvalid);
        EXPECT_NE(m.find("outside the type's range"), std::string::npos) << m;
    }
    {
        LiveLookup l(params("MATCH (n:" + label_ + " {id: $key}) RETURN n.n AS n", "n:BIGINT"));
        l.op->open();
        const auto [c, m] = failure([&] { (void)l.run({"k1"}); });
        EXPECT_EQ(c, code::kValueInvalid);
        EXPECT_EQ(m.find("secret-text"), std::string::npos) << m;
    }
    {
        LiveLookup l(
            params("MATCH (n:" + label_ + "_Dup {group: $key}) RETURN n.group AS g", "g:VARCHAR"));
        l.op->open();
        const auto [c, m] = failure([&] { (void)l.run({"g"}); });
        EXPECT_EQ(c, code::kMultipleRows);
        EXPECT_NE(m.find("returned 2 rows"), std::string::npos) << m;
    }
}

TEST_F(Neo4jLookupLive, TheOpenCheckRefusesEveryStatementThatMayNotOnlyRead) {
    oracle_->run("CREATE (:" + label_ + " {id: 'k1', name: 'before'})");
    const std::string m = "MATCH (n:" + label_ + " {id: $key}) ";
    const std::vector<std::string> statements = {
        "CREATE (n:" + label_ + " {id: $key}) RETURN n.id AS name",
        "MERGE (n:" + label_ + " {id: $key}) RETURN n.id AS name",
        m + "SET n.name = 'after' RETURN n.name AS name",
        m + "DETACH DELETE n RETURN $key AS name",
        m + "FOREACH (i IN [1] | SET n.name = 'after') RETURN n.name AS name",
        "CALL db.labels() YIELD label WITH label WHERE label = $key RETURN label AS name",
        "LOAD CSV FROM 'file:///clink-none.csv' AS r WITH r WHERE r[0] = $key RETURN r[0] AS name",
        "CALL () { " + m + "RETURN n.name AS name } IN TRANSACTIONS RETURN name",
        "CREATE INDEX lookup_probe_idx IF NOT EXISTS FOR (n:" + label_ + ") ON (n.name)",
    };
    for (const auto& statement : statements) {
        LiveLookup l(params(statement, "name:VARCHAR"));
        const auto [c, msg] = failure([&] { l.op->open(); });
        EXPECT_FALSE(c.empty()) << "accepted: " << statement;
        EXPECT_TRUE(c == code::kStatementRefused) << statement << ": " << msg;
    }
    // Nothing ran: the graph and the schema are as they were.
    EXPECT_EQ(nodes(), 1);
    EXPECT_EQ(oracle_->rows("MATCH (n:" + label_ + ") RETURN n.name")
                  .at(0)
                  .at(0)
                  .at("_value")
                  .as_string(),
              "before");
    EXPECT_EQ(oracle_->count("SHOW INDEXES YIELD name WHERE name = 'lookup_probe_idx' RETURN "
                             "count(*)"),
              0);
}

TEST_F(Neo4jLookupLive, TheServerItselfRefusesAWriteSentInReadModeAndNothingChanges) {
    // The open check is clink's; this is what the server enforces on its own
    // when a request says READ, as every lookup request does. Sent directly,
    // so no clink check stands in between.
    oracle_->run("CREATE (:" + label_ + " {id: 'k1', name: 'before'})");
    const std::string labels_before =
        oracle_->rows("CALL db.labels() YIELD label RETURN collect(label)")
            .at(0)
            .at(0)
            .serialize(0);
    const std::vector<std::string> writes = {
        "CREATE (:" + label_ + " {id: 'k2'})",
        "MERGE (:" + label_ + " {id: 'k3'})",
        "MATCH (n:" + label_ + ") SET n.name = 'after'",
        "MATCH (n:" + label_ + ") DETACH DELETE n",
        "CREATE INDEX lookup_probe_ro IF NOT EXISTS FOR (n:" + label_ + ") ON (n.name)",
        "CALL db.createLabel('" + label_ + "_Created')",
    };
    for (const auto& w : writes) {
        EXPECT_EQ(oracle_->error_code(w, "READ"), "Neo.ClientError.Statement.AccessMode") << w;
    }
    EXPECT_EQ(nodes(), 1);
    EXPECT_EQ(text(oracle_->rows("MATCH (n:" + label_ + ") RETURN n.name").at(0).at(0)), "before");
    EXPECT_EQ(
        oracle_->count("SHOW INDEXES YIELD name WHERE name = 'lookup_probe_ro' RETURN count(*)"),
        0);
    EXPECT_EQ(oracle_->rows("CALL db.labels() YIELD label RETURN collect(label)")
                  .at(0)
                  .at(0)
                  .serialize(0),
              labels_before);
    // The same write in WRITE mode runs: the refusal above was the access mode.
    EXPECT_EQ(oracle_->error_code("CREATE (:" + label_ + " {id: 'k2'})", "WRITE"), "");
    EXPECT_EQ(nodes(), 2);
    // This edition has no roles, so a read-only principal cannot be made here.
    EXPECT_EQ(oracle_->error_code("SHOW ROLES", "READ", "system"),
              "Neo.ClientError.Statement.UnsupportedAdministrationCommand");
}

TEST_F(Neo4jLookupLive, TheServersOwnTimeoutEndsASlowRead) {
    oracle_->run("CREATE (:" + label_ + " {id: 'k1'})");
    const std::string slow = "MATCH (n:" + label_ +
                             " {id: $key}) UNWIND range(1, 400000000) AS x WITH n, count(x) AS c "
                             "RETURN c AS c";
    LiveLookup l(
        params(slow, "c:BIGINT", {{"server_timeout_s", "1"}, {"request_timeout_ms", "20000"}}));
    l.op->open();
    const auto start = Clock::now();
    const auto [c, m] = failure([&] { (void)l.run({"k1"}); });
    const auto took = Clock::now() - start;
    EXPECT_EQ(c, code::kTimeout) << m;
    EXPECT_LT(took, 6s);
}

TEST_F(Neo4jLookupLive, ACancelEndsTheWaitAtOnceAndTheServerTimeoutEndsTheRead) {
    oracle_->run("CREATE (:" + label_ + " {id: 'k1'})");
    const std::string marker = "lookup_cancel_" + label_;
    const std::string slow = "MATCH (n:" + label_ +
                             " {id: $key}) UNWIND range(1, 400000000) AS x WITH n, x WHERE '" +
                             marker + "' <> '' WITH n, count(x) AS c RETURN c AS c";
    auto opts = parse_lookup_options(
        params(slow, "c:BIGINT", {{"server_timeout_s", "3"}, {"request_timeout_ms", "20000"}}),
        0,
        1);
    auto cancel = std::make_shared<std::atomic<bool>>(false);
    LookupPool pool(opts, clink::CancelSignal{cancel, nullptr}, nullptr, 1);
    pool.open();
    auto call = std::make_shared<LookupCall>();
    call->key = R"({"$type":"String","_value":"k1"})";
    pool.submit(call);
    const auto running = [&] {
        return oracle_->count("SHOW TRANSACTIONS YIELD currentQuery WHERE currentQuery CONTAINS '" +
                              marker + "' AND NOT currentQuery CONTAINS 'SHOW' RETURN count(*)");
    };
    for (int i = 0; i < 200 && running() == 0; ++i) {
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_GT(running(), 0) << "the read never started";
    const auto start = Clock::now();
    cancel->store(true);
    while (!call->done.load() && Clock::now() - start < 5s) {
        std::this_thread::sleep_for(2ms);
    }
    const auto waited = Clock::now() - start;
    ASSERT_TRUE(call->done.load());
    EXPECT_LT(waited, 500ms) << "the cancel did not end the wait promptly";
    ASSERT_TRUE(call->error);
    try {
        std::rethrow_exception(call->error);
    } catch (const Neo4jError& e) {
        EXPECT_EQ(e.code(), code::kCancelled);
    }
    // The server stops the read itself, at the latest at server_timeout_s.
    while (running() > 0 && Clock::now() - start < 10s) {
        std::this_thread::sleep_for(50ms);
    }
    const auto server_done = Clock::now() - start;
    EXPECT_EQ(running(), 0);
    EXPECT_LT(server_done, 4500ms);
    std::printf(
        "[neo4j-lookup-live] cancel: client wait ended after %lld ms, server read after "
        "%lld ms (server_timeout_s=3)\n",
        static_cast<long long>(
            std::chrono::duration_cast<std::chrono::milliseconds>(waited).count()),
        static_cast<long long>(
            std::chrono::duration_cast<std::chrono::milliseconds>(server_done).count()));
    pool.stop();
}

TEST_F(Neo4jLookupLive, AReplyPastTheByteBoundFails) {
    oracle_->run("CREATE (:" + label_ + " {id: 'k1'})");
    LiveLookup l(params("MATCH (n:" + label_ +
                            " {id: $key}) RETURN reduce(s = '', i IN range(1, 2000) | s + "
                            "'xxxxxxxxxx') AS name",
                        "name:VARCHAR",
                        {{"max_response_bytes", "4096"}}));
    l.op->open();
    const auto [c, m] = failure([&] { (void)l.run({"k1"}); });
    EXPECT_EQ(c, code::kResponseTooLarge) << m;
}
