// The Neo4j sink against a real server: the pinned build in
// docker/integration-services.yml, started by scripts/neo4j-live.sh. Every case
// skips unless its server is named, and the gate script fails on any skip:
//
//   CLINK_NEO4J_TEST_ENDPOINT     http://localhost:17474 (the neo4j service)
//   CLINK_NEO4J_TEST_PASSWORD     the neo4j user's password
//   CLINK_NEO4J_TEST_CONTAINER    that service's container, for the cases that
//                                 stop and start the server
//   CLINK_NEO4J_TEST_TLS_ENDPOINT https://localhost:17443 (the neo4j-tls service)
//   CLINK_NEO4J_TEST_TLS_CA       the CA that signed its certificate
//   CLINK_NEO4J_TEST_TLS_WRONG_CA a CA that did not
//
// Each case writes under labels of its own and removes them, and its
// constraints, when it ends. What a case asserts about the graph it reads back
// through its own client (Oracle below), not through the sink: the nodes,
// relationships and properties, compared with what the case's input implies.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <httplib.h>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/row.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#include "neo4j_sink.hpp"
#include "options.hpp"
#include "tests/integration/docker_command.hpp"
#include "tests/integration/tcp_fault_proxy.hpp"

using clink::config::JsonArray;
using clink::config::JsonObject;
using clink::config::JsonValue;
using clink::itest::TcpFaultProxy;
using clink::neo4j::Neo4jSink;
using clink::neo4j::Neo4jSinkError;
using clink::neo4j::parse_options;
using clink::sql::Row;
using namespace std::chrono_literals;
namespace code = clink::neo4j::code;

namespace {

std::string env(const char* name) {
    const char* v = std::getenv(name);
    return v == nullptr ? std::string{} : std::string{v};
}

// host:port of an http(s)://host:port endpoint.
std::pair<std::string, int> host_port(const std::string& endpoint) {
    const auto scheme_end = endpoint.find("://");
    const auto rest = endpoint.substr(scheme_end + 3);
    const auto colon = rest.rfind(':');
    return {rest.substr(0, colon), std::stoi(rest.substr(colon + 1))};
}

// A Cypher client of the case's own, over the Query API with plain JSON
// parameters and typed results: independent of the sink's batching and
// encoding.
class Oracle {
public:
    Oracle(std::string endpoint, std::string password, std::string ca_file = {})
        : endpoint_(std::move(endpoint)),
          password_(std::move(password)),
          ca_file_(std::move(ca_file)) {}

    // The typed rows of `statement` against `database`; throws on any error.
    std::vector<JsonArray> rows(const std::string& statement,
                                const std::string& params_json = "{}",
                                const std::string& database = "neo4j") const {
        httplib::Client cli(endpoint_);
        if (!ca_file_.empty()) {
            cli.set_ca_cert_path(ca_file_);
        }
        cli.set_basic_auth("neo4j", password_);
        cli.set_read_timeout(std::chrono::seconds{60});
        std::string body = "{\"statement\":";
        JsonValue{statement}.serialize_into(body);
        body += ",\"parameters\":" + params_json + "}";
        const auto res = cli.Post("/db/" + database + "/query/v2",
                                  httplib::Headers{{"Accept", "application/vnd.neo4j.query"}},
                                  body,
                                  "application/json");
        if (!res) {
            throw std::runtime_error("oracle: " + httplib::to_string(res.error()));
        }
        const JsonValue reply = clink::config::parse(res->body);
        if (reply.contains("errors")) {
            throw std::runtime_error("oracle: " + res->body);
        }
        std::vector<JsonArray> out;
        for (const auto& r : reply.at("data").at("values").as_array()) {
            out.push_back(r.as_array());
        }
        return out;
    }

    void run(const std::string& statement,
             const std::string& params_json = "{}",
             const std::string& database = "neo4j") const {
        (void)rows(statement, params_json, database);
    }

    std::int64_t count(const std::string& statement) const {
        const auto r = rows(statement);
        return std::stoll(r.at(0).at(0).at("_value").as_string());
    }

    bool reachable() const {
        try {
            run("RETURN 1");
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }

private:
    std::string endpoint_;
    std::string password_;
    std::string ca_file_;
};

std::string text(const JsonValue& typed) {
    const auto& v = typed.at("_value");
    return v.is_string() ? v.as_string() : v.serialize(0);
}

Row row_of(std::vector<std::pair<std::string, JsonValue>> cells) {
    JsonObject obj;
    for (auto& [k, v] : cells) {
        obj[k] = std::move(v);
    }
    Row r;
    r.values = clink::sql::row_columns_from_json(std::move(obj));
    return r;
}

// A sink wired the way its runner wires it, with metrics and a cancel signal
// the case controls.
struct LiveSink {
    LiveSink(const std::map<std::string, std::string>& params, std::uint32_t subtask = 0) {
        sink = std::make_unique<Neo4jSink>(parse_options(params, subtask, 2));
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{7000 + subtask}, "neo4j_sink", nullptr, &metrics);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        sink->attach_runtime(ctx.get());
    }
    ~LiveSink() {
        if (sink) {
            sink->close();
            sink->attach_runtime(nullptr);
        }
    }
    void send(const std::vector<Row>& rows) {
        clink::Batch<Row> b;
        for (const auto& r : rows) {
            b.emplace(r);
        }
        sink->on_data(b);
    }
    void barrier(std::uint64_t id) {
        sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{id}});
    }

    clink::MetricsRegistry metrics;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
    std::unique_ptr<Neo4jSink> sink;
};

std::string refusal_code(const std::function<void()>& f, std::string* message = nullptr) {
    try {
        f();
    } catch (const Neo4jSinkError& e) {
        if (message != nullptr) {
            *message = e.what();
        }
        return e.code();
    }
    return {};
}

class Neo4jLive : public ::testing::Test {
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
        tag_ = "t" + std::to_string(::getpid()) + "_" + std::to_string(++counter);
    }

    void TearDown() override {
        if (!oracle_) {
            return;
        }
        try {
            for (const auto& label : labels_) {
                oracle_->run("MATCH (n:" + label + ") DETACH DELETE n");
            }
            for (const auto& c : constraints_) {
                oracle_->run("DROP CONSTRAINT " + c + " IF EXISTS");
            }
        } catch (const std::exception& e) {
            ADD_FAILURE() << "cleaning up failed: " << e.what();
        }
    }

    // A label of this case's own, removed when it ends.
    std::string label(const std::string& base) {
        labels_.push_back(base + "_" + tag_);
        return labels_.back();
    }

    void unique_constraint(const std::string& label, const std::string& property) {
        const std::string name = "c_" + label + "_" + property;
        oracle_->run("CREATE CONSTRAINT " + name + " IF NOT EXISTS FOR (n:" + label +
                     ") REQUIRE n." + property + " IS UNIQUE");
        constraints_.push_back(name);
    }

    std::map<std::string, std::string> params(const std::string& statement,
                                              const std::string& column_types,
                                              std::map<std::string, std::string> extra = {}) const {
        std::map<std::string, std::string> p = {{"endpoint", endpoint_},
                                                {"username", "neo4j"},
                                                {"password", password_},
                                                {"statement", statement},
                                                {"sql_column_types", column_types},
                                                {"flush_interval_ms", "0"}};
        for (auto& [k, v] : extra) {
            p[k] = v;
        }
        return p;
    }

    // Wait until the server answers again after a restart.
    void await_server(std::chrono::seconds limit) const {
        const auto until = std::chrono::steady_clock::now() + limit;
        while (std::chrono::steady_clock::now() < until) {
            if (oracle_->reachable()) {
                return;
            }
            std::this_thread::sleep_for(500ms);
        }
        FAIL() << "the server did not come back within " << limit.count() << " s";
    }

    std::string endpoint_;
    std::string password_;
    std::string tag_;
    std::unique_ptr<Oracle> oracle_;
    std::vector<std::string> labels_;
    std::vector<std::string> constraints_;
};

std::vector<Row> account_rows(std::int64_t from, std::int64_t to) {
    std::vector<Row> out;
    for (auto id = from; id < to; ++id) {
        out.push_back(
            row_of({{"id", JsonValue{id}}, {"name", JsonValue{"acct-" + std::to_string(id)}}}));
    }
    return out;
}

// id -> name of every node with `label`.
std::map<std::int64_t, std::string> nodes_by_id(const Oracle& o, const std::string& label) {
    std::map<std::int64_t, std::string> out;
    for (const auto& r : o.rows("MATCH (n:" + label + ") RETURN n.id, n.name ORDER BY n.id")) {
        const auto id = std::stoll(text(r[0]));
        EXPECT_FALSE(out.contains(id)) << "two nodes carry id " << id;
        out[id] = text(r[1]);
    }
    return out;
}

std::map<std::int64_t, std::string> expected_accounts(std::int64_t from, std::int64_t to) {
    std::map<std::int64_t, std::string> out;
    for (auto id = from; id < to; ++id) {
        out[id] = "acct-" + std::to_string(id);
    }
    return out;
}

}  // namespace

// ---- writes, batching, types --------------------------------------------------

TEST_F(Neo4jLive, BatchesAndAPartialBatchLandEveryRowOnce) {
    const auto acct = label("Account");
    unique_constraint(acct, "id");
    LiveSink s(
        params("UNWIND $rows AS row MERGE (a:" + acct + " {id: row.id}) SET a.name = row.name",
               "id:BIGINT;name:VARCHAR",
               {{"batch_size", "100"}}));
    s.sink->open();
    s.send(account_rows(0, 250));
    EXPECT_EQ(s.sink->counts().batches_committed, 2U) << "two full batches as the rows arrived";
    EXPECT_EQ(oracle_->count("MATCH (a:" + acct + ") RETURN count(a)"), 200);
    s.barrier(1);
    EXPECT_EQ(s.sink->counts().batches_committed, 3U) << "the barrier sent the partial batch";
    EXPECT_EQ(nodes_by_id(*oracle_, acct), expected_accounts(0, 250));
    EXPECT_EQ(s.metrics.counter("clink_neo4j_rows_committed_total{op_id=\"7000\"}").value(), 250U);
}

TEST_F(Neo4jLive, EveryTypeRoundTripsThroughTheServer) {
    const auto probe = label("TypeProbe");
    const std::string statement =
        "UNWIND $rows AS row CREATE (n:" + probe +
        " {id: row.id, ti: row.ti, si: row.si, i: row.i, bi: row.bi, r: row.r, d: row.d, b: row.b, "
        "s: row.s, day: row.day, ts: row.ts, tstz: row.tstz, xs: row.xs, nested_first: "
        "row.nested[0][1], nested_size: size(row.nested), mk: row.m.k, rx: row.rec.x, ry: "
        "row.rec.y})";
    const std::string types =
        "id:BIGINT;ti:TINYINT;si:SMALLINT;i:INTEGER;bi:BIGINT;r:REAL;d:DOUBLE;b:BOOLEAN;s:VARCHAR;"
        "day:DATE;ts:TIMESTAMP(3);tstz:TIMESTAMP(3) WITH TIME ZONE;xs:BIGINT ARRAY;"
        "nested:BIGINT ARRAY ARRAY;m:MAP<VARCHAR, VARCHAR>;rec:ROW<x BIGINT, y VARCHAR>";
    LiveSink s(params(statement, types));
    s.sink->open();
    const std::string tricky = "quote\" back\\ nl\n \xC3\xA9\xF0\x9F\x99\x82 end";
    s.send({row_of({{"id", JsonValue{std::int64_t{1}}},
                    {"ti", JsonValue{std::int64_t{-128}}},
                    {"si", JsonValue{std::int64_t{32767}}},
                    {"i", JsonValue{std::int64_t{-2147483648}}},
                    {"bi", JsonValue{std::int64_t{9007199254740993}}},
                    {"r", JsonValue{0.1}},
                    {"d", JsonValue{std::nan("")}},
                    {"b", JsonValue{true}},
                    {"s", JsonValue{tricky}},
                    {"day", JsonValue{std::int64_t{19782}}},
                    {"ts", JsonValue{std::int64_t{1700000000123}}},
                    {"tstz", JsonValue{std::int64_t{-1}}},
                    {"xs",
                     JsonValue{JsonArray{JsonValue{std::int64_t{-9223372036854775807 - 1}},
                                         JsonValue{std::int64_t{9223372036854775807}}}}},
                    {"nested",
                     JsonValue{JsonArray{JsonValue{
                         JsonArray{JsonValue{std::int64_t{1}}, JsonValue{std::int64_t{2}}}}}}},
                    {"m", JsonValue{JsonObject{{"k", JsonValue{std::string("v")}}}}},
                    {"rec",
                     JsonValue{JsonObject{{"x", JsonValue{std::int64_t{5}}},
                                          {"y", JsonValue{std::string("why")}}}}}}),
            row_of({{"id", JsonValue{std::int64_t{2}}},
                    {"d", JsonValue{-std::numeric_limits<double>::infinity()}},
                    {"r", JsonValue{std::numeric_limits<double>::max()}},
                    {"bi", JsonValue{std::numeric_limits<std::int64_t>::min()}}})});
    s.barrier(1);
    const auto got = oracle_->rows(
        "MATCH (n:" + probe +
        ") RETURN n.id, n.ti, n.si, n.i, n.bi, n.r, n.d, n.b, n.s, n.day, n.ts, n.tstz, n.xs, "
        "n.nested_first, n.nested_size, n.mk, n.rx, n.ry ORDER BY n.id");
    ASSERT_EQ(got.size(), 2U);
    const auto& a = got[0];
    const std::vector<std::pair<std::string, std::string>> want = {
        {"Integer", "1"},
        {"Integer", "-128"},
        {"Integer", "32767"},
        {"Integer", "-2147483648"},
        {"Integer", "9007199254740993"},
        {"Float", "0.1"},
        {"Float", "NaN"},
        {"Boolean", "true"},
        {"String", tricky},
        {"Date", "2024-02-29"},
        {"LocalDateTime", "2023-11-14T22:13:20.123"},
        {"OffsetDateTime", "1969-12-31T23:59:59.999Z"},
    };
    for (std::size_t i = 0; i < want.size(); ++i) {
        EXPECT_EQ(a[i].at("$type").as_string(), want[i].first) << "column " << i;
        EXPECT_EQ(text(a[i]), want[i].second) << "column " << i;
    }
    const auto& xs = a[12].at("_value").as_array();
    ASSERT_EQ(xs.size(), 2U);
    EXPECT_EQ(text(xs[0]), "-9223372036854775808");
    EXPECT_EQ(text(xs[1]), "9223372036854775807");
    EXPECT_EQ(text(a[13]), "2");
    EXPECT_EQ(text(a[14]), "1");
    EXPECT_EQ(text(a[15]), "v");
    EXPECT_EQ(text(a[16]), "5");
    EXPECT_EQ(text(a[17]), "why");
    const auto& b = got[1];
    EXPECT_EQ(text(b[6]), "-Infinity");
    EXPECT_EQ(std::strtod(text(b[5]).c_str(), nullptr), std::numeric_limits<double>::max());
    EXPECT_EQ(text(b[4]), "-9223372036854775808");
    // A null is no property at all.
    EXPECT_EQ(b[1].at("$type").as_string(), "Null");
    EXPECT_EQ(b[8].at("$type").as_string(), "Null");
}

// ---- refusals -----------------------------------------------------------------

TEST_F(Neo4jLive, InvalidCypherIsRefusedAtOpenNamingTheServerError) {
    LiveSink s(params("UNWIND $rows AS row CREAT (n {id: row.id})", "id:BIGINT"));
    std::string m;
    EXPECT_EQ(refusal_code([&] { s.sink->open(); }, &m), code::kStatementRefused);
    EXPECT_NE(m.find("Neo.ClientError.Statement.SyntaxError"), std::string::npos) << m;
    EXPECT_NE(m.find("database 'neo4j'"), std::string::npos) << m;
}

TEST_F(Neo4jLive, StatementsAgainstTheContractAreRefusedByTheServersOwnPlan) {
    const auto acct = label("Account");
    for (const std::string& statement : {
             "UNWIND $rows AS row MERGE (a:" + acct + " {id: row.id}) RETURN count(a) AS c",
             "UNWIND $rows AS row CALL (row) { MERGE (a:" + acct +
                 " {id: row.id}) } IN TRANSACTIONS OF 10 ROWS",
             "UNWIND $rows AS row MATCH (a:" + acct +
                 " {id: row.id}) WITH a WHERE a.id < 0 SET a.x = 1 "
                 "WITH 1 AS one MATCH (z:" +
                 acct + ") WHERE z.id = $other SET z.y = 2",
         }) {
        SCOPED_TRACE(statement);
        LiveSink s(params(statement, "id:BIGINT"));
        std::string m;
        EXPECT_EQ(refusal_code([&] { s.sink->open(); }, &m), code::kStatementRefused) << m;
    }
    LiveSink read_only(params(
        "UNWIND $rows AS row MATCH (a:" + acct + " {id: row.id}) WITH a SKIP 0 LIMIT 0 SET a.x = 1",
        "id:BIGINT"));
    EXPECT_NO_THROW(read_only.sink->open()) << "a statement that writes is accepted";
    LiveSink reads(
        params("UNWIND $rows AS row MATCH (a:" + acct + " {id: row.id}) WITH count(a) AS c FINISH",
               "id:BIGINT"));
    std::string m;
    EXPECT_EQ(refusal_code([&] { reads.sink->open(); }, &m), code::kStatementRefused);
    EXPECT_NE(m.find("only reads"), std::string::npos) << m;
}

TEST_F(Neo4jLive, AWrongPasswordIsRefusedAsAuthenticationWithoutQuotingIt) {
    auto p = params("UNWIND $rows AS row MERGE (a:X {id: row.id})", "id:BIGINT");
    p["password"] = "not-the-password-31f9";
    LiveSink s(p);
    std::string m;
    EXPECT_EQ(refusal_code([&] { s.sink->open(); }, &m), code::kAuthentication);
    EXPECT_NE(m.find("Neo.ClientError.Security.Unauthorized"), std::string::npos) << m;
    EXPECT_EQ(m.find("not-the-password-31f9"), std::string::npos) << m;
}

TEST_F(Neo4jLive, AUserNotAllowedOnTheDatabaseIsRefusedAsAuthorisation) {
    // Community has no roles; a user whose password must change first is the
    // refusal it does have for credentials that are valid but not allowed.
    const std::string user = "clink_expired_" + tag_;
    oracle_->run(
        "CREATE USER " + user + " IF NOT EXISTS SET PASSWORD 'expired-pw-0042' CHANGE REQUIRED",
        "{}",
        "system");
    auto p = params("UNWIND $rows AS row MERGE (a:X {id: row.id})", "id:BIGINT");
    p["username"] = user;
    p["password"] = "expired-pw-0042";
    LiveSink s(p);
    std::string m;
    EXPECT_EQ(refusal_code([&] { s.sink->open(); }, &m), code::kAuthorisation);
    EXPECT_NE(m.find("Neo.ClientError.Security.CredentialsExpired"), std::string::npos) << m;
    oracle_->run("DROP USER " + user + " IF EXISTS", "{}", "system");
}

TEST_F(Neo4jLive, AFailingBatchRollsBackEveryRowOfIt) {
    const auto acct = label("Account");
    unique_constraint(acct, "id");
    LiveSink s(params("UNWIND $rows AS row CREATE (a:" + acct + " {id: row.id, name: row.name})",
                      "id:BIGINT;name:VARCHAR"));
    s.sink->open();
    s.send(account_rows(0, 10));
    s.barrier(1);
    // The second batch's last row repeats an id from the first.
    auto second = account_rows(10, 20);
    second.push_back(account_rows(3, 4).front());
    s.send(second);
    std::string m;
    EXPECT_EQ(refusal_code([&] { s.barrier(2); }, &m), code::kServerRefused);
    EXPECT_NE(m.find("ConstraintValidationFailed"), std::string::npos) << m;
    EXPECT_EQ(m.find("acct-3"), std::string::npos) << "a row value leaked: " << m;
    EXPECT_EQ(nodes_by_id(*oracle_, acct), expected_accounts(0, 10))
        << "none of the refused batch's rows was kept";
    EXPECT_EQ(s.sink->buffered_rows(), 11U);
}

// ---- checkpoints --------------------------------------------------------------

namespace {

struct Step {
    bool barrier{false};
    std::int64_t from{0};
    std::int64_t to{0};
    std::uint64_t checkpoint{0};
};

class ScriptedRowSource final : public clink::Source<Row> {
public:
    explicit ScriptedRowSource(std::vector<Step> steps) : steps_(std::move(steps)) {}
    bool produce(clink::Emitter<Row>& out) override {
        if (cancelled() || next_ >= steps_.size()) {
            return false;
        }
        const Step& s = steps_[next_++];
        if (s.barrier) {
            return out.emit_barrier(clink::CheckpointBarrier{clink::CheckpointId{s.checkpoint}});
        }
        clink::Batch<Row> b;
        for (const auto& r : account_rows(s.from, s.to)) {
            b.emplace(r);
        }
        return out.emit_data(std::move(b));
    }

    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

private:
    std::vector<Step> steps_;
    std::size_t next_{0};
};

}  // namespace

TEST_F(Neo4jLive, ABarrierWhoseFlushFailsDoesNotCompleteItsCheckpoint) {
    const auto acct = label("Account");
    unique_constraint(acct, "id");
    // Rows 0-9, checkpoint 1, rows 10-19 and row 5 again, checkpoint 2: the
    // second interval's batch breaks the constraint and is refused.
    std::vector<Step> steps = {
        {false, 0, 10, 0}, {true, 0, 0, 1}, {false, 10, 20, 0}, {false, 5, 6, 0}, {true, 0, 0, 2}};
    clink::Dag dag;
    const auto src = dag.add_source<Row>(std::make_shared<ScriptedRowSource>(steps));
    dag.add_sink<Row>(
        src,
        std::make_shared<Neo4jSink>(parse_options(
            params("UNWIND $rows AS row CREATE (a:" + acct + " {id: row.id, name: row.name})",
                   "id:BIGINT;name:VARCHAR"),
            0,
            1)));
    std::mutex mu;
    std::vector<std::pair<std::uint64_t, bool>> acks;
    clink::JobConfig config;
    config.state_backend = std::make_shared<clink::InMemoryStateBackend>();
    config.on_checkpoint_ack = [&](clink::CheckpointId id, bool ok, const std::string&) {
        const std::lock_guard<std::mutex> lock(mu);
        acks.emplace_back(id.value(), ok);
    };
    clink::LocalExecutor exec(std::move(dag), config);
    exec.run();
    const auto errors = exec.operator_errors();
    ASSERT_FALSE(errors.empty()) << "the refused batch did not fail the job";
    EXPECT_NE(errors.front().second.find("neo4j sink"), std::string::npos) << errors.front().second;
    {
        const std::lock_guard<std::mutex> lock(mu);
        bool first_ok = false;
        for (const auto& [id, ok] : acks) {
            if (id == 1 && ok) {
                first_ok = true;
            }
            EXPECT_FALSE(id == 2 && ok) << "checkpoint 2 completed with its interval refused";
        }
        EXPECT_TRUE(first_ok) << "checkpoint 1, whose batch committed, was not acknowledged";
    }
    EXPECT_EQ(nodes_by_id(*oracle_, acct), expected_accounts(0, 10));
}

// ---- outages, lost replies, cancellation -----------------------------------------

TEST_F(Neo4jLive, AServerRestartWithinTheRetryWindowIsRiddenOut) {
    const auto container = env("CLINK_NEO4J_TEST_CONTAINER");
    if (container.empty()) {
        GTEST_SKIP() << "set CLINK_NEO4J_TEST_CONTAINER (scripts/neo4j-live.sh does)";
    }
    const auto acct = label("Account");
    unique_constraint(acct, "id");
    LiveSink s(
        params("UNWIND $rows AS row MERGE (a:" + acct + " {id: row.id}) SET a.name = row.name",
               "id:BIGINT;name:VARCHAR",
               {{"retry_window_ms", "180000"}, {"request_timeout_ms", "10000"}}));
    s.sink->open();
    s.send(account_rows(0, 50));
    s.barrier(1);
    s.send(account_rows(50, 100));
    const auto stopped = clink::test::run_docker_command("docker stop -t 30 " + container);
    ASSERT_EQ(stopped.status, 0) << stopped.out;
    std::atomic<bool> done{false};
    std::string failure;
    std::thread flusher([&] {
        try {
            s.barrier(2);
        } catch (const std::exception& e) {
            failure = e.what();
        }
        done.store(true);
    });
    // The fault engages while the job is active: attempts fail before the
    // server is back.
    const auto failed_before = [&] { return s.sink->counts().attempts_failed; };
    for (int i = 0; i < 100 && failed_before() == 0; ++i) {
        std::this_thread::sleep_for(100ms);
    }
    EXPECT_GT(failed_before(), 0U) << "no attempt failed while the server was down";
    EXPECT_FALSE(done.load()) << "the barrier returned while the server was down";
    const auto started = clink::test::run_docker_command("docker start " + container);
    ASSERT_EQ(started.status, 0) << started.out;
    flusher.join();
    EXPECT_TRUE(failure.empty()) << failure;
    await_server(120s);
    EXPECT_EQ(nodes_by_id(*oracle_, acct), expected_accounts(0, 100));
    EXPECT_GE(s.sink->counts().retries, 1U);
}

TEST_F(Neo4jLive, AnOutageLongerThanTheRetryWindowFailsTheBarrier) {
    const auto acct = label("Account");
    const auto [host, port] = host_port(endpoint_);
    TcpFaultProxy proxy(static_cast<std::uint16_t>(port), host == "localhost" ? "127.0.0.1" : host);
    auto p = params("UNWIND $rows AS row MERGE (a:" + acct + " {id: row.id}) SET a.name = row.name",
                    "id:BIGINT;name:VARCHAR",
                    {{"retry_window_ms", "1500"}});
    p["endpoint"] = "http://127.0.0.1:" + std::to_string(proxy.port());
    LiveSink s(p);
    s.sink->open();
    s.send(account_rows(0, 20));
    proxy.set_refusing(true);
    const auto start = std::chrono::steady_clock::now();
    EXPECT_EQ(refusal_code([&] { s.barrier(1); }), code::kRetriesExhausted);
    EXPECT_LT(std::chrono::steady_clock::now() - start, 15s);
    EXPECT_GE(proxy.refused(), 1U) << "the outage did not engage";
    EXPECT_GE(s.sink->counts().attempts_failed, 2U);
    EXPECT_EQ(s.sink->counts().outcome_unknown, 0U)
        << "a refused connection is a request never sent, not an unknown outcome";
    proxy.set_refusing(false);
    EXPECT_EQ(oracle_->count("MATCH (a:" + acct + ") RETURN count(a)"), 0);
    EXPECT_EQ(s.sink->buffered_rows(), 20U);
}

namespace {

// Hold the reply to the next batch request, wait until the server has
// committed the batch (its rows are visible to another client), then reset
// the connection: a commit whose reply is lost.
void lose_the_reply_after_the_commit(TcpFaultProxy& proxy,
                                     LiveSink& s,
                                     const Oracle& oracle,
                                     const std::string& count_query,
                                     std::int64_t committed_count,
                                     std::string& failure) {
    proxy.hold_at_upload(1);
    std::thread flusher([&] {
        try {
            s.barrier(1);
        } catch (const std::exception& e) {
            failure = e.what();
        }
    });
    bool witnessed = false;
    for (int i = 0; i < 300 && !witnessed; ++i) {
        witnessed = proxy.uploads_held() == 1 && oracle.count(count_query) == committed_count;
        if (!witnessed) {
            std::this_thread::sleep_for(50ms);
        }
    }
    EXPECT_TRUE(witnessed) << "the batch was not committed while its reply was held";
    proxy.drop_held();
    flusher.join();
}

}  // namespace

TEST_F(Neo4jLive, ALostReplyAfterACommitIsRetriedAndAMergeConverges) {
    const auto acct = label("Account");
    unique_constraint(acct, "id");
    const auto [host, port] = host_port(endpoint_);
    TcpFaultProxy proxy(static_cast<std::uint16_t>(port), host == "localhost" ? "127.0.0.1" : host);
    proxy.set_request_marker("POST /db/");
    auto p = params("UNWIND $rows AS row MERGE (a:" + acct + " {id: row.id}) SET a.name = row.name",
                    "id:BIGINT;name:VARCHAR");
    p["endpoint"] = "http://127.0.0.1:" + std::to_string(proxy.port());
    LiveSink s(p);
    s.sink->open();
    s.send(account_rows(0, 40));
    std::string failure;
    lose_the_reply_after_the_commit(
        proxy, s, *oracle_, "MATCH (a:" + acct + ") RETURN count(a)", 40, failure);
    EXPECT_TRUE(failure.empty()) << failure;
    EXPECT_EQ(s.sink->counts().outcome_unknown, 1U);
    EXPECT_EQ(s.sink->counts().rows_maybe_applied_twice, 40U);
    EXPECT_EQ(s.sink->counts().batches_committed, 1U);
    EXPECT_EQ(nodes_by_id(*oracle_, acct), expected_accounts(0, 40))
        << "the batch, applied twice, should have converged";
}

TEST_F(Neo4jLive, ALostReplyAfterACommitDuplicatesWhatACreateWrites) {
    // The honest half: a statement that is not safe to replay is applied twice.
    const auto acct = label("Account");
    const auto [host, port] = host_port(endpoint_);
    TcpFaultProxy proxy(static_cast<std::uint16_t>(port), host == "localhost" ? "127.0.0.1" : host);
    proxy.set_request_marker("POST /db/");
    auto p = params("UNWIND $rows AS row CREATE (a:" + acct + " {id: row.id, name: row.name})",
                    "id:BIGINT;name:VARCHAR");
    p["endpoint"] = "http://127.0.0.1:" + std::to_string(proxy.port());
    LiveSink s(p);
    s.sink->open();
    s.send(account_rows(0, 10));
    std::string failure;
    lose_the_reply_after_the_commit(
        proxy, s, *oracle_, "MATCH (a:" + acct + ") RETURN count(a)", 10, failure);
    EXPECT_TRUE(failure.empty()) << failure;
    EXPECT_EQ(s.sink->counts().outcome_unknown, 1U);
    const auto per_id = oracle_->rows("MATCH (a:" + acct + ") RETURN a.id, count(*) ORDER BY a.id");
    ASSERT_EQ(per_id.size(), 10U);
    for (const auto& r : per_id) {
        EXPECT_EQ(text(r[1]), "2") << "id " << text(r[0]) << " was not written twice";
    }
}

TEST_F(Neo4jLive, ACancelWhileTheServerIsUnresponsiveIsBounded) {
    const auto acct = label("Account");
    const auto [host, port] = host_port(endpoint_);
    TcpFaultProxy proxy(static_cast<std::uint16_t>(port), host == "localhost" ? "127.0.0.1" : host);
    auto p = params("UNWIND $rows AS row MERGE (a:" + acct + " {id: row.id})",
                    "id:BIGINT",
                    {{"request_timeout_ms", "120000"}, {"retry_window_ms", "300000"}});
    p["endpoint"] = "http://127.0.0.1:" + std::to_string(proxy.port());
    LiveSink s(p);
    s.sink->open();
    s.send(account_rows(0, 5));
    proxy.set_blackhole(true);
    std::thread canceller([&] {
        std::this_thread::sleep_for(1s);
        s.cancel->store(true);
    });
    const auto start = std::chrono::steady_clock::now();
    EXPECT_EQ(refusal_code([&] { s.barrier(1); }), code::kCancelled);
    const auto took = std::chrono::steady_clock::now() - start;
    canceller.join();
    EXPECT_GE(took, 900ms) << "the barrier returned before the cancel";
    EXPECT_LT(took, 4s) << "the cancel waited on the unresponsive server";
    proxy.set_blackhole(false);
}

// ---- replay-safe mapping ------------------------------------------------------

namespace {

struct Transfer {
    std::int64_t event_id;
    std::int64_t sender;
    std::int64_t recipient;
    std::int64_t amount;
};

Row transfer_row(const Transfer& t) {
    return row_of({{"event_id", JsonValue{t.event_id}},
                   {"sender", JsonValue{t.sender}},
                   {"recipient", JsonValue{t.recipient}},
                   {"amount", JsonValue{t.amount}}});
}

// Accounts and transfers keyed by stable ids under uniqueness constraints. A
// transfer's relationships are created only by the transaction that creates
// its node, which the constraint lets exactly one transaction do, so a replay
// or a concurrent duplicate finds the node and creates nothing.
std::string transfer_statement(const std::string& account, const std::string& transfer) {
    return "UNWIND $rows AS row "
           "MERGE (s:" +
           account +
           " {id: row.sender}) "
           "MERGE (r:" +
           account +
           " {id: row.recipient}) "
           "MERGE (t:" +
           transfer +
           " {event_id: row.event_id}) "
           "ON CREATE SET t.amount = row.amount, t._created = true "
           "WITH s, r, t "
           "CALL (s, r, t) { WITH s, r, t WHERE t._created "
           "  CREATE (s)-[:SENT]->(t) CREATE (t)-[:TO]->(r) } "
           "REMOVE t._created";
}

}  // namespace

TEST_F(Neo4jLive, AReplaySafeMappingConvergesUnderReplayAndConcurrentWriters) {
    const auto acct = label("Account");
    const auto xfer = label("Transfer");
    unique_constraint(acct, "id");
    unique_constraint(xfer, "event_id");
    std::vector<Transfer> transfers;
    std::mt19937_64 rng(7);
    for (std::int64_t e = 1; e <= 300; ++e) {
        const std::int64_t s = static_cast<std::int64_t>(rng() % 25);
        std::int64_t r = static_cast<std::int64_t>(rng() % 25);
        if (r == s) {
            r = (r + 1) % 25;
        }
        transfers.push_back({e, s, r, static_cast<std::int64_t>(100 + rng() % 9900)});
    }
    const auto p = params(transfer_statement(acct, xfer),
                          "event_id:BIGINT;sender:BIGINT;recipient:BIGINT;amount:BIGINT",
                          {{"batch_size", "37"}});
    // Two writers, as two subtasks would be, each sending every event, in a
    // different order and batching, at the same time; then one replays a run.
    LiveSink w1(p, 0);
    LiveSink w2(p, 1);
    w1.sink->open();
    w2.sink->open();
    auto shuffled = transfers;
    std::shuffle(shuffled.begin(), shuffled.end(), rng);
    std::thread a([&] {
        std::vector<Row> rows;
        for (const auto& t : transfers) {
            rows.push_back(transfer_row(t));
        }
        w1.send(rows);
        w1.barrier(1);
    });
    std::thread b([&] {
        std::vector<Row> rows;
        for (const auto& t : shuffled) {
            rows.push_back(transfer_row(t));
        }
        w2.send(rows);
        w2.barrier(1);
    });
    a.join();
    b.join();
    std::vector<Row> replay;
    for (std::size_t i = 100; i < 200; ++i) {
        replay.push_back(transfer_row(transfers[i]));
        replay.push_back(transfer_row(transfers[i]));  // a duplicate inside one batch too
    }
    w1.send(replay);
    w1.barrier(2);

    // The oracle: every transfer once, with its amount, one SENT from its
    // sender and one TO its recipient; every account once.
    std::set<std::int64_t> accounts;
    for (const auto& t : transfers) {
        accounts.insert(t.sender);
        accounts.insert(t.recipient);
    }
    const auto acct_rows = oracle_->rows("MATCH (a:" + acct + ") RETURN a.id ORDER BY a.id");
    std::set<std::int64_t> got_accounts;
    for (const auto& r : acct_rows) {
        EXPECT_TRUE(got_accounts.insert(std::stoll(text(r[0]))).second)
            << "an account appears twice";
    }
    EXPECT_EQ(got_accounts, accounts);
    const auto rows = oracle_->rows(
        "MATCH (t:" + xfer +
        ") "
        "OPTIONAL MATCH (s)-[sent:SENT]->(t) OPTIONAL MATCH (t)-[to:TO]->(r) "
        "RETURN t.event_id, t.amount, collect(DISTINCT s.id), collect(DISTINCT r.id), "
        "count(DISTINCT sent), count(DISTINCT to), t._created ORDER BY t.event_id");
    ASSERT_EQ(rows.size(), transfers.size());
    for (std::size_t i = 0; i < rows.size(); ++i) {
        const auto& r = rows[i];
        const auto& t = transfers[i];
        EXPECT_EQ(text(r[0]), std::to_string(t.event_id));
        EXPECT_EQ(text(r[1]), std::to_string(t.amount));
        ASSERT_EQ(r[2].at("_value").as_array().size(), 1U) << "event " << t.event_id;
        EXPECT_EQ(text(r[2].at("_value").as_array()[0]), std::to_string(t.sender));
        ASSERT_EQ(r[3].at("_value").as_array().size(), 1U) << "event " << t.event_id;
        EXPECT_EQ(text(r[3].at("_value").as_array()[0]), std::to_string(t.recipient));
        EXPECT_EQ(text(r[4]), "1") << "event " << t.event_id << " has more than one SENT";
        EXPECT_EQ(text(r[5]), "1") << "event " << t.event_id << " has more than one TO";
        EXPECT_EQ(r[6].at("$type").as_string(), "Null") << "the marker property was left behind";
    }
    EXPECT_EQ(oracle_->count("MATCH (:" + acct + ")-[x:SENT]->(:" + xfer + ") RETURN count(x)"),
              static_cast<std::int64_t>(transfers.size()));
    EXPECT_EQ(oracle_->count("MATCH (:" + xfer + ")-[x:TO]->(:" + acct + ") RETURN count(x)"),
              static_cast<std::int64_t>(transfers.size()));
}

TEST_F(Neo4jLive, AStaleEventNeverOverwritesANewerPropertyEvenWithConcurrentWriters) {
    const auto acct = label("Account");
    unique_constraint(acct, "id");
    // The newest event per account wins, by (at, event_id). The statement takes
    // the node's write lock (SET a._lock) before it reads the stored position,
    // so two writers cannot both read the old one and overwrite each other.
    // Cypher runs each clause over every row before the next, so two rows of
    // one batch for one account would both compare with the stored value and
    // the later row would win, newer or not: the batch is first reduced to the
    // newest row per account.
    const std::string statement =
        "UNWIND $rows AS row "
        "WITH row ORDER BY row.at DESC, row.event_id DESC "
        "WITH row.id AS id, collect(row)[0] AS row "
        "MERGE (a:" +
        acct +
        " {id: id}) "
        "SET a._lock = true "
        "WITH a, row, (a.risk_at IS NULL OR row.at > a.risk_at OR "
        "(row.at = a.risk_at AND row.event_id > a.risk_event)) AS newer "
        "SET a.risk = CASE WHEN newer THEN row.risk ELSE a.risk END, "
        "a.risk_at = CASE WHEN newer THEN row.at ELSE a.risk_at END, "
        "a.risk_event = CASE WHEN newer THEN row.event_id ELSE a.risk_event END "
        "REMOVE a._lock";
    const std::string types = "id:BIGINT;event_id:BIGINT;at:BIGINT;risk:BIGINT";
    struct Update {
        std::int64_t id, event_id, at, risk;
    };
    std::vector<Update> updates;
    std::mt19937_64 rng(11);
    for (std::int64_t e = 1; e <= 400; ++e) {
        updates.push_back({static_cast<std::int64_t>(rng() % 10),
                           e,
                           static_cast<std::int64_t>(rng() % 50),  // ties on purpose
                           static_cast<std::int64_t>(rng() % 1000)});
    }
    std::map<std::int64_t, Update> newest;
    for (const auto& u : updates) {
        auto it = newest.find(u.id);
        if (it == newest.end() ||
            std::tie(u.at, u.event_id) > std::tie(it->second.at, it->second.event_id)) {
            newest[u.id] = u;
        }
    }
    const auto row_for = [](const Update& u) {
        return row_of({{"id", JsonValue{u.id}},
                       {"event_id", JsonValue{u.event_id}},
                       {"at", JsonValue{u.at}},
                       {"risk", JsonValue{u.risk}}});
    };
    const auto p = params(statement, types, {{"batch_size", "16"}});
    LiveSink w1(p, 0);
    LiveSink w2(p, 1);
    w1.sink->open();
    w2.sink->open();
    std::thread a([&] {
        std::vector<Row> rows;
        for (std::size_t i = 0; i < updates.size(); i += 2) {
            rows.push_back(row_for(updates[i]));
        }
        w1.send(rows);
        w1.barrier(1);
    });
    std::thread b([&] {
        std::vector<Row> rows;
        for (std::size_t i = updates.size(); i-- > 1;) {
            if (i % 2 == 1) {
                rows.push_back(row_for(updates[i]));
            }
        }
        w2.send(rows);
        w2.barrier(1);
    });
    a.join();
    b.join();
    // Then a replay of the oldest half, after the newer values are in.
    std::vector<Row> stale;
    for (std::size_t i = 0; i < updates.size() / 2; ++i) {
        stale.push_back(row_for(updates[i]));
    }
    w1.send(stale);
    w1.barrier(2);

    const auto rows =
        oracle_->rows("MATCH (a:" + acct +
                      ") RETURN a.id, a.risk, a.risk_at, a.risk_event, a._lock ORDER BY a.id");
    ASSERT_EQ(rows.size(), newest.size());
    for (const auto& r : rows) {
        const auto& want = newest.at(std::stoll(text(r[0])));
        EXPECT_EQ(text(r[1]), std::to_string(want.risk)) << "account " << want.id;
        EXPECT_EQ(text(r[2]), std::to_string(want.at)) << "account " << want.id;
        EXPECT_EQ(text(r[3]), std::to_string(want.event_id)) << "account " << want.id;
        EXPECT_EQ(r[4].at("$type").as_string(), "Null") << "the lock property was left behind";
    }
    const auto deadlocks = w1.sink->counts().retries + w2.sink->counts().retries;
    std::printf("[neo4j-live] concurrent writers: %llu retried batch(es)\n",
                static_cast<unsigned long long>(deadlocks));
}

// ---- TLS ----------------------------------------------------------------------

class Neo4jLiveTls : public Neo4jLive {
protected:
    void SetUp() override {
        Neo4jLive::SetUp();
        if (IsSkipped()) {
            return;
        }
        tls_endpoint_ = env("CLINK_NEO4J_TEST_TLS_ENDPOINT");
        ca_ = env("CLINK_NEO4J_TEST_TLS_CA");
        wrong_ca_ = env("CLINK_NEO4J_TEST_TLS_WRONG_CA");
        if (tls_endpoint_.empty() || ca_.empty() || wrong_ca_.empty()) {
            GTEST_SKIP() << "set CLINK_NEO4J_TEST_TLS_ENDPOINT, CLINK_NEO4J_TEST_TLS_CA and "
                            "CLINK_NEO4J_TEST_TLS_WRONG_CA (scripts/neo4j-live.sh does)";
        }
    }

    std::map<std::string, std::string> tls_params(const std::string& endpoint,
                                                  std::map<std::string, std::string> extra) {
        auto p = params("UNWIND $rows AS row MERGE (a:TlsProbe_" + tag_ + " {id: row.id})",
                        "id:BIGINT",
                        std::move(extra));
        p["endpoint"] = endpoint;
        return p;
    }

    std::string tls_endpoint_;
    std::string ca_;
    std::string wrong_ca_;
};

TEST_F(Neo4jLiveTls, TheConfiguredCaVerifiesTheServerAndARowLands) {
    // The TLS service is a server of its own: read it back, and clean it up,
    // through a client that trusts the same CA.
    const Oracle tls_oracle(tls_endpoint_, password_, ca_);
    LiveSink s(tls_params(tls_endpoint_, {{"tls_ca_file", ca_}}));
    s.sink->open();
    s.send({row_of({{"id", JsonValue{std::int64_t{1}}}})});
    s.barrier(1);
    EXPECT_EQ(s.sink->counts().rows_committed, 1U);
    EXPECT_EQ(tls_oracle.count("MATCH (n:TlsProbe_" + tag_ + " {id: 1}) RETURN count(n)"), 1);
    tls_oracle.run("MATCH (n:TlsProbe_" + tag_ + ") DETACH DELETE n");
}

TEST_F(Neo4jLiveTls, ACaThatDidNotSignTheCertificateIsRefused) {
    LiveSink s(tls_params(tls_endpoint_, {{"tls_ca_file", wrong_ca_}}));
    std::string m;
    EXPECT_EQ(refusal_code([&] { s.sink->open(); }, &m), code::kTlsVerify) << m;
    LiveSink system_store(tls_params(tls_endpoint_, {}));
    EXPECT_EQ(refusal_code([&] { system_store.sink->open(); }), code::kTlsVerify)
        << "the system's trust store does not know the test CA";
}

TEST_F(Neo4jLiveTls, AHostTheCertificateDoesNotNameIsRefusedBeforeAnythingIsSent) {
    // The certificate names localhost and no address, and the CA is the right
    // one: only the hostname check stands between the sink and the server.
    const auto [host, port] = host_port(tls_endpoint_);
    TcpFaultProxy proxy(static_cast<std::uint16_t>(port));
    proxy.set_request_marker("POST /db/");
    LiveSink s(
        tls_params("https://127.0.0.1:" + std::to_string(proxy.port()), {{"tls_ca_file", ca_}}));
    std::string m;
    EXPECT_EQ(refusal_code([&] { s.sink->open(); }, &m), code::kTlsVerify) << m;
    EXPECT_GE(proxy.accepted(), 1U) << "the connection was not even attempted";
}
