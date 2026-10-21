// The Neo4j sink's recovery cells: a coordinator and two clink_node workers run
// a SQL job from a replayable Kafka topic (transfer events 1 to N) into the
// pinned Neo4j server, and each cell faults one layer while rows flow:
//
//   1. a worker parked at neo4j.before_request (a batch encoded, nothing of it
//      sent) is SIGKILLed;
//   2. a worker parked at neo4j.after_commit (the server reported the batch
//      committed, the barrier that asked for it not yet acknowledged) is
//      SIGKILLed, so the restored job replays rows the graph already holds;
//   3. the Neo4j server itself is stopped and started again while the job
//      runs, inside the sink's retry window.
//
// The statement is the replay-safe mapping: accounts and transfers keyed by
// stable ids under uniqueness constraints, a transfer's SENT and TO created
// only by the transaction that creates the transfer. The pass criteria, read
// back through the case's own client rather than the sink: every transfer
// exactly once, with its amount, one SENT from its sender and one TO its
// recipient; every account once; and the restarts and faults the cell names,
// witnessed while the job ran.
//
// Needs the server scripts/neo4j-live.sh starts (CLINK_NEO4J_TEST_ENDPOINT,
// CLINK_NEO4J_TEST_PASSWORD, and CLINK_NEO4J_TEST_CONTAINER for cell 3), Docker
// for the Kafka broker, and the clink_node and clink_submit_sql binaries of a
// build with fault injection. Each cell prints one "[neo4j-recovery]" line.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/connectors/kafka_message.hpp"
#include "clink/connectors/kafka_sink.hpp"
#include "clink/core/base64.hpp"
#include "clink/http_connector/http_request.hpp"

#include "tests/integration/cluster_harness.hpp"
#include "tests/integration/docker_command.hpp"
#include "tests/integration/docker_kafka.hpp"

namespace {

using namespace std::chrono_literals;
using clink::config::JsonValue;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::Process;
using clink::itest::ProcOptions;
using clink::itest::ScopedDiagnostics;
using clink::test::DockerKafka;

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

std::string env(const char* name) {
    const char* v = std::getenv(name);
    return v == nullptr ? std::string{} : std::string{v};
}

std::int64_t sender_of(std::int64_t e) {
    return e % 37;
}
std::int64_t recipient_of(std::int64_t e) {
    return (e * 7 + 3) % 37 == e % 37 ? (e + 1) % 37 : (e * 7 + 3) % 37;
}
std::int64_t amount_of(std::int64_t e) {
    return 100 + (e * 13) % 9900;
}

std::string event_json(std::int64_t e) {
    return "{\"event_id\":" + std::to_string(e) + ",\"sender\":\"acct-" +
           std::to_string(sender_of(e)) + "\",\"recipient\":\"acct-" +
           std::to_string(recipient_of(e)) + "\",\"amount\":" + std::to_string(amount_of(e)) + "}";
}

// Produces events 1, 2, 3, ... to a two-partition topic at a steady rate. The
// producer is idempotent, so a batch it retries is not written twice.
class Feeder {
public:
    Feeder(std::string brokers, std::string topic, std::int64_t limit, int per_second)
        : brokers_(std::move(brokers)),
          topic_(std::move(topic)),
          limit_(limit),
          per_tick_(std::max(per_second / 10, 1)) {}
    ~Feeder() { stop(); }
    void start() {
        thread_ = std::thread([this] { run(); });
    }
    std::int64_t stop() {
        stop_.store(true);
        if (thread_.joinable()) {
            thread_.join();
        }
        return produced_.load();
    }
    [[nodiscard]] const std::string& error() const { return error_; }

private:
    void run() {
        try {
            clink::KafkaSink::Options opts;
            opts.brokers = brokers_;
            opts.topic = topic_;
            opts.metric_prefix.clear();
            opts.acks = "all";
            opts.conf["enable.idempotence"] = "true";
            clink::KafkaSink sink(std::move(opts));
            sink.open();
            std::int64_t next = 1;
            while (!stop_.load() && next <= limit_) {
                clink::Batch<clink::KafkaMessage> batch;
                for (int i = 0; i < per_tick_ && next <= limit_; ++i, ++next) {
                    clink::KafkaMessage m{event_json(next)};
                    m.partition = static_cast<std::int32_t>(next % 2);
                    batch.emplace(std::move(m));
                }
                sink.on_data(batch);
                sink.flush();
                produced_.store(next - 1);
                std::this_thread::sleep_for(100ms);
            }
            sink.close();
        } catch (const std::exception& e) {
            error_ = e.what();
        }
    }
    std::string brokers_;
    std::string topic_;
    std::int64_t limit_;
    int per_tick_;
    std::thread thread_;
    std::atomic<bool> stop_{false};
    std::atomic<std::int64_t> produced_{0};
    std::string error_;
};

// A Cypher client of the case's own, plain JSON in and typed JSON out.
class Oracle {
public:
    Oracle(const std::string& endpoint, const std::string& password) {
        clink::http_connector::HttpRequest::Options o;
        o.base_url = endpoint;
        o.headers["Authorization"] = "Basic " + clink::base64_encode("neo4j:" + password);
        o.headers["Accept"] = "application/vnd.neo4j.query";
        o.rw_timeout_ms = 60000;
        http_ = std::make_unique<clink::http_connector::HttpRequest>(std::move(o));
    }
    std::vector<clink::config::JsonArray> rows(const std::string& statement) {
        std::string body = "{\"statement\":";
        JsonValue{statement}.serialize_into(body);
        body += "}";
        const auto r = http_->post("/db/neo4j/query/v2", body, "application/json");
        if (r.status == 0) {
            throw std::runtime_error("oracle: " + r.error);
        }
        const JsonValue reply = clink::config::parse(r.body);
        if (reply.contains("errors")) {
            throw std::runtime_error("oracle: " + r.body);
        }
        std::vector<clink::config::JsonArray> out;
        for (const auto& row : reply.at("data").at("values").as_array()) {
            out.push_back(row.as_array());
        }
        return out;
    }
    void run(const std::string& statement) { (void)rows(statement); }
    std::int64_t count(const std::string& statement) {
        try {
            return std::stoll(rows(statement).at(0).at(0).at("_value").as_string());
        } catch (const std::exception&) {
            return -1;
        }
    }

private:
    std::unique_ptr<clink::http_connector::HttpRequest> http_;
};

std::string text(const JsonValue& typed) {
    const auto& v = typed.at("_value");
    return v.is_string() ? v.as_string() : v.serialize(0);
}

class Neo4jRecovery : public ::testing::Test {
protected:
    static void TearDownTestSuite() { kafka_.reset(); }

    void SetUp() override {
        endpoint_ = env("CLINK_NEO4J_TEST_ENDPOINT");
        password_ = env("CLINK_NEO4J_TEST_PASSWORD");
        if (endpoint_.empty() || password_.empty()) {
            GTEST_SKIP() << "set CLINK_NEO4J_TEST_ENDPOINT and CLINK_NEO4J_TEST_PASSWORD "
                            "(scripts/neo4j-live.sh does)";
        }
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(sql_binary())) {
            GTEST_SKIP() << "the clink_node or clink_submit_sql binary is not built";
        }
        if (kafka_ == nullptr) {
            kafka_ = std::make_unique<DockerKafka>();
        }
        static int counter = 0;
        suffix_ = std::to_string(::getpid()) + "_" + std::to_string(++counter);
        account_ = "RAccount_" + suffix_;
        transfer_ = "RTransfer_" + suffix_;
        topic_ = "clink_neo4j_recovery_" + suffix_;
        kafka_->create_topic(topic_, 2);
        oracle_ = std::make_unique<Oracle>(endpoint_, password_);
        oracle_->run("CREATE CONSTRAINT c_" + account_ + " IF NOT EXISTS FOR (a:" + account_ +
                     ") REQUIRE a.id IS UNIQUE");
        oracle_->run("CREATE CONSTRAINT c_" + transfer_ + " IF NOT EXISTS FOR (t:" + transfer_ +
                     ") REQUIRE t.event_id IS UNIQUE");
    }

    void TearDown() override {
        if (!oracle_) {
            return;
        }
        try {
            oracle_->run("MATCH (n:" + account_ + ") DETACH DELETE n");
            oracle_->run("MATCH (n:" + transfer_ + ") DETACH DELETE n");
            oracle_->run("DROP CONSTRAINT c_" + account_ + " IF EXISTS");
            oracle_->run("DROP CONSTRAINT c_" + transfer_ + " IF EXISTS");
        } catch (const std::exception& e) {
            ADD_FAILURE() << "cleaning up failed: " << e.what();
        }
    }

    std::string statement() const {
        return "UNWIND $rows AS row "
               "MERGE (s:" +
               account_ +
               " {id: row.sender}) "
               "MERGE (r:" +
               account_ +
               " {id: row.recipient}) "
               "MERGE (t:" +
               transfer_ +
               " {event_id: row.event_id}) "
               "ON CREATE SET t.amount = row.amount, t._created = true "
               // Deliberately not replay-safe: how many times the row was
               // applied, so a cell can see that a replay happened.
               "SET t.writes = coalesce(t.writes, 0) + 1 "
               "WITH s, r, t "
               "CALL (s, r, t) { WITH s, r, t WHERE t._created "
               "CREATE (s)-[:SENT]->(t) CREATE (t)-[:TO]->(r) } "
               "REMOVE t._created";
    }

    std::string job_sql() const {
        const std::string cols =
            "(event_id BIGINT, sender VARCHAR, recipient VARCHAR, amount BIGINT)";
        return "CREATE TABLE src " + cols + " WITH (connector='kafka', format='json', brokers='" +
               kafka_->brokers() + "', topic='" + topic_ + "', group_id='" + topic_ +
               "', auto_offset_reset='earliest'); "
               "CREATE TABLE graph " +
               cols + " WITH (connector='neo4j', endpoint='" + endpoint_ +
               "', username='neo4j', password='env://CLINK_NEO4J_RECOVERY_PW', statement='" +
               statement() +
               "', batch_size='200', flush_interval_ms='200', request_timeout_ms='10000', "
               "retry_window_ms='120000'); INSERT INTO graph SELECT * FROM src;";
    }

    ProcOptions worker_opts(std::string fault = {}) const {
        ProcOptions o;
        o.fault = std::move(fault);
        o.env = {{"CLINK_NEO4J_RECOVERY_PW", password_}};
        return o;
    }

    std::unique_ptr<Cluster> make_cluster(const ProcOptions& w0, const ProcOptions& w1) {
        ClusterSpec spec;
        spec.node_binary = node_binary();
        spec.workers = 2;
        spec.slots_per_worker = 3;
        spec.http = true;
        auto cluster = std::make_unique<Cluster>(spec);
        if (!cluster->start_coordinator() || !cluster->start_worker(0, w0) ||
            !cluster->start_worker(1, w1) || !cluster->await_workers_registered(2)) {
            ADD_FAILURE() << "the cluster did not start";
        }
        return cluster;
    }

    void submit(Cluster& cluster) {
        Process submit;
        ASSERT_TRUE(submit.spawn("submit-sql",
                                 sql_binary(),
                                 {sql_binary().string(),
                                  "-e",
                                  job_sql(),
                                  "--coordinator-host",
                                  "127.0.0.1",
                                  "--coordinator-port",
                                  std::to_string(cluster.http_port()),
                                  "--name",
                                  "neo4j-recovery",
                                  "--checkpoint-dir",
                                  cluster.checkpoint_dir().string(),
                                  "--checkpoint-interval-ms",
                                  "1000",
                                  "--alignment",
                                  "aligned",
                                  "--max-restarts-on-worker-loss",
                                  "8",
                                  "--parallelism",
                                  "2"},
                                 cluster.log_dir()));
        const auto code = submit.await_exit(60s);
        ASSERT_TRUE(code.has_value());
        ASSERT_EQ(*code, 0) << submit.read_log();
    }

    std::vector<std::string> worker_logs(Cluster& cluster) const {
        std::vector<std::string> out = archived_logs_;
        for (std::size_t i = 0; i < 2; ++i) {
            out.push_back(cluster.worker(i).read_log());
        }
        return out;
    }

    std::size_t count_in_workers(Cluster& cluster, const std::string& needle) const {
        std::size_t n = 0;
        for (const auto& log : worker_logs(cluster)) {
            for (auto pos = log.find(needle); pos != std::string::npos;
                 pos = log.find(needle, pos + 1)) {
                ++n;
            }
        }
        return n;
    }

    void await_open(Cluster& cluster) {
        ASSERT_TRUE(clink::itest::await(
            [&] { return count_in_workers(cluster, "neo4j sink open:") >= 2; }, 90s))
            << "the sink did not open on both subtasks";
    }

    void kill_and_replace(Cluster& cluster, std::size_t idx) {
        cluster.worker(idx).kill_hard();
        ASSERT_TRUE(cluster.worker(idx).await_exit(10s).has_value());
        archived_logs_.push_back(cluster.worker(idx).read_log());
        ASSERT_TRUE(cluster.restart_worker(idx, worker_opts()));
    }

    static std::size_t restarts(const Cluster& cluster) {
        return cluster.count_in_coordinator_log("[coordinator.restart] [info] job_id=1 attempt=");
    }

    static std::uint64_t latest_completed(const Cluster& cluster) {
        std::uint64_t latest = 0;
        std::error_code ec;
        for (const auto& e :
             std::filesystem::directory_iterator(cluster.checkpoint_dir() / "_jobs" / "1", ec)) {
            const auto name = e.path().filename().string();
            if (name.rfind("COMPLETED-", 0) == 0) {
                try {
                    latest = std::max<std::uint64_t>(latest, std::stoull(name.substr(10)));
                } catch (const std::exception&) {
                }
            }
        }
        return latest;
    }

    // Every transfer has landed and two more checkpoints completed after it,
    // so nothing sent before is still on its way.
    void await_all_landed(Cluster& cluster, std::chrono::seconds timeout) {
        const std::string q = "MATCH (t:" + transfer_ + ") RETURN count(t)";
        ASSERT_TRUE(clink::itest::await([&] { return oracle_->count(q) >= expected_; }, timeout))
            << "only " << oracle_->count(q) << " of " << expected_ << " transfers landed";
        const auto after = latest_completed(cluster);
        ASSERT_TRUE(
            clink::itest::await([&] { return latest_completed(cluster) >= after + 2; }, 60s))
            << "checkpoints stopped completing after every transfer had landed";
    }

    static void cancel_job(const Cluster& cluster) {
        clink::http_connector::HttpRequest::Options o;
        o.base_url = "http://127.0.0.1:" + std::to_string(cluster.http_port());
        clink::http_connector::HttpRequest http(std::move(o));
        const auto r = http.post("/api/v1/jobs/1/cancel", "", "application/json");
        EXPECT_NE(r.body.find("\"ok\":true"), std::string::npos)
            << "the cancel was not accepted: " << r.body;
    }

    // The graph against the events: every transfer once with its amount, one
    // SENT from its sender and one TO its recipient, every account once.
    void expect_exact_graph() {
        std::set<std::string> accounts;
        for (std::int64_t e = 1; e <= expected_; ++e) {
            accounts.insert("acct-" + std::to_string(sender_of(e)));
            accounts.insert("acct-" + std::to_string(recipient_of(e)));
        }
        std::set<std::string> got_accounts;
        for (const auto& r : oracle_->rows("MATCH (a:" + account_ + ") RETURN a.id")) {
            EXPECT_TRUE(got_accounts.insert(text(r[0])).second)
                << "account " << text(r[0]) << " twice";
        }
        EXPECT_EQ(got_accounts, accounts);
        const auto rows = oracle_->rows(
            "MATCH (t:" + transfer_ +
            ") OPTIONAL MATCH (s)-[sent:SENT]->(t) "
            "OPTIONAL MATCH (t)-[to:TO]->(r) RETURN t.event_id, t.amount, collect(s.id), "
            "collect(r.id), count(DISTINCT sent), count(DISTINCT to) ORDER BY t.event_id");
        ASSERT_EQ(static_cast<std::int64_t>(rows.size()), expected_) << "transfers lost or doubled";
        std::size_t wrong = 0;
        for (std::size_t i = 0; i < rows.size(); ++i) {
            const auto& r = rows[i];
            const auto e = static_cast<std::int64_t>(i + 1);
            const bool ok =
                text(r[0]) == std::to_string(e) && text(r[1]) == std::to_string(amount_of(e)) &&
                r[2].at("_value").as_array().size() == 1 &&
                text(r[2].at("_value").as_array()[0]) == "acct-" + std::to_string(sender_of(e)) &&
                r[3].at("_value").as_array().size() == 1 &&
                text(r[3].at("_value").as_array()[0]) ==
                    "acct-" + std::to_string(recipient_of(e)) &&
                text(r[4]) == "1" && text(r[5]) == "1";
            if (!ok && ++wrong <= 5) {
                ADD_FAILURE() << "transfer row " << i << " is not event " << e << " as produced";
            }
        }
        EXPECT_EQ(wrong, 0U);
    }

    // Transfers whose row was applied more than once: by a replay, or by a
    // retry after an unknown outcome.
    std::int64_t applied_twice() {
        return oracle_->count("MATCH (t:" + transfer_ + ") WHERE t.writes > 1 RETURN count(t)");
    }

    void report(const char* cell, std::size_t restarts_spent, Cluster& cluster) {
        std::printf(
            "[neo4j-recovery] cell=%s produced=%lld restarts=%zu attempts_failed_lines=%zu "
            "outcome_unknown_lines=%zu transfers_applied_more_than_once=%lld\n",
            cell,
            static_cast<long long>(expected_),
            restarts_spent,
            count_in_workers(cluster, " failed ("),
            count_in_workers(cluster, "failed (unknown)"),
            static_cast<long long>(applied_twice()));
        std::fflush(stdout);
    }

    void kill_at(const std::string& point, const char* cell);

    static std::unique_ptr<DockerKafka> kafka_;
    std::string endpoint_;
    std::string password_;
    std::string suffix_;
    std::string account_;
    std::string transfer_;
    std::string topic_;
    std::int64_t expected_{0};
    std::unique_ptr<Oracle> oracle_;
    std::vector<std::string> archived_logs_;
};

std::unique_ptr<DockerKafka> Neo4jRecovery::kafka_;

void Neo4jRecovery::kill_at(const std::string& point, const char* cell) {
    // The fourth time worker 1 reaches the point it parks there and prints the
    // fault framework's witness line; the kill lands while it is parked.
    auto cluster = make_cluster(worker_opts(), worker_opts(point + "=block@4"));
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster);
    await_open(*cluster);
    expected_ = 4000;
    Feeder feed(kafka_->brokers(), topic_, expected_, 1000);
    feed.start();
    const std::string witness = "[fault.injection] fired: " + point;
    ASSERT_TRUE(clink::itest::await([&] { return cluster->worker(1).log_contains(witness); }, 120s))
        << "worker 1 never reached " << point;
    kill_and_replace(*cluster, 1);
    ASSERT_TRUE(cluster->await_workers_registered(3));
    await_all_landed(*cluster, 240s);
    EXPECT_EQ(feed.stop(), expected_) << feed.error();
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report(cell, spent, *cluster);
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    if (point == "neo4j.after_commit") {
        // The parked batch committed and its checkpoint never completed, so
        // the restored job applied its rows again: the fault engaged, and the
        // graph below must have absorbed it.
        EXPECT_GT(applied_twice(), 0) << "no committed row was replayed";
    }
    expect_exact_graph();
}

}  // namespace

TEST_F(Neo4jRecovery, AWorkerKilledBeforeItsRequestLosesNothing) {
    kill_at("neo4j.before_request", "before_request");
}

TEST_F(Neo4jRecovery, AWorkerKilledAfterACommitBeforeItsCheckpointReplaysIntoTheSameGraph) {
    kill_at("neo4j.after_commit", "after_commit");
}

TEST_F(Neo4jRecovery, AServerRestartWhileTheJobRunsIsRiddenOutWithoutARestart) {
    const auto container = env("CLINK_NEO4J_TEST_CONTAINER");
    if (container.empty()) {
        GTEST_SKIP() << "set CLINK_NEO4J_TEST_CONTAINER (scripts/neo4j-live.sh does)";
    }
    auto cluster = make_cluster(worker_opts(), worker_opts());
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster);
    await_open(*cluster);
    expected_ = 4000;
    Feeder feed(kafka_->brokers(), topic_, expected_, 1000);
    feed.start();
    const std::string q = "MATCH (t:" + transfer_ + ") RETURN count(t)";
    ASSERT_TRUE(clink::itest::await([&] { return oracle_->count(q) > 500; }, 120s));
    const auto stopped = clink::test::run_docker_command("docker stop -t 30 " + container);
    ASSERT_EQ(stopped.status, 0) << stopped.out;
    // The outage engages while the job runs: the sinks keep failing against
    // the stopped server, retrying, before it is started again.
    ASSERT_TRUE(
        clink::itest::await([&] { return count_in_workers(*cluster, " failed (") >= 4; }, 60s))
        << "the attempts did not keep failing while the server was down";
    const auto started = clink::test::run_docker_command("docker start " + container);
    ASSERT_EQ(started.status, 0) << started.out;
    await_all_landed(*cluster, 300s);
    EXPECT_EQ(feed.stop(), expected_) << feed.error();
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report("server_restart", spent, *cluster);
    EXPECT_EQ(spent, 0U) << "an outage inside the retry window spent a restart";
    expect_exact_graph();
}
