// The Neo4j connector's recovery cells. First the sink's: a coordinator and two
// clink_node workers run a SQL job from a replayable Kafka topic (transfer
// events 1 to N) into the pinned Neo4j server, and each cell faults one layer
// while rows flow:
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
// Then the lookup's (Neo4jLookupRecovery): the same cluster runs a SQL job that
// enriches each transfer's recipient from a reference graph seeded before the
// job starts (so no lookup depends on the job's own progress), through a
// LEFT JOIN on a function='neo4j' lookup table, into a Kafka topic. The cells
// prove that the enrichment arrives as the graph says, that a worker killed
// with lookups in flight loses no input, that a database outage longer than
// the lookup's retry budget fails the task and a restart finishes the job,
// that a replay after a graph change enriches from the graph as it is then,
// and that a lookup that fails never reaches the output as a no-match. The
// expected rows come from the reference graph's own definition, not from what
// the job wrote.
//
// Needs the server scripts/neo4j-live.sh starts (CLINK_NEO4J_TEST_ENDPOINT,
// CLINK_NEO4J_TEST_PASSWORD, and CLINK_NEO4J_TEST_CONTAINER for the outage
// cells), Docker for the Kafka broker, and the clink_node and clink_submit_sql
// binaries of a build with fault injection. Each cell prints one
// "[neo4j-recovery]" or "[neo4j-lookup-recovery]" line.

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
#include "clink/kafka/consume_all.hpp"

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

// Produces events first, first + 1, ... up to `limit` to a two-partition topic
// at a steady rate. The producer is idempotent, so a batch it retries is not
// written twice.
class Feeder {
public:
    Feeder(std::string brokers,
           std::string topic,
           std::int64_t limit,
           int per_second,
           std::int64_t first = 1)
        : brokers_(std::move(brokers)),
          topic_(std::move(topic)),
          limit_(limit),
          first_(first),
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
    // Waits until every event up to the limit has been produced.
    std::int64_t finish() {
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
            std::int64_t next = first_;
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
    std::int64_t first_;
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

namespace {

// Stops the case's Neo4j container and starts it again, and on the way out
// makes sure it is running and answering, whatever the case asserted, so the
// cases after it find the server up.
class ServerGuard {
public:
    explicit ServerGuard(std::string container) : container_(std::move(container)) {}
    ~ServerGuard() {
        if (stopped_) {
            start();
        }
    }
    ServerGuard(const ServerGuard&) = delete;
    ServerGuard& operator=(const ServerGuard&) = delete;
    void stop() {
        const auto r = clink::test::run_docker_command("docker stop -t 30 " + container_);
        EXPECT_EQ(r.status, 0) << r.out;
        stopped_ = true;
    }
    void start() {
        const auto r = clink::test::run_docker_command("docker start " + container_);
        EXPECT_EQ(r.status, 0) << r.out;
        stopped_ = false;
        Oracle probe(env("CLINK_NEO4J_TEST_ENDPOINT"), env("CLINK_NEO4J_TEST_PASSWORD"));
        EXPECT_TRUE(clink::itest::await([&] { return probe.count("RETURN 1") == 1; }, 120s))
            << "the server did not answer again after its restart";
    }

private:
    std::string container_;
    bool stopped_{false};
};

// The lookup's cells. The reference graph: an account node for every
// acct-<i>, i from 0 to 36, except those with i % 6 == 5, which have none and
// so are no-matches; acct-<i> has shared = i % 4 + bump and evidence
// = prefix + i.
class Neo4jLookupRecovery : public Neo4jRecovery {
protected:
    void SetUp() override {
        Neo4jRecovery::SetUp();
        if (IsSkipped() || HasFatalFailure()) {
            return;
        }
        ref_ = "RRef_" + suffix_;
        out_topic_ = topic_ + "_out";
        kafka_->create_topic(out_topic_, 1);
        oracle_->run("CREATE CONSTRAINT c_" + ref_ + " IF NOT EXISTS FOR (a:" + ref_ +
                     ") REQUIRE a.id IS UNIQUE");
        seed_reference("ev-", 0);
    }

    void TearDown() override {
        if (oracle_) {
            try {
                oracle_->run("MATCH (n:" + ref_ + ") DETACH DELETE n");
                oracle_->run("DROP CONSTRAINT c_" + ref_ + " IF EXISTS");
            } catch (const std::exception& e) {
                ADD_FAILURE() << "cleaning up the reference graph failed: " << e.what();
            }
        }
        Neo4jRecovery::TearDown();
    }

    void seed_reference(const std::string& prefix, int bump) {
        oracle_->run("UNWIND range(0, 36) AS i WITH i WHERE i % 6 <> 5 MERGE (a:" + ref_ +
                     " {id: 'acct-' + toString(i)}) SET a.shared = i % 4 + " +
                     std::to_string(bump) + ", a.evidence = '" + prefix + "' + toString(i)");
    }

    struct Expected {
        bool matched{false};
        std::int64_t shared{0};
        std::string evidence;
    };
    static Expected expected(std::int64_t e, const std::string& prefix, int bump) {
        const auto i = recipient_of(e);
        if (i % 6 == 5) {
            return {};
        }
        return {true, i % 4 + bump, prefix + std::to_string(i)};
    }

    std::string lookup_job_sql(bool exactly_once, const std::string& lookup_extra = {}) const {
        const std::string cols =
            "(event_id BIGINT, sender VARCHAR, recipient VARCHAR, amount BIGINT)";
        return "CREATE TABLE src " + cols + " WITH (connector='kafka', format='json', brokers='" +
               kafka_->brokers() + "', topic='" + topic_ + "', group_id='" + topic_ +
               "', auto_offset_reset='earliest'); "
               "CREATE TABLE ref (recipient VARCHAR, shared BIGINT, evidence VARCHAR, matched "
               "BOOLEAN, observed_at TIMESTAMP(3)) WITH (connector='lookup', function='neo4j', "
               "endpoint='" +
               endpoint_ +
               "', username='neo4j', password='env://CLINK_NEO4J_RECOVERY_PW', "
               "statement='MATCH (a:" +
               ref_ +
               " {id: $key}) RETURN a.shared AS shared, a.evidence AS evidence', "
               "matched_column='matched', observed_at_column='observed_at', "
               "max_concurrent_requests='4', max_in_flight='64'" +
               lookup_extra +
               "); "
               "CREATE TABLE out (event_id BIGINT, recipient VARCHAR, shared BIGINT, evidence "
               "VARCHAR, matched BOOLEAN, observed_at TIMESTAMP(3)) WITH (connector='kafka', "
               "format='json', brokers='" +
               kafka_->brokers() + "', topic='" + out_topic_ + "'" +
               (exactly_once ? ", delivery_guarantee='exactly_once', transactional_id='lookup-" +
                                   suffix_ + "'"
                             : std::string{}) +
               "); "
               "INSERT INTO out SELECT s.event_id, s.recipient, r.shared, r.evidence, r.matched, "
               "r.observed_at FROM src s LEFT JOIN ref r ON s.recipient = r.recipient;";
    }

    void submit_lookup(Cluster& cluster,
                       const std::string& sql,
                       std::uint64_t checkpoint_interval_ms,
                       std::uint32_t max_restarts) {
        Process submit;
        ASSERT_TRUE(submit.spawn("submit-sql",
                                 sql_binary(),
                                 {sql_binary().string(),
                                  "-e",
                                  sql,
                                  "--coordinator-host",
                                  "127.0.0.1",
                                  "--coordinator-port",
                                  std::to_string(cluster.http_port()),
                                  "--name",
                                  "neo4j-lookup-recovery",
                                  "--checkpoint-dir",
                                  cluster.checkpoint_dir().string(),
                                  "--checkpoint-interval-ms",
                                  std::to_string(checkpoint_interval_ms),
                                  "--alignment",
                                  "aligned",
                                  "--max-restarts-on-worker-loss",
                                  std::to_string(max_restarts),
                                  "--parallelism",
                                  "2"},
                                 cluster.log_dir()));
        const auto code = submit.await_exit(60s);
        ASSERT_TRUE(code.has_value());
        ASSERT_EQ(*code, 0) << submit.read_log();
    }

    void await_lookup_open(Cluster& cluster) {
        ASSERT_TRUE(clink::itest::await(
            [&] { return count_in_workers(cluster, "neo4j lookup open:") >= 2; }, 90s))
            << "the lookup did not open on both subtasks";
    }

    // Every record now in the output topic, parsed; empty while the topic
    // cannot be drained (a transaction still open pins it).
    std::vector<JsonValue> output() const {
        std::vector<JsonValue> rows;
        try {
            for (const auto& p :
                 clink::kafka::consume_all_committed(kafka_->brokers(), out_topic_, 20s)) {
                rows.push_back(clink::config::parse(p));
            }
        } catch (const std::exception&) {
            return {};
        }
        return rows;
    }

    static std::set<std::int64_t> event_ids(const std::vector<JsonValue>& rows) {
        std::set<std::int64_t> ids;
        for (const auto& r : rows) {
            ids.insert(r.at("event_id").as_int());
        }
        return ids;
    }

    // Waits until every event 1..expected_ is in the output; returns it.
    std::vector<JsonValue> await_every_event(std::chrono::seconds timeout) {
        std::vector<JsonValue> rows;
        const bool all = clink::itest::await(
            [&] {
                rows = output();
                return static_cast<std::int64_t>(event_ids(rows).size()) >= expected_;
            },
            timeout);
        EXPECT_TRUE(all) << "only " << event_ids(rows).size() << " of " << expected_
                         << " events reached the output";
        return rows;
    }

    // Whether `row` is event `e` enriched as the graph (prefix, bump) says.
    static bool enriched_as(const JsonValue& row, const std::string& prefix, int bump) {
        const auto e = row.at("event_id").as_int();
        const auto want = expected(e, prefix, bump);
        if (row.at("recipient").as_string() != "acct-" + std::to_string(recipient_of(e)) ||
            row.at("matched").as_bool() != want.matched || row.at("observed_at").is_null()) {
            return false;
        }
        if (!want.matched) {
            return row.at("shared").is_null() && row.at("evidence").is_null();
        }
        return row.at("shared").is_integral_number() && row.at("shared").as_int() == want.shared &&
               row.at("evidence").is_string() && row.at("evidence").as_string() == want.evidence;
    }

    // Every event exactly once, each enriched as the seeded graph says.
    void expect_each_event_once_as_seeded(const std::vector<JsonValue>& rows) {
        std::map<std::int64_t, int> seen;
        std::size_t wrong = 0;
        std::size_t no_match = 0;
        for (const auto& r : rows) {
            ++seen[r.at("event_id").as_int()];
            if (!enriched_as(r, "ev-", 0) && ++wrong <= 5) {
                ADD_FAILURE() << "a row not enriched as the graph says: " << r.serialize(0);
            }
            no_match += r.at("matched").as_bool() ? 0 : 1;
        }
        std::size_t want_no_match = 0;
        for (std::int64_t e = 1; e <= expected_; ++e) {
            want_no_match += expected(e, "ev-", 0).matched ? 0 : 1;
        }
        EXPECT_EQ(static_cast<std::int64_t>(seen.size()), expected_) << "events lost";
        EXPECT_EQ(rows.size(), seen.size()) << "events duplicated in the committed output";
        EXPECT_EQ(wrong, 0U);
        EXPECT_EQ(no_match, want_no_match)
            << "no-matches are the accounts the graph lacks, no more";
    }

    void report_lookup(const char* cell,
                       std::size_t restarts_spent,
                       Cluster& cluster,
                       std::size_t rows) {
        std::printf(
            "[neo4j-lookup-recovery] cell=%s produced=%lld output_rows=%zu restarts=%zu "
            "attempt_failed_lines=%zu\n",
            cell,
            static_cast<long long>(expected_),
            rows,
            restarts_spent,
            count_in_workers(cluster, "neo4j lookup: "));
        std::fflush(stdout);
    }

    std::string ref_;
    std::string out_topic_;
};

}  // namespace

TEST_F(Neo4jLookupRecovery, EveryEventIsEnrichedAsTheReferenceGraphSays) {
    auto cluster = make_cluster(worker_opts(), worker_opts());
    ScopedDiagnostics diagnostics(*cluster);
    submit_lookup(*cluster, lookup_job_sql(true), 1000, 4);
    await_lookup_open(*cluster);
    expected_ = 3000;
    Feeder feed(kafka_->brokers(), topic_, expected_, 1000);
    feed.start();
    const auto rows = await_every_event(240s);
    EXPECT_EQ(feed.stop(), expected_) << feed.error();
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report_lookup("expected", spent, *cluster, rows.size());
    EXPECT_EQ(spent, 0U);
    expect_each_event_once_as_seeded(rows);
}

TEST_F(Neo4jLookupRecovery, AWorkerKilledWithLookupsInFlightLosesNoInput) {
    // The hundredth time a request thread on worker 1 has an answer, it parks
    // there before handing it over: that lookup, and those queued behind it,
    // are in flight when the worker dies.
    const std::string point = "neo4j.lookup_after_reply";
    auto cluster = make_cluster(worker_opts(), worker_opts(point + "=block@100"));
    ScopedDiagnostics diagnostics(*cluster);
    submit_lookup(*cluster, lookup_job_sql(true), 1000, 8);
    await_lookup_open(*cluster);
    expected_ = 4000;
    Feeder feed(kafka_->brokers(), topic_, expected_, 1000);
    feed.start();
    const std::string witness = "[fault.injection] fired: " + point;
    ASSERT_TRUE(clink::itest::await([&] { return cluster->worker(1).log_contains(witness); }, 120s))
        << "worker 1 never parked a lookup";
    kill_and_replace(*cluster, 1);
    ASSERT_TRUE(cluster->await_workers_registered(3));
    const auto rows = await_every_event(300s);
    EXPECT_EQ(feed.stop(), expected_) << feed.error();
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report_lookup("killed_in_flight", spent, *cluster, rows.size());
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    expect_each_event_once_as_seeded(rows);
}

TEST_F(Neo4jLookupRecovery, AnOutageInsideTheRetryBudgetIsRiddenOutWithoutARestart) {
    const auto container = env("CLINK_NEO4J_TEST_CONTAINER");
    if (container.empty()) {
        GTEST_SKIP() << "set CLINK_NEO4J_TEST_CONTAINER (scripts/neo4j-live.sh does)";
    }
    ServerGuard guard(container);
    auto cluster = make_cluster(worker_opts(), worker_opts());
    ScopedDiagnostics diagnostics(*cluster);
    // A budget longer than the server takes to restart: each lookup in flight
    // waits it out, and the operator holds its input meanwhile.
    submit_lookup(*cluster,
                  lookup_job_sql(false,
                                 ", max_retries='60', retry_window_ms='120000', "
                                 "request_timeout_ms='3000', connect_timeout_ms='1000'"),
                  1000,
                  4);
    await_lookup_open(*cluster);
    // Phase 1, the server up: the first events are all enriched.
    constexpr std::int64_t kBefore = 300;
    expected_ = kBefore;
    Feeder before(kafka_->brokers(), topic_, kBefore, 300);
    before.start();
    EXPECT_EQ(before.finish(), kBefore) << before.error();
    (void)await_every_event(120s);
    // Phase 2, the server down: the next events arrive while every read fails.
    guard.stop();
    expected_ = 600;
    Feeder during(kafka_->brokers(), topic_, expected_, 300, kBefore + 1);
    during.start();
    EXPECT_EQ(during.finish(), expected_) << during.error();
    ASSERT_TRUE(clink::itest::await(
        [&] { return count_in_workers(*cluster, "neo4j lookup: database") >= 4; }, 60s))
        << "the lookups did not keep failing while the server was down";
    // Nothing of phase 2 came out while the server was down: its rows are held
    // in flight, not written as no-matches; and no restart was spent.
    const auto held = output();
    EXPECT_EQ(static_cast<std::int64_t>(event_ids(held).size()), kBefore)
        << "rows left the lookup while the server was down";
    EXPECT_EQ(restarts(*cluster), 0U);
    // Phase 3, the server back: every event once, as the graph says.
    guard.start();
    const auto rows = await_every_event(300s);
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report_lookup("outage_ridden_out", spent, *cluster, rows.size());
    EXPECT_EQ(spent, 0U) << "an outage inside the retry budget spent a restart";
    // No restart, so no replay: each event once, as the graph says.
    expect_each_event_once_as_seeded(rows);
}

TEST_F(Neo4jLookupRecovery, AnOutagePastTheRetryBudgetFailsTheJobAndWritesNoNoMatch) {
    const auto container = env("CLINK_NEO4J_TEST_CONTAINER");
    if (container.empty()) {
        GTEST_SKIP() << "set CLINK_NEO4J_TEST_CONTAINER (scripts/neo4j-live.sh does)";
    }
    ServerGuard guard(container);
    auto cluster = make_cluster(worker_opts(), worker_opts());
    ScopedDiagnostics diagnostics(*cluster);
    // One retry within two seconds, and three restarts: the outage outlasts
    // both, so the job must end FAILED rather than enrich without the graph.
    submit_lookup(*cluster,
                  lookup_job_sql(true,
                                 ", max_retries='1', retry_window_ms='2000', "
                                 "request_timeout_ms='2000', connect_timeout_ms='1000'"),
                  1000,
                  3);
    await_lookup_open(*cluster);
    // Phase 1, the server up: the first events are all enriched and committed.
    constexpr std::int64_t kBefore = 300;
    expected_ = kBefore;
    Feeder before(kafka_->brokers(), topic_, kBefore, 300);
    before.start();
    EXPECT_EQ(before.finish(), kBefore) << before.error();
    (void)await_every_event(120s);
    // Phase 2, the server down: the next events' lookups fail past their
    // budget on every restart the job is allowed.
    guard.stop();
    Feeder during(kafka_->brokers(), topic_, 600, 300, kBefore + 1);
    during.start();
    EXPECT_EQ(during.finish(), 600) << during.error();
    ASSERT_TRUE(clink::itest::await(
        [&] { return cluster->count_in_coordinator_log("[coordinator.complete]") >= 1; }, 120s))
        << "the job did not end while the server was down";
    const auto spent = restarts(*cluster);
    EXPECT_EQ(spent, 3U) << "each restart met the outage, until the budget was spent";
    EXPECT_GE(cluster->count_in_coordinator_log("job_id=1 failed"), 1U);
    EXPECT_GE(count_in_workers(*cluster, "neo4j lookup: database"), 1U);
    guard.start();
    const auto rows = output();
    report_lookup("outage_past_budget", spent, *cluster, rows.size());
    // The committed output is phase 1, exactly, as the graph says: no event
    // read while the server was down reached it, as a no-match or otherwise.
    std::set<std::int64_t> ids;
    std::size_t wrong = 0;
    for (const auto& r : rows) {
        EXPECT_TRUE(ids.insert(r.at("event_id").as_int()).second) << "a committed duplicate";
        if (!enriched_as(r, "ev-", 0) && ++wrong <= 5) {
            ADD_FAILURE() << "a row not enriched as the graph says: " << r.serialize(0);
        }
    }
    EXPECT_EQ(wrong, 0U);
    std::set<std::int64_t> want;
    for (std::int64_t e = 1; e <= kBefore; ++e) {
        want.insert(e);
    }
    EXPECT_EQ(ids, want) << "the committed output is not exactly the events read before the outage";
}

TEST_F(Neo4jLookupRecovery, AReplayAfterAGraphChangeEnrichesFromTheGraphAsItIsThen) {
    // The job's first checkpoint is taken as soon as it is up, before any
    // event exists, and the interval keeps the next one beyond the cell. So
    // the restore after the kill starts from the beginning of the topic and
    // replays every event, and the output is at-least-once: each event's
    // first enrichment stays in it beside the one the replay produces.
    auto cluster = make_cluster(worker_opts(), worker_opts());
    ScopedDiagnostics diagnostics(*cluster);
    submit_lookup(*cluster, lookup_job_sql(false), 600000, 4);
    await_lookup_open(*cluster);
    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(*cluster) >= 1; }, 60s))
        << "the job's first checkpoint did not complete";
    expected_ = 600;
    Feeder feed(kafka_->brokers(), topic_, expected_, 1000);
    feed.start();
    (void)await_every_event(120s);
    EXPECT_EQ(feed.stop(), expected_) << feed.error();
    ASSERT_EQ(latest_completed(*cluster), 1U)
        << "a checkpoint after the first completed; the replay would be partial";
    // Phase 1 is complete and seen: every event enriched from graph A.
    seed_reference("ev2-", 10);
    const auto changed_at = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
    kill_and_replace(*cluster, 1);
    ASSERT_TRUE(cluster->await_workers_registered(3));
    std::vector<JsonValue> rows;
    const bool replayed = clink::itest::await(
        [&] {
            rows = output();
            std::set<std::int64_t> b;
            for (const auto& r : rows) {
                if (r.at("observed_at").as_int() >= changed_at) {
                    b.insert(r.at("event_id").as_int());
                }
            }
            return static_cast<std::int64_t>(b.size()) >= expected_;
        },
        180s);
    EXPECT_TRUE(replayed) << "the replay did not enrich every event again";
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report_lookup("graph_change_replay", spent, *cluster, rows.size());
    EXPECT_EQ(spent, 1U);
    // Each event twice or more: before the change as graph A, after it as
    // graph B, and never anything else. The event id alone does not say which
    // enrichment is current; observed_at does.
    std::map<std::int64_t, std::pair<int, int>> by_event;
    std::size_t wrong = 0;
    for (const auto& r : rows) {
        const auto e = r.at("event_id").as_int();
        const bool after = r.at("observed_at").as_int() >= changed_at;
        if (!enriched_as(r, after ? "ev2-" : "ev-", after ? 10 : 0) && ++wrong <= 5) {
            ADD_FAILURE() << "a row not enriched as the graph was when it was read: "
                          << r.serialize(0);
        }
        auto& [a, b] = by_event[e];
        (after ? b : a) += 1;
    }
    EXPECT_EQ(wrong, 0U);
    ASSERT_EQ(static_cast<std::int64_t>(by_event.size()), expected_);
    for (const auto& [e, counts] : by_event) {
        EXPECT_GE(counts.first, 1) << "event " << e << " has no enrichment from before the change";
        EXPECT_GE(counts.second, 1) << "event " << e << " has no enrichment from after the change";
    }
}

TEST_F(Neo4jLookupRecovery, ALookupThatFailsFailsTheJobAndNeverWritesANoMatch) {
    // acct-13's shared is text, which its BIGINT column cannot hold: every
    // lookup for it fails. Event 12 is the first sent to acct-13.
    oracle_->run("MATCH (a:" + ref_ + " {id: 'acct-13'}) SET a.shared = 'text-not-a-number'");
    auto cluster = make_cluster(worker_opts(), worker_opts());
    ScopedDiagnostics diagnostics(*cluster);
    submit_lookup(*cluster, lookup_job_sql(false), 1000, 2);
    await_lookup_open(*cluster);
    expected_ = 200;
    Feeder feed(kafka_->brokers(), topic_, expected_, 200);
    feed.start();
    ASSERT_TRUE(clink::itest::await(
        [&] { return cluster->count_in_coordinator_log("[coordinator.complete]") >= 1; }, 180s))
        << "the job did not end";
    (void)feed.stop();
    const auto spent = restarts(*cluster);
    EXPECT_EQ(spent, 2U) << "the failure recurred on every restart the budget allowed";
    EXPECT_GE(cluster->count_in_coordinator_log("job_id=1 failed"), 1U);
    EXPECT_GE(count_in_workers(*cluster, "column 'shared' (BIGINT): the statement returned String"),
              1U);
    EXPECT_EQ(count_in_workers(*cluster, "text-not-a-number"), 0U) << "a value reached a log";
    const auto rows = output();
    report_lookup("failing_lookup", spent, *cluster, rows.size());
    std::size_t wrong = 0;
    for (const auto& r : rows) {
        EXPECT_NE(r.at("recipient").as_string(), "acct-13")
            << "a failed lookup reached the output: " << r.serialize(0);
        if (!enriched_as(r, "ev-", 0) && ++wrong <= 5) {
            ADD_FAILURE() << "a row not enriched as the graph says: " << r.serialize(0);
        }
    }
    EXPECT_EQ(wrong, 0U);
}
