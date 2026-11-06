// The MongoDB connector's recovery cells: a coordinator and two clink_node
// workers run a job graph that reads a collection's change stream
// (mongo_cdc_source) and writes it into MongoDB, while a writer changes the
// source collection. Each cell faults one layer while changes flow:
//
//   1. a worker parked at mongodb.sink_before_write (a batch checked, nothing
//      of it sent) is SIGKILLed;
//   2. a worker parked at mongodb.sink_after_write (MongoDB acknowledged the
//      batch, the barrier that asked for it not yet acknowledged) is
//      SIGKILLed, so the restored job writes documents the collection already
//      holds;
//   3. a worker parked at mongodb.source_after_emit (change events handed to
//      the job, no checkpoint covering them yet) is SIGKILLed, so the restored
//      source reads them again from its checkpointed resume token;
//   4. the MongoDB server is stopped and started again while the job runs;
//   5. inserts, updates and deletes are replicated by key (mongo_upsert_sink)
//      into a mirror, with a worker parked after a write killed;
//   6. a worker parked at the source's first emission, before any checkpoint
//      of the job completed, is SIGKILLed: the job restarts from no
//      checkpoint, and the source resumes where the run's stream started;
//   7. cell 5's mirror as a SQL job (connector='mongodb' tables, mode='upsert'),
//      submitted by clink_submit_sql, with a worker parked after a write
//      killed.
//
// Cells 1 to 4 and 6 copy inserts with the insert sink. Cells 1 to 3, 5 and 7 let
// a checkpoint complete before the writer starts, so the kill restores from
// one. Each kill lands while records are in flight: when the worker parks, the
// source collection holds documents the destination does not. The pass
// criteria, read back through the case's
// own client rather than anything clink reports: the destination holds exactly
// one document per inserted source document, equal to it plus the change
// metadata (cells 1-4), or the mirror holds every surviving source document as
// the source holds it and a delete's tombstone for each deleted one (cells 5
// and 7, which also check that the mirror had not converged at the kill);
// and the restarts and the faults each cell names, witnessed in the logs while
// the job ran.
//
// Needs the replica set scripts/mongodb-live.sh starts (CLINK_MONGODB_TEST_URI,
// and CLINK_MONGODB_TEST_CONTAINER for the outage cell), and the clink_node
// binary of a build with fault injection (and clink_submit_sql for cell 7).
// Each cell prints one "[mongodb-recovery]" line.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <gtest/gtest.h>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/uri.hpp>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/http_connector/http_request.hpp"

#include "../checkpoint_markers.hpp"
#include "tests/integration/cluster_harness.hpp"
#include "tests/integration/docker_command.hpp"

namespace {

using namespace std::chrono_literals;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::ProcOptions;
using clink::itest::ScopedDiagnostics;

std::filesystem::path recovery_node_binary() {
#ifdef CLINK_NODE_BINARY
    return std::filesystem::path{CLINK_NODE_BINARY};
#else
    return {};
#endif
}

std::string env_or_empty(const char* name) {
    const char* v = std::getenv(name);
    return v == nullptr ? std::string{} : std::string{v};
}

mongocxx::client verifier_client(const std::string& uri) {
    static std::once_flag once;
    std::call_once(once, [] { new mongocxx::instance{}; });  // process lifetime
    return mongocxx::client{mongocxx::uri{uri}};
}

// A numeric field as an integer, whichever BSON width it was stored with: a
// value travels the string channel as relaxed Extended JSON, which writes an
// int64 that fits 32 bits as a plain number, read back as int32.
std::int64_t as_int(const bsoncxx::document::element& e) {
    switch (e.type()) {
        case bsoncxx::type::k_int32:
            return e.get_int32().value;
        case bsoncxx::type::k_int64:
            return e.get_int64().value;
        default:
            return -1;
    }
}

std::int32_t value_of(std::int64_t i) {
    return static_cast<std::int32_t>((i * 7919) % 100000);
}

// Inserts documents first, first + 1, ... up to `limit` into the source
// collection at a steady rate, from its own client.
class Writer {
public:
    Writer(std::string uri, std::string coll, std::int64_t limit, int per_second)
        : uri_(std::move(uri)),
          coll_(std::move(coll)),
          limit_(limit),
          per_tick_(std::max(per_second / 20, 1)) {}
    ~Writer() { stop(); }
    void start() {
        thread_ = std::thread([this] { run(); });
    }
    std::int64_t stop() {
        stop_.store(true);
        if (thread_.joinable()) {
            thread_.join();
        }
        return written_.load();
    }
    [[nodiscard]] std::int64_t written() const { return written_.load(); }
    [[nodiscard]] const std::string& error() const { return error_; }
    [[nodiscard]] bool done() const { return written_.load() >= limit_; }

private:
    void run() {
        try {
            auto client = verifier_client(uri_);
            auto c = client["test"][coll_];
            std::int64_t next = 1;
            while (!stop_.load() && next <= limit_) {
                for (int k = 0; k < per_tick_ && next <= limit_; ++k, ++next) {
                    c.insert_one(make_document(kvp("_id", next),
                                               kvp("v", value_of(next)),
                                               kvp("pad", "0123456789abcdef")));
                    written_.store(next);
                }
                std::this_thread::sleep_for(50ms);
            }
        } catch (const std::exception& e) {
            error_ = e.what();
        }
    }

    std::string uri_;
    std::string coll_;
    std::int64_t limit_;
    int per_tick_;
    std::atomic<bool> stop_{false};
    std::atomic<std::int64_t> written_{0};
    std::string error_;
    std::thread thread_;
};

class MongoRecovery : public ::testing::Test {
protected:
    void SetUp() override {
        uri_ = env_or_empty("CLINK_MONGODB_TEST_URI");
        if (uri_.empty()) {
            GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI (scripts/mongodb-live.sh does)";
        }
        if (!std::filesystem::exists(recovery_node_binary())) {
            GTEST_SKIP() << "the clink_node binary is not built";
        }
        static int counter = 0;
        suffix_ = std::to_string(::getpid()) + "_" + std::to_string(++counter);
        src_ = "rsrc_" + suffix_;
        dst_ = "rdst_" + suffix_;
        client_ = std::make_unique<mongocxx::client>(verifier_client(uri_));
    }

    void TearDown() override {
        if (!client_) {
            return;
        }
        try {
            (*client_)["test"][src_].drop();
            (*client_)["test"][dst_].drop();
        } catch (const std::exception& e) {
            ADD_FAILURE() << "cleaning up failed: " << e.what();
        }
    }

    clink::cluster::JobGraphSpec graph(const std::string& sink_type) const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "mongo_cdc_source";
        src.id = "src";
        src.uid = "mongodb-recovery-src";
        src.out_channel = std::string{clink::cluster::kChannelString};
        src.params = {
            {"uri", uri_}, {"database", "test"}, {"collection", src_}, {"max_await_ms", "200"}};
        g.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.type = sink_type;
        snk.id = "snk";
        snk.uid = "mongodb-recovery-snk";
        snk.inputs = {"src"};
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = {{"uri", uri_},
                      {"database", "test"},
                      {"collection", dst_},
                      {"batch_records", "200"},
                      {"linger_ms", "200"}};
        g.ops.push_back(snk);
        return g;
    }

    // A coordinator and worker 0 only, armed with `fault`: the job submitted next
    // places every task on worker 0, so the fault parks the task it names. Worker
    // 1 joins after the deploy, unarmed, as the spare a restart can use; so is
    // worker 0's replacement, so nothing parks again once the job is restored.
    std::unique_ptr<Cluster> make_cluster(const std::string& fault) {
        ClusterSpec spec;
        spec.node_binary = recovery_node_binary();
        spec.workers = 2;
        spec.slots_per_worker = 3;
        spec.http = true;
        auto cluster = std::make_unique<Cluster>(spec);
        if (!cluster->start_coordinator() || !cluster->start_worker(0, worker_options(fault)) ||
            !cluster->await_workers_registered(1)) {
            ADD_FAILURE() << "the cluster did not start";
        }
        return cluster;
    }

    // Every worker can read the URI the SQL cell's tables name by reference.
    ProcOptions worker_options(std::string fault = {}) const {
        ProcOptions o;
        o.fault = std::move(fault);
        o.env = {{"CLINK_MONGODB_RECOVERY_URI", uri_}};
        return o;
    }

    void add_spare_worker(Cluster& cluster) {
        ASSERT_TRUE(cluster.start_worker(1, worker_options()));
        ASSERT_TRUE(cluster.await_workers_registered(2));
        started_workers_ = 2;
    }

    static clink::http_connector::HttpRequest coordinator(const Cluster& cluster) {
        clink::http_connector::HttpRequest::Options o;
        o.base_url = "http://127.0.0.1:" + std::to_string(cluster.http_port());
        return clink::http_connector::HttpRequest(std::move(o));
    }

    void submit(Cluster& cluster, const std::string& sink_type, int interval_ms = 1000) {
        auto http = coordinator(cluster);
        const auto r = http.post("/api/v1/jobs/spec?name=mongodb-recovery&checkpoint_dir=" +
                                     cluster.checkpoint_dir().string() +
                                     "&checkpoint_interval_ms=" + std::to_string(interval_ms) +
                                     "&alignment=aligned&max_restarts_on_worker_loss=8",
                                 graph(sink_type).to_json(),
                                 "application/json");
        ASSERT_NE(r.body.find("\"ok\":true"), std::string::npos)
            << "the job was not accepted: " << r.body;
    }

    // `sql` compiled and submitted by clink_submit_sql, as a deployment does.
    void submit_sql(Cluster& cluster, const std::string& sql) {
        clink::itest::Process submit;
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
                                  "mongodb-recovery-sql",
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

    static std::filesystem::path sql_binary() {
#ifdef CLINK_SUBMIT_SQL_BINARY
        return std::filesystem::path{CLINK_SUBMIT_SQL_BINARY};
#else
        return {};
#endif
    }

    // Inserts, updates and deletes for the mirror cells: expected_ documents
    // with v 0, then v 1 and v 2 for each, then every fifth deleted.
    void mirror_workload() const {
        auto own = verifier_client(uri_);
        auto c = own["test"][src_];
        for (std::int64_t i = 1; i <= expected_; ++i) {
            c.insert_one(make_document(kvp("_id", i), kvp("v", 0)));
        }
        for (int round = 1; round <= 2; ++round) {
            for (std::int64_t i = 1; i <= expected_; ++i) {
                c.update_one(make_document(kvp("_id", i)),
                             make_document(kvp("$set", make_document(kvp("v", round)))));
            }
        }
        for (std::int64_t i = 5; i <= expected_; i += 5) {
            c.delete_one(make_document(kvp("_id", i)));
        }
    }

    // The mirror holds every surviving source document as the source holds it,
    // and a delete's tombstone for each deleted one.
    void expect_mirrored() {
        auto mirror = (*client_)["test"][dst_];
        ASSERT_TRUE(clink::itest::await(
            [&] {
                return mirror.count_documents(make_document(kvp("v", 2))) ==
                           expected_ - expected_ / 5 &&
                       mirror.count_documents(make_document(kvp("__op", "delete"))) ==
                           expected_ / 5;
            },
            240s))
            << "the mirror did not converge: " << mirror.count_documents(make_document(kvp("v", 2)))
            << " documents at v=2, " << mirror.count_documents(make_document(kvp("__op", "delete")))
            << " tombstones";
        EXPECT_EQ(mirror.count_documents(make_document()), expected_);
        std::size_t wrong = 0;
        for (auto&& s : (*client_)["test"][src_].find(make_document())) {
            const auto id = as_int(s["_id"]);
            auto m = mirror.find_one(make_document(kvp("_id", id)));
            if ((!m || as_int(m->view()["v"]) != as_int(s["v"])) && ++wrong <= 5) {
                ADD_FAILURE() << "document " << id << " is not mirrored as the source holds it";
            }
        }
        EXPECT_EQ(wrong, 0U);
    }

    static void cancel_job(const Cluster& cluster) {
        auto http = coordinator(cluster);
        const auto r = http.post("/api/v1/jobs/1/cancel", "", "application/json");
        EXPECT_NE(r.body.find("\"ok\":true"), std::string::npos)
            << "the cancel was not accepted: " << r.body;
    }

    std::vector<std::string> worker_logs(Cluster& cluster) const {
        std::vector<std::string> out = archived_logs_;
        for (std::size_t i = 0; i < started_workers_; ++i) {
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

    // The source's stream is open: a change stream delivers only what happens
    // after it opens, so the writer starts after this.
    void await_stream_open(Cluster& cluster) {
        ASSERT_TRUE(clink::itest::await(
            [&] { return count_in_workers(cluster, "change stream open on test." + src_) >= 1; },
            90s))
            << "the source never opened its change stream";
    }

    std::int64_t count(const std::string& coll) {
        return (*client_)["test"][coll].count_documents(make_document());
    }

    static std::size_t restarts(const Cluster& cluster) {
        return cluster.count_in_coordinator_log("[coordinator.restart] [info] job_id=1 attempt=");
    }

    static std::uint64_t latest_completed(const Cluster& cluster) {
        std::uint64_t latest = 0;
        std::error_code ec;
        for (const auto& e :
             std::filesystem::directory_iterator(cluster.checkpoint_dir() / "_jobs" / "1", ec)) {
            if (const auto id =
                    clink::test_markers::marker_id(e.path().filename().string(), "COMPLETED-")) {
                latest = std::max(latest, *id);
            }
        }
        return latest;
    }

    // A checkpoint of the job has completed, so a kill from here restores from one.
    void await_first_checkpoint(Cluster& cluster) {
        ASSERT_TRUE(clink::itest::await([&] { return latest_completed(cluster) >= 1; }, 60s))
            << "no checkpoint completed";
    }

    // Every document has landed and two more checkpoints completed after it,
    // so nothing sent before is still on its way.
    void await_all_landed(Cluster& cluster, std::int64_t want, std::chrono::seconds timeout) {
        ASSERT_TRUE(clink::itest::await([&] { return count(dst_) >= want; }, timeout))
            << "only " << count(dst_) << " of " << want << " documents landed";
        const auto after = latest_completed(cluster);
        ASSERT_TRUE(
            clink::itest::await([&] { return latest_completed(cluster) >= after + 2; }, 60s))
            << "checkpoints stopped completing after every document had landed";
    }

    void kill_and_replace(Cluster& cluster, std::size_t idx) {
        cluster.worker(idx).kill_hard();
        ASSERT_TRUE(cluster.worker(idx).await_exit(10s).has_value());
        archived_logs_.push_back(cluster.worker(idx).read_log());
        ASSERT_TRUE(cluster.restart_worker(idx, worker_options()));
    }

    // Whether worker 0, the armed one, has parked at `point`.
    static bool parked_at(Cluster& cluster, const std::string& point) {
        const std::string witness = "[fault.injection] fired: " + point;
        return clink::itest::await([&] { return cluster.worker(0).log_contains(witness); }, 120s);
    }

    // The destination against the writer's documents: exactly one per inserted
    // document, equal to it, with the insert's change metadata.
    void expect_exact_copies(std::int64_t n) {
        mongocxx::options::find sorted;
        sorted.sort(make_document(kvp("_id", 1)));
        std::int64_t seen = 0;
        std::size_t wrong = 0;
        for (auto&& d : (*client_)["test"][dst_].find(make_document(), sorted)) {
            ++seen;
            const auto id = as_int(d["_id"]);
            const bool ok = id == seen && as_int(d["v"]) == value_of(id) &&
                            d["__op"].get_string().value == "insert" &&
                            d["__image"].get_string().value == "full" &&
                            d["__ns"].get_string().value == "test." + src_ &&
                            as_int(d["__id"]) == id;
            if (!ok && ++wrong <= 5) {
                ADD_FAILURE() << "document " << seen << " is not the insert of document " << seen
                              << ": " << bsoncxx::to_json(d);
            }
        }
        EXPECT_EQ(seen, n) << "documents lost or doubled";
        EXPECT_EQ(wrong, 0U);
    }

    // The cluster's logs, kept where CLINK_MONGODB_RECOVERY_LOG_DIR names (the
    // gate's report directory), whether the cell passed or not.
    void keep_logs(const char* cell, Cluster& cluster) const {
        const auto into = env_or_empty("CLINK_MONGODB_RECOVERY_LOG_DIR");
        if (into.empty()) {
            return;
        }
        std::error_code ec;
        const auto dir = std::filesystem::path(into) / cell;
        std::filesystem::create_directories(dir, ec);
        std::filesystem::copy(cluster.log_dir(),
                              dir,
                              std::filesystem::copy_options::recursive |
                                  std::filesystem::copy_options::overwrite_existing,
                              ec);
        for (std::size_t i = 0; i < archived_logs_.size(); ++i) {
            std::ofstream(dir / ("killed-worker-" + std::to_string(i) + ".log"))
                << archived_logs_[i];
        }
    }

    // `extra` adds the mirror cells' measure of what was in flight.
    void report(const char* cell,
                std::size_t restarts_spent,
                Cluster& cluster,
                std::int64_t inserted_at_fault,
                std::int64_t landed_at_fault,
                const std::string& extra = {}) {
        keep_logs(cell, cluster);
        std::printf(
            "[mongodb-recovery] cell=%s produced=%lld restarts=%zu inserted_at_fault=%lld "
            "landed_at_fault=%lld replayed_batches_recognised=%zu%s\n",
            cell,
            static_cast<long long>(expected_),
            restarts_spent,
            static_cast<long long>(inserted_at_fault),
            static_cast<long long>(landed_at_fault),
            count_in_workers(cluster, "were already stored exactly as they arrive"),
            extra.c_str());
        std::fflush(stdout);
    }

    // The mirror's documents at their final value when the worker was killed,
    // against the number that end there.
    std::string converged_at_fault(std::int64_t settled) const {
        return " converged_at_fault=" + std::to_string(settled) + "/" +
               std::to_string(expected_ - expected_ / 5);
    }

    void kill_at(const std::string& point, const char* cell, const std::string& rule);

    std::string uri_;
    std::string suffix_;
    std::string src_;
    std::string dst_;
    std::int64_t expected_{0};
    std::unique_ptr<mongocxx::client> client_;
    std::vector<std::string> archived_logs_;
    std::size_t started_workers_{1};
};

void MongoRecovery::kill_at(const std::string& point, const char* cell, const std::string& rule) {
    // Worker 0 runs the job; the rule's number of times it reaches the point,
    // it parks there, and the kill lands while it is parked.
    auto cluster = make_cluster(point + "=" + rule);
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, "mongo_sink");
    await_stream_open(*cluster);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    expected_ = 3000;
    Writer writer(uri_, src_, expected_, 600);
    writer.start();
    ASSERT_TRUE(parked_at(*cluster, point)) << "worker 0 never reached " << point;
    // In flight: the source holds documents the destination does not.
    const auto inserted = writer.written();
    const auto landed = count(dst_);
    EXPECT_LT(landed, inserted) << "nothing was in flight when the worker parked";
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));
    await_all_landed(*cluster, expected_, 240s);
    EXPECT_EQ(writer.stop(), expected_) << writer.error();
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report(cell, spent, *cluster, inserted, landed);
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    EXPECT_GE(count_in_workers(*cluster, "resuming after the checkpoint's resume token"), 1U)
        << "the restored source did not resume from its checkpoint";
    if (point == "mongodb.sink_after_write") {
        // The parked batch was acknowledged and its checkpoint never
        // completed, so the restored job wrote its documents again: the fault
        // engaged, and the insert sink recognised them as its own.
        EXPECT_GE(count_in_workers(*cluster, "were already stored exactly as they arrive"), 1U)
            << "no acknowledged batch was replayed";
    }
    expect_exact_copies(expected_);
}

}  // namespace

TEST_F(MongoRecovery, AWorkerKilledBeforeItsWriteLosesNothing) {
    kill_at("mongodb.sink_before_write", "sink_before_write", "block@4");
}

TEST_F(MongoRecovery, AWorkerKilledAfterAWriteBeforeItsCheckpointReplaysIntoTheSameDocuments) {
    kill_at("mongodb.sink_after_write", "sink_after_write", "block@4");
}

TEST_F(MongoRecovery, AWorkerKilledWithChangeEventsInFlightReadsThemAgain) {
    kill_at("mongodb.source_after_emit", "source_after_emit", "block@6");
}

TEST_F(MongoRecovery, AServerRestartWhileTheJobRunsLosesNothing) {
    const auto container = env_or_empty("CLINK_MONGODB_TEST_CONTAINER");
    if (container.empty()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_CONTAINER (scripts/mongodb-live.sh does)";
    }
    auto cluster = make_cluster("");
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, "mongo_sink");
    await_stream_open(*cluster);
    add_spare_worker(*cluster);
    expected_ = 3000;
    Writer writer(uri_, src_, expected_, 300);
    writer.start();
    ASSERT_TRUE(clink::itest::await([&] { return count(dst_) > 300; }, 120s));
    const auto inserted = writer.written();
    const auto landed = count(dst_);
    // The writer pauses with the server: it fails while it is down, so it is
    // restarted after.
    writer.stop();
    const auto stopped = clink::test::run_docker_command("docker stop -t 30 " + container);
    ASSERT_EQ(stopped.status, 0) << stopped.out;
    std::this_thread::sleep_for(5s);
    const auto started = clink::test::run_docker_command("docker start " + container);
    ASSERT_EQ(started.status, 0) << started.out;
    ASSERT_TRUE(clink::itest::await(
        [&] {
            try {
                auto probe = verifier_client(uri_);
                const auto hello = probe["admin"].run_command(make_document(kvp("hello", 1)));
                return hello.view()["isWritablePrimary"] &&
                       hello.view()["isWritablePrimary"].get_bool().value;
            } catch (const std::exception&) {
                return false;
            }
        },
        120s))
        << "the server did not come back as primary";
    // Write the rest where the writer stopped.
    {
        auto c = (*client_)["test"][src_];
        for (std::int64_t i = writer.written() + 1; i <= expected_; ++i) {
            c.insert_one(make_document(
                kvp("_id", i), kvp("v", value_of(i)), kvp("pad", "0123456789abcdef")));
        }
    }
    await_all_landed(*cluster, expected_, 300s);
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report("server_restart", spent, *cluster, inserted, landed);
    expect_exact_copies(expected_);
}

// Inserts, updates and deletes, replicated by key into a mirror; a worker
// parked after a write is killed, so the restored job writes images again.
TEST_F(MongoRecovery, AMirrorByKeyConvergesOnTheSourceAfterAWorkerIsKilled) {
    auto cluster = make_cluster("mongodb.sink_after_write=block@3");
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, "mongo_upsert_sink");
    await_stream_open(*cluster);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    expected_ = 600;
    std::thread workload([this] { mirror_workload(); });
    const bool parked = parked_at(*cluster, "mongodb.sink_after_write");
    if (!parked) {
        workload.join();
        FAIL() << "worker 0 never parked after a write";
    }
    // In flight: the mirror had not caught up with the source when the worker
    // was killed.
    const auto inserted = count(src_);
    const auto landed = count(dst_);
    const auto settled = (*client_)["test"][dst_].count_documents(make_document(kvp("v", 2)));
    kill_and_replace(*cluster, 0);
    workload.join();
    ASSERT_TRUE(cluster->await_workers_registered(2));
    EXPECT_LT(settled, expected_ - expected_ / 5) << "the mirror had converged before the kill";
    expect_mirrored();
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report("mirror_after_write", spent, *cluster, inserted, landed, converged_at_fault(settled));
    EXPECT_EQ(spent, 1U);
}

// The same mirror as a SQL job, compiled and submitted by clink_submit_sql at
// parallelism 2 to the coordinator: the change stream read as typed rows, the
// mirror written by mode='upsert' keyed by its PRIMARY KEY, the URI named by an
// env:// reference that each worker resolves. A worker parked after a write is
// killed while the changes flow.
TEST_F(MongoRecovery, ASqlMirrorConvergesOnTheSourceAfterAWorkerIsKilled) {
    if (!std::filesystem::exists(sql_binary())) {
        GTEST_SKIP() << "the clink_submit_sql binary is not built";
    }
    auto cluster = make_cluster("mongodb.sink_after_write=block@3");
    ScopedDiagnostics diagnostics(*cluster);
    const std::string uri = "uri='env://CLINK_MONGODB_RECOVERY_URI', database='test', ";
    submit_sql(*cluster,
               "CREATE TABLE changes (__id BIGINT, __op VARCHAR, v BIGINT) WITH "
               "(connector='mongodb', " +
                   uri + "collection='" + src_ +
                   "', max_await_ms='200'); "
                   "CREATE TABLE mirror (_id BIGINT PRIMARY KEY, __op VARCHAR, v BIGINT) WITH "
                   "(connector='mongodb', " +
                   uri + "collection='" + dst_ +
                   "', mode='upsert', batch_records='200', linger_ms='200'); "
                   "INSERT INTO mirror SELECT __id AS _id, __op, v FROM changes;");
    await_stream_open(*cluster);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    expected_ = 600;
    std::thread workload([this] { mirror_workload(); });
    const bool parked = parked_at(*cluster, "mongodb.sink_after_write");
    if (!parked) {
        workload.join();
        FAIL() << "worker 0 never parked after a write";
    }
    const auto inserted = count(src_);
    const auto landed = count(dst_);
    const auto settled = (*client_)["test"][dst_].count_documents(make_document(kvp("v", 2)));
    kill_and_replace(*cluster, 0);
    workload.join();
    ASSERT_TRUE(cluster->await_workers_registered(2));
    EXPECT_LT(settled, expected_ - expected_ / 5) << "the mirror had converged before the kill";
    expect_mirrored();
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report(
        "sql_mirror_after_write", spent, *cluster, inserted, landed, converged_at_fault(settled));
    EXPECT_EQ(spent, 1U);
}

// The worker parks at the source's first emission, before any checkpoint of the
// job completes (a long interval keeps them away), and is killed. The job
// restarts from no checkpoint. A source that opened at the current time then
// would skip every change since the run's stream started; this one resumes
// where it started, from the record it made when it first opened.
TEST_F(MongoRecovery, AWorkerKilledBeforeAnyCheckpointResumesWhereTheRunStarted) {
    auto cluster = make_cluster("mongodb.source_after_emit=block@1");
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, "mongo_sink", /*interval_ms=*/10000);
    await_stream_open(*cluster);
    add_spare_worker(*cluster);
    expected_ = 3000;
    Writer writer(uri_, src_, expected_, 600);
    writer.start();
    ASSERT_TRUE(parked_at(*cluster, "mongodb.source_after_emit")) << "the source never emitted";
    ASSERT_EQ(latest_completed(*cluster), 0U) << "a checkpoint completed before the kill";
    const auto inserted = writer.written();
    const auto landed = count(dst_);
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));
    await_all_landed(*cluster, expected_, 240s);
    EXPECT_EQ(writer.stop(), expected_) << writer.error();
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report("before_first_checkpoint", spent, *cluster, inserted, landed);
    EXPECT_EQ(spent, 1U);
    EXPECT_GE(cluster->count_in_coordinator_log("restore point: checkpoint 0"), 1U)
        << "the restart restored from a checkpoint, so the case did not test a restart "
           "before the first one";
    EXPECT_GE(count_in_workers(*cluster, "resuming where this run's stream started"), 1U)
        << "the restarted source did not resume where the run started";
    expect_exact_copies(expected_);
}
