// The Kinesis sink's recovery cells: a coordinator and two clink_node workers
// run a job that reads one LocalStack Kinesis stream (kinesis_source) into the
// Kinesis sink, which writes a second stream keyed by each record's "key"
// member, while the test writes 1,200 unique records to the first. Each cell
// parks worker 0 at one of the sink's fault points and kills it:
//
//   1. parallelism 1, parked after PutRecords stored a request and before any
//      checkpoint covers it (kinesis_sink.after_write): the restart replays that
//      request's records, which reach the stream twice.
//   2. parallelism 1, parked with a request built and not sent
//      (kinesis_sink.before_write): nothing leaves the sink while it is parked.
//   3. cell 1 as a SQL job (a connector='kinesis' table into a
//      connector='kinesis' table with partition_key='key'), submitted by
//      clink_submit_sql at parallelism 2.
//
// The contract checked is the sink's: every record stored at least once with
// its own partition key, nothing else, each byte for byte (as parsed JSON on
// the SQL path, which writes each row afresh). Duplicates are allowed, and the
// cells parked after a store require some, since the kill came before a
// checkpoint covered the stored request. The output stream is read with the
// SDK's own Kinesis client; the fault and the restart are witnessed in the logs.
// These are the sink's cells: the source's are test_kinesis_recovery.cpp, with
// their own verdicts and logs.
//
// Needs the endpoint scripts/kinesis-live.sh starts (CLINK_KINESIS_TEST_ENDPOINT,
// and the AWS variables the SDK signs with), the clink_node binary of a build
// with fault injection, and clink_submit_sql for cell 3. Each cell prints one
// "[kinesis-sink-recovery]" line and, with CLINK_KINESIS_SINK_RECOVERY_LOG_DIR
// set, keeps the cluster's logs there.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/CreateStreamRequest.h>
#include <aws/kinesis/model/DeleteStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamSummaryRequest.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/ListShardsRequest.h>
#include <aws/kinesis/model/PutRecordRequest.h>
#include <aws/kinesis/model/ShardIteratorType.h>
#include <aws/kinesis/model/StreamStatus.h>
#include <gtest/gtest.h>

#include "clink/aws/aws_client.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/config/json.hpp"
#include "clink/http_connector/http_request.hpp"

#include "../checkpoint_markers.hpp"
#include "tests/integration/cluster_harness.hpp"

namespace {

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::ProcOptions;

std::filesystem::path ksr_node_binary() {
#ifdef CLINK_NODE_BINARY
    return std::filesystem::path{CLINK_NODE_BINARY};
#else
    return {};
#endif
}

std::filesystem::path ksr_sql_binary() {
#ifdef CLINK_SUBMIT_SQL_BINARY
    return std::filesystem::path{CLINK_SUBMIT_SQL_BINARY};
#else
    return {};
#endif
}

std::string ksr_env(const char* name) {
    const char* v = std::getenv(name);
    return v == nullptr ? std::string{} : std::string{v};
}

clink::aws::AwsClientOptions ksr_client(const std::string& endpoint) {
    clink::aws::AwsClientOptions o;
    o.region = "us-east-1";
    o.endpoint_override = endpoint;
    o.request_timeout_ms = 60000;
    return o;
}

// One record of the output stream, as the reader found it.
struct KsrRead {
    std::string key;
    std::string data;
};

// The stream the job reads, the stream the sink writes, and a reader of the
// second.
class KsrPlumbing {
public:
    KsrPlumbing(const std::string& endpoint, const std::string& name)
        : input_(name + "-in"), output_(name + "-out") {
        clink::aws::ensure_aws_initialized();
        client_ = std::make_unique<Aws::Kinesis::KinesisClient>(
            clink::aws::make_client_config(ksr_client(endpoint)));
    }
    ~KsrPlumbing() {
        for (const auto& s : {input_, output_}) {
            Aws::Kinesis::Model::DeleteStreamRequest req;
            req.SetStreamName(s);
            client_->DeleteStream(req);
        }
    }
    KsrPlumbing(const KsrPlumbing&) = delete;
    KsrPlumbing& operator=(const KsrPlumbing&) = delete;

    [[nodiscard]] const std::string& input() const { return input_; }
    [[nodiscard]] const std::string& output() const { return output_; }

    bool create(int input_shards, int output_shards) {
        for (const auto& [s, shards] : std::vector<std::pair<std::string, int>>{
                 {input_, input_shards}, {output_, output_shards}}) {
            Aws::Kinesis::Model::CreateStreamRequest req;
            req.SetStreamName(s);
            req.SetShardCount(shards);
            client_->CreateStream(req);
        }
        for (int i = 0; i < 300; ++i) {
            bool both = true;
            for (const auto& s : {input_, output_}) {
                Aws::Kinesis::Model::DescribeStreamSummaryRequest d;
                d.SetStreamName(s);
                const auto r = client_->DescribeStreamSummary(d);
                both = both && r.IsSuccess() &&
                       r.GetResult().GetStreamDescriptionSummary().GetStreamStatus() ==
                           Aws::Kinesis::Model::StreamStatus::ACTIVE;
            }
            if (both) {
                return true;
            }
            std::this_thread::sleep_for(100ms);
        }
        return false;
    }

    bool put(const std::string& key, const std::string& data) {
        Aws::Kinesis::Model::PutRecordRequest req;
        req.SetStreamName(input_);
        req.SetPartitionKey(key);
        req.SetData(Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(data.data()),
                                           data.size()));
        for (int attempt = 0; attempt < 20; ++attempt) {
            if (client_->PutRecord(req).IsSuccess()) {
                return true;
            }
            std::this_thread::sleep_for(100ms);
        }
        return false;
    }

    // Every record of the output stream, each shard from its oldest record.
    std::vector<KsrRead> output_records() {
        std::vector<KsrRead> out;
        Aws::Kinesis::Model::ListShardsRequest list;
        list.SetStreamName(output_);
        const auto lr = client_->ListShards(list);
        if (!lr.IsSuccess()) {
            ADD_FAILURE() << "ListShards: " << lr.GetError().GetMessage();
            return out;
        }
        for (const auto& shard : lr.GetResult().GetShards()) {
            Aws::Kinesis::Model::GetShardIteratorRequest gi;
            gi.SetStreamName(output_);
            gi.SetShardId(shard.GetShardId());
            gi.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);
            const auto ir = client_->GetShardIterator(gi);
            if (!ir.IsSuccess()) {
                ADD_FAILURE() << "GetShardIterator: " << ir.GetError().GetMessage();
                continue;
            }
            auto iterator = ir.GetResult().GetShardIterator();
            for (int round = 0; round < 1000 && !iterator.empty(); ++round) {
                Aws::Kinesis::Model::GetRecordsRequest gr;
                gr.SetShardIterator(iterator);
                gr.SetLimit(10000);
                const auto rr = client_->GetRecords(gr);
                if (!rr.IsSuccess()) {
                    ADD_FAILURE() << "GetRecords: " << rr.GetError().GetMessage();
                    break;
                }
                for (const auto& r : rr.GetResult().GetRecords()) {
                    const auto& d = r.GetData();
                    out.push_back(
                        KsrRead{r.GetPartitionKey(),
                                std::string(reinterpret_cast<const char*>(d.GetUnderlyingData()),
                                            d.GetLength())});
                }
                if (rr.GetResult().GetRecords().empty()) {
                    break;
                }
                iterator = rr.GetResult().GetNextShardIterator();
            }
        }
        return out;
    }

private:
    std::string input_;
    std::string output_;
    std::unique_ptr<Aws::Kinesis::KinesisClient> client_;
};

// Writes `limit` unique records at about `per_second` to the input stream, each
// a JSON object with its id, its key (twelve keys in turn) and padding, and
// keeps what it wrote.
class KsrWriter {
public:
    KsrWriter(KsrPlumbing& plumbing, int limit, int per_second)
        : plumbing_(plumbing), limit_(limit), per_tick_(std::max(per_second / 20, 1)) {}
    ~KsrWriter() { stop(); }
    KsrWriter(const KsrWriter&) = delete;
    KsrWriter& operator=(const KsrWriter&) = delete;

    void start() {
        thread_ = std::thread([this] { run(); });
    }
    void stop() {
        stop_.store(true);
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    void await_done() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    [[nodiscard]] int written() const { return written_.load(); }
    [[nodiscard]] std::string error() const {
        std::lock_guard lock(mu_);
        return error_;
    }
    [[nodiscard]] std::vector<std::string> records() const {
        std::lock_guard lock(mu_);
        return records_;
    }

private:
    void run() {
        int next = 0;
        while (!stop_.load() && next < limit_) {
            for (int t = 0; t < per_tick_ && next < limit_; ++t, ++next) {
                const std::string key = "key-" + std::to_string(next % 12);
                const std::string data =
                    "{\"id\":" + std::to_string(next) + ",\"key\":\"" + key + "\",\"pad\":\"" +
                    std::string(static_cast<std::size_t>(next % 61), 'x') + "\"}";
                const bool ok = plumbing_.put(key, data);
                std::lock_guard lock(mu_);
                if (!ok) {
                    error_ = "PutRecord failed for " + data;
                    return;
                }
                records_.push_back(data);
                written_.store(next + 1);
            }
            std::this_thread::sleep_for(50ms);
        }
    }

    KsrPlumbing& plumbing_;
    int limit_;
    int per_tick_;
    std::atomic<bool> stop_{false};
    std::atomic<int> written_{0};
    mutable std::mutex mu_;
    std::vector<std::string> records_;
    std::string error_;
    std::thread thread_;
};

// The identity a record is compared by: its bytes, or on the SQL path, which
// writes each row afresh, its id, key and padding as parsed JSON.
std::string ksr_identity(const std::string& data, bool parsed) {
    if (!parsed) {
        return data;
    }
    try {
        const auto j = clink::config::parse(data);
        const auto& o = j.as_object();
        return std::to_string(o.at("id").as_int()) + "|" + o.at("key").as_string() + "|" +
               o.at("pad").as_string();
    } catch (const std::exception&) {
        return "<unparsable>" + data;
    }
}

// The key a written record must arrive with: its "key" member.
std::string ksr_expected_key(const std::string& data) {
    return clink::config::parse(data).as_object().at("key").as_string();
}

struct KsrVerdict {
    std::size_t arrived{0};
    std::size_t missing{0};
    std::size_t unexpected{0};
    std::size_t duplicates{0};
    std::size_t wrong_key{0};
};

KsrVerdict ksr_judge(const std::vector<std::string>& written,
                     const std::vector<KsrRead>& arrived,
                     bool parsed,
                     bool report) {
    std::map<std::string, std::string> want;  // identity -> key
    for (const auto& w : written) {
        want[ksr_identity(w, parsed)] = ksr_expected_key(w);
    }
    std::map<std::string, int> got;
    KsrVerdict v;
    v.arrived = arrived.size();
    for (const auto& a : arrived) {
        const auto id = ksr_identity(a.data, parsed);
        ++got[id];
        if (const auto it = want.find(id); it != want.end() && it->second != a.key) {
            if (++v.wrong_key <= 5 && report) {
                ADD_FAILURE() << "record " << id << " arrived with key '" << a.key << "', not '"
                              << it->second << "'";
            }
        }
    }
    for (const auto& [id, key] : want) {
        const auto it = got.find(id);
        if (it == got.end()) {
            if (++v.missing <= 5 && report) {
                ADD_FAILURE() << "record " << id << " never reached the output stream";
            }
        } else if (it->second > 1) {
            v.duplicates += static_cast<std::size_t>(it->second - 1);
        }
    }
    for (const auto& [id, n] : got) {
        if (want.count(id) == 0) {
            v.unexpected += static_cast<std::size_t>(n);
            if (v.unexpected <= 5 && report) {
                ADD_FAILURE() << "the output stream holds " << id << ", which was never written";
            }
        }
    }
    return v;
}

class KinesisSinkRecovery : public ::testing::Test {
protected:
    void SetUp() override {
        endpoint_ = ksr_env("CLINK_KINESIS_TEST_ENDPOINT");
        if (endpoint_.empty()) {
            GTEST_SKIP() << "set CLINK_KINESIS_TEST_ENDPOINT (scripts/kinesis-live.sh does)";
        }
        if (!std::filesystem::exists(ksr_node_binary())) {
            GTEST_SKIP() << "the clink_node binary is not built";
        }
        static int counter = 0;
        plumbing_ = std::make_unique<KsrPlumbing>(
            endpoint_,
            "clink-ksink-recovery-" + std::to_string(::getpid()) + "-" + std::to_string(++counter));
    }

    clink::cluster::JobGraphSpec graph(std::uint32_t parallelism) const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "kinesis_source";
        src.id = "src";
        src.uid = "kinesis-sink-recovery-src";
        src.parallelism = parallelism;
        src.out_channel = std::string{clink::cluster::kChannelString};
        src.params = {{"stream", plumbing_->input()},
                      {"region", "us-east-1"},
                      {"endpoint_override", endpoint_},
                      {"initial_position", "trim_horizon"},
                      {"poll_interval_ms", "50"},
                      {"discovery_interval_ms", "500"},
                      {"max_records_per_poll", "40"}};
        g.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.type = "kinesis_sink";
        snk.id = "snk";
        snk.uid = "kinesis-sink-recovery-snk";
        snk.inputs = {"src"};
        snk.parallelism = parallelism;
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = {{"stream", plumbing_->output()},
                      {"region", "us-east-1"},
                      {"endpoint_override", endpoint_},
                      {"partition_key", "key"},
                      {"batch_records", "20"}};
        g.ops.push_back(snk);
        return g;
    }

    std::string sql() const {
        const std::string common = "region='us-east-1', endpoint_override='" + endpoint_ + "'";
        return "CREATE TABLE feed (id BIGINT, key STRING, pad STRING) WITH (connector='kinesis', "
               "format='json', stream='" +
               plumbing_->input() + "', " + common +
               ", initial_position='trim_horizon', poll_interval_ms='50', "
               "discovery_interval_ms='500', max_records_per_poll='40');\n"
               "CREATE TABLE keyed (id BIGINT, key STRING, pad STRING) WITH (connector='kinesis', "
               "format='json', stream='" +
               plumbing_->output() + "', " + common +
               ", partition_key='key', batch_records='20');\n"
               "INSERT INTO keyed SELECT id, key, pad FROM feed;\n";
    }

    static ProcOptions worker_options(std::string fault = {}) {
        ProcOptions o;
        o.fault = std::move(fault);
        o.env = {{"AWS_ACCESS_KEY_ID", "test"},
                 {"AWS_SECRET_ACCESS_KEY", "test"},
                 {"AWS_REGION", "us-east-1"},
                 {"AWS_EC2_METADATA_DISABLED", "true"}};
        return o;
    }

    std::unique_ptr<Cluster> make_cluster(const std::string& fault, int slots) {
        ClusterSpec spec;
        spec.node_binary = ksr_node_binary();
        spec.workers = 2;
        spec.slots_per_worker = slots;
        spec.http = true;
        auto cluster = std::make_unique<Cluster>(spec);
        if (!cluster->start_coordinator() || !cluster->start_worker(0, worker_options(fault)) ||
            !cluster->await_workers_registered(1)) {
            ADD_FAILURE() << "the cluster did not start";
        }
        return cluster;
    }

    void submit_graph(Cluster& cluster, std::uint32_t parallelism) {
        clink::http_connector::HttpRequest::Options o;
        o.base_url = "http://127.0.0.1:" + std::to_string(cluster.http_port());
        clink::http_connector::HttpRequest http(std::move(o));
        const auto r = http.post("/api/v1/jobs/spec?name=kinesis-sink-recovery&checkpoint_dir=" +
                                     cluster.checkpoint_dir().string() +
                                     "&checkpoint_interval_ms=1000&alignment=aligned"
                                     "&max_restarts_on_worker_loss=8",
                                 graph(parallelism).to_json(),
                                 "application/json");
        ASSERT_NE(r.body.find("\"ok\":true"), std::string::npos)
            << "the job was not accepted: " << r.body;
    }

    void submit_sql(Cluster& cluster, int parallelism) const {
        clink::itest::Process submit;
        ASSERT_TRUE(submit.spawn("submit-sql",
                                 ksr_sql_binary(),
                                 {ksr_sql_binary().string(),
                                  "-e",
                                  sql(),
                                  "--coordinator-host",
                                  "127.0.0.1",
                                  "--coordinator-port",
                                  std::to_string(cluster.http_port()),
                                  "--name",
                                  "kinesis-sink-recovery-sql",
                                  "--checkpoint-dir",
                                  cluster.checkpoint_dir().string(),
                                  "--checkpoint-interval-ms",
                                  "1000",
                                  "--alignment",
                                  "aligned",
                                  "--max-restarts-on-worker-loss",
                                  "8",
                                  "--parallelism",
                                  std::to_string(parallelism)},
                                 cluster.log_dir()));
        const auto code = submit.await_exit(60s);
        ASSERT_TRUE(code.has_value());
        ASSERT_EQ(*code, 0) << submit.read_log();
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

    void keep_logs(const char* cell, Cluster& cluster) const {
        const auto into = ksr_env("CLINK_KINESIS_SINK_RECOVERY_LOG_DIR");
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
        std::ofstream(dir / "killed-worker-0.log") << killed_log_;
    }

    struct Cell {
        const char* name;
        std::uint32_t parallelism;
        std::string point;
        int ordinal;
        bool after_store;  // parked after a store no checkpoint covers
        bool sql;
    };

    void run_cell(const Cell& c) {
        ASSERT_TRUE(
            plumbing_->create(static_cast<int>(std::max<std::uint32_t>(c.parallelism, 1)), 4));
        // Submitted while only worker 0 is registered, so the sink's subtasks run
        // where the fault is armed; worker 1 takes the restart.
        auto cluster = make_cluster(c.point + "=block@" + std::to_string(c.ordinal), c.sql ? 8 : 4);
        KsrWriter writer(*plumbing_, 1200, 120);
        writer.start();
        if (c.sql) {
            submit_sql(*cluster, static_cast<int>(c.parallelism));
        } else {
            submit_graph(*cluster, c.parallelism);
        }
        ASSERT_TRUE(cluster->start_worker(1, worker_options()));
        ASSERT_TRUE(cluster->await_workers_registered(2));
        const std::string witness = "[fault.injection] fired: " + c.point;
        ASSERT_TRUE(
            clink::itest::await([&] { return cluster->worker(0).log_contains(witness); }, 120s))
            << "worker 0 never reached " << c.point << "\n"
            << cluster->worker(0).read_log();
        const auto written_at_kill = writer.written();
        const auto arrived_at_park = plumbing_->output_records().size();
        // Parked at parallelism 1, the sink's only subtask sends nothing more:
        // the output stream is watched for a while and must not move.
        auto arrived_at_kill = arrived_at_park;
        for (const auto until = std::chrono::steady_clock::now() + 1500ms;
             std::chrono::steady_clock::now() < until && arrived_at_kill == arrived_at_park;) {
            std::this_thread::sleep_for(250ms);
            arrived_at_kill = plumbing_->output_records().size();
        }
        const auto checkpoint_at_kill = latest_completed(*cluster);
        cluster->worker(0).kill_hard();
        ASSERT_TRUE(cluster->worker(0).await_exit(10s).has_value());
        killed_log_ = cluster->worker(0).read_log();
        ASSERT_TRUE(cluster->restart_worker(0, worker_options()));
        writer.await_done();
        ASSERT_EQ(writer.error(), "");
        const auto written = writer.records();
        ASSERT_EQ(written.size(), 1200u);
        // Every record arrives; then two more checkpoints complete.
        const bool arrived = clink::itest::await(
            [&] {
                const auto v = ksr_judge(written, plumbing_->output_records(), c.sql, false);
                return v.missing == 0;
            },
            240s);
        EXPECT_TRUE(arrived) << "records were still missing from the output stream";
        const auto after = latest_completed(*cluster);
        EXPECT_TRUE(
            clink::itest::await([&] { return latest_completed(*cluster) >= after + 2; }, 60s))
            << "checkpoints stopped completing";
        const auto v = ksr_judge(written, plumbing_->output_records(), c.sql, true);
        const auto restarts =
            cluster->count_in_coordinator_log("[coordinator.restart] [info] job_id=1 attempt=");
        EXPECT_EQ(v.missing, 0u);
        EXPECT_EQ(v.unexpected, 0u);
        EXPECT_EQ(v.wrong_key, 0u);
        EXPECT_GE(restarts, 1u) << "the kill did not restart the job";
        EXPECT_NE(killed_log_.find(witness), std::string::npos);
        if (c.parallelism == 1) {
            EXPECT_EQ(arrived_at_kill, arrived_at_park)
                << "the output stream moved while the sink was parked at " << c.point;
        }
        if (c.after_store) {
            // The parked request was stored and no checkpoint covered it, so
            // the replay sent its records again.
            EXPECT_GT(arrived_at_kill, 0u);
            EXPECT_GE(v.duplicates, 1u) << "the stored, uncovered request was not sent again";
        }
        std::printf(
            "[kinesis-sink-recovery] cell=%s parallelism=%u written=%zu arrived=%zu missing=%zu "
            "unexpected=%zu wrong_key=%zu duplicates=%zu restarts=%zu written_at_kill=%d "
            "arrived_at_park=%zu arrived_at_kill=%zu checkpoint_at_kill=%llu\n",
            c.name,
            c.parallelism,
            written.size(),
            v.arrived,
            v.missing,
            v.unexpected,
            v.wrong_key,
            v.duplicates,
            restarts,
            written_at_kill,
            arrived_at_park,
            arrived_at_kill,
            static_cast<unsigned long long>(checkpoint_at_kill));
        std::fflush(stdout);
        keep_logs(c.name, *cluster);
    }

    std::string endpoint_;
    std::unique_ptr<KsrPlumbing> plumbing_;
    std::string killed_log_;
};

}  // namespace

TEST_F(KinesisSinkRecovery, AWorkerKilledAfterAnUncoveredStoreLosesNothing) {
    run_cell({"p1_kill_after_store", 1, "kinesis_sink.after_write", 15, true, false});
}

TEST_F(KinesisSinkRecovery, AWorkerKilledBeforeASendLosesNothing) {
    run_cell({"p1_kill_before_send", 1, "kinesis_sink.before_write", 10, false, false});
}

TEST_F(KinesisSinkRecovery, ASqlJobKilledAfterAnUncoveredStoreLosesNothing) {
    if (!std::filesystem::exists(ksr_sql_binary())) {
        GTEST_SKIP() << "the clink_submit_sql binary is not built";
    }
    run_cell({"sql_p2_kill_after_store", 2, "kinesis_sink.after_write", 12, true, true});
}
