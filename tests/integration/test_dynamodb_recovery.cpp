// The DynamoDB sink's recovery cells: a coordinator and two clink_node workers
// run a job that reads a LocalStack stream (kinesis_source) into the DynamoDB
// sink, while the test writes records for twelve keys, each record naming one of
// four items of its key, its sort key written as one of four equal numerals (3,
// 3.0, 3E0, 30E-1). Each cell parks worker 0 at one of the sink's fault points
// with writes no checkpoint covers, kills it, and checks the table against the
// test's own record of what it wrote:
//
//   1. parallelism 1, parked after a BatchWriteItem the service applied
//      (dynamodb_sink.after_write) and SIGKILLed: the restart replays records
//      whose writes the table already holds.
//   2. parallelism 2, two shards: every record of a key goes to one shard, and so
//      to one source and one sink subtask. Parked after a write and SIGKILLed.
//   3. parallelism 1, parked with a request built and not sent
//      (dynamodb_sink.before_write) and SIGKILLed: the buffered records die with
//      the worker and come back from the stream.
//
// The contract checked is the sink's final-state one: with every record of an
// item reaching one sink subtask in order, the item ends at the last record
// written for it. The pass criteria read the table with the SDK's own client,
// item by item and attribute by attribute, and count nothing clink reports; the
// fault and the restart are witnessed in the logs.
//
// Needs the endpoint scripts/dynamodb-live.sh starts (CLINK_DYNAMODB_TEST_ENDPOINT,
// serving DynamoDB and Kinesis, and the AWS variables the SDK signs with) and the
// clink_node binary of a build with fault injection. Each cell prints one
// "[dynamodb-recovery]" line and, with CLINK_DYNAMODB_RECOVERY_LOG_DIR set, keeps
// the cluster's logs there.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/AttributeDefinition.h>
#include <aws/dynamodb/model/CreateTableRequest.h>
#include <aws/dynamodb/model/DeleteTableRequest.h>
#include <aws/dynamodb/model/DescribeTableRequest.h>
#include <aws/dynamodb/model/KeySchemaElement.h>
#include <aws/dynamodb/model/ScanRequest.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/CreateStreamRequest.h>
#include <aws/kinesis/model/DeleteStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamSummaryRequest.h>
#include <aws/kinesis/model/PutRecordRequest.h>
#include <aws/kinesis/model/StreamStatus.h>
#include <gtest/gtest.h>

#include "clink/aws/aws_client.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/http_connector/http_request.hpp"

#include "../checkpoint_markers.hpp"
#include "tests/integration/cluster_harness.hpp"

namespace {

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::ProcOptions;

constexpr int kKeys = 12;
constexpr int kItemsPerKey = 4;

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

clink::aws::AwsClientOptions client_options(const std::string& endpoint) {
    clink::aws::AwsClientOptions o;
    o.region = "us-east-1";
    o.endpoint_override = endpoint;
    o.request_timeout_ms = 60000;  // the emulator starts its DynamoDB on first use
    return o;
}

// ---- the stream the job reads ---------------------------------------------------------

class DdbCellStream {
public:
    DdbCellStream(const std::string& endpoint, std::string name) : name_(std::move(name)) {
        clink::aws::ensure_aws_initialized();
        client_ = std::make_unique<Aws::Kinesis::KinesisClient>(
            clink::aws::make_client_config(client_options(endpoint)));
    }
    ~DdbCellStream() {
        Aws::Kinesis::Model::DeleteStreamRequest req;
        req.SetStreamName(name_);
        client_->DeleteStream(req);
    }
    DdbCellStream(const DdbCellStream&) = delete;
    DdbCellStream& operator=(const DdbCellStream&) = delete;

    [[nodiscard]] const std::string& name() const { return name_; }

    bool create(int shards) {
        Aws::Kinesis::Model::CreateStreamRequest req;
        req.SetStreamName(name_);
        req.SetShardCount(shards);
        client_->CreateStream(req);
        for (int i = 0; i < 300; ++i) {
            Aws::Kinesis::Model::DescribeStreamSummaryRequest d;
            d.SetStreamName(name_);
            auto r = client_->DescribeStreamSummary(d);
            if (r.IsSuccess() && r.GetResult().GetStreamDescriptionSummary().GetStreamStatus() ==
                                     Aws::Kinesis::Model::StreamStatus::ACTIVE) {
                return true;
            }
            std::this_thread::sleep_for(100ms);
        }
        return false;
    }

    bool put(const std::string& key, const std::string& data) {
        Aws::Kinesis::Model::PutRecordRequest req;
        req.SetStreamName(name_);
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

private:
    std::string name_;
    std::unique_ptr<Aws::Kinesis::KinesisClient> client_;
};

// ---- the table the job writes ---------------------------------------------------------

// An item as the test compares it: attribute -> type and text.
using CellItem = std::map<std::string, std::string>;

// A numeral the service returned, as an integer's text: the cells write only
// integers, so "3", "3.0" and "3E0" all read back as "3".
std::string integer_text(const std::string& numeral) {
    const long double v = std::stold(numeral);
    return std::to_string(static_cast<long long>(v));
}

class DdbCellTable {
public:
    DdbCellTable(const std::string& endpoint, std::string name) : name_(std::move(name)) {
        clink::aws::ensure_aws_initialized();
        client_ = std::make_unique<Aws::DynamoDB::DynamoDBClient>(
            clink::aws::make_client_config(client_options(endpoint)));
    }
    ~DdbCellTable() {
        Aws::DynamoDB::Model::DeleteTableRequest req;
        req.SetTableName(name_);
        client_->DeleteTable(req);
    }
    DdbCellTable(const DdbCellTable&) = delete;
    DdbCellTable& operator=(const DdbCellTable&) = delete;

    [[nodiscard]] const std::string& name() const { return name_; }

    bool create() {
        using namespace Aws::DynamoDB::Model;
        CreateTableRequest req;
        req.SetTableName(name_);
        req.SetBillingMode(BillingMode::PAY_PER_REQUEST);
        req.AddAttributeDefinitions(AttributeDefinition().WithAttributeName("pk").WithAttributeType(
            ScalarAttributeType::S));
        req.AddAttributeDefinitions(AttributeDefinition().WithAttributeName("sk").WithAttributeType(
            ScalarAttributeType::N));
        req.AddKeySchema(KeySchemaElement().WithAttributeName("pk").WithKeyType(KeyType::HASH));
        req.AddKeySchema(KeySchemaElement().WithAttributeName("sk").WithKeyType(KeyType::RANGE));
        client_->CreateTable(req);
        for (int i = 0; i < 300; ++i) {
            DescribeTableRequest d;
            d.SetTableName(name_);
            auto r = client_->DescribeTable(d);
            if (r.IsSuccess() && r.GetResult().GetTable().GetTableStatus() == TableStatus::ACTIVE) {
                return true;
            }
            std::this_thread::sleep_for(100ms);
        }
        return false;
    }

    // Every item, by a consistent paginated Scan, keyed by "<pk>/<sk>".
    std::map<std::string, CellItem> items() {
        using Aws::DynamoDB::Model::ValueType;
        std::map<std::string, CellItem> out;
        Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> start;
        do {
            Aws::DynamoDB::Model::ScanRequest req;
            req.SetTableName(name_);
            req.SetConsistentRead(true);
            if (!start.empty()) {
                req.SetExclusiveStartKey(start);
            }
            const auto r = client_->Scan(req);
            if (!r.IsSuccess()) {
                ADD_FAILURE() << "Scan failed: " << r.GetError().GetMessage();
                return out;
            }
            for (const auto& row : r.GetResult().GetItems()) {
                CellItem item;
                for (const auto& [k, v] : row) {
                    if (v.GetType() == ValueType::NUMBER) {
                        item[k] = "N:" + integer_text(v.GetN());
                    } else if (v.GetType() == ValueType::STRING) {
                        item[k] = "S:" + std::string(v.GetS());
                    } else {
                        item[k] = "<other type>";
                    }
                }
                out[item["pk"] + "/" + item["sk"]] = std::move(item);
            }
            start = r.GetResult().GetLastEvaluatedKey();
        } while (!start.empty());
        return out;
    }

private:
    std::string name_;
    std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client_;
};

// ---- the records the test writes --------------------------------------------------------

// Writes `limit` records at about `per_second`, in order, and keeps the item
// each record names with what that record says, so the last record of an item
// is the item's expected state.
class DdbCellWriter {
public:
    DdbCellWriter(DdbCellStream& stream, int limit, int per_second)
        : stream_(stream), limit_(limit), per_tick_(std::max(per_second / 20, 1)) {}
    ~DdbCellWriter() { stop(); }
    DdbCellWriter(const DdbCellWriter&) = delete;
    DdbCellWriter& operator=(const DdbCellWriter&) = delete;

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
    // The expected item for every item written to, keyed by "<pk>/<sk>".
    [[nodiscard]] std::map<std::string, CellItem> expected() const {
        std::lock_guard lock(mu_);
        return expected_;
    }

private:
    void run() {
        int next = 0;
        while (!stop_.load() && next < limit_) {
            for (int t = 0; t < per_tick_ && next < limit_; ++t, ++next) {
                const std::string pk = "k" + std::to_string(next % kKeys);
                const int group = (next / kKeys) % kItemsPerKey;
                // One of four numerals for the same sort key, by round.
                const std::string g = std::to_string(group);
                const int round = (next / (kKeys * kItemsPerKey)) % 4;
                const std::string form = round == 0   ? g
                                         : round == 1 ? g + ".0"
                                         : round == 2 ? g + "E0"
                                                      : std::to_string(group * 10) + "E-1";
                const std::string pad =
                    "record-" + std::to_string(next) + "-" + std::string(32, 'p');
                const std::string data = R"({"pk":")" + pk + R"(","sk":)" + form + R"(,"seq":)" +
                                         std::to_string(next) + R"(,"form":")" + form +
                                         R"(","pad":")" + pad + R"("})";
                const bool ok = stream_.put(pk, data);
                std::lock_guard lock(mu_);
                if (!ok) {
                    error_ = "PutRecord failed for " + data;
                    return;
                }
                const std::string sk = "N:" + std::to_string(group);
                expected_["S:" + pk + "/" + sk] = CellItem{{"pk", "S:" + pk},
                                                           {"sk", sk},
                                                           {"seq", "N:" + std::to_string(next)},
                                                           {"form", "S:" + form},
                                                           {"pad", "S:" + pad}};
                written_.store(next + 1);
            }
            std::this_thread::sleep_for(50ms);
        }
    }

    DdbCellStream& stream_;
    int limit_;
    int per_tick_;
    std::atomic<bool> stop_{false};
    std::atomic<int> written_{0};
    mutable std::mutex mu_;
    std::map<std::string, CellItem> expected_;
    std::string error_;
    std::thread thread_;
};

struct Verdict {
    std::size_t items{0};
    std::size_t missing{0};
    std::size_t wrong{0};
    std::size_t unexpected{0};
};

// The table against the expected items: each present, each attribute equal,
// nothing else. Reports the first few differences.
Verdict judge(const std::map<std::string, CellItem>& expected,
              const std::map<std::string, CellItem>& table,
              bool report) {
    Verdict v;
    v.items = table.size();
    for (const auto& [key, want] : expected) {
        const auto it = table.find(key);
        if (it == table.end()) {
            if (++v.missing <= 5 && report) {
                ADD_FAILURE() << "item " << key << " is missing";
            }
            continue;
        }
        if (it->second != want) {
            if (++v.wrong <= 5 && report) {
                std::string got;
                std::string w;
                for (const auto& [k, x] : it->second) {
                    got += k + "=" + x + " ";
                }
                for (const auto& [k, x] : want) {
                    w += k + "=" + x + " ";
                }
                ADD_FAILURE() << "item " << key << " holds\n  " << got << "\nwhere the last record "
                              << "written for it was\n  " << w;
            }
        }
    }
    for (const auto& [key, item] : table) {
        (void)item;
        if (expected.count(key) == 0 && ++v.unexpected <= 5 && report) {
            ADD_FAILURE() << "item " << key << " was never written";
        }
    }
    return v;
}

// ---- the cells ---------------------------------------------------------------------------

class DynamoDbRecovery : public ::testing::Test {
protected:
    void SetUp() override {
        endpoint_ = env_or_empty("CLINK_DYNAMODB_TEST_ENDPOINT");
        if (endpoint_.empty()) {
            GTEST_SKIP() << "set CLINK_DYNAMODB_TEST_ENDPOINT (scripts/dynamodb-live.sh does)";
        }
        if (!std::filesystem::exists(recovery_node_binary())) {
            GTEST_SKIP() << "the clink_node binary is not built";
        }
        static int counter = 0;
        const auto suffix = std::to_string(::getpid()) + "-" + std::to_string(++counter);
        stream_ = std::make_unique<DdbCellStream>(endpoint_, "clink-ddb-recovery-" + suffix);
        table_ = std::make_unique<DdbCellTable>(endpoint_, "clink-ddb-recovery-" + suffix);
        ASSERT_TRUE(table_->create()) << "the table did not become ACTIVE";
    }

    clink::cluster::JobGraphSpec graph(std::uint32_t parallelism) const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "kinesis_source";
        src.id = "src";
        src.uid = "dynamodb-recovery-src";
        src.parallelism = parallelism;
        src.out_channel = std::string{clink::cluster::kChannelString};
        src.params = {{"stream", stream_->name()},
                      {"region", "us-east-1"},
                      {"endpoint_override", endpoint_},
                      {"initial_position", "trim_horizon"},
                      {"poll_interval_ms", "50"},
                      {"discovery_interval_ms", "500"},
                      {"max_records_per_poll", "40"}};
        g.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.type = "dynamodb_sink";
        snk.id = "snk";
        snk.uid = "dynamodb-recovery-snk";
        snk.inputs = {"src"};
        snk.parallelism = parallelism;  // forward: source subtask i feeds sink subtask i
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = {{"table", table_->name()},
                      {"partition_key", "pk"},
                      {"sort_key", "sk"},
                      {"region", "us-east-1"},
                      {"endpoint_override", endpoint_},
                      {"batch_records", "10"}};
        g.ops.push_back(snk);
        return g;
    }

    // The AWS SDK signs with these; LocalStack accepts any credentials.
    static ProcOptions worker_options(std::string fault = {}) {
        ProcOptions o;
        o.fault = std::move(fault);
        o.env = {{"AWS_ACCESS_KEY_ID", "test"},
                 {"AWS_SECRET_ACCESS_KEY", "test"},
                 {"AWS_REGION", "us-east-1"},
                 {"AWS_EC2_METADATA_DISABLED", "true"}};
        return o;
    }

    // A coordinator and worker 0, armed with `fault`: the job submitted next
    // places every task on worker 0. Worker 1 joins after the deploy, unarmed,
    // as the spare the restart uses.
    std::unique_ptr<Cluster> make_cluster(const std::string& fault) {
        ClusterSpec spec;
        spec.node_binary = recovery_node_binary();
        spec.workers = 2;
        spec.slots_per_worker = 4;
        spec.http = true;
        auto cluster = std::make_unique<Cluster>(spec);
        if (!cluster->start_coordinator() || !cluster->start_worker(0, worker_options(fault)) ||
            !cluster->await_workers_registered(1)) {
            ADD_FAILURE() << "the cluster did not start";
        }
        return cluster;
    }

    void submit(Cluster& cluster, std::uint32_t parallelism) {
        clink::http_connector::HttpRequest::Options o;
        o.base_url = "http://127.0.0.1:" + std::to_string(cluster.http_port());
        clink::http_connector::HttpRequest http(std::move(o));
        const auto r = http.post("/api/v1/jobs/spec?name=dynamodb-recovery&checkpoint_dir=" +
                                     cluster.checkpoint_dir().string() +
                                     "&checkpoint_interval_ms=1000&alignment=aligned"
                                     "&max_restarts_on_worker_loss=8",
                                 graph(parallelism).to_json(),
                                 "application/json");
        ASSERT_NE(r.body.find("\"ok\":true"), std::string::npos)
            << "the job was not accepted: " << r.body;
    }

    void add_spare_worker(Cluster& cluster) {
        ASSERT_TRUE(cluster.start_worker(1, worker_options()));
        ASSERT_TRUE(cluster.await_workers_registered(2));
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

    static bool parked_at(Cluster& cluster, const std::string& point) {
        const std::string witness = "[fault.injection] fired: " + point;
        return clink::itest::await([&] { return cluster.worker(0).log_contains(witness); }, 120s);
    }

    void kill_and_replace(Cluster& cluster) {
        cluster.worker(0).kill_hard();
        ASSERT_TRUE(cluster.worker(0).await_exit(10s).has_value());
        killed_log_ = cluster.worker(0).read_log();
        ASSERT_TRUE(cluster.restart_worker(0, worker_options()));
    }

    // The table reaches the expected items, and two more checkpoints complete.
    void await_converged(Cluster& cluster, const std::map<std::string, CellItem>& expected) {
        const bool converged = clink::itest::await(
            [&] {
                const auto v = judge(expected, table_->items(), /*report=*/false);
                return v.missing == 0 && v.wrong == 0 && v.unexpected == 0;
            },
            240s);
        if (!converged) {
            ADD_FAILURE() << "the table did not reach the last record of every item";
            return;
        }
        const auto after = latest_completed(cluster);
        ASSERT_TRUE(
            clink::itest::await([&] { return latest_completed(cluster) >= after + 2; }, 60s))
            << "checkpoints stopped completing";
    }

    void keep_logs(const char* cell, Cluster& cluster) const {
        const auto into = env_or_empty("CLINK_DYNAMODB_RECOVERY_LOG_DIR");
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

    // One cell: write, park worker 0 at `point`, kill it, and judge the table.
    void run_cell(const char* cell,
                  std::uint32_t parallelism,
                  const std::string& point,
                  int ordinal) {
        ASSERT_TRUE(stream_->create(static_cast<int>(parallelism)));
        auto cluster = make_cluster(point + "=block@" + std::to_string(ordinal));
        DdbCellWriter writer(*stream_, 1200, 120);
        writer.start();
        submit(*cluster, parallelism);
        add_spare_worker(*cluster);
        ASSERT_TRUE(parked_at(*cluster, point)) << "worker 0 never reached " << point << "\n"
                                                << cluster->worker(0).read_log();
        const auto written_at_kill = writer.written();
        const auto items_at_kill = table_->items().size();
        const auto completed_at_kill = latest_completed(*cluster);
        kill_and_replace(*cluster);
        writer.await_done();
        ASSERT_EQ(writer.error(), "");
        const auto expected = writer.expected();
        ASSERT_EQ(expected.size(), static_cast<std::size_t>(kKeys * kItemsPerKey));
        await_converged(*cluster, expected);
        const auto v = judge(expected, table_->items(), /*report=*/true);
        const auto n_restarts = restarts(*cluster);
        EXPECT_GE(n_restarts, 1u) << "the kill did not restart the job";
        EXPECT_NE(killed_log_.find("[fault.injection] fired: " + point), std::string::npos);
        std::printf(
            "[dynamodb-recovery] cell=%s written=%d items=%zu expected_items=%zu missing=%zu "
            "wrong=%zu unexpected=%zu restarts=%zu written_at_kill=%d items_at_kill=%zu "
            "checkpoint_at_kill=%llu\n",
            cell,
            writer.written(),
            v.items,
            expected.size(),
            v.missing,
            v.wrong,
            v.unexpected,
            n_restarts,
            written_at_kill,
            items_at_kill,
            static_cast<unsigned long long>(completed_at_kill));
        std::fflush(stdout);
        keep_logs(cell, *cluster);
    }

    std::string endpoint_;
    std::unique_ptr<DdbCellStream> stream_;
    std::unique_ptr<DdbCellTable> table_;
    std::string killed_log_;
};

}  // namespace

// Parallelism 1: parked after a write the service applied and no checkpoint
// covers, killed; the restart rewrites those items and every item converges.
TEST_F(DynamoDbRecovery, AWorkerKilledAfterAnUncoveredWriteConverges) {
    run_cell("p1_kill_after_write", 1, "dynamodb_sink.after_write", 20);
}

// Parallelism 2: each item has one writing subtask; killed after a write.
TEST_F(DynamoDbRecovery, TwoSubtasksKilledAfterAWriteConverge) {
    run_cell("p2_kill_after_write", 2, "dynamodb_sink.after_write", 15);
}

// Parked with a request built and not sent: the records it held come back from
// the stream after the restart.
TEST_F(DynamoDbRecovery, AWorkerKilledBeforeAWriteLosesNothing) {
    run_cell("p1_kill_before_write", 1, "dynamodb_sink.before_write", 12);
}
