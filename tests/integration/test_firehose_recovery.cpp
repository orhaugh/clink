// The Firehose sink's recovery cells: a coordinator and two clink_node workers
// run a job that reads a LocalStack Kinesis stream (kinesis_source) into the
// Firehose sink, whose DirectPut delivery stream writes to a LocalStack S3
// bucket, while the test writes 1,200 unique records. Each cell parks worker 0
// at one of the sink's fault points and kills it:
//
//   1. parallelism 1, parked after PutRecordBatch accepted a batch and before any
//      checkpoint covers it (firehose_sink.after_write): the restart replays that
//      batch, which reaches the bucket twice.
//   2. parallelism 2, the same.
//   3. parallelism 1, parked with a request built and not sent
//      (firehose_sink.before_write).
//
// The contract checked is the sink's: every record accepted by Firehose at
// least once, nothing else, each byte for byte. Duplicates are allowed, and the
// cells parked after an acceptance require some, since the kill came before a
// checkpoint covered the accepted batch. The bucket is read with the SDK's own
// S3 client; the fault and the restart are witnessed in the logs.
//
// Needs the endpoint scripts/firehose-live.sh starts (CLINK_FIREHOSE_TEST_ENDPOINT,
// serving Firehose, S3, STS and Kinesis, and the AWS variables the SDK signs
// with) and the clink_node binary of a build with fault injection. Each cell
// prints one "[firehose-recovery]" line and, with CLINK_FIREHOSE_RECOVERY_LOG_DIR
// set, keeps the cluster's logs there.

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
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/firehose/FirehoseClient.h>
#include <aws/firehose/model/CreateDeliveryStreamRequest.h>
#include <aws/firehose/model/DeleteDeliveryStreamRequest.h>
#include <aws/firehose/model/DescribeDeliveryStreamRequest.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/CreateStreamRequest.h>
#include <aws/kinesis/model/DeleteStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamSummaryRequest.h>
#include <aws/kinesis/model/PutRecordRequest.h>
#include <aws/kinesis/model/StreamStatus.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
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

std::filesystem::path fh_node_binary() {
#ifdef CLINK_NODE_BINARY
    return std::filesystem::path{CLINK_NODE_BINARY};
#else
    return {};
#endif
}

std::string fh_env(const char* name) {
    const char* v = std::getenv(name);
    return v == nullptr ? std::string{} : std::string{v};
}

clink::aws::AwsClientOptions fh_cell_client(const std::string& endpoint) {
    clink::aws::AwsClientOptions o;
    o.region = "us-east-1";
    o.endpoint_override = endpoint;
    o.request_timeout_ms = 60000;
    return o;
}

// The stream the job reads, the delivery stream it writes, and the bucket that
// receives what Firehose accepted.
class FhCellPlumbing {
public:
    FhCellPlumbing(const std::string& endpoint, const std::string& name)
        : stream_(name), delivery_(name), bucket_(name + "-dest") {
        clink::aws::ensure_aws_initialized();
        const auto cfg = clink::aws::make_client_config(fh_cell_client(endpoint));
        kinesis_ = std::make_unique<Aws::Kinesis::KinesisClient>(cfg);
        firehose_ = std::make_unique<Aws::Firehose::FirehoseClient>(cfg);
        s3_ = std::make_unique<Aws::S3::S3Client>(
            cfg, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
    }
    ~FhCellPlumbing() {
        Aws::Kinesis::Model::DeleteStreamRequest ks;
        ks.SetStreamName(stream_);
        kinesis_->DeleteStream(ks);
        Aws::Firehose::Model::DeleteDeliveryStreamRequest fs;
        fs.SetDeliveryStreamName(delivery_);
        firehose_->DeleteDeliveryStream(fs);
    }
    FhCellPlumbing(const FhCellPlumbing&) = delete;
    FhCellPlumbing& operator=(const FhCellPlumbing&) = delete;

    [[nodiscard]] const std::string& stream() const { return stream_; }
    [[nodiscard]] const std::string& delivery() const { return delivery_; }

    bool create(int shards) {
        Aws::Kinesis::Model::CreateStreamRequest ks;
        ks.SetStreamName(stream_);
        ks.SetShardCount(shards);
        kinesis_->CreateStream(ks);
        Aws::S3::Model::CreateBucketRequest b;
        b.SetBucket(bucket_);
        if (!s3_->CreateBucket(b).IsSuccess()) {
            return false;
        }
        using namespace Aws::Firehose::Model;
        CreateDeliveryStreamRequest fs;
        fs.SetDeliveryStreamName(delivery_);
        fs.SetDeliveryStreamType(DeliveryStreamType::DirectPut);
        ExtendedS3DestinationConfiguration s3;
        s3.SetRoleARN("arn:aws:iam::000000000000:role/firehose");
        s3.SetBucketARN("arn:aws:s3:::" + bucket_);
        s3.SetPrefix("out/");
        s3.SetBufferingHints(BufferingHints().WithSizeInMBs(1).WithIntervalInSeconds(60));
        fs.SetExtendedS3DestinationConfiguration(s3);
        if (!firehose_->CreateDeliveryStream(fs).IsSuccess()) {
            return false;
        }
        for (int i = 0; i < 300; ++i) {
            Aws::Kinesis::Model::DescribeStreamSummaryRequest d;
            d.SetStreamName(stream_);
            auto kr = kinesis_->DescribeStreamSummary(d);
            DescribeDeliveryStreamRequest fd;
            fd.SetDeliveryStreamName(delivery_);
            auto fr = firehose_->DescribeDeliveryStream(fd);
            if (kr.IsSuccess() && fr.IsSuccess() &&
                kr.GetResult().GetStreamDescriptionSummary().GetStreamStatus() ==
                    Aws::Kinesis::Model::StreamStatus::ACTIVE &&
                fr.GetResult().GetDeliveryStreamDescription().GetDeliveryStreamStatus() ==
                    DeliveryStreamStatus::ACTIVE) {
                return true;
            }
            std::this_thread::sleep_for(100ms);
        }
        return false;
    }

    bool put(const std::string& key, const std::string& data) {
        Aws::Kinesis::Model::PutRecordRequest req;
        req.SetStreamName(stream_);
        req.SetPartitionKey(key);
        req.SetData(Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(data.data()),
                                           data.size()));
        for (int attempt = 0; attempt < 20; ++attempt) {
            if (kinesis_->PutRecord(req).IsSuccess()) {
                return true;
            }
            std::this_thread::sleep_for(100ms);
        }
        return false;
    }

    // Every record in the bucket: each object split at the newline delimiter.
    std::vector<std::string> delivered() {
        std::vector<std::string> keys;
        Aws::String token;
        do {
            Aws::S3::Model::ListObjectsV2Request req;
            req.SetBucket(bucket_);
            req.SetPrefix("out/");
            if (!token.empty()) {
                req.SetContinuationToken(token);
            }
            const auto r = s3_->ListObjectsV2(req);
            if (!r.IsSuccess()) {
                ADD_FAILURE() << "ListObjectsV2 failed: " << r.GetError().GetMessage();
                return {};
            }
            for (const auto& o : r.GetResult().GetContents()) {
                keys.push_back(o.GetKey());
            }
            token = r.GetResult().GetIsTruncated() ? r.GetResult().GetNextContinuationToken() : "";
        } while (!token.empty());
        std::vector<std::string> records;
        for (const auto& key : keys) {
            Aws::S3::Model::GetObjectRequest get;
            get.SetBucket(bucket_);
            get.SetKey(key);
            auto r = s3_->GetObject(get);
            if (!r.IsSuccess()) {
                ADD_FAILURE() << "GetObject " << key << " failed";
                continue;
            }
            std::stringstream body;
            body << r.GetResult().GetBody().rdbuf();
            const std::string content = body.str();
            std::size_t start = 0;
            while (start < content.size()) {
                const auto end = content.find('\n', start);
                if (end == std::string::npos) {
                    records.push_back("<cut>" + content.substr(start));
                    break;
                }
                records.push_back(content.substr(start, end - start));
                start = end + 1;
            }
        }
        return records;
    }

private:
    std::string stream_;
    std::string delivery_;
    std::string bucket_;
    std::unique_ptr<Aws::Kinesis::KinesisClient> kinesis_;
    std::unique_ptr<Aws::Firehose::FirehoseClient> firehose_;
    std::unique_ptr<Aws::S3::S3Client> s3_;
};

// Writes `limit` unique records at about `per_second`, keyed over twelve
// partition keys, and keeps what it wrote.
class FhCellWriter {
public:
    FhCellWriter(FhCellPlumbing& plumbing, int limit, int per_second)
        : plumbing_(plumbing), limit_(limit), per_tick_(std::max(per_second / 20, 1)) {}
    ~FhCellWriter() { stop(); }
    FhCellWriter(const FhCellWriter&) = delete;
    FhCellWriter& operator=(const FhCellWriter&) = delete;

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
                const std::string key = "k" + std::to_string(next % 12);
                const std::string data = "fh-" + std::to_string(next) + "-" + key + "-" +
                                         std::string(static_cast<std::size_t>(next % 61), 'x');
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

    FhCellPlumbing& plumbing_;
    int limit_;
    int per_tick_;
    std::atomic<bool> stop_{false};
    std::atomic<int> written_{0};
    mutable std::mutex mu_;
    std::vector<std::string> records_;
    std::string error_;
    std::thread thread_;
};

struct FhVerdict {
    std::size_t arrived{0};
    std::size_t missing{0};
    std::size_t unexpected{0};
    std::size_t duplicates{0};
};

FhVerdict fh_judge(const std::vector<std::string>& written,
                   const std::vector<std::string>& arrived,
                   bool report) {
    std::map<std::string, int> got;
    for (const auto& r : arrived) {
        ++got[r];
    }
    FhVerdict v;
    v.arrived = arrived.size();
    std::map<std::string, int> want;
    for (const auto& r : written) {
        ++want[r];
    }
    for (const auto& [r, n] : want) {
        const auto it = got.find(r);
        if (it == got.end()) {
            if (++v.missing <= 5 && report) {
                ADD_FAILURE() << "record " << r << " never reached the bucket";
            }
        } else if (it->second > n) {
            v.duplicates += static_cast<std::size_t>(it->second - n);
        }
    }
    for (const auto& [r, n] : got) {
        if (want.count(r) == 0) {
            v.unexpected += static_cast<std::size_t>(n);
            if (v.unexpected <= 5 && report) {
                ADD_FAILURE() << "the bucket holds " << r << ", which was never written";
            }
        }
    }
    return v;
}

class FirehoseRecovery : public ::testing::Test {
protected:
    void SetUp() override {
        endpoint_ = fh_env("CLINK_FIREHOSE_TEST_ENDPOINT");
        if (endpoint_.empty()) {
            GTEST_SKIP() << "set CLINK_FIREHOSE_TEST_ENDPOINT (scripts/firehose-live.sh does)";
        }
        if (!std::filesystem::exists(fh_node_binary())) {
            GTEST_SKIP() << "the clink_node binary is not built";
        }
        static int counter = 0;
        plumbing_ = std::make_unique<FhCellPlumbing>(
            endpoint_,
            "clink-fh-recovery-" + std::to_string(::getpid()) + "-" + std::to_string(++counter));
    }

    clink::cluster::JobGraphSpec graph(std::uint32_t parallelism) const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "kinesis_source";
        src.id = "src";
        src.uid = "firehose-recovery-src";
        src.parallelism = parallelism;
        src.out_channel = std::string{clink::cluster::kChannelString};
        src.params = {{"stream", plumbing_->stream()},
                      {"region", "us-east-1"},
                      {"endpoint_override", endpoint_},
                      {"initial_position", "trim_horizon"},
                      {"poll_interval_ms", "50"},
                      {"discovery_interval_ms", "500"},
                      {"max_records_per_poll", "40"}};
        g.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.type = "firehose_sink";
        snk.id = "snk";
        snk.uid = "firehose-recovery-snk";
        snk.inputs = {"src"};
        snk.parallelism = parallelism;
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = {{"delivery_stream", plumbing_->delivery()},
                      {"region", "us-east-1"},
                      {"endpoint_override", endpoint_},
                      {"batch_records", "20"}};
        g.ops.push_back(snk);
        return g;
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

    std::unique_ptr<Cluster> make_cluster(const std::string& fault) {
        ClusterSpec spec;
        spec.node_binary = fh_node_binary();
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
        const auto r = http.post("/api/v1/jobs/spec?name=firehose-recovery&checkpoint_dir=" +
                                     cluster.checkpoint_dir().string() +
                                     "&checkpoint_interval_ms=1000&alignment=aligned"
                                     "&max_restarts_on_worker_loss=8",
                                 graph(parallelism).to_json(),
                                 "application/json");
        ASSERT_NE(r.body.find("\"ok\":true"), std::string::npos)
            << "the job was not accepted: " << r.body;
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
        const auto into = fh_env("CLINK_FIREHOSE_RECOVERY_LOG_DIR");
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

    void run_cell(const char* cell,
                  std::uint32_t parallelism,
                  const std::string& point,
                  int ordinal,
                  bool expect_duplicates) {
        ASSERT_TRUE(plumbing_->create(static_cast<int>(parallelism)));
        auto cluster = make_cluster(point + "=block@" + std::to_string(ordinal));
        FhCellWriter writer(*plumbing_, 1200, 120);
        writer.start();
        submit(*cluster, parallelism);
        ASSERT_TRUE(cluster->start_worker(1, worker_options()));
        ASSERT_TRUE(cluster->await_workers_registered(2));
        const std::string witness = "[fault.injection] fired: " + point;
        ASSERT_TRUE(
            clink::itest::await([&] { return cluster->worker(0).log_contains(witness); }, 120s))
            << "worker 0 never reached " << point << "\n"
            << cluster->worker(0).read_log();
        const auto written_at_kill = writer.written();
        const auto arrived_at_kill = plumbing_->delivered().size();
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
                const auto v = fh_judge(written, plumbing_->delivered(), false);
                return v.missing == 0;
            },
            240s);
        EXPECT_TRUE(arrived) << "records were still missing from the bucket";
        const auto after = latest_completed(*cluster);
        EXPECT_TRUE(
            clink::itest::await([&] { return latest_completed(*cluster) >= after + 2; }, 60s))
            << "checkpoints stopped completing";
        const auto v = fh_judge(written, plumbing_->delivered(), true);
        const auto restarts =
            cluster->count_in_coordinator_log("[coordinator.restart] [info] job_id=1 attempt=");
        EXPECT_EQ(v.missing, 0u);
        EXPECT_EQ(v.unexpected, 0u);
        EXPECT_GE(restarts, 1u) << "the kill did not restart the job";
        EXPECT_NE(killed_log_.find(witness), std::string::npos);
        if (expect_duplicates) {
            // The parked batch was accepted and no checkpoint covered it, so the
            // replay sent it again.
            EXPECT_GE(v.duplicates, 1u) << "the accepted, uncovered batch was not sent again";
        }
        std::printf(
            "[firehose-recovery] cell=%s written=%zu arrived=%zu missing=%zu unexpected=%zu "
            "duplicates=%zu restarts=%zu written_at_kill=%d arrived_at_kill=%zu "
            "checkpoint_at_kill=%llu\n",
            cell,
            written.size(),
            v.arrived,
            v.missing,
            v.unexpected,
            v.duplicates,
            restarts,
            written_at_kill,
            arrived_at_kill,
            static_cast<unsigned long long>(checkpoint_at_kill));
        std::fflush(stdout);
        keep_logs(cell, *cluster);
    }

    std::string endpoint_;
    std::unique_ptr<FhCellPlumbing> plumbing_;
    std::string killed_log_;
};

}  // namespace

TEST_F(FirehoseRecovery, AWorkerKilledAfterAnUncoveredAcceptanceLosesNothing) {
    run_cell(
        "p1_kill_after_accept", 1, "firehose_sink.after_write", 15, /*expect_duplicates=*/true);
}

TEST_F(FirehoseRecovery, TwoSubtasksKilledAfterAnAcceptanceLoseNothing) {
    run_cell(
        "p2_kill_after_accept", 2, "firehose_sink.after_write", 12, /*expect_duplicates=*/true);
}

TEST_F(FirehoseRecovery, AWorkerKilledBeforeASendLosesNothing) {
    run_cell(
        "p1_kill_before_send", 1, "firehose_sink.before_write", 10, /*expect_duplicates=*/false);
}
