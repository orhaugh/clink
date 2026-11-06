// The Kinesis source's recovery cells: a coordinator and two clink_node workers
// run a job graph that reads a LocalStack stream (kinesis_source) into the file
// two-phase-commit sink, while the test writes records "k<key>:<ordinal>" with
// twelve partition keys and reshards the stream under the running job. Each
// cell kills worker 0 while records are in flight and checks what the sink
// committed against the test's own record of what it wrote:
//
//   1. parallelism 1: the stream is split while the job reads, worker 0 parks at
//      kinesis.source_after_emit (records handed to the job, no checkpoint
//      covering them) and is SIGKILLed, and the split's children are merged
//      after the restore. Every record is committed once, and every key's
//      records in the order written, across the split and the merge.
//   2. parallelism 2: the stream is resharded uniformly (every shard split)
//      before worker 0 parks and is SIGKILLed. Every record is committed once,
//      and each shard's records in the order written.
//   3. latest mode: a checkpoint completes before the first record, worker 0 is
//      SIGKILLed, and records are written while the job is down. The restored
//      source keeps the first run's start time: every record written after the
//      job started is committed once, and none written before it.
//
// The pass criteria read the sink's committed files and the stream's own
// listing, not anything clink reports; the restarts, the fault and the source's
// reshard handling are witnessed in the logs. Ordering is checked only where the
// source promises it: per key at parallelism 1, per shard above it.
//
// Needs the stream endpoint scripts/kinesis-live.sh starts
// (CLINK_KINESIS_TEST_ENDPOINT and the AWS variables the SDK signs with) and
// the clink_node binary of a build with fault injection. Each cell prints one
// "[kinesis-recovery]" line and, with CLINK_KINESIS_RECOVERY_LOG_DIR set, keeps
// the cluster's logs there.

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
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/CreateStreamRequest.h>
#include <aws/kinesis/model/DeleteStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamSummaryRequest.h>
#include <aws/kinesis/model/ListShardsRequest.h>
#include <aws/kinesis/model/MergeShardsRequest.h>
#include <aws/kinesis/model/PutRecordRequest.h>
#include <aws/kinesis/model/SplitShardRequest.h>
#include <aws/kinesis/model/StreamStatus.h>
#include <aws/kinesis/model/UpdateShardCountRequest.h>
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

// ---- the stream, driven with the SDK ----------------------------------------------

struct ListedShard {
    std::string id;
    std::string parent;
    std::string adjacent_parent;
    std::string start_hash;
    std::string end_hash;
    bool open{true};
};

// 128-bit hash keys; __extension__ keeps -Wpedantic quiet about __int128.
__extension__ typedef unsigned __int128 u128;
u128 parse_u128(const std::string& s) {
    u128 v = 0;
    for (const char ch : s) {
        v = v * 10 + static_cast<unsigned>(ch - '0');
    }
    return v;
}
std::string format_u128(u128 v) {
    std::string out;
    do {
        out.insert(out.begin(), static_cast<char>('0' + static_cast<int>(v % 10)));
        v /= 10;
    } while (v > 0);
    return out;
}

class Stream {
public:
    Stream(const std::string& endpoint, std::string name) : name_(std::move(name)) {
        clink::aws::ensure_aws_initialized();
        clink::aws::AwsClientOptions o;
        o.region = "us-east-1";
        o.endpoint_override = endpoint;
        client_ = std::make_unique<Aws::Kinesis::KinesisClient>(clink::aws::make_client_config(o));
    }
    ~Stream() {
        Aws::Kinesis::Model::DeleteStreamRequest req;
        req.SetStreamName(name_);
        client_->DeleteStream(req);
    }
    Stream(const Stream&) = delete;
    Stream& operator=(const Stream&) = delete;

    [[nodiscard]] const std::string& name() const { return name_; }

    bool create(int shards) {
        Aws::Kinesis::Model::CreateStreamRequest req;
        req.SetStreamName(name_);
        req.SetShardCount(shards);
        client_->CreateStream(req);
        return await_active();
    }

    bool await_active() {
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

    // Returns the shard the record went to, empty on failure.
    std::string put(const std::string& key, const std::string& data) {
        std::lock_guard lock(mu_);
        Aws::Kinesis::Model::PutRecordRequest req;
        req.SetStreamName(name_);
        req.SetPartitionKey(key);
        req.SetData(Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(data.data()),
                                           data.size()));
        for (int attempt = 0; attempt < 20; ++attempt) {
            auto r = client_->PutRecord(req);
            if (r.IsSuccess()) {
                return r.GetResult().GetShardId();
            }
            std::this_thread::sleep_for(100ms);  // the stream is resharding
        }
        return {};
    }

    std::vector<ListedShard> shards() {
        std::lock_guard lock(mu_);
        std::vector<ListedShard> out;
        std::string token;
        do {
            Aws::Kinesis::Model::ListShardsRequest req;
            if (token.empty()) {
                req.SetStreamName(name_);
            } else {
                req.SetNextToken(token);
            }
            auto r = client_->ListShards(req);
            if (!r.IsSuccess()) {
                ADD_FAILURE() << "ListShards: " << r.GetError().GetMessage();
                return out;
            }
            for (const auto& sh : r.GetResult().GetShards()) {
                out.push_back(
                    ListedShard{sh.GetShardId(),
                                sh.GetParentShardId(),
                                sh.GetAdjacentParentShardId(),
                                sh.GetHashKeyRange().GetStartingHashKey(),
                                sh.GetHashKeyRange().GetEndingHashKey(),
                                sh.GetSequenceNumberRange().GetEndingSequenceNumber().empty()});
            }
            token = r.GetResult().GetNextToken();
        } while (!token.empty());
        return out;
    }

    bool split_in_half(const std::string& id) {
        for (const auto& sh : shards()) {
            if (sh.id != id) {
                continue;
            }
            std::lock_guard lock(mu_);
            Aws::Kinesis::Model::SplitShardRequest req;
            req.SetStreamName(name_);
            req.SetShardToSplit(id);
            req.SetNewStartingHashKey(
                format_u128((parse_u128(sh.start_hash) + parse_u128(sh.end_hash)) / 2 + 1));
            if (!client_->SplitShard(req).IsSuccess()) {
                return false;
            }
            break;
        }
        return await_active();
    }

    bool merge(const std::string& a, const std::string& b) {
        {
            std::lock_guard lock(mu_);
            Aws::Kinesis::Model::MergeShardsRequest req;
            req.SetStreamName(name_);
            req.SetShardToMerge(a);
            req.SetAdjacentShardToMerge(b);
            if (!client_->MergeShards(req).IsSuccess()) {
                return false;
            }
        }
        return await_active();
    }

    bool scale_to(int shards) {
        {
            std::lock_guard lock(mu_);
            Aws::Kinesis::Model::UpdateShardCountRequest req;
            req.SetStreamName(name_);
            req.SetTargetShardCount(shards);
            req.SetScalingType(Aws::Kinesis::Model::ScalingType::UNIFORM_SCALING);
            if (!client_->UpdateShardCount(req).IsSuccess()) {
                return false;
            }
        }
        return await_active();
    }

private:
    std::string name_;
    std::unique_ptr<Aws::Kinesis::KinesisClient> client_;
    std::mutex mu_;
};

// Writes "k<i % 12>:<ordinal>" records at a steady rate from its own thread,
// keeping the order it wrote them in and the shard each went to.
class Writer {
public:
    Writer(Stream& stream, std::string prefix, int limit, int per_second)
        : stream_(stream),
          prefix_(std::move(prefix)),
          limit_(limit),
          per_tick_(std::max(per_second / 20, 1)) {}
    ~Writer() { stop(); }
    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;

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
    [[nodiscard]] std::vector<std::string> order() const {
        std::lock_guard lock(mu_);
        return order_;
    }
    [[nodiscard]] std::map<std::string, std::string> where() const {
        std::lock_guard lock(mu_);
        return where_;
    }
    [[nodiscard]] std::string error() const {
        std::lock_guard lock(mu_);
        return error_;
    }

private:
    void run() {
        std::map<std::string, int> ordinal;
        int next = 0;
        while (!stop_.load() && next < limit_) {
            for (int k = 0; k < per_tick_ && next < limit_; ++k, ++next) {
                const std::string key = "k" + std::to_string(next % 12);
                const std::string data = prefix_ + key + ":" + std::to_string(ordinal[key]++);
                const auto shard = stream_.put(key, data);
                std::lock_guard lock(mu_);
                if (shard.empty()) {
                    error_ = "PutRecord failed for " + data;
                    return;
                }
                order_.push_back(data);
                where_[data] = shard;
                written_.store(next + 1);
            }
            std::this_thread::sleep_for(50ms);
        }
    }

    Stream& stream_;
    std::string prefix_;
    int limit_;
    int per_tick_;
    std::atomic<bool> stop_{false};
    std::atomic<int> written_{0};
    mutable std::mutex mu_;
    std::vector<std::string> order_;
    std::map<std::string, std::string> where_;
    std::string error_;
    std::thread thread_;
};

// ---- the committed output ------------------------------------------------------------

// Every committed line, in commit order: files by checkpoint id, lines in order.
std::vector<std::string> committed_lines(const std::filesystem::path& dir) {
    std::vector<std::pair<std::uint64_t, std::filesystem::path>> files;
    std::error_code ec;
    for (const auto& e : std::filesystem::directory_iterator(dir / "committed", ec)) {
        const auto stem = e.path().stem().string();  // sub<N>-<run>-<ckpt>
        const auto dash = stem.rfind('-');
        if (dash == std::string::npos) {
            continue;
        }
        files.emplace_back(std::stoull(stem.substr(dash + 1)), e.path());
    }
    std::sort(files.begin(), files.end());
    std::vector<std::string> lines;
    for (const auto& [ckpt, path] : files) {
        std::ifstream in(path);
        for (std::string line; std::getline(in, line);) {
            if (!line.empty()) {
                lines.push_back(line);
            }
        }
    }
    return lines;
}

struct Verdict {
    std::size_t committed{0};
    std::size_t missing{0};
    std::size_t doubled{0};
    std::size_t unexpected{0};
    std::size_t out_of_order{0};
};

// Committed against written: every written record once, nothing else, and the
// order the cell promises (per key, or per shard).
Verdict judge(const std::vector<std::string>& committed,
              const std::vector<std::string>& written,
              const std::map<std::string, std::string>& where,
              bool per_key) {
    Verdict v;
    v.committed = committed.size();
    std::map<std::string, int> count;
    for (const auto& c : committed) {
        ++count[c];
    }
    const std::set<std::string> wrote(written.begin(), written.end());
    for (const auto& w : written) {
        const auto it = count.find(w);
        if (it == count.end()) {
            if (++v.missing <= 5) {
                ADD_FAILURE() << "record " << w << " was written and never committed";
            }
        } else if (it->second > 1 && ++v.doubled <= 5) {
            ADD_FAILURE() << "record " << w << " was committed " << it->second << " times";
        }
    }
    for (const auto& [c, n] : count) {
        if (wrote.count(c) == 0 && ++v.unexpected <= 5) {
            ADD_FAILURE() << "committed record " << c << " was never written in this cell";
        }
    }
    // The position of each record in the written order, then the committed
    // sequence of each key (or shard) must climb.
    std::map<std::string, std::size_t> rank;
    for (std::size_t i = 0; i < written.size(); ++i) {
        rank[written[i]] = i;
    }
    std::map<std::string, std::size_t> last;
    for (const auto& c : committed) {
        const auto r = rank.find(c);
        if (r == rank.end()) {
            continue;
        }
        std::string group;
        if (per_key) {
            group = c.substr(0, c.find(':'));
        } else {
            const auto w = where.find(c);
            group = w == where.end() ? std::string{} : w->second;
        }
        const auto it = last.find(group);
        if (it != last.end() && r->second < it->second && ++v.out_of_order <= 5) {
            ADD_FAILURE() << "record " << c << " was committed after a later record of "
                          << (per_key ? "key " : "shard ") << group;
        }
        last[group] = std::max(it == last.end() ? 0 : it->second, r->second);
    }
    return v;
}

// ---- the cells -------------------------------------------------------------------------

class KinesisRecovery : public ::testing::Test {
protected:
    void SetUp() override {
        endpoint_ = env_or_empty("CLINK_KINESIS_TEST_ENDPOINT");
        if (endpoint_.empty()) {
            GTEST_SKIP() << "set CLINK_KINESIS_TEST_ENDPOINT (scripts/kinesis-live.sh does)";
        }
        if (!std::filesystem::exists(recovery_node_binary())) {
            GTEST_SKIP() << "the clink_node binary is not built";
        }
        static int counter = 0;
        stream_ = std::make_unique<Stream>(
            endpoint_,
            "clink-recovery-" + std::to_string(::getpid()) + "-" + std::to_string(++counter));
    }

    clink::cluster::JobGraphSpec graph(std::uint32_t parallelism,
                                       const std::string& initial_position) const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "kinesis_source";
        src.id = "src";
        src.uid = "kinesis-recovery-src";
        src.parallelism = parallelism;
        src.out_channel = std::string{clink::cluster::kChannelString};
        src.params = {{"stream", stream_->name()},
                      {"region", "us-east-1"},
                      {"endpoint_override", endpoint_},
                      {"initial_position", initial_position},
                      {"poll_interval_ms", "50"},
                      {"discovery_interval_ms", "500"},
                      {"max_records_per_poll", "40"}};
        g.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.type = "file_2pc_sink_string";
        snk.id = "snk";
        snk.uid = "kinesis-recovery-snk";
        snk.inputs = {"src"};
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = {{"dir", out_dir_.string()}};
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
        out_dir_ = cluster->checkpoint_dir().parent_path() / "kinesis-out";
        return cluster;
    }

    void submit(Cluster& cluster, std::uint32_t parallelism, const std::string& initial_position) {
        clink::http_connector::HttpRequest::Options o;
        o.base_url = "http://127.0.0.1:" + std::to_string(cluster.http_port());
        clink::http_connector::HttpRequest http(std::move(o));
        const auto r = http.post("/api/v1/jobs/spec?name=kinesis-recovery&checkpoint_dir=" +
                                     cluster.checkpoint_dir().string() +
                                     "&checkpoint_interval_ms=1000&alignment=aligned"
                                     "&max_restarts_on_worker_loss=8",
                                 graph(parallelism, initial_position).to_json(),
                                 "application/json");
        ASSERT_NE(r.body.find("\"ok\":true"), std::string::npos)
            << "the job was not accepted: " << r.body;
    }

    void add_spare_worker(Cluster& cluster) {
        ASSERT_TRUE(cluster.start_worker(1, worker_options()));
        ASSERT_TRUE(cluster.await_workers_registered(2));
        started_workers_ = 2;
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

    // Every source subtask has listed the stream once.
    void await_source_listed(Cluster& cluster, std::uint32_t parallelism) {
        ASSERT_TRUE(clink::itest::await(
            [&] { return count_in_workers(cluster, " listed shards (") >= parallelism; }, 90s))
            << "the source never listed the stream";
    }

    void await_first_checkpoint(Cluster& cluster) {
        ASSERT_TRUE(clink::itest::await([&] { return latest_completed(cluster) >= 1; }, 60s))
            << "no checkpoint completed";
    }

    // Every written record is committed and two more checkpoints completed
    // after, so nothing is still on its way.
    void await_all_committed(Cluster& cluster, std::size_t want) {
        ASSERT_TRUE(
            clink::itest::await([&] { return committed_lines(out_dir_).size() >= want; }, 240s))
            << "only " << committed_lines(out_dir_).size() << " of " << want
            << " records were committed";
        const auto after = latest_completed(cluster);
        ASSERT_TRUE(
            clink::itest::await([&] { return latest_completed(cluster) >= after + 2; }, 60s))
            << "checkpoints stopped completing";
    }

    void kill_and_replace(Cluster& cluster, std::size_t idx) {
        cluster.worker(idx).kill_hard();
        ASSERT_TRUE(cluster.worker(idx).await_exit(10s).has_value());
        archived_logs_.push_back(cluster.worker(idx).read_log());
        ASSERT_TRUE(cluster.restart_worker(idx, worker_options()));
    }

    static bool parked_at(Cluster& cluster, const std::string& point) {
        const std::string witness = "[fault.injection] fired: " + point;
        return clink::itest::await([&] { return cluster.worker(0).log_contains(witness); }, 120s);
    }

    void keep_logs(const char* cell, Cluster& cluster) const {
        const auto into = env_or_empty("CLINK_KINESIS_RECOVERY_LOG_DIR");
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

    void report(const char* cell,
                Cluster& cluster,
                const Verdict& v,
                std::size_t written,
                std::size_t written_at_kill,
                std::size_t committed_at_kill,
                std::size_t shards_listed) {
        keep_logs(cell, cluster);
        std::printf(
            "[kinesis-recovery] cell=%s written=%zu committed=%zu missing=%zu doubled=%zu "
            "unexpected=%zu out_of_order=%zu restarts=%zu written_at_kill=%zu "
            "committed_at_kill=%zu shards_listed=%zu shards_read_to_end=%zu "
            "children_released=%zu\n",
            cell,
            written,
            v.committed,
            v.missing,
            v.doubled,
            v.unexpected,
            v.out_of_order,
            restarts(cluster),
            written_at_kill,
            committed_at_kill,
            shards_listed,
            count_in_workers(cluster, " read to its end"),
            count_in_workers(cluster, " is eligible (parents "));
        std::fflush(stdout);
    }

    std::string endpoint_;
    std::unique_ptr<Stream> stream_;
    std::filesystem::path out_dir_;
    std::vector<std::string> archived_logs_;
    std::size_t started_workers_{1};
};

}  // namespace

// Parallelism 1: split while the job reads, kill with records in flight, merge
// after the restore. Every record once; every key in the order written.
TEST_F(KinesisRecovery, ParallelismOneKeepsEveryKeyInOrderThroughASplitAKillAndAMerge) {
    ASSERT_TRUE(stream_->create(2));
    auto cluster = make_cluster("kinesis.source_after_emit=block@40");
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, 1, "trim_horizon");
    await_source_listed(*cluster, 1);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    Writer writer(*stream_, "", 2400, 300);
    writer.start();
    ASSERT_TRUE(clink::itest::await([&] { return writer.written() >= 300; }, 60s));
    ASSERT_TRUE(stream_->split_in_half("shardId-000000000000")) << "the split failed";
    ASSERT_TRUE(parked_at(*cluster, "kinesis.source_after_emit"))
        << "worker 0 never reached kinesis.source_after_emit";
    const std::size_t written_at_kill = static_cast<std::size_t>(writer.written());
    const std::size_t committed_at_kill = committed_lines(out_dir_).size();
    EXPECT_LT(committed_at_kill, written_at_kill) << "nothing was in flight at the kill";
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));
    ASSERT_TRUE(clink::itest::await(
        [&] { return count_in_workers(*cluster, "recovery from a checkpoint") >= 1; }, 120s))
        << "the restored source did not recover from a checkpoint";
    std::vector<std::string> children;
    for (const auto& sh : stream_->shards()) {
        if (sh.parent == "shardId-000000000000") {
            children.push_back(sh.id);
        }
    }
    ASSERT_EQ(children.size(), 2U) << "the split did not happen";
    ASSERT_TRUE(stream_->merge(children[0], children[1])) << "the merge failed";
    writer.await_done();
    ASSERT_TRUE(writer.error().empty()) << writer.error();
    const auto written = writer.order();
    await_all_committed(*cluster, written.size());
    const auto listed = stream_->shards();
    const auto verdict = judge(committed_lines(out_dir_), written, writer.where(), true);
    report("p1_split_kill_merge",
           *cluster,
           verdict,
           written.size(),
           written_at_kill,
           committed_at_kill,
           listed.size());
    EXPECT_EQ(restarts(*cluster), 1U) << "one worker loss is one restart";
    const bool merged = std::any_of(listed.begin(), listed.end(), [&](const ListedShard& sh) {
        return sh.parent == children[0] && sh.adjacent_parent == children[1];
    });
    EXPECT_TRUE(merged) << "the merge child is not in the stream's listing";
    EXPECT_GE(count_in_workers(*cluster, "shard shardId-000000000000 read to its end"), 1U);
    EXPECT_GE(count_in_workers(*cluster, " is eligible (parents " + children[0] + ", "), 1U)
        << "the merge child was never released after both parents";
}

// Parallelism 2 through a uniform reshard and a kill: every record once, each
// shard in the order written.
TEST_F(KinesisRecovery, TwoSubtasksReadEveryRecordOnceThroughAReshardAndAKill) {
    ASSERT_TRUE(stream_->create(3));
    auto cluster = make_cluster("kinesis.source_after_emit=block@50");
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, 2, "trim_horizon");
    await_source_listed(*cluster, 2);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    Writer writer(*stream_, "", 2400, 300);
    writer.start();
    ASSERT_TRUE(clink::itest::await([&] { return writer.written() >= 300; }, 60s));
    ASSERT_TRUE(stream_->scale_to(6)) << "the reshard failed";
    ASSERT_TRUE(parked_at(*cluster, "kinesis.source_after_emit"))
        << "worker 0 never reached kinesis.source_after_emit";
    const std::size_t written_at_kill = static_cast<std::size_t>(writer.written());
    const std::size_t committed_at_kill = committed_lines(out_dir_).size();
    EXPECT_LT(committed_at_kill, written_at_kill) << "nothing was in flight at the kill";
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));
    writer.await_done();
    ASSERT_TRUE(writer.error().empty()) << writer.error();
    const auto written = writer.order();
    await_all_committed(*cluster, written.size());
    const auto listed = stream_->shards();
    const auto verdict = judge(committed_lines(out_dir_), written, writer.where(), false);
    report("p2_reshard_kill",
           *cluster,
           verdict,
           written.size(),
           written_at_kill,
           committed_at_kill,
           listed.size());
    EXPECT_EQ(restarts(*cluster), 1U) << "one worker loss is one restart";
    EXPECT_GE(listed.size(), 9U) << "the reshard did not happen";
    EXPECT_GE(count_in_workers(*cluster, "recovery from a checkpoint written at parallelism 2"), 2U)
        << "both restored subtasks should recover from the checkpoint";
}

// Latest mode, killed after a checkpoint and before the first record: the
// restored source keeps the first run's start, so what was written while the
// job was down is read, and nothing written before the job started.
TEST_F(KinesisRecovery, LatestModeKilledBeforeTheFirstRecordReadsWhatTheOutageWrote) {
    ASSERT_TRUE(stream_->create(2));
    Writer before(*stream_, "before-", 60, 600);
    before.start();
    before.await_done();
    ASSERT_TRUE(before.error().empty()) << before.error();
    // Margin for the emulator's clock against the workers': arrival times come
    // from the service, the start time from the worker.
    std::this_thread::sleep_for(1500ms);
    auto cluster = make_cluster("");
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, 1, "latest");
    await_source_listed(*cluster, 1);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    ASSERT_TRUE(committed_lines(out_dir_).empty()) << "latest read records written before it";
    std::this_thread::sleep_for(1500ms);
    kill_and_replace(*cluster, 0);
    Writer during(*stream_, "", 600, 300);
    during.start();
    ASSERT_TRUE(cluster->await_workers_registered(2));
    during.await_done();
    ASSERT_TRUE(during.error().empty()) << during.error();
    const auto written = during.order();
    await_all_committed(*cluster, written.size());
    const auto verdict = judge(committed_lines(out_dir_), written, during.where(), true);
    report("latest_kill_before_first_record",
           *cluster,
           verdict,
           written.size(),
           0,
           0,
           stream_->shards().size());
    EXPECT_EQ(restarts(*cluster), 1U) << "one worker loss is one restart";
    EXPECT_GE(count_in_workers(*cluster, "recovery from a checkpoint"), 1U)
        << "the restored source started afresh instead of recovering";
}
