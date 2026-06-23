// SQL GROUP BY end-to-end on the disaggregated remote-read:// state backend.
//
// This is the whole-stack proof: a SQL aggregation job, with the async-state
// execution path enabled, runs in a real in-process cluster (JM + TM) whose
// per-subtask state backend is built by the StateBackendFactory from a
// remote-read://<bucket>/<prefix> URI - so keyed aggregate state lives in S3
// (content-addressed objects + per-checkpoint manifests via S3RemotePool),
// cold reads defer to S3 through the AsyncExecutionController, and a fresh job
// restores that state lazily. It ties together every layer that was built in
// isolation: the state_backend_uri wire plumbing (decoupled from the JM's
// local coordination checkpoint_dir), the factory-built RemoteReadBackend on
// the deploy path, the async aggregate operator on the runner, and lazy
// restore from S3.
//
// Three phases:
//   1. Run a GROUP BY SUM over a large single-key batch to completion. On
//      bounded-source exit the runtime emits an end-of-stream checkpoint
//      barrier (CheckpointId == UINT64_MAX, see dag.hpp), so the operator's
//      final aggregate state is committed to S3 in full. Assert the output is
//      correct.
//   2. A FRESH cluster + job (same SQL, same S3 prefix) restores from that
//      checkpoint and folds in a tiny second batch. Its hot tier starts empty,
//      so the only way the recovered total can appear is a cold read from S3 -
//      this is the load-bearing restore proof.
//   3. A control job on a FRESH (empty) prefix with no restore consumes the
//      same second batch; its total reflects only that batch. restored ==
//      batch1 + batch2 while control == batch2 is the exact, deterministic
//      signal that state round-tripped through S3.
//
// Gated on CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET (MinIO / LocalStack);
// skipped otherwise.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/application/job_submitter.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/job_manager.hpp"
#include "clink/cluster/task_manager.hpp"
#include "clink/config/json.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/s3/install.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/optimizer.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/physical_plan.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace clink::sql {
namespace {

// The end-of-stream checkpoint id the runtime stamps on the terminal barrier
// a bounded source emits when it exits (dag.hpp). It captures the operator's
// complete final state - exactly what we restore from.
constexpr std::uint64_t kEndOfStreamCheckpoint = std::numeric_limits<std::uint64_t>::max();

bool s3_available() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT") != nullptr &&
           std::getenv("CLINK_S3_TEST_BUCKET") != nullptr;
}

void write_lines(const std::filesystem::path& path, const std::vector<std::string>& lines) {
    std::ofstream out(path);
    for (const auto& l : lines)
        out << l << "\n";
}

std::vector<std::string> read_lines(const std::filesystem::path& path) {
    std::ifstream in(path);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty())
            lines.push_back(line);
    }
    return lines;
}

// Install SQL ops + the remote-read:// scheme once on the process-wide
// registries (re-installing SQL ops throws). Shared across tests.
void ensure_installed_once() {
    static bool done = [] {
        cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::sql::install(reg);
        clink::s3::install_state_backend();  // remote-read:// scheme
        return true;
    }();
    (void)done;
}

// Compile an INSERT INTO <out> SELECT user_id, SUM(amount) ... GROUP BY user_id
// over a file-connector source/sink, with the async-state aggregate path on.
cluster::JobGraphSpec compile_groupby(const std::filesystem::path& in_path,
                                      const std::filesystem::path& out_path) {
    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    Binder b(cat);
    auto plan = b.bind_insert(
        std::get<ast::InsertStmt>(parse("INSERT INTO out_t SELECT user_id, SUM(amount) AS total "
                                        "FROM orders GROUP BY user_id")
                                      .statements[0]));
    plan = optimize(std::move(plan));
    PhysicalPlanner pp;
    pp.set_async_state_for_aggregation(true);  // route GROUP BY through async state
    return pp.compile(static_cast<const LogicalSink&>(*plan));
}

struct InProcessCluster {
    cluster::JobManager jm;
    std::uint16_t jm_port{};
    std::unique_ptr<cluster::TaskManager> tm;

    InProcessCluster(const std::string& tm_id, std::size_t slots) {
        jm_port = jm.start();
        jm.expect_tms({tm_id});
        cluster::TaskManager::Config cfg;
        cfg.slot_count = slots;
        tm = std::make_unique<cluster::TaskManager>(tm_id, "127.0.0.1", cfg);
        tm->connect_to_jm("127.0.0.1", jm_port);
        std::this_thread::sleep_for(150ms);
    }
    ~InProcessCluster() {
        if (tm)
            tm->stop();
        jm.stop();
    }
};

// Last emitted `total` for the given user_id across the NDJSON sink output
// (the unbounded GROUP BY emits a running total on every input; the last is
// the final aggregate). Returns -1 if the key never appears.
std::int64_t last_total_for_key(const std::filesystem::path& out_path, std::int64_t user_id) {
    std::int64_t total = -1;
    for (const auto& line : read_lines(out_path)) {
        auto js = clink::config::parse(line);
        if (!js.is_object())
            continue;
        if (static_cast<std::int64_t>(js.at("user_id").as_number()) == user_id)
            total = static_cast<std::int64_t>(js.at("total").as_number());
    }
    return total;
}

std::string remote_read_uri(const std::string& prefix) {
    const std::string bucket = std::getenv("CLINK_S3_TEST_BUCKET");
    const std::string endpoint = std::getenv("CLINK_S3_TEST_ENDPOINT");
    return "remote-read://" + bucket + "/" + prefix + "?endpoint=" + endpoint + "&region=us-east-1";
}

// Same, but pins a tiny hot-tier budget so the working set cannot fit in RAM:
// committed keys are evicted LRU-first and cold-refetched from S3 on next read.
std::string remote_read_uri_evict(const std::string& prefix, std::size_t hot_max_bytes) {
    return remote_read_uri(prefix) + "&hot_max_bytes=" + std::to_string(hot_max_bytes);
}

// Bump the keyed aggregate operator to `p` subtasks so the GROUP BY runs sharded
// across the cluster (the upstream key-by edge fans out to the p keyed subtasks
// by key group). The file source/sink stay at 1. Returns the modified spec.
cluster::JobGraphSpec with_parallelism(cluster::JobGraphSpec spec, std::uint32_t p) {
    for (auto& op : spec.ops) {
        if (op.type == "aggregate_row") {
            op.parallelism = p;  // static (min/max stay 0 = no autoscaling)
        }
    }
    return spec;
}

// Run one GROUP BY job to completion against the cluster, optionally restoring
// from a prior checkpoint on the given S3 prefix. Returns the final total for
// user_id=1 from the sink output.
std::int64_t run_groupby_job(const std::string& tm_id,
                             const std::filesystem::path& in_path,
                             const std::filesystem::path& out_path,
                             const std::string& state_prefix,
                             bool restore,
                             std::uint64_t restore_ckpt) {
    InProcessCluster cluster(tm_id, 4);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 60s;
    // checkpoint_dir stays empty: state lives entirely in S3 via the
    // decoupled state_backend_uri; the end-of-stream barrier still commits it.
    opts.checkpoint.state_backend_uri = remote_read_uri(state_prefix);
    if (restore) {
        opts.checkpoint.restore_from_dir = remote_read_uri(state_prefix);  // presence flag
        opts.checkpoint.restore_from_checkpoint_id = restore_ckpt;
    }
    auto result = submitter.submit(compile_groupby(in_path, out_path).to_json(), {}, opts);
    EXPECT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    return last_total_for_key(out_path, 1);
}

// The final (last-emitted) total for EVERY key in the sink output.
std::map<std::int64_t, std::int64_t> all_totals(const std::filesystem::path& out_path) {
    std::map<std::int64_t, std::int64_t> totals;
    for (const auto& line : read_lines(out_path)) {
        auto js = clink::config::parse(line);
        if (!js.is_object())
            continue;
        totals[static_cast<std::int64_t>(js.at("user_id").as_number())] =
            static_cast<std::int64_t>(js.at("total").as_number());
    }
    return totals;
}

// Run a GROUP BY job with an explicit (tiny) hot-tier budget + a sharded
// aggregate parallelism, optionally restoring from a prior checkpoint. Returns
// the per-key final totals.
std::map<std::int64_t, std::int64_t> run_groupby_evict(const std::string& tm_id,
                                                       const std::filesystem::path& in_path,
                                                       const std::filesystem::path& out_path,
                                                       const std::string& state_prefix,
                                                       std::size_t hot_max_bytes,
                                                       std::uint32_t parallelism,
                                                       bool restore,
                                                       std::uint64_t restore_ckpt) {
    InProcessCluster cluster(tm_id, 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 90s;
    opts.checkpoint.state_backend_uri = remote_read_uri_evict(state_prefix, hot_max_bytes);
    if (restore) {
        opts.checkpoint.restore_from_dir = remote_read_uri_evict(state_prefix, hot_max_bytes);
        opts.checkpoint.restore_from_checkpoint_id = restore_ckpt;
    }
    auto spec = with_parallelism(compile_groupby(in_path, out_path), parallelism);
    auto result = submitter.submit(spec.to_json(), {}, opts);
    EXPECT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    return all_totals(out_path);
}

}  // namespace

// Cluster-scale disaggregation proof: a many-key GROUP BY runs SHARDED across
// subtasks over S3, with a hot-tier budget far below the working set so keyed
// state genuinely spills to S3 (committed keys evicted LRU-first, cold-refetched
// on next read), and a fresh job restores all of it lazily. Correct per-key
// totals through a tiny hot tier == state-exceeds-RAM works end-to-end at
// parallelism over real object storage.
TEST(SqlRemoteReadE2E, MultiKeyEvictionAndRestoreShardedOverS3) {
    if (!s3_available()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET (MinIO/LocalStack)";
    }
    ensure_installed_once();

    const auto pid = std::to_string(::getpid());
    const auto tmp = std::filesystem::temp_directory_path();
    const auto in1 = tmp / ("clink-rr-mk-in1-" + pid + ".ndjson");
    const auto out1 = tmp / ("clink-rr-mk-out1-" + pid + ".ndjson");
    const auto in2 = tmp / ("clink-rr-mk-in2-" + pid + ".ndjson");
    const auto out2 = tmp / ("clink-rr-mk-out2-" + pid + ".ndjson");
    for (const auto& p : {in1, out1, in2, out2})
        std::filesystem::remove(p);

    constexpr std::int64_t kKeys = 20;
    constexpr std::int64_t kB1PerKey = 10;
    constexpr std::int64_t kB2PerKey = 3;
    {
        std::vector<std::string> r1;
        std::vector<std::string> r2;
        for (std::int64_t k = 1; k <= kKeys; ++k) {
            const std::string row = R"({"user_id":)" + std::to_string(k) + R"(,"amount":1})";
            for (int i = 0; i < kB1PerKey; ++i)
                r1.push_back(row);
            for (int i = 0; i < kB2PerKey; ++i)
                r2.push_back(row);
        }
        write_lines(in1, r1);
        write_lines(in2, r2);
    }

    const std::string prefix = "clink-sql-rr-mk/" + pid;
    constexpr std::size_t kHot = 32;   // << the 20-key working set: forces eviction
    constexpr std::uint32_t kPar = 4;  // sharded GROUP BY across 4 keyed subtasks

    // Phase 1: build 20-key aggregate state, sharded, evicting under the budget.
    auto built = run_groupby_evict("tm-rr-mk-build",
                                   in1,
                                   out1,
                                   prefix,
                                   kHot,
                                   kPar,
                                   /*restore=*/false,
                                   0);
    ASSERT_EQ(built.size(), static_cast<std::size_t>(kKeys));
    for (std::int64_t k = 1; k <= kKeys; ++k)
        EXPECT_EQ(built[k], kB1PerKey) << "build key " << k;

    // Phase 2: fresh cluster restores all 20 keys lazily from S3 (hot tier empty)
    // and folds batch 2 touching every key. Under the tiny budget each key is
    // cold-read from S3, filled, then evicted as others arrive (heavy churn).
    auto restored = run_groupby_evict("tm-rr-mk-restore",
                                      in2,
                                      out2,
                                      prefix,
                                      kHot,
                                      kPar,
                                      /*restore=*/true,
                                      kEndOfStreamCheckpoint);
    ASSERT_EQ(restored.size(), static_cast<std::size_t>(kKeys));
    for (std::int64_t k = 1; k <= kKeys; ++k)
        EXPECT_EQ(restored[k], kB1PerKey + kB2PerKey) << "restored key " << k;

    for (const auto& p : {in1, out1, in2, out2})
        std::filesystem::remove(p);
}

TEST(SqlRemoteReadE2E, GroupByAsyncCheckpointsToS3AndRestores) {
    if (!s3_available()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET (MinIO/LocalStack)";
    }
    ensure_installed_once();

    const auto pid = std::to_string(::getpid());
    const auto tmp = std::filesystem::temp_directory_path();
    const auto in1 = tmp / ("clink-rr-e2e-in1-" + pid + ".ndjson");
    const auto out1 = tmp / ("clink-rr-e2e-out1-" + pid + ".ndjson");
    const auto in2 = tmp / ("clink-rr-e2e-in2-" + pid + ".ndjson");
    const auto out2 = tmp / ("clink-rr-e2e-out2-" + pid + ".ndjson");
    const auto outc = tmp / ("clink-rr-e2e-outc-" + pid + ".ndjson");
    for (const auto& p : {in1, out1, in2, out2, outc})
        std::filesystem::remove(p);

    // Batch 1: a large single-key stream whose full aggregate is committed to
    // S3 at end-of-stream. Batch 2: a tiny stream on the same key.
    constexpr std::int64_t kBatch1Rows = 5000;
    constexpr std::int64_t kBatch2Rows = 3;
    {
        std::vector<std::string> rows(static_cast<std::size_t>(kBatch1Rows),
                                      R"({"user_id":1,"amount":1})");
        write_lines(in1, rows);
        std::vector<std::string> rows2(static_cast<std::size_t>(kBatch2Rows),
                                       R"({"user_id":1,"amount":1})");
        write_lines(in2, rows2);
    }

    const std::string prefix_main = "clink-sql-rr-e2e/" + pid + "/main";
    const std::string prefix_ctrl = "clink-sql-rr-e2e/" + pid + "/ctrl";

    // ---- Phase 1: build aggregate state and commit it to S3. ----
    const std::int64_t built =
        run_groupby_job("tm-rr-e2e-build", in1, out1, prefix_main, /*restore=*/false, 0);
    EXPECT_EQ(built, kBatch1Rows);  // GROUP BY SUM over batch 1

    // ---- Phase 2: fresh cluster restores that state from S3, folds batch 2. ----
    const std::int64_t restored = run_groupby_job("tm-rr-e2e-restore",
                                                  in2,
                                                  out2,
                                                  prefix_main,
                                                  /*restore=*/true,
                                                  kEndOfStreamCheckpoint);

    // ---- Phase 3: control - fresh empty prefix, no restore. ----
    const std::int64_t control =
        run_groupby_job("tm-rr-e2e-control", in2, outc, prefix_ctrl, /*restore=*/false, 0);

    // Control had no prior state, so it reflects only batch 2.
    EXPECT_EQ(control, kBatch2Rows);
    // The restored job's hot tier started empty; the only source of the batch-1
    // total is a cold read of the checkpoint from S3. So it must equal batch1 +
    // batch2 exactly, and dwarf the control.
    EXPECT_EQ(restored, kBatch1Rows + kBatch2Rows);
    EXPECT_GT(restored, control);

    for (const auto& p : {in1, out1, in2, out2, outc})
        std::filesystem::remove(p);
}

}  // namespace clink::sql
