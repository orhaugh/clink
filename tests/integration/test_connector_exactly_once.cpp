// Multi-connector exactly-once under failure: the Postgres and Kafka arms.
//
// The file 2PC sink's exactly-once claim is pinned in test_fault_recovery.cpp;
// nothing held a REAL connector to the same standard under failure. These
// scenarios run the same bounded source against postgres_2pc_sink (PREPARE
// TRANSACTION / COMMIT PREPARED riding CommittingSink), SIGKILL workers at the
// same moments, and compare the table's contents to the exact multiset the
// source promises. Postgres also offers a witness the file sink cannot:
// pg_prepared_xacts. A prepared-but-unresolved transaction visible there
// after the job completed is a leak the row counts would never show - it
// holds locks and blocks vacuum forever - so every scenario asserts the
// prepared set drains to empty as well.
//
// The job plugin CARRIES the connector (clink_node links no impls), so this
// also exercises the bundle-registry path for a connector sink factory:
// examples/postgres_2pc_job.cpp links clink::postgres and installs its
// factories at define_job time.
//
// Server: a throwaway Postgres container per suite (DockerPostgres), started
// with max_prepared_transactions=64 since 2PC is refused at zero. Skips when
// Docker is unreachable, like the CDC suites.

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "tests/integration/cluster_harness.hpp"
#include "tests/integration/docker_kafka.hpp"
#include "tests/integration/docker_localstack.hpp"
#include "tests/integration/docker_postgres.hpp"
#include "tests/integration/two_pc_output.hpp"

#ifdef CLINK_HAS_KAFKA
#include "clink/kafka/consume_all.hpp"
#endif
#ifdef CLINK_S3_2PC_JOB_PATH
#include "clink/connectors/arrow_s3_lifecycle.hpp"
#include "clink/s3/read_all.hpp"
#endif

namespace {

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::Process;
using clink::itest::ScopedDiagnostics;

std::filesystem::path node_binary() {
#ifdef CLINK_NODE_BINARY
    return std::filesystem::path{CLINK_NODE_BINARY};
#else
    return {};
#endif
}
std::filesystem::path submit_binary() {
#ifdef CLINK_SUBMIT_BINARY
    return std::filesystem::path{CLINK_SUBMIT_BINARY};
#else
    return {};
#endif
}
std::filesystem::path postgres_2pc_job() {
#ifdef CLINK_POSTGRES_2PC_JOB_PATH
    return std::filesystem::path{CLINK_POSTGRES_2PC_JOB_PATH};
#else
    return {};
#endif
}
std::filesystem::path kafka_2pc_job() {
#ifdef CLINK_KAFKA_2PC_JOB_PATH
    return std::filesystem::path{CLINK_KAFKA_2PC_JOB_PATH};
#else
    return {};
#endif
}
std::filesystem::path s3_2pc_job() {
#ifdef CLINK_S3_2PC_JOB_PATH
    return std::filesystem::path{CLINK_S3_2PC_JOB_PATH};
#else
    return {};
#endif
}

ClusterSpec two_worker_spec() {
    ClusterSpec s;
    s.node_binary = node_binary();
    s.workers = 2;
    s.slots_per_worker = 4;
    return s;
}

std::unique_ptr<Process> submit_job(Cluster& c,
                                    const std::filesystem::path& job_so,
                                    int max_restarts) {
    auto p = std::make_unique<Process>();
    std::vector<std::string> argv{submit_binary().string(),
                                  "--job=" + job_so.string(),
                                  "--coordinator-host=127.0.0.1",
                                  "--coordinator-port=" + std::to_string(c.coordinator_port()),
                                  "--wait-timeout-s=90",
                                  "--checkpoint-dir=" + c.checkpoint_dir().string(),
                                  "--checkpoint-interval-ms=150",
                                  "--max-restarts-on-worker-loss=" + std::to_string(max_restarts)};
    const bool ok = p->spawn("submit", submit_binary(), std::move(argv), c.log_dir());
    return ok ? std::move(p) : nullptr;
}

// The highest COMPLETED-N under the checkpoint tree, or 0. Same scan as the
// fault-recovery suite's.
std::uint64_t latest_completed(const std::filesystem::path& ckpt_root) {
    std::uint64_t latest = 0;
    std::error_code ec;
    for (const auto& e : std::filesystem::recursive_directory_iterator(ckpt_root, ec)) {
        if (ec) {
            break;
        }
        if (!e.is_regular_file()) {
            continue;
        }
        const auto name = e.path().filename().string();
        if (name.rfind("COMPLETED-", 0) != 0) {
            continue;
        }
        try {
            const auto id = static_cast<std::uint64_t>(std::stoull(name.substr(10)));
            latest = std::max(latest, id);
        } catch (const std::exception&) {
        }
    }
    return latest;
}

class PostgresExactlyOnceTest : public ::testing::Test {
protected:
    // One container for the whole suite (startup is seconds); one TABLE per
    // test, so scenarios stay isolated without paying the container again.
    static void SetUpTestSuite() {
        if (!clink::test::DockerPostgres::docker_available()) {
            return;  // per-test SetUp skips
        }
        clink::test::DockerPostgresOptions opts;
        // 2PC is refused outright at the default max_prepared_transactions=0.
        opts.postgres_args = {"-c", "max_prepared_transactions=64"};
        pg_ = new clink::test::DockerPostgres(opts);
    }

    static void TearDownTestSuite() {
        delete pg_;
        pg_ = nullptr;
    }

    void SetUp() override {
        if (!clink::test::DockerPostgres::docker_available()) {
            GTEST_SKIP() << "Docker not available; skipping Postgres exactly-once suite";
        }
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(submit_binary()) ||
            !std::filesystem::exists(postgres_2pc_job())) {
            GTEST_SKIP() << "cluster binaries or the postgres 2PC job plugin are not built";
        }
        ASSERT_NE(pg_, nullptr);
        // Lowercased: the sink quotes the identifier, so a mixed-case name
        // here would name a DIFFERENT relation than the unquoted CREATE.
        table_ = std::string{"clink_xo_"} +
                 ::testing::UnitTest::GetInstance()->current_test_info()->name();
        std::transform(table_.begin(), table_.end(), table_.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        pg_->exec("DROP TABLE IF EXISTS " + table_);
        pg_->exec("CREATE TABLE " + table_ + " (v text)");
        ::setenv("CLINK_PG_DSN", pg_->conninfo().c_str(), 1);
        ::setenv("CLINK_PG_TABLE", table_.c_str(), 1);
    }

    static ClusterSpec spec() { return two_worker_spec(); }

    static void bring_up(Cluster& c) {
        ASSERT_TRUE(c.start_coordinator()) << "coordinator did not come up";
        ASSERT_TRUE(c.start_worker(0));
        ASSERT_TRUE(c.start_worker(1));
        ASSERT_TRUE(c.await_workers_registered(2));
    }

    static std::unique_ptr<Process> submit(Cluster& c, int max_restarts) {
        return submit_job(c, postgres_2pc_job(), max_restarts);
    }

    std::vector<std::string> rows() const { return pg_->column("SELECT v FROM " + table_); }
    long committed_count() const {
        return std::stol(pg_->scalar("SELECT count(*) FROM " + table_));
    }
    long prepared_count() const {
        return std::stol(pg_->scalar("SELECT count(*) FROM pg_prepared_xacts"));
    }

    static clink::test::DockerPostgres* pg_;
    std::string table_;
};

clink::test::DockerPostgres* PostgresExactlyOnceTest::pg_ = nullptr;

// The premise, before any fault: a clean run commits each record exactly once
// and leaves no prepared transaction behind. Without this, a failure below
// could be the connector, the plugin path, or the verifier - no way to tell.
TEST_F(PostgresExactlyOnceTest, ACleanRunCommitsEveryRecordExactlyOnce) {
    constexpr int kTotal = 40;
    ::setenv("CLINK_2PC_TOTAL", "40", 1);
    ::setenv("CLINK_2PC_TICK_MS", "50", 1);

    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);
    auto sub = submit(c, /*max_restarts=*/0);
    ASSERT_NE(sub, nullptr);
    const auto code = sub->await_exit(90s);
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0);

    const auto v = clink::itest::verify_exactly_once_records(rows(), kTotal);
    EXPECT_TRUE(v.clean()) << "a CLEAN Postgres 2PC run did not commit each record exactly "
                              "once: "
                           << clink::itest::describe(v);
    EXPECT_EQ(prepared_count(), 0) << "the job completed but left a prepared transaction behind";
}

TEST_F(PostgresExactlyOnceTest, EveryRecordIsCommittedExactlyOnceAcrossAWorkerKill) {
    constexpr int kTotal = 40;
    ::setenv("CLINK_2PC_TOTAL", "40", 1);
    ::setenv("CLINK_2PC_TICK_MS", "50", 1);

    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);
    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);

    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; }, 45s));
    // Vacuity guard: the run must still be mid-flight, or the "recovery"
    // below recovers nothing.
    ASSERT_LT(committed_count(), kTotal)
        << "the job already finished before the kill; the recovery path never ran";

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    const auto code = sub->await_exit(120s);
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0) << "the job did not recover from the worker kill";

    const auto v = clink::itest::verify_exactly_once_records(rows(), kTotal);
    EXPECT_TRUE(v.clean()) << "a worker kill broke Postgres exactly-once: "
                           << clink::itest::describe(v);
    EXPECT_EQ(prepared_count(), 0)
        << "recovery completed but left a prepared transaction behind (locks and vacuum "
           "are blocked until someone resolves it by hand)";
}

TEST_F(PostgresExactlyOnceTest, AWorkerKilledInsideTheCommitWindowStaysExactlyOnce) {
    constexpr int kTotal = 40;
    ::setenv("CLINK_2PC_TOTAL", "40", 1);
    ::setenv("CLINK_2PC_TICK_MS", "50", 1);

    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    // Hold the first commit open on both workers, whichever hosts the sink.
    // The fault registry seeds from CLINK_FAULT_INJECT in every module,
    // including the job plugin's own copy of CommittingSink, so the delay
    // fires inside the connector sink carried by the .so.
    ASSERT_TRUE(c.start_worker(0, {.fault = "sink.before_commit=delay:4000@1"}));
    ASSERT_TRUE(c.start_worker(1, {.fault = "sink.before_commit=delay:4000@1"}));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);

    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; }, 45s));
    // The witness the file sink cannot offer: the transaction is PREPARED
    // (durable on the server) while its commit is held by the armed delay.
    // This is the vacuity guard - the kill below provably lands inside a
    // real commit window, not before or after one.
    ASSERT_TRUE(clink::itest::await([&] { return prepared_count() > 0; }, 30s))
        << "no prepared transaction appeared; the commit window never opened";
    ASSERT_EQ(committed_count(), 0)
        << "rows are visible while the commit should still be held; the fault did not arm "
           "inside the plugin";

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    const auto code = sub->await_exit(120s);
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0) << "the job did not recover from a kill inside the commit window";

    const auto v = clink::itest::verify_exactly_once_records(rows(), kTotal);
    EXPECT_TRUE(v.clean()) << "a kill inside the commit window broke Postgres exactly-once: "
                           << clink::itest::describe(v);
    EXPECT_EQ(prepared_count(), 0)
        << "the run completed but a prepared transaction is still pending; the recovery "
           "reconcile did not resolve every gid";
}

// The longer-horizon arm: thirty times the records of the file suite, two
// separated worker losses, and the same exact verdict. Long enough for many
// checkpoint cycles (and their prepared-transaction churn) to expose drift a
// two-second run cannot.
TEST_F(PostgresExactlyOnceTest, StaysExactlyOnceAcrossTwoSeparatedKillsOnALongerRun) {
    constexpr int kTotal = 1200;
    ::setenv("CLINK_2PC_TOTAL", "1200", 1);
    ::setenv("CLINK_2PC_TICK_MS", "5", 1);

    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);
    auto sub = submit(c, /*max_restarts=*/3);
    ASSERT_NE(sub, nullptr);

    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; }, 45s));
    ASSERT_LT(committed_count(), kTotal) << "finished before the first kill; nothing recovered";

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    // Settle on a CONDITION: the restarted job has committed further rows,
    // so the first recovery is complete before the second loss arrives.
    const auto before_second = committed_count();
    ASSERT_TRUE(clink::itest::await([&] { return committed_count() > before_second; }, 60s))
        << "no commit progress after the first kill; the restart never drained";
    ASSERT_TRUE(c.restart_worker(0));

    c.worker(1).kill_hard();
    ASSERT_TRUE(c.await_process_gone(1));

    const auto code = sub->await_exit(180s);
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0) << "the job did not survive two separated worker losses";

    const auto v = clink::itest::verify_exactly_once_records(rows(), kTotal);
    EXPECT_TRUE(v.clean()) << "two separated kills on a longer run broke Postgres "
                              "exactly-once: "
                           << clink::itest::describe(v);
    EXPECT_EQ(prepared_count(), 0)
        << "the longer run completed with a prepared transaction still pending";
}

#ifdef CLINK_HAS_KAFKA

// The verdict for the transactional Kafka sink under PROCESS LOSS, on the
// commit-confirmed restore protocol: restores select the newest checkpoint
// whose broker commits provably EXECUTED (CONFIRMED-N), so a commit that
// died with the worker is REPLAYED rather than skipped - missing records
// are never acceptable any more. The residual window moved to the other
// side: a worker that dies after commit_transaction returned but before
// its confirmation was sent replays an interval whose transaction DID
// commit, so duplicates bounded by one contiguous checkpoint interval per
// kill remain the documented cost (librdkafka has no transaction resume;
// closing that too is not possible from this side of the client).
void expect_exactly_once_modulo_commit_window(const std::vector<std::string>& records,
                                              int total,
                                              std::size_t max_dup_runs,
                                              std::size_t max_run_len) {
    const auto v = clink::itest::verify_exactly_once_records(records, total);
    EXPECT_TRUE(v.missing.empty())
        << "records are MISSING despite the commit-confirmed restore protocol - the restore "
           "selected past an unexecuted commit: "
        << clink::itest::describe(v);
    EXPECT_TRUE(v.unexpected.empty())
        << "unexpected records are never acceptable: " << clink::itest::describe(v);
    if (v.duplicated.empty()) {
        return;
    }
    // Group the duplicated records ("record-N xK") into contiguous runs.
    std::vector<long> ids;
    ids.reserve(v.duplicated.size());
    for (const auto& d : v.duplicated) {
        ids.push_back(std::stol(d.substr(std::string{"record-"}.size())));
    }
    std::sort(ids.begin(), ids.end());
    std::size_t runs = 1;
    std::size_t run_len = 1;
    std::size_t longest = 1;
    for (std::size_t i = 1; i < ids.size(); ++i) {
        if (ids[i] == ids[i - 1] + 1) {
            ++run_len;
        } else {
            ++runs;
            run_len = 1;
        }
        longest = std::max(longest, run_len);
    }
    EXPECT_LE(runs, max_dup_runs) << "duplicated records form " << runs
                                  << " contiguous runs; at most " << max_dup_runs
                                  << " (one per kill) are attributable to the documented "
                                     "commit-executed-but-unconfirmed window: "
                                  << clink::itest::describe(v);
    EXPECT_LE(longest, max_run_len)
        << "a duplicated run spans " << longest << " records, more than one checkpoint "
        << "interval's worth (" << max_run_len << " with slack): " << clink::itest::describe(v);
    std::cout << "[kafka-2pc] documented commit-window duplication observed: "
              << v.duplicated.size() << " record(s) in " << runs << " run(s)\n";
}

// The Kafka arm. Same bounded source, same moments, same verdict - but the
// sink is a broker transaction (kafka_2pc_sink_string) and the observer is a
// read_committed consumer draining the topic to EOF, i.e. exactly what a
// correctly configured downstream application sees. Records inside open or
// aborted transactions are invisible to it; an ABANDONED transaction caps the
// last stable offset, so it surfaces as missing tail records.
class KafkaExactlyOnceTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        if (!clink::test::DockerKafka::docker_available()) {
            return;  // per-test SetUp skips
        }
        kafka_ = new clink::test::DockerKafka();
    }

    static void TearDownTestSuite() {
        delete kafka_;
        kafka_ = nullptr;
    }

    void SetUp() override {
        if (!clink::test::DockerKafka::docker_available()) {
            GTEST_SKIP() << "Docker not available; skipping Kafka exactly-once suite";
        }
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(submit_binary()) ||
            !std::filesystem::exists(kafka_2pc_job())) {
            GTEST_SKIP() << "cluster binaries or the kafka 2PC job plugin are not built";
        }
        ASSERT_NE(kafka_, nullptr);
        topic_ = std::string{"clink_xo_"} +
                 ::testing::UnitTest::GetInstance()->current_test_info()->name();
        kafka_->create_topic(topic_);
        ::setenv("CLINK_KAFKA_BROKERS", kafka_->brokers().c_str(), 1);
        ::setenv("CLINK_KAFKA_TOPIC", topic_.c_str(), 1);
    }

    std::vector<std::string> committed() const {
        return clink::kafka::consume_all_committed(kafka_->brokers(), topic_);
    }

    static std::unique_ptr<Process> submit(Cluster& c, int max_restarts) {
        return submit_job(c, kafka_2pc_job(), max_restarts);
    }

    static clink::test::DockerKafka* kafka_;
    std::string topic_;
};

clink::test::DockerKafka* KafkaExactlyOnceTest::kafka_ = nullptr;

// The premise: a clean run's committed output is the exact multiset, through
// the connector-carrying plugin path and a real broker.
TEST_F(KafkaExactlyOnceTest, ACleanRunCommitsEveryRecordExactlyOnce) {
    constexpr int kTotal = 40;
    ::setenv("CLINK_2PC_TOTAL", "40", 1);
    ::setenv("CLINK_2PC_TICK_MS", "50", 1);

    Cluster c(two_worker_spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));
    auto sub = submit(c, /*max_restarts=*/0);
    ASSERT_NE(sub, nullptr);
    const auto code = sub->await_exit(90s);
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0);

    const auto v = clink::itest::verify_exactly_once_records(committed(), kTotal);
    EXPECT_TRUE(v.clean()) << "a CLEAN Kafka 2PC run did not commit each record exactly once: "
                           << clink::itest::describe(v);
}

// RE-ENABLED with the item-51 fix: the two failure modes this test kept
// hitting were both downstream of the source runner acking checkpoints
// after an in-memory put with no persist (COMPLETED markers publishing
// while the source's snapshot was still a .part temp file - the artifacts
// that cracked the case are described in test_checkpoint_ack_durability.cpp,
// which pins the invariant at the runner contract). The source subtask's
// ack now comes from its terminal runner, strictly after persist().
TEST_F(KafkaExactlyOnceTest, EveryRecordIsCommittedExactlyOnceAcrossAWorkerKill) {
    constexpr int kTotal = 40;
    ::setenv("CLINK_2PC_TOTAL", "40", 1);
    ::setenv("CLINK_2PC_TICK_MS", "50", 1);

    Cluster c(two_worker_spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));
    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);

    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; }, 45s));
    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    const auto code = sub->await_exit(120s);
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0) << "the job did not recover from the worker kill";
    // Vacuity guard, post hoc rather than via a mid-run topic drain (a
    // read_committed drain chases a live topic's moving EOF, so its answer
    // dates from whenever it caught the head, not from the kill): a kill
    // that landed after completion restarts nothing.
    ASSERT_GT(c.count_in_coordinator_log("restart"), 0)
        << "the job finished before the kill; the recovery path never ran";

    // One kill: at most one interval lost through the documented window; the
    // isolation split stays in the diagnostics via the helper's describe().
    expect_exactly_once_modulo_commit_window(committed(),
                                             kTotal,
                                             /*max_lost_runs=*/1,
                                             /*max_run_len=*/6);  // 150ms / 50ms tick, 2x slack
}

// The moment that decides the claim for a broker transaction: the worker dies
// IMMEDIATELY after commit_transaction returns, and the commit was held open
// long enough beforehand that the source demonstrably produced further
// records while the transaction was still uncommitted. Exactly-once then
// requires that those post-barrier records were NOT part of the committed
// transaction - because the checkpoint the job restores from sits at the
// barrier, and everything after it is replayed and produced again.
TEST_F(KafkaExactlyOnceTest, AWorkerKilledRightAfterTheBrokerCommitStaysExactlyOnce) {
    constexpr int kTotal = 40;
    ::setenv("CLINK_2PC_TOTAL", "40", 1);
    ::setenv("CLINK_2PC_TICK_MS", "50", 1);

    // ONE worker, so the sink's placement - and therefore the armed fault's
    // target - is deterministic. The worker dies by the fault; the test then
    // respawns it WITHOUT the fault (the supervisor's move), and the redeploy
    // restores from the checkpoint whose commit just ran.
    ClusterSpec one;
    one.node_binary = node_binary();
    one.workers = 1;
    one.slots_per_worker = 4;
    Cluster c(one);
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    // Hold the first commit 800ms (sixteen ticks of records arrive while the
    // transaction is prepared-but-uncommitted), then die the instant the
    // broker commit returns. The fault registry seeds from the environment
    // in every module, the job plugin included.
    ASSERT_TRUE(c.start_worker(
        0, {.fault = "sink.before_commit=delay:800@1,sink.after_external_commit=exit:1@1"}));
    ASSERT_TRUE(c.await_workers_registered(1));

    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);

    // The armed exit kills the worker right after the first broker commit.
    ASSERT_TRUE(c.await_process_gone(0))
        << "the kill-after-commit fault never fired; the scenario under test never happened";
    ASSERT_TRUE(c.restart_worker(0));  // fresh process, no fault armed

    const auto code = sub->await_exit(120s);
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0) << "the job did not recover from a kill right after the broker commit";

    // Under the commit-confirmed restore protocol this kill lands in the
    // documented residual window: the commit EXECUTED, the confirmation
    // died with the process, so the restore replays the interval as
    // duplicates. Nothing may be missing, and the F94 property this test
    // was built for still bites through the run-length bound: post-barrier
    // records riding the committed transaction duplicated SEVENTEEN
    // records here (the whole hold window), not the interval's handful.
    expect_exactly_once_modulo_commit_window(committed(),
                                             kTotal,
                                             /*max_dup_runs=*/1,
                                             /*max_run_len=*/6);
}

// The protocol's decisive case: the worker dies BETWEEN a checkpoint
// completing and its broker commit executing. Pre-protocol this was the
// documented one-interval LOSS (the transaction died unresumable and the
// restore selected the completed checkpoint, skipping its interval). Now
// the restore selects the newest CONFIRMED checkpoint instead, replays the
// tail, and re-produces the dead transaction's records into a fresh
// transaction: nothing may be missing. The exit is armed at
// sink.before_commit on the SECOND commit, so checkpoint one confirms
// normally (the restore target exists) and checkpoint two completes but
// dies pre-commit.
TEST_F(KafkaExactlyOnceTest, ACommitThatDiesWithTheWorkerIsReplayedNotLost) {
    constexpr int kTotal = 40;
    ::setenv("CLINK_2PC_TOTAL", "40", 1);
    ::setenv("CLINK_2PC_TICK_MS", "50", 1);

    ClusterSpec one;
    one.node_binary = node_binary();
    one.workers = 1;
    one.slots_per_worker = 4;
    Cluster c(one);
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0, {.fault = "sink.before_commit=exit:1@2"}));
    ASSERT_TRUE(c.await_workers_registered(1));

    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);

    ASSERT_TRUE(c.await_process_gone(0))
        << "the die-before-commit fault never fired; the scenario under test never happened";
    ASSERT_TRUE(c.restart_worker(0));  // fresh process, no fault armed

    const auto code = sub->await_exit(120s);
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0) << "the job did not recover from a death between completion and commit";

    const auto v = clink::itest::verify_exactly_once_records(committed(), kTotal);
    EXPECT_TRUE(v.missing.empty())
        << "the interval whose commit died with the worker was LOST - the restore did not "
           "select the confirmed checkpoint: "
        << clink::itest::describe(v);
    EXPECT_TRUE(v.unexpected.empty()) << clink::itest::describe(v);
}

// The longer-horizon arm, mirroring the Postgres one: thirty times the
// records, two separated worker losses, same exact verdict.
// RE-ENABLED with its sibling above (the item-51 fix). One caveat stands,
// documented on the wrapper and in the connector doc: a checkpoint that
// COMPLETED whose broker commit had not executed when the worker died
// cannot be recovered by a new producer (librdkafka has no transaction
// resume), and the restore replays from past it. The item-51 fix narrows
// completion to persisted state but cannot close that client-side window;
// if this test ever reds with exactly one contiguous interval missing
// adjacent to a kill, it is that window, and closing it needs a
// commit-confirmed restore protocol.
TEST_F(KafkaExactlyOnceTest, StaysExactlyOnceAcrossTwoSeparatedKillsOnALongerRun) {
    constexpr int kTotal = 1200;
    ::setenv("CLINK_2PC_TOTAL", "1200", 1);
    ::setenv("CLINK_2PC_TICK_MS", "5", 1);

    Cluster c(two_worker_spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));
    auto sub = submit(c, /*max_restarts=*/3);
    ASSERT_NE(sub, nullptr);

    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; }, 45s));

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    // Settle on a CONDITION: checkpoints advance again, so the first restart
    // completed, before the second loss arrives. Markers rather than a topic
    // drain: a read_committed drain chases a live topic's moving EOF.
    const auto before_second = latest_completed(c.checkpoint_dir());
    ASSERT_TRUE(clink::itest::await(
        [&] { return latest_completed(c.checkpoint_dir()) > before_second; }, 60s))
        << "no checkpoint progress after the first kill; the restart never drained";
    ASSERT_TRUE(c.restart_worker(0));

    c.worker(1).kill_hard();
    ASSERT_TRUE(c.await_process_gone(1));

    const auto code = sub->await_exit(180s);
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0) << "the job did not survive two separated worker losses";

    // Two kills: at most two intervals lost through the documented window.
    expect_exactly_once_modulo_commit_window(committed(),
                                             kTotal,
                                             /*max_lost_runs=*/2,
                                             /*max_run_len=*/60);  // 150ms / 5ms tick, 2x slack
}

#endif  // CLINK_HAS_KAFKA

#ifdef CLINK_S3_2PC_JOB_PATH

// The AWS SDK must be shut down on the main thread after the tests and
// before static destruction - finalising from atexit is explicitly
// non-viable (arrow_s3_lifecycle.hpp) - and a test process that initialised
// S3 (the read_all/ensure_bucket helpers do) and exits without this
// SEGFAULTS at exit, AFTER every test has already passed: ctest reports the
// crash, a grep for test results sees only green. A gtest Environment tears
// down after the last test and before main returns; finalize_arrow_s3() is
// a no-op when S3 was never touched, so registering it unconditionally is
// safe for every test in this binary.
class ClinkS3LifecycleEnvironment final : public ::testing::Environment {
public:
    void TearDown() override { clink::connectors::finalize_arrow_s3(); }
};
const auto* const kClinkS3Lifecycle =
    ::testing::AddGlobalTestEnvironment(new ClinkS3LifecycleEnvironment);

// The S3 arm. Same bounded source, same moments - the sink stages one NDJSON
// object per (subtask, checkpoint) as a multipart upload at the barrier and
// completes it at CommitCheckpoint, so visibility is atomic and commits are
// RECOVERABLE (the persisted handle re-completes idempotently at restore).
// Verdicts are therefore strict, like Postgres and unlike Kafka. The in-doubt
// witness is ListMultipartUploads: staged-but-uncommitted output sits there,
// which makes the held-commit window provable from the outside. Orphaned
// uploads from a killed incarnation whose checkpoint never became durable
// stay there BY DESIGN (production expires them with a lifecycle rule), so
// only the clean run asserts the pending set is empty.
class S3ExactlyOnceTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        if (!clink::test::DockerLocalstack::docker_available()) {
            return;  // per-test SetUp skips
        }
        s3_ = new clink::test::DockerLocalstack();
    }

    static void TearDownTestSuite() {
        delete s3_;
        s3_ = nullptr;
    }

    void SetUp() override {
        if (!clink::test::DockerLocalstack::docker_available()) {
            GTEST_SKIP() << "Docker or curl not available; skipping S3 exactly-once suite";
        }
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(submit_binary()) ||
            !std::filesystem::exists(s3_2pc_job())) {
            GTEST_SKIP() << "cluster binaries or the s3 2PC job plugin are not built";
        }
        ASSERT_NE(s3_, nullptr);
        // One bucket per test; S3 bucket names must be lowercase.
        bucket_ = std::string{"clink-xo-"} +
                  ::testing::UnitTest::GetInstance()->current_test_info()->name();
        std::transform(bucket_.begin(), bucket_.end(), bucket_.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        ::setenv("AWS_ACCESS_KEY_ID", "test", 1);
        ::setenv("AWS_SECRET_ACCESS_KEY", "test", 1);
        ::setenv("AWS_EC2_METADATA_DISABLED", "true", 1);
        clink::s3::ensure_bucket(s3_->endpoint(), "us-east-1", bucket_);
        ::setenv("CLINK_S3_BUCKET", bucket_.c_str(), 1);
        ::setenv("CLINK_S3_ENDPOINT", s3_->endpoint().c_str(), 1);
        ::setenv("CLINK_S3_PREFIX", "xo", 1);
    }

    std::vector<std::string> committed() const {
        return clink::s3::read_all_lines(s3_->endpoint(), "us-east-1", bucket_, "xo");
    }
    std::size_t pending_multiparts() const {
        return clink::s3::pending_multipart_count(s3_->endpoint(), "us-east-1", bucket_, "xo");
    }

    static std::unique_ptr<Process> submit(Cluster& c, int max_restarts) {
        return submit_job(c, s3_2pc_job(), max_restarts);
    }

    static clink::test::DockerLocalstack* s3_;
    std::string bucket_;
};

clink::test::DockerLocalstack* S3ExactlyOnceTest::s3_ = nullptr;

// The premise: a clean run commits each record exactly once, and no multipart
// upload is left pending - a clean completion must leave nothing staged.
TEST_F(S3ExactlyOnceTest, ACleanRunCommitsEveryRecordExactlyOnce) {
    constexpr int kTotal = 40;
    ::setenv("CLINK_2PC_TOTAL", "40", 1);
    ::setenv("CLINK_2PC_TICK_MS", "50", 1);

    Cluster c(two_worker_spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));
    auto sub = submit(c, /*max_restarts=*/0);
    ASSERT_NE(sub, nullptr);
    const auto code = sub->await_exit(90s);
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0);

    const auto v = clink::itest::verify_exactly_once_records(committed(), kTotal);
    EXPECT_TRUE(v.clean()) << "a CLEAN S3 2PC run did not commit each record exactly once: "
                           << clink::itest::describe(v);
    EXPECT_EQ(pending_multiparts(), 0u)
        << "the job completed cleanly but left a multipart upload staged";
}

TEST_F(S3ExactlyOnceTest, EveryRecordIsCommittedExactlyOnceAcrossAWorkerKill) {
    constexpr int kTotal = 40;
    ::setenv("CLINK_2PC_TOTAL", "40", 1);
    ::setenv("CLINK_2PC_TICK_MS", "50", 1);

    Cluster c(two_worker_spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));
    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);

    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; }, 45s));
    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    const auto code = sub->await_exit(120s);
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0) << "the job did not recover from the worker kill";
    ASSERT_GT(c.count_in_coordinator_log("restart"), 0)
        << "the job finished before the kill; the recovery path never ran";

    const auto v = clink::itest::verify_exactly_once_records(committed(), kTotal);
    EXPECT_TRUE(v.clean()) << "a worker kill broke S3 exactly-once: " << clink::itest::describe(v);
}

// The commit window, held open and provable: the checkpoint's object is
// STAGED (visible in ListMultipartUploads, invisible in the object listing)
// while the armed delay holds the commit; the kill lands inside that window;
// recovery re-completes the persisted handle idempotently.
TEST_F(S3ExactlyOnceTest, AWorkerKilledInsideTheCommitWindowStaysExactlyOnce) {
    constexpr int kTotal = 40;
    ::setenv("CLINK_2PC_TOTAL", "40", 1);
    ::setenv("CLINK_2PC_TICK_MS", "50", 1);

    Cluster c(two_worker_spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0, {.fault = "sink.before_commit=delay:4000@1"}));
    ASSERT_TRUE(c.start_worker(1, {.fault = "sink.before_commit=delay:4000@1"}));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);

    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; }, 45s));
    // The witness: staged output exists while nothing is visible yet.
    ASSERT_TRUE(clink::itest::await([&] { return pending_multiparts() > 0; }, 30s))
        << "no multipart upload appeared; the commit window never opened";
    ASSERT_TRUE(committed().empty())
        << "objects are visible while the commit should still be held; the fault did not arm "
           "inside the plugin";

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    const auto code = sub->await_exit(120s);
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0) << "the job did not recover from a kill inside the commit window";

    const auto v = clink::itest::verify_exactly_once_records(committed(), kTotal);
    EXPECT_TRUE(v.clean()) << "a kill inside the commit window broke S3 exactly-once: "
                           << clink::itest::describe(v);
}

// The longer-horizon arm: thirty times the records, two separated worker
// losses, strict verdict - S3 commits are recoverable, so unlike Kafka there
// is no tolerated loss window.
TEST_F(S3ExactlyOnceTest, StaysExactlyOnceAcrossTwoSeparatedKillsOnALongerRun) {
    constexpr int kTotal = 1200;
    ::setenv("CLINK_2PC_TOTAL", "1200", 1);
    ::setenv("CLINK_2PC_TICK_MS", "5", 1);

    Cluster c(two_worker_spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));
    auto sub = submit(c, /*max_restarts=*/3);
    ASSERT_NE(sub, nullptr);

    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; }, 45s));

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    const auto before_second = latest_completed(c.checkpoint_dir());
    ASSERT_TRUE(clink::itest::await(
        [&] { return latest_completed(c.checkpoint_dir()) > before_second; }, 60s))
        << "no checkpoint progress after the first kill; the restart never drained";
    ASSERT_TRUE(c.restart_worker(0));

    c.worker(1).kill_hard();
    ASSERT_TRUE(c.await_process_gone(1));

    const auto code = sub->await_exit(180s);
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0) << "the job did not survive two separated worker losses";

    const auto v = clink::itest::verify_exactly_once_records(committed(), kTotal);
    EXPECT_TRUE(v.clean()) << "two separated kills on a longer run broke S3 exactly-once: "
                           << clink::itest::describe(v);
}

#endif  // CLINK_S3_2PC_JOB_PATH

}  // namespace
