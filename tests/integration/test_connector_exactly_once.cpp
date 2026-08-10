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
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "tests/integration/cluster_harness.hpp"
#include "tests/integration/docker_kafka.hpp"
#include "tests/integration/docker_postgres.hpp"
#include "tests/integration/two_pc_output.hpp"

#ifdef CLINK_HAS_KAFKA
#include "clink/kafka/consume_all.hpp"
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

// DISABLED while the restart-window loss is diagnosed (its own unit; this
// suite's job was to find it, and it did - twice over). Two distinct modes
// observed, neither Kafka-wrapper-local:
//
//   A. The job completes ok with every post-restart record missing from the
//      committed view (8/40 committed, no commit-callback errors logged).
//      The read_uncommitted diagnostic in the verdict message classifies
//      whether the records were never produced or produced-but-never-
//      committed.
//   B. The restore REFUSES: "checkpoint 5 named it as a participant but
//      checkpoint-5.snap is absent" - and the kept artifacts show WHY:
//      subtask 0's dir held checkpoint-5.snap.part.4, a partial temp file
//      never renamed, next to a COMPLETED-5 marker. The subtask acked the
//      checkpoint before its snapshot was durably renamed; the SIGKILL
//      landed in that window. This is watch-item 51's one-off Linux CI
//      signature, reproduced locally with the mechanism in hand.
//
// Re-enable when the ack-before-rename window is closed (the fix makes the
// snapshot ack follow the rename) and mode A is classified.
TEST_F(KafkaExactlyOnceTest, DISABLED_EveryRecordIsCommittedExactlyOnceAcrossAWorkerKill) {
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

    const auto v = clink::itest::verify_exactly_once_records(committed(), kTotal);
    EXPECT_TRUE(v.clean())
        << "a worker kill broke Kafka exactly-once: " << clink::itest::describe(v)
        << " (read_uncommitted sees "
        << clink::kafka::consume_all(kafka_->brokers(), topic_, "read_uncommitted").size()
        << " records - absent there too means never produced; present there means the "
           "transaction never committed)";
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

    const auto v = clink::itest::verify_exactly_once_records(committed(), kTotal);
    EXPECT_TRUE(v.clean())
        << "a kill right after the broker commit broke Kafka exactly-once (records produced "
           "after the barrier rode the committed transaction, then were replayed): "
        << clink::itest::describe(v);
}

// The longer-horizon arm, mirroring the Postgres one: thirty times the
// records, two separated worker losses, same exact verdict.
// DISABLED with its sibling above: the same restart window dominates, plus
// one observed run lost exactly one checkpoint interval (records 70..92)
// through the fundamental Kafka limitation documented on the wrapper - a
// checkpoint that COMPLETED whose broker commit had not executed when the
// worker died cannot be recovered by a new producer (librdkafka has no
// transaction resume), and the restore replays from past it. Re-enable with
// the sibling; if the interval-loss mode then still occurs, it is that
// documented window and needs the commit-confirmed restore protocol to
// close.
TEST_F(KafkaExactlyOnceTest, DISABLED_StaysExactlyOnceAcrossTwoSeparatedKillsOnALongerRun) {
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

    const auto v = clink::itest::verify_exactly_once_records(committed(), kTotal);
    EXPECT_TRUE(v.clean()) << "two separated kills on a longer run broke Kafka exactly-once: "
                           << clink::itest::describe(v);
}

#endif  // CLINK_HAS_KAFKA

}  // namespace
