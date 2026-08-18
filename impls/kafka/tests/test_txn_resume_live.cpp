// The prepared-transaction resume against a REAL broker: a genuine orphan
// (produced, flushed, producer destroyed without commit) is invisible to a
// read_committed consumer, pins the last stable offset, and is then
// committed by the wire-level resume path - after which the records are
// exactly the ones a downstream application sees. The fenced case pins the
// fallback: once a successor has initialised the transactional.id, the old
// identity must be REFUSED, never claimed committed. Skips without Docker,
// same convention as the exactly-once suites.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <sys/wait.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/connectors/kafka_message.hpp"
#include "clink/connectors/kafka_sink.hpp"
#include "clink/connectors/txn_resume_registry.hpp"
#include "clink/kafka/consume_all.hpp"
#include "clink/kafka/txn_resume.hpp"
#include "clink/runtime/network/connection.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/time/event_time.hpp"
#include "clink/time/watermark.hpp"

#include "tests/integration/docker_kafka.hpp"

namespace {

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::KafkaMessage;
using clink::KafkaSink;
using clink::kafka::ResumeOutcome;
using clink::kafka::TxnIdentity;

std::unique_ptr<clink::test::DockerKafka> broker_;

class TxnResumeLive : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        if (clink::test::DockerKafka::docker_available()) {
            broker_ = std::make_unique<clink::test::DockerKafka>();
        }
    }
    static void TearDownTestSuite() { broker_.reset(); }

    void SetUp() override {
        if (broker_ == nullptr) {
            GTEST_SKIP() << "Docker not available";
        }
        topic_ = std::string{"clink_resume_"} +
                 ::testing::UnitTest::GetInstance()->current_test_info()->name();
        broker_->create_topic(topic_);
    }

    static std::pair<std::string, std::uint16_t> broker_addr() {
        const auto brokers = broker_->brokers();
        const auto colon = brokers.rfind(':');
        return {brokers.substr(0, colon),
                static_cast<std::uint16_t>(std::stoi(brokers.substr(colon + 1)))};
    }

    // Produce `values` inside a transaction and CRASH: the work happens in
    // a forked child that reports its producer identity through a pipe and
    // then _exit()s without running a single destructor. The child's death
    // drops its sockets with no protocol goodbye, and the transaction
    // stays open broker-side, pinning the last stable offset - exactly the
    // state a killed worker leaves behind. (An in-process simulation would
    // be dishonest for the OPEN TAIL case, which the 2PC wrapper's close()
    // does abort; the prepared case is pinned separately by
    // CloseLeavesAPreparedTransactionForTheResolver below.)
    TxnIdentity produce_orphan(const std::string& txn_id, const std::vector<std::string>& values) {
        int fds[2] = {-1, -1};
        EXPECT_EQ(::pipe(fds), 0);
        const pid_t child = ::fork();
        if (child == 0) {
            ::close(fds[0]);
            KafkaSink::Options o;
            o.brokers = broker_->brokers();
            o.topic = topic_;
            o.transactional_id = txn_id;
            KafkaSink sink(o);
            sink.open();
            Batch<KafkaMessage> b;
            for (const auto& v : values) {
                b.emplace(KafkaMessage{v});
            }
            sink.on_data(b);
            sink.flush();
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
            while (!sink.producer_identity().has_value() &&
                   std::chrono::steady_clock::now() < deadline) {
                sink.flush();  // drives librdkafka's poll loop -> callbacks
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            std::int64_t pid = -1;
            std::int16_t epoch = -1;
            if (const auto ident = sink.producer_identity(); ident.has_value()) {
                pid = ident->producer_id;
                epoch = ident->producer_epoch;
            }
            (void)!::write(fds[1], &pid, sizeof(pid));
            (void)!::write(fds[1], &epoch, sizeof(epoch));
            ::close(fds[1]);
            ::_exit(0);  // the crash: no destructors, no abort, no goodbye
        }
        ::close(fds[1]);
        TxnIdentity txn;
        txn.transactional_id = txn_id;
        std::int64_t pid = -1;
        std::int16_t epoch = -1;
        if (::read(fds[0], &pid, sizeof(pid)) == sizeof(pid) &&
            ::read(fds[0], &epoch, sizeof(epoch)) == sizeof(epoch)) {
            txn.producer_id = pid;
            txn.producer_epoch = epoch;
        }
        ::close(fds[0]);
        int status = 0;
        (void)::waitpid(child, &status, 0);
        EXPECT_TRUE(txn.complete()) << "the child never captured its producer identity";
        return txn;
    }

    static clink::kafka::ConnectFn connect_plain_fn() {
        return [](const std::string& host, std::uint16_t port) {
            return clink::network::connect_plain(host, port);
        };
    }

    std::string topic_;
};

TEST_F(TxnResumeLive, AnOrphanedTransactionIsCommittedByTheResumePath) {
    const std::vector<std::string> values = {"o1", "o2", "o3"};
    const auto txn = produce_orphan("resume-live-happy", values);
    ASSERT_TRUE(txn.complete());

    // The orphan's records exist uncommitted but are invisible to the
    // committed view - either the drain returns without them (partition
    // EOF fires at the last stable offset) or it cannot reach EOF at all
    // and throws. Both are the same fact: an open transaction hides its
    // records from every correctly configured consumer.
    const auto uncommitted = clink::kafka::consume_all(
        broker_->brokers(), topic_, "read_uncommitted", std::chrono::seconds(20));
    EXPECT_EQ(uncommitted, values) << "the produced records must exist broker-side";
    try {
        const auto committed_before = clink::kafka::consume_all_committed(
            broker_->brokers(), topic_, std::chrono::seconds(5));
        EXPECT_TRUE(committed_before.empty())
            << "records of an OPEN transaction leaked into the committed view";
    } catch (const std::exception&) {
        // Equally valid: the open transaction pinned the drain short of EOF.
    }

    // The resume: commit the orphan with the dead producer's identity.
    const auto [host, port] = broker_addr();
    const auto outcome = clink::kafka::resume_commit(host, port, txn, connect_plain_fn());
    ASSERT_TRUE(outcome.committed()) << outcome.detail;

    EXPECT_EQ(
        clink::kafka::consume_all_committed(broker_->brokers(), topic_, std::chrono::seconds(20)),
        values)
        << "after the resume, a downstream read_committed consumer sees exactly the "
           "orphaned records - no loss, no duplicates";
}

TEST_F(TxnResumeLive, AFencedIdentityIsRefusedAndTheOrphanStaysAborted) {
    const auto txn = produce_orphan("resume-live-fenced", {"f1", "f2"});
    ASSERT_TRUE(txn.complete());

    // A successor initialises the same transactional.id: the broker bumps
    // the epoch and aborts the orphan - exactly what a restarted sink does
    // before this resume path existed.
    {
        KafkaSink::Options o;
        o.brokers = broker_->brokers();
        o.topic = topic_;
        o.transactional_id = "resume-live-fenced";
        KafkaSink successor(o);
        successor.open();
        successor.abort_transaction();
        successor.close();
    }

    const auto [host, port] = broker_addr();
    const auto outcome = clink::kafka::resume_commit(host, port, txn, connect_plain_fn());
    EXPECT_FALSE(outcome.committed())
        << "a fenced identity claimed committed - the false-confirm defect";
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::Refused) << outcome.detail;

    EXPECT_TRUE(
        clink::kafka::consume_all_committed(broker_->brokers(), topic_, std::chrono::seconds(20))
            .empty())
        << "the fenced orphan's records must stay invisible";
}

// --- SASL against a real authenticated broker ---------------------------------
//
// The pinned Redpanda speaks SCRAM ONLY (its config validation rejects
// PLAIN outright - verified empirically against v24.2.7), so the live SASL
// truths this suite CAN pin are the refusal shapes: a PLAIN handshake must
// be refused by a real broker naming the mechanisms it does enable, and an
// unauthenticated resume against a SASL-required listener must surface as
// the transport fallback (the broker drops the connection) - never as a
// guess. The PLAIN happy path is pinned hermetically in test_txn_resume.cpp
// and stays that way until SCRAM ships or the broker pin gains PLAIN.
class TxnResumeSaslLive : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        if (clink::test::DockerKafka::docker_available()) {
            clink::test::DockerKafkaOptions opts;
            opts.sasl = true;
            sasl_broker_ = std::make_unique<clink::test::DockerKafka>(opts);
        }
    }
    static void TearDownTestSuite() { sasl_broker_.reset(); }

    void SetUp() override {
        if (sasl_broker_ == nullptr) {
            GTEST_SKIP() << "Docker not available";
        }
    }

    static std::pair<std::string, std::uint16_t> broker_addr() {
        const auto brokers = sasl_broker_->brokers();
        const auto colon = brokers.rfind(':');
        return {brokers.substr(0, colon),
                static_cast<std::uint16_t>(std::stoi(brokers.substr(colon + 1)))};
    }

    static TxnIdentity some_identity() {
        TxnIdentity txn;
        txn.transactional_id = "sasl-live";
        txn.producer_id = 7;
        txn.producer_epoch = 0;
        return txn;
    }

    static std::unique_ptr<clink::test::DockerKafka> sasl_broker_;
};

std::unique_ptr<clink::test::DockerKafka> TxnResumeSaslLive::sasl_broker_;

// The crown arm: the WHOLE resume circle on an authenticated broker. A
// genuine orphan is produced with SCRAM credentials (fork + _exit, no
// destructors, exactly as the unauthenticated suite does), the resume
// authenticates via SCRAM-SHA-256 - both connections, server signature
// verified - and commits it; an authenticated read_committed drain then
// sees exactly the orphaned records. This is what "SCRAM ships" means.
TEST_F(TxnResumeSaslLive, AScramAuthenticatedResumeCommitsAGenuineOrphan) {
    const std::string topic = "clink_scram_resume";
    sasl_broker_->create_topic(topic);
    const std::vector<std::pair<std::string, std::string>> client_sasl = {
        {"security.protocol", "sasl_plaintext"},
        {"sasl.mechanism", "SCRAM-SHA-256"},
        {"sasl.username", "admin"},
        {"sasl.password", "secret"}};

    // Produce the orphan in a forked child, with the sink authenticating
    // via librdkafka's own SCRAM - independent of the resume's
    // implementation, so the two sides cross-check each other.
    const std::vector<std::string> values = {"s1", "s2", "s3"};
    int fds[2] = {-1, -1};
    ASSERT_EQ(::pipe(fds), 0);
    const pid_t child = ::fork();
    if (child == 0) {
        ::close(fds[0]);
        KafkaSink::Options o;
        o.brokers = sasl_broker_->brokers();
        o.topic = topic;
        o.transactional_id = "scram-resume-live";
        for (const auto& [k, v] : client_sasl) {
            o.conf[k] = v;
        }
        KafkaSink sink(o);
        sink.open();
        Batch<KafkaMessage> b;
        for (const auto& v : values) {
            b.emplace(KafkaMessage{v});
        }
        sink.on_data(b);
        sink.flush();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!sink.producer_identity().has_value() &&
               std::chrono::steady_clock::now() < deadline) {
            sink.flush();
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        std::int64_t pid = -1;
        std::int16_t epoch = -1;
        if (const auto ident = sink.producer_identity(); ident.has_value()) {
            pid = ident->producer_id;
            epoch = ident->producer_epoch;
        }
        (void)!::write(fds[1], &pid, sizeof(pid));
        (void)!::write(fds[1], &epoch, sizeof(epoch));
        ::close(fds[1]);
        ::_exit(0);  // the crash: no destructors, no abort, no goodbye
    }
    ::close(fds[1]);
    TxnIdentity txn;
    txn.transactional_id = "scram-resume-live";
    std::int64_t pid = -1;
    std::int16_t epoch = -1;
    if (::read(fds[0], &pid, sizeof(pid)) == sizeof(pid) &&
        ::read(fds[0], &epoch, sizeof(epoch)) == sizeof(epoch)) {
        txn.producer_id = pid;
        txn.producer_epoch = epoch;
    }
    ::close(fds[0]);
    int status = 0;
    (void)::waitpid(child, &status, 0);
    ASSERT_TRUE(txn.complete()) << "the child never captured its producer identity";

    clink::kafka::ResumeAuth auth;
    auth.mechanism = "SCRAM-SHA-256";
    auth.username = "admin";
    auth.password = "secret";
    const auto [host, port] = broker_addr();
    const auto outcome = clink::kafka::resume_commit(
        host,
        port,
        txn,
        [](const std::string& h, std::uint16_t p) { return clink::network::connect_plain(h, p); },
        auth);
    ASSERT_TRUE(outcome.committed()) << outcome.detail;

    EXPECT_EQ(clink::kafka::consume_all_committed(
                  sasl_broker_->brokers(), topic, std::chrono::seconds{20}, client_sasl),
              values)
        << "after the SCRAM-authenticated resume, a downstream read_committed consumer sees "
           "exactly the orphaned records";
}

// Wrong SCRAM credentials must be refused by the REAL broker with its own
// message - and nothing may be committed.
TEST_F(TxnResumeSaslLive, WrongScramCredentialsAreRefusedByTheRealBroker) {
    clink::kafka::ResumeAuth auth;
    auth.mechanism = "SCRAM-SHA-256";
    auth.username = "admin";
    auth.password = "not-the-password";
    const auto [host, port] = broker_addr();
    const auto outcome = clink::kafka::resume_commit(
        host,
        port,
        some_identity(),
        [](const std::string& h, std::uint16_t p) { return clink::network::connect_plain(h, p); },
        auth);
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::Refused) << outcome.detail;
}

TEST_F(TxnResumeSaslLive, APlainHandshakeIsRefusedByAScramOnlyBrokerNamingScram) {
    clink::kafka::ResumeAuth auth;
    auth.mechanism = "PLAIN";
    auth.username = "admin";
    auth.password = "secret";
    const auto [host, port] = broker_addr();
    const auto outcome = clink::kafka::resume_commit(
        host,
        port,
        some_identity(),
        [](const std::string& h, std::uint16_t p) { return clink::network::connect_plain(h, p); },
        auth);
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::Refused)
        << "a real broker's mechanism refusal must be FINAL, never retried into a guess: "
        << outcome.detail;
    EXPECT_NE(outcome.detail.find("UNSUPPORTED_SASL_MECHANISM"), std::string::npos)
        << outcome.detail;
    EXPECT_NE(outcome.detail.find("SCRAM"), std::string::npos)
        << "the refusal must carry what the broker WOULD accept: " << outcome.detail;
}

TEST_F(TxnResumeSaslLive, NoCredentialsAgainstASaslBrokerFallsBackAsTransportError) {
    const auto [host, port] = broker_addr();
    const auto outcome = clink::kafka::resume_commit(
        host, port, some_identity(), [](const std::string& h, std::uint16_t p) {
            return clink::network::connect_plain(h, p);
        });
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::TransportError)
        << "a SASL-required listener drops unauthenticated traffic; the resume must fall "
           "back to the bounded contract, never claim a verdict: "
        << outcome.detail;
}

// qual01-20260818a: a checkpoint COMPLETED, its commit broadcast reached
// some sinks and not others (a restart drain was tearing the job down),
// and the torn-down sinks' close() ABORTED their prepared transactions.
// In-doubt resolution then found the handles fenced, fell back to the
// bounded replay, and the slices that HAD committed were re-emitted:
// 13,519 identical-value duplicates in one window. The sink-level
// invariant this pins: close() may abort the OPEN TAIL transaction
// (records after the last barrier - the restore replays them), but a
// barrier-sealed PREPARED transaction belongs to the checkpoint protocol,
// and only a commit, an abort broadcast, or the resolver may finalise it.
// A prepared transaction left pending costs read_committed consumers
// latency up to transaction.timeout.ms if nothing resolves it; an aborted
// one costs correctness whenever its checkpoint completed.
TEST_F(TxnResumeLive, CloseLeavesAPreparedTransactionForTheResolver) {
    auto& registry = clink::cluster::OperatorRegistry::default_instance();
    const auto* factory =
        registry.find_sink("kafka_2pc_sink_string", clink::cluster::ChannelType{"string"});
    ASSERT_NE(factory, nullptr);

    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"brokers", broker_->brokers()},
                  {"topic", topic_},
                  {"transactional_id", "resume-live-close-prepared"}};
    auto boxed = factory->build(ctx);
    auto sink = std::static_pointer_cast<clink::Sink<std::string>>(boxed);
    ASSERT_NE(sink, nullptr);

    clink::InMemoryStateBackend state;
    clink::RuntimeContext rctx(clink::OperatorId{43}, "resume_close", &state, nullptr);
    sink->set_id(clink::OperatorId{43});
    sink->attach_runtime(&rctx);

    sink->open();
    Batch<std::string> committed_interval;
    committed_interval.emplace(std::string{"c1"});
    committed_interval.emplace(std::string{"c2"});
    sink->on_data(committed_interval);
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));  // identity capture
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});          // T_1 PREPARED

    // Records after the barrier: the open tail. close() aborting THESE is
    // correct - no checkpoint covers them and the restore replays them.
    Batch<std::string> tail;
    tail.emplace(std::string{"tail-must-abort"});
    sink->on_data(tail);

    const std::string key = std::string(clink::connectors::kTxnResumeStateKeyPrefix) + "sub0";
    const auto staged = state.get_operator_state(
        clink::OperatorId{43}, clink::StateBackend::KeyView{key.data(), key.size()});
    ASSERT_TRUE(staged.has_value()) << "the barrier must stage a resume handle";
    const std::string handle(reinterpret_cast<const char*>(staged->data()), staged->size());

    // The teardown that races a commit broadcast: no on_commit arrives.
    sink->close();

    // The resolver must still be able to finalise T_1 with the staged
    // handle, exactly as it would after a process death.
    const auto resolver = clink::connectors::TxnResumeRegistry::instance().find("kafka_2pc");
    ASSERT_TRUE(resolver.has_value());
    const auto resolution = (*resolver)(handle);
    EXPECT_TRUE(resolution.committed)
        << "close() must leave the PREPARED transaction for the resolver; it was finalised "
           "some other way: "
        << resolution.detail;

    // The committed view holds exactly the prepared interval: the tail's
    // transaction died with close (correct), the prepared one committed
    // via the resolver (the fix), nothing twice.
    EXPECT_EQ(
        clink::kafka::consume_all_committed(broker_->brokers(), topic_, std::chrono::seconds(20)),
        (std::vector<std::string>{"c1", "c2"}))
        << "prepared interval must be exactly-once after close + resolve";
}

TEST_F(TxnResumeLive, TheWrapperStagesAResolvableHandleAndErasesItOnCommit) {
    // The full sink-side half through the real factory: the 2PC wrapper
    // stages a handle whose identity the registered resolver could use,
    // and erases it once the commit executes.
    auto& registry = clink::cluster::OperatorRegistry::default_instance();
    const auto* factory =
        registry.find_sink("kafka_2pc_sink_string", clink::cluster::ChannelType{"string"});
    ASSERT_NE(factory, nullptr);

    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"brokers", broker_->brokers()},
                  {"topic", topic_},
                  {"transactional_id", "resume-live-wrapper"}};
    auto boxed = factory->build(ctx);
    auto sink = std::static_pointer_cast<clink::Sink<std::string>>(boxed);
    ASSERT_NE(sink, nullptr);

    clink::InMemoryStateBackend state;
    clink::RuntimeContext rctx(clink::OperatorId{42}, "resume_wrapper", &state, nullptr);
    sink->set_id(clink::OperatorId{42});
    sink->attach_runtime(&rctx);
    EXPECT_TRUE(sink->stages_state_at_barrier());

    sink->open();
    Batch<std::string> b;
    b.emplace(std::string{"w1"});
    sink->on_data(b);
    // Give the stats tick time to deliver the identity before the barrier
    // stages the handle (bounded; an absent identity stages an unresumable
    // handle, which the assertion below would catch).
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});

    const std::string key = std::string(clink::connectors::kTxnResumeStateKeyPrefix) + "sub0";
    const auto staged = state.get_operator_state(
        clink::OperatorId{42}, clink::StateBackend::KeyView{key.data(), key.size()});
    ASSERT_TRUE(staged.has_value()) << "the barrier must stage a resume handle";
    const std::string handle(reinterpret_cast<const char*>(staged->data()), staged->size());
    EXPECT_NE(handle.find("\"resolver\":\"kafka_2pc\""), std::string::npos) << handle;
    EXPECT_NE(handle.find("\"transactional_id\":\"resume-live-wrapper\""), std::string::npos);
    EXPECT_EQ(handle.find("\"producer_id\":\"-1\""), std::string::npos)
        << "the staged handle carries no identity - resume would be impossible: " << handle;

    sink->on_commit(1);
    EXPECT_FALSE(state
                     .get_operator_state(clink::OperatorId{42},
                                         clink::StateBackend::KeyView{key.data(), key.size()})
                     .has_value())
        << "an executed commit must erase its handle";
    sink->close();
}

// Builds the 2PC sink through the factory with a RuntimeContext carrying a
// commit-receipt directory, so the receipt + replay-suppression surface runs
// exactly as deployed. Returns the sink; `rctx` and `state` must outlive it.
std::shared_ptr<clink::Sink<std::string>> build_receipt_sink(const std::string& topic,
                                                             const std::string& txn_id,
                                                             clink::RuntimeContext& rctx) {
    auto& registry = clink::cluster::OperatorRegistry::default_instance();
    const auto* factory =
        registry.find_sink("kafka_2pc_sink_string", clink::cluster::ChannelType{"string"});
    if (factory == nullptr) {
        return nullptr;
    }
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"brokers", broker_->brokers()}, {"topic", topic}, {"transactional_id", txn_id}};
    auto sink = std::static_pointer_cast<clink::Sink<std::string>>(factory->build(ctx));
    sink->set_id(clink::OperatorId{44});
    sink->attach_runtime(&rctx);
    return sink;
}

TEST_F(TxnResumeLive, ACommitWritesAReceiptAndFreshStartsNeverSuppress) {
    const auto receipt_dir = std::filesystem::temp_directory_path() /
                             ("clink_receipts_" + std::to_string(::getpid()) + "_" +
                              ::testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::remove_all(receipt_dir);
    std::filesystem::create_directories(receipt_dir);
    // A lingering receipt from some earlier run. A FRESH start (restore id
    // 0) reprocesses from scratch and owes the user its full output, so it
    // must not arm suppression from this.
    {
        std::ofstream stale(receipt_dir / clink::connectors::commit_receipt_file_name(0, 9));
        stale << "wm=99999\n";
    }

    clink::InMemoryStateBackend state;
    clink::RuntimeContext rctx(clink::OperatorId{44}, "receipt_fresh", &state, nullptr);
    rctx.set_commit_receipts(receipt_dir.string(), /*restore_from_ckpt=*/0);
    auto sink = build_receipt_sink(topic_, "receipt-live-fresh", rctx);
    ASSERT_NE(sink, nullptr);

    sink->open();
    Batch<std::string> b;
    b.emplace(std::string{"v1"}, clink::EventTime{100});  // far below the stale horizon
    sink->on_data(b);
    sink->on_watermark(clink::Watermark{clink::EventTime{200}});
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->on_commit(1);
    sink->close();

    EXPECT_EQ(
        clink::kafka::consume_all_committed(broker_->brokers(), topic_, std::chrono::seconds(20)),
        (std::vector<std::string>{"v1"}))
        << "a fresh start must publish everything - stale receipts arm nothing";
    // The commit dropped a durable receipt carrying the sealing barrier's
    // watermark horizon.
    std::ifstream in(receipt_dir / clink::connectors::commit_receipt_file_name(0, 1));
    std::string line;
    ASSERT_TRUE(in.is_open() && std::getline(in, line))
        << "an executed commit must write its receipt";
    EXPECT_EQ(line, "wm=200");
    std::filesystem::remove_all(receipt_dir);
}

TEST_F(TxnResumeLive, AnArmedRestoreSuppressesAlreadyPublishedReemissions) {
    // The partial-commit fallback: this subtask's commit for checkpoint 3
    // executed (receipt on disk, horizon wm=10000) but recovery restored
    // from checkpoint 1 because a SIBLING subtask's transaction was lost.
    // The replay re-produces this subtask's already-published interval; the
    // armed sink must swallow exactly the re-emissions at or below the
    // horizon and pass everything else - including a record it cannot
    // judge (no event time), which degrades loudly to bounded replay
    // rather than silently dropping data.
    const auto receipt_dir = std::filesystem::temp_directory_path() /
                             ("clink_receipts_" + std::to_string(::getpid()) + "_" +
                              ::testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::remove_all(receipt_dir);
    std::filesystem::create_directories(receipt_dir);
    {
        std::ofstream receipt(receipt_dir / clink::connectors::commit_receipt_file_name(0, 3));
        receipt << "wm=10000\n";
    }

    clink::InMemoryStateBackend state;
    clink::RuntimeContext rctx(clink::OperatorId{44}, "receipt_armed", &state, nullptr);
    rctx.set_commit_receipts(receipt_dir.string(), /*restore_from_ckpt=*/1);
    auto sink = build_receipt_sink(topic_, "receipt-live-armed", rctx);
    ASSERT_NE(sink, nullptr);

    sink->open();  // arms from the receipt: horizon 10000
    Batch<std::string> replayed;
    replayed.emplace(std::string{"old-a"}, clink::EventTime{9000});
    replayed.emplace(std::string{"old-b"}, clink::EventTime{10000});  // == horizon: covered
    sink->on_data(replayed);

    Batch<std::string> unjudgeable;
    unjudgeable.emplace(std::string{"no-ts"});  // no event time: pass, loudly
    sink->on_data(unjudgeable);

    Batch<std::string> past_horizon;
    past_horizon.emplace(std::string{"edge-d"}, clink::EventTime{10001});  // first new pane
    sink->on_data(past_horizon);

    sink->on_watermark(clink::Watermark{clink::EventTime{10500}});  // retires suppression

    Batch<std::string> fresh;
    fresh.emplace(std::string{"new-c"}, clink::EventTime{10200});
    sink->on_data(fresh);

    sink->on_barrier(CheckpointBarrier{CheckpointId{10}});
    sink->on_commit(10);
    sink->close();

    EXPECT_EQ(
        clink::kafka::consume_all_committed(broker_->brokers(), topic_, std::chrono::seconds(20)),
        (std::vector<std::string>{"no-ts", "edge-d", "new-c"}))
        << "exactly the receipted horizon is swallowed: nothing below it twice, nothing "
           "above it lost";
    std::filesystem::remove_all(receipt_dir);
}

}  // namespace
