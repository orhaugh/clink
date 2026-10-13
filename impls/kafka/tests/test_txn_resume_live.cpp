// The prepared-transaction resume against a REAL broker: a genuine orphan
// (produced, flushed, producer destroyed without commit) is invisible to a
// read_committed consumer, pins the last stable offset, and is then
// committed by the wire-level resume path - after which the records are
// exactly the ones a downstream application sees. The fenced case pins the
// fallback: once a successor has initialised the transactional.id, the old
// identity must be REFUSED, never claimed committed. Skips without Docker,
// same convention as the exactly-once suites.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <sys/wait.h>

#include "clink/cluster/in_doubt_resolution.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/protocol_trace.hpp"
#include "clink/connectors/kafka_message.hpp"
#include "clink/connectors/kafka_sink.hpp"
#include "clink/connectors/txn_resume_registry.hpp"
#include "clink/fault/fault_injection.hpp"
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

// The ack-window question, pinned against the real broker: a worker can be
// killed AFTER its EndTxn(commit) took effect broker-side but BEFORE the
// process recorded anything (the receipt fsync, or even the client ack
// arriving). Recovery's wire path then re-sends EndTxn(commit) with the
// SAME identity. Whatever the broker answers here defines whether the
// resume path alone rescues that window - qual01-20260818d's 4,560
// duplicates rode exactly this gap. This test pins the observed behaviour
// so a broker upgrade that changes it fails loudly instead of silently
// changing the recovery contract.
TEST_F(TxnResumeLive, ResumingAnAlreadyCommittedTransactionIsIdempotentlyCommitted) {
    KafkaSink::Options o;
    o.brokers = broker_->brokers();
    o.topic = topic_;
    o.transactional_id = "resume-live-recommit";
    o.metric_prefix.clear();
    KafkaSink sink(o);
    sink.open();
    Batch<KafkaMessage> b;
    b.emplace(KafkaMessage{"r1"});
    b.emplace(KafkaMessage{"r2"});
    sink.on_data(b);
    sink.flush();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!sink.producer_identity().has_value() && std::chrono::steady_clock::now() < deadline) {
        sink.flush();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    const auto ident = sink.producer_identity();
    ASSERT_TRUE(ident.has_value());
    sink.commit_transaction();  // the commit that a kill would leave unrecorded
    sink.close();

    TxnIdentity txn;
    txn.transactional_id = "resume-live-recommit";
    txn.producer_id = ident->producer_id;
    txn.producer_epoch = ident->producer_epoch;
    const auto [host, port] = broker_addr();
    const auto outcome = clink::kafka::resume_commit(host, port, txn, connect_plain_fn());
    EXPECT_TRUE(outcome.committed())
        << "re-EndTxn(commit) for an already-committed transaction with the same identity "
           "answered '"
        << outcome.detail
        << "' - the wire path no longer rescues the kill-between-commit-and-receipt window "
           "and recovery needs a different proof for it";
    EXPECT_EQ(
        clink::kafka::consume_all_committed(broker_->brokers(), topic_, std::chrono::seconds(20)),
        (std::vector<std::string>{"r1", "r2"}))
        << "the re-commit must be idempotent: nothing lost, nothing twice";
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

// A fresh start in a checkpoint directory an earlier run of the same job id
// used. Job ids restart at 1 in every coordinator process and the receipts
// directory is per job id, so the earlier run's receipts and its unresolved
// marker sit where this run's would. The run's deploy carries a checkpoint-id
// floor above all of them: the open arms nothing and settles nothing, and the
// run owes, and publishes, its full output.
TEST_F(TxnResumeLive, AFreshStartAboveAnEarlierRunsReceiptsArmsNothing) {
    const auto receipt_dir = std::filesystem::temp_directory_path() /
                             ("clink_receipts_" + std::to_string(::getpid()) + "_" +
                              ::testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::remove_all(receipt_dir);
    std::filesystem::create_directories(receipt_dir);
    const std::string txn_id = "receipt-live-fresh-floor";
    // The earlier run: receipts for 5 and 9 with a horizon above everything
    // this run emits, and an unresolved marker for 7.
    for (const std::uint64_t ckpt : {5U, 9U}) {
        std::ofstream(receipt_dir / clink::connectors::commit_receipt_file_name(0, ckpt))
            << "wm=99999\n";
    }
    const auto marker =
        receipt_dir / (clink::connectors::commit_receipt_file_name(0, 7) + ".unresolved");
    std::ofstream(marker) << R"({"v":1,"resolver":"kafka_2pc","bootstrap":")" << broker_->brokers()
                          << R"(","transactional_id":")" << txn_id
                          << R"(","producer_id":"1","producer_epoch":"0","ckpt":"7","wm":"99999"})";

    clink::InMemoryStateBackend state;
    clink::RuntimeContext rctx(clink::OperatorId{44}, "receipt_fresh_floor", &state, nullptr);
    rctx.set_commit_receipts(receipt_dir.string(), /*restore_from_ckpt=*/0);
    rctx.set_run_checkpoint_floor(9);
    auto sink = build_receipt_sink(topic_, txn_id, rctx);
    ASSERT_NE(sink, nullptr);

    sink->open();
    EXPECT_TRUE(std::filesystem::exists(marker))
        << "the open settled an earlier run's marker as though it were this run's orphan";
    Batch<std::string> b;
    b.emplace(std::string{"v1"}, clink::EventTime{100});
    b.emplace(std::string{"v2"}, clink::EventTime{200});
    sink->on_data(b);
    sink->on_watermark(clink::Watermark{clink::EventTime{300}});
    sink->on_barrier(CheckpointBarrier{CheckpointId{10}});
    sink->on_commit(10);
    sink->close();

    EXPECT_EQ(
        clink::kafka::consume_all_committed(broker_->brokers(), topic_, std::chrono::seconds(20)),
        (std::vector<std::string>{"v1", "v2"}))
        << "an earlier run's receipts swallowed this run's output";
    std::ifstream in(receipt_dir / clink::connectors::commit_receipt_file_name(0, 10));
    std::string line;
    ASSERT_TRUE(in.is_open() && std::getline(in, line)) << "the commit must write its receipt";
    EXPECT_EQ(line, "wm=300");
    std::filesystem::remove_all(receipt_dir);
}

// A resubmit that restores from checkpoint R of an earlier run in the same
// directory continues that run. The earlier run's commits above R are
// published, the restore's replay re-emits them, and their receipts must
// swallow the re-emissions. The run's checkpoint-id floor is R
// (new_run_checkpoint_floor, the rule the submit applies), not the newest
// marker, which would leave those receipts below the floor and publish their
// output a second time.
TEST_F(TxnResumeLive, AResubmitFromACheckpointArmsFromTheReceiptsAboveIt) {
    const auto ckpt_dir = std::filesystem::temp_directory_path() /
                          ("clink_resubmit_" + std::to_string(::getpid()) + "_" +
                           ::testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::remove_all(ckpt_dir);
    constexpr clink::cluster::JobId kJob = 1;
    const auto markers = ckpt_dir / "_jobs" / std::to_string(kJob);
    std::filesystem::create_directories(markers);
    const auto receipts = clink::cluster::commit_receipt_dir_for(ckpt_dir.string(), kJob);
    const std::string txn_id = "receipt-live-resubmit";

    // The earlier run: checkpoints 1 to 4, each completed, committed, receipted
    // and confirmed.
    {
        clink::InMemoryStateBackend state;
        clink::RuntimeContext rctx(clink::OperatorId{44}, "resubmit_earlier", &state, nullptr);
        rctx.set_commit_receipts(receipts.string(), /*restore_from_ckpt=*/0);
        rctx.set_run_checkpoint_floor(
            clink::cluster::new_run_checkpoint_floor(ckpt_dir.string(), kJob, 0, 0));
        auto sink = build_receipt_sink(topic_, txn_id, rctx);
        ASSERT_NE(sink, nullptr);
        sink->open();
        for (std::uint64_t c = 1; c <= 4; ++c) {
            const auto at = static_cast<std::int64_t>(100 * c);
            Batch<std::string> b;
            b.emplace("a" + std::to_string(c), clink::EventTime{at});
            sink->on_data(b);
            sink->on_watermark(clink::Watermark{clink::EventTime{at + 50}});
            sink->on_barrier(CheckpointBarrier{CheckpointId{c}});
            sink->on_commit(c);
            std::ofstream(markers / ("COMPLETED-" + std::to_string(c))) << "checkpoint=" << c;
            std::ofstream(markers / ("CONFIRMED-" + std::to_string(c))) << "checkpoint=" << c;
        }
        sink->close();
    }

    // The resubmit restores from checkpoint 2 of that run. It numbers its own
    // checkpoints above every marker (4), and its floor is the restore point.
    constexpr std::uint64_t kRestore = 2;
    const auto floor = clink::cluster::new_run_checkpoint_floor(
        ckpt_dir.string(), kJob, kRestore, /*numbering_floor=*/4);
    EXPECT_EQ(floor, kRestore) << "a restore continues the run that took the checkpoint";
    clink::InMemoryStateBackend state;
    clink::RuntimeContext rctx(clink::OperatorId{44}, "resubmit", &state, nullptr);
    rctx.set_commit_receipts(receipts.string(), kRestore);
    rctx.set_run_checkpoint_floor(floor);
    auto sink = build_receipt_sink(topic_, txn_id, rctx);
    ASSERT_NE(sink, nullptr);
    sink->open();  // arms from checkpoint 4's receipt: horizon 450
    Batch<std::string> replayed;
    replayed.emplace(std::string{"a3"}, clink::EventTime{300});
    replayed.emplace(std::string{"a4"}, clink::EventTime{400});
    sink->on_data(replayed);
    Batch<std::string> fresh;
    fresh.emplace(std::string{"b1"}, clink::EventTime{500});
    sink->on_data(fresh);
    sink->on_watermark(clink::Watermark{clink::EventTime{600}});
    sink->on_barrier(CheckpointBarrier{CheckpointId{5}});
    sink->on_commit(5);
    sink->close();

    EXPECT_EQ(
        clink::kafka::consume_all_committed(broker_->brokers(), topic_, std::chrono::seconds(20)),
        (std::vector<std::string>{"a1", "a2", "a3", "a4", "b1"}))
        << "the resubmit republished output the earlier run's receipts above its restore point "
           "prove published";
    std::filesystem::remove_all(ckpt_dir);
}

// --- settling an orphan at a reopening sink's open, on a real broker ------------
//
// settle_orphan against a genuine orphan (produced, flushed, the producer
// _exit()ed in a fork): it aborts an undecided one under the identity
// DescribeTransactions reports, after which the old identity's commit is
// refused; and when a commit lands between its describe and its abort, the
// abort is refused and the next describe proves the commit.

TEST_F(TxnResumeLive, TheSettleAbortsAGenuineOrphanAndTheOldCommitIsThenRefused) {
    const auto txn = produce_orphan("settle-live-abort", {"x1", "x2"});
    ASSERT_TRUE(txn.complete());
    const auto [host, port] = broker_addr();
    int before_abort = 0;
    const auto settled = clink::kafka::settle_orphan(
        host, port, txn, connect_plain_fn(), {}, [&before_abort] { ++before_abort; });
    ASSERT_EQ(settled.status, clink::kafka::OrphanSettlement::Status::Aborted) << settled.detail;
    EXPECT_EQ(before_abort, 1);
    EXPECT_EQ(settled.producer_id, txn.producer_id);

    // The old incarnation's commit, arriving after the abort, must not land.
    const auto late = clink::kafka::resume_commit(host, port, txn, connect_plain_fn());
    EXPECT_FALSE(late.committed()) << "a commit after the settle's abort landed: " << late.detail;
    std::cout << "SETTLE-LIVE abort: " << settled.detail
              << "; the old commit after it: " << late.detail << "\n";
    EXPECT_TRUE(
        clink::kafka::consume_all_committed(broker_->brokers(), topic_, std::chrono::seconds(20))
            .empty())
        << "the aborted orphan's records must stay invisible";
}

TEST_F(TxnResumeLive, TheSettleProvesACommitThatLandsBetweenItsDescribeAndItsAbort) {
    const std::vector<std::string> values = {"y1", "y2", "y3"};
    const auto txn = produce_orphan("settle-live-commit-wins", values);
    ASSERT_TRUE(txn.complete());
    const auto [host, port] = broker_addr();
    // The old incarnation's commit lands in the one window that matters: the
    // describe has seen Ongoing, the abort is not yet sent.
    clink::kafka::ResumeOutcome raced;
    int before_abort = 0;
    const auto settled = clink::kafka::settle_orphan(
        host, port, txn, connect_plain_fn(), {}, [&, h = host, p = port] {
            ++before_abort;
            raced = clink::kafka::resume_commit(h, p, txn, connect_plain_fn());
        });
    ASSERT_TRUE(raced.committed()) << raced.detail;
    EXPECT_EQ(before_abort, 1) << "the settle must have described the orphan Ongoing first";
    EXPECT_EQ(settled.status, clink::kafka::OrphanSettlement::Status::Committed)
        << "the abort was sent after the commit landed; the broker must refuse it and the next "
           "describe prove the commit: "
        << settled.detail;
    std::cout << "SETTLE-LIVE commit-wins: " << settled.detail << "\n";
    EXPECT_EQ(
        clink::kafka::consume_all_committed(broker_->brokers(), topic_, std::chrono::seconds(20)),
        values)
        << "the commit that won is the only copy, and the abort undid nothing";

    // A settle that runs after the commit has completed proves it directly.
    int again = 0;
    const auto later =
        clink::kafka::settle_orphan(host, port, txn, connect_plain_fn(), {}, [&again] { ++again; });
    EXPECT_EQ(later.status, clink::kafka::OrphanSettlement::Status::Committed) << later.detail;
    EXPECT_EQ(again, 0) << "a decided transaction is described, never aborted";
}

// The producer epochs DescribeTransactions reports around a commit and an
// abort on the pinned broker, next to the epoch librdkafka's statistics
// report: the facts the settle's proof rule and orphan_commit_proven's
// exact-epoch rule rest on. Printed (EPOCH-PROBE lines) and pinned.
TEST_F(TxnResumeLive, DescribeReportsTheProducerEpochsAroundACommitAndAnAbort) {
    const std::string txn_id = "epoch-probe";
    // A wide topic, so the first commit has a control marker to write to
    // every partition and the describes during it have a window to land in.
    constexpr int kPartitions = 64;
    const std::string wide = topic_ + "_wide";
    broker_->create_topic(wide, kPartitions);
    KafkaSink::Options o;
    o.brokers = broker_->brokers();
    o.topic = wide;
    o.transactional_id = txn_id;
    o.metric_prefix.clear();
    KafkaSink sink(o);
    sink.open();
    const auto [host, port] = broker_addr();
    const auto describe = [&, h = host, p = port] {
        return clink::kafka::describe_transaction_state(h, p, txn_id, connect_plain_fn());
    };
    // The statistics epoch, refreshed: drive librdkafka's poll loop past a
    // couple of statistics ticks (100 ms each).
    const auto stats_epoch = [&sink] {
        for (int i = 0; i < 4; ++i) {
            sink.flush();
            std::this_thread::sleep_for(std::chrono::milliseconds(60));
        }
        const auto ident = sink.producer_identity();
        return ident.has_value() ? static_cast<int>(ident->producer_epoch) : -1;
    };
    const auto text = [](const std::optional<clink::kafka::DescribeState>& d) {
        return d.has_value() ? d->state + "@" + std::to_string(d->producer_epoch) +
                                   " pid=" + std::to_string(d->producer_id)
                             : std::string{"<no answer>"};
    };
    std::vector<std::pair<std::string, std::optional<clink::kafka::DescribeState>>> seen;
    const auto note = [&](const std::string& when) {
        const auto d = describe();
        std::cout << "EPOCH-PROBE " << when << ": describe " << text(d) << ", stats epoch "
                  << stats_epoch() << "\n";
        seen.emplace_back(when, d);
        return d;
    };

    const auto after_init = note("after init_transactions");
    const auto produce = [&sink](const std::string& v) {
        Batch<KafkaMessage> b;
        b.emplace(KafkaMessage{v});
        sink.on_data(b);
        sink.flush();
    };
    {
        Batch<KafkaMessage> b;
        for (int p = 0; p < kPartitions; ++p) {
            KafkaMessage m{"e1"};
            m.partition = p;
            b.emplace(std::move(m));
        }
        sink.on_data(b);
        sink.flush();
    }
    const auto ongoing_1 = note("first transaction, records flushed");

    // Commit while a second thread describes as fast as it can, to catch the
    // PrepareCommit window if the broker exposes one.
    std::atomic<bool> committing{true};
    std::mutex during_mu;
    std::vector<std::string> during;
    int samples = 0;
    std::thread poller([&] {
        while (committing.load()) {
            const auto d = describe();
            std::lock_guard lock(during_mu);
            ++samples;
            if (during.empty() || during.back() != text(d)) {
                during.push_back(text(d));
            }
        }
    });
    sink.commit_transaction();
    committing.store(false);
    poller.join();
    std::cout << "EPOCH-PROBE during the commit (" << samples << " describes):";
    for (const auto& d : during) {
        std::cout << " " << d;
    }
    std::cout << "\n";
    const auto committed_1 = note("after commit_transaction");

    produce("e2");
    const auto ongoing_2 = note("second transaction, records flushed");
    sink.abort_transaction();
    const auto aborted_2 = note("after abort_transaction");
    produce("e3");
    const auto ongoing_3 = note("third transaction, records flushed");
    // A successor opens the same transactional id: the fence a reopening
    // sink's init_transactions applies.
    KafkaSink successor(o);
    successor.open();
    const auto fenced = describe();
    std::cout << "EPOCH-PROBE after a successor's init_transactions: describe " << text(fenced)
              << "\n";
    successor.close();
    sink.close();

    ASSERT_TRUE(after_init.has_value() && ongoing_1.has_value() && committed_1.has_value() &&
                ongoing_2.has_value() && aborted_2.has_value() && ongoing_3.has_value() &&
                fenced.has_value());
    EXPECT_EQ(ongoing_1->state, "Ongoing");
    EXPECT_EQ(committed_1->state, "CompleteCommit");
    EXPECT_EQ(ongoing_2->state, "Ongoing");
    EXPECT_EQ(aborted_2->state, "CompleteAbort");
    // What the settle's proof rests on: a completed commit is described under
    // the producer id and at or above the epoch its transaction was described
    // Ongoing at, and a successor's init moves the epoch past the old one.
    for (const auto& [when, d] : seen) {
        EXPECT_EQ(d->producer_id, ongoing_1->producer_id)
            << when << ": only the successor's init re-initialises the transactional id";
    }
    EXPECT_GE(committed_1->producer_epoch, ongoing_1->producer_epoch);
    EXPECT_GE(aborted_2->producer_epoch, ongoing_2->producer_epoch);
    EXPECT_TRUE(fenced->producer_id != ongoing_3->producer_id ||
                fenced->producer_epoch > ongoing_3->producer_epoch)
        << "a successor's init must fence the old identity: " << text(fenced);
}

// --- the orphan raced at a reopening open, through the engine's own sink ---------
//
// Needs the fault points (compiled into test builds).
#if CLINK_FAULT_ENABLED
//
// The deterministic gate for the defect formal/ExactlyOnce.tla found once a
// worker declared lost could run on (mutant orphan_left_to_init). Two
// kafka_2pc sinks share one transactional id in this process, each with its
// own runtime context, and one receipt directory. The OLD incarnation seals
// checkpoint 2 and holds its commit at sink.before_commit; the walk's
// .unresolved marker for 2 is on disk; the NEW incarnation opens restored from
// checkpoint 1 and is held at sink.orphan_described, its describe having seen
// the orphan Ongoing. The test then picks which reaches the broker first.
//
// Restored from nothing instead (a restart before any checkpoint of the run
// was confirmed), the orphan is checkpoint 1, and the new incarnation carries
// the run's checkpoint-id floor its deploy would carry.
struct RaceRestore {
    std::uint64_t restore_point{1};
    std::optional<std::uint64_t> run_floor;
};

class OrphanRace {
public:
    OrphanRace(const std::string& topic, const std::string& txn_id, RaceRestore restore = {})
        : restore_(restore),
          topic_(topic),
          dir_(std::filesystem::temp_directory_path() /
               ("clink_orphan_race_" + std::to_string(::getpid()) + "_" + txn_id)),
          old_rctx_(clink::OperatorId{45}, "race_old", &old_state_, nullptr),
          new_rctx_(clink::OperatorId{46}, "race_new", &new_state_, nullptr) {
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_ / "trace");
        ::setenv("CLINK_PROTOCOL_TRACE_DIR", (dir_ / "trace").c_str(), 1);
        clink::protocol_trace::reset_for_tests();
        old_rctx_.set_commit_receipts(dir_.string(), /*restore_from_ckpt=*/0);
        new_rctx_.set_commit_receipts(dir_.string(), restore_.restore_point);
        if (restore_.run_floor.has_value()) {
            new_rctx_.set_run_checkpoint_floor(*restore_.run_floor);
        }
        old_ = build(txn_id, old_rctx_, clink::OperatorId{45});
        new_ = build(txn_id, new_rctx_, clink::OperatorId{46});
    }

    ~OrphanRace() {
        clink::fault::Registry::instance().reset();
        ::unsetenv("CLINK_PROTOCOL_TRACE_DIR");
        clink::protocol_trace::reset_for_tests();
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    OrphanRace(const OrphanRace&) = delete;
    OrphanRace& operator=(const OrphanRace&) = delete;
    OrphanRace(OrphanRace&&) = delete;
    OrphanRace& operator=(OrphanRace&&) = delete;

    // The orphaned checkpoint: the one after the restore point.
    [[nodiscard]] std::uint64_t orphan() const { return restore_.restore_point + 1; }

    // The old incarnation commits the restore point's checkpoint, if any,
    // then seals the orphan's, and the walk's marker for it carries its staged
    // handle. Returns the handle.
    std::string seal_the_orphan() {
        old_->open();
        if (restore_.restore_point > 0) {
            Batch<std::string> one;
            one.emplace(std::string{"a1"}, clink::EventTime{100});
            one.emplace(std::string{"a2"}, clink::EventTime{200});
            old_->on_data(one);
            old_->on_watermark(clink::Watermark{clink::EventTime{300}});
            old_->on_barrier(CheckpointBarrier{CheckpointId{1}});
            old_->on_commit(1);
        }
        old_->on_data(interval_two());
        old_->on_watermark(clink::Watermark{clink::EventTime{600}});
        old_->on_barrier(CheckpointBarrier{CheckpointId{orphan()}});
        const std::string key = std::string(clink::connectors::kTxnResumeStateKeyPrefix) + "sub0";
        const auto staged = old_state_.get_operator_state(
            clink::OperatorId{45}, clink::StateBackend::KeyView{key.data(), key.size()});
        if (!staged.has_value()) {
            return {};
        }
        std::string handle(reinterpret_cast<const char*>(staged->data()), staged->size());
        std::ofstream(marker_path()) << handle;
        return handle;
    }

    // The orphan's interval, as the restore's replay re-produces it.
    static Batch<std::string> interval_two() {
        Batch<std::string> two;
        two.emplace(std::string{"b1"}, clink::EventTime{400});
        two.emplace(std::string{"b2"}, clink::EventTime{500});
        return two;
    }

    // The new incarnation runs on after its open: the replay of the orphan's
    // interval, one record past it, and the next checkpoint committed.
    void replay_and_commit_three() {
        new_->on_data(interval_two());
        Batch<std::string> three;
        three.emplace(std::string{"c1"}, clink::EventTime{700});
        new_->on_data(three);
        new_->on_watermark(clink::Watermark{clink::EventTime{800}});
        new_->on_barrier(CheckpointBarrier{CheckpointId{orphan() + 1}});
        new_->on_commit(orphan() + 1);
        new_->close();
    }

    // What read_committed must see: the restore point's interval, if any,
    // then the orphan's and the record after it, each once.
    [[nodiscard]] std::vector<std::string> exactly_once() const {
        std::vector<std::string> out;
        if (restore_.restore_point > 0) {
            out = {"a1", "a2"};
        }
        out.insert(out.end(), {"b1", "b2", "c1"});
        return out;
    }

    [[nodiscard]] std::filesystem::path receipt_path(std::uint64_t ckpt) const {
        return dir_ / clink::connectors::commit_receipt_file_name(0, ckpt);
    }
    [[nodiscard]] std::filesystem::path marker_path() const {
        return dir_ / (clink::connectors::commit_receipt_file_name(0, orphan()) + ".unresolved");
    }

    // The new incarnation's SinkOpens line (the one carrying the settle).
    [[nodiscard]] std::string settled_open_line() const {
        std::string found;
        for (const auto& f : std::filesystem::directory_iterator(dir_ / "trace")) {
            std::ifstream in(f.path());
            std::string line;
            while (std::getline(in, line)) {
                if (line.find("\"event\":\"SinkOpens\"") != std::string::npos &&
                    line.find("\"orphan\"") != std::string::npos) {
                    found = line;
                }
            }
        }
        return found;
    }

    clink::Sink<std::string>& old_sink() { return *old_; }
    clink::Sink<std::string>& new_sink() { return *new_; }
    [[nodiscard]] bool built() const { return old_ != nullptr && new_ != nullptr; }

private:
    std::shared_ptr<clink::Sink<std::string>> build(const std::string& txn_id,
                                                    clink::RuntimeContext& rctx,
                                                    clink::OperatorId id) {
        auto& registry = clink::cluster::OperatorRegistry::default_instance();
        const auto* factory =
            registry.find_sink("kafka_2pc_sink_string", clink::cluster::ChannelType{"string"});
        if (factory == nullptr) {
            return nullptr;
        }
        clink::cluster::OperatorBuildContext ctx;
        ctx.params = {
            {"brokers", broker_->brokers()}, {"topic", topic_}, {"transactional_id", txn_id}};
        auto sink = std::static_pointer_cast<clink::Sink<std::string>>(factory->build(ctx));
        sink->set_id(id);
        sink->attach_runtime(&rctx);
        return sink;
    }

    RaceRestore restore_;
    std::string topic_;
    std::filesystem::path dir_;
    clink::InMemoryStateBackend old_state_;
    clink::InMemoryStateBackend new_state_;
    clink::RuntimeContext old_rctx_;
    clink::RuntimeContext new_rctx_;
    std::shared_ptr<clink::Sink<std::string>> old_;
    std::shared_ptr<clink::Sink<std::string>> new_;
};

// Parks the calling test until `point` has been reached `n` times (bounded).
bool wait_for_hits(std::string_view point, std::uint64_t n) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (clink::fault::Registry::instance().hits(point) < n) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return true;
}

void arm_race_points() {
    using clink::fault::Action;
    namespace points = clink::fault::points;
    auto& reg = clink::fault::Registry::instance();
    reg.reset();
    for (const auto* point : {points::kSinkBeforeCommit,
                              points::kSinkBetweenCommitAndReceipt,
                              points::kSinkOrphanDescribed}) {
        reg.arm(
            clink::fault::Rule{.point = point, .ordinal = 1, .action = Action::Block, .arg = 0});
    }
}

// Joins a race thread whatever the test's assertions did, first releasing
// every parked fault point so the thread can finish.
struct JoinOnExit {
    std::thread& thread;
    explicit JoinOnExit(std::thread& t) : thread(t) {}
    JoinOnExit(const JoinOnExit&) = delete;
    JoinOnExit& operator=(const JoinOnExit&) = delete;
    ~JoinOnExit() {
        if (thread.joinable()) {
            clink::fault::Registry::instance().reset();
            thread.join();
        }
    }
};

// Order A: the old incarnation's commit reaches the broker after the new
// open's describe saw the orphan Ongoing and before its abort, and its
// receipt is held back so only the new open can write one.
void commit_wins_during_the_open(const std::string& topic,
                                 const std::string& txn_id,
                                 RaceRestore restore) {
    OrphanRace race(topic, txn_id, restore);
    ASSERT_TRUE(race.built());
    const auto handle = race.seal_the_orphan();
    ASSERT_FALSE(handle.empty()) << "the barrier must stage a resume handle";
    ASSERT_EQ(handle.find("\"producer_id\":\"-1\""), std::string::npos) << handle;

    namespace points = clink::fault::points;
    auto& reg = clink::fault::Registry::instance();
    arm_race_points();
    std::thread old_commit([&] {
        try {
            race.old_sink().on_commit(race.orphan());
        } catch (const std::exception&) {
            // Fenced by the new incarnation's init once its receipt landed:
            // the commit itself executed, which is what this order is about.
        }
    });
    const JoinOnExit join_commit{old_commit};
    ASSERT_TRUE(wait_for_hits(points::kSinkBeforeCommit, 1));
    std::string open_error;
    std::thread new_open([&] {
        try {
            race.new_sink().open();
        } catch (const std::exception& e) {
            open_error = e.what();
        }
    });
    const JoinOnExit join_open{new_open};
    ASSERT_TRUE(wait_for_hits(points::kSinkOrphanDescribed, 1))
        << "the new open never described the orphan Ongoing";

    reg.release(points::kSinkBeforeCommit);
    ASSERT_TRUE(wait_for_hits(points::kSinkBetweenCommitAndReceipt, 1))
        << "the old incarnation's broker commit never executed";
    reg.release(points::kSinkOrphanDescribed);
    new_open.join();
    ASSERT_TRUE(open_error.empty()) << open_error;

    EXPECT_TRUE(std::filesystem::exists(race.receipt_path(race.orphan())))
        << "the open's abort was refused by the commit that won; it must prove the commit and "
           "write the receipt itself before init_transactions (the old incarnation is still "
           "parked before its own receipt)";
    EXPECT_FALSE(std::filesystem::exists(race.marker_path()));
    const auto line = race.settled_open_line();
    EXPECT_NE(line.find("\"orphan\":" + std::to_string(race.orphan())), std::string::npos) << line;
    EXPECT_NE(line.find("\"settled\":\"committed\""), std::string::npos) << line;

    reg.release(points::kSinkBetweenCommitAndReceipt);
    old_commit.join();
    race.old_sink().close();
    reg.reset();

    // Suppression armed from the orphan's receipt swallows the replay.
    race.replay_and_commit_three();
    EXPECT_EQ(
        clink::kafka::consume_all_committed(broker_->brokers(), topic, std::chrono::seconds(20)),
        race.exactly_once())
        << "the orphan's interval must be published exactly once: by the old incarnation's "
           "commit, with the replay suppressed";
}

TEST_F(TxnResumeLive, AnOrphanCommittedDuringTheOpenIsReceiptedBeforeTheFence) {
    commit_wins_during_the_open(topic_, "race-commit-wins", RaceRestore{});
}

// The same race on a restart that restores from nothing: no checkpoint of the
// run is confirmed yet, so the restore point is 0, and the deploy carries the
// run's checkpoint-id floor (0 for a run that began in an empty directory).
// The open used to take restore point 0 for a fresh start and skip both the
// settle and suppression (mutant open_skips_restore_zero).
TEST_F(TxnResumeLive, AnOrphanRacedAtARestartFromNothingIsSettledBeforeTheFence) {
    commit_wins_during_the_open(
        topic_, "race-restart-from-nothing", RaceRestore{.restore_point = 0, .run_floor = 0});
}

// A commit that executed without its receipt (a kill in the ack window), which
// the walk could not settle, and a restart that restores from nothing: no lost
// worker is needed. The reopening sink must find the commit, write its receipt
// and suppress the replay, or the replay from the start publishes the interval
// a second time.
TEST_F(TxnResumeLive, ACommitWithoutItsReceiptIsReceiptedAtARestartFromNothing) {
    OrphanRace race(topic_,
                    "receiptless-restart-from-nothing",
                    RaceRestore{.restore_point = 0, .run_floor = 0});
    ASSERT_TRUE(race.built());
    const auto handle = race.seal_the_orphan();
    ASSERT_FALSE(handle.empty()) << "the barrier must stage a resume handle";
    ASSERT_EQ(handle.find("\"producer_id\":\"-1\""), std::string::npos) << handle;

    namespace points = clink::fault::points;
    auto& reg = clink::fault::Registry::instance();
    reg.reset();
    reg.arm(clink::fault::Rule{.point = points::kSinkBetweenCommitAndReceipt,
                               .ordinal = 1,
                               .action = clink::fault::Action::Block,
                               .arg = 0});
    std::thread old_commit([&] {
        try {
            race.old_sink().on_commit(race.orphan());
        } catch (const std::exception&) {
            // Fenced once the new incarnation has opened; the commit executed.
        }
    });
    const JoinOnExit join_commit{old_commit};
    ASSERT_TRUE(wait_for_hits(points::kSinkBetweenCommitAndReceipt, 1))
        << "the broker commit never executed";

    // The restart from nothing reopens the sink while the commit has no receipt.
    std::string open_error;
    try {
        race.new_sink().open();
    } catch (const std::exception& e) {
        open_error = e.what();
    }
    EXPECT_TRUE(open_error.empty()) << open_error;
    EXPECT_TRUE(std::filesystem::exists(race.receipt_path(race.orphan())))
        << "the open restored from nothing left the committed orphan without a receipt";
    EXPECT_FALSE(std::filesystem::exists(race.marker_path()));
    const auto line = race.settled_open_line();
    EXPECT_NE(line.find("\"orphan\":1"), std::string::npos) << line;
    EXPECT_NE(line.find("\"settled\":\"committed\""), std::string::npos) << line;

    reg.release(points::kSinkBetweenCommitAndReceipt);
    old_commit.join();
    race.old_sink().close();
    reg.reset();

    race.replay_and_commit_three();
    EXPECT_EQ(
        clink::kafka::consume_all_committed(broker_->brokers(), topic_, std::chrono::seconds(20)),
        race.exactly_once())
        << "the committed interval was published again by the replay from the start";
}

TEST_F(TxnResumeLive, AnOrphanAbortedByTheOpenRefusesTheOldCommitAndIsReplayedOnce) {
    OrphanRace race(topic_, "race-abort-wins");
    ASSERT_TRUE(race.built());
    const auto handle = race.seal_the_orphan();
    ASSERT_FALSE(handle.empty()) << "the barrier must stage a resume handle";
    ASSERT_EQ(handle.find("\"producer_id\":\"-1\""), std::string::npos) << handle;

    namespace points = clink::fault::points;
    auto& reg = clink::fault::Registry::instance();
    arm_race_points();
    std::string old_error;
    std::thread old_commit([&] {
        try {
            race.old_sink().on_commit(2);
        } catch (const std::exception& e) {
            old_error = e.what();
        }
    });
    const JoinOnExit join_commit{old_commit};
    ASSERT_TRUE(wait_for_hits(points::kSinkBeforeCommit, 1));
    std::string open_error;
    std::thread new_open([&] {
        try {
            race.new_sink().open();
        } catch (const std::exception& e) {
            open_error = e.what();
        }
    });
    const JoinOnExit join_open{new_open};
    ASSERT_TRUE(wait_for_hits(points::kSinkOrphanDescribed, 1))
        << "the new open never described the orphan Ongoing";

    // Order B: the new open's abort reaches the broker first.
    reg.release(points::kSinkOrphanDescribed);
    new_open.join();
    ASSERT_TRUE(open_error.empty()) << open_error;
    EXPECT_FALSE(std::filesystem::exists(race.receipt_path(2)))
        << "the open ended the orphan; no receipt may claim its commit";
    EXPECT_FALSE(std::filesystem::exists(race.marker_path()));
    const auto line = race.settled_open_line();
    EXPECT_NE(line.find("\"orphan\":2"), std::string::npos) << line;
    EXPECT_NE(line.find("\"settled\":\"aborted\""), std::string::npos) << line;

    reg.release(points::kSinkBeforeCommit);
    old_commit.join();
    EXPECT_FALSE(old_error.empty()) << "the old incarnation's commit of an aborted, fenced "
                                       "transaction must fail, never read as executed";
    std::cout << "ORPHAN-RACE abort wins; the old incarnation's commit: " << old_error << "\n";
    EXPECT_EQ(reg.hits(points::kSinkBetweenCommitAndReceipt), 0U)
        << "the old incarnation reached its receipt: its commit executed after the abort";
    EXPECT_FALSE(std::filesystem::exists(race.receipt_path(2)));
    race.old_sink().close();
    reg.reset();

    // Nothing is armed from checkpoint 2, so the replay is its only copy.
    race.replay_and_commit_three();
    EXPECT_EQ(
        clink::kafka::consume_all_committed(broker_->brokers(), topic_, std::chrono::seconds(20)),
        (std::vector<std::string>{"a1", "a2", "b1", "b2", "c1"}))
        << "the orphan's interval must be published exactly once: by the replay";
}

#endif  // CLINK_FAULT_ENABLED

}  // namespace
