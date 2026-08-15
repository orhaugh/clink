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
    // then _exit()s without running a single destructor. That is the only
    // honest simulation - a graceful KafkaSink teardown politely ABORTS
    // the open transaction (librdkafka's producer shutdown does), so an
    // in-process "orphan" would quietly stop being one. The child's death
    // drops its sockets with no protocol goodbye, and the transaction
    // stays open broker-side, pinning the last stable offset - exactly the
    // state a killed worker leaves behind.
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

}  // namespace
