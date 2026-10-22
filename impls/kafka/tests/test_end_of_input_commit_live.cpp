// The transactional Kafka sink at the end of its input, against a real broker.
//
// A bounded job's last checkpoint seals a transaction at its barrier, and the
// coordinator's CommitCheckpoint for it reaches the sink's worker on another
// thread, possibly after the sink's input has already ended (the source that
// waits for that commit may be on another worker). The sink's runner holds
// the teardown open for a commit only while the sink reports one staged
// (Sink::staged_commits_outstanding). This sink used to report none, so the
// runner went straight to its teardown and the late commit was refused: safe,
// since the prepared transaction is left for in-doubt resolution, but the
// job's last interval then waited for a restore to publish it when the commit
// could simply have run.
//
// Each test drives the sink through its own runner with commits dispatched as
// the worker dispatches them (tests/test_helpers/sink_runner_harness.hpp).
// Skips without Docker, as the other live suites do.

#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/commit_dispatch_gate.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/kafka/consume_all.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#include "tests/integration/docker_kafka.hpp"
#include "tests/test_helpers/sanitizer_slack.hpp"
#include "tests/test_helpers/sink_runner_harness.hpp"

namespace {

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::StreamElement;
using clink::test_support::AfterInput;
using clink::test_support::scale_slack;
using Harness = clink::test_support::SinkRunnerHarness<std::string>;
using namespace std::chrono_literals;

std::unique_ptr<clink::test::DockerKafka> eoi_broker_;

class KafkaTxnEndOfInputLive : public ::testing::Test {
protected:
    static void TearDownTestSuite() { eoi_broker_.reset(); }

    void SetUp() override {
        if (!clink::test::DockerKafka::docker_available()) {
            GTEST_SKIP() << "Docker not available";
        }
        // Started by the first test, not in SetUpTestSuite: a throw there
        // makes gtest skip every test, which ctest counts as passing.
        if (eoi_broker_ == nullptr) {
            eoi_broker_ = std::make_unique<clink::test::DockerKafka>();
        }
        name_ = ::testing::UnitTest::GetInstance()->current_test_info()->name();
        topic_ = "clink_eoi_" + name_;
        eoi_broker_->create_topic(topic_);
    }

    // The transactional sink as a deployed job builds it.
    [[nodiscard]] std::shared_ptr<clink::Sink<std::string>> build_sink() const {
        const auto* factory = clink::cluster::OperatorRegistry::default_instance().find_sink(
            "kafka_2pc_sink_string", clink::cluster::ChannelType{"string"});
        if (factory == nullptr) {
            return nullptr;
        }
        clink::cluster::OperatorBuildContext ctx;
        ctx.params = {{"brokers", eoi_broker_->brokers()},
                      {"topic", topic_},
                      {"transactional_id", "eoi-" + name_}};
        return std::static_pointer_cast<clink::Sink<std::string>>(factory->build(ctx));
    }

    [[nodiscard]] std::vector<std::string> committed(std::chrono::milliseconds bound) const {
        return clink::kafka::consume_all_committed(eoi_broker_->brokers(), topic_, bound);
    }

    static StreamElement<std::string> rows(const std::vector<std::string>& values) {
        Batch<std::string> b;
        for (const auto& v : values) {
            b.emplace(v);
        }
        return StreamElement<std::string>::data(std::move(b));
    }

    std::string name_;
    std::string topic_;
};

}  // namespace

// The final checkpoint's commit arrives once the input has ended. The runner
// must still be holding the sink open for it, the commit must run rather than
// be refused, and the teardown follows it.
TEST_F(KafkaTxnEndOfInputLive, AFinalCommitArrivingAfterTheInputEndedIsCommittedNotRefused) {
    auto sink = build_sink();
    ASSERT_NE(sink, nullptr);
    ASSERT_TRUE(sink->stages_state_at_barrier());
    clink::InMemoryStateBackend state;
    Harness h(sink, &state);

    h.push(rows({"e1", "e2"}));
    h.push(StreamElement<std::string>::barrier(CheckpointBarrier{CheckpointId{1}}));
    h.end_input();
    h.start();

    EXPECT_EQ(h.await_end_of_input(scale_slack(60s)), AfterInput::Waiting)
        << "the runner began the sink's teardown while checkpoint 1's transaction was still "
           "waiting for its commit";
    // The coordinator's CommitCheckpoint for checkpoint 1, after the end of input.
    EXPECT_NO_THROW(h.commit(1)) << "the final checkpoint's commit was refused";

    ASSERT_TRUE(h.join(scale_slack(30s))) << "the runner did not tear down after the commit";
    EXPECT_FALSE(h.failure()) << "the runner failed instead of finishing cleanly";
    EXPECT_TRUE(h.dispatch_retired()) << "the teardown did not fence commit dispatch";
    EXPECT_TRUE(sink->runtime_detached()) << "the teardown did not detach the sink";
    EXPECT_THROW(h.commit(2), clink::cluster::CommitDispatchRefused);
    EXPECT_EQ(committed(20s), (std::vector<std::string>{"e1", "e2"}))
        << "the final interval is not published";
}

// An AbortCheckpoint for the final checkpoint resolves its transaction just as
// a commit does. The runner must let the teardown go at once: still waiting,
// it would hold the subtask for 30 s and then fail a healthy one.
TEST_F(KafkaTxnEndOfInputLive, AnAbortedFinalCheckpointDoesNotHoldTheTeardown) {
    auto sink = build_sink();
    ASSERT_NE(sink, nullptr);
    clink::InMemoryStateBackend state;
    Harness h(sink, &state);

    h.push(rows({"a1", "a2"}));
    h.push(StreamElement<std::string>::barrier(CheckpointBarrier{CheckpointId{1}}));
    h.end_input();
    h.start();

    EXPECT_EQ(h.await_end_of_input(scale_slack(60s)), AfterInput::Waiting);
    EXPECT_NO_THROW(h.abort(1)) << "the final checkpoint's abort was refused";

    // Well inside the 30 s the wait would otherwise run to.
    ASSERT_TRUE(h.join(scale_slack(10s)))
        << "the runner went on waiting after the final checkpoint was aborted";
    EXPECT_FALSE(h.failure()) << "the runner failed after an abort it should have accepted";
    EXPECT_TRUE(committed(5s).empty()) << "an aborted interval was published";
}

// A terminal barrier seals and commits the tail on the runner's own thread, so
// nothing is left staged for the end-of-stream wait to hold the sink open for.
TEST_F(KafkaTxnEndOfInputLive, ATerminalCheckpointCommittedLocallyDoesNotHoldTheTeardown) {
    auto sink = build_sink();
    ASSERT_NE(sink, nullptr);
    clink::InMemoryStateBackend state;
    Harness h(sink, &state);

    h.push(rows({"t1", "t2"}));
    h.push(StreamElement<std::string>::barrier(
        CheckpointBarrier{CheckpointId{std::numeric_limits<std::uint64_t>::max()},
                          /*terminal=*/true}));
    h.end_input();
    h.start();

    ASSERT_TRUE(h.join(scale_slack(15s)))
        << "the runner waited at the end of its input for a commit it had made itself";
    EXPECT_FALSE(h.failure()) << "the runner failed instead of finishing cleanly";
    EXPECT_EQ(committed(20s), (std::vector<std::string>{"t1", "t2"}));
}
