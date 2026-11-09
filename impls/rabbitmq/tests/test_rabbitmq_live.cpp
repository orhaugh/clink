// RabbitMQ LIVE integration test. SKIPPED unless CLINK_RABBITMQ_TEST_ENDPOINT is set (the
// broker host, e.g. localhost); the live gate, scripts/brokers-live.sh, sets it and fails on
// any skip. Port/user/password come from CLINK_RABBITMQ_TEST_{PORT,USER,PASSWORD} (defaults
// 5672 / guest / guest). Proves against a real broker: the sink publishes (with publisher
// confirms) and the source consumes the same messages back; and the source's acknowledgement
// contract (RabbitMqSourceAckLive), judged by what a fresh consumer of the queue receives once
// the source has gone, never by the source's own counters.

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/rabbitmq/rabbitmq_sink.hpp"
#include "clink/rabbitmq/rabbitmq_source.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using clink::Batch;
using clink::Emitter;
using clink::StreamElement;
using clink::rabbitmq::RabbitMqConnParams;
using clink::rabbitmq::RabbitMqSink;
using clink::rabbitmq::RabbitMqSource;

namespace {

bool rmq_configured() {
    return std::getenv("CLINK_RABBITMQ_TEST_ENDPOINT") != nullptr;
}

std::string env_or(const char* k, const std::string& dflt) {
    const char* v = std::getenv(k);
    return v != nullptr ? std::string(v) : dflt;
}

RabbitMqConnParams conn_from_env() {
    RabbitMqConnParams c;
    c.host = std::getenv("CLINK_RABBITMQ_TEST_ENDPOINT");
    c.port = std::stoi(env_or("CLINK_RABBITMQ_TEST_PORT", "5672"));
    c.user = env_or("CLINK_RABBITMQ_TEST_USER", "guest");
    c.password = env_or("CLINK_RABBITMQ_TEST_PASSWORD", "guest");
    return c;
}

std::string unique_queue() {
    static int n = 0;
    return "clink_it_" + std::to_string(static_cast<long>(::getpid())) + "_" + std::to_string(n++);
}

}  // namespace

TEST(RabbitMqLive, PublishThenConsumeRoundTrip) {
    if (!rmq_configured()) {
        GTEST_SKIP() << "set CLINK_RABBITMQ_TEST_ENDPOINT (+ _PORT/_USER/_PASSWORD)";
    }
    const std::string queue = unique_queue();

    // Open the source first: it declares the (durable) queue and registers the consumer, so the
    // messages the sink publishes next are routed to a queue that already has a consumer.
    RabbitMqSource::Options so;
    so.conn = conn_from_env();
    so.queue = queue;
    so.poll_timeout = std::chrono::milliseconds{200};
    RabbitMqSource src(std::move(so));
    src.open();

    // Default exchange ("") routes by routing_key == queue name.
    RabbitMqSink::Options ko;
    ko.conn = conn_from_env();
    ko.routing_key = queue;
    RabbitMqSink sink(std::move(ko));
    sink.open();

    Batch<std::string> out;
    out.emplace(R"({"id":1,"v":"a"})");
    out.emplace(R"({"id":2,"v":"b"})");
    out.emplace(R"({"id":3,"v":"c"})");
    sink.on_data(out);
    sink.flush();  // block until the broker confirms all three (throws on failure)

    std::vector<std::string> got;
    Emitter<std::string> em([&](StreamElement<std::string> e) -> bool {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                got.push_back(rec.value());
            }
        }
        return true;
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
    while (got.size() < 3 && std::chrono::steady_clock::now() < deadline) {
        src.produce(em);
    }

    ASSERT_EQ(got.size(), 3u) << "did not consume all published messages";
    auto has = [&](const std::string& needle) {
        return std::any_of(got.begin(), got.end(), [&](const std::string& s) {
            return s.find(needle) != std::string::npos;
        });
    };
    EXPECT_TRUE(has("\"id\":1"));
    EXPECT_TRUE(has("\"id\":2"));
    EXPECT_TRUE(has("\"id\":3"));

    sink.close();
    src.close();
}

// A message no queue is bound to receive has to fail the barrier flush. The
// sink used to publish with mandatory unset, so the exchange dropped such a
// message and the broker confirmed it anyway: the flush succeeded and the
// message was in no queue. Mandatory publishes come back as a basic.return,
// and the flush fails.
TEST(RabbitMqLive, ABarrierFlushFailsForAMessageNoQueueReceives) {
    if (!rmq_configured()) {
        GTEST_SKIP() << "set CLINK_RABBITMQ_TEST_ENDPOINT (+ _PORT/_USER/_PASSWORD)";
    }
    // The default exchange routes by queue name, and no queue has this one.
    RabbitMqSink::Options ko;
    ko.conn = conn_from_env();
    ko.routing_key = unique_queue() + "_unbound";
    ko.confirm_timeout = std::chrono::milliseconds{5000};
    RabbitMqSink sink(std::move(ko));
    sink.open();

    Batch<std::string> out;
    out.emplace(R"({"id":1,"v":"a"})");
    sink.on_data(out);
    try {
        sink.on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
        ADD_FAILURE() << "the barrier flush succeeded, though no queue received the message";
    } catch (const std::exception& e) {
        EXPECT_NE(std::string{e.what()}.find("no queue is bound"), std::string::npos) << e.what();
    }
}

// mandatory='false' keeps the exchange's own behaviour, for a topology that
// publishes where nothing may be listening: the message is dropped, and the
// flush succeeds.
TEST(RabbitMqLive, WithoutMandatoryAnUnroutableMessageIsDroppedQuietly) {
    if (!rmq_configured()) {
        GTEST_SKIP() << "set CLINK_RABBITMQ_TEST_ENDPOINT (+ _PORT/_USER/_PASSWORD)";
    }
    RabbitMqSink::Options ko;
    ko.conn = conn_from_env();
    ko.routing_key = unique_queue() + "_unbound";
    ko.mandatory = false;
    RabbitMqSink sink(std::move(ko));
    sink.open();

    Batch<std::string> out;
    out.emplace(R"({"id":1,"v":"a"})");
    sink.on_data(out);
    EXPECT_NO_THROW(sink.on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}}));
    sink.close();
}

// --- the source's acknowledgement contract ----------------------------------

namespace {

void publish(const std::string& queue, const std::string& tag, int n) {
    RabbitMqSink::Options ko;
    ko.conn = conn_from_env();
    ko.routing_key = queue;
    RabbitMqSink sink(std::move(ko));
    sink.open();
    Batch<std::string> b;
    for (int i = 0; i < n; ++i) {
        b.emplace(tag + std::to_string(i));
    }
    sink.on_data(b);
    sink.flush();
    sink.close();
}

std::vector<std::string> tagged(const std::string& tag, int n) {
    std::vector<std::string> v;
    for (int i = 0; i < n; ++i) {
        v.push_back(tag + std::to_string(i));
    }
    return v;
}

RabbitMqSource::Options source_opts(const std::string& queue) {
    RabbitMqSource::Options o;
    o.conn = conn_from_env();
    o.queue = queue;
    o.poll_timeout = std::chrono::milliseconds{100};
    return o;
}

// Produce until `want` records have arrived or `ms` has passed.
std::vector<std::string> drain(RabbitMqSource& src, std::size_t want, int ms) {
    std::vector<std::string> got;
    Emitter<std::string> em([&](StreamElement<std::string> e) -> bool {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                got.push_back(rec.value());
            }
        }
        return true;
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{ms};
    while (got.size() < want && std::chrono::steady_clock::now() < deadline) {
        src.produce(em);
    }
    return got;
}

// What the queue holds for a consumer that comes after: a fresh consumer
// drains it for a while, acknowledging nothing.
std::set<std::string> left_in(const std::string& queue) {
    RabbitMqSource judge(source_opts(queue));
    judge.open();
    const auto got = drain(judge, 1000000, 1500);
    judge.close();
    return {got.begin(), got.end()};
}

std::set<std::string> set_of(std::vector<std::string> a, const std::vector<std::string>& b = {}) {
    a.insert(a.end(), b.begin(), b.end());
    return {a.begin(), a.end()};
}

}  // namespace

TEST(RabbitMqSourceAckLive, AnAbortedCheckpointsMessagesAreDeliveredAgain) {
    if (!rmq_configured()) {
        GTEST_SKIP() << "set CLINK_RABBITMQ_TEST_ENDPOINT";
    }
    const auto queue = unique_queue();
    clink::InMemoryStateBackend backend;
    {
        RabbitMqSource src(source_opts(queue));
        src.open();
        publish(queue, "a", 6);
        ASSERT_EQ(drain(src, 6, 10000).size(), 6u);
        src.snapshot_offset(backend, clink::OperatorId{1}, clink::CheckpointId{1});
        src.notify_checkpoint_aborted(clink::CheckpointId{1});
        drain(src, 1, 300);  // turns in which an ack would be sent
        src.close();
    }
    EXPECT_EQ(left_in(queue), set_of(tagged("a", 6)));
}

TEST(RabbitMqSourceAckLive, ACrashAfterCompletionBeforeTheAckDeliversAgain) {
    if (!rmq_configured()) {
        GTEST_SKIP() << "set CLINK_RABBITMQ_TEST_ENDPOINT";
    }
    const auto queue = unique_queue();
    clink::InMemoryStateBackend backend;
    {
        RabbitMqSource src(source_opts(queue));
        src.open();
        publish(queue, "c", 4);
        ASSERT_EQ(drain(src, 4, 10000).size(), 4u);
        src.snapshot_offset(backend, clink::OperatorId{1}, clink::CheckpointId{1});
        src.notify_checkpoint_complete(clink::CheckpointId{1});
        // Dies before a turn sends the ack: no produce, no close.
    }
    EXPECT_EQ(left_in(queue), set_of(tagged("c", 4))) << "duplicates, not a loss";
}

TEST(RabbitMqSourceAckLive, EachCompletionAcknowledgesOnlyTheIntervalsItCovers) {
    if (!rmq_configured()) {
        GTEST_SKIP() << "set CLINK_RABBITMQ_TEST_ENDPOINT";
    }
    const auto first = unique_queue();
    const auto second = unique_queue();
    clink::InMemoryStateBackend backend;
    for (const auto& queue : {first, second}) {
        RabbitMqSource src(source_opts(queue));
        src.open();
        publish(queue, "x", 3);
        ASSERT_EQ(drain(src, 3, 10000).size(), 3u);
        src.snapshot_offset(backend, clink::OperatorId{1}, clink::CheckpointId{1});
        publish(queue, "y", 2);
        ASSERT_EQ(drain(src, 2, 10000).size(), 2u);
        src.snapshot_offset(backend, clink::OperatorId{1}, clink::CheckpointId{2});
        publish(queue, "z", 1);
        ASSERT_EQ(drain(src, 1, 10000).size(), 1u);  // after the last barrier
        src.notify_checkpoint_complete(clink::CheckpointId{1});
        if (queue == second) {
            src.notify_checkpoint_complete(clink::CheckpointId{2});
            src.notify_checkpoint_complete(clink::CheckpointId{1});  // late
            src.notify_checkpoint_complete(clink::CheckpointId{2});  // repeated
            src.notify_checkpoint_aborted(clink::CheckpointId{1});   // late and contradictory
            src.notify_checkpoint_complete(clink::CheckpointId{9});  // above anything sealed
        }
        drain(src, 1, 300);
        // Dies here, so what was not acknowledged goes back to the queue.
    }
    EXPECT_EQ(left_in(first), set_of(tagged("y", 2), tagged("z", 1)))
        << "checkpoint 1's completion reached past its interval";
    EXPECT_EQ(left_in(second), set_of(tagged("z", 1)))
        << "only the open interval's message stays unacknowledged";
}

// Checkpoint 1 aborts and checkpoint 2 completes in the same run. The ack of
// checkpoint 2's messages must not take checkpoint 1's with it, as a single
// cumulative basic.ack(multiple) up to checkpoint 2's last tag would.
TEST(RabbitMqSourceAckLive, AnAbortedIntervalIsNotAcknowledgedByALaterCompletion) {
    if (!rmq_configured()) {
        GTEST_SKIP() << "set CLINK_RABBITMQ_TEST_ENDPOINT";
    }
    const auto queue = unique_queue();
    clink::InMemoryStateBackend backend;
    {
        RabbitMqSource src(source_opts(queue));
        src.open();
        publish(queue, "a", 2);
        ASSERT_EQ(drain(src, 2, 10000).size(), 2u);
        src.snapshot_offset(backend, clink::OperatorId{1}, clink::CheckpointId{1});
        publish(queue, "b", 2);
        ASSERT_EQ(drain(src, 2, 10000).size(), 2u);
        src.snapshot_offset(backend, clink::OperatorId{1}, clink::CheckpointId{2});
        src.notify_checkpoint_aborted(clink::CheckpointId{1});
        src.notify_checkpoint_complete(clink::CheckpointId{2});
        drain(src, 1, 300);
    }
    EXPECT_EQ(left_in(queue), set_of(tagged("a", 2)));
}

TEST(RabbitMqSourceAckLive, CloseAcknowledgesCompletedIntervalsOnly) {
    if (!rmq_configured()) {
        GTEST_SKIP() << "set CLINK_RABBITMQ_TEST_ENDPOINT";
    }
    const auto queue = unique_queue();
    clink::InMemoryStateBackend backend;
    {
        RabbitMqSource src(source_opts(queue));
        src.open();
        publish(queue, "f", 2);
        ASSERT_EQ(drain(src, 2, 10000).size(), 2u);
        src.snapshot_offset(backend, clink::OperatorId{1}, clink::CheckpointId{1});
        publish(queue, "g", 2);
        ASSERT_EQ(drain(src, 2, 10000).size(), 2u);
        src.snapshot_offset(
            backend, clink::OperatorId{1}, clink::CheckpointId{2});  // never completes
        publish(queue, "h", 1);
        ASSERT_EQ(drain(src, 1, 10000).size(), 1u);
        // Checkpoint 1 completes as the job is cancelled: no produce turn
        // follows, so only close() can acknowledge it.
        src.notify_checkpoint_complete(clink::CheckpointId{1});
        src.cancel();
        src.close();
    }
    EXPECT_EQ(left_in(queue), set_of(tagged("g", 2), tagged("h", 1)))
        << "close acknowledged what no completed checkpoint covers, or not what one does";
}

// The broker hands the consumer at most `prefetch` unacknowledged messages,
// and the source acknowledges only at checkpoint completion: so what it holds
// is bounded by prefetch, and reading resumes once a checkpoint completes.
TEST(RabbitMqSourceAckLive, ThePrefetchBoundsWhatIsHeldUntilACheckpointCompletes) {
    if (!rmq_configured()) {
        GTEST_SKIP() << "set CLINK_RABBITMQ_TEST_ENDPOINT";
    }
    const auto queue = unique_queue();
    clink::InMemoryStateBackend backend;
    auto o = source_opts(queue);
    o.prefetch = 5;
    RabbitMqSource src(std::move(o));
    src.open();
    publish(queue, "p", 12);
    auto got = drain(src, 12, 1500);
    EXPECT_EQ(got.size(), 5u) << "the broker delivered past the prefetch";
    src.snapshot_offset(backend, clink::OperatorId{1}, clink::CheckpointId{1});
    src.notify_checkpoint_complete(clink::CheckpointId{1});
    const auto more = drain(src, 12, 1500);
    EXPECT_EQ(more.size(), 5u) << "reading did not resume after the completion";
    src.close();
}

// The default prefetch, measured: with nothing acknowledged before a checkpoint
// completes, it is the most a subtask reads per checkpoint.
TEST(RabbitMqSourceAckLive, TheDefaultPrefetchIsWhatOneCheckpointCanRead) {
    if (!rmq_configured()) {
        GTEST_SKIP() << "set CLINK_RABBITMQ_TEST_ENDPOINT";
    }
    const auto queue = unique_queue();
    const RabbitMqSource::Options defaults;
    RabbitMqSource src(source_opts(queue));
    src.open();
    const int published = static_cast<int>(defaults.prefetch) + 500;
    publish(queue, "d", published);
    const auto got = drain(src, static_cast<std::size_t>(published), 3000);
    EXPECT_EQ(got.size(), static_cast<std::size_t>(defaults.prefetch));
    src.close();
}
