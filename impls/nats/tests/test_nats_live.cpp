// NATS JetStream LIVE integration test. SKIPPED unless CLINK_NATS_TEST_ENDPOINT is set (a NATS
// URL, e.g. nats://localhost:4222). Proves against a real JetStream-enabled server: the sink
// publishes (acks awaited) and the source's durable pull consumer reads the same messages back.
// The test creates the stream up front via the nats.c API (stream provisioning is normally an
// admin task, so the connectors do not do it).

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>
#include <nats/nats.h>

#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/nats/nats_sink.hpp"
#include "clink/nats/nats_source.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::Emitter;
using clink::StreamElement;
using clink::nats::NatsSink;
using clink::nats::NatsSource;

namespace {

bool nats_configured() {
    return std::getenv("CLINK_NATS_TEST_ENDPOINT") != nullptr;
}
std::string nats_url() {
    return std::getenv("CLINK_NATS_TEST_ENDPOINT");
}

std::string unique_name(const char* prefix) {
    static int n = 0;
    return std::string(prefix) + "_" + std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(n++);
}

// Create a JetStream stream over `subject`. Returns true on success.
bool create_stream(const std::string& stream, const std::string& subject) {
    natsConnection* nc = nullptr;
    if (natsConnection_ConnectTo(&nc, nats_url().c_str()) != NATS_OK) {
        return false;
    }
    jsCtx* js = nullptr;
    bool ok = (natsConnection_JetStream(&js, nc, nullptr) == NATS_OK);
    if (ok) {
        jsStreamConfig cfg;
        jsStreamConfig_Init(&cfg);
        cfg.Name = stream.c_str();
        const char* subjects[] = {subject.c_str()};
        cfg.Subjects = subjects;
        cfg.SubjectsLen = 1;
        ok = (js_AddStream(nullptr, js, &cfg, nullptr, nullptr) == NATS_OK);
    }
    jsCtx_Destroy(js);
    natsConnection_Destroy(nc);
    return ok;
}

}  // namespace

TEST(NatsLive, PublishThenConsumeRoundTrip) {
    if (!nats_configured()) {
        GTEST_SKIP() << "set CLINK_NATS_TEST_ENDPOINT (e.g. nats://localhost:4222)";
    }
    const std::string stream = unique_name("CLINK_IT");
    const std::string subject = "clink.it." + unique_name("s");
    ASSERT_TRUE(create_stream(stream, subject)) << "could not create JetStream stream";

    NatsSink::Options ko;
    ko.conn.url = nats_url();
    ko.subject = subject;
    NatsSink sink(std::move(ko));
    sink.open();

    Batch<std::string> out;
    out.emplace(R"({"id":1,"v":"a"})");
    out.emplace(R"({"id":2,"v":"b"})");
    out.emplace(R"({"id":3,"v":"c"})");
    sink.on_data(out);
    sink.flush();  // block until the server acks all three (throws on failure)

    NatsSource::Options so;
    so.conn.url = nats_url();
    so.subject = subject;
    so.stream = stream;
    so.durable = unique_name("d");
    so.fetch_timeout = std::chrono::milliseconds{500};
    NatsSource src(std::move(so));
    src.open();

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

// A publish the server refuses, here one to a subject no stream is bound to,
// has to fail the barrier flush: the message is in no stream, and only a
// failed checkpoint makes the job publish it again. nats.c reports a refusal
// only to an error handler; before the sink installed one, the flush
// succeeded and the message was gone.
TEST(NatsLive, ABarrierFlushFailsWhenTheServerRefusesAPublish) {
    if (!nats_configured()) {
        GTEST_SKIP() << "set CLINK_NATS_TEST_ENDPOINT (e.g. nats://localhost:4222)";
    }
    NatsSink::Options ko;
    ko.conn.url = nats_url();
    ko.subject = "clink.it.unbound." + unique_name("s");
    ko.publish_timeout = std::chrono::milliseconds{5000};
    NatsSink sink(std::move(ko));
    sink.open();

    Batch<std::string> out;
    out.emplace(R"({"id":1,"v":"a"})");
    sink.on_data(out);
    try {
        sink.on_barrier(CheckpointBarrier{CheckpointId{1}});
        ADD_FAILURE() << "the barrier flush succeeded, though no stream holds the message";
    } catch (const std::exception& e) {
        EXPECT_NE(std::string{e.what()}.find("refused 1 publish"), std::string::npos) << e.what();
    }
    sink.close();
}

// --- the source's acknowledgement contract ----------------------------------
//
// Judged by what another subscriber of the same durable consumer receives once
// the source has gone, never by the source's own counters. AckWait is 2 s here,
// so a message nobody acknowledges or extends comes back quickly.

namespace {

struct AckStream {
    AckStream()
        : stream(unique_name("CLINK_ACK")),
          subject("clink.ack." + unique_name("s")),
          durable(unique_name("d")) {
        ok = create_stream(stream, subject);
    }
    void publish(const std::string& tag, int n) const {
        NatsSink::Options ko;
        ko.conn.url = nats_url();
        ko.subject = subject;
        NatsSink sink(std::move(ko));
        sink.open();
        Batch<std::string> b;
        for (int i = 0; i < n; ++i) {
            b.emplace(tag + std::to_string(i));
        }
        sink.on_data(b);
        sink.flush();
        sink.close();
    }
    NatsSource::Options source() const {
        NatsSource::Options o;
        o.conn.url = nats_url();
        o.subject = subject;
        o.stream = stream;
        o.durable = durable;
        o.fetch_timeout = std::chrono::milliseconds{200};
        o.ack_wait = std::chrono::seconds{2};
        return o;
    }
    std::string stream;
    std::string subject;
    std::string durable;
    bool ok{false};
};

std::vector<std::string> tagged(const std::string& tag, int n) {
    std::vector<std::string> v;
    for (int i = 0; i < n; ++i) {
        v.push_back(tag + std::to_string(i));
    }
    return v;
}

std::set<std::string> set_of(std::vector<std::string> a, const std::vector<std::string>& b = {}) {
    a.insert(a.end(), b.begin(), b.end());
    return {a.begin(), a.end()};
}

// Produce until `want` records have arrived or `ms` has passed.
std::vector<std::string> drain(NatsSource& src, std::size_t want, int ms) {
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

// What the durable consumer delivers to another subscriber over `ms`: enough
// for an AckWait to pass, so nothing unacknowledged stays hidden.
std::set<std::string> left_in(const AckStream& st, int ms = 4500) {
    NatsSource judge(st.source());
    judge.open();
    const auto got = drain(judge, 1000000, ms);
    judge.close();
    return {got.begin(), got.end()};
}

}  // namespace

TEST(NatsSourceAckLive, AnAbortedCheckpointsMessagesAreDeliveredAgain) {
    if (!nats_configured()) {
        GTEST_SKIP() << "set CLINK_NATS_TEST_ENDPOINT";
    }
    AckStream st;
    ASSERT_TRUE(st.ok);
    clink::InMemoryStateBackend backend;
    st.publish("a", 6);
    {
        NatsSource src(st.source());
        src.open();
        ASSERT_EQ(drain(src, 6, 10000).size(), 6u);
        src.snapshot_offset(backend, clink::OperatorId{1}, CheckpointId{1});
        src.notify_checkpoint_aborted(CheckpointId{1});
        drain(src, 7, 300);
        src.close();
    }
    EXPECT_EQ(left_in(st), set_of(tagged("a", 6)));
}

TEST(NatsSourceAckLive, ACrashAfterCompletionBeforeTheAckDeliversAgain) {
    if (!nats_configured()) {
        GTEST_SKIP() << "set CLINK_NATS_TEST_ENDPOINT";
    }
    AckStream st;
    ASSERT_TRUE(st.ok);
    clink::InMemoryStateBackend backend;
    st.publish("c", 4);
    {
        NatsSource src(st.source());
        src.open();
        ASSERT_EQ(drain(src, 4, 10000).size(), 4u);
        src.snapshot_offset(backend, clink::OperatorId{1}, CheckpointId{1});
        src.notify_checkpoint_complete(CheckpointId{1});
        // Dies before a turn sends the ack: no produce, no close.
    }
    EXPECT_EQ(left_in(st), set_of(tagged("c", 4))) << "duplicates, not a loss";
}

TEST(NatsSourceAckLive, EachCompletionAcknowledgesOnlyTheIntervalsItCovers) {
    if (!nats_configured()) {
        GTEST_SKIP() << "set CLINK_NATS_TEST_ENDPOINT";
    }
    AckStream st;
    ASSERT_TRUE(st.ok);
    clink::InMemoryStateBackend backend;
    {
        NatsSource src(st.source());
        src.open();
        st.publish("x", 3);
        ASSERT_EQ(drain(src, 3, 10000).size(), 3u);
        src.snapshot_offset(backend, clink::OperatorId{1}, CheckpointId{1});
        st.publish("y", 2);
        ASSERT_EQ(drain(src, 2, 10000).size(), 2u);
        src.snapshot_offset(backend, clink::OperatorId{1}, CheckpointId{2});
        st.publish("z", 1);
        ASSERT_EQ(drain(src, 1, 10000).size(), 1u);  // after the last barrier
        src.notify_checkpoint_complete(CheckpointId{2});
        src.notify_checkpoint_complete(CheckpointId{1});  // late
        src.notify_checkpoint_complete(CheckpointId{2});  // repeated
        src.notify_checkpoint_aborted(CheckpointId{1});   // late and contradictory
        src.notify_checkpoint_complete(CheckpointId{9});  // above anything sealed
        drain(src, 1, 300);
        // Dies here: only the open interval's message is unacknowledged.
    }
    EXPECT_EQ(left_in(st), set_of(tagged("z", 1)));
}

TEST(NatsSourceAckLive, CloseAcknowledgesCompletedIntervalsOnly) {
    if (!nats_configured()) {
        GTEST_SKIP() << "set CLINK_NATS_TEST_ENDPOINT";
    }
    AckStream st;
    ASSERT_TRUE(st.ok);
    clink::InMemoryStateBackend backend;
    {
        NatsSource src(st.source());
        src.open();
        st.publish("f", 2);
        ASSERT_EQ(drain(src, 2, 10000).size(), 2u);
        src.snapshot_offset(backend, clink::OperatorId{1}, CheckpointId{1});
        st.publish("g", 2);
        ASSERT_EQ(drain(src, 2, 10000).size(), 2u);
        src.snapshot_offset(backend, clink::OperatorId{1}, CheckpointId{2});  // never completes
        st.publish("h", 1);
        ASSERT_EQ(drain(src, 1, 10000).size(), 1u);
        // Checkpoint 1 completes as the job is cancelled: no produce turn
        // follows, so only close() can acknowledge it.
        src.notify_checkpoint_complete(CheckpointId{1});
        src.cancel();
        src.close();
    }
    EXPECT_EQ(left_in(st), set_of(tagged("g", 2), tagged("h", 1)))
        << "close acknowledged what no completed checkpoint covers, or not what one does";
}

TEST(NatsSourceAckLive, MaxAckPendingBoundsWhatIsHeldUntilACheckpointCompletes) {
    if (!nats_configured()) {
        GTEST_SKIP() << "set CLINK_NATS_TEST_ENDPOINT";
    }
    AckStream st;
    ASSERT_TRUE(st.ok);
    clink::InMemoryStateBackend backend;
    auto o = st.source();
    o.max_ack_pending = 5;
    NatsSource src(std::move(o));
    src.open();
    st.publish("p", 12);
    EXPECT_EQ(drain(src, 12, 1500).size(), 5u) << "the server delivered past max_ack_pending";
    src.snapshot_offset(backend, clink::OperatorId{1}, CheckpointId{1});
    src.notify_checkpoint_complete(CheckpointId{1});
    EXPECT_EQ(drain(src, 12, 1500).size(), 5u) << "reading did not resume after the completion";
    src.close();
}

// The checkpoint takes three times AckWait. The source keeps its messages in
// progress, so neither another subscriber nor the source itself receives them
// again, and the completion acknowledges each once.
TEST(NatsSourceAckLive, MessagesStayInProgressWhileACheckpointIsDelayed) {
    if (!nats_configured()) {
        GTEST_SKIP() << "set CLINK_NATS_TEST_ENDPOINT";
    }
    AckStream st;
    ASSERT_TRUE(st.ok);
    clink::InMemoryStateBackend backend;
    st.publish("l", 3);
    NatsSource src(st.source());
    src.open();
    ASSERT_EQ(drain(src, 3, 10000).size(), 3u);
    src.snapshot_offset(backend, clink::OperatorId{1}, CheckpointId{1});
    NatsSource rival(st.source());
    rival.open();
    std::vector<std::string> again;
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds{6};
    while (std::chrono::steady_clock::now() < until) {
        for (const auto& r : drain(src, 1000, 200)) {
            again.push_back(r);
        }
        for (const auto& r : drain(rival, 1000, 200)) {
            again.push_back(r);
        }
    }
    EXPECT_TRUE(again.empty()) << again.size() << " deliveries again while the checkpoint was out";
    src.notify_checkpoint_complete(CheckpointId{1});
    drain(src, 1, 300);
    src.close();
    rival.close();
    EXPECT_TRUE(left_in(st).empty()) << "a completed checkpoint's messages came back";
}
