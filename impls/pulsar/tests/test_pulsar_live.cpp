// Apache Pulsar LIVE integration test. SKIPPED unless CLINK_PULSAR_TEST_ENDPOINT is set (a
// Pulsar service URL, e.g. pulsar://localhost:6650). Proves against a real broker: the sink
// publishes (acks awaited) and the source consumes the same messages back. The topic is
// auto-created by the broker (allowAutoTopicCreation), so no provisioning is needed; the source
// subscribes BEFORE the sink publishes so the subscription cursor covers the messages.

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/pulsar/pulsar_sink.hpp"
#include "clink/pulsar/pulsar_source.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using clink::Batch;
using clink::Emitter;
using clink::StreamElement;
using clink::pulsar::PulsarSink;
using clink::pulsar::PulsarSource;

namespace {

bool pulsar_configured() {
    return std::getenv("CLINK_PULSAR_TEST_ENDPOINT") != nullptr;
}
std::string pulsar_url() {
    return std::getenv("CLINK_PULSAR_TEST_ENDPOINT");
}

std::string unique_suffix() {
    static int n = 0;
    return std::to_string(static_cast<long>(::getpid())) + "_" + std::to_string(n++);
}

}  // namespace

TEST(PulsarLive, PublishThenConsumeRoundTrip) {
    if (!pulsar_configured()) {
        GTEST_SKIP() << "set CLINK_PULSAR_TEST_ENDPOINT (e.g. pulsar://localhost:6650)";
    }
    const std::string topic = "clink-it-" + unique_suffix();

    // Subscribe first so the (durable) subscription cursor is positioned before the messages the
    // sink publishes next; the broker auto-creates the topic on subscribe.
    PulsarSource::Options so;
    so.conn.service_url = pulsar_url();
    so.topic = topic;
    so.subscription = "clink-it-sub-" + unique_suffix();
    so.receive_timeout = std::chrono::milliseconds{500};
    PulsarSource src(std::move(so));
    src.open();

    PulsarSink::Options ko;
    ko.conn.service_url = pulsar_url();
    ko.topic = topic;
    PulsarSink sink(std::move(ko));
    sink.open();

    Batch<std::string> out;
    out.emplace(R"({"id":1,"v":"a"})");
    out.emplace(R"({"id":2,"v":"b"})");
    out.emplace(R"({"id":3,"v":"c"})");
    sink.on_data(out);
    sink.flush();  // block until the broker persists all three (throws on failure)

    std::vector<std::string> got;
    Emitter<std::string> em([&](StreamElement<std::string> e) -> bool {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                got.push_back(rec.value());
            }
        }
        return true;
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{15};
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

// An exclusive subscription replays a restore in topic order from the checkpointed position.
// The run checkpoints after thirty messages, acknowledges that checkpoint, goes on to fifty and
// acknowledges a second checkpoint covering them, then is restored from the FIRST checkpoint, as
// a restore from an older point (a savepoint, a confirmed checkpoint behind the newest) would be.
// The restored source must emit exactly the thirty-first message onwards, in order: nothing the
// first checkpoint covered, nothing missing although the second checkpoint acknowledged it, no
// copy. A shared subscription would leave the acknowledged twenty with the broker for good.
TEST(PulsarLive, AnExclusiveSubscriptionReplaysInOrderFromItsCheckpointedPosition) {
    if (!pulsar_configured()) {
        GTEST_SKIP() << "set CLINK_PULSAR_TEST_ENDPOINT (e.g. pulsar://localhost:6650)";
    }
    const std::string topic = "clink-it-seek-" + unique_suffix();
    const std::string subscription = "clink-it-seek-sub-" + unique_suffix();
    const clink::OperatorId op{7};
    auto options = [&] {
        PulsarSource::Options so;
        so.conn.service_url = pulsar_url();
        so.topic = topic;
        so.subscription = subscription;
        so.subscription_type = "exclusive";
        so.receive_timeout = std::chrono::milliseconds{500};
        so.max_batch_size = 1;  // a message a turn, so a barrier falls after exactly the n-th
        return so;
    };
    std::vector<std::string> got;
    Emitter<std::string> em([&](StreamElement<std::string> e) -> bool {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                got.push_back(rec.value());
            }
        }
        return true;
    });
    auto produce_until = [&](PulsarSource& src, std::size_t n) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{15};
        while (got.size() < n && std::chrono::steady_clock::now() < deadline) {
            src.produce(em);
        }
    };
    auto name = [](int i) { return std::string("m") + (i < 10 ? "0" : "") + std::to_string(i); };

    clink::Snapshot first;
    {
        auto backend = std::make_shared<clink::InMemoryStateBackend>();
        PulsarSource src(options());
        src.open();  // subscribes before the messages are published

        PulsarSink::Options ko;
        ko.conn.service_url = pulsar_url();
        ko.topic = topic;
        PulsarSink sink(std::move(ko));
        sink.open();
        Batch<std::string> out;
        for (int i = 0; i < 60; ++i) {
            out.emplace(name(i));
        }
        sink.on_data(out);
        sink.flush();
        sink.close();

        produce_until(src, 30);
        ASSERT_EQ(got.size(), 30u);
        src.snapshot_offset(*backend, op, clink::CheckpointId{1});
        first = backend->snapshot(clink::CheckpointId{1});
        src.notify_checkpoint_complete(clink::CheckpointId{1});
        produce_until(src, 50);
        ASSERT_EQ(got.size(), 50u);
        src.snapshot_offset(*backend, op, clink::CheckpointId{2});
        src.notify_checkpoint_complete(clink::CheckpointId{2});
        // One more turn issues the second checkpoint's acknowledgements.
        src.produce(em);
        src.close();
    }
    std::vector<std::string> before(got.begin(), got.begin() + 30);
    for (int i = 0; i < 30; ++i) {
        ASSERT_EQ(before[static_cast<std::size_t>(i)], name(i)) << "the first run's order";
    }

    got.clear();
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    backend->restore(first);
    PulsarSource restored(options());
    EXPECT_TRUE(restored.restore_offset(*backend, op)) << "the checkpoint recorded no position";
    restored.open();
    produce_until(restored, 30);
    // Nothing more may arrive: a copy of anything would show up here.
    for (int i = 0; i < 3; ++i) {
        restored.produce(em);
    }
    std::vector<std::string> want;
    for (int i = 30; i < 60; ++i) {
        want.push_back(name(i));
    }
    EXPECT_EQ(got, want) << "the restore did not replay the topic in order from the checkpoint";
    restored.close();
}
