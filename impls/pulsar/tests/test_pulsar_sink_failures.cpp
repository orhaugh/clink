// The Pulsar sink's failure paths against a scripted broker: a failed send,
// answers that do not come and a publish the client refuses, each followed by
// another record, barrier and flush; reopening; a cancel during a wait and
// before a publish; close(); and the count and byte windows, which push what
// the producer batches before they wait.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/pulsar/pulsar_publisher.hpp"
#include "clink/pulsar/pulsar_sink.hpp"
#include "clink/runtime/runtime_context.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::pulsar::PulsarAnswers;
using clink::pulsar::PulsarPublisher;
using clink::pulsar::PulsarPublisherSettings;
using clink::pulsar::PulsarSink;

namespace {

// What the stand-in broker knows and does, shared by every connection a sink
// opens to it. By default it persists each message, answering when the sink
// next waits.
struct FakeBroker {
    std::mutex mu;
    std::vector<std::string> published;
    std::vector<PulsarPublisherSettings> connects;
    std::size_t pushes{0};
    // The nth publish's answer (1-based): an empty string persists it, text
    // fails it with that result.
    std::function<std::string(std::uint64_t n)> failure;
    std::atomic<bool> silent{false};  // answers withheld
    std::uint64_t publish_fails_at{0};
    bool connect_fails{false};
    // The most messages, and bytes, that were unanswered when a publish came.
    std::uint64_t most_unanswered{0};
    std::uint64_t most_unanswered_bytes{0};

    std::size_t published_count() {
        const std::lock_guard<std::mutex> lock(mu);
        return published.size();
    }
};

class FakePublisher final : public PulsarPublisher {
public:
    explicit FakePublisher(std::shared_ptr<FakeBroker> b) : b_(std::move(b)) {}

    void publish(std::string_view body) override {
        const std::uint64_t n = ++sent_;
        if (b_->publish_fails_at == n) {
            throw std::runtime_error("pulsar_message_create failed");
        }
        const std::lock_guard<std::mutex> lock(b_->mu);
        b_->most_unanswered = std::max<std::uint64_t>(b_->most_unanswered, queue_.size());
        std::uint64_t bytes = 0;
        for (const auto& q : queue_) {
            bytes += q.bytes;
        }
        b_->most_unanswered_bytes = std::max(b_->most_unanswered_bytes, bytes);
        b_->published.emplace_back(body);
        queue_.push_back(Pending{body.size(), b_->failure ? b_->failure(n) : std::string{}});
    }

    void push() override {
        const std::lock_guard<std::mutex> lock(b_->mu);
        ++b_->pushes;
    }

    PulsarAnswers wait(std::uint64_t /*seen*/, std::chrono::milliseconds timeout) override {
        {
            const std::lock_guard<std::mutex> lock(b_->mu);
            if (!b_->silent.load() && !queue_.empty()) {
                for (const auto& q : queue_) {
                    answers_.answered_bytes += q.bytes;
                    if (q.failure.empty()) {
                        ++answers_.stored;
                    } else if (answers_.failed++ == 0) {
                        answers_.first_failure = q.failure;
                    }
                }
                queue_.clear();
                return answers_;
            }
        }
        std::this_thread::sleep_for(timeout);
        return answers_;
    }

    [[nodiscard]] PulsarAnswers answers() const override { return answers_; }

    void close() noexcept override {}

private:
    struct Pending {
        std::size_t bytes;
        std::string failure;
    };
    std::shared_ptr<FakeBroker> b_;
    std::uint64_t sent_{0};
    std::deque<Pending> queue_;
    PulsarAnswers answers_;
};

PulsarSink::Options opts_for(const std::shared_ptr<FakeBroker>& b) {
    PulsarSink::Options o;
    o.topic = "orders";
    o.publisher_factory =
        [b](const PulsarPublisherSettings& set) -> std::unique_ptr<PulsarPublisher> {
        {
            const std::lock_guard<std::mutex> lock(b->mu);
            b->connects.push_back(set);
        }
        if (b->connect_fails) {
            throw std::runtime_error("pulsar_sink: create_producer: ConnectError");
        }
        return std::make_unique<FakePublisher>(b);
    };
    return o;
}

Batch<std::string> batch(std::initializer_list<std::string> records) {
    Batch<std::string> out;
    for (const auto& r : records) {
        out.emplace(std::string{r});
    }
    return out;
}

template <typename F>
std::string thrown(F&& f) {
    try {
        std::forward<F>(f)();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

const CheckpointBarrier kBarrier{clink::CheckpointId{1}};

void expect_refuses_everything(PulsarSink& sink) {
    EXPECT_NE(thrown([&] { sink.on_data(batch({"z"})); }).find("failed earlier"),
              std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("failed earlier"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.flush(); }).find("failed earlier"), std::string::npos);
}

// A sink attached to a runtime context whose cancel flag the case sets.
struct PulsarAttached {
    explicit PulsarAttached(PulsarSink& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{4347}, "pulsar_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        s.attach_runtime(ctx.get());
    }
    ~PulsarAttached() { sink.attach_runtime(nullptr); }
    PulsarAttached(const PulsarAttached&) = delete;
    PulsarAttached& operator=(const PulsarAttached&) = delete;
    PulsarSink& sink;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
};

}  // namespace

TEST(PulsarSinkFailures, ABarrierPushesTheBatchAndWaitsUntilEveryMessageIsPersisted) {
    auto b = std::make_shared<FakeBroker>();
    auto o = opts_for(b);
    o.max_pending = 7;
    o.batching = false;
    o.send_timeout = std::chrono::milliseconds{900};
    PulsarSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({"a", std::string{"b\0c", 3}, ""}));
    EXPECT_NO_THROW(sink.on_barrier(kBarrier));
    EXPECT_EQ(b->published, (std::vector<std::string>{"a", std::string{"b\0c", 3}, ""}));
    EXPECT_EQ(b->pushes, 1U) << "the barrier sends what the producer batches first";
    ASSERT_EQ(b->connects.size(), 1U);
    EXPECT_EQ(b->connects[0].topic, "orders");
    EXPECT_FALSE(b->connects[0].batching);
    EXPECT_EQ(b->connects[0].send_timeout, std::chrono::milliseconds{900});
    EXPECT_EQ(b->connects[0].max_pending, 14) << "the client's queue sits above the window";
    sink.on_data(batch({"d"}));
    EXPECT_NO_THROW(sink.on_barrier(kBarrier));
}

TEST(PulsarSinkFailures, AFailedSendFailsTheFlushOnceEveryMessageIsAnswered) {
    auto b = std::make_shared<FakeBroker>();
    b->failure = [](std::uint64_t n) {
        return n == 2 ? std::string{"MessageTooBig"} : std::string{};
    };
    PulsarSink sink(opts_for(b));
    sink.open();
    sink.on_data(batch({"a", "b", "c"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("the client failed 1 send(s) to 'orders' (first: MessageTooBig)"),
              std::string::npos)
        << why;
    EXPECT_NE(
        why.find("of the 3 messages published since the last complete flush, 2 were "
                 "persisted, 1 failed (one failed on the client's send timeout may still be on the "
                 "topic), and the other 0 are unanswered"),
        std::string::npos)
        << why;
    expect_refuses_everything(sink);

    b->failure = nullptr;
    sink.open();
    sink.on_data(batch({"d"}));
    EXPECT_NO_THROW(sink.on_barrier(kBarrier));
}

TEST(PulsarSinkFailures, AnswersThatDoNotComeFailPastTheSendTimeout) {
    auto b = std::make_shared<FakeBroker>();
    b->silent.store(true);
    auto o = opts_for(b);
    o.send_timeout = std::chrono::milliseconds{100};
    PulsarSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({"a", "b"}));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_GE(took, std::chrono::milliseconds{100});
    EXPECT_LT(took, std::chrono::seconds{5});
    EXPECT_NE(why.find("had not answered every message within the send timeout (100 ms)"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("0 were persisted, and the other 2 are unanswered and may or may not"),
              std::string::npos)
        << why;
    expect_refuses_everything(sink);
}

TEST(PulsarSinkFailures, APublishTheClientRefusesFailsTheSink) {
    auto b = std::make_shared<FakeBroker>();
    b->publish_fails_at = 2;
    PulsarSink sink(opts_for(b));
    sink.open();
    const auto why = thrown([&] { sink.on_data(batch({"a", "b", "c"})); });
    EXPECT_NE(why.find("publishing to 'orders' failed (pulsar_message_create failed)"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("of the 2 messages published since the last complete flush"),
              std::string::npos)
        << why;
    EXPECT_EQ(b->published_count(), 1U);
    expect_refuses_everything(sink);
}

TEST(PulsarSinkFailures, ACancelEndsAWaitForAnswers) {
    auto b = std::make_shared<FakeBroker>();
    b->silent.store(true);
    PulsarSink sink(opts_for(b));  // waits up to the 30 s send timeout
    PulsarAttached a(sink);
    sink.open();
    sink.on_data(batch({"a"}));
    std::thread canceller([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        a.cancel->store(true);
    });
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    canceller.join();
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds{2});
    EXPECT_NE(why.find("the task was cancelled while waiting for a flush's answers"),
              std::string::npos)
        << why;
    EXPECT_NO_THROW(sink.close());
}

TEST(PulsarSinkFailures, ACancelRequestedBeforeAPublishSendsNothing) {
    auto b = std::make_shared<FakeBroker>();
    PulsarSink sink(opts_for(b));
    PulsarAttached a(sink);
    sink.open();
    a.cancel->store(true);
    EXPECT_NE(thrown([&] { sink.on_data(batch({"a"})); }).find("cancelled before publishing"),
              std::string::npos);
    EXPECT_EQ(b->published_count(), 0U);
}

TEST(PulsarSinkFailures, TheCountAndByteWindowsHoldABurstUntilAnswersCome) {
    auto counted = std::make_shared<FakeBroker>();
    auto o = opts_for(counted);
    o.max_pending = 2;
    PulsarSink by_count(std::move(o));
    by_count.open();
    by_count.on_data(batch({"a", "b", "c", "d", "e"}));
    by_count.on_barrier(kBarrier);
    EXPECT_EQ(counted->published_count(), 5U);
    EXPECT_LT(counted->most_unanswered, 2U) << "a publish came with the window full";
    EXPECT_GE(counted->pushes, 2U) << "a full window pushes the batch before it waits";

    auto sized = std::make_shared<FakeBroker>();
    o = opts_for(sized);
    o.max_pending_bytes = 10;
    PulsarSink by_bytes(std::move(o));
    by_bytes.open();
    by_bytes.on_data(batch({"123456", "123456", "123456"}));
    by_bytes.on_barrier(kBarrier);
    EXPECT_LT(sized->most_unanswered_bytes, 10U) << "a publish came with the byte window full";
}

TEST(PulsarSinkFailures, AWindowThatStaysFullFailsPastTheSendTimeout) {
    auto b = std::make_shared<FakeBroker>();
    b->silent.store(true);
    auto o = opts_for(b);
    o.max_pending = 1;
    o.send_timeout = std::chrono::milliseconds{50};
    PulsarSink sink(std::move(o));
    sink.open();
    const auto why = thrown([&] { sink.on_data(batch({"a", "b"})); });
    EXPECT_NE(why.find("while waiting with max_pending (1)"), std::string::npos) << why;
    EXPECT_EQ(b->published_count(), 1U);
}

TEST(PulsarSinkFailures, CloseNeverWaitsForAnswersItselfAndNeverThrows) {
    auto b = std::make_shared<FakeBroker>();
    b->silent.store(true);
    PulsarSink sink(opts_for(b));
    sink.open();
    sink.on_data(batch({"a"}));
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_NO_THROW(sink.close());
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds{1});
    EXPECT_NO_THROW(sink.close());
    EXPECT_NO_THROW(sink.flush()) << "a closed sink that never failed has nothing to flush";
}

TEST(PulsarSinkFailures, ARefusedProducerLeavesTheSinkUnopened) {
    auto b = std::make_shared<FakeBroker>();
    b->connect_fails = true;
    PulsarSink sink(opts_for(b));
    EXPECT_NE(thrown([&] { sink.open(); }).find("ConnectError"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_data(batch({"a"})); }).find("before open()"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("before open()"), std::string::npos);
    EXPECT_NO_THROW(sink.close());
}

TEST(PulsarSinkFailures, AZeroSendTimeoutIsRefused) {
    auto b = std::make_shared<FakeBroker>();
    auto o = opts_for(b);
    o.send_timeout = std::chrono::milliseconds{0};
    EXPECT_NE(thrown([&] { PulsarSink sink(std::move(o)); }).find("send timeout must be positive"),
              std::string::npos);
}
