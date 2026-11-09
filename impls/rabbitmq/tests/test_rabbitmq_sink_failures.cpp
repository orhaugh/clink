// The RabbitMQ sink's failure paths against a scripted broker: a nack, a
// returned message, a closed channel, a lost connection and confirms that do
// not come, each followed by another record, barrier and flush; reopening; a
// cancel during a publish, during a wait and before one; close(); and the
// record and unconfirmed bounds.

#include <atomic>
#include <chrono>
#include <condition_variable>
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
#include "clink/rabbitmq/rabbitmq_publisher.hpp"
#include "clink/rabbitmq/rabbitmq_sink.hpp"
#include "clink/runtime/runtime_context.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::rabbitmq::PublisherEvent;
using clink::rabbitmq::PublisherSettings;
using clink::rabbitmq::RabbitMqPublisher;
using clink::rabbitmq::RabbitMqSink;

namespace {

using Kind = PublisherEvent::Kind;

// What the stand-in broker knows and does, shared by every connection a sink
// opens to it. By default it acks each message, one confirm per publish, in
// order, as the sink asks for confirms.
struct FakeBroker {
    std::mutex mu;
    std::vector<std::string> published;  // every body, in order
    std::vector<PublisherSettings> connects;
    std::vector<bool> closes;  // each close: orderly or not
    // Replaces the default answer to the nth publish (1-based, the delivery
    // tag): the events the broker sends for it, in order.
    std::function<std::vector<PublisherEvent>(std::uint64_t tag)> answer;
    // A publish that blocks until the connection is cancelled.
    std::function<bool(std::uint64_t tag)> publish_blocks;
    // Confirms withheld: next() waits out its timeout, or until cancelled.
    std::atomic<bool> silent{false};
    bool connect_fails{false};

    std::size_t published_count() {
        const std::lock_guard<std::mutex> lock(mu);
        return published.size();
    }
};

class FakePublisher final : public RabbitMqPublisher {
public:
    explicit FakePublisher(std::shared_ptr<FakeBroker> b) : b_(std::move(b)) {}

    void publish(std::string_view body) override {
        if (cancelled_.load()) {
            throw std::runtime_error("Bad file descriptor");
        }
        const std::uint64_t tag = ++tags_;
        if (b_->publish_blocks && b_->publish_blocks(tag)) {
            std::unique_lock<std::mutex> lock(m_);
            cv_.wait_for(lock, std::chrono::seconds{10}, [this] { return cancelled_.load(); });
            throw std::runtime_error("socket is not connected");
        }
        {
            const std::lock_guard<std::mutex> lock(b_->mu);
            b_->published.emplace_back(body);
        }
        if (b_->answer) {
            for (auto& e : b_->answer(tag)) {
                pending_.push_back(std::move(e));
            }
        } else {
            pending_.push_back(PublisherEvent{Kind::Ack, tag, false, {}});
        }
    }

    PublisherEvent next(std::chrono::milliseconds timeout) override {
        if (cancelled_.load()) {
            return PublisherEvent{Kind::Lost, 0, false, "Bad file descriptor"};
        }
        if (b_->silent.load() || pending_.empty()) {
            std::unique_lock<std::mutex> lock(m_);
            cv_.wait_for(lock, timeout, [this] { return cancelled_.load(); });
            if (cancelled_.load()) {
                return PublisherEvent{Kind::Lost, 0, false, "Bad file descriptor"};
            }
            return PublisherEvent{Kind::Timeout, 0, false, {}};
        }
        auto e = std::move(pending_.front());
        pending_.pop_front();
        return e;
    }

    void cancel() noexcept override {
        cancelled_.store(true);
        cv_.notify_all();
    }

    void close(bool orderly) noexcept override {
        const std::lock_guard<std::mutex> lock(b_->mu);
        b_->closes.push_back(orderly);
    }

private:
    std::shared_ptr<FakeBroker> b_;
    std::uint64_t tags_{0};
    std::deque<PublisherEvent> pending_;
    std::atomic<bool> cancelled_{false};
    std::mutex m_;
    std::condition_variable cv_;
};

RabbitMqSink::Options opts_for(const std::shared_ptr<FakeBroker>& b) {
    RabbitMqSink::Options o;
    o.routing_key = "q";
    o.publisher_factory = [b](const PublisherSettings& s) -> std::unique_ptr<RabbitMqPublisher> {
        {
            const std::lock_guard<std::mutex> lock(b->mu);
            b->connects.push_back(s);
        }
        if (b->connect_fails) {
            throw std::runtime_error("rabbitmq_sink: connect to nowhere:5672 failed: timeout");
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

void expect_refuses_everything(RabbitMqSink& sink) {
    EXPECT_NE(thrown([&] { sink.on_data(batch({"z"})); }).find("failed earlier"),
              std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("failed earlier"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.flush(); }).find("failed earlier"), std::string::npos);
}

// A sink attached to a runtime context whose cancel flag the case sets.
struct RmqAttached {
    explicit RmqAttached(RabbitMqSink& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{4345}, "rabbitmq_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        s.attach_runtime(ctx.get());
    }
    ~RmqAttached() { sink.attach_runtime(nullptr); }
    RmqAttached(const RmqAttached&) = delete;
    RmqAttached& operator=(const RmqAttached&) = delete;
    RabbitMqSink& sink;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
};

}  // namespace

TEST(RabbitMqSinkFailures, ABarrierWaitsForEveryConfirmAndAMultipleAckCoversItsRange) {
    auto b = std::make_shared<FakeBroker>();
    // One multiple ack for the first three, one ack for the fourth.
    b->answer = [](std::uint64_t tag) -> std::vector<PublisherEvent> {
        if (tag == 3) {
            return {PublisherEvent{Kind::Ack, 3, true, {}}};
        }
        if (tag == 4) {
            return {PublisherEvent{Kind::Ack, 4, false, {}}};
        }
        return {};
    };
    auto o = opts_for(b);
    o.exchange = "ex";
    o.content_type = "text/plain";
    o.persistent = false;
    o.mandatory = false;
    RabbitMqSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({"a", "b", std::string{"c\0d", 3}, ""}));
    EXPECT_NO_THROW(sink.on_barrier(kBarrier));
    EXPECT_EQ(b->published, (std::vector<std::string>{"a", "b", std::string{"c\0d", 3}, ""}));
    ASSERT_EQ(b->connects.size(), 1U);
    EXPECT_EQ(b->connects[0].exchange, "ex");
    EXPECT_EQ(b->connects[0].routing_key, "q");
    EXPECT_EQ(b->connects[0].content_type, "text/plain");
    EXPECT_FALSE(b->connects[0].persistent);
    EXPECT_FALSE(b->connects[0].mandatory);
    EXPECT_EQ(b->connects[0].conn.connect_timeout, std::chrono::milliseconds{10000});
    sink.close();
    EXPECT_EQ(b->closes, (std::vector<bool>{true}));
}

TEST(RabbitMqSinkFailures, ANackFailsTheSinkUntilItIsOpenedAgain) {
    auto b = std::make_shared<FakeBroker>();
    b->answer = [](std::uint64_t tag) -> std::vector<PublisherEvent> {
        if (tag == 2) {
            return {PublisherEvent{Kind::Nack, 2, false, {}}};
        }
        return {PublisherEvent{Kind::Ack, tag, false, {}}};
    };
    RabbitMqSink sink(opts_for(b));
    sink.open();
    sink.on_data(batch({"a", "b", "c"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("the broker refused 1 message(s) (basic.nack)"), std::string::npos) << why;
    // The reading went on past the nack, so every message is accounted for.
    EXPECT_NE(why.find("of the 3 messages published since the last complete flush, 2 were "
                       "confirmed, 1 refused (basic.nack), and the other 0 are unconfirmed"),
              std::string::npos)
        << why;
    expect_refuses_everything(sink);
    sink.close();
    EXPECT_EQ(b->closes.back(), false) << "a failed sink closes without the handshake";

    b->answer = nullptr;
    sink.open();
    sink.on_data(batch({"d"}));
    EXPECT_NO_THROW(sink.on_barrier(kBarrier));
}

TEST(RabbitMqSinkFailures, AReturnedMessageFailsTheFlushOnceTheRestAreConfirmed) {
    auto b = std::make_shared<FakeBroker>();
    // The broker returns the second message ahead of its ack.
    b->answer = [](std::uint64_t tag) -> std::vector<PublisherEvent> {
        if (tag == 2) {
            return {PublisherEvent{Kind::Return, 0, false, "312 NO_ROUTE"},
                    PublisherEvent{Kind::Ack, 2, false, {}}};
        }
        return {PublisherEvent{Kind::Ack, tag, false, {}}};
    };
    RabbitMqSink sink(opts_for(b));
    sink.open();
    sink.on_data(batch({"a", "b", "c"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("the broker returned 1 message(s) that no queue is bound to receive"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("312 NO_ROUTE"), std::string::npos) << why;
    EXPECT_NE(why.find("2 were confirmed, 1 returned unroutable, and the other 0"),
              std::string::npos)
        << why;
    expect_refuses_everything(sink);
}

TEST(RabbitMqSinkFailures, AClosedChannelFailsTheFlushWithTheBrokersReason) {
    auto b = std::make_shared<FakeBroker>();
    b->answer = [](std::uint64_t tag) -> std::vector<PublisherEvent> {
        if (tag == 1) {
            return {PublisherEvent{
                Kind::Closed,
                0,
                false,
                "the broker closed the channel: 404 NOT_FOUND - no exchange 'ex' in vhost '/'"}};
        }
        return {};
    };
    RabbitMqSink sink(opts_for(b));
    sink.open();
    sink.on_data(batch({"a", "b"}));
    const auto why = thrown([&] { sink.flush(); });
    EXPECT_NE(why.find("404 NOT_FOUND - no exchange 'ex'"), std::string::npos) << why;
    EXPECT_NE(why.find("the other 2 are unconfirmed"), std::string::npos) << why;
    expect_refuses_everything(sink);
}

TEST(RabbitMqSinkFailures, ALostConnectionLeavesTheUnconfirmedUnknown) {
    auto b = std::make_shared<FakeBroker>();
    b->answer = [](std::uint64_t tag) -> std::vector<PublisherEvent> {
        if (tag == 2) {
            return {PublisherEvent{Kind::Lost, 0, false, "connection reset by peer"}};
        }
        return {PublisherEvent{Kind::Ack, tag, false, {}}};
    };
    RabbitMqSink sink(opts_for(b));
    sink.open();
    sink.on_data(batch({"a", "b", "c"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("the connection to the broker failed (connection reset by peer)"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("1 were confirmed, and the other 2 are unconfirmed and may or may not"),
              std::string::npos)
        << why;
    expect_refuses_everything(sink);
}

TEST(RabbitMqSinkFailures, ConfirmsThatDoNotComeFailTheFlushAtTheConfirmTimeout) {
    auto b = std::make_shared<FakeBroker>();
    b->silent.store(true);
    auto o = opts_for(b);
    o.confirm_timeout = std::chrono::milliseconds{200};
    RabbitMqSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({"a", "b"}));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_GE(took, std::chrono::milliseconds{200});
    EXPECT_LT(took, std::chrono::seconds{3});
    EXPECT_NE(why.find("within the confirm timeout (200 ms)"), std::string::npos) << why;
    EXPECT_NE(why.find("0 were confirmed, and the other 2 are unconfirmed"), std::string::npos)
        << why;
    expect_refuses_everything(sink);
}

TEST(RabbitMqSinkFailures, ACancelEndsAWaitForConfirms) {
    auto b = std::make_shared<FakeBroker>();
    b->silent.store(true);
    RabbitMqSink sink(opts_for(b));  // waits up to 30 s
    RmqAttached a(sink);
    sink.open();
    sink.on_data(batch({"a"}));
    std::thread canceller([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        a.cancel->store(true);
    });
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    canceller.join();
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds{5});
    EXPECT_NE(why.find("the task was cancelled while waiting for a flush's confirms"),
              std::string::npos)
        << why;
    EXPECT_NO_THROW(sink.close());
    EXPECT_EQ(b->closes.back(), false);
}

TEST(RabbitMqSinkFailures, ACancelEndsAPublishTheBrokerDoesNotRead) {
    auto b = std::make_shared<FakeBroker>();
    b->publish_blocks = [](std::uint64_t tag) { return tag == 2; };
    RabbitMqSink sink(opts_for(b));
    RmqAttached a(sink);
    sink.open();
    std::thread canceller([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        a.cancel->store(true);
    });
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_data(batch({"a", "b", "c"})); });
    canceller.join();
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds{5});
    EXPECT_NE(why.find("the task was cancelled publishing to exchange ''"), std::string::npos)
        << why;
    EXPECT_NE(why.find("of the 2 messages published since the last complete flush"),
              std::string::npos)
        << why;
    EXPECT_EQ(b->published_count(), 1U);
    expect_refuses_everything(sink);
}

TEST(RabbitMqSinkFailures, ACancelRequestedBeforeAPublishSendsNothing) {
    auto b = std::make_shared<FakeBroker>();
    RabbitMqSink sink(opts_for(b));
    RmqAttached a(sink);
    sink.open();
    a.cancel->store(true);
    EXPECT_NE(thrown([&] { sink.on_data(batch({"a"})); }).find("cancelled before publishing"),
              std::string::npos);
    EXPECT_EQ(b->published_count(), 0U);
}

TEST(RabbitMqSinkFailures, CloseDiscardsWhatIsUnconfirmedAndNeverThrows) {
    auto b = std::make_shared<FakeBroker>();
    b->silent.store(true);
    RabbitMqSink sink(opts_for(b));
    sink.open();
    sink.on_data(batch({"a"}));
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_NO_THROW(sink.close());
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds{1})
        << "close() waited for confirms";
    EXPECT_NO_THROW(sink.close());
    EXPECT_NO_THROW(sink.flush()) << "a closed sink that never failed has nothing to flush";
}

TEST(RabbitMqSinkFailures, AnOversizedRecordFailsTheSinkBeforeItIsPublished) {
    auto b = std::make_shared<FakeBroker>();
    auto o = opts_for(b);
    o.max_record_bytes = 8;
    RabbitMqSink sink(std::move(o));
    sink.open();
    const auto why = thrown([&] { sink.on_data(batch({"12345678", "secret-123"})); });
    EXPECT_NE(why.find("a record is 10 bytes, more than max_record_bytes (8)"), std::string::npos)
        << why;
    EXPECT_EQ(why.find("secret"), std::string::npos) << why;
    EXPECT_EQ(b->published_count(), 1U);
    expect_refuses_everything(sink);
}

TEST(RabbitMqSinkFailures, AtMaxUnconfirmedTheSinkWaitsForConfirmsBeforePublishingMore) {
    auto b = std::make_shared<FakeBroker>();
    auto o = opts_for(b);
    o.max_unconfirmed = 2;
    RabbitMqSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({"a", "b", "c", "d", "e"}));
    EXPECT_EQ(b->published_count(), 5U);
    EXPECT_NO_THROW(sink.on_barrier(kBarrier));

    // A broker that stops confirming holds the sink at the bound, and the wait
    // fails at the confirm timeout rather than publishing on.
    auto stalled = std::make_shared<FakeBroker>();
    stalled->silent.store(true);
    o = opts_for(stalled);
    o.max_unconfirmed = 2;
    o.confirm_timeout = std::chrono::milliseconds{100};
    RabbitMqSink held(std::move(o));
    held.open();
    const auto why = thrown([&] { held.on_data(batch({"a", "b", "c"})); });
    EXPECT_NE(why.find("while waiting with max_unconfirmed (2) messages unconfirmed"),
              std::string::npos)
        << why;
    EXPECT_EQ(stalled->published_count(), 2U);
}

TEST(RabbitMqSinkFailures, AFailedConnectLeavesTheSinkUnopened) {
    auto b = std::make_shared<FakeBroker>();
    b->connect_fails = true;
    RabbitMqSink sink(opts_for(b));
    EXPECT_NE(thrown([&] { sink.open(); }).find("failed: timeout"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_data(batch({"a"})); }).find("before open()"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("before open()"), std::string::npos);
    EXPECT_NO_THROW(sink.close());
}
