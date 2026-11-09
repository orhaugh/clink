// The NATS JetStream sink's failure paths against a scripted server: a
// refusal, answers that do not come, a closed connection and a publish the
// library refuses, each followed by another record, barrier and flush;
// reopening; a cancel during a wait and before a publish; close(); and the
// count and byte windows.

#include <algorithm>
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
#include "clink/nats/nats_publisher.hpp"
#include "clink/nats/nats_sink.hpp"
#include "clink/runtime/runtime_context.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::nats::NatsAnswers;
using clink::nats::NatsPublisher;
using clink::nats::NatsPublisherSettings;
using clink::nats::NatsSink;

namespace {

// What the stand-in server knows and does, shared by every connection a sink
// opens to it. By default it stores each message, answering when the sink next
// waits.
struct FakeServer {
    std::mutex mu;
    std::vector<std::string> published;
    std::vector<NatsPublisherSettings> connects;
    // The nth publish's answer (1-based): an empty string stores it, text
    // refuses it with that text.
    std::function<std::string(std::uint64_t n)> refusal;
    std::atomic<bool> silent{false};  // answers withheld
    std::uint64_t publish_fails_at{0};
    bool closed{false};  // wait() finds the connection closed
    bool connect_fails{false};
    // The most messages, and bytes, that were unanswered when a publish came.
    std::uint64_t most_unanswered{0};
    std::uint64_t most_unanswered_bytes{0};

    std::size_t published_count() {
        const std::lock_guard<std::mutex> lock(mu);
        return published.size();
    }
};

class FakePublisher final : public NatsPublisher {
public:
    explicit FakePublisher(std::shared_ptr<FakeServer> s) : s_(std::move(s)) {}

    void publish(std::string_view body) override {
        const std::uint64_t n = ++sent_;
        if (s_->publish_fails_at == n) {
            throw std::runtime_error("js_PublishAsync: Maximum Payload Exceeded");
        }
        const std::lock_guard<std::mutex> lock(s_->mu);
        s_->most_unanswered = std::max<std::uint64_t>(s_->most_unanswered, queue_.size());
        std::uint64_t bytes = 0;
        for (const auto& q : queue_) {
            bytes += q.bytes;
        }
        s_->most_unanswered_bytes = std::max(s_->most_unanswered_bytes, bytes);
        s_->published.emplace_back(body);
        queue_.push_back(Pending{body.size(), s_->refusal ? s_->refusal(n) : std::string{}});
    }

    NatsAnswers wait(std::uint64_t /*seen*/, std::chrono::milliseconds timeout) override {
        if (s_->closed) {
            throw std::runtime_error("the connection to the server is closed");
        }
        {
            const std::lock_guard<std::mutex> lock(s_->mu);
            if (!s_->silent.load() && !queue_.empty()) {
                for (const auto& q : queue_) {
                    answers_.answered_bytes += q.bytes;
                    if (q.refusal.empty()) {
                        ++answers_.acked;
                    } else if (answers_.refused++ == 0) {
                        answers_.first_refusal = q.refusal;
                    }
                }
                queue_.clear();
                return answers_;
            }
        }
        std::this_thread::sleep_for(timeout);
        return answers_;
    }

    [[nodiscard]] NatsAnswers answers() const override { return answers_; }

    void close() noexcept override { closed_ = true; }

private:
    struct Pending {
        std::size_t bytes;
        std::string refusal;
    };
    std::shared_ptr<FakeServer> s_;
    std::uint64_t sent_{0};
    std::deque<Pending> queue_;
    NatsAnswers answers_;
    bool closed_{false};
};

NatsSink::Options opts_for(const std::shared_ptr<FakeServer>& s) {
    NatsSink::Options o;
    o.subject = "orders";
    o.publisher_factory = [s](const NatsPublisherSettings& set) -> std::unique_ptr<NatsPublisher> {
        {
            const std::lock_guard<std::mutex> lock(s->mu);
            s->connects.push_back(set);
        }
        if (s->connect_fails) {
            throw std::runtime_error(
                "nats_sink: connect to nats://nowhere:4222: No Server Available");
        }
        return std::make_unique<FakePublisher>(s);
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

void expect_refuses_everything(NatsSink& sink) {
    EXPECT_NE(thrown([&] { sink.on_data(batch({"z"})); }).find("failed earlier"),
              std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("failed earlier"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.flush(); }).find("failed earlier"), std::string::npos);
}

// A sink attached to a runtime context whose cancel flag the case sets.
struct NatsAttached {
    explicit NatsAttached(NatsSink& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{4346}, "nats_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        s.attach_runtime(ctx.get());
    }
    ~NatsAttached() { sink.attach_runtime(nullptr); }
    NatsAttached(const NatsAttached&) = delete;
    NatsAttached& operator=(const NatsAttached&) = delete;
    NatsSink& sink;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
};

}  // namespace

TEST(NatsSinkFailures, ABarrierWaitsUntilEveryMessageIsStored) {
    auto s = std::make_shared<FakeServer>();
    auto o = opts_for(s);
    o.max_pending = 7;
    o.conn.connect_timeout = std::chrono::milliseconds{500};
    o.conn.write_timeout = std::chrono::milliseconds{700};
    NatsSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({"a", std::string{"b\0c", 3}, ""}));
    EXPECT_NO_THROW(sink.on_barrier(kBarrier));
    EXPECT_EQ(s->published, (std::vector<std::string>{"a", std::string{"b\0c", 3}, ""}));
    ASSERT_EQ(s->connects.size(), 1U);
    EXPECT_EQ(s->connects[0].subject, "orders");
    EXPECT_EQ(s->connects[0].max_pending, 7);
    EXPECT_EQ(s->connects[0].conn.connect_timeout, std::chrono::milliseconds{500});
    EXPECT_EQ(s->connects[0].conn.write_timeout, std::chrono::milliseconds{700});
    // A second interval starts from the answers the first ended with.
    sink.on_data(batch({"d"}));
    EXPECT_NO_THROW(sink.on_barrier(kBarrier));
}

TEST(NatsSinkFailures, ARefusalFailsTheFlushOnceEveryMessageIsAnswered) {
    auto s = std::make_shared<FakeServer>();
    s->refusal = [](std::uint64_t n) {
        return n == 2 ? std::string{"maximum messages exceeded"} : std::string{};
    };
    NatsSink sink(opts_for(s));
    sink.open();
    sink.on_data(batch({"a", "b", "c"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("the server refused 1 publish(es) to 'orders' (first: maximum messages "
                       "exceeded)"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("of the 3 messages published since the last complete flush, 2 were "
                       "stored, 1 refused, and the other 0 are unanswered"),
              std::string::npos)
        << why;
    expect_refuses_everything(sink);

    s->refusal = nullptr;
    sink.open();
    sink.on_data(batch({"d"}));
    EXPECT_NO_THROW(sink.on_barrier(kBarrier));
}

TEST(NatsSinkFailures, AnswersThatDoNotComeFailAtThePublishTimeout) {
    auto s = std::make_shared<FakeServer>();
    s->silent.store(true);
    auto o = opts_for(s);
    o.publish_timeout = std::chrono::milliseconds{200};
    NatsSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({"a", "b"}));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_GE(took, std::chrono::milliseconds{200});
    EXPECT_LT(took, std::chrono::seconds{3});
    EXPECT_NE(why.find("within the publish timeout (200 ms)"), std::string::npos) << why;
    EXPECT_NE(why.find("0 were stored, and the other 2 are unanswered and may or may not"),
              std::string::npos)
        << why;
    expect_refuses_everything(sink);
}

TEST(NatsSinkFailures, AClosedConnectionFailsTheFlush) {
    auto s = std::make_shared<FakeServer>();
    s->closed = true;
    NatsSink sink(opts_for(s));
    sink.open();
    sink.on_data(batch({"a"}));
    const auto why = thrown([&] { sink.flush(); });
    EXPECT_NE(why.find("the connection to the server failed (the connection to the server is "
                       "closed)"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("the other 1 are unanswered"), std::string::npos) << why;
    expect_refuses_everything(sink);
}

TEST(NatsSinkFailures, APublishTheLibraryRefusesFailsTheSink) {
    auto s = std::make_shared<FakeServer>();
    s->publish_fails_at = 2;
    NatsSink sink(opts_for(s));
    sink.open();
    const auto why = thrown([&] { sink.on_data(batch({"a", "b", "c"})); });
    EXPECT_NE(why.find("publishing to 'orders' failed (js_PublishAsync: Maximum Payload "
                       "Exceeded)"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("of the 2 messages published since the last complete flush"),
              std::string::npos)
        << why;
    EXPECT_EQ(s->published_count(), 1U);
    expect_refuses_everything(sink);
}

TEST(NatsSinkFailures, ACancelEndsAWaitForAnswers) {
    auto s = std::make_shared<FakeServer>();
    s->silent.store(true);
    NatsSink sink(opts_for(s));  // waits up to 30 s
    NatsAttached a(sink);
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

TEST(NatsSinkFailures, ACancelRequestedBeforeAPublishSendsNothing) {
    auto s = std::make_shared<FakeServer>();
    NatsSink sink(opts_for(s));
    NatsAttached a(sink);
    sink.open();
    a.cancel->store(true);
    EXPECT_NE(thrown([&] { sink.on_data(batch({"a"})); }).find("cancelled before publishing"),
              std::string::npos);
    EXPECT_EQ(s->published_count(), 0U);
}

TEST(NatsSinkFailures, TheCountAndByteWindowsHoldPublishesUntilAnswersCome) {
    auto counted = std::make_shared<FakeServer>();
    auto o = opts_for(counted);
    o.max_pending = 2;
    NatsSink by_count(std::move(o));
    by_count.open();
    by_count.on_data(batch({"a", "b", "c", "d", "e"}));
    by_count.on_barrier(kBarrier);
    EXPECT_EQ(counted->published_count(), 5U);
    EXPECT_LT(counted->most_unanswered, 2U) << "a publish came with the window full";

    auto sized = std::make_shared<FakeServer>();
    o = opts_for(sized);
    o.max_pending_bytes = 10;
    NatsSink by_bytes(std::move(o));
    by_bytes.open();
    by_bytes.on_data(batch({"123456", "123456", "123456"}));
    by_bytes.on_barrier(kBarrier);
    EXPECT_LT(sized->most_unanswered_bytes, 10U) << "a publish came with the byte window full";
}

TEST(NatsSinkFailures, AWindowThatStaysFullFailsAtThePublishTimeout) {
    auto s = std::make_shared<FakeServer>();
    s->silent.store(true);
    auto o = opts_for(s);
    o.max_pending = 1;
    o.publish_timeout = std::chrono::milliseconds{100};
    NatsSink sink(std::move(o));
    sink.open();
    const auto why = thrown([&] { sink.on_data(batch({"a", "b"})); });
    EXPECT_NE(why.find("while waiting with max_pending (1)"), std::string::npos) << why;
    EXPECT_EQ(s->published_count(), 1U);
}

TEST(NatsSinkFailures, CloseDiscardsWhatIsUnansweredAndNeverThrows) {
    auto s = std::make_shared<FakeServer>();
    s->silent.store(true);
    NatsSink sink(opts_for(s));
    sink.open();
    sink.on_data(batch({"a"}));
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_NO_THROW(sink.close());
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds{1});
    EXPECT_NO_THROW(sink.close());
    EXPECT_NO_THROW(sink.flush()) << "a closed sink that never failed has nothing to flush";
}

TEST(NatsSinkFailures, AFailedConnectLeavesTheSinkUnopened) {
    auto s = std::make_shared<FakeServer>();
    s->connect_fails = true;
    NatsSink sink(opts_for(s));
    EXPECT_NE(thrown([&] { sink.open(); }).find("No Server Available"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_data(batch({"a"})); }).find("before open()"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("before open()"), std::string::npos);
    EXPECT_NO_THROW(sink.close());
}
