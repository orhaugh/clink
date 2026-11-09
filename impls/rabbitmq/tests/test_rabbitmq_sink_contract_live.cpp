// The RabbitMQ sink's contract against a live broker. SKIPPED unless
// CLINK_RABBITMQ_TEST_ENDPOINT names a broker host (port, user and password
// from CLINK_RABBITMQ_TEST_{PORT,USER,PASSWORD}); scripts/brokers-live.sh runs
// it against the pinned RabbitMQ 3 image and fails when any case is skipped.
//
// Every case judges by what the broker holds, read through an AMQP connection
// of the test's own: each queue's messages, their properties and their count.
// The cases that need a broker that stops answering, or stops reading, connect
// the sink through a TCP proxy (tests/integration/tcp_fault_proxy.hpp) and
// blackhole it.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/core/record.hpp"
#include "clink/rabbitmq/rabbitmq_sink.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "tests/integration/tcp_fault_proxy.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::rabbitmq::RabbitMqSink;

namespace {

bool rmq_live() {
    return std::getenv("CLINK_RABBITMQ_TEST_ENDPOINT") != nullptr;
}

std::string env_or(const char* k, const std::string& dflt) {
    const char* v = std::getenv(k);
    return v != nullptr ? std::string(v) : dflt;
}

std::string host() {
    return std::getenv("CLINK_RABBITMQ_TEST_ENDPOINT");
}
int port() {
    return std::stoi(env_or("CLINK_RABBITMQ_TEST_PORT", "5672"));
}

#define REQUIRE_RMQ()                                                                     \
    do {                                                                                  \
        if (!rmq_live()) {                                                                \
            GTEST_SKIP() << "set CLINK_RABBITMQ_TEST_ENDPOINT (+ _PORT/_USER/_PASSWORD)"; \
        }                                                                                 \
    } while (0)

std::string unique(const std::string& what) {
    static std::atomic<int> n{0};
    return "clink_sinkc_" + std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(n.fetch_add(1)) + "_" + what;
}

std::string text_of(const amqp_bytes_t& b) {
    return b.bytes == nullptr ? std::string{}
                              : std::string(static_cast<const char*>(b.bytes), b.len);
}

// The test's own AMQP connection: queues declared, read and deleted apart from
// the sink's publisher.
class TestAmqp {
public:
    struct Message {
        std::string body;
        int delivery_mode{0};
        std::string content_type;
    };

    TestAmqp() {
        c_ = amqp_new_connection();
        amqp_socket_t* s = amqp_tcp_socket_new(c_);
        if (s == nullptr || amqp_socket_open(s, host().c_str(), port()) != AMQP_STATUS_OK) {
            throw std::runtime_error("the test cannot connect to the broker");
        }
        check(amqp_login(c_,
                         "/",
                         0,
                         AMQP_DEFAULT_FRAME_SIZE,
                         0,
                         AMQP_SASL_METHOD_PLAIN,
                         env_or("CLINK_RABBITMQ_TEST_USER", "guest").c_str(),
                         env_or("CLINK_RABBITMQ_TEST_PASSWORD", "guest").c_str()),
              "login");
        amqp_channel_open(c_, 1);
        check(amqp_get_rpc_reply(c_), "channel.open");
    }
    ~TestAmqp() {
        amqp_channel_close(c_, 1, AMQP_REPLY_SUCCESS);
        amqp_connection_close(c_, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(c_);
    }
    TestAmqp(const TestAmqp&) = delete;
    TestAmqp& operator=(const TestAmqp&) = delete;

    // A durable queue; with max_length, one that refuses publishes once full.
    void declare(const std::string& q, std::int64_t max_length = -1) {
        amqp_table_entry_t entries[2];
        amqp_table_t args{0, entries};
        if (max_length >= 0) {
            entries[0].key = amqp_cstring_bytes("x-max-length");
            entries[0].value.kind = AMQP_FIELD_KIND_I64;
            entries[0].value.value.i64 = max_length;
            entries[1].key = amqp_cstring_bytes("x-overflow");
            entries[1].value.kind = AMQP_FIELD_KIND_UTF8;
            entries[1].value.value.bytes = amqp_cstring_bytes("reject-publish");
            args.num_entries = 2;
        }
        (void)amqp_queue_declare(c_, 1, amqp_cstring_bytes(q.c_str()), 0, 1, 0, 0, args);
        check(amqp_get_rpc_reply(c_), "queue.declare " + q);
    }

    void remove(const std::string& q) {
        (void)amqp_queue_delete(c_, 1, amqp_cstring_bytes(q.c_str()), 0, 0);
        (void)amqp_get_rpc_reply(c_);
    }

    std::uint32_t count(const std::string& q) {
        const auto* ok =
            amqp_queue_declare(c_, 1, amqp_cstring_bytes(q.c_str()), 1, 0, 0, 0, amqp_empty_table);
        check(amqp_get_rpc_reply(c_), "queue.declare passive " + q);
        return ok == nullptr ? 0 : ok->message_count;
    }

    // Every message the queue holds, oldest first, taken off it.
    std::vector<Message> take(const std::string& q) {
        std::vector<Message> out;
        for (;;) {
            const amqp_rpc_reply_t r = amqp_basic_get(c_, 1, amqp_cstring_bytes(q.c_str()), 1);
            check(r, "basic.get " + q);
            if (r.reply.id == AMQP_BASIC_GET_EMPTY_METHOD) {
                return out;
            }
            amqp_message_t m{};
            check(amqp_read_message(c_, 1, &m, 0), "reading a message");
            Message got;
            got.body = text_of(m.body);
            if ((m.properties._flags & AMQP_BASIC_DELIVERY_MODE_FLAG) != 0) {
                got.delivery_mode = m.properties.delivery_mode;
            }
            if ((m.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG) != 0) {
                got.content_type = text_of(m.properties.content_type);
            }
            amqp_destroy_message(&m);
            amqp_maybe_release_buffers(c_);
            out.push_back(std::move(got));
        }
    }

    std::vector<std::string> bodies(const std::string& q) {
        std::vector<std::string> out;
        for (auto& m : take(q)) {
            out.push_back(std::move(m.body));
        }
        return out;
    }

private:
    static void check(const amqp_rpc_reply_t& r, const std::string& what) {
        if (r.reply_type != AMQP_RESPONSE_NORMAL) {
            throw std::runtime_error("the test's " + what + " failed");
        }
    }

    amqp_connection_state_t c_{nullptr};
};

RabbitMqSink::Options sink_opts(const std::string& routing_key) {
    RabbitMqSink::Options o;
    o.conn.host = host();
    o.conn.port = port();
    o.conn.user = env_or("CLINK_RABBITMQ_TEST_USER", "guest");
    o.conn.password = env_or("CLINK_RABBITMQ_TEST_PASSWORD", "guest");
    o.routing_key = routing_key;
    return o;
}

// The sink's options, connected through `proxy` instead of to the broker.
RabbitMqSink::Options proxied_opts(const std::string& routing_key,
                                   const clink::itest::TcpFaultProxy& proxy) {
    auto o = sink_opts(routing_key);
    o.conn.host = "127.0.0.1";
    o.conn.port = proxy.port();
    return o;
}

Batch<std::string> batch(const std::vector<std::string>& records) {
    Batch<std::string> b;
    for (const auto& r : records) {
        b.emplace(std::string{r});
    }
    return b;
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

// Waits up to `limit` for `done`, checking every 20 ms.
bool eventually(const std::function<bool()>& done,
                std::chrono::milliseconds limit = std::chrono::milliseconds{5000}) {
    const auto until = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < until) {
        if (done()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
    }
    return done();
}

// A sink attached to a runtime context whose cancel flag the case sets.
struct Attached {
    explicit Attached(RabbitMqSink& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{81}, "rabbitmq_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        s.attach_runtime(ctx.get());
    }
    ~Attached() { sink.attach_runtime(nullptr); }
    Attached(const Attached&) = delete;
    Attached& operator=(const Attached&) = delete;
    void cancel_after(std::chrono::milliseconds wait) {
        canceller = std::thread([this, wait] {
            std::this_thread::sleep_for(wait);
            cancel->store(true);
        });
    }
    RabbitMqSink& sink;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
    std::thread canceller;
};

}  // namespace

TEST(RabbitMqSinkContractLive, MessagesLandExactlyAsWrittenWithTheirProperties) {
    REQUIRE_RMQ();
    TestAmqp t;
    const auto q = unique("exact");
    t.declare(q);
    const std::vector<std::string> records = {R"({"id":1,"amount":12345678901234567890.5})",
                                              std::string{"nul\0inside", 10},
                                              "caf\xc3\xa9 \xf0\x9f\x98\x80",
                                              "",
                                              std::string(1U << 20, 'x')};
    RabbitMqSink sink(sink_opts(q));
    sink.open();
    sink.on_data(batch(records));
    sink.on_barrier(kBarrier);
    sink.close();
    const auto got = t.take(q);
    ASSERT_EQ(got.size(), records.size());
    for (std::size_t i = 0; i < got.size(); ++i) {
        EXPECT_EQ(got[i].body, records[i]) << i;
        EXPECT_EQ(got[i].delivery_mode, 2) << i;
        EXPECT_EQ(got[i].content_type, "application/json") << i;
    }

    auto o = sink_opts(q);
    o.persistent = false;
    o.content_type = "text/plain";
    RabbitMqSink transient(std::move(o));
    transient.open();
    transient.on_data(batch({"t"}));
    transient.on_barrier(kBarrier);
    transient.close();
    const auto plain = t.take(q);
    ASSERT_EQ(plain.size(), 1U);
    EXPECT_EQ(plain[0].delivery_mode, 1);
    EXPECT_EQ(plain[0].content_type, "text/plain");
    t.remove(q);
}

TEST(RabbitMqSinkContractLive, ManyMessagesPublishWithinTheUnconfirmedBound) {
    REQUIRE_RMQ();
    TestAmqp t;
    const auto q = unique("bound");
    t.declare(q);
    auto o = sink_opts(q);
    o.max_unconfirmed = 10;
    RabbitMqSink sink(std::move(o));
    sink.open();
    std::vector<std::string> records;
    for (int i = 0; i < 1000; ++i) {
        records.push_back(std::to_string(i));
    }
    sink.on_data(batch(records));
    sink.on_barrier(kBarrier);
    sink.close();
    EXPECT_EQ(t.bodies(q), records);
    t.remove(q);
}

// A queue at x-max-length with x-overflow=reject-publish nacks what it cannot
// take, at once, while it confirms the persistent messages it took once they
// are on disk, so the nacks can arrive first: the flush reads on, fails
// saying how many were confirmed and refused, and the queue holds exactly the
// confirmed ones.
TEST(RabbitMqSinkContractLive, AFullQueueNacksPublishesAndTheSinkSaysHowManyWereConfirmed) {
    REQUIRE_RMQ();
    TestAmqp t;
    const auto q = unique("full");
    t.declare(q, 2);
    RabbitMqSink sink(sink_opts(q));
    sink.open();
    sink.on_data(batch({"a", "b", "c", "d", "e"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("the broker refused 3 message(s) (basic.nack)"), std::string::npos) << why;
    EXPECT_NE(why.find("of the 5 messages published since the last complete flush, 2 were "
                       "confirmed, 3 refused (basic.nack), and the other 0 are unconfirmed"),
              std::string::npos)
        << why;
    EXPECT_NE(thrown([&] { sink.on_data(batch({"f"})); }).find("failed earlier"),
              std::string::npos);
    EXPECT_NE(thrown([&] { sink.flush(); }).find("failed earlier"), std::string::npos);
    sink.close();
    EXPECT_EQ(t.bodies(q), (std::vector<std::string>{"a", "b"}));

    sink.open();
    sink.on_data(batch({"g"}));
    EXPECT_NO_THROW(sink.on_barrier(kBarrier));
    sink.close();
    EXPECT_EQ(t.bodies(q), (std::vector<std::string>{"g"}));
    t.remove(q);
}

TEST(RabbitMqSinkContractLive, UnroutableMandatoryMessagesAreReturnedAndCounted) {
    REQUIRE_RMQ();
    RabbitMqSink sink(sink_opts(unique("unbound")));
    sink.open();
    sink.on_data(batch({"a", "b"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("returned 2 message(s) that no queue is bound to receive"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("312 NO_ROUTE"), std::string::npos) << why;
    EXPECT_NE(why.find("0 were confirmed, 2 returned unroutable, and the other 0"),
              std::string::npos)
        << why;
    sink.close();
}

TEST(RabbitMqSinkContractLive, AnExchangeThatDoesNotExistClosesTheChannelAndFailsTheSink) {
    REQUIRE_RMQ();
    auto o = sink_opts("rk");
    o.exchange = unique("missing_exchange");
    RabbitMqSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({"a"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("the broker closed the channel: 404 NOT_FOUND"), std::string::npos) << why;
    EXPECT_NE(why.find("the other 1 are unconfirmed"), std::string::npos) << why;
    EXPECT_NE(thrown([&] { sink.on_data(batch({"b"})); }).find("failed earlier"),
              std::string::npos);
    const auto t0 = std::chrono::steady_clock::now();
    sink.close();
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds{1});
}

TEST(RabbitMqSinkContractLive, ARecordAboveMaxRecordBytesIsRefusedBeforeItIsPublished) {
    REQUIRE_RMQ();
    TestAmqp t;
    const auto q = unique("big");
    t.declare(q);
    auto o = sink_opts(q);
    o.max_record_bytes = 1024;
    RabbitMqSink sink(std::move(o));
    sink.open();
    const auto why = thrown([&] { sink.on_data(batch({std::string(2048, 'x')})); });
    EXPECT_NE(why.find("a record is 2048 bytes, more than max_record_bytes (1024)"),
              std::string::npos)
        << why;
    sink.close();
    EXPECT_EQ(t.count(q), 0U);
    t.remove(q);
}

// Confirms that do not come within confirm_timeout fail the flush with the
// messages unconfirmed, and they are only that: once the network carries
// again, the broker enqueues the messages the sink gave up on.
TEST(RabbitMqSinkContractLive, ConfirmsThatDoNotComeLeaveTheMessagesUnknownAndTheyMayLand) {
    REQUIRE_RMQ();
    TestAmqp t;
    const auto q = unique("timeout");
    t.declare(q);
    clink::itest::TcpFaultProxy proxy(static_cast<std::uint16_t>(port()), host());
    auto o = proxied_opts(q, proxy);
    o.confirm_timeout = std::chrono::milliseconds{500};
    RabbitMqSink sink(std::move(o));
    sink.open();
    proxy.set_blackhole(true);
    sink.on_data(batch({"a", "b", "c"}));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_GE(took, std::chrono::milliseconds{500});
    EXPECT_LT(took, std::chrono::seconds{3});
    EXPECT_NE(why.find("within the confirm timeout (500 ms)"), std::string::npos) << why;
    EXPECT_NE(why.find("0 were confirmed, and the other 3 are unconfirmed and may or may not"),
              std::string::npos)
        << why;
    proxy.set_blackhole(false);
    EXPECT_TRUE(eventually([&] { return t.count(q) == 3; }))
        << "the unconfirmed messages reached the queue once the network carried again";
    sink.close();
    t.remove(q);
}

TEST(RabbitMqSinkContractLive, ACancelEndsAWaitForConfirmsOnASilentBroker) {
    REQUIRE_RMQ();
    TestAmqp t;
    const auto q = unique("cancel_wait");
    t.declare(q);
    clink::itest::TcpFaultProxy proxy(static_cast<std::uint16_t>(port()), host());
    RabbitMqSink sink(proxied_opts(q, proxy));  // waits up to 30 s for confirms
    Attached a(sink);
    sink.open();
    proxy.set_blackhole(true);
    sink.on_data(batch({"a"}));
    a.cancel_after(std::chrono::milliseconds{300});
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    a.canceller.join();
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds{2});
    EXPECT_NE(why.find("the task was cancelled while waiting for a flush's confirms"),
              std::string::npos)
        << why;
    const auto c0 = std::chrono::steady_clock::now();
    EXPECT_NO_THROW(sink.close());
    EXPECT_LT(std::chrono::steady_clock::now() - c0, std::chrono::seconds{1});
    proxy.set_blackhole(false);
    t.remove(q);
}

// A broker that stops reading (a memory or disk alarm blocks publishing
// connections this way) holds a publish once the socket's buffers are full;
// librabbitmq writes with no deadline, so only the cancel watch ends it.
TEST(RabbitMqSinkContractLive, ACancelEndsAPublishTheBrokerHasStoppedReading) {
    REQUIRE_RMQ();
    TestAmqp t;
    const auto q = unique("cancel_publish");
    t.declare(q);
    clink::itest::TcpFaultProxy proxy(static_cast<std::uint16_t>(port()), host());
    RabbitMqSink sink(proxied_opts(q, proxy));
    Attached a(sink);
    sink.open();
    proxy.set_blackhole(true);
    std::vector<std::string> records(64, std::string(1U << 20, 'x'));
    a.cancel_after(std::chrono::milliseconds{1000});
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_data(batch(records)); });
    a.canceller.join();
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_GE(took, std::chrono::milliseconds{900}) << "the publishes never blocked";
    EXPECT_LT(took, std::chrono::seconds{4});
    EXPECT_NE(why.find("the task was cancelled publishing to"), std::string::npos) << why;
    EXPECT_NO_THROW(sink.close());
    proxy.set_blackhole(false);
    t.remove(q);
}

// A peer that accepts the connection and says nothing holds the handshake:
// connect_timeout bounds it.
TEST(RabbitMqSinkContractLive, AHandshakeThatStallsFailsAtTheConnectTimeout) {
    REQUIRE_RMQ();
    clink::itest::TcpFaultProxy proxy(static_cast<std::uint16_t>(port()), host());
    proxy.set_blackhole(true);
    auto o = proxied_opts("rk", proxy);
    o.conn.connect_timeout = std::chrono::milliseconds{500};
    RabbitMqSink sink(std::move(o));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.open(); });
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_FALSE(why.empty());
    EXPECT_LT(took, std::chrono::seconds{3}) << why;
    EXPECT_NE(thrown([&] { sink.on_data(batch({"a"})); }).find("before open()"), std::string::npos);
}

TEST(RabbitMqSinkContractLive, TheFactoryBuildsASinkThatPublishes) {
    REQUIRE_RMQ();
    TestAmqp t;
    const auto q = unique("factory");
    t.declare(q);
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "rabbitmq_sink_string", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"host", host()},
                  {"port", std::to_string(port())},
                  {"user", env_or("CLINK_RABBITMQ_TEST_USER", "guest")},
                  {"password", env_or("CLINK_RABBITMQ_TEST_PASSWORD", "guest")},
                  {"routing_key", q},
                  {"max_unconfirmed", "2"},
                  {"confirm_timeout_ms", "5000"}};
    auto sink = std::static_pointer_cast<clink::Sink<std::string>>(f->build(ctx));
    sink->open();
    sink->on_data(batch({R"({"id":1})", R"({"id":2})", R"({"id":3})"}));
    sink->on_barrier(kBarrier);
    sink->close();
    EXPECT_EQ(t.bodies(q), (std::vector<std::string>{R"({"id":1})", R"({"id":2})", R"({"id":3})"}));
    t.remove(q);
}
