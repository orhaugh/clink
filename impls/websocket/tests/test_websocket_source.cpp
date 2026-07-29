// WebSocketSource end to end against an in-process WebSocket server: a raw
// listening socket that performs the server side of the RFC 6455 opening
// handshake and then runs a per-test script of frames. CI-runnable, no
// external endpoint. The protocol layer itself is pinned to the RFC's own
// byte examples in test_ws_protocol.cpp, so this file is free to reuse it
// on the server side without the tests becoming self-referential.

#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <optional>
#include <poll.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "clink/operators/operator_base.hpp"
#include "clink/websocket/websocket_source.hpp"
#include "clink/websocket/ws_protocol.hpp"

namespace {

using namespace clink::websocket;
using namespace std::chrono_literals;

// ---- minimal in-process WebSocket server ---------------------------------

class TestWsServer {
public:
    // The script runs once per accepted connection, in accept order.
    using Script = std::function<void(TestWsServer&, int fd)>;

    explicit TestWsServer(std::vector<Script> connection_scripts)
        : scripts_(std::move(connection_scripts)) {
        listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_OK(listener_ >= 0, "socket");
        int one = 1;
        ::setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        struct sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ASSERT_OK(::bind(listener_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0,
                  "bind");
        socklen_t len = sizeof(addr);
        ::getsockname(listener_, reinterpret_cast<struct sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        ASSERT_OK(::listen(listener_, 4) == 0, "listen");
        thread_ = std::thread([this] { run_(); });
    }

    ~TestWsServer() {
        stop_.store(true);
        if (listener_ >= 0) {
            ::shutdown(listener_, SHUT_RDWR);
            ::close(listener_);
        }
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    [[nodiscard]] std::uint16_t port() const { return port_; }
    [[nodiscard]] std::string url() const {
        return "ws://127.0.0.1:" + std::to_string(port_) + "/feed";
    }

    // ---- helpers for scripts ----

    // Complete the server side of the opening handshake; returns false on
    // any read failure (the test then fails via its own assertions).
    bool handshake(int fd) {
        std::string req;
        char buf[4096];
        while (req.find("\r\n\r\n") == std::string::npos) {
            const auto n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0) {
                return false;
            }
            req.append(buf, static_cast<std::size_t>(n));
            if (req.size() > 64 * 1024) {
                return false;
            }
        }
        const std::string marker = "Sec-WebSocket-Key: ";
        const auto at = req.find(marker);
        if (at == std::string::npos) {
            return false;
        }
        const auto end = req.find("\r\n", at);
        const std::string key = req.substr(at + marker.size(), end - (at + marker.size()));
        const std::string resp =
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Accept: " +
            accept_key_for(key) + "\r\n\r\n";
        return send_raw(fd, resp);
    }

    bool send_raw(int fd, const std::string& data) {
        std::size_t off = 0;
        while (off < data.size()) {
#ifdef MSG_NOSIGNAL
            const auto n = ::send(fd, data.data() + off, data.size() - off, MSG_NOSIGNAL);
#else
            const auto n = ::send(fd, data.data() + off, data.size() - off, 0);
#endif
            if (n <= 0) {
                return false;
            }
            off += static_cast<std::size_t>(n);
        }
        return true;
    }

    bool send_frame(int fd, Opcode op, const std::string& payload, bool fin = true) {
        return send_raw(fd, encode_frame(op, payload, /*mask=*/false, 0, fin));
    }

    // Read one message from the client (client frames arrive masked; the
    // decoder unmasks). nullopt on socket close/timeout.
    std::optional<MessageReader::Message> read_message(int fd) {
        std::string error;
        while (true) {
            if (auto msg = reader_.next_message(error); msg.has_value()) {
                return msg;
            }
            if (!error.empty()) {
                return std::nullopt;
            }
            char buf[4096];
            const auto n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0) {
                return std::nullopt;
            }
            reader_.append(buf, static_cast<std::size_t>(n));
        }
    }

    // Observability for assertions after the run.
    std::vector<std::string> client_texts;  // text payloads the scripts read
    std::vector<std::string> client_pongs;  // pong payloads the scripts read

private:
    static void ASSERT_OK(bool ok, const char* what) {
        if (!ok) {
            FAIL() << what << " failed: " << std::strerror(errno);
        }
    }

    void run_() {
        for (auto& script : scripts_) {
            struct pollfd pfd = {};
            pfd.fd = listener_;
            pfd.events = POLLIN;
            if (::poll(&pfd, 1, 15'000) <= 0 || stop_.load()) {
                return;
            }
            const int fd = ::accept(listener_, nullptr, nullptr);
            if (fd < 0) {
                return;
            }
            struct timeval tv = {};
            tv.tv_sec = 10;
            ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
#ifdef SO_NOSIGPIPE
            int one = 1;
            ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
            reader_ = MessageReader{};  // fresh per connection
            script(*this, fd);
            ::close(fd);
        }
    }

    std::vector<Script> scripts_;
    int listener_{-1};
    std::uint16_t port_{0};
    std::thread thread_;
    std::atomic<bool> stop_{false};
    MessageReader reader_;
};

// ---- driving the source ---------------------------------------------------

struct Collected {
    std::vector<std::string> values;
    bool saw_max_watermark{false};
};

clink::Emitter<std::string> emitter_into(Collected& out) {
    return clink::Emitter<std::string>([&out](clink::StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                out.values.push_back(rec.value());
            }
        } else if (e.is_watermark() && e.as_watermark() == clink::Watermark::max()) {
            out.saw_max_watermark = true;
        }
        return true;
    });
}

// Run produce() until the source reports exhaustion or the deadline hits.
void drive(WebSocketSource& src,
           clink::Emitter<std::string>& em,
           std::chrono::seconds deadline = 15s) {
    const auto until = std::chrono::steady_clock::now() + deadline;
    while (src.produce(em)) {
        ASSERT_LT(std::chrono::steady_clock::now(), until) << "source did not finish in time";
    }
}

WebSocketSourceOptions base_options(const TestWsServer& server) {
    WebSocketSourceOptions o;
    o.url = server.url();
    o.block = 200ms;
    o.open_timeout = 5s;
    o.reconnect_backoff_max = 200ms;
    return o;
}

// ---- tests -----------------------------------------------------------------

TEST(WebSocketSource, EmitsTextMessagesInOrder) {
    TestWsServer server({[](TestWsServer& s, int fd) {
        ASSERT_TRUE(s.handshake(fd));
        s.send_frame(fd, Opcode::Text, R"({"seq":1})");
        s.send_frame(fd, Opcode::Text, R"({"seq":2})");
        s.send_frame(fd, Opcode::Text, R"({"seq":3})");
        s.send_frame(fd, Opcode::Close, {});
    }});

    auto opts = base_options(server);
    opts.max_messages = 3;
    WebSocketSource src(std::move(opts));
    src.open();
    Collected got;
    auto em = emitter_into(got);
    drive(src, em);
    src.close();

    EXPECT_EQ(got.values,
              (std::vector<std::string>{R"({"seq":1})", R"({"seq":2})", R"({"seq":3})"}));
    EXPECT_TRUE(got.saw_max_watermark) << "a bounded run must close event time";
}

TEST(WebSocketSource, SubscribeIsSentAfterEveryConnect) {
    const std::string sub = R"({"op":"subscribe","channel":"trades"})";
    TestWsServer server({
        [&](TestWsServer& s, int fd) {
            ASSERT_TRUE(s.handshake(fd));
            auto msg = s.read_message(fd);
            ASSERT_TRUE(msg.has_value());
            s.client_texts.push_back(msg->payload);
            s.send_frame(fd, Opcode::Text, "a");
            // Hard drop, no close frame: the client must treat it as a lost
            // connection and re-dial.
        },
        [&](TestWsServer& s, int fd) {
            ASSERT_TRUE(s.handshake(fd));
            auto msg = s.read_message(fd);
            ASSERT_TRUE(msg.has_value());
            s.client_texts.push_back(msg->payload);
            s.send_frame(fd, Opcode::Text, "b");
            s.send_frame(fd, Opcode::Text, "c");
            s.send_frame(fd, Opcode::Close, {});
        },
    });

    auto opts = base_options(server);
    opts.subscribe = sub;
    opts.max_messages = 3;
    WebSocketSource src(std::move(opts));
    src.open();
    Collected got;
    auto em = emitter_into(got);
    drive(src, em);
    src.close();

    EXPECT_EQ(got.values, (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(server.client_texts, (std::vector<std::string>{sub, sub}))
        << "the subscription must be re-sent on reconnect";
}

TEST(WebSocketSource, AnswersServerPingsWithMatchingPongs) {
    TestWsServer server({[](TestWsServer& s, int fd) {
        ASSERT_TRUE(s.handshake(fd));
        s.send_frame(fd, Opcode::Ping, "keepalive-7");
        auto msg = s.read_message(fd);
        ASSERT_TRUE(msg.has_value());
        ASSERT_EQ(msg->opcode, Opcode::Pong);
        s.client_pongs.push_back(msg->payload);
        s.send_frame(fd, Opcode::Text, "done");
        s.send_frame(fd, Opcode::Close, {});
    }});

    auto opts = base_options(server);
    opts.max_messages = 1;
    WebSocketSource src(std::move(opts));
    src.open();
    Collected got;
    auto em = emitter_into(got);
    drive(src, em);
    src.close();

    EXPECT_EQ(got.values, (std::vector<std::string>{"done"}));
    EXPECT_EQ(server.client_pongs, (std::vector<std::string>{"keepalive-7"}))
        << "RFC 6455: a pong must echo the ping payload";
}

TEST(WebSocketSource, ServerCloseWithoutReconnectEndsTheStream) {
    TestWsServer server({[](TestWsServer& s, int fd) {
        ASSERT_TRUE(s.handshake(fd));
        s.send_frame(fd, Opcode::Text, "x");
        s.send_frame(fd, Opcode::Text, "y");
        s.send_frame(fd, Opcode::Close, "bye");
    }});

    auto opts = base_options(server);
    opts.reconnect = false;  // an unbounded source, ended by the server
    WebSocketSource src(std::move(opts));
    src.open();
    Collected got;
    auto em = emitter_into(got);
    drive(src, em);
    src.close();

    EXPECT_EQ(got.values, (std::vector<std::string>{"x", "y"}));
    EXPECT_TRUE(got.saw_max_watermark);
}

TEST(WebSocketSource, BinaryMessagesAreSkippedNotEmitted) {
    TestWsServer server({[](TestWsServer& s, int fd) {
        ASSERT_TRUE(s.handshake(fd));
        s.send_frame(fd, Opcode::Binary, std::string("\x01\x02\x03", 3));
        s.send_frame(fd, Opcode::Text, "text-wins");
        s.send_frame(fd, Opcode::Close, {});
    }});

    auto opts = base_options(server);
    opts.max_messages = 1;
    WebSocketSource src(std::move(opts));
    src.open();
    Collected got;
    auto em = emitter_into(got);
    drive(src, em);
    src.close();

    EXPECT_EQ(got.values, (std::vector<std::string>{"text-wins"}));
}

TEST(WebSocketSource, FragmentedMessageArrivesWhole) {
    TestWsServer server({[](TestWsServer& s, int fd) {
        ASSERT_TRUE(s.handshake(fd));
        s.send_raw(fd, encode_frame(Opcode::Text, R"({"px":)", false, 0, /*fin=*/false));
        s.send_raw(fd, encode_frame(Opcode::Continuation, "42.5}", false, 0, /*fin=*/true));
        s.send_frame(fd, Opcode::Close, {});
    }});

    auto opts = base_options(server);
    opts.max_messages = 1;
    WebSocketSource src(std::move(opts));
    src.open();
    Collected got;
    auto em = emitter_into(got);
    drive(src, em);
    src.close();

    EXPECT_EQ(got.values, (std::vector<std::string>{R"({"px":42.5})"}));
}

TEST(WebSocketSource, DormantNonZeroSubtaskNeverConnects) {
    WebSocketSourceOptions opts;
    opts.url = "ws://127.0.0.1:1/never-dialled";  // would fail if dialled
    opts.subtask_idx = 1;
    opts.parallelism = 2;
    WebSocketSource src(std::move(opts));
    EXPECT_TRUE(src.dormant());
    src.open();  // no connection attempt
    Collected got;
    auto em = emitter_into(got);
    EXPECT_TRUE(src.produce(em));  // idles
    src.cancel();
    EXPECT_FALSE(src.produce(em));
    EXPECT_TRUE(got.values.empty());
}

TEST(WebSocketSource, MalformedUrlIsRejectedAtConstruction) {
    WebSocketSourceOptions opts;
    opts.url = "http://not-a-websocket/";
    EXPECT_THROW(WebSocketSource{std::move(opts)}, std::runtime_error);
}

TEST(WebSocketSource, OpenFailsFastOnARefusedConnection) {
    WebSocketSourceOptions opts;
    // Loopback port 1 is reliably closed for unprivileged use: the connect
    // is refused immediately, and open() must surface that as an error
    // rather than sitting out its timeout.
    opts.url = "ws://127.0.0.1:1/";
    opts.open_timeout = 2s;
    WebSocketSource src(std::move(opts));
    EXPECT_THROW(src.open(), std::runtime_error);
}

}  // namespace
