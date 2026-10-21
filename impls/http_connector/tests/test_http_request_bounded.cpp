// HttpRequest::post_bounded and interrupt(): the reply is read up to a bound
// and no further, a failed exchange says whether the whole request had been
// written, and another thread can end an exchange that is waiting on the
// server. Against in-process servers: an httplib server for replies, and a raw
// listener for a peer that reads the request and then never answers.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <httplib.h>
#include <string>
#include <thread>
#include <unistd.h>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "clink/http_connector/http_request.hpp"

using clink::http_connector::BoundedRequest;
using clink::http_connector::BoundedResponse;
using clink::http_connector::HttpRequest;
using Failure = BoundedResponse::Failure;
using namespace std::chrono_literals;

namespace {

class ReplyServer {
public:
    ReplyServer() {
        svr_.Post("/echo", [this](const httplib::Request& req, httplib::Response& res) {
            last_body_ = req.body;
            last_content_type_ = req.get_header_value("Content-Type");
            last_accept_ = req.get_header_value("Accept");
            res.set_header("X-Mixed-Case", "yes");
            res.status = 202;
            res.set_content("{\"ok\":true}", "application/json");
        });
        svr_.Post("/big", [](const httplib::Request&, httplib::Response& res) {
            res.status = 202;
            res.set_content(std::string(64 * 1024, 'x'), "application/json");
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(2ms);
        }
    }
    ~ReplyServer() {
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }
    [[nodiscard]] int port() const { return port_; }
    std::string last_body_;
    std::string last_content_type_;
    std::string last_accept_;

private:
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
};

// Accepts one connection, reads the request until the peer has sent
// `read_bytes`, then either closes it (reset) or holds it open without
// answering until destroyed.
class SilentPeer {
public:
    enum class After { Close, Hold };
    explicit SilentPeer(After after) : after_(after) {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        EXPECT_EQ(::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
        EXPECT_EQ(::listen(fd_, 4), 0);
        socklen_t len = sizeof(addr);
        ::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this] { run(); });
    }
    ~SilentPeer() {
        stop_.store(true);
        ::shutdown(fd_, SHUT_RDWR);
        ::close(fd_);
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }
    [[nodiscard]] std::size_t received() const { return received_.load(); }

private:
    void run() {
        const int c = ::accept(fd_, nullptr, nullptr);
        if (c < 0) {
            return;
        }
        char buf[4096];
        // Read until the request's blank line and body have arrived: the
        // client sends both in one go, so a short quiet spell ends it.
        timeval tv{0, 200 * 1000};
        ::setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!stop_.load()) {
            const ssize_t n = ::recv(c, buf, sizeof(buf), 0);
            if (n <= 0) {
                break;
            }
            received_.fetch_add(static_cast<std::size_t>(n));
        }
        if (after_ == After::Hold) {
            while (!stop_.load()) {
                std::this_thread::sleep_for(10ms);
            }
        }
        linger lg{1, 0};
        ::setsockopt(c, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
        ::close(c);
    }
    After after_;
    int fd_{-1};
    int port_{0};
    std::thread thread_;
    std::atomic<bool> stop_{false};
    std::atomic<std::size_t> received_{0};
};

// A port nothing listens on: bound, then closed again.
int unused_port() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    const int port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

HttpRequest client_for(const std::string& url, int rw_timeout_ms = 30000) {
    HttpRequest::Options o;
    o.base_url = url;
    o.connect_timeout_ms = 2000;
    o.rw_timeout_ms = rw_timeout_ms;
    return HttpRequest(std::move(o));
}

}  // namespace

TEST(HttpRequestBounded, ACompleteReplyKeepsItsStatusHeadersAndBody) {
    ReplyServer server;
    auto client = client_for(server.url());
    BoundedRequest req;
    req.headers["Accept"] = "application/vnd.test";
    const auto r = client.post_bounded("/echo", "{\"a\":1}", "application/vnd.test", req);
    EXPECT_TRUE(r.complete);
    EXPECT_EQ(r.failure, Failure::None);
    EXPECT_TRUE(r.request_written);
    EXPECT_FALSE(r.interrupted);
    EXPECT_EQ(r.status, 202);
    EXPECT_EQ(r.body, "{\"ok\":true}");
    ASSERT_TRUE(r.headers.contains("x-mixed-case"));
    EXPECT_EQ(r.headers.at("x-mixed-case"), "yes");
    EXPECT_EQ(server.last_body_, "{\"a\":1}");
    EXPECT_EQ(server.last_content_type_, "application/vnd.test");
    EXPECT_EQ(server.last_accept_, "application/vnd.test");
}

TEST(HttpRequestBounded, AReplyPastTheBoundStopsBeingRead) {
    ReplyServer server;
    auto client = client_for(server.url());
    BoundedRequest req;
    req.max_response_bytes = 1000;
    const auto r = client.post_bounded("/big", "{}", "application/json", req);
    EXPECT_FALSE(r.complete);
    EXPECT_EQ(r.failure, Failure::TooLarge);
    EXPECT_TRUE(r.request_written) << "the server answered, so it had the whole request";
    EXPECT_EQ(r.status, 202) << "the status line arrived before the body";
    EXPECT_LE(r.body.size(), 1000U);
    EXPECT_NE(r.error.find("1000"), std::string::npos) << r.error;
    // The client stays usable: the next exchange opens a fresh connection.
    const auto again = client.post_bounded("/echo", "{}", "application/json", BoundedRequest{});
    EXPECT_TRUE(again.complete);
    EXPECT_EQ(again.status, 202);
}

TEST(HttpRequestBounded, ARefusedConnectionWroteNothing) {
    auto client = client_for("http://127.0.0.1:" + std::to_string(unused_port()));
    const auto r = client.post_bounded("/echo", "{}", "application/json", BoundedRequest{});
    EXPECT_FALSE(r.complete);
    EXPECT_EQ(r.failure, Failure::Connect);
    EXPECT_FALSE(r.request_written);
    EXPECT_EQ(r.status, 0);
    EXPECT_FALSE(r.error.empty());
}

TEST(HttpRequestBounded, AReplyLostAfterTheRequestWasWrittenSaysSo) {
    SilentPeer peer(SilentPeer::After::Close);
    auto client = client_for(peer.url());
    const std::string body(2000, 'b');
    const auto r = client.post_bounded("/echo", body, "application/json", BoundedRequest{});
    EXPECT_FALSE(r.complete);
    EXPECT_EQ(r.failure, Failure::Read);
    EXPECT_TRUE(r.request_written);
    EXPECT_EQ(r.status, 0) << "no status line arrived";
    EXPECT_GE(peer.received(), body.size()) << "the peer did receive the whole request";
}

TEST(HttpRequestBounded, InterruptEndsAnExchangeWaitingOnTheServer) {
    SilentPeer peer(SilentPeer::After::Hold);
    auto client = client_for(peer.url(), 30000);
    std::atomic<bool> done{false};
    BoundedResponse r;
    const auto start = std::chrono::steady_clock::now();
    std::thread caller([&] {
        r = client.post_bounded("/echo", "{}", "application/json", BoundedRequest{});
        done.store(true);
    });
    // Let the request reach the peer and the client settle into its read.
    std::this_thread::sleep_for(500ms);
    EXPECT_FALSE(done.load()) << "the peer never answers, so the call must still be waiting";
    client.interrupt();
    caller.join();
    const auto took = std::chrono::steady_clock::now() - start;
    EXPECT_LT(took, 5s) << "the call waited for its 30 s read timeout instead";
    EXPECT_FALSE(r.complete);
    EXPECT_TRUE(r.interrupted);
    EXPECT_TRUE(r.request_written);
}

TEST(HttpRequestBounded, AnInterruptWithNothingInFlightLeavesTheClientUsable) {
    ReplyServer server;
    auto client = client_for(server.url());
    client.interrupt();
    const auto r = client.post_bounded("/echo", "{}", "application/json", BoundedRequest{});
    EXPECT_TRUE(r.complete) << r.error;
    EXPECT_FALSE(r.interrupted) << "a new exchange starts without the earlier interrupt";
}

TEST(HttpRequestBounded, ACaFileThatDoesNotLoadFailsBeforeTheRequestIsWritten) {
    if (!HttpRequest::tls_supported()) {
        GTEST_SKIP() << "this build has no TLS";
    }
    ReplyServer server;  // plain HTTP: the client gets no further than its CA file
    HttpRequest::Options o;
    o.base_url = "https://127.0.0.1:" + std::to_string(server.port());
    o.connect_timeout_ms = 2000;
    o.ca_cert_path = "/nonexistent/clink-test-ca.pem";
    HttpRequest client(std::move(o));
    const auto r = client.post_bounded("/echo", "{}", "application/json", BoundedRequest{});
    EXPECT_FALSE(r.complete);
    EXPECT_FALSE(r.request_written);
    EXPECT_TRUE(r.failure == Failure::TlsVerify || r.failure == Failure::TlsHandshake)
        << "failure " << static_cast<int>(r.failure) << ": " << r.error;
    EXPECT_TRUE(server.last_body_.empty()) << "nothing reached the server";
}
