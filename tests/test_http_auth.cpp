// Token auth on the HTTP control plane (roadmap F6). A server started with a
// token (CLINK_AUTH_TOKEN in production) answers 401 to any request without a
// matching `Authorization: Bearer <token>`, and serves the handler otherwise;
// with no token configured, auth is off (backward compatible).

#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/socket.h>

#include "clink/http/http_client.hpp"
#include "clink/http/http_server.hpp"

using namespace clink::http;

namespace {

HttpResponse pong(const HttpRequest&) {
    HttpResponse r;
    r.status = 200;
    r.content_type = "text/plain";
    r.body = "pong";
    return r;
}

// Holds descriptors open until every new one the process creates lands at or
// above FD_SETSIZE, raising the soft RLIMIT_NOFILE to make room. ok() is false
// when the hard limit leaves no room, which the caller skips on.
class DescriptorsAboveFdSetsize {
public:
    DescriptorsAboveFdSetsize() {
        constexpr rlim_t kWant = FD_SETSIZE + 256;
        if (::getrlimit(RLIMIT_NOFILE, &saved_) != 0) {
            return;
        }
        if (saved_.rlim_cur < kWant) {
            if (saved_.rlim_max != RLIM_INFINITY && saved_.rlim_max < kWant) {
                return;
            }
            rlimit raised = saved_;
            raised.rlim_cur = kWant;
            if (::setrlimit(RLIMIT_NOFILE, &raised) != 0) {
                return;
            }
            restore_ = true;
        }
        for (;;) {
            const int fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
            if (fd < 0) {
                return;
            }
            if (fd >= FD_SETSIZE) {
                ::close(fd);  // the next descriptor is this one again
                ok_ = true;
                return;
            }
            held_.push_back(fd);
        }
    }
    ~DescriptorsAboveFdSetsize() {
        for (const int fd : held_) {
            ::close(fd);
        }
        if (restore_) {
            ::setrlimit(RLIMIT_NOFILE, &saved_);
        }
    }
    DescriptorsAboveFdSetsize(const DescriptorsAboveFdSetsize&) = delete;
    DescriptorsAboveFdSetsize& operator=(const DescriptorsAboveFdSetsize&) = delete;
    DescriptorsAboveFdSetsize(DescriptorsAboveFdSetsize&&) = delete;
    DescriptorsAboveFdSetsize& operator=(DescriptorsAboveFdSetsize&&) = delete;

    [[nodiscard]] bool ok() const { return ok_; }

private:
    rlimit saved_{};
    bool restore_{false};
    bool ok_{false};
    std::vector<int> held_;
};

// One plain HTTP/1.1 GET over a raw socket, returning the status line, or an
// empty string if no response came back. Raw so that only the server side
// is under test here.
std::string raw_get_status_line(std::uint16_t port, const char* path) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return {};
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    // A server that never answers fails the test rather than hanging it.
    timeval bound{};
    bound.tv_sec = 10;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &bound, sizeof(bound));
    std::string response;
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
        const std::string req = std::string{"GET "} + path +
                                " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
        if (::send(fd, req.data(), req.size(), 0) == static_cast<ssize_t>(req.size())) {
            char buf[512];
            ssize_t n = 0;
            while (response.find("\r\n") == std::string::npos &&
                   (n = ::recv(fd, buf, sizeof(buf), 0)) > 0) {
                response.append(buf, static_cast<std::size_t>(n));
            }
        }
    }
    ::close(fd);
    return response.substr(0, response.find("\r\n"));
}

}  // namespace

TEST(HttpAuth, TokenGateRejectsUnauthenticatedAndAllowsBearer) {
    HttpServer server;
    server.get("/ping", pong);
    server.set_auth_token("s3cret");
    const std::uint16_t port = server.start("127.0.0.1", 0);

    // No Authorization header -> 401 before the handler runs.
    {
        HttpClient c("127.0.0.1", port);
        const auto r = c.get("/ping");
        EXPECT_EQ(r.status, 401) << r.body;
        EXPECT_NE(r.body, "pong");
    }
    // Wrong token -> 401.
    {
        HttpClient c("127.0.0.1", port);
        c.set_bearer_token("wrong");
        const auto r = c.get("/ping");
        EXPECT_EQ(r.status, 401) << r.body;
    }
    // Correct token -> the handler runs.
    {
        HttpClient c("127.0.0.1", port);
        c.set_bearer_token("s3cret");
        const auto r = c.get("/ping");
        EXPECT_EQ(r.status, 200) << r.body;
        EXPECT_EQ(r.body, "pong");
    }

    server.stop();
}

TEST(HttpAuth, NoTokenConfiguredLeavesAuthOff) {
    HttpServer server;
    server.get("/ping", pong);
    // No set_auth_token: unauthenticated requests are served (trusted-network
    // default, unchanged behaviour).
    const std::uint16_t port = server.start("127.0.0.1", 0);

    HttpClient c("127.0.0.1", port);
    const auto r = c.get("/ping");
    EXPECT_EQ(r.status, 200) << r.body;
    EXPECT_EQ(r.body, "pong");

    server.stop();
}

// The start/stop race that wedged a 0.05s test for its full 180s ctest
// bound in CI (watch item 64): httplib's Server::stop() is a NO-OP until
// the listen thread has set is_running_, so a stop() outrunning a
// slow-starting listen thread was silently lost and the join hung on an
// accept() nobody would ever wake. stop() now waits for the listen loop
// to be ready before stopping it, which makes this hammer deterministic:
// every cycle must complete, including the ones where stop() wins the
// old race (the first iterations, before the thread scheduler warms up,
// are exactly the hostile interleaving). Pre-fix this wedges within a
// handful of cycles on a loaded machine; the ctest timeout is the
// failure detector.
TEST(HttpLifecycle, ImmediateStopAfterStartNeverWedges) {
    for (int i = 0; i < 200; ++i) {
        HttpServer server;
        server.get("/ping", pong);
        (void)server.start("127.0.0.1", 0);
        server.stop();  // as close to start() as the code can express
    }
}

// The HTTP subsystem must keep working once descriptor numbers pass
// FD_SETSIZE, which they do on any long-running node: clink_node raises
// RLIMIT_NOFILE and a busy worker holds many sockets. httplib's default
// select() cannot take such a descriptor, and instead of waiting it gives up:
// the server answers 500 to the connection, the client cannot read its
// response, and the listen loop skips the bounded wait that keeps stop()
// out of the Darwin close-versus-accept race, going straight to a blocking
// accept(). The build defines CPPHTTPLIB_USE_POLL so httplib waits with
// poll(), which has no such limit. Here every descriptor the server and
// client open - listener, accepted socket, client socket - is above it.
TEST(HttpLifecycle, ServesWhenDescriptorsAreAboveFdSetsize) {
    DescriptorsAboveFdSetsize high;
    if (!high.ok()) {
        GTEST_SKIP() << "RLIMIT_NOFILE leaves no room for descriptors above FD_SETSIZE";
    }
    HttpServer server;
    server.get("/ping", pong);
    const std::uint16_t port = server.start("127.0.0.1", 0);

    EXPECT_EQ(raw_get_status_line(port, "/ping"), "HTTP/1.1 200 OK");

    HttpClient c("127.0.0.1", port);
    const auto r = c.get("/ping");
    EXPECT_EQ(r.status, 200) << r.error;
    EXPECT_EQ(r.body, "pong");

    server.stop();
}
