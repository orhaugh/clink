// Phase 28e: io_uring reactor smoke + integration tests.
//
// Linux-only; conditionally compiled. When CLINK_HAS_URING isn't
// defined (macOS, or Linux without liburing), the file collapses to
// a single SUCCESS test so the binary still has at least one
// TEST_F-discovered entry.

#if defined(__linux__) && defined(CLINK_HAS_URING)

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "clink/async/io_uring_reactor.hpp"
#include "clink/async/task.hpp"

using namespace clink::async;
using namespace std::chrono_literals;

namespace {

// Attempt to construct a reactor; on failure (typically because
// io_uring is blocked by a container's seccomp profile, or the
// kernel is older than 5.1), GTEST_SKIP the calling test rather
// than fail. liburing's queue_init throws via the constructor.
[[nodiscard]] std::unique_ptr<IoUringReactor> try_make_reactor() {
    try {
        return std::make_unique<IoUringReactor>(/*queue_depth=*/256);
    } catch (const std::runtime_error& e) {
        // Returning nullptr lets the test SKIP with the message;
        // EXPECT-style helpers don't work inside a constructor.
        return nullptr;
    }
}

// Launch the reactor on its own thread for tests. The factory
// returns a (reactor*, thread) pair; teardown calls reactor->stop()
// then joins.
struct ReactorThread {
    std::unique_ptr<IoUringReactor> reactor;
    std::thread loop;

    explicit ReactorThread(std::unique_ptr<IoUringReactor> r) : reactor(std::move(r)) {
        if (reactor) {
            loop = std::thread([this] { reactor->run(); });
        }
    }

    ~ReactorThread() {
        if (reactor) {
            reactor->stop();
        }
        if (loop.joinable()) {
            loop.join();
        }
    }

    [[nodiscard]] bool valid() const noexcept { return static_cast<bool>(reactor); }
    IoUringReactor& operator*() { return *reactor; }
    IoUringReactor* operator->() { return reactor.get(); }
};

// Helper macro: build a reactor or skip the test.
#define MAKE_REACTOR_OR_SKIP(rt_var)                                                   \
    ReactorThread rt_var(try_make_reactor());                                          \
    if (!rt_var.valid()) {                                                             \
        GTEST_SKIP() << "io_uring not available (kernel too old or seccomp blocked); " \
                        "skipping io_uring tests";                                     \
    }

// Coroutine that suspends on the reactor for `ms`, then co_returns
// the elapsed milliseconds as observed by the awaiting end.
Task<long> sleep_and_measure(IoUringReactor& reactor, std::chrono::milliseconds ms) {
    const auto start = std::chrono::steady_clock::now();
    co_await reactor.sleep_for(ms);
    const auto elapsed = std::chrono::steady_clock::now() - start;
    co_return std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
}

}  // namespace

TEST(IoUringReactor, ConstructsAndStopsCleanly) {
    // Most basic: build a reactor, run the loop, stop it. Verifies
    // io_uring_queue_init + queue_exit and the NOP-wakeup stop path.
    MAKE_REACTOR_OR_SKIP(rt);
    std::this_thread::sleep_for(5ms);  // let the loop thread enter wait_cqe
    // rt destructor calls stop + join.
}

TEST(IoUringReactor, SleepResumesAfterRoughlyTheRequestedDuration) {
    MAKE_REACTOR_OR_SKIP(rt);
    auto task = sleep_and_measure(*rt, 50ms);
    task.resume();
    // Spin briefly waiting for completion. The reactor's loop runs
    // on its own thread; the coroutine body resumes there and
    // returns to its caller's coroutine frame.
    const auto deadline = std::chrono::steady_clock::now() + 500ms;
    while (!task.done() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_TRUE(task.done()) << "sleep_for never completed";
    const long elapsed = task.get();
    // Slack against scheduler / timer-fire granularity. 50ms target,
    // accept 40-200ms.
    EXPECT_GE(elapsed, 40);
    EXPECT_LE(elapsed, 200) << "elapsed=" << elapsed << "ms is unreasonably late";
}

TEST(IoUringReactor, DiagnosticsCountSubmittedAndCompleted) {
    MAKE_REACTOR_OR_SKIP(rt);
    EXPECT_EQ(rt->ops_submitted(), 0u);
    EXPECT_EQ(rt->completions_handled(), 0u);

    auto task = sleep_and_measure(*rt, 10ms);
    task.resume();
    const auto deadline = std::chrono::steady_clock::now() + 500ms;
    while (!task.done() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_TRUE(task.done());

    // One timeout SQE submitted + completed. The stop-time NOP is
    // not yet visible because stop() happens in the dtor; we observe
    // the count before the dtor runs.
    EXPECT_GE(rt->ops_submitted(), 1u);
    EXPECT_GE(rt->completions_handled(), 1u);
}

TEST(IoUringReactor, MultipleConcurrentSleepsAllComplete) {
    MAKE_REACTOR_OR_SKIP(rt);
    constexpr int kCount = 8;
    std::vector<Task<long>> tasks;
    tasks.reserve(kCount);
    for (int i = 0; i < kCount; ++i) {
        tasks.push_back(sleep_and_measure(*rt, 20ms));
        tasks.back().resume();
    }
    // All tasks should complete in approximately the same 20ms
    // window because the kernel timers fire concurrently from the
    // CQ side. Wait for everyone with a generous deadline.
    const auto deadline = std::chrono::steady_clock::now() + 500ms;
    auto all_done = [&] {
        for (auto& t : tasks) {
            if (!t.done())
                return false;
        }
        return true;
    };
    while (!all_done() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    for (auto& t : tasks) {
        ASSERT_TRUE(t.done()) << "at least one sleep never completed";
        const long elapsed = t.get();
        EXPECT_GE(elapsed, 15);
        EXPECT_LE(elapsed, 200);
    }
    EXPECT_GE(rt->ops_submitted(), static_cast<unsigned>(kCount));
    EXPECT_GE(rt->completions_handled(), static_cast<unsigned>(kCount));
}

// --- Socket awaitables --------------------------------------------

namespace {

// Bind a TCP socket to 127.0.0.1 on an OS-assigned port and start
// listening. Returns (listen_fd, port). Caller closes the fd.
[[nodiscard]] std::pair<int, std::uint16_t> open_loopback_listener() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return {-1, 0};
    }
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;  // OS-assigned
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return {-1, 0};
    }
    if (::listen(fd, 16) < 0) {
        ::close(fd);
        return {-1, 0};
    }
    sockaddr_in bound{};
    socklen_t len = sizeof(bound);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &len) < 0) {
        ::close(fd);
        return {-1, 0};
    }
    return {fd, ntohs(bound.sin_port)};
}

// Coroutine: accept one connection on listen_fd, read into buf, echo
// it back. co_returns the bytes echoed (or negative errno).
Task<int> echo_once(IoUringReactor& reactor, int listen_fd) {
    const int conn_fd = co_await reactor.accept_async(listen_fd);
    if (conn_fd < 0) {
        co_return conn_fd;
    }
    char buf[256];
    const int n = co_await reactor.read_async(conn_fd, buf, sizeof(buf));
    if (n <= 0) {
        ::close(conn_fd);
        co_return n;
    }
    const int wrote = co_await reactor.write_async(conn_fd, buf, static_cast<unsigned>(n));
    ::close(conn_fd);
    co_return wrote;
}

// Coroutine: connect to 127.0.0.1:port, write `payload`, read echo
// back. co_returns the bytes read (or negative errno on any step).
// On success, the buffer pointed to by `out` contains the echoed
// bytes.
Task<int> connect_send_recv(IoUringReactor& reactor,
                            std::uint16_t port,
                            const std::string& payload,
                            char* out,
                            unsigned out_cap) {
    const int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        co_return -errno;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    const int cr =
        co_await reactor.connect_async(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (cr < 0) {
        ::close(sock);
        co_return cr;
    }
    const int wr =
        co_await reactor.write_async(sock, payload.data(), static_cast<unsigned>(payload.size()));
    if (wr < 0) {
        ::close(sock);
        co_return wr;
    }
    const int rr = co_await reactor.read_async(sock, out, out_cap);
    ::close(sock);
    co_return rr;
}

}  // namespace

TEST(IoUringReactor, TcpLoopbackEchoRoundTrip) {
    // End-to-end proof that accept / read / write / connect awaiters
    // wire together: one coroutine drives a server-side echo while
    // another connects + writes + reads through the same reactor.
    MAKE_REACTOR_OR_SKIP(rt);

    auto [listen_fd, port] = open_loopback_listener();
    ASSERT_GE(listen_fd, 0) << "open_loopback_listener failed: " << std::strerror(errno);

    auto server = echo_once(*rt, listen_fd);
    server.resume();

    char client_buf[256] = {0};
    const std::string payload = "hello-io_uring";
    auto client = connect_send_recv(*rt, port, payload, client_buf, sizeof(client_buf));
    client.resume();

    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while ((!server.done() || !client.done()) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_TRUE(server.done()) << "echo_once never completed";
    ASSERT_TRUE(client.done()) << "connect_send_recv never completed";

    const int server_wrote = server.get();
    EXPECT_EQ(server_wrote, static_cast<int>(payload.size()))
        << "server echoed " << server_wrote << " bytes; errno-as-negative if < 0";

    const int client_read = client.get();
    ASSERT_GT(client_read, 0) << "client read failed: errno=" << -client_read << " ("
                              << std::strerror(-client_read) << ")";
    EXPECT_EQ(std::string(client_buf, static_cast<std::size_t>(client_read)), payload);

    ::close(listen_fd);
}

#else  // non-Linux or no liburing

#include <gtest/gtest.h>

TEST(IoUringReactor, SkippedOnThisPlatform) {
    GTEST_SKIP() << "io_uring backend requires Linux + liburing; not built on this platform";
}

#endif  // __linux__ && CLINK_HAS_URING
