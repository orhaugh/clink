// CountingSocketFactory, its wrapper streams and SocketControl, over a raw
// loopback connection and with no ::clickhouse::Client: the client cannot be
// driven into an INSERT against a plain TCP peer, so these pin the socket
// layer the transport's guarantees rest on.

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <poll.h>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>

#include <arpa/inet.h>
#include <clickhouse/base/input.h>
#include <clickhouse/base/output.h>
#include <clickhouse/base/socket.h>
#include <clickhouse/client.h>
#include <clickhouse/exceptions.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "native/errors.hpp"
#include "native/socket_factory.hpp"

namespace {

using namespace std::chrono_literals;
using clink::clickhouse::native::CountingSocketFactory;
using clink::clickhouse::native::socket_fd;
using clink::clickhouse::native::SocketControl;
using SfClock = std::chrono::steady_clock;

// An fd closed when it goes out of scope.
class SfFd {
public:
    explicit SfFd(int fd = -1) noexcept : fd_(fd) {}
    ~SfFd() { reset(); }
    SfFd(SfFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    SfFd& operator=(SfFd&& other) noexcept {
        if (this != &other) {
            reset();
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }
    SfFd(const SfFd&) = delete;
    SfFd& operator=(const SfFd&) = delete;

    [[nodiscard]] int get() const noexcept { return fd_; }
    void reset() noexcept {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = -1;
    }

private:
    int fd_;
};

bool sf_readable(int fd, std::chrono::milliseconds wait) {
    pollfd p{};
    p.fd = fd;
    p.events = POLLIN;
    return ::poll(&p, 1, static_cast<int>(wait.count())) > 0;
}

void sf_set_recv_timeout(int fd, std::chrono::milliseconds timeout) {
    timeval tv{};
    tv.tv_sec = static_cast<time_t>(timeout.count() / 1000);
    tv.tv_usec = static_cast<suseconds_t>((timeout.count() % 1000) * 1000);
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
}

// A TCP listener on 127.0.0.1 with a port the kernel picks.
class SfListener {
public:
    SfListener() : fd_(::socket(AF_INET, SOCK_STREAM, 0)) {
        if (fd_.get() < 0) {
            throw std::system_error(errno, std::system_category(), "socket");
        }
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = 0;
        ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        if (::bind(fd_.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof addr) != 0 ||
            ::listen(fd_.get(), 8) != 0) {
            throw std::system_error(errno, std::system_category(), "bind or listen");
        }
        socklen_t len = sizeof addr;
        ::getsockname(fd_.get(), reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
    }

    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }
    [[nodiscard]] ::clickhouse::Endpoint endpoint() const { return {"127.0.0.1", port_}; }

    // The next connection, or an empty fd if none arrives in time.
    SfFd accept_peer(std::chrono::milliseconds wait = 2000ms) {
        if (!sf_readable(fd_.get(), wait)) {
            return SfFd{};
        }
        SfFd peer(::accept(fd_.get(), nullptr, nullptr));
        if (peer.get() >= 0) {
            sf_set_recv_timeout(peer.get(), 5000ms);
        }
        return peer;
    }

    [[nodiscard]] bool has_pending(std::chrono::milliseconds wait) const {
        return sf_readable(fd_.get(), wait);
    }

private:
    SfFd fd_;
    std::uint16_t port_{0};
};

::clickhouse::ClientOptions sf_client_options() {
    ::clickhouse::ClientOptions opts;
    opts.SetConnectionConnectTimeout(2000ms)
        .SetConnectionRecvTimeout(5000ms)
        .SetConnectionSendTimeout(5000ms);
    return opts;
}

std::string sf_pattern(std::size_t n, unsigned seed = 1) {
    std::string out(n, '\0');
    for (std::size_t i = 0; i < n; ++i) {
        out[i] = static_cast<char>((i * 31U + seed) & 0xffU);
    }
    return out;
}

int sf_send_flags() {
#if defined(MSG_NOSIGNAL)
    return MSG_NOSIGNAL;
#else
    return 0;
#endif
}

ssize_t sf_send_raw(int fd, std::string_view bytes) {
    return ::send(fd, bytes.data(), bytes.size(), sf_send_flags());
}

void sf_send_all(int fd, std::string_view bytes) {
    while (!bytes.empty()) {
        const ssize_t n = sf_send_raw(fd, bytes);
        if (n <= 0) {
            return;
        }
        bytes.remove_prefix(static_cast<std::size_t>(n));
    }
}

// Up to `n` bytes; fewer only if the stream ends or stalls for 5 s.
std::string sf_recv_exactly(int fd, std::size_t n) {
    std::string out;
    char buf[8192];
    while (out.size() < n) {
        const ssize_t got = ::recv(fd, buf, std::min(sizeof buf, n - out.size()), 0);
        if (got <= 0) {
            break;
        }
        out.append(buf, static_cast<std::size_t>(got));
    }
    return out;
}

// What arrives before end of stream, or nullopt if the stream is still open
// after `wait`.
std::optional<std::string> sf_recv_until_eof(int fd, std::chrono::milliseconds wait = 1000ms) {
    std::string out;
    const auto until = SfClock::now() + wait;
    char buf[4096];
    while (true) {
        const auto left =
            std::chrono::duration_cast<std::chrono::milliseconds>(until - SfClock::now());
        if (left <= 0ms || !sf_readable(fd, left)) {
            return std::nullopt;
        }
        const ssize_t got = ::recv(fd, buf, sizeof buf, 0);
        if (got <= 0) {
            return out;
        }
        out.append(buf, static_cast<std::size_t>(got));
    }
}

void sf_write_all(::clickhouse::OutputStream& out, std::string_view bytes) {
    while (!bytes.empty()) {
        const std::size_t n = out.Write(bytes.data(), bytes.size());
        ASSERT_GT(n, 0U);
        bytes.remove_prefix(n);
    }
}

std::string sf_read_exactly(::clickhouse::InputStream& in, std::size_t n) {
    std::string out(n, '\0');
    std::size_t have = 0;
    while (have < n) {
        const std::size_t got = in.Read(out.data() + have, n - have);
        if (got == 0) {
            break;
        }
        have += got;
    }
    out.resize(have);
    return out;
}

// Waits until `writing` is set and the bytes `control` has seen written have
// grown and then stood still for `quiet`; false when that has not happened
// within 5 s. A send with room in the socket buffers returns at once, so a
// writer whose count stands still that long is blocked in one.
bool sf_writer_blocks(const SocketControl& control,
                      const std::atomic<bool>& writing,
                      std::chrono::milliseconds quiet) {
    const auto give_up = SfClock::now() + 5s;
    while (!writing && SfClock::now() < give_up) {
        std::this_thread::sleep_for(1ms);
    }
    std::uint64_t last = control.bytes_written();
    auto since = SfClock::now();
    while (SfClock::now() < give_up) {
        std::this_thread::sleep_for(10ms);
        const std::uint64_t count = control.bytes_written();
        if (count != last) {
            last = count;
            since = SfClock::now();
        } else if (count > 0 && SfClock::now() - since >= quiet) {
            return true;
        }
    }
    return false;
}

// The errno a std::system_error from `f` carries; 0 when `f` does not throw.
template <typename F>
int sf_system_errno(F&& f) {
    try {
        f();
    } catch (const std::system_error& e) {
        return e.code().value();
    }
    return 0;
}

// Stands in for a cancel that wins the race with a real connect: the TCP
// connect has completed, the wrapper has not yet attached the fd, and the
// control is poisoned. Keeps a duplicate of the fd, so the test can tell a
// shutdown (which ends the connection for every descriptor) from a close of
// the wrapper's own descriptor (which does not while the duplicate lives).
class SfPoisoningInner final : public ::clickhouse::NonSecureSocketFactory {
public:
    SfPoisoningInner(std::shared_ptr<SocketControl> control, std::shared_ptr<int> dup_fd)
        : control_(std::move(control)), dup_fd_(std::move(dup_fd)) {}

protected:
    std::unique_ptr<::clickhouse::Socket> doConnect(
        const ::clickhouse::NetworkAddress& address,
        const ::clickhouse::ClientOptions& opts) override {
        auto socket = NonSecureSocketFactory::doConnect(address, opts);
        *dup_fd_ = ::dup(socket_fd(*socket));
        control_->poison();
        return socket;
    }

private:
    std::shared_ptr<SocketControl> control_;
    std::shared_ptr<int> dup_fd_;
};

// An inner stream that accepts nothing, which a broken TLS layer or a
// zero-length send could produce.
class SfZeroOutput final : public ::clickhouse::OutputStream {
protected:
    std::size_t DoWrite(const void* /*data*/, std::size_t /*len*/) override { return 0; }
};

class SfZeroWriteSocket final : public ::clickhouse::Socket {
public:
    using Socket::Socket;
    std::unique_ptr<::clickhouse::OutputStream> makeOutputStream() const override {
        return std::make_unique<SfZeroOutput>();
    }
};

class SfZeroWriteInner final : public ::clickhouse::NonSecureSocketFactory {
protected:
    std::unique_ptr<::clickhouse::Socket> doConnect(
        const ::clickhouse::NetworkAddress& address,
        const ::clickhouse::ClientOptions& opts) override {
        const ::clickhouse::SocketTimeoutParams timeouts{opts.connection_connect_timeout,
                                                         opts.connection_recv_timeout,
                                                         opts.connection_send_timeout};
        return std::make_unique<SfZeroWriteSocket>(address, timeouts);
    }
};

#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)
std::filesystem::path sf_temp_file(const std::string& name, const std::string& contents) {
    const auto path = std::filesystem::temp_directory_path() /
                      ("clink-native-sf-" + std::to_string(::getpid()) + "-" + name);
    std::ofstream(path) << contents;
    return path;
}
#endif

}  // namespace

TEST(NativeSocketFactory, CountedBytesEqualTheBytesThePeerReceived) {
    SfListener listener;
    auto control = std::make_shared<SocketControl>();
    const auto opts = sf_client_options();
    CountingSocketFactory factory(opts, false, control);
    const auto socket = factory.connect(opts, listener.endpoint());
    const SfFd peer = listener.accept_peer();
    ASSERT_GE(peer.get(), 0);

    // Larger than the client's 8 KiB buffer and the loopback socket buffer,
    // so the writes are split, partly bypass the buffer, and need a reader.
    const std::string sent = sf_pattern(300000);
    std::string received;
    std::thread reader([&] { received = sf_recv_exactly(peer.get(), sent.size()); });
    {
        ::clickhouse::BufferedOutput out(socket->makeOutputStream());
        sf_write_all(out, sent);
        out.Flush();
    }
    reader.join();
    EXPECT_EQ(received.size(), sent.size());
    EXPECT_TRUE(received == sent);
    EXPECT_EQ(control->bytes_written(), sent.size());

    const std::string reply = sf_pattern(70000, 7);
    std::thread writer([&] { sf_send_all(peer.get(), reply); });
    ::clickhouse::BufferedInput in(socket->makeInputStream());
    const std::string got = sf_read_exactly(in, reply.size());
    writer.join();
    EXPECT_TRUE(got == reply);
    EXPECT_EQ(control->bytes_read(), reply.size());
}

TEST(NativeSocketFactory, APoisonMakesTheNextWriteThrowAndNothingMoreReachesThePeer) {
    SfListener listener;
    auto control = std::make_shared<SocketControl>();
    const auto opts = sf_client_options();
    CountingSocketFactory factory(opts, false, control);
    const auto socket = factory.connect(opts, listener.endpoint());
    const SfFd peer = listener.accept_peer();
    ASSERT_GE(peer.get(), 0);
    const auto out = socket->makeOutputStream();
    const auto in = socket->makeInputStream();

    sf_write_all(*out, "abc");
    ASSERT_EQ(sf_recv_exactly(peer.get(), 3), "abc");

    control->poison();
    EXPECT_TRUE(control->poisoned());
    EXPECT_EQ(sf_system_errno([&] { (void)out->Write("def", 3); }), ECONNABORTED);
    EXPECT_EQ(sf_system_errno([&] { out->Flush(); }), ECONNABORTED);
    char buf[4];
    EXPECT_EQ(sf_system_errno([&] { (void)in->Read(buf, sizeof buf); }), ECONNABORTED);
    // The poison shut the socket down: the peer sees the end of the stream,
    // with nothing after "abc".
    EXPECT_EQ(sf_recv_until_eof(peer.get()), std::optional<std::string>(""));
    EXPECT_EQ(control->bytes_written(), 3U);
}

TEST(NativeSocketFactory, APoisonFromAnotherThreadWakesABlockedReadWithin100Ms) {
    SfListener listener;
    auto control = std::make_shared<SocketControl>();
    const auto opts = sf_client_options();
    CountingSocketFactory factory(opts, false, control);
    const auto socket = factory.connect(opts, listener.endpoint());
    const SfFd peer = listener.accept_peer();
    ASSERT_GE(peer.get(), 0);
    const auto in = socket->makeInputStream();

    SfClock::time_point poisoned_at;
    std::thread poisoner([&] {
        std::this_thread::sleep_for(50ms);
        poisoned_at = SfClock::now();
        control->poison();
    });
    char buf[16];
    const int err = sf_system_errno([&] { (void)in->Read(buf, sizeof buf); });
    const auto woke_at = SfClock::now();
    poisoner.join();

    EXPECT_EQ(err, ECONNABORTED);
    EXPECT_LT(woke_at - poisoned_at, 100ms);
}

// A server that stops reading mid-INSERT leaves send_block blocked in send
// with the socket buffers full. Only the shutdown of the write side wakes it;
// without that the writer would stay there until the send timeout, which is
// 5 s here.
TEST(NativeSocketFactory, APoisonFromAnotherThreadWakesABlockedSendWithin100Ms) {
    SfListener listener;
    auto control = std::make_shared<SocketControl>();
    const auto opts = sf_client_options();
    CountingSocketFactory factory(opts, false, control);
    const auto socket = factory.connect(opts, listener.endpoint());
    // Accepted and never read.
    const SfFd peer = listener.accept_peer();
    ASSERT_GE(peer.get(), 0);
    const auto out = socket->makeOutputStream();

    std::atomic<bool> writing{false};
    std::atomic<bool> returned{false};
    int err = 0;
    SfClock::time_point woke_at;
    std::thread writer([&] {
        const std::string chunk = sf_pattern(64 * 1024);
        writing = true;
        // 256 MiB at most, far more than the buffers of both ends hold.
        err = sf_system_errno([&] {
            for (int i = 0; i < 4096; ++i) {
                sf_write_all(*out, chunk);
            }
        });
        woke_at = SfClock::now();
        returned = true;
    });
    const bool blocked = sf_writer_blocks(*control, writing, 200ms);
    const bool still_writing = !returned;
    const SfClock::time_point poisoned_at = SfClock::now();
    control->poison();
    writer.join();

    EXPECT_TRUE(blocked) << "the writer never blocked in send";
    EXPECT_TRUE(still_writing) << "the writer finished before the poison";
    EXPECT_EQ(err, ECONNABORTED);
    EXPECT_LT(woke_at - poisoned_at, 100ms)
        << std::chrono::duration_cast<std::chrono::milliseconds>(woke_at - poisoned_at).count()
        << " ms";
}

TEST(NativeSocketFactory, ConnectRefusesAfterAPoisonAndOpensNoConnection) {
    SfListener listener;
    auto control = std::make_shared<SocketControl>();
    const auto opts = sf_client_options();
    CountingSocketFactory factory(opts, false, control);

    control->poison();
    EXPECT_EQ(sf_system_errno([&] { (void)factory.connect(opts, listener.endpoint()); }),
              ECONNABORTED);
    EXPECT_FALSE(listener.has_pending(100ms));
    EXPECT_EQ(control->bytes_written(), 0U);
}

TEST(NativeSocketFactory, APoisonDuringTheInnerConnectShutsTheSocketDownAtAttach) {
    SfListener listener;
    auto control = std::make_shared<SocketControl>();
    auto dup_fd = std::make_shared<int>(-1);
    const auto opts = sf_client_options();
    CountingSocketFactory factory(std::make_unique<SfPoisoningInner>(control, dup_fd), control);

    EXPECT_EQ(sf_system_errno([&] { (void)factory.connect(opts, listener.endpoint()); }),
              ECONNABORTED);
    const SfFd duplicate(*dup_fd);
    ASSERT_GE(duplicate.get(), 0);
    const SfFd peer = listener.accept_peer();
    ASSERT_GE(peer.get(), 0);

    // The duplicate keeps the connection open after the wrapper closed its
    // own descriptor, so only a shutdown explains an end of stream at the
    // peer and a refused send on the duplicate.
    EXPECT_EQ(sf_recv_until_eof(peer.get()), std::optional<std::string>(""));
    errno = 0;
    EXPECT_EQ(sf_send_raw(duplicate.get(), "x"), -1);
    EXPECT_EQ(errno, EPIPE);
}

TEST(NativeSocketFactory, AReadOrWritePastTheDeadlineThrowsTimedOut) {
    SfListener listener;
    auto control = std::make_shared<SocketControl>();
    const auto opts = sf_client_options();
    CountingSocketFactory factory(opts, false, control);
    const auto socket = factory.connect(opts, listener.endpoint());
    const SfFd peer = listener.accept_peer();
    ASSERT_GE(peer.get(), 0);
    const auto out = socket->makeOutputStream();
    const auto in = socket->makeInputStream();
    sf_send_all(peer.get(), "hello");
    char buf[5];

    control->set_deadline(SfClock::now() - 1ms);
    EXPECT_EQ(sf_system_errno([&] { (void)out->Write("abc", 3); }), ETIMEDOUT);
    // Data is waiting, and the read still refuses.
    EXPECT_EQ(sf_system_errno([&] { (void)in->Read(buf, sizeof buf); }), ETIMEDOUT);
    EXPECT_FALSE(sf_readable(peer.get(), 50ms));
    EXPECT_EQ(control->bytes_written(), 0U);
    EXPECT_EQ(control->bytes_read(), 0U);

    // A deadline is not a poison: once cleared, the same streams work.
    control->set_deadline(std::nullopt);
    EXPECT_EQ(sf_read_exactly(*in, 5), "hello");
    sf_write_all(*out, "abc");
    EXPECT_EQ(sf_recv_exactly(peer.get(), 3), "abc");

    control->set_deadline(SfClock::now() + 150ms);
    sf_write_all(*out, "d");
    EXPECT_EQ(sf_recv_exactly(peer.get(), 1), "d");
    std::this_thread::sleep_for(200ms);
    EXPECT_EQ(sf_system_errno([&] { (void)out->Write("e", 1); }), ETIMEDOUT);
    EXPECT_EQ(control->bytes_written(), 4U);
}

TEST(NativeSocketFactory, AnInnerWriteOfNothingThrowsRatherThanReturningZero) {
    SfListener listener;
    auto control = std::make_shared<SocketControl>();
    const auto opts = sf_client_options();
    CountingSocketFactory factory(std::make_unique<SfZeroWriteInner>(), control);
    const auto socket = factory.connect(opts, listener.endpoint());
    const auto out = socket->makeOutputStream();

    // BufferedOutput would loop for ever on a 0, so the wrapper must throw.
    EXPECT_EQ(sf_system_errno([&] { (void)out->Write("abc", 3); }), EIO);
    EXPECT_EQ(control->bytes_written(), 0U);
}

TEST(NativeSocketFactory, SocketFdReturnsTheLiveFd) {
    SfListener listener;
    const auto opts = sf_client_options();
    ::clickhouse::NonSecureSocketFactory plain;
    const auto socket = plain.connect(opts, listener.endpoint());
    const auto* tcp = dynamic_cast<const ::clickhouse::Socket*>(socket.get());
    ASSERT_NE(tcp, nullptr);
    const int fd = socket_fd(*tcp);
    ASSERT_GE(fd, 0);

    sockaddr_in remote{};
    socklen_t len = sizeof remote;
    ASSERT_EQ(::getpeername(fd, reinterpret_cast<sockaddr*>(&remote), &len), 0);
    EXPECT_EQ(ntohs(remote.sin_port), listener.port());
    const SfFd peer = listener.accept_peer();
    ASSERT_GE(peer.get(), 0);
    EXPECT_EQ(sf_send_raw(fd, "z"), 1);
    EXPECT_EQ(sf_recv_exactly(peer.get(), 1), "z");
}

TEST(NativeSocketControl, AttachAfterAPoisonShutsTheFdDownAtOnce) {
    int pair[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, pair), 0);
    const SfFd near(pair[0]);
    const SfFd far(pair[1]);

    SocketControl control;
    control.poison();
    control.attach_fd(near.get());
    EXPECT_EQ(sf_recv_until_eof(far.get(), 100ms), std::optional<std::string>(""));
    errno = 0;
    EXPECT_EQ(sf_send_raw(near.get(), "x"), -1);
    EXPECT_EQ(errno, EPIPE);
    control.detach_fd(near.get());
}

TEST(NativeSocketControl, APoisonShutsDownTheAttachedFdAndNoDetachedOne) {
    int first[2];
    int second[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, first), 0);
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, second), 0);
    const SfFd a(first[0]);
    const SfFd a_peer(first[1]);
    const SfFd b(second[0]);
    const SfFd b_peer(second[1]);

    SocketControl control;
    control.attach_fd(a.get());
    EXPECT_FALSE(sf_readable(a_peer.get(), 50ms));
    // A detach naming another fd, as a socket being replaced would, leaves
    // the attachment alone.
    control.detach_fd(b.get());
    control.poison();
    EXPECT_EQ(sf_recv_until_eof(a_peer.get(), 100ms), std::optional<std::string>(""));

    // Detached before the poison, so the poison has nothing to shut down.
    SocketControl other;
    other.attach_fd(b.get());
    other.detach_fd(b.get());
    other.poison();
    EXPECT_EQ(sf_send_raw(b.get(), "y"), 1);
    EXPECT_EQ(sf_recv_exactly(b_peer.get(), 1), "y");
}

TEST(NativeSocketControl, CountersStartAtZeroAndAddUp) {
    SocketControl control;
    EXPECT_EQ(control.bytes_written(), 0U);
    EXPECT_EQ(control.bytes_read(), 0U);
    EXPECT_FALSE(control.poisoned());
    control.add_written(5);
    control.add_written(7);
    control.add_read(3);
    EXPECT_EQ(control.bytes_written(), 12U);
    EXPECT_EQ(control.bytes_read(), 3U);
    EXPECT_NO_THROW(control.check_deadline());
}

#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)

TEST(NativeSocketFactory, AMalformedCaFileThrowsOpenSslErrorWhenTheFactoryIsBuilt) {
    const auto path = sf_temp_file("bad-ca.pem", "this is not a certificate\n");
    auto opts = sf_client_options();
    opts.SetSSLOptions(::clickhouse::ClientOptions::SSLOptions{}
                           .SetPathToCAFiles({path.string()})
                           .SetUseDefaultCALocations(false));
    auto control = std::make_shared<SocketControl>();
    EXPECT_THROW(
        { CountingSocketFactory factory(opts, true, control); }, ::clickhouse::OpenSSLError);
    std::filesystem::remove(path);
}

// OpenSSL only records a CA directory here and looks inside it during the
// handshake, so a factory over a directory that does not exist still builds.
// That is why the transport checks the directory itself before it builds one.
TEST(NativeSocketFactory, AMissingCaDirectoryStillBuildsAFactory) {
    auto opts = sf_client_options();
    opts.SetSSLOptions(::clickhouse::ClientOptions::SSLOptions{}
                           .SetPathToCADirectory("/nonexistent/clink-native-sf-ca.d")
                           .SetUseDefaultCALocations(false));
    auto control = std::make_shared<SocketControl>();
    EXPECT_NO_THROW({ CountingSocketFactory factory(opts, true, control); });
}

TEST(NativeSocketFactory, ATlsFactoryOverTheDefaultCaLocationsBuildsWithoutConnecting) {
    SfListener listener;
    auto opts = sf_client_options();
    opts.SetSSLOptions(::clickhouse::ClientOptions::SSLOptions{});
    auto control = std::make_shared<SocketControl>();
    EXPECT_NO_THROW({ CountingSocketFactory factory(opts, true, control); });
    EXPECT_FALSE(listener.has_pending(50ms));
}

#else

TEST(NativeSocketFactory, TlsRefusesTlsUnavailableOnABuildWithoutIt) {
    auto control = std::make_shared<SocketControl>();
    try {
        CountingSocketFactory factory(sf_client_options(), true, control);
        FAIL() << "a TLS factory was built without TLS support";
    } catch (const clink::clickhouse::native::NativeSinkError& e) {
        EXPECT_EQ(e.code(), clink::clickhouse::native::code::kTlsUnavailable);
    }
}

#endif
