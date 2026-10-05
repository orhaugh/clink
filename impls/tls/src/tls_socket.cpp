#include "clink/runtime/network/tls_socket.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

#include "clink/fault/fault_injection.hpp"
#include "clink/runtime/network/network_socket.hpp"

#ifdef CLINK_HAS_OPENSSL
#include <fcntl.h>
#include <poll.h>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/time.h>
#endif

namespace clink::network {

#ifdef CLINK_HAS_OPENSSL

namespace {

std::string ossl_last_error() {
    char buf[256] = {};
    ERR_error_string_n(ERR_get_error(), buf, sizeof(buf));
    return std::string{buf};
}

// Why a handshake step failed, from OpenSSL's error queue, or from errno or
// the end of the stream when the queue is empty (a peer that hung up mid
// handshake leaves nothing there).
std::string handshake_error(int ssl_error) {
    if (ERR_peek_error() != 0) {
        return ossl_last_error();
    }
    if (ssl_error == SSL_ERROR_SYSCALL && errno != 0) {
        return std::strerror(errno);
    }
    return "peer closed the connection";
}

bool set_nonblocking(int fd, bool on) {
    const int flags = ::fcntl(fd, F_GETFL);
    if (flags < 0) {
        return false;
    }
    const int want = on ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
    return want == flags || ::fcntl(fd, F_SETFL, want) == 0;
}

// True once `fd` has been shut down (by this process from another thread, as
// the coordinator does to evict or stop an admission) or reset by its peer:
// poll() reports POLLHUP or POLLERR. Linux and Darwin both report a local
// shutdown in both directions as POLLHUP, though on Linux data that arrived
// before it stays readable. Linux reports a peer's FIN as no more than
// readable, but Darwin reports it as POLLHUP too, and there a client that sent
// its last handshake flight, then its first frame, then closed (a TLS 1.3
// client is done once its Finished is sent) still has a handshake that can
// complete and a frame to read. Darwin's shutdown(SHUT_RD) discards whatever
// was buffered and anything that arrives after, and a FIN leaves it readable,
// so there bytes still waiting mean the peer half-closed, not that the socket
// was shut down here.
bool socket_hung_up(int fd) {
    pollfd p{fd, POLLIN, 0};
    int rc = 0;
    do {
        rc = ::poll(&p, 1, 0);
    } while (rc < 0 && errno == EINTR);
    if (rc <= 0 || (p.revents & (POLLHUP | POLLERR | POLLNVAL)) == 0) {
        return false;
    }
#if defined(__linux__)
    return true;
#else
    if ((p.revents & POLLNVAL) != 0) {
        return true;
    }
    int waiting = 0;
    return ::ioctl(fd, FIONREAD, &waiting) != 0 || waiting <= 0;
#endif
}

// How long one wait for a handshake slot lasts before the waiter checks
// whether its handshake has been abandoned. Nothing that abandons one (a
// shutdown of the socket, the stop wake) can reach a condition variable, so
// this bounds how long an evicted or stopped handshake keeps its thread and
// its descriptor while the slots are busy.
constexpr auto kStepWaitSlice = std::chrono::milliseconds{10};

// Refuse TLS 1.2 renegotiation in both directions. A renegotiation runs a
// full handshake (key exchange, the certificate signature) inside SSL_read, on
// whichever thread is reading: on the coordinator, an admission thread reading
// a first frame, or a session's reader, neither of which holds a handshake
// slot (see set_max_concurrent_handshake_steps). OpenSSL 3 already refuses a
// client's, but 1.1.1, which this builds against too, allows it by default, so
// a client could make the server sign as often as it liked on one connection.
// Nothing here renegotiates; TLS 1.3 has no renegotiation at all.
void refuse_renegotiation(SSL_CTX* ctx) {
    SSL_CTX_set_options(ctx, SSL_OP_NO_RENEGOTIATION);
}

void init_openssl() {
    static const bool _ = [] {
        SSL_library_init();
        SSL_load_error_strings();
        OpenSSL_add_all_algorithms();
        return true;
    }();
    (void)_;
}

}  // namespace

// ---------- TlsServerContext ----------

struct TlsServerContext::Impl {
    SSL_CTX* ctx{nullptr};

    // Handshake steps computing now, and the most allowed at once (see
    // set_max_concurrent_handshake_steps).
    std::mutex step_mu;
    std::condition_variable step_cv;
    std::size_t steps_running{0};
    std::size_t max_steps{std::max<std::size_t>(std::thread::hardware_concurrency() / 2, 1)};

    enum class StepWait : std::uint8_t { Acquired, TimedOut, Abandoned };

    // Take a slot, waiting until `deadline` at most, and giving up as soon as
    // `abandoned()` is true: it is checked before the first wait and after
    // each slice of it (kStepWaitSlice), outside the lock.
    template <typename Abandoned>
    StepWait acquire_step(std::chrono::steady_clock::time_point deadline,
                          const Abandoned& abandoned) {
        const auto free = [this] { return steps_running < max_steps; };
        for (;;) {
            if (abandoned()) {
                return StepWait::Abandoned;
            }
            std::unique_lock lock(step_mu);
            if (free()) {
                ++steps_running;
                return StepWait::Acquired;
            }
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                return StepWait::TimedOut;
            }
            // Never past the deadline; deadline - now cannot overflow, even
            // for time_point::max().
            const auto slice_end =
                deadline - now > kStepWaitSlice ? now + kStepWaitSlice : deadline;
            (void)step_cv.wait_until(lock, slice_end, free);
        }
    }
    std::size_t step_limit() {
        std::lock_guard lock(step_mu);
        return max_steps;
    }
    void release_step() {
        {
            std::lock_guard lock(step_mu);
            --steps_running;
        }
        step_cv.notify_one();
    }
};

bool TlsServerContext::is_real_implementation() {
    return true;
}

TlsServerContext::TlsServerContext(const std::string& cert_path, const std::string& key_path)
    : impl_(std::make_unique<Impl>()) {
    init_openssl();
    impl_->ctx = SSL_CTX_new(TLS_server_method());
    if (impl_->ctx == nullptr) {
        throw std::runtime_error("TlsServerContext: SSL_CTX_new failed: " + ossl_last_error());
    }
    SSL_CTX_set_min_proto_version(impl_->ctx, TLS1_2_VERSION);
    refuse_renegotiation(impl_->ctx);
    if (SSL_CTX_use_certificate_file(impl_->ctx, cert_path.c_str(), SSL_FILETYPE_PEM) != 1) {
        const auto err = ossl_last_error();
        SSL_CTX_free(impl_->ctx);
        impl_->ctx = nullptr;
        throw std::runtime_error("TlsServerContext: load cert failed: " + err);
    }
    if (SSL_CTX_use_PrivateKey_file(impl_->ctx, key_path.c_str(), SSL_FILETYPE_PEM) != 1) {
        const auto err = ossl_last_error();
        SSL_CTX_free(impl_->ctx);
        impl_->ctx = nullptr;
        throw std::runtime_error("TlsServerContext: load key failed: " + err);
    }
    if (SSL_CTX_check_private_key(impl_->ctx) != 1) {
        const auto err = ossl_last_error();
        SSL_CTX_free(impl_->ctx);
        impl_->ctx = nullptr;
        throw std::runtime_error("TlsServerContext: cert/key mismatch: " + err);
    }
}

TlsServerContext::~TlsServerContext() {
    if (impl_ && impl_->ctx != nullptr) {
        SSL_CTX_free(impl_->ctx);
    }
}

void TlsServerContext::set_client_ca_path(const std::string& ca_path) {
    if (SSL_CTX_load_verify_locations(impl_->ctx, ca_path.c_str(), nullptr) != 1) {
        throw std::runtime_error("TlsServerContext::set_client_ca_path: " + ossl_last_error());
    }
    SSL_CTX_set_verify(impl_->ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
}

void TlsServerContext::set_max_concurrent_handshake_steps(std::size_t n) {
    {
        std::lock_guard lock(impl_->step_mu);
        impl_->max_steps = std::max<std::size_t>(n, 1);
    }
    impl_->step_cv.notify_all();
}

void* TlsServerContext::native_handle() const noexcept {
    return impl_->ctx;
}

// ---------- TlsClientContext ----------

struct TlsClientContext::Impl {
    SSL_CTX* ctx{nullptr};
};

bool TlsClientContext::is_real_implementation() {
    return true;
}

TlsClientContext::TlsClientContext(const std::string& ca_path) : impl_(std::make_unique<Impl>()) {
    init_openssl();
    impl_->ctx = SSL_CTX_new(TLS_client_method());
    if (impl_->ctx == nullptr) {
        throw std::runtime_error("TlsClientContext: SSL_CTX_new failed: " + ossl_last_error());
    }
    SSL_CTX_set_min_proto_version(impl_->ctx, TLS1_2_VERSION);
    refuse_renegotiation(impl_->ctx);
    if (SSL_CTX_load_verify_locations(impl_->ctx, ca_path.c_str(), nullptr) != 1) {
        const auto err = ossl_last_error();
        SSL_CTX_free(impl_->ctx);
        impl_->ctx = nullptr;
        throw std::runtime_error("TlsClientContext: load CA failed: " + err);
    }
    SSL_CTX_set_verify(impl_->ctx, SSL_VERIFY_PEER, nullptr);
}

TlsClientContext::~TlsClientContext() {
    if (impl_ && impl_->ctx != nullptr) {
        SSL_CTX_free(impl_->ctx);
    }
}

void TlsClientContext::set_client_cert(const std::string& cert_path, const std::string& key_path) {
    if (SSL_CTX_use_certificate_file(impl_->ctx, cert_path.c_str(), SSL_FILETYPE_PEM) != 1) {
        throw std::runtime_error("TlsClientContext::set_client_cert: cert: " + ossl_last_error());
    }
    if (SSL_CTX_use_PrivateKey_file(impl_->ctx, key_path.c_str(), SSL_FILETYPE_PEM) != 1) {
        throw std::runtime_error("TlsClientContext::set_client_cert: key: " + ossl_last_error());
    }
    if (SSL_CTX_check_private_key(impl_->ctx) != 1) {
        throw std::runtime_error("TlsClientContext::set_client_cert: mismatch");
    }
}

void* TlsClientContext::native_handle() const noexcept {
    return impl_->ctx;
}

// ---------- TlsSocket ----------

struct TlsSocket::Impl {
    int fd{-1};
    SSL* ssl{nullptr};
};

TlsSocket::~TlsSocket() {
    close();
}

TlsSocket::TlsSocket(TlsSocket&& other) noexcept = default;
TlsSocket& TlsSocket::operator=(TlsSocket&& other) noexcept = default;

TlsSocket TlsSocket::accept(int listener_fd, const TlsServerContext& ctx) {
    return accept(listener_fd, ctx, TlsAcceptOptions{});
}

TlsSocket TlsSocket::accept(int listener_fd,
                            const TlsServerContext& ctx,
                            const TlsAcceptOptions& opts) {
    const int fd = NetworkSocket::accept_one(listener_fd);
    if (fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return {};  // a non-blocking listener whose connection has gone
        }
        throw std::runtime_error(std::string{"TlsSocket::accept: TCP accept failed: "} +
                                 std::strerror(errno));
    }
    return handshake_accepted(fd, ctx, opts);
}

TlsSocket TlsSocket::handshake_accepted(int fd,
                                        const TlsServerContext& ctx,
                                        const TlsAcceptOptions& opts) {
    // The deadline runs from here, so it covers the whole handshake.
    // Saturating: a timeout meant as "no limit" is no deadline, not an
    // overflow into the past that fails every handshake at once.
    const auto deadline = opts.handshake_timeout.count() > 0
                              ? NetworkSocket::deadline_after(opts.handshake_timeout)
                              : std::chrono::steady_clock::time_point::max();
    if (fd < 0) {
        throw std::runtime_error("TlsSocket::accept: no socket to handshake on");
    }
    // Owned from here: every throw below closes the connection on the way out.
    TlsSocket out;
    out.impl_ = std::make_unique<Impl>();
    out.impl_->fd = fd;
    out.impl_->ssl =
        SSL_new(static_cast<SSL_CTX*>(const_cast<TlsServerContext&>(ctx).native_handle()));
    if (out.impl_->ssl == nullptr) {
        throw std::runtime_error("TlsSocket::accept: SSL_new failed");
    }
    SSL_set_fd(out.impl_->ssl, fd);
    CLINK_FAULT_POINT(clink::fault::points::kTlsAcceptBeforeHandshake);

    // Non-blocking, so every wait for the client happens in wait_ready below,
    // where the deadline and the wake reach it. A blocking SSL_accept waits
    // in recv() for as long as the client cares to send nothing, and the
    // thread doing it is usually the only one accepting connections.
    if (!set_nonblocking(fd, true)) {
        throw std::runtime_error(std::string{"TlsSocket::accept: cannot make the socket "
                                             "non-blocking: "} +
                                 std::strerror(errno));
    }
    auto& steps = *ctx.impl_;
    // Why the handshake is to be given up without another step, or nullptr.
    // The coordinator abandons one by shutting its socket down (at the
    // admission deadline, to evict it, or on stop) and wakes the stop wake;
    // neither can reach a thread waiting for a slot, so the wait checks this.
    const auto abandoned_because = [&]() -> const char* {
        if (opts.wake != nullptr && opts.wake->woken()) {
            return "stopping";
        }
        if (socket_hung_up(fd)) {
            return "the connection was shut down";
        }
        return nullptr;
    };
    const auto abandon = [](const char* why) {
        return std::runtime_error(std::string{"TlsSocket::accept: handshake abandoned: "} + why);
    };
    for (;;) {
        // One slot per step, released before any wait on the client, so the
        // CPU a flood of handshakes can take is bounded and a stalled one
        // holds nothing.
        switch (steps.acquire_step(deadline, [&] { return abandoned_because() != nullptr; })) {
            case TlsServerContext::Impl::StepWait::Acquired:
                break;
            case TlsServerContext::Impl::StepWait::TimedOut:
                throw std::runtime_error(
                    "TlsSocket::accept: handshake not completed within " +
                    std::to_string(opts.handshake_timeout.count()) +
                    " ms: waited for a handshake slot (max_concurrent_handshake_steps=" +
                    std::to_string(steps.step_limit()) + ")");
            case TlsServerContext::Impl::StepWait::Abandoned: {
                const char* why = abandoned_because();
                throw abandon(why != nullptr ? why : "the connection was shut down");
            }
        }
        int rc = 0;
        int ssl_error = SSL_ERROR_NONE;
        const char* gone = nullptr;
        try {
            CLINK_FAULT_POINT(clink::fault::points::kTlsHandshakeStep);
            // Again, now the slot is held: the wait for it can be long, and on
            // Linux a ClientHello that arrived before the socket was shut down
            // is still readable, so the step would spend its key exchange and
            // signature on a connection that has already been dropped.
            gone = abandoned_because();
            if (gone == nullptr) {
                ERR_clear_error();
                errno = 0;
                rc = SSL_accept(out.impl_->ssl);
                ssl_error = rc == 1 ? SSL_ERROR_NONE : SSL_get_error(out.impl_->ssl, rc);
            }
        } catch (...) {
            steps.release_step();
            throw;
        }
        // errno is what handshake_error reports: keep it across the release.
        const int step_errno = errno;
        steps.release_step();
        errno = step_errno;
        if (gone != nullptr) {
            throw abandon(gone);
        }
        if (rc == 1) {
            break;
        }
        if (ssl_error != SSL_ERROR_WANT_READ && ssl_error != SSL_ERROR_WANT_WRITE) {
            throw std::runtime_error("TlsSocket::accept: handshake failed: " +
                                     handshake_error(ssl_error));
        }
        switch (
            NetworkSocket::wait_ready(fd, ssl_error == SSL_ERROR_WANT_WRITE, opts.wake, deadline)) {
            case NetworkSocket::WaitResult::Ready:
                continue;
            case NetworkSocket::WaitResult::Woken:
                throw std::runtime_error("TlsSocket::accept: handshake abandoned: stopping");
            case NetworkSocket::WaitResult::TimedOut:
                throw std::runtime_error("TlsSocket::accept: handshake not completed within " +
                                         std::to_string(opts.handshake_timeout.count()) + " ms");
            case NetworkSocket::WaitResult::Failed:
                throw std::runtime_error(
                    std::string{"TlsSocket::accept: waiting for the handshake failed: "} +
                    std::strerror(errno));
        }
    }
    // Every reader of the connection from here on expects a blocking socket.
    if (!set_nonblocking(fd, false)) {
        throw std::runtime_error(std::string{"TlsSocket::accept: cannot make the socket "
                                             "blocking again: "} +
                                 std::strerror(errno));
    }
    return out;
}

TlsSocket TlsSocket::connect(const std::string& host,
                             std::uint16_t port,
                             const TlsClientContext& ctx) {
    const int fd = NetworkSocket::connect_to(host, port);
    if (fd < 0) {
        throw std::runtime_error("TlsSocket::connect: TCP connect failed");
    }
    TlsSocket out;
    out.impl_ = std::make_unique<Impl>();
    out.impl_->fd = fd;
    out.impl_->ssl =
        SSL_new(static_cast<SSL_CTX*>(const_cast<TlsClientContext&>(ctx).native_handle()));
    if (out.impl_->ssl == nullptr) {
        NetworkSocket::close(fd);
        throw std::runtime_error("TlsSocket::connect: SSL_new failed");
    }
    SSL_set_fd(out.impl_->ssl, fd);
    SSL_set_tlsext_host_name(out.impl_->ssl, host.c_str());
    if (SSL_connect(out.impl_->ssl) != 1) {
        const auto err = ossl_last_error();
        SSL_free(out.impl_->ssl);
        NetworkSocket::close(fd);
        out.impl_.reset();
        throw std::runtime_error("TlsSocket::connect: handshake failed: " + err);
    }
    return out;
}

bool TlsSocket::send_all(const std::byte* buf, std::size_t len) {
    if (!impl_ || impl_->ssl == nullptr) {
        return false;
    }
    while (len > 0) {
        const int n = SSL_write(impl_->ssl, buf, static_cast<int>(len));
        if (n <= 0) {
            return false;
        }
        buf += n;
        len -= static_cast<std::size_t>(n);
    }
    return true;
}

bool TlsSocket::recv_all(std::byte* buf, std::size_t len) {
    if (!impl_ || impl_->ssl == nullptr) {
        return false;
    }
    while (len > 0) {
        const int n = SSL_read(impl_->ssl, buf, static_cast<int>(len));
        if (n <= 0) {
            return false;
        }
        buf += n;
        len -= static_cast<std::size_t>(n);
    }
    return true;
}

void TlsSocket::shutdown_write() {
    if (impl_ && impl_->ssl != nullptr) {
        SSL_shutdown(impl_->ssl);
    }
}

void TlsSocket::shutdown_read() {
    if (impl_ && impl_->fd >= 0) {
        NetworkSocket::shutdown_read(impl_->fd);
    }
}

void TlsSocket::interrupt() {
    if (impl_ && impl_->fd >= 0) {
        NetworkSocket::shutdown_read(impl_->fd);
        NetworkSocket::shutdown_write(impl_->fd);
    }
}

void TlsSocket::close() {
    if (!impl_) {
        return;
    }
    if (impl_->ssl != nullptr) {
        SSL_free(impl_->ssl);
        impl_->ssl = nullptr;
    }
    if (impl_->fd >= 0) {
        NetworkSocket::close(impl_->fd);
        impl_->fd = -1;
    }
    impl_.reset();
}

bool TlsSocket::is_open() const noexcept {
    return impl_ && impl_->ssl != nullptr;
}

bool TlsSocket::set_recv_timeout(std::chrono::milliseconds timeout) {
    if (!impl_ || impl_->fd < 0) {
        return false;
    }
    // SSL_read on a blocking socket whose recv() times out returns <= 0 with
    // SSL_ERROR_WANT_READ, which recv_all reports as a failed read.
    struct timeval tv{};
    tv.tv_sec = static_cast<decltype(tv.tv_sec)>(timeout.count() / 1000);
    tv.tv_usec = static_cast<decltype(tv.tv_usec)>((timeout.count() % 1000) * 1000);
    return ::setsockopt(impl_->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0;
}

bool TlsSocket::set_send_timeout(std::chrono::milliseconds timeout) {
    if (!impl_ || impl_->fd < 0) {
        return false;
    }
    // SSL_write on a blocking socket whose send() times out returns <= 0 with
    // SSL_ERROR_WANT_WRITE, which send_all reports as a failed write.
    struct timeval tv{};
    tv.tv_sec = static_cast<decltype(tv.tv_sec)>(timeout.count() / 1000);
    tv.tv_usec = static_cast<decltype(tv.tv_usec)>((timeout.count() % 1000) * 1000);
    return ::setsockopt(impl_->fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == 0;
}

#else  // !CLINK_HAS_OPENSSL

struct TlsServerContext::Impl {};
struct TlsClientContext::Impl {};
struct TlsSocket::Impl {};

bool TlsServerContext::is_real_implementation() {
    return false;
}
bool TlsClientContext::is_real_implementation() {
    return false;
}

TlsServerContext::TlsServerContext(const std::string&, const std::string&) {
    throw std::runtime_error(
        "TlsServerContext: built without OpenSSL. Install OpenSSL and "
        "reconfigure cmake.");
}
TlsServerContext::~TlsServerContext() = default;
void TlsServerContext::set_client_ca_path(const std::string&) {}
void* TlsServerContext::native_handle() const noexcept {
    return nullptr;
}

TlsClientContext::TlsClientContext(const std::string&) {
    throw std::runtime_error("TlsClientContext: built without OpenSSL.");
}
TlsClientContext::~TlsClientContext() = default;
void TlsClientContext::set_client_cert(const std::string&, const std::string&) {}
void* TlsClientContext::native_handle() const noexcept {
    return nullptr;
}

TlsSocket::~TlsSocket() = default;
TlsSocket::TlsSocket(TlsSocket&&) noexcept = default;
TlsSocket& TlsSocket::operator=(TlsSocket&&) noexcept = default;
TlsSocket TlsSocket::accept(int, const TlsServerContext&) {
    return {};
}
TlsSocket TlsSocket::accept(int, const TlsServerContext&, const TlsAcceptOptions&) {
    return {};
}
TlsSocket TlsSocket::handshake_accepted(int fd, const TlsServerContext&, const TlsAcceptOptions&) {
    NetworkSocket::close(fd);
    return {};
}
bool TlsSocket::set_recv_timeout(std::chrono::milliseconds) {
    return false;
}
bool TlsSocket::set_send_timeout(std::chrono::milliseconds) {
    return false;
}
void TlsServerContext::set_max_concurrent_handshake_steps(std::size_t) {}
TlsSocket TlsSocket::connect(const std::string&, std::uint16_t, const TlsClientContext&) {
    return {};
}
bool TlsSocket::send_all(const std::byte*, std::size_t) {
    return false;
}
bool TlsSocket::recv_all(std::byte*, std::size_t) {
    return false;
}
void TlsSocket::shutdown_write() {}
void TlsSocket::shutdown_read() {}
void TlsSocket::interrupt() {}
void TlsSocket::close() {}
bool TlsSocket::is_open() const noexcept {
    return false;
}

#endif

}  // namespace clink::network
