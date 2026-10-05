#include "clink/runtime/network/network_socket.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdexcept>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

namespace clink::network {

namespace {

// On Linux, writing to a closed socket raises SIGPIPE - which by
// default terminates the process. macOS / BSD have SO_NOSIGPIPE per
// socket, but Linux only offers MSG_NOSIGNAL on each send call. The
// least-invasive cross-platform fix is to ignore SIGPIPE process-wide
// and rely on the failed-write errno (EPIPE) instead. Installed once
// at static init.
struct SigpipeIgnorer {
    SigpipeIgnorer() noexcept { std::signal(SIGPIPE, SIG_IGN); }
};
[[maybe_unused]] SigpipeIgnorer kSigpipeIgnorer;

// Resolve host via getaddrinfo so we accept both numeric IPs and DNS names.
// The earlier inet_pton-only path silently failed for hostnames (e.g.
// docker-compose service names), which made cross-process testing on
// anything but 127.0.0.1 break. Null when the host does not resolve.
addrinfo* resolve_ipv4(const std::string& host, std::uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    const std::string port_str = std::to_string(port);
    if (::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0) {
        return nullptr;
    }
    return res;
}

int new_tcp_socket(const addrinfo& ai) {
    const int fd = ::socket(ai.ai_family, ai.ai_socktype, ai.ai_protocol);
    if (fd >= 0) {
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }
    return fd;
}

}  // namespace

int NetworkSocket::connect_to(const std::string& host, std::uint16_t port) {
    addrinfo* res = resolve_ipv4(host, port);
    if (res == nullptr) {
        return -1;
    }

    int fd = new_tcp_socket(*res);
    if (fd < 0) {
        ::freeaddrinfo(res);
        return -1;
    }

    if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        // A blocking connect interrupted by a signal (EINTR) continues in
        // the background and the socket's state is ambiguous; the portable
        // recovery is a fresh socket and a fresh connect, bounded so a
        // genuinely unreachable peer still fails promptly.
        int attempts = 0;
        while (errno == EINTR && attempts < 3) {
            ::close(fd);
            fd = new_tcp_socket(*res);
            if (fd < 0) {
                break;
            }
            if (::connect(fd, res->ai_addr, res->ai_addrlen) == 0) {
                ::freeaddrinfo(res);
                return fd;
            }
            ++attempts;
        }
        if (fd >= 0) {
            ::close(fd);
        }
        ::freeaddrinfo(res);
        return -1;
    }
    ::freeaddrinfo(res);
    return fd;
}

NetworkSocket::ConnectAttempt NetworkSocket::connect_within(const std::string& host,
                                                            std::uint16_t port,
                                                            std::chrono::milliseconds timeout) {
    ConnectAttempt out;
    addrinfo* res = resolve_ipv4(host, port);
    if (res == nullptr) {
        out.unresolved = true;
        return out;
    }
    // From here, not from the call (see the header).
    if (timeout.count() > 0) {
        out.deadline = deadline_after(timeout);
    }
    const int fd = new_tcp_socket(*res);
    // Closes fd and records why.
    const auto fail = [&out, fd](int err) {
        if (fd >= 0) {
            ::close(fd);
        }
        out.error = err;
        return out;
    };
    if (fd < 0) {
        const int err = errno;
        ::freeaddrinfo(res);
        return fail(err);
    }
    const int flags = ::fcntl(fd, F_GETFL);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
        const int err = errno;
        ::freeaddrinfo(res);
        return fail(err);
    }
    const int rc = ::connect(fd, res->ai_addr, res->ai_addrlen);
    const int connect_errno = errno;
    ::freeaddrinfo(res);
    // A connect interrupted by a signal (EINTR) carries on in the background
    // like one in progress, and completes or fails the same way.
    if (rc != 0 && connect_errno != EINPROGRESS && connect_errno != EINTR) {
        return fail(connect_errno);
    }
    if (rc != 0) {
        switch (wait_ready(fd, /*want_write=*/true, nullptr, out.deadline)) {
            case WaitResult::Ready:
                break;
            case WaitResult::TimedOut:
                return fail(ETIMEDOUT);
            case WaitResult::Woken:  // no wake was given
            case WaitResult::Failed:
                return fail(errno);
        }
        int err = 0;
        socklen_t len = sizeof(err);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0) {
            return fail(errno);
        }
        if (err != 0) {
            return fail(err);
        }
    }
    // Every reader of the socket from here on expects a blocking one.
    if (::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) != 0) {
        return fail(errno);
    }
    out.fd = fd;
    return out;
}

int NetworkSocket::listen_on(std::uint16_t& port, std::string_view bind_host) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    if (bind_host == "0.0.0.0") {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    } else if (bind_host == "127.0.0.1") {
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    } else {
        // Specific IPv4 address (e.g., "10.0.1.5"). inet_pton accepts a
        // C-string; std::string_view isn't NUL-terminated so copy first.
        const std::string host_str{bind_host};
        if (::inet_pton(AF_INET, host_str.c_str(), &addr.sin_addr) != 1) {
            ::close(fd);
            return -1;
        }
    }
    addr.sin_port = htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
        ::close(fd);
        return -1;
    }
    port = ntohs(addr.sin_port);

    // Backlog accepts spurious SYN retries from the same producer
    // during startup: macOS retransmits SYNs aggressively if the
    // accept loop hasn't reached accept() yet, and we historically
    // ran with backlog=1 which dropped retries on the floor. 128
    // is the standard "way more than needed" choice that matches
    // most Linux defaults; it adds no overhead since the kernel
    // only allocates queue slots on actual pending connections.
    if (::listen(fd, /*backlog*/ 128) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

int NetworkSocket::accept_one(int listener_fd) {
    sockaddr_in peer{};
    socklen_t len = sizeof(peer);
    int fd = -1;
    for (;;) {
        fd = ::accept(listener_fd, reinterpret_cast<sockaddr*>(&peer), &len);
        if (fd >= 0) {
            break;
        }
        // EINTR (a signal landed while blocked) and ECONNABORTED (the
        // pending connection died in the backlog before we reached it)
        // are transients the next accept can succeed past - and at graph
        // width they stop being rare: a wide deploy has hundreds of
        // accepts in flight, one takes the transient, and treating it as
        // terminal turned into a CLEAN end-of-stream for the task, whose
        // upstream then hit the reset backlog connection as "peer gone"
        // and a whole-job restart followed (QUAL-06, followups item 72:
        // intermittent at ~292 deployed tasks, deterministic at ~1,160).
        // Anything else stays terminal, with errno preserved for the
        // caller to classify. EBADF/EINVAL are among those: the listener
        // was closed or is no longer listening. The waiting overload below
        // reports its own wake as ECANCELED, so a caller stopped that way
        // never has to read them as shutdown.
        if (errno == EINTR || errno == ECONNABORTED) {
            continue;
        }
        return -1;
    }
    // An accepted socket inherits O_NONBLOCK from a non-blocking listener on
    // Darwin and the BSDs (not on Linux), and every reader of an accepted fd
    // - recv_all, the TLS handshake - expects a blocking one.
    if (const int flags = ::fcntl(fd, F_GETFL); flags >= 0 && (flags & O_NONBLOCK) != 0) {
        ::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
    }
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

bool NetworkSocket::wait_for_connection(int listener_fd, const AcceptWake& wake) {
    return wait_for_connection(listener_fd, wake, std::chrono::steady_clock::time_point::max());
}

bool NetworkSocket::wait_for_connection(int listener_fd,
                                        const AcceptWake& wake,
                                        std::chrono::steady_clock::time_point deadline) {
    // poll() skips a negative fd, which would turn a missing listener into a
    // wait only a wake can end; fail it the way accept() would.
    if (listener_fd < 0) {
        errno = EBADF;
        return false;
    }
    // Non-blocking, so the accept that follows can never park: readiness can
    // be stale by the time accept() runs (the pending connection was reset
    // and dropped from the queue), and a blocking accept() would then sleep
    // where only a new connection could wake it.
    if (const int flags = ::fcntl(listener_fd, F_GETFL); flags >= 0 && (flags & O_NONBLOCK) == 0) {
        ::fcntl(listener_fd, F_SETFL, flags | O_NONBLOCK);
    }
    const bool bounded = deadline != std::chrono::steady_clock::time_point::max();
    for (;;) {
        int timeout_ms = -1;
        if (bounded) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                errno = ETIMEDOUT;
                return false;
            }
            // Rounded up, as in wait_ready, so the wait never ends just short
            // of the deadline and spins on a zero timeout.
            const auto left = std::chrono::ceil<std::chrono::milliseconds>(deadline - now).count();
            timeout_ms = static_cast<int>(std::min<long long>(left, 24LL * 60 * 60 * 1000));
        }
        pollfd fds[2] = {{listener_fd, POLLIN, 0}, {wake.fd(), POLLIN, 0}};
        const nfds_t nfds = wake.fd() >= 0 ? 2 : 1;
        const int rc = ::poll(fds, nfds, timeout_ms);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        // Woken wins over a pending connection: the owner is stopping and
        // will close the listener, which resets anything still queued.
        if (wake.woken()) {
            errno = ECANCELED;
            return false;
        }
        if ((fds[0].revents & POLLIN) != 0) {
            return true;
        }
        // Ready without a connection and without a wake: the descriptor is
        // not a usable listener (closed, or shut down by someone else).
        // Waiting again would spin, so fail it as accept() would.
        if (fds[0].revents != 0) {
            errno = (fds[0].revents & POLLNVAL) != 0 ? EBADF : EINVAL;
            return false;
        }
        // rc == 0: the deadline, checked at the top.
    }
}

int NetworkSocket::accept_one(int listener_fd, const AcceptWake& wake) {
    for (;;) {
        if (!wait_for_connection(listener_fd, wake)) {
            return -1;
        }
        const int fd = accept_one(listener_fd);
        if (fd >= 0) {
            // Woken wins here too: on Darwin the wake leaves the listener
            // alone, so a connection can still be accepted after it.
            if (wake.woken()) {
                NetworkSocket::close(fd);
                errno = ECANCELED;
                return -1;
            }
            return fd;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            continue;
        }
        // A wake between the wait and the accept (on Linux it shuts the
        // listener down, so accept() fails EINVAL) is still a wake.
        if (wake.woken()) {
            errno = ECANCELED;
        }
        return -1;
    }
}

std::chrono::steady_clock::time_point NetworkSocket::deadline_after(
    std::chrono::milliseconds timeout) {
    using clock = std::chrono::steady_clock;
    const auto now = clock::now();
    if (timeout.count() <= 0) {
        return now;
    }
    // Compared in the clock's own unit, so neither the conversion of
    // `timeout` nor the addition can overflow.
    const auto headroom = clock::time_point::max() - now;
    if (timeout >= std::chrono::duration_cast<std::chrono::milliseconds>(headroom)) {
        return clock::time_point::max();
    }
    return now + std::chrono::duration_cast<clock::duration>(timeout);
}

NetworkSocket::WaitResult NetworkSocket::wait_ready(
    int fd,
    bool want_write,
    const AcceptWake* wake,
    std::chrono::steady_clock::time_point deadline) {
    if (fd < 0) {
        errno = EBADF;
        return WaitResult::Failed;
    }
    const bool bounded = deadline != std::chrono::steady_clock::time_point::max();
    for (;;) {
        if (wake != nullptr && wake->woken()) {
            return WaitResult::Woken;
        }
        int timeout_ms = -1;
        if (bounded) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                return WaitResult::TimedOut;
            }
            // Rounded up, so a wait never ends just short of the deadline and
            // spins on a zero timeout.
            const auto left = std::chrono::ceil<std::chrono::milliseconds>(deadline - now).count();
            timeout_ms = static_cast<int>(std::min<long long>(left, 24LL * 60 * 60 * 1000));
        }
        pollfd fds[2] = {{fd, static_cast<short>(want_write ? POLLOUT : POLLIN), 0}, {-1, 0, 0}};
        nfds_t nfds = 1;
        if (wake != nullptr) {
#if defined(__linux__)
            // The wake shuts the listener down, which poll() reports as
            // POLLHUP whatever is asked for. Asking for nothing keeps a
            // pending connection, which is POLLIN, from waking this wait.
            fds[1] = {wake->listener_fd_, 0, 0};
#else
            fds[1] = {wake->read_fd_, POLLIN, 0};
#endif
            nfds = 2;
        }
        const int rc = ::poll(fds, nfds, timeout_ms);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return WaitResult::Failed;
        }
        if (wake != nullptr && wake->woken()) {
            return WaitResult::Woken;
        }
        // An error or hangup counts as ready: the I/O call the caller makes
        // next is what reports it.
        if (fds[0].revents != 0) {
            return WaitResult::Ready;
        }
        // The wake's descriptor is ready but nobody woke it: it is not usable
        // (the listener was closed under us, say). Waiting again would spin.
        if (nfds == 2 && fds[1].revents != 0) {
            errno = (fds[1].revents & POLLNVAL) != 0 ? EBADF : EINVAL;
            return WaitResult::Failed;
        }
        // rc == 0: the deadline, checked at the top.
    }
}

bool NetworkSocket::wait_for_wake(const AcceptWake& wake,
                                  std::chrono::steady_clock::time_point deadline) {
    const bool bounded = deadline != std::chrono::steady_clock::time_point::max();
    for (;;) {
        if (wake.woken()) {
            return true;
        }
        int timeout_ms = -1;
        if (bounded) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                return false;
            }
            // Rounded up, as in wait_ready.
            const auto left = std::chrono::ceil<std::chrono::milliseconds>(deadline - now).count();
            timeout_ms = static_cast<int>(std::min<long long>(left, 24LL * 60 * 60 * 1000));
        }
#if defined(__linux__)
        // The wake shuts the listener down, which poll() reports as POLLHUP
        // whatever is asked for; asking for nothing keeps a pending
        // connection (POLLIN) from ending the wait.
        pollfd p{wake.listener_fd_, 0, 0};
#else
        pollfd p{wake.read_fd_, POLLIN, 0};
#endif
        const int rc = ::poll(&p, 1, timeout_ms);
        if (rc < 0 && errno != EINTR) {
            // poll() itself failing: report the wake's state, and let the
            // caller's next wait (on the listener) see the failure.
            return wake.woken();
        }
        if (rc > 0 && !wake.woken()) {
            // Ready but nobody woke it: not usable (the listener closed under
            // us, say). Waiting again would spin.
            return false;
        }
    }
}

#if defined(__linux__)

// Linux: shutdown() on the listener is the wake (see the header). No pipe.
AcceptWake::AcceptWake(int listener_fd) : listener_fd_(listener_fd) {}

AcceptWake::~AcceptWake() = default;

void AcceptWake::wake() noexcept {
    if (!woken_.exchange(true, std::memory_order_acq_rel) && listener_fd_ >= 0) {
        ::shutdown(listener_fd_, SHUT_RD);
    }
}

#else

AcceptWake::AcceptWake(int listener_fd) : listener_fd_(listener_fd) {
    int fds[2] = {-1, -1};
    if (::pipe(fds) != 0) {
        throw std::runtime_error(std::string{"AcceptWake: pipe failed: "} + std::strerror(errno));
    }
    read_fd_ = fds[0];
    write_fd_ = fds[1];
    // Not inherited by a spawned child, and a wake can never block its
    // caller: once the pipe is full it is readable anyway.
    for (const int fd : fds) {
        ::fcntl(fd, F_SETFD, FD_CLOEXEC);
    }
    ::fcntl(write_fd_, F_SETFL, ::fcntl(write_fd_, F_GETFL) | O_NONBLOCK);
}

AcceptWake::~AcceptWake() {
    ::close(read_fd_);
    ::close(write_fd_);
}

void AcceptWake::wake() noexcept {
    // Flag first: a waiter that sees the pipe ready reads it as a wake.
    woken_.store(true, std::memory_order_release);
    const char byte = 1;
    // EAGAIN means the pipe is already full, which is already a wake.
    while (::write(write_fd_, &byte, 1) < 0 && errno == EINTR) {
    }
}

#endif

bool NetworkSocket::send_all(int fd, const std::byte* buf, std::size_t len) {
    while (len > 0) {
        const auto n = ::send(fd, buf, len, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        buf += n;
        len -= static_cast<std::size_t>(n);
    }
    return true;
}

bool NetworkSocket::recv_all(int fd, std::byte* buf, std::size_t len) {
    while (len > 0) {
        const auto n = ::recv(fd, buf, len, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;  // peer closed
        }
        buf += n;
        len -= static_cast<std::size_t>(n);
    }
    return true;
}

void NetworkSocket::shutdown_write(int fd) {
    ::shutdown(fd, SHUT_WR);
}

void NetworkSocket::shutdown_read(int fd) {
    ::shutdown(fd, SHUT_RD);
}

void NetworkSocket::close(int fd) {
    if (fd >= 0) {
        ::close(fd);
    }
}

}  // namespace clink::network
