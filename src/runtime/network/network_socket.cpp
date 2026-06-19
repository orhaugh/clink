#include "clink/runtime/network/network_socket.hpp"

#include <cerrno>
#include <csignal>
#include <cstring>
#include <netdb.h>
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

}  // namespace

int NetworkSocket::connect_to(const std::string& host, std::uint16_t port) {
    // Resolve host via getaddrinfo so we accept both numeric IPs and
    // DNS names. The earlier inet_pton-only path silently failed for
    // hostnames (e.g. docker-compose service names), which made
    // cross-process testing on anything but 127.0.0.1 break.
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    const std::string port_str = std::to_string(port);
    if (::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0 || res == nullptr) {
        return -1;
    }

    int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) {
        ::freeaddrinfo(res);
        return -1;
    }
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        ::close(fd);
        ::freeaddrinfo(res);
        return -1;
    }
    ::freeaddrinfo(res);
    return fd;
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
    int fd = ::accept(listener_fd, reinterpret_cast<sockaddr*>(&peer), &len);
    if (fd < 0) {
        return -1;
    }
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

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
