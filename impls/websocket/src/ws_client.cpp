#include "clink/websocket/ws_client.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <random>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <utility>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#ifdef CLINK_WEBSOCKET_TLS
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#endif

namespace clink::websocket {

namespace {

using Clock = std::chrono::steady_clock;

[[noreturn]] void throw_errno(const std::string& name, const std::string& stage) {
    throw std::runtime_error(name + ": " + stage + ": " + std::strerror(errno));
}

std::chrono::milliseconds remaining(Clock::time_point deadline) {
    const auto left =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
    return left.count() > 0 ? left : std::chrono::milliseconds{0};
}

// One 4-byte mask key per frame, from a per-connection PRNG seeded by
// random_device. Masking exists to defeat proxy cache poisoning, not for
// secrecy, so a PRNG is the appropriate strength.
class MaskKeys {
public:
    MaskKeys() : rng_(std::random_device{}()) {}
    std::uint32_t next() { return rng_(); }

private:
    std::mt19937 rng_;
};

}  // namespace

struct WsClient::Impl {
    WsClientOptions opts;
    WsUrl url;
    int fd{-1};
    bool ws_open{false};     // opening handshake completed
    bool close_sent{false};  // our close frame has gone out
    MessageReader reader;    // frame reassembly
    MaskKeys mask_keys;

#ifdef CLINK_WEBSOCKET_TLS
    SSL_CTX* ssl_ctx{nullptr};
    SSL* ssl{nullptr};
#endif

    explicit Impl(WsClientOptions o) : opts(std::move(o)), reader(opts.max_message_bytes) {
        auto parsed = parse_ws_url(opts.url);
        if (!parsed.has_value()) {
            throw std::runtime_error(opts.name + ": malformed WebSocket URL '" + opts.url +
                                     "' (expected ws://host[:port]/path or wss://...)");
        }
        url = std::move(*parsed);
#ifndef CLINK_WEBSOCKET_TLS
        if (url.tls) {
            throw std::runtime_error(
                opts.name + ": '" + opts.url +
                "' is wss:// but this build has no TLS support (the websocket impl was built "
                "without OpenSSL); use ws:// or rebuild with OpenSSL available");
        }
#endif
    }

    ~Impl() { teardown(); }

    void teardown() noexcept {
#ifdef CLINK_WEBSOCKET_TLS
        if (ssl != nullptr) {
            SSL_free(ssl);
            ssl = nullptr;
        }
        if (ssl_ctx != nullptr) {
            SSL_CTX_free(ssl_ctx);
            ssl_ctx = nullptr;
        }
#endif
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
        ws_open = false;
    }

    // ---- raw I/O -----------------------------------------------------------

    // Wait for readability/writability with a deadline. Returns false on
    // timeout, throws on a poll error.
    bool wait_io(short events, Clock::time_point deadline) {
        while (true) {
            struct pollfd pfd = {};
            pfd.fd = fd;
            pfd.events = events;
            const auto left = remaining(deadline);
            if (left.count() == 0) {
                return false;
            }
            const int rc = ::poll(&pfd, 1, static_cast<int>(left.count()));
            if (rc > 0) {
                return true;
            }
            if (rc == 0) {
                return false;
            }
            if (errno == EINTR) {
                continue;
            }
            throw_errno(opts.name, "poll");
        }
    }

    // Send all of `data` before `deadline`. Throws on failure or timeout.
    void send_all(const char* data, std::size_t len, Clock::time_point deadline) {
        std::size_t off = 0;
        while (off < len) {
#ifdef CLINK_WEBSOCKET_TLS
            if (ssl != nullptr) {
                const int rc = SSL_write(ssl, data + off, static_cast<int>(len - off));
                if (rc > 0) {
                    off += static_cast<std::size_t>(rc);
                    continue;
                }
                const int err = SSL_get_error(ssl, rc);
                if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
                    if (!wait_io(err == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT, deadline)) {
                        throw std::runtime_error(opts.name + ": send timed out");
                    }
                    continue;
                }
                throw std::runtime_error(opts.name + ": TLS send failed");
            }
#endif
#ifdef MSG_NOSIGNAL
            const auto rc = ::send(fd, data + off, len - off, MSG_NOSIGNAL);
#else
            const auto rc = ::send(fd, data + off, len - off, 0);
#endif
            if (rc > 0) {
                off += static_cast<std::size_t>(rc);
                continue;
            }
            if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                if (!wait_io(POLLOUT, deadline)) {
                    throw std::runtime_error(opts.name + ": send timed out");
                }
                continue;
            }
            if (rc < 0 && errno == EINTR) {
                continue;
            }
            throw_errno(opts.name, "send");
        }
    }

    // One read of whatever is available (up to `cap`), waiting until the
    // deadline for the first byte. Returns 0 on timeout, -1 on orderly EOF.
    long recv_some(char* buf, std::size_t cap, Clock::time_point deadline) {
#ifdef CLINK_WEBSOCKET_TLS
        if (ssl != nullptr) {
            while (true) {
                // Drain buffered TLS records before polling the socket.
                if (SSL_pending(ssl) == 0 && !wait_io(POLLIN, deadline)) {
                    return 0;
                }
                const int rc = SSL_read(ssl, buf, static_cast<int>(cap));
                if (rc > 0) {
                    return rc;
                }
                const int err = SSL_get_error(ssl, rc);
                if (err == SSL_ERROR_ZERO_RETURN) {
                    return -1;
                }
                if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
                    continue;  // wait_io above re-arms
                }
                // A peer that drops TCP without a TLS close-notify surfaces
                // as SSL_ERROR_SYSCALL with errno 0 or ECONNRESET: EOF.
                if (err == SSL_ERROR_SYSCALL &&
                    (errno == 0 || errno == ECONNRESET || errno == EPIPE)) {
                    return -1;
                }
                throw std::runtime_error(opts.name + ": TLS receive failed");
            }
        }
#endif
        while (true) {
            if (!wait_io(POLLIN, deadline)) {
                return 0;
            }
            const auto rc = ::recv(fd, buf, cap, 0);
            if (rc > 0) {
                return static_cast<long>(rc);
            }
            if (rc == 0) {
                return -1;  // orderly EOF
            }
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            if (errno == ECONNRESET) {
                return -1;
            }
            throw_errno(opts.name, "recv");
        }
    }

    // ---- connection establishment -------------------------------------------

    void tcp_connect(Clock::time_point deadline) {
        struct addrinfo hints = {};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        struct addrinfo* res = nullptr;
        const std::string port_str = std::to_string(url.port);
        const int gai = ::getaddrinfo(url.host.c_str(), port_str.c_str(), &hints, &res);
        if (gai != 0 || res == nullptr) {
            throw std::runtime_error(opts.name + ": could not resolve '" + url.host +
                                     "': " + ::gai_strerror(gai));
        }
        std::string last_error = "no addresses";
        for (struct addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
            fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
            if (fd < 0) {
                last_error = std::strerror(errno);
                continue;
            }
            // Non-blocking connect so the open_timeout deadline holds even
            // against an unresponsive host.
            const int flags = ::fcntl(fd, F_GETFL, 0);
            ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
#ifdef SO_NOSIGPIPE
            int one = 1;
            ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
            if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0 ||
                (errno == EINPROGRESS && wait_io(POLLOUT, deadline) && sock_error() == 0)) {
                int nd = 1;
                ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));
                ::freeaddrinfo(res);
                return;
            }
            last_error = std::strerror(sock_error() != 0 ? sock_error() : errno);
            ::close(fd);
            fd = -1;
        }
        ::freeaddrinfo(res);
        throw std::runtime_error(opts.name + ": could not connect to " + url.host + ":" + port_str +
                                 ": " + last_error);
    }

    int sock_error() {
        int err = 0;
        socklen_t len = sizeof(err);
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
        return err;
    }

#ifdef CLINK_WEBSOCKET_TLS
    void tls_connect(Clock::time_point deadline) {
        ssl_ctx = SSL_CTX_new(TLS_client_method());
        if (ssl_ctx == nullptr) {
            throw std::runtime_error(opts.name + ": SSL_CTX_new failed");
        }
        SSL_CTX_set_default_verify_paths(ssl_ctx);
        ssl = SSL_new(ssl_ctx);
        if (ssl == nullptr) {
            throw std::runtime_error(opts.name + ": SSL_new failed");
        }
        SSL_set_fd(ssl, fd);
        // SNI + hostname verification (disabled together by tls_verify=false,
        // for self-signed endpoints in development).
        SSL_set_tlsext_host_name(ssl, url.host.c_str());
        if (opts.tls_verify) {
            SSL_set_verify(ssl, SSL_VERIFY_PEER, nullptr);
            SSL_set1_host(ssl, url.host.c_str());
        }
        while (true) {
            const int rc = SSL_connect(ssl);
            if (rc == 1) {
                return;
            }
            const int err = SSL_get_error(ssl, rc);
            if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
                if (!wait_io(err == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT, deadline)) {
                    throw std::runtime_error(opts.name + ": TLS handshake timed out");
                }
                continue;
            }
            const auto reason = ERR_reason_error_string(ERR_peek_last_error());
            throw std::runtime_error(opts.name + ": TLS handshake with '" + url.host + "' failed" +
                                     (reason != nullptr ? std::string{": "} + reason : ""));
        }
    }
#endif

    void ws_handshake(Clock::time_point deadline) {
        std::array<std::uint8_t, 16> nonce{};
        std::random_device rd;
        for (auto& b : nonce) {
            b = static_cast<std::uint8_t>(rd() & 0xFF);
        }
        const std::string key = client_key_from_nonce(nonce);
        std::string host_header = url.host;
        const bool default_port = (url.tls && url.port == 443) || (!url.tls && url.port == 80);
        if (!default_port) {
            host_header += ":" + std::to_string(url.port);
        }
        const std::string request = build_handshake_request(host_header, url.path, key);
        send_all(request.data(), request.size(), deadline);

        std::string response;
        while (true) {
            if (auto head = parse_handshake_response(response); head.has_value()) {
                if (head->status != 101) {
                    throw std::runtime_error(opts.name + ": server refused the upgrade (HTTP " +
                                             std::to_string(head->status) + ")");
                }
                const auto* accept = find_header(*head, "sec-websocket-accept");
                if (accept == nullptr || *accept != accept_key_for(key)) {
                    throw std::runtime_error(opts.name +
                                             ": handshake Sec-WebSocket-Accept mismatch");
                }
                // Bytes after the head are already frames.
                if (response.size() > head->header_bytes) {
                    reader.append(response.data() + head->header_bytes,
                                  response.size() - head->header_bytes);
                }
                ws_open = true;
                return;
            }
            if (response.size() > 64 * 1024) {
                throw std::runtime_error(opts.name + ": handshake response head exceeds 64 KiB");
            }
            char buf[4096];
            const long n = recv_some(buf, sizeof(buf), deadline);
            if (n == 0) {
                throw std::runtime_error(opts.name + ": handshake timed out");
            }
            if (n < 0) {
                throw std::runtime_error(opts.name +
                                         ": connection closed during the opening handshake");
            }
            response.append(buf, static_cast<std::size_t>(n));
        }
    }

    void send_frame(Opcode op, const std::string& payload) {
        const std::string frame =
            encode_frame(op, payload, /*mask=*/true, mask_keys.next(), /*fin=*/true);
        // A frame send gets a fresh short deadline: the socket is healthy or
        // it is not; open_timeout doubles as the bound.
        send_all(frame.data(), frame.size(), Clock::now() + opts.open_timeout);
    }
};

WsClient::WsClient(WsClientOptions opts) : impl_(std::make_unique<Impl>(std::move(opts))) {}

WsClient::~WsClient() = default;

void WsClient::open() {
    if (impl_->ws_open) {
        return;
    }
    impl_->teardown();
    // A fresh MessageReader per connection: fragments never straddle sockets.
    impl_->reader = MessageReader{impl_->opts.max_message_bytes};
    impl_->close_sent = false;
    const auto deadline = Clock::now() + impl_->opts.open_timeout;
    impl_->tcp_connect(deadline);
#ifdef CLINK_WEBSOCKET_TLS
    if (impl_->url.tls) {
        impl_->tls_connect(deadline);
    }
#endif
    impl_->ws_handshake(deadline);
}

bool WsClient::connected() const noexcept {
    return impl_->ws_open;
}

void WsClient::send_text(const std::string& payload) {
    if (!impl_->ws_open) {
        throw std::runtime_error(impl_->opts.name + ": send_text on a closed connection");
    }
    impl_->send_frame(Opcode::Text, payload);
}

void WsClient::send_ping(const std::string& payload) {
    if (!impl_->ws_open) {
        throw std::runtime_error(impl_->opts.name + ": send_ping on a closed connection");
    }
    impl_->send_frame(Opcode::Ping, payload);
}

WsClient::PollResult WsClient::poll(std::chrono::milliseconds block) {
    PollResult out;
    if (!impl_->ws_open) {
        out.closed = true;
        return out;
    }
    const auto deadline = Clock::now() + block;
    while (true) {
        // Drain everything already decoded before touching the socket.
        std::string error;
        while (auto msg = impl_->reader.next_message(error)) {
            switch (msg->opcode) {
                case Opcode::Text:
                    out.texts.push_back(std::move(msg->payload));
                    break;
                case Opcode::Binary:
                    ++out.binaries_skipped;
                    break;
                case Opcode::Ping:
                    // Mandatory pong, echoing the ping payload.
                    impl_->send_frame(Opcode::Pong, msg->payload);
                    break;
                case Opcode::Pong:
                    break;  // keepalive answered; nothing to deliver
                case Opcode::Close:
                    if (!impl_->close_sent) {
                        try {
                            impl_->send_frame(Opcode::Close, msg->payload);
                        } catch (const std::exception&) {
                            // The reply is best-effort; the peer is going away.
                        }
                        impl_->close_sent = true;
                    }
                    impl_->teardown();
                    out.closed = true;
                    return out;
                case Opcode::Continuation:
                    break;  // never surfaced by MessageReader
            }
        }
        if (!error.empty()) {
            impl_->teardown();
            throw std::runtime_error(impl_->opts.name + ": protocol error: " + error);
        }
        if (!out.texts.empty() || out.binaries_skipped > 0) {
            return out;  // deliver what we have; do not wait out the block
        }
        char buf[16 * 1024];
        const long n = impl_->recv_some(buf, sizeof(buf), deadline);
        if (n == 0) {
            return out;  // block elapsed quietly
        }
        if (n < 0) {
            impl_->teardown();
            out.closed = true;
            return out;
        }
        impl_->reader.append(buf, static_cast<std::size_t>(n));
    }
}

void WsClient::close() {
    if (impl_->ws_open && !impl_->close_sent) {
        try {
            impl_->send_frame(Opcode::Close, {});
        } catch (const std::exception&) {
            // Best-effort: the socket may already be gone.
        }
        impl_->close_sent = true;
    }
    impl_->teardown();
}

}  // namespace clink::websocket
