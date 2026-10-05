#include "clink/runtime/network/tls_connection.hpp"

#include <atomic>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "clink/runtime/network/network_socket.hpp"

namespace clink::network {

namespace {

class TlsConnectionImpl final : public Connection {
public:
    TlsConnectionImpl(TlsSocket sock, std::shared_ptr<void> ctx_anchor)
        : sock_(std::move(sock)), ctx_anchor_(std::move(ctx_anchor)) {}

    // The session is freed here, once every thread that used the connection
    // has let go of it, never by close() or shutdown_read(): see close().
    ~TlsConnectionImpl() override { sock_.close(); }

    TlsConnectionImpl(const TlsConnectionImpl&) = delete;
    TlsConnectionImpl& operator=(const TlsConnectionImpl&) = delete;

    // Serialize sends: the coordinator writes a single connection from multiple threads
    // (periodic checkpoint loop + client-triggered savepoint, deploy, cancel).
    // Concurrent SSL_write on one SSL* is undefined and interleaves records;
    // this lock makes each frame's send atomic. See PlainTcpConnection::send_all.
    bool send_all(const std::byte* buf, std::size_t len) override {
        std::lock_guard<std::mutex> lk(send_mu_);
        if (closed_.load(std::memory_order_acquire)) {
            return false;
        }
        return sock_.send_all(buf, len);
    }

    bool recv_all(std::byte* buf, std::size_t len) override {
        if (closed_.load(std::memory_order_acquire)) {
            return false;
        }
        return sock_.recv_all(buf, len);
    }

    void shutdown_write() override { sock_.shutdown_write(); }

    // Wakes a reader blocked in recv_all by shutting the socket down for
    // reading: SSL_read then sees the end of the stream and fails. It used to
    // free the session instead, which pulled the SSL out from under that very
    // reader (a use-after-free in SSL_read on every Worker::stop() over TLS).
    void shutdown_read() override { sock_.shutdown_read(); }

    // The cross-thread wake, not the release, as for plain TCP: shuts the
    // socket down both ways so a blocked reader or writer fails, and refuses
    // further IO. The destructor frees the session and the descriptor.
    void close() override {
        if (!closed_.exchange(true, std::memory_order_acq_rel)) {
            sock_.interrupt();
        }
    }

    bool is_open() const noexcept override {
        return !closed_.load(std::memory_order_acquire) && sock_.is_open();
    }

    // The coordinator bounds a new connection's first frame with this; without
    // it, a TLS client that completed its handshake and sent nothing held the
    // accept thread for ever.
    bool set_recv_timeout(std::chrono::milliseconds timeout) override {
        return sock_.set_recv_timeout(timeout);
    }

    // The coordinator bounds its replies to a peer it has not yet admitted
    // with this, so a TLS peer that never reads cannot hold the replying
    // thread in SSL_write.
    bool set_send_timeout(std::chrono::milliseconds timeout) override {
        return sock_.set_send_timeout(timeout);
    }

private:
    TlsSocket sock_;
    std::mutex send_mu_;
    std::atomic<bool> closed_{false};
    // Holds the SSL_CTX shared_ptr alive for as long as this connection
    // exists. Without it, a TlsServerContext / TlsClientContext destroyed
    // before its accepted/connected sockets would free SSL_CTX while
    // SSL_free was still expected to call back into it on session
    // teardown.
    std::shared_ptr<void> ctx_anchor_;
};

}  // namespace

std::unique_ptr<Connection> accept_tls_connection(int listener_fd,
                                                  std::shared_ptr<TlsServerContext> ctx,
                                                  const TlsAcceptOptions& opts) {
    if (!ctx) {
        throw std::runtime_error("accept_tls_connection: null TlsServerContext");
    }
    TlsSocket sock = TlsSocket::accept(listener_fd, *ctx, opts);
    if (!sock.is_open()) {
        return nullptr;  // nothing left to accept on a non-blocking listener
    }
    return std::make_unique<TlsConnectionImpl>(std::move(sock), std::move(ctx));
}

std::unique_ptr<Connection> accept_tls_connection(int listener_fd,
                                                  std::shared_ptr<TlsServerContext> ctx) {
    return accept_tls_connection(listener_fd, std::move(ctx), TlsAcceptOptions{});
}

std::unique_ptr<Connection> handshake_accepted_tls_connection(int fd,
                                                              std::shared_ptr<TlsServerContext> ctx,
                                                              const TlsAcceptOptions& opts) {
    if (!ctx) {
        NetworkSocket::close(fd);
        throw std::runtime_error("handshake_accepted_tls_connection: null TlsServerContext");
    }
    TlsSocket sock = TlsSocket::handshake_accepted(fd, *ctx, opts);
    if (!sock.is_open()) {
        // Only the build without OpenSSL, which has closed the socket.
        throw std::runtime_error("handshake_accepted_tls_connection: built without OpenSSL");
    }
    return std::make_unique<TlsConnectionImpl>(std::move(sock), std::move(ctx));
}

std::unique_ptr<Connection> connect_tls_connection(const std::string& host,
                                                   std::uint16_t port,
                                                   std::shared_ptr<TlsClientContext> ctx) {
    if (!ctx) {
        throw std::runtime_error("connect_tls_connection: null TlsClientContext");
    }
    TlsSocket sock = TlsSocket::connect(host, port, *ctx);
    return std::make_unique<TlsConnectionImpl>(std::move(sock), std::move(ctx));
}

}  // namespace clink::network
