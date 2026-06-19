#include "clink/runtime/network/tls_connection.hpp"

#include <stdexcept>
#include <utility>

namespace clink::network {

namespace {

class TlsConnectionImpl final : public Connection {
public:
    TlsConnectionImpl(TlsSocket sock, std::shared_ptr<void> ctx_anchor)
        : sock_(std::move(sock)), ctx_anchor_(std::move(ctx_anchor)) {}

    ~TlsConnectionImpl() override { close(); }

    TlsConnectionImpl(const TlsConnectionImpl&) = delete;
    TlsConnectionImpl& operator=(const TlsConnectionImpl&) = delete;

    bool send_all(const std::byte* buf, std::size_t len) override {
        return sock_.send_all(buf, len);
    }

    bool recv_all(std::byte* buf, std::size_t len) override { return sock_.recv_all(buf, len); }

    void shutdown_write() override { sock_.shutdown_write(); }

    void shutdown_read() override {
        // TLS has no spec-defined unidirectional half-close. The watchdog
        // uses shutdown_read to wake a blocked recv() so the reader thread
        // can exit; the simplest equivalent on TLS is to tear down the
        // session, which causes pending recv() calls to return.
        sock_.close();
    }

    void close() override { sock_.close(); }

    bool is_open() const noexcept override { return sock_.is_open(); }

private:
    TlsSocket sock_;
    // Holds the SSL_CTX shared_ptr alive for as long as this connection
    // exists. Without it, a TlsServerContext / TlsClientContext destroyed
    // before its accepted/connected sockets would free SSL_CTX while
    // SSL_free was still expected to call back into it on session
    // teardown.
    std::shared_ptr<void> ctx_anchor_;
};

}  // namespace

std::unique_ptr<Connection> accept_tls_connection(int listener_fd,
                                                  std::shared_ptr<TlsServerContext> ctx) {
    if (!ctx) {
        throw std::runtime_error("accept_tls_connection: null TlsServerContext");
    }
    TlsSocket sock = TlsSocket::accept(listener_fd, *ctx);
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
