#pragma once

// A TLS listener on 127.0.0.1 for testing a client's certificate checks
// without the client's real server. It presents one certificate and counts
// the handshakes clients complete, and says nothing after them. A client that
// refuses the certificate aborts the handshake, so it never counts; one that
// accepts it counts and then sees the connection close. The clients under
// test all finish the handshake before they speak their own protocol.
//
// Needs OpenSSL linked into the test and the openssl CLI on PATH for the
// certificates (make_self_signed_cert returns empty without it).

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <poll.h>
#include <string>
#include <thread>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <openssl/ssl.h>
#include <sys/socket.h>
#include <sys/time.h>

namespace clink::test_helpers {

// A self-signed certificate (cert.pem) and its key (key.pem) with these
// subjectAltNames, none when empty, and subject CN, in a fresh directory
// keyed by pid and `tag`. Empty when the openssl CLI is unavailable.
inline std::filesystem::path make_self_signed_cert(const std::string& subject_alt_names,
                                                   const std::string& common_name,
                                                   const std::string& tag) {
    auto dir = std::filesystem::temp_directory_path() /
               ("clink_tls_fixture_" + std::to_string(::getpid()) + "_" + tag);
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    std::filesystem::create_directories(dir, ec);
    if (ec) {
        return {};
    }
    const std::string cmd =
        "openssl req -x509 -newkey rsa:2048 -nodes -keyout " + (dir / "key.pem").string() +
        " -out " + (dir / "cert.pem").string() + " -days 1 -subj /CN=" + common_name +
        (subject_alt_names.empty() ? "" : " -addext subjectAltName=" + subject_alt_names) +
        " > /dev/null 2>&1";
    if (std::system(cmd.c_str()) != 0) {
        return {};
    }
    return dir;
}

class TlsHandshakeServer {
public:
    // Serve the certificate and key in `cert_dir` (as make_self_signed_cert
    // writes them) on an ephemeral port until destroyed.
    explicit TlsHandshakeServer(const std::filesystem::path& cert_dir) {
        // A client that gives up mid-handshake must not kill the test with
        // SIGPIPE when the server writes to it.
        std::signal(SIGPIPE, SIG_IGN);
        ctx_ = SSL_CTX_new(TLS_server_method());
        if (ctx_ == nullptr ||
            SSL_CTX_use_certificate_chain_file(ctx_, (cert_dir / "cert.pem").c_str()) != 1 ||
            SSL_CTX_use_PrivateKey_file(ctx_, (cert_dir / "key.pem").c_str(), SSL_FILETYPE_PEM) !=
                1) {
            return;
        }
        // No TLS 1.3 session tickets: the server writes them after the
        // client's Finished, and a client that closes at once would turn a
        // completed handshake into a failed SSL_accept.
        SSL_CTX_set_num_tickets(ctx_, 0);
        listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listener_ < 0) {
            return;
        }
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = 0;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        socklen_t len = sizeof(addr);
        if (::bind(listener_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
            ::listen(listener_, 16) != 0 ||
            ::getsockname(listener_, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
            return;
        }
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this] { serve(); });
    }
    ~TlsHandshakeServer() {
        stop_.store(true);
        if (thread_.joinable()) {
            thread_.join();
        }
        if (listener_ >= 0) {
            ::close(listener_);
        }
        SSL_CTX_free(ctx_);
    }
    TlsHandshakeServer(const TlsHandshakeServer&) = delete;
    TlsHandshakeServer& operator=(const TlsHandshakeServer&) = delete;
    TlsHandshakeServer(TlsHandshakeServer&&) = delete;
    TlsHandshakeServer& operator=(TlsHandshakeServer&&) = delete;

    [[nodiscard]] bool listening() const { return port_ != 0; }
    [[nodiscard]] std::uint16_t port() const { return port_; }
    // Handshakes a client completed, so accepted the certificate.
    [[nodiscard]] int handshakes() const { return handshakes_.load(); }
    // Connections that reached the listener at all.
    [[nodiscard]] int connections() const { return connections_.load(); }

private:
    void serve() {
        while (!stop_.load()) {
            pollfd p{.fd = listener_, .events = POLLIN, .revents = 0};
            if (::poll(&p, 1, 50) <= 0) {
                continue;
            }
            const int fd = ::accept(listener_, nullptr, nullptr);
            if (fd < 0) {
                continue;
            }
            connections_.fetch_add(1);
            // A client that stalls mid-handshake holds the server for no
            // longer than this.
            const timeval limit{.tv_sec = 5, .tv_usec = 0};
            ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &limit, sizeof(limit));
            ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &limit, sizeof(limit));
            SSL* ssl = SSL_new(ctx_);
            if (ssl != nullptr) {
                SSL_set_fd(ssl, fd);
                if (SSL_accept(ssl) == 1) {
                    handshakes_.fetch_add(1);
                }
                SSL_free(ssl);
            }
            ::close(fd);
        }
    }

    SSL_CTX* ctx_{nullptr};
    int listener_{-1};
    std::uint16_t port_{0};
    std::atomic<bool> stop_{false};
    std::atomic<int> handshakes_{0};
    std::atomic<int> connections_{0};
    std::thread thread_;
};

}  // namespace clink::test_helpers
