// wss:// against an in-process TLS server: what the client sends as SNI and
// what it checks the server's certificate against. The server completes the
// TLS handshake and closes, with no WebSocket behind it, so a client that
// accepts the certificate gets as far as its opening handshake and fails
// there; one that refuses it fails in the TLS handshake. The certificate is
// self-signed and trusted through SSL_CERT_FILE, which the client's
// SSL_CTX_set_default_verify_paths reads.

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <poll.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <openssl/ssl.h>
#include <sys/socket.h>

#include "clink/websocket/ws_client.hpp"

namespace {

// A self-signed certificate and key for `subject_alt_names`, made with the
// openssl CLI in a fresh directory; empty when the CLI is unavailable.
std::filesystem::path make_cert(const std::string& subject_alt_names, const std::string& tag) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ws_tls_" + std::to_string(::getpid()) + "_" + tag);
    std::filesystem::create_directories(dir);
    const std::string cmd =
        "openssl req -x509 -newkey rsa:2048 -nodes -keyout " + (dir / "key.pem").string() +
        " -out " + (dir / "cert.pem").string() +
        " -days 1 -subj /CN=unused -addext subjectAltName=" + subject_alt_names +
        " > /dev/null 2>&1";
    return std::system(cmd.c_str()) == 0 ? dir : std::filesystem::path{};
}

// One TLS connection on 127.0.0.1: the handshake, if the client completes it,
// then close. Records the server name the client sent, "" for none.
class OneShotTlsServer {
public:
    explicit OneShotTlsServer(const std::filesystem::path& dir) {
        ctx_ = SSL_CTX_new(TLS_server_method());
        SSL_CTX_use_certificate_file(ctx_, (dir / "cert.pem").c_str(), SSL_FILETYPE_PEM);
        SSL_CTX_use_PrivateKey_file(ctx_, (dir / "key.pem").c_str(), SSL_FILETYPE_PEM);
        // Recorded while the ClientHello is processed: once the handshake is done,
        // OpenSSL keeps the name only if a callback accepted it.
        SSL_CTX_set_tlsext_servername_arg(ctx_, &server_name_);
        SSL_CTX_set_tlsext_servername_callback(
            ctx_, +[](SSL* ssl, int*, void* arg) -> int {
                const char* name = SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name);
                *static_cast<std::string*>(arg) = name != nullptr ? name : "";
                return SSL_TLSEXT_ERR_OK;
            });
        listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        ::bind(listener_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listener_, 1);
        socklen_t len = sizeof(addr);
        ::getsockname(listener_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this] {
            // Bounded, so a client that never connects cannot hang the test.
            pollfd ready{listener_, POLLIN, 0};
            if (::poll(&ready, 1, 5000) != 1) {
                return;
            }
            const int fd = ::accept(listener_, nullptr, nullptr);
            if (fd < 0) {
                return;
            }
            SSL* ssl = SSL_new(ctx_);
            SSL_set_fd(ssl, fd);
            (void)SSL_accept(ssl);
            SSL_free(ssl);
            ::close(fd);
        });
    }
    ~OneShotTlsServer() {
        join();
        ::close(listener_);
        SSL_CTX_free(ctx_);
    }
    OneShotTlsServer(const OneShotTlsServer&) = delete;
    OneShotTlsServer& operator=(const OneShotTlsServer&) = delete;

    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }
    // Wait for the one connection to be done with.
    void join() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    // Read after join(): the server thread writes it.
    [[nodiscard]] const std::string& server_name() const noexcept { return server_name_; }

private:
    SSL_CTX* ctx_{nullptr};
    int listener_{-1};
    std::uint16_t port_{0};
    std::thread thread_;
    std::string server_name_;
};

// The error open() raises against `host` on a one-shot server with the
// certificate in `dir`, trusted as its own CA, and the name the client sent.
struct Attempt {
    std::string error;
    std::string server_name;
};
Attempt open_against(const std::filesystem::path& dir, const std::string& host) {
    ::setenv("SSL_CERT_FILE", (dir / "cert.pem").c_str(), 1);
    Attempt out;
    {
        OneShotTlsServer server(dir);
        clink::websocket::WsClientOptions o;
        o.url = "wss://" + host + ":" + std::to_string(server.port()) + "/";
        o.open_timeout = std::chrono::milliseconds{5000};
        o.name = "ws";
        clink::websocket::WsClient client(o);
        try {
            client.open();
        } catch (const std::exception& e) {
            out.error = e.what();
        }
        client.close();
        server.join();
        out.server_name = server.server_name();
    }
    ::unsetenv("SSL_CERT_FILE");
    return out;
}

bool refused_the_certificate(const std::string& error) {
    return error.find("certificate verify failed") != std::string::npos;
}

}  // namespace

TEST(WsClientTls, AnAddressIsCheckedAgainstTheCertificateIpEntriesAndNotSentAsSni) {
    const auto by_ip = make_cert("IP:127.0.0.1", "ip");
    const auto by_name = make_cert("DNS:localhost", "name");
    if (by_ip.empty() || by_name.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable, can't fixture self-signed cert";
    }
    const auto accepted = open_against(by_ip, "127.0.0.1");
    EXPECT_FALSE(refused_the_certificate(accepted.error)) << accepted.error;
    EXPECT_EQ(accepted.server_name, "") << "an address was sent as SNI";

    const auto refused = open_against(by_name, "127.0.0.1");
    EXPECT_TRUE(refused_the_certificate(refused.error))
        << "a certificate naming no address was accepted for one: " << refused.error;
    std::filesystem::remove_all(by_ip);
    std::filesystem::remove_all(by_name);
}

TEST(WsClientTls, ANameIsCheckedAgainstTheCertificateDnsNamesAndSentAsSni) {
    const auto good = make_cert("DNS:localhost", "good");
    const auto other = make_cert("DNS:other.invalid", "other");
    if (good.empty() || other.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable, can't fixture self-signed cert";
    }
    const auto accepted = open_against(good, "localhost");
    EXPECT_FALSE(refused_the_certificate(accepted.error)) << accepted.error;
    EXPECT_EQ(accepted.server_name, "localhost");

    const auto refused = open_against(other, "localhost");
    EXPECT_TRUE(refused_the_certificate(refused.error)) << refused.error;
    std::filesystem::remove_all(good);
    std::filesystem::remove_all(other);
}
