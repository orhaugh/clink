#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/runtime/network/network_socket.hpp"
#include "clink/runtime/network/tls_socket.hpp"

using namespace clink;
using namespace clink::network;

namespace {

// Generate a self-signed cert + private key in a fresh temp dir using the
// openssl CLI. Avoids hand-rolling X.509 from the OpenSSL C API for what
// is genuinely fixture work. Returns the dir path; cert is at "cert.pem"
// and key at "key.pem" inside it.
std::filesystem::path generate_self_signed_cert() {
    const auto dir =
        std::filesystem::temp_directory_path() / ("clink_tls_test_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir);
    const auto cert = dir / "cert.pem";
    const auto key = dir / "key.pem";
    const std::string cmd = "openssl req -x509 -newkey rsa:2048 -nodes -keyout " + key.string() +
                            " -out " + cert.string() +
                            " -days 1 -subj /CN=localhost"
                            " > /dev/null 2>&1";
    const int rc = std::system(cmd.c_str());
    if (rc != 0) {
        return {};  // openssl CLI unavailable / failed
    }
    return dir;
}

}  // namespace

TEST(TlsSocket, RoundTripsBytesOverTlsWithSelfSignedCert) {
    if (!TlsServerContext::is_real_implementation()) {
        GTEST_SKIP() << "Built without OpenSSL";
    }

    const auto cert_dir = generate_self_signed_cert();
    if (cert_dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable, can't fixture self-signed cert";
    }
    const auto cert = (cert_dir / "cert.pem").string();
    const auto key = (cert_dir / "key.pem").string();

    TlsServerContext server_ctx(cert, key);
    // Trust the self-signed cert as its own CA.
    TlsClientContext client_ctx(cert);

    std::uint16_t port = 0;
    const int listener = NetworkSocket::listen_on(port, "127.0.0.1");
    ASSERT_GE(listener, 0);

    std::thread sender([&] {
        TlsSocket sink = TlsSocket::connect("127.0.0.1", port, client_ctx);
        const std::string payload = "hello over TLS";
        ASSERT_TRUE(
            sink.send_all(reinterpret_cast<const std::byte*>(payload.data()), payload.size()));
        sink.shutdown_write();
    });

    TlsSocket source = TlsSocket::accept(listener, server_ctx);
    NetworkSocket::close(listener);

    std::array<std::byte, 14> buf{};
    ASSERT_TRUE(source.recv_all(buf.data(), buf.size()));
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(buf.data()), buf.size()), "hello over TLS");

    sender.join();
    std::filesystem::remove_all(cert_dir);
}
