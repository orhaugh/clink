#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <poll.h>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include "clink/fault/fault_injection.hpp"
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

// A connection shut down from another thread (as the coordinator does to evict
// or stop an admission) is a handshake abandoned. On Linux a ClientHello that
// arrived before the shutdown is still readable, so a step taken anyway spent
// the full key exchange and signature on a connection already dropped.
TEST(TlsSocket, AHandshakeOnASocketAlreadyShutDownTakesNoStep) {
    if (!TlsServerContext::is_real_implementation()) {
        GTEST_SKIP() << "Built without OpenSSL";
    }
    const auto cert_dir = generate_self_signed_cert();
    if (cert_dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable, can't fixture self-signed cert";
    }
    TlsServerContext server_ctx((cert_dir / "cert.pem").string(), (cert_dir / "key.pem").string());
    TlsClientContext client_ctx((cert_dir / "cert.pem").string());
    std::uint16_t port = 0;
    const int listener = NetworkSocket::listen_on(port, "127.0.0.1");
    ASSERT_GE(listener, 0);
    std::thread client([&] {
        try {
            (void)TlsSocket::connect("127.0.0.1", port, client_ctx);
        } catch (const std::exception&) {
            // Expected: the server gives up on it.
        }
    });
    const int fd = NetworkSocket::accept_one(listener);
    NetworkSocket::close(listener);
    ASSERT_GE(fd, 0);
    // The ClientHello is in.
    pollfd readable{fd, POLLIN, 0};
    ASSERT_EQ(::poll(&readable, 1, 10'000), 1);
    // Shut down through a duplicate, as the coordinator does.
    const int dup = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
    ASSERT_GE(dup, 0);
    NetworkSocket::shutdown_read(dup);
    NetworkSocket::shutdown_write(dup);
    NetworkSocket::close(dup);

    namespace fault = clink::fault;
    fault::Registry::instance().reset();
    fault::ScopedFault observe{
        fault::Rule{.point = fault::points::kTlsHandshakeStep, .action = fault::Action::Observe}};
    std::string error;
    try {
        (void)TlsSocket::handshake_accepted(fd, server_ctx, TlsAcceptOptions{});
    } catch (const std::exception& e) {
        error = e.what();
    }
    EXPECT_NE(error.find("handshake abandoned"), std::string::npos)
        << "a handshake on a socket already shut down was attempted (" << error << ")";
    EXPECT_EQ(fault::Registry::instance().hits(fault::points::kTlsHandshakeStep), 0u)
        << "a handshake step was taken on a socket already shut down";
    client.join();
    std::filesystem::remove_all(cert_dir);
}

// A TLS 1.3 client is done with its handshake once it has sent its Finished,
// so it may send its first frame and close before the server has taken the
// step that reads that Finished. Darwin reports the client's FIN as a hangup,
// which the handshake read as its socket having been shut down under it, and
// abandoned a handshake that could complete, with the frame unread.
TEST(TlsSocket, AClientThatClosesRightAfterItsHandshakeStillHasItsDataRead) {
    if (!TlsServerContext::is_real_implementation()) {
        GTEST_SKIP() << "Built without OpenSSL";
    }
    const auto cert_dir = generate_self_signed_cert();
    if (cert_dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable, can't fixture self-signed cert";
    }
    TlsServerContext server_ctx((cert_dir / "cert.pem").string(), (cert_dir / "key.pem").string());
    TlsClientContext client_ctx((cert_dir / "cert.pem").string());
    std::uint16_t port = 0;
    const int listener = NetworkSocket::listen_on(port, "127.0.0.1");
    ASSERT_GE(listener, 0);

    std::atomic<bool> client_done{false};
    std::thread client([&] {
        try {
            TlsSocket conn = TlsSocket::connect("127.0.0.1", port, client_ctx);
            const std::string payload = "first frame";
            (void)conn.send_all(reinterpret_cast<const std::byte*>(payload.data()), payload.size());
        } catch (const std::exception&) {
            // The server's assertions say what went wrong.
        }
        client_done.store(true);  // the socket is closed by now
    });
    const int fd = NetworkSocket::accept_one(listener);
    NetworkSocket::close(listener);
    ASSERT_GE(fd, 0);
    // The ClientHello is in, so the first step answers it and the second is
    // the one that reads the client's Finished.
    pollfd readable{fd, POLLIN, 0};
    ASSERT_EQ(::poll(&readable, 1, 10'000), 1);

    namespace fault = clink::fault;
    fault::Registry::instance().reset();
    fault::ScopedFault hold{fault::Rule{
        .point = fault::points::kTlsHandshakeStep, .ordinal = 2, .action = fault::Action::Block}};
    std::string error;
    std::string received;
    std::thread server([&] {
        try {
            TlsSocket conn = TlsSocket::handshake_accepted(fd, server_ctx, TlsAcceptOptions{});
            std::array<std::byte, 11> buf{};
            if (conn.recv_all(buf.data(), buf.size())) {
                received.assign(reinterpret_cast<const char*>(buf.data()), buf.size());
            }
        } catch (const std::exception& e) {
            error = e.what();
        }
    });
    // Held before the second step until the client has finished, sent its
    // data and closed.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
    while (std::chrono::steady_clock::now() < deadline &&
           (fault::Registry::instance().hits(fault::points::kTlsHandshakeStep) < 2 ||
            !client_done.load())) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    EXPECT_TRUE(client_done.load());
    fault::Registry::instance().release(fault::points::kTlsHandshakeStep);
    server.join();
    client.join();
    EXPECT_EQ(error, "") << "a handshake whose client had sent everything and closed was abandoned";
    EXPECT_EQ(received, "first frame");
    std::filesystem::remove_all(cert_dir);
}

// --- Renegotiation -------------------------------------------------------
//
// A TLS 1.2 renegotiation is a full handshake (key exchange, the certificate
// signature) run inside SSL_read, on whichever thread is reading. On the
// coordinator that is an admission thread reading a first frame, or a
// session's reader, and neither holds one of the handshake slots that bound
// what handshakes may take from the process. OpenSSL 1.1.1, which this builds
// against too, lets a client renegotiate by default, as often as it likes.

namespace {

// A raw OpenSSL context, for the peer the code under test is talking to, held
// to TLS 1.2: TLS 1.3 has no renegotiation.
struct RawTls12Ctx {
    SSL_CTX* ctx;
    explicit RawTls12Ctx(bool server)
        : ctx(SSL_CTX_new(server ? TLS_server_method() : TLS_client_method())) {
        SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
        SSL_CTX_set_max_proto_version(ctx, TLS1_2_VERSION);
    }
    ~RawTls12Ctx() { SSL_CTX_free(ctx); }
    RawTls12Ctx(const RawTls12Ctx&) = delete;
    RawTls12Ctx& operator=(const RawTls12Ctx&) = delete;
};

}  // namespace

TEST(TlsSocket, BothContextsRefuseRenegotiation) {
    if (!TlsServerContext::is_real_implementation()) {
        GTEST_SKIP() << "Built without OpenSSL";
    }
    const auto cert_dir = generate_self_signed_cert();
    if (cert_dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable, can't fixture self-signed cert";
    }
    TlsServerContext server_ctx((cert_dir / "cert.pem").string(), (cert_dir / "key.pem").string());
    TlsClientContext client_ctx((cert_dir / "cert.pem").string());
    // Whatever the OpenSSL version's own default: 1.1.1 allows a client's.
    EXPECT_NE(SSL_CTX_get_options(static_cast<SSL_CTX*>(server_ctx.native_handle())) &
                  SSL_OP_NO_RENEGOTIATION,
              0U)
        << "the server context allows renegotiation";
    EXPECT_NE(SSL_CTX_get_options(static_cast<SSL_CTX*>(client_ctx.native_handle())) &
                  SSL_OP_NO_RENEGOTIATION,
              0U)
        << "the client context allows renegotiation";
    std::filesystem::remove_all(cert_dir);
}

TEST(TlsSocket, AClientInitiatedRenegotiationIsRefused) {
    if (!TlsServerContext::is_real_implementation()) {
        GTEST_SKIP() << "Built without OpenSSL";
    }
    const auto cert_dir = generate_self_signed_cert();
    if (cert_dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable, can't fixture self-signed cert";
    }
    TlsServerContext server_ctx((cert_dir / "cert.pem").string(), (cert_dir / "key.pem").string());
    std::uint16_t port = 0;
    const int listener = NetworkSocket::listen_on(port, "127.0.0.1");
    ASSERT_GE(listener, 0);

    // The server reads, as an admission thread reads a first frame; the
    // renegotiation, if allowed, runs inside that read.
    std::atomic<bool> server_read{false};
    std::thread server([&] {
        TlsSocket conn = TlsSocket::accept(listener, server_ctx);
        std::byte b{};
        server_read.store(conn.recv_all(&b, 1));
    });

    RawTls12Ctx client_ctx(/*server=*/false);
    const int fd = NetworkSocket::connect_to("127.0.0.1", port);
    ASSERT_GE(fd, 0);
    SSL* ssl = SSL_new(client_ctx.ctx);
    SSL_set_fd(ssl, fd);
    ASSERT_EQ(SSL_connect(ssl), 1);
    ASSERT_EQ(SSL_version(ssl), TLS1_2_VERSION);
    // A ClientHello on the established session.
    ASSERT_EQ(SSL_renegotiate(ssl), 1);
    const int rc = SSL_do_handshake(ssl);
    EXPECT_NE(rc, 1) << "the server completed a client-initiated renegotiation";
    EXPECT_NE(SSL_renegotiate_pending(ssl), 0) << "the renegotiation was carried out";

    SSL_free(ssl);
    NetworkSocket::close(fd);
    server.join();
    EXPECT_FALSE(server_read.load());
    NetworkSocket::close(listener);
    std::filesystem::remove_all(cert_dir);
}

TEST(TlsSocket, AServerInitiatedRenegotiationIsRefusedByTheClient) {
    if (!TlsServerContext::is_real_implementation()) {
        GTEST_SKIP() << "Built without OpenSSL";
    }
    const auto cert_dir = generate_self_signed_cert();
    if (cert_dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable, can't fixture self-signed cert";
    }
    RawTls12Ctx server_ctx(/*server=*/true);
    ASSERT_EQ(SSL_CTX_use_certificate_file(
                  server_ctx.ctx, (cert_dir / "cert.pem").string().c_str(), SSL_FILETYPE_PEM),
              1);
    ASSERT_EQ(SSL_CTX_use_PrivateKey_file(
                  server_ctx.ctx, (cert_dir / "key.pem").string().c_str(), SSL_FILETYPE_PEM),
              1);
    TlsClientContext client_ctx((cert_dir / "cert.pem").string());
    std::uint16_t port = 0;
    const int listener = NetworkSocket::listen_on(port, "127.0.0.1");
    ASSERT_GE(listener, 0);

    // The client sends a byte once it has read one, so the server's read
    // after its HelloRequest is where a renegotiation the client agreed to
    // would complete.
    std::thread client([&] {
        TlsSocket conn = TlsSocket::connect("127.0.0.1", port, client_ctx);
        std::byte b{};
        if (conn.recv_all(&b, 1)) {
            (void)conn.send_all(&b, 1);
        }
    });

    const int fd = NetworkSocket::accept_one(listener);
    ASSERT_GE(fd, 0);
    SSL* ssl = SSL_new(server_ctx.ctx);
    SSL_set_fd(ssl, fd);
    ASSERT_EQ(SSL_accept(ssl), 1);
    ASSERT_EQ(SSL_version(ssl), TLS1_2_VERSION);
    // A HelloRequest, then the byte the client waits for.
    ASSERT_EQ(SSL_renegotiate(ssl), 1);
    ASSERT_EQ(SSL_do_handshake(ssl), 1);
    const std::byte x{0x78};
    ASSERT_EQ(SSL_write(ssl, &x, 1), 1);
    std::byte back{};
    const int n = SSL_read(ssl, &back, 1);
    EXPECT_FALSE(n == 1 && SSL_renegotiate_pending(ssl) == 0)
        << "the client carried out a renegotiation the server asked for";

    SSL_free(ssl);
    NetworkSocket::close(fd);
    client.join();
    NetworkSocket::close(listener);
    std::filesystem::remove_all(cert_dir);
}

// The coordinator bounds its replies to a peer it has not admitted with
// set_send_timeout. Without it, a TLS peer that never read held the thread
// replying in SSL_write for as long as it kept the connection open.
TEST(TlsSocket, AWriteToAPeerThatNeverReadsFailsWithinTheSendTimeout) {
    if (!TlsServerContext::is_real_implementation()) {
        GTEST_SKIP() << "Built without OpenSSL";
    }
    const auto cert_dir = generate_self_signed_cert();
    if (cert_dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable, can't fixture self-signed cert";
    }
    TlsServerContext server_ctx((cert_dir / "cert.pem").string(), (cert_dir / "key.pem").string());
    TlsClientContext client_ctx((cert_dir / "cert.pem").string());
    std::uint16_t port = 0;
    const int listener = NetworkSocket::listen_on(port, "127.0.0.1");
    ASSERT_GE(listener, 0);

    // Connects, then reads nothing until the test is done, or until a bound
    // well past the timeout, so a broken build fails rather than hangs.
    std::mutex mu;
    std::condition_variable cv;
    bool done = false;
    std::thread client([&] {
        TlsSocket conn = TlsSocket::connect("127.0.0.1", port, client_ctx);
        std::unique_lock lock(mu);
        cv.wait_for(lock, std::chrono::seconds{20}, [&] { return done; });
    });
    TlsSocket conn = TlsSocket::accept(listener, server_ctx);
    NetworkSocket::close(listener);
    ASSERT_TRUE(conn.set_send_timeout(std::chrono::milliseconds{300}));
    // Far more than the two sockets' buffers hold.
    const std::vector<std::byte> big(std::size_t{64} * 1024 * 1024, std::byte{0x5a});
    const auto started = std::chrono::steady_clock::now();
    EXPECT_FALSE(conn.send_all(big.data(), big.size()));
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds{10})
        << "a write to a peer that never reads outlasted its send timeout";
    {
        std::lock_guard lock(mu);
        done = true;
    }
    cv.notify_all();
    client.join();
    std::filesystem::remove_all(cert_dir);
}
