// The Pulsar client's broker-certificate checks, against a local TLS listener
// rather than a broker. libpulsar finishes the handshake before it sends its
// CONNECT, so whether the client accepted the certificate shows on the
// listener: a refused certificate never completes a handshake, an accepted
// one does, and the open then fails on the missing broker either way.

#include <filesystem>
#include <stdexcept>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "clink/pulsar/pulsar_sink.hpp"

#include "test_helpers/tls_handshake_server.hpp"

using clink::pulsar::PulsarSink;
using clink::test_helpers::make_self_signed_cert;
using clink::test_helpers::TlsHandshakeServer;

namespace {

// Open a sink over TLS to `server` as `host`, trusting the certificate in
// `trusted`, and return how many handshakes the listener saw complete. The
// open fails, as there is no broker; a short operation timeout bounds it.
int handshakes_on_open(const TlsHandshakeServer& server,
                       const std::filesystem::path& trusted,
                       const std::string& host,
                       bool verify_hostname) {
    PulsarSink::Options o;
    o.conn.service_url = "pulsar+ssl://" + host + ":" + std::to_string(server.port());
    o.conn.operation_timeout_s = 3;
    o.conn.tls_ca = (trusted / "cert.pem").string();
    o.conn.tls_verify_hostname = verify_hostname;
    o.topic = "clink-tls";
    PulsarSink sink(std::move(o));
    EXPECT_THROW(sink.open(), std::runtime_error);
    sink.close();
    return server.handshakes();
}

}  // namespace

TEST(PulsarTls, ABrokerCertificateNamingAnotherHostIsRefusedByDefault) {
    const auto dir = make_self_signed_cert("DNS:broker.invalid", "broker.invalid", "pulsar_other");
    if (dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    TlsHandshakeServer server(dir);
    ASSERT_TRUE(server.listening());
    EXPECT_EQ(handshakes_on_open(server, dir, "127.0.0.1", /*verify_hostname=*/true), 0);
    EXPECT_GT(server.connections(), 0) << "the client never reached the listener";
}

TEST(PulsarTls, ARefusedCertificateSaysHowToTellItFromADeadBroker) {
    const auto dir = make_self_signed_cert("DNS:broker.invalid", "broker.invalid", "pulsar_msg");
    if (dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    TlsHandshakeServer server(dir);
    ASSERT_TRUE(server.listening());
    PulsarSink::Options o;
    o.conn.service_url = "pulsar+ssl://127.0.0.1:" + std::to_string(server.port());
    o.conn.operation_timeout_s = 3;
    o.conn.tls_ca = (dir / "cert.pem").string();
    o.topic = "clink-tls";
    PulsarSink sink(std::move(o));
    try {
        sink.open();
        FAIL() << "open succeeded with no broker";
    } catch (const std::runtime_error& e) {
        const std::string error = e.what();
        EXPECT_NE(error.find("certificate verify failed"), std::string::npos) << error;
        EXPECT_NE(error.find("tls_verify_hostname=false"), std::string::npos) << error;
    }
    sink.close();
}

TEST(PulsarTls, ABrokerCertificateNamingTheHostIsAccepted) {
    const auto dir = make_self_signed_cert("IP:127.0.0.1", "broker", "pulsar_ip");
    if (dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    TlsHandshakeServer server(dir);
    ASSERT_TRUE(server.listening());
    EXPECT_GT(handshakes_on_open(server, dir, "127.0.0.1", /*verify_hostname=*/true), 0);
}

TEST(PulsarTls, TheHostnameOptOutAcceptsAnotherHostOnATrustedChain) {
    const auto dir = make_self_signed_cert("DNS:broker.invalid", "broker.invalid", "pulsar_out");
    if (dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    TlsHandshakeServer server(dir);
    ASSERT_TRUE(server.listening());
    EXPECT_GT(handshakes_on_open(server, dir, "127.0.0.1", /*verify_hostname=*/false), 0);
}

TEST(PulsarTls, TheHostnameOptOutStillChecksTheChain) {
    const auto served = make_self_signed_cert("DNS:broker.invalid", "broker.invalid", "pulsar_s");
    const auto other = make_self_signed_cert("DNS:broker.invalid", "broker.invalid", "pulsar_o");
    if (served.empty() || other.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    TlsHandshakeServer server(served);
    ASSERT_TRUE(server.listening());
    EXPECT_EQ(handshakes_on_open(server, other, "127.0.0.1", /*verify_hostname=*/false), 0);
    EXPECT_GT(server.connections(), 0) << "the client never reached the listener";
}

TEST(PulsarTls, AMissingTrustFileIsNamed) {
    PulsarSink::Options o;
    o.conn.service_url = "pulsar+ssl://127.0.0.1:1";
    o.conn.operation_timeout_s = 3;
    o.conn.tls_ca = "/nonexistent/clink-ca.pem";
    o.topic = "clink-tls";
    PulsarSink sink(std::move(o));
    try {
        sink.open();
        FAIL() << "open succeeded with a missing trust file";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("tls_ca /nonexistent/clink-ca.pem"), std::string::npos)
            << e.what();
    }
}
