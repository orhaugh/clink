// The Redis client's server-certificate checks, against a local TLS listener
// rather than a Redis server. The handshake is over before the client sends a
// command, and a Connection with no password and db 0 sends none, so these
// need no server and run in every TLS-enabled build.

#ifdef CLINK_REDIS_TLS

#include <chrono>
#include <exception>
#include <filesystem>
#include <functional>
#include <string>

#include <gtest/gtest.h>

#include "clink/redis/redis_client.hpp"

#include "test_helpers/tls_handshake_server.hpp"

using clink::redis::Connection;
using clink::redis::ConnectOptions;
using clink::test_helpers::make_self_signed_cert;
using clink::test_helpers::TlsHandshakeServer;

namespace {

// Connect by TLS as `host` to a listener presenting the certificate in
// `served`, trusting the certificate in `trusted`, with `adjust` applied to
// the options. Returns the error the connect threw, or empty when it
// connected.
std::string connect_error(const std::filesystem::path& served,
                          const std::filesystem::path& trusted,
                          const std::string& host,
                          const std::function<void(ConnectOptions&)>& adjust = {}) {
    TlsHandshakeServer server(served);
    if (!server.listening()) {
        return "the test could not listen";
    }
    ConnectOptions o;
    o.host = host;
    o.port = server.port();
    o.connect_timeout = std::chrono::milliseconds{5000};
    o.tls = true;
    o.tls_ca = (trusted / "cert.pem").string();
    if (adjust) {
        adjust(o);
    }
    try {
        Connection c(o);
    } catch (const std::exception& e) {
        return e.what();
    }
    return {};
}

bool mentions(const std::string& text, const std::string& what) {
    return text.find(what) != std::string::npos;
}

}  // namespace

TEST(RedisTls, ACertificateNamingTheAddressConnects) {
    const auto dir = make_self_signed_cert("IP:127.0.0.1", "redis", "ip_ok");
    if (dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    EXPECT_EQ(connect_error(dir, dir, "127.0.0.1"), "");
}

TEST(RedisTls, ACertificateNamingAnotherHostIsRefusedAndTheErrorSaysWhy) {
    const auto dir = make_self_signed_cert("DNS:redis.invalid", "redis.invalid", "other");
    if (dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    const std::string error = connect_error(dir, dir, "127.0.0.1");
    EXPECT_TRUE(mentions(error, "does not match the host 127.0.0.1")) << error;
    EXPECT_TRUE(mentions(error, "DNS:redis.invalid")) << error;
    EXPECT_TRUE(mentions(error, "tls_verify_hostname=false")) << error;
}

TEST(RedisTls, TlsSniSetsTheNameTheCertificateMustCarry) {
    const auto dir = make_self_signed_cert("DNS:redis.invalid", "redis.invalid", "sni");
    if (dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    EXPECT_EQ(connect_error(
                  dir, dir, "127.0.0.1", [](ConnectOptions& o) { o.tls_sni = "redis.invalid"; }),
              "");
    const std::string error = connect_error(
        dir, dir, "127.0.0.1", [](ConnectOptions& o) { o.tls_sni = "cache.invalid"; });
    EXPECT_TRUE(mentions(error, "does not match the host cache.invalid")) << error;
}

TEST(RedisTls, AWildcardCoversOneWholeLabel) {
    const auto dir = make_self_signed_cert("DNS:*.redis.invalid", "redis.invalid", "wildcard");
    if (dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    EXPECT_EQ(connect_error(
                  dir, dir, "127.0.0.1", [](ConnectOptions& o) { o.tls_sni = "a.redis.invalid"; }),
              "");
    const std::string error = connect_error(
        dir, dir, "127.0.0.1", [](ConnectOptions& o) { o.tls_sni = "a.b.redis.invalid"; });
    EXPECT_TRUE(mentions(error, "does not match the host a.b.redis.invalid")) << error;
}

TEST(RedisTls, TheHostnameOptOutStillChecksTheChain) {
    const auto served = make_self_signed_cert("DNS:redis.invalid", "redis.invalid", "served");
    const auto other = make_self_signed_cert("DNS:redis.invalid", "redis.invalid", "other_ca");
    if (served.empty() || other.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    const auto opt_out = [](ConnectOptions& o) { o.tls_verify_hostname = false; };
    EXPECT_EQ(connect_error(served, served, "127.0.0.1", opt_out), "");
    const std::string error = connect_error(served, other, "127.0.0.1", opt_out);
    EXPECT_TRUE(mentions(error, "TLS handshake failed")) << error;
    EXPECT_FALSE(mentions(error, "does not match the host")) << error;
}

TEST(RedisTls, TlsVerifyFalseChecksNeitherChainNorName) {
    const auto served = make_self_signed_cert("DNS:redis.invalid", "redis.invalid", "nv_served");
    const auto other = make_self_signed_cert("DNS:redis.invalid", "redis.invalid", "nv_other");
    if (served.empty() || other.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    EXPECT_EQ(
        connect_error(served, other, "127.0.0.1", [](ConnectOptions& o) { o.tls_verify = false; }),
        "");
}

TEST(RedisTls, AClientCertificateNeedsItsKey) {
    const auto dir = make_self_signed_cert("IP:127.0.0.1", "redis", "cert_no_key");
    if (dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    const std::string error = connect_error(dir, dir, "127.0.0.1", [&](ConnectOptions& o) {
        o.tls_cert = (dir / "cert.pem").string();
    });
    EXPECT_TRUE(mentions(error, "TLS context init failed")) << error;
}

#endif  // CLINK_REDIS_TLS
