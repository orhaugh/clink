// QUAL-12: the control plane's own security refusals.
//
// Every case below was a live downgrade before the validation existed
// (followups item 81). The engine refused credentials-without-transport
// in its Kafka connector and asserted the established transport in its
// Postgres one, while its OWN control plane came up plaintext on ports
// configured for TLS - once with a warning, three times with nothing
// logged at all.
//
// The negative half matters as much as the positive: a deliberate
// plaintext deployment must stay legal, because an operator who asked
// for nothing and got nothing has not been downgraded, and a check that
// refuses that would be a check nobody can run.

#include <chrono>
#include <string>

#include <gtest/gtest.h>

#include "../tools/cli_tls_args.hpp"

using clink::cli::validate_coordinator_tls_args;
using clink::cli::validate_worker_tls_args;

namespace {
constexpr bool kLinked = true;
constexpr bool kNotLinked = false;
}  // namespace

// --- the legitimate configurations -----------------------------------------

TEST(CliTlsArgs, NoTlsFlagsIsLegalOnEitherBuild) {
    EXPECT_FALSE(validate_coordinator_tls_args("", "", "", kLinked).has_value());
    EXPECT_FALSE(validate_coordinator_tls_args("", "", "", kNotLinked).has_value());
    EXPECT_FALSE(validate_worker_tls_args("", "", "", kLinked).has_value());
    EXPECT_FALSE(validate_worker_tls_args("", "", "", kNotLinked).has_value());
}

TEST(CliTlsArgs, ACompleteServerPairIsAccepted) {
    EXPECT_FALSE(validate_coordinator_tls_args("c.pem", "k.pem", "", kLinked).has_value());
}

TEST(CliTlsArgs, ACompleteMutualTlsConfigurationIsAccepted) {
    EXPECT_FALSE(validate_coordinator_tls_args("c.pem", "k.pem", "ca.pem", kLinked).has_value());
    EXPECT_FALSE(validate_worker_tls_args("ca.pem", "cc.pem", "ck.pem", kLinked).has_value());
}

TEST(CliTlsArgs, AWorkerMayVerifyTheServerWithoutOfferingAClientCert) {
    EXPECT_FALSE(validate_worker_tls_args("ca.pem", "", "", kLinked).has_value());
}

// --- the refusals ------------------------------------------------------------

TEST(CliTlsArgs, TlsRequestedOnABuildWithoutTlsIsRefused) {
    // Was: a warning, then a PLAINTEXT listener on the port the operator
    // configured for TLS.
    const auto co = validate_coordinator_tls_args("c.pem", "k.pem", "", kNotLinked);
    ASSERT_TRUE(co.has_value());
    EXPECT_NE(co->find("built without TLS support"), std::string::npos) << *co;
    const auto wk = validate_worker_tls_args("ca.pem", "", "", kNotLinked);
    ASSERT_TRUE(wk.has_value());
    EXPECT_NE(wk->find("built without TLS support"), std::string::npos) << *wk;
}

TEST(CliTlsArgs, AnyLoneTlsFlagOnANonTlsBuildIsRefused) {
    // Each flag alone, so a partial configuration cannot slip through the
    // unsupported-build gate.
    EXPECT_TRUE(validate_coordinator_tls_args("c.pem", "", "", kNotLinked).has_value());
    EXPECT_TRUE(validate_coordinator_tls_args("", "k.pem", "", kNotLinked).has_value());
    EXPECT_TRUE(validate_coordinator_tls_args("", "", "ca.pem", kNotLinked).has_value());
    EXPECT_TRUE(validate_worker_tls_args("", "cc.pem", "", kNotLinked).has_value());
    EXPECT_TRUE(validate_worker_tls_args("", "", "ck.pem", kNotLinked).has_value());
}

TEST(CliTlsArgs, AnIncompleteServerPairIsRefusedAndNamesTheMissingHalf) {
    // Was the SILENT one: the enabling condition is `cert && key` with no
    // else branch, so the listener came up plaintext with nothing logged.
    const auto no_key = validate_coordinator_tls_args("c.pem", "", "", kLinked);
    ASSERT_TRUE(no_key.has_value());
    EXPECT_NE(no_key->find("--tls-key"), std::string::npos) << *no_key;
    const auto no_cert = validate_coordinator_tls_args("", "k.pem", "", kLinked);
    ASSERT_TRUE(no_cert.has_value());
    EXPECT_NE(no_cert->find("--tls-cert"), std::string::npos) << *no_cert;
}

TEST(CliTlsArgs, MutualTlsWithoutAServerCertificateIsRefused) {
    const auto err = validate_coordinator_tls_args("", "", "ca.pem", kLinked);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("mTLS"), std::string::npos) << *err;
}

TEST(CliTlsArgs, AnIncompleteClientPairIsRefusedAndNamesTheMissingHalf) {
    const auto no_key = validate_worker_tls_args("ca.pem", "cc.pem", "", kLinked);
    ASSERT_TRUE(no_key.has_value());
    EXPECT_NE(no_key->find("--tls-client-key"), std::string::npos) << *no_key;
    const auto no_cert = validate_worker_tls_args("ca.pem", "", "ck.pem", kLinked);
    ASSERT_TRUE(no_cert.has_value());
    EXPECT_NE(no_cert->find("--tls-client-cert"), std::string::npos) << *no_cert;
}

TEST(CliTlsArgs, ClientCredentialsWithoutAProtectedTransportAreRefused) {
    // The Kafka connector's rule, applied to the engine's own transport:
    // credentials configured for a connection that is not protected at
    // all. Was: the whole TLS block skipped, plaintext, nothing said.
    const auto err = validate_worker_tls_args("", "cc.pem", "ck.pem", kLinked);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("not protected"), std::string::npos) << *err;
}

TEST(CliTlsArgs, EveryRefusalExplainsWhatWouldHaveHappened) {
    // A refusal an operator cannot act on is a refusal they will work
    // around. Each message must name the consequence it prevented.
    const std::string cases[] = {
        *validate_coordinator_tls_args("c.pem", "k.pem", "", kNotLinked),
        *validate_coordinator_tls_args("c.pem", "", "", kLinked),
        *validate_coordinator_tls_args("", "", "ca.pem", kLinked),
        *validate_worker_tls_args("ca.pem", "cc.pem", "", kLinked),
        *validate_worker_tls_args("", "cc.pem", "ck.pem", kLinked),
    };
    for (const auto& msg : cases) {
        EXPECT_NE(msg.find("Refusing to start"), std::string::npos) << msg;
        const bool names_consequence =
            msg.find("plaintext") != std::string::npos ||
            msg.find("not protected") != std::string::npos ||
            msg.find("without the client certificate") != std::string::npos;
        EXPECT_TRUE(names_consequence) << "refusal does not say what it prevented: " << msg;
    }
}

// --- --handshake-timeout-ms ----------------------------------------------------
//
// Both roles take the flag, parsed by the one function, so a value one refuses
// the other refuses too, bar the range: the coordinator's 0 leaves its side of
// the handshake to the admission deadline, while a worker's connect to the
// coordinator has no other bound, so a worker refuses 0.

using clink::cli::HandshakeRole;
using clink::cli::kMaxHandshakeTimeoutMs;
using clink::cli::parse_handshake_timeout_ms;

TEST(CliTlsArgs, AHandshakeTimeoutInRangeIsTakenByEitherRole) {
    for (const auto role : {HandshakeRole::Coordinator, HandshakeRole::Worker}) {
        std::chrono::milliseconds out{0};
        EXPECT_FALSE(parse_handshake_timeout_ms(role, "5000", out).has_value());
        EXPECT_EQ(out, std::chrono::milliseconds{5000});
        EXPECT_FALSE(parse_handshake_timeout_ms(role, "1", out).has_value());
        EXPECT_EQ(out, std::chrono::milliseconds{1});
        EXPECT_FALSE(parse_handshake_timeout_ms(role, std::to_string(kMaxHandshakeTimeoutMs), out)
                         .has_value());
        EXPECT_EQ(out, std::chrono::milliseconds{kMaxHandshakeTimeoutMs});
    }
}

TEST(CliTlsArgs, ZeroIsTheCoordinatorsAdmissionDeadlineButNoBoundForAWorker) {
    std::chrono::milliseconds out{7};
    EXPECT_FALSE(parse_handshake_timeout_ms(HandshakeRole::Coordinator, "0", out).has_value());
    EXPECT_EQ(out, std::chrono::milliseconds{0});
    out = std::chrono::milliseconds{7};
    const auto err = parse_handshake_timeout_ms(HandshakeRole::Worker, "0", out);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("worker: --handshake-timeout-ms=0 is out of range: give 1 to"),
              std::string::npos)
        << *err;
    EXPECT_EQ(out, std::chrono::milliseconds{7}) << "a refused value must not be applied";
}

TEST(CliTlsArgs, ABadHandshakeTimeoutIsRefusedByEitherRole) {
    // "5s" and "5000ms" were read as 5 and 5000 by the lenient parse the
    // coordinator used, and "" or "abc" threw out of it.
    const std::string not_numbers[] = {"", "abc", "5s", "5000ms", " 5000", "5000 ", "+5000", "1e3"};
    const std::string out_of_range[] = {
        "-1", "86400001", "99999999999999999999", "-99999999999999999999"};
    for (const auto role : {HandshakeRole::Coordinator, HandshakeRole::Worker}) {
        const std::string who = role == HandshakeRole::Coordinator ? "coordinator" : "worker";
        for (const auto& v : not_numbers) {
            std::chrono::milliseconds out{7};
            const auto err = parse_handshake_timeout_ms(role, v, out);
            ASSERT_TRUE(err.has_value()) << who << " took --handshake-timeout-ms=" << v;
            EXPECT_NE(err->find(who + ": --handshake-timeout-ms=" + v +
                                " is not a whole number of milliseconds"),
                      std::string::npos)
                << *err;
            EXPECT_EQ(out, std::chrono::milliseconds{7});
        }
        for (const auto& v : out_of_range) {
            std::chrono::milliseconds out{7};
            const auto err = parse_handshake_timeout_ms(role, v, out);
            ASSERT_TRUE(err.has_value()) << who << " took --handshake-timeout-ms=" << v;
            EXPECT_NE(err->find(" is out of range: "), std::string::npos) << *err;
            EXPECT_NE(err->find(std::to_string(kMaxHandshakeTimeoutMs) + " (one day)"),
                      std::string::npos)
                << *err;
            EXPECT_EQ(out, std::chrono::milliseconds{7});
        }
    }
}
