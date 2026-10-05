#pragma once

// Control-plane TLS argument validation: the engine's own transport held
// to the standard its connectors already meet.
//
// The Kafka connector refuses credentials without a protecting protocol,
// and postgres_tls.hpp states the doctrine plainly - "asked for TLS and
// did not get it -> throw" - because the one thing a security
// configuration must never do is silently give you less than you asked
// for. The control plane did not follow either rule. Every path below
// was a live downgrade before this existed (followups item 81):
//
//   * a TLS flag on a binary built WITHOUT clink_tls: warned, then
//     served PLAINTEXT on the port the operator believed was encrypted;
//   * --tls-cert without --tls-key on a TLS build: the enabling
//     condition is `cert && key`, and there was NO else branch - the
//     listener came up plaintext with nothing logged at all;
//   * --tls-client-ca alone: mTLS asked for, no server certificate, so
//     the same silent plaintext listener;
//   * a worker's --tls-client-cert/--tls-client-key without --tls-ca:
//     client credentials configured for a connection that is not
//     protected at all - the exact shape the Kafka connector refuses.
//
// This is pure: it inspects the argument set and returns the refusal
// message, or nullopt when the configuration is coherent. The caller
// prints and exits. Nothing here weakens a deliberate plaintext
// deployment - passing no TLS flags at all remains entirely legitimate,
// because that operator asked for nothing and gets nothing, which is not
// a downgrade.

#include <charconv>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <system_error>

namespace clink::cli {

// The longest --handshake-timeout-ms accepted, for either role: one day. A
// TLS handshake takes milliseconds; anything near this is a mistake, not a
// deadline.
inline constexpr long long kMaxHandshakeTimeoutMs = 24LL * 60 * 60 * 1000;

// The worker's --handshake-timeout-ms when none is given: the TLS client's own
// default (network::kDefaultTlsConnectTimeout), which clink_node pins it to.
inline constexpr long long kDefaultWorkerHandshakeTimeoutMs = 5000;

enum class HandshakeRole : std::uint8_t { Coordinator, Worker };

// --handshake-timeout-ms, for either role: a whole number of milliseconds and
// nothing else, so neither "-1" nor "5s" (read as 5 ms by a lenient parse)
// slips through. The coordinator takes 0 to one day, 0 leaving its side of
// the handshake bounded by the admission deadline alone; a worker takes 1 to
// one day, since nothing else bounds its connect to the coordinator. Returns
// the refusal message, or nullopt with `out` set.
inline std::optional<std::string> parse_handshake_timeout_ms(HandshakeRole role,
                                                             const std::string& text,
                                                             std::chrono::milliseconds& out) {
    const bool coordinator = role == HandshakeRole::Coordinator;
    const std::string prefix =
        std::string{coordinator ? "coordinator" : "worker"} + ": --handshake-timeout-ms=" + text;
    const long long min = coordinator ? 0 : 1;
    const std::string range =
        "give " + std::to_string(min) + " to " + std::to_string(kMaxHandshakeTimeoutMs) +
        " (one day)" +
        (coordinator ? "; 0 leaves the handshake bounded only by the admission deadline"
                     : "; nothing else bounds a worker's connect to the coordinator");
    long long ms = 0;
    const char* last = text.data() + text.size();
    const auto [end, ec] = std::from_chars(text.data(), last, ms);
    if (end != last || text.empty() ||
        (ec != std::errc{} && ec != std::errc::result_out_of_range)) {
        return prefix + " is not a whole number of milliseconds: " + range;
    }
    if (ec == std::errc::result_out_of_range || ms < min || ms > kMaxHandshakeTimeoutMs) {
        return prefix + " is out of range: " + range;
    }
    out = std::chrono::milliseconds{ms};
    return std::nullopt;
}

// `linked` reports whether the binary actually carries TLS support.
inline std::optional<std::string> validate_coordinator_tls_args(const std::string& cert,
                                                                const std::string& key,
                                                                const std::string& client_ca,
                                                                bool linked) {
    const bool any = !cert.empty() || !key.empty() || !client_ca.empty();
    if (!any) {
        return std::nullopt;  // no TLS asked for; nothing to downgrade
    }
    if (!linked) {
        return "TLS was requested (--tls-cert/--tls-key/--tls-client-ca) but this binary was "
               "built without TLS support. Refusing to start: serving plaintext on a port "
               "configured for TLS is the downgrade this check exists to prevent. Rebuild "
               "with the TLS impl enabled, or remove the --tls-* flags to run plaintext "
               "deliberately.";
    }
    if (cert.empty() != key.empty()) {
        return std::string{"--tls-cert and --tls-key must be given together (missing "} +
               (cert.empty() ? "--tls-cert" : "--tls-key") +
               "). Refusing to start: an incomplete pair would have served plaintext.";
    }
    if (!client_ca.empty() && cert.empty()) {
        return "--tls-client-ca requests mTLS but no server certificate was given "
               "(--tls-cert/--tls-key). Refusing to start: the listener would have been "
               "plaintext, verifying nothing.";
    }
    return std::nullopt;
}

inline std::optional<std::string> validate_worker_tls_args(const std::string& ca,
                                                           const std::string& client_cert,
                                                           const std::string& client_key,
                                                           bool linked) {
    const bool any = !ca.empty() || !client_cert.empty() || !client_key.empty();
    if (!any) {
        return std::nullopt;
    }
    if (!linked) {
        return "TLS was requested (--tls-ca/--tls-client-cert/--tls-client-key) but this "
               "binary was built without TLS support. Refusing to start: connecting in "
               "plaintext to a coordinator configured for TLS is the downgrade this check "
               "exists to prevent.";
    }
    if (client_cert.empty() != client_key.empty()) {
        return std::string{
                   "--tls-client-cert and --tls-client-key must be given together "
                   "(missing "} +
               (client_cert.empty() ? "--tls-client-cert" : "--tls-client-key") +
               "). Refusing to start: an incomplete pair would have connected without the "
               "client certificate mTLS requires.";
    }
    if (ca.empty() && !client_cert.empty()) {
        return "--tls-client-cert/--tls-client-key were given without --tls-ca. Refusing to "
               "start: that configures client credentials for a connection that is not "
               "protected at all, which is the shape the Kafka connector already refuses.";
    }
    return std::nullopt;
}

}  // namespace clink::cli
