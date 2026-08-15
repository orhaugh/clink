#pragma once

// SCRAM-SHA-256 client (RFC 5802 / RFC 7677) for the prepared-transaction
// resume. Pure message construction and verification - no sockets - so the
// whole exchange is pinned against RFC 7677's published test vector rather
// than against this implementation's own output.
//
// Scope honesty:
//   * SHA-256 only (what Kafka brokers and the pinned Redpanda enable).
//   * No channel binding (GS2 header "n,,", c=biws) - matching every Kafka
//     client; the transport-security pairing is the TLS dialer.
//   * Username escaping per RFC 5802 (= -> =3D, , -> =2C); full SASLprep is
//     NOT applied, so non-ASCII usernames are refused by the caller's
//     broker rather than silently mis-encoded here.
//   * The SERVER is verified too: client_final computes the expected
//     ServerSignature, and a server-final whose v= does not match - or a
//     server nonce that does not extend the client's - must be treated as
//     a refusal by the caller. A server that cannot prove knowledge of the
//     credentials is indistinguishable from an attacker.
//
// Only built when the tree carries clink::tls (OpenSSL provides the HMAC /
// PBKDF2 / SHA-256 primitives); the resume refuses the mechanism loudly in
// builds without it.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace clink::kafka::scram {

// client-first-message. `bare` is what AuthMessage uses; `full` (with the
// "n,," GS2 header) is what goes over the wire.
struct ClientFirst {
    std::string bare;
    std::string full;
    std::string nonce;  // the client nonce, verbatim
};
[[nodiscard]] ClientFirst client_first(const std::string& username,
                                       const std::string& client_nonce);

// Parsed server-first-message: r= (combined nonce), s= (decoded salt),
// i= (iteration count), plus the raw message for AuthMessage.
struct ServerFirst {
    std::string nonce;
    std::vector<std::byte> salt;
    std::uint32_t iterations{0};
    std::string raw;
};
[[nodiscard]] std::optional<ServerFirst> parse_server_first(const std::string& message);

// client-final-message plus the ServerSignature the server-final MUST
// carry. nullopt when the server nonce does not extend the client's (a
// reflected or foreign nonce is an attack shape, not a parse quirk) or the
// crypto primitives fail.
struct ClientFinal {
    std::string message;
    std::vector<std::byte> expected_server_signature;
};
[[nodiscard]] std::optional<ClientFinal> client_final(const std::string& password,
                                                      const ClientFirst& first,
                                                      const ServerFirst& server);

// The v= payload of server-final-message, base64-decoded. nullopt on a
// malformed message or an e= error reply.
[[nodiscard]] std::optional<std::vector<std::byte>> parse_server_final_signature(
    const std::string& message);

// A fresh random client nonce (base64 of 18 random bytes, so it is
// printable and attribute-safe).
[[nodiscard]] std::string random_nonce();

// Exposed for the RFC-vector tests.
[[nodiscard]] std::string base64_encode(const std::vector<std::byte>& in);
[[nodiscard]] std::optional<std::vector<std::byte>> base64_decode(const std::string& in);

}  // namespace clink::kafka::scram
