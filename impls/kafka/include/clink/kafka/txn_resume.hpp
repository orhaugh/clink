#pragma once

// Prepared-transaction resume over the Kafka wire protocol.
//
// The one window the 2PC Kafka sink could not close: a checkpoint COMPLETES,
// the worker dies before the broker commit executes, and the transaction is
// orphaned - librdkafka exposes no way for a successor to commit it, because
// init_transactions() with the same transactional.id epoch-bumps and ABORTS
// it. Neither does the Java client through supported APIs; Flink resumes by
// reflecting into the Java producer's private producerId/epoch fields, and
// KIP-939 exists precisely to sanction this operation at the protocol level.
//
// The protocol itself needs only three facts to finalise the orphan -
// transactional.id, producer_id, producer_epoch - and one request:
// EndTxn(commit) sent to the transaction coordinator BEFORE any successor
// calls init_transactions. The producer identity is observable (librdkafka
// reports it in its statistics callback), so the sink persists it in the
// staged handle at every barrier and this module speaks the three requests
// recovery needs, hand-encoded like the rest of this tree's protocols:
//
//   ApiVersions v0        - does this broker still speak the versions below?
//   FindCoordinator v1/v2 - which broker coordinates this transactional.id?
//   EndTxn v1             - commit the prepared transaction.
//
// Honesty rules, learned from F-series findings on this exact path:
//   * ONLY error_code 0 is Committed. Every other outcome - fenced epoch,
//     timed-out transaction, unsupported version, refused auth, transport
//     failure - reports itself and the caller falls back to the
//     commit-confirmed contract (bounded replay). A false "committed" here
//     recreates the false-confirm defect item 52 fixed.
//   * Non-flexible protocol versions are spoken except where none exists:
//     DescribeTransactions v0 (KIP-664, flexible-only) is the one compact
//     exchange, used to disambiguate a fenced EndTxn - the staged epoch
//     comes from librdkafka's periodic statistics while the broker bumps
//     the epoch on every commit, so a mid-run handle is chronically one
//     commit stale and a COMMITTED transaction reads as fenced. A broker
//     that has dropped the spoken versions gets Unsupported, not a guess.
//   * Transport is the engine's own Connection seam, so a fake broker tests
//     the full path byte-for-byte and TLS can slot in via the factory.
//
// SASL: PLAIN and SCRAM-SHA-256 are spoken (SaslHandshake v1 +
// SaslAuthenticate v1 rounds on each connection, before anything else)
// when the caller supplies credentials. SCRAM (clink/kafka/scram.hpp,
// pinned against RFC 7677's test vector) verifies the SERVER too: a
// server-final whose signature does not match, a nonce that does not
// extend the client's, or an iteration count below the RFC floor is a
// loud Refused. Credentials come from the RESOLVING process's environment
// at resolution time - never from the staged handle, because durable
// checkpoint state must not carry secrets. Any other mechanism, and any
// authentication refusal, is a loud Refused and recovery falls back to
// the bounded contract. PLAIN over a plaintext connection sends the
// password in the clear; pair it with the TLS ConnectFn exactly as you
// would configure the sink with sasl_ssl.

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "clink/runtime/network/connection.hpp"

namespace clink::kafka {

// The facts that identify a prepared transaction to its coordinator.
struct TxnIdentity {
    std::string transactional_id;
    std::int64_t producer_id{-1};
    std::int16_t producer_epoch{-1};

    [[nodiscard]] bool complete() const noexcept {
        return !transactional_id.empty() && producer_id >= 0 && producer_epoch >= 0;
    }
};

struct ResumeOutcome {
    enum class Status {
        Committed,       // EndTxn returned error_code 0: the records are published
        Refused,         // the broker answered and said no (fenced, timed out, auth)
        Unsupported,     // broker no longer speaks the versions this module does
        TransportError,  // connect/read/write failed before a broker verdict
    };
    Status status{Status::TransportError};
    std::string detail;

    [[nodiscard]] bool committed() const noexcept { return status == Status::Committed; }
};

// Connection factory: production passes network::connect_plain (or a TLS
// dialer); tests pass a factory that dials a fake broker.
using ConnectFn = std::function<std::unique_ptr<network::Connection>(const std::string& host,
                                                                     std::uint16_t port)>;

// SASL credentials for the resume dialogue. Default-constructed = no
// authentication (the pre-SASL behaviour, byte-for-byte). Only "PLAIN" is
// supported; any other mechanism value is refused locally before a byte is
// sent, so a typo cannot silently downgrade to unauthenticated.
struct ResumeAuth {
    // "" = none. "PLAIN" is always spoken; "SCRAM-SHA-256" is spoken when
    // the build carries clink::tls (OpenSSL provides its crypto) and is
    // refused loudly otherwise. Any other value is refused locally.
    std::string mechanism;
    std::string username;
    std::string password;

    [[nodiscard]] bool enabled() const noexcept { return !mechanism.empty(); }
};

// The whole recovery step against one bootstrap address:
// [SASL] -> ApiVersions -> FindCoordinator(transactional.id) -> connect to
// the coordinator -> [SASL] -> EndTxn(commit). Retries the retriable
// coordinator codes (NOT_COORDINATOR, COORDINATOR_NOT_AVAILABLE,
// COORDINATOR_LOAD_IN_PROGRESS) a bounded number of times; everything else
// is a final verdict. Authentication failures are final: credentials do
// not improve on retry.
[[nodiscard]] ResumeOutcome resume_commit(const std::string& bootstrap_host,
                                          std::uint16_t bootstrap_port,
                                          const TxnIdentity& txn,
                                          const ConnectFn& connect,
                                          const ResumeAuth& auth = {});

// --- wire encoding, exposed for the frame tests -----------------------------

namespace wire {

inline constexpr std::int16_t kApiVersionsKey = 18;
inline constexpr std::int16_t kFindCoordinatorKey = 10;
inline constexpr std::int16_t kEndTxnKey = 26;
inline constexpr std::int16_t kDescribeTransactionsKey = 65;
inline constexpr std::int16_t kSaslHandshakeKey = 17;
inline constexpr std::int16_t kSaslAuthenticateKey = 36;

// Complete request frames (size prefix included), non-flexible encoding.
[[nodiscard]] std::vector<std::byte> api_versions_request_v0(std::int32_t correlation_id);
[[nodiscard]] std::vector<std::byte> find_coordinator_request(std::int16_t version,
                                                              std::int32_t correlation_id,
                                                              const std::string& txn_id);
[[nodiscard]] std::vector<std::byte> end_txn_request_v1(std::int32_t correlation_id,
                                                        const TxnIdentity& txn,
                                                        bool commit);
[[nodiscard]] std::vector<std::byte> sasl_handshake_request_v1(std::int32_t correlation_id,
                                                               const std::string& mechanism);
// auth_bytes for PLAIN is '\0' + username + '\0' + password (empty authzid);
// plain_auth_bytes builds it, sasl_authenticate_request_v1 wraps any bytes.
[[nodiscard]] std::vector<std::byte> plain_auth_bytes(const std::string& username,
                                                      const std::string& password);
[[nodiscard]] std::vector<std::byte> sasl_authenticate_request_v1(
    std::int32_t correlation_id, const std::vector<std::byte>& auth_bytes);

struct ApiRange {
    std::int16_t min{0};
    std::int16_t max{-1};
};

// Parsed response bodies (input excludes the size prefix, includes the
// correlation id). nullopt = malformed / truncated.
struct ApiVersionsResponse {
    std::int16_t error_code{0};
    std::optional<ApiRange> find_coordinator;
    std::optional<ApiRange> end_txn;
    std::optional<ApiRange> describe_transactions;
};
[[nodiscard]] std::optional<ApiVersionsResponse> parse_api_versions_response_v0(
    const std::vector<std::byte>& body, std::int32_t expected_correlation_id);

// DescribeTransactions v0 (KIP-664; flexible/compact encoding - the one
// place this module speaks it). The QUERY the resume path needs when
// EndTxn answers INVALID_PRODUCER_EPOCH: the staged handle's epoch comes
// from librdkafka's PERIODIC statistics callback while the broker bumps
// the producer epoch on every commit, so a handle staged mid-run is
// chronically one commit stale and a legitimately committed transaction
// reads as fenced. The transaction's actual state answers what the fenced
// EndTxn cannot: CompleteCommit with our producer id = the commit landed.
[[nodiscard]] std::vector<std::byte> describe_transactions_request_v0(std::int32_t correlation_id,
                                                                      const std::string& txn_id);

struct DescribeTransactionsState {
    std::int16_t error_code{0};
    std::string state;  // "Empty", "Ongoing", "PrepareCommit", "CompleteCommit", ...
    std::int64_t producer_id{-1};
    std::int16_t producer_epoch{-1};
};
[[nodiscard]] std::optional<DescribeTransactionsState> parse_describe_transactions_response_v0(
    const std::vector<std::byte>& body, std::int32_t expected_correlation_id);

struct FindCoordinatorResponse {
    std::int16_t error_code{0};
    std::string host;
    std::int32_t port{0};
};
[[nodiscard]] std::optional<FindCoordinatorResponse> parse_find_coordinator_response(
    const std::vector<std::byte>& body, std::int32_t expected_correlation_id);

// EndTxn v0/v1 response: throttle + error_code.
[[nodiscard]] std::optional<std::int16_t> parse_end_txn_response(
    const std::vector<std::byte>& body, std::int32_t expected_correlation_id);

// SaslHandshake v1 response: error_code + the broker's enabled mechanisms
// (surfaced in the refusal detail so an operator sees what WOULD work).
struct SaslHandshakeResponse {
    std::int16_t error_code{0};
    std::vector<std::string> mechanisms;
};
[[nodiscard]] std::optional<SaslHandshakeResponse> parse_sasl_handshake_response_v1(
    const std::vector<std::byte>& body, std::int32_t expected_correlation_id);

// SaslAuthenticate v1 response: error_code, nullable error_message, and
// auth_bytes (the server's SASL payload - SCRAM's server-first and
// server-final ride here; PLAIN's is empty). session_lifetime_ms is not
// consumed. auth_bytes missing entirely (a truncated tail) parses as
// empty, so PLAIN exchanges against terse fakes keep working.
struct SaslAuthenticateResponse {
    std::int16_t error_code{0};
    std::string error_message;
    std::vector<std::byte> auth_bytes;
};
[[nodiscard]] std::optional<SaslAuthenticateResponse> parse_sasl_authenticate_response_v1(
    const std::vector<std::byte>& body, std::int32_t expected_correlation_id);

// Kafka protocol error-code names for the codes this path can meet;
// "error <n>" for the rest. Exposed so tests pin the mapping.
[[nodiscard]] std::string error_name(std::int16_t code);

}  // namespace wire

}  // namespace clink::kafka
