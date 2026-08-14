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
//   * Only non-flexible protocol versions are spoken (the flexible tagged
//     encoding is a later increment); a broker that has dropped them gets
//     Unsupported, not a guess.
//   * Transport is the engine's own Connection seam, so a fake broker tests
//     the full path byte-for-byte and TLS can slot in via the factory.
//
// SASL is not spoken yet: on a SASL-required listener the requests fail and
// recovery falls back. Recorded as the module's standing limitation.

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

// The whole recovery step against one bootstrap address:
// ApiVersions -> FindCoordinator(transactional.id) -> connect to the
// coordinator -> EndTxn(commit). Retries the retriable coordinator codes
// (NOT_COORDINATOR, COORDINATOR_NOT_AVAILABLE, COORDINATOR_LOAD_IN_PROGRESS)
// a bounded number of times; everything else is a final verdict.
[[nodiscard]] ResumeOutcome resume_commit(const std::string& bootstrap_host,
                                          std::uint16_t bootstrap_port,
                                          const TxnIdentity& txn,
                                          const ConnectFn& connect);

// --- wire encoding, exposed for the frame tests -----------------------------

namespace wire {

inline constexpr std::int16_t kApiVersionsKey = 18;
inline constexpr std::int16_t kFindCoordinatorKey = 10;
inline constexpr std::int16_t kEndTxnKey = 26;

// Complete request frames (size prefix included), non-flexible encoding.
[[nodiscard]] std::vector<std::byte> api_versions_request_v0(std::int32_t correlation_id);
[[nodiscard]] std::vector<std::byte> find_coordinator_request(std::int16_t version,
                                                              std::int32_t correlation_id,
                                                              const std::string& txn_id);
[[nodiscard]] std::vector<std::byte> end_txn_request_v1(std::int32_t correlation_id,
                                                        const TxnIdentity& txn,
                                                        bool commit);

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
};
[[nodiscard]] std::optional<ApiVersionsResponse> parse_api_versions_response_v0(
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

// Kafka protocol error-code names for the codes this path can meet;
// "error <n>" for the rest. Exposed so tests pin the mapping.
[[nodiscard]] std::string error_name(std::int16_t code);

}  // namespace wire

}  // namespace clink::kafka
