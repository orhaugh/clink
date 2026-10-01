#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <string>

namespace clink::clickhouse::native {

enum class Phase : std::uint8_t { Connect, Metadata, Begin, Send, End };

enum class Signal : std::uint8_t {
    Server,
    Transport,
    Tls,
    TlsVerify,
    Protocol,
    Unimplemented,
    Validation,
    BadOptional,
    Conversion,
    HeaderDrift,
    Other
};

enum class FailureClass : std::uint8_t {
    TransientNotWritten,
    InDoubt,
    MergeBackpressure,
    Resource,
    Permanent,
    ClientDefect,
    Unclassified
};
inline constexpr std::size_t kFailureClasses = 7;

struct Failure {
    Signal signal{Signal::Other};
    Phase phase{Phase::Connect};
    int code{0};  // ClickHouse code for Signal::Server
    std::string message;
    bool after_send{false};  // a send_block call started in this attempt
    // The code of a NativeSinkError of our own (a CA the transport could not
    // load, a refusal from a re-probe, a conversion failure), so that failing
    // on it does not rename it. Null for every other exception. Points at
    // storage that lives for the process.
    const char* sink_code{nullptr};
};

// Inspects the in-flight exception (call inside a catch block). `phase` is
// where the call was; Connect also covers building the socket factory.
[[nodiscard]] Failure to_failure(std::exception_ptr e, Phase phase, bool after_send);
// The signal, the phase and the quorum flag only. Knows nothing about the
// INSERT's size or its earlier attempts.
[[nodiscard]] FailureClass classify(const Failure& f, bool quorum_inserts);
[[nodiscard]] const char* to_string(FailureClass c);  // metric label values

// What the writer knows about the INSERT that failed.
struct AttemptState {
    std::uint64_t rows{0};                   // rows in this INSERT (or half)
    std::uint32_t unclassified_attempts{0};  // earlier Unclassified failures of it
    std::uint32_t validation_attempts{0};    // earlier ValidationError failures of it
};

enum class Action : std::uint8_t {
    Retry,        // back off, rebuild, resend; the token follows the resend rule
    SplitHalves,  // abandon, split by rows, resend each half under a fresh token
    Fail          // permanent: fail the task with `fail_code`
};

struct Decision {
    Action action{Action::Fail};
    const char* fail_code{nullptr};  // set when action == Fail
};

// Below this many rows a memory-limit failure is retried instead of split.
inline constexpr std::uint64_t kSplitMinRows = 1000;
// Unclassified failures retried before the INSERT fails.
inline constexpr std::uint32_t kUnclassifiedRetries = 3;
// ValidationError failures retried before the INSERT fails.
inline constexpr std::uint32_t kValidationRetries = 1;

// The policy that depends on the INSERT: the split threshold and the attempt
// caps. A retry "by phase" is still Action::Retry; whether the resend is in
// doubt follows the failure's phase and after_send.
[[nodiscard]] Decision action_for(FailureClass c, const Failure& f, const AttemptState& s);

}  // namespace clink::clickhouse::native
