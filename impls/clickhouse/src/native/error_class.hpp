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
};

// Inspects the in-flight exception (call inside a catch block).
[[nodiscard]] Failure to_failure(std::exception_ptr e, Phase phase, bool after_send);
[[nodiscard]] FailureClass classify(const Failure& f, bool quorum_inserts);
[[nodiscard]] const char* to_string(FailureClass c);  // metric label values

}  // namespace clink::clickhouse::native
