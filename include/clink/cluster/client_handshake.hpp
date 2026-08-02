#pragma once

// Client-side handling of a refused handshake.
//
// A coordinator that refuses a client on protocol grounds replies with a
// SubmitJobAck carrying ok=false and the reason, then closes. That is the
// right frame for `clink submit`, which is expecting one. Every other CLI
// tool is expecting its own ack kind and would otherwise report
// "unexpected reply kind 12" - technically true, and useless.
//
// This turns that into the actual reason. It is deliberately a decode of a
// frame the tool did NOT expect rather than a new message kind: adding a
// kind would not help, because a client old enough to be refused is also
// old enough not to know the new kind.

#include <optional>
#include <string>

#include "clink/cluster/messages.hpp"
#include "clink/cluster/protocol.hpp"

namespace clink::cluster {

// Call from a client tool's "unexpected reply kind" branch, before
// reporting the kind. Returns the coordinator's reason when the frame is a
// refusal, or nullopt when it is genuinely something unexpected.
//
// `r` must be positioned just after the kind byte.
[[nodiscard]] inline std::optional<std::string> protocol_rejection_message(MessageKind kind,
                                                                           MessageReader& r) {
    if (kind != MessageKind::SubmitJobAck) {
        return std::nullopt;
    }
    try {
        auto ack = decode_submit_job_ack(r);
        if (!ack.ok && !ack.message.empty()) {
            return ack.message;
        }
    } catch (const std::exception&) {
        // A frame that will not decode as a refusal is not one.
    }
    return std::nullopt;
}

}  // namespace clink::cluster
