#pragma once

// Per-job durability, parsed from a submission's query parameters.
//
// Until this existed the HTTP submit endpoints accepted only
// ?state_backend, so a SQL job submitted to a cluster could not enable
// periodic checkpointing at all: no completed checkpoints, therefore no
// exactly-once commit for a 2PC sink and no worker-loss recovery - while
// the identical compiled-job submission path had both. The gap was
// silent, which is the failure mode worth the most care: the job ran and
// produced output, it simply could never recover.
//
// Keyed on a plain string map rather than an HttpRequest so the parsing
// contract is testable without standing up a server, and so this header
// costs no HTTP dependency.

#include <cstdint>
#include <map>
#include <string>

#include "clink/cluster/protocol.hpp"

namespace clink::cluster {

// An absent parameter leaves the field at its default, so every existing
// caller behaves exactly as before. A malformed value sets *error and is
// REFUSED by the caller rather than defaulted: a mistyped interval that
// silently became "no checkpointing" would reproduce the same silence
// this exists to fix.
[[nodiscard]] inline CheckpointConfig checkpoint_config_from_query(
    const std::map<std::string, std::string>& query, std::string* error) {
    CheckpointConfig ckpt;
    const auto str_param = [&](const char* key) -> std::string {
        auto it = query.find(key);
        return it == query.end() ? std::string{} : it->second;
    };
    // Values arrive already percent-decoded, so a state-backend URI
    // carrying its own query (remote-read://...?hot_max_bytes=N)
    // round-trips intact.
    ckpt.state_backend_uri = str_param("state_backend");
    ckpt.checkpoint_dir = str_param("checkpoint_dir");
    ckpt.restore_from_dir = str_param("restore_from_dir");
    const auto parse_int = [&](const char* key, auto& target) {
        const auto raw = str_param(key);
        if (raw.empty()) {
            return;
        }
        try {
            std::size_t consumed = 0;
            const auto value = std::stoll(raw, &consumed);
            if (consumed != raw.size()) {
                *error = std::string{"query parameter "} + key + " is not an integer: " + raw;
                return;
            }
            if (value < 0) {
                *error = std::string{"query parameter "} + key + " must not be negative: " + raw;
                return;
            }
            target = static_cast<std::remove_reference_t<decltype(target)>>(value);
        } catch (const std::exception&) {
            *error = std::string{"query parameter "} + key + " is not an integer: " + raw;
        }
    };
    parse_int("checkpoint_interval_ms", ckpt.interval_ms);
    parse_int("restore_from_checkpoint_id", ckpt.restore_from_checkpoint_id);
    parse_int("max_restarts_on_worker_loss", ckpt.max_restarts_on_worker_loss);
    if (const auto alignment = str_param("alignment"); !alignment.empty()) {
        if (alignment == "aligned") {
            ckpt.alignment = CheckpointAlignment::Aligned;
        } else if (alignment == "unaligned") {
            ckpt.alignment = CheckpointAlignment::Unaligned;
        } else if (alignment == "adaptive") {
            ckpt.alignment = CheckpointAlignment::Adaptive;
        } else {
            *error =
                "query parameter alignment must be 'aligned', 'unaligned' or 'adaptive' "
                "(got '" +
                alignment + "')";
        }
    }
    return ckpt;
}

}  // namespace clink::cluster
