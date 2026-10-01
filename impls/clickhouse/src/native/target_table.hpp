#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "native/column_plan.hpp"
#include "native/insert_transport.hpp"
#include "native/statements.hpp"

namespace clink::clickhouse::native {

// SharedMergeTree is recognised so that the refusal can name it: the native
// sink refuses it with target_engine_unsupported, because nothing tests its
// deduplication yet.
enum class EngineFamily : std::uint8_t {
    MergeTree,
    ReplicatedMergeTree,
    SharedMergeTree,
    Distributed,
    Null,
    Other
};

struct DistributedTarget {
    std::string cluster, database, table;
    std::size_t replicas{0};  // rows system.clusters lists for the cluster
};

struct TargetInfo {
    // Who answered this probe. A kept-token resend is safe only to the same
    // identity, so the writer compares the identity of every new client.
    ServerIdentity server;
    bool tested_line{false};           // 26.3 or 26.8
    bool keep_token_on_resend{false};  // the lines where a resent token was proved safe
    // The line-conditional switches, the quorum flag and the squash
    // thresholds of the server that answered. The writer pins the thresholds
    // from the opener's probe and ignores those a re-probe reads.
    ServerCapabilities caps;
    std::string engine;
    EngineFamily family{EngineFamily::Other};
    std::optional<DistributedTarget> distributed;
    // The target's own columns; for a Distributed target, the Distributed
    // table's, which are what the INSERT is checked against.
    std::vector<TargetColumn> columns;
    std::uint64_t dedup_window{0};
    bool keeps_dedup_log{false};
    std::string dedup_report;  // "replicated_deduplication_window=10000 (server default)"
    std::string async_report;  // "async_insert=0 (table unset, server default 0)"
};

// Runs every open-time server and table check over a connected transport:
// the settings gate, the target's engine and columns, its effective
// async_insert and its deduplication window, and for a Distributed target the
// same checks on every replica's local table. Keeps no state between calls,
// so the writer calls it again on every new client it builds.
//
// Throws NativeSinkError for a refusal, and a refusal always rests on a read
// that succeeded and showed the problem. Lets the client's exceptions
// through, including a cluster read that fails because a replica is
// unreachable, so the caller's retry loop can classify them. The one client
// error it turns into a refusal is a permission error on a cluster read,
// which no retry can cure.
[[nodiscard]] TargetInfo probe_target(InsertTransport& transport, const SinkOptions& opts);

// Pieces, exposed for tests.

// The raw value text of `name` in the top-level SETTINGS clause of
// engine_full, quotes included ("1", "'0'", "true"). nullopt when there is no
// such setting, and also when engine_full cannot be scanned (an unclosed
// quote or bracket).
[[nodiscard]] std::optional<std::string> engine_full_setting(std::string_view engine_full,
                                                             std::string_view name);
// Distributed(cluster, database, table[, ...]). Each of the first three must
// be a quoted string or an identifier, and none may be empty; anything else
// gives nullopt. `replicas` is left 0.
[[nodiscard]] std::optional<DistributedTarget> parse_distributed(std::string_view engine_full);
[[nodiscard]] EngineFamily engine_family(std::string_view engine);
[[nodiscard]] bool is_tested_line(std::uint64_t major, std::uint64_t minor) noexcept;

}  // namespace clink::clickhouse::native
