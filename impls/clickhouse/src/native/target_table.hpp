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
    std::size_t replicas{0};
};

struct TargetInfo {
    ServerIdentity server;
    bool tested_line{false};           // 26.3 or 26.8
    bool keep_token_on_resend{false};  // the lines where a resent token was proved safe
    ServerCapabilities caps;
    std::string engine;
    EngineFamily family{EngineFamily::Other};
    std::optional<DistributedTarget> distributed;
    std::vector<TargetColumn> columns;
    std::uint64_t dedup_window{0};
    bool keeps_dedup_log{false};
    std::string dedup_report;  // "replicated_deduplication_window=10000 (server default)"
    std::string async_report;  // "async_insert=0 (table unset, server default 0)"
};

// Runs every open-time server and table check over a connected transport.
// Throws NativeSinkError for a refusal. Lets the client's exceptions through,
// so the caller's retry loop can classify them.
[[nodiscard]] TargetInfo probe_target(InsertTransport& transport, const SinkOptions& opts);

// Pieces, exposed for tests.
[[nodiscard]] std::optional<std::string> engine_full_setting(std::string_view engine_full,
                                                             std::string_view name);
[[nodiscard]] std::optional<DistributedTarget> parse_distributed(std::string_view engine_full);
[[nodiscard]] EngineFamily engine_family(std::string_view engine);
[[nodiscard]] bool is_tested_line(std::uint64_t major, std::uint64_t minor) noexcept;

}  // namespace clink::clickhouse::native
