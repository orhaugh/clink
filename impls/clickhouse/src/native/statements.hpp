#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "native/column_plan.hpp"
#include "native/sql_text.hpp"

namespace clink::clickhouse::native {

struct ServerCapabilities {
    bool deduplicate_insert{false};              // system.settings lists it
    bool use_strict_insert_block_limits{false};  // system.settings lists it
    bool quorum{false};                          // insert_quorum is not '0'
    // Read from the server opened against and then pinned for the sink's life:
    // every INSERT sends these values, so the server's squash boundaries
    // depend on the statement, not on whichever profile or replica takes a
    // resend. A re-probe never changes them.
    std::uint64_t min_insert_block_size_rows{0};
    std::uint64_t min_insert_block_size_bytes{0};
    std::uint64_t max_partitions_per_insert_block{0};  // for the report
};

struct InsertText {
    std::string database, table;
    const ColumnPlan* plan{nullptr};
    ServerCapabilities caps;
    Token token;
    std::string sink_id;  // sanitised to [A-Za-z0-9_.-]
    std::uint32_t subtask{0};
};
// The full INSERT text, ending in VALUES, with the settings in a fixed order.
// Throws std::invalid_argument for a missing plan or column list, or for a
// token TokenSource did not issue.
[[nodiscard]] std::string insert_statement(const InsertText& in);

// `name` with every character outside [A-Za-z0-9_.-] replaced by `_`, so the
// operator name can go into log_comment unquoted and match system.query_log.
// insert_statement applies it too.
[[nodiscard]] std::string sanitise_sink_id(std::string_view name);

// SETTINGS max_execution_time=<s>, timeout_overflow_mode='throw'
[[nodiscard]] std::string bounded_settings(std::chrono::seconds budget);
// min(10 s, receive_timeout), rounded up to whole seconds, at least 1.
[[nodiscard]] std::chrono::seconds metadata_budget(std::chrono::milliseconds receive_timeout);

// Every selected expression is wrapped in CAST(... AS String), and every
// SELECT ends with bounded_settings(budget).
[[nodiscard]] std::string select_server_settings(std::chrono::seconds budget);
[[nodiscard]] std::string select_table(std::string_view db,
                                       std::string_view table,
                                       std::chrono::seconds budget);
[[nodiscard]] std::string select_columns(std::string_view db,
                                         std::string_view table,
                                         std::chrono::seconds budget);
[[nodiscard]] std::string select_merge_tree_settings(std::chrono::seconds budget);
[[nodiscard]] std::string select_replicated_merge_tree_settings(std::chrono::seconds budget);
[[nodiscard]] std::string select_cluster_replica_count(std::string_view cluster,
                                                       std::chrono::seconds budget);
[[nodiscard]] std::string select_cluster_tables(std::string_view cluster,
                                                std::string_view db,
                                                std::string_view table,
                                                std::chrono::seconds budget);
[[nodiscard]] std::string select_cluster_merge_tree_settings(std::string_view cluster,
                                                             std::chrono::seconds budget);
[[nodiscard]] std::string select_cluster_replicated_merge_tree_settings(
    std::string_view cluster, std::chrono::seconds budget);

// The settings every INSERT sends, which a server must list or the sink
// refuses it. The deduplication switch is not here: a server needs
// deduplicate_insert or insert_deduplicate, either will do.
[[nodiscard]] const std::vector<std::string>& required_settings();
// Sent only where the server lists them, and never a reason to refuse:
// deduplicate_insert and use_strict_insert_block_limits.
[[nodiscard]] const std::vector<std::string>& line_conditional_settings();

}  // namespace clink::clickhouse::native
