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
    bool deduplicate_insert{false};               // system.settings lists it
    bool use_strict_insert_block_limits{false};   // system.settings lists it
    bool quorum{false};                           // insert_quorum is not '0'
    std::uint64_t min_insert_block_size_rows{0};  // for the report
    std::uint64_t max_partitions_per_insert_block{0};
};

struct InsertText {
    std::string database, table;
    const ColumnPlan* plan{nullptr};
    ServerCapabilities caps;
    Token token;
    std::string sink_id;  // sanitised to [A-Za-z0-9_.-]
    std::uint32_t subtask{0};
};
[[nodiscard]] std::string insert_statement(const InsertText& in);

// SETTINGS max_execution_time=<s>, timeout_overflow_mode='throw'
[[nodiscard]] std::string bounded_settings(std::chrono::seconds budget);
[[nodiscard]] std::chrono::seconds metadata_budget(std::chrono::milliseconds receive_timeout);

[[nodiscard]] std::string select_server_settings(std::chrono::seconds budget);
[[nodiscard]] std::string select_table(std::string_view db,
                                       std::string_view table,
                                       std::chrono::seconds budget);
[[nodiscard]] std::string select_columns(std::string_view db,
                                         std::string_view table,
                                         std::chrono::seconds budget);
[[nodiscard]] std::string select_merge_tree_settings(std::chrono::seconds budget);
[[nodiscard]] std::string select_cluster_replica_count(std::string_view cluster,
                                                       std::chrono::seconds budget);
[[nodiscard]] std::string select_cluster_tables(std::string_view cluster,
                                                std::string_view db,
                                                std::string_view table,
                                                std::chrono::seconds budget);
[[nodiscard]] std::string select_cluster_merge_tree_settings(std::string_view cluster,
                                                             std::chrono::seconds budget);

[[nodiscard]] const std::vector<std::string>& required_settings();
[[nodiscard]] const std::vector<std::string>& line_conditional_settings();

}  // namespace clink::clickhouse::native
