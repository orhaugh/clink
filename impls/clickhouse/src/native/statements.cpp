#include "native/statements.hpp"

#include <algorithm>
#include <cstddef>
#include <stdexcept>

namespace clink::clickhouse::native {

namespace {

// The MergeTree-level settings the open checks read: async_insert, which the
// server ORs into the query's own, and the two deduplication windows for the
// report.
constexpr std::string_view kMergeTreeSettingNames =
    "('async_insert', 'non_replicated_deduplication_window', "
    "'replicated_deduplication_window')";

// Also read, and never required on their own: insert_deduplicate, the older
// deduplication switch a server may have instead of deduplicate_insert, and
// two settings the open report and the quorum error rules use.
constexpr std::string_view kAlsoProbedSettings[] = {
    "insert_deduplicate",
    "max_partitions_per_insert_block",
    "insert_quorum",
};

bool is_lower_hex(char c) noexcept {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
}

// A default-constructed Token would give every INSERT the same deduplication
// token, so a later batch could be dropped as a resend of an earlier one.
void require_issued_token(const Token& token) {
    bool ok = token.seq != 0 && token.nonce_hex.size() == 32;
    for (const char c : token.nonce_hex) {
        ok = ok && is_lower_hex(c);
    }
    if (!ok) {
        throw std::invalid_argument("insert_statement: the token is not one TokenSource issued (" +
                                    token.text() + ")");
    }
}

std::string in_list(const std::vector<std::string_view>& names) {
    std::string out = "(";
    for (std::size_t i = 0; i < names.size(); ++i) {
        if (i > 0) {
            out += ", ";
        }
        out += quote_string(names[i]);
    }
    out += ")";
    return out;
}

std::string bounded(std::string sql, std::chrono::seconds budget) {
    sql += ' ';
    sql += bounded_settings(budget);
    return sql;
}

// A clusterAllReplicas read with skip_unavailable_shards=0 pinned. A profile
// that sets it to 1 would have the server leave an unreachable replica out of
// the result instead of failing the read, so a replica outage would look like
// a replica without the table or without its settings rows.
std::string cluster_bounded(std::string sql, std::chrono::seconds budget) {
    sql = bounded(std::move(sql), budget);
    sql += ", skip_unavailable_shards=0";
    return sql;
}

// The first two columns of every cluster read. hostName() and tcpPort() run on
// each replica, so the first column names the replica a row came from, for a
// refusal to cite. Two instances on one machine share a host name and, without
// a plain TCP port, both report the default port, so rows are matched up across
// reads by the second column, the server's own UUID, which no two instances
// share.
constexpr std::string_view kReplicaColumns =
    "CAST(concat(hostName(), ':', toString(tcpPort())) AS String), "
    "CAST(serverUUID() AS String)";

std::string merge_tree_settings_from(std::string_view system_table, std::chrono::seconds budget) {
    return bounded("SELECT CAST(name AS String), CAST(value AS String) FROM " +
                       std::string(system_table) + " WHERE name IN " +
                       std::string(kMergeTreeSettingNames),
                   budget);
}

std::string cluster_merge_tree_settings_from(std::string_view cluster,
                                             std::string_view system_table,
                                             std::chrono::seconds budget) {
    return cluster_bounded("SELECT " + std::string(kReplicaColumns) +
                               ", CAST(name AS String), CAST(value AS String) FROM "
                               "clusterAllReplicas(" +
                               quote_string(cluster) + ", " + std::string(system_table) +
                               ") WHERE name IN " + std::string(kMergeTreeSettingNames),
                           budget);
}

}  // namespace

std::string sanitise_sink_id(std::string_view name) {
    std::string out(name);
    for (char& c : out) {
        const bool keep = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                          (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-';
        if (!keep) {
            c = '_';
        }
    }
    return out;
}

std::string insert_statement(const InsertText& in) {
    if (in.plan == nullptr || in.plan->column_list_sql.empty()) {
        // Without a column list the server expects every column, including
        // the ones the plan leaves to their defaults.
        throw std::invalid_argument("insert_statement: no column plan for " +
                                    qualified_table(in.database, in.table));
    }
    require_issued_token(in.token);

    std::string sql = "INSERT INTO " + qualified_table(in.database, in.table) + " " +
                      in.plan->column_list_sql +
                      " SETTINGS async_insert=0, wait_for_async_insert=1, ";
    sql += in.caps.deduplicate_insert ? "deduplicate_insert='enable'" : "insert_deduplicate=1";
    sql += ", insert_deduplication_token=" + quote_string(in.token.text());
    sql +=
        ", input_format_native_allow_types_conversion=0, input_format_null_as_default=0, "
        "throw_on_max_partitions_per_insert_block=1, distributed_foreground_insert=1";
    // Sent explicitly so that a resend under the same token is squashed into
    // the same blocks as the first attempt, whatever profile it meets.
    sql += ", min_insert_block_size_rows=" + std::to_string(in.caps.min_insert_block_size_rows);
    sql += ", min_insert_block_size_bytes=" + std::to_string(in.caps.min_insert_block_size_bytes);
    if (in.caps.use_strict_insert_block_limits) {
        // A server that lacks the setting rejects it, so it is sent only
        // where system.settings lists it.
        sql += ", use_strict_insert_block_limits=0";
    }
    // The sequence matches the token's, so system.query_log rows correlate
    // with the token; BeginInsert drops the query id, so this is the only
    // handle on the INSERT there.
    sql += ", log_comment=" +
           quote_string("clink:" + sanitise_sink_id(in.sink_id) + ":sub" +
                        std::to_string(in.subtask) + ":" + std::to_string(in.token.seq));
    sql += " VALUES";
    return sql;
}

std::string bounded_settings(std::chrono::seconds budget) {
    // max_execution_time=0 means no limit, so a budget below one second
    // would remove the bound rather than tighten it.
    const auto limit = std::max<std::chrono::seconds::rep>(budget.count(), 1);
    return "SETTINGS max_execution_time=" + std::to_string(limit) +
           ", timeout_overflow_mode='throw'";
}

std::chrono::seconds metadata_budget(std::chrono::milliseconds receive_timeout) {
    const auto capped = std::min(receive_timeout, std::chrono::milliseconds{10'000});
    return std::max(std::chrono::ceil<std::chrono::seconds>(capped), std::chrono::seconds{1});
}

std::string select_server_settings(std::chrono::seconds budget) {
    std::vector<std::string_view> names;
    for (const auto& name : required_settings()) {
        names.emplace_back(name);
    }
    for (const auto& name : line_conditional_settings()) {
        names.emplace_back(name);
    }
    for (const auto name : kAlsoProbedSettings) {
        names.push_back(name);
    }
    return bounded(
        "SELECT CAST(name AS String), CAST(value AS String) FROM system.settings WHERE name IN " +
            in_list(names),
        budget);
}

std::string select_table(std::string_view db, std::string_view table, std::chrono::seconds budget) {
    return bounded(
        "SELECT CAST(engine AS String), CAST(engine_full AS String) FROM system.tables WHERE "
        "database = " +
            quote_string(db) + " AND name = " + quote_string(table),
        budget);
}

std::string select_columns(std::string_view db,
                           std::string_view table,
                           std::chrono::seconds budget) {
    return bounded(
        "SELECT CAST(name AS String), CAST(type AS String), CAST(default_kind AS String), "
        "CAST(position AS String) FROM system.columns WHERE database = " +
            quote_string(db) + " AND table = " + quote_string(table) + " ORDER BY position",
        budget);
}

std::string select_merge_tree_settings(std::chrono::seconds budget) {
    return merge_tree_settings_from("system.merge_tree_settings", budget);
}

std::string select_replicated_merge_tree_settings(std::chrono::seconds budget) {
    // A server's <replicated_merge_tree> section overrides <merge_tree> for
    // Replicated tables, and only this table shows the result.
    return merge_tree_settings_from("system.replicated_merge_tree_settings", budget);
}

std::string select_cluster_replica_count(std::string_view cluster, std::chrono::seconds budget) {
    return bounded("SELECT CAST(count() AS String) FROM system.clusters WHERE cluster = " +
                       quote_string(cluster),
                   budget);
}

std::string select_macros(std::chrono::seconds budget) {
    return bounded("SELECT CAST(macro AS String), CAST(substitution AS String) FROM system.macros",
                   budget);
}

std::string select_cluster_tables(std::string_view cluster,
                                  std::string_view db,
                                  std::string_view table,
                                  std::chrono::seconds budget) {
    return cluster_bounded("SELECT " + std::string(kReplicaColumns) +
                               ", CAST(engine AS String), CAST(engine_full AS String) FROM "
                               "clusterAllReplicas(" +
                               quote_string(cluster) + ", system.tables) WHERE database = " +
                               quote_string(db) + " AND name = " + quote_string(table),
                           budget);
}

std::string select_cluster_merge_tree_settings(std::string_view cluster,
                                               std::chrono::seconds budget) {
    return cluster_merge_tree_settings_from(cluster, "system.merge_tree_settings", budget);
}

std::string select_cluster_replicated_merge_tree_settings(std::string_view cluster,
                                                          std::chrono::seconds budget) {
    return cluster_merge_tree_settings_from(
        cluster, "system.replicated_merge_tree_settings", budget);
}

const std::vector<std::string>& required_settings() {
    static const std::vector<std::string> names = {
        "async_insert",
        "wait_for_async_insert",
        "insert_deduplication_token",
        "input_format_native_allow_types_conversion",
        "input_format_null_as_default",
        "throw_on_max_partitions_per_insert_block",
        "log_comment",
        "max_execution_time",
        "timeout_overflow_mode",
        "distributed_foreground_insert",
        "min_insert_block_size_rows",
        "min_insert_block_size_bytes",
    };
    return names;
}

const std::vector<std::string>& line_conditional_settings() {
    static const std::vector<std::string> names = {
        "deduplicate_insert",
        "use_strict_insert_block_limits",
    };
    return names;
}

}  // namespace clink::clickhouse::native
