#include "native/statements.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native {

std::string insert_statement(const InsertText& /*in*/) {
    not_implemented("insert_statement");
}

std::string bounded_settings(std::chrono::seconds /*budget*/) {
    not_implemented("bounded_settings");
}

std::chrono::seconds metadata_budget(std::chrono::milliseconds /*receive_timeout*/) {
    not_implemented("metadata_budget");
}

std::string select_server_settings(std::chrono::seconds /*budget*/) {
    not_implemented("select_server_settings");
}

std::string select_table(std::string_view /*db*/,
                         std::string_view /*table*/,
                         std::chrono::seconds /*budget*/) {
    not_implemented("select_table");
}

std::string select_columns(std::string_view /*db*/,
                           std::string_view /*table*/,
                           std::chrono::seconds /*budget*/) {
    not_implemented("select_columns");
}

std::string select_merge_tree_settings(std::chrono::seconds /*budget*/) {
    not_implemented("select_merge_tree_settings");
}

std::string select_cluster_replica_count(std::string_view /*cluster*/,
                                         std::chrono::seconds /*budget*/) {
    not_implemented("select_cluster_replica_count");
}

std::string select_cluster_tables(std::string_view /*cluster*/,
                                  std::string_view /*db*/,
                                  std::string_view /*table*/,
                                  std::chrono::seconds /*budget*/) {
    not_implemented("select_cluster_tables");
}

std::string select_cluster_merge_tree_settings(std::string_view /*cluster*/,
                                               std::chrono::seconds /*budget*/) {
    not_implemented("select_cluster_merge_tree_settings");
}

const std::vector<std::string>& required_settings() {
    not_implemented("required_settings");
}

const std::vector<std::string>& line_conditional_settings() {
    not_implemented("line_conditional_settings");
}

}  // namespace clink::clickhouse::native
