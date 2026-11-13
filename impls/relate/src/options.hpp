#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "errors.hpp"
#include "types.hpp"

namespace clink::relate {

struct SinkOptions {
    // The store: a directory, or a single file whose name ends in .rl. Created
    // when it does not exist. A relative path is read against the working
    // directory of the process that runs the sink.
    std::string path;
    // The Cypher run once for each batch, reading its rows from $rows.
    std::string statement;
    // Schema statements (constraints, indexes) applied each time the sink
    // opens, before the statement is checked.
    std::vector<std::string> schema;
    std::uint64_t batch_size{1000};
    std::uint64_t batch_bytes{4ULL << 20};
    std::uint64_t flush_interval_ms{1000};
    std::uint64_t retry_window_ms{30000};
    std::vector<Column> columns;
    std::uint32_t subtask{0};
    std::uint32_t parallelism{1};
};

// Every option the sink reads, refused when malformed, out of range or
// meaningless here; keys the SQL planner puts on every sink op are accepted
// and ignored.
[[nodiscard]] SinkOptions parse_options(const std::map<std::string, std::string>& params,
                                        std::uint32_t subtask,
                                        std::uint32_t parallelism);

// The options a refusal lists as accepted.
[[nodiscard]] const std::vector<std::string>& option_names();

// Splits Cypher at the semicolons that end statements: not those inside a
// quoted string, a backticked name or a comment. Blank statements are dropped.
[[nodiscard]] std::vector<std::string> split_statements(std::string_view text);

}  // namespace clink::relate
