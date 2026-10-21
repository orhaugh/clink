#pragma once

// The output row of a SQL lookup join, shared by every lookup implementation:
// the generic async_lookup_join_row operator, which calls a function from
// AsyncFunctionRegistry, and the connector lookups that the planner lowers to
// their own operator (neo4j_lookup_join_row).

#include <string>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_kind.hpp"

namespace clink::sql {

// The aliases and columns of one lookup join, as the planner describes them:
// the probe stream's columns and the lookup table's declared columns.
struct LookupJoinShape {
    std::string probe_alias;
    std::string dim_alias;
    std::vector<std::string> probe_columns;
    std::vector<std::string> dim_columns;
};

// Splits the planner's comma-separated column list.
inline std::vector<std::string> split_lookup_columns(const std::string& csv) {
    std::vector<std::string> out;
    std::size_t start = 0;
    while (start < csv.size()) {
        const auto comma = csv.find(',', start);
        const auto end = comma == std::string::npos ? csv.size() : comma;
        if (end > start) {
            out.push_back(csv.substr(start, end - start));
        }
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return out;
}

// The row a lookup join emits for `probe` and the lookup's row `dim`: each
// probe column as <probe_alias>_<column>, then each lookup column as
// <dim_alias>_<column>, a column a row lacks as NULL, carrying the probe's
// change kind (a lookup enriches a row, so a delete stays a delete).
inline Row merge_lookup_row(const LookupJoinShape& shape, const Row& probe, const Row& dim) {
    Row out;
    for (const auto& c : shape.probe_columns) {
        const auto it = probe.values.find(c);
        out.values[shape.probe_alias + "_" + c] =
            it != probe.values.end() ? it->second : clink::config::JsonValue{};
    }
    for (const auto& c : shape.dim_columns) {
        const auto it = dim.values.find(c);
        out.values[shape.dim_alias + "_" + c] =
            it != dim.values.end() ? it->second : clink::config::JsonValue{};
    }
    copy_row_kind(probe, out);
    return out;
}

}  // namespace clink::sql
