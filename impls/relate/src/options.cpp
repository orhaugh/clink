#include "options.hpp"

#include <algorithm>
#include <charconv>
#include <system_error>

namespace clink::relate {

namespace {

constexpr std::string_view kPrefix = "relate sink: ";

[[noreturn]] void refuse(const char* code, const std::string& message) {
    throw RelateError(code, std::string(kPrefix) + message);
}

// The keys the SQL planner, the materialised-view code and the catalog put on
// a sink op whatever its connector (the same set the Neo4j and native
// ClickHouse sinks accept). They are accepted and ignored.
const std::vector<std::string>& planner_keys() {
    static const std::vector<std::string> keys = {
        "bounded",
        "columnar_decode",
        "commit_group",
        "decimal_columns",
        "definition_sql",
        "delim",
        "forced_singleton",
        "freshness",
        "freshness_ms",
        "primary_key",
        "query",
        "refresh_arm",
        "schema_columns",
        "state_ttl",
        "state_ttl_domain",
        "view_kind",
        "watermark_delay_ms",
    };
    return keys;
}

bool listed(const std::vector<std::string>& list, const std::string& key) {
    return std::find(list.begin(), list.end(), key) != list.end();
}

std::string shown(const std::string& key, const std::string& value) {
    return key + "='" + value + "'";
}

std::uint64_t parse_uint(const std::string& key,
                         const std::string& s,
                         std::uint64_t min,
                         std::uint64_t max) {
    std::uint64_t out = 0;
    const auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    if (s.empty() || ec != std::errc{} || ptr != s.data() + s.size() || out < min || out > max) {
        refuse(code::kOptionInvalid,
               shown(key, s) + " is not a whole number from " + std::to_string(min) + " to " +
                   std::to_string(max));
    }
    return out;
}

bool blank(std::string_view s) {
    return std::all_of(
        s.begin(), s.end(), [](char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r'; });
}

const std::string* get(const std::map<std::string, std::string>& params, const char* key) {
    const auto it = params.find(key);
    return it == params.end() ? nullptr : &it->second;
}

}  // namespace

const std::vector<std::string>& option_names() {
    static const std::vector<std::string> names = {
        "path",
        "statement",
        "schema",
        "batch_size",
        "batch_bytes",
        "flush_interval_ms",
        "retry_window_ms",
        "delivery_guarantee",
        "mode",
        "changelog",
        "write_mode",
    };
    return names;
}

std::vector<std::string> split_statements(std::string_view text) {
    std::vector<std::string> out;
    std::string current;
    const auto finish = [&] {
        if (!blank(current)) {
            const auto first = current.find_first_not_of(" \t\r\n");
            const auto last = current.find_last_not_of(" \t\r\n");
            out.push_back(current.substr(first, last - first + 1));
        }
        current.clear();
    };
    char quote = 0;  // ', " or ` while inside one
    bool line_comment = false;
    bool block_comment = false;
    for (std::size_t i = 0; i < text.size(); ++i) {
        const char c = text[i];
        const char next = i + 1 < text.size() ? text[i + 1] : '\0';
        if (line_comment) {
            current += c;
            line_comment = c != '\n';
            continue;
        }
        if (block_comment) {
            current += c;
            if (c == '*' && next == '/') {
                current += next;
                ++i;
                block_comment = false;
            }
            continue;
        }
        if (quote != 0) {
            current += c;
            if (c == '\\' && quote != '`' && next != '\0') {
                current += next;
                ++i;
            } else if (c == quote) {
                quote = 0;
            }
            continue;
        }
        if (c == '\'' || c == '"' || c == '`') {
            quote = c;
            current += c;
        } else if (c == '/' && next == '/') {
            line_comment = true;
            current += c;
        } else if (c == '/' && next == '*') {
            block_comment = true;
            current += c;
        } else if (c == ';') {
            finish();
        } else {
            current += c;
        }
    }
    finish();
    return out;
}

SinkOptions parse_options(const std::map<std::string, std::string>& params,
                          std::uint32_t subtask,
                          std::uint32_t parallelism) {
    SinkOptions o;
    o.subtask = subtask;
    o.parallelism = parallelism;

    for (const auto& [key, value] : params) {
        if (key == "json_non_finite") {
            refuse(code::kOptionInvalid,
                   "json_non_finite does not apply: the sink writes typed values, and NaN and the "
                   "infinities are stored as Relate floats. Remove the option");
        }
        if (key == "sql_column_types" || listed(planner_keys(), key) ||
            listed(option_names(), key)) {
            continue;
        }
        std::string accepted;
        for (const auto& name : option_names()) {
            accepted += (accepted.empty() ? "" : ", ") + name;
        }
        refuse(code::kOptionInvalid, "unknown option '" + key + "'; the sink reads: " + accepted);
    }

    // What would change the guarantee is refused here as well as in the
    // planner: a job built directly on the Dag never passes through it.
    if (const auto* g = get(params, "delivery_guarantee"); g != nullptr && *g != "at_least_once") {
        refuse(code::kGuaranteeUnsupported,
               shown("delivery_guarantee", *g) +
                   " is not offered: the sink delivers at least once. Relate commits a batch in "
                   "one transaction and cannot hold one prepared for a later commit, so a batch "
                   "a restore replays is written again. Use a statement that is safe to replay "
                   "(see the connector page)");
    }
    if (const auto* m = get(params, "mode"); m != nullptr && *m != "append") {
        refuse(code::kGuaranteeUnsupported,
               shown("mode", *m) +
                   " is not offered: the statement decides how a row changes the graph, so "
                   "write a MERGE on a stable key instead of mode='upsert'");
    }
    if (const auto* c = get(params, "changelog"); c != nullptr && *c != "false") {
        refuse(code::kGuaranteeUnsupported,
               shown("changelog", *c) +
                   " is not offered: rows reach the statement without their change kind, so a "
                   "retraction would be written as a new row");
    }
    if (const auto* w = get(params, "write_mode"); w != nullptr && *w != "append") {
        refuse(code::kGuaranteeUnsupported,
               shown("write_mode", *w) + " is not offered: the sink only appends batches");
    }

    const auto* path = get(params, "path");
    if (path == nullptr || blank(*path)) {
        refuse(code::kOptionInvalid,
               "path is required: the Relate store to write, a directory or a single file "
               "whose name ends in .rl");
    }
    if (*path == ":memory:") {
        refuse(code::kOptionInvalid,
               "path=':memory:' is not offered: an in-memory store is gone when the task ends, "
               "so nothing could read what the sink wrote. Name a directory or a .rl file");
    }
    if (path->find("://") != std::string::npos || path->find('\0') != std::string::npos) {
        refuse(code::kOptionInvalid,
               shown("path", *path) + " must be a filesystem path, not a URI");
    }
    o.path = *path;

    const auto* statement = get(params, "statement");
    if (statement == nullptr || blank(*statement)) {
        refuse(code::kOptionInvalid,
               "statement is required: the Cypher to run for each batch, reading its rows from "
               "$rows (for example UNWIND $rows AS row MERGE ...)");
    }
    if (statement->find('\0') != std::string::npos) {
        refuse(code::kOptionInvalid, "statement holds a NUL character");
    }
    o.statement = *statement;

    if (const auto* schema = get(params, "schema")) {
        if (schema->find('\0') != std::string::npos) {
            refuse(code::kOptionInvalid, "schema holds a NUL character");
        }
        o.schema = split_statements(*schema);
    }

    if (const auto* v = get(params, "batch_size")) {
        // Relate's parameter builder takes at most 100000 entries in a list.
        o.batch_size = parse_uint("batch_size", *v, 1, 100'000);
    }
    if (const auto* v = get(params, "batch_bytes")) {
        // Under the builder's 16 MiB limit for one statement's parameters.
        o.batch_bytes = parse_uint("batch_bytes", *v, 1024, 12ULL << 20);
    }
    if (const auto* v = get(params, "flush_interval_ms")) {
        o.flush_interval_ms = parse_uint("flush_interval_ms", *v, 0, 3'600'000);
    }
    if (const auto* v = get(params, "retry_window_ms")) {
        o.retry_window_ms = parse_uint("retry_window_ms", *v, 0, 3'600'000);
    }

    const auto* types = get(params, "sql_column_types");
    if (types == nullptr || types->empty()) {
        refuse(code::kOptionInvalid,
               "the sink needs the table's declared columns (sql_column_types), which the SQL "
               "planner supplies; declare the table's columns");
    }
    o.columns = parse_column_types(*types);
    if (const auto problems = unsupported_columns(o.columns); !problems.empty()) {
        std::string all;
        for (const auto& p : problems) {
            all += (all.empty() ? "" : "; ") + p;
        }
        refuse(code::kTypeUnsupported, all);
    }
    return o;
}

}  // namespace clink::relate
