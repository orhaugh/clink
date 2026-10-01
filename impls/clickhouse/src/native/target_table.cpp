#include "native/target_table.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <map>
#include <utility>

#include <clickhouse/error_codes.h>
#include <clickhouse/exceptions.h>

#include "native/errors.hpp"
#include "native/sql_text.hpp"

namespace clink::clickhouse::native {

namespace {

namespace ch = ::clickhouse;

using Settings = std::map<std::string, std::string, std::less<>>;

constexpr std::string_view kMergeTreeTable = "merge_tree_settings";
constexpr std::string_view kReplicatedTable = "replicated_merge_tree_settings";

// The upstream defect that makes asynchronous inserts unsafe for a sink that
// counts acknowledged rows: an acknowledged row can be lost on that path.
constexpr std::string_view kAsyncReason =
    "which the native sink refuses while upstream issue #121174 can lose acknowledged rows on "
    "that path";

bool is_space(char c) noexcept {
    return std::isspace(static_cast<unsigned char>(c)) != 0;
}

std::string_view trim(std::string_view s) noexcept {
    while (!s.empty() && is_space(s.front())) {
        s.remove_prefix(1);
    }
    while (!s.empty() && is_space(s.back())) {
        s.remove_suffix(1);
    }
    return s;
}

std::string join(const std::vector<std::string>& parts, std::string_view sep) {
    std::string out;
    for (std::size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) {
            out += sep;
        }
        out += parts[i];
    }
    return out;
}

// Walks `text` and calls visit(i, depth) for every character outside quoted
// text, where depth counts the brackets open around it. An opening bracket is
// visited at the depth outside it, and a closing one at the depth it returns
// to, so a caller can find where a bracketed list ends. Quoted text (single,
// double or back quotes, with backslash escapes and doubled quotes) is skipped
// whole. Returns false when a quote is left open or the brackets do not
// balance, so an absent clause can be told apart from text that could not be
// followed.
template <typename Visit>
bool walk(std::string_view text, Visit&& visit) {
    char quote = 0;
    int depth = 0;
    for (std::size_t i = 0; i < text.size(); ++i) {
        const char c = text[i];
        if (quote != 0) {
            if (c == '\\') {
                ++i;
            } else if (c == quote) {
                if (i + 1 < text.size() && text[i + 1] == quote) {
                    ++i;
                } else {
                    quote = 0;
                }
            }
            continue;
        }
        if (c == '\'' || c == '"' || c == '`') {
            quote = c;
            continue;
        }
        if (c == ')' || c == ']' || c == '}') {
            if (depth == 0) {
                return false;
            }
            --depth;
            visit(i, depth);
            continue;
        }
        visit(i, depth);
        if (c == '(' || c == '[' || c == '{') {
            ++depth;
        }
    }
    return quote == 0 && depth == 0;
}

// Splits on `sep` outside quotes and brackets. nullopt when `text` cannot be
// scanned.
std::optional<std::vector<std::string_view>> split_top_level(std::string_view text, char sep) {
    std::vector<std::string_view> parts;
    std::size_t start = 0;
    const bool ok = walk(text, [&](std::size_t i, int depth) {
        if (depth == 0 && text[i] == sep) {
            parts.push_back(text.substr(start, i - start));
            start = i + 1;
        }
    });
    if (!ok) {
        return std::nullopt;
    }
    parts.push_back(text.substr(start));
    return parts;
}

// One quoted string or identifier, unquoted. A bare identifier is
// [A-Za-z_][A-Za-z0-9_]*; anything else, an expression or a function call
// such as currentDatabase(), gives nullopt.
std::optional<std::string> literal_or_identifier(std::string_view arg) {
    arg = trim(arg);
    if (arg.empty()) {
        return std::nullopt;
    }
    const char q = arg.front();
    if (q == '\'' || q == '"' || q == '`') {
        std::string out;
        for (std::size_t i = 1; i < arg.size(); ++i) {
            const char c = arg[i];
            if (c == '\\') {
                if (++i == arg.size()) {
                    return std::nullopt;
                }
                switch (arg[i]) {
                    case 'n':
                        out += '\n';
                        break;
                    case 't':
                        out += '\t';
                        break;
                    case 'r':
                        out += '\r';
                        break;
                    case '0':
                        out += '\0';
                        break;
                    default:
                        out += arg[i];
                        break;
                }
            } else if (c == q) {
                if (i + 1 < arg.size() && arg[i + 1] == q) {
                    out += q;
                    ++i;
                } else if (i + 1 == arg.size()) {
                    return out;
                } else {
                    return std::nullopt;  // text after the closing quote
                }
            } else {
                out += c;
            }
        }
        return std::nullopt;  // never closed
    }
    const auto head_ok = [](char c) {
        return std::isalpha(static_cast<unsigned char>(c)) != 0 || c == '_';
    };
    const auto tail_ok = [&](char c) {
        return head_ok(c) || std::isdigit(static_cast<unsigned char>(c)) != 0;
    };
    if (!head_ok(arg.front()) || !std::all_of(arg.begin(), arg.end(), tail_ok)) {
        return std::nullopt;
    }
    return std::string(arg);
}

// What a settings lookup in engine_full found. `readable` is false when the
// text could not be scanned, which is different from the setting being unset.
struct SettingLookup {
    bool readable{true};
    std::optional<std::string> value;
};

SettingLookup lookup_engine_setting(std::string_view engine_full, std::string_view name) {
    constexpr std::string_view kKeyword = "SETTINGS";
    std::optional<std::size_t> clause;
    const bool ok = walk(engine_full, [&](std::size_t i, int depth) {
        if (clause || depth != 0 || engine_full.substr(i, kKeyword.size()) != kKeyword) {
            return;
        }
        const std::size_t end = i + kKeyword.size();
        const bool word_start = i > 0 && is_space(engine_full[i - 1]);
        const bool word_end = end == engine_full.size() || is_space(engine_full[end]);
        if (word_start && word_end) {
            clause = end;
        }
    });
    if (!ok) {
        return {false, std::nullopt};
    }
    if (!clause) {
        return {};
    }
    const auto pairs = split_top_level(engine_full.substr(*clause), ',');
    if (!pairs) {
        return {false, std::nullopt};
    }
    SettingLookup found;
    for (const std::string_view pair : *pairs) {
        const std::size_t eq = pair.find('=');
        if (eq == std::string_view::npos) {
            return {false, std::nullopt};
        }
        std::string_view key = trim(pair.substr(0, eq));
        if (key.size() >= 2 && key.front() == '`' && key.back() == '`') {
            key = key.substr(1, key.size() - 2);
        }
        if (key == name && !found.value) {
            found.value = std::string(trim(pair.substr(eq + 1)));
        }
    }
    return found;
}

std::string_view strip_single_quotes(std::string_view v) noexcept {
    v = trim(v);
    if (v.size() >= 2 && v.front() == '\'' && v.back() == '\'') {
        v = v.substr(1, v.size() - 2);
    }
    return v;
}

// 0, '0' and false are off; 1, '1' and true are on; anything else is
// unreadable, and a refusal says so rather than guess.
std::optional<bool> setting_bool(std::string_view raw) {
    std::string v(strip_single_quotes(raw));
    std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    if (v == "0" || v == "false") {
        return false;
    }
    if (v == "1" || v == "true") {
        return true;
    }
    return std::nullopt;
}

std::optional<std::uint64_t> setting_uint(std::string_view raw) {
    const std::string_view v = strip_single_quotes(raw);
    std::uint64_t out = 0;
    const auto [end, ec] = std::from_chars(v.data(), v.data() + v.size(), out);
    if (v.empty() || ec != std::errc{} || end != v.data() + v.size()) {
        return std::nullopt;
    }
    return out;
}

// A row shorter than its result is a transport defect, not a server answer
// the probe could refuse on, so it surfaces as the client's protocol error.
const std::string& cell(const std::vector<std::string>& row, std::size_t i) {
    if (i >= row.size()) {
        throw ch::ProtocolError("metadata result row has " + std::to_string(row.size()) +
                                " columns, expected at least " + std::to_string(i + 1));
    }
    return row[i];
}

Settings name_values(const ResultSet& rs) {
    Settings out;
    for (const auto& row : rs.rows) {
        out.emplace(cell(row, 0), cell(row, 1));
    }
    return out;
}

// host -> name -> value, from a clusterAllReplicas settings read.
std::map<std::string, Settings, std::less<>> per_host(const ResultSet& rs) {
    std::map<std::string, Settings, std::less<>> out;
    for (const auto& row : rs.rows) {
        out[cell(row, 0)].emplace(cell(row, 1), cell(row, 2));
    }
    return out;
}

std::string version_text(const ServerIdentity& s) {
    return std::to_string(s.major) + "." + std::to_string(s.minor) + "." + std::to_string(s.patch);
}

std::string endpoint_text(const Endpoint& e) {
    const bool v6 = e.host.find(':') != std::string::npos;
    return (v6 ? "[" + e.host + "]" : e.host) + ":" + std::to_string(e.port);
}

bool is_merge_tree_family(EngineFamily f) noexcept {
    return f == EngineFamily::MergeTree || f == EngineFamily::ReplicatedMergeTree;
}

// --- The settings gate ---------------------------------------------------------

void check_server_settings(InsertTransport& transport,
                           std::chrono::seconds budget,
                           TargetInfo& info) {
    const Settings s =
        name_values(transport.select(MetaQuery::ServerSettings, select_server_settings(budget)));
    const std::string where =
        "server " + version_text(info.server) + " at " + endpoint_text(info.server.endpoint);

    std::vector<std::string> missing;
    for (const auto& name : required_settings()) {
        if (!s.contains(name)) {
            missing.push_back(name);
        }
    }
    // Either deduplication switch will do: the INSERT sends whichever the
    // server has.
    if (!s.contains("deduplicate_insert") && !s.contains("insert_deduplicate")) {
        missing.emplace_back("deduplicate_insert or insert_deduplicate");
    }
    if (!missing.empty()) {
        throw NativeSinkError(code::kServerSettingsUnsupported,
                              where + " lacks settings the native sink sends with every INSERT: " +
                                  join(missing, ", ") +
                                  ". Use a server line that has them; 26.3 and 26.8 are tested.");
    }

    for (const auto& name : line_conditional_settings()) {
        const bool listed = s.contains(name);
        if (name == "deduplicate_insert") {
            info.caps.deduplicate_insert = listed;
        } else if (name == "use_strict_insert_block_limits") {
            info.caps.use_strict_insert_block_limits = listed;
        }
    }

    // The thresholds are sent on every INSERT, so a value the probe cannot
    // read as a whole number cannot be pinned faithfully.
    const auto threshold = [&](const char* name) {
        const std::string& raw = s.find(name)->second;
        const auto v = setting_uint(raw);
        if (!v) {
            throw NativeSinkError(code::kServerSettingsUnsupported,
                                  where + " reports " + name + "='" + raw +
                                      "', which the native sink cannot read as a whole number "
                                      "to send with every INSERT.");
        }
        return *v;
    };
    info.caps.min_insert_block_size_rows = threshold("min_insert_block_size_rows");
    info.caps.min_insert_block_size_bytes = threshold("min_insert_block_size_bytes");

    // Read for the report and the error rules only, never required.
    if (const auto it = s.find("max_partitions_per_insert_block"); it != s.end()) {
        info.caps.max_partitions_per_insert_block = setting_uint(it->second).value_or(0);
    }
    if (const auto it = s.find("insert_quorum"); it != s.end()) {
        const std::string_view q = strip_single_quotes(it->second);
        info.caps.quorum = !q.empty() && q != "0";
    }

    info.tested_line = is_tested_line(info.server.major, info.server.minor);
    // The two tested lines are also the two where a resent token was proved
    // to be deduplicated.
    info.keep_token_on_resend = info.tested_line;
}

// --- Target metadata -------------------------------------------------------------

std::optional<DefaultKind> default_kind_of(std::string_view text) {
    if (text.empty()) {
        return DefaultKind::None;
    }
    if (text == "DEFAULT") {
        return DefaultKind::Default;
    }
    if (text == "MATERIALIZED") {
        return DefaultKind::Materialized;
    }
    if (text == "ALIAS") {
        return DefaultKind::Alias;
    }
    if (text == "EPHEMERAL") {
        return DefaultKind::Ephemeral;
    }
    return std::nullopt;
}

std::vector<TargetColumn> read_columns(InsertTransport& transport,
                                       const SinkOptions& opts,
                                       std::chrono::seconds budget,
                                       const std::string& qualified) {
    const ResultSet rs =
        transport.select(MetaQuery::Columns, select_columns(opts.database, opts.table, budget));
    std::vector<TargetColumn> columns;
    columns.reserve(rs.rows.size());
    for (const auto& row : rs.rows) {
        TargetColumn c;
        c.name = cell(row, 0);
        // Passed through verbatim: the column plan parses it.
        c.type = cell(row, 1);
        const auto kind = default_kind_of(cell(row, 2));
        if (!kind) {
            throw NativeSinkError(code::kTargetUnreadable,
                                  "column " + quote_identifier(c.name) + " of " + qualified +
                                      " has default kind '" + cell(row, 2) +
                                      "', which the native sink does not know");
        }
        c.default_kind = *kind;
        const auto position = setting_uint(cell(row, 3));
        if (!position || *position > UINT32_MAX) {
            throw NativeSinkError(code::kTargetUnreadable,
                                  "column " + quote_identifier(c.name) + " of " + qualified +
                                      " has position '" + cell(row, 3) +
                                      "', which is not a column position");
        }
        c.position = static_cast<std::uint32_t>(*position);
        columns.push_back(std::move(c));
    }
    if (columns.empty()) {
        // system.columns shows only what the user may see, so an empty list
        // for a table system.tables showed is most likely a missing grant.
        throw NativeSinkError(code::kTargetUnreadable,
                              "system.columns lists no columns for " + qualified +
                                  ", so the native sink cannot check its rows against it; the "
                                  "sink's user may lack SHOW COLUMNS on the table");
    }
    return columns;
}

void refuse_engine(const std::string& qualified,
                   const std::string& where,
                   const std::string& engine,
                   EngineFamily family) {
    if (family == EngineFamily::SharedMergeTree) {
        throw NativeSinkError(code::kTargetEngineUnsupported,
                              qualified + where + " uses " + engine +
                                  "; SharedMergeTree targets are not supported yet, because "
                                  "nothing tests their deduplication. Write to a MergeTree, "
                                  "ReplicatedMergeTree, Distributed or Null table.");
    }
    throw NativeSinkError(code::kTargetEngineUnsupported,
                          qualified + where + " uses engine " + engine +
                              ", which the native sink does not write to. It accepts the "
                              "MergeTree and ReplicatedMergeTree families, Distributed and Null.");
}

// --- Effective async_insert and the deduplication window ---------------------

// The MergeTree-level defaults of one server. `replicated` is nullopt on a
// line without system.replicated_merge_tree_settings.
struct ServerDefaults {
    Settings merge_tree;
    std::optional<Settings> replicated;
};

struct DefaultValue {
    std::optional<std::string> value;
    // Which configuration section the value came from, for the remedy.
    bool from_replicated_section{false};
};

// For a Replicated table a row in system.replicated_merge_tree_settings wins.
// That table shows <merge_tree> with <replicated_merge_tree> applied on top,
// so the replicated section supplied the value only where the two differ.
DefaultValue server_default(const ServerDefaults& d, bool replicated, std::string_view name) {
    const auto mt = d.merge_tree.find(name);
    if (replicated && d.replicated) {
        if (const auto it = d.replicated->find(name); it != d.replicated->end()) {
            const bool differs = mt == d.merge_tree.end() || mt->second != it->second;
            return {it->second, differs};
        }
    }
    if (mt != d.merge_tree.end()) {
        return {mt->second, false};
    }
    return {};
}

// One MergeTree-family table to assess, on the server the probe is connected
// to or on one replica behind a Distributed target.
struct LocalTable {
    std::string qualified;  // `db`.`t`
    std::string where;      // "" for the target, " on replica <host> of cluster `c`"
    std::string server;     // "the server" or "<host>", for the remedies
    std::string engine_full;
    EngineFamily family{EngineFamily::MergeTree};
};

struct LocalAssessment {
    std::string async_report;
    std::uint64_t window{0};
    std::string dedup_report;
};

[[noreturn]] void refuse_async(const LocalTable& t,
                               const std::string& why,
                               const std::string& fix) {
    throw NativeSinkError(code::kTargetAsyncInsert, t.qualified + t.where + " " + why + ". " + fix);
}

std::string alter_fix(const LocalTable& t) {
    const std::string alter = "ALTER TABLE " + t.qualified + " MODIFY SETTING async_insert = 0";
    return t.where.empty() ? "Run: " + alter : "Run on " + t.server + ": " + alter;
}

LocalAssessment assess_local(const LocalTable& t, const ServerDefaults& defaults) {
    const bool replicated = t.family == EngineFamily::ReplicatedMergeTree;
    const std::string absent_note = replicated && !defaults.replicated
                                        ? ", " + std::string(kReplicatedTable) + " absent, " +
                                              std::string(kMergeTreeTable) + " used"
                                        : "";
    LocalAssessment out;

    // The table's own setting, if any, decides; otherwise the server default.
    const SettingLookup table = lookup_engine_setting(t.engine_full, "async_insert");
    if (!table.readable) {
        refuse_async(t,
                     "has an engine_full the native sink cannot scan, so it cannot tell whether "
                     "the table takes inserts asynchronously: " +
                         t.engine_full,
                     alter_fix(t));
    }
    if (table.value) {
        const auto on = setting_bool(*table.value);
        if (!on) {
            refuse_async(t,
                         "has table setting async_insert=" + *table.value +
                             ", which the native sink cannot read as 0 or 1",
                         alter_fix(t));
        }
        if (*on) {
            refuse_async(t,
                         "takes inserts asynchronously (table setting async_insert=" +
                             *table.value + "), " + std::string(kAsyncReason),
                         alter_fix(t));
        }
        out.async_report = "async_insert=0 (table setting)";
    } else {
        const DefaultValue d = server_default(defaults, replicated, "async_insert");
        const std::string section =
            d.from_replicated_section ? "replicated_merge_tree" : "merge_tree";
        const std::string source =
            d.from_replicated_section ? " from " + std::string(kReplicatedTable) : "";
        const std::string config_fix = "Set <" + section + "><async_insert>0</async_insert></" +
                                       section + "> in the configuration of " + t.server + ", or " +
                                       (t.where.empty() ? "run: " : "run on " + t.server + ": ") +
                                       "ALTER TABLE " + t.qualified +
                                       " MODIFY SETTING async_insert = 0";
        if (!d.value) {
            // A line without the row has no MergeTree-level setting to OR in.
            out.async_report =
                "async_insert=0 (table unset, no MergeTree-level async_insert on this server" +
                absent_note + ")";
        } else {
            const auto on = setting_bool(*d.value);
            if (!on) {
                refuse_async(
                    t,
                    "has table async_insert unset and a server default async_insert=" + *d.value +
                        " in <" + section + ">, which the native sink cannot read as 0 or 1",
                    config_fix);
            }
            if (*on) {
                refuse_async(t,
                             "takes inserts asynchronously (table unset, server default "
                             "async_insert=" +
                                 *d.value + " in <" + section + ">), " + std::string(kAsyncReason),
                             config_fix);
            }
            out.async_report =
                "async_insert=0 (table unset, server default 0" + source + absent_note + ")";
        }
    }

    // The deduplication window, for the report and the duplicate accounting.
    // A value that cannot be read counts as no log, the cautious reading.
    const std::string name =
        replicated ? "replicated_deduplication_window" : "non_replicated_deduplication_window";
    const SettingLookup table_window = lookup_engine_setting(t.engine_full, name);
    if (table_window.value) {
        const auto v = setting_uint(*table_window.value);
        out.window = v.value_or(0);
        out.dedup_report =
            v ? name + "=" + std::to_string(*v) + " (table)"
              : name + "=" + *table_window.value + " (table, unreadable, counted as 0)";
    } else {
        const DefaultValue d = server_default(defaults, replicated, name);
        if (!d.value) {
            out.dedup_report = name + "=0 (not listed by this server" + absent_note + ")";
        } else {
            const auto v = setting_uint(*d.value);
            out.window = v.value_or(0);
            out.dedup_report =
                v ? name + "=" + std::to_string(*v) + " (server default" + absent_note + ")"
                  : name + "=" + *d.value + " (server default, unreadable, counted as 0" +
                        absent_note + ")";
        }
    }
    return out;
}

bool is_permission_error(int code) noexcept {
    return code == ch::ACCESS_DENIED || code == ch::DATABASE_ACCESS_DENIED;
}

bool is_unknown_table(const ch::ServerException& e) noexcept {
    return e.GetCode() == ch::UNKNOWN_TABLE;
}

// A cluster read. A permission error means the replicas' settings cannot be
// checked at all, and no retry will change that, so it refuses with the
// grants the check needs. Every other failure, an unreachable replica above
// all, goes through unchanged for the caller's retry loop.
template <typename Read>
ResultSet cluster_read(const SinkOptions& opts,
                       const std::string& qualified,
                       const DistributedTarget& d,
                       Read read) {
    try {
        return read();
    } catch (const ch::ServerException& e) {
        if (!is_permission_error(e.GetCode())) {
            throw;
        }
        const std::string user = quote_identifier(opts.user);
        throw NativeSinkError(
            code::kTargetAsyncInsert,
            qualified + " is a Distributed table over cluster " + quote_identifier(d.cluster) +
                ", and the native sink cannot check whether its replicas take inserts "
                "asynchronously, because the cluster read was denied (code " +
                std::to_string(e.GetCode()) + ": " + e.GetException().display_text +
                "). Grant the sink's user what the check reads: GRANT REMOTE ON *.* TO " + user +
                "; GRANT SELECT ON system.clusters TO " + user +
                "; GRANT SELECT ON system.tables TO " + user +
                "; GRANT SELECT ON system.merge_tree_settings TO " + user +
                "; GRANT SELECT ON system.replicated_merge_tree_settings TO " + user);
    }
}

void probe_distributed(InsertTransport& transport,
                       const SinkOptions& opts,
                       std::chrono::seconds budget,
                       const std::string& qualified,
                       DistributedTarget d,
                       TargetInfo& info) {
    const std::string cluster = quote_identifier(d.cluster);
    const std::string local = qualified_table(d.database, d.table);

    const ResultSet count = cluster_read(opts, qualified, d, [&] {
        return transport.select(MetaQuery::ClusterReplicaCount,
                                select_cluster_replica_count(d.cluster, budget));
    });
    std::uint64_t replicas = 0;
    if (!count.rows.empty()) {
        const auto v = setting_uint(cell(count.rows.front(), 0));
        if (!v) {
            throw NativeSinkError(code::kTargetUnreadable,
                                  "system.clusters gave '" + cell(count.rows.front(), 0) +
                                      "' as the replica count of cluster " + cluster);
        }
        replicas = *v;
    }
    if (replicas == 0) {
        throw NativeSinkError(code::kTargetUnreadable,
                              qualified + " is a Distributed table over cluster " + cluster +
                                  ", which system.clusters on this server does not list, so its "
                                  "replicas cannot be checked");
    }
    d.replicas = static_cast<std::size_t>(replicas);

    const ResultSet tables = cluster_read(opts, qualified, d, [&] {
        return transport.select(MetaQuery::ClusterTables,
                                select_cluster_tables(d.cluster, d.database, d.table, budget));
    });
    if (tables.rows.size() < replicas) {
        std::vector<std::string> present;
        for (const auto& row : tables.rows) {
            present.push_back(cell(row, 0));
        }
        throw NativeSinkError(
            code::kTargetMissing,
            local + ", the local table behind Distributed " + qualified + ", exists on " +
                std::to_string(tables.rows.size()) + " of the " + std::to_string(replicas) +
                " replicas of cluster " + cluster +
                (present.empty() ? std::string(" (none)")
                                 : " (present on " + join(present, ", ") + ")") +
                "; create it on every replica first; the sink does not create tables");
    }

    std::vector<LocalTable> locals;
    bool any_replicated = false;
    for (const auto& row : tables.rows) {
        LocalTable t;
        t.qualified = local;
        t.server = cell(row, 0);
        t.where = " on replica " + t.server + " of cluster " + cluster;
        t.engine_full = cell(row, 2);
        t.family = engine_family(cell(row, 1));
        if (t.family == EngineFamily::SharedMergeTree) {
            refuse_engine(local, t.where, cell(row, 1), t.family);
        }
        if (!is_merge_tree_family(t.family)) {
            refuse_async(t,
                         "uses engine " + cell(row, 1) +
                             ", so the native sink cannot check whether it takes inserts "
                             "asynchronously",
                         "Point the Distributed table at MergeTree or ReplicatedMergeTree "
                         "family local tables");
        }
        any_replicated = any_replicated || t.family == EngineFamily::ReplicatedMergeTree;
        locals.push_back(std::move(t));
    }

    const auto merge_tree = per_host(cluster_read(opts, qualified, d, [&] {
        return transport.select(MetaQuery::ClusterMergeTreeSettings,
                                select_cluster_merge_tree_settings(d.cluster, budget));
    }));
    std::optional<std::map<std::string, Settings, std::less<>>> replicated;
    if (any_replicated) {
        try {
            replicated = per_host(cluster_read(opts, qualified, d, [&] {
                return transport.select(
                    MetaQuery::ClusterReplicatedMergeTreeSettings,
                    select_cluster_replicated_merge_tree_settings(d.cluster, budget));
            }));
        } catch (const ch::ServerException& e) {
            // A line without the table: fall back to merge_tree_settings, and
            // the reports say so.
            if (!is_unknown_table(e)) {
                throw;
            }
        }
    }

    std::vector<std::string> reports;
    std::optional<LocalAssessment> first;
    for (const LocalTable& t : locals) {
        ServerDefaults defaults;
        if (const auto it = merge_tree.find(t.server); it != merge_tree.end()) {
            defaults.merge_tree = it->second;
        }
        if (replicated) {
            const auto it = replicated->find(t.server);
            defaults.replicated = it != replicated->end() ? it->second : Settings{};
        }
        LocalAssessment a = assess_local(t, defaults);
        reports.push_back(t.server + ": " + a.async_report);
        if (!first) {
            first = std::move(a);
        }
    }

    info.async_report =
        "async_insert=0 on every replica of cluster " + cluster + " (" + join(reports, "; ") + ")";
    // The initiator does not deduplicate, and passing the token through to
    // the shards is untested, so a Distributed target counts as keeping no
    // log whatever its local tables keep.
    info.dedup_window = first->window;
    info.keeps_dedup_log = false;
    info.dedup_report = first->dedup_report + " on " + locals.front().server +
                        "'s local table; treated as no log, since the Distributed table does "
                        "not deduplicate";
    info.distributed = std::move(d);
}

void probe_local(InsertTransport& transport,
                 std::chrono::seconds budget,
                 const std::string& qualified,
                 const std::string& engine_full,
                 TargetInfo& info) {
    ServerDefaults defaults;
    defaults.merge_tree = name_values(
        transport.select(MetaQuery::MergeTreeSettings, select_merge_tree_settings(budget)));
    if (info.family == EngineFamily::ReplicatedMergeTree) {
        try {
            defaults.replicated =
                name_values(transport.select(MetaQuery::ReplicatedMergeTreeSettings,
                                             select_replicated_merge_tree_settings(budget)));
        } catch (const ch::ServerException& e) {
            if (!is_unknown_table(e)) {
                throw;
            }
        }
    }
    LocalTable t;
    t.qualified = qualified;
    t.server = "the server";
    t.engine_full = engine_full;
    t.family = info.family;
    LocalAssessment a = assess_local(t, defaults);
    info.async_report = std::move(a.async_report);
    info.dedup_window = a.window;
    info.keeps_dedup_log = a.window > 0;
    info.dedup_report = std::move(a.dedup_report);
}

}  // namespace

TargetInfo probe_target(InsertTransport& transport, const SinkOptions& opts) {
    const std::chrono::seconds budget = metadata_budget(opts.receive_timeout);
    TargetInfo info;
    info.server = transport.server();
    check_server_settings(transport, budget, info);

    const std::string qualified = qualified_table(opts.database, opts.table);
    const ResultSet table =
        transport.select(MetaQuery::Table, select_table(opts.database, opts.table, budget));
    if (table.rows.empty()) {
        throw NativeSinkError(code::kTargetMissing,
                              qualified + " does not exist on " + version_text(info.server) +
                                  " at " + endpoint_text(info.server.endpoint) +
                                  "; create it first; the sink does not create tables");
    }
    info.engine = cell(table.rows.front(), 0);
    const std::string engine_full = cell(table.rows.front(), 1);
    info.family = engine_family(info.engine);
    if (info.family == EngineFamily::SharedMergeTree || info.family == EngineFamily::Other) {
        refuse_engine(qualified, "", info.engine, info.family);
    }
    std::optional<DistributedTarget> distributed;
    if (info.family == EngineFamily::Distributed) {
        distributed = parse_distributed(engine_full);
        if (!distributed) {
            throw NativeSinkError(code::kTargetUnreadable,
                                  qualified +
                                      " is a Distributed table whose engine arguments the native "
                                      "sink cannot read; it needs the cluster, a named database "
                                      "and the table, each as a string or an identifier: " +
                                      engine_full);
        }
    }

    info.columns = read_columns(transport, opts, budget, qualified);

    switch (info.family) {
        case EngineFamily::Null:
            // No MergeTree settings: the query's own async_insert=0 decides,
            // and nothing is kept to deduplicate against.
            info.async_report = "async_insert=0 (Null table, the query's own setting)";
            info.dedup_report = "none (Null table)";
            break;
        case EngineFamily::Distributed:
            probe_distributed(transport, opts, budget, qualified, std::move(*distributed), info);
            break;
        default:
            probe_local(transport, budget, qualified, engine_full, info);
            break;
    }
    return info;
}

std::optional<std::string> engine_full_setting(std::string_view engine_full,
                                               std::string_view name) {
    SettingLookup found = lookup_engine_setting(engine_full, name);
    if (!found.readable) {
        return std::nullopt;
    }
    return std::move(found.value);
}

std::optional<DistributedTarget> parse_distributed(std::string_view engine_full) {
    constexpr std::string_view kName = "Distributed";
    std::string_view s = trim(engine_full);
    if (!s.starts_with(kName)) {
        return std::nullopt;
    }
    s = trim(s.substr(kName.size()));
    if (s.empty() || s.front() != '(') {
        return std::nullopt;
    }
    std::optional<std::size_t> close;
    const bool ok = walk(s, [&](std::size_t i, int depth) {
        if (!close && i > 0 && depth == 0 && s[i] == ')') {
            close = i;
        }
    });
    if (!ok || !close) {
        return std::nullopt;
    }
    const auto args = split_top_level(s.substr(1, *close - 1), ',');
    if (!args || args->size() < 3) {
        return std::nullopt;
    }
    auto cluster = literal_or_identifier((*args)[0]);
    auto database = literal_or_identifier((*args)[1]);
    auto table = literal_or_identifier((*args)[2]);
    if (!cluster || !database || !table || cluster->empty() || database->empty() ||
        table->empty()) {
        return std::nullopt;
    }
    DistributedTarget out;
    out.cluster = std::move(*cluster);
    out.database = std::move(*database);
    out.table = std::move(*table);
    return out;
}

EngineFamily engine_family(std::string_view engine) {
    if (engine == "Distributed") {
        return EngineFamily::Distributed;
    }
    if (engine == "Null") {
        return EngineFamily::Null;
    }
    constexpr std::string_view kSuffix = "MergeTree";
    if (!engine.ends_with(kSuffix)) {
        return EngineFamily::Other;
    }
    // The variant (Replacing, Summing, ...) sits between the prefix and the
    // suffix; only the prefix decides the family.
    const std::string_view stem = engine.substr(0, engine.size() - kSuffix.size());
    if (stem.starts_with("Shared")) {
        return EngineFamily::SharedMergeTree;
    }
    if (stem.starts_with("Replicated")) {
        return EngineFamily::ReplicatedMergeTree;
    }
    return EngineFamily::MergeTree;
}

bool is_tested_line(std::uint64_t major, std::uint64_t minor) noexcept {
    return major == 26 && (minor == 3 || minor == 8);
}

}  // namespace clink::clickhouse::native
