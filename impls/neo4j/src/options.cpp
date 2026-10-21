#include "options.hpp"

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <filesystem>
#include <string_view>
#include <system_error>

#include "clink/http_connector/http_request.hpp"

namespace clink::neo4j {

namespace {

constexpr std::string_view kPrefix = "neo4j sink: ";
constexpr std::string_view kEnvPrefix = "env://";

[[noreturn]] void refuse(const char* code, const std::string& message) {
    throw Neo4jError(code, std::string(kPrefix) + message);
}

// A value as the job gave it and as the sink reads it; they differ only for an
// env:// reference.
struct Value {
    std::string raw;
    std::string resolved;
    [[nodiscard]] bool from_env() const { return raw.starts_with(kEnvPrefix); }
};

// How a refusal quotes a value. One read from the environment is named by its
// reference alone: env:// keeps secrets out of job specs, and a refusal goes
// back to whoever submitted the job.
std::string shown(const std::string& key, const Value& v) {
    if (v.from_env()) {
        return key + " (the value of " + v.raw + ")";
    }
    return key + "='" + v.resolved + "'";
}

// The keys the SQL planner, the materialised-view code and the catalog put on
// a sink op whatever its connector, read off the code that sets them (the same
// set the native ClickHouse sink accepts). They are accepted and ignored.
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

std::uint64_t parse_uint(const std::string& key,
                         const Value& v,
                         std::uint64_t min,
                         std::uint64_t max) {
    std::uint64_t out = 0;
    const auto& s = v.resolved;
    const auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    if (s.empty() || ec != std::errc{} || ptr != s.data() + s.size() || out < min || out > max) {
        refuse(code::kOptionInvalid,
               shown(key, v) + " is not a whole number from " + std::to_string(min) + " to " +
                   std::to_string(max));
    }
    return out;
}

// Neo4j database names: ASCII letters, digits, dots, dashes and underscores,
// starting with a letter or digit, at most 63 characters. Percent-encoded in
// the path all the same.
bool valid_database(const std::string& name) {
    if (name.empty() || name.size() > 63 ||
        std::isalnum(static_cast<unsigned char>(name[0])) == 0) {
        return false;
    }
    return std::all_of(name.begin(), name.end(), [](char c) {
        return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '.' || c == '-' || c == '_';
    });
}

std::string percent_encode(const std::string& s) {
    static constexpr char kHex[] = "0123456789ABCDEF";
    std::string out;
    for (const char ch : s) {
        const auto c = static_cast<unsigned char>(ch);
        if (std::isalnum(c) != 0 || c == '-' || c == '.' || c == '_' || c == '~') {
            out += ch;
        } else {
            out += '%';
            out += kHex[c >> 4];
            out += kHex[c & 0x0F];
        }
    }
    return out;
}

// scheme://host[:port], with nothing after the authority but an optional "/".
void parse_endpoint(const Value& v, ConnectionOptions& o) {
    std::string e = v.resolved;
    if (e.ends_with('/')) {
        e.pop_back();
    }
    std::string_view rest;
    if (e.starts_with("http://")) {
        rest = std::string_view(e).substr(7);
    } else if (e.starts_with("https://")) {
        rest = std::string_view(e).substr(8);
        o.https = true;
    } else {
        refuse(code::kOptionInvalid, shown("endpoint", v) + " must start with http:// or https://");
    }
    if (rest.empty()) {
        refuse(code::kOptionInvalid, shown("endpoint", v) + " names no host");
    }
    if (rest.find('@') != std::string_view::npos) {
        refuse(code::kOptionInvalid,
               "endpoint must not carry credentials; use username and password (or bearer_token)");
    }
    if (rest.find_first_of("/?#") != std::string_view::npos) {
        refuse(code::kOptionInvalid,
               shown("endpoint", v) +
                   " must be scheme://host[:port] with no path, query or fragment; the sink "
                   "adds /db/<database>/query/v2 itself");
    }
    if (const auto colon = rest.rfind(':');
        colon != std::string_view::npos && rest.find(']') == std::string_view::npos) {
        const auto port = rest.substr(colon + 1);
        unsigned value = 0;
        const auto [ptr, ec] = std::from_chars(port.data(), port.data() + port.size(), value);
        if (port.empty() || ec != std::errc{} || ptr != port.data() + port.size() || value == 0 ||
            value > 65535) {
            refuse(code::kOptionInvalid, shown("endpoint", v) + " has an invalid port");
        }
    }
    o.endpoint = e;
}

using ValueMap = std::map<std::string, Value>;

// Reads every param `accept` admits into a map, resolving env:// references.
// `strip` is a prefix taken off a key before it is looked up (the lookup's
// options arrive as "neo4j.<key>"); a param `skip` matches is passed over.
template <typename Accept, typename Skip>
ValueMap read_values(const std::map<std::string, std::string>& params,
                     const std::string& strip,
                     const std::vector<std::string>& names,
                     const char* who,
                     Accept&& accept,
                     Skip&& skip) {
    ValueMap values;
    for (const auto& [raw_key, raw] : params) {
        if (skip(raw_key)) {
            continue;
        }
        std::string key = raw_key;
        if (!strip.empty()) {
            if (!key.starts_with(strip)) {
                refuse(code::kOptionInvalid,
                       "unexpected parameter '" + raw_key + "' on the " + who);
            }
            key = key.substr(strip.size());
        }
        if (!accept(key)) {
            std::string accepted;
            for (const auto& n : names) {
                accepted += (accepted.empty() ? "" : ", ") + n;
            }
            refuse(code::kOptionInvalid,
                   "unknown option '" + key + "'; the " + who + " accepts " + accepted);
        }
        Value v{raw, raw};
        if (v.from_env()) {
            const std::string var = raw.substr(kEnvPrefix.size());
            const char* got = var.empty() ? nullptr : std::getenv(var.c_str());
            if (got == nullptr || *got == '\0') {
                refuse(code::kSecretUnset,
                       key + " is " + raw + ", and that variable is unset or empty on this worker");
            }
            v.resolved = got;
        }
        values.emplace(key, std::move(v));
    }
    return values;
}

const Value* get(const ValueMap& values, const char* key) {
    const auto it = values.find(key);
    return it == values.end() ? nullptr : &it->second;
}

// The options the sink and the lookup share, by the same rules. `who` names
// the reader in messages; `o` carries the defaults on entry.
void parse_connection(const ValueMap& values, ConnectionOptions& o, const char* who) {
    const auto* endpoint = get(values, "endpoint");
    if (endpoint == nullptr || endpoint->resolved.empty()) {
        refuse(code::kOptionInvalid,
               "'endpoint' is required (http://host:7474 or https://host:7473)");
    }
    parse_endpoint(*endpoint, o);
    if (o.https && !clink::http_connector::HttpRequest::tls_supported()) {
        refuse(code::kTlsUnavailable,
               std::string("endpoint is https:// but this build of the ") + who +
                   " has no TLS support");
    }
    if (const auto* d = get(values, "database")) {
        if (!valid_database(d->resolved)) {
            refuse(code::kOptionInvalid,
                   shown("database", *d) +
                       " is not a Neo4j database name (letters, digits, '.', '-' and '_', starting "
                       "with a letter or digit, at most 63 characters)");
        }
        o.database = d->resolved;
    }
    const auto* user = get(values, "username");
    const auto* pass = get(values, "password");
    const auto* bearer = get(values, "bearer_token");
    if ((user != nullptr) != (pass != nullptr)) {
        refuse(code::kOptionInvalid, "username and password must be given together");
    }
    if (bearer != nullptr && user != nullptr) {
        refuse(code::kOptionInvalid,
               "give either username and password, or bearer_token, not both");
    }
    if (user != nullptr) {
        if (user->resolved.empty()) {
            refuse(code::kOptionInvalid, "username is empty");
        }
        o.username = user->resolved;
        o.password = pass->resolved;
    }
    if (bearer != nullptr) {
        if (bearer->resolved.empty()) {
            refuse(code::kOptionInvalid, "bearer_token is empty");
        }
        o.bearer_token = bearer->resolved;
    }
    if (const auto* ca = get(values, "tls_ca_file")) {
        if (!o.https) {
            refuse(code::kOptionInvalid, "tls_ca_file is set but the endpoint is not https://");
        }
        std::error_code ec;
        if (!std::filesystem::is_regular_file(ca->resolved, ec)) {
            refuse(code::kOptionInvalid, shown("tls_ca_file", *ca) + " is not a readable file");
        }
        o.tls_ca_file = ca->resolved;
    }
    if (const auto* v = get(values, "connect_timeout_ms")) {
        o.connect_timeout_ms = parse_uint("connect_timeout_ms", *v, 1, 600'000);
    }
    if (const auto* v = get(values, "request_timeout_ms")) {
        o.request_timeout_ms = parse_uint("request_timeout_ms", *v, 1, 3'600'000);
    }
    if (const auto* v = get(values, "retry_window_ms")) {
        o.retry_window_ms = parse_uint("retry_window_ms", *v, 0, 86'400'000);
    }
    if (const auto* v = get(values, "max_response_bytes")) {
        o.max_response_bytes = parse_uint("max_response_bytes", *v, 1024, 256ULL << 20);
    }
}

std::string required_statement(const ValueMap& values, const char* hint) {
    const auto* statement = get(values, "statement");
    if (statement == nullptr ||
        statement->resolved.find_first_not_of(" \t\r\n") == std::string::npos) {
        refuse(code::kOptionInvalid, std::string("'statement' is required: ") + hint);
    }
    return statement->resolved;
}

std::vector<std::string> split_csv(const std::string& csv) {
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

const Column* find_column(const std::vector<Column>& cols, const std::string& name) {
    for (const auto& c : cols) {
        if (c.name == name) {
            return &c;
        }
    }
    return nullptr;
}

bool is_integer_kind(Kind k) {
    return k == Kind::TinyInt || k == Kind::SmallInt || k == Kind::Integer || k == Kind::BigInt;
}

int integer_rank(Kind k) {
    switch (k) {
        case Kind::TinyInt:
            return 1;
        case Kind::SmallInt:
            return 2;
        case Kind::Integer:
            return 3;
        default:
            return 4;
    }
}

// The result types a lookup decodes in this release: scalars only.
bool is_result_kind(Kind k) {
    return is_integer_kind(k) || k == Kind::Double || k == Kind::Boolean || k == Kind::Varchar;
}

}  // namespace

std::string ConnectionOptions::query_path() const {
    return "/db/" + percent_encode(database) + "/query/v2";
}

const std::vector<std::string>& option_names() {
    static const std::vector<std::string> names = {
        "endpoint",
        "database",
        "statement",
        "username",
        "password",
        "bearer_token",
        "tls_ca_file",
        "batch_size",
        "batch_bytes",
        "flush_interval_ms",
        "connect_timeout_ms",
        "request_timeout_ms",
        "retry_window_ms",
        "max_response_bytes",
        "delivery_guarantee",
        "mode",
        "changelog",
        "write_mode",
    };
    return names;
}

SinkOptions parse_options(const std::map<std::string, std::string>& params,
                          std::uint32_t subtask,
                          std::uint32_t parallelism) {
    SinkOptions o;
    o.subtask = subtask;
    o.parallelism = parallelism;
    for (const auto& [key, raw] : params) {
        if (key == "json_non_finite") {
            refuse(code::kOptionInvalid,
                   "json_non_finite does not apply: the sink writes typed values, and NaN and the "
                   "infinities are stored as Neo4j floats. Remove the option");
        }
    }
    const ValueMap values = read_values(
        params,
        "",
        option_names(),
        "sink",
        [](const std::string& key) { return listed(option_names(), key); },
        [](const std::string& key) {
            return key == "sql_column_types" || listed(planner_keys(), key);
        });

    // What would change the guarantee is refused here as well as in the
    // planner: a job built directly on the Dag never passes through it.
    if (const auto* g = get(values, "delivery_guarantee");
        g != nullptr && g->resolved != "at_least_once") {
        refuse(code::kGuaranteeUnsupported,
               shown("delivery_guarantee", *g) +
                   " is not offered: the sink delivers at least once. A transaction that commits "
                   "with its reply lost cannot be told from one that did not, and a Query API "
                   "transaction cannot outlive the worker that opened it, so neither exactly-once "
                   "nor effectively-once is available. Use a statement that is safe to replay "
                   "(see the connector page)");
    }
    if (const auto* m = get(values, "mode"); m != nullptr && m->resolved != "append") {
        refuse(code::kGuaranteeUnsupported,
               shown("mode", *m) +
                   " is not offered: the statement decides how a row changes the graph, so "
                   "write a MERGE on a stable key instead of mode='upsert'");
    }
    if (const auto* c = get(values, "changelog"); c != nullptr && c->resolved != "false") {
        refuse(code::kGuaranteeUnsupported,
               shown("changelog", *c) +
                   " is not offered: rows reach the statement without their change kind, so a "
                   "retraction would be written as a new row");
    }
    if (const auto* w = get(values, "write_mode"); w != nullptr && w->resolved != "append") {
        refuse(code::kGuaranteeUnsupported,
               shown("write_mode", *w) + " is not offered: the sink only appends batches");
    }

    parse_connection(values, o, "sink");
    o.statement = required_statement(
        values,
        "the Cypher to run for each batch, reading its rows from $rows (for example UNWIND $rows "
        "AS row MERGE ...)");

    if (const auto* v = get(values, "batch_size")) {
        o.batch_size = parse_uint("batch_size", *v, 1, 100'000);
    }
    if (const auto* v = get(values, "batch_bytes")) {
        o.batch_bytes = parse_uint("batch_bytes", *v, 1024, 256ULL << 20);
    }
    if (const auto* v = get(values, "flush_interval_ms")) {
        o.flush_interval_ms = parse_uint("flush_interval_ms", *v, 0, 3'600'000);
    }

    const auto types = params.find("sql_column_types");
    if (types == params.end() || types->second.empty()) {
        refuse(code::kOptionInvalid,
               "the sink needs the table's declared columns (sql_column_types), which the SQL "
               "planner supplies; declare the table's columns");
    }
    o.columns = parse_column_types(types->second);
    if (const auto problems = unsupported_columns(o.columns); !problems.empty()) {
        std::string all;
        for (const auto& p : problems) {
            all += (all.empty() ? "" : "; ") + p;
        }
        refuse(code::kTypeUnsupported, all);
    }
    return o;
}

const std::vector<std::string>& lookup_option_names() {
    static const std::vector<std::string> names = {
        "endpoint",
        "database",
        "statement",
        "username",
        "password",
        "bearer_token",
        "tls_ca_file",
        "connect_timeout_ms",
        "request_timeout_ms",
        "retry_window_ms",
        "max_retries",
        "server_timeout_s",
        "max_response_bytes",
        "max_in_flight",
        "max_concurrent_requests",
        "matched_column",
        "observed_at_column",
        "bookmark_column",
    };
    return names;
}

Neo4jError relabelled(const Neo4jError& e, std::string_view prefix) {
    std::string message = e.what();
    if (message.starts_with(kPrefix)) {
        message.replace(0, kPrefix.size(), prefix);
    }
    return Neo4jError(e.code(), message);
}

namespace {

LookupOptions parse_lookup(const std::map<std::string, std::string>& params,
                           std::uint32_t subtask,
                           std::uint32_t parallelism) {
    // The join, described by the planner; required, and never user-written.
    static const std::vector<std::string> kJoinKeys = {
        "function_name",
        "probe_alias",
        "dim_alias",
        "probe_columns",
        "dim_columns",
        "join_type",
        "probe_key_column",
        "probe_key_type",
        "dim_key_column",
        "dim_column_types",
    };
    LookupOptions o;
    o.subtask = subtask;
    o.parallelism = parallelism;
    // A lookup holds up its record until the reply, so its defaults are
    // tighter than the sink's.
    o.connect_timeout_ms = 2000;
    o.request_timeout_ms = 5000;
    o.retry_window_ms = 30000;
    o.max_response_bytes = 64ULL << 10;

    const ValueMap values = read_values(
        params,
        "neo4j.",
        lookup_option_names(),
        "lookup",
        [](const std::string& key) { return listed(lookup_option_names(), key); },
        [](const std::string& key) { return listed(kJoinKeys, key); });
    const auto join = [&](const char* key) -> std::string {
        const auto it = params.find(key);
        if (it == params.end() || it->second.empty()) {
            refuse(code::kOptionInvalid,
                   std::string("the lookup join needs '") + key +
                       "', which the SQL planner supplies; declare the lookup table with "
                       "connector='lookup' and function='neo4j' and JOIN it on one equality");
        }
        return it->second;
    };

    parse_connection(values, o, "lookup");
    o.statement = required_statement(
        values,
        "the read-only Cypher to run for each lookup, taking the probe's key as $key and "
        "returning the lookup table's columns (for example MATCH (a:Account {id: $key}) RETURN "
        "a.name AS name)");

    o.probe_alias = join("probe_alias");
    o.dim_alias = join("dim_alias");
    o.probe_columns = split_csv(join("probe_columns"));
    o.probe_key_column = join("probe_key_column");
    o.dim_key_column = join("dim_key_column");
    const std::string join_type = join("join_type");
    if (join_type != "inner" && join_type != "left_outer") {
        refuse(code::kOptionInvalid,
               "join_type='" + join_type + "' is not a lookup join; INNER and LEFT are supported");
    }
    o.outer = join_type == "left_outer";
    o.dim_columns = parse_column_types(join("dim_column_types"));
    o.key_type = parse_column_types("key:" + join("probe_key_type")).front().type;

    if (const auto* v = get(values, "max_retries")) {
        o.max_retries = parse_uint("max_retries", *v, 0, 100);
    }
    if (const auto* v = get(values, "server_timeout_s")) {
        o.server_timeout_s = parse_uint("server_timeout_s", *v, 1, 3600);
        if (o.server_timeout_s * 1000 >= o.request_timeout_ms) {
            refuse(code::kOptionInvalid,
                   "server_timeout_s=" + std::to_string(o.server_timeout_s) +
                       " is not shorter than request_timeout_ms=" +
                       std::to_string(o.request_timeout_ms) +
                       ", so the client would give up before the server's limit acts; lower "
                       "it or raise request_timeout_ms");
        }
    }
    if (const auto* v = get(values, "max_in_flight")) {
        o.max_in_flight = parse_uint("max_in_flight", *v, 1, 10'000);
    }
    if (const auto* v = get(values, "max_concurrent_requests")) {
        o.max_concurrent_requests = parse_uint("max_concurrent_requests", *v, 1, 64);
    }

    // The key: one integer or text value on each side of the ON equality.
    const auto key_kind_ok = [](Kind k) { return is_integer_kind(k) || k == Kind::Varchar; };
    if (!key_kind_ok(o.key_type.kind)) {
        refuse(code::kTypeUnsupported,
               "the probe key column '" + o.probe_key_column + "' is " + o.key_type.spelling +
                   "; a Neo4j lookup key is TINYINT, SMALLINT, INTEGER, BIGINT or VARCHAR");
    }
    const Column* key_col = find_column(o.dim_columns, o.dim_key_column);
    if (key_col == nullptr) {
        refuse(code::kOptionInvalid,
               "the lookup key column '" + o.dim_key_column + "' is not a declared column");
    }
    const bool same_family =
        (is_integer_kind(o.key_type.kind) && is_integer_kind(key_col->type.kind)) ||
        (o.key_type.kind == Kind::Varchar && key_col->type.kind == Kind::Varchar);
    if (!same_family) {
        refuse(code::kTypeUnsupported,
               "the ON equality compares the probe's " + o.key_type.spelling + " column '" +
                   o.probe_key_column + "' with the lookup's " + key_col->type.spelling +
                   " column '" + o.dim_key_column +
                   "'; declare both as integers or both as VARCHAR");
    }
    if (is_integer_kind(key_col->type.kind) &&
        integer_rank(key_col->type.kind) < integer_rank(o.key_type.kind)) {
        refuse(code::kTypeUnsupported,
               "the lookup key column '" + o.dim_key_column + "' is " + key_col->type.spelling +
                   ", narrower than the probe's " + o.key_type.spelling +
                   " key, so a matched key might not fit it; declare it " + o.key_type.spelling +
                   " or wider");
    }

    // Metadata columns: declared, of their one type, and none of them the key
    // or another metadata column.
    const auto metadata =
        [&](const char* option, Kind kind, const char* type_name, std::string& into) {
            const auto* v = get(values, option);
            if (v == nullptr) {
                return;
            }
            const Column* c = find_column(o.dim_columns, v->resolved);
            if (c == nullptr) {
                refuse(code::kOptionInvalid, shown(option, *v) + " names no declared column");
            }
            if (c->type.kind != kind || (kind == Kind::Timestamp && c->type.with_time_zone)) {
                refuse(code::kTypeUnsupported,
                       shown(option, *v) + " must be declared " + type_name + ", not " +
                           c->type.spelling);
            }
            if (c->name == o.dim_key_column || c->name == o.matched_column ||
                c->name == o.observed_at_column || c->name == o.bookmark_column) {
                refuse(code::kOptionInvalid,
                       shown(option, *v) + " is already the key or another metadata column");
            }
            into = c->name;
        };
    metadata("matched_column", Kind::Boolean, "BOOLEAN", o.matched_column);
    metadata("observed_at_column", Kind::Timestamp, "TIMESTAMP", o.observed_at_column);
    metadata("bookmark_column", Kind::Varchar, "VARCHAR", o.bookmark_column);

    // What the statement returns: every other declared column, scalars only.
    std::vector<std::string> problems;
    for (const auto& c : o.dim_columns) {
        if (c.name == o.dim_key_column || c.name == o.matched_column ||
            c.name == o.observed_at_column || c.name == o.bookmark_column) {
            continue;
        }
        if (c.type.kind == Kind::Real) {
            problems.push_back("column '" + c.name +
                               "' (REAL): Neo4j floats are 64-bit, and REAL would round them; "
                               "declare DOUBLE");
            continue;
        }
        if (!is_result_kind(c.type.kind)) {
            problems.push_back("column '" + c.name + "' (" + c.type.spelling +
                               "): a lookup returns scalars in this release (TINYINT, SMALLINT, "
                               "INTEGER, BIGINT, DOUBLE, BOOLEAN or VARCHAR); return a node's "
                               "properties, not the node, and join a list into text");
            continue;
        }
        o.result_columns.push_back(c);
    }
    if (!problems.empty()) {
        std::string all;
        for (const auto& p : problems) {
            all += (all.empty() ? "" : "; ") + p;
        }
        refuse(code::kTypeUnsupported, all);
    }
    if (o.result_columns.empty()) {
        refuse(code::kOptionInvalid,
               "the lookup table declares no column for the statement to return besides the key "
               "and the metadata columns; declare at least one");
    }
    return o;
}

}  // namespace

LookupOptions parse_lookup_options(const std::map<std::string, std::string>& params,
                                   std::uint32_t subtask,
                                   std::uint32_t parallelism) {
    try {
        return parse_lookup(params, subtask, parallelism);
    } catch (const Neo4jError& e) {
        throw relabelled(e, "neo4j lookup: ");
    }
}

}  // namespace clink::neo4j
