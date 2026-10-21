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
    throw Neo4jSinkError(code, std::string(kPrefix) + message);
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
void parse_endpoint(const Value& v, SinkOptions& o) {
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

}  // namespace

std::string SinkOptions::query_path() const {
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
    std::map<std::string, Value> values;
    for (const auto& [key, raw] : params) {
        if (key == "sql_column_types" || listed(planner_keys(), key)) {
            continue;
        }
        if (key == "json_non_finite") {
            refuse(code::kOptionInvalid,
                   "json_non_finite does not apply: the sink writes typed values, and NaN and the "
                   "infinities are stored as Neo4j floats. Remove the option");
        }
        if (!listed(option_names(), key)) {
            std::string accepted;
            for (const auto& n : option_names()) {
                accepted += (accepted.empty() ? "" : ", ") + n;
            }
            refuse(code::kOptionInvalid,
                   "unknown option '" + key + "'; the sink accepts " + accepted);
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
    const auto get = [&](const char* key) -> const Value* {
        const auto it = values.find(key);
        return it == values.end() ? nullptr : &it->second;
    };

    // What would change the guarantee is refused here as well as in the
    // planner: a job built directly on the Dag never passes through it.
    if (const auto* g = get("delivery_guarantee"); g != nullptr && g->resolved != "at_least_once") {
        refuse(code::kGuaranteeUnsupported,
               shown("delivery_guarantee", *g) +
                   " is not offered: the sink delivers at least once. A transaction that commits "
                   "with its reply lost cannot be told from one that did not, and a Query API "
                   "transaction cannot outlive the worker that opened it, so neither exactly-once "
                   "nor effectively-once is available. Use a statement that is safe to replay "
                   "(see the connector page)");
    }
    if (const auto* m = get("mode"); m != nullptr && m->resolved != "append") {
        refuse(code::kGuaranteeUnsupported,
               shown("mode", *m) +
                   " is not offered: the statement decides how a row changes the graph, so "
                   "write a MERGE on a stable key instead of mode='upsert'");
    }
    if (const auto* c = get("changelog"); c != nullptr && c->resolved != "false") {
        refuse(code::kGuaranteeUnsupported,
               shown("changelog", *c) +
                   " is not offered: rows reach the statement without their change kind, so a "
                   "retraction would be written as a new row");
    }
    if (const auto* w = get("write_mode"); w != nullptr && w->resolved != "append") {
        refuse(code::kGuaranteeUnsupported,
               shown("write_mode", *w) + " is not offered: the sink only appends batches");
    }

    const auto* endpoint = get("endpoint");
    if (endpoint == nullptr || endpoint->resolved.empty()) {
        refuse(code::kOptionInvalid,
               "'endpoint' is required (http://host:7474 or https://host:7473)");
    }
    parse_endpoint(*endpoint, o);
    if (o.https && !clink::http_connector::HttpRequest::tls_supported()) {
        refuse(code::kTlsUnavailable,
               "endpoint is https:// but this build of the sink has no TLS support");
    }
    if (const auto* d = get("database")) {
        if (!valid_database(d->resolved)) {
            refuse(code::kOptionInvalid,
                   shown("database", *d) +
                       " is not a Neo4j database name (letters, digits, '.', '-' and '_', starting "
                       "with a letter or digit, at most 63 characters)");
        }
        o.database = d->resolved;
    }
    const auto* statement = get("statement");
    if (statement == nullptr ||
        statement->resolved.find_first_not_of(" \t\r\n") == std::string::npos) {
        refuse(code::kOptionInvalid,
               "'statement' is required: the Cypher to run for each batch, reading its rows "
               "from $rows (for example UNWIND $rows AS row MERGE ...)");
    }
    o.statement = statement->resolved;

    const auto* user = get("username");
    const auto* pass = get("password");
    const auto* bearer = get("bearer_token");
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

    if (const auto* ca = get("tls_ca_file")) {
        if (!o.https) {
            refuse(code::kOptionInvalid, "tls_ca_file is set but the endpoint is not https://");
        }
        std::error_code ec;
        if (!std::filesystem::is_regular_file(ca->resolved, ec)) {
            refuse(code::kOptionInvalid, shown("tls_ca_file", *ca) + " is not a readable file");
        }
        o.tls_ca_file = ca->resolved;
    }

    if (const auto* v = get("batch_size")) {
        o.batch_size = parse_uint("batch_size", *v, 1, 100'000);
    }
    if (const auto* v = get("batch_bytes")) {
        o.batch_bytes = parse_uint("batch_bytes", *v, 1024, 256ULL << 20);
    }
    if (const auto* v = get("flush_interval_ms")) {
        o.flush_interval_ms = parse_uint("flush_interval_ms", *v, 0, 3'600'000);
    }
    if (const auto* v = get("connect_timeout_ms")) {
        o.connect_timeout_ms = parse_uint("connect_timeout_ms", *v, 1, 600'000);
    }
    if (const auto* v = get("request_timeout_ms")) {
        o.request_timeout_ms = parse_uint("request_timeout_ms", *v, 1, 3'600'000);
    }
    if (const auto* v = get("retry_window_ms")) {
        o.retry_window_ms = parse_uint("retry_window_ms", *v, 0, 86'400'000);
    }
    if (const auto* v = get("max_response_bytes")) {
        o.max_response_bytes = parse_uint("max_response_bytes", *v, 1024, 256ULL << 20);
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

}  // namespace clink::neo4j
