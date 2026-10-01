#include "native/sink_options.hpp"

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <functional>
#include <limits>
#include <optional>
#include <string_view>
#include <system_error>
#include <utility>

#include "clink/connectors/capability.hpp"
#include "clink/plugin/plugin.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native {

namespace {

constexpr std::string_view kFactoryPrefix = "clickhouse_native_sink: ";
constexpr std::string_view kEnvPrefix = "env://";
constexpr std::size_t kMaxEndpoints = 16;
constexpr std::uint16_t kDefaultPort = 9000;
constexpr std::uint16_t kDefaultSecurePort = 9440;
constexpr std::uint64_t kUnbounded = std::numeric_limits<std::uint64_t>::max();
constexpr std::uint64_t kMaxBatchRows = (1ULL << 31) - 1;

// Of the tolerated keys, these are read: a value that would change the
// guarantee is refused here as well as in the planner, because Dag-direct
// jobs never pass through the planner.
constexpr std::string_view kGuaranteeKeys[] = {
    "changelog", "delivery_guarantee", "mode", "write_mode"};

[[noreturn]] void refuse(const char* code, const std::string& message) {
    throw NativeSinkError(code, std::string(kFactoryPrefix) + message);
}

// A value as the job gave it and as the sink reads it. The two differ only
// for an env:// reference.
struct Value {
    std::string raw;
    std::string resolved;
    [[nodiscard]] bool from_env() const { return raw.starts_with(kEnvPrefix); }
};

// How a refusal quotes a value. One read from the environment also names its
// reference, because the job spec shows only the reference.
std::string shown(const Value& v) {
    std::string out = "'" + v.resolved + "'";
    if (v.from_env()) {
        out += " (from " + v.raw + ")";
    }
    return out;
}

bool listed(const std::vector<std::string>& keys, std::string_view key) {
    return std::find(keys.begin(), keys.end(), key) != keys.end();
}

bool is_guarantee_key(std::string_view key) {
    return std::find(std::begin(kGuaranteeKeys), std::end(kGuaranteeKeys), key) !=
           std::end(kGuaranteeKeys);
}

// The whole text must be the number. std::from_chars takes no sign, leading
// space or base prefix for an unsigned type, and anything left after the
// digits refuses the value, so "12abc", " 12" and "-1" never read as numbers.
std::optional<std::uint64_t> to_integer(std::string_view text, std::uint64_t lo, std::uint64_t hi) {
    std::uint64_t out = 0;
    const char* first = text.data();
    const char* last = first + text.size();
    const auto [ptr, ec] = std::from_chars(first, last, out);
    if (ec != std::errc{} || ptr != last || out < lo || out > hi) {
        return std::nullopt;
    }
    return out;
}

std::uint64_t parse_integer(std::string_view key,
                            const Value& v,
                            std::uint64_t lo,
                            std::uint64_t hi) {
    if (const auto n = to_integer(v.resolved, lo, hi)) {
        return *n;
    }
    const std::string range = hi == kUnbounded
                                  ? "of at least " + std::to_string(lo)
                                  : "from " + std::to_string(lo) + " to " + std::to_string(hi);
    refuse(code::kOptionInvalid,
           "option '" + std::string(key) + "' must be an integer " + range + "; got " + shown(v));
}

std::chrono::milliseconds parse_millis(std::string_view key,
                                       const Value& v,
                                       std::uint64_t lo,
                                       std::uint64_t hi) {
    return std::chrono::milliseconds(
        static_cast<std::chrono::milliseconds::rep>(parse_integer(key, v, lo, hi)));
}

bool parse_bool(std::string_view key, const Value& v) {
    if (v.resolved == "true") {
        return true;
    }
    if (v.resolved == "false") {
        return false;
    }
    refuse(code::kOptionInvalid,
           "option '" + std::string(key) + "' must be 'true' or 'false'; got " + shown(v));
}

const std::string& non_empty(std::string_view key, const Value& v) {
    if (v.resolved.empty()) {
        refuse(code::kOptionInvalid, "option '" + std::string(key) + "' must not be empty");
    }
    return v.resolved;
}

std::string_view trim(std::string_view s) {
    const auto first = s.find_first_not_of(" \t");
    if (first == std::string_view::npos) {
        return {};
    }
    const auto last = s.find_last_not_of(" \t");
    return s.substr(first, last - first + 1);
}

// "h1:p1,h2:p2". Every entry names its port, so a list reads the same
// whether or not TLS is on. An IPv6 address carries its own colons, so it is
// written in brackets, and an unbracketed host with a colon is refused rather
// than split at a guess.
std::vector<Endpoint> parse_endpoints(const Value& v) {
    if (v.resolved.empty()) {
        refuse(code::kOptionInvalid, "option 'endpoints' must not be empty");
    }
    std::vector<std::string_view> entries;
    std::string_view rest = v.resolved;
    while (true) {
        const auto comma = rest.find(',');
        entries.push_back(rest.substr(0, comma));
        if (comma == std::string_view::npos) {
            break;
        }
        rest.remove_prefix(comma + 1);
    }
    if (entries.size() > kMaxEndpoints) {
        refuse(code::kOptionInvalid,
               "option 'endpoints' lists " + std::to_string(entries.size()) + " servers; at most " +
                   std::to_string(kMaxEndpoints) + " are accepted");
    }

    std::vector<Endpoint> out;
    out.reserve(entries.size());
    for (std::size_t i = 0; i < entries.size(); ++i) {
        const std::string_view entry = trim(entries[i]);
        if (entry.empty()) {
            refuse(code::kOptionInvalid,
                   "option 'endpoints' has an empty entry at position " + std::to_string(i + 1));
        }
        const std::string quoted = "'" + std::string(entry) + "'";
        std::string_view host;
        std::string_view port;
        if (entry.front() == '[') {
            const auto close = entry.find(']');
            if (close == std::string_view::npos || close + 1 >= entry.size() ||
                entry[close + 1] != ':') {
                refuse(code::kOptionInvalid,
                       "option 'endpoints' entry " + quoted + " is not host:port");
            }
            host = entry.substr(1, close - 1);
            port = entry.substr(close + 2);
        } else {
            const auto colon = entry.rfind(':');
            if (colon == std::string_view::npos) {
                refuse(code::kOptionInvalid,
                       "option 'endpoints' entry " + quoted + " is not host:port");
            }
            host = entry.substr(0, colon);
            if (host.find(':') != std::string_view::npos) {
                refuse(code::kOptionInvalid,
                       "option 'endpoints' entry " + quoted +
                           " has more than one ':'; write an IPv6 address in brackets, as "
                           "[addr]:port");
            }
            port = entry.substr(colon + 1);
        }
        if (host.empty()) {
            refuse(code::kOptionInvalid,
                   "option 'endpoints' entry " + quoted + " has an empty host");
        }
        const auto number = to_integer(port, 1, 65535);
        if (!number) {
            refuse(code::kOptionInvalid,
                   "option 'endpoints' entry " + quoted + " must have a port from 1 to 65535");
        }
        out.push_back(Endpoint{std::string(host), static_cast<std::uint16_t>(*number)});
    }
    return out;
}

void check_guarantee(const std::map<std::string, Value, std::less<>>& values) {
    if (const auto it = values.find("mode");
        it != values.end() && it->second.resolved != "append") {
        refuse(code::kDeliveryUnsupported,
               "mode=" + shown(it->second) +
                   " is not supported; this sink appends rows, so mode must be 'append'");
    }
    if (const auto it = values.find("delivery_guarantee"); it != values.end()) {
        // The vocabulary and the ordering belong to the capability contract,
        // so a guarantee added there is judged here without a second list.
        const auto requested = connectors::delivery_from_string(it->second.resolved);
        if (!requested) {
            refuse(code::kOptionInvalid,
                   "delivery_guarantee=" + shown(it->second) + " is not a recognised guarantee");
        }
        if (connectors::strength(*requested) >
            connectors::strength(connectors::DeliveryGuarantee::AtLeastOnce)) {
            refuse(code::kDeliveryUnsupported,
                   "delivery_guarantee=" + shown(it->second) +
                       " is stronger than this sink provides; it delivers at least once");
        }
    }
    if (const auto it = values.find("changelog"); it != values.end()) {
        if (parse_bool("changelog", it->second)) {
            refuse(code::kDeliveryUnsupported,
                   "changelog='true' is not supported; this sink writes inserts only");
        }
    }
    if (const auto it = values.find("write_mode");
        it != values.end() && it->second.resolved == "overwrite") {
        refuse(code::kDeliveryUnsupported,
               "write_mode='overwrite' is not supported; each refresh would append the whole "
               "result again");
    }
}

std::string render_endpoint(const Endpoint& e) {
    const bool v6 = e.host.find(':') != std::string::npos;
    return (v6 ? "[" + e.host + "]" : e.host) + ":" + std::to_string(e.port);
}

std::string_view compression_name(Compression c) {
    switch (c) {
        case Compression::None:
            return "none";
        case Compression::Lz4:
            return "lz4";
        case Compression::Zstd:
            return "zstd";
    }
    return "unknown";
}

}  // namespace

SinkOptions parse_sink_options(const std::map<std::string, std::string>& params,
                               std::uint32_t subtask_idx,
                               std::uint32_t parallelism) {
    const auto& own = own_option_keys();
    const auto& tolerated = pass_through_keys();

    // Keys first, over the whole map, so a misspelt key is reported as itself
    // and not as whatever its absence or its neighbour's value breaks.
    for (const auto& [key, raw] : params) {
        if (listed(own, key) || listed(tolerated, key)) {
            continue;
        }
        std::string accepted;
        for (const auto& k : own) {
            // Internal: the planner supplies it, so naming it would only
            // invite a user to set it by hand.
            if (k == "sql_column_types") {
                continue;
            }
            accepted += (accepted.empty() ? "" : ", ") + k;
        }
        refuse(code::kUnknownOption,
               "unknown option '" + key + "'. Accepted options: " + accepted + ".");
    }

    SinkOptions opts;
    opts.subtask_idx = subtask_idx;
    opts.parallelism = parallelism;

    // Resolve every value the sink reads. A tolerated key the sink ignores is
    // left alone: its value has no effect here, so an unset variable in it is
    // not this sink's failure to report.
    std::map<std::string, Value, std::less<>> values;
    for (const auto& [key, raw] : params) {
        if (!listed(own, key)) {
            opts.passed_through.push_back(key);
            if (!is_guarantee_key(key)) {
                continue;
            }
        }
        Value v{raw, clink::plugin::BuildContext::resolve_secret(raw)};
        if (v.from_env() && v.resolved.empty()) {
            if (raw.size() == kEnvPrefix.size()) {
                refuse(code::kSecretUnset,
                       "option '" + key + "' is 'env://' with no variable name");
            }
            refuse(code::kSecretUnset,
                   "option '" + key + "' names " + raw +
                       ", which is unset in this worker's environment");
        }
        values.emplace(key, std::move(v));
    }
    const auto get = [&values](std::string_view key) -> const Value* {
        const auto it = values.find(key);
        return it == values.end() ? nullptr : &it->second;
    };

    check_guarantee(values);

    if (const Value* v = get("secure")) {
        opts.tls.enabled = parse_bool("secure", *v);
    }
    Endpoint single{"localhost", opts.tls.enabled ? kDefaultSecurePort : kDefaultPort};
    const Value* host = get("host");
    const Value* port = get("port");
    const Value* endpoints = get("endpoints");
    if (host != nullptr) {
        single.host = non_empty("host", *host);
    }
    if (port != nullptr) {
        single.port = static_cast<std::uint16_t>(parse_integer("port", *port, 1, 65535));
    }
    std::vector<Endpoint> list;
    if (endpoints != nullptr) {
        list = parse_endpoints(*endpoints);
    }
    if (const Value* v = get("database")) {
        opts.database = non_empty("database", *v);
    }
    if (const Value* v = get("table")) {
        opts.table = non_empty("table", *v);
    }
    if (const Value* v = get("user")) {
        opts.user = v->resolved;
    }
    if (const Value* v = get("password")) {
        opts.password = v->resolved;
    }
    if (const Value* v = get("insert_format"); v != nullptr && v->resolved != "native") {
        refuse(code::kOptionInvalid,
               "option 'insert_format' must be 'native' for this sink; got " + shown(*v));
    }
    if (const Value* v = get("tls_ca_file")) {
        opts.tls.ca_file = non_empty("tls_ca_file", *v);
    }
    if (const Value* v = get("tls_ca_dir")) {
        opts.tls.ca_dir = non_empty("tls_ca_dir", *v);
    }
    if (const Value* v = get("tls_verify")) {
        opts.tls.verify = parse_bool("tls_verify", *v);
    }
    if (const Value* v = get("batch_rows")) {
        opts.batch_rows = parse_integer("batch_rows", *v, 1, kMaxBatchRows);
    }
    if (const Value* v = get("batch_bytes")) {
        // One block can be as large as 16 MiB, and a batch must hold at least
        // one block, so anything under 1 MiB could never be met.
        opts.batch_bytes = parse_integer("batch_bytes", *v, 1ULL << 20, kUnbounded);
    }
    if (const Value* v = get("batch_interval_ms")) {
        opts.batch_interval = parse_millis("batch_interval_ms", *v, 1, 3'600'000);
    }
    if (const Value* v = get("compression")) {
        if (v->resolved == "lz4") {
            opts.compression = Compression::Lz4;
        } else if (v->resolved == "zstd") {
            opts.compression = Compression::Zstd;
        } else if (v->resolved == "none") {
            opts.compression = Compression::None;
        } else {
            refuse(code::kOptionInvalid,
                   "option 'compression' must be 'lz4', 'zstd' or 'none'; got " + shown(*v));
        }
    }
    // Zero is never a usable timeout: as a socket timeout it means wait for
    // ever, and as the connect poll's it means give up at once.
    if (const Value* v = get("connect_timeout_ms")) {
        opts.connect_timeout = parse_millis("connect_timeout_ms", *v, 1, 600'000);
    }
    if (const Value* v = get("send_timeout_ms")) {
        opts.send_timeout = parse_millis("send_timeout_ms", *v, 1, 600'000);
    }
    if (const Value* v = get("receive_timeout_ms")) {
        opts.receive_timeout = parse_millis("receive_timeout_ms", *v, 1, 600'000);
    }
    if (const Value* v = get("retry_window_ms")) {
        opts.retry_window = parse_millis("retry_window_ms", *v, 1000, 86'400'000);
    }
    if (const Value* v = get("sql_column_types")) {
        opts.sql_column_types = v->resolved;
    }

    if (endpoints != nullptr && (host != nullptr || port != nullptr)) {
        refuse(code::kOptionConflict,
               std::string("options 'endpoints' and '") + (host != nullptr ? "host" : "port") +
                   "' are both set; list every server in 'endpoints' instead");
    }
    opts.endpoints = endpoints != nullptr ? std::move(list) : std::vector<Endpoint>{single};
    for (const char* key : {"tls_ca_dir", "tls_ca_file", "tls_verify"}) {
        if (get(key) != nullptr && !opts.tls.enabled) {
            refuse(code::kOptionConflict, "option '" + std::string(key) + "' needs secure='true'");
        }
    }

    if (get("table") == nullptr) {
        refuse(code::kOptionInvalid, "option 'table' is required");
    }
    if (opts.sql_column_types.empty()) {
        refuse(code::kOptionInvalid,
               "the native sink is built from SQL; a Dag-direct job must pass sql_column_types");
    }

#if !defined(CLINK_CLICKHOUSE_NATIVE_TLS)
    if (opts.tls.enabled) {
        refuse(code::kTlsUnavailable,
               "secure='true' is not available: this build of the native sink was compiled "
               "without TLS support");
    }
#endif
    return opts;
}

const std::vector<std::string>& own_option_keys() {
    // Sorted, because the unknown-option message lists them in this order.
    static const std::vector<std::string> keys = {
        "batch_bytes",
        "batch_interval_ms",
        "batch_rows",
        "compression",
        "connect_timeout_ms",
        "database",
        "endpoints",
        "host",
        "insert_format",
        "password",
        "port",
        "receive_timeout_ms",
        "retry_window_ms",
        "secure",
        "send_timeout_ms",
        "sql_column_types",
        "table",
        "tls_ca_dir",
        "tls_ca_file",
        "tls_verify",
        "user",
    };
    return keys;
}

const std::vector<std::string>& pass_through_keys() {
    // Read off the code that puts keys on a sink op, not assumed: the table
    // options the planner interprets but build_params copies anyway, the
    // planner's own parameters, the materialised-view backing keys, and the
    // ClickHouse source's keys, which a table that is both read and written
    // carries onto its sink op. A key that appears there later must be added
    // here, or every such table is refused as having an unknown option.
    static const std::vector<std::string> keys = {
        "batch_size",
        "bounded",
        "changelog",
        "columnar_decode",
        "commit_group",
        "decimal_columns",
        "definition_sql",
        "delim",
        "delivery_guarantee",
        "forced_singleton",
        "freshness",
        "freshness_ms",
        "mode",
        "partition_by",
        "primary_key",
        "query",
        "refresh_arm",
        "schema_columns",
        "state_ttl",
        "state_ttl_domain",
        "view_kind",
        "watermark_delay_ms",
        "write_mode",
    };
    return keys;
}

std::string describe(const SinkOptions& options) {
    std::string out;
    const auto line = [&out](std::string_view key, std::string_view value) {
        if (!out.empty()) {
            out += '\n';
        }
        out += key;
        out += '=';
        out += value;
    };
    const auto flag = [](bool b) { return b ? "true" : "false"; };

    std::string endpoints;
    for (const auto& e : options.endpoints) {
        endpoints += (endpoints.empty() ? "" : ",") + render_endpoint(e);
    }
    std::string passed;
    for (const auto& k : options.passed_through) {
        passed += (passed.empty() ? "" : ",") + k;
    }

    line("endpoints", endpoints);
    line("database", options.database);
    line("table", options.table);
    line("user", options.user);
    // Whether a password is configured is worth reporting; what it is never is.
    line("password", options.password.empty() ? "unset" : "set");
    line("secure", flag(options.tls.enabled));
    line("tls_ca_file", options.tls.ca_file.empty() ? "none" : options.tls.ca_file);
    line("tls_ca_dir", options.tls.ca_dir.empty() ? "none" : options.tls.ca_dir);
    line("tls_verify", flag(options.tls.verify));
    line("batch_rows", std::to_string(options.batch_rows));
    line("batch_bytes", std::to_string(options.batch_bytes));
    line("batch_interval_ms", std::to_string(options.batch_interval.count()));
    line("compression", compression_name(options.compression));
    line("connect_timeout_ms", std::to_string(options.connect_timeout.count()));
    line("send_timeout_ms", std::to_string(options.send_timeout.count()));
    line("receive_timeout_ms", std::to_string(options.receive_timeout.count()));
    line("retry_window_ms", std::to_string(options.retry_window.count()));
    line("sql_column_types", options.sql_column_types);
    line("passed_through", passed.empty() ? "none" : passed);
    return out;
}

}  // namespace clink::clickhouse::native
