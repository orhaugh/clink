#pragma once

#include <array>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/http_connector/batched_http_bulk_sink.hpp"
#include "clink/operators/operator_base.hpp"

// InfluxDB v2 line-protocol sink, built on the shared BatchedHttpBulkSink. Each input
// record is a JSON object string (e.g. SQL row_to_json_string); the sink converts it to one
// InfluxDB line-protocol point and POSTs newline-delimited batches to /api/v2/write. Delivery
// is AT-LEAST-ONCE (the base sink replays on failure); InfluxDB writes are idempotent for an
// identical (measurement, tag set, field set, timestamp), so supplying a timestamp_field makes
// replay effectively-once. v1 maps every JSON number to a float field (predictable; avoids the
// float-vs-integer field-type conflicts InfluxDB rejects across points), strings to quoted
// string fields, and booleans to t/f. Each record MUST yield at least one field (line protocol
// requires it); a record with only tags + timestamp is out of contract.
namespace clink::http_connector {

namespace influx_detail {

// Backslash-escape the line-protocol special characters in a measurement name (comma + space).
inline std::string escape_measurement(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (c == ',' || c == ' ') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    return out;
}

// Backslash-escape a tag key, tag value, or field key (comma + equals + space).
inline std::string escape_key(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (c == ',' || c == '=' || c == ' ') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    return out;
}

// A string field VALUE: wrapped in double quotes, with '"' and '\' backslash-escaped.
inline std::string quote_field_string(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    out.push_back('"');
    for (char c : s) {
        if (c == '"' || c == '\\') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

// Format a double locale-independently as the shortest round-trippable decimal (std::to_string
// would honour the global C locale - a ',' decimal separator would corrupt line protocol).
inline std::string format_double(double d) {
    std::array<char, 32> buf{};
    auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), d);
    if (ec != std::errc{}) {
        return "0";
    }
    return std::string(buf.data(), ptr);
}

// Render a scalar JsonValue as a TAG value (always a string in line protocol). Nested/array
// values are serialised to compact JSON text. The caller escapes the result.
inline std::string tag_value_string(const clink::config::JsonValue& v) {
    if (v.is_string()) {
        return v.as_string();
    }
    if (v.is_bool()) {
        return v.as_bool() ? "true" : "false";
    }
    if (v.is_number()) {
        return format_double(v.as_number());
    }
    return v.serialize(0);
}

// Render a scalar JsonValue as a FIELD value: string -> quoted, bool -> t/f, number -> float
// (no 'i' suffix - every JSON number is a float field, so the same field never flips between
// integer and float across points, which InfluxDB rejects). Returns empty for null/unsupported.
inline std::string field_value_string(const clink::config::JsonValue& v) {
    if (v.is_string()) {
        return quote_field_string(v.as_string());
    }
    if (v.is_bool()) {
        return v.as_bool() ? "t" : "f";
    }
    if (v.is_number()) {
        return format_double(v.as_number());  // float field (no 'i')
    }
    return {};  // null / object / array: not a scalar field
}

// Render an integer timestamp from a numeric JsonValue (line protocol wants an integer in the
// chosen precision unit). Returns empty if not a finite number.
inline std::string timestamp_string(const clink::config::JsonValue& v) {
    if (!v.is_number()) {
        return {};
    }
    return std::to_string(static_cast<std::int64_t>(v.as_number()));
}

}  // namespace influx_detail

// Render ONE JSON-object record into an InfluxDB line-protocol point, appended to `out` (no
// trailing newline - the Ndjson framing adds it). Standalone (not a lambda) so the escaping +
// tag/field/timestamp mapping is unit-testable without a live server. `tag_set` must contain
// exactly the entries of `tag_keys` (passed separately to preserve tag emission order).
inline void render_influx_line(std::string& out,
                               const std::string& measurement,
                               const std::vector<std::string>& tag_keys,
                               const std::unordered_set<std::string>& tag_set,
                               const std::string& ts_field,
                               const std::string& rec) {
    clink::config::JsonValue j;
    try {
        j = clink::config::parse(rec);
    } catch (...) {
        // Not JSON: emit a bare measurement, which InfluxDB rejects (400 -> DLQ/throw),
        // surfacing the malformed record rather than silently dropping it.
        out += influx_detail::escape_measurement(measurement);
        out += " ";
        return;
    }
    out += influx_detail::escape_measurement(measurement);
    if (!j.is_object()) {
        out += " ";  // non-object JSON: bare measurement -> InfluxDB 400 -> DLQ
        return;
    }
    const auto& obj = j.as_object();
    // Tags, in the configured order, when present and non-null.
    for (const auto& tk : tag_keys) {
        auto it = obj.find(tk);
        if (it == obj.end() || it->second.is_null()) {
            continue;
        }
        out += ',';
        out += influx_detail::escape_key(tk);
        out += '=';
        out += influx_detail::escape_key(influx_detail::tag_value_string(it->second));
    }
    // Fields: every key that is neither a tag nor the timestamp field.
    std::string fields;
    for (const auto& [k, v] : obj) {
        if (tag_set.count(k) != 0 || (!ts_field.empty() && k == ts_field) || v.is_null()) {
            continue;
        }
        const std::string fv = influx_detail::field_value_string(v);
        if (fv.empty()) {
            continue;  // object/array field: not a line-protocol scalar
        }
        if (!fields.empty()) {
            fields += ',';
        }
        fields += influx_detail::escape_key(k);
        fields += '=';
        fields += fv;
    }
    out += ' ';
    out += fields;  // may be empty -> out-of-contract record (InfluxDB 400 -> DLQ)
    // Timestamp, if a numeric field was configured.
    if (!ts_field.empty()) {
        auto it = obj.find(ts_field);
        if (it != obj.end()) {
            const std::string ts = influx_detail::timestamp_string(it->second);
            if (!ts.empty()) {
                out += ' ';
                out += ts;
            }
        }
    }
}

struct InfluxDbOptions {
    std::string url;              // scheme://host[:port]   (required, e.g. http://localhost:8086)
    std::string org;              // v2 organisation        (required)
    std::string bucket;           // v2 bucket              (required)
    std::string token;            // v2 API token -> "Authorization: Token <token>" (required)
    std::string measurement;      // line-protocol measurement name (required)
    std::string tag_keys;         // CSV of record fields to emit as tags (optional)
    std::string timestamp_field;  // record field holding the point timestamp (optional)
    std::string precision{"ns"};  // ns|us|ms|s  (write query precision)
    std::string headers;          // extra "K: V; ..." headers
    bool verify_tls{true};
    std::size_t batch_records{500};
    std::size_t batch_bytes{4 * 1024 * 1024};
    int max_retries{4};
    std::chrono::milliseconds max_age{0};  // linger: flush a partial batch this old
    DlqPolicy dlq_policy{DlqPolicy::Fail};
    std::string name{"influxdb_sink"};
};

// Split a comma-separated list, trimming surrounding whitespace; empty entries dropped.
inline std::vector<std::string> influx_split_csv(const std::string& csv) {
    std::vector<std::string> out;
    std::size_t pos = 0;
    while (pos <= csv.size()) {
        const auto comma = csv.find(',', pos);
        const auto end = comma == std::string::npos ? csv.size() : comma;
        auto tok = csv.substr(pos, end - pos);
        const auto b = tok.find_first_not_of(" \t");
        const auto e = tok.find_last_not_of(" \t");
        if (b != std::string::npos) {
            out.push_back(tok.substr(b, e - b + 1));
        }
        if (comma == std::string::npos) {
            break;
        }
        pos = comma + 1;
    }
    return out;
}

// URL-encode a query-parameter value (org/bucket/precision may contain spaces or reserved
// characters). Conservative unreserved set per RFC 3986.
inline std::string influx_url_encode(const std::string& s) {
    static const char* hex = "0123456789ABCDEF";
    std::string out;
    out.reserve(s.size());
    for (char ch : s) {
        const auto c = static_cast<unsigned char>(ch);
        if (std::isalnum(c) != 0 || c == '-' || c == '_' || c == '.' || c == '~') {
            out.push_back(ch);
        } else {
            out.push_back('%');
            out.push_back(hex[c >> 4]);
            out.push_back(hex[c & 0x0F]);
        }
    }
    return out;
}

inline std::shared_ptr<Sink<std::string>> make_influxdb_sink(InfluxDbOptions o) {
    if (o.url.empty()) {
        throw std::runtime_error(o.name + ": 'url' is required");
    }
    if (o.org.empty() || o.bucket.empty()) {
        throw std::runtime_error(o.name + ": 'org' and 'bucket' are required");
    }
    if (o.token.empty()) {
        throw std::runtime_error(o.name + ": 'token' is required");
    }
    if (o.measurement.empty()) {
        throw std::runtime_error(o.name + ": 'measurement' is required");
    }
    if (o.precision != "ns" && o.precision != "us" && o.precision != "ms" && o.precision != "s") {
        throw std::runtime_error(o.name + ": 'precision' must be one of ns|us|ms|s");
    }

    BatchedHttpBulkSink<std::string>::Options opts;
    opts.http.base_url = o.url;
    opts.http.headers = parse_headers(o.headers);
    opts.http.headers["Authorization"] = "Token " + o.token;
    opts.http.verify_tls = o.verify_tls;
    opts.path = "/api/v2/write?org=" + influx_url_encode(o.org) +
                "&bucket=" + influx_url_encode(o.bucket) +
                "&precision=" + influx_url_encode(o.precision);
    opts.content_type = "text/plain; charset=utf-8";
    opts.framing = BulkFraming::Ndjson;  // one point per line
    opts.max_records = o.batch_records;
    opts.max_bytes = o.batch_bytes;
    opts.max_retries = o.max_retries;
    opts.max_age = o.max_age;
    opts.dlq_policy = o.dlq_policy;
    opts.name = o.name;
    // InfluxDB v2 write is whole-batch: 204 = all points written, 400/422 = malformed line(s)
    // (permanent / poison), 429/503 = backpressure (retry). The default whole-batch handler
    // (no response_handler) already classifies these correctly, so none is supplied.

    auto tag_keys = influx_split_csv(o.tag_keys);
    std::unordered_set<std::string> tag_set(tag_keys.begin(), tag_keys.end());

    auto render = [measurement = o.measurement,
                   tag_keys = std::move(tag_keys),
                   tag_set = std::move(tag_set),
                   ts_field = o.timestamp_field](std::string& out, const std::string& rec) {
        render_influx_line(out, measurement, tag_keys, tag_set, ts_field, rec);
    };
    return std::make_shared<BatchedHttpBulkSink<std::string>>(std::move(opts), std::move(render));
}

}  // namespace clink::http_connector
