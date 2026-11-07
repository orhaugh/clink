#pragma once

// InfluxDB v2 line-protocol sink, built on the shared BatchedHttpBulkSink. Each
// input record is a JSON object string (e.g. SQL row_to_json_string); the sink
// turns it into exactly one line-protocol point and POSTs newline-delimited
// batches to /api/v2/write.
//
// A point is identified by its measurement, its tag set and its timestamp. With
// timestamp_field set, every record must carry a whole-number timestamp in the
// write precision, within InfluxDB's time range; it is written exactly, never
// through a double, and a record without one is refused rather than given the
// server's clock. The configured tags must all be present and non-empty. Every
// name and value is escaped as line protocol requires, and what line protocol
// cannot carry safely (a line break or NUL in a name or tag value, a NUL in a
// string, a name ending in a backslash, a measurement starting with '#', the
// reserved keys) is refused, so one record never becomes two lines or none. A
// refused record goes the way the bad-record policy says, before anything is
// sent: dead-lettered under dlq=drop, failing the sink otherwise.
//
// Numbers: number_fields='float' (the default) writes every JSON number as a
// float field, as earlier releases did, and refuses an integer past 2^53, which
// a float cannot hold exactly; number_fields='typed' writes an integer as an
// integer field and any other number as a float field.
//
// Delivery is at-least-once. A batch counts as written only on HTTP 204. A 400
// means InfluxDB wrote none of the request; a 422 means it wrote some points and
// dropped others without saying which (a partial write), which fails the sink
// under dlq=fail and is dead-lettered as one report under dlq=drop. With a
// timestamp, a replayed point lands on the same series and time, and InfluxDB
// merges it into the point already there; without one, the server's clock
// stamps each write and a replay adds points. The contract is in
// docs/connectors/http.md ("influxdb_sink").

#include <array>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/http_connector/batched_http_bulk_sink.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::http_connector {

// InfluxDB's time range in nanoseconds: the write API refuses a point outside it
// ("time outside range -9223372036854775806 - 9223372036854775806").
inline constexpr std::int64_t kInfluxMaxTimeNs = 9223372036854775806;
// The largest integer a float field holds exactly.
inline constexpr std::int64_t kInfluxMaxExactFloatInt = std::int64_t{1} << 53;

struct InfluxDbOptions {
    std::string url;              // scheme://host[:port]   (required, e.g. http://localhost:8086)
    std::string org;              // v2 organisation        (required)
    std::string bucket;           // v2 bucket              (required)
    std::string token;            // v2 API token -> "Authorization: Token <token>" (required)
    std::string measurement;      // line-protocol measurement name (required)
    std::string tag_keys;         // CSV of record fields written as tags (optional)
    std::string timestamp_field;  // record field holding the point timestamp (optional)
    std::string precision{"ns"};  // ns|us|ms|s: the unit of the timestamps written
    // "float": every JSON number a float field; "typed": an integer an integer
    // field (suffix i), any other number a float field.
    std::string number_fields{"float"};
    std::string headers;  // extra "K: V; ..." headers
    bool verify_tls{true};
    int connect_timeout_ms{5000};
    int request_timeout_ms{30000};
    std::size_t batch_records{500};
    std::size_t batch_bytes{4 * 1024 * 1024};
    int max_retries{4};
    std::chrono::milliseconds max_age{0};  // linger: flush a partial batch this old
    DlqPolicy dlq_policy{DlqPolicy::Fail};
    std::string name{"influxdb_sink"};
};

namespace influx_detail {

// Locale-independent shortest round-trip text of a double (std::to_string would
// honour the C locale, and a ',' decimal separator would corrupt the line).
inline std::string format_double(double d) {
    std::array<char, 32> buf{};
    auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), d);
    if (ec != std::errc{}) {
        return "0";
    }
    return std::string(buf.data(), ptr);
}

// Why `s`, a measurement name, tag key, tag value or field key, cannot be
// written in line protocol, or empty when it can. A line break or NUL cannot be
// escaped (a line break ends the point, and InfluxDB drops NUL), and a trailing
// backslash would escape the separator after it.
inline std::string unwritable_name(std::string_view s) {
    if (s.empty()) {
        return "is empty";
    }
    for (const char c : s) {
        if (c == '\n' || c == '\r') {
            return "holds a line break, which line protocol cannot escape";
        }
        if (c == '\0') {
            return "holds a NUL character";
        }
    }
    if (s.back() == '\\') {
        return "ends in a backslash, which would escape the separator after it";
    }
    return {};
}

// Backslash-escape the characters `special` names; a backslash elsewhere is
// literal in line protocol.
inline void append_escaped(std::string& out, std::string_view s, std::string_view special) {
    for (const char c : s) {
        if (special.find(c) != std::string_view::npos) {
            out.push_back('\\');
        }
        out.push_back(c);
    }
}

// A string field value: quoted, with '"' and '\' escaped. Line breaks and tabs
// are written as they are, inside the quotes, where InfluxDB keeps them.
inline void append_string_field(std::string& out, std::string_view s) {
    out.push_back('"');
    for (const char c : s) {
        if (c == '"' || c == '\\') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    out.push_back('"');
}

inline const char* kind_of(const clink::config::JsonValue& v) {
    if (v.is_object()) {
        return "a JSON object";
    }
    if (v.is_array()) {
        return "a JSON array";
    }
    return "a value of another type";
}

}  // namespace influx_detail

// Split a comma-separated list, trimming surrounding whitespace; empty entries
// are kept as empty strings so the caller can refuse them.
inline std::vector<std::string> influx_split_csv(const std::string& csv) {
    std::vector<std::string> out;
    if (csv.empty()) {
        return out;
    }
    std::size_t pos = 0;
    while (pos <= csv.size()) {
        const auto comma = csv.find(',', pos);
        const auto end = comma == std::string::npos ? csv.size() : comma;
        auto tok = csv.substr(pos, end - pos);
        const auto b = tok.find_first_not_of(" \t");
        const auto e = tok.find_last_not_of(" \t");
        out.push_back(b == std::string::npos ? std::string{} : tok.substr(b, e - b + 1));
        if (comma == std::string::npos) {
            break;
        }
        pos = comma + 1;
    }
    return out;
}

// The line-protocol encoding of one sink's records. Its constructor checks the
// measurement, tags, timestamp field, precision and number mode; render()
// checks each record.
class InfluxLineRenderer {
public:
    explicit InfluxLineRenderer(const InfluxDbOptions& o) : name_(o.name) {
        const auto refuse = [this](const std::string& why) {
            throw std::runtime_error(name_ + ": " + why);
        };
        if (const auto why = influx_detail::unwritable_name(o.measurement); !why.empty()) {
            refuse("'measurement' " + why);
        }
        if (o.measurement.front() == '#') {
            refuse(
                "'measurement' starts with '#', which makes the line a comment that InfluxDB "
                "accepts and drops");
        }
        influx_detail::append_escaped(measurement_, o.measurement, ", ");

        if (o.precision == "ns") {
            unit_ = 1;
        } else if (o.precision == "us") {
            unit_ = 1000;
        } else if (o.precision == "ms") {
            unit_ = 1000000;
        } else if (o.precision == "s") {
            unit_ = 1000000000;
        } else {
            refuse("'precision' must be one of ns, us, ms, s; it is '" + o.precision + "'");
        }
        precision_ = o.precision;
        ts_max_ = kInfluxMaxTimeNs / unit_;

        if (o.number_fields == "typed") {
            typed_ = true;
        } else if (o.number_fields != "float") {
            refuse("'number_fields' must be 'float' or 'typed'; it is '" + o.number_fields + "'");
        }

        ts_field_ = o.timestamp_field;
        if (!ts_field_.empty()) {
            if (const auto why = influx_detail::unwritable_name(ts_field_); !why.empty()) {
                refuse("'timestamp_field' " + why);
            }
        }
        std::unordered_set<std::string> seen;
        for (const auto& t : influx_split_csv(o.tag_keys)) {
            if (const auto why = influx_detail::unwritable_name(t); !why.empty()) {
                refuse("a tag in 'tags' " + why);
            }
            if (t == "time" || t == "_field" || t == "_measurement") {
                refuse("'tags' names '" + t + "', a tag key InfluxDB reserves");
            }
            if (t == ts_field_) {
                refuse("'tags' names the timestamp field '" + t + "'");
            }
            if (!seen.insert(t).second) {
                refuse("'tags' names '" + t + "' twice");
            }
            std::string escaped;
            influx_detail::append_escaped(escaped, t, ",= ");
            tags_.push_back(Tag{t, std::move(escaped)});
        }
    }

    [[nodiscard]] bool timestamped() const noexcept { return !ts_field_.empty(); }

    // Appends the record's point to `out`, without a trailing newline (the
    // batch's framing adds it). Throws UnrenderableRecord, naming the rule and
    // the field but never a value, and leaves `out` unchanged.
    void render(std::string& out, const std::string& record) const {
        clink::config::JsonValue j;
        try {
            j = clink::config::parse(record);
        } catch (const std::exception&) {
            refuse("is not valid JSON, or holds a number the JSON reader cannot read");
        }
        if (!j.is_object()) {
            refuse("is not a JSON object");
        }
        const auto& obj = j.as_object();
        std::string line = measurement_;

        for (const auto& t : tags_) {
            const auto it = obj.find(t.key);
            if (it == obj.end()) {
                refuse("has no '" + t.key + "' field for its tag");
            }
            const auto& v = it->second;
            std::string value;
            if (v.is_null()) {
                refuse("has a null '" + t.key + "' tag");
            } else if (v.is_string()) {
                value = v.as_string();
            } else if (v.is_bool()) {
                value = v.as_bool() ? "true" : "false";
            } else if (v.is_integral_number()) {
                value = std::to_string(v.as_int());
            } else if (v.is_number()) {
                value = influx_detail::format_double(v.as_number());
            } else {
                refuse("has " + std::string{influx_detail::kind_of(v)} + " for its '" + t.key +
                       "' tag");
            }
            if (const auto why = influx_detail::unwritable_name(value); !why.empty()) {
                refuse("has a '" + t.key + "' tag that " + why);
            }
            line.push_back(',');
            line += t.escaped;
            line.push_back('=');
            influx_detail::append_escaped(line, value, ",= ");
        }

        std::string fields;
        for (const auto& [k, v] : obj) {
            if (is_tag(k) || (!ts_field_.empty() && k == ts_field_) || v.is_null()) {
                continue;  // InfluxDB has no null: a null field is left out
            }
            if (const auto why = influx_detail::unwritable_name(k); !why.empty()) {
                refuse("has a field name that " + why);
            }
            if (k == "time") {
                refuse("has a field named 'time', which InfluxDB refuses");
            }
            if (!fields.empty()) {
                fields.push_back(',');
            }
            influx_detail::append_escaped(fields, k, ",= ");
            fields.push_back('=');
            append_field_value(fields, record, k, v);
        }
        if (fields.empty()) {
            refuse("has no field to write: line protocol needs at least one");
        }
        line.push_back(' ');
        line += fields;

        if (!ts_field_.empty()) {
            const auto it = obj.find(ts_field_);
            if (it == obj.end() || it->second.is_null()) {
                refuse(std::string{it == obj.end() ? "has no '" : "has a null '"} + ts_field_ +
                       "' (timestamp_field)");
            }
            const auto& v = it->second;
            if (!v.is_integral_number()) {
                refuse("has a '" + ts_field_ +
                       "' (timestamp_field) that is not a JSON integer within 64 bits: a "
                       "timestamp is a whole number of " +
                       precision_ + " since the epoch, written without a fraction or exponent");
            }
            const std::int64_t ts = v.as_int();
            if (ts > ts_max_ || ts < -ts_max_) {
                refuse("has a '" + ts_field_ +
                       "' (timestamp_field) outside InfluxDB's time range "
                       "at precision " +
                       precision_ + " (" + std::to_string(-ts_max_) + " to " +
                       std::to_string(ts_max_) + ")");
            }
            line.push_back(' ');
            line += std::to_string(ts);
        }
        out += line;
    }

private:
    struct Tag {
        std::string key;
        std::string escaped;
    };

    [[noreturn]] void refuse(const std::string& why) const {
        throw UnrenderableRecord(name_ + ": a record " + why);
    }

    [[nodiscard]] bool is_tag(const std::string& k) const {
        for (const auto& t : tags_) {
            if (t.key == k) {
                return true;
            }
        }
        return false;
    }

    // Whether `field` was written as an integer token (no fraction, no
    // exponent), read from the record's own text: the JSON reader holds an
    // integer past 64 bits as a double, which cannot tell 1e19 from 10000000000000000000.
    static bool integer_token(const std::string& record, const std::string& field) {
        const auto raw = clink::config::raw_number_tokens(record, {{field, 0}});
        const auto it = raw.find(field);
        return it != raw.end() && it->second.find_first_of(".eE") == std::string::npos;
    }

    void append_field_value(std::string& out,
                            const std::string& record,
                            const std::string& k,
                            const clink::config::JsonValue& v) const {
        if (v.is_string()) {
            const auto& s = v.as_string();
            if (s.find('\0') != std::string::npos) {
                refuse("has a NUL character in its '" + k + "' string, which InfluxDB drops");
            }
            influx_detail::append_string_field(out, s);
            return;
        }
        if (v.is_bool()) {
            out += v.as_bool() ? "t" : "f";
            return;
        }
        if (v.is_integral_number()) {
            const std::int64_t n = v.as_int();
            if (typed_) {
                out += std::to_string(n);
                out.push_back('i');
                return;
            }
            if (n > kInfluxMaxExactFloatInt || n < -kInfluxMaxExactFloatInt) {
                refuse("has an integer '" + k +
                       "' past 2^53, which a float field cannot hold exactly (number_fields="
                       "'typed' writes integers as integer fields)");
            }
            out += std::to_string(n);  // no suffix: a float field, as in earlier releases
            return;
        }
        if (v.is_number()) {
            const double d = v.as_number();
            if (std::fabs(d) >= 9223372036854775808.0 && integer_token(record, k)) {
                refuse("has an integer '" + k + "' outside 64 bits, which no field type holds");
            }
            out += influx_detail::format_double(d);
            return;
        }
        refuse("has " + std::string{influx_detail::kind_of(v)} + " in its '" + k +
               "' field, which line protocol cannot write as one field");
    }

    std::string name_;
    std::string measurement_;  // escaped
    std::vector<Tag> tags_;
    std::string ts_field_;
    std::string precision_;
    std::int64_t unit_{1};
    std::int64_t ts_max_{kInfluxMaxTimeNs};
    bool typed_{false};
};

// InfluxDB's account of a refused write, from its JSON body's "message", with
// the single-quoted line it quotes emptied (it carries the record's values),
// cut to 300 bytes. Field and measurement names, which it double-quotes, stay.
inline std::string influx_error_message(const std::string& body) {
    std::string msg = body;
    try {
        const auto j = clink::config::parse(body);
        if (j.is_object()) {
            if (const auto it = j.as_object().find("message");
                it != j.as_object().end() && it->second.is_string()) {
                msg = it->second.as_string();
            }
        }
    } catch (const std::exception&) {  // NOLINT(bugprone-empty-catch) not JSON: the body as it is
    }
    std::string out;
    bool quoted = false;
    for (const char c : msg) {
        if (c == '\'') {
            if (!quoted) {
                out += "'...";
            } else {
                out.push_back('\'');
            }
            quoted = !quoted;
            continue;
        }
        if (!quoted) {
            out.push_back((c == '\n' || c == '\r') ? ' ' : c);
        }
    }
    if (quoted) {
        out.push_back('\'');
    }
    if (out.size() > 300) {
        out.resize(297);
        out += "...";
    }
    return out;
}

// What an InfluxDB v2 write response means for the request's records:
//   204                 every point written
//   another 2xx         not a confirmation: resent, then the sink fails
//   400                 the whole request refused, nothing written (a line it could not parse)
//   422                 a partial write: some points dropped, the response says
//                       how many ("dropped=N") but not which
//   401, 403            the token refused: resent, then the sink fails, never dead-lettered
//   404                 no such organisation or bucket: as 401
//   413                 the request larger than the server takes: as 401
//   anything else       transient (throttling, 5xx, no response): resent
inline BulkResult influx_write_result(const HttpResponse& res, std::size_t record_count) {
    BulkResult r;
    if (res.status == 204) {
        r.ok = true;
        return r;
    }
    std::vector<std::size_t> all(record_count);
    for (std::size_t i = 0; i < record_count; ++i) {
        all[i] = i;
    }
    const std::string msg = res.status == 0 ? std::string{} : influx_error_message(res.body);
    if (res.status >= 200 && res.status < 300) {
        r.failed_transient = std::move(all);
        r.detail = "InfluxDB answers a write it accepted with 204; a " +
                   std::to_string(res.status) + " does not confirm the points";
        return r;
    }
    if (res.status == 400) {
        r.failed_permanent = std::move(all);
        r.detail = "InfluxDB refused the whole request and wrote none of it: " + msg;
        return r;
    }
    if (res.status == 422) {
        std::size_t dropped = 0;
        if (const auto at = res.body.find("dropped="); at != std::string::npos) {
            const char* begin = res.body.data() + at + 8;
            const char* end = res.body.data() + res.body.size();
            std::uint64_t n = 0;
            if (const auto [p, ec] = std::from_chars(begin, end, n); ec == std::errc{}) {
                dropped = static_cast<std::size_t>(n);
            }
        }
        r.refused_unattributed = dropped > 0 ? dropped : 1;
        r.detail = "a partial write, InfluxDB dropping " +
                   (dropped > 0 ? std::to_string(dropped) : std::string{"at least one"}) +
                   " point(s): " + msg;
        return r;
    }
    r.failed_transient = std::move(all);
    if (res.status == 401 || res.status == 403) {
        r.detail = "the token was refused; check it, and that it may write to the bucket: " + msg;
    } else if (res.status == 404) {
        r.detail = "no such organisation or bucket: " + msg;
    } else if (res.status == 413) {
        r.detail =
            "the request was larger than the server takes (its http-max-body-size); "
            "lower batch_bytes";
    } else if (!msg.empty()) {
        r.detail = msg;
    }
    return r;
}

// URL-encode a query-parameter value (org/bucket may contain spaces or reserved
// characters). Conservative unreserved set per RFC 3986.
inline std::string influx_url_encode(const std::string& s) {
    static const char* hex = "0123456789ABCDEF";
    std::string out;
    out.reserve(s.size());
    for (const char ch : s) {
        const auto c = static_cast<unsigned char>(ch);
        if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            c == '-' || c == '_' || c == '.' || c == '~') {
            out.push_back(ch);
        } else {
            out.push_back('%');
            out.push_back(hex[c >> 4U]);
            out.push_back(hex[c & 0x0FU]);
        }
    }
    return out;
}

inline std::shared_ptr<Sink<std::string>> make_influxdb_sink(InfluxDbOptions o) {
    const auto refuse = [&o](const std::string& why) {
        throw std::runtime_error(o.name + ": " + why);
    };
    if (o.url.empty()) {
        refuse("'url' is required");
    }
    if (o.org.empty() || o.bucket.empty()) {
        refuse("'org' and 'bucket' are required");
    }
    if (o.token.empty()) {
        refuse("'token' is required");
    }
    if (o.measurement.empty()) {
        refuse("'measurement' is required");
    }
    const auto ranged = [&refuse](
                            const char* key, std::int64_t v, std::int64_t lo, std::int64_t hi) {
        if (v < lo || v > hi) {
            refuse(std::string{"'"} + key + "' must be " + std::to_string(lo) + " to " +
                   std::to_string(hi) + "; it is " + std::to_string(v));
        }
    };
    ranged("batch_records", static_cast<std::int64_t>(o.batch_records), 1, 100000);
    ranged("batch_bytes", static_cast<std::int64_t>(o.batch_bytes), 1024, 256LL * 1024 * 1024);
    ranged("max_retries", o.max_retries, 0, BatchedHttpBulkSink<std::string>::kMaxRetries);
    ranged("connect_timeout_ms", o.connect_timeout_ms, 1, 600000);
    ranged("request_timeout_ms", o.request_timeout_ms, 1, 600000);
    ranged("linger_ms", o.max_age.count(), 0, 3600000);
    auto renderer = std::make_shared<const InfluxLineRenderer>(o);

    BatchedHttpBulkSink<std::string>::Options opts;
    opts.http.base_url = o.url;
    opts.http.headers = parse_headers(o.headers);
    opts.http.headers["Authorization"] = "Token " + o.token;
    opts.http.verify_tls = o.verify_tls;
    opts.http.connect_timeout_ms = o.connect_timeout_ms;
    opts.http.rw_timeout_ms = o.request_timeout_ms;
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
    opts.response_handler = influx_write_result;

    auto render = [renderer](std::string& out, const std::string& rec) {
        renderer->render(out, rec);
    };
    return std::make_shared<BatchedHttpBulkSink<std::string>>(std::move(opts), std::move(render));
}

}  // namespace clink::http_connector
