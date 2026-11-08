#include "clink/http_connector/prometheus_push_sink.hpp"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace clink::http_connector {

namespace {

constexpr const char* kContentType = "text/plain; version=0.0.4; charset=utf-8";
// The largest integer a gauge (an IEEE double) holds exactly, and every one below it.
constexpr std::int64_t kMaxExactInt = 9007199254740992;
// A push's reply is empty; anything longer than this is not the gateway's.
constexpr std::size_t kMaxReplyBytes = 64 * 1024;
// The most the sink reads of the gateway's query API when it adopts its group.
constexpr std::size_t kMaxQueryBytes = std::size_t{64} * 1024 * 1024;
constexpr std::chrono::milliseconds kRetryBase{200};
// How often the pause before a retry looks at the task's cancel signal.
constexpr std::chrono::milliseconds kCancelPoll{50};

bool valid_utf8(std::string_view s) noexcept {
    std::size_t i = 0;
    while (i < s.size()) {
        const auto c = static_cast<unsigned char>(s[i]);
        if (c < 0x80) {
            ++i;
            continue;
        }
        std::size_t len = 0;
        std::uint32_t cp = 0;
        if ((c & 0xE0) == 0xC0) {
            len = 2;
            cp = c & 0x1FU;
        } else if ((c & 0xF0) == 0xE0) {
            len = 3;
            cp = c & 0x0FU;
        } else if ((c & 0xF8) == 0xF0) {
            len = 4;
            cp = c & 0x07U;
        } else {
            return false;
        }
        if (i + len > s.size()) {
            return false;
        }
        for (std::size_t k = 1; k < len; ++k) {
            const auto cc = static_cast<unsigned char>(s[i + k]);
            if ((cc & 0xC0) != 0x80) {
                return false;
            }
            cp = (cp << 6) | (cc & 0x3FU);
        }
        const bool overlong =
            (len == 2 && cp < 0x80) || (len == 3 && cp < 0x800) || (len == 4 && cp < 0x10000);
        if (overlong || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) {
            return false;
        }
        i += len;
    }
    return true;
}

bool ascii_alpha(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}
bool ascii_digit(char c) {
    return c >= '0' && c <= '9';
}

// [a-zA-Z_][a-zA-Z0-9_]*, and not __-prefixed, which Prometheus reserves.
bool label_name_ok(std::string_view n) {
    if (n.empty() || !(ascii_alpha(n[0]) || n[0] == '_')) {
        return false;
    }
    for (const char c : n) {
        if (!(ascii_alpha(c) || ascii_digit(c) || c == '_')) {
            return false;
        }
    }
    return !(n.size() >= 2 && n[0] == '_' && n[1] == '_');
}

// [a-zA-Z_:][a-zA-Z0-9_:]*
bool metric_name_ok(std::string_view n) {
    if (n.empty() || !(ascii_alpha(n[0]) || n[0] == '_' || n[0] == ':')) {
        return false;
    }
    return std::all_of(n.begin(), n.end(), [](char c) {
        return ascii_alpha(c) || ascii_digit(c) || c == '_' || c == ':';
    });
}

// A name as a message may show it: printable ASCII kept, any other byte as \xNN.
std::string shown(std::string_view s) {
    static const char* const kHex = "0123456789abcdef";
    std::string out;
    for (const char c : s.substr(0, 128)) {
        const auto u = static_cast<unsigned char>(c);
        if (u >= 0x20 && u < 0x7F) {
            out.push_back(c);
        } else {
            out += "\\x";
            out.push_back(kHex[u >> 4]);
            out.push_back(kHex[u & 0x0F]);
        }
    }
    return s.size() > 128 ? out + "..." : out;
}

const char* kind_of(const clink::config::JsonValue& v) {
    if (v.is_object()) {
        return "an object";
    }
    if (v.is_array()) {
        return "an array";
    }
    if (v.is_string()) {
        return "a string";
    }
    if (v.is_bool()) {
        return "a boolean";
    }
    if (v.is_null()) {
        return "null";
    }
    return "a number";
}

// Prometheus label values escape only backslash, double quote and line feed;
// every other character, control characters included, is written as it is.
void append_label_value(std::string& out, std::string_view v) {
    for (const char c : v) {
        switch (c) {
            case '\\':
                out += "\\\\";
                break;
            case '"':
                out += "\\\"";
                break;
            case '\n':
                out += "\\n";
                break;
            default:
                out.push_back(c);
        }
    }
}

// # HELP text escapes backslash and line feed (it is not quoted).
std::string escape_help(std::string_view in) {
    std::string s;
    for (const char c : in) {
        if (c == '\\') {
            s += "\\\\";
        } else if (c == '\n') {
            s += "\\n";
        } else {
            s.push_back(c);
        }
    }
    return s;
}

// The label block {a="x",b="y"} of a label set, in name order (std::map's), or
// "" for none: one series, one block.
std::string render_block(const std::map<std::string, std::string>& labels) {
    if (labels.empty()) {
        return {};
    }
    std::string s = "{";
    bool first = true;
    for (const auto& [k, v] : labels) {
        if (!first) {
            s.push_back(',');
        }
        first = false;
        s += k;
        s += "=\"";
        append_label_value(s, v);
        s.push_back('"');
    }
    s.push_back('}');
    return s;
}

std::string base64url_nopad(std::string_view in) {
    static const char t[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    auto uc = [](char c) { return static_cast<unsigned>(static_cast<unsigned char>(c)); };
    std::string out;
    std::size_t i = 0;
    while (i + 3 <= in.size()) {
        const unsigned n = (uc(in[i]) << 16) | (uc(in[i + 1]) << 8) | uc(in[i + 2]);
        out.push_back(t[(n >> 18) & 63]);
        out.push_back(t[(n >> 12) & 63]);
        out.push_back(t[(n >> 6) & 63]);
        out.push_back(t[n & 63]);
        i += 3;
    }
    const std::size_t rem = in.size() - i;
    if (rem == 1) {
        const unsigned n = uc(in[i]) << 16;
        out.push_back(t[(n >> 18) & 63]);
        out.push_back(t[(n >> 12) & 63]);
    } else if (rem == 2) {
        const unsigned n = (uc(in[i]) << 16) | (uc(in[i + 1]) << 8);
        out.push_back(t[(n >> 18) & 63]);
        out.push_back(t[(n >> 12) & 63]);
        out.push_back(t[(n >> 6) & 63]);
    }
    return out;
}

// One "/<label>/<value>" path segment, in the gateway's "<label>@base64/<value>"
// form (URL-safe alphabet) when the value is empty or holds any byte outside
// [A-Za-z0-9._-]. The gateway decodes %2F before it splits the path, so a slash
// survives only in base64.
std::string path_segment(const std::string& label, const std::string& value) {
    const bool plain = !value.empty() && std::all_of(value.begin(), value.end(), [](char c) {
        return ascii_alpha(c) || ascii_digit(c) || c == '-' || c == '_' || c == '.';
    });
    if (plain) {
        return "/" + label + "/" + value;
    }
    if (value.empty()) {
        return "/" + label + "@base64/=";  // the gateway's form for an empty value
    }
    return "/" + label + "@base64/" + base64url_nopad(value);
}

std::string trim(std::string_view s) {
    const auto b = s.find_first_not_of(" \t");
    const auto e = s.find_last_not_of(" \t");
    return b == std::string_view::npos ? std::string{} : std::string{s.substr(b, e - b + 1)};
}

// Whether `s` is a JSON number: -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?,
// and whether it is an integer (no fraction, no exponent).
bool json_number_text(std::string_view s, bool& integral) {
    std::size_t i = 0;
    auto digits = [&] {
        const std::size_t from = i;
        while (i < s.size() && ascii_digit(s[i])) {
            ++i;
        }
        return i > from;
    };
    if (i < s.size() && s[i] == '-') {
        ++i;
    }
    if (i < s.size() && s[i] == '0') {
        ++i;
    } else if (!digits()) {
        return false;
    }
    integral = true;
    if (i < s.size() && s[i] == '.') {
        ++i;
        integral = false;
        if (!digits()) {
            return false;
        }
    }
    if (i < s.size() && (s[i] == 'e' || s[i] == 'E')) {
        ++i;
        integral = false;
        if (i < s.size() && (s[i] == '+' || s[i] == '-')) {
            ++i;
        }
        if (!digits()) {
            return false;
        }
    }
    return i == s.size();
}

// A value the gateway reports: NaN, +Inf, -Inf, or a Go float.
std::optional<double> reported_value(std::string_view s) {
    if (s == "NaN") {
        return std::numeric_limits<double>::quiet_NaN();
    }
    if (s == "+Inf") {
        return std::numeric_limits<double>::infinity();
    }
    if (s == "-Inf") {
        return -std::numeric_limits<double>::infinity();
    }
    double d = 0;
    const auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), d);
    if (ec != std::errc{} || ptr != s.data() + s.size()) {
        return std::nullopt;
    }
    return d;
}

// A field of a JSON object, or null.
const clink::config::JsonValue* field(const clink::config::JsonObject& o, std::string_view k) {
    const auto it = o.find(k);
    return it == o.end() ? nullptr : &it->second;
}

std::string failure_text(const BoundedResponse& r) {
    if (r.interrupted) {
        return r.request_written ? "no reply within request_timeout_ms after the request was sent"
                                 : "the request was not sent within request_timeout_ms";
    }
    switch (r.failure) {
        case BoundedResponse::Failure::Connect:
            return "the connection was not made (" + r.error + ")";
        case BoundedResponse::Failure::TlsHandshake:
        case BoundedResponse::Failure::TlsVerify:
            return "TLS failed (" + r.error + ")";
        case BoundedResponse::Failure::Write:
            return "the request was not sent whole (" + r.error + ")";
        case BoundedResponse::Failure::Read:
            return "the reply was lost after the request was sent (" + r.error + ")";
        case BoundedResponse::Failure::TooLarge:
            return "the reply was larger than any the gateway sends";
        case BoundedResponse::Failure::Other:
        case BoundedResponse::Failure::None:
            break;
    }
    return r.error.empty() ? std::string{"no complete reply"} : r.error;
}

}  // namespace

// ---- the renderer -----------------------------------------------------------------------

PromSampleRenderer::PromSampleRenderer(const PromPushOptions& o)
    : name_(o.name),
      metric_(o.metric_name),
      value_field_(o.value_field),
      max_sample_bytes_(o.max_sample_bytes) {
    const auto bad = [&](const std::string& why) {
        throw std::invalid_argument(name_ + ": " + why);
    };
    if (!metric_name_ok(metric_)) {
        bad("'metric_name' must be a Prometheus metric name ([a-zA-Z_:][a-zA-Z0-9_:]*); it is '" +
            shown(metric_) + "'");
    }
    if (metric_ == "push_time_seconds" || metric_ == "push_failure_time_seconds") {
        bad("'metric_name' may not be '" + metric_ +
            "': the gateway writes that metric itself, over any pushed one");
    }
    if (o.push_series == PromPushSeries::All &&
        (metric_ == "labels" || metric_ == "last_push_successful")) {
        bad("'metric_name' may not be '" + metric_ +
            "' with push_series='all': the gateway's query API, which the sink reads on a "
            "restart, names a field of each group so");
    }
    if (value_field_.empty()) {
        bad("'value_field' must not be empty");
    }
    if (o.job.empty()) {
        bad("'job' is required and must not be empty");
    }
    if (!valid_utf8(o.job)) {
        bad("'job' is not valid UTF-8");
    }
    if (!valid_utf8(o.help)) {
        bad("'help' is not valid UTF-8");
    }
    group_["job"] = o.job;
    path_ = "/metrics" + path_segment("job", o.job);
    if (!o.grouping.empty()) {
        std::size_t pos = 0;
        while (pos <= o.grouping.size()) {
            auto comma = o.grouping.find(',', pos);
            if (comma == std::string::npos) {
                comma = o.grouping.size();
            }
            const std::string_view entry{o.grouping.data() + pos, comma - pos};
            const auto eq = entry.find('=');
            if (trim(entry).empty()) {
                bad("'grouping' has an empty entry; it takes name=value pairs separated by commas");
            }
            if (eq == std::string_view::npos) {
                bad("'grouping' has an entry without '=': '" + shown(entry) +
                    "'; it takes name=value pairs separated by commas");
            }
            const std::string label = trim(entry.substr(0, eq));
            const std::string value = trim(entry.substr(eq + 1));
            if (!label_name_ok(label)) {
                bad("'grouping' names '" + shown(label) +
                    "', which is not a Prometheus label name ([a-zA-Z_][a-zA-Z0-9_]*, not "
                    "starting with __)");
            }
            if (label == "job") {
                bad("'grouping' may not name 'job': the 'job' option is the group's job");
            }
            if (group_.contains(label)) {
                bad("'grouping' names '" + label + "' twice");
            }
            if (!valid_utf8(value)) {
                bad("'grouping' has a value for '" + label + "' that is not valid UTF-8");
            }
            group_[label] = value;
            path_ += path_segment(label, value);
            pos = comma + 1;
        }
    }
    if (!o.help.empty()) {
        preamble_ = "# HELP " + metric_ + " " + escape_help(o.help) + "\n";
    }
    preamble_ += "# TYPE " + metric_ + " gauge\n";
}

void PromSampleRenderer::refuse(const std::string& why) const {
    throw UnrenderableRecord(name_ + ": a record " + why);
}

std::string PromSampleRenderer::line(const std::string& labels, double value) const {
    std::string out = metric_;
    out += labels;
    out.push_back(' ');
    out += pushgateway_value_text(value);
    out.push_back('\n');
    return out;
}

std::size_t PromSampleRenderer::line_size(const std::string& labels, double value) const {
    return metric_.size() + labels.size() + pushgateway_value_text(value).size() + 2;
}

PromSampleRenderer::Sample PromSampleRenderer::render(const std::string& record) const {
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
    const std::string vf = "'" + shown(value_field_) + "' (value_field)";

    // The value, read whole.
    const auto vit = obj.find(value_field_);
    if (vit == obj.end()) {
        refuse("has no " + vf);
    }
    Sample s;
    const auto& v = vit->second;
    const auto past_2_53 = [&] {
        refuse("has an integer " + vf +
               " past 2^53, which a gauge (an IEEE double) cannot hold exactly");
    };
    if (v.is_null()) {
        refuse("has a null " + vf);
    } else if (v.is_bool()) {
        s.value = v.as_bool() ? 1.0 : 0.0;
    } else if (v.is_integral_number()) {
        const std::int64_t n = v.as_int();
        if (n > kMaxExactInt || n < -kMaxExactInt) {
            past_2_53();
        }
        s.value = static_cast<double>(n);
    } else if (v.is_number()) {
        s.value = v.as_number();
        if (std::isfinite(s.value) && std::fabs(s.value) >= 9223372036854775808.0) {
            // An integer token past 64 bits reads as a double; tell it from 1e19.
            const auto raw = clink::config::raw_number_tokens(record, {{value_field_, 0}});
            const auto it = raw.find(value_field_);
            if (it != raw.end() && it->second.find_first_of(".eE") == std::string::npos) {
                past_2_53();
            }
        }
    } else if (v.is_string()) {
        const std::string& t = v.as_string();
        bool integral = false;
        if (t == "NaN") {
            s.value = std::numeric_limits<double>::quiet_NaN();
        } else if (t == "Infinity" || t == "+Inf") {
            s.value = std::numeric_limits<double>::infinity();
        } else if (t == "-Infinity" || t == "-Inf") {
            s.value = -std::numeric_limits<double>::infinity();
        } else if (!json_number_text(t, integral)) {
            refuse("has a string " + vf +
                   " that is not a decimal number, NaN, Infinity, -Infinity, +Inf or -Inf");
        } else if (integral) {
            std::int64_t n = 0;
            const auto [ptr, ec] = std::from_chars(t.data(), t.data() + t.size(), n);
            if (ec != std::errc{} || ptr != t.data() + t.size() || n > kMaxExactInt ||
                n < -kMaxExactInt) {
                past_2_53();
            }
            s.value = static_cast<double>(n);
        } else {
            const auto [ptr, ec] = std::from_chars(t.data(), t.data() + t.size(), s.value);
            if (ec != std::errc{} || ptr != t.data() + t.size() || !std::isfinite(s.value)) {
                refuse("has a string " + vf + " outside the range of a double");
            }
        }
    } else {
        refuse("has " + std::string{kind_of(v)} + " in its " + vf + ", which is not a number");
    }

    // The labels: every other field.
    std::map<std::string, std::string> labels;
    std::vector<std::string> numeric;  // number-valued labels, read from the record's text
    std::map<std::string, int> numeric_set;
    for (const auto& [k, f] : obj) {
        if (k == value_field_) {
            continue;
        }
        if (!label_name_ok(k)) {
            refuse("has a field '" + shown(k) +
                   "' whose name is not a Prometheus label name ([a-zA-Z_][a-zA-Z0-9_]*, not "
                   "starting with __)");
        }
        std::optional<std::string> lv;  // nullopt: the label is absent
        if (f.is_null()) {
            // absent
        } else if (f.is_string()) {
            const std::string& t = f.as_string();
            if (!valid_utf8(t)) {
                refuse("has a '" + k + "' label that is not valid UTF-8");
            }
            if (!t.empty()) {
                lv = t;
            }
        } else if (f.is_bool()) {
            lv = f.as_bool() ? "true" : "false";
        } else if (f.is_number()) {
            const double d = f.as_number();
            if (!std::isfinite(d)) {
                // Only a tagged NaN or infinity string reads as one.
                lv = std::isnan(d) ? "NaN" : (d > 0 ? "Infinity" : "-Infinity");
            } else {
                numeric.push_back(k);
                numeric_set.emplace(k, 0);
                lv = std::string{};  // its text, filled in below
            }
        } else {
            refuse("has " + std::string{kind_of(f)} + " in its '" + k +
                   "' field, which cannot be a label value");
        }
        if (lv) {
            labels[k] = std::move(*lv);
        }
    }
    if (!numeric.empty()) {
        // A number's label value is its JSON text exactly as written, so an
        // integer keeps every digit and 1.50 stays 1.50.
        const auto raw = clink::config::raw_number_tokens(record, numeric_set);
        for (const auto& k : numeric) {
            const auto it = raw.find(k);
            if (it == raw.end() || it->second.empty()) {
                refuse("has a number in its '" + k + "' field whose text could not be read");
            }
            labels[k] = it->second;
        }
    }
    // A field named after a grouping label is the path's to carry: absent or
    // equal, it adds nothing; different, the gateway would overwrite it.
    for (const auto& [g, gv] : group_) {
        const auto it = labels.find(g);
        if (it == labels.end()) {
            continue;
        }
        if (it->second != gv) {
            refuse("has a '" + g + "' field that differs from the group's '" + g +
                   "': the gateway would replace it with the group's value");
        }
        labels.erase(it);
    }
    s.labels = render_block(labels);
    if (const auto size = line_size(s.labels, s.value); size > max_sample_bytes_) {
        refuse("makes a sample line of " + std::to_string(size) + " bytes, more than " +
               "max_sample_bytes (" + std::to_string(max_sample_bytes_) + ")");
    }
    return s;
}

std::string PromSampleRenderer::labels_of(
    const std::map<std::string, std::string>& reported) const {
    std::map<std::string, std::string> labels;
    for (const auto& [k, v] : reported) {
        if (const auto g = group_.find(k); g != group_.end()) {
            if (v != g->second) {
                throw std::runtime_error(name_ + ": the gateway holds a series of " + metric_ +
                                         " in the group whose '" + k +
                                         "' differs from the group's");
            }
            continue;
        }
        if (v.empty()) {
            continue;  // absent, as an empty value means; the gateway adds instance=""
        }
        if (!label_name_ok(k) || !valid_utf8(v)) {
            throw std::runtime_error(name_ + ": the gateway holds a series of " + metric_ +
                                     " with a label '" + shown(k) + "' the sink cannot push");
        }
        labels[k] = v;
    }
    return render_block(labels);
}

// ---- responses --------------------------------------------------------------------------

std::string pushgateway_redact(const std::string& body) {
    std::string first = body.substr(0, body.find('\n'));
    if (first.size() > 400) {
        first.resize(400);
    }
    std::string out;
    std::size_t i = 0;
    while (i < first.size()) {
        const char c = first[i];
        if (c == '"') {
            // Skip to the closing quote, past escaped ones.
            ++i;
            while (i < first.size() && first[i] != '"') {
                i += first[i] == '\\' ? std::size_t{2} : std::size_t{1};
            }
            ++i;
            out += "\"...\"";
            continue;
        }
        if (c == '{') {
            int depth = 0;
            bool quoted = false;
            while (i < first.size()) {
                const char d = first[i];
                if (quoted) {
                    if (d == '\\') {
                        ++i;
                    } else if (d == '"') {
                        quoted = false;
                    }
                } else if (d == '"') {
                    quoted = true;
                } else if (d == '{') {
                    ++depth;
                } else if (d == '}' && --depth == 0) {
                    ++i;
                    break;
                }
                ++i;
            }
            out += "{...}";
            continue;
        }
        const auto u = static_cast<unsigned char>(c);
        out.push_back(u >= 0x20 && u < 0x7F ? c : '?');
        ++i;
    }
    return trim(out);
}

PromPushVerdict pushgateway_push_result(const BoundedResponse& r) {
    using Kind = PromPushVerdict::Kind;
    if (r.interrupted || r.failure != BoundedResponse::Failure::None || !r.complete) {
        // A request the gateway received whole may have been applied; resending
        // it replaces the same series with the same values.
        return {Kind::Retry,
                failure_text(r) + (r.request_written ? ", so the push may have been applied"
                                                     : ", so the push was not applied")};
    }
    const std::string status = "HTTP " + std::to_string(r.status);
    const std::string said = pushgateway_redact(r.body);
    const std::string with = said.empty() ? status : status + ": " + said;
    if (r.status == 200) {
        return {Kind::Pushed, {}};
    }
    if (r.status == 202) {
        return {Kind::Pushed, "queued without the gateway's consistency check"};
    }
    if (r.status >= 200 && r.status < 300) {
        return {Kind::Retry,
                status + " is not a confirmation: the gateway answers a push with 200, or 202"};
    }
    if (status_rejects_records(r.status)) {
        return {Kind::Refused, with};
    }
    if (r.status == 401 || r.status == 403 || r.status == 407) {
        return {Kind::Retry, with + " (the credentials were refused)"};
    }
    if (r.status == 404) {
        return {Kind::Retry, with + " (no Pushgateway push API at this URL)"};
    }
    if (r.status == 413) {
        return {Kind::Retry,
                with + " (the push is larger than the server takes; lower batch_bytes)"};
    }
    if (r.status == 429) {
        return {Kind::Retry, with + " (throttled)"};
    }
    return {Kind::Retry, with};
}

std::string pushgateway_value_text(double v) {
    if (std::isnan(v)) {
        return "NaN";
    }
    if (std::isinf(v)) {
        return v > 0 ? "+Inf" : "-Inf";
    }
    char buf[64];
    const auto res = std::to_chars(buf, buf + sizeof(buf), v);  // shortest round trip
    return std::string(buf, res.ptr);
}

// ---- the sink ---------------------------------------------------------------------------

namespace {

PromPushOptions checked(PromPushOptions o) {
    const auto bad = [&](const std::string& why) {
        throw std::invalid_argument(o.name + ": " + why);
    };
    const auto within = [&](const char* key, std::int64_t v, std::int64_t lo, std::int64_t hi) {
        if (v < lo || v > hi) {
            bad(std::string{"'"} + key + "' must be from " + std::to_string(lo) + " to " +
                std::to_string(hi) + "; it is " + std::to_string(v));
        }
    };
    if (o.url.empty()) {
        bad("'url' is required");
    }
    if (const auto scheme = o.url.find("://"); scheme != std::string::npos) {
        const auto authority = o.url.substr(scheme + 3, o.url.find('/', scheme + 3) - scheme - 3);
        if (authority.find('@') != std::string::npos) {
            bad("'url' carries credentials; put them in 'headers' (Authorization) instead");
        }
    }
    if (!HttpRequest::tls_supported() && o.url.rfind("https://", 0) == 0) {
        bad("https URL but this build has no TLS support (rebuild the http_connector module "
            "with OpenSSL)");
    }
    within("batch_records", static_cast<std::int64_t>(o.batch_records), 1, 100000);
    within("max_series", static_cast<std::int64_t>(o.max_series), 1, 1000000);
    within("batch_bytes", static_cast<std::int64_t>(o.batch_bytes), 1024, 268435456);
    within("max_sample_bytes",
           static_cast<std::int64_t>(o.max_sample_bytes),
           64,
           static_cast<std::int64_t>(o.batch_bytes));
    within("max_retries", o.max_retries, 0, RetryPolicy::kMaxRetries);
    within("linger_ms", o.max_age.count(), 0, 3600000);
    within("connect_timeout_ms", o.connect_timeout_ms, 1, 600000);
    within("request_timeout_ms", o.request_timeout_ms, 1, 600000);
    return o;
}

}  // namespace

PrometheusPushSink::PrometheusPushSink(PromPushOptions o)
    : opts_(checked(std::move(o))),
      renderer_(opts_),
      before_write_point_(opts_.name + ".before_write"),
      after_write_point_(opts_.name + ".after_write") {
    if (renderer_.preamble().size() + opts_.max_sample_bytes > opts_.batch_bytes) {
        throw std::invalid_argument(opts_.name +
                                    ": 'batch_bytes' must hold the # HELP and # TYPE lines and "
                                    "one sample of max_sample_bytes");
    }
    body_bytes_ = renderer_.preamble().size();
}

void PrometheusPushSink::open() {
    watch_.reset();  // it watches the client being replaced
    HttpRequest::Options ho;
    ho.base_url = opts_.url;
    ho.headers = parse_headers(opts_.headers);
    ho.verify_tls = opts_.verify_tls;
    ho.connect_timeout_ms = opts_.connect_timeout_ms;
    ho.rw_timeout_ms = opts_.request_timeout_ms;
    client_ = std::make_unique<HttpRequest>(std::move(ho));
    if (this->runtime() != nullptr) {
        watch_ = std::make_unique<CancelWatch>(*client_);
    }
    series_.clear();
    body_bytes_ = renderer_.preamble().size();
    unpushed_records_ = 0;
    unusable_.clear();
    // A run restored from a checkpoint finds the series its predecessor pushed
    // in the group: in All mode it holds them too, or its first push would
    // hide every one the replay does not name again.
    adopt_pending_ = opts_.push_series == PromPushSeries::All && this->runtime() != nullptr &&
                     this->runtime()->restore_from_checkpoint_id() > 0;
}

void PrometheusPushSink::close() {
    watch_.reset();
    client_.reset();
}

void PrometheusPushSink::on_data(const Batch<std::string>& batch) {
    check_usable_();
    for (const auto& rec : batch) {
        ingest_(rec.value());
        if ((opts_.push_series == PromPushSeries::Batch && series_.size() >= opts_.batch_records) ||
            linger_elapsed_()) {
            push_();
        }
    }
}

void PrometheusPushSink::on_barrier(CheckpointBarrier /*b*/) {
    check_usable_();
    // All mode pushes the series it holds at every barrier, changed or not, so a
    // gateway that lost them (restarted without persistence) has them again.
    if (!series_.empty() || adopt_pending_) {
        push_();
    }
}

void PrometheusPushSink::flush() {
    check_usable_();
    if (opts_.push_series == PromPushSeries::Batch ? !series_.empty() : unpushed_records_ > 0) {
        push_();
    }
}

void PrometheusPushSink::ingest_(const std::string& rec) {
    PromSampleRenderer::Sample s;
    try {
        s = renderer_.render(rec);
    } catch (const UnrenderableRecord& e) {
        clink::metrics::connector::error_inc(opts_.name, "sink");
        if (opts_.dlq_policy != DlqPolicy::Drop) {
            // The record is in no push and in no dead letter: a later barrier
            // that succeeded would cover it as pushed.
            unusable_ = e.what();
            throw;
        }
        if (auto* rt = this->runtime(); rt != nullptr) {
            rt->report_bad_record(clink::BadRecord{.payload = rec,
                                                   .error = e.what(),
                                                   .connector = opts_.name,
                                                   .direction = "sink",
                                                   .location = where_()});
        }
        clink::metrics::connector::dropped_records_inc(opts_.name, 1);
        return;
    }
    hold_(s);
}

void PrometheusPushSink::hold_(const PromSampleRenderer::Sample& s) {
    const std::size_t size = renderer_.line_size(s.labels, s.value);
    auto it = series_.find(s.labels);
    std::size_t old = it == series_.end() ? 0 : renderer_.line_size(it->first, it->second);
    if (opts_.push_series == PromPushSeries::Batch) {
        if (!series_.empty() && body_bytes_ - old + size > opts_.batch_bytes) {
            try {
                push_();
            } catch (const std::exception& e) {
                unusable_ =
                    std::string{
                        "a push to make room for a record failed, and the record "
                        "is not held ("} +
                    e.what() + ")";
                throw;
            }
            it = series_.end();
            old = 0;
        }
    } else {
        const char* bound = nullptr;
        if (it == series_.end() && series_.size() >= opts_.max_series) {
            bound = "max_series";
        } else if (body_bytes_ - old + size > opts_.batch_bytes) {
            bound = "batch_bytes";
        }
        if (bound != nullptr) {
            clink::metrics::connector::error_inc(opts_.name, "sink");
            unusable_ = std::string{"a record would take the series the sink holds past "} + bound +
                        " (max_series " + std::to_string(opts_.max_series) + ", batch_bytes " +
                        std::to_string(opts_.batch_bytes) +
                        "), and push_series='all' cannot push part of them; raise " + bound;
            throw std::runtime_error(opts_.name + ": " + unusable_);
        }
    }
    if (unpushed_records_ == 0) {
        first_unpushed_at_ = std::chrono::steady_clock::now();  // linger clock
    }
    series_[s.labels] = s.value;
    body_bytes_ = body_bytes_ - old + size;
    ++unpushed_records_;
}

void PrometheusPushSink::push_() {
    if (adopt_pending_) {
        adopt_group_();
    }
    if (series_.empty()) {
        unpushed_records_ = 0;
        return;
    }
    std::string body;
    body.reserve(body_bytes_);
    body = renderer_.preamble();
    for (const auto& [labels, value] : series_) {
        body += renderer_.line(labels, value);
    }
    const auto t0 = std::chrono::steady_clock::now();
    (void)exchange_("push", renderer_.path(), &body, kMaxReplyBytes);
    (void)CLINK_FAULT_POINT(after_write_point_);
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    if (unpushed_records_ > 0) {
        clink::metrics::connector::records_out_inc(opts_.name, unpushed_records_);
    }
    clink::metrics::connector::bytes_out_inc(opts_.name, body.size());
    clink::metrics::connector::commit_latency_observe(opts_.name, static_cast<std::uint64_t>(dt));
    unpushed_records_ = 0;
    if (opts_.push_series == PromPushSeries::Batch) {
        series_.clear();
        body_bytes_ = renderer_.preamble().size();
    }
}

void PrometheusPushSink::adopt_group_() {
    const auto fail = [&](const std::string& why) {
        clink::metrics::connector::error_inc(opts_.name, "sink");
        throw std::runtime_error(opts_.name + ": on a restart, " + why);
    };
    const auto r =
        exchange_("read of the gateway's series", "/api/v1/metrics", nullptr, kMaxQueryBytes);
    clink::config::JsonValue j;
    try {
        j = clink::config::parse(r.body);
    } catch (const std::exception&) {
        fail("the gateway's /api/v1/metrics answer is not JSON the sink can read");
    }
    const auto* data = j.is_object() ? field(j.as_object(), "data") : nullptr;
    if (data == nullptr || !data->is_array()) {
        fail("the gateway's /api/v1/metrics answer has no 'data' list");
    }
    const auto strings = [&](const clink::config::JsonValue& v) {
        std::map<std::string, std::string> out;
        if (!v.is_object()) {
            fail("the gateway's /api/v1/metrics answer has labels that are not an object");
        }
        for (const auto& [k, s] : v.as_object()) {
            if (!s.is_string()) {
                fail(
                    "the gateway's /api/v1/metrics answer has a label value that is not a "
                    "string");
            }
            out[k] = s.as_string();
        }
        return out;
    };
    std::map<std::string, double> found;
    for (const auto& group : data->as_array()) {
        if (!group.is_object()) {
            continue;
        }
        const auto& g = group.as_object();
        const auto* labels = field(g, "labels");
        if (labels == nullptr || strings(*labels) != renderer_.group()) {
            continue;
        }
        const auto* family = field(g, renderer_.metric());
        if (family == nullptr) {
            break;  // the group holds no series of the metric
        }
        const auto* type = family->is_object() ? field(family->as_object(), "type") : nullptr;
        if (type == nullptr || !type->is_string() || type->as_string() != "GAUGE") {
            fail("the group holds " + renderer_.metric() + " as " +
                 (type != nullptr && type->is_string() ? type->as_string()
                                                       : std::string{"something"}) +
                 ", not as the gauge the sink pushes");
        }
        const auto* metrics = field(family->as_object(), "metrics");
        if (metrics == nullptr || !metrics->is_array()) {
            fail("the gateway's /api/v1/metrics answer has no series list for " +
                 renderer_.metric());
        }
        for (const auto& m : metrics->as_array()) {
            const auto* ml = m.is_object() ? field(m.as_object(), "labels") : nullptr;
            const auto* mv = m.is_object() ? field(m.as_object(), "value") : nullptr;
            if (ml == nullptr || mv == nullptr || !mv->is_string()) {
                fail(
                    "the gateway's /api/v1/metrics answer has a series without labels or a "
                    "value");
            }
            const auto value = reported_value(mv->as_string());
            if (!value) {
                fail("the gateway reports a value of " + renderer_.metric() +
                     " the sink cannot read");
            }
            found[renderer_.labels_of(strings(*ml))] = *value;
        }
        break;
    }
    // What the run received since it opened is newer than what its
    // predecessor left, so it keeps its own value of a series.
    for (const auto& [labels, value] : found) {
        if (series_.emplace(labels, value).second) {
            body_bytes_ += renderer_.line_size(labels, value);
        }
    }
    if (series_.size() > opts_.max_series || body_bytes_ > opts_.batch_bytes) {
        fail("the series the group held and those received since make " +
             std::to_string(series_.size()) + " series in " + std::to_string(body_bytes_) +
             " bytes, more than max_series (" + std::to_string(opts_.max_series) +
             ") or batch_bytes (" + std::to_string(opts_.batch_bytes) + ") allow");
    }
    adopt_pending_ = false;
}

BoundedResponse PrometheusPushSink::exchange_(const char* what,
                                              const std::string& path,
                                              const std::string* body,
                                              std::size_t max_response_bytes) {
    const std::string where = opts_.url + path;
    const std::string pending = body != nullptr ? "; what it carried stays pending" : "";
    std::string why;
    for (int attempt = 0; attempt <= opts_.max_retries; ++attempt) {
        if (attempt > 0) {
            wait_before_retry_(attempt, why);
        }
        if (body != nullptr) {
            (void)CLINK_FAULT_POINT(before_write_point_);
        }
        auto* rt = this->runtime();
        const bool watched = watch_ != nullptr && rt != nullptr;
        if (watched) {
            watch_->arm(rt->cancel_signal(),
                        std::chrono::steady_clock::now() +
                            std::chrono::milliseconds{opts_.request_timeout_ms});
        }
        BoundedRequest req;
        req.max_response_bytes = max_response_bytes;
        BoundedResponse r = body != nullptr ? client_->post_bounded(path, *body, kContentType, req)
                                            : client_->get_bounded(path, req);
        if (watched && watch_->disarm()) {
            if (rt->cancel_requested()) {
                clink::metrics::connector::error_inc(opts_.name, "sink");
                throw std::runtime_error(opts_.name + ": cancelled while a " + what + " to " +
                                         where + " was in flight" + pending);
            }
            r.interrupted = true;  // past request_timeout_ms: as a lost reply
        }
        if (body != nullptr) {
            auto verdict = pushgateway_push_result(r);
            if (verdict.kind == PromPushVerdict::Kind::Pushed) {
                return r;
            }
            if (verdict.kind == PromPushVerdict::Kind::Refused) {
                clink::metrics::connector::error_inc(opts_.name, "sink");
                throw std::runtime_error(opts_.name + ": the gateway refused the push to " + where +
                                         " (" + verdict.detail + "); nothing of it was applied" +
                                         pending);
            }
            why = std::move(verdict.detail);
        } else {
            if (r.complete && !r.interrupted && r.status == 200) {
                return r;
            }
            why = r.complete && !r.interrupted
                      ? "HTTP " + std::to_string(r.status) + ": " + pushgateway_redact(r.body)
                      : failure_text(r);
        }
    }
    clink::metrics::connector::error_inc(opts_.name, "sink");
    throw std::runtime_error(opts_.name + ": " + what + " to " + where + " failed after " +
                             std::to_string(opts_.max_retries + 1) + " attempt(s) (" + why + ")" +
                             pending);
}

void PrometheusPushSink::wait_before_retry_(int attempt, const std::string& why) const {
    const auto until = std::chrono::steady_clock::now() + backoff_delay(attempt, kRetryBase);
    const auto* rt = this->runtime();
    for (;;) {
        if (rt != nullptr && rt->cancel_requested()) {
            throw std::runtime_error(opts_.name + ": cancelled while retrying a request to " +
                                     where_() + " (" + why + ")");
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= until) {
            return;
        }
        std::this_thread::sleep_for(
            std::min<std::chrono::steady_clock::duration>(kCancelPoll, until - now));
    }
}

void PrometheusPushSink::check_usable_() const {
    if (!unusable_.empty()) {
        throw std::runtime_error(opts_.name +
                                 ": the sink threw a record away and keeps no copy of it, so it "
                                 "refuses to push or acknowledge a barrier until it is opened "
                                 "again (" +
                                 unusable_ + ")");
    }
}

bool PrometheusPushSink::linger_elapsed_() const {
    return opts_.max_age.count() > 0 && unpushed_records_ > 0 &&
           std::chrono::steady_clock::now() - first_unpushed_at_ >= opts_.max_age;
}

std::string PrometheusPushSink::where_() const {
    return opts_.url + renderer_.path();
}

}  // namespace clink::http_connector
