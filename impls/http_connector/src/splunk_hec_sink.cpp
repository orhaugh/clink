#include "clink/http_connector/splunk_hec_sink.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <random>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <utility>

#include "clink/config/json.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/logging.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace clink::http_connector {

namespace {

constexpr char kLogComponent[] = "splunk_hec.sink";
constexpr char kContentType[] = "application/json";
constexpr char kChannelHeader[] = "X-Splunk-Request-Channel";
// The deepest nesting a record may have.
constexpr std::size_t kMaxDepth = 100;
// An HEC reply is a short JSON object; anything longer is not HEC's.
constexpr std::size_t kMaxReplyBytes = 64 * 1024;
constexpr std::chrono::milliseconds kRetryBase{200};
// How often a pause looks at the task's cancel signal.
constexpr std::chrono::milliseconds kCancelPoll{50};

// The event endpoints, as the end of `path`; the acknowledgement endpoint
// replaces whichever one `path` ends with.
constexpr std::array<std::string_view, 3> kEventEndpoints{
    "/services/collector/event/1.0", "/services/collector/event", "/services/collector"};
constexpr std::string_view kAckEndpoint = "/services/collector/ack";

// The HEC status codes Splunk documents ("Troubleshoot HTTP Event Collector"),
// each with the HTTP status it comes with.
struct HecCode {
    int code;
    int http;
    const char* text;
};
constexpr std::array<HecCode, 28> kCodes{{
    {0, 200, "Success"},
    {1, 403, "Token disabled"},
    {2, 401, "Token is required"},
    {3, 401, "Invalid authorization"},
    {4, 403, "Invalid token"},
    {5, 400, "No data"},
    {6, 400, "Invalid data format"},
    {7, 400, "Incorrect index"},
    {8, 500, "Internal server error"},
    {9, 503, "Server is busy"},
    {10, 400, "Data channel is missing"},
    {11, 400, "Invalid data channel"},
    {12, 400, "Event field is required"},
    {13, 400, "Event field cannot be blank"},
    {14, 400, "ACK is disabled"},
    {15, 400, "Error in handling indexed fields"},
    {16, 400, "Query string authorization is not enabled"},
    {17, 200, "HEC is healthy"},
    {18, 503, "HEC is unhealthy, queues are full"},
    {19, 503, "HEC is unhealthy, ack service unavailable"},
    {20, 503, "HEC is unhealthy, queues are full, ack service unavailable"},
    {21, 400, "Invalid token"},
    {22, 400, "Token disabled"},
    {23, 503, "Server is shutting down"},
    {24, 200, "HEC queue is approaching its capacity limit"},
    {25, 200, "HEC ACK is approaching its capacity limit"},
    {26, 429, "HEC queue is at capacity and cannot process any more requests"},
    {27, 429, "HEC ACK channel is at capacity and cannot process any more requests"},
}};

const HecCode* find_code(std::int64_t code) {
    for (const auto& c : kCodes) {
        if (c.code == code) {
            return &c;
        }
    }
    return nullptr;
}

// ---- strict JSON, for the records ---------------------------------------------------------

// The length of the well-formed UTF-8 sequence starting at `i` (Unicode table
// 3-7: no overlong form, no surrogate, nothing past U+10FFFF), or 0.
std::size_t utf8_sequence(std::string_view s, std::size_t i) {
    const auto at = [&](std::size_t k) { return static_cast<unsigned char>(s[k]); };
    const unsigned char c = at(i);
    if (c < 0x80) {
        return 1;
    }
    std::size_t len = 0;
    unsigned char lo = 0x80;
    unsigned char hi = 0xBF;
    if (c >= 0xC2 && c <= 0xDF) {
        len = 2;
    } else if (c >= 0xE0 && c <= 0xEF) {
        len = 3;
        lo = c == 0xE0 ? 0xA0 : 0x80;
        hi = c == 0xED ? 0x9F : 0xBF;
    } else if (c >= 0xF0 && c <= 0xF4) {
        len = 4;
        lo = c == 0xF0 ? 0x90 : 0x80;
        hi = c == 0xF4 ? 0x8F : 0xBF;
    } else {
        return 0;
    }
    if (i + len > s.size()) {
        return 0;
    }
    if (at(i + 1) < lo || at(i + 1) > hi) {
        return 0;
    }
    for (std::size_t k = 2; k < len; ++k) {
        if (at(i + k) < 0x80 || at(i + k) > 0xBF) {
            return 0;
        }
    }
    return len;
}

bool valid_utf8(std::string_view s) {
    for (std::size_t i = 0; i < s.size();) {
        const std::size_t n = utf8_sequence(s, i);
        if (n == 0) {
            return false;
        }
        i += n;
    }
    return true;
}

void append_utf8(std::string& out, std::uint32_t cp) {
    if (cp < 0x80) {
        out.push_back(static_cast<char>(cp));
    } else if (cp < 0x800) {
        out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp < 0x10000) {
        out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
}

// Reads one JSON object and writes it to `out` without the whitespace between
// its tokens, every token otherwise copied as written. run() returns why the
// text is not one, or "" when it is; `out` is then incomplete, and the caller
// discards it. Messages give byte offsets, never content.
class EventScanner {
public:
    EventScanner(std::string_view in, std::string& out) : in_(in), out_(out) {}

    std::string run() {
        skip_ws_();
        if (pos_ == in_.size()) {
            return "is empty or only whitespace";
        }
        switch (in_[pos_]) {
            case '{':
                break;
            case '[':
                return "is a JSON array, not an object";
            case '"':
                return "is a JSON string, not an object";
            case 't':
            case 'f':
                return "is a JSON boolean, not an object";
            case 'n':
                return "is JSON null, not an object";
            default:
                if (in_[pos_] == '-' || (in_[pos_] >= '0' && in_[pos_] <= '9')) {
                    return "is a JSON number, not an object";
                }
                return "is not JSON (byte " + std::to_string(pos_) + ")";
        }
        if (!object_(1)) {
            return error_;
        }
        skip_ws_();
        if (pos_ != in_.size()) {
            return "has more after its object (byte " + std::to_string(pos_) + ")";
        }
        return {};
    }

private:
    bool fail_(const char* why) {
        error_ = std::string{why} + " (byte " + std::to_string(std::min(pos_, in_.size())) + ")";
        return false;
    }
    [[nodiscard]] bool at_end_() const { return pos_ >= in_.size(); }
    [[nodiscard]] char peek_() const { return at_end_() ? '\0' : in_[pos_]; }

    void skip_ws_() {
        while (!at_end_() &&
               (in_[pos_] == ' ' || in_[pos_] == '\t' || in_[pos_] == '\n' || in_[pos_] == '\r')) {
            ++pos_;
        }
    }

    bool object_(std::size_t depth) {
        if (depth > kMaxDepth) {
            return fail_("nests more than 100 levels deep");
        }
        const std::size_t start = pos_;
        out_.push_back('{');
        ++pos_;
        skip_ws_();
        if (peek_() == '}') {
            out_.push_back('}');
            ++pos_;
            return true;
        }
        std::vector<std::string> keys;
        for (;;) {
            if (peek_() != '"') {
                return fail_(at_end_() ? "ends inside an object"
                                       : "has an object key that is not a string");
            }
            std::string key;
            if (!string_(&key)) {
                return false;
            }
            keys.push_back(std::move(key));
            skip_ws_();
            if (peek_() != ':') {
                return fail_(at_end_() ? "ends inside an object" : "lacks ':' after an object key");
            }
            out_.push_back(':');
            ++pos_;
            skip_ws_();
            if (!value_(depth)) {
                return false;
            }
            skip_ws_();
            if (peek_() == ',') {
                out_.push_back(',');
                ++pos_;
                skip_ws_();
                continue;
            }
            if (peek_() == '}') {
                out_.push_back('}');
                ++pos_;
                break;
            }
            return fail_(at_end_() ? "ends inside an object" : "lacks ',' or '}' in an object");
        }
        std::sort(keys.begin(), keys.end());
        if (std::adjacent_find(keys.begin(), keys.end()) != keys.end()) {
            pos_ = start;
            return fail_("repeats a key in the object that starts here");
        }
        return true;
    }

    bool array_(std::size_t depth) {
        if (depth > kMaxDepth) {
            return fail_("nests more than 100 levels deep");
        }
        out_.push_back('[');
        ++pos_;
        skip_ws_();
        if (peek_() == ']') {
            out_.push_back(']');
            ++pos_;
            return true;
        }
        for (;;) {
            if (!value_(depth)) {
                return false;
            }
            skip_ws_();
            if (peek_() == ',') {
                out_.push_back(',');
                ++pos_;
                skip_ws_();
                continue;
            }
            if (peek_() == ']') {
                out_.push_back(']');
                ++pos_;
                return true;
            }
            return fail_(at_end_() ? "ends inside an array" : "lacks ',' or ']' in an array");
        }
    }

    bool value_(std::size_t depth) {
        switch (peek_()) {
            case '{':
                return object_(depth + 1);
            case '[':
                return array_(depth + 1);
            case '"':
                return string_(nullptr);
            case 't':
                return literal_("true");
            case 'f':
                return literal_("false");
            case 'n':
                return literal_("null");
            default:
                break;
        }
        if (peek_() == '-' || (peek_() >= '0' && peek_() <= '9')) {
            return number_();
        }
        return fail_(at_end_() ? "ends where a value should be"
                               : "has something that is not a JSON value");
    }

    bool literal_(std::string_view word) {
        if (in_.substr(pos_, word.size()) != word) {
            return fail_("has something that is not a JSON value");
        }
        out_.append(word);
        pos_ += word.size();
        return true;
    }

    // -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?, copied as written.
    bool number_() {
        const std::size_t start = pos_;
        const auto digits = [this] {
            const std::size_t from = pos_;
            while (!at_end_() && in_[pos_] >= '0' && in_[pos_] <= '9') {
                ++pos_;
            }
            return pos_ - from;
        };
        if (peek_() == '-') {
            ++pos_;
        }
        if (peek_() == '0') {
            ++pos_;
        } else if (digits() == 0) {
            return fail_("has a malformed number");
        }
        if (peek_() == '.') {
            ++pos_;
            if (digits() == 0) {
                return fail_("has a malformed number");
            }
        }
        if (peek_() == 'e' || peek_() == 'E') {
            ++pos_;
            if (peek_() == '+' || peek_() == '-') {
                ++pos_;
            }
            if (digits() == 0) {
                return fail_("has a malformed number");
            }
        }
        out_.append(in_.substr(start, pos_ - start));
        return true;
    }

    bool hex4_(std::size_t at, std::uint32_t& cp) const {
        if (at + 4 > in_.size()) {
            return false;
        }
        cp = 0;
        for (std::size_t k = at; k < at + 4; ++k) {
            const char c = in_[k];
            cp <<= 4;
            if (c >= '0' && c <= '9') {
                cp |= static_cast<std::uint32_t>(c - '0');
            } else if (c >= 'a' && c <= 'f') {
                cp |= static_cast<std::uint32_t>(c - 'a' + 10);
            } else if (c >= 'A' && c <= 'F') {
                cp |= static_cast<std::uint32_t>(c - 'A' + 10);
            } else {
                return false;
            }
        }
        return true;
    }

    // A string, copied as written; `decoded`, when given, receives its value.
    bool string_(std::string* decoded) {
        const std::size_t start = pos_;
        ++pos_;  // the opening quote
        while (!at_end_()) {
            const auto c = static_cast<unsigned char>(in_[pos_]);
            if (c == '"') {
                ++pos_;
                out_.append(in_.substr(start, pos_ - start));
                return true;
            }
            if (c < 0x20) {
                return fail_("has a raw control character in a string");
            }
            if (c == '\\') {
                if (pos_ + 1 >= in_.size()) {
                    break;
                }
                const char e = in_[pos_ + 1];
                char plain = 0;
                switch (e) {
                    case '"':
                    case '\\':
                    case '/':
                        plain = e;
                        break;
                    case 'b':
                        plain = '\b';
                        break;
                    case 'f':
                        plain = '\f';
                        break;
                    case 'n':
                        plain = '\n';
                        break;
                    case 'r':
                        plain = '\r';
                        break;
                    case 't':
                        plain = '\t';
                        break;
                    case 'u': {
                        std::uint32_t cp = 0;
                        if (!hex4_(pos_ + 2, cp)) {
                            return fail_("has a malformed \\u escape in a string");
                        }
                        pos_ += 6;
                        if (cp >= 0xDC00 && cp <= 0xDFFF) {
                            return fail_("has an unpaired surrogate escape in a string");
                        }
                        if (cp >= 0xD800 && cp <= 0xDBFF) {
                            std::uint32_t low = 0;
                            if (pos_ + 1 >= in_.size() || in_[pos_] != '\\' ||
                                in_[pos_ + 1] != 'u' || !hex4_(pos_ + 2, low) || low < 0xDC00 ||
                                low > 0xDFFF) {
                                return fail_("has an unpaired surrogate escape in a string");
                            }
                            pos_ += 6;
                            cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                        }
                        if (decoded != nullptr) {
                            append_utf8(*decoded, cp);
                        }
                        continue;
                    }
                    default:
                        return fail_("has an invalid escape in a string");
                }
                if (decoded != nullptr) {
                    decoded->push_back(plain);
                }
                pos_ += 2;
                continue;
            }
            const std::size_t n = utf8_sequence(in_, pos_);
            if (n == 0) {
                return fail_("is not valid UTF-8");
            }
            if (decoded != nullptr) {
                decoded->append(in_.substr(pos_, n));
            }
            pos_ += n;
        }
        return fail_("ends inside a string");
    }

    std::string_view in_;
    std::string& out_;
    std::size_t pos_{0};
    std::string error_;
};

// ---- replies ----------------------------------------------------------------------------

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
            return "the reply was larger than any HEC sends";
        case BoundedResponse::Failure::Other:
        case BoundedResponse::Failure::None:
            break;
    }
    return r.error.empty() ? std::string{"no complete reply"} : r.error;
}

// Printable ASCII of a server's text, at most 120 characters.
std::string printable(std::string_view s) {
    std::string out;
    for (const char c : s.substr(0, 120)) {
        const auto u = static_cast<unsigned char>(c);
        out.push_back(u >= 0x20 && u < 0x7F ? c : '?');
    }
    return out;
}

// A reply body read as HEC's: an object whose "code" is an integer.
struct HecReply {
    std::int64_t code{0};
    std::string text;
    const clink::config::JsonObject* fields{nullptr};
};

std::optional<HecReply> hec_reply(const clink::config::JsonValue& v) {
    if (!v.is_object()) {
        return std::nullopt;
    }
    const auto& o = v.as_object();
    const auto code = o.find("code");
    if (code == o.end() || !code->second.is_integral_number()) {
        return std::nullopt;
    }
    HecReply out;
    out.code = code->second.as_int();
    if (const auto text = o.find("text"); text != o.end() && text->second.is_string()) {
        out.text = text->second.as_string();
    }
    out.fields = &o;
    return out;
}

std::optional<clink::config::JsonValue> parsed(const std::string& body) {
    try {
        return clink::config::parse(body);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::string described(const BoundedResponse& r, const std::optional<HecReply>& hec) {
    std::string out = "HTTP " + std::to_string(r.status);
    if (hec) {
        out += ", HEC code " + std::to_string(hec->code);
        if (!hec->text.empty()) {
            out += " (" + printable(hec->text) + ")";
        } else if (const auto* known = find_code(hec->code)) {
            out += std::string{" ("} + known->text + ")";
        }
        return out;
    }
    if (r.body.empty()) {
        return out + " with an empty body";
    }
    const auto ct = r.headers.find("content-type");
    return out + " with a " +
           (ct == r.headers.end() ? std::string{"body"} : printable(ct->second) + " body") +
           " of " + std::to_string(r.body.size()) + " bytes that is not an HEC reply";
}

// An unsigned decimal integer, from a JSON integer or a string of digits.
std::optional<std::uint64_t> ack_number(const clink::config::JsonValue& v) {
    if (v.is_integral_number()) {
        return v.as_int() < 0
                   ? std::nullopt
                   : std::optional<std::uint64_t>{static_cast<std::uint64_t>(v.as_int())};
    }
    if (v.is_string()) {
        const auto& s = v.as_string();
        std::uint64_t out = 0;
        const auto* end = s.data() + s.size();
        const auto [ptr, ec] = std::from_chars(s.data(), end, out);
        if (!s.empty() && ec == std::errc{} && ptr == end) {
            return out;
        }
    }
    return std::nullopt;
}

std::string auth_refused(const std::string& said) {
    return "HEC refused the token (" + said +
           "): check that the token exists, is enabled and is the one configured";
}

// ---- options ----------------------------------------------------------------------------

std::string lower(std::string_view s) {
    std::string out(s);
    for (auto& c : out) {
        if (c >= 'A' && c <= 'Z') {
            c = static_cast<char>(c - 'A' + 'a');
        }
    }
    return out;
}

bool has_control(std::string_view s) {
    return std::any_of(s.begin(), s.end(), [](char c) {
        const auto u = static_cast<unsigned char>(c);
        return u < 0x20 || u == 0x7F;
    });
}

SplunkHecOptions checked(SplunkHecOptions o) {
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
    const bool https = o.url.rfind("https://", 0) == 0;
    if (!https && o.url.rfind("http://", 0) != 0) {
        bad("'url' must start with http:// or https://");
    }
    {
        const auto from = o.url.find("://") + 3;
        const auto slash = o.url.find('/', from);
        const auto authority = o.url.substr(from, slash - from);
        if (authority.empty()) {
            bad("'url' names no host");
        }
        if (authority.find('@') != std::string::npos) {
            bad("'url' carries credentials; the token goes in 'token'");
        }
        if (o.url.find_first_of("?#") != std::string::npos) {
            bad("'url' carries a query or fragment; it is scheme://host[:port], and the token "
                "goes in 'token'");
        }
        if (slash != std::string::npos && slash + 1 != o.url.size()) {
            bad("'url' carries a path; it is scheme://host[:port], and the endpoint goes in "
                "'path'");
        }
        if (slash != std::string::npos) {
            o.url.pop_back();  // the one trailing '/'
        }
        if (has_control(o.url) || o.url.find(' ') != std::string::npos) {
            bad("'url' holds a space or a control character");
        }
    }
    if (https && !HttpRequest::tls_supported()) {
        bad("https URL but this build has no TLS support (rebuild the http_connector module "
            "with OpenSSL)");
    }
    if (o.token.empty()) {
        bad("'token' is required");
    }
    if (std::any_of(o.token.begin(), o.token.end(), [](char c) {
            const auto u = static_cast<unsigned char>(c);
            return u <= 0x20 || u >= 0x7F;
        })) {
        bad("'token' must be printable ASCII with no space");
    }
    if (o.path.empty() || o.path[0] != '/' || o.path.find_first_of("?# ") != std::string::npos ||
        has_control(o.path)) {
        bad("'path' must start with '/' and hold no query, fragment, space or control character");
    }
    if (std::none_of(kEventEndpoints.begin(), kEventEndpoints.end(), [&](std::string_view e) {
            return o.path.size() >= e.size() &&
                   o.path.compare(o.path.size() - e.size(), e.size(), e) == 0;
        })) {
        bad("'path' must end with /services/collector/event, /services/collector/event/1.0 or "
            "/services/collector: the sink writes JSON events, not raw text, and the health and "
            "acknowledgement endpoints take no events");
    }
    for (const auto& [key, value] : parse_headers(o.headers)) {
        const std::string k = lower(key);
        if (k == "authorization") {
            bad("'headers' sets Authorization, which the sink sets from 'token'; two credentials "
                "would leave it unclear which HEC reads");
        }
        if (k == lower(kChannelHeader)) {
            bad("'headers' sets X-Splunk-Request-Channel; ack='indexer' chooses the channel");
        }
        if (k == "content-type" || k == "content-length" || k == "transfer-encoding" ||
            k == "host") {
            bad("'headers' sets " + key + ", which the request sets itself");
        }
        if (has_control(key) || has_control(value)) {
            bad("'headers' holds a control character");
        }
    }
    within("batch_records", static_cast<std::int64_t>(o.batch_records), 1, 100000);
    within("batch_bytes", static_cast<std::int64_t>(o.batch_bytes), 1024, 268435456);
    within("max_retries", o.max_retries, 0, RetryPolicy::kMaxRetries);
    within("linger_ms", o.max_age.count(), 0, 3600000);
    within("connect_timeout_ms", o.connect_timeout_ms, 1, 600000);
    within("request_timeout_ms", o.request_timeout_ms, 1, 600000);
    within("max_pending_acks", static_cast<std::int64_t>(o.max_pending_acks), 1, 10000);
    within("ack_poll_ms", o.ack_poll_interval.count(), 10, 60000);
    within("ack_timeout_ms", o.ack_timeout.count(), 1000, 3600000);
    return o;
}

std::string ack_path_for(const std::string& path) {
    for (const auto e : kEventEndpoints) {
        if (path.size() >= e.size() && path.compare(path.size() - e.size(), e.size(), e) == 0) {
            return path.substr(0, path.size() - e.size()) + std::string{kAckEndpoint};
        }
    }
    return std::string{kAckEndpoint};
}

// A random (version 4) GUID, upper-case as Splunk's examples write one.
std::string new_channel() {
    std::random_device rd;
    std::seed_seq seed{
        rd(),
        rd(),
        rd(),
        rd(),
        static_cast<unsigned>(std::chrono::steady_clock::now().time_since_epoch().count())};
    std::mt19937_64 gen(seed);
    std::array<unsigned char, 16> b{};
    for (std::size_t i = 0; i < b.size(); i += 8) {
        const std::uint64_t r = gen();
        for (std::size_t k = 0; k < 8; ++k) {
            b[i + k] = static_cast<unsigned char>(r >> (8 * k));
        }
    }
    b[6] = static_cast<unsigned char>((b[6] & 0x0F) | 0x40);
    b[8] = static_cast<unsigned char>((b[8] & 0x3F) | 0x80);
    constexpr char kHex[] = "0123456789ABCDEF";
    std::string out;
    for (std::size_t i = 0; i < b.size(); ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            out.push_back('-');
        }
        out.push_back(kHex[b[i] >> 4]);
        out.push_back(kHex[b[i] & 0x0F]);
    }
    return out;
}

}  // namespace

// ---- the renderer -----------------------------------------------------------------------

HecEventRenderer::HecEventRenderer(const SplunkHecOptions& o)
    : name_(o.name), max_envelope_bytes_(o.batch_bytes) {
    clink::config::JsonObject meta;
    const auto take = [&](const char* key, const std::string& v) {
        if (v.empty()) {
            return;
        }
        if (!valid_utf8(v) || has_control(v)) {
            throw std::invalid_argument(name_ + ": '" + key +
                                        "' must be valid UTF-8 with no control character");
        }
        meta[key] = clink::config::JsonValue{v};
    };
    take("host", o.host);
    take("index", o.index);
    take("source", o.source);
    take("sourcetype", o.sourcetype);
    if (!meta.empty()) {
        const std::string m = clink::config::JsonValue{std::move(meta)}.serialize(0);
        metadata_ = "," + m.substr(1, m.size() - 2);
    }
    // {"event":{}<metadata>}: the smallest envelope must fit one request.
    if (std::string_view{"{\"event\":{}}"}.size() + metadata_.size() > max_envelope_bytes_) {
        throw std::invalid_argument(name_ + ": 'batch_bytes' (" +
                                    std::to_string(max_envelope_bytes_) +
                                    ") cannot hold the metadata and an event");
    }
}

void HecEventRenderer::render(std::string& out, std::string_view record) const {
    std::string envelope;
    envelope.reserve(record.size() + metadata_.size() + 11);
    envelope += "{\"event\":";
    const std::string why = EventScanner(record, envelope).run();
    if (!why.empty()) {
        throw UnrenderableRecord(name_ + ": a record " + why +
                                 "; each record must be exactly one JSON object");
    }
    envelope += metadata_;
    envelope.push_back('}');
    if (envelope.size() > max_envelope_bytes_) {
        throw UnrenderableRecord(name_ + ": a record's envelope is " +
                                 std::to_string(envelope.size()) +
                                 " bytes, more than one request " + "may carry (batch_bytes, " +
                                 std::to_string(max_envelope_bytes_) + ")");
    }
    out += envelope;
}

// ---- replies ----------------------------------------------------------------------------

std::string splunk_hec_describe(const BoundedResponse& r) {
    if (r.interrupted || r.failure != BoundedResponse::Failure::None || !r.complete) {
        return failure_text(r);
    }
    const auto v = parsed(r.body);
    return described(r, v ? hec_reply(*v) : std::nullopt);
}

HecEventVerdict splunk_hec_event_result(const BoundedResponse& r,
                                        std::size_t events,
                                        SplunkHecAck ack) {
    using Kind = HecEventVerdict::Kind;
    const auto verdict = [](Kind k, std::string detail) {
        HecEventVerdict v;
        v.kind = k;
        v.detail = std::move(detail);
        return v;
    };
    if (r.interrupted || r.failure != BoundedResponse::Failure::None || !r.complete) {
        return verdict(
            Kind::Retry,
            failure_text(r) + (r.request_written
                                   ? ", so HEC may have accepted some or all of the events"
                                   : ", so HEC received none of the events"));
    }
    const auto v = parsed(r.body);
    const auto hec = v ? hec_reply(*v) : std::nullopt;
    const std::string said = described(r, hec);
    const HecCode* known = hec ? find_code(hec->code) : nullptr;

    if (r.status == 429 || r.status >= 500) {
        // A busy server takes events into its queue until the queue blocks and
        // then gives up on the rest, so some of the request may be in.
        return verdict(Kind::Retry, said + ", so HEC may have accepted some of the events");
    }
    if (r.status == 200) {
        if (!hec) {
            return verdict(Kind::Failed,
                           said +
                               ": HEC answers an accepted request with {\"text\":\"Success\","
                               "\"code\":0}, so this is not counted as accepted");
        }
        if (hec->code != 0 && hec->code != 24 && hec->code != 25) {
            return verdict(Kind::Failed, said + ", which is not an acceptance of the events");
        }
        const auto& o = *hec->fields;
        const auto id = o.find("ackId");
        const auto id_doc = o.find("ackID");  // the spelling Splunk's documentation shows
        std::optional<std::uint64_t> ack_id;
        if (id != o.end() || id_doc != o.end()) {
            const auto a = id != o.end() ? ack_number(id->second) : std::nullopt;
            const auto b = id_doc != o.end() ? ack_number(id_doc->second) : std::nullopt;
            if ((id != o.end() && !a) || (id_doc != o.end() && !b) || (a && b && *a != *b)) {
                return verdict(Kind::Failed,
                               said + " with an ackId that is not one non-negative integer");
            }
            ack_id = a ? a : b;
        }
        if (ack == SplunkHecAck::Indexer && !ack_id) {
            return verdict(Kind::Failed,
                           said +
                               " without an ackId: the token does not use indexer "
                               "acknowledgement, so nothing can confirm the events were indexed; "
                               "enable it on the token, or set ack='none'");
        }
        if (ack == SplunkHecAck::None && ack_id) {
            return verdict(Kind::Failed,
                           said +
                               " with an ackId: the token uses indexer acknowledgement, which "
                               "ack='none' does not wait for; set ack='indexer'");
        }
        HecEventVerdict out = verdict(Kind::Accepted, hec->code == 0 ? std::string{} : said);
        out.ack_id = ack_id;
        return out;
    }
    if (r.status == 401 || r.status == 403 ||
        (r.status == 400 && hec && (hec->code == 21 || hec->code == 22))) {
        return verdict(Kind::Failed, auth_refused(said));
    }
    if (r.status == 400 && hec) {
        if (known != nullptr && known->http != 400) {
            return verdict(
                Kind::Failed,
                said + ", a code HEC sends with HTTP " + std::to_string(known->http) + ", not 400");
        }
        switch (hec->code) {
            case 6:
            case 12:
            case 13: {
                const auto& o = *hec->fields;
                const auto n = o.find("invalid-event-number");
                if (n == o.end() || !n->second.is_integral_number() || n->second.as_int() < 0 ||
                    static_cast<std::uint64_t>(n->second.as_int()) >= events) {
                    return verdict(Kind::Failed,
                                   said + " naming no event of the " + std::to_string(events) +
                                       " the request carried, so no event can be set aside");
                }
                HecEventVerdict out = verdict(Kind::RefusedEvent, said);
                out.event = static_cast<std::size_t>(n->second.as_int());
                return out;
            }
            case 7:
                return verdict(Kind::Failed,
                               said +
                                   ": the index does not exist or the token may not write to it; "
                                   "check 'index' and the token's allowed indexes");
            case 10:
                return verdict(Kind::Failed,
                               said +
                                   ": the token requires indexer acknowledgement, which needs a "
                                   "channel; set ack='indexer'");
            case 11:
                return verdict(Kind::Failed, said + ": HEC did not accept the channel");
            case 14:
                return verdict(Kind::Failed,
                               said +
                                   ": the token does not use indexer acknowledgement; set "
                                   "ack='none', or enable it on the token");
            default:
                return verdict(Kind::Failed, said);
        }
    }
    if (r.status == 413) {
        return verdict(Kind::Failed,
                       said +
                           ": the request is larger than the server takes (its "
                           "max_content_length); lower batch_bytes");
    }
    if (r.status == 404) {
        return verdict(Kind::Failed, said + ": no HEC event endpoint at this URL and path");
    }
    return verdict(Kind::Failed, said);
}

HecAckVerdict splunk_hec_ack_result(const BoundedResponse& r,
                                    const std::vector<std::uint64_t>& asked) {
    using Kind = HecAckVerdict::Kind;
    HecAckVerdict out;
    if (r.interrupted || r.failure != BoundedResponse::Failure::None || !r.complete) {
        out.kind = Kind::Retry;
        out.detail = failure_text(r);
        return out;
    }
    const auto v = parsed(r.body);
    const auto hec = v ? hec_reply(*v) : std::nullopt;
    const std::string said = described(r, hec);
    if (r.status == 429 || r.status >= 500) {
        out.kind = Kind::Retry;
        out.detail = said;
        return out;
    }
    if (r.status == 401 || r.status == 403 ||
        (r.status == 400 && hec && (hec->code == 21 || hec->code == 22))) {
        out.detail = auth_refused(said);
        return out;
    }
    if (r.status == 400 && hec && hec->code == 14) {
        out.detail = said +
                     ": the token does not use indexer acknowledgement; set ack='none', or "
                     "enable it on the token";
        return out;
    }
    if (r.status != 200) {
        out.detail = said;
        return out;
    }
    const auto malformed = [&](const std::string& why) {
        out.kind = Kind::Failed;
        out.indexed.clear();
        out.detail = "HTTP 200 with an acknowledgement reply that " + why;
        return out;
    };
    if (!v || !v->is_object()) {
        return malformed("is not a JSON object");
    }
    const auto acks = v->as_object().find("acks");
    if (acks == v->as_object().end() || !acks->second.is_object()) {
        return malformed("has no \"acks\" object");
    }
    for (const auto& [key, value] : acks->second.as_object()) {
        std::uint64_t id = 0;
        const auto* end = key.data() + key.size();
        const auto [ptr, ec] = std::from_chars(key.data(), end, id);
        if (key.empty() || ec != std::errc{} || ptr != end) {
            return malformed("names an ackId that is not a number");
        }
        if (std::find(asked.begin(), asked.end(), id) == asked.end()) {
            return malformed("reports ackId " + std::to_string(id) + ", which was not asked about");
        }
        if (!value.is_bool()) {
            return malformed("reports ackId " + std::to_string(id) + " as neither true nor false");
        }
        if (value.as_bool()) {
            out.indexed.push_back(id);
        }
    }
    // An asked ackId the reply leaves out has, as false does, no status yet.
    out.kind = Kind::Read;
    return out;
}

// ---- the sink ---------------------------------------------------------------------------

SplunkHecSink::SplunkHecSink(SplunkHecOptions o)
    : opts_(checked(std::move(o))),
      renderer_(opts_),
      ack_path_(ack_path_for(opts_.path)),
      before_write_point_(opts_.name + ".before_write"),
      after_write_point_(opts_.name + ".after_write") {}

void SplunkHecSink::open() {
    watch_.reset();  // it watches the client being replaced
    HttpRequest::Options ho;
    ho.base_url = opts_.url;
    ho.headers = parse_headers(opts_.headers);
    // HEC's scheme is the word "Splunk", not "Bearer".
    ho.headers["Authorization"] = "Splunk " + opts_.token;
    channel_.clear();
    if (opts_.ack == SplunkHecAck::Indexer) {
        // A new channel per run: the acknowledgements a previous run awaited
        // belong to its channel, and its events come again by replay.
        channel_ = new_channel();
        ho.headers[kChannelHeader] = channel_;
    }
    ho.verify_tls = opts_.verify_tls;
    ho.connect_timeout_ms = opts_.connect_timeout_ms;
    ho.rw_timeout_ms = opts_.request_timeout_ms;
    client_ = std::make_unique<HttpRequest>(std::move(ho));
    if (this->runtime() != nullptr) {
        watch_ = std::make_unique<CancelWatch>(*client_);
    }
    events_.clear();
    pending_bytes_ = 0;
    unacked_.clear();
    unusable_.clear();
}

void SplunkHecSink::close() {
    watch_.reset();
    client_.reset();
}

void SplunkHecSink::on_data(const Batch<std::string>& batch) {
    check_usable_();
    for (const auto& rec : batch) {
        append_(rec.value());
        while (!events_.empty() &&
               (events_.size() >= opts_.batch_records || pending_bytes_ >= opts_.batch_bytes)) {
            send_();
        }
        if (linger_elapsed_()) {
            send_();
        }
    }
}

void SplunkHecSink::on_barrier(CheckpointBarrier /*b*/) {
    check_usable_();
    while (!events_.empty()) {
        send_();
    }
    if (opts_.ack == SplunkHecAck::Indexer) {
        await_acks_(0);
    }
}

void SplunkHecSink::flush() {
    check_usable_();
    while (!events_.empty()) {
        send_();
    }
    if (opts_.ack == SplunkHecAck::Indexer) {
        await_acks_(0);
    }
}

void SplunkHecSink::append_(const std::string& rec) {
    std::string envelope;
    try {
        renderer_.render(envelope, rec);
    } catch (const UnrenderableRecord& e) {
        clink::metrics::connector::error_inc(opts_.name, "sink");
        if (opts_.dlq_policy != DlqPolicy::Drop) {
            // The record is in no request and in no dead letter: a later
            // barrier that passed would cover it as written.
            unusable_ = e.what();
            throw;
        }
        if (auto* rt = this->runtime(); rt != nullptr) {
            rt->report_bad_record(clink::BadRecord{.payload = rec,
                                                   .error = e.what(),
                                                   .connector = opts_.name,
                                                   .direction = "sink",
                                                   .location = opts_.url + opts_.path});
        }
        clink::metrics::connector::dropped_records_inc(opts_.name, 1);
        return;
    }
    if (events_.empty()) {
        first_buffered_at_ = std::chrono::steady_clock::now();
    }
    pending_bytes_ += envelope.size() + (events_.empty() ? std::size_t{0} : std::size_t{1});
    events_.push_back(std::move(envelope));
}

// The body all buffered events would make, one envelope per line.
void SplunkHecSink::recount_() {
    pending_bytes_ = events_.empty() ? std::size_t{0} : events_.size() - 1;
    for (const auto& e : events_) {
        pending_bytes_ += e.size();
    }
}

// Sends the longest run of buffered events one request may carry, until HEC
// accepts it or the sink throws with the events kept.
void SplunkHecSink::send_() {
    if (opts_.ack == SplunkHecAck::Indexer) {
        await_acks_(opts_.max_pending_acks - 1);
    }
    std::size_t n = 0;
    std::size_t bytes = 0;
    while (n < events_.size() && n < opts_.batch_records) {
        const std::size_t next =
            bytes + events_[n].size() + (n == 0 ? std::size_t{0} : std::size_t{1});
        if (n > 0 && next > opts_.batch_bytes) {
            break;
        }
        bytes = next;
        ++n;
    }
    const std::string where = opts_.url + opts_.path;
    const auto t0 = std::chrono::steady_clock::now();
    int retries = 0;
    for (;;) {
        std::string body;
        body.reserve(bytes);
        for (std::size_t i = 0; i < n; ++i) {
            if (i > 0) {
                body.push_back('\n');
            }
            body += events_[i];
        }
        (void)CLINK_FAULT_POINT(before_write_point_);
        const BoundedResponse r = exchange_(opts_.path, body);
        HecEventVerdict v = splunk_hec_event_result(r, n, opts_.ack);
        switch (v.kind) {
            case HecEventVerdict::Kind::Accepted: {
                if (opts_.ack == SplunkHecAck::Indexer) {
                    if (std::any_of(unacked_.begin(), unacked_.end(), [&](const Unacked& u) {
                            return u.ack_id == *v.ack_id;
                        })) {
                        // Whichever request a true status for that ackId meant,
                        // it would confirm both.
                        unusable_ = "HEC accepted a request to " + where + " under ackId " +
                                    std::to_string(*v.ack_id) +
                                    ", which an earlier request awaiting acknowledgement holds, "
                                    "so their acknowledgements cannot be told apart";
                        fail_(unusable_);
                    }
                    unacked_.push_back(Unacked{*v.ack_id, n, std::chrono::steady_clock::now()});
                }
                if (!v.detail.empty()) {
                    clink::log::warn(kLogComponent,
                                     opts_.name + ": HEC accepted a request to " + where + " (" +
                                         v.detail + ")");
                }
                events_.erase(events_.begin(), events_.begin() + static_cast<std::ptrdiff_t>(n));
                recount_();
                if (!events_.empty()) {
                    first_buffered_at_ = std::chrono::steady_clock::now();
                }
                const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    std::chrono::steady_clock::now() - t0)
                                    .count();
                clink::metrics::connector::records_out_inc(opts_.name, n);
                clink::metrics::connector::bytes_out_inc(opts_.name, body.size());
                clink::metrics::connector::commit_latency_observe(opts_.name,
                                                                  static_cast<std::uint64_t>(dt));
                (void)CLINK_FAULT_POINT(after_write_point_);
                return;
            }
            case HecEventVerdict::Kind::RefusedEvent: {
                const std::string what = "HEC refused event " + std::to_string(v.event + 1) +
                                         " of the " + std::to_string(n) + " in a request to " +
                                         where + " (" + v.detail + ")";
                if (opts_.dlq_policy != DlqPolicy::Drop) {
                    fail_(what +
                          "; the events before it may have been accepted, and every event of "
                          "the request stays pending");
                }
                if (auto* rt = this->runtime(); rt != nullptr) {
                    rt->report_bad_record(clink::BadRecord{.payload = events_[v.event],
                                                           .error = what,
                                                           .connector = opts_.name,
                                                           .direction = "sink",
                                                           .location = where});
                }
                clink::metrics::connector::error_inc(opts_.name, "sink");
                clink::metrics::connector::dropped_records_inc(opts_.name, 1);
                clink::metrics::connector::permanent_failures_inc(opts_.name);
                // The others go again, those before it too: nothing confirms
                // that HEC kept them.
                bytes -= events_[v.event].size() + (n > 1 ? std::size_t{1} : std::size_t{0});
                events_.erase(events_.begin() + static_cast<std::ptrdiff_t>(v.event));
                recount_();
                --n;
                if (n == 0) {
                    return;
                }
                continue;
            }
            case HecEventVerdict::Kind::Retry:
                if (retries == opts_.max_retries) {
                    fail_("a request to " + where + " was not accepted after " +
                          std::to_string(opts_.max_retries + 1) + " attempt(s) (" + v.detail +
                          "); its " + std::to_string(n) + " event(s) stay pending");
                }
                ++retries;
                pause_(backoff_delay(retries, kRetryBase),
                       "retrying a request to " + where + " (" + v.detail + ")");
                continue;
            case HecEventVerdict::Kind::Failed:
                fail_("a request to " + where + " was not accepted (" + v.detail + "); its " +
                      std::to_string(n) + " event(s) stay pending");
        }
    }
}

// Queries acknowledgements until at most `at_most` accepted requests await
// one. A request past ack_timeout, or a reply that makes them unreadable,
// fails the sink for good: it holds the ackIds but not the events.
void SplunkHecSink::await_acks_(std::size_t at_most) {
    const std::string where = opts_.url + ack_path_;
    while (unacked_.size() > at_most) {
        std::vector<std::uint64_t> asked;
        std::string body = "{\"acks\":[";
        for (const auto& u : unacked_) {
            body += (asked.empty() ? "" : ",") + std::to_string(u.ack_id);
            asked.push_back(u.ack_id);
        }
        body += "]}";
        const HecAckVerdict v = splunk_hec_ack_result(exchange_(ack_path_, body), asked);
        if (v.kind == HecAckVerdict::Kind::Failed) {
            unusable_ = "acknowledgements from " + where + " could not be read (" + v.detail +
                        "); " + std::to_string(unacked_.size()) +
                        " accepted request(s) stay unconfirmed";
            fail_(unusable_);
        }
        if (v.kind == HecAckVerdict::Kind::Read) {
            std::erase_if(unacked_, [&](const Unacked& u) {
                return std::find(v.indexed.begin(), v.indexed.end(), u.ack_id) != v.indexed.end();
            });
        }
        if (unacked_.size() <= at_most) {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        for (const auto& u : unacked_) {
            if (now - u.sent >= opts_.ack_timeout) {
                unusable_ =
                    "HEC did not acknowledge ackId " + std::to_string(u.ack_id) + " (" +
                    std::to_string(u.events) + " event(s)) on channel " + channel_ +
                    " within ack_timeout_ms (" + std::to_string(opts_.ack_timeout.count()) +
                    "); the events may or may not be indexed, and the job's replay from "
                    "its last checkpoint sends them again" +
                    (v.kind == HecAckVerdict::Kind::Retry ? " (the last query: " + v.detail + ")"
                                                          : std::string{});
                fail_(unusable_);
            }
        }
        pause_(opts_.ack_poll_interval, "waiting for acknowledgements from " + where);
    }
}

BoundedResponse SplunkHecSink::exchange_(const std::string& path, const std::string& body) {
    auto* rt = this->runtime();
    const bool watched = watch_ != nullptr && rt != nullptr;
    if (watched) {
        watch_->arm(
            rt->cancel_signal(),
            std::chrono::steady_clock::now() + std::chrono::milliseconds{opts_.request_timeout_ms});
    }
    BoundedRequest req;
    req.max_response_bytes = kMaxReplyBytes;
    BoundedResponse r = client_->post_bounded(path, body, kContentType, req);
    if (watched && watch_->disarm()) {
        if (rt->cancel_requested()) {
            fail_("cancelled while a request to " + opts_.url + path + " was in flight" +
                  (path == ack_path_ ? std::string{"; the acknowledgements stay pending"}
                                     : std::string{"; its events stay pending"}));
        }
        r.interrupted = true;  // past request_timeout_ms: as a lost reply
    }
    return r;
}

void SplunkHecSink::pause_(std::chrono::milliseconds d, const std::string& doing) const {
    const auto until = std::chrono::steady_clock::now() + d;
    const auto* rt = this->runtime();
    for (;;) {
        if (rt != nullptr && rt->cancel_requested()) {
            fail_("cancelled while " + doing);
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= until) {
            return;
        }
        std::this_thread::sleep_for(
            std::min<std::chrono::steady_clock::duration>(kCancelPoll, until - now));
    }
}

void SplunkHecSink::check_usable_() const {
    if (!unusable_.empty()) {
        throw std::runtime_error(opts_.name +
                                 ": the sink holds events it cannot confirm or keeps no copy "
                                 "of, so it refuses to write or acknowledge a barrier until it is "
                                 "opened again (" +
                                 unusable_ + ")");
    }
}

void SplunkHecSink::fail_(const std::string& why) const {
    clink::metrics::connector::error_inc(opts_.name, "sink");
    throw std::runtime_error(opts_.name + ": " + why);
}

bool SplunkHecSink::linger_elapsed_() const {
    return opts_.max_age.count() > 0 && !events_.empty() &&
           std::chrono::steady_clock::now() - first_buffered_at_ >= opts_.max_age;
}

}  // namespace clink::http_connector
