// The HTTP polling source (see http_poll_source.hpp): the page reader, which
// checks a whole body before anything of it is kept, and the poll loop, whose
// waits, retries and requests are bounded and end on cancel.

#include "clink/http_connector/http_poll_source.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <ctime>
#include <functional>
#include <iomanip>
#include <locale>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "clink/config/json.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/http_connector/cancel_watch.hpp"
#include "clink/http_connector/http_bulk_post.hpp"  // parse_headers
#include "clink/http_connector/http_request.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/logging.hpp"
#include "clink/state/state_backend.hpp"

namespace clink::http_connector {

namespace {

using Clock = std::chrono::steady_clock;
using std::chrono::milliseconds;

constexpr char kLogComponent[] = "http.poll_source";
// The longest one produce() call waits, so the runner hands barriers through
// while a poll interval or a backoff runs.
constexpr milliseconds kMaxWaitSlice{100};
// Deeper nesting fails the page rather than the reader's stack.
constexpr int kMaxDepth = 256;
// The key PollingSource kept the cursor under, so a checkpoint an earlier
// release wrote restores.
constexpr const char* kCursorKey = "__poll_cursor__";

// ---- a strict JSON reader that keeps every token as it was served ------------

// Why a page was refused. Thrown inside the reader, caught by read_http_poll_page.
struct BadPage {
    std::string why;
};

[[noreturn]] void malformed(std::size_t at, const char* what) {
    throw BadPage{"the response is not valid JSON (" + std::string{what} + " at byte " +
                  std::to_string(at) + ")"};
}

bool is_ws(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}
bool is_digit(char c) {
    return c >= '0' && c <= '9';
}

void skip_ws(std::string_view s, std::size_t& i) {
    while (i < s.size() && is_ws(s[i])) {
        ++i;
    }
}

const char* kind_of(char first) {
    switch (first) {
        case '{':
            return "an object";
        case '[':
            return "an array";
        case '"':
            return "a string";
        case 't':
        case 'f':
            return "a boolean";
        case 'n':
            return "null";
        default:
            return "a number";
    }
}

// At most 64 bytes of `text` for a message, cut on a character boundary.
std::string shown(std::string_view text) {
    constexpr std::size_t kMax = 64;
    if (text.size() <= kMax) {
        return std::string{text};
    }
    std::size_t cut = kMax;
    while (cut > 0 && (static_cast<unsigned char>(text[cut]) & 0xC0U) == 0x80U) {
        --cut;
    }
    return std::string{text.substr(0, cut)} + "...";
}

void append_utf8(std::string& out, std::uint32_t cp) {
    if (cp < 0x80U) {
        out.push_back(static_cast<char>(cp));
    } else if (cp < 0x800U) {
        out.push_back(static_cast<char>(0xC0U | (cp >> 6U)));
        out.push_back(static_cast<char>(0x80U | (cp & 0x3FU)));
    } else if (cp < 0x10000U) {
        out.push_back(static_cast<char>(0xE0U | (cp >> 12U)));
        out.push_back(static_cast<char>(0x80U | ((cp >> 6U) & 0x3FU)));
        out.push_back(static_cast<char>(0x80U | (cp & 0x3FU)));
    } else {
        out.push_back(static_cast<char>(0xF0U | (cp >> 18U)));
        out.push_back(static_cast<char>(0x80U | ((cp >> 12U) & 0x3FU)));
        out.push_back(static_cast<char>(0x80U | ((cp >> 6U) & 0x3FU)));
        out.push_back(static_cast<char>(0x80U | (cp & 0x3FU)));
    }
}

// One UTF-8 sequence from a lead byte of 0x80 or more: the shortest form, no
// surrogate, nothing past U+10FFFF.
void utf8_sequence(std::string_view s, std::size_t& i) {
    const auto lead = static_cast<unsigned char>(s[i]);
    std::size_t more = 0;
    std::uint32_t cp = 0;
    if (lead >= 0xC2U && lead <= 0xDFU) {
        more = 1;
        cp = lead & 0x1FU;
    } else if (lead >= 0xE0U && lead <= 0xEFU) {
        more = 2;
        cp = lead & 0x0FU;
    } else if (lead >= 0xF0U && lead <= 0xF4U) {
        more = 3;
        cp = lead & 0x07U;
    } else {
        malformed(i, "a byte that is not UTF-8");
    }
    if (i + more >= s.size()) {
        malformed(i, "a UTF-8 sequence cut short");
    }
    for (std::size_t k = 1; k <= more; ++k) {
        const auto b = static_cast<unsigned char>(s[i + k]);
        if ((b & 0xC0U) != 0x80U) {
            malformed(i + k, "a byte that is not UTF-8");
        }
        cp = (cp << 6U) | (b & 0x3FU);
    }
    if ((more == 2 && cp < 0x800U) || (more == 3 && cp < 0x10000U) || cp > 0x10FFFFU ||
        (cp >= 0xD800U && cp <= 0xDFFFU)) {
        malformed(i, "a UTF-8 sequence that is no character");
    }
    i += more + 1;
}

std::uint32_t hex4(std::string_view s, std::size_t at) {
    if (at + 4 > s.size()) {
        malformed(at, "a \\u escape cut short");
    }
    std::uint32_t v = 0;
    for (std::size_t k = 0; k < 4; ++k) {
        const char c = s[at + k];
        std::uint32_t h = 0;
        if (c >= '0' && c <= '9') {
            h = static_cast<std::uint32_t>(c - '0');
        } else if (c >= 'a' && c <= 'f') {
            h = static_cast<std::uint32_t>(c - 'a' + 10);
        } else if (c >= 'A' && c <= 'F') {
            h = static_cast<std::uint32_t>(c - 'A' + 10);
        } else {
            malformed(at + k, "a \\u escape that is not hexadecimal");
        }
        v = (v << 4U) | h;
    }
    return v;
}

// A string from its opening quote; its value, unescaped, goes to `out` when
// given. A string must be valid Unicode: a lone surrogate escape is refused.
void read_string(std::string_view s, std::size_t& i, std::string* out) {
    const std::size_t open = i;
    ++i;
    for (;;) {
        if (i >= s.size()) {
            malformed(open, "a string that does not end");
        }
        const auto c = static_cast<unsigned char>(s[i]);
        if (c == '"') {
            ++i;
            return;
        }
        if (c < 0x20U) {
            malformed(i, "a control character in a string");
        }
        if (c == '\\') {
            if (i + 1 >= s.size()) {
                malformed(open, "a string that does not end");
            }
            char plain = 0;
            switch (s[i + 1]) {
                case '"':
                    plain = '"';
                    break;
                case '\\':
                    plain = '\\';
                    break;
                case '/':
                    plain = '/';
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
                    std::uint32_t cp = hex4(s, i + 2);
                    std::size_t used = 6;
                    if (cp >= 0xD800U && cp <= 0xDBFFU) {
                        if (i + 12 > s.size() || s[i + 6] != '\\' || s[i + 7] != 'u') {
                            malformed(i, "a lone surrogate in a \\u escape");
                        }
                        const std::uint32_t low = hex4(s, i + 8);
                        if (low < 0xDC00U || low > 0xDFFFU) {
                            malformed(i, "a lone surrogate in a \\u escape");
                        }
                        cp = 0x10000U + ((cp - 0xD800U) << 10U) + (low - 0xDC00U);
                        used = 12;
                    } else if (cp >= 0xDC00U && cp <= 0xDFFFU) {
                        malformed(i, "a lone surrogate in a \\u escape");
                    }
                    if (out != nullptr) {
                        append_utf8(*out, cp);
                    }
                    i += used;
                    continue;
                }
                default:
                    malformed(i, "an escape JSON does not have");
            }
            if (out != nullptr) {
                out->push_back(plain);
            }
            i += 2;
            continue;
        }
        if (c >= 0x80U) {
            const std::size_t start = i;
            utf8_sequence(s, i);
            if (out != nullptr) {
                out->append(s.substr(start, i - start));
            }
            continue;
        }
        if (out != nullptr) {
            out->push_back(static_cast<char>(c));
        }
        ++i;
    }
}

void read_number(std::string_view s, std::size_t& i) {
    const std::size_t start = i;
    if (s[i] == '-') {
        ++i;
    }
    if (i >= s.size() || !is_digit(s[i])) {
        malformed(start, "a malformed number");
    }
    if (s[i] == '0') {
        ++i;
    } else {
        while (i < s.size() && is_digit(s[i])) {
            ++i;
        }
    }
    if (i < s.size() && s[i] == '.') {
        ++i;
        if (i >= s.size() || !is_digit(s[i])) {
            malformed(start, "a malformed number");
        }
        while (i < s.size() && is_digit(s[i])) {
            ++i;
        }
    }
    if (i < s.size() && (s[i] == 'e' || s[i] == 'E')) {
        ++i;
        if (i < s.size() && (s[i] == '+' || s[i] == '-')) {
            ++i;
        }
        if (i >= s.size() || !is_digit(s[i])) {
            malformed(start, "a malformed number");
        }
        while (i < s.size() && is_digit(s[i])) {
            ++i;
        }
    }
}

void read_literal(std::string_view s, std::size_t& i, std::string_view word) {
    if (s.substr(i, word.size()) != word) {
        malformed(i, "an unexpected character");
    }
    i += word.size();
}

// Visitors: a member's key, unescaped, and where its value lies; an element's
// extent.
using OnMember = std::function<void(const std::string& key, std::size_t begin, std::size_t end)>;
using OnElement = std::function<void(std::size_t begin, std::size_t end)>;

void read_value(std::string_view s, std::size_t& i, int depth);

void read_object(std::string_view s, std::size_t& i, int depth, const OnMember* on_member) {
    if (depth > kMaxDepth) {
        malformed(i, "nesting deeper than 256 levels");
    }
    ++i;
    skip_ws(s, i);
    if (i < s.size() && s[i] == '}') {
        ++i;
        return;
    }
    for (;;) {
        skip_ws(s, i);
        if (i >= s.size() || s[i] != '"') {
            malformed(i, "an object key that is not a string");
        }
        std::string key;
        read_string(s, i, on_member != nullptr ? &key : nullptr);
        skip_ws(s, i);
        if (i >= s.size() || s[i] != ':') {
            malformed(i, "a missing ':' after an object key");
        }
        ++i;
        skip_ws(s, i);
        const std::size_t begin = i;
        read_value(s, i, depth);
        if (on_member != nullptr) {
            (*on_member)(key, begin, i);
        }
        skip_ws(s, i);
        if (i >= s.size()) {
            malformed(i, "an object that does not end");
        }
        if (s[i] == ',') {
            ++i;
            continue;
        }
        if (s[i] == '}') {
            ++i;
            return;
        }
        malformed(i, "a missing ',' or '}' in an object");
    }
}

void read_array(std::string_view s, std::size_t& i, int depth, const OnElement* on_element) {
    if (depth > kMaxDepth) {
        malformed(i, "nesting deeper than 256 levels");
    }
    ++i;
    skip_ws(s, i);
    if (i < s.size() && s[i] == ']') {
        ++i;
        return;
    }
    for (;;) {
        skip_ws(s, i);
        const std::size_t begin = i;
        read_value(s, i, depth);
        if (on_element != nullptr) {
            (*on_element)(begin, i);
        }
        skip_ws(s, i);
        if (i >= s.size()) {
            malformed(i, "an array that does not end");
        }
        if (s[i] == ',') {
            ++i;
            continue;
        }
        if (s[i] == ']') {
            ++i;
            return;
        }
        malformed(i, "a missing ',' or ']' in an array");
    }
}

void read_value(std::string_view s, std::size_t& i, int depth) {
    if (i >= s.size()) {
        malformed(i, "a missing value");
    }
    switch (s[i]) {
        case '{':
            read_object(s, i, depth + 1, nullptr);
            return;
        case '[':
            read_array(s, i, depth + 1, nullptr);
            return;
        case '"':
            read_string(s, i, nullptr);
            return;
        case 't':
            read_literal(s, i, "true");
            return;
        case 'f':
            read_literal(s, i, "false");
            return;
        case 'n':
            read_literal(s, i, "null");
            return;
        default:
            if (s[i] == '-' || is_digit(s[i])) {
                read_number(s, i);
                return;
            }
            malformed(i, "an unexpected character");
    }
}

// A valid JSON value on one line: the whitespace between tokens dropped, every
// token kept byte for byte.
std::string minified(std::string_view v) {
    std::string out;
    out.reserve(v.size());
    bool in_string = false;
    for (std::size_t k = 0; k < v.size(); ++k) {
        const char c = v[k];
        if (in_string) {
            out.push_back(c);
            if (c == '\\') {
                out.push_back(v[++k]);
            } else if (c == '"') {
                in_string = false;
            }
        } else if (c == '"') {
            in_string = true;
            out.push_back(c);
        } else if (!is_ws(c)) {
            out.push_back(c);
        }
    }
    return out;
}

// ---- cursors -------------------------------------------------------------------------

// The canonical text of an integer written as `t` ("-0" is "0"), or nothing
// when `t` is not a decimal integer of at most kHttpPollMaxIntegerDigits.
std::optional<std::string> canonical_integer(std::string_view t) {
    bool negative = false;
    if (!t.empty() && t[0] == '-') {
        negative = true;
        t.remove_prefix(1);
    }
    if (t.empty() || t.size() > kHttpPollMaxIntegerDigits ||
        !std::all_of(t.begin(), t.end(), is_digit) || (t.size() > 1 && t[0] == '0')) {
        return std::nullopt;
    }
    if (t == "0") {
        return std::string{"0"};
    }
    return (negative ? std::string{"-"} : std::string{}) + std::string{t};
}

// The cursor that record `n`'s value `token` gives under `type`.
std::string cursor_of(std::string_view token,
                      HttpPollCursorType type,
                      const std::string& field,
                      std::size_t n) {
    const std::string who = "record " + std::to_string(n) + "'s '" + field + "'";
    std::string text;
    if (token[0] == '"') {
        std::size_t i = 0;
        read_string(token, i, &text);
    } else if (token[0] == '-' || is_digit(token[0])) {
        text = std::string{token};
    } else {
        throw BadPage{who + " is " + kind_of(token[0]) + ", which cannot be a cursor"};
    }
    if (type == HttpPollCursorType::Integer) {
        auto canonical = canonical_integer(text);
        if (!canonical.has_value()) {
            throw BadPage{who + " " + shown(token) + " is not an integer of at most " +
                          std::to_string(kHttpPollMaxIntegerDigits) +
                          " digits, which cursor_type 'integer' needs"};
        }
        return std::move(*canonical);
    }
    if (text.empty()) {
        throw BadPage{who + " is an empty string, which cannot be a cursor"};
    }
    if (text.size() > kHttpPollMaxCursorBytes) {
        throw BadPage{who + " is longer than " + std::to_string(kHttpPollMaxCursorBytes) +
                      " bytes"};
    }
    return text;
}

// ---- HTTP outcomes -------------------------------------------------------------------

// Statuses worth another attempt: a request timeout or throttling, and the
// server errors that pass. 501 and 505 describe the request, which another
// attempt does not change.
bool transient_status(int status) {
    return status == 408 || status == 425 || status == 429 ||
           (status >= 500 && status != 501 && status != 505);
}

std::string permanent_hint(int status) {
    switch (status) {
        case 401:
            return ": the endpoint refused the credentials; check 'headers'";
        case 403:
            return ": the credentials may not read this resource";
        case 404:
            return ": check 'url' and 'path'";
        default:
            return "";
    }
}

std::string trimmed(std::string_view v) {
    while (!v.empty() && is_ws(v.front())) {
        v.remove_prefix(1);
    }
    while (!v.empty() && is_ws(v.back())) {
        v.remove_suffix(1);
    }
    return std::string{v};
}

// What a Retry-After header asks for: delay-seconds or an HTTP date. Nothing
// for any other form, which leaves the backoff in charge.
std::optional<milliseconds> retry_after(const std::map<std::string, std::string>& headers) {
    const auto it = headers.find("retry-after");
    if (it == headers.end()) {
        return std::nullopt;
    }
    const std::string v = trimmed(it->second);
    if (!v.empty() && std::all_of(v.begin(), v.end(), is_digit)) {
        if (v.size() > 9) {
            return milliseconds{std::chrono::hours{24 * 365}};
        }
        return std::chrono::duration_cast<milliseconds>(std::chrono::seconds{std::stoll(v)});
    }
    std::tm tm{};
    std::istringstream in(v);
    in.imbue(std::locale::classic());
    in >> std::get_time(&tm, "%a, %d %b %Y %H:%M:%S GMT");
    if (in.fail()) {
        return std::nullopt;
    }
    const auto when = std::chrono::system_clock::from_time_t(timegm(&tm));
    const auto now = std::chrono::system_clock::now();
    if (when <= now) {
        return milliseconds{0};
    }
    return std::chrono::duration_cast<milliseconds>(when - now);
}

std::string ms(milliseconds d) {
    return std::to_string(d.count()) + " ms";
}

// scheme://host[:port] and nothing else, or why not. The URL itself is never
// echoed: one with a user in it carries a password.
std::string base_url_problem(const std::string& url) {
    std::string_view rest = url;
    if (rest.rfind("http://", 0) == 0) {
        rest.remove_prefix(7);
    } else if (rest.rfind("https://", 0) == 0) {
        rest.remove_prefix(8);
    } else {
        return "must start with http:// or https://";
    }
    if (rest.find('@') != std::string_view::npos) {
        return "must not carry a user or password; send credentials in 'headers'";
    }
    if (std::any_of(rest.begin(), rest.end(), [](char c) {
            return static_cast<unsigned char>(c) <= 0x20U || c == 0x7F;
        })) {
        return "must not hold spaces or control characters";
    }
    if (rest.find_first_of("/?#") != std::string_view::npos) {
        return "must be scheme://host[:port] only; put the path and any fixed query in 'path'";
    }
    std::string_view port;
    if (!rest.empty() && rest.front() == '[') {
        const auto close = rest.find(']');
        if (close == std::string_view::npos || close == 1) {
            return "names no host";
        }
        const std::string_view after = rest.substr(close + 1);
        if (!after.empty()) {
            if (after.front() != ':') {
                return "must be scheme://host[:port] only";
            }
            port = after.substr(1);
            if (port.empty()) {
                return "has an empty port";
            }
        }
    } else {
        const auto colon = rest.rfind(':');
        if (colon != std::string_view::npos) {
            port = rest.substr(colon + 1);
            rest = rest.substr(0, colon);
            if (port.empty()) {
                return "has an empty port";
            }
        }
        if (rest.empty()) {
            return "names no host";
        }
    }
    if (!port.empty()) {
        if (port.size() > 5 || !std::all_of(port.begin(), port.end(), is_digit) ||
            std::stoi(std::string{port}) < 1 || std::stoi(std::string{port}) > 65535) {
            return "has a port that is not 1 to 65535";
        }
    }
    return {};
}

void check_range(
    const std::string& name, const char* option, long long value, long long lo, long long hi) {
    if (value < lo || value > hi) {
        throw std::runtime_error(name + ": '" + option + "' must be " + std::to_string(lo) +
                                 " to " + std::to_string(hi) + "; it is " + std::to_string(value));
    }
}

void validate(const HttpPollOptions& o) {
    if (o.name.empty()) {
        throw std::runtime_error("http_poll_source: 'name' is empty");
    }
    const std::string& n = o.name;
    if (o.url.empty()) {
        throw std::runtime_error(n + ": 'url' is required");
    }
    if (const auto problem = base_url_problem(o.url); !problem.empty()) {
        throw std::runtime_error(n + ": 'url' " + problem);
    }
    if (!HttpRequest::tls_supported() && o.url.rfind("https://", 0) == 0) {
        throw std::runtime_error(n +
                                 ": https URL but this build has no TLS support (rebuild the "
                                 "http_connector module with OpenSSL)");
    }
    if (o.path.empty() || o.path.front() != '/') {
        throw std::runtime_error(n + ": 'path' must start with '/'");
    }
    if (std::any_of(o.path.begin(), o.path.end(), [](char c) {
            return static_cast<unsigned char>(c) <= 0x20U || c == 0x7F || c == '#';
        })) {
        throw std::runtime_error(
            n + ": 'path' must not hold spaces, control characters or a fragment ('#')");
    }
    const bool cursor_mode = !o.cursor_field.empty();
    if (o.cursor_param.empty() != o.cursor_field.empty()) {
        throw std::runtime_error(
            n +
            ": cursor mode takes both 'cursor_param' (the query parameter the cursor is sent "
            "as) and 'cursor_field' (the record field it is read from), and resource mode "
            "neither; this source has only '" +
            (cursor_mode ? std::string{"cursor_field"} : std::string{"cursor_param"}) + "'");
    }
    if (!cursor_mode) {
        if (!o.initial_cursor.empty()) {
            throw std::runtime_error(n +
                                     ": 'initial_cursor' needs cursor mode ('cursor_param' "
                                     "and 'cursor_field')");
        }
        if (o.cursor_type != HttpPollCursorType::Opaque) {
            throw std::runtime_error(n +
                                     ": 'cursor_type' needs cursor mode ('cursor_param' "
                                     "and 'cursor_field')");
        }
    } else if (!o.initial_cursor.empty()) {
        if (o.cursor_type == HttpPollCursorType::Integer &&
            canonical_integer(o.initial_cursor) != o.initial_cursor) {
            throw std::runtime_error(
                n +
                ": 'initial_cursor' must be an integer written without a plus sign or leading "
                "zeros, of at most " +
                std::to_string(kHttpPollMaxIntegerDigits) + " digits, for cursor_type 'integer'");
        }
        if (o.initial_cursor.size() > kHttpPollMaxCursorBytes) {
            throw std::runtime_error(n + ": 'initial_cursor' is longer than " +
                                     std::to_string(kHttpPollMaxCursorBytes) + " bytes");
        }
    }
    check_range(n, "poll_interval_ms", o.interval.count(), 0, 3600000);
    if (!(o.jitter_frac >= 0.0 && o.jitter_frac <= 1.0)) {
        throw std::runtime_error(n + ": 'jitter_frac' must be 0 to 1");
    }
    check_range(n, "max_retries", o.max_retries, 0, HttpPollSource::kMaxRetries);
    check_range(n, "retry_base_backoff_ms", o.retry_base_backoff.count(), 1, 60000);
    check_range(n, "max_backoff_ms", o.max_backoff.count(), o.retry_base_backoff.count(), 600000);
    check_range(n, "retry_timeout_ms", o.retry_timeout.count(), 1, 3600000);
    check_range(n, "connect_timeout_ms", o.connect_timeout.count(), 1, 600000);
    check_range(n, "request_timeout_ms", o.request_timeout.count(), 1, 600000);
    check_range(n,
                "max_response_bytes",
                static_cast<long long>(std::min<std::size_t>(o.max_response_bytes, 1ULL << 40U)),
                64,
                1LL << 30);
    check_range(n,
                "max_response_records",
                static_cast<long long>(std::min<std::size_t>(o.max_response_records, 1ULL << 40U)),
                1,
                10000000);
}

}  // namespace

// ---- reading a page -------------------------------------------------------------------

int compare_integer_cursors(std::string_view a, std::string_view b) {
    const bool neg_a = !a.empty() && a.front() == '-';
    const bool neg_b = !b.empty() && b.front() == '-';
    if (neg_a != neg_b) {
        return neg_a ? -1 : 1;
    }
    if (neg_a) {
        a.remove_prefix(1);
        b.remove_prefix(1);
    }
    int magnitude = 0;
    if (a.size() != b.size()) {
        magnitude = a.size() < b.size() ? -1 : 1;
    } else if (a != b) {
        magnitude = a < b ? -1 : 1;
    }
    return neg_a ? -magnitude : magnitude;
}

HttpPollPage read_http_poll_page(std::string_view body,
                                 const HttpPollPageRules& rules,
                                 const std::string& cursor) {
    HttpPollPage page;
    try {
        // The whole document first: valid JSON, and where its array is.
        std::size_t i = 0;
        skip_ws(body, i);
        if (i >= body.size()) {
            throw BadPage{"the response has no body"};
        }
        const std::size_t root_at = i;
        const char root = body[i];
        std::size_t array_at = std::string::npos;
        int named = 0;
        if (!rules.records_field.empty() && root == '{') {
            const OnMember find = [&](const std::string& key, std::size_t begin, std::size_t) {
                if (key == rules.records_field) {
                    ++named;
                    array_at = begin;
                }
            };
            read_object(body, i, 1, &find);
        } else {
            read_value(body, i, 0);
            array_at = root_at;
        }
        skip_ws(body, i);
        if (i != body.size()) {
            malformed(i, "more after the end of the document");
        }
        const std::string& rf = rules.records_field;
        if (rf.empty()) {
            if (root != '[') {
                throw BadPage{std::string{"the response is "} + kind_of(root) +
                              ", not an array of records; name the field that holds them with "
                              "records_field"};
            }
        } else if (root != '{') {
            throw BadPage{std::string{"the response is "} + kind_of(root) +
                          ", not an object holding '" + rf + "'"};
        } else if (named == 0) {
            throw BadPage{"the response has no '" + rf + "'"};
        } else if (named > 1) {
            throw BadPage{"the response has '" + rf + "' more than once"};
        } else if (body[array_at] != '[') {
            throw BadPage{"the response's '" + rf + "' is " + kind_of(body[array_at]) +
                          ", not an array"};
        }

        // The elements, bounded in number.
        std::vector<std::pair<std::size_t, std::size_t>> spans;
        {
            std::size_t j = array_at;
            const OnElement collect = [&](std::size_t begin, std::size_t end) {
                if (spans.size() == rules.max_records) {
                    throw BadPage{"the response holds more than " +
                                  std::to_string(rules.max_records) +
                                  " records (max_response_records)"};
                }
                spans.emplace_back(begin, end);
            };
            read_array(body, j, 1, &collect);
        }

        // Cursor mode: every record's cursor, against the contract.
        const std::string& cf = rules.cursor_field;
        const bool cursor_mode = !cf.empty();
        const bool integer = rules.cursor_type == HttpPollCursorType::Integer;
        std::string previous = cursor;
        std::map<std::string, std::size_t> seen;  // opaque: value -> record number
        page.records.reserve(spans.size());
        for (std::size_t k = 0; k < spans.size(); ++k) {
            const auto [begin, end] = spans[k];
            const std::size_t n = k + 1;
            if (cursor_mode) {
                if (body[begin] != '{') {
                    throw BadPage{"record " + std::to_string(n) + " is " + kind_of(body[begin]) +
                                  ", not an object holding '" + cf + "'"};
                }
                int found = 0;
                std::size_t value_at = 0;
                std::size_t value_end = 0;
                std::size_t j = begin;
                const OnMember find = [&](const std::string& key, std::size_t b, std::size_t e) {
                    if (key == cf) {
                        ++found;
                        value_at = b;
                        value_end = e;
                    }
                };
                read_object(body, j, 1, &find);
                if (found == 0) {
                    throw BadPage{"record " + std::to_string(n) + " has no '" + cf + "'"};
                }
                if (found > 1) {
                    throw BadPage{"record " + std::to_string(n) + " has '" + cf +
                                  "' more than once"};
                }
                std::string value = cursor_of(
                    body.substr(value_at, value_end - value_at), rules.cursor_type, cf, n);
                const std::string who = "record " + std::to_string(n) + "'s '" + cf + "' ";
                if (integer && !previous.empty()) {
                    const int order = compare_integer_cursors(value, previous);
                    if (order <= 0 && k == 0) {
                        throw BadPage{who + value + " is not after the cursor " + previous +
                                      " the request carried: the endpoint did not answer strictly "
                                      "after the cursor (it ignored the cursor, or includes the "
                                      "record at it)"};
                    }
                    if (order == 0) {
                        throw BadPage{"records " + std::to_string(n - 1) + " and " +
                                      std::to_string(n) + " share the '" + cf + "' " + value +
                                      ": the cursor only moves past a value, so a page ending "
                                      "inside a run of equal values would skip the rest of it; "
                                      "each value must be unique"};
                    }
                    if (order < 0) {
                        throw BadPage{who + value + " comes after record " + std::to_string(n - 1) +
                                      "'s " + previous + ": the values must rise"};
                    }
                } else if (!integer) {
                    if (!cursor.empty() && value == cursor) {
                        throw BadPage{who + "is the cursor the request carried (" + shown(value) +
                                      "): the endpoint did not answer strictly after the cursor "
                                      "(it ignored the cursor, or includes the record at it)"};
                    }
                    const auto [at, inserted] = seen.emplace(value, n);
                    if (!inserted) {
                        throw BadPage{"records " + std::to_string(at->second) + " and " +
                                      std::to_string(n) + " share the '" + cf + "' " +
                                      shown(value) + ": each value must be unique"};
                    }
                }
                previous = std::move(value);
            }
            std::string text = minified(body.substr(begin, end - begin));
            // Everything emitted must be readable by clink's own JSON reader,
            // which SQL decodes rows with and which holds no integer past 64
            // bits and no number past a double's range.
            try {
                (void)clink::config::parse(text);
            } catch (const clink::config::ParseError& e) {
                throw BadPage{"record " + std::to_string(n) +
                              " holds a value clink's JSON reader cannot decode (" + e.what() +
                              "); an endpoint must send integers past 64 bits, and numbers past "
                              "a double's range, as strings"};
            }
            page.records.push_back(std::move(text));
        }
        if (cursor_mode && !spans.empty()) {
            page.last_cursor = previous;
        }
        page.valid = true;
    } catch (const BadPage& bad) {
        page = HttpPollPage{};
        page.why = bad.why;
    }
    return page;
}

// ---- the source -----------------------------------------------------------------------

struct HttpPollSource::Impl {
    explicit Impl(HttpPollOptions o)
        : opts(std::move(o)),
          headers(parse_headers(opts.headers)),
          cursor(opts.initial_cursor),
          after_emit_point(opts.name + ".after_emit") {
        rules.records_field = opts.records_field;
        rules.cursor_field = opts.cursor_field;
        rules.cursor_type = opts.cursor_type;
        rules.max_records = opts.max_response_records;
    }

    HttpPollOptions opts;
    HttpPollPageRules rules;
    std::map<std::string, std::string> headers;

    // The source thread's.
    std::string cursor;
    std::unique_ptr<HttpRequest> client;
    std::unique_ptr<CancelWatch> watch;  // declared after the client, so destroyed first
    // The ETag of the last response, and the request it answered. Sent as
    // If-None-Match only when the next request is that same one, and dropped
    // with the position it describes on a restore or a reopen.
    std::string etag_target;
    std::string etag;
    bool finished{false};
    Clock::time_point next_poll{};  // the epoch: the first poll is immediate
    // The poll under way: when its first attempt started, how many attempts
    // have failed, and when the next one is due.
    Clock::time_point poll_started{};
    int failures{0};
    Clock::time_point next_attempt{};
    std::mt19937 rng{std::random_device{}()};
    const std::string after_emit_point;

    // Cancellation, from any thread.
    std::shared_ptr<std::atomic<bool>> cancel_flag{std::make_shared<std::atomic<bool>>(false)};
    std::mutex mu;
    std::condition_variable wake;

    [[nodiscard]] bool cursor_mode() const noexcept { return !opts.cursor_field.empty(); }

    [[nodiscard]] std::string target() const {
        std::string t = opts.path;
        if (cursor_mode() && !cursor.empty()) {
            t.push_back(t.find('?') == std::string::npos ? '?' : '&');
            t += url_encode(opts.cursor_param) + "=" + url_encode(cursor);
        }
        return t;
    }

    // What an error names: the request's path without its query string, which
    // can carry a credential as well as the cursor.
    [[nodiscard]] std::string what() const {
        const auto q = opts.path.find('?');
        return opts.name + ": GET " +
               (q == std::string::npos ? opts.path : opts.path.substr(0, q)) + ": ";
    }

    [[noreturn]] void fail(const std::string& why) const {
        clink::metrics::connector::error_inc("http", "source");
        throw std::runtime_error(what() + why);
    }

    void ensure_client() {
        if (client) {
            return;
        }
        HttpRequest::Options h;
        h.base_url = opts.url;
        h.headers = headers;
        h.verify_tls = opts.verify_tls;
        h.connect_timeout_ms = static_cast<int>(opts.connect_timeout.count());
        // The watch ends a request at request_timeout; the socket's own
        // timeout stays behind it as a backstop, so a stall is reported as
        // the request running out of time rather than as a read error.
        h.rw_timeout_ms = static_cast<int>(opts.request_timeout.count()) + 1000;
        client = std::make_unique<HttpRequest>(std::move(h));
        watch = std::make_unique<CancelWatch>(*client);
    }

    void wait_until(const HttpPollSource& source, Clock::time_point deadline) {
        std::unique_lock lock(mu);
        wake.wait_until(lock, deadline, [&] { return source.cancelled(); });
    }

    [[nodiscard]] milliseconds interval_now() {
        if (opts.jitter_frac <= 0.0) {
            return opts.interval;
        }
        std::uniform_real_distribution<double> u01(0.0, 1.0);
        const double factor = 1.0 + opts.jitter_frac * (2.0 * u01(rng) - 1.0);
        return milliseconds{
            static_cast<long long>(static_cast<double>(opts.interval.count()) * factor)};
    }

    [[nodiscard]] milliseconds backoff(int failed) const {
        auto d = opts.retry_base_backoff;
        for (int k = 1; k < failed && d < opts.max_backoff; ++k) {
            d *= 2;
        }
        return std::min(d, opts.max_backoff);
    }

    // A failed attempt of the poll under way: schedule the next, or fail the
    // poll once its attempts or its time are spent.
    void retry_or_fail(const std::string& why, std::optional<milliseconds> asked) {
        clink::metrics::connector::error_inc("http", "source");
        const auto now = Clock::now();
        ++failures;
        const std::string attempts =
            std::to_string(failures) + (failures == 1 ? " attempt" : " attempts");
        if (failures > opts.max_retries) {
            throw std::runtime_error(what() + why + "; gave up after " + attempts +
                                     " (max_retries " + std::to_string(opts.max_retries) + ")");
        }
        milliseconds delay = backoff(failures);
        const bool waiting_as_asked = asked.has_value() && *asked > delay;
        if (waiting_as_asked) {
            delay = *asked;
        }
        if (now + delay > poll_started + opts.retry_timeout) {
            throw std::runtime_error(what() + why + "; " +
                                     (waiting_as_asked
                                          ? "the endpoint asked to wait " + ms(delay) + ", which"
                                          : std::string{"the next attempt"}) +
                                     " would end past retry_timeout_ms (" + ms(opts.retry_timeout) +
                                     ") after " + attempts);
        }
        next_attempt = now + delay;
        if ((failures & (failures - 1)) == 0) {
            clink::log::warn(
                kLogComponent,
                what() + why + "; attempt " + std::to_string(failures + 1) + " in " + ms(delay));
        }
    }

    // A poll that ended with a response the source accepts: emit its records,
    // move the cursor past them, and keep conditional state only for a request
    // the next poll repeats.
    void accept(Emitter<std::string>& out,
                HttpPollPage& page,
                const BoundedResponse& res,
                const std::string& request) {
        failures = 0;
        const bool empty = page.records.empty();
        if (!empty) {
            std::uint64_t bytes = 0;
            const auto count = page.records.size();
            Batch<std::string> batch;
            for (auto& r : page.records) {
                bytes += r.size();
                batch.emplace(std::move(r));
            }
            if (!out.emit_data(std::move(batch))) {
                // The output closed under a teardown: the cursor stays where it was.
                next_poll = poll_started + interval_now();
                return;
            }
            clink::metrics::connector::records_in_inc("http", count);
            clink::metrics::connector::bytes_in_inc("http", bytes);
            if (cursor_mode()) {
                cursor = page.last_cursor;
            }
            (void)CLINK_FAULT_POINT(after_emit_point);
        }
        const bool repeats = !cursor_mode() || empty;
        const auto et = res.headers.find("etag");
        if (repeats && res.status == 200 && et != res.headers.end() && !et->second.empty()) {
            etag_target = request;
            etag = et->second;
        } else {
            etag_target.clear();
            etag.clear();
        }
        if (opts.bounded && (!cursor_mode() || empty)) {
            finished = true;
        }
        next_poll = poll_started + interval_now();
    }

    // One attempt of the poll under way.
    void attempt(const HttpPollSource& source, Emitter<std::string>& out) {
        ensure_client();
        const auto start = Clock::now();
        if (failures == 0) {
            poll_started = start;
        }
        const std::string request = target();
        BoundedRequest req;
        req.max_response_bytes = opts.max_response_bytes;
        const bool conditional = !opts.bounded && !etag.empty() && etag_target == request;
        if (conditional) {
            req.headers["If-None-Match"] = etag;
        }
        watch->arm(CancelSignal{cancel_flag, nullptr}, start + opts.request_timeout);
        BoundedResponse res = client->get_bounded(request, req);
        const bool interrupted = watch->disarm();
        if (source.cancelled()) {
            return;  // nothing of a cancelled poll is kept
        }
        if (!res.complete) {
            if (interrupted || res.interrupted) {
                retry_or_fail("no complete response within request_timeout_ms (" +
                                  ms(opts.request_timeout) + ")",
                              std::nullopt);
                return;
            }
            switch (res.failure) {
                case BoundedResponse::Failure::TooLarge:
                    fail("the response passed max_response_bytes (" +
                         std::to_string(opts.max_response_bytes) + ")");
                case BoundedResponse::Failure::TlsVerify:
                    fail("the server's certificate did not verify (" + res.error + ")");
                default:
                    retry_or_fail("no response (" + res.error + ")", std::nullopt);
                    return;
            }
        }
        const int status = res.status;
        if (status == 200) {
            HttpPollPage page = read_http_poll_page(res.body, rules, cursor);
            if (!page.valid) {
                fail(page.why + "; nothing of it was emitted and the cursor did not move");
            }
            accept(out, page, res, request);
            return;
        }
        if (status == 204) {
            HttpPollPage empty;
            empty.valid = true;
            accept(out, empty, res, request);
            return;
        }
        if (status == 304) {
            if (!conditional) {
                fail("HTTP 304 to a request that carried no If-None-Match");
            }
            failures = 0;
            next_poll = poll_started + interval_now();
            return;
        }
        if (status >= 200 && status < 300) {
            fail("HTTP " + std::to_string(status) +
                 ", which is not 200, 204 or 304: records are read only from a 200");
        }
        if (status >= 300 && status < 400) {
            fail("HTTP " + std::to_string(status) +
                 ", a redirect the source does not follow; point 'url' and 'path' at the "
                 "records");
        }
        if (transient_status(status)) {
            retry_or_fail("HTTP " + std::to_string(status), retry_after(res.headers));
            return;
        }
        fail("HTTP " + std::to_string(status) + permanent_hint(status));
    }
};

HttpPollSource::HttpPollSource(HttpPollOptions opts) {
    validate(opts);
    impl_ = std::make_unique<Impl>(std::move(opts));
}

HttpPollSource::~HttpPollSource() = default;

void HttpPollSource::open() {
    auto& im = *impl_;
    im.etag_target.clear();
    im.etag.clear();
    im.finished = false;
    im.failures = 0;
    im.next_poll = {};
}

void HttpPollSource::close() {
    auto& im = *impl_;
    im.watch.reset();
    im.client.reset();
}

bool HttpPollSource::produce(Emitter<std::string>& out) {
    auto& im = *impl_;
    if (cancelled() || im.finished) {
        return false;
    }
    const auto now = Clock::now();
    const auto due = im.failures > 0 ? im.next_attempt : im.next_poll;
    if (now < due) {
        im.wait_until(*this, std::min(due, now + kMaxWaitSlice));
        return !cancelled();
    }
    im.attempt(*this, out);
    return !cancelled() && !im.finished;
}

void HttpPollSource::cancel() {
    Source<std::string>::cancel();
    auto& im = *impl_;
    im.cancel_flag->store(true, std::memory_order_release);
    {
        const std::lock_guard lock(im.mu);
    }
    im.wake.notify_all();
}

bool HttpPollSource::is_bounded() const noexcept {
    return impl_->opts.bounded;
}

std::string HttpPollSource::name() const {
    return impl_->opts.name;
}

void HttpPollSource::snapshot_offset(StateBackend& backend,
                                     OperatorId op_id,
                                     CheckpointId /*ckpt_id*/) {
    const auto& cursor = impl_->cursor;
    backend.put_operator_state(
        op_id, kCursorKey, StateBackend::ValueView{cursor.data(), cursor.size()});
}

bool HttpPollSource::restore_offset(StateBackend& backend, OperatorId op_id) {
    auto& im = *impl_;
    im.etag_target.clear();
    im.etag.clear();
    im.finished = false;
    im.failures = 0;
    im.next_poll = {};
    const auto v = backend.get_operator_state(op_id, StateBackend::KeyView{kCursorKey});
    if (!v.has_value()) {
        return false;
    }
    std::string restored(reinterpret_cast<const char*>(v->data()), v->size());
    if (!im.cursor_mode()) {
        if (!restored.empty()) {
            throw std::runtime_error(
                im.opts.name +
                ": the checkpoint holds a cursor, but this source is in resource mode; restore it "
                "with the cursor options it ran with, or start it without the checkpoint");
        }
        return true;
    }
    if (!restored.empty() && im.opts.cursor_type == HttpPollCursorType::Integer) {
        auto canonical = canonical_integer(restored);
        if (!canonical.has_value()) {
            throw std::runtime_error(im.opts.name + ": the checkpoint's cursor " + shown(restored) +
                                     " is not an integer, which cursor_type 'integer' needs; it "
                                     "was written under another cursor_type");
        }
        restored = std::move(*canonical);
    }
    im.cursor = std::move(restored);
    return true;
}

std::string HttpPollSource::cursor() const {
    return impl_->cursor;
}

bool HttpPollSource::cursor_mode() const noexcept {
    return impl_->cursor_mode();
}

std::shared_ptr<Source<std::string>> make_http_poll_source(HttpPollOptions o) {
    return std::make_shared<HttpPollSource>(std::move(o));
}

}  // namespace clink::http_connector
