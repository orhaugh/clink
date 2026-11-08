// The Kinesis sink: options, the partition-key rules, the record and request
// limits, the buffer, the PutRecords loop with its response judge and retry
// budget, and the failed state that keeps a sink which dropped records from
// acknowledging a later barrier. The service calls are behind
// KinesisWriteService.

#include "clink/aws/kinesis_sink.hpp"

#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <thread>
#include <utility>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/logging.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace clink::aws {

namespace {

constexpr char kLogComponent[] = "kinesis.sink";
constexpr char kConnector[] = "kinesis";
// How often a retry pause looks at the task's cancel signal.
constexpr std::chrono::milliseconds kCancelPoll{50};
// A number's exponent is read up to this magnitude: far past any plain decimal
// that fits in a partition key, and far from overflow.
constexpr std::int64_t kExponentCap = 1'000'000'000;

// ---- UTF-8 ------------------------------------------------------------------------------

// The length of the valid UTF-8 sequence at s[i], or 0 when there is none
// (Unicode table 3-7: no overlong forms, no surrogates, nothing past U+10FFFF).
std::size_t utf8_sequence(std::string_view s, std::size_t i) {
    const auto at = [&](std::size_t k) { return static_cast<unsigned char>(s[k]); };
    const auto in = [&](std::size_t k, unsigned lo, unsigned hi) {
        return k < s.size() && at(k) >= lo && at(k) <= hi;
    };
    const auto c = at(i);
    if (c < 0x80) {
        return 1;
    }
    if (c >= 0xC2 && c <= 0xDF) {
        return in(i + 1, 0x80, 0xBF) ? 2 : 0;
    }
    if (c == 0xE0) {
        return in(i + 1, 0xA0, 0xBF) && in(i + 2, 0x80, 0xBF) ? 3 : 0;
    }
    if ((c >= 0xE1 && c <= 0xEC) || c == 0xEE || c == 0xEF) {
        return in(i + 1, 0x80, 0xBF) && in(i + 2, 0x80, 0xBF) ? 3 : 0;
    }
    if (c == 0xED) {
        return in(i + 1, 0x80, 0x9F) && in(i + 2, 0x80, 0xBF) ? 3 : 0;
    }
    if (c == 0xF0) {
        return in(i + 1, 0x90, 0xBF) && in(i + 2, 0x80, 0xBF) && in(i + 3, 0x80, 0xBF) ? 4 : 0;
    }
    if (c >= 0xF1 && c <= 0xF3) {
        return in(i + 1, 0x80, 0xBF) && in(i + 2, 0x80, 0xBF) && in(i + 3, 0x80, 0xBF) ? 4 : 0;
    }
    if (c == 0xF4) {
        return in(i + 1, 0x80, 0x8F) && in(i + 2, 0x80, 0xBF) && in(i + 3, 0x80, 0xBF) ? 4 : 0;
    }
    return 0;
}

void append_utf8(std::string& out, char32_t cp) {
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

bool is_digit(char c) {
    return c >= '0' && c <= '9';
}

// ---- numbers ------------------------------------------------------------------------

// A JSON number token's exact value in plain decimal: no exponent, no leading
// zeros, no point for an integer and no trailing zeros after one, '-' for a
// negative value and "0" for zero of either sign. Empty when that form would be
// longer than `max_chars`. The token has passed JSON's number grammar.
std::string plain_decimal(std::string_view token, std::size_t max_chars) {
    std::size_t i = 0;
    const bool negative = token[i] == '-';
    if (negative) {
        ++i;
    }
    const auto int_begin = i;
    while (i < token.size() && is_digit(token[i])) {
        ++i;
    }
    const auto int_part = token.substr(int_begin, i - int_begin);
    std::string_view frac_part;
    if (i < token.size() && token[i] == '.') {
        const auto frac_begin = ++i;
        while (i < token.size() && is_digit(token[i])) {
            ++i;
        }
        frac_part = token.substr(frac_begin, i - frac_begin);
    }
    std::int64_t exponent = 0;
    if (i < token.size() && (token[i] == 'e' || token[i] == 'E')) {
        ++i;
        bool exponent_negative = false;
        if (token[i] == '+' || token[i] == '-') {
            exponent_negative = token[i] == '-';
            ++i;
        }
        for (; i < token.size(); ++i) {
            exponent = std::min<std::int64_t>((exponent * 10) + (token[i] - '0'), kExponentCap);
        }
        if (exponent_negative) {
            exponent = -exponent;
        }
    }
    // value = digits x 10^e
    std::string digits;
    digits.reserve(int_part.size() + frac_part.size());
    digits.append(int_part);
    digits.append(frac_part);
    const auto first = digits.find_first_not_of('0');
    if (first == std::string::npos) {
        return "0";
    }
    digits.erase(0, first);
    std::int64_t e = exponent - static_cast<std::int64_t>(frac_part.size());
    const auto last = digits.find_last_not_of('0');
    e += static_cast<std::int64_t>(digits.size() - 1 - last);
    digits.erase(last + 1);
    const auto len = static_cast<std::int64_t>(digits.size());
    std::int64_t plain = 0;
    if (e >= 0) {
        plain = len + e;
    } else if (len > -e) {
        plain = len + 1;
    } else {
        plain = 2 - e;  // "0." then -e - len zeros, then the digits
    }
    if (plain + (negative ? 1 : 0) > static_cast<std::int64_t>(max_chars)) {
        return {};
    }
    std::string out;
    out.reserve(static_cast<std::size_t>(plain) + 1);
    if (negative) {
        out.push_back('-');
    }
    if (e >= 0) {
        out += digits;
        out.append(static_cast<std::size_t>(e), '0');
    } else if (len > -e) {
        const auto point = static_cast<std::size_t>(len + e);
        out.append(digits, 0, point);
        out.push_back('.');
        out.append(digits, point);
    } else {
        out += "0.";
        out.append(static_cast<std::size_t>(-e - len), '0');
        out += digits;
    }
    return out;
}

// ---- the record's key -------------------------------------------------------------

// Reads a record as one JSON object and finds one top-level member, keeping a
// number's digits exactly. Strict RFC 8259: UTF-8 throughout, no raw control
// characters in strings, nothing but whitespace after the object. Iterative, so
// a deeply nested record costs memory, never stack.
class KeyScanner {
public:
    KeyScanner(std::string_view text, std::string_view field) : s_(text), field_(field) {}

    KinesisKeyResult run() {
        ws();
        if (pos_ >= s_.size() || s_[pos_] != '{') {
            return refuse(s_.find_first_not_of(" \t\r\n") == std::string_view::npos
                              ? "it is not a JSON object: it is empty"
                              : "it is not a JSON object");
        }
        ++pos_;
        ws();
        if (pos_ < s_.size() && s_[pos_] == '}') {
            ++pos_;
        } else {
            for (;;) {
                ws();
                if (pos_ >= s_.size() || s_[pos_] != '"') {
                    return refuse("it is not valid JSON: a member name is not a string");
                }
                std::string name;
                bool lone = false;
                if (!string_at(&name, lone)) {
                    return refuse(problem_);
                }
                ws();
                if (!eat(':')) {
                    return refuse("it is not valid JSON: a member name is not followed by ':'");
                }
                ws();
                if (!lone && name == field_) {
                    if (found_) {
                        return refuse("member \"" + std::string{field_} +
                                      "\" appears more than once, so its key is ambiguous");
                    }
                    found_ = true;
                    if (!key_value()) {
                        return refuse(problem_);
                    }
                } else if (!skip_value()) {
                    return refuse(problem_);
                }
                ws();
                if (eat(',')) {
                    continue;
                }
                if (eat('}')) {
                    break;
                }
                return refuse("it is not valid JSON: a member is not followed by ',' or '}'");
            }
        }
        ws();
        if (pos_ != s_.size()) {
            return refuse("it is not valid JSON: something follows the object");
        }
        if (!found_) {
            return refuse("it has no member \"" + std::string{field_} + "\"");
        }
        if (auto problem = kinesis_key_problem(key_); !problem.empty()) {
            return refuse("member \"" + std::string{field_} + "\" " + problem);
        }
        return KinesisKeyResult{true, std::move(key_), {}};
    }

private:
    static KinesisKeyResult refuse(std::string why) {
        return KinesisKeyResult{false, {}, std::move(why)};
    }

    bool fail(std::string why) {
        problem_ = std::move(why);
        return false;
    }

    void ws() {
        while (pos_ < s_.size() &&
               (s_[pos_] == ' ' || s_[pos_] == '\t' || s_[pos_] == '\n' || s_[pos_] == '\r')) {
            ++pos_;
        }
    }

    bool eat(char c) {
        if (pos_ < s_.size() && s_[pos_] == c) {
            ++pos_;
            return true;
        }
        return false;
    }

    bool hex4(char32_t& cp) {
        if (s_.size() - pos_ < 4) {
            return fail("it is not valid JSON: a \\u escape is cut short");
        }
        cp = 0;
        for (int k = 0; k < 4; ++k) {
            const char c = s_[pos_++];
            cp <<= 4;
            if (c >= '0' && c <= '9') {
                cp |= static_cast<char32_t>(c - '0');
            } else if (c >= 'a' && c <= 'f') {
                cp |= static_cast<char32_t>(c - 'a' + 10);
            } else if (c >= 'A' && c <= 'F') {
                cp |= static_cast<char32_t>(c - 'A' + 10);
            } else {
                return fail("it is not valid JSON: a \\u escape holds a non-hex digit");
            }
        }
        return true;
    }

    // The string at pos_ (an opening quote). With `out`, decodes it there; `lone`
    // is set when it holds an escaped surrogate without its pair, which no UTF-8
    // string can hold.
    bool string_at(std::string* out, bool& lone) {
        ++pos_;
        for (;;) {
            if (pos_ >= s_.size()) {
                return fail("it is not valid JSON: a string is not closed");
            }
            const char c = s_[pos_];
            if (c == '"') {
                ++pos_;
                return true;
            }
            if (c == '\\') {
                if (++pos_ >= s_.size()) {
                    return fail("it is not valid JSON: a string is not closed");
                }
                const char e = s_[pos_++];
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
                        char32_t cp = 0;
                        if (!hex4(cp)) {
                            return false;
                        }
                        if (cp >= 0xD800 && cp <= 0xDBFF) {
                            char32_t low = 0;
                            if (s_.size() - pos_ >= 6 && s_[pos_] == '\\' && s_[pos_ + 1] == 'u') {
                                const auto mark = pos_;
                                pos_ += 2;
                                if (!hex4(low)) {
                                    return false;
                                }
                                if (low >= 0xDC00 && low <= 0xDFFF) {
                                    cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                                } else {
                                    pos_ = mark;  // read the next escape on its own
                                    lone = true;
                                    continue;
                                }
                            } else {
                                lone = true;
                                continue;
                            }
                        } else if (cp >= 0xDC00 && cp <= 0xDFFF) {
                            lone = true;
                            continue;
                        }
                        if (out != nullptr) {
                            append_utf8(*out, cp);
                        }
                        continue;
                    }
                    default:
                        return fail("it is not valid JSON: a string holds an invalid escape");
                }
                if (out != nullptr) {
                    out->push_back(plain);
                }
                continue;
            }
            if (static_cast<unsigned char>(c) < 0x20) {
                return fail("it is not valid JSON: a string holds an unescaped control character");
            }
            const auto n = utf8_sequence(s_, pos_);
            if (n == 0) {
                return fail("it is not valid UTF-8 at byte " + std::to_string(pos_));
            }
            if (out != nullptr) {
                out->append(s_.substr(pos_, n));
            }
            pos_ += n;
        }
    }

    bool number_at(std::string_view& token) {
        const auto start = pos_;
        eat('-');
        if (pos_ >= s_.size()) {
            return fail("it is not valid JSON: a number is malformed");
        }
        if (s_[pos_] == '0') {
            ++pos_;
        } else if (s_[pos_] >= '1' && s_[pos_] <= '9') {
            while (pos_ < s_.size() && is_digit(s_[pos_])) {
                ++pos_;
            }
        } else {
            return fail("it is not valid JSON: a number is malformed");
        }
        if (eat('.')) {
            if (pos_ >= s_.size() || !is_digit(s_[pos_])) {
                return fail("it is not valid JSON: a number is malformed");
            }
            while (pos_ < s_.size() && is_digit(s_[pos_])) {
                ++pos_;
            }
        }
        if (pos_ < s_.size() && (s_[pos_] == 'e' || s_[pos_] == 'E')) {
            ++pos_;
            if (pos_ < s_.size() && (s_[pos_] == '+' || s_[pos_] == '-')) {
                ++pos_;
            }
            if (pos_ >= s_.size() || !is_digit(s_[pos_])) {
                return fail("it is not valid JSON: a number is malformed");
            }
            while (pos_ < s_.size() && is_digit(s_[pos_])) {
                ++pos_;
            }
        }
        token = s_.substr(start, pos_ - start);
        return true;
    }

    bool literal_at(std::string_view word) {
        if (s_.substr(pos_, word.size()) != word) {
            return fail("it is not valid JSON: an unexpected character at byte " +
                        std::to_string(pos_));
        }
        pos_ += word.size();
        return true;
    }

    // The key member's value.
    bool key_value() {
        const std::string member = "member \"" + std::string{field_} + "\" ";
        if (pos_ >= s_.size()) {
            return fail("it is not valid JSON: the record ends inside a value");
        }
        const char c = s_[pos_];
        if (c == '"') {
            bool lone = false;
            if (!string_at(&key_, lone)) {
                return false;
            }
            if (lone) {
                return fail(member +
                            "holds an escaped surrogate without its pair, which no UTF-8 string, "
                            "so no partition key, can hold");
            }
            return true;
        }
        if (c == '-' || is_digit(c)) {
            std::string_view token;
            if (!number_at(token)) {
                return false;
            }
            key_ = plain_decimal(token, kKinesisMaxPartitionKeyChars);
            if (key_.empty()) {
                return fail(member + "is a number whose plain decimal form is longer than " +
                            std::to_string(kKinesisMaxPartitionKeyChars) +
                            " characters, the most a partition key holds");
            }
            return true;
        }
        if (c == 't' || c == 'f') {
            const std::string_view word = c == 't' ? "true" : "false";
            if (!literal_at(word)) {
                return false;
            }
            key_ = std::string{word};
            return true;
        }
        if (c == 'n') {
            return literal_at("null") && fail(member + "is null");
        }
        if (c == '{' || c == '[') {
            return fail(member + "is " + (c == '{' ? "an object" : "an array") +
                        "; a partition key comes from a string, a number or a boolean");
        }
        return fail("it is not valid JSON: an unexpected character at byte " +
                    std::to_string(pos_));
    }

    // A member name inside a nested object, then its ':'.
    bool nested_name() {
        ws();
        if (pos_ >= s_.size() || s_[pos_] != '"') {
            return fail("it is not valid JSON: a member name is not a string");
        }
        bool lone = false;
        if (!string_at(nullptr, lone)) {
            return false;
        }
        ws();
        return eat(':') || fail("it is not valid JSON: a member name is not followed by ':'");
    }

    // One value of any kind, checked and passed over.
    bool skip_value() {
        std::vector<char> open;  // the containers the scan is inside
        for (;;) {
            ws();
            if (pos_ >= s_.size()) {
                return fail("it is not valid JSON: the record ends inside a value");
            }
            const char c = s_[pos_];
            if (c == '{' || c == '[') {
                ++pos_;
                ws();
                if (pos_ < s_.size() && s_[pos_] == (c == '{' ? '}' : ']')) {
                    ++pos_;
                } else {
                    open.push_back(c);
                    if (c == '{' && !nested_name()) {
                        return false;
                    }
                    continue;
                }
            } else if (c == '"') {
                bool lone = false;
                if (!string_at(nullptr, lone)) {
                    return false;
                }
            } else if (c == '-' || is_digit(c)) {
                std::string_view token;
                if (!number_at(token)) {
                    return false;
                }
            } else if (!literal_at(c == 't' ? "true" : c == 'f' ? "false" : "null")) {
                return false;
            }
            // A value is complete: close what it completes, or go on to the next.
            for (;;) {
                if (open.empty()) {
                    return true;
                }
                ws();
                if (pos_ >= s_.size()) {
                    return fail("it is not valid JSON: the record ends inside a value");
                }
                const char close = open.back() == '{' ? '}' : ']';
                if (s_[pos_] == ',') {
                    ++pos_;
                    if (open.back() == '{' && !nested_name()) {
                        return false;
                    }
                    break;
                }
                if (s_[pos_] == close) {
                    ++pos_;
                    open.pop_back();
                    continue;
                }
                return fail(
                    std::string{"it is not valid JSON: a value is not followed by ',' or '"} +
                    close + "'");
            }
        }
    }

    std::string_view s_;
    std::string_view field_;
    std::size_t pos_{0};
    bool found_{false};
    std::string key_;
    std::string problem_;
};

// ---- the service's answers -----------------------------------------------------------

// The code without the protocol prefix some services add.
std::string bare_error_code(std::string_view code) {
    if (const auto hash = code.rfind('#'); hash != std::string_view::npos) {
        code.remove_prefix(hash + 1);
    }
    if (const auto colon = code.find(':'); colon != std::string_view::npos) {
        code = code.substr(0, colon);
    }
    return std::string{code};
}

// A service message with anything that identifies credentials removed: access
// key ids (AKIA..., ASIA...) and the values after Credential=, Signature= and
// X-Amz-Security-Token=. Error text reaches logs and job failure reports.
std::string without_credentials(std::string_view message) {
    std::string out;
    out.reserve(message.size());
    const auto upper_alnum = [](char c) {
        return (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
    };
    std::size_t i = 0;
    while (i < message.size()) {
        bool replaced = false;
        for (const std::string_view marker : {std::string_view{"Credential="},
                                              std::string_view{"Signature="},
                                              std::string_view{"X-Amz-Security-Token="}}) {
            if (message.substr(i, marker.size()) == marker) {
                out.append(marker);
                out += "<removed>";
                i += marker.size();
                while (i < message.size() && message[i] != ',' && message[i] != ' ' &&
                       message[i] != '&' && message[i] != '\'' && message[i] != '"') {
                    ++i;
                }
                replaced = true;
                break;
            }
        }
        if (replaced) {
            continue;
        }
        const auto rest = message.substr(i);
        if ((rest.substr(0, 4) == "AKIA" || rest.substr(0, 4) == "ASIA") && rest.size() >= 20 &&
            std::all_of(rest.begin() + 4, rest.begin() + 20, upper_alnum) &&
            (i == 0 || !upper_alnum(message[i - 1]))) {
            out += "<access key id>";
            i += 20;
            continue;
        }
        out.push_back(message[i++]);
    }
    return out;
}

// What to check for an error the sink does not retry, by code.
std::string permanent_hint(const std::string& code) {
    if (code == "ResourceNotFoundException") {
        return " Check that the stream exists in the configured region (and at "
               "endpoint_override, if set).";
    }
    if (code.find("AccessDenied") != std::string::npos || code == "UnrecognizedClientException" ||
        code == "InvalidSignatureException" || code == "SignatureDoesNotMatch" ||
        code.find("MissingAuthenticationToken") != std::string::npos ||
        code.find("IncompleteSignature") != std::string::npos || code == "InvalidClientTokenId" ||
        code.find("ExpiredToken") != std::string::npos || code == "NotAuthorized") {
        return " Check the credentials the AWS SDK's default chain finds (environment, profile, "
               "container or instance role): the sink needs kinesis:PutRecords on the stream.";
    }
    if (code.rfind("KMS", 0) == 0) {
        return " The stream's server-side encryption key is not usable (disabled, missing, not "
               "accessible, or the account is not subscribed to KMS).";
    }
    if (code == "InvalidArgumentException" || code == "ValidationException" ||
        code == "ValidationError") {
        return " The sink checks every record and request against Kinesis's documented limits "
               "before sending it, so check that max_record_bytes does not exceed the stream's "
               "maximum record size (MaxRecordSizeInKiB x 1024); otherwise a refusal the sink "
               "did not foresee may be a defect in the sink, and the service's message names "
               "the rule.";
    }
    return "";
}

std::string error_text(const KinesisPutError& e) {
    std::string out = e.code;
    if (e.http_status > 0) {
        out += " (HTTP " + std::to_string(e.http_status) + ")";
    }
    if (!e.message.empty()) {
        out += ": " + without_credentials(e.message);
    }
    return out;
}

// A sequence number as the service writes one: 0, or up to 129 digits with no
// leading zero.
bool valid_sequence_number(std::string_view s) {
    if (s.empty() || s.size() > 129 || !std::all_of(s.begin(), s.end(), is_digit)) {
        return false;
    }
    return s == "0" || s.front() != '0';
}

bool valid_name_chars(std::string_view s) {
    return std::all_of(s.begin(), s.end(), [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || is_digit(c) || c == '_' ||
               c == '.' || c == '-';
    });
}

// A ShardId as the service writes one: 1 to 128 of a-z, A-Z, 0-9, '_', '.', '-'.
bool valid_shard_id(std::string_view s) {
    return !s.empty() && s.size() <= 128 && valid_name_chars(s);
}

// ---- options ------------------------------------------------------------------------

bool valid_stream_name(std::string_view s) {
    return !s.empty() && s.size() <= 128 && valid_name_chars(s);
}

// arn:<partition>:kinesis:<region>:<12-digit account>:stream/<name>, at most
// 2048 characters, as the service's StreamARN pattern requires.
bool valid_stream_arn(std::string_view s) {
    if (s.size() > 2048 || s.rfind("arn:aws", 0) != 0) {
        return false;
    }
    std::vector<std::string_view> parts;
    for (std::size_t from = 0; parts.size() < 5;) {
        const auto colon = s.find(':', from);
        if (colon == std::string_view::npos) {
            return false;
        }
        parts.push_back(s.substr(from, colon - from));
        from = colon + 1;
        if (parts.size() == 5) {
            parts.push_back(s.substr(from));
        }
    }
    const auto& account = parts[4];
    const auto& resource = parts[5];
    return parts[2] == "kinesis" && account.size() == 12 &&
           std::all_of(account.begin(), account.end(), is_digit) &&
           resource.rfind("stream/", 0) == 0 && resource.size() > 7 &&
           resource.find_first_of(" \t\r\n") == std::string_view::npos;
}

// An endpoint must be an http(s) URL naming a host, with no user information:
// a URL that carried credentials would put them in every message naming it.
bool valid_endpoint(const std::string& e) {
    std::string_view rest{e};
    if (rest.rfind("https://", 0) == 0) {
        rest.remove_prefix(8);
    } else if (rest.rfind("http://", 0) == 0) {
        rest.remove_prefix(7);
    } else {
        return false;
    }
    const auto authority = rest.substr(0, rest.find_first_of("/?#"));
    return !authority.empty() && authority.find('@') == std::string_view::npos &&
           authority.find_first_of(" \t\r\n") == std::string_view::npos;
}

std::chrono::milliseconds backoff_delay(int round,
                                        std::chrono::milliseconds base,
                                        std::chrono::milliseconds cap) {
    auto d = base;
    for (int i = 1; i < round && d < cap; ++i) {
        d *= 2;
    }
    return std::min(d, cap);
}

std::string with_thousands(std::size_t n) {
    std::string digits = std::to_string(n);
    for (auto i = static_cast<std::ptrdiff_t>(digits.size()) - 3; i > 0; i -= 3) {
        digits.insert(static_cast<std::size_t>(i), ",");
    }
    return digits;
}

// Ends the call in flight on a KinesisWriteService once the task that owns the
// sink is cancelled, so a cancelled task stops at once rather than waiting out
// the SDK's request timeout. One thread per sink, asleep unless a call is armed.
class CallWatch {
public:
    explicit CallWatch(std::shared_ptr<KinesisWriteService> service)
        : service_(std::move(service)), thread_([this] { run(); }) {}

    ~CallWatch() {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        thread_.join();
    }

    CallWatch(const CallWatch&) = delete;
    CallWatch& operator=(const CallWatch&) = delete;
    CallWatch(CallWatch&&) = delete;
    CallWatch& operator=(CallWatch&&) = delete;

    void arm(CancelSignal cancel) {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            armed_ = true;
            cancel_ = std::move(cancel);
        }
        cv_.notify_all();
    }

    // True when the watch aborted the call armed last.
    bool disarm() {
        const std::lock_guard<std::mutex> lock(mu_);
        armed_ = false;
        return aborted_;
    }

private:
    void run() {
        std::unique_lock<std::mutex> lock(mu_);
        while (!stop_) {
            if (!armed_ || aborted_) {
                cv_.wait(lock, [this] { return stop_ || (armed_ && !aborted_); });
                continue;
            }
            cv_.wait_for(lock, std::chrono::milliseconds{20});
            if (stop_ || !armed_ || !cancel_.requested()) {
                continue;
            }
            aborted_ = true;
            lock.unlock();
            service_->abort();
            lock.lock();
        }
    }

    std::shared_ptr<KinesisWriteService> service_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool stop_{false};
    bool armed_{false};
    bool aborted_{false};
    CancelSignal cancel_;
    std::thread thread_;
};

// Why one request's records could not be stored, and what to check; flush()
// adds what it had sent and fails the sink.
class SendFailure : public std::runtime_error {
public:
    explicit SendFailure(const std::string& why, std::string hint = {})
        : std::runtime_error(why), hint_(std::move(hint)) {}
    [[nodiscard]] const std::string& hint() const noexcept { return hint_; }

private:
    std::string hint_;
};

}  // namespace

// ---- partition keys -----------------------------------------------------------------

std::size_t kinesis_key_chars(std::string_view key) {
    std::size_t units = 0;
    for (std::size_t i = 0; i < key.size();) {
        const auto n = std::max<std::size_t>(utf8_sequence(key, i), 1);
        units += n == 4 ? 2 : 1;
        i += n;
    }
    return units;
}

std::string kinesis_key_problem(std::string_view key) {
    if (key.empty()) {
        return "is an empty string; a partition key holds 1 to 256 characters";
    }
    for (std::size_t i = 0; i < key.size();) {
        const auto n = utf8_sequence(key, i);
        if (n == 0) {
            return "is not valid UTF-8";
        }
        i += n;
    }
    if (key.find('\0') != std::string_view::npos) {
        return "holds U+0000, at which the AWS SDK for C++ cuts a string when it writes the "
               "request, so the record would be routed by a shorter key";
    }
    if (const auto chars = kinesis_key_chars(key); chars > kKinesisMaxPartitionKeyChars) {
        return "is " + std::to_string(chars) +
               " characters long, counted in UTF-16 code units; a partition key holds at most " +
               std::to_string(kKinesisMaxPartitionKeyChars);
    }
    return {};
}

KinesisKeyResult kinesis_record_key(std::string_view record, std::string_view field) {
    return KeyScanner(record, field).run();
}

// ---- responses ----------------------------------------------------------------------

KinesisPutVerdict judge_kinesis_response(std::size_t sent, const KinesisPutResult& result) {
    KinesisPutVerdict v;
    if (result.entries.size() != sent) {
        v.why = "returned " + std::to_string(result.entries.size()) + " entries for " +
                std::to_string(sent) + " records";
        return v;
    }
    std::set<std::string_view> sequences;
    for (std::size_t i = 0; i < sent; ++i) {
        const auto& e = result.entries[i];
        const auto at = "entry " + std::to_string(i) + " ";
        const bool stored = !e.sequence_number.empty() || !e.shard_id.empty();
        const bool failed = !e.error_code.empty();
        if (stored == failed) {
            v.why = at + (stored ? "holds both a stored record's SequenceNumber or ShardId and an "
                                   "ErrorCode"
                                 : "holds neither a SequenceNumber and ShardId nor an ErrorCode");
            v.failed.clear();
            return v;
        }
        if (failed) {
            v.failed.push_back(i);
            continue;
        }
        if (!valid_sequence_number(e.sequence_number)) {
            v.why = at + (e.sequence_number.empty() ? "has a ShardId but no SequenceNumber"
                                                    : "holds a malformed SequenceNumber");
            v.failed.clear();
            return v;
        }
        if (!valid_shard_id(e.shard_id)) {
            v.why = at + (e.shard_id.empty() ? "has a SequenceNumber but no ShardId"
                                             : "holds a malformed ShardId");
            v.failed.clear();
            return v;
        }
        if (!sequences.insert(e.sequence_number).second) {
            v.why = at + "repeats the SequenceNumber of an earlier entry, which names one record";
            v.failed.clear();
            return v;
        }
    }
    if (result.failed_record_count < 0 ||
        static_cast<std::uint64_t>(result.failed_record_count) != v.failed.size()) {
        v.why = "reports FailedRecordCount " + std::to_string(result.failed_record_count) +
                " where " + std::to_string(v.failed.size()) + " entries failed";
        v.failed.clear();
        return v;
    }
    v.valid = true;
    return v;
}

KinesisPutFault classify_kinesis_put_error(std::string_view raw_code,
                                           int http_status,
                                           bool sdk_retryable) {
    // A request refused for the stream's throughput, or the KMS key's, stored
    // nothing. InternalFailure, ServiceUnavailable and a missing reply leave the
    // request's outcome unknown. Credentials, access, a missing stream, KMS
    // configuration and a refused request fail at once, as for the other AWS
    // connectors: the job's restart opens the sink again.
    static const std::set<std::string, std::less<>> kThrottled = {
        "ProvisionedThroughputExceededException",
        "ThrottlingException",
        "Throttling",
        "ThrottledException",
        "LimitExceededException",
        "TooManyRequestsException",
        "RequestLimitExceeded",
        "KMSThrottlingException",
        "SlowDown",
    };
    static const std::set<std::string, std::less<>> kPermanent = {
        "AccessDeniedException",
        "AccessDenied",
        "NotAuthorized",
        "UnrecognizedClientException",
        "InvalidSignatureException",
        "SignatureDoesNotMatch",
        "IncompleteSignature",
        "IncompleteSignatureException",
        "MissingAuthenticationToken",
        "MissingAuthenticationTokenException",
        "InvalidClientTokenId",
        "InvalidAccessKeyId",
        "ExpiredToken",
        "ExpiredTokenException",
        "RequestExpired",
        "OptInRequired",
        "InvalidArgumentException",
        "ValidationException",
        "ValidationError",
        "ResourceNotFoundException",
        "KMSAccessDeniedException",
        "KMSDisabledException",
        "KMSInvalidStateException",
        "KMSNotFoundException",
        "KMSOptInRequired",
        "DryRunOperationException",
        "SerializationException",
        "UnknownOperationException",
        "MalformedHttpRequestException",
        "RequestEntityTooLargeException",
    };
    static const std::set<std::string, std::less<>> kTransient = {
        "InternalFailure",
        "InternalFailureException",
        "InternalServerError",
        "InternalServerErrorException",
        "ServiceUnavailable",
        "ServiceUnavailableException",
        "RequestTimeout",
        "RequestTimeoutException",
        "RequestAbortedException",
        // The stream is being updated (resharding); DELETING becomes
        // ResourceNotFound, which is permanent.
        "ResourceInUseException",
    };
    const std::string code = bare_error_code(raw_code);
    if (kThrottled.count(code) != 0 || http_status == 429) {
        return KinesisPutFault::Throttled;
    }
    if (kPermanent.count(code) != 0) {
        return KinesisPutFault::Permanent;
    }
    if (kTransient.count(code) != 0 || http_status == 0 || http_status >= 500) {
        return KinesisPutFault::Transient;
    }
    return sdk_retryable ? KinesisPutFault::Transient : KinesisPutFault::Permanent;
}

// ---- the sink -----------------------------------------------------------------------

struct KinesisSink::Impl {
    explicit Impl(KinesisSinkOptions o)
        : opts(std::move(o)),
          before_write_point(opts.name + ".before_write"),
          after_write_point(opts.name + ".after_write") {}

    struct Entry {
        std::string data;
        std::string key;
    };

    // A record of the batch on_data is given, checked and keyed or refused.
    struct Checked {
        std::string key;
        std::string refusal;  // non-empty: refused
    };

    KinesisSinkOptions opts;
    std::shared_ptr<KinesisWriteService> service;
    bool opened{false};
    std::vector<Entry> pending;
    std::size_t pending_bytes{0};  // data and keys, as the request limit counts them
    std::size_t pending_data_bytes{0};
    std::uint64_t next_key{0};  // the numbered keys' next value, without partition_key
    std::uint64_t dropped{0};
    std::chrono::steady_clock::time_point first_buffered_at{};
    std::optional<std::string> failure;  // why this instance stopped
    std::string before_write_point;
    std::string after_write_point;
    std::unique_ptr<CallWatch> watch;

    // Stops this instance: drops what is buffered, refuses every later record,
    // flush and barrier until open(), and throws `why` with how many records were
    // dropped and, last, what to check.
    [[noreturn]] void fail_now(std::string why,
                               std::size_t dropping,
                               const std::string& hint = {}) {
        if (dropping > 0) {
            why += "; " + std::to_string(dropping) +
                   " buffered record(s) not yet accepted are dropped with this sink instance";
        }
        why +=
            ". The sink accepts nothing more, and the job's restart replays every record after "
            "the last completed checkpoint." +
            hint;
        pending.clear();
        pending_bytes = 0;
        pending_data_bytes = 0;
        failure = why;
        clink::metrics::connector::error_inc(kConnector, "sink");
        clink::metrics::connector::permanent_failures_inc(kConnector);
        clink::log::error(kLogComponent, opts.name + ": " + why);
        throw std::runtime_error(opts.name + ": " + why);
    }

    void check_usable() const {
        if (failure) {
            throw std::runtime_error(opts.name + ": the sink failed earlier and dropped records, " +
                                     "so it refuses to send or acknowledge a barrier (" + *failure +
                                     ")");
        }
        if (!opened) {
            throw std::runtime_error(opts.name + ": open() has not been called");
        }
    }

    // The pause before a retry, cut short when the task is cancelled.
    void pause(RuntimeContext* rt, std::chrono::milliseconds delay) {
        const auto until = std::chrono::steady_clock::now() + delay;
        for (;;) {
            if (rt != nullptr && rt->cancel_requested()) {
                throw std::runtime_error("cancelled while waiting to retry a PutRecords");
            }
            const auto now = std::chrono::steady_clock::now();
            if (now >= until) {
                return;
            }
            std::this_thread::sleep_for(
                std::min<std::chrono::steady_clock::duration>(kCancelPoll, until - now));
        }
    }

    void arm(RuntimeContext* rt) {
        if (rt == nullptr) {
            return;
        }
        if (!watch) {
            watch = std::make_unique<CallWatch>(service);
        }
        watch->arm(rt->cancel_signal());
    }

    bool disarm() { return watch && watch->disarm(); }

    void open() {
        failure.reset();
        pending.clear();
        pending_bytes = 0;
        pending_data_bytes = 0;
        watch.reset();
        service = opts.service ? opts.service : make_kinesis_write_sdk_service(opts.client);
        opened = true;
        clink::log::info(
            kLogComponent,
            opts.name + ": writing to stream '" + opts.stream + "', each record keyed " +
                (opts.partition_key.empty() ? std::string{"by a number this instance counts"}
                                            : "by its member \"" + opts.partition_key + "\"") +
                ", up to " + std::to_string(opts.batch_records) + " records and " +
                std::to_string(opts.max_request_bytes) + " bytes per PutRecords, records up to " +
                std::to_string(opts.max_record_bytes) + " bytes with their keys, dlq=" +
                (opts.dlq_policy == DlqPolicy::Drop ? "drop" : "fail"));
    }

    // Keys every record of `batch` or refuses it, in order. Numbered keys go to
    // the records kept, so a refused record takes no number.
    std::vector<Checked> check(const Batch<std::string>& batch) const {
        std::vector<Checked> out;
        out.reserve(batch.size());
        std::uint64_t number = next_key;
        std::size_t i = 0;
        for (const auto& rec : batch) {
            ++i;
            const auto& data = rec.value();
            const auto where =
                "record " + std::to_string(i) + " of a batch of " + std::to_string(batch.size());
            Checked c;
            if (opts.partition_key.empty()) {
                c.key = std::to_string(number);
            } else {
                auto k = kinesis_record_key(data, opts.partition_key);
                if (!k.ok) {
                    c.refusal = where + " has no usable partition key: " + k.why;
                    out.push_back(std::move(c));
                    continue;
                }
                c.key = std::move(k.key);
            }
            const auto bytes = data.size() + c.key.size();
            if (bytes > opts.max_record_bytes) {
                c.refusal = where + " is " + with_thousands(bytes) + " bytes with its " +
                            std::to_string(c.key.size()) +
                            "-byte partition key; the stream's records take at most " +
                            with_thousands(opts.max_record_bytes) +
                            " (max_record_bytes), so split or compress it before the sink, or "
                            "raise the stream's maximum record size and max_record_bytes with it";
                c.key.clear();
            } else if (opts.partition_key.empty()) {
                ++number;
            }
            out.push_back(std::move(c));
        }
        return out;
    }

    void drop(RuntimeContext* rt, const std::string& record, const std::string& why) {
        if (rt != nullptr) {
            rt->report_bad_record(clink::BadRecord{.payload = record,
                                                   .error = why,
                                                   .connector = kConnector,
                                                   .direction = "sink",
                                                   .location = opts.stream});
        }
        clink::metrics::connector::dropped_records_inc(kConnector, 1);
        clink::metrics::connector::permanent_failures_inc(kConnector);
        ++dropped;
        if ((dropped & (dropped - 1)) == 0) {
            clink::log::warn(kLogComponent,
                             opts.name + ": dropped " + std::to_string(dropped) +
                                 " record(s) to the dead-letter path under dlq=drop; last: " + why);
        }
    }

    void buffer(const std::string& data, std::string key) {
        if (pending.empty()) {
            first_buffered_at = std::chrono::steady_clock::now();
        }
        pending_bytes += data.size() + key.size();
        pending_data_bytes += data.size();
        pending.push_back(Entry{data, std::move(key)});
    }

    // Sends the buffered records in one PutRecords and its retries.
    void send(RuntimeContext* rt) {
        std::vector<std::size_t> outstanding(pending.size());
        for (std::size_t i = 0; i < outstanding.size(); ++i) {
            outstanding[i] = i;
        }
        const auto deadline = std::chrono::steady_clock::now() + opts.retry_timeout;
        int idle = 0;  // attempts in a row that stored nothing
        int attempts = 0;
        std::string last_problem;
        for (;;) {
            if (attempts > 0) {
                if (idle > opts.max_retries) {
                    throw SendFailure(std::to_string(outstanding.size()) + " of " +
                                      std::to_string(pending.size()) +
                                      " record(s) of a PutRecords were still not stored after " +
                                      std::to_string(idle) +
                                      " attempt(s) in a row that stored nothing (max_retries=" +
                                      std::to_string(opts.max_retries) + "; last: " + last_problem +
                                      ")");
                }
                const auto delay =
                    backoff_delay(std::max(idle, 1), opts.retry_base_backoff, opts.max_backoff);
                if (std::chrono::steady_clock::now() + delay > deadline) {
                    throw SendFailure(std::to_string(outstanding.size()) + " of " +
                                      std::to_string(pending.size()) +
                                      " record(s) of a PutRecords were still not stored when the "
                                      "retry budget of " +
                                      std::to_string(opts.retry_timeout.count()) + " ms ran out (" +
                                      std::to_string(attempts) +
                                      " attempt(s); last: " + last_problem + ")");
                }
                if ((attempts & (attempts - 1)) == 0) {
                    clink::log::warn(kLogComponent,
                                     opts.name + ": " + std::to_string(outstanding.size()) +
                                         " record(s) not stored (" + last_problem + "); attempt " +
                                         std::to_string(attempts + 1) + " in " +
                                         std::to_string(delay.count()) + " ms");
                }
                try {
                    pause(rt, delay);
                } catch (const std::runtime_error& e) {
                    throw SendFailure(std::string{e.what()} + "; " +
                                      std::to_string(outstanding.size()) +
                                      " record(s) of it not stored");
                }
            }
            ++attempts;
            (void)CLINK_FAULT_POINT(before_write_point);
            std::vector<KinesisPutRecord> records;
            records.reserve(outstanding.size());
            for (const auto i : outstanding) {
                records.push_back(KinesisPutRecord{pending[i].data, pending[i].key});
            }
            arm(rt);
            auto result = service->put_records(opts.stream, records);
            const bool aborted = disarm();
            if (aborted || result.error.fault == KinesisPutFault::Cancelled) {
                throw SendFailure("cancelled while a PutRecords of " +
                                  std::to_string(outstanding.size()) +
                                  " record(s) was in flight; whether they were stored is unknown, "
                                  "so a replay may store them twice");
            }
            if (!result.error.ok()) {
                if (result.error.fault == KinesisPutFault::Permanent) {
                    throw SendFailure("PutRecords was refused: " + error_text(result.error),
                                      permanent_hint(bare_error_code(result.error.code)));
                }
                // Throttled: nothing was stored. Transient: some or all of it
                // may have been; sending it again can store those twice.
                ++idle;
                last_problem = error_text(result.error);
                continue;
            }
            const auto verdict = judge_kinesis_response(outstanding.size(), result);
            if (!verdict.valid) {
                throw SendFailure("PutRecords's response does not account for its request: it " +
                                  verdict.why + ". Which of the " +
                                  std::to_string(outstanding.size()) +
                                  " record(s) Kinesis stored is unknown, so the flush fails "
                                  "rather than count them stored, and a replay may store some "
                                  "twice");
            }
            if (verdict.failed.empty()) {
                (void)CLINK_FAULT_POINT(after_write_point);
                return;
            }
            std::vector<std::size_t> again;
            again.reserve(verdict.failed.size());
            for (const auto pos : verdict.failed) {
                again.push_back(outstanding[pos]);
            }
            const auto& first_failed = result.entries[verdict.failed.front()];
            idle = again.size() < outstanding.size() ? 0 : idle + 1;
            last_problem = std::to_string(again.size()) + " of " +
                           std::to_string(outstanding.size()) + " failed, first with " +
                           first_failed.error_code +
                           (first_failed.error_message.empty()
                                ? std::string{}
                                : ": " + without_credentials(first_failed.error_message));
            outstanding = std::move(again);
        }
    }

    void flush(RuntimeContext* rt) {
        check_usable();
        if (pending.empty()) {
            return;
        }
        const auto n = pending.size();
        const auto bytes = pending_data_bytes;
        const auto t0 = std::chrono::steady_clock::now();
        try {
            send(rt);
        } catch (const SendFailure& e) {
            fail_now(std::string{e.what()} + " (in a flush of " + std::to_string(n) + " record(s))",
                     n,
                     e.hint());
        }
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::records_out_inc(kConnector, n);
        clink::metrics::connector::bytes_out_inc(kConnector, bytes);
        clink::metrics::connector::commit_latency_observe(kConnector,
                                                          static_cast<std::uint64_t>(dt));
        pending.clear();
        pending_bytes = 0;
        pending_data_bytes = 0;
    }

    bool linger_elapsed() const {
        return opts.max_age.count() > 0 && !pending.empty() &&
               std::chrono::steady_clock::now() - first_buffered_at >= opts.max_age;
    }
};

KinesisSink::KinesisSink(KinesisSinkOptions opts) {
    const auto refuse = [&](const std::string& why) {
        throw std::runtime_error(opts.name + ": " + why);
    };
    if (opts.stream.empty()) {
        refuse("'stream' is required");
    }
    if (opts.stream.rfind("arn:", 0) == 0 ? !valid_stream_arn(opts.stream)
                                          : !valid_stream_name(opts.stream)) {
        refuse(
            "'stream' must be a stream name (1 to 128 characters from a-z, A-Z, 0-9, '_', '.' and "
            "'-') or a stream ARN (arn:<partition>:kinesis:<region>:<account>:stream/<name>)");
    }
    if (!opts.partition_key.empty()) {
        for (std::size_t i = 0; i < opts.partition_key.size();) {
            const auto n = utf8_sequence(opts.partition_key, i);
            if (n == 0) {
                refuse("'partition_key' is not valid UTF-8");
            }
            i += n;
        }
    }
    if (opts.batch_records < 1 || opts.batch_records > kKinesisMaxBatchRecords) {
        refuse("'batch_records' must be 1 to 500 (PutRecords takes at most 500 records); it is " +
               std::to_string(opts.batch_records));
    }
    if (opts.max_record_bytes < 1 || opts.max_record_bytes > kKinesisMaxRecordBytes) {
        refuse("'max_record_bytes' must be 1 to " + std::to_string(kKinesisMaxRecordBytes) +
               " (10 MiB, the most a stream's maximum record size can be raised to); it is " +
               std::to_string(opts.max_record_bytes));
    }
    if (opts.max_request_bytes < opts.max_record_bytes ||
        opts.max_request_bytes > kKinesisMaxRequestBytes) {
        refuse("'max_request_bytes' must be " + std::to_string(opts.max_record_bytes) +
               " (max_record_bytes, so the largest record fits alone) to " +
               std::to_string(kKinesisMaxRequestBytes) + " (10 MiB); it is " +
               std::to_string(opts.max_request_bytes));
    }
    if (opts.max_retries < 0 || opts.max_retries > kMaxRetries) {
        refuse("'max_retries' must be 0 to " + std::to_string(kMaxRetries) + "; it is " +
               std::to_string(opts.max_retries));
    }
    if (opts.retry_base_backoff.count() <= 0 || opts.max_backoff < opts.retry_base_backoff) {
        refuse("the retry backoff must be positive, and its cap no smaller than its base");
    }
    if (opts.retry_timeout.count() <= 0) {
        refuse("'retry_timeout_ms' must be positive");
    }
    if (opts.max_age.count() < 0) {
        refuse("'linger_ms' must not be negative");
    }
    if (opts.client.endpoint_override && !valid_endpoint(*opts.client.endpoint_override)) {
        // The value is not repeated: it may hold credentials.
        refuse(
            "'endpoint_override' must be an http:// or https:// URL naming a host, without user "
            "information");
    }
    if (opts.client.region && opts.client.region->empty()) {
        refuse("'region' is empty");
    }
    if (opts.client.connect_timeout_ms <= 0 || opts.client.connect_timeout_ms > 600000 ||
        opts.client.request_timeout_ms <= 0 || opts.client.request_timeout_ms > 600000) {
        refuse("'connect_timeout_ms' and 'request_timeout_ms' must be 1 to 600000");
    }
    impl_ = std::make_unique<Impl>(std::move(opts));
}

KinesisSink::~KinesisSink() = default;

void KinesisSink::open() {
    impl_->open();
}

void KinesisSink::close() {
    impl_->watch.reset();
}

void KinesisSink::on_data(const Batch<std::string>& batch) {
    impl_->check_usable();
    auto* rt = runtime();
    auto checked = impl_->check(batch);
    if (impl_->opts.dlq_policy == DlqPolicy::Fail) {
        for (const auto& c : checked) {
            if (!c.refusal.empty()) {
                impl_->fail_now(c.refusal + ". Nothing of that batch was buffered or sent",
                                impl_->pending.size());
            }
        }
    }
    std::size_t i = 0;
    for (const auto& rec : batch) {
        auto& c = checked[i++];
        if (!c.refusal.empty()) {
            impl_->drop(rt, rec.value(), c.refusal);
            continue;
        }
        if (impl_->opts.partition_key.empty()) {
            ++impl_->next_key;
        }
        const auto need = rec.value().size() + c.key.size();
        // Send what is buffered first when this record would push the request
        // past its count or byte limit.
        if (!impl_->pending.empty() &&
            (impl_->pending.size() >= impl_->opts.batch_records ||
             impl_->pending_bytes + need > impl_->opts.max_request_bytes)) {
            impl_->flush(rt);
        }
        impl_->buffer(rec.value(), std::move(c.key));
        if (impl_->pending.size() >= impl_->opts.batch_records || impl_->linger_elapsed()) {
            impl_->flush(rt);
        }
    }
}

void KinesisSink::on_barrier(CheckpointBarrier /*b*/) {
    impl_->flush(runtime());
}

void KinesisSink::flush() {
    impl_->flush(runtime());
}

std::string KinesisSink::name() const {
    return impl_->opts.name;
}

std::size_t KinesisSink::pending_records() const {
    return impl_->pending.size();
}

bool KinesisSink::failed() const {
    return impl_->failure.has_value();
}

}  // namespace clink::aws
