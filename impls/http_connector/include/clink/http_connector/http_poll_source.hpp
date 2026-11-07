#pragma once

// HTTP polling source: GETs a JSON endpoint on an interval and emits the
// elements of the array each response carries, one record per element, as
// single-line JSON with every number as the endpoint wrote it. Two modes, each
// bounded or not:
//
//   Cursor mode (cursor_param and cursor_field). Each request carries the
//   cursor as a query parameter, and the endpoint answers the records strictly
//   after it, ascending by cursor_field, each value unique. Every page is
//   checked against that contract before a record is emitted or the cursor
//   moves: each record is an object with one usable cursor, the values rise
//   (cursor_type integer, compared exactly) or at least never repeat (opaque),
//   and none is at or before the cursor the request carried. The checkpoint
//   holds the cursor, so a restore polls again from it: at-least-once, and
//   exactly-once into a two-phase-commit sink, while the endpoint keeps what
//   it served. Bounded: ends at the first valid empty page.
//
//   Resource mode (neither). Every request is the same, and each response's
//   elements are emitted; one the server confirms unchanged (304 to the ETag
//   it gave for that same request) emits nothing. Nothing is replayable: the
//   checkpoint holds no position, and a restart reads the resource as it is
//   then. Bounded: ends after the first valid response.
//
// A poll counts only on HTTP 200 with a JSON body whose array is where
// records_field says (the body itself when it is empty), HTTP 204 (an empty
// page) or HTTP 304 to a conditional request. Anything else fails the poll and
// moves nothing: a missing, null or mistyped array, malformed JSON, a record
// clink's JSON reader cannot decode (an integer past 64 bits, a number past a
// double's range), another 2xx or 3xx, a page over max_response_records, a body
// over max_response_bytes.
// Throttling (429, 503, honouring Retry-After), 408, 425, other 5xx but 501 and
// 505, and transport failures are retried with backoff, bounded by max_retries
// and retry_timeout; any other 4xx fails at once. A request is bounded by
// request_timeout as a whole, and a cancel ends a wait or a request at once.
// Errors name the request's path without its query string, and never a header.
// See docs/connectors/http.md ("http_poll_source").

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "clink/operators/operator_base.hpp"

namespace clink::http_connector {

// Percent-encode a query-parameter value (RFC 3986 unreserved kept verbatim).
inline std::string url_encode(const std::string& v) {
    static const char* hex = "0123456789ABCDEF";
    std::string out;
    out.reserve(v.size());
    for (const char ch : v) {
        const auto c = static_cast<unsigned char>(ch);
        const bool unreserved = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                                (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' ||
                                c == '~';
        if (unreserved) {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('%');
            out.push_back(hex[c >> 4]);
            out.push_back(hex[c & 0x0F]);
        }
    }
    return out;
}

// How a record's cursor is read and compared.
enum class HttpPollCursorType : std::uint8_t {
    // A JSON string, or a number's text as written, sent back unchanged. Two
    // tokens are only ever compared for equality: no order is assumed.
    Opaque,
    // A decimal integer, as a JSON integer or a string of one, kept to every
    // digit and compared by value: up to 64 bits as a number (clink's JSON
    // reader holds no wider integer), up to kHttpPollMaxIntegerDigits as a string.
    Integer,
};

inline constexpr std::size_t kHttpPollMaxCursorBytes = 1024;
inline constexpr std::size_t kHttpPollMaxIntegerDigits = 100;

struct HttpPollOptions {
    std::string url;        // scheme://host[:port] (required); no user, path or query in it
    std::string path{"/"};  // request path from '/', which may carry a fixed query string
    std::string headers;    // "K: V; ..." (e.g. auth)
    bool verify_tls{true};
    // Cursor mode takes both; resource mode neither.
    std::string cursor_param;  // the query parameter the cursor is sent as
    std::string cursor_field;  // the record field each record's cursor is read from
    HttpPollCursorType cursor_type{HttpPollCursorType::Opaque};  // option cursor_type
    std::string records_field;  // response field holding the array (else the response is the array)
    std::string initial_cursor;  // cursor mode: where a fresh run starts (empty: no cursor)
    // From the start of one poll to the start of the next (option poll_interval_ms).
    std::chrono::milliseconds interval{1000};
    double jitter_frac{0.0};  // +/- fraction on the poll interval, 0 to 1
    // Cursor mode: end at the first valid empty page. Resource mode: end after
    // the first valid response.
    bool bounded{false};
    // Retries one poll may make after its first attempt, 0 to HttpPollSource::kMaxRetries.
    int max_retries{4};
    std::chrono::milliseconds retry_base_backoff{200};  // doubled per retry (option ..._ms)
    std::chrono::milliseconds max_backoff{10000};       // the longest backoff (option ..._ms)
    // The longest one poll may take from its first attempt to its last
    // (option retry_timeout_ms). A Retry-After that would end past it fails the
    // poll at once rather than waiting.
    std::chrono::milliseconds retry_timeout{60000};
    std::chrono::milliseconds connect_timeout{5000};   // option connect_timeout_ms
    std::chrono::milliseconds request_timeout{30000};  // one request, whole (option ..._ms)
    std::size_t max_response_bytes{std::size_t{16} * 1024 * 1024};  // the body, decompressed
    std::size_t max_response_records{100000};                       // elements in one response
    std::string name{"http_poll_source"};
};

// ---- reading a page -------------------------------------------------------------------

struct HttpPollPageRules {
    std::string records_field;  // empty: the body is the array
    std::string cursor_field;   // empty: resource mode, no cursor is read
    HttpPollCursorType cursor_type{HttpPollCursorType::Opaque};
    std::size_t max_records{100000};
};

// A body that passed every check, or why it did not.
struct HttpPollPage {
    bool valid{false};
    std::string why;
    std::vector<std::string> records;  // each element, single-line, as served
    std::string last_cursor;           // cursor mode: the last record's; empty for an empty page
};

// Reads the body of an HTTP 200 under `rules`, for a request that carried
// `cursor` (empty: none). The whole body is checked before anything is kept,
// so a page that fails a check yields no record.
[[nodiscard]] HttpPollPage read_http_poll_page(std::string_view body,
                                               const HttpPollPageRules& rules,
                                               const std::string& cursor);

// Two integer cursors as canonical decimal text ("-12", "0", "340"): negative,
// zero or positive as `a` is less than, equal to or greater than `b`.
[[nodiscard]] int compare_integer_cursors(std::string_view a, std::string_view b);

// ---- the source -----------------------------------------------------------------------

class HttpPollSource final : public Source<std::string> {
public:
    static constexpr int kMaxRetries = 20;

    // Checks the options; touches no endpoint.
    explicit HttpPollSource(HttpPollOptions opts);
    ~HttpPollSource() override;
    HttpPollSource(const HttpPollSource&) = delete;
    HttpPollSource& operator=(const HttpPollSource&) = delete;
    HttpPollSource(HttpPollSource&&) = delete;
    HttpPollSource& operator=(HttpPollSource&&) = delete;

    // Starts afresh from the cursor it holds (restored or initial): no
    // conditional request, retry or completion carried over.
    void open() override;
    void close() override;
    bool produce(Emitter<std::string>& out) override;
    // Ends a wait or a request in flight at once; nothing it held is emitted.
    void cancel() override;
    [[nodiscard]] bool is_bounded() const noexcept override;
    std::string name() const override;

    // The cursor, under the key the source has always used; empty in resource
    // mode and before the first record.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override;
    // Refuses a cursor the configured type cannot read, or any cursor in
    // resource mode. Drops the conditional state with the position it described.
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;

    // The cursor the next request carries; empty when it carries none.
    [[nodiscard]] std::string cursor() const;
    [[nodiscard]] bool cursor_mode() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] std::shared_ptr<Source<std::string>> make_http_poll_source(HttpPollOptions o);

}  // namespace clink::http_connector
