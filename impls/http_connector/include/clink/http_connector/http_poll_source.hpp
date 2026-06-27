#pragma once

// HTTP polling source: GET a JSON REST endpoint on an interval, emit the
// returned array elements, and cursor on a record field. Built on the generic
// PollingSource<std::string> (which owns the produce loop + cursor checkpoint)
// and the keep-alive HttpRequest. At-least-once.
//
// Per poll: GET <path>[?<cursor_param>=<cursor>], parse the response, take the
// records array (the response itself if it is an array, else response[
// records_field]), emit each element as a single-line JSON object string, and
// set the next cursor from the LAST record's cursor_field (so the API must
// return records ASCENDING by that field; use an exclusive cursor server-side to
// avoid re-emitting the boundary record).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/connectors/polling_source.hpp"
#include "clink/http_connector/http_bulk_post.hpp"  // parse_headers
#include "clink/http_connector/http_request.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::http_connector {

// Percent-encode a query-parameter value (RFC 3986 unreserved kept verbatim).
inline std::string url_encode(const std::string& v) {
    static const char* hex = "0123456789ABCDEF";
    std::string out;
    out.reserve(v.size());
    for (unsigned char c : v) {
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

struct HttpPollOptions {
    std::string url;        // scheme://host[:port]   (required)
    std::string path{"/"};  // request path (may carry a fixed query string)
    std::string headers;    // "K: V; ..." (e.g. auth)
    bool verify_tls{true};
    std::string cursor_param;    // query-param name to send the cursor as (optional)
    std::string cursor_field;    // record field holding the next cursor (optional)
    std::string records_field;   // response field holding the array (else response is the array)
    std::string initial_cursor;  // starting cursor
    std::chrono::milliseconds interval{1000};
    double jitter_frac{0.0};                            // +/- fraction on the poll interval
    int max_retries{4};                                 // transient-GET retries within one poll
    std::chrono::milliseconds retry_base_backoff{200};  // backoff between transient retries
    std::string name{"http_poll_source"};
};

namespace detail {

// Mutable per-source poll state held behind a shared_ptr so the (copyable)
// std::function poll callback carries it. produce() is single-threaded on the
// source runner, so no synchronisation is needed.
struct HttpPollState {
    HttpRequest::Options http;
    std::string path;
    std::string cursor_param;
    std::string cursor_field;
    std::string records_field;
    int max_retries{4};
    std::chrono::milliseconds retry_base_backoff{200};
    std::string name;
    std::unique_ptr<HttpRequest> client;  // created lazily on the first poll
    std::string last_etag;                // last response ETag, for If-None-Match
};

inline std::string scalar_cursor_text(const clink::config::JsonValue& v) {
    if (v.is_string()) {
        return v.as_string();
    }
    if (v.is_number()) {
        return v.serialize(0);  // compact number text
    }
    if (v.is_bool()) {
        return v.as_bool() ? "true" : "false";
    }
    return {};
}

}  // namespace detail

inline std::shared_ptr<Source<std::string>> make_http_poll_source(HttpPollOptions o) {
    if (o.url.empty()) {
        throw std::runtime_error(o.name + ": 'url' is required");
    }
    if (!HttpRequest::tls_supported() && o.url.rfind("https://", 0) == 0) {
        throw std::runtime_error(o.name +
                                 ": https URL but this build has no TLS support (rebuild the "
                                 "http_connector module with OpenSSL)");
    }
    if (o.max_retries < 0) {
        o.max_retries = 0;
    } else if (o.max_retries > 20) {
        o.max_retries = 20;
    }

    auto state = std::make_shared<detail::HttpPollState>();
    state->http.base_url = o.url;
    state->http.headers = parse_headers(o.headers);
    state->http.verify_tls = o.verify_tls;
    state->path = o.path;
    state->cursor_param = o.cursor_param;
    state->cursor_field = o.cursor_field;
    state->records_field = o.records_field;
    state->max_retries = o.max_retries;
    state->retry_base_backoff = o.retry_base_backoff;
    state->name = o.name;

    PollingSource<std::string>::Options popts;
    popts.interval = o.interval;
    popts.jitter_frac = o.jitter_frac;
    popts.initial_cursor = o.initial_cursor;
    popts.name = o.name;

    auto poll = [state](const std::string& cursor) -> PollingSource<std::string>::PollResult {
        if (!state->client) {
            state->client = std::make_unique<HttpRequest>(state->http);
        }
        std::string req_path = state->path;
        if (!state->cursor_param.empty() && !cursor.empty()) {
            req_path.push_back(req_path.find('?') == std::string::npos ? '?' : '&');
            req_path += state->cursor_param + "=" + url_encode(cursor);
        }

        // Conditional GET: send If-None-Match with the last ETag so an unchanged
        // resource returns 304 (no body re-download). Bounded EXPONENTIAL-BACKOFF
        // retry on transport / 5xx / 429 (transient); a 4xx is a permanent
        // request error - throw immediately rather than spin.
        std::map<std::string, std::string> req_headers;
        if (!state->last_etag.empty()) {
            req_headers["If-None-Match"] = state->last_etag;
        }
        HttpResponse res;
        for (int attempt = 0; attempt <= state->max_retries; ++attempt) {
            if (attempt > 0) {
                std::this_thread::sleep_for(backoff_delay(attempt, state->retry_base_backoff));
            }
            res = state->client->get(req_path, req_headers);
            // 304 Not Modified is a successful conditional-GET outcome, not a
            // failure - stop retrying.
            if ((res.status >= 200 && res.status < 300) || res.status == 304) {
                break;
            }
            const bool transient = classify_http_status(res.status) == HttpFailureClass::Transient;
            if (!transient || attempt == state->max_retries) {
                clink::metrics::connector::error_inc("http", "source");
                std::string detail =
                    res.status == 0 ? res.error : "HTTP " + std::to_string(res.status);
                throw std::runtime_error(state->name + ": GET " + state->http.base_url + req_path +
                                         " failed (" + detail + ")");
            }
        }
        if (res.status == 304) {
            return {};  // not modified: no new records, cursor unchanged
        }
        // Capture the ETag for the next conditional GET.
        if (auto et = res.headers.find("etag"); et != res.headers.end()) {
            state->last_etag = et->second;
        }

        clink::config::JsonValue body;
        try {
            body = clink::config::parse(res.body);
        } catch (...) {
            throw std::runtime_error(state->name + ": response is not valid JSON");
        }

        // Locate the records array.
        const clink::config::JsonValue* arr = nullptr;
        if (!state->records_field.empty()) {
            if (body.is_object()) {
                const auto& obj = body.as_object();
                if (auto it = obj.find(state->records_field);
                    it != obj.end() && it->second.is_array()) {
                    arr = &it->second;
                }
            }
        } else if (body.is_array()) {
            arr = &body;
        }

        PollingSource<std::string>::PollResult out;
        if (arr == nullptr) {
            return out;  // no array (e.g. empty/{} response): nothing this poll
        }
        std::uint64_t bytes = 0;
        for (const auto& elem : arr->as_array()) {
            out.records.push_back(elem.serialize(0));  // single-line JSON
            bytes += out.records.back().size();
            if (!state->cursor_field.empty() && elem.is_object()) {
                const auto& obj = elem.as_object();
                if (auto it = obj.find(state->cursor_field); it != obj.end()) {
                    std::string c = detail::scalar_cursor_text(it->second);
                    if (!c.empty()) {
                        out.next_cursor = std::move(c);  // last record wins (ascending order)
                    }
                }
            }
        }
        if (!out.records.empty()) {
            clink::metrics::connector::records_in_inc("http", out.records.size());
            clink::metrics::connector::bytes_in_inc("http", bytes);
        }
        return out;
    };

    return std::make_shared<PollingSource<std::string>>(std::move(popts), std::move(poll));
}

}  // namespace clink::http_connector
