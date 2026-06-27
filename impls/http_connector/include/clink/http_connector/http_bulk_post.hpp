#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <map>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "clink/http_connector/http_request.hpp"

// The retry/backoff core shared by every HTTP-family sink (the batched bulk
// sink, the Elasticsearch/Splunk renderers built on it, and the Prometheus
// pushgateway sink which buffers differently but POSTs the same way). Keeping
// the delivery semantics in ONE place means at-least-once + bounded backoff is
// defined once, not re-derived per sink.
namespace clink::http_connector {

// Decides whether an HTTP response counts as a successful delivery. Default
// (empty) = any 2xx. Some APIs return 2xx with a per-item error report in the
// body (Elasticsearch _bulk returns HTTP 200 with "errors":true on partial
// failure), so those sinks supply a validator that also inspects the body.
using ResponseValidator = std::function<bool(const HttpResponse&)>;

// Whether an HTTP failure is worth retrying (a transient outage / backpressure)
// or is a permanent request error (a poison record that will fail identically
// on replay). Drives retry-vs-DLQ.
enum class HttpFailureClass { Transient, Permanent };

// status 0 (transport error: connect/timeout/tls), 429 (rate limited) and 5xx
// are transient; any other non-2xx (4xx except 429) is permanent. A 2xx is not a
// failure and should not be classified.
inline HttpFailureClass classify_http_status(int status) {
    if (status == 0 || status == 429 || status >= 500) {
        return HttpFailureClass::Transient;
    }
    return HttpFailureClass::Permanent;
}

// Parse a "K1: V1; K2: V2" header string into a map. Empty -> no headers.
// Splits each pair on the FIRST ':', so a value may itself contain ':' (e.g.
// "Authorization: Bearer a:b"). Whitespace around key/value is trimmed.
inline std::map<std::string, std::string> parse_headers(const std::string& spec) {
    std::map<std::string, std::string> out;
    auto trim = [](std::string s) {
        const auto b = s.find_first_not_of(" \t");
        const auto e = s.find_last_not_of(" \t");
        return b == std::string::npos ? std::string{} : s.substr(b, e - b + 1);
    };
    std::size_t pos = 0;
    while (pos < spec.size()) {
        auto semi = spec.find(';', pos);
        if (semi == std::string::npos) {
            semi = spec.size();
        }
        const std::string pair = spec.substr(pos, semi - pos);
        const auto colon = pair.find(':');
        if (colon != std::string::npos) {
            auto k = trim(pair.substr(0, colon));
            auto v = trim(pair.substr(colon + 1));
            if (!k.empty()) {
                out.emplace(std::move(k), std::move(v));
            }
        }
        pos = semi + 1;
    }
    return out;
}

// Bounded exponential-backoff retry policy.
struct RetryPolicy {
    int max_retries{4};  // attempts beyond the first
    std::chrono::milliseconds base_backoff{200};

    // Upper bound on retry attempts. Caps both the attempt count and the
    // backoff exponent (1 << (attempt-1)) so a misconfigured max_retries can
    // neither overflow the shift nor schedule an absurd sleep on the runner.
    static constexpr int kMaxRetries = 20;

    // Clamp into [0, kMaxRetries]: negatives would skip the POST loop entirely
    // (throw without ever sending); large values would overflow the backoff
    // shift / schedule a multi-year sleep.
    int clamped_max_retries() const {
        if (max_retries < 0) {
            return 0;
        }
        if (max_retries > kMaxRetries) {
            return kMaxRetries;
        }
        return max_retries;
    }
};

// Bounded exponential backoff delay before retry attempt N (1-based): base *
// 2^(N-1), capped at 30s. The caller must keep `attempt` bounded (RetryPolicy
// clamps max_retries to kMaxRetries=20) so the unsigned shift cannot overflow.
inline std::chrono::milliseconds backoff_delay(int attempt, std::chrono::milliseconds base) {
    auto d = base * (1u << (attempt - 1));
    constexpr std::chrono::milliseconds kMaxBackoff{30000};
    return d > kMaxBackoff ? kMaxBackoff : d;
}

// POST `body` to `client` at `path`, retrying on failure with bounded
// exponential backoff. Returns on the first delivery the validator accepts;
// THROWS std::runtime_error when retries are exhausted, so the runner fails the
// subtask and the job replays from the last checkpoint (at-least-once, never a
// silent drop). `sink_name` + `base_url` only flavour the error message.
inline void post_with_retry(HttpRequest& client,
                            const std::string& path,
                            const std::string& body,
                            const std::string& content_type,
                            const RetryPolicy& policy,
                            const ResponseValidator& response_ok,
                            const std::string& sink_name,
                            const std::string& base_url) {
    const int max_retries = policy.clamped_max_retries();
    HttpResponse res;
    for (int attempt = 0; attempt <= max_retries; ++attempt) {
        if (attempt > 0) {
            std::this_thread::sleep_for(backoff_delay(attempt, policy.base_backoff));
        }
        res = client.post(path, body, content_type);
        const bool ok = response_ok ? response_ok(res) : (res.status >= 200 && res.status < 300);
        if (ok) {
            return;  // delivered
        }
    }
    // Retries exhausted: fail loudly so the job restarts and replays from the
    // last checkpoint (at-least-once), rather than dropping the batch. A 2xx
    // that the validator rejected (e.g. ES _bulk "errors":true) keeps a body
    // snippet so the failure is diagnosable.
    std::string detail = res.status == 0 ? res.error : "HTTP " + std::to_string(res.status);
    if (res.status >= 200 && res.status < 300 && !res.body.empty()) {
        detail += " body: " + res.body.substr(0, 256);
    }
    throw std::runtime_error(sink_name + ": POST " + base_url + path + " failed after " +
                             std::to_string(max_retries + 1) + " attempts (" + detail + ")");
}

}  // namespace clink::http_connector
