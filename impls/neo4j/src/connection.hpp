#pragma once

// What the sink and the lookup share to reach the Query API: the client built
// from the connection options, the watch that ends a request on a deadline or
// a cancel, the retry backoff, and how a server refusal maps to an error code.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>

#include "clink/http_connector/http_request.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "options.hpp"
#include "reply.hpp"

namespace clink::neo4j {

// The typed JSON media type the Query API reads and writes.
inline constexpr char kTypedJson[] = "application/vnd.neo4j.query";

// A client for `o`'s endpoint: its timeouts, CA file and credentials, with the
// certificate and the host name always verified.
[[nodiscard]] std::unique_ptr<clink::http_connector::HttpRequest> make_client(
    const ConnectionOptions& o);

// Ends the request in flight when the task is cancelled or the attempt's
// deadline passes. One thread per client, asleep unless a request is in
// flight; it touches nothing but HttpRequest::interrupt(), so it can hide no
// error and holds no data. Joined by stop() and the destructor.
class RequestWatch {
public:
    enum class Why : std::uint8_t { None, Cancelled, Deadline };

    explicit RequestWatch(clink::http_connector::HttpRequest& http);
    ~RequestWatch();
    RequestWatch(const RequestWatch&) = delete;
    RequestWatch& operator=(const RequestWatch&) = delete;

    void arm(std::chrono::steady_clock::time_point deadline, CancelSignal cancel);
    // Why the watch interrupted the request armed last, if it did.
    Why disarm();
    void stop();

private:
    void run();

    clink::http_connector::HttpRequest& http_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool armed_{false};
    bool stop_{false};
    std::chrono::steady_clock::time_point deadline_{};
    CancelSignal cancel_;
    Why why_{Why::None};
    std::thread thread_;
};

// Full jitter: the nth wait is uniform in [0, min(5 s, 100 ms * 2^n)].
class Backoff {
public:
    std::chrono::milliseconds next();

private:
    std::mt19937_64 rng_ = std::mt19937_64(std::random_device{}());
    unsigned n_{0};
};

// Waits `d` in slices of at most 50 ms, returning false as soon as the task is
// cancelled or `stop` (when given) is set.
[[nodiscard]] bool cancellable_wait(std::chrono::milliseconds d,
                                    const CancelSignal& cancel,
                                    const std::atomic<bool>* stop = nullptr);

// The error code for a refusal of `c`.
[[nodiscard]] const char* error_code_for(ErrorClass c);

// `metric{op_id="<op>"}`, and the same with one more label.
[[nodiscard]] std::string tagged(const char* metric, std::uint64_t op);
[[nodiscard]] std::string tagged(const char* metric,
                                 std::uint64_t op,
                                 std::string_view key,
                                 std::string_view value);

}  // namespace clink::neo4j
