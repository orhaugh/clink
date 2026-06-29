#pragma once

// HttpPool over io_uring socket awaitables.
//
// A coroutine-aware HTTP/1.1 client with connection-reuse pooling.
// Composes the io_uring socket awaitables (connect / write / read)
// into a coherent request/response API:
//
//   IoUringReactor reactor;
//   std::thread loop([&]{ reactor.run(); });
//   HttpPool pool(reactor, {.host = "127.0.0.1", .port = 8080});
//
//   auto t = pool.get("/api/v1/widgets/42");
//   t.resume();
//   ... wait for done ...
//   auto resp = t.get();
//   EXPECT_EQ(resp.status, 200);
//
// HTTP/1.1 features:
//   - GET and POST methods.
//   - Request: Host header is auto-set from config; Content-Length
//     auto-set on POST body. Caller can supply additional headers.
//   - Response: status line + arbitrary headers (case-folded to
//     lowercase keys) + Content-Length body. Connection: keep-alive
//     is the default and the fd returns to the pool; Connection:
//     close drops the fd.
//
// Out of scope (next slice):
//   - Transfer-Encoding: chunked response bodies.
//   - HTTPS / TLS.
//   - HTTP/2.
//   - DNS resolution (host must be a dotted-quad IPv4 string).
//   - Connection-acquire backpressure beyond max_connections fast-
//     fail; users compose RetryPolicy externally.
//
// Linux-only (io_uring-based). On non-Linux the header is empty so
// callers that conditionally use it don't break the build.

#include <chrono>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "clink/async/io_uring_reactor.hpp"
#include "clink/async/task.hpp"

namespace clink::async {

#if defined(__linux__) && defined(CLINK_HAS_URING)

struct HttpRequest {
    std::string method{"GET"};
    std::string path{"/"};
    std::string body;
    // Additional headers (case-insensitive on the wire; the pool
    // serialises them verbatim). The Host header is auto-set from
    // pool config and should not be supplied here.
    std::map<std::string, std::string> headers;
};

struct HttpResponse {
    // HTTP status code. 0 indicates a transport failure (connect /
    // read / write error); `error` carries a description in that
    // case. >=100 indicates a parsed server response.
    int status{0};
    // Headers as they appeared on the wire; keys lowercased so
    // `Content-Length` and `content-length` round-trip to the same
    // map entry.
    std::map<std::string, std::string> headers;
    std::string body;
    // Populated when status == 0 (transport error before headers
    // arrive). Empty when status >= 100.
    std::string error;
};

struct HttpPoolConfig {
    // Dotted-quad IPv4 address. DNS is out of scope; callers
    // resolve hostnames before constructing the pool.
    std::string host{"127.0.0.1"};
    std::uint16_t port{80};
    // Cap on simultaneously-leased connections. Requests that would
    // push past this cap fast-fail with status=0, error="pool exhausted".
    // Users compose backpressure / retry policy externally.
    std::size_t max_connections{16};
    // Initial read buffer size; the pool grows it as needed.
    std::size_t initial_read_chunk{4096};
};

class HttpPool {
public:
    HttpPool(IoUringReactor& reactor, HttpPoolConfig cfg)
        : reactor_(reactor), cfg_(std::move(cfg)) {}
    ~HttpPool();

    HttpPool(const HttpPool&) = delete;
    HttpPool& operator=(const HttpPool&) = delete;
    HttpPool(HttpPool&&) = delete;
    HttpPool& operator=(HttpPool&&) = delete;

    // Issue a request; co_return the response.
    Task<HttpResponse> request(HttpRequest req);

    // Convenience wrappers.
    Task<HttpResponse> get(std::string path);
    Task<HttpResponse> post(std::string path,
                            std::string body,
                            std::string content_type = "application/json");

    // Diagnostic accessors. leased_count() is the number of
    // connections currently checked out; available_count() is the
    // size of the keep-alive free list.
    [[nodiscard]] std::size_t leased_count() const;
    [[nodiscard]] std::size_t available_count() const;

private:
    // Acquire a connected fd: prefer a pooled one, else open a new
    // socket + connect. Returns >=0 fd or negative -errno.
    Task<int> acquire_fd_();

    // Return an fd to the pool for keep-alive reuse. If keep_alive
    // is false (Connection: close, parse error, etc.) the fd is
    // closed instead.
    void release_fd_(int fd, bool keep_alive);

    // Build the on-wire request bytes (request line + headers + body).
    std::string serialize_request_(const HttpRequest& req) const;

    // Read + parse a response from `fd`. The fd is left in a state
    // suitable for keep-alive reuse on success (status >= 100,
    // body fully read).
    Task<HttpResponse> read_response_(int fd);

    IoUringReactor& reactor_;
    HttpPoolConfig cfg_;
    mutable std::mutex mu_;
    std::deque<int> free_fds_;  // pooled idle keep-alive fds
    std::size_t leased_{0};     // outstanding leased fds
};

#endif  // __linux__ && CLINK_HAS_URING

}  // namespace clink::async
