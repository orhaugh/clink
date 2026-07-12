// Minimal HTTP/1.1 client used by the JM to proxy /api/v1/tms/:id/*
// requests through to the target TM's HTTP server. Same pimpl shape
// as HttpServer so cpp-httplib stays out of public headers.
//
// Designed for short-lived, blocking, single-request usage from a
// handler thread. NOT a connection pool - each call to get() makes
// a fresh TCP connection. For the dashboard's polling rate (1-5 Hz)
// this is fine; if we ever need keep-alive or pipelining, swap the
// pimpl without touching callers.

#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace clink::http {

struct HttpClientResponse {
    int status{0};      // 0 on connection / transport error
    std::string body;   // raw response body
    std::string error;  // human-readable, populated when status == 0
};

class HttpClient {
public:
    HttpClient(std::string host, std::uint16_t port);
    ~HttpClient();

    // Send `Authorization: Bearer <token>` on every subsequent request, so this
    // client can talk to a server started with CLINK_AUTH_TOKEN. Empty clears it.
    void set_bearer_token(const std::string& token);

    HttpClient(const HttpClient&) = delete;
    HttpClient& operator=(const HttpClient&) = delete;
    HttpClient(HttpClient&&) = delete;
    HttpClient& operator=(HttpClient&&) = delete;

    // Blocking GET. Returns status=0 + error message on transport
    // failures (connect/read/timeout). The caller can use that to
    // emit 502 Bad Gateway on the proxy side.
    HttpClientResponse get(const std::string& path);

    // Blocking POST with a fixed-size body. content_type defaults to
    // "application/json" since that's the only consumer at the moment
    // (clink_submit_sql -> JM JobGraphSpec submission).
    HttpClientResponse post(const std::string& path,
                            const std::string& body,
                            const std::string& content_type = "application/json");

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::http
