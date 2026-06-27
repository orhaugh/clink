#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>

// A small blocking HTTP/1.1 request client for connector sinks. Unlike the
// minimal control-plane client in clink/http/http_client.hpp, this one
// supports custom headers (auth), https (when the module is built with
// OpenSSL), keep-alive connection reuse for throughput, and a configurable
// content-type. cpp-httplib is confined to the .cpp (pimpl), so this header
// stays dependency-free and the impl module is the only place that needs the
// httplib include + the optional OpenSSL link.
namespace clink::http_connector {

struct HttpResponse {
    int status{0};      // 0 on transport error (connect/read/timeout/tls)
    std::string body;   // raw response body
    std::string error;  // human-readable, populated when status == 0
};

// Reusable POST client bound to one base URL (scheme://host[:port]). Holds a
// keep-alive connection so a high-throughput sink reuses one socket. Not
// thread-safe: one client per sink subtask (the sink owns it on its runner
// thread).
class HttpRequest {
public:
    struct Options {
        std::string base_url;                        // e.g. "http://host:9200" or "https://host"
        std::map<std::string, std::string> headers;  // default headers applied to every request
        int connect_timeout_ms{5000};
        int rw_timeout_ms{30000};
        bool verify_tls{true};  // https only; false skips server-cert verification
    };

    explicit HttpRequest(Options opts);
    ~HttpRequest();

    HttpRequest(const HttpRequest&) = delete;
    HttpRequest& operator=(const HttpRequest&) = delete;
    HttpRequest(HttpRequest&&) noexcept;
    HttpRequest& operator=(HttpRequest&&) noexcept;

    // Blocking POST. Returns status=0 + error on transport failure (including a
    // request to an https URL when this module was built without OpenSSL).
    HttpResponse post(const std::string& path,
                      const std::string& body,
                      const std::string& content_type);

    // Blocking GET (for polling sources). `path` may carry a query string. Same
    // transport-error convention as post().
    HttpResponse get(const std::string& path);

    // Blocking PUT (e.g. creating a resource / an index mapping). Same
    // transport-error convention as post().
    HttpResponse put(const std::string& path,
                     const std::string& body,
                     const std::string& content_type);

    // True iff this build can talk https (CPPHTTPLIB_OPENSSL_SUPPORT). Lets a
    // sink reject an https URL up front with a clear message.
    static bool tls_supported() noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::http_connector
