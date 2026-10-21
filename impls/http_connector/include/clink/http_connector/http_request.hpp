#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>

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
    // Response headers with LOWER-CASED names (HTTP header names are
    // case-insensitive), e.g. headers["etag"] or headers["location"] (the latter
    // for a 307 redirect, as WebHDFS CREATE/OPEN use). Populated by get/post/put.
    std::map<std::string, std::string> headers;
};

// One request of post_bounded().
struct BoundedRequest {
    // Sent on this request only, beside the client's default headers.
    std::map<std::string, std::string> headers;
    // The most reply body bytes read. A longer reply ends the exchange with
    // BoundedResponse::too_large, without reading the rest.
    std::size_t max_response_bytes{1U << 20};
};

// What post_bounded() saw. Unlike HttpResponse, an exchange that fails keeps
// what arrived before the failure (the status line, the headers, the body so
// far), and says whether the whole request had been written: a request the
// server never received in full cannot have been acted on, while one whose
// reply was lost may have been.
struct BoundedResponse {
    enum class Failure : std::uint8_t {
        None,          // complete
        Connect,       // the connection was not made (refused, unreachable, timed out)
        TlsHandshake,  // the TLS handshake failed
        TlsVerify,  // the CA file did not load, or the server's certificate or name did not verify
        Write,      // writing the request failed
        Read,       // reading the reply failed or timed out
        TooLarge,   // the reply passed max_response_bytes
        Other,
    };
    int status{0};                               // 0 until a status line arrived
    std::map<std::string, std::string> headers;  // lower-cased names
    std::string body;                            // at most max_response_bytes
    bool complete{false};                        // the whole reply arrived within the bound
    Failure failure{Failure::None};
    // The whole request was written before the failure. False for Connect,
    // TlsHandshake, TlsVerify and Write; true for Read and TooLarge.
    bool request_written{false};
    bool interrupted{false};  // interrupt() ended the exchange
    std::string error;        // what ended an incomplete exchange
};

// Reusable POST client bound to one base URL (scheme://host[:port]). Holds a
// keep-alive connection so a high-throughput sink reuses one socket. Not
// thread-safe: one client per sink subtask (the sink owns it on its runner
// thread). interrupt() is the one exception.
class HttpRequest {
public:
    struct Options {
        std::string base_url;                        // e.g. "http://host:9200" or "https://host"
        std::map<std::string, std::string> headers;  // default headers applied to every request
        int connect_timeout_ms{5000};
        int rw_timeout_ms{30000};
        bool verify_tls{true};  // https only; false skips server-cert verification
        // https only: PEM file of the CAs that may sign the server's certificate, in
        // place of the system's trust store. Empty: the system's.
        std::string ca_cert_path;
        // https only: check that the certificate names the host connected to, as
        // well as that a trusted CA signed it. Ignored when verify_tls is false.
        bool verify_hostname{true};
        // Optional bearer-token provider, consulted before EACH request: when set, an
        // "Authorization: Bearer <provider()>" header is added per-request, so a token that
        // rotates (e.g. a re-read file) is picked up without rebuilding the client. Leave a
        // static `Authorization` out of `headers` when using this. Empty result = no auth header.
        std::function<std::string()> auth_token_provider;
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

    // Blocking GET (for polling sources). `path` may carry a query string;
    // `extra_headers` are sent on this request only (e.g. If-None-Match). The
    // returned HttpResponse carries the response headers. Same transport-error
    // convention as post().
    HttpResponse get(const std::string& path);
    HttpResponse get(const std::string& path,
                     const std::map<std::string, std::string>& extra_headers);

    // Blocking PUT (e.g. creating a resource / an index mapping). Same
    // transport-error convention as post().
    HttpResponse put(const std::string& path,
                     const std::string& body,
                     const std::string& content_type);

    // Blocking DELETE (e.g. WebHDFS op=DELETE to remove a staging object). Same
    // transport-error convention as post().
    HttpResponse del(const std::string& path);

    // Blocking POST whose reply is read up to request.max_response_bytes and
    // no further. Never throws for a transport failure; see BoundedResponse.
    BoundedResponse post_bounded(const std::string& path,
                                 const std::string& body,
                                 const std::string& content_type,
                                 const BoundedRequest& request);

    // Ends the exchange in flight on another thread, which returns at once
    // with BoundedResponse::interrupted (a connect or TLS handshake under way
    // finishes or times out first). Safe from any thread at any time; with
    // nothing in flight it closes the idle connection and the next request
    // opens a new one.
    void interrupt() noexcept;

    // True iff this build can talk https (CPPHTTPLIB_OPENSSL_SUPPORT). Lets a
    // sink reject an https URL up front with a clear message.
    static bool tls_supported() noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::http_connector
