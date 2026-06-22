// clink::http::HttpServer - thin C++ wrapper around cpp-httplib.
//
// The header keeps cpp-httplib OUT of the public surface: the
// implementation lives in src/http/http_server.cpp where the
// httplib.h include happens. Callers get a small register-routes /
// start / stop API that doesn't drag in HTTP/1.1 machinery at the
// include site.
//
// Lifetime: owned by whoever spawned it (typically clink_node's
// run_jm / run_tm). Construct, register routes, call start(host, port).
// The server spawns its own thread pool; start() returns immediately.
// stop() is idempotent and joins the listener thread before returning.
//
// Threading: handler callbacks may run concurrently on the server's
// internal worker pool. Handlers must capture by value or use
// shared_ptr / weak_ptr lifetimes - references to mainloop stack
// frames are NOT safe.

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>

namespace clink::http {

// One file (or text field) from a multipart/form-data request body.
// For text-only fields (e.g. <input type=text name=foo>), filename is
// empty and content holds the raw text. For file uploads, filename is
// the client-supplied name and content is the raw bytes.
struct UploadedFile {
    std::string filename;
    std::string content_type;
    std::string content;
};

// Inputs to a route handler. Query / path params are parsed by the
// server before invoking the callback. body is the raw request body
// (POST/PUT); empty for GET. headers is a case-folded map of request
// headers we forward. files is populated from multipart/form-data
// bodies (cpp-httplib parses the body into named parts before invoking
// the handler).
struct HttpRequest {
    std::string method;                              // "GET", "POST", ...
    std::string path;                                // "/api/v1/jobs/42"
    std::map<std::string, std::string> path_params;  // {":id" -> "42"} for routes with patterns
    std::map<std::string, std::string> query;        // ?foo=bar
    std::map<std::string, std::string> headers;
    std::string body;
    std::map<std::string, UploadedFile> files;
};

// Outputs from a route handler. status defaults to 200 OK; set to
// 4xx/5xx for error replies. content_type defaults to
// "application/json" (clink's API is JSON-first); override for
// HTML, plain text, or Prometheus exposition.
struct HttpResponse {
    int status{200};
    std::string content_type{"application/json"};
    std::string body;
    // Optional extra headers; e.g. Cache-Control: no-store for live
    // metrics endpoints. Empty by default.
    std::map<std::string, std::string> headers;
};

using Handler = std::function<HttpResponse(const HttpRequest&)>;

// Server-Sent Events support.
//
// SseChunk is one event frame to send over a chunked text/event-stream
// connection. `event` is optional; if non-empty it becomes an "event: X"
// line in the SSE frame. `data` is the JSON payload (or "" for a comment
// heartbeat - see SsePuller below).
struct SseChunk {
    std::string event;
    std::string data;
};

// SsePuller is invoked repeatedly on the HTTP worker thread for one
// connection. Each call should:
//   * block up to ~heartbeat_seconds for the next event,
//   * return SseChunk{...} to send it,
//   * return SseChunk{"", ""} to send a SSE comment line as a keepalive,
//   * return std::nullopt to close the stream cleanly.
// Pullers MUST be thread-safe wrt destruction: the framework drops the
// puller when the client disconnects, which can race with a publisher
// callback the puller installed. The recommended pattern is to capture
// shared_ptr-managed state (event queue + condvar + closed flag) so
// late callbacks no-op safely.
using SsePuller = std::function<std::optional<SseChunk>()>;

// SseFactory is invoked once per accepted connection. It receives the
// initial request and returns the puller that produces events for the
// lifetime of that connection.
using SseFactory = std::function<SsePuller(const HttpRequest&)>;

class HttpServer {
public:
    HttpServer();
    ~HttpServer();

    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;
    HttpServer(HttpServer&&) = delete;
    HttpServer& operator=(HttpServer&&) = delete;

    // Register routes. `path` may contain named placeholders prefixed
    // with `:` (e.g. "/api/v1/jobs/:id") - matched values land in
    // HttpRequest::path_params keyed by the placeholder name including
    // the colon. Wildcard "*" suffix matches anything (e.g.
    // "/assets/*") and exposes the matched tail as path_params["*"].
    void get(const std::string& path, Handler handler);
    void post(const std::string& path, Handler handler);

    // Register a Server-Sent Events route. The factory is called once
    // per accepted connection to build the puller; the puller is then
    // invoked until it returns std::nullopt or the client disconnects.
    // Sets Content-Type: text/event-stream and disables proxy buffering.
    void sse(const std::string& path, SseFactory factory);

    // Enable CORS for browser clients on a different origin (e.g. a
    // standalone console dev server). Adds Access-Control-Allow-Origin to
    // every response and answers OPTIONS preflight with 204 + the allow
    // headers. `allow_origin` is echoed verbatim ("*" for any origin, or a
    // specific scheme://host:port). Call before start(). Off by default:
    // same-origin deployments (the SPA served by the node) need no CORS.
    void enable_cors(const std::string& allow_origin);

    // Start listening on `host`:`port`. Spawns a background thread to
    // run the accept loop. Returns the actually-bound port (matches
    // `port` if non-zero; OS-picked otherwise). Throws on bind failure.
    // Idempotent: a second call is a no-op if already started.
    std::uint16_t start(const std::string& host, std::uint16_t port);

    // Stop the listener and join the worker pool. Idempotent; safe to
    // call multiple times.
    void stop();

    bool is_running() const noexcept;
    std::uint16_t bound_port() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    std::atomic<bool> running_{false};
    std::uint16_t bound_port_{0};
};

}  // namespace clink::http
