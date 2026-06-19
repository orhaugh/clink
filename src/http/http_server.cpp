// HttpServer implementation. cpp-httplib stays confined to this
// translation unit - the public header doesn't include httplib.h so
// downstream callers don't pay its compile cost.

#include "clink/http/http_server.hpp"

#include <chrono>
#include <httplib.h>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace clink::http {

namespace {

// httplib's request param/header iteration uses a multimap-ish API.
// Flatten into the simpler std::map<string,string> our handlers use.
// Multiple values for the same key are joined with ", " (matches the
// HTTP spec for repeatable header fields and is what users expect for
// repeated ?k=v query params).
std::map<std::string, std::string> flatten_pairs(const httplib::Params& params) {
    std::map<std::string, std::string> out;
    for (const auto& [k, v] : params) {
        auto it = out.find(k);
        if (it == out.end()) {
            out.emplace(k, v);
        } else {
            it->second += ", ";
            it->second += v;
        }
    }
    return out;
}

std::map<std::string, std::string> flatten_headers(const httplib::Headers& headers) {
    std::map<std::string, std::string> out;
    for (const auto& [k, v] : headers) {
        // Header field names are case-insensitive per RFC 7230 §3.2;
        // we lower-case so route handlers can look up consistently
        // without per-callsite folding.
        std::string lower;
        lower.reserve(k.size());
        for (char c : k) {
            lower.push_back(static_cast<char>(::tolower(static_cast<unsigned char>(c))));
        }
        auto it = out.find(lower);
        if (it == out.end()) {
            out.emplace(std::move(lower), v);
        } else {
            it->second += ", ";
            it->second += v;
        }
    }
    return out;
}

HttpRequest build_request(const httplib::Request& req) {
    HttpRequest r;
    r.method = req.method;
    r.path = req.path;
    r.body = req.body;
    r.query = flatten_pairs(req.params);
    r.headers = flatten_headers(req.headers);
    for (const auto& [k, v] : req.path_params) {
        // cpp-httplib strips the ":" from path-pattern names; we keep
        // the convention that path_params is keyed by the bare name
        // (no colon) so callers do path_params.at("id"), not
        // path_params.at(":id").
        r.path_params.emplace(k, v);
    }
    // Multipart form data: cpp-httplib parses each part into name +
    // filename + content + content_type. We flatten into a simple map;
    // duplicate part names get the last one (uncommon for our routes
    // and gives `<input name=foo>` semantics).
    for (const auto& [name, mp] : req.files) {
        UploadedFile f;
        f.filename = mp.filename;
        f.content_type = mp.content_type;
        f.content = mp.content;
        r.files[name] = std::move(f);
    }
    return r;
}

void apply_response(const HttpResponse& src, httplib::Response& dst) {
    dst.status = src.status;
    dst.set_content(src.body, src.content_type);
    for (const auto& [k, v] : src.headers) {
        dst.set_header(k, v);
    }
}

}  // namespace

struct HttpServer::Impl {
    httplib::Server server;
    std::thread listen_thread;

    // Wrap a clink Handler so it adapts to httplib's signature
    // (void(const Request&, Response&)) with structured exception
    // propagation -> 500 with the exception message in the body.
    static httplib::Server::Handler adapt(Handler h) {
        return [h = std::move(h)](const httplib::Request& req, httplib::Response& resp) {
            try {
                const auto r = h(build_request(req));
                apply_response(r, resp);
            } catch (const std::exception& e) {
                resp.status = 500;
                resp.set_content(std::string{"{\"error\":\"handler threw: "} + e.what() + "\"}",
                                 "application/json");
            } catch (...) {
                resp.status = 500;
                resp.set_content("{\"error\":\"handler threw unknown exception\"}",
                                 "application/json");
            }
        };
    }
};

HttpServer::HttpServer() : impl_(std::make_unique<Impl>()) {
    // Default error handler returns a small JSON 4xx/5xx body instead
    // of the cpp-httplib HTML default - matches our API-first stance.
    impl_->server.set_error_handler([](const httplib::Request& /*req*/, httplib::Response& resp) {
        if (resp.body.empty()) {
            resp.set_content(std::string{"{\"error\":\""} + std::to_string(resp.status) + "\"}",
                             "application/json");
        }
    });
}

HttpServer::~HttpServer() {
    stop();
}

void HttpServer::get(const std::string& path, Handler handler) {
    impl_->server.Get(path, Impl::adapt(std::move(handler)));
}

void HttpServer::post(const std::string& path, Handler handler) {
    impl_->server.Post(path, Impl::adapt(std::move(handler)));
}

void HttpServer::sse(const std::string& path, SseFactory factory) {
    // Wrap factory in a GET handler that switches to chunked output.
    // cpp-httplib drives the chunked provider on the same worker that
    // accepted the request, so puller invocations are serialised per
    // connection. The provider callback returns false to close the
    // stream (puller said end-of-stream or sink.write() failed because
    // the client went away).
    impl_->server.Get(
        path, [factory = std::move(factory)](const httplib::Request& req, httplib::Response& resp) {
            auto puller = factory(build_request(req));
            // Disable proxy buffering. Without these, NGINX et al. will
            // hold the response back until the connection closes,
            // defeating the point of SSE.
            resp.set_header("Cache-Control", "no-cache, no-store");
            resp.set_header("X-Accel-Buffering", "no");
            resp.set_chunked_content_provider(
                "text/event-stream",
                [puller = std::move(puller)](std::size_t /*offset*/, httplib::DataSink& sink) {
                    std::optional<SseChunk> chunk;
                    try {
                        chunk = puller();
                    } catch (...) {
                        sink.done();
                        return false;
                    }
                    if (!chunk.has_value()) {
                        sink.done();
                        return false;
                    }
                    std::string out;
                    if (chunk->event.empty() && chunk->data.empty()) {
                        // Heartbeat: SSE comment line. Most clients silently
                        // skip lines beginning with ":".
                        out = ": heartbeat\n\n";
                    } else {
                        if (!chunk->event.empty()) {
                            out += "event: ";
                            out += chunk->event;
                            out += '\n';
                        }
                        // data: can contain newlines; SSE wants each line
                        // prefixed. Our payloads are single-line JSON, but
                        // handle the general case anyway so a future
                        // multi-line payload doesn't silently break parsing.
                        std::size_t start = 0;
                        while (start <= chunk->data.size()) {
                            const auto nl = chunk->data.find('\n', start);
                            const auto end = nl == std::string::npos ? chunk->data.size() : nl;
                            out += "data: ";
                            out.append(chunk->data, start, end - start);
                            out += '\n';
                            if (nl == std::string::npos)
                                break;
                            start = nl + 1;
                        }
                        out += '\n';
                    }
                    if (!sink.write(out.data(), out.size())) {
                        return false;  // client disconnected
                    }
                    return true;
                });
        });
}

std::uint16_t HttpServer::start(const std::string& host, std::uint16_t port) {
    if (running_.load(std::memory_order_acquire)) {
        return bound_port_;
    }
    // bind_to_any_port returns the OS-picked port when port==0; bind
    // ours otherwise. Bound port lookup matches the request - we read
    // it after binding so the caller can advertise it.
    const auto actually_bound = port == 0 ? impl_->server.bind_to_any_port(host)
                                          : (impl_->server.bind_to_port(host, port) ? port : -1);
    if (actually_bound < 0) {
        throw std::runtime_error("HttpServer::start: bind failed on " + host + ":" +
                                 std::to_string(port));
    }
    bound_port_ = static_cast<std::uint16_t>(actually_bound);
    running_.store(true, std::memory_order_release);
    // listen_after_bind blocks; spawn it so start() returns.
    impl_->listen_thread = std::thread([this] { impl_->server.listen_after_bind(); });
    return bound_port_;
}

void HttpServer::stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    impl_->server.stop();
    if (impl_->listen_thread.joinable()) {
        impl_->listen_thread.join();
    }
}

bool HttpServer::is_running() const noexcept {
    return running_.load(std::memory_order_acquire);
}

std::uint16_t HttpServer::bound_port() const noexcept {
    return bound_port_;
}

}  // namespace clink::http
