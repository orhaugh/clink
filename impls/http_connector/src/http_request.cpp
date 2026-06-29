// HttpRequest impl. cpp-httplib is confined to this translation unit. When the
// module is built with OpenSSL (CPPHTTPLIB_OPENSSL_SUPPORT defined by the
// CMakeLists), httplib::Client transparently handles https base URLs.

#include "clink/http_connector/http_request.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <httplib.h>
#include <utility>

namespace clink::http_connector {

namespace {
// Fill an HttpResponse from an httplib result: status/body on success, status 0
// + error on transport failure, and the response headers with LOWER-CASED names
// (HTTP header names are case-insensitive; a caller looks up e.g. "etag").
template <typename Result>
HttpResponse to_response(Result&& res) {
    HttpResponse out;
    if (!res) {
        out.status = 0;
        out.error = httplib::to_string(res.error());
        return out;
    }
    out.status = res->status;
    out.body = std::move(res->body);
    for (const auto& [k, v] : res->headers) {
        std::string lower = k;
        std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });
        out.headers.emplace(std::move(lower), v);
    }
    return out;
}
}  // namespace

struct HttpRequest::Impl {
    httplib::Client client;
    explicit Impl(const Options& o) : client(o.base_url) {
        client.set_keep_alive(true);
        client.set_connection_timeout(std::chrono::milliseconds{o.connect_timeout_ms});
        client.set_read_timeout(std::chrono::milliseconds{o.rw_timeout_ms});
        client.set_write_timeout(std::chrono::milliseconds{o.rw_timeout_ms});
        httplib::Headers h;
        for (const auto& [k, v] : o.headers) {
            h.emplace(k, v);
        }
        client.set_default_headers(std::move(h));
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
        client.enable_server_certificate_verification(o.verify_tls);
#endif
    }
};

HttpRequest::HttpRequest(Options opts) : impl_(std::make_unique<Impl>(opts)) {}
HttpRequest::~HttpRequest() = default;
HttpRequest::HttpRequest(HttpRequest&&) noexcept = default;
HttpRequest& HttpRequest::operator=(HttpRequest&&) noexcept = default;

HttpResponse HttpRequest::post(const std::string& path,
                               const std::string& body,
                               const std::string& content_type) {
    return to_response(impl_->client.Post(path, body, content_type));
}

HttpResponse HttpRequest::get(const std::string& path) {
    return to_response(impl_->client.Get(path));
}

HttpResponse HttpRequest::get(const std::string& path,
                              const std::map<std::string, std::string>& extra_headers) {
    httplib::Headers h;
    for (const auto& [k, v] : extra_headers) {
        h.emplace(k, v);
    }
    return to_response(impl_->client.Get(path, h));
}

HttpResponse HttpRequest::put(const std::string& path,
                              const std::string& body,
                              const std::string& content_type) {
    return to_response(impl_->client.Put(path, body, content_type));
}

HttpResponse HttpRequest::del(const std::string& path) {
    return to_response(impl_->client.Delete(path));
}

bool HttpRequest::tls_supported() noexcept {
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
    return true;
#else
    return false;
#endif
}

}  // namespace clink::http_connector
