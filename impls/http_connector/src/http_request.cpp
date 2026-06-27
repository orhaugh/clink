// HttpRequest impl. cpp-httplib is confined to this translation unit. When the
// module is built with OpenSSL (CPPHTTPLIB_OPENSSL_SUPPORT defined by the
// CMakeLists), httplib::Client transparently handles https base URLs.

#include "clink/http_connector/http_request.hpp"

#include <chrono>
#include <httplib.h>
#include <utility>

namespace clink::http_connector {

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
    HttpResponse out;
    auto res = impl_->client.Post(path, body, content_type);
    if (!res) {
        out.status = 0;
        out.error = httplib::to_string(res.error());
        return out;
    }
    out.status = res->status;
    out.body = std::move(res->body);
    return out;
}

HttpResponse HttpRequest::get(const std::string& path) {
    HttpResponse out;
    auto res = impl_->client.Get(path);
    if (!res) {
        out.status = 0;
        out.error = httplib::to_string(res.error());
        return out;
    }
    out.status = res->status;
    out.body = std::move(res->body);
    return out;
}

bool HttpRequest::tls_supported() noexcept {
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
    return true;
#else
    return false;
#endif
}

}  // namespace clink::http_connector
