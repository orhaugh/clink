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

namespace {
std::string lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return s;
}

// Where a failed exchange stood, from httplib's error. Connect and both TLS
// steps happen before a byte of the request is written; a write error leaves
// the request short of its Content-Length, so the server cannot act on it.
// Every later failure comes after the whole request was written.
BoundedResponse::Failure failure_of(httplib::Error e) {
    using F = BoundedResponse::Failure;
    switch (e) {
        case httplib::Error::Connection:
        case httplib::Error::ConnectionTimeout:
        case httplib::Error::BindIPAddress:
        case httplib::Error::ProxyConnection:
            return F::Connect;
        case httplib::Error::SSLConnection:
            return F::TlsHandshake;
        case httplib::Error::SSLLoadingCerts:
        case httplib::Error::SSLServerVerification:
        case httplib::Error::SSLServerHostnameVerification:
            return F::TlsVerify;
        case httplib::Error::Write:
            return F::Write;
        case httplib::Error::Read:
        case httplib::Error::Canceled:
        case httplib::Error::SSLPeerCouldBeClosed_:
            return F::Read;
        default:
            return F::Other;
    }
}

bool written_before(BoundedResponse::Failure f) {
    using F = BoundedResponse::Failure;
    return f == F::Read || f == F::TooLarge || f == F::Other;
}
}  // namespace

struct HttpRequest::Impl {
    httplib::Client client;
    std::function<std::string()> auth_token_provider;
    std::atomic<bool> interrupted{false};
    explicit Impl(const Options& o)
        : client(o.base_url), auth_token_provider(o.auth_token_provider) {
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
        client.enable_server_hostname_verification(o.verify_tls && o.verify_hostname);
        if (!o.ca_cert_path.empty()) {
            client.set_ca_cert_path(o.ca_cert_path);
        }
#endif
    }

    // Per-request headers: a fresh bearer token when a provider is set, else none. Consulted on
    // every request so a rotating token (e.g. a re-read file) is picked up without a new client.
    httplib::Headers auth_headers() const {
        if (!auth_token_provider) {
            return {};
        }
        return {{"Authorization", "Bearer " + auth_token_provider()}};
    }
    bool has_auth_provider() const { return static_cast<bool>(auth_token_provider); }
};

HttpRequest::HttpRequest(Options opts) : impl_(std::make_unique<Impl>(opts)) {}
HttpRequest::~HttpRequest() = default;
HttpRequest::HttpRequest(HttpRequest&&) noexcept = default;
HttpRequest& HttpRequest::operator=(HttpRequest&&) noexcept = default;

HttpResponse HttpRequest::post(const std::string& path,
                               const std::string& body,
                               const std::string& content_type) {
    if (impl_->has_auth_provider()) {
        return to_response(impl_->client.Post(path, impl_->auth_headers(), body, content_type));
    }
    return to_response(impl_->client.Post(path, body, content_type));
}

HttpResponse HttpRequest::get(const std::string& path) {
    if (impl_->has_auth_provider()) {
        return to_response(impl_->client.Get(path, impl_->auth_headers()));
    }
    return to_response(impl_->client.Get(path));
}

HttpResponse HttpRequest::get(const std::string& path,
                              const std::map<std::string, std::string>& extra_headers) {
    httplib::Headers h;
    for (const auto& [k, v] : extra_headers) {
        h.emplace(k, v);
    }
    if (impl_->has_auth_provider()) {
        h.emplace("Authorization", "Bearer " + impl_->auth_token_provider());
    }
    return to_response(impl_->client.Get(path, h));
}

HttpResponse HttpRequest::put(const std::string& path,
                              const std::string& body,
                              const std::string& content_type) {
    if (impl_->has_auth_provider()) {
        return to_response(impl_->client.Put(path, impl_->auth_headers(), body, content_type));
    }
    return to_response(impl_->client.Put(path, body, content_type));
}

HttpResponse HttpRequest::del(const std::string& path) {
    if (impl_->has_auth_provider()) {
        return to_response(impl_->client.Delete(path, impl_->auth_headers()));
    }
    return to_response(impl_->client.Delete(path));
}

BoundedResponse HttpRequest::post_bounded(const std::string& path,
                                          const std::string& body,
                                          const std::string& content_type,
                                          const BoundedRequest& request) {
    BoundedResponse out;
    impl_->interrupted.store(false, std::memory_order_release);
    httplib::Request req;
    req.method = "POST";
    req.path = path;
    for (const auto& [k, v] : request.headers) {
        req.headers.emplace(k, v);
    }
    if (impl_->has_auth_provider()) {
        req.headers.emplace("Authorization", "Bearer " + impl_->auth_token_provider());
    }
    req.headers.emplace("Content-Type", content_type);
    req.body = body;
    // The status line and headers, before any of the body.
    req.response_handler = [&out](const httplib::Response& res) {
        out.status = res.status;
        for (const auto& [k, v] : res.headers) {
            out.headers.emplace(lower(k), v);
        }
        return true;
    };
    bool too_large = false;
    req.content_receiver = [&out, &too_large, &request](
                               const char* data, std::size_t n, std::uint64_t, std::uint64_t) {
        if (out.body.size() + n > request.max_response_bytes) {
            too_large = true;
            return false;
        }
        out.body.append(data, n);
        return true;
    };
    httplib::Response res;
    httplib::Error error = httplib::Error::Success;
    const bool ok = impl_->client.send(req, res, error);
    if (ok) {
        out.complete = true;
        out.request_written = true;
        return out;
    }
    out.failure = too_large ? BoundedResponse::Failure::TooLarge : failure_of(error);
    out.request_written = written_before(out.failure);
    out.interrupted = impl_->interrupted.load(std::memory_order_acquire);
    out.error = too_large
                    ? "the reply passed " + std::to_string(request.max_response_bytes) + " bytes"
                    : httplib::to_string(error);
    return out;
}

void HttpRequest::interrupt() noexcept {
    impl_->interrupted.store(true, std::memory_order_release);
    impl_->client.stop();
}

bool HttpRequest::tls_supported() noexcept {
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
    return true;
#else
    return false;
#endif
}

}  // namespace clink::http_connector
