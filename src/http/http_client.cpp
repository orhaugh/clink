// HttpClient impl. cpp-httplib confined to this translation unit.

#include "clink/http/http_client.hpp"

#include <chrono>
#include <httplib.h>
#include <string>
#include <utility>

namespace clink::http {

struct HttpClient::Impl {
    httplib::Client client;

    Impl(const std::string& host, std::uint16_t port) : client(host, port) {
        // Keep the proxy snappy on slow / unreachable peers. The
        // dashboard polls; we'd rather surface "worker not responding" in
        // 2s than hang the request thread for the OS default
        // (~75s on most platforms).
        client.set_connection_timeout(std::chrono::seconds{2});
        client.set_read_timeout(std::chrono::seconds{5});
        client.set_write_timeout(std::chrono::seconds{5});
    }
};

HttpClient::HttpClient(std::string host, std::uint16_t port)
    : impl_(std::make_unique<Impl>(host, port)) {}

HttpClient::~HttpClient() = default;

void HttpClient::set_bearer_token(const std::string& token) {
    if (token.empty()) {
        impl_->client.set_default_headers({});
    } else {
        impl_->client.set_default_headers({{"Authorization", "Bearer " + token}});
    }
}

HttpClientResponse HttpClient::get(const std::string& path) {
    HttpClientResponse out;
    auto res = impl_->client.Get(path);
    if (!res) {
        out.status = 0;
        // httplib::to_string returns a short string like "Connection".
        // Good enough for the proxy's 502 body.
        out.error = httplib::to_string(res.error());
        return out;
    }
    out.status = res->status;
    out.body = std::move(res->body);
    return out;
}

HttpClientResponse HttpClient::post(const std::string& path,
                                    const std::string& body,
                                    const std::string& content_type) {
    HttpClientResponse out;
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

}  // namespace clink::http
