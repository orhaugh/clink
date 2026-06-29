// HttpPool implementation.

#include "clink/async/http_pool.hpp"

#if defined(__linux__) && defined(CLINK_HAS_URING)

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <sstream>
#include <string_view>
#include <unistd.h>
#include <utility>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace clink::async {

namespace {

std::string lower(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    }
    return out;
}

void trim_inplace(std::string& s) {
    auto is_ws = [](char c) { return c == ' ' || c == '\t' || c == '\r' || c == '\n'; };
    while (!s.empty() && is_ws(s.front()))
        s.erase(s.begin());
    while (!s.empty() && is_ws(s.back()))
        s.pop_back();
}

}  // namespace

HttpPool::~HttpPool() {
    std::lock_guard lock(mu_);
    for (int fd : free_fds_) {
        ::close(fd);
    }
    free_fds_.clear();
    // Outstanding leased connections are the caller's problem; we
    // can't safely close fds they may still be reading from.
}

std::size_t HttpPool::leased_count() const {
    std::lock_guard lock(mu_);
    return leased_;
}

std::size_t HttpPool::available_count() const {
    std::lock_guard lock(mu_);
    return free_fds_.size();
}

std::string HttpPool::serialize_request_(const HttpRequest& req) const {
    std::string out;
    out.reserve(256 + req.body.size());
    out += req.method;
    out += ' ';
    out += req.path;
    out += " HTTP/1.1\r\n";
    out += "Host: " + cfg_.host + ":" + std::to_string(cfg_.port) + "\r\n";
    out += "Connection: keep-alive\r\n";
    if (!req.body.empty()) {
        out += "Content-Length: " + std::to_string(req.body.size()) + "\r\n";
    }
    bool has_content_type = false;
    for (const auto& [k, v] : req.headers) {
        // Skip headers we manage ourselves.
        const auto lk = lower(k);
        if (lk == "host" || lk == "connection" || lk == "content-length") {
            continue;
        }
        if (lk == "content-type") {
            has_content_type = true;
        }
        out += k;
        out += ": ";
        out += v;
        out += "\r\n";
    }
    if (!req.body.empty() && !has_content_type) {
        out += "Content-Type: application/octet-stream\r\n";
    }
    out += "\r\n";
    out += req.body;
    return out;
}

Task<int> HttpPool::acquire_fd_() {
    // Step 1: lock-only attempt to claim a pooled fd or pre-allocate
    // a leased slot for a new connection. Never holds the lock
    // across the connect_async suspension.
    int existing_fd = -1;
    bool need_new = false;
    {
        std::lock_guard lock(mu_);
        if (!free_fds_.empty()) {
            existing_fd = free_fds_.front();
            free_fds_.pop_front();
            ++leased_;
        } else if (leased_ < cfg_.max_connections) {
            ++leased_;
            need_new = true;
        }
    }
    if (existing_fd >= 0) {
        co_return existing_fd;
    }
    if (!need_new) {
        // Pool at capacity; fast-fail. The caller decides retry policy.
        co_return -EMFILE;
    }

    // Step 2: open + connect (suspends; no lock held).
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        const int err = errno;
        std::lock_guard lock(mu_);
        --leased_;
        co_return -err;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg_.port);
    if (::inet_pton(AF_INET, cfg_.host.c_str(), &addr.sin_addr) != 1) {
        ::close(fd);
        std::lock_guard lock(mu_);
        --leased_;
        co_return -EINVAL;
    }
    const int cr =
        co_await reactor_.connect_async(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (cr < 0) {
        ::close(fd);
        std::lock_guard lock(mu_);
        --leased_;
        co_return cr;
    }
    co_return fd;
}

void HttpPool::release_fd_(int fd, bool keep_alive) {
    if (fd < 0)
        return;
    if (!keep_alive) {
        ::close(fd);
        std::lock_guard lock(mu_);
        --leased_;
        return;
    }
    std::lock_guard lock(mu_);
    --leased_;
    free_fds_.push_back(fd);
}

Task<HttpResponse> HttpPool::read_response_(int fd) {
    HttpResponse resp;
    std::string buf;
    buf.reserve(cfg_.initial_read_chunk);
    std::vector<char> chunk(cfg_.initial_read_chunk);

    // Read into buf until "\r\n\r\n" marks the end of headers.
    std::size_t header_end = std::string::npos;
    while (header_end == std::string::npos) {
        const int n =
            co_await reactor_.read_async(fd, chunk.data(), static_cast<unsigned>(chunk.size()));
        if (n <= 0) {
            resp.status = 0;
            resp.error =
                "read failed before headers complete (errno=" + std::to_string(n < 0 ? -n : 0) +
                ")";
            co_return resp;
        }
        buf.append(chunk.data(), static_cast<std::size_t>(n));
        header_end = buf.find("\r\n\r\n");
        if (buf.size() > 64 * 1024 && header_end == std::string::npos) {
            resp.status = 0;
            resp.error = "response header exceeded 64KiB without terminator";
            co_return resp;
        }
    }

    // Status line.
    auto first_eol = buf.find("\r\n");
    auto status_line = std::string_view{buf}.substr(0, first_eol);
    auto sp1 = status_line.find(' ');
    auto sp2 =
        sp1 == std::string_view::npos ? std::string_view::npos : status_line.find(' ', sp1 + 1);
    if (sp1 == std::string_view::npos || sp2 == std::string_view::npos) {
        resp.status = 0;
        resp.error = "malformed status line";
        co_return resp;
    }
    auto code_str = status_line.substr(sp1 + 1, sp2 - sp1 - 1);
    int code = 0;
    auto [code_end, code_ec] =
        std::from_chars(code_str.data(), code_str.data() + code_str.size(), code);
    if (code_ec != std::errc{} || code_end != code_str.data() + code_str.size()) {
        resp.status = 0;
        resp.error = "non-numeric status code";
        co_return resp;
    }
    resp.status = code;

    // Headers: walk lines between status_line and the empty marker.
    std::size_t pos = first_eol + 2;
    while (pos < header_end) {
        const auto eol = buf.find("\r\n", pos);
        if (eol == std::string::npos || eol > header_end)
            break;
        const auto colon = buf.find(':', pos);
        if (colon != std::string::npos && colon < eol) {
            std::string name = lower(std::string_view{buf}.substr(pos, colon - pos));
            std::string value{std::string_view{buf}.substr(colon + 1, eol - colon - 1)};
            trim_inplace(value);
            resp.headers[std::move(name)] = std::move(value);
        }
        pos = eol + 2;
    }

    // Body: Content-Length first; chunked TE deferred to a follow-on.
    std::size_t content_length = 0;
    bool has_content_length = false;
    if (auto it = resp.headers.find("content-length"); it != resp.headers.end()) {
        auto& s = it->second;
        std::uint64_t n = 0;
        auto [_, ec] = std::from_chars(s.data(), s.data() + s.size(), n);
        if (ec == std::errc{}) {
            content_length = static_cast<std::size_t>(n);
            has_content_length = true;
        }
    }
    if (!has_content_length) {
        if (auto it = resp.headers.find("transfer-encoding");
            it != resp.headers.end() && lower(it->second).find("chunked") != std::string::npos) {
            resp.status = 0;
            resp.error = "chunked transfer-encoding not supported in 28e-3 (use Content-Length)";
            co_return resp;
        }
        // No content-length and no chunked: assume empty body.
        co_return resp;
    }

    // Body bytes already in buf (after the \r\n\r\n marker).
    const std::size_t body_start = header_end + 4;
    const std::size_t already = buf.size() - body_start;
    resp.body.assign(buf.data() + body_start, std::min(already, content_length));

    while (resp.body.size() < content_length) {
        const std::size_t want = content_length - resp.body.size();
        const std::size_t chunk_sz = std::min(want, chunk.size());
        const int n =
            co_await reactor_.read_async(fd, chunk.data(), static_cast<unsigned>(chunk_sz));
        if (n <= 0) {
            resp.status = 0;
            resp.error = "read failed during body";
            co_return resp;
        }
        resp.body.append(chunk.data(), static_cast<std::size_t>(n));
    }
    co_return resp;
}

Task<HttpResponse> HttpPool::request(HttpRequest req) {
    const int fd = co_await acquire_fd_();
    if (fd < 0) {
        HttpResponse err;
        err.status = 0;
        err.error = "acquire failed (errno=" + std::to_string(-fd) + ")";
        co_return err;
    }

    const auto wire = serialize_request_(req);

    // Write request - may need multiple write_async calls if the
    // kernel partial-writes. write_async returns bytes-written or
    // -errno on a single call.
    std::size_t off = 0;
    while (off < wire.size()) {
        const int n = co_await reactor_.write_async(
            fd, wire.data() + off, static_cast<unsigned>(wire.size() - off));
        if (n <= 0) {
            release_fd_(fd, /*keep_alive=*/false);
            HttpResponse err;
            err.status = 0;
            err.error = "write failed (errno=" + std::to_string(n < 0 ? -n : 0) + ")";
            co_return err;
        }
        off += static_cast<std::size_t>(n);
    }

    auto resp = co_await read_response_(fd);

    // Decide keep-alive: any "Connection: close" header (request or
    // response side) means the server will close after this; we
    // mirror by dropping the fd. Status 0 means parse failure; drop too.
    bool keep_alive = resp.status >= 100;
    if (keep_alive) {
        if (auto it = resp.headers.find("connection");
            it != resp.headers.end() && lower(it->second) == "close") {
            keep_alive = false;
        }
    }
    release_fd_(fd, keep_alive);
    co_return resp;
}

Task<HttpResponse> HttpPool::get(std::string path) {
    HttpRequest req;
    req.method = "GET";
    req.path = std::move(path);
    return request(std::move(req));
}

Task<HttpResponse> HttpPool::post(std::string path, std::string body, std::string content_type) {
    HttpRequest req;
    req.method = "POST";
    req.path = std::move(path);
    req.body = std::move(body);
    req.headers["Content-Type"] = std::move(content_type);
    return request(std::move(req));
}

}  // namespace clink::async

#endif  // __linux__ && CLINK_HAS_URING
