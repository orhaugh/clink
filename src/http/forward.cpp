// forward_request. cpp-httplib confined to this translation unit.

#include "clink/http/forward.hpp"

#include <httplib.h>
#include <string>
#include <utility>

#include "clink/http/json_writer.hpp"

namespace clink::http {

namespace {

std::string lower(std::string_view s) {
    std::string out(s);
    for (auto& c : out) {
        c = static_cast<char>(::tolower(static_cast<unsigned char>(c)));
    }
    return out;
}

HttpResponse unreachable(const std::string& host, std::uint16_t port, const std::string& error) {
    HttpResponse out;
    out.status = 502;
    JsonWriter w;
    w.begin_object();
    w.kv("error", "forward failed");
    w.kv("detail", host + ":" + std::to_string(port) + " did not answer: " + error);
    w.end_object();
    out.body = w.str();
    return out;
}

}  // namespace

bool is_forwarded(const HttpRequest& request) {
    return request.headers.count(lower(kForwardedHeader)) != 0;
}

HttpResponse forward_request(const HttpRequest& request,
                             const std::string& host,
                             std::uint16_t port,
                             std::chrono::milliseconds read_timeout) {
    httplib::Client client(host, port);
    client.set_connection_timeout(std::chrono::seconds{2});
    client.set_read_timeout(read_timeout);
    client.set_write_timeout(std::chrono::seconds{30});

    httplib::Headers headers{{std::string{kForwardedHeader}, "1"}};
    for (const char* name : {"authorization", "accept"}) {
        if (const auto it = request.headers.find(name); it != request.headers.end()) {
            headers.emplace(name, it->second);
        }
    }
    httplib::Params params(request.query.begin(), request.query.end());
    const auto target = httplib::append_query_params(request.path, params);

    httplib::Result res;
    if (request.method == "GET" || request.method == "HEAD") {
        // A HEAD reaches the GET route; the server drops the body it answers.
        res = client.Get(target, headers);
    } else if (request.method == "POST" && !request.files.empty()) {
        httplib::MultipartFormDataItems parts;
        for (const auto& [name, file] : request.files) {
            parts.push_back({name, file.content, file.filename, file.content_type});
        }
        res = client.Post(target, headers, parts);
    } else if (request.method == "POST") {
        const auto it = request.headers.find("content-type");
        res = client.Post(target,
                          headers,
                          request.body,
                          it == request.headers.end() ? "application/json" : it->second);
    } else {
        HttpResponse out;
        out.status = 405;
        out.body = R"({"error":"method not forwarded"})";
        return out;
    }
    if (!res) {
        return unreachable(host, port, httplib::to_string(res.error()));
    }
    HttpResponse out;
    out.status = res->status;
    out.body = std::move(res->body);
    if (res->has_header("Content-Type")) {
        out.content_type = res->get_header_value("Content-Type");
    }
    for (const char* name : {"WWW-Authenticate", "Retry-After", "Cache-Control"}) {
        if (res->has_header(name)) {
            out.headers[name] = res->get_header_value(name);
        }
    }
    return out;
}

}  // namespace clink::http
