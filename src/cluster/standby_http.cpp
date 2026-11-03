#include "clink/cluster/standby_http.hpp"

#include <utility>

#include "clink/http/forward.hpp"
#include "clink/http/json_writer.hpp"

namespace clink::cluster {

namespace {

http::HttpResponse refused(const std::string& detail) {
    http::HttpResponse out;
    out.status = 503;
    out.headers["Retry-After"] = "1";
    http::JsonWriter w;
    w.begin_object();
    w.kv("error", "standby");
    w.kv("detail", detail);
    w.end_object();
    out.body = w.str();
    return out;
}

http::HttpResponse plain(int status, std::string body) {
    http::HttpResponse out;
    out.status = status;
    out.content_type = "text/plain";
    out.body = std::move(body);
    return out;
}

}  // namespace

bool answered_by_this_node(std::string_view method, std::string_view path) {
    if (method != "GET" && method != "HEAD") {
        return false;
    }
    if (path == "/livez" || path == "/readyz" || path == "/api/v1/health" ||
        path.starts_with("/metrics")) {
        return true;
    }
    return !path.starts_with("/api/") && path != "/api";
}

std::optional<LeaderEndpoint> leader_to_forward_to(HaCoordinator& ha,
                                                   const std::string& self_host,
                                                   std::uint16_t self_http_port) {
    auto leader = ha.current_leader_endpoint();
    if (!leader || leader->http_port == 0 ||
        (leader->host == self_host && leader->http_port == self_http_port)) {
        return std::nullopt;
    }
    return leader;
}

std::optional<http::HttpResponse> answer_as_standby(HaCoordinator& ha,
                                                    const std::string& self_host,
                                                    std::uint16_t self_http_port,
                                                    const http::HttpRequest& request,
                                                    std::chrono::milliseconds read_timeout) {
    if (ha.is_leader() || answered_by_this_node(request.method, request.path)) {
        return std::nullopt;
    }
    if (http::is_forwarded(request)) {
        return refused(
            "this Coordinator is a standby, and the request was forwarded to it as the leader");
    }
    const auto leader = leader_to_forward_to(ha, self_host, self_http_port);
    if (!leader) {
        return refused("this Coordinator is a standby, and no leader serving HTTP is registered");
    }
    return http::forward_request(request, leader->host, leader->http_port, read_timeout);
}

std::optional<http::HttpResponse> answer_while_recovering(const http::HttpRequest& request) {
    const std::string_view path = request.path;
    if (path != "/api/v1/jobs" && !path.starts_with("/api/v1/jobs/")) {
        return std::nullopt;
    }
    http::HttpResponse out;
    out.status = 503;
    out.headers["Retry-After"] = "2";
    http::JsonWriter w;
    w.begin_object();
    w.kv("error", "recovering");
    w.kv("detail",
         "this Coordinator has just taken over and is recovering the jobs its predecessor ran");
    w.end_object();
    out.body = w.str();
    return out;
}

http::HttpResponse coordinator_readiness(HaCoordinator* ha,
                                         bool control_port_bound,
                                         bool jobs_recovered,
                                         const std::string& self_host,
                                         std::uint16_t self_http_port) {
    if (ha == nullptr || ha->is_leader()) {
        if (!control_port_bound) {
            return plain(503, "starting\n");
        }
        if (!jobs_recovered) {
            return plain(503, "recovering\n");
        }
        return plain(200, ha == nullptr ? "ok\n" : "leader\n");
    }
    if (leader_to_forward_to(*ha, self_host, self_http_port)) {
        return plain(200, "standby\n");
    }
    return plain(503, "no leader\n");
}

}  // namespace clink::cluster
