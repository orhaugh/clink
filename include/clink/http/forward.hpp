#pragma once

// Answering a request on another node's behalf. A standby Coordinator forwards
// the API it is asked for to the leader, so a client that reaches either
// Coordinator, as through a Kubernetes Service spreading requests across both,
// gets the leader's answer (include/clink/cluster/standby_http.hpp).

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

#include "clink/http/http_server.hpp"

namespace clink::http {

// Marks a forwarded request. The node it reaches answers it, or refuses it, but
// never forwards it again, so two nodes that each take the other for the leader
// cannot pass a request back and forth.
inline constexpr std::string_view kForwardedHeader = "X-Clink-Forwarded";

// Whether `request` reached this node by forwarding.
bool is_forwarded(const HttpRequest& request);

// `request` sent on to host:port as this node received it: the same method,
// path, query and body, or multipart parts, with its Authorization and Accept
// headers, marked with kForwardedHeader. Answers what that node answered (its
// status, content type, body, and the WWW-Authenticate, Retry-After and
// Cache-Control headers), or 502 naming it when it cannot be reached. The body
// is held in memory both ways.
HttpResponse forward_request(const HttpRequest& request,
                             const std::string& host,
                             std::uint16_t port,
                             std::chrono::milliseconds read_timeout);

}  // namespace clink::http
