#pragma once

// How a Coordinator under HA answers its HTTP API. The leader answers all of it.
// A standby answers what is about itself alone (its probes, health, metrics and
// the console's files) and forwards the rest to the leader, so a client that
// reaches either Coordinator gets the leader's answer, and a Service may spread
// requests across both (docs/internals/distributed-runtime.md, HA leader
// election).

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "clink/cluster/ha_coordinator.hpp"
#include "clink/http/http_server.hpp"

namespace clink::cluster {

// A request a Coordinator answers about itself, leader or standby: a GET of
// /livez, /readyz, /api/v1/health or /metrics, or of anything outside /api/
// (the console's files).
bool answered_by_this_node(std::string_view method, std::string_view path);

// The leader a standby forwards to: the registered leader, when it records an
// HTTP port and is not this node itself (a record a previous run of this node
// left behind).
std::optional<LeaderEndpoint> leader_to_forward_to(HaCoordinator& ha,
                                                   const std::string& self_host,
                                                   std::uint16_t self_http_port);

// How a standby answers `request`: nullopt when this node answers it itself (it
// leads, or the request is about this node); otherwise the leader's answer, or
// 503 when there is no leader to forward to or the request was forwarded once
// already, which a standby never does twice.
std::optional<http::HttpResponse> answer_as_standby(HaCoordinator& ha,
                                                    const std::string& self_host,
                                                    std::uint16_t self_http_port,
                                                    const http::HttpRequest& request,
                                                    std::chrono::milliseconds read_timeout);

// How a leader that has not yet recovered the jobs its predecessor ran answers
// `request`: a request for the job API (/api/v1/jobs and everything under it)
// is refused with 503 and Retry-After, as a list without those jobs, a 404 for
// one of them or a submission numbered among them would each be wrong; nullopt
// for any other request, which it answers as usual.
std::optional<http::HttpResponse> answer_while_recovering(const http::HttpRequest& request);

// GET /readyz for a Coordinator: 200 when it can answer the API, as the leader
// with its control port bound and its predecessor's jobs recovered, or as a
// standby with a leader to forward to; 503 otherwise. `ha` is null without HA,
// where the Coordinator is ready once bound.
http::HttpResponse coordinator_readiness(HaCoordinator* ha,
                                         bool control_port_bound,
                                         bool jobs_recovered,
                                         const std::string& self_host,
                                         std::uint16_t self_http_port);

}  // namespace clink::cluster
