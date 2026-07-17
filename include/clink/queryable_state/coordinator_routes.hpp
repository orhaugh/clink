#pragma once

// coordinator-side HTTP route registration for Queryable State multi-worker
// discovery. Layered on top of the worker-side Registry+server triple
// (registry.hpp / server.hpp / client.hpp) and the coordinator's existing
// per-job placement tracking.
//
// Adds two routes to the HttpServer:
//
//   GET /api/v1/queryable_state/job/:job_id/workers
//     -> { "workers": [{ "host": "...", "port": NNN }, ...] }
//
//   GET /api/v1/queryable_state/job/:job_id/op/:role/route?key=<hex>
//     -> { "host": "...", "port": NNN, "subtask_idx": N,
//          "topology_version": V }
//     404 if no subtask covers the key's key-group, or job/role
//     unknown.
//
//   GET /api/v1/queryable_state/job/:job_id/topology_version
//     -> { "version": V }   (V == 0 means job unknown)
//
// The `topology_version` is a per-job counter the coordinator bumps on every
// successful (re)deploy. Clients piggyback on it for cache
// invalidation: a cached `(kg → WorkerTarget)` entry captured at version
// V is stale once the coordinator reports a different V. The topology_version
// endpoint is a cheap call clients periodically poll to detect
// rescales without re-fetching individual routes.
//
// The `/workers` list is the set of unique worker HTTP targets hosting any
// subtask of the given job at the moment the request was served. Used
// by ClusterClient for brute-force iteration when key-group-aware
// routing isn't worth a coordinator round-trip (small parallelism).
//
// The `/route` endpoint is the kg-aware fast path. The coordinator computes
// key_group_for_key(key_bytes) and returns the single subtask
// responsible for that group, plus its worker's HTTP target. Used by
// `RoutedClient` in cluster_client.hpp.
//
// Empty list response (still 200) for: job not found, job has no
// running subtasks yet, or every hosting worker has dropped HTTP.

#include <algorithm>
#include <cstdint>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "clink/cluster/coordinator.hpp"
#include "clink/config/json.hpp"
#include "clink/http/http_client.hpp"
#include "clink/http/http_server.hpp"
#include "clink/queryable_state/server.hpp"  // hex_decode, json_escape

namespace clink::queryable_state {

namespace detail {

inline std::string json_string(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    out.push_back('"');
    for (char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            default:
                out.push_back(c);
        }
    }
    out.push_back('"');
    return out;
}

// One fanned-out scan over every subtask of (job, role): the JSON scan
// route and the Arrow scan route (arrow_scan.hpp) share this.
struct FannedScan {
    std::vector<std::pair<std::string, std::string>> entries;  // key -> value JSON
    bool truncated{false};
    bool any_target{false};  // the role resolved to at least one HTTP worker
    bool any_slot{false};    // at least one subtask served the slot
};

inline FannedScan fan_out_scan(cluster::Coordinator& coordinator,
                               cluster::JobId job,
                               const std::string& role,
                               const std::string& slot,
                               std::size_t limit) {
    FannedScan out;
    const auto targets = coordinator.subtask_targets_for_role(job, role);
    out.any_target = !targets.empty();
    for (const auto& t : targets) {
        if (out.entries.size() >= limit) {
            out.truncated = true;
            break;
        }
        http::HttpClient client(t.host, t.port);
        auto worker_resp = client.get("/api/v1/queryable_state/op/" + role + "/subtask/" +
                                      std::to_string(t.subtask_idx) + "/json/" + slot +
                                      "/scan?limit=" + std::to_string(limit - out.entries.size()));
        if (worker_resp.status != 200) {
            continue;  // subtask without the slot (or gone): skip
        }
        try {
            auto js = clink::config::parse(worker_resp.body);
            out.any_slot = true;
            for (const auto& entry : js.at("entries").as_array()) {
                if (out.entries.size() >= limit) {
                    out.truncated = true;
                    break;
                }
                out.entries.emplace_back(entry.at("key").as_string(),
                                         entry.at("value").serialize(0));
            }
            if (js.contains("truncated") && js.at("truncated").is_bool() &&
                js.at("truncated").as_bool()) {
                out.truncated = true;
            }
        } catch (...) {
            continue;  // malformed worker body: skip that subtask
        }
    }
    return out;
}

}  // namespace detail

inline void register_coordinator_routes(http::HttpServer& server,
                                        cluster::Coordinator& coordinator) {
    server.get("/api/v1/queryable_state/job/:job_id/workers",
               [&coordinator](const http::HttpRequest& req) -> http::HttpResponse {
                   http::HttpResponse resp;
                   auto it = req.path_params.find("job_id");
                   if (it == req.path_params.end()) {
                       resp.status = 400;
                       resp.body = "{\"error\":\"missing job_id\"}";
                       return resp;
                   }
                   cluster::JobId job{};
                   try {
                       job = static_cast<cluster::JobId>(std::stoull(it->second));
                   } catch (...) {
                       resp.status = 400;
                       resp.body = "{\"error\":\"malformed job_id\"}";
                       return resp;
                   }
                   const auto workers = coordinator.workers_hosting_job(job);
                   std::string body = "{\"workers\":[";
                   for (std::size_t i = 0; i < workers.size(); ++i) {
                       if (i > 0) {
                           body.push_back(',');
                       }
                       body += "{\"host\":";
                       body += detail::json_string(workers[i].first);
                       body += ",\"port\":";
                       body += std::to_string(workers[i].second);
                       body += "}";
                   }
                   body += "]}";
                   resp.body = std::move(body);
                   return resp;
               });

    server.get(
        "/api/v1/queryable_state/job/:job_id/op/:role/route",
        [&coordinator](const http::HttpRequest& req) -> http::HttpResponse {
            http::HttpResponse resp;
            auto job_it = req.path_params.find("job_id");
            auto role_it = req.path_params.find("role");
            if (job_it == req.path_params.end() || role_it == req.path_params.end()) {
                resp.status = 400;
                resp.body = "{\"error\":\"missing job_id or role\"}";
                return resp;
            }
            cluster::JobId job{};
            try {
                job = static_cast<cluster::JobId>(std::stoull(job_it->second));
            } catch (...) {
                resp.status = 400;
                resp.body = "{\"error\":\"malformed job_id\"}";
                return resp;
            }
            auto key_it = req.query.find("key");
            if (key_it == req.query.end()) {
                resp.status = 400;
                resp.body = "{\"error\":\"missing key\"}";
                return resp;
            }
            auto decoded = detail::hex_decode(key_it->second);
            if (!decoded.has_value()) {
                resp.status = 400;
                resp.body = "{\"error\":\"malformed key hex\"}";
                return resp;
            }
            auto target = coordinator.route_key_for_job(
                job, role_it->second, std::span<const std::byte>{decoded->data(), decoded->size()});
            if (!target.has_value()) {
                resp.status = 404;
                resp.body = "{\"error\":\"no subtask covers key\"}";
                return resp;
            }
            const auto version = coordinator.topology_version(job);
            std::string body = "{\"host\":";
            body += detail::json_string(target->host);
            body += ",\"port\":";
            body += std::to_string(target->port);
            body += ",\"subtask_idx\":";
            body += std::to_string(target->subtask_idx);
            body += ",\"topology_version\":";
            body += std::to_string(version);
            body += "}";
            resp.body = std::move(body);
            return resp;
        });

    // JSON serving route - the one-call lookup a consumer actually uses:
    //   GET /api/v1/queryable_state/job/:job_id/op/:role/json/:slot?key=<string>
    // The coordinator fans the lookup out across the role's subtasks (in subtask
    // order) and relays the first hit verbatim, so the client needs no
    // codec, no hex, and no worker discovery. Fan-out is correct at any
    // parallelism without reproducing the shuffle's key hashing here; the
    // key-group fast path (/route + a direct worker call) remains available
    // to latency-sensitive clients. Keys must be URL-safe (percent-encode
    // anything else); the raw query value is forwarded verbatim.
    //   200 -> the owning worker's body: {"key":"...","value":{...}}
    //   404 -> job/role unknown, no worker exposes HTTP, or key not found
    server.get("/api/v1/queryable_state/job/:job_id/op/:role/json/:slot",
               [&coordinator](const http::HttpRequest& req) -> http::HttpResponse {
                   http::HttpResponse resp;
                   auto job_it = req.path_params.find("job_id");
                   auto role_it = req.path_params.find("role");
                   auto slot_it = req.path_params.find("slot");
                   if (job_it == req.path_params.end() || role_it == req.path_params.end() ||
                       slot_it == req.path_params.end()) {
                       resp.status = 400;
                       resp.body = "{\"error\":\"missing job_id / role / slot\"}";
                       return resp;
                   }
                   cluster::JobId job{};
                   try {
                       job = static_cast<cluster::JobId>(std::stoull(job_it->second));
                   } catch (...) {
                       resp.status = 400;
                       resp.body = "{\"error\":\"malformed job_id\"}";
                       return resp;
                   }
                   auto key_it = req.query.find("key");
                   if (key_it == req.query.end()) {
                       resp.status = 400;
                       resp.body = "{\"error\":\"missing key=<string> query param\"}";
                       return resp;
                   }
                   const auto targets = coordinator.subtask_targets_for_role(job, role_it->second);
                   if (targets.empty()) {
                       resp.status = 404;
                       resp.body =
                           "{\"error\":\"job or role not found (or no hosting worker "
                           "exposes HTTP)\"}";
                       return resp;
                   }
                   for (const auto& t : targets) {
                       http::HttpClient client(t.host, t.port);
                       auto worker_resp =
                           client.get("/api/v1/queryable_state/op/" + role_it->second +
                                      "/subtask/" + std::to_string(t.subtask_idx) + "/json/" +
                                      slot_it->second + "?key=" + key_it->second);
                       if (worker_resp.status == 200) {
                           resp.body = worker_resp.body;
                           return resp;
                       }
                   }
                   resp.status = 404;
                   resp.body = "{\"error\":\"key not found\"}";
                   return resp;
               });

    // JSON scan route - state-as-table in one call. Concatenates bounded
    // scans from every subtask of the role (in subtask order), stopping
    // at ?limit=N (default 1000, clamped to 100000). `truncated` is true
    // when any subtask truncated or the limit cut the fan-out short.
    // The SQL connector='queryable_state' source reads this route.
    // JSON scan route - state-as-table in one call. Concatenates bounded
    // scans from every subtask of the role (in subtask order), stopping
    // at ?limit=N (default 1000, clamped to 100000). `truncated` is true
    // when any subtask truncated or the limit cut the fan-out short.
    // The SQL connector='queryable_state' source reads this route.
    server.get("/api/v1/queryable_state/job/:job_id/op/:role/json/:slot/scan",
               [&coordinator](const http::HttpRequest& req) -> http::HttpResponse {
                   http::HttpResponse resp;
                   auto job_it = req.path_params.find("job_id");
                   auto role_it = req.path_params.find("role");
                   auto slot_it = req.path_params.find("slot");
                   if (job_it == req.path_params.end() || role_it == req.path_params.end() ||
                       slot_it == req.path_params.end()) {
                       resp.status = 400;
                       resp.body = "{\"error\":\"missing job_id / role / slot\"}";
                       return resp;
                   }
                   cluster::JobId job{};
                   try {
                       job = static_cast<cluster::JobId>(std::stoull(job_it->second));
                   } catch (...) {
                       resp.status = 400;
                       resp.body = "{\"error\":\"malformed job_id\"}";
                       return resp;
                   }
                   std::size_t limit = 1000;
                   if (auto it = req.query.find("limit"); it != req.query.end()) {
                       try {
                           limit = static_cast<std::size_t>(std::stoull(it->second));
                       } catch (...) {
                           resp.status = 400;
                           resp.body = "{\"error\":\"malformed limit\"}";
                           return resp;
                       }
                   }
                   limit = std::min<std::size_t>(limit, 100'000);
                   const auto scan = detail::fan_out_scan(
                       coordinator, job, role_it->second, slot_it->second, limit);
                   if (!scan.any_target) {
                       resp.status = 404;
                       resp.body =
                           "{\"error\":\"job or role not found (or no hosting worker "
                           "exposes HTTP)\"}";
                       return resp;
                   }
                   if (!scan.any_slot) {
                       resp.status = 404;
                       resp.body = "{\"error\":\"slot not registered on any subtask\"}";
                       return resp;
                   }
                   std::string body = "{\"entries\":[";
                   for (std::size_t i = 0; i < scan.entries.size(); ++i) {
                       if (i > 0) {
                           body.push_back(',');
                       }
                       body += "{\"key\":" + detail::json_escape(scan.entries[i].first) +
                               ",\"value\":" + scan.entries[i].second + "}";
                   }
                   body += "],\"truncated\":";
                   body += scan.truncated ? "true" : "false";
                   body += "}";
                   resp.body = std::move(body);
                   return resp;
               });

    server.get("/api/v1/queryable_state/job/:job_id/topology_version",
               [&coordinator](const http::HttpRequest& req) -> http::HttpResponse {
                   http::HttpResponse resp;
                   auto it = req.path_params.find("job_id");
                   if (it == req.path_params.end()) {
                       resp.status = 400;
                       resp.body = "{\"error\":\"missing job_id\"}";
                       return resp;
                   }
                   cluster::JobId job{};
                   try {
                       job = static_cast<cluster::JobId>(std::stoull(it->second));
                   } catch (...) {
                       resp.status = 400;
                       resp.body = "{\"error\":\"malformed job_id\"}";
                       return resp;
                   }
                   const auto version = coordinator.topology_version(job);
                   resp.body = "{\"version\":" + std::to_string(version) + "}";
                   return resp;
               });
}

}  // namespace clink::queryable_state
