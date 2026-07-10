#pragma once

// JM-side HTTP route registration for Queryable State multi-TM
// discovery. Layered on top of the TM-side Registry+server triple
// (registry.hpp / server.hpp / client.hpp) and the JM's existing
// per-job placement tracking.
//
// Adds two routes to the HttpServer:
//
//   GET /api/v1/queryable_state/job/:job_id/tms
//     -> { "tms": [{ "host": "...", "port": NNN }, ...] }
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
// The `topology_version` is a per-job counter the JM bumps on every
// successful (re)deploy. Clients piggyback on it for cache
// invalidation: a cached `(kg → TmTarget)` entry captured at version
// V is stale once the JM reports a different V. The topology_version
// endpoint is a cheap call clients periodically poll to detect
// rescales without re-fetching individual routes.
//
// The `/tms` list is the set of unique TM HTTP targets hosting any
// subtask of the given job at the moment the request was served. Used
// by ClusterClient for brute-force iteration when key-group-aware
// routing isn't worth a JM round-trip (small parallelism).
//
// The `/route` endpoint is the kg-aware fast path. The JM computes
// key_group_for_key(key_bytes) and returns the single subtask
// responsible for that group, plus its TM's HTTP target. Used by
// `RoutedClient` in cluster_client.hpp.
//
// Empty list response (still 200) for: job not found, job has no
// running subtasks yet, or every hosting TM has dropped HTTP.

#include <algorithm>
#include <cstdint>
#include <span>
#include <string>

#include "clink/cluster/job_manager.hpp"
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

}  // namespace detail

inline void register_jm_routes(http::HttpServer& server, cluster::JobManager& jm) {
    server.get("/api/v1/queryable_state/job/:job_id/tms",
               [&jm](const http::HttpRequest& req) -> http::HttpResponse {
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
                   const auto tms = jm.tms_hosting_job(job);
                   std::string body = "{\"tms\":[";
                   for (std::size_t i = 0; i < tms.size(); ++i) {
                       if (i > 0) {
                           body.push_back(',');
                       }
                       body += "{\"host\":";
                       body += detail::json_string(tms[i].first);
                       body += ",\"port\":";
                       body += std::to_string(tms[i].second);
                       body += "}";
                   }
                   body += "]}";
                   resp.body = std::move(body);
                   return resp;
               });

    server.get(
        "/api/v1/queryable_state/job/:job_id/op/:role/route",
        [&jm](const http::HttpRequest& req) -> http::HttpResponse {
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
            auto target = jm.route_key_for_job(
                job, role_it->second, std::span<const std::byte>{decoded->data(), decoded->size()});
            if (!target.has_value()) {
                resp.status = 404;
                resp.body = "{\"error\":\"no subtask covers key\"}";
                return resp;
            }
            const auto version = jm.topology_version(job);
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
    // The JM fans the lookup out across the role's subtasks (in subtask
    // order) and relays the first hit verbatim, so the client needs no
    // codec, no hex, and no TM discovery. Fan-out is correct at any
    // parallelism without reproducing the shuffle's key hashing here; the
    // key-group fast path (/route + a direct TM call) remains available
    // to latency-sensitive clients. Keys must be URL-safe (percent-encode
    // anything else); the raw query value is forwarded verbatim.
    //   200 -> the owning TM's body: {"key":"...","value":{...}}
    //   404 -> job/role unknown, no TM exposes HTTP, or key not found
    server.get("/api/v1/queryable_state/job/:job_id/op/:role/json/:slot",
               [&jm](const http::HttpRequest& req) -> http::HttpResponse {
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
                   const auto targets = jm.subtask_targets_for_role(job, role_it->second);
                   if (targets.empty()) {
                       resp.status = 404;
                       resp.body =
                           "{\"error\":\"job or role not found (or no hosting TM "
                           "exposes HTTP)\"}";
                       return resp;
                   }
                   for (const auto& t : targets) {
                       http::HttpClient client(t.host, t.port);
                       auto tm_resp =
                           client.get("/api/v1/queryable_state/op/" + role_it->second +
                                      "/subtask/" + std::to_string(t.subtask_idx) + "/json/" +
                                      slot_it->second + "?key=" + key_it->second);
                       if (tm_resp.status == 200) {
                           resp.body = tm_resp.body;
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
    server.get("/api/v1/queryable_state/job/:job_id/op/:role/json/:slot/scan",
               [&jm](const http::HttpRequest& req) -> http::HttpResponse {
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
                   const auto targets = jm.subtask_targets_for_role(job, role_it->second);
                   if (targets.empty()) {
                       resp.status = 404;
                       resp.body =
                           "{\"error\":\"job or role not found (or no hosting TM "
                           "exposes HTTP)\"}";
                       return resp;
                   }
                   // Merge the subtasks' entry arrays. Each TM body is
                   // {"entries":[...],"truncated":bool}; parse and re-emit rather
                   // than splice text, so entry counting is exact regardless of
                   // what the value documents contain.
                   std::string body = "{\"entries\":[";
                   bool truncated = false;
                   bool any_slot = false;
                   std::size_t taken = 0;
                   bool first = true;
                   for (const auto& t : targets) {
                       if (taken >= limit) {
                           truncated = true;
                           break;
                       }
                       http::HttpClient client(t.host, t.port);
                       auto tm_resp = client.get("/api/v1/queryable_state/op/" + role_it->second +
                                                 "/subtask/" + std::to_string(t.subtask_idx) +
                                                 "/json/" + slot_it->second +
                                                 "/scan?limit=" + std::to_string(limit - taken));
                       if (tm_resp.status != 200) {
                           continue;  // subtask without the slot (or gone): skip
                       }
                       try {
                           auto js = clink::config::parse(tm_resp.body);
                           any_slot = true;
                           for (const auto& entry : js.at("entries").as_array()) {
                               if (taken >= limit) {
                                   truncated = true;
                                   break;
                               }
                               if (!first) {
                                   body.push_back(',');
                               }
                               body += entry.serialize(0);
                               first = false;
                               ++taken;
                           }
                           if (js.contains("truncated") && js.at("truncated").is_bool() &&
                               js.at("truncated").as_bool()) {
                               truncated = true;
                           }
                       } catch (...) {
                           continue;  // malformed TM body: skip that subtask
                       }
                   }
                   if (!any_slot) {
                       resp.status = 404;
                       resp.body = "{\"error\":\"slot not registered on any subtask\"}";
                       return resp;
                   }
                   body += "],\"truncated\":";
                   body += truncated ? "true" : "false";
                   body += "}";
                   resp.body = std::move(body);
                   return resp;
               });

    server.get("/api/v1/queryable_state/job/:job_id/topology_version",
               [&jm](const http::HttpRequest& req) -> http::HttpResponse {
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
                   const auto version = jm.topology_version(job);
                   resp.body = "{\"version\":" + std::to_string(version) + "}";
                   return resp;
               });
}

}  // namespace clink::queryable_state
