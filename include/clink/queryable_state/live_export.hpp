#pragma once

// Live whole-job state export over HTTP - the running-job analogue of
// `clink state-export`. The TM route merges every hosted subtask
// backend's live Arrow export for a job into one canonical
// state-snapshot stream (docs/internals/state-snapshot-format.md); the
// JM route fans out across every TM hosting the job and merges their
// streams, so one GET returns the whole job's keyed state while it
// runs.
//
// Consistency: each backend export is a per-subtask atomic
// point-in-time view. The merged stream is NOT a checkpoint-consistent
// global cut (no barrier alignment across subtasks) - use a savepoint
// where cross-subtask consistency matters. Backends without a complete
// live view (the disaggregated RemoteReadBackend) refuse; their
// subtasks are reported in the X-Clink-Skipped-Subtasks header rather
// than silently omitted.
//
//   TM: GET /api/v1/state/export/job/:job_id
//   JM: GET /api/v1/state/export/job/:job_id
//
// Both respond with Content-Type application/vnd.apache.arrow.stream.

#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

#include "clink/cluster/job_manager.hpp"
#include "clink/cluster/task_manager.hpp"
#include "clink/http/http_client.hpp"
#include "clink/http/http_server.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace clink::queryable_state {

namespace live_export_detail {

inline std::string body_from_bytes(const std::vector<std::byte>& bytes) {
    return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

}  // namespace live_export_detail

// TM-side route: this TM's share of the job's live state as one stream.
inline void register_tm_state_export_route(http::HttpServer& server, cluster::TaskManager& tm) {
    server.get("/api/v1/state/export/job/:job_id",
               [&tm](const http::HttpRequest& req) -> http::HttpResponse {
                   http::HttpResponse resp;
                   auto job_it = req.path_params.find("job_id");
                   if (job_it == req.path_params.end()) {
                       resp.status = 400;
                       resp.body = "{\"error\":\"missing job_id\"}";
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
                   auto exported = tm.export_job_state_arrow(job);
                   if (!exported.has_value()) {
                       resp.status = 404;
                       resp.body = "{\"error\":\"no state backends hosted for this job\"}";
                       return resp;
                   }
                   resp.body = live_export_detail::body_from_bytes(exported->bytes);
                   resp.content_type = "application/vnd.apache.arrow.stream";
                   if (exported->skipped_subtasks > 0) {
                       resp.headers["X-Clink-Skipped-Subtasks"] =
                           std::to_string(exported->skipped_subtasks);
                   }
                   return resp;
               });
}

// JM-side route: fan out over every TM hosting the job, merge into one
// canonical stream. 404 when the job resolves to no HTTP-exposing TM.
inline void register_jm_state_export_route(http::HttpServer& server, cluster::JobManager& jm) {
    server.get("/api/v1/state/export/job/:job_id",
               [&jm](const http::HttpRequest& req) -> http::HttpResponse {
                   http::HttpResponse resp;
                   auto job_it = req.path_params.find("job_id");
                   if (job_it == req.path_params.end()) {
                       resp.status = 400;
                       resp.body = "{\"error\":\"missing job_id\"}";
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
                   const auto tms = jm.tms_hosting_job(job);
                   if (tms.empty()) {
                       resp.status = 404;
                       resp.body = "{\"error\":\"job not found or no hosting TM exposes HTTP\"}";
                       return resp;
                   }
                   std::vector<std::vector<std::byte>> parts;
                   for (const auto& [host, port] : tms) {
                       http::HttpClient client(host, port);
                       auto tm_resp = client.get("/api/v1/state/export/job/" + std::to_string(job));
                       if (tm_resp.status != 200) {
                           continue;  // TM hosts no backends for the job (or is gone)
                       }
                       std::vector<std::byte> part(tm_resp.body.size());
                       if (!tm_resp.body.empty()) {
                           std::memcpy(part.data(), tm_resp.body.data(), tm_resp.body.size());
                       }
                       parts.push_back(std::move(part));
                   }
                   if (parts.empty()) {
                       resp.status = 404;
                       resp.body = "{\"error\":\"no TM returned state for this job\"}";
                       return resp;
                   }
                   // NOTE: the HTTP client surfaces no response headers, so a
                   // TM-level X-Clink-Skipped-Subtasks count does not propagate
                   // through this merged response; query a TM directly to see it.
                   const auto merged = InMemoryStateBackend::merge_snapshot_bytes(parts);
                   resp.body = live_export_detail::body_from_bytes(merged);
                   resp.content_type = "application/vnd.apache.arrow.stream";
                   return resp;
               });
}

}  // namespace clink::queryable_state
