#pragma once

// Arrow bulk reads for queryable-state scans: turn scanned (key, value
// JSON document) entries into one Arrow IPC stream, so a state scan is
// directly readable by anything that speaks Arrow (pyarrow, polars,
// duckdb) with no clink client code. The stream rides plain HTTP - this
// is deliberately NOT Arrow Flight, so serving state adds no gRPC
// dependency.
//
// Schema: `__key` (utf8) followed by the value documents' fields in the
// first entry's field order. Field kinds are inferred from the first
// object document: number -> float64 (JSON numbers are doubles, so a
// BIGINT total arrives as float64), bool -> bool, string -> utf8,
// anything else -> utf8 carrying the serialised JSON. A field that is
// missing - or of a different kind - in a later row becomes null.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "clink/config/json.hpp"
#include "clink/queryable_state/jm_routes.hpp"
#include "clink/queryable_state/registry.hpp"
#include "clink/queryable_state/server.hpp"

namespace clink::queryable_state {

// The IPC stream's media type, set on the serving routes.
inline constexpr const char* kArrowStreamContentType = "application/vnd.apache.arrow.stream";

inline std::optional<std::string> arrow_ipc_from_scan(
    const std::vector<std::pair<std::string, std::string>>& entries, std::string* error) {
    auto fail = [&](std::string msg) -> std::optional<std::string> {
        if (error != nullptr) {
            *error = std::move(msg);
        }
        return std::nullopt;
    };
    // Parse every value document once, up front.
    std::vector<config::JsonValue> docs;
    docs.reserve(entries.size());
    for (const auto& [key, value_json] : entries) {
        try {
            docs.push_back(config::parse(value_json));
        } catch (const std::exception& e) {
            return fail("unparseable value document: " + std::string{e.what()});
        }
    }
    // Infer the schema from the first object document (deterministic: the
    // document's own field order).
    enum class Kind : std::uint8_t { Number, Boolean, Text };
    std::vector<std::pair<std::string, Kind>> fields;
    for (const auto& doc : docs) {
        if (!doc.is_object()) {
            continue;
        }
        for (const auto& [name, v] : doc.as_object()) {
            fields.emplace_back(name,
                                v.is_number() ? Kind::Number
                                : v.is_bool() ? Kind::Boolean
                                              : Kind::Text);
        }
        break;
    }
    arrow::FieldVector schema_fields;
    schema_fields.push_back(arrow::field("__key", arrow::utf8()));
    for (const auto& [name, kind] : fields) {
        schema_fields.push_back(arrow::field(name,
                                             kind == Kind::Number    ? arrow::float64()
                                             : kind == Kind::Boolean ? arrow::boolean()
                                                                     : arrow::utf8()));
    }
    auto schema = arrow::schema(std::move(schema_fields));

    arrow::StringBuilder key_builder;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    builders.reserve(fields.size());
    for (const auto& [name, kind] : fields) {
        switch (kind) {
            case Kind::Number:
                builders.push_back(std::make_unique<arrow::DoubleBuilder>());
                break;
            case Kind::Boolean:
                builders.push_back(std::make_unique<arrow::BooleanBuilder>());
                break;
            case Kind::Text:
                builders.push_back(std::make_unique<arrow::StringBuilder>());
                break;
        }
    }
    for (std::size_t row = 0; row < entries.size(); ++row) {
        if (!key_builder.Append(entries[row].first).ok()) {
            return fail("append failed for __key");
        }
        const auto& doc = docs[row];
        for (std::size_t f = 0; f < fields.size(); ++f) {
            const auto& [name, kind] = fields[f];
            const config::JsonValue* v = nullptr;
            if (doc.is_object()) {
                const auto& obj = doc.as_object();
                if (auto it = obj.find(name); it != obj.end()) {
                    v = &it->second;
                }
            }
            arrow::Status st;
            switch (kind) {
                case Kind::Number: {
                    auto& b = static_cast<arrow::DoubleBuilder&>(*builders[f]);
                    st = (v != nullptr && v->is_number()) ? b.Append(v->as_number())
                                                          : b.AppendNull();
                    break;
                }
                case Kind::Boolean: {
                    auto& b = static_cast<arrow::BooleanBuilder&>(*builders[f]);
                    st = (v != nullptr && v->is_bool()) ? b.Append(v->as_bool()) : b.AppendNull();
                    break;
                }
                case Kind::Text: {
                    auto& b = static_cast<arrow::StringBuilder&>(*builders[f]);
                    if (v == nullptr || v->is_null()) {
                        st = b.AppendNull();
                    } else if (v->is_string()) {
                        st = b.Append(v->as_string());
                    } else {
                        st = b.Append(v->serialize(0));
                    }
                    break;
                }
            }
            if (!st.ok()) {
                return fail("append failed for field '" + name + "': " + st.ToString());
            }
        }
    }
    arrow::ArrayVector arrays;
    arrays.reserve(fields.size() + 1);
    {
        std::shared_ptr<arrow::Array> key_array;
        if (!key_builder.Finish(&key_array).ok()) {
            return fail("finish failed for __key");
        }
        arrays.push_back(std::move(key_array));
    }
    for (auto& b : builders) {
        std::shared_ptr<arrow::Array> arr;
        if (!b->Finish(&arr).ok()) {
            return fail("builder finish failed");
        }
        arrays.push_back(std::move(arr));
    }
    auto batch =
        arrow::RecordBatch::Make(schema, static_cast<std::int64_t>(entries.size()), arrays);

    auto sink_r = arrow::io::BufferOutputStream::Create();
    if (!sink_r.ok()) {
        return fail("BufferOutputStream: " + sink_r.status().ToString());
    }
    auto sink = *sink_r;
    auto writer_r = arrow::ipc::MakeStreamWriter(sink, schema);
    if (!writer_r.ok()) {
        return fail("MakeStreamWriter: " + writer_r.status().ToString());
    }
    if (auto st = (*writer_r)->WriteRecordBatch(*batch); !st.ok()) {
        return fail("WriteRecordBatch: " + st.ToString());
    }
    if (auto st = (*writer_r)->Close(); !st.ok()) {
        return fail("writer Close: " + st.ToString());
    }
    auto buf_r = sink->Finish();
    if (!buf_r.ok()) {
        return fail("Finish: " + buf_r.status().ToString());
    }
    return (*buf_r)->ToString();
}

// TM-side Arrow scan route (subtask-scoped):
//   GET .../op/:role/subtask/:n/json/:slot/scan.arrow?limit=N
// Body = one Arrow IPC stream of the scan (schema above); truncation is
// signalled via the X-Clink-Truncated header since the body is binary.
// Kept in this header (not server.hpp) so only Arrow-serving binaries
// pay for the Arrow includes.
inline void register_arrow_scan_route(http::HttpServer& server, Registry& registry) {
    server.get("/api/v1/queryable_state/op/:role/subtask/:subtask/json/:slot/scan.arrow",
               [&registry](const http::HttpRequest& req) -> http::HttpResponse {
                   http::HttpResponse resp;
                   auto role_it = req.path_params.find("role");
                   auto sub_it = req.path_params.find("subtask");
                   auto slot_it = req.path_params.find("slot");
                   if (role_it == req.path_params.end() || sub_it == req.path_params.end() ||
                       slot_it == req.path_params.end()) {
                       resp.status = 400;
                       resp.body = "{\"error\":\"missing role / subtask / slot\"}";
                       return resp;
                   }
                   std::uint32_t subtask_idx = 0;
                   try {
                       subtask_idx = static_cast<std::uint32_t>(std::stoul(sub_it->second));
                   } catch (...) {
                       resp.status = 400;
                       resp.body = "{\"error\":\"malformed subtask\"}";
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
                   const std::string composed =
                       compose_subtask_slot(role_it->second, subtask_idx, slot_it->second);
                   auto result = registry.scan_json(composed, limit);
                   if (!result.has_value()) {
                       resp.status = 404;
                       resp.body = "{\"error\":\"slot not registered\"}";
                       return resp;
                   }
                   std::string err;
                   auto ipc = arrow_ipc_from_scan(result->entries, &err);
                   if (!ipc.has_value()) {
                       resp.status = 500;
                       resp.body = "{\"error\":" + detail::json_escape(err) + "}";
                       return resp;
                   }
                   resp.content_type = kArrowStreamContentType;
                   resp.headers["X-Clink-Truncated"] = result->truncated ? "true" : "false";
                   resp.body = std::move(*ipc);
                   return resp;
               });
}

// JM-side Arrow scan route - the bulk state-as-DataFrame read:
//   GET .../job/:job_id/op/:role/json/:slot/scan.arrow?limit=N
// Fans out across the role's subtasks (the same merge the JSON scan
// uses) and returns ONE Arrow IPC stream, directly readable by pyarrow /
// polars / duckdb from the HTTP response body.
inline void register_jm_arrow_scan_route(http::HttpServer& server, cluster::JobManager& jm) {
    server.get("/api/v1/queryable_state/job/:job_id/op/:role/json/:slot/scan.arrow",
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
                   const auto scan =
                       detail::fan_out_scan(jm, job, role_it->second, slot_it->second, limit);
                   if (!scan.any_target) {
                       resp.status = 404;
                       resp.body =
                           "{\"error\":\"job or role not found (or no hosting TM "
                           "exposes HTTP)\"}";
                       return resp;
                   }
                   if (!scan.any_slot) {
                       resp.status = 404;
                       resp.body = "{\"error\":\"slot not registered on any subtask\"}";
                       return resp;
                   }
                   std::string err;
                   auto ipc = arrow_ipc_from_scan(scan.entries, &err);
                   if (!ipc.has_value()) {
                       resp.status = 500;
                       resp.body = "{\"error\":" + detail::json_escape(err) + "}";
                       return resp;
                   }
                   resp.content_type = kArrowStreamContentType;
                   resp.headers["X-Clink-Truncated"] = scan.truncated ? "true" : "false";
                   resp.body = std::move(*ipc);
                   return resp;
               });
}

}  // namespace clink::queryable_state
