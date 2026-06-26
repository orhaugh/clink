// HTTP connector factory registration.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/http_connector/batched_http_bulk_sink.hpp"
#include "clink/http_connector/bulk_sink_builders.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/http_connector/install.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::http_connector {

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // http_sink: POST batched records to an HTTP(S) endpoint (a webhook or any
    // bulk-ingest API). Each record is one std::string (a JSON object on the
    // SQL path, produced by row_to_json_string). Batches flush on
    // batch_records / batch_bytes, every checkpoint barrier (at-least-once),
    // and at end-of-stream. Params:
    //   url (required)      - scheme://host[:port], e.g. http://collector:8080
    //   path (default "/")  - request path
    //   bulk_format ("json_array" [default] | "ndjson") - request body framing.
    //     NOT "format": on the SQL path "format" is the channel selector (a
    //     connector='http' table must declare format='json' to reach the Row
    //     channel that bridges to this string sink), so the framing uses a
    //     distinct key.
    //   content_type (default "application/json")
    //   headers             - "K1: V1; K2: V2" (e.g. "Authorization: Bearer xyz")
    //   batch_records (default 500), batch_bytes (default 4194304)
    //   max_retries (default 4; clamped to [0, 20])
    //   verify_tls ("true" [default] | "false")  - https server-cert verification
    reg.register_sink<std::string>(
        "http_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            BatchedHttpBulkSink<std::string>::Options opts;
            opts.http.base_url = ctx.param_or("url");
            if (opts.http.base_url.empty()) {
                throw std::runtime_error("http_sink: 'url' is required");
            }
            opts.http.headers = parse_headers(ctx.param_or("headers", ""));
            opts.http.verify_tls = ctx.param_or("verify_tls", "true") != "false";
            opts.path = ctx.param_or("path", "/");
            opts.content_type = ctx.param_or("content_type", "application/json");
            const auto fmt = ctx.param_or("bulk_format", "json_array");
            opts.framing =
                (fmt == "ndjson" || fmt == "NDJSON") ? BulkFraming::Ndjson : BulkFraming::JsonArray;
            opts.max_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 500));
            opts.max_bytes =
                static_cast<std::size_t>(ctx.param_int64_or("batch_bytes", 4 * 1024 * 1024));
            opts.max_retries = static_cast<int>(ctx.param_int64_or("max_retries", 4));
            opts.name = "http_sink";

            // The record is already a serialized JSON object string (SQL path)
            // or whatever the upstream string channel carries; append verbatim.
            auto render = [](std::string& out, const std::string& rec) { out += rec; };
            return std::make_shared<BatchedHttpBulkSink<std::string>>(std::move(opts),
                                                                      std::move(render));
        });

    // elasticsearch_sink / opensearch_sink: bulk-index JSON-object records into
    // Elasticsearch / OpenSearch (identical _bulk API) via the NDJSON action+doc
    // protocol. At-least-once; idempotent (effectively-once on replay) when
    // document_id is set. Params:
    //   url (required)        - scheme://host[:port], e.g. https://es:9200
    //   index (required)      - target index for the _index action metadata
    //   document_id           - record field name to use as the _id (optional;
    //                           idempotent overwrite on re-delivery)
    //   path (default "/_bulk")
    //   headers               - "K: V; ..." (e.g. "Authorization: ApiKey abc")
    //   batch_records (default 500), batch_bytes (default 4194304)
    //   max_retries (default 4; clamped to [0, 20])
    //   verify_tls ("true" [default] | "false")
    auto es_factory = [](const std::string& name) {
        return [name](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            EsBulkOptions o;
            o.url = ctx.param_or("url");
            o.index = ctx.param_or("index");
            o.document_id = ctx.param_or("document_id", "");
            o.headers = ctx.param_or("headers", "");
            o.path = ctx.param_or("path", "/_bulk");
            o.verify_tls = ctx.param_or("verify_tls", "true") != "false";
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 500));
            o.batch_bytes =
                static_cast<std::size_t>(ctx.param_int64_or("batch_bytes", 4 * 1024 * 1024));
            o.max_retries = static_cast<int>(ctx.param_int64_or("max_retries", 4));
            o.name = name;
            return make_es_bulk_sink(std::move(o));
        };
    };
    reg.register_sink<std::string>("elasticsearch_sink", es_factory("elasticsearch_sink"));
    reg.register_sink<std::string>("opensearch_sink", es_factory("opensearch_sink"));
}

}  // namespace clink::http_connector
