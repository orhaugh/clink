// HTTP connector factory registration.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/http_connector/batched_http_bulk_sink.hpp"
#include "clink/http_connector/bulk_sink_builders.hpp"
#include "clink/http_connector/http_poll_source.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/http_connector/install.hpp"
#include "clink/http_connector/prometheus_push_sink.hpp"
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

    // splunk_hec_sink: POST batched JSON events to a Splunk HTTP Event Collector
    // (/services/collector/event) with `Authorization: Splunk <token>`. Each row
    // is wrapped in the HEC event envelope. At-least-once. Params:
    //   url (required)        - scheme://host[:port], e.g. https://splunk:8088
    //   token (required)      - HEC token (sets the Authorization header)
    //   path (default "/services/collector/event")
    //   sourcetype            - e.g. "_json" to auto-extract the event fields
    //   source, host, index   - optional event metadata (index must be writable)
    //   headers               - extra "K: V; ..." (rarely needed)
    //   batch_records (default 500), batch_bytes (default 4194304)
    //   max_retries (default 4; clamped to [0, 20])
    //   verify_tls ("true" [default] | "false")
    reg.register_sink<std::string>(
        "splunk_hec_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            SplunkHecOptions o;
            o.url = ctx.param_or("url");
            o.token = ctx.param_or("token", "");
            o.path = ctx.param_or("path", "/services/collector/event");
            o.sourcetype = ctx.param_or("sourcetype", "");
            o.source = ctx.param_or("source", "");
            o.host = ctx.param_or("host", "");
            o.index = ctx.param_or("index", "");
            o.headers = ctx.param_or("headers", "");
            o.verify_tls = ctx.param_or("verify_tls", "true") != "false";
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 500));
            o.batch_bytes =
                static_cast<std::size_t>(ctx.param_int64_or("batch_bytes", 4 * 1024 * 1024));
            o.max_retries = static_cast<int>(ctx.param_int64_or("max_retries", 4));
            o.name = "splunk_hec_sink";
            return make_splunk_hec_sink(std::move(o));
        });

    // prometheus_sink: push batched records as gauge samples to a Prometheus
    // Pushgateway (POST /metrics/job/<job>{/<label>/<value>}). One gauge metric
    // name per sink; each row's value_field is the float and the other scalar
    // fields become labels. Dedup is last-write-wins per label set (the gateway
    // rejects duplicate series in one push). At-least-once. Params:
    //   url (required)        - pushgateway base, e.g. http://pushgateway:9091
    //   job (required)        - grouping job (first path segment)
    //   grouping              - extra static path labels "k=v,k2=v2"
    //   metric_name (default "clink_value")
    //   value_field (default "value") - the record field holding the gauge value
    //   help                  - optional # HELP text
    //   headers               - optional "K: V; ..." (fronting-proxy auth)
    //   batch_records (default 500; max DISTINCT series per push)
    //   max_retries (default 4; clamped to [0, 20])
    //   verify_tls ("true" [default] | "false")
    reg.register_sink<std::string>(
        "prometheus_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            PromPushOptions o;
            o.url = ctx.param_or("url");
            o.job = ctx.param_or("job", "");
            o.grouping = ctx.param_or("grouping", "");
            o.metric_name = ctx.param_or("metric_name", "clink_value");
            o.value_field = ctx.param_or("value_field", "value");
            o.help = ctx.param_or("help", "");
            o.headers = ctx.param_or("headers", "");
            o.verify_tls = ctx.param_or("verify_tls", "true") != "false";
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 500));
            o.max_retries = static_cast<int>(ctx.param_int64_or("max_retries", 4));
            o.name = "prometheus_sink";
            return make_prometheus_pushgateway_sink(std::move(o));
        });

    // http_poll_source: GET a JSON REST endpoint on an interval, emit the array
    // elements, cursor on a record field. At-least-once (the cursor is
    // checkpointed). Params:
    //   url (required)        - scheme://host[:port]
    //   path (default "/")    - request path (may carry a fixed query string)
    //   headers               - "K: V; ..." (auth)
    //   cursor_param          - query-param name to send the cursor as
    //   cursor_field          - record field holding the next cursor (API must
    //                           return records ascending by it)
    //   records_field         - response field holding the array (else the
    //                           response itself is the array)
    //   initial_cursor        - starting cursor on a fresh run
    //   poll_interval_ms (default 1000)
    //   max_retries (default 4; clamped to [0, 20]) - transient-GET retries
    //   verify_tls ("true" [default] | "false")
    reg.register_source<std::string>(
        "http_poll_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            HttpPollOptions o;
            o.url = ctx.param_or("url");
            o.path = ctx.param_or("path", "/");
            o.headers = ctx.param_or("headers", "");
            o.verify_tls = ctx.param_or("verify_tls", "true") != "false";
            o.cursor_param = ctx.param_or("cursor_param", "");
            o.cursor_field = ctx.param_or("cursor_field", "");
            o.records_field = ctx.param_or("records_field", "");
            o.initial_cursor = ctx.param_or("initial_cursor", "");
            o.interval = std::chrono::milliseconds{ctx.param_int64_or("poll_interval_ms", 1000)};
            o.max_retries = static_cast<int>(ctx.param_int64_or("max_retries", 4));
            o.name = "http_poll_source";
            return make_http_poll_source(std::move(o));
        });
}

}  // namespace clink::http_connector
