// HTTP connector factory registration.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <map>
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
#include "clink/http_connector/pubsub.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::http_connector {

namespace {
// dlq="drop" routes a permanently-failed (4xx) batch to the dead-letter path
// (count + metric) instead of failing the job; anything else keeps the
// fail-and-replay default.
DlqPolicy parse_dlq(const clink::plugin::BuildContext& ctx) {
    return ctx.param_or("dlq", "fail") == "drop" ? DlqPolicy::Drop : DlqPolicy::Fail;
}

// Build the HTTP transport options shared by the Pub/Sub sink and source. The
// base URL resolves in priority order: the `endpoint` option, else the
// PUBSUB_EMULATOR_HOST env var ("host:port" -> http://host:port), else the real
// service. Auth: `auth_token` -> "Authorization: Bearer <token>"; arbitrary
// `headers` are merged too (and a header-supplied Authorization wins). The
// emulator needs no auth.
HttpRequest::Options pubsub_http_options(const clink::plugin::BuildContext& ctx) {
    HttpRequest::Options http;
    std::string endpoint = ctx.param_or("endpoint", "");
    if (endpoint.empty()) {
        if (const char* env = std::getenv("PUBSUB_EMULATOR_HOST"); env != nullptr && *env != '\0') {
            endpoint = std::string("http://") + env;
        }
    }
    http.base_url = endpoint.empty() ? "https://pubsub.googleapis.com" : endpoint;
    http.verify_tls = ctx.param_or("verify_tls", "true") != "false";
    std::map<std::string, std::string> headers;
    if (const std::string token = ctx.param_or("auth_token", ""); !token.empty()) {
        headers["Authorization"] = "Bearer " + token;
    }
    for (auto& kv : parse_headers(ctx.param_or("headers", ""))) {
        headers[kv.first] = kv.second;  // explicit headers override the token-built one
    }
    http.headers = std::move(headers);
    return http;
}
}  // namespace

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
            opts.dlq_policy = parse_dlq(ctx);
            opts.max_age = std::chrono::milliseconds{ctx.param_int64_or("linger_ms", 0)};
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
            o.dlq_policy = parse_dlq(ctx);
            o.max_age = std::chrono::milliseconds{ctx.param_int64_or("linger_ms", 0)};
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
            o.dlq_policy = parse_dlq(ctx);
            o.max_age = std::chrono::milliseconds{ctx.param_int64_or("linger_ms", 0)};
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
            o.max_age = std::chrono::milliseconds{ctx.param_int64_or("linger_ms", 0)};
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
            o.jitter_frac = std::stod(ctx.param_or("jitter_frac", "0"));
            o.bounded = ctx.param_or("bounded", "") == "true";  // one-shot poll
            o.max_retries = static_cast<int>(ctx.param_int64_or("max_retries", 4));
            o.retry_base_backoff =
                std::chrono::milliseconds{ctx.param_int64_or("retry_base_backoff_ms", 200)};
            o.name = "http_poll_source";
            return make_http_poll_source(std::move(o));
        });

    // pubsub_sink: publish batched records to a Google Cloud Pub/Sub topic via the
    // REST :publish API ({"messages":[{"data":"<base64>"}]}). At-least-once;
    // Pub/Sub publish has no producer dedup key so a replay re-publishes. Each
    // record is one std::string (a JSON object on the SQL path). Params:
    //   project (required), topic (required)
    //   endpoint              - override base URL (e.g. http://emulator:8085).
    //                           Else PUBSUB_EMULATOR_HOST, else the real service.
    //   auth_token            - bearer token -> "Authorization: Bearer <token>"
    //   headers               - extra "K: V; ..." (merged; overrides auth_token)
    //   batch_records (default 1000; capped at the 1000-message publish limit)
    //   batch_bytes (default 9437184; stay under the ~10MB request limit)
    //   max_retries (default 4; clamped to [0, 20])
    //   verify_tls ("true" [default] | "false")
    reg.register_sink<std::string>(
        "pubsub_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            PubSubSinkOptions o;
            o.http = pubsub_http_options(ctx);
            o.project = ctx.param_or("project");
            o.topic = ctx.param_or("topic");
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 1000));
            o.batch_bytes =
                static_cast<std::size_t>(ctx.param_int64_or("batch_bytes", 9 * 1024 * 1024));
            o.max_retries = static_cast<int>(ctx.param_int64_or("max_retries", 4));
            o.dlq_policy = parse_dlq(ctx);
            o.linger = std::chrono::milliseconds{ctx.param_int64_or("linger_ms", 0)};
            o.name = "pubsub_sink";
            return make_pubsub_publish_sink(std::move(o));
        });

    // pubsub_source: pull from a Google Cloud Pub/Sub subscription via the REST
    // :pull API, emit each message's base64-decoded data, and :acknowledge on
    // each checkpoint (the ack is the offset commit). At-least-once: unacked
    // messages are redelivered by the server after the subscription ackDeadline,
    // which is also crash recovery. Multiple subtasks pull the SAME subscription;
    // Pub/Sub load-balances messages across them (no client-side sharding).
    // Params:
    //   project (required), subscription (required)
    //   endpoint / auth_token / headers / verify_tls - as pubsub_sink
    //   max_messages (default 1000; capped at 1000) - Pull maxMessages
    //   return_immediately ("true" [default] | "false")
    //   poll_interval_ms (default 500) - sleep after an empty pull
    //   max_retries (default 4; clamped to [0, 20]) - transient-pull retries
    reg.register_source<std::string>(
        "pubsub_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            PubSubSourceOptions o;
            o.http = pubsub_http_options(ctx);
            o.project = ctx.param_or("project");
            o.subscription = ctx.param_or("subscription");
            o.max_messages = static_cast<int>(ctx.param_int64_or("max_messages", 1000));
            o.return_immediately = ctx.param_or("return_immediately", "true") != "false";
            o.poll_interval =
                std::chrono::milliseconds{ctx.param_int64_or("poll_interval_ms", 500)};
            o.max_retries = static_cast<int>(ctx.param_int64_or("max_retries", 4));
            o.retry_base_backoff =
                std::chrono::milliseconds{ctx.param_int64_or("retry_base_backoff_ms", 200)};
            o.name = "pubsub_source";
            return std::make_shared<PubSubPullSource>(std::move(o));
        });
}

}  // namespace clink::http_connector
