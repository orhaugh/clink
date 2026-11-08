// HTTP connector factory registration.

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "clink/connectors/capability.hpp"
#include "clink/http_connector/batched_http_bulk_sink.hpp"
#include "clink/http_connector/bulk_sink_builders.hpp"
#include "clink/http_connector/http_model_provider.hpp"
#include "clink/http_connector/http_poll_source.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/http_connector/influxdb_sink.hpp"
#include "clink/http_connector/install.hpp"
#include "clink/http_connector/prometheus_push_sink.hpp"
#include "clink/http_connector/pubsub.hpp"
#include "clink/http_connector/splunk_hec_sink.hpp"
#include "clink/http_connector/token_source.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
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
    // `auth_token_file` (refreshing) takes precedence over a static `auth_token`: the file is
    // re-read per request when it changes, so an external refresher rotating the token is picked
    // up without a restart. With a provider set, do not also add a static Authorization header.
    if (const std::string token_file = ctx.param_or("auth_token_file", ""); !token_file.empty()) {
        http.auth_token_provider = make_file_token_provider(token_file);
    } else if (const std::string token = ctx.param_or("auth_token", ""); !token.empty()) {
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
    // Capability records. One per user-visible connector: these share an
    // HTTP client, not semantics.
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "http",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"cpp-httplib"},
        .runtime_dependencies = {"http endpoint"},
        .formats = {"text", "json", "bytes"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        // The sink posts with retries and flushes at every barrier
        // (at-least-once). The polling source has a record of its own,
        // http_poll, since what it can replay depends on how it is set up.
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"none", "bearer-token", "headers"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {},
        .available_in_sql = true,
        .limitations = {"the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "http_poll",
        .version = "2",
        .is_source = true,
        .is_sink = false,
        .build_dependencies = {"cpp-httplib"},
        .runtime_dependencies = {"http endpoint"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Either,
        // In cursor mode the checkpoint carries the cursor and a restore polls
        // again from it, so the source replays from a recorded position while
        // the endpoint answers strictly after a cursor and keeps what it
        // served: the endpoint's property, not the source's, hence the
        // limitations. In resource mode it keeps no position at all, so it
        // replays only with the cursor options. The record before this one
        // declared the source replayable however it was set up.
        .replayable = true,
        .offset_model = clink::connectors::OffsetModel::OpaqueToken,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"none", "headers"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"connect_timeout_ms", "request_timeout_ms", "retry_timeout_ms"},
        .available_in_sql = true,
        .limitations = {"replays only in cursor mode, and only while the endpoint answers the "
                        "records strictly after the cursor, ascending, each cursor value unique, "
                        "and keeps what it served; a page that breaks what the source can check "
                        "of that fails the poll",
                        "resource mode replays nothing: a restart reads the resource as it is "
                        "then",
                        "one subtask polls, whatever the parallelism",
                        "pagination by page token, offset or a cursor made of several fields is "
                        "not supported"},
        .replay_requires_options = {"cursor_field", "cursor_param"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "elasticsearch",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"cpp-httplib"},
        .runtime_dependencies = {"elasticsearch/opensearch _bulk API"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        // Bulk index with _id from document_id: replays overwrite.
        .idempotency_key_option = "document_id",
        .auth_methods = {"none", "basic", "bearer-token", "api-key"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {},
        .available_in_sql = true,
        .limitations = {"without document_id a resend or a replay indexes duplicate documents",
                        "with document_id a replay rewrites each id from the checkpoint on, so a "
                        "document can show an older record until the replay reaches its latest; "
                        "records of one id are ordered only within one sink subtask, and not "
                        "against other writers",
                        "a batch counts as written only when the bulk response confirms every "
                        "operation; anything else is resent and then fails the job",
                        "document_id takes a non-empty string of at most 512 bytes, an integer, a "
                        "number exact to six decimal places or a boolean; any other value is "
                        "refused",
                        "the sink writes to one URL: in front of a cluster, a load balancer or a "
                        "node that stays up; while it is down, flushes fail and the job replays",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "opensearch",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"cpp-httplib"},
        .runtime_dependencies = {"elasticsearch/opensearch _bulk API"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        // Bulk index with _id from document_id: replays overwrite.
        .idempotency_key_option = "document_id",
        .auth_methods = {"none", "basic", "bearer-token", "api-key"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {},
        .available_in_sql = true,
        .limitations = {"without document_id a resend or a replay indexes duplicate documents",
                        "with document_id a replay rewrites each id from the checkpoint on, so a "
                        "document can show an older record until the replay reaches its latest; "
                        "records of one id are ordered only within one sink subtask, and not "
                        "against other writers",
                        "a batch counts as written only when the bulk response confirms every "
                        "operation; anything else is resent and then fails the job",
                        "document_id takes a non-empty string of at most 512 bytes, an integer, a "
                        "number exact to six decimal places or a boolean; any other value is "
                        "refused",
                        "the sink writes to one URL: in front of a cluster, a load balancer or a "
                        "node that stays up; while it is down, flushes fail and the job replays",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "influxdb",
        .version = "2",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"cpp-httplib"},
        .runtime_dependencies = {"influxdb v2 write API"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        // A replayed point with its timestamp lands on the same series and time.
        .idempotency_key_option = "timestamp_field",
        .auth_methods = {"token"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"connect_timeout_ms", "request_timeout_ms"},
        .available_in_sql = true,
        .limitations =
            {"at-least-once: a replay rewrites the points after the last checkpoint",
             "without timestamp_field the server stamps each write, so a replay adds points",
             "with timestamp_field a replayed point merges into the point of its series and "
             "time: fields it carries win, fields it lacks keep their earlier values",
             "points of one series written by two sink subtasks or another writer can end at "
             "an older value",
             "a partial write (HTTP 422) does not say which points were dropped: dlq=fail fails "
             "the sink, dlq=drop dead-letters the request as one report",
             "the sink must be the only sink on its chain: Dag::add_sink refuses another sink "
             "beside it"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "prometheus",
        .version = "2",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"cpp-httplib"},
        .runtime_dependencies = {"prometheus pushgateway"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        // A push replaces the group's series of the metric, so a replay ends at
        // each series' latest value rather than duplicating; the gateway keeps
        // values, not samples.
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"none", "basic"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"connect_timeout_ms", "request_timeout_ms"},
        .available_in_sql = true,
        .limitations =
            {"a push replaces every series of the metric in the group with the series it "
             "carries: with push_series='batch' (the default) those of the records since the "
             "previous push, so a series no record named since then disappears; with "
             "push_series='all' every series the sink holds, up to max_series",
             "the gateway keeps each series' latest value, not every sample: a scrape sees the "
             "values present when it runs, and a replay can show an older value until it "
             "reaches the latest",
             "the sink must be its group's only writer of its metric and runs on one subtask; "
             "another writer's push replaces the sink's series, and the sink's replaces theirs",
             "with push_series='all' a series stays in the gateway until the group is deleted, "
             "and a run restored from a checkpoint adopts the series the group holds",
             "label names must be Prometheus label names and are refused, not rewritten; an "
             "integer value must be within 2^53",
             "the sink must be the only sink on its chain: Dag::add_sink refuses another sink "
             "beside it"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "splunk_hec",
        // 2: each record must be one JSON object, checked and written exactly;
        // the reply is read as HEC's; ack='indexer' waits for indexer
        // acknowledgement at every barrier.
        .version = "2",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"cpp-httplib"},
        .runtime_dependencies = {"splunk HTTP event collector"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"hec-token"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"connect_timeout_ms", "request_timeout_ms", "ack_timeout_ms"},
        .available_in_sql = true,
        .limitations = {"no dedup key: an event HEC accepted is indexed again when it is sent "
                        "again, after a lost reply, a busy server or a replay",
                        "a request HEC refuses may have been partly accepted; the sink sends "
                        "every event but a refused one again",
                        "with ack='none' a barrier waits for HEC's acceptance, not for indexing; "
                        "ack='indexer' waits for indexer acknowledgement, which is not "
                        "searchability",
                        "each record must be one JSON object; raw events, per-event time and "
                        "indexed fields are not written",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "pubsub",
        .version = "1",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {"cpp-httplib"},
        .runtime_dependencies = {"google cloud pub/sub (or emulator)"},
        .formats = {"text", "json", "bytes"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        // The subscription's ack state is the position: the source acknowledges
        // once the checkpoint that covers a message completes, and the server
        // delivers again whatever was not acknowledged.
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::Acknowledgement,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"bearer-token", "emulator-unauthenticated"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {},
        .available_in_sql = true,
        .limitations = {"publish has no dedup key: a replay re-publishes",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it",
                        "source messages a completed checkpoint covers but whose acknowledge had "
                        "not landed are delivered again after a crash (duplicates)",
                        "a source message held past max_lease is delivered again under a new "
                        "ackId (a duplicate)",
                        "a restore from a checkpoint older than one the source acknowledged "
                        "(commit-confirmed restore, a savepoint) does not see that "
                        "checkpoint's messages again",
                        "the source holds at most max_outstanding unacknowledged messages per "
                        "subtask; a job that completes no checkpoint stops pulling there",
                        "on_decode_error='drop' acknowledges and drops a message whose data is "
                        "not base64; the default 'fail' stops the subtask instead"},
    });

    using clink::plugin::BuildContext;

    // SQL-native AI: register the HTTP model-inference provider so ML_PREDICT with a
    // model declared WITH (provider='http', endpoint='...') can run against a REST
    // inference endpoint. Registered into the process-wide ModelProviderRegistry
    // (not the PluginRegistry) - the ml_predict_row operator looks it up by name.
    clink::sql::ModelProviderRegistry::global().register_provider(
        "http", [](const std::map<std::string, std::string>& opts) {
            return make_http_model_provider(opts);
        });

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
    // protocol. At-least-once; with document_id each record replaces its id's
    // document, so a replay converges (make_es_bulk_sink has the contract). Params:
    //   url (required)        - scheme://host[:port], e.g. https://es:9200
    //   index (required)      - target index for the _index action metadata
    //   document_id           - record field whose value names the _id (optional;
    //                           replacement by id; es_document_id has the rules)
    //   path (default "/_bulk")
    //   headers               - "K: V; ..." (e.g. "Authorization: ApiKey abc")
    //   batch_records (default 500), batch_bytes (default 4194304)
    //   max_retries (default 4; clamped to [0, 20])
    //   verify_tls ("true" [default] | "false")
    //   tls_ca_file           - PEM CAs for an https server's certificate (default:
    //                           the system's trust store)
    auto es_factory = [](const std::string& name) {
        return [name](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            EsBulkOptions o;
            o.url = ctx.param_or("url");
            o.index = ctx.param_or("index");
            o.document_id = ctx.param_or("document_id", "");
            o.headers = ctx.param_or("headers", "");
            o.path = ctx.param_or("path", "/_bulk");
            o.verify_tls = ctx.param_or("verify_tls", "true") != "false";
            o.tls_ca_file = ctx.param_or("tls_ca_file", "");
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

    // splunk_hec_sink: write each JSON-object row as the "event" of one HEC
    // envelope and POST them, one per line, to a Splunk HTTP Event Collector
    // with `Authorization: Splunk <token>`. At-least-once; with ack='indexer' a
    // barrier also waits for HEC's indexer acknowledgement of every request
    // before it (SplunkHecSink has the contract). Every option is parsed
    // strictly. Params:
    //   url (required)        - scheme://host[:port], e.g. https://splunk:8088
    //   token (required)      - HEC token; env://VAR keeps it out of the job spec
    //   path (default "/services/collector/event"; or .../event/1.0 or /services/collector)
    //   sourcetype, source, host, index - optional metadata for every event
    //   headers               - extra "K: V; ..." (never Authorization or the channel)
    //   batch_records (default 500; 1 to 100000) - the most events one request carries
    //   batch_bytes (default 4194304; 1024 to 268435456) - the most bytes one request carries
    //   max_retries (default 4; 0 to 20), linger_ms (default 0; up to 3600000)
    //   connect_timeout_ms (default 5000), request_timeout_ms (default 30000)
    //   ack ("none" [default] | "indexer")
    //   max_pending_acks (default 100; 1 to 10000), ack_poll_ms (default 1000; 10 to
    //   60000), ack_timeout_ms (default 120000; 1000 to 3600000) - ack='indexer' only
    //   dlq ("fail" [default] | "drop"), verify_tls ("true" [default] | "false")
    reg.register_sink<std::string>(
        "splunk_hec_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            const auto given = [&ctx](const char* key) { return ctx.params.contains(key); };
            const auto refuse = [](const std::string& why) {
                throw std::runtime_error("splunk_hec_sink: " + why);
            };
            // Read whole and within its range before it is narrowed: "12abc",
            // "abc" and "-1" are refused rather than read as 12, the default
            // or a wrapped size.
            const auto ranged =
                [&](const char* key, std::int64_t fallback, std::int64_t lo, std::int64_t hi) {
                    if (!given(key)) {
                        return fallback;
                    }
                    const std::string v = ctx.param_or(key);
                    std::int64_t out = 0;
                    const auto* end = v.data() + v.size();
                    const auto [ptr, ec] = std::from_chars(v.data(), end, out);
                    if (v.empty() || ec != std::errc{} || ptr != end) {
                        refuse(std::string{"'"} + key + "' must be an integer; it is '" + v + "'");
                    }
                    if (out < lo || out > hi) {
                        refuse(std::string{"'"} + key + "' must be from " + std::to_string(lo) +
                               " to " + std::to_string(hi) + "; it is " + v);
                    }
                    return out;
                };
            const auto one_of = [&](const char* key,
                                    const std::string& fallback,
                                    const std::vector<std::string>& allowed) {
                const std::string v = ctx.param_or(key, fallback);
                for (const auto& a : allowed) {
                    if (v == a) {
                        return v;
                    }
                }
                std::string list;
                for (const auto& a : allowed) {
                    list += (list.empty() ? "'" : ", '") + a + "'";
                }
                refuse(std::string{"'"} + key + "' must be one of " + list + "; it is '" + v + "'");
                return fallback;
            };
            SplunkHecOptions o;
            o.url = ctx.param_or("url");
            o.token = ctx.param_or("token", "");
            o.path = ctx.param_or("path", "/services/collector/event");
            o.sourcetype = ctx.param_or("sourcetype", "");
            o.source = ctx.param_or("source", "");
            o.host = ctx.param_or("host", "");
            o.index = ctx.param_or("index", "");
            o.headers = ctx.param_or("headers", "");
            o.verify_tls = one_of("verify_tls", "true", {"true", "false"}) == "true";
            o.batch_records = static_cast<std::size_t>(ranged("batch_records", 500, 1, 100000));
            o.batch_bytes =
                static_cast<std::size_t>(ranged("batch_bytes", 4 * 1024 * 1024, 1024, 268435456));
            o.max_retries = static_cast<int>(ranged("max_retries", 4, 0, 20));
            o.max_age = std::chrono::milliseconds{ranged("linger_ms", 0, 0, 3600000)};
            o.connect_timeout_ms = static_cast<int>(ranged("connect_timeout_ms", 5000, 1, 600000));
            o.request_timeout_ms = static_cast<int>(ranged("request_timeout_ms", 30000, 1, 600000));
            o.ack = one_of("ack", "none", {"none", "indexer"}) == "indexer" ? SplunkHecAck::Indexer
                                                                            : SplunkHecAck::None;
            if (o.ack == SplunkHecAck::None) {
                for (const char* key : {"max_pending_acks", "ack_poll_ms", "ack_timeout_ms"}) {
                    if (given(key)) {
                        refuse(std::string{"'"} + key +
                               "' applies to ack='indexer'; with ack='none' a request is done "
                               "once HEC accepts it");
                    }
                }
            }
            o.max_pending_acks =
                static_cast<std::size_t>(ranged("max_pending_acks", 100, 1, 10000));
            o.ack_poll_interval = std::chrono::milliseconds{ranged("ack_poll_ms", 1000, 10, 60000)};
            o.ack_timeout =
                std::chrono::milliseconds{ranged("ack_timeout_ms", 120000, 1000, 3600000)};
            o.dlq_policy = one_of("dlq", "fail", {"fail", "drop"}) == "drop" ? DlqPolicy::Drop
                                                                             : DlqPolicy::Fail;
            o.name = "splunk_hec_sink";
            return make_splunk_hec_sink(std::move(o));
        });

    // influxdb_sink: convert each JSON-object row to exactly one InfluxDB v2
    // line-protocol point and POST newline-delimited batches to /api/v2/write with
    // `Authorization: Token <token>`. At-least-once; with timestamp_field every
    // record carries its point's time, so a replay lands on the same points
    // (make_influxdb_sink has the contract). Every option is parsed strictly.
    // Params:
    //   url (required)        - scheme://host[:port], e.g. http://influxdb:8086
    //   org, bucket (required)- InfluxDB v2 organisation + bucket
    //   token (required)      - API token (sets the Authorization header)
    //   measurement (required)- line-protocol measurement name
    //   tags                  - CSV of row fields written as tags (each required per record)
    //   timestamp_field       - row field holding the point timestamp (else server-assigned)
    //   precision (default "ns") - ns|us|ms|s: the timestamps' unit
    //   number_fields (default "float") - "float" | "typed" (integers as integer fields)
    //   headers               - extra "K: V; ..." (rarely needed)
    //   connect_timeout_ms (default 5000), request_timeout_ms (default 30000)
    //   batch_records (default 500; 1 to 100000), batch_bytes (default 4194304)
    //   max_retries (default 4; 0 to 20), linger_ms (default 0)
    //   dlq ("fail" [default] | "drop"), verify_tls ("true" [default] | "false")
    reg.register_sink<std::string>(
        "influxdb_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            const auto given = [&ctx](const char* key) { return ctx.params.contains(key); };
            const auto refuse = [](const std::string& why) {
                throw std::runtime_error("influxdb_sink: " + why);
            };
            // Read whole: "12abc" or "abc" is refused rather than read as 12 or
            // as the default.
            const auto integer = [&](const char* key, std::int64_t fallback) {
                if (!given(key)) {
                    return fallback;
                }
                const std::string v = ctx.param_or(key);
                std::int64_t out = 0;
                const auto* end = v.data() + v.size();
                const auto [ptr, ec] = std::from_chars(v.data(), end, out);
                if (v.empty() || ec != std::errc{} || ptr != end) {
                    refuse(std::string{"'"} + key + "' must be an integer; it is '" + v + "'");
                }
                return out;
            };
            const auto one_of = [&](const char* key,
                                    const std::string& fallback,
                                    const std::vector<std::string>& allowed) {
                const std::string v = ctx.param_or(key, fallback);
                for (const auto& a : allowed) {
                    if (v == a) {
                        return v;
                    }
                }
                std::string list;
                for (const auto& a : allowed) {
                    list += (list.empty() ? "'" : ", '") + a + "'";
                }
                refuse(std::string{"'"} + key + "' must be one of " + list + "; it is '" + v + "'");
                return fallback;
            };
            InfluxDbOptions o;
            o.url = ctx.param_or("url");
            o.org = ctx.param_or("org", "");
            o.bucket = ctx.param_or("bucket", "");
            o.token = ctx.param_or("token", "");
            o.measurement = ctx.param_or("measurement", "");
            o.tag_keys = ctx.param_or("tags", "");
            o.timestamp_field = ctx.param_or("timestamp_field", "");
            o.precision = one_of("precision", "ns", {"ns", "us", "ms", "s"});
            o.number_fields = one_of("number_fields", "float", {"float", "typed"});
            o.headers = ctx.param_or("headers", "");
            o.verify_tls = one_of("verify_tls", "true", {"true", "false"}) == "true";
            o.connect_timeout_ms = static_cast<int>(integer("connect_timeout_ms", 5000));
            o.request_timeout_ms = static_cast<int>(integer("request_timeout_ms", 30000));
            o.batch_records = static_cast<std::size_t>(integer("batch_records", 500));
            o.batch_bytes = static_cast<std::size_t>(integer("batch_bytes", 4 * 1024 * 1024));
            o.max_retries = static_cast<int>(integer("max_retries", 4));
            o.dlq_policy = one_of("dlq", "fail", {"fail", "drop"}) == "drop" ? DlqPolicy::Drop
                                                                             : DlqPolicy::Fail;
            o.max_age = std::chrono::milliseconds{integer("linger_ms", 0)};
            o.name = "influxdb_sink";
            // Negative counts are refused before they are narrowed.
            for (const char* key : {"batch_records", "batch_bytes"}) {
                if (integer(key, 1) < 1) {
                    refuse(std::string{"'"} + key + "' must be at least 1");
                }
            }
            return make_influxdb_sink(std::move(o));
        });

    // prometheus_sink: push each JSON-object row as one gauge sample to a
    // Prometheus Pushgateway (POST /metrics/job/<job>{/<label>/<value>}). One
    // gauge metric and one group per sink, which must be the group's only
    // writer of the metric, so the sink runs on one subtask. push_series says
    // which series each push carries (PrometheusPushSink has the contract).
    // Every option is parsed strictly. Params:
    //   url (required)        - gateway base, e.g. http://pushgateway:9091
    //   job (required)        - the group's job label
    //   grouping              - the group's other labels "k=v,k2=v2"
    //   metric_name (default "clink_value"), value_field (default "value"), help
    //   push_series ("batch" [default] | "all")
    //   batch_records (default 500; 1 to 100000) - push_series='batch' only
    //   max_series (default 10000; 1 to 1000000) - push_series='all' only
    //   batch_bytes (default 4194304; 1024 to 268435456) - the most one push carries
    //   max_sample_bytes (default 65536, or half a smaller batch_bytes; 64 to batch_bytes)
    //   max_retries (default 4; 0 to 20), linger_ms (default 0; up to 3600000)
    //   connect_timeout_ms (default 5000), request_timeout_ms (default 30000)
    //   dlq ("fail" [default] | "drop"), headers, verify_tls ("true" [default] | "false")
    reg.register_sink<std::string>(
        "prometheus_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            const auto given = [&ctx](const char* key) { return ctx.params.contains(key); };
            const auto refuse = [](const std::string& why) {
                throw std::runtime_error("prometheus_sink: " + why);
            };
            // Read whole and within its range before it is narrowed: "12abc",
            // "abc" and "-1" are refused rather than read as 12, the default
            // or a wrapped size.
            const auto ranged =
                [&](const char* key, std::int64_t fallback, std::int64_t lo, std::int64_t hi) {
                    if (!given(key)) {
                        return fallback;
                    }
                    const std::string v = ctx.param_or(key);
                    std::int64_t out = 0;
                    const auto* end = v.data() + v.size();
                    const auto [ptr, ec] = std::from_chars(v.data(), end, out);
                    if (v.empty() || ec != std::errc{} || ptr != end) {
                        refuse(std::string{"'"} + key + "' must be an integer; it is '" + v + "'");
                    }
                    if (out < lo || out > hi) {
                        refuse(std::string{"'"} + key + "' must be from " + std::to_string(lo) +
                               " to " + std::to_string(hi) + "; it is " + v);
                    }
                    return out;
                };
            const auto one_of = [&](const char* key,
                                    const std::string& fallback,
                                    const std::vector<std::string>& allowed) {
                const std::string v = ctx.param_or(key, fallback);
                for (const auto& a : allowed) {
                    if (v == a) {
                        return v;
                    }
                }
                std::string list;
                for (const auto& a : allowed) {
                    list += (list.empty() ? "'" : ", '") + a + "'";
                }
                refuse(std::string{"'"} + key + "' must be one of " + list + "; it is '" + v + "'");
                return fallback;
            };
            if (ctx.parallelism > 1) {
                refuse(
                    "runs on one subtask, as its group's only writer of its metric: each "
                    "push replaces every series of the metric in the group, so " +
                    std::to_string(ctx.parallelism) +
                    " subtasks would replace one another's series; run it at parallelism 1");
            }
            PromPushOptions o;
            o.url = ctx.param_or("url");
            o.job = ctx.param_or("job", "");
            o.grouping = ctx.param_or("grouping", "");
            o.metric_name = ctx.param_or("metric_name", "clink_value");
            o.value_field = ctx.param_or("value_field", "value");
            o.help = ctx.param_or("help", "");
            o.headers = ctx.param_or("headers", "");
            o.verify_tls = one_of("verify_tls", "true", {"true", "false"}) == "true";
            o.push_series = one_of("push_series", "batch", {"batch", "all"}) == "all"
                                ? PromPushSeries::All
                                : PromPushSeries::Batch;
            if (o.push_series == PromPushSeries::All && given("batch_records")) {
                refuse(
                    "'batch_records' applies to push_series='batch'; with 'all' every push "
                    "carries every series the sink holds, bounded by 'max_series'");
            }
            if (o.push_series == PromPushSeries::Batch && given("max_series")) {
                refuse(
                    "'max_series' applies to push_series='all'; with 'batch' a push carries "
                    "at most 'batch_records' series");
            }
            o.batch_records = static_cast<std::size_t>(ranged("batch_records", 500, 1, 100000));
            o.max_series = static_cast<std::size_t>(ranged("max_series", 10000, 1, 1000000));
            o.batch_bytes =
                static_cast<std::size_t>(ranged("batch_bytes", 4 * 1024 * 1024, 1024, 268435456));
            // Unless given, 64 KiB or half of a smaller batch_bytes, so a small
            // batch_bytes alone still holds a sample.
            o.max_sample_bytes = static_cast<std::size_t>(ranged(
                "max_sample_bytes",
                std::min<std::int64_t>(64 * 1024, static_cast<std::int64_t>(o.batch_bytes / 2)),
                64,
                static_cast<std::int64_t>(o.batch_bytes)));
            o.max_retries = static_cast<int>(ranged("max_retries", 4, 0, 20));
            o.max_age = std::chrono::milliseconds{ranged("linger_ms", 0, 0, 3600000)};
            o.connect_timeout_ms = static_cast<int>(ranged("connect_timeout_ms", 5000, 1, 600000));
            o.request_timeout_ms = static_cast<int>(ranged("request_timeout_ms", 30000, 1, 600000));
            o.dlq_policy = one_of("dlq", "fail", {"fail", "drop"}) == "drop" ? DlqPolicy::Drop
                                                                             : DlqPolicy::Fail;
            o.name = "prometheus_sink";
            return make_prometheus_pushgateway_sink(std::move(o));
        });

    // http_poll_source: GET a JSON endpoint on an interval and emit the array
    // elements. Cursor mode (cursor_param + cursor_field) checkpoints the
    // cursor and replays from it; resource mode (neither) keeps no position.
    // Every option is parsed strictly: a value the source cannot read fails
    // the build rather than falling back to a default. Params:
    //   url (required)        - scheme://host[:port]
    //   path (default "/")    - request path (may carry a fixed query string)
    //   headers               - "K: V; ..." (auth)
    //   verify_tls            - "true" (default) | "false"
    //   cursor_param          - query parameter the cursor is sent as
    //   cursor_field          - record field each record's cursor is read from
    //   cursor_type           - "opaque" (default) | "integer"
    //   records_field         - response field holding the array (else the
    //                           response itself is the array)
    //   initial_cursor        - cursor mode: where a fresh run starts
    //   bounded               - "false" (default) | "true"
    //   poll_interval_ms (default 1000; 0 to 3600000)
    //   jitter_frac (default 0; 0 to 1)
    //   max_retries (default 4; 0 to 20) - retries one poll may make
    //   retry_base_backoff_ms (default 200), max_backoff_ms (default 10000)
    //   retry_timeout_ms (default 60000) - the longest one poll may take
    //   connect_timeout_ms (default 5000), request_timeout_ms (default 30000)
    //   max_response_bytes (default 16777216), max_response_records (default 100000)
    reg.register_source<std::string>(
        "http_poll_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            const auto given = [&ctx](const char* key) { return ctx.params.contains(key); };
            const auto refuse = [](const std::string& why) {
                throw std::runtime_error("http_poll_source: " + why);
            };
            const auto integer = [&](const char* key, std::int64_t fallback) {
                if (!given(key)) {
                    return fallback;
                }
                const std::string v = ctx.param_or(key);
                std::int64_t out = 0;
                const auto* end = v.data() + v.size();
                const auto [ptr, ec] = std::from_chars(v.data(), end, out);
                if (v.empty() || ec != std::errc{} || ptr != end) {
                    refuse(std::string{"'"} + key + "' must be an integer; it is '" + v + "'");
                }
                return out;
            };
            const auto boolean = [&](const char* key, bool fallback) {
                if (!given(key)) {
                    return fallback;
                }
                const std::string v = ctx.param_or(key);
                if (v != "true" && v != "false") {
                    refuse(std::string{"'"} + key + "' must be 'true' or 'false'; it is '" + v +
                           "'");
                }
                return v == "true";
            };
            const auto text = [&](const char* key) {
                const std::string v = ctx.param_or(key, "");
                if (given(key) && v.empty()) {
                    refuse(std::string{"'"} + key + "' is empty; leave it out instead");
                }
                return v;
            };
            HttpPollOptions o;
            o.url = ctx.param_or("url");
            o.path = ctx.param_or("path", "/");
            o.headers = ctx.param_or("headers", "");
            o.verify_tls = boolean("verify_tls", true);
            o.cursor_param = text("cursor_param");
            o.cursor_field = text("cursor_field");
            o.records_field = text("records_field");
            o.initial_cursor = text("initial_cursor");
            if (given("cursor_type")) {
                if (o.cursor_field.empty()) {
                    refuse("'cursor_type' needs cursor mode ('cursor_param' and 'cursor_field')");
                }
                const std::string t = ctx.param_or("cursor_type");
                if (t == "integer") {
                    o.cursor_type = HttpPollCursorType::Integer;
                } else if (t != "opaque") {
                    refuse("'cursor_type' must be 'opaque' or 'integer'; it is '" + t + "'");
                }
            }
            o.bounded = boolean("bounded", false);
            o.interval = std::chrono::milliseconds{integer("poll_interval_ms", 1000)};
            if (given("jitter_frac")) {
                const std::string v = ctx.param_or("jitter_frac");
                char* end = nullptr;
                const double j = std::strtod(v.c_str(), &end);
                if (v.empty() || end != v.c_str() + v.size() || !(j >= 0.0 && j <= 1.0)) {
                    refuse("'jitter_frac' must be a number from 0 to 1; it is '" + v + "'");
                }
                o.jitter_frac = j;
            }
            // Range-checked as an int64 before it is narrowed; the source
            // checks every range again.
            const auto max_retries = integer("max_retries", 4);
            if (max_retries < 0 || max_retries > HttpPollSource::kMaxRetries) {
                refuse("'max_retries' must be 0 to " + std::to_string(HttpPollSource::kMaxRetries) +
                       "; it is " + std::to_string(max_retries));
            }
            o.max_retries = static_cast<int>(max_retries);
            o.retry_base_backoff = std::chrono::milliseconds{integer("retry_base_backoff_ms", 200)};
            o.max_backoff = std::chrono::milliseconds{integer("max_backoff_ms", 10000)};
            o.retry_timeout = std::chrono::milliseconds{integer("retry_timeout_ms", 60000)};
            o.connect_timeout = std::chrono::milliseconds{integer("connect_timeout_ms", 5000)};
            o.request_timeout = std::chrono::milliseconds{integer("request_timeout_ms", 30000)};
            const auto max_bytes = integer("max_response_bytes", 16LL * 1024 * 1024);
            const auto max_records = integer("max_response_records", 100000);
            if (max_bytes < 0 || max_records < 0) {
                refuse("'max_response_bytes' and 'max_response_records' must be positive");
            }
            o.max_response_bytes = static_cast<std::size_t>(max_bytes);
            o.max_response_records = static_cast<std::size_t>(max_records);
            o.name = "http_poll_source";
            return clink::read_on_first_subtask<std::string>(ctx.subtask_idx,
                                                             make_http_poll_source(std::move(o)));
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
    // :pull API, emit each message's base64-decoded data, and :acknowledge once
    // the checkpoint that covers it completes (the ack is the offset commit).
    // At-least-once: the source holds each message's lease until then, and the
    // server delivers again whatever was not acknowledged, which is also crash
    // recovery. Multiple subtasks pull the SAME subscription; Pub/Sub
    // load-balances messages across them (no client-side sharding).
    // Params:
    //   project (required), subscription (required)
    //   endpoint / auth_token / headers / verify_tls - as pubsub_sink
    //   max_messages (default 1000; capped at 1000) - Pull maxMessages
    //   return_immediately ("true" [default] | "false")
    //   poll_interval_ms (default 500) - sleep after an empty pull
    //   max_retries (default 4; clamped to [0, 20]) - transient pull/ack retries
    //   ack_deadline_s (default 60; 10 to 600) - lease taken and renewed per message
    //   max_lease_s (default 3600) - how long one message's lease is renewed
    //   max_outstanding (default 10000) - messages held unacknowledged before pulls pause
    //   on_decode_error ("fail" [default] | "drop") - data that is not base64
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
            const auto ack_deadline_s = ctx.param_int64_or("ack_deadline_s", 60);
            if (ack_deadline_s < 10 || ack_deadline_s > 600) {
                throw std::runtime_error(
                    "pubsub_source: 'ack_deadline_s' must be between 10 and 600 seconds");
            }
            o.ack_deadline = std::chrono::seconds{ack_deadline_s};
            const auto max_lease_s = ctx.param_int64_or("max_lease_s", 3600);
            if (max_lease_s < ack_deadline_s) {
                throw std::runtime_error(
                    "pubsub_source: 'max_lease_s' must be at least 'ack_deadline_s'");
            }
            o.max_lease = std::chrono::seconds{max_lease_s};
            const auto max_outstanding = ctx.param_int64_or("max_outstanding", 10000);
            if (max_outstanding < 1) {
                throw std::runtime_error("pubsub_source: 'max_outstanding' must be at least 1");
            }
            o.max_outstanding = static_cast<std::size_t>(max_outstanding);
            const std::string on_decode_error = ctx.param_or("on_decode_error", "fail");
            if (on_decode_error != "fail" && on_decode_error != "drop") {
                throw std::runtime_error(
                    "pubsub_source: 'on_decode_error' must be 'fail' or 'drop', not '" +
                    on_decode_error + "'");
            }
            o.on_decode_error = on_decode_error == "drop"
                                    ? PubSubSourceOptions::OnDecodeError::Drop
                                    : PubSubSourceOptions::OnDecodeError::Fail;
            o.name = "pubsub_source";
            return std::make_shared<PubSubPullSource>(std::move(o));
        });
}

}  // namespace clink::http_connector
