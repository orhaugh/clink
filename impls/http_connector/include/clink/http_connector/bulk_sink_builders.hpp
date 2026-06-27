#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/config/json.hpp"
#include "clink/http_connector/batched_http_bulk_sink.hpp"
#include "clink/operators/operator_base.hpp"

// Plain (registry-independent) builders for the HTTP-family sinks, so they are
// unit-testable without the cluster/registry plumbing and reusable from a
// programmatic API. The SQL factories in register_factories.cpp adapt a
// BuildContext onto these.
namespace clink::http_connector {

// parse_headers lives in http_bulk_post.hpp (a generic HTTP utility shared with
// the Prometheus sink); it is in scope here transitively.

// Scalar string form of a JsonValue for an Elasticsearch document _id (string
// as-is; number/bool stringified; nested -> JSON text). It is JSON-escaped when
// the action object is serialized, so this only needs the scalar text.
inline std::string es_id_to_string(const clink::config::JsonValue& v) {
    if (v.is_string()) {
        return v.as_string();
    }
    if (v.is_bool()) {
        return v.as_bool() ? "true" : "false";
    }
    if (v.is_number()) {
        const double d = v.as_number();
        // Integral AND within int64 range -> plain integer. The range guard is
        // load-bearing: the float->int64 cast is UB outside int64, and on
        // saturation would collapse distinct large ids (e.g. > 9.2e18) onto
        // INT64_MAX, silently overwriting different documents. 2^63 is exactly
        // representable as a double, so the upper bound is a strict '<'.
        constexpr double kInt64Lo = -9223372036854775808.0;          // -2^63
        constexpr double kInt64HiExclusive = 9223372036854775808.0;  //  2^63
        if (std::isfinite(d) && d >= kInt64Lo && d < kInt64HiExclusive &&
            d == static_cast<double>(static_cast<std::int64_t>(d))) {
            return std::to_string(static_cast<std::int64_t>(d));
        }
        // Fractional or out-of-int64-range: format the double with no cast.
        // Deterministic, so idempotency on re-delivery still holds.
        return std::to_string(d);
    }
    return v.serialize(0);
}

// The HTTP-like status of one Elasticsearch _bulk response item. An item is
// {"<action>": {"status": N, ...}} (action = index/create/update/delete). A
// missing/odd shape returns 500 so the record is RETRIED (the safe direction),
// never silently dropped.
inline int es_item_status(const clink::config::JsonValue& item) {
    if (!item.is_object() || item.as_object().empty()) {
        return 500;
    }
    const auto& action = item.as_object().begin()->second;  // the single action result
    if (!action.is_object()) {
        return 500;
    }
    auto it = action.as_object().find("status");
    if (it != action.as_object().end() && it->second.is_number()) {
        return static_cast<int>(it->second.as_number());
    }
    return 500;
}

// Parse an Elasticsearch _bulk response into a per-record verdict. `count` is the
// number of records POSTed (== items[] length). Each item's status: 2xx success,
// 429/5xx transient (resend that item), other 4xx (400 mapping / 409 version)
// permanent (DLQ that item). A non-2xx HTTP status, a parse failure, or an
// items[] whose length does not match the batch falls back to whole-batch
// classification so the whole batch is retried/DLQ'd rather than mis-attributed.
inline BulkResult es_bulk_result(const HttpResponse& res, std::size_t count) {
    if (res.status < 200 || res.status >= 300) {
        return whole_batch_result(res, count, {});  // HTTP-level failure, no per-item info
    }
    clink::config::JsonValue body;
    try {
        body = clink::config::parse(res.body);
    } catch (...) {
        return whole_batch_result(res, count, [](const HttpResponse&) { return false; });
    }
    if (!body.is_object()) {
        return BulkResult{true, {}, {}};  // unexpected 2xx shape: accept (do not wedge)
    }
    const auto& obj = body.as_object();
    if (auto e = obj.find("errors");
        e != obj.end() && e->second.is_bool() && !e->second.as_bool()) {
        return BulkResult{true, {}, {}};  // errors:false fast path
    }
    auto items_it = obj.find("items");
    if (items_it == obj.end() || !items_it->second.is_array() ||
        items_it->second.as_array().size() != count) {
        // errors:true but no usable per-item array: retry the whole batch.
        return whole_batch_result(res, count, [](const HttpResponse&) { return false; });
    }
    const auto& items = items_it->second.as_array();
    BulkResult br;
    for (std::size_t i = 0; i < items.size(); ++i) {
        const int st = es_item_status(items[i]);
        if (st >= 200 && st < 300) {
            continue;  // this item indexed
        }
        if (st == 429 || st >= 500) {
            br.failed_transient.push_back(i);
        } else {
            br.failed_permanent.push_back(i);  // 400 mapping / 409 version: poison
        }
    }
    br.ok = br.failed_transient.empty() && br.failed_permanent.empty();
    return br;
}

struct EsBulkOptions {
    std::string url;          // scheme://host[:port]   (required)
    std::string index;        // target index           (required)
    std::string document_id;  // record field -> _id (optional; idempotent)
    std::string headers;      // "K: V; ..." header spec
    std::string path{"/_bulk"};
    bool verify_tls{true};
    std::size_t batch_records{500};
    std::size_t batch_bytes{4 * 1024 * 1024};
    int max_retries{4};
    std::chrono::milliseconds max_age{0};  // linger: flush a partial batch this old
    DlqPolicy dlq_policy{DlqPolicy::Fail};
    std::string name{"elasticsearch_sink"};
};

// Build an Elasticsearch / OpenSearch _bulk sink (their bulk APIs are
// identical). Each input record is a JSON object string (e.g. SQL
// row_to_json_string); the sink emits NDJSON pairs - an action line
// {"index":{"_index":...[,"_id":...]}} then the source doc. document_id makes
// writes idempotent (re-delivery on replay overwrites the same _id), the path
// to effectively-once over an at-least-once stream.
inline std::shared_ptr<Sink<std::string>> make_es_bulk_sink(EsBulkOptions o) {
    if (o.url.empty()) {
        throw std::runtime_error(o.name + ": 'url' is required");
    }
    if (o.index.empty()) {
        throw std::runtime_error(o.name + ": 'index' is required");
    }

    BatchedHttpBulkSink<std::string>::Options opts;
    opts.http.base_url = o.url;
    opts.http.headers = parse_headers(o.headers);
    opts.http.verify_tls = o.verify_tls;
    opts.path = o.path;
    opts.content_type = "application/x-ndjson";
    opts.framing = BulkFraming::Ndjson;
    opts.max_records = o.batch_records;
    opts.max_bytes = o.batch_bytes;
    opts.max_retries = o.max_retries;
    opts.max_age = o.max_age;
    opts.dlq_policy = o.dlq_policy;
    opts.name = o.name;

    // _bulk returns HTTP 200 even when individual items fail ("errors":true with a
    // per-item items[]). The per-item handler resends ONLY the transient (429/5xx)
    // items and DLQs/throws the permanent (4xx mapping/version) ones, so a partial
    // failure never re-indexes the already-written documents (the whole-batch
    // retry did, duplicating them when there is no document_id).
    opts.response_handler = [](const HttpResponse& r, std::size_t count) {
        return es_bulk_result(r, count);
    };

    auto render = [index = o.index, id_field = o.document_id](std::string& out,
                                                              const std::string& rec) {
        clink::config::JsonObject meta;
        meta["_index"] = clink::config::JsonValue{index};
        if (!id_field.empty()) {
            try {
                auto j = clink::config::parse(rec);
                if (j.is_object()) {
                    const auto& obj = j.as_object();
                    if (auto it = obj.find(id_field); it != obj.end() && !it->second.is_null()) {
                        meta["_id"] = clink::config::JsonValue{es_id_to_string(it->second)};
                    }
                }
            } catch (...) {
            }
        }
        clink::config::JsonObject action;
        action["index"] = clink::config::JsonValue{std::move(meta)};
        out += clink::config::JsonValue{std::move(action)}.serialize(0);
        out += '\n';
        out += rec;  // source doc; the Ndjson framing appends the trailing '\n'
    };
    return std::make_shared<BatchedHttpBulkSink<std::string>>(std::move(opts), std::move(render));
}

struct SplunkHecOptions {
    std::string url;    // scheme://host[:port]   (required)
    std::string token;  // HEC token              (required)
    std::string path{"/services/collector/event"};
    std::string sourcetype;  // optional (e.g. "_json" to auto-extract event fields)
    std::string source;      // optional
    std::string host;        // optional
    std::string index;       // optional (must be writable by the token)
    std::string headers;     // extra "K: V; ..." (the token already sets auth)
    bool verify_tls{true};
    std::size_t batch_records{500};
    std::size_t batch_bytes{4 * 1024 * 1024};
    int max_retries{4};
    std::chrono::milliseconds max_age{0};  // linger: flush a partial batch this old
    DlqPolicy dlq_policy{DlqPolicy::Fail};
    std::string name{"splunk_hec_sink"};
};

// Build a Splunk HTTP Event Collector (HEC) sink. Each input record is a JSON
// object string (e.g. SQL row_to_json_string); the sink wraps it in the HEC
// event envelope {"event":<doc>[,"sourcetype":...,"index":...]} and POSTs a
// batch of concatenated event objects (NDJSON form) to /services/collector/event
// with `Authorization: Splunk <token>`. At-least-once (HEC ingestion is append:
// re-delivery on replay indexes the event twice).
inline std::shared_ptr<Sink<std::string>> make_splunk_hec_sink(SplunkHecOptions o) {
    if (o.url.empty()) {
        throw std::runtime_error(o.name + ": 'url' is required");
    }
    if (o.token.empty()) {
        throw std::runtime_error(o.name + ": 'token' is required");
    }

    BatchedHttpBulkSink<std::string>::Options opts;
    opts.http.base_url = o.url;
    opts.http.headers = parse_headers(o.headers);
    // HEC auth is the literal keyword "Splunk" (NOT "Bearer") then the token.
    opts.http.headers["Authorization"] = "Splunk " + o.token;
    opts.http.verify_tls = o.verify_tls;
    opts.path = o.path;
    opts.content_type = "application/json";
    // A batch is concatenated JSON event objects; NDJSON (one object per line)
    // is the readable form HEC accepts. NOT a JSON array.
    opts.framing = BulkFraming::Ndjson;
    opts.max_records = o.batch_records;
    opts.max_bytes = o.batch_bytes;
    opts.max_retries = o.max_retries;
    opts.max_age = o.max_age;
    opts.dlq_policy = o.dlq_policy;
    opts.name = o.name;
    // Unlike ES _bulk, HEC never returns HTTP 200 with a failure code - the
    // body "code" tracks the HTTP status (200 => accepted, including the health
    // codes 17/24/25). So a plain 2xx check is correct; no custom validator.

    // Precompute the static event-metadata suffix once, escaped via the JSON
    // serializer: ,"sourcetype":"...","index":"..." etc. Empty if none set.
    clink::config::JsonObject meta;
    if (!o.sourcetype.empty()) {
        meta["sourcetype"] = clink::config::JsonValue{o.sourcetype};
    }
    if (!o.source.empty()) {
        meta["source"] = clink::config::JsonValue{o.source};
    }
    if (!o.host.empty()) {
        meta["host"] = clink::config::JsonValue{o.host};
    }
    if (!o.index.empty()) {
        meta["index"] = clink::config::JsonValue{o.index};
    }
    std::string meta_suffix;
    if (!meta.empty()) {
        const std::string m = clink::config::JsonValue{std::move(meta)}.serialize(0);
        meta_suffix = "," + m.substr(1, m.size() - 2);  // strip the {} braces, lead with ','
    }

    auto render = [meta_suffix = std::move(meta_suffix)](std::string& out, const std::string& rec) {
        // The HEC event envelope. `rec` is a single-line JSON object (SQL
        // path), embedded verbatim as the "event" value (Splunk extracts its
        // fields when sourcetype=_json). The Ndjson framing adds the trailing
        // newline that separates events.
        out += "{\"event\":";
        out += rec;
        out += meta_suffix;
        out += '}';
    };
    return std::make_shared<BatchedHttpBulkSink<std::string>>(std::move(opts), std::move(render));
}

}  // namespace clink::http_connector
