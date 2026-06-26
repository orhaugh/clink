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

// Parse a "K1: V1; K2: V2" header string into a map. Empty -> no headers.
// Splits each pair on the FIRST ':', so a value may itself contain ':' (e.g.
// "Authorization: Bearer a:b"). Whitespace around key/value is trimmed.
inline std::map<std::string, std::string> parse_headers(const std::string& spec) {
    std::map<std::string, std::string> out;
    auto trim = [](std::string s) {
        const auto b = s.find_first_not_of(" \t");
        const auto e = s.find_last_not_of(" \t");
        return b == std::string::npos ? std::string{} : s.substr(b, e - b + 1);
    };
    std::size_t pos = 0;
    while (pos < spec.size()) {
        auto semi = spec.find(';', pos);
        if (semi == std::string::npos) {
            semi = spec.size();
        }
        const std::string pair = spec.substr(pos, semi - pos);
        const auto colon = pair.find(':');
        if (colon != std::string::npos) {
            auto k = trim(pair.substr(0, colon));
            auto v = trim(pair.substr(colon + 1));
            if (!k.empty()) {
                out.emplace(std::move(k), std::move(v));
            }
        }
        pos = semi + 1;
    }
    return out;
}

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
    opts.name = o.name;

    // _bulk returns HTTP 200 even when individual items fail; success requires
    // 2xx AND "errors":false in the body. A 2xx whose body has no "errors"
    // field (unexpected) is accepted rather than wedging the pipeline.
    opts.response_ok = [](const HttpResponse& r) -> bool {
        if (r.status < 200 || r.status >= 300) {
            return false;
        }
        try {
            auto j = clink::config::parse(r.body);
            if (j.is_object()) {
                const auto& obj = j.as_object();
                if (auto it = obj.find("errors"); it != obj.end() && it->second.is_bool()) {
                    return !it->second.as_bool();
                }
            }
        } catch (...) {
        }
        return true;
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

}  // namespace clink::http_connector
