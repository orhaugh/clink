#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/http_connector/batched_http_bulk_sink.hpp"
#include "clink/http_connector/splunk_hec_sink.hpp"  // make_splunk_hec_sink
#include "clink/operators/operator_base.hpp"

// Plain (registry-independent) builders for the HTTP-family sinks, so they are
// unit-testable without the cluster/registry plumbing and reusable from a
// programmatic API. The SQL factories in register_factories.cpp adapt a
// BuildContext onto these.
namespace clink::http_connector {

// parse_headers lives in http_bulk_post.hpp (a generic HTTP utility shared with
// the Prometheus sink); it is in scope here transitively.

// ---- document_id: the _id a record names ---------------------------------
//
// The value of the field document_id names becomes the document's _id, as text:
//
//   string    itself. Refused when empty or longer than 512 bytes: the servers
//             refuse an empty _id for its record, and a longer one for the whole
//             request, which would fail every record sent with it.
//   integer   its exact decimal digits at any size ("-9223372036854775808",
//             "18446744073709551615"), however the record writes it: 42, 42.0 and
//             4.2e1 all name "42".
//   fraction  the six-decimal form the sink has always written ("0.500000"), when
//             the record's number has at most six decimal places and a double
//             holds it to six places. Refused otherwise: that form would give it
//             the _id of a different number.
//   boolean   "true" or "false".
//
// A null, an absent field, an object or an array is refused. The _id is text in
// one namespace: the number 42 and the string "42" name one document, as do 0.5
// and "0.500000", and true and "true".
inline constexpr std::size_t kEsMaxIdBytes = 512;
inline constexpr std::size_t kEsFractionDigits = 6;

struct EsDocumentId {
    std::string id;       // the _id, when the value names one
    std::string refusal;  // why it does not, otherwise
};

namespace detail {

// A JSON number token as its exact decimal value: the sign, the integer digits
// with no leading zeros ("0" for none) and the fraction digits with no trailing
// zeros. Refused, with why, for a token that is not a JSON number, or whose
// integer part would run past `max_int_digits` or fraction past
// `max_frac_digits`: no _id can carry it then. Built by moving the decimal
// point, so it never rounds.
struct ExactDecimal {
    bool negative{false};
    std::string int_digits;
    std::string frac_digits;
    std::string refusal;
};

inline ExactDecimal exact_decimal(std::string_view tok,
                                  std::size_t max_int_digits,
                                  std::size_t max_frac_digits) {
    const auto is_digit = [](char c) { return c >= '0' && c <= '9'; };
    ExactDecimal d;
    std::size_t i = 0;
    if (i < tok.size() && tok[i] == '-') {
        d.negative = true;
        ++i;
    }
    std::string digits;  // the mantissa's digits, integer part then fraction
    while (i < tok.size() && is_digit(tok[i])) {
        digits += tok[i++];
    }
    const std::size_t int_len = digits.size();
    if (int_len == 0) {
        d.refusal = "the number " + std::string(tok) + ", which is not a JSON numeral";
        return d;
    }
    if (i < tok.size() && tok[i] == '.') {
        ++i;
        while (i < tok.size() && is_digit(tok[i])) {
            digits += tok[i++];
        }
    }
    long long exponent = 0;
    if (i < tok.size() && (tok[i] == 'e' || tok[i] == 'E')) {
        ++i;
        bool negative_exponent = false;
        if (i < tok.size() && (tok[i] == '+' || tok[i] == '-')) {
            negative_exponent = tok[i] == '-';
            ++i;
        }
        while (i < tok.size() && is_digit(tok[i])) {
            // Saturates far past every bound below, so it cannot overflow.
            if (exponent < 1000000) {
                exponent = exponent * 10 + (tok[i] - '0');
            }
            ++i;
        }
        if (negative_exponent) {
            exponent = -exponent;
        }
    }
    if (i != tok.size()) {
        d.refusal = "the number " + std::string(tok) + ", which is not a JSON numeral";
        return d;
    }
    // The decimal point sits `point` digits into `digits`.
    long long point = static_cast<long long>(int_len) + exponent;
    std::size_t lead = 0;
    while (lead < digits.size() && digits[lead] == '0') {
        ++lead;
    }
    digits.erase(0, lead);
    point -= static_cast<long long>(lead);
    while (!digits.empty() && digits.back() == '0' &&
           static_cast<long long>(digits.size()) > point) {
        digits.pop_back();  // a zero after the point
    }
    if (digits.empty()) {
        d.negative = false;  // -0 is 0
        d.int_digits = "0";
        return d;
    }
    if (point > static_cast<long long>(max_int_digits)) {
        d.refusal = "the number " + std::string(tok) + ", whose " + std::to_string(point) +
                    " integer digits are more than an _id may hold";
        return d;
    }
    const long long frac_len = static_cast<long long>(digits.size()) - point;
    if (frac_len > static_cast<long long>(max_frac_digits)) {
        d.refusal = "the number " + std::string(tok) + ", with more than " +
                    std::to_string(max_frac_digits) +
                    " decimal places, which the sink's six-decimal form would round onto "
                    "another number's _id";
        return d;
    }
    if (point <= 0) {
        d.int_digits = "0";
        d.frac_digits = std::string(static_cast<std::size_t>(-point), '0') + digits;
    } else if (static_cast<std::size_t>(point) >= digits.size()) {
        d.int_digits = digits + std::string(static_cast<std::size_t>(point) - digits.size(), '0');
    } else {
        d.int_digits = digits.substr(0, static_cast<std::size_t>(point));
        d.frac_digits = digits.substr(static_cast<std::size_t>(point));
    }
    return d;
}

}  // namespace detail

// The _id the value of a record's document_id field names (see above). `token`
// is the record's own text for that number when it is not an integer the JSON
// reader holds exactly, which is what keeps a large or fractional number from
// being rounded first; empty otherwise.
inline EsDocumentId es_document_id(const clink::config::JsonValue& v, std::string_view token) {
    if (v.is_string()) {
        const auto& s = v.as_string();
        if (s.empty()) {
            return {{}, "an empty string, which names no document"};
        }
        if (s.size() > kEsMaxIdBytes) {
            return {{},
                    "a string of " + std::to_string(s.size()) +
                        " bytes, longer than the 512 bytes an _id may hold"};
        }
        return {s, {}};
    }
    if (v.is_bool()) {
        return {v.as_bool() ? "true" : "false", {}};
    }
    if (v.is_integral_number()) {
        return {std::to_string(v.as_int()), {}};
    }
    if (v.is_number()) {
        const double d = v.as_number();
        if (!std::isfinite(d)) {
            return {{}, "a number that is not finite"};
        }
        if (token.empty()) {
            return {{}, "a number whose text the sink could not read back"};
        }
        auto ex = detail::exact_decimal(token, kEsMaxIdBytes - 1, kEsFractionDigits);
        if (!ex.refusal.empty()) {
            return {{}, std::move(ex.refusal)};
        }
        const std::string sign = ex.negative ? "-" : "";
        if (ex.frac_digits.empty()) {
            return {sign + ex.int_digits, {}};
        }
        // The six-decimal form, kept where it is exact so a replay of older
        // records still overwrites the documents they wrote.
        const std::string written = sign + ex.int_digits + "." + ex.frac_digits +
                                    std::string(kEsFractionDigits - ex.frac_digits.size(), '0');
        const std::string six = std::to_string(d);
        if (six != written) {
            return {{},
                    "the number " + std::string(token) +
                        ", whose fraction a double cannot hold to six decimal places"};
        }
        return {six, {}};
    }
    if (v.is_null()) {
        return {{}, "null"};
    }
    return {{}, "an object or an array, which names no single document"};
}

// Why a record cannot be one line of a _bulk body, or nullptr when it can: it
// must be a JSON object and hold no line break. A record that spans lines breaks
// the NDJSON framing and the server fails the whole request with it; one that is
// not an object fails as a record, with a 400 from OpenSearch but a 500 from
// Elasticsearch, which would be retried forever.
inline const char* es_record_shape_problem(std::string_view rec) {
    if (rec.find('\n') != std::string_view::npos) {
        return "spans more than one line";
    }
    const auto first = rec.find_first_not_of(" \t\r");
    if (first == std::string_view::npos || rec[first] != '{') {
        return "is not a JSON object";
    }
    return nullptr;
}

// Item failures that both servers report as 400 for a record but that concern
// its index: the index is closed, or its name is not valid. They refuse every
// record alike, so they are resent and then fail the job; dead-lettered, they
// would drop every record written while the index refused them (seen from
// Elasticsearch 8.13 and OpenSearch 2.17).
inline bool es_index_level_error(std::string_view type) {
    return type == "index_closed_exception" || type == "invalid_index_name_exception";
}

namespace detail {

inline std::vector<std::size_t> every_position(std::size_t count) {
    std::vector<std::size_t> all(count);
    for (std::size_t i = 0; i < count; ++i) {
        all[i] = i;
    }
    return all;
}

// A 2xx whose body does not establish the outcome of every operation sent:
// nothing of the batch counts as written, and all of it is resent (and, once
// retries run out, the job fails and replays). Never dead-lettered.
inline BulkResult es_unconfirmed(std::size_t count, std::string why) {
    BulkResult br;
    br.failed_transient = every_position(count);
    br.detail = "the bulk response does not confirm the batch: " + std::move(why);
    return br;
}

inline std::string es_error_text(const clink::config::JsonValue* error) {
    if (error == nullptr || !error->is_object()) {
        return {};
    }
    const auto& e = error->as_object();
    std::string out;
    if (auto t = e.find("type"); t != e.end() && t->second.is_string()) {
        out = t->second.as_string();
    }
    if (auto r = e.find("reason"); r != e.end() && r->second.is_string()) {
        out += (out.empty() ? "" : ": ") + r->second.as_string().substr(0, 160);
    }
    return out;
}

}  // namespace detail

// Parse an Elasticsearch / OpenSearch _bulk response into a per-record verdict.
// `count` is the number of operations POSTed, each an `index` action.
//
// A batch counts as written only when the response confirms every operation: a
// JSON object with a boolean `errors` and an `items` array of exactly `count`
// entries, each a single `index` result with an HTTP status, `errors` agreeing
// with the statuses. Anything else under a 2xx (a body that is not JSON or not
// an object, a missing or short array, another action, an item with no status,
// a summary that contradicts the items) is unconfirmed: the whole batch is
// resent, never dead-lettered.
//
// Per item: 2xx is written. 400, 409 and 422 refuse the record itself (a mapping
// or parse failure, a version conflict), so it is dead-lettered or fails the job
// per `dlq`, except a 400 about the index (es_index_level_error). Every other
// failure (429, 5xx, 403 for a write-blocked index, 404 for a missing one) is
// resent. A non-2xx status for the whole request (a request the server could
// not validate, credentials, a body too large, throttling, an outage, or no
// response) concerns no one record: the batch is resent and, once retries run
// out, the job fails and replays.
inline BulkResult es_bulk_result(const HttpResponse& res, std::size_t count) {
    if (res.status < 200 || res.status >= 300) {
        BulkResult br;
        br.failed_transient = detail::every_position(count);
        if (res.status != 0) {
            try {
                const auto body = clink::config::parse(res.body);
                if (body.is_object()) {
                    const auto& o = body.as_object();
                    if (auto e = o.find("error"); e != o.end()) {
                        br.detail = detail::es_error_text(&e->second);
                    }
                }
            } catch (const std::exception&) {
            }
        }
        return br;
    }
    clink::config::JsonValue body;
    try {
        body = clink::config::parse(res.body);
    } catch (const std::exception&) {
        return detail::es_unconfirmed(count, "the body is not JSON");
    }
    if (!body.is_object()) {
        return detail::es_unconfirmed(count, "the body is not a JSON object");
    }
    const auto& obj = body.as_object();
    const auto errors_it = obj.find("errors");
    if (errors_it == obj.end() || !errors_it->second.is_bool()) {
        return detail::es_unconfirmed(count, "it has no boolean 'errors'");
    }
    const auto items_it = obj.find("items");
    if (items_it == obj.end() || !items_it->second.is_array()) {
        return detail::es_unconfirmed(count, "it has no 'items' array");
    }
    const auto& items = items_it->second.as_array();
    if (items.size() != count) {
        return detail::es_unconfirmed(count,
                                      "it accounts for " + std::to_string(items.size()) +
                                          " operation(s) of the " + std::to_string(count) +
                                          " sent");
    }
    BulkResult br;
    std::size_t failed = 0;
    std::string failures;  // "item N: <status> <error>" for the first few, for the summary
    for (std::size_t i = 0; i < items.size(); ++i) {
        const std::string at = "item " + std::to_string(i);
        const auto& item = items[i];
        if (!item.is_object() || item.as_object().size() != 1) {
            return detail::es_unconfirmed(count, at + " is not a single action result");
        }
        const auto& [action, result] = *item.as_object().begin();
        if (action != "index") {
            return detail::es_unconfirmed(
                count, at + " is a '" + action + "' result for an 'index' operation");
        }
        if (!result.is_object()) {
            return detail::es_unconfirmed(count, at + " is not an object");
        }
        const auto& r = result.as_object();
        const auto status_it = r.find("status");
        if (status_it == r.end() || !status_it->second.is_integral_number() ||
            status_it->second.as_int() < 100 || status_it->second.as_int() > 599) {
            return detail::es_unconfirmed(count, at + " has no HTTP status");
        }
        const int st = static_cast<int>(status_it->second.as_int());
        const auto error_it = r.find("error");
        const clink::config::JsonValue* error = error_it == r.end() ? nullptr : &error_it->second;
        if (st >= 200 && st < 300) {
            if (error != nullptr) {
                return detail::es_unconfirmed(count, at + " succeeded with an error attached");
            }
            continue;
        }
        ++failed;
        if (failed <= 3) {
            const std::string text = detail::es_error_text(error);
            failures += (failures.empty() ? "" : "; ") + at + ": " + std::to_string(st) +
                        (text.empty() ? "" : " " + text);
        }
        std::string type;
        if (error != nullptr && error->is_object()) {
            if (auto t = error->as_object().find("type");
                t != error->as_object().end() && t->second.is_string()) {
                type = t->second.as_string();
            }
        }
        if (status_rejects_records(st) && !es_index_level_error(type)) {
            br.failed_permanent.push_back(i);
        } else {
            br.failed_transient.push_back(i);
        }
    }
    if (errors_it->second.as_bool() != (failed > 0)) {
        return detail::es_unconfirmed(
            count,
            failed > 0 ? "'errors' is false while " + failures
                       : std::string{"'errors' is true while every item succeeded"});
    }
    br.ok = failed == 0;
    if (failed > 0) {
        br.detail = std::to_string(failed) + " of " + std::to_string(count) +
                    " operation(s) failed: " + failures + (failed > 3 ? "; ..." : "");
    }
    return br;
}

struct EsBulkOptions {
    std::string url;          // scheme://host[:port]   (required)
    std::string index;        // target index           (required)
    std::string document_id;  // record field -> _id (optional; idempotent)
    std::string headers;      // "K: V; ..." header spec
    std::string path{"/_bulk"};
    bool verify_tls{true};
    // https: PEM file of the CAs that may sign the server's certificate, in
    // place of the system's trust store (a cluster's own CA). Empty: the system's.
    std::string tls_ca_file;
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
    opts.http.ca_cert_path = o.tls_ca_file;
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
    // per-item items[]). The per-item handler resends the transient items and
    // DLQs/throws the permanent (document) ones, and accepts a batch only when
    // the response confirms every operation (es_bulk_result). With document_id
    // the renderer names each record's _id as its key, so a resent record goes
    // out with the later records of the same document, and a partial failure
    // never re-indexes already-written documents without one.
    opts.response_handler = [](const HttpResponse& r, std::size_t count) {
        return es_bulk_result(r, count);
    };

    auto render = [index = o.index, id_field = o.document_id, name = o.name](
                      std::string& out, std::string& key, const std::string& rec) {
        if (const char* why = es_record_shape_problem(rec)) {
            throw UnrenderableRecord(name + ": a record " + why +
                                     "; the _bulk API takes each record as one JSON object on "
                                     "one line");
        }
        clink::config::JsonObject meta;
        meta["_index"] = clink::config::JsonValue{index};
        if (!id_field.empty()) {
            // document_id promises that a replay overwrites the same document.
            // A record without a usable id would get a generated _id, and its
            // replay a second one, so it is refused instead; so is a value that
            // names no document or could share an _id with another value.
            std::string refusal;
            EsDocumentId doc;
            try {
                const auto j = clink::config::parse(rec);
                const auto& obj = j.as_object();  // es_record_shape_problem saw '{'
                const auto it = obj.find(id_field);
                if (it == obj.end() || it->second.is_null()) {
                    refusal = "has no '" + id_field + "' field, or it is null";
                } else {
                    std::string token;
                    if (it->second.is_number() && !it->second.is_integral_number()) {
                        const auto tokens = clink::config::raw_number_tokens(rec, {{id_field, 0}});
                        if (auto t = tokens.find(id_field); t != tokens.end()) {
                            token = t->second;
                        }
                    }
                    doc = es_document_id(it->second, token);
                    if (!doc.refusal.empty()) {
                        refusal = "has '" + id_field + "' " + doc.refusal;
                    }
                }
            } catch (const std::exception&) {
                refusal = "is not valid JSON";
            }
            if (!refusal.empty()) {
                throw UnrenderableRecord(
                    name + ": a record " + refusal +
                    ", so it names no document _id; document_id needs a non-empty string, an "
                    "integer or an exactly written number in every record (a generated _id "
                    "would be duplicated by a replay)");
            }
            key = doc.id;
            meta["_id"] = clink::config::JsonValue{std::move(doc.id)};
        }
        clink::config::JsonObject action;
        action["index"] = clink::config::JsonValue{std::move(meta)};
        clink::config::JsonValue{std::move(action)}.serialize_into(out);
        out += '\n';
        out += rec;  // source doc; the Ndjson framing appends the trailing '\n'
    };
    return std::make_shared<BatchedHttpBulkSink<std::string>>(std::move(opts), std::move(render));
}

}  // namespace clink::http_connector
