#pragma once

#include <charconv>
#include <cstddef>
#include <cstring>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <vector>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/connectors/text_format.hpp"
#include "clink/core/codec.hpp"

// clink::sql::Row - a dynamic-schema row carrying multiple typed
// values on the per-record wire.
//
// Phase 3.2 takes the pragmatic route: each row is a JSON object
// (column name -> value). Serialization is plain UTF-8 JSON bytes,
// which has 3 properties we want for Phase 3:
//
//   1. Debuggable: messages are human-readable in transit.
//   2. No new ABI: rides the existing per-record Codec<T> wire path.
//   3. Schema-decoupled: the codec doesn't need to know the table
//      schema at construction time, so wire-side code stays generic.
//
// Phase 10 (perf) will likely swap this for an Arrow-RecordBatch-
// batched wire path, but the Row value type stays - the Codec<Row>
// just learns an alternate encoding.

namespace clink::sql {

struct Row {
    clink::config::JsonObject values;

    [[nodiscard]] bool has_column(const std::string& name) const {
        return values.find(name) != values.end();
    }

    // Returns the value for `name` rendered as a string. Used by the
    // JSON predicate evaluator and the project operator. Numeric and
    // bool values get stringified; null returns std::nullopt.
    [[nodiscard]] std::optional<std::string> get_string(const std::string& name) const {
        auto it = values.find(name);
        if (it == values.end() || it->second.is_null())
            return std::nullopt;
        if (clink::config::is_dec_string(it->second))
            return it->second.as_string().substr(1);  // #56: canonical decimal, sentinel stripped
        if (it->second.is_string())
            return it->second.as_string();
        if (it->second.is_number()) {
            double d = it->second.as_number();
            // Integer-valued doubles print without a trailing ".000000".
            if (d == static_cast<double>(static_cast<std::int64_t>(d))) {
                return std::to_string(static_cast<std::int64_t>(d));
            }
            return std::to_string(d);
        }
        if (it->second.is_bool())
            return it->second.as_bool() ? "true" : "false";
        // Arrays / nested objects round-trip through JSON serialization.
        return it->second.serialize(0);
    }
};

// JSON-encoded Codec<Row>. Each row encodes to one JSON object body
// (no trailing newline). The wire framing layer above handles record
// boundaries.
inline clink::Codec<Row> row_json_codec() {
    using Bytes = clink::Codec<Row>::Bytes;
    using BytesView = clink::Codec<Row>::BytesView;
    // Shared append-body: encode (wrap) and encode_into (direct) serialise the
    // same JSON object; encode_into appends to the caller-cleared buffer,
    // avoiding the per-put Bytes allocation. Byte-identical by construction.
    auto body = [](const Row& r, Bytes& out) {
        clink::config::JsonValue v{clink::config::JsonObject{r.values}};
        std::string s = v.serialize(0);
        const auto* p = reinterpret_cast<const std::byte*>(s.data());
        out.insert(out.end(), p, p + s.size());
    };
    return clink::Codec<Row>{
        .encode = [body](const Row& r) -> Bytes {
            Bytes out;
            body(r, out);
            return out;
        },
        .decode = [](BytesView b) -> std::optional<Row> {
            std::string text(reinterpret_cast<const char*>(b.data()), b.size());
            try {
                auto j = clink::config::parse(text);
                if (!j.is_object())
                    return std::nullopt;
                Row r;
                r.values = j.as_object();
                return r;
            } catch (...) {
                return std::nullopt;
            }
        },
        .encode_into = body,
    };
}

// JSON-encoded Codec for a LIST of Rows (a JSON array of the row objects).
// Backs the async/disaggregated KeyedState path of the stream-stream INNER join,
// where each join key's entry list (the rows seen on one side) round-trips
// through the remote pool. Shares Codec<Row>'s per-row JSON shape.
inline clink::Codec<std::vector<Row>> row_list_json_codec() {
    using Bytes = clink::Codec<std::vector<Row>>::Bytes;
    using BytesView = clink::Codec<std::vector<Row>>::BytesView;
    // Shared append-body (see row_json_codec): encode wraps, encode_into appends
    // to the caller-cleared buffer. Byte-identical by construction.
    auto body = [](const std::vector<Row>& rows, Bytes& out) {
        clink::config::JsonArray arr;
        arr.reserve(rows.size());
        for (const auto& r : rows) {
            arr.emplace_back(clink::config::JsonObject{r.values});
        }
        const std::string s = clink::config::JsonValue{std::move(arr)}.serialize(0);
        const auto* p = reinterpret_cast<const std::byte*>(s.data());
        out.insert(out.end(), p, p + s.size());
    };
    return clink::Codec<std::vector<Row>>{
        .encode = [body](const std::vector<Row>& rows) -> Bytes {
            Bytes out;
            body(rows, out);
            return out;
        },
        .decode = [](BytesView b) -> std::optional<std::vector<Row>> {
            std::string text(reinterpret_cast<const char*>(b.data()), b.size());
            try {
                auto j = clink::config::parse(text);
                if (!j.is_array()) {
                    return std::nullopt;
                }
                std::vector<Row> rows;
                rows.reserve(j.as_array().size());
                for (const auto& e : j.as_array()) {
                    if (!e.is_object()) {
                        return std::nullopt;
                    }
                    Row r;
                    r.values = e.as_object();
                    rows.push_back(std::move(r));
                }
                return rows;
            } catch (...) {
                return std::nullopt;
            }
        },
        .encode_into = body,
    };
}

// Channel-type identifier under which Row is registered with the
// TypeRegistry. SQL multi-column tables flow through this channel.
inline constexpr std::string_view kChannelRow = "row";

// Line-oriented JSON text format for FileSource<Row> / FileSink<Row>.
// One row per line; lines that don't parse as JSON objects are skipped
// (decoder returns nullopt). The encoder emits the JSON object with
// no trailing newline; FileSink adds the newline.
inline clink::TextFormat<Row> row_json_text_format() {
    return clink::TextFormat<Row>{
        .decode = [](std::string_view line) -> std::optional<Row> {
            if (line.empty())
                return std::nullopt;
            try {
                auto j = clink::config::parse(line);
                if (!j.is_object())
                    return std::nullopt;
                Row r;
                r.values = j.as_object();
                return r;
            } catch (...) {
                return std::nullopt;
            }
        },
        .encode = [](const Row& r) -> std::string {
            // #56: render dec-strings as clean unquoted JSON numbers for
            // external output (the wire codec above keeps the tagged form).
            clink::config::JsonValue v{clink::config::JsonObject{r.values}};
            return clink::config::serialize_output(v);
        },
    };
}

// #56: shortest fixed-notation text for a double (round-trips to the same
// double, no exponent), so a decimal column read from JSON can be re-quantised
// from the parsed double with the most precision the double carries.
inline std::string double_to_fixed_text(double d) {
    char buf[64];
    auto res = std::to_chars(buf, buf + sizeof(buf), d, std::chars_format::fixed);
    return std::string(buf, res.ptr);
}

// #56: re-quantise a row's named DECIMAL columns to their declared scale
// (HALF_UP), tagging the value as an exact dec-string. Used on BOTH the
// schema-aware source (born exact at ingestion) and the sink (the value is
// quantised to the column scale on assignment, per SQL). Within double
// precision: source values beyond ~15-17 significant digits are limited by the
// generic JSON parse to double - documented v1 boundary.
inline void requantise_row_decimals(Row& r, const std::map<std::string, int>& decimal_scales) {
    for (const auto& [col, scale] : decimal_scales) {
        auto it = r.values.find(col);
        if (it == r.values.end() || it->second.is_null())
            continue;
        std::optional<clink::config::Decimal> d;
        if (clink::config::is_dec_string(it->second)) {
            d = clink::config::as_decimal(it->second);
        } else if (it->second.is_number()) {
            d = clink::config::dec_parse(double_to_fixed_text(it->second.as_number()));
        } else if (it->second.is_string()) {
            d = clink::config::dec_parse(it->second.as_string());
        }
        if (d) {
            if (auto q = clink::config::dec_rescale(*d, scale))
                it->second = clink::config::make_dec_value(*q);
        }
    }
}

// Schema-aware NDJSON format: the named DECIMAL columns are re-quantised on
// both decode (source ingestion) and encode (sink output, then rendered as a
// clean unquoted number via serialize_output).
inline clink::TextFormat<Row> row_json_text_format_with_decimals(
    std::map<std::string, int> decimal_scales) {
    if (decimal_scales.empty())
        return row_json_text_format();
    auto base = row_json_text_format();
    return clink::TextFormat<Row>{
        .decode = [decimal_scales, base](std::string_view line) -> std::optional<Row> {
            auto r = base.decode(line);
            if (r)
                requantise_row_decimals(*r, decimal_scales);
            return r;
        },
        .encode = [decimal_scales](const Row& r) -> std::string {
            Row q = r;
            requantise_row_decimals(q, decimal_scales);
            clink::config::JsonValue v{clink::config::JsonObject{q.values}};
            return clink::config::serialize_output(v);
        },
    };
}

// SQLOPT-4 projection pushdown: drop every column not in `keep` from a decoded
// row. An empty `keep` means keep all (the projection hint was not set), which
// preserves the pre-pushdown behaviour exactly. Applied source-side so a narrow
// query carries only the columns it needs downstream.
inline void project_row(Row& r, const std::set<std::string>& want) {
    if (want.empty())
        return;
    for (auto it = r.values.begin(); it != r.values.end();) {
        // Always preserve the synthetic "__row_kind" changelog marker (see
        // row_kind.hpp kRowKindField, not includable here without a cycle): it
        // rides on a source row but is never a declared column, so it is absent
        // from the projected set. Dropping it would turn a changelog source into
        // a plain insert stream and break retraction.
        const bool keep_it = it->first == "__row_kind" || want.count(it->first) != 0;
        it = keep_it ? std::next(it) : r.values.erase(it);
    }
}

// Convenience overload taking the keep list as a vector (builds the set once).
// The hot decode path uses the set overload with a set built once per format.
inline void project_row(Row& r, const std::vector<std::string>& keep) {
    if (keep.empty())
        return;
    project_row(r, std::set<std::string>(keep.begin(), keep.end()));
}

// Schema-aware NDJSON format that ALSO narrows each decoded row to the projected
// columns (the optimizer's projected_columns hint). Projection runs AFTER
// decimal requantisation, so a surviving DECIMAL column keeps its exact tag.
// Encode is unchanged (the sink writes whatever columns the row carries).
inline clink::TextFormat<Row> row_json_text_format_projected(
    std::map<std::string, int> decimal_scales, std::vector<std::string> projected) {
    if (projected.empty())
        return row_json_text_format_with_decimals(std::move(decimal_scales));
    auto base = row_json_text_format_with_decimals(std::move(decimal_scales));
    // Build the keep set ONCE and capture it (mirrors the decimal closure), so
    // the per-row decode path does no container allocation.
    std::set<std::string> want(projected.begin(), projected.end());
    return clink::TextFormat<Row>{
        .decode = [base, want = std::move(want)](std::string_view line) -> std::optional<Row> {
            auto r = base.decode(line);
            if (r)
                project_row(*r, want);
            return r;
        },
        .encode = base.encode,
    };
}

}  // namespace clink::sql
