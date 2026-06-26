#pragma once

// make_row_columnar_arrow_batcher - a schema-driven columnar ArrowBatcher
// for the SQL Row type.
//
// The Row channel's default batcher serialises each row as opaque JSON
// in a single value_bytes:binary column. This batcher instead maps each
// declared SQL column to its own typed Arrow column, so the wire/Parquet
// layout is genuinely columnar and externally readable field-by-field.
//
// Given the table's column schema (name + Arrow type), the generated
// batcher emits {event_time:int64(null), col1:type1, col2:type2, ...}
// and converts clink::config::JsonValue <-> the typed Arrow value per
// column. Every value column is nullable; an absent or JSON-null field
// becomes an Arrow null.
//
// v1 type coverage (the SQL types exercised today):
//   BIGINT/INT      -> int64 / int32   (read out of the JSON number; the
//                                        JSON-number-is-double boundary is
//                                        the pre-existing Row limit)
//   DOUBLE/REAL     -> float64 / float32
//   BOOLEAN         -> bool
//   VARCHAR/CHAR    -> utf8
//   DECIMAL(p,s)    -> decimal128(p,s)  (via the exact dec-string path)
//   anything else   -> utf8             (stringified fallback)

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#ifdef CLINK_HAS_ARROW

#include <arrow/api.h>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/sql/row.hpp"

namespace clink::sql {

// One declared column: name + its Arrow logical type.
struct RowColumn {
    std::string name;
    std::shared_ptr<arrow::DataType> type;
};

// Engine-only sidecar column carrying Record::source_partition through the
// columnar path (attached by json_string_to_row_columnar). It is consumed
// columnar by the watermark assigner only and must NEVER materialise into a
// Row value, so the self-describing reader below drops it. Defined here (not in
// json_string_to_row_columnar.hpp) so rows_from_record_batch can see it without
// a circular include. Unlike "__key" / "__row_kind", which are real routing /
// changelog fields that must reach Row.values, this is pure watermark metadata.
inline constexpr const char* kSourcePartitionColumn = "__source_partition";

namespace row_columnar_detail {

// The Arrow type a column is actually stored as: the declared type when
// it is in the supported set, else utf8 (the stringified fallback).
inline std::shared_ptr<arrow::DataType> effective_type(const std::shared_ptr<arrow::DataType>& t) {
    if (!t) {
        return arrow::utf8();
    }
    switch (t->id()) {
        case arrow::Type::INT64:
        case arrow::Type::INT32:
        case arrow::Type::DOUBLE:
        case arrow::Type::FLOAT:
        case arrow::Type::BOOL:
        case arrow::Type::STRING:
        case arrow::Type::DECIMAL128:
            return t;
        default:
            return arrow::utf8();
    }
}

// JSON value for column `name` in row `r`, or nullptr if absent/null.
inline const clink::config::JsonValue* field(const Row& r, const std::string& name) {
    auto it = r.values.find(name);
    if (it == r.values.end() || it->second.is_null()) {
        return nullptr;
    }
    return &it->second;
}

// utf8 rendering of a JSON value (the fallback + VARCHAR path).
inline std::string to_utf8(const clink::config::JsonValue& v) {
    if (clink::config::is_dec_string(v)) {
        return v.as_string().substr(1);  // strip the dec sentinel
    }
    if (v.is_string()) {
        return v.as_string();
    }
    if (v.is_bool()) {
        return v.as_bool() ? "true" : "false";
    }
    if (v.is_number()) {
        const double d = v.as_number();
        if (d == static_cast<double>(static_cast<std::int64_t>(d))) {
            return std::to_string(static_cast<std::int64_t>(d));
        }
        return std::to_string(d);
    }
    return v.serialize(0);
}

inline std::shared_ptr<arrow::Array> build_column(const std::string& name,
                                                  const std::shared_ptr<arrow::DataType>& eff,
                                                  const Batch<Row>& batch) {
    const auto n = static_cast<std::int64_t>(batch.size());
    std::shared_ptr<arrow::Array> out;
    switch (eff->id()) {
        case arrow::Type::INT64: {
            arrow::Int64Builder b;
            (void)b.Reserve(n);
            for (const auto& rec : batch) {
                const auto* v = field(rec.value(), name);
                if (v && v->is_number())
                    (void)b.Append(static_cast<std::int64_t>(v->as_number()));
                else
                    (void)b.AppendNull();
            }
            (void)b.Finish(&out);
            break;
        }
        case arrow::Type::INT32: {
            arrow::Int32Builder b;
            (void)b.Reserve(n);
            for (const auto& rec : batch) {
                const auto* v = field(rec.value(), name);
                if (v && v->is_number())
                    (void)b.Append(static_cast<std::int32_t>(v->as_number()));
                else
                    (void)b.AppendNull();
            }
            (void)b.Finish(&out);
            break;
        }
        case arrow::Type::DOUBLE: {
            arrow::DoubleBuilder b;
            (void)b.Reserve(n);
            for (const auto& rec : batch) {
                const auto* v = field(rec.value(), name);
                if (v && v->is_number())
                    (void)b.Append(v->as_number());
                else
                    (void)b.AppendNull();
            }
            (void)b.Finish(&out);
            break;
        }
        case arrow::Type::FLOAT: {
            arrow::FloatBuilder b;
            (void)b.Reserve(n);
            for (const auto& rec : batch) {
                const auto* v = field(rec.value(), name);
                if (v && v->is_number())
                    (void)b.Append(static_cast<float>(v->as_number()));
                else
                    (void)b.AppendNull();
            }
            (void)b.Finish(&out);
            break;
        }
        case arrow::Type::BOOL: {
            arrow::BooleanBuilder b;
            (void)b.Reserve(n);
            for (const auto& rec : batch) {
                const auto* v = field(rec.value(), name);
                if (v && v->is_bool())
                    (void)b.Append(v->as_bool());
                else
                    (void)b.AppendNull();
            }
            (void)b.Finish(&out);
            break;
        }
        case arrow::Type::DECIMAL128: {
            const auto& dt = static_cast<const arrow::Decimal128Type&>(*eff);
            const int scale = dt.scale();
            arrow::Decimal128Builder b(eff);
            (void)b.Reserve(n);
            for (const auto& rec : batch) {
                const auto* v = field(rec.value(), name);
                std::optional<clink::config::Decimal> d;
                if (v != nullptr) {
                    d = clink::config::as_decimal(*v);
                    if (d) {
                        d = clink::config::dec_rescale(*d, scale);
                    }
                }
                if (d)
                    (void)b.Append(d->unscaled);
                else
                    (void)b.AppendNull();
            }
            (void)b.Finish(&out);
            break;
        }
        default: {  // utf8 (declared STRING or the stringified fallback)
            arrow::StringBuilder b;
            (void)b.Reserve(n);
            for (const auto& rec : batch) {
                const auto* v = field(rec.value(), name);
                if (v != nullptr)
                    (void)b.Append(to_utf8(*v));
                else
                    (void)b.AppendNull();
            }
            (void)b.Finish(&out);
            break;
        }
    }
    return out;
}

// Read row `i` of column `arr` (effective type `eff`) into a JsonValue.
// A null cell yields a JSON-null value.
inline clink::config::JsonValue read_cell(const std::shared_ptr<arrow::DataType>& eff,
                                          const arrow::Array& arr,
                                          std::int64_t i) {
    if (arr.IsNull(i)) {
        return clink::config::JsonValue{};  // null
    }
    switch (eff->id()) {
        case arrow::Type::INT64:
            return clink::config::JsonValue{static_cast<const arrow::Int64Array&>(arr).Value(i)};
        case arrow::Type::INT32:
            return clink::config::JsonValue{
                static_cast<std::int64_t>(static_cast<const arrow::Int32Array&>(arr).Value(i))};
        case arrow::Type::DOUBLE:
            return clink::config::JsonValue{static_cast<const arrow::DoubleArray&>(arr).Value(i)};
        case arrow::Type::FLOAT:
            return clink::config::JsonValue{
                static_cast<double>(static_cast<const arrow::FloatArray&>(arr).Value(i))};
        case arrow::Type::BOOL:
            return clink::config::JsonValue{static_cast<const arrow::BooleanArray&>(arr).Value(i)};
        case arrow::Type::DECIMAL128: {
            const auto& dt = static_cast<const arrow::Decimal128Type&>(*eff);
            const auto& darr = static_cast<const arrow::Decimal128Array&>(arr);
            arrow::Decimal128 dv(darr.GetValue(i));
            return clink::config::make_dec_value(clink::config::Decimal{dv, dt.scale()});
        }
        default:
            return clink::config::JsonValue{
                static_cast<const arrow::StringArray&>(arr).GetString(i)};
    }
}

}  // namespace row_columnar_detail

// Build a columnar ArrowBatcher<Row> from a table's column schema.
inline ArrowBatcher<Row> make_row_columnar_arrow_batcher(std::vector<RowColumn> columns) {
    // Resolve each column's effective Arrow type once.
    struct Resolved {
        std::string name;
        std::shared_ptr<arrow::DataType> eff;
    };
    auto resolved = std::make_shared<std::vector<Resolved>>();
    resolved->reserve(columns.size());
    for (auto& c : columns) {
        resolved->push_back({c.name, row_columnar_detail::effective_type(c.type)});
    }

    auto schema_fn = [resolved] {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(resolved->size() + 1);
        fields.push_back(arrow_event_time_field());
        for (const auto& c : *resolved) {
            fields.push_back(arrow::field(c.name, c.eff, /*nullable=*/true));
        }
        return arrow::schema(fields);
    };

    auto build = [resolved,
                  schema_fn](const Batch<Row>& batch) -> std::shared_ptr<arrow::RecordBatch> {
        const auto n = static_cast<std::int64_t>(batch.size());
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        arrays.reserve(resolved->size() + 1);

        arrow::Int64Builder t_b;
        if (auto s = t_b.Reserve(n); !s.ok())
            return nullptr;
        for (const auto& rec : batch) {
            if (auto s = clink::detail::append_event_time(t_b, rec.event_time()); !s.ok())
                return nullptr;
        }
        std::shared_ptr<arrow::Array> t_arr;
        if (auto s = t_b.Finish(&t_arr); !s.ok())
            return nullptr;
        arrays.push_back(std::move(t_arr));

        for (const auto& c : *resolved) {
            auto arr = row_columnar_detail::build_column(c.name, c.eff, batch);
            if (arr == nullptr)
                return nullptr;
            arrays.push_back(std::move(arr));
        }
        return arrow::RecordBatch::Make(schema_fn(), n, arrays);
    };

    // Eager row decode of a typed RecordBatch into Records - the byte-identical
    // reconstruction a row consumer sees. Used as the columnar Batch's lazy
    // materialize fn below, so it runs ONLY if something actually iterates rows.
    auto materialize = [resolved](const arrow::RecordBatch& b) -> std::vector<Record<Row>> {
        const auto* t_arr = dynamic_cast<const arrow::Int64Array*>(b.column(0).get());
        const auto n = b.num_rows();
        std::vector<Row> rows(static_cast<std::size_t>(n));
        for (std::size_t ci = 0; ci < resolved->size(); ++ci) {
            const auto& c = (*resolved)[ci];
            const auto& col = *b.column(static_cast<int>(ci) + 1);
            for (std::int64_t i = 0; i < n; ++i) {
                rows[static_cast<std::size_t>(i)].values[c.name] =
                    row_columnar_detail::read_cell(c.eff, col, i);
            }
        }
        std::vector<Record<Row>> recs;
        recs.reserve(static_cast<std::size_t>(n));
        for (std::int64_t i = 0; i < n; ++i) {
            std::optional<EventTime> ts;
            if (t_arr != nullptr)
                ts = clink::detail::read_event_time(*t_arr, i);
            if (ts.has_value())
                recs.emplace_back(std::move(rows[static_cast<std::size_t>(i)]), *ts);
            else
                recs.emplace_back(std::move(rows[static_cast<std::size_t>(i)]));
        }
        return recs;
    };

    auto parse = [resolved,
                  materialize](const arrow::RecordBatch& batch) -> std::optional<Batch<Row>> {
        if (batch.num_columns() < static_cast<int>(resolved->size()) + 1)
            return std::nullopt;
        if (dynamic_cast<const arrow::Int64Array*>(batch.column(0).get()) == nullptr)
            return std::nullopt;
        // Keep the data COLUMNAR: hand the typed RecordBatch downstream AS a
        // columnar Batch<Row> (Arrow sidecar set, rows materialised lazily by the
        // fn above only if a row consumer touches them). This is what lets a
        // Parquet-sourced query ride the columnar operator fast paths (filter /
        // project / aggregate / window) instead of decoding every row at the
        // source. Zero data copy: the shared container reuses the SAME column
        // arrays.
        auto shared = arrow::RecordBatch::Make(batch.schema(), batch.num_rows(), batch.columns());
        return Batch<Row>{
            std::move(shared), static_cast<std::size_t>(batch.num_rows()), materialize};
    };

    return ArrowBatcher<Row>{std::move(schema_fn), std::move(build), std::move(parse)};
}

// ---------------------------------------------------------------------
// Self-describing reader: RecordBatch -> Rows using the batch's OWN schema
// ---------------------------------------------------------------------
//
// make_row_columnar_arrow_batcher above needs the table's column schema up
// front. rows_from_record_batch instead reads the field names and types from
// the RecordBatch itself, so it reconstructs Rows from ANY columnar Row batch
// regardless of which table produced it. This is the materialization seam for
// the columnar SQL operators (their output sidecar) and for the sidecar-
// preserving Row wire batcher (received columnar frames). The layout is the
// same one every columnar Row producer emits: {event_time:int64(nullable),
// <named typed value column>...}. Returns nullopt when column 0 is not an
// int64 event-time column or a value column carries a type outside the
// supported set (so read_cell never mis-casts an Arrow array).
// True iff a RecordBatch has the columnar Row layout rows_from_record_batch can
// decode: column 0 is an int64 event-time column and every value column carries
// a supported type. Lets the wire batcher reject an unexpected sidecar at recv
// (a hard protocol error) instead of silently materializing zero rows.
inline bool row_record_batch_supported(const arrow::RecordBatch& batch) {
    if (batch.num_columns() < 1)
        return false;
    if (dynamic_cast<const arrow::Int64Array*>(batch.column(0).get()) == nullptr)
        return false;
    for (int ci = 1; ci < batch.num_columns(); ++ci) {
        switch (batch.schema()->field(ci)->type()->id()) {
            case arrow::Type::INT64:
            case arrow::Type::INT32:
            case arrow::Type::DOUBLE:
            case arrow::Type::FLOAT:
            case arrow::Type::BOOL:
            case arrow::Type::DECIMAL128:
            case arrow::Type::STRING:
                break;
            default:
                return false;
        }
    }
    return true;
}

inline std::optional<std::vector<Record<Row>>> rows_from_record_batch(
    const arrow::RecordBatch& batch) {
    if (batch.num_columns() < 1)
        return std::nullopt;
    const auto* t_arr = dynamic_cast<const arrow::Int64Array*>(batch.column(0).get());
    if (t_arr == nullptr)
        return std::nullopt;
    const auto n = batch.num_rows();
    const int ncol = batch.num_columns();

    struct Col {
        std::string name;
        std::shared_ptr<arrow::DataType> type;
        const arrow::Array* arr;
    };
    std::vector<Col> cols;
    cols.reserve(static_cast<std::size_t>(ncol - 1));
    for (int ci = 1; ci < ncol; ++ci) {
        const auto& f = batch.schema()->field(ci);
        // The engine partition column is watermark metadata, not a Row value -
        // dropping it here keeps it from leaking into output rows (and the sink
        // JSON) when a row-only op materialises a columnar batch that still
        // carries it (e.g. an OVER / semi-join / top-N-per-key op downstream of
        // the columnar row_compute_key, which has no columnar fast path).
        if (f->name() == kSourcePartitionColumn) {
            continue;
        }
        switch (f->type()->id()) {
            case arrow::Type::INT64:
            case arrow::Type::INT32:
            case arrow::Type::DOUBLE:
            case arrow::Type::FLOAT:
            case arrow::Type::BOOL:
            case arrow::Type::DECIMAL128:
            case arrow::Type::STRING:
                break;
            default:
                return std::nullopt;  // unknown type: refuse rather than mis-cast
        }
        cols.push_back({f->name(), f->type(), batch.column(ci).get()});
    }

    std::vector<Record<Row>> out;
    out.reserve(static_cast<std::size_t>(n));
    for (std::int64_t i = 0; i < n; ++i) {
        Row r;
        for (const auto& c : cols) {
            r.values[c.name] = row_columnar_detail::read_cell(c.type, *c.arr, i);
        }
        const auto ts = clink::detail::read_event_time(*t_arr, i);
        if (ts.has_value())
            out.emplace_back(std::move(r), *ts);
        else
            out.emplace_back(std::move(r));
    }
    return out;
}

// MaterializeFn (the closure a columnar Batch<Row> carries for lazy row
// decode) wrapping rows_from_record_batch. An unsupported schema yields an
// empty row vector; callers only attach this to batches with a supported
// layout (operator outputs use the supported type set, and the wire batcher
// validates via row_record_batch_supported and rejects an unexpected sidecar
// at recv BEFORE attaching this), so the empty branch is defensive only.
inline Batch<Row>::MaterializeFn row_materialize_fn() {
    return [](const arrow::RecordBatch& b) -> std::vector<Record<Row>> {
        auto rows = rows_from_record_batch(b);
        return rows ? std::move(*rows) : std::vector<Record<Row>>{};
    };
}

// ---------------------------------------------------------------------
// Sidecar-preserving Row wire batcher
// ---------------------------------------------------------------------
//
// The Row channel's default wire batcher (make_default_arrow_batcher<Row>) wraps
// each row's JSON in a single value_bytes:binary column - it MATERIALIZES a
// columnar batch to rows and re-encodes them as JSON to cross a TM boundary, so
// columnar never survives a shuffle. This batcher instead keeps columnar data
// columnar end-to-end:
//
//   build():  a columnar batch ships its typed Arrow RecordBatch verbatim (no
//             materialization, no re-encode); a row-form batch falls back to the
//             JSON binary layout (lossless for ARRAY/MAP/ROW and every JsonValue
//             shape, unchanged from the default).
//   parse():  a binary-fallback frame decodes to Rows via the codec (unchanged);
//             a typed columnar frame is handed downstream AS a columnar
//             Batch<Row> (Arrow sidecar set, rows lazily materialized) so the
//             receiving operator chain - filter / project / aggregate / window -
//             rides the columnar fast path after the shuffle.
//   schema:   left empty so NetworkChannelSource skips its fixed-schema equality
//             gate (the columnar schema varies per edge; frames are validated
//             structurally by parse instead).
inline ArrowBatcher<Row> make_row_wire_batcher(clink::Codec<Row> codec) {
    auto fallback = clink::make_default_arrow_batcher<Row>(std::move(codec));
    auto fb_build = fallback.build;
    auto fb_parse = fallback.parse;

    auto build = [fb_build](const Batch<Row>& batch) -> std::shared_ptr<arrow::RecordBatch> {
        if (batch.is_columnar() && batch.arrow()) {
            return batch.arrow();  // typed sidecar verbatim - no materialization
        }
        return fb_build(batch);  // row-form: per-record JSON in value_bytes
    };

    auto parse = [fb_parse](const arrow::RecordBatch& batch) -> std::optional<Batch<Row>> {
        // Binary-fallback layout {event_time, value_bytes:binary} -> JSON decode.
        if (batch.num_columns() == 2) {
            const auto& f1 = batch.schema()->field(1);
            if (f1->name() == "value_bytes" && f1->type()->id() == arrow::Type::BINARY) {
                return fb_parse(batch);
            }
        }
        // An unexpected sidecar schema (column 0 not int64, or a value column
        // outside the supported set) is a hard protocol error: returning nullopt
        // makes NetworkChannelSource treat it as a recv failure rather than
        // silently materializing zero rows downstream.
        if (!row_record_batch_supported(batch)) {
            return std::nullopt;
        }
        // Typed columnar layout: keep it columnar. Rebuild a shared_ptr container
        // over the SAME column arrays (zero data copy) so the Batch can own a
        // sidecar; rows are reconstructed lazily by the self-describing reader.
        auto shared = arrow::RecordBatch::Make(batch.schema(), batch.num_rows(), batch.columns());
        return Batch<Row>{
            std::move(shared), static_cast<std::size_t>(batch.num_rows()), row_materialize_fn()};
    };

    return ArrowBatcher<Row>{/*schema=*/{}, std::move(build), std::move(parse)};
}

// ---------------------------------------------------------------------
// Schema (de)serialisation for the job-spec params channel
// ---------------------------------------------------------------------
//
// The SQL planner has the column schema at compile time but the runtime
// factory only receives string params. These encode the schema as
// "name:code;name:code;..." (`;` between columns, `:` between name and
// type code). Codes: i64 i32 f64 f32 bool str dec_<p>_<s>. Assumes plain
// identifier column names (no ';'/':'), same constraint as decimal_columns.

inline std::string row_schema_type_code(const std::shared_ptr<arrow::DataType>& t) {
    if (!t)
        return "str";
    switch (t->id()) {
        case arrow::Type::INT64:
            return "i64";
        case arrow::Type::INT32:
            return "i32";
        case arrow::Type::DOUBLE:
            return "f64";
        case arrow::Type::FLOAT:
            return "f32";
        case arrow::Type::BOOL:
            return "bool";
        case arrow::Type::DECIMAL128: {
            const auto& d = static_cast<const arrow::Decimal128Type&>(*t);
            return "dec_" + std::to_string(d.precision()) + "_" + std::to_string(d.scale());
        }
        case arrow::Type::STRING:
        default:
            return "str";
    }
}

inline std::string serialize_row_schema(const std::vector<RowColumn>& columns) {
    std::string out;
    for (const auto& c : columns) {
        if (!out.empty())
            out += ';';
        out += c.name + ':' + row_schema_type_code(c.type);
    }
    return out;
}

inline std::shared_ptr<arrow::DataType> row_schema_type_from_code(const std::string& code) {
    if (code == "i64")
        return arrow::int64();
    if (code == "i32")
        return arrow::int32();
    if (code == "f64")
        return arrow::float64();
    if (code == "f32")
        return arrow::float32();
    if (code == "bool")
        return arrow::boolean();
    if (code.rfind("dec_", 0) == 0) {
        const auto rest = code.substr(4);
        const auto us = rest.find('_');
        if (us != std::string::npos) {
            try {
                const int p = std::stoi(rest.substr(0, us));
                const int s = std::stoi(rest.substr(us + 1));
                return arrow::decimal128(p, s);
            } catch (...) {
            }
        }
    }
    return arrow::utf8();
}

inline std::vector<RowColumn> parse_row_schema(const std::string& spec) {
    std::vector<RowColumn> cols;
    std::size_t pos = 0;
    while (pos < spec.size()) {
        const auto semi = spec.find(';', pos);
        const std::string entry = spec.substr(pos, semi - pos);
        const auto colon = entry.rfind(':');
        if (colon != std::string::npos) {
            cols.push_back(RowColumn{entry.substr(0, colon),
                                     row_schema_type_from_code(entry.substr(colon + 1))});
        }
        if (semi == std::string::npos)
            break;
        pos = semi + 1;
    }
    return cols;
}

}  // namespace clink::sql

#endif  // CLINK_HAS_ARROW
