#pragma once

#include <cmath>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <simdjson.h>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "clink/config/json.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace clink::sql {

// kSourcePartitionColumn (the engine-only partition sidecar column) is defined
// in row_columnar_batcher.hpp so the self-describing row reader there can drop
// it on materialisation. The assigner reads it columnar via
// with_columnar_partitions; this operator's materialize restores it onto the
// row records, so a row consumer (or the assigner's row fallback) is unchanged.

// Columnar variant of json_string_to_row (Wave 2).
//
// Decodes a batch of NDJSON payload strings DIRECTLY into typed Arrow column
// builders (Wave 2 increment 3) - one JSON parse per line, then each value
// appended straight to its column, with NO intermediate per-record Row / Record
// vector and NO second column-build pass. The emitted Batch<Row> is_columnar(),
// so the downstream columnar fast paths (filter / project / aggregate / window)
// light up on the Kafka path, where they would otherwise stay dormant (the
// source decodes to row form). Rows are reconstructed lazily (the materialize
// closure) only if a row consumer touches them.
//
// Correctness contract - byte-equivalence with json_string_to_row:
//   The direct decoder goes columnar ONLY when every record round-trips exactly
//   - exactly the declared columns (no extra / missing field) and every value
//   representable in its declared Arrow type with an identity round-trip
//   (append_cell_ mirrors make_row_columnar_arrow_batcher's build_column +
//   read_cell). The moment any record is not faithful (wrong type, non-integer
//   or out-of-range int, number-in-string, extra / missing field, non-JSON
//   line), the WHOLE batch falls back to the plain row decode (fmt_, identical
//   to json_string_to_row). FLOAT (lossy double<->float) and DECIMAL128
//   (exact-or-fails coercion) columns, and any "__"-reserved column name, are
//   excluded at the schema level (schema_capable_) and always take the row path.
//
//   A PARTITIONED record (Record::source_partition set - every record from a
//   Kafka topic) is carried through the columnar path via the engine-only
//   kSourcePartitionColumn sidecar column, which the downstream partition-aware
//   watermark assigner reads (with_columnar_partitions) to keep per-partition
//   watermarking without materialising. The lazy materialize closure restores
//   source_partition onto the row records too, so the assigner's row fallback
//   (or any row consumer) stays byte- and metadata-equivalent to
//   json_string_to_row.
class JsonStringToRowColumnarOperator final : public Operator<std::string, Row> {
public:
    explicit JsonStringToRowColumnarOperator(std::vector<RowColumn> columns,
                                             std::string name = "json_string_to_row_columnar")
        // The row fallback MUST use the same schema-aware decode the columnar arms
        // are written against, or this operator would disagree with itself
        // depending on which carrier a batch took.
        : fmt_(row_json_text_format_for_columns(columns)), name_(std::move(name)) {
        resolved_.reserve(columns.size());
        data_fields_.reserve(columns.size() + 1);
        data_fields_.push_back(clink::arrow_event_time_field());  // sidecar column 0
        std::set<std::string> seen;
        for (const auto& c : columns) {
            auto eff = row_columnar_detail::effective_type(c.type);
            if (!columnar_capable_type_(eff->id())) {
                schema_capable_ = false;
            }
            // A declared column in the engine-reserved "__" namespace would
            // collide with the partition sidecar column this operator appends
            // (a duplicate name makes GetFieldIndex ambiguous -> the partition
            // reader silently yields nothing -> per-partition watermarking
            // collapses to a global watermark). Refuse the columnar path for
            // such a schema; the row fallback is always correct.
            if (c.name.rfind("__", 0) == 0) {
                schema_capable_ = false;
            }
            // A duplicated declared name defeats the count+per-key-find
            // faithfulness gate (obj.size() matches the inflated column count
            // and the duplicate key is found twice, so an undeclared field can
            // slip through and be silently dropped). The catalog does not reject
            // duplicate column names, so guard here: force the always-correct
            // row fallback for such a (malformed) schema.
            if (!seen.insert(c.name).second) {
                schema_capable_ = false;
            }
            if (eff->id() == arrow::Type::DECIMAL128) {
                // Needed per line to recover exact digits (see the parse loop).
                decimal_scales_[c.name] = static_cast<const arrow::Decimal128Type&>(*eff).scale();
            }
            data_fields_.push_back(arrow::field(c.name, eff, /*nullable=*/true));
            resolved_.push_back({c.name, std::move(eff)});
        }
    }

    void process(const StreamElement<std::string>& element, Emitter<Row>& out) override {
        if (element.is_data()) {
            const Batch<std::string>& in = element.as_data();

            // Adaptive damper: a batch that fails the faithfulness gate pays
            // a partial columnar parse AND the full row re-parse, so a
            // systematically unfaithful stream (extra fields, wrong types)
            // would run at up to double decode cost forever. After
            // kFallbackThreshold consecutive fallback batches, stop
            // attempting and only probe every kProbeInterval batches; one
            // columnar success re-arms the always-attempt mode. Faithful
            // streams never enter the damper; occasional bad batches reset
            // on the next good one. process() is single-threaded per
            // subtask, so plain members suffice.
            bool attempt = false;
            if (consecutive_fallbacks_ < kFallbackThreshold) {
                attempt = true;
            } else if (++batches_since_probe_ >= kProbeInterval) {
                attempt = true;
                batches_since_probe_ = 0;
            }

            // Fast path: parse straight into typed columns. Returns nullopt and
            // forces the row fallback the moment any record is not faithfully
            // representable (so the result is byte-identical to the row decode).
            if (attempt) {
                if (auto fast = build_columnar_ondemand_(in); fast.has_value()) {
                    consecutive_fallbacks_ = 0;
                    out.emit_data(std::move(*fast));
                    return;
                }
                if (auto columnar = build_columnar_direct_(in); columnar.has_value()) {
                    consecutive_fallbacks_ = 0;
                    out.emit_data(std::move(*columnar));
                    return;
                }
                if (schema_capable_) {
                    // Only data-driven fallbacks feed the damper; an
                    // incapable schema already short-circuits in
                    // build_columnar_direct_ at zero cost.
                    ++consecutive_fallbacks_;
                }
            }

            // Row fallback - identical to json_string_to_row, preserving each
            // record's event_time and source_partition.
            Batch<Row> rows;
            rows.reserve(in.size());
            for (const auto& rec : in) {
                Row row = fmt_.decode(rec.value()).value_or(Row{});
                Record<Row> out_rec = rec.event_time().has_value()
                                          ? Record<Row>(std::move(row), *rec.event_time())
                                          : Record<Row>(std::move(row));
                if (auto p = rec.source_partition(); p.has_value()) {
                    out_rec.set_source_partition(*p);
                }
                rows.push(std::move(out_rec));
            }
            out.emit_data(std::move(rows));
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    std::string name() const override { return name_; }

private:
    struct Resolved {
        std::string name;
        std::shared_ptr<arrow::DataType> eff;
    };

    // Types whose JSON->Arrow->read_cell round-trip is an exact identity for a
    // conforming value, measured against the byte-equivalence REFERENCE: the row
    // decode this operator itself falls back to (fmt_).
    //
    // FLOAT and DECIMAL128 are here because that reference is now SCHEMA-AWARE
    // (row_json_text_format_for_columns): it rounds declared FLOAT columns through
    // float32 and ingests declared DECIMAL columns exactly, quantised to their
    // scale. Both carriers therefore produce the same value for the same input,
    // which is what previously blocked them - the old plain reference kept every
    // number as a full-precision double and never produced a dec-string, so a
    // columnar float32 truncation or dec-string was a visible difference.
    //
    // Temporal types are not listed: effective_type maps them to utf8, so they
    // ride the STRING case (string round-trips) or fall back (a numeric epoch
    // value).
    static bool columnar_capable_type_(arrow::Type::type id) {
        switch (id) {
            case arrow::Type::INT64:
            case arrow::Type::INT32:
            case arrow::Type::DOUBLE:
            case arrow::Type::FLOAT:
            case arrow::Type::DECIMAL128:
            case arrow::Type::BOOL:
            case arrow::Type::STRING:
                return true;
            default:
                return false;
        }
    }

    // Validate one JSON value against its declared column type and append it to
    // the column builder, mirroring row_columnar_detail::build_column's
    // conversions exactly. Returns false if the value is not faithfully
    // representable (the caller then abandons the columnar path). A JSON null
    // (present key, null value) appends a null cell, which round-trips to null -
    // faithful for every capable type.
    static bool append_cell_(const arrow::DataType& eff,
                             arrow::ArrayBuilder* b,
                             const clink::config::JsonValue& v) {
        if (v.is_null()) {
            return b->AppendNull().ok();
        }
        switch (eff.id()) {
            case arrow::Type::INT64: {
                if (!v.is_number()) {
                    return false;
                }
                const double d = v.as_number();
                if (!std::isfinite(d) || d != std::floor(d)) {
                    return false;  // non-integer would be truncated
                }
                if (d < -9223372036854775808.0 || d >= 9223372036854775808.0) {
                    return false;  // outside int64 -> lossy / UB cast
                }
                return static_cast<arrow::Int64Builder*>(b)
                    ->Append(static_cast<std::int64_t>(d))
                    .ok();
            }
            case arrow::Type::INT32: {
                if (!v.is_number()) {
                    return false;
                }
                const double d = v.as_number();
                if (!std::isfinite(d) || d != std::floor(d)) {
                    return false;
                }
                if (d < -2147483648.0 || d > 2147483647.0) {
                    return false;
                }
                return static_cast<arrow::Int32Builder*>(b)
                    ->Append(static_cast<std::int32_t>(d))
                    .ok();
            }
            case arrow::Type::DOUBLE:
                if (!v.is_number()) {
                    return false;
                }
                return static_cast<arrow::DoubleBuilder*>(b)->Append(v.as_number()).ok();
            case arrow::Type::FLOAT: {
                // read_cell returns (double)(float)v, and the row reference
                // coerces the same way (coerce_row_floats), so the round trip is
                // an identity for any number.
                if (!v.is_number()) {
                    return false;
                }
                return static_cast<arrow::FloatBuilder*>(b)
                    ->Append(static_cast<float>(v.as_number()))
                    .ok();
            }
            case arrow::Type::DECIMAL128: {
                // The scale-quantised decimal the row reference would carry for
                // this value, from the one shared resolver. nullopt means the
                // value does not denote a decimal at this scale - the row path
                // would leave it untouched (still a number or string) while this
                // path can only store a decimal, so the batch must fall back
                // rather than store something else.
                const auto d =
                    row_decimal_for(v, static_cast<const arrow::Decimal128Type&>(eff).scale());
                if (!d) {
                    return false;
                }
                return static_cast<arrow::Decimal128Builder*>(b)->Append(d->unscaled).ok();
            }
            case arrow::Type::BOOL:
                if (!v.is_bool()) {
                    return false;
                }
                return static_cast<arrow::BooleanBuilder*>(b)->Append(v.as_bool()).ok();
            case arrow::Type::STRING:
                // A number/bool in a string column would be stringified by
                // to_utf8; a dec-string would have its sentinel stripped.
                if (!v.is_string() || clink::config::is_dec_string(v)) {
                    return false;
                }
                return static_cast<arrow::StringBuilder*>(b)->Append(v.as_string()).ok();
            default:
                return false;  // unreachable: schema_capable_ excludes these
        }
    }

    // Column index for a field name. Linear over a handful of declared columns,
    // which beats a hash for these widths and keeps the comparison to a length
    // check plus a short memcmp.
    [[nodiscard]] int column_index_(std::string_view name) const noexcept {
        for (std::size_t i = 0; i < resolved_.size(); ++i) {
            if (resolved_[i].name.size() == name.size() && resolved_[i].name == name) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }

    // Append one simdjson on-demand value to a typed builder, mirroring
    // append_cell_'s acceptance rules exactly. Returns false to bail the batch.
    //
    // Anything this cannot decide is refused rather than guessed: a false return
    // costs a fallback to the DOM path (and, failing that, to rows), which is
    // always correct. That asymmetry is deliberate - the columnar carrier only
    // ever fires where it provably matches the row decode.
    static bool append_ondemand_(const arrow::DataType& eff,
                                 arrow::ArrayBuilder* b,
                                 simdjson::ondemand::value& v,
                                 int decimal_scale) {
        if (v.is_null()) {
            return b->AppendNull().ok();
        }
        switch (eff.id()) {
            case arrow::Type::INT64: {
                std::int64_t out{};
                if (v.get_int64().get(out) == simdjson::SUCCESS) {
                    return static_cast<arrow::Int64Builder*>(b)->Append(out).ok();
                }
                // A JSON numeral like 5.0 is integral to the row decode (which
                // parses every number as a double), so accept it the same way
                // rather than diverging.
                double d{};
                if (v.get_double().get(d) != simdjson::SUCCESS)
                    return false;
                if (!std::isfinite(d) || d != std::floor(d))
                    return false;
                if (d < -9223372036854775808.0 || d >= 9223372036854775808.0)
                    return false;
                return static_cast<arrow::Int64Builder*>(b)
                    ->Append(static_cast<std::int64_t>(d))
                    .ok();
            }
            case arrow::Type::INT32: {
                double d{};
                if (v.get_double().get(d) != simdjson::SUCCESS)
                    return false;
                if (!std::isfinite(d) || d != std::floor(d))
                    return false;
                if (d < -2147483648.0 || d > 2147483647.0)
                    return false;
                return static_cast<arrow::Int32Builder*>(b)
                    ->Append(static_cast<std::int32_t>(d))
                    .ok();
            }
            case arrow::Type::DOUBLE: {
                double d{};
                if (v.get_double().get(d) != simdjson::SUCCESS)
                    return false;
                return static_cast<arrow::DoubleBuilder*>(b)->Append(d).ok();
            }
            case arrow::Type::FLOAT: {
                double d{};
                if (v.get_double().get(d) != simdjson::SUCCESS)
                    return false;
                return static_cast<arrow::FloatBuilder*>(b)->Append(static_cast<float>(d)).ok();
            }
            case arrow::Type::BOOL: {
                bool x{};
                if (v.get_bool().get(x) != simdjson::SUCCESS)
                    return false;
                return static_cast<arrow::BooleanBuilder*>(b)->Append(x).ok();
            }
            case arrow::Type::STRING: {
                std::string_view sv;
                if (v.get_string().get(sv) != simdjson::SUCCESS)
                    return false;
                return static_cast<arrow::StringBuilder*>(b)
                    ->Append(sv.data(), static_cast<std::int32_t>(sv.size()))
                    .ok();
            }
            case arrow::Type::DECIMAL128: {
                // The exact digits come from the raw token, read here during the
                // walk - so unlike the DOM path there is no second scan of the
                // line to recover them.
                clink::config::JsonValue jv;
                if (v.type() == simdjson::ondemand::json_type::number) {
                    const std::string_view tok = v.raw_json_token();
                    std::size_t n = 0;
                    while (n < tok.size() &&
                           (std::isdigit(static_cast<unsigned char>(tok[n])) || tok[n] == '-' ||
                            tok[n] == '+' || tok[n] == '.' || tok[n] == 'e' || tok[n] == 'E')) {
                        ++n;
                    }
                    auto d = clink::config::dec_parse(tok.substr(0, n));
                    if (!d)
                        return false;
                    jv = clink::config::make_dec_value(*d);
                } else {
                    std::string_view sv;
                    if (v.get_string().get(sv) != simdjson::SUCCESS)
                        return false;
                    jv = clink::config::JsonValue{std::string(sv)};
                }
                const auto q = row_decimal_for(jv, decimal_scale);
                if (!q)
                    return false;
                return static_cast<arrow::Decimal128Builder*>(b)->Append(q->unscaled).ok();
            }
            default:
                return false;
        }
    }

    // On-demand decode: walk each document's fields ONCE, straight into the typed
    // builders, with no intermediate JsonObject and no per-column lookup of a
    // sorted vector.
    //
    // The DOM path it replaces claimed to decode "directly into typed Arrow
    // column builders", but built a full generic JsonObject per record first -
    // a std::string per key, a heap string for any value past SSO, a sorted
    // insert - and then binary-searched it once per declared column. Profiling
    // the q0 chain ranked from_dom, the pair-vector growth and memcmp far above
    // simdjson itself, which was about 10% of decode.
    //
    // Returns nullopt to fall back to the DOM path, which then falls back to
    // rows. Both fallbacks are correct, so any case this refuses is merely slow.
    std::optional<Batch<Row>> build_columnar_ondemand_(const Batch<std::string>& in) const {
        if (!schema_capable_) {
            return std::nullopt;
        }
        const auto n = static_cast<std::int64_t>(in.size());
        auto* pool = arrow::default_memory_pool();

        arrow::Int64Builder t_b(pool);
        if (!t_b.Reserve(n).ok()) {
            return std::nullopt;
        }
        std::vector<std::unique_ptr<arrow::ArrayBuilder>> col_b(resolved_.size());
        for (std::size_t ci = 0; ci < resolved_.size(); ++ci) {
            if (!arrow::MakeBuilder(pool, resolved_[ci].eff, &col_b[ci]).ok()) {
                return std::nullopt;
            }
            (void)col_b[ci]->Reserve(n);
        }

        std::vector<std::optional<std::int32_t>> parts;
        parts.reserve(in.size());
        bool any_partition = false;
        std::vector<char> seen(resolved_.size(), 0);

        for (const auto& rec : in) {
            // On-demand requires SIMDJSON_PADDING readable bytes past the end.
            // Reusing one grown-once buffer keeps that off the per-record path;
            // a padded_string per line would allocate for every record.
            const std::string& line = rec.value();
            pad_buf_.assign(line.begin(), line.end());
            pad_buf_.resize(line.size() + simdjson::SIMDJSON_PADDING, '\0');

            simdjson::ondemand::document doc;
            if (parser_.iterate(pad_buf_.data(), line.size(), pad_buf_.size()).get(doc) !=
                simdjson::SUCCESS) {
                return std::nullopt;
            }
            simdjson::ondemand::object obj;
            if (doc.get_object().get(obj) != simdjson::SUCCESS) {
                return std::nullopt;
            }

            std::fill(seen.begin(), seen.end(), 0);
            std::size_t filled = 0;
            for (auto field : obj) {
                std::string_view key;
                if (field.unescaped_key().get(key) != simdjson::SUCCESS) {
                    return std::nullopt;
                }
                const int ci = column_index_(key);
                if (ci < 0) {
                    return std::nullopt;  // undeclared field: the row decode would keep it
                }
                if (seen[static_cast<std::size_t>(ci)]) {
                    return std::nullopt;  // duplicate key
                }
                simdjson::ondemand::value val;
                if (field.value().get(val) != simdjson::SUCCESS) {
                    return std::nullopt;
                }
                int scale = 0;
                if (resolved_[static_cast<std::size_t>(ci)].eff->id() == arrow::Type::DECIMAL128) {
                    scale = static_cast<const arrow::Decimal128Type&>(
                                *resolved_[static_cast<std::size_t>(ci)].eff)
                                .scale();
                }
                if (!append_ondemand_(*resolved_[static_cast<std::size_t>(ci)].eff,
                                      col_b[static_cast<std::size_t>(ci)].get(),
                                      val,
                                      scale)) {
                    return std::nullopt;
                }
                seen[static_cast<std::size_t>(ci)] = 1;
                ++filled;
            }
            if (filled != resolved_.size()) {
                return std::nullopt;  // missing declared column
            }
            if (!clink::detail::append_event_time(t_b, rec.event_time()).ok()) {
                return std::nullopt;
            }
            auto p = rec.source_partition();
            if (p.has_value()) {
                any_partition = true;
            }
            parts.push_back(p);
        }

        std::vector<std::shared_ptr<arrow::Array>> arrays;
        arrays.reserve(resolved_.size() + 1);
        std::shared_ptr<arrow::Array> t_arr;
        if (!t_b.Finish(&t_arr).ok()) {
            return std::nullopt;
        }
        arrays.push_back(std::move(t_arr));
        for (std::size_t ci = 0; ci < resolved_.size(); ++ci) {
            std::shared_ptr<arrow::Array> a;
            if (!col_b[ci]->Finish(&a).ok()) {
                return std::nullopt;
            }
            arrays.push_back(std::move(a));
        }
        auto rb = arrow::RecordBatch::Make(arrow::schema(data_fields_), n, std::move(arrays));
        return wrap_columnar_(std::move(rb), parts, any_partition);
    }

    // Single-pass direct decode: parse each line once and append straight to the
    // typed column builders. Returns nullopt (forcing the row fallback) for a
    // non-capable schema or the moment any record is not faithful.
    std::optional<Batch<Row>> build_columnar_direct_(const Batch<std::string>& in) const {
        if (!schema_capable_) {
            return std::nullopt;
        }
        const auto n = static_cast<std::int64_t>(in.size());
        auto* pool = arrow::default_memory_pool();

        arrow::Int64Builder t_b(pool);
        if (!t_b.Reserve(n).ok()) {
            return std::nullopt;
        }
        std::vector<std::unique_ptr<arrow::ArrayBuilder>> col_b(resolved_.size());
        for (std::size_t ci = 0; ci < resolved_.size(); ++ci) {
            if (!arrow::MakeBuilder(pool, resolved_[ci].eff, &col_b[ci]).ok()) {
                return std::nullopt;
            }
            (void)col_b[ci]->Reserve(n);
        }

        std::vector<std::optional<std::int32_t>> parts;
        parts.reserve(in.size());
        bool any_partition = false;

        for (const auto& rec : in) {
            clink::config::JsonValue jv;
            try {
                jv = clink::config::parse(rec.value());
            } catch (...) {
                return std::nullopt;  // unparseable -> row fallback (== json_string_to_row)
            }
            if (!jv.is_object()) {
                return std::nullopt;
            }
            const auto& obj = jv.as_object();
            if (obj.size() != resolved_.size()) {
                return std::nullopt;  // extra or missing field
            }
            // A DECIMAL column's digits cannot be recovered from the parsed value:
            // the generic parse has already rounded the numeral to a double. Re-read
            // the untruncated token straight from the line, exactly as the row
            // decode does (ingest_exact_decimals), so a money column is ingested
            // exactly on this carrier too. Only for schemas that declare one.
            std::map<std::string, std::string> dec_tokens;
            if (!decimal_scales_.empty()) {
                dec_tokens = clink::config::raw_number_tokens(rec.value(), decimal_scales_);
            }
            for (std::size_t ci = 0; ci < resolved_.size(); ++ci) {
                auto it = obj.find(resolved_[ci].name);
                if (it == obj.end()) {
                    return std::nullopt;  // declared column absent
                }
                const clink::config::JsonValue* value = &it->second;
                clink::config::JsonValue exact;
                if (!dec_tokens.empty() && it->second.is_number()) {
                    auto tok = dec_tokens.find(resolved_[ci].name);
                    if (tok != dec_tokens.end()) {
                        if (auto d = clink::config::dec_parse(tok->second)) {
                            exact = clink::config::make_dec_value(*d);
                            value = &exact;
                        }
                    }
                }
                if (!append_cell_(*resolved_[ci].eff, col_b[ci].get(), *value)) {
                    return std::nullopt;
                }
            }
            if (!clink::detail::append_event_time(t_b, rec.event_time()).ok()) {
                return std::nullopt;
            }
            auto p = rec.source_partition();
            if (p.has_value()) {
                any_partition = true;
            }
            parts.push_back(p);
        }

        std::vector<std::shared_ptr<arrow::Array>> arrays;
        arrays.reserve(resolved_.size() + 1);
        std::shared_ptr<arrow::Array> t_arr;
        if (!t_b.Finish(&t_arr).ok()) {
            return std::nullopt;
        }
        arrays.push_back(std::move(t_arr));
        for (std::size_t ci = 0; ci < resolved_.size(); ++ci) {
            std::shared_ptr<arrow::Array> a;
            if (!col_b[ci]->Finish(&a).ok()) {
                return std::nullopt;
            }
            arrays.push_back(std::move(a));
        }
        auto rb = arrow::RecordBatch::Make(arrow::schema(data_fields_), n, std::move(arrays));
        return wrap_columnar_(std::move(rb), parts, any_partition);
    }

    // Append the engine-only kSourcePartitionColumn (when any record is
    // partitioned) and wrap the RecordBatch as a columnar Batch<Row> whose lazy
    // materialize restores the declared columns (byte-identical to
    // make_row_columnar_arrow_batcher's materialize) AND source_partition.
    std::optional<Batch<Row>> wrap_columnar_(std::shared_ptr<arrow::RecordBatch> rb,
                                             const std::vector<std::optional<std::int32_t>>& parts,
                                             bool any_partition) const {
        if (any_partition) {
            arrow::Int32Builder pb;
            if (!pb.Reserve(static_cast<std::int64_t>(parts.size())).ok()) {
                return std::nullopt;
            }
            for (const auto& p : parts) {
                if (p.has_value()) {
                    if (!pb.Append(*p).ok()) {
                        return std::nullopt;
                    }
                } else if (!pb.AppendNull().ok()) {
                    return std::nullopt;
                }
            }
            std::shared_ptr<arrow::Array> p_arr;
            if (!pb.Finish(&p_arr).ok()) {
                return std::nullopt;
            }
            auto added = rb->AddColumn(
                rb->num_columns(), arrow::field(kSourcePartitionColumn, arrow::int32()), p_arr);
            if (!added.ok()) {
                return std::nullopt;
            }
            rb = *added;
        }

        const auto n = static_cast<std::size_t>(rb->num_rows());
        auto resolved = resolved_;  // capture by value for lifetime safety
        auto materialize = [resolved](const arrow::RecordBatch& b) -> std::vector<Record<Row>> {
            const auto* t_arr = dynamic_cast<const arrow::Int64Array*>(b.column(0).get());
            const int part_idx = b.schema()->GetFieldIndex(kSourcePartitionColumn);
            const arrow::Int32Array* p_arr =
                part_idx >= 0 ? dynamic_cast<const arrow::Int32Array*>(b.column(part_idx).get())
                              : nullptr;
            const auto rn = b.num_rows();
            std::vector<Row> decoded(static_cast<std::size_t>(rn));
            for (std::size_t ci = 0; ci < resolved.size(); ++ci) {
                const auto& c = resolved[ci];
                const auto& col = *b.column(static_cast<int>(ci) + 1);
                for (std::int64_t i = 0; i < rn; ++i) {
                    decoded[static_cast<std::size_t>(i)].values[c.name] =
                        row_columnar_detail::read_cell(c.eff, col, i);
                }
            }
            std::vector<Record<Row>> recs;
            recs.reserve(static_cast<std::size_t>(rn));
            for (std::int64_t i = 0; i < rn; ++i) {
                std::optional<EventTime> ts;
                if (t_arr != nullptr) {
                    ts = clink::detail::read_event_time(*t_arr, i);
                }
                Record<Row> rec =
                    ts.has_value()
                        ? Record<Row>(std::move(decoded[static_cast<std::size_t>(i)]), *ts)
                        : Record<Row>(std::move(decoded[static_cast<std::size_t>(i)]));
                if (p_arr != nullptr && !p_arr->IsNull(i)) {
                    rec.set_source_partition(p_arr->Value(i));
                }
                recs.push_back(std::move(rec));
            }
            return recs;
        };
        return Batch<Row>{std::move(rb), n, std::move(materialize)};
    }

    clink::TextFormat<Row> fmt_;
    // Declared DECIMAL columns and their scales; empty for a schema without one,
    // which is what keeps the per-line token scan off every other schema's path.
    std::map<std::string, int> decimal_scales_;
    // On-demand parser and its padded scratch buffer. process() is
    // single-threaded per subtask, so plain mutable members suffice; both are
    // reused across records so the fast path allocates nothing per line.
    mutable simdjson::ondemand::parser parser_;
    mutable std::vector<char> pad_buf_;
    std::string name_;
    std::vector<Resolved> resolved_;
    std::vector<std::shared_ptr<arrow::Field>> data_fields_;  // [event_time, declared...]
    bool schema_capable_{true};

    // Adaptive damper state (see process). Tuning: 8 consecutive bad
    // batches (~2k records at the default batch shape) to conclude the
    // stream is systematically unfaithful; probe every 64th batch after
    // that, bounding the wasted partial parse at ~1.6% while re-arming
    // within seconds when the data recovers.
    static constexpr int kFallbackThreshold = 8;
    static constexpr std::uint32_t kProbeInterval = 64;
    int consecutive_fallbacks_{0};
    std::uint32_t batches_since_probe_{0};
};

}  // namespace clink::sql
