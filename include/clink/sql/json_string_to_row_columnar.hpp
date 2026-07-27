#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <set>
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
    // Defined out of line alongside the on-demand decoder: the constructor's
    // cleanup path has to destroy od_, which needs the complete type.
    // `projected` is an optional keep-list of column names. Empty means "keep
    // everything", which is the behaviour every caller had before it existed.
    //
    // It is deliberately NOT a narrowing of `columns`. The faithfulness gate that makes
    // the columnar carrier safe works by requiring the JSON object's fields to match the
    // DECLARED schema exactly - an undeclared field bails the batch, because the row
    // decode would have kept it. Narrowing the declared schema instead would make every
    // record's unprojected fields "undeclared", so every batch would fail the gate, pay a
    // partial columnar parse plus a full row re-parse, and after eight consecutive
    // failures the damper would stop trying columnar on 63 of every 64 batches -
    // surrendering the measured +69% columnar gain to buy a narrower batch. So the full
    // schema stays in the gate and `projected` only decides which columns get BUILT:
    // declared-but-unprojected means consume-and-discard.
    explicit JsonStringToRowColumnarOperator(std::vector<RowColumn> columns,
                                             std::vector<std::string> projected = {},
                                             std::string name = "json_string_to_row_columnar");

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

    // Out of line: od_ points at an incomplete type here.
    ~JsonStringToRowColumnarOperator() override;

private:
    struct Resolved {
        std::string name;
        std::shared_ptr<arrow::DataType> eff;
        // Cached from `eff` at construction. The decode loop reached for eff->id()
        // twice per FIELD - once to test for DECIMAL128 to pick up a scale, once to
        // dispatch the append - which is a pointer chase and a virtual call each, a
        // dozen or more per record, for a value that cannot change after construction.
        arrow::Type::type type_id{arrow::Type::NA};
        // Only meaningful for DECIMAL128; 0 elsewhere.
        std::int32_t scale{0};
        // False when the column is declared but not in the projection keep-list: its
        // value is still parsed and type-checked by the gate, then discarded instead of
        // appended to a builder, and it is omitted from the emitted Arrow schema.
        bool projected{true};
        // Precomputed name length, so the lookup's reject test is two integer
        // comparisons before it touches any bytes.
        std::uint32_t name_len{0};
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
    // which beats a hash for these widths, with a length and first-byte filter in
    // front of the comparison so most candidates are rejected without a memcmp.
    // Byte-compare two names of KNOWN-equal length, inline, without calling memcmp.
    // Column names are a handful of bytes, and at that width the call to
    // _platform_memcmp costs more than the comparison: it was 8.8% of decode self
    // time on top of the lookup frame itself. An explicit loop the compiler can
    // unroll and vectorise removes the call entirely. No over-read - it never
    // touches a byte past `n`.
    [[nodiscard]] static bool name_eq_(const char* a, const char* b, std::size_t n) noexcept {
        for (std::size_t i = 0; i < n; ++i) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] int column_index_(std::string_view name) const noexcept {
        for (std::size_t i = 0; i < resolved_.size(); ++i) {
            const auto& r = resolved_[i];
            if (r.name_len != name.size() || r.name[0] != name[0]) {
                continue;
            }
            if (name_eq_(r.name.data(), name.data(), name.size())) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }

    // Column index for the field at POSITION k of the object, using the column that
    // position matched last time as the first guess.
    //
    // A stream of JSON records is homogeneous in practice - the producer emits the
    // same fields in the same order every time - so position k lands on the same
    // column on essentially every record after the first. That turns the lookup from
    // a scan averaging half the columns (21 string comparisons per nexmark bid
    // record, which showed up as ~10% of decode self time in memcmp) into one
    // comparison.
    //
    // The guess is always VERIFIED by comparing the name, and a miss falls through to
    // the full scan, so this can never change which column a field decodes into - only
    // how many comparisons it takes to find it. Field order is allowed to vary
    // per record; it just costs the scan when it does.
    [[nodiscard]] int column_index_at_(std::string_view name, std::size_t k) const noexcept {
        if (k < pos_hint_.size()) {
            const auto guess = pos_hint_[k];
            const auto& r = resolved_[guess];  // pos_hint_ only ever holds valid indices
            if (r.name_len == name.size() && name_eq_(r.name.data(), name.data(), name.size())) {
                return static_cast<int>(guess);
            }
        }
        return column_index_miss_(name, k);
    }

    // The out-of-order / first-record path, deliberately kept OUT of line and marked
    // cold. Inlined, its scan loop made the whole lookup too large for the compiler to
    // inline into the decode loop at all, and the lookup showed as its own 13% frame;
    // splitting it leaves the hot path a length compare and a short byte compare.
    [[gnu::noinline, gnu::cold]] int column_index_miss_(std::string_view name,
                                                        std::size_t k) const noexcept {
        const int found = column_index_(name);
        if (found >= 0 && k < pos_hint_.size()) {
            pos_hint_[k] = static_cast<std::uint32_t>(found);
        }
        return found;
    }

    // On-demand decode: walk each document's fields ONCE, straight into the typed
    // builders, with no intermediate JsonObject and no per-column lookup of a
    // sorted vector. Defined in src/sql/json_string_to_row_columnar.cpp.
    //
    // Out of line ON PURPOSE. It needs simdjson's on-demand types, and simdjson is
    // a private implementation detail of the engine - including it here would put
    // it on the include path of every downstream consumer of clink's public
    // headers, which is how this first broke the container build while compiling
    // fine locally.
    //
    // Returns nullopt to fall back to the DOM path below, which then falls back
    // to rows. Both fallbacks are correct, so any case this refuses is merely slow.
    std::optional<Batch<Row>> build_columnar_ondemand_(const Batch<std::string>& in) const;

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
        // No builder for an unprojected column - see the on-demand arm; the two arms must
        // make the same projection decision or one batch's schema would depend on which of
        // them decoded it.
        std::vector<std::unique_ptr<arrow::ArrayBuilder>> col_b(resolved_.size());
        for (std::size_t ci = 0; ci < resolved_.size(); ++ci) {
            if (!resolved_[ci].projected) {
                continue;
            }
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
                // Declared but unprojected: its presence is still required (the check
                // above), it is simply not stored.
                if (!resolved_[ci].projected) {
                    continue;
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
            if (!resolved_[ci].projected) {
                continue;  // matches data_fields_, which also skipped it
            }
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
            // Resolve BY NAME, once per batch. resolved[] carries every DECLARED column -
            // unprojected ones included, because the decode gate needs them - while the
            // batch carries only the projected ones, so a declared index is not a batch
            // index. Name lookup is immune to that, and to reordering, where tracking the
            // output position by hand is only correct as long as both loops agree.
            for (const auto& c : resolved) {
                const int idx = b.schema()->GetFieldIndex(c.name);
                if (idx < 0) {
                    continue;  // unprojected, so not in the batch
                }
                const auto& col = *b.column(idx);
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
    // On-demand parser + padded scratch buffer, behind an opaque type so the
    // header names no simdjson type. Created lazily by the decoder and reused
    // across records, so the fast path allocates nothing per line. process() is
    // single-threaded per subtask, so a plain mutable member suffices.
    struct Ondemand;
    mutable std::unique_ptr<Ondemand> od_;
    // Field-position -> column-index guess for column_index_at_. Mutable because it
    // is a pure cache: it changes nothing about the decode's result, only the number
    // of comparisons taken to reach it. Sized to the column count in the constructor.
    mutable std::vector<std::uint32_t> pos_hint_;
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
