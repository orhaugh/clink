// Out-of-line half of JsonStringToRowColumnarOperator: the simdjson on-demand
// decoder.
//
// It lives here rather than in the header because simdjson is a PRIVATE
// implementation detail of the engine. Including <simdjson.h> from a public
// header puts it on the include path of everything that consumes clink's
// headers - which compiled fine locally, where the SQL target already had
// simdjson's include directory, and broke the container build with
// "fatal error: simdjson.h: No such file or directory" the moment a translation
// unit without that path compiled it.

#include "clink/sql/json_string_to_row_columnar.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <set>
#include <simdjson.h>
#include <string_view>
#include <vector>

#include "clink/config/decimal.hpp"

namespace clink::sql {

// Parser + padded scratch buffer, reused across records so the fast path
// allocates nothing per line. On-demand requires SIMDJSON_PADDING readable bytes
// past the end of the document, which is why the line is copied into a
// grown-once buffer rather than parsed in place.
struct JsonStringToRowColumnarOperator::Ondemand {
    simdjson::ondemand::parser parser;
    std::vector<char> pad;
};

JsonStringToRowColumnarOperator::JsonStringToRowColumnarOperator(std::vector<RowColumn> columns,
                                                                 std::vector<std::string> projected,
                                                                 std::string name)
    // The row fallback MUST use the same schema-aware decode the columnar arms
    // are written against, or this operator would disagree with itself depending on which
    // carrier a batch took - so it takes the SAME projection. row_json_text_format_projected
    // already preserves the synthetic __row_kind changelog marker, which is why the keep-list
    // goes through it rather than being applied by hand.
    : fmt_(row_json_text_format_for_columns_projected(columns, projected)), name_(std::move(name)) {
    // Empty keep-list means keep everything.
    const bool project = !projected.empty();
    const std::set<std::string> keep(projected.begin(), projected.end());
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
        // A backslash in a declared name is the one case where comparing a RAW JSON
        // key against it could match while the key's UNESCAPED form differs, which
        // would put a field in the wrong column. Vanishingly rare and refused rather
        // than reasoned about; the row fallback handles such a schema correctly.
        if (c.name.find('\\') != std::string::npos) {
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
        const auto tid = eff->id();
        const std::int32_t sc = tid == arrow::Type::DECIMAL128
                                    ? static_cast<const arrow::Decimal128Type&>(*eff).scale()
                                    : 0;
        const bool want = !project || keep.count(c.name) > 0;
        // Only projected columns appear in the emitted Arrow schema. An unprojected one
        // still gets a Resolved entry, because the field walk must recognise it as
        // DECLARED - and type-check its value - rather than treat it as undeclared and bail
        // the whole batch.
        if (want) {
            data_fields_.push_back(arrow::field(c.name, eff, /*nullable=*/true));
        }
        resolved_.push_back(
            {c.name, std::move(eff), tid, sc, want, static_cast<std::uint32_t>(c.name.size())});
    }
    // Identity is the right initial guess: a producer emitting fields in declared
    // order is the common case, so the first record already hits.
    pos_hint_.resize(resolved_.size());
    for (std::size_t i = 0; i < pos_hint_.size(); ++i) {
        pos_hint_[i] = static_cast<std::uint32_t>(i);
    }
}

JsonStringToRowColumnarOperator::~JsonStringToRowColumnarOperator() = default;

namespace {

// Append one on-demand value to a typed builder, mirroring append_cell_'s
// acceptance rules exactly. Returns false to bail the batch.
//
// Takes the Arrow type id by value rather than the DataType, so the dispatch reads a
// cached byte instead of chasing a shared_ptr and calling a virtual id() per field.
//
// Anything this cannot decide is refused rather than guessed: a false return
// costs a fallback to the DOM path (and, failing that, to rows), which is always
// correct. That asymmetry is deliberate - the columnar carrier only ever fires
// where it provably matches the row decode.
bool append_ondemand(arrow::Type::type type_id,
                     arrow::ArrayBuilder* b,
                     simdjson::ondemand::value& v,
                     std::int32_t decimal_scale) {
    if (v.is_null()) {
        return b->AppendNull().ok();
    }
    switch (type_id) {
        case arrow::Type::INT64: {
            std::int64_t out{};
            if (v.get_int64().get(out) == simdjson::SUCCESS) {
                return static_cast<arrow::Int64Builder*>(b)->Append(out).ok();
            }
            // A numeral like 5.0 is integral to the row decode, which parses
            // every JSON number as a double - accept it the same way rather
            // than diverging.
            double d{};
            if (v.get_double().get(d) != simdjson::SUCCESS) {
                return false;
            }
            if (!std::isfinite(d) || d != std::floor(d)) {
                return false;
            }
            if (d < -9223372036854775808.0 || d >= 9223372036854775808.0) {
                return false;
            }
            return static_cast<arrow::Int64Builder*>(b)->Append(static_cast<std::int64_t>(d)).ok();
        }
        case arrow::Type::INT32: {
            double d{};
            if (v.get_double().get(d) != simdjson::SUCCESS) {
                return false;
            }
            if (!std::isfinite(d) || d != std::floor(d)) {
                return false;
            }
            if (d < -2147483648.0 || d > 2147483647.0) {
                return false;
            }
            return static_cast<arrow::Int32Builder*>(b)->Append(static_cast<std::int32_t>(d)).ok();
        }
        case arrow::Type::DOUBLE: {
            double d{};
            if (v.get_double().get(d) != simdjson::SUCCESS) {
                return false;
            }
            return static_cast<arrow::DoubleBuilder*>(b)->Append(d).ok();
        }
        case arrow::Type::FLOAT: {
            double d{};
            if (v.get_double().get(d) != simdjson::SUCCESS) {
                return false;
            }
            return static_cast<arrow::FloatBuilder*>(b)->Append(static_cast<float>(d)).ok();
        }
        case arrow::Type::BOOL: {
            bool x{};
            if (v.get_bool().get(x) != simdjson::SUCCESS) {
                return false;
            }
            return static_cast<arrow::BooleanBuilder*>(b)->Append(x).ok();
        }
        case arrow::Type::STRING: {
            std::string_view sv;
            if (v.get_string().get(sv) != simdjson::SUCCESS) {
                return false;
            }
            return static_cast<arrow::StringBuilder*>(b)
                ->Append(sv.data(), static_cast<std::int32_t>(sv.size()))
                .ok();
        }
        case arrow::Type::DECIMAL128: {
            // Exact digits from the raw token, read during this same walk - so
            // unlike the DOM path there is no second scan of the line for them.
            clink::config::JsonValue jv;
            if (v.type() == simdjson::ondemand::json_type::number) {
                const std::string_view tok = v.raw_json_token();
                std::size_t n = 0;
                while (n < tok.size() &&
                       (std::isdigit(static_cast<unsigned char>(tok[n])) != 0 || tok[n] == '-' ||
                        tok[n] == '+' || tok[n] == '.' || tok[n] == 'e' || tok[n] == 'E')) {
                    ++n;
                }
                auto d = clink::config::dec_parse(tok.substr(0, n));
                if (!d) {
                    return false;
                }
                jv = clink::config::make_dec_value(*d);
            } else {
                std::string_view sv;
                if (v.get_string().get(sv) != simdjson::SUCCESS) {
                    return false;
                }
                jv = clink::config::JsonValue{std::string(sv)};
            }
            const auto q = row_decimal_for(jv, decimal_scale);
            if (!q) {
                return false;
            }
            return static_cast<arrow::Decimal128Builder*>(b)->Append(q->unscaled).ok();
        }
        default:
            return false;
    }
}

}  // namespace

std::optional<Batch<Row>> JsonStringToRowColumnarOperator::build_columnar_ondemand_(
    const Batch<std::string>& in) const {
    if (!schema_capable_) {
        return std::nullopt;
    }
    if (!od_) {
        od_ = std::make_unique<Ondemand>();
    }
    const auto n = static_cast<std::int64_t>(in.size());
    auto* pool = arrow::default_memory_pool();

    arrow::Int64Builder t_b(pool);
    if (!t_b.Reserve(n).ok()) {
        return std::nullopt;
    }
    // An unprojected column gets NO builder: nothing downstream reads it, so building it
    // is the whole cost this projection exists to avoid. Its slot stays null and the field
    // walk below skips the append for it.
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
    std::vector<char> seen(resolved_.size(), 0);

    // Per-record iterate(), with the scratch buffer grown MONOTONICALLY and the line
    // copied in - not assign() + resize() per record, which changed the vector's size
    // twice and zero-filled SIMDJSON_PADDING bytes for every single line. simdjson
    // requires the padding to be ALLOCATED and readable, not zeroed; only the first
    // `len` bytes are parsed, so whatever a previous longer line left in the padding
    // is never part of a document.
    //
    // MEASURED ALTERNATIVE, REJECTED: concatenating the batch into one buffer and
    // parsing it with a single iterate_many(). That is the API designed for streams of
    // small documents and it amortises stage1 (simdjson's SIMD structural prescan,
    // 13% of decode self time) from once per record to once per batch - but it
    // measured 5.85M rec/s against 5.87M... in fact SLOWER than 6.26M for the
    // per-record path, on ~120-byte documents. Streaming mode carries its own
    // per-document bookkeeping (document_reference, boundary rescan, _streaming
    // disabling some fast paths) and on these document sizes that costs more than the
    // stage1 it saves. Tried with batch_size at both 1MB and the exact buffer length,
    // in case parser capacity was hurting cache locality; both lost. Do not re-try
    // without measuring.
    for (const auto& rec : in) {
        const std::string& line = rec.value();
        const std::size_t need = line.size() + simdjson::SIMDJSON_PADDING;
        if (od_->pad.size() < need) {
            od_->pad.resize(need);
        }
        std::memcpy(od_->pad.data(), line.data(), line.size());

        simdjson::ondemand::document doc;
        if (od_->parser.iterate(od_->pad.data(), line.size(), od_->pad.size()).get(doc) !=
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
            // escaped_key(), not unescaped_key(): the raw key bytes as they sit in the
            // input buffer, with no unescaping pass and no copy into simdjson's string
            // buffer. Real column names contain no escapes, so for them the raw key IS
            // the unescaped key and the comparison is identical.
            //
            // A key that DOES carry an escape simply fails to match, which returns -1,
            // bails the batch and falls back to the DOM path where the key is properly
            // unescaped - the same refuse-rather-than-guess asymmetry the rest of this
            // decoder relies on. It cannot match the WRONG column either: raw bytes
            // equal to a column name can only unescape to something else if the name
            // itself contains a backslash, and such a schema is refused up front
            // (schema_capable_).
            std::string_view key;
            if (field.escaped_key().get(key) != simdjson::SUCCESS) {
                return std::nullopt;
            }
            const int ci = column_index_at_(key, filled);
            if (ci < 0) {
                return std::nullopt;  // undeclared field: the row decode would keep it
            }
            const auto uci = static_cast<std::size_t>(ci);
            if (seen[uci] != 0) {
                return std::nullopt;  // duplicate key
            }
            simdjson::ondemand::value val;
            if (field.value().get(val) != simdjson::SUCCESS) {
                return std::nullopt;
            }
            const auto& res = resolved_[uci];
            // Declared but unprojected: consume the field and drop it. It still counts
            // toward `filled`, so the "every declared column present" gate below is
            // unchanged - the projection must not turn a faithfulness check into a pass.
            if (res.projected) {
                if (!append_ondemand(res.type_id, col_b[uci].get(), val, res.scale)) {
                    return std::nullopt;
                }
            }
            seen[uci] = 1;
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
    // Same order and count as data_fields_, which also skipped the unprojected columns.
    for (std::size_t ci = 0; ci < resolved_.size(); ++ci) {
        if (!resolved_[ci].projected) {
            continue;
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

}  // namespace clink::sql
