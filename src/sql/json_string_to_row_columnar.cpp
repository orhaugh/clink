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

#include <cctype>
#include <cmath>
#include <cstdint>
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
                                                                 std::string name)
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

JsonStringToRowColumnarOperator::~JsonStringToRowColumnarOperator() = default;

namespace {

// Append one on-demand value to a typed builder, mirroring append_cell_'s
// acceptance rules exactly. Returns false to bail the batch.
//
// Anything this cannot decide is refused rather than guessed: a false return
// costs a fallback to the DOM path (and, failing that, to rows), which is always
// correct. That asymmetry is deliberate - the columnar carrier only ever fires
// where it provably matches the row decode.
bool append_ondemand(const arrow::DataType& eff,
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
        const std::string& line = rec.value();
        od_->pad.assign(line.begin(), line.end());
        od_->pad.resize(line.size() + simdjson::SIMDJSON_PADDING, '\0');

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
            std::string_view key;
            if (field.unescaped_key().get(key) != simdjson::SUCCESS) {
                return std::nullopt;
            }
            const int ci = column_index_(key);
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
            int scale = 0;
            if (resolved_[uci].eff->id() == arrow::Type::DECIMAL128) {
                scale = static_cast<const arrow::Decimal128Type&>(*resolved_[uci].eff).scale();
            }
            if (!append_ondemand(*resolved_[uci].eff, col_b[uci].get(), val, scale)) {
                return std::nullopt;
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

}  // namespace clink::sql
