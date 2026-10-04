#include "native/column_plan.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <set>
#include <unordered_map>
#include <utility>

#include "native/errors.hpp"
#include "native/sql_text.hpp"

namespace clink::clickhouse::native {

namespace {

// A rescale factor travels as an int64, so a decimal can gain at most 18
// digits of scale on the way in. 10^18 is the largest power of ten int64
// holds.
constexpr int kMaxRescaleDigits = 18;

std::int64_t pow10(int n) {
    std::int64_t v = 1;
    for (int i = 0; i < n; ++i) {
        v *= 10;
    }
    return v;
}

const char* default_kind_name(DefaultKind k) {
    switch (k) {
        case DefaultKind::None:
            return "none";
        case DefaultKind::Default:
            return "DEFAULT";
        case DefaultKind::Materialized:
            return "MATERIALIZED";
        case DefaultKind::Alias:
            return "ALIAS";
        case DefaultKind::Ephemeral:
            return "EPHEMERAL";
    }
    return "none";
}

const char* conversion_name(Conversion c) {
    switch (c) {
        case Conversion::Copy:
            return "copy";
        case Conversion::WidenInt:
            return "widen_int";
        case Conversion::NarrowInt:
            return "narrow_int";
        case Conversion::SignedToUnsigned:
            return "signed_to_unsigned";
        case Conversion::RealToDouble:
            return "real_to_double";
        case Conversion::BoolUnpack:
            return "bool_unpack";
        case Conversion::StringZeroCopy:
            return "string_zero_copy";
        case Conversion::StringToFixed:
            return "string_to_fixed";
        case Conversion::StringToEnum:
            return "string_to_enum";
        case Conversion::StringToUuid:
            return "string_to_uuid";
        case Conversion::StringToIpv4:
            return "string_to_ipv4";
        case Conversion::StringToIpv6:
            return "string_to_ipv6";
        case Conversion::DecimalRescale:
            return "decimal_rescale";
        case Conversion::TimestampToDateTime64:
            return "timestamp_to_datetime64";
        case Conversion::TimestampToDateTime:
            return "timestamp_to_datetime";
        case Conversion::DateToDate32:
            return "date_to_date32";
        case Conversion::DateToDate:
            return "date_to_date";
        case Conversion::List:
            return "list";
        case Conversion::Map:
            return "map";
        case Conversion::Struct:
            return "struct";
    }
    return "copy";
}

// The width in bytes of a signed integer target, or 0 for any other kind.
int signed_width(ChKind k) {
    switch (k) {
        case ChKind::Int8:
            return 1;
        case ChKind::Int16:
            return 2;
        case ChKind::Int32:
            return 4;
        case ChKind::Int64:
            return 8;
        case ChKind::Int128:
            return 16;
        default:
            return 0;
    }
}

bool is_unsigned_up_to_64(ChKind k) {
    return k == ChKind::UInt8 || k == ChKind::UInt16 || k == ChKind::UInt32 || k == ChKind::UInt64;
}

// The width in bytes of an unsigned target up to 64 bits, or 0.
int unsigned_width(ChKind k) {
    switch (k) {
        case ChKind::UInt8:
            return 1;
        case ChKind::UInt16:
            return 2;
        case ChKind::UInt32:
            return 4;
        case ChKind::UInt64:
            return 8;
        default:
            return 0;
    }
}

int sql_int_width(SqlKind k) {
    switch (k) {
        case SqlKind::TinyInt:
        case SqlKind::UTinyInt:
            return 1;
        case SqlKind::SmallInt:
        case SqlKind::USmallInt:
            return 2;
        case SqlKind::Integer:
        case SqlKind::UInteger:
            return 4;
        case SqlKind::BigInt:
        case SqlKind::UBigInt:
            return 8;
        default:
            return 0;
    }
}

// The remedies a problem names. A SQL table is changed in its SELECT or its
// declaration; a typed struct in its fields or its CLINK_FIELDS declaration.
// The SQL texts are the ones the sink has always printed.
struct Remedies {
    bool typed{false};

    [[nodiscard]] const char* change_source() const {
        return typed ? "change the struct field's type or the target column" : "CAST in the SELECT";
    }
    [[nodiscard]] const char* narrow_float() const {
        return typed ? "change the struct field's type or the target column"
                     : "use REAL in the clink table or CAST in the SELECT";
    }
    [[nodiscard]] const char* or_change_source() const {
        return typed ? "or change the struct field's type" : "or CAST in the SELECT";
    }
    [[nodiscard]] std::string declare_decimal(int precision, int scale) const {
        return typed ? "or give the struct field decimal128(" + std::to_string(precision) + ", " +
                           std::to_string(scale) + ")"
                     : "or declare the clink column DECIMAL(" + std::to_string(precision) + ", " +
                           std::to_string(scale) + ")";
    }
    [[nodiscard]] const char* declare_scale() const {
        return typed ? "give the struct field a scale of at least "
                     : "declare the clink column with a scale of at least ";
    }
    [[nodiscard]] const char* source_side() const {
        return typed ? "the nested struct" : "the clink table";
    }
    [[nodiscard]] const char* same_fields() const {
        return typed ? "the nested struct and the Tuple must have the same fields"
                     : "the clink ROW and the Tuple must have the same fields";
    }
    [[nodiscard]] const char* no_columns() const {
        return typed ? "the batcher's schema has no columns besides event_time"
                     : "the clink table declares no columns";
    }
    [[nodiscard]] const char* not_produced() const {
        return typed ? "has no default and the struct has no field for it"
                     : "has no default and the query does not produce it";
    }
    [[nodiscard]] const char* appears_twice() const {
        return typed ? "appears twice in the batcher's schema" : "appears twice in the clink table";
    }
    [[nodiscard]] const char* drop_or_add() const {
        return typed ? "remove the field from the CLINK_FIELDS declaration or add the column to "
                       "the table"
                     : "drop it from the SELECT or add it to the table";
    }
    [[nodiscard]] const char* drop_computed() const {
        return typed ? "remove the field from the CLINK_FIELDS declaration"
                     : "drop it from the SELECT";
    }
    [[nodiscard]] const char* whose_rows() const {
        return typed ? " cannot take this struct's rows:" : " cannot take this table's rows:";
    }
};

bool any_zero_copy(const ColumnBinding& b) {
    return b.zero_copy || std::any_of(b.children.begin(), b.children.end(), any_zero_copy);
}

std::string describe(const ColumnBinding& b) {
    std::string out = conversion_name(b.conversion);
    if (!b.children.empty()) {
        out += '(';
        for (std::size_t i = 0; i < b.children.size(); ++i) {
            if (i != 0) {
                out += ", ";
            }
            out += describe(b.children[i]);
        }
        out += ')';
    }
    if (b.multiplier != 1) {
        out += " multiplier=" + std::to_string(b.multiplier);
    }
    if (b.divisor != 1) {
        out += " divisor=" + std::to_string(b.divisor);
    }
    return out;
}

std::string ascii_lower(std::string_view s) {
    std::string out(s);
    for (char& c : out) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return out;
}

// Applies the type rules to one declared column, or to one element of a
// composite, recording a problem for every refusal it finds. `where` names
// the spot for the message ("column `a`", "column `a`, element").
class Binder {
public:
    Binder(std::vector<PlanProblem>& problems, std::string column, Remedies remedies)
        : problems_(problems), column_(std::move(column)), remedies_(remedies) {}

    std::optional<ColumnBinding> bind(const SqlType& src,
                                      const ChType& dst,
                                      const std::string& name,
                                      int index,
                                      const std::string& where) {
        if (src.kind == SqlKind::Unsupported) {
            return refuse(
                where, "the clink type " + src.spelling + " is not supported by the native sink");
        }
        if (src.kind == SqlKind::Bytea || src.kind == SqlKind::Time) {
            return refuse(where,
                          src.spelling +
                              " has no mapping in the native sink; leave the column out of the "
                              "table or declare it as another type");
        }
        if (dst.kind == ChKind::Unsupported) {
            const std::string& reason = dst.unsupported_reason;
            return refuse(where,
                          reason.starts_with(dst.spelling) ? reason : dst.spelling + ": " + reason);
        }

        ColumnBinding b;
        b.name = name;
        b.input_index = index;
        b.source = src;
        b.target = dst;
        b.expected_header_type = client_header_spelling(dst.spelling);

        std::optional<Conversion> conversion;
        switch (src.kind) {
            case SqlKind::TinyInt:
            case SqlKind::SmallInt:
            case SqlKind::Integer:
            case SqlKind::BigInt: {
                const int from = sql_int_width(src.kind);
                if (const int to = signed_width(dst.kind); to != 0) {
                    conversion = to == from
                                     ? Conversion::Copy
                                     : (to > from ? Conversion::WidenInt : Conversion::NarrowInt);
                } else if (is_unsigned_up_to_64(dst.kind)) {
                    conversion = Conversion::SignedToUnsigned;
                }
                break;
            }
            case SqlKind::UTinyInt:
            case SqlKind::USmallInt:
            case SqlKind::UInteger:
            case SqlKind::UBigInt: {
                // Every value of a wider type holds the source's, so only a
                // narrower or a same-width signed target is checked per value.
                const int from = sql_int_width(src.kind);
                if (const int to = unsigned_width(dst.kind); to != 0) {
                    conversion = to == from
                                     ? Conversion::Copy
                                     : (to > from ? Conversion::WidenInt : Conversion::NarrowInt);
                } else if (const int to_signed = signed_width(dst.kind); to_signed != 0) {
                    conversion = to_signed > from ? Conversion::WidenInt : Conversion::NarrowInt;
                }
                break;
            }
            case SqlKind::Real:
                if (dst.kind == ChKind::Float32) {
                    conversion = Conversion::Copy;
                } else if (dst.kind == ChKind::Float64) {
                    conversion = Conversion::RealToDouble;
                }
                break;
            case SqlKind::Double:
                if (dst.kind == ChKind::Float64) {
                    conversion = Conversion::Copy;
                } else if (dst.kind == ChKind::Float32) {
                    return refuse(where,
                                  src.spelling + " into " + dst.spelling + " narrows; " +
                                      remedies_.narrow_float());
                }
                break;
            case SqlKind::Boolean:
                if (dst.kind == ChKind::Bool || dst.kind == ChKind::UInt8) {
                    conversion = Conversion::BoolUnpack;
                }
                break;
            case SqlKind::Varchar:
                conversion = string_conversion(dst.kind);
                // A String target, LowCardinality(String) included, takes
                // views into the chunk's own buffers, so the chunk must
                // outlive the INSERT.
                b.zero_copy = conversion == Conversion::StringZeroCopy;
                break;
            case SqlKind::Decimal:
                if (dst.kind == ChKind::Decimal) {
                    if (!decimal_fits(src, dst, where)) {
                        return std::nullopt;
                    }
                    conversion = Conversion::DecimalRescale;
                    b.multiplier = pow10(dst.scale - src.scale);
                }
                break;
            case SqlKind::Timestamp:
                // The value's unit, not the declared precision, sets the
                // figures: a Row carries every TIMESTAMP(p) as milliseconds,
                // and a batcher schema says its unit. Any source goes into any
                // target unit. A value finer than the target is refused per
                // value by the converter, never floored.
                if (dst.kind == ChKind::DateTime64) {
                    conversion = Conversion::TimestampToDateTime64;
                    if (dst.precision >= src.unit_digits) {
                        b.multiplier = pow10(dst.precision - src.unit_digits);
                    } else {
                        b.divisor = pow10(src.unit_digits - dst.precision);
                    }
                } else if (dst.kind == ChKind::DateTime) {
                    conversion = Conversion::TimestampToDateTime;
                    b.divisor = pow10(src.unit_digits);
                }
                break;
            case SqlKind::Date:
                if (dst.kind == ChKind::Date32) {
                    conversion = Conversion::DateToDate32;
                } else if (dst.kind == ChKind::Date) {
                    conversion = Conversion::DateToDate;
                }
                break;
            case SqlKind::Array:
                if (dst.kind == ChKind::Array) {
                    auto element = bind(src.children.at(0),
                                        dst.children.at(0),
                                        name + ".element",
                                        0,
                                        where + ", element");
                    if (!element) {
                        return std::nullopt;
                    }
                    b.children.push_back(std::move(*element));
                    conversion = Conversion::List;
                }
                break;
            case SqlKind::Map:
                if (dst.kind == ChKind::Map) {
                    auto key = bind(
                        src.children.at(0), dst.children.at(0), name + ".key", 0, where + ", key");
                    auto value = bind(src.children.at(1),
                                      dst.children.at(1),
                                      name + ".value",
                                      1,
                                      where + ", value");
                    if (!key || !value) {
                        return std::nullopt;
                    }
                    b.children.push_back(std::move(*key));
                    b.children.push_back(std::move(*value));
                    conversion = Conversion::Map;
                }
                break;
            case SqlKind::Row:
                if (dst.kind == ChKind::Tuple) {
                    if (!bind_struct(src, dst, name, where, b)) {
                        return std::nullopt;
                    }
                    conversion = Conversion::Struct;
                }
                break;
            case SqlKind::Bytea:
            case SqlKind::Time:
            case SqlKind::Unsupported:
                break;
        }
        if (!conversion) {
            return refuse(where,
                          src.spelling + " into " + dst.spelling + " is not supported; " +
                              remedies_.change_source());
        }
        b.conversion = *conversion;
        return b;
    }

private:
    std::vector<PlanProblem>& problems_;
    std::string column_;
    Remedies remedies_;

    std::nullopt_t refuse(const std::string& where, const std::string& message) {
        problems_.push_back(PlanProblem{column_, where + ": " + message});
        return std::nullopt;
    }

    static std::optional<Conversion> string_conversion(ChKind k) {
        switch (k) {
            case ChKind::String:
                return Conversion::StringZeroCopy;
            case ChKind::FixedString:
                return Conversion::StringToFixed;
            case ChKind::Enum8:
            case ChKind::Enum16:
                return Conversion::StringToEnum;
            case ChKind::UUID:
                return Conversion::StringToUuid;
            case ChKind::IPv4:
                return Conversion::StringToIpv4;
            case ChKind::IPv6:
                return Conversion::StringToIpv6;
            default:
                return std::nullopt;
        }
    }

    // Every DECIMAL(p, s) value fits Decimal(P, S) exactly when the target
    // keeps every fractional digit (S >= s) and has as many integer digits
    // (P - S >= p - s). P >= p alone is not enough: DECIMAL(10, 2) into
    // Decimal(10, 4) leaves 6 integer digits for 8, and a value that only
    // overflows at run time fails again on every replay.
    bool decimal_fits(const SqlType& src, const ChType& dst, const std::string& where) {
        const std::string pair = src.spelling + " into " + dst.spelling;
        if (dst.scale < src.scale) {
            refuse(where,
                   pair + " drops " + std::to_string(src.scale - dst.scale) +
                       " fractional digits; raise the target's scale to " +
                       std::to_string(src.scale) + " " + remedies_.or_change_source());
            return false;
        }
        const int target_integer_digits = dst.precision - dst.scale;
        const int source_integer_digits = src.precision - src.scale;
        if (target_integer_digits < source_integer_digits) {
            const int fitting = target_integer_digits + src.scale;
            std::string remedy = "widen the target";
            if (fitting >= 1) {
                remedy += " " + remedies_.declare_decimal(fitting, src.scale);
            }
            refuse(where,
                   pair + " leaves " + std::to_string(target_integer_digits) +
                       " integer digits for " + std::to_string(source_integer_digits) + "; " +
                       remedy);
            return false;
        }
        if (dst.scale - src.scale > kMaxRescaleDigits) {
            refuse(where,
                   pair + " rescales by " + std::to_string(dst.scale - src.scale) +
                       " digits, more than the " + std::to_string(kMaxRescaleDigits) +
                       " the native sink supports; " + remedies_.declare_scale() +
                       std::to_string(dst.scale - kMaxRescaleDigits));
            return false;
        }
        return true;
    }

    // ROW into Tuple: the same arity, element by element in order, and, when
    // the Tuple is named, the same names in the same order.
    bool bind_struct(const SqlType& src,
                     const ChType& dst,
                     const std::string& name,
                     const std::string& where,
                     ColumnBinding& b) {
        if (src.children.size() != dst.children.size()) {
            refuse(where,
                   src.spelling + " has " + std::to_string(src.children.size()) + " fields and " +
                       dst.spelling + " has " + std::to_string(dst.children.size()) + "; " +
                       remedies_.same_fields());
            return false;
        }
        bool ok = true;
        for (std::size_t i = 0; i < src.children.size(); ++i) {
            const std::string& field = src.field_names.at(i);
            if (!dst.element_names.empty() && dst.element_names[i] != field) {
                refuse(where,
                       "field " + std::to_string(i + 1) + " is " + quote_identifier(field) +
                           " in " + remedies_.source_side() + " and " +
                           quote_identifier(dst.element_names[i]) + " in " + dst.spelling +
                           "; rename one so that they match");
                ok = false;
                continue;
            }
            auto child = bind(src.children[i],
                              dst.children[i],
                              name + "." + field,
                              static_cast<int>(i),
                              where + ", field " + quote_identifier(field));
            if (!child) {
                ok = false;
                continue;
            }
            b.children.push_back(std::move(*child));
        }
        return ok;
    }
};

}  // namespace

std::string ColumnPlan::report() const {
    std::string out =
        "clickhouse native sink column plan: columns=" + std::to_string(columns.size()) +
        " omitted=" + std::to_string(omitted.size()) +
        " retains_chunks=" + (retains_chunks ? "true" : "false");
    for (const auto& b : columns) {
        out += "\n  " + quote_identifier(b.name) +
               (kind == InputKind::TypedStruct ? ": arrow=" : ": sql=") + b.source.spelling +
               " target=" + b.target.spelling + " conversion=" + describe(b) +
               " zero_copy=" + (any_zero_copy(b) ? "true" : "false");
    }
    for (const auto& t : omitted) {
        out += "\n  omitted " + quote_identifier(t.name) + ": " + default_kind_name(t.default_kind);
    }
    return out;
}

PlanResult compile_column_plan(const std::vector<SqlColumn>& input,
                               const std::vector<TargetColumn>& target,
                               InputKind kind) {
    PlanResult result;
    auto& problems = result.problems;
    ColumnPlan plan;
    plan.kind = kind;
    const Remedies remedies{kind == InputKind::TypedStruct};

    if (input.empty()) {
        problems.push_back(PlanProblem{"", remedies.no_columns()});
    }

    std::unordered_map<std::string, const TargetColumn*> by_name;
    for (const auto& t : target) {
        by_name.emplace(t.name, &t);
    }
    std::set<std::string, std::less<>> produced;
    for (const auto& c : input) {
        produced.insert(c.name);
    }

    // The target's columns the input lacks come first, in the table's order.
    // Every non-plain kind is left to the server: DEFAULT and EPHEMERAL take
    // their default, MATERIALIZED and ALIAS are computed.
    for (const auto& t : target) {
        if (produced.contains(t.name)) {
            continue;
        }
        if (t.default_kind == DefaultKind::None) {
            problems.push_back(PlanProblem{
                t.name,
                "target column " + quote_identifier(t.name) + " " + remedies.not_produced()});
        } else {
            plan.omitted.push_back(t);
        }
    }

    std::set<std::string, std::less<>> seen;
    for (std::size_t i = 0; i < input.size(); ++i) {
        const SqlColumn& in = input[i];
        const std::string quoted = quote_identifier(in.name);
        const std::string where = "column " + quoted;
        if (!seen.insert(in.name).second) {
            problems.push_back(PlanProblem{in.name, where + " " + remedies.appears_twice()});
            continue;
        }
        const auto it = by_name.find(in.name);
        if (it == by_name.end()) {
            // ClickHouse names are case-sensitive, so a case-only difference
            // is a missing column, but it is worth saying why.
            const std::string folded = ascii_lower(in.name);
            const TargetColumn* near = nullptr;
            for (const auto& t : target) {
                if (ascii_lower(t.name) == folded) {
                    near = &t;
                    break;
                }
            }
            std::string message = where + " is not in the target table; ";
            if (near != nullptr) {
                message += "the target has " + quote_identifier(near->name) +
                           ", which differs only in case, and ClickHouse names are case-sensitive";
            } else {
                message += remedies.drop_or_add();
            }
            problems.push_back(PlanProblem{in.name, std::move(message)});
            continue;
        }
        const TargetColumn& t = *it->second;
        if (t.default_kind == DefaultKind::Materialized || t.default_kind == DefaultKind::Alias) {
            problems.push_back(PlanProblem{in.name,
                                           quoted + " is " + default_kind_name(t.default_kind) +
                                               "; the server computes it; " +
                                               remedies.drop_computed()});
            continue;
        }
        Binder binder(problems, in.name, remedies);
        auto binding =
            binder.bind(in.type, parse_ch_type(t.type), in.name, static_cast<int>(i), where);
        if (!binding) {
            continue;
        }
        if (binding->expected_header_type.empty()) {
            // The header check compares against this spelling, so a target
            // the client cannot build a header column for cannot be written.
            problems.push_back(PlanProblem{
                in.name, where + ": clickhouse-cpp cannot build a column for " + t.type});
            continue;
        }
        plan.columns.push_back(std::move(*binding));
    }

    if (!problems.empty()) {
        return result;
    }
    plan.retains_chunks = std::any_of(plan.columns.begin(), plan.columns.end(), any_zero_copy);
    plan.column_list_sql = "(";
    for (std::size_t i = 0; i < plan.columns.size(); ++i) {
        if (i != 0) {
            plan.column_list_sql += ", ";
        }
        plan.column_list_sql += quote_identifier(plan.columns[i].name);
    }
    plan.column_list_sql += ")";
    result.plan = std::move(plan);
    return result;
}

ColumnPlan compile_or_refuse(const std::vector<SqlColumn>& input,
                             const std::vector<TargetColumn>& target,
                             const std::string& qualified_table,
                             InputKind kind) {
    PlanResult result = compile_column_plan(input, target, kind);
    if (result.plan) {
        return std::move(*result.plan);
    }
    std::string message = qualified_table + Remedies{kind == InputKind::TypedStruct}.whose_rows();
    for (const auto& p : result.problems) {
        message += "\n  - " + p.message;
    }
    throw NativeSinkError(code::kColumnPlan, message);
}

std::vector<std::string> header_drift(const ColumnPlan& plan,
                                      const std::vector<HeaderColumn>& header) {
    std::vector<std::string> drift;
    const std::size_t n = std::max(plan.columns.size(), header.size());
    for (std::size_t i = 0; i < n; ++i) {
        if (i < plan.columns.size() && i < header.size()) {
            const ColumnBinding& b = plan.columns[i];
            const HeaderColumn& h = header[i];
            if (b.name != h.name) {
                drift.push_back("position " + std::to_string(i + 1) + ": plan column " +
                                quote_identifier(b.name) + " " + b.expected_header_type +
                                ", server column " + quote_identifier(h.name) + " " + h.type);
            } else if (b.expected_header_type != h.type) {
                drift.push_back("column " + quote_identifier(b.name) + ": plan " +
                                b.expected_header_type + ", server " + h.type);
            }
        } else if (i < plan.columns.size()) {
            const ColumnBinding& b = plan.columns[i];
            drift.push_back("column " + quote_identifier(b.name) + ": plan " +
                            b.expected_header_type + ", missing from the server header");
        } else {
            const HeaderColumn& h = header[i];
            drift.push_back("column " + quote_identifier(h.name) + ": not in the plan, server " +
                            h.type);
        }
    }
    return drift;
}

}  // namespace clink::clickhouse::native
