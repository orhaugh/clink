#include "cdc_row_encoder.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace clink::postgres::cdc {

namespace {

constexpr std::array<std::string_view, 6> kMetadataColumns = {
    "__op", "__lsn", "__version", "__deleted", "__table", "__xid"};

std::string upper(std::string_view s) {
    std::string out{s};
    for (char& c : out) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return out;
}

bool all_digits(std::string_view s) {
    return !s.empty() && std::all_of(s.begin(), s.end(), [](char c) {
        return std::isdigit(static_cast<unsigned char>(c)) != 0;
    });
}

// "-123" / "123": an optional sign and at least one digit.
bool is_integer_text(std::string_view s) {
    if (!s.empty() && s.front() == '-') {
        s.remove_prefix(1);
    }
    return all_digits(s);
}

bool is_one_of(std::string_view v, std::initializer_list<std::string_view> set) {
    return std::find(set.begin(), set.end(), v) != set.end();
}

void append_json_string(std::string& out, std::string_view s) {
    out.push_back('"');
    for (const char ch : s) {
        const auto c = static_cast<unsigned char>(ch);
        switch (ch) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            case '\b':
                out += "\\b";
                break;
            case '\f':
                out += "\\f";
                break;
            default:
                if (c < 0x20) {
                    std::array<char, 8> buf{};
                    std::snprintf(buf.data(), buf.size(), "\\u%04x", static_cast<unsigned>(c));
                    out += buf.data();
                } else {
                    out.push_back(ch);
                }
        }
    }
    out.push_back('"');
}

std::string supported_sources(Kind k) {
    switch (k) {
        case Kind::BigInt:
            return "int2, int4 or int8";
        case Kind::Integer:
            return "int2 or int4";
        case Kind::SmallInt:
            return "int2";
        case Kind::Double:
            return "float4 or float8";
        case Kind::Real:
            return "float4";
        case Kind::Decimal:
            return "numeric, int2, int4 or int8";
        case Kind::Boolean:
            return "bool";
        case Kind::Date:
            return "date";
        case Kind::Timestamp:
            return "timestamp";
        case Kind::TimestampTz:
            return "timestamptz";
        default:
            return "any type";
    }
}

// Days since 1970-01-01 of a proleptic Gregorian date.
std::int64_t days_from_civil(std::int64_t y, int m, int d) {
    y -= m <= 2 ? 1 : 0;
    const std::int64_t era = (y >= 0 ? y : y - 399) / 400;
    const std::int64_t yoe = y - (era * 400);
    const std::int64_t mp = m > 2 ? m - 3 : m + 9;
    const std::int64_t doy = (((153 * mp) + 2) / 5) + d - 1;
    const std::int64_t doe = (yoe * 365) + (yoe / 4) - (yoe / 100) + doy;
    return (era * 146097) + doe - 719468;
}

bool leap(int y) {
    return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
}

int days_in_month(int y, int m) {
    static constexpr std::array<int, 12> kDays = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    return m == 2 && leap(y) ? 29 : kDays[static_cast<std::size_t>(m - 1)];
}

// Reads exactly `n` digits at `pos`, advancing it.
std::optional<int> take_digits(std::string_view s, std::size_t& pos, std::size_t n) {
    if (pos + n > s.size() || !all_digits(s.substr(pos, n))) {
        return std::nullopt;
    }
    int v = 0;
    for (std::size_t i = 0; i < n; ++i) {
        v = v * 10 + (s[pos + i] - '0');
    }
    pos += n;
    return v;
}

struct CivilDate {
    int year{0};
    int month{0};
    int day{0};
};

// 'YYYY-MM-DD' at the start of `s`, a real date. Advances `pos`.
std::optional<CivilDate> parse_date_prefix(std::string_view s, std::size_t& pos) {
    const auto y = take_digits(s, pos, 4);
    if (!y || pos >= s.size() || s[pos++] != '-') {
        return std::nullopt;
    }
    const auto m = take_digits(s, pos, 2);
    if (!m || pos >= s.size() || s[pos++] != '-') {
        return std::nullopt;
    }
    const auto d = take_digits(s, pos, 2);
    if (!d || *y < 1 || *m < 1 || *m > 12 || *d < 1 || *d > days_in_month(*y, *m)) {
        return std::nullopt;
    }
    return CivilDate{*y, *m, *d};
}

[[noreturn]] void refuse_value(const ColumnPlan& col,
                               std::string_view pg_type,
                               std::string_view text,
                               const std::string& why) {
    throw EncodeError("column " + col.name + " (" + std::string{pg_type} + " into " + col.sql_type +
                      "): the value " + std::string{text} + " " + why);
}

// Digits of a plain decimal numeral ("-12.340"): significant integer digits and
// fraction digits after trailing zeros are dropped.
struct DecimalShape {
    std::size_t int_digits{0};
    std::size_t frac_digits{0};
};

std::optional<DecimalShape> decimal_shape(std::string_view text) {
    if (!text.empty() && (text.front() == '-' || text.front() == '+')) {
        text.remove_prefix(1);
    }
    const auto dot = text.find('.');
    std::string_view ip = text.substr(0, dot);
    std::string_view fp = dot == std::string_view::npos ? std::string_view{} : text.substr(dot + 1);
    if (ip.empty() && fp.empty()) {
        return std::nullopt;
    }
    if ((!ip.empty() && !all_digits(ip)) || (!fp.empty() && !all_digits(fp))) {
        return std::nullopt;
    }
    while (!ip.empty() && ip.front() == '0') {
        ip.remove_prefix(1);
    }
    while (!fp.empty() && fp.back() == '0') {
        fp.remove_suffix(1);
    }
    return DecimalShape{ip.size(), fp.size()};
}

}  // namespace

bool is_metadata_column(std::string_view name) {
    return std::find(kMetadataColumns.begin(), kMetadataColumns.end(), name) !=
           kMetadataColumns.end();
}

std::vector<ColumnPlan> plan_columns(std::string_view declared_types) {
    std::vector<ColumnPlan> out;
    std::size_t pos = 0;
    while (pos < declared_types.size()) {
        const auto semi = declared_types.find(';', pos);
        const std::string_view entry = declared_types.substr(
            pos, semi == std::string_view::npos ? std::string_view::npos : semi - pos);
        pos = semi == std::string_view::npos ? declared_types.size() : semi + 1;
        if (entry.empty()) {
            continue;
        }
        const auto colon = entry.find(':');
        if (colon == std::string_view::npos || colon == 0) {
            throw EncodeError("declared column '" + std::string{entry} +
                              "' is not of the form name:TYPE");
        }
        ColumnPlan col;
        col.name = std::string{entry.substr(0, colon)};
        col.sql_type = std::string{entry.substr(colon + 1)};
        const std::string t = upper(col.sql_type);

        if (col.name.rfind("__", 0) == 0) {
            const auto need = [&](std::string_view want, Kind kind) {
                if (t != want) {
                    throw EncodeError("metadata column " + col.name + " must be declared " +
                                      std::string{want} + " (declared " + col.sql_type + ")");
                }
                col.kind = kind;
            };
            if (col.name == "__op") {
                need("VARCHAR", Kind::MetaOp);
            } else if (col.name == "__lsn") {
                need("VARCHAR", Kind::MetaLsn);
            } else if (col.name == "__version") {
                need("BIGINT", Kind::MetaVersion);
            } else if (col.name == "__deleted") {
                need("BOOLEAN", Kind::MetaDeleted);
            } else if (col.name == "__table") {
                need("VARCHAR", Kind::MetaTable);
            } else if (col.name == "__xid") {
                need("BIGINT", Kind::MetaXid);
            } else {
                throw EncodeError("column " + col.name +
                                  ": names starting with __ are reserved for change metadata "
                                  "(__op, __lsn, __version, __deleted, __table, __xid)");
            }
            out.push_back(std::move(col));
            continue;
        }

        if (t == "BIGINT") {
            col.kind = Kind::BigInt;
        } else if (t == "INTEGER") {
            col.kind = Kind::Integer;
        } else if (t == "SMALLINT") {
            col.kind = Kind::SmallInt;
        } else if (t == "DOUBLE") {
            col.kind = Kind::Double;
        } else if (t == "REAL") {
            col.kind = Kind::Real;
        } else if (t == "BOOLEAN") {
            col.kind = Kind::Boolean;
        } else if (t == "DATE") {
            col.kind = Kind::Date;
        } else if (t == "VARCHAR") {
            col.kind = Kind::Varchar;
        } else if (t.rfind("TIMESTAMP(", 0) == 0) {
            col.kind =
                t.find("WITH TIME ZONE") != std::string::npos ? Kind::TimestampTz : Kind::Timestamp;
        } else if (t.rfind("DECIMAL(", 0) == 0) {
            int p = 0;
            int s = 0;
            if (std::sscanf(t.c_str(), "DECIMAL(%d, %d)", &p, &s) != 2 &&
                std::sscanf(t.c_str(), "DECIMAL(%d,%d)", &p, &s) != 2) {
                throw EncodeError("column " + col.name + ": cannot read the type " + col.sql_type);
            }
            col.kind = Kind::Decimal;
            col.precision = p;
            col.scale = s;
        } else {
            throw EncodeError("column " + col.name + ": SQL type " + col.sql_type +
                              " is not supported by the PostgreSQL CDC source. Supported: "
                              "BIGINT, INTEGER, SMALLINT, DOUBLE, REAL, DECIMAL(p, s), BOOLEAN, "
                              "DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE and VARCHAR");
        }
        out.push_back(std::move(col));
    }
    return out;
}

std::optional<std::string> type_pair_problem(std::string_view pg_type, const ColumnPlan& column) {
    bool ok = false;
    switch (column.kind) {
        case Kind::Varchar:
            ok = true;
            break;
        case Kind::BigInt:
            ok = is_one_of(pg_type, {"int2", "int4", "int8"});
            break;
        case Kind::Integer:
            ok = is_one_of(pg_type, {"int2", "int4"});
            break;
        case Kind::SmallInt:
            ok = pg_type == "int2";
            break;
        case Kind::Double:
            ok = is_one_of(pg_type, {"float4", "float8"});
            break;
        case Kind::Real:
            ok = pg_type == "float4";
            break;
        case Kind::Decimal:
            ok = is_one_of(pg_type, {"numeric", "int2", "int4", "int8"});
            break;
        case Kind::Boolean:
            ok = pg_type == "bool";
            break;
        case Kind::Date:
            ok = pg_type == "date";
            break;
        case Kind::Timestamp:
            ok = pg_type == "timestamp";
            break;
        case Kind::TimestampTz:
            ok = pg_type == "timestamptz";
            break;
        default:
            ok = false;
    }
    if (ok) {
        return std::nullopt;
    }
    return "column " + column.name + ": PostgreSQL type " +
           (pg_type.empty() ? std::string{"(unknown)"} : std::string{pg_type}) +
           " cannot be read as " + column.sql_type + " (it takes " +
           supported_sources(column.kind) +
           "); declare the column VARCHAR to carry PostgreSQL's text form";
}

std::optional<std::uint64_t> parse_lsn(std::string_view text) {
    const auto slash = text.find('/');
    if (slash == std::string_view::npos || slash == 0 || slash + 1 >= text.size()) {
        return std::nullopt;
    }
    auto hex = [](std::string_view h) -> std::optional<std::uint64_t> {
        if (h.empty() || h.size() > 8) {
            return std::nullopt;
        }
        std::uint64_t v = 0;
        for (const char c : h) {
            v <<= 4;
            if (c >= '0' && c <= '9') {
                v |= static_cast<std::uint64_t>(c - '0');
            } else if (c >= 'A' && c <= 'F') {
                v |= static_cast<std::uint64_t>(c - 'A' + 10);
            } else if (c >= 'a' && c <= 'f') {
                v |= static_cast<std::uint64_t>(c - 'a' + 10);
            } else {
                return std::nullopt;
            }
        }
        return v;
    };
    const auto hi = hex(text.substr(0, slash));
    const auto lo = hex(text.substr(slash + 1));
    if (!hi || !lo) {
        return std::nullopt;
    }
    return (*hi << 32) | *lo;
}

std::int64_t timestamp_to_epoch_ms(std::string_view text, bool with_zone) {
    const auto fail = [&](const std::string& why) -> EncodeError {
        return EncodeError("timestamp " + std::string{text} + " " + why);
    };
    if (text == "infinity" || text == "-infinity") {
        throw fail("is infinite, which the engine's TIMESTAMP cannot hold");
    }
    if (text.size() >= 3 && text.substr(text.size() - 3) == " BC") {
        throw fail("is before the common era, which the engine's TIMESTAMP cannot hold");
    }
    std::size_t pos = 0;
    const auto date = parse_date_prefix(text, pos);
    if (!date || pos >= text.size() || text[pos++] != ' ') {
        throw fail("is not in the ISO form YYYY-MM-DD HH:MM:SS (is DateStyle set to ISO?)");
    }
    const auto hh = take_digits(text, pos, 2);
    if (!hh || pos >= text.size() || text[pos++] != ':') {
        throw fail("is not in the ISO form YYYY-MM-DD HH:MM:SS");
    }
    const auto mi = take_digits(text, pos, 2);
    if (!mi || pos >= text.size() || text[pos++] != ':') {
        throw fail("is not in the ISO form YYYY-MM-DD HH:MM:SS");
    }
    const auto ss = take_digits(text, pos, 2);
    if (!ss || *hh > 23 || *mi > 59 || *ss > 59) {
        throw fail("is not in the ISO form YYYY-MM-DD HH:MM:SS");
    }
    int millis = 0;
    if (pos < text.size() && text[pos] == '.') {
        ++pos;
        const std::size_t start = pos;
        while (pos < text.size() && std::isdigit(static_cast<unsigned char>(text[pos])) != 0) {
            ++pos;
        }
        const std::string_view frac = text.substr(start, pos - start);
        if (frac.empty()) {
            throw fail("has an empty fraction");
        }
        for (std::size_t i = 0; i < frac.size(); ++i) {
            if (i < 3) {
                millis = millis * 10 + (frac[i] - '0');
            } else if (frac[i] != '0') {
                throw fail(
                    "has digits below the millisecond, which the engine's millisecond TIMESTAMP "
                    "cannot hold; store timestamp(3) in PostgreSQL, or declare the column VARCHAR "
                    "to carry the full text");
            }
        }
        for (std::size_t i = frac.size(); i < 3; ++i) {
            millis *= 10;
        }
    }
    std::int64_t offset_seconds = 0;
    if (pos < text.size()) {
        if (!with_zone || (text[pos] != '+' && text[pos] != '-')) {
            throw fail("carries text after the time that is not a UTC offset");
        }
        const int sign = text[pos++] == '-' ? -1 : 1;
        const auto oh = take_digits(text, pos, 2);
        if (!oh) {
            throw fail("has a malformed UTC offset");
        }
        int om = 0;
        int os = 0;
        if (pos < text.size()) {
            if (text[pos++] != ':') {
                throw fail("has a malformed UTC offset");
            }
            const auto m = take_digits(text, pos, 2);
            if (!m) {
                throw fail("has a malformed UTC offset");
            }
            om = *m;
            if (pos < text.size()) {
                if (text[pos++] != ':') {
                    throw fail("has a malformed UTC offset");
                }
                const auto s = take_digits(text, pos, 2);
                if (!s) {
                    throw fail("has a malformed UTC offset");
                }
                os = *s;
            }
        }
        if (pos != text.size()) {
            throw fail("has a malformed UTC offset");
        }
        offset_seconds = sign * ((static_cast<std::int64_t>(*oh) * 60 + om) * 60 + os);
    } else if (with_zone) {
        throw fail("has no UTC offset");
    }
    const std::int64_t days = days_from_civil(date->year, date->month, date->day);
    const std::int64_t secs =
        days * 86400 + (static_cast<std::int64_t>(*hh) * 60 + *mi) * 60 + *ss - offset_seconds;
    return secs * 1000 + millis;
}

std::string convert_value(std::string_view pg_type,
                          std::string_view text,
                          const ColumnPlan& column) {
    if (auto problem = type_pair_problem(pg_type, column)) {
        throw EncodeError(*problem);
    }
    switch (column.kind) {
        case Kind::BigInt:
        case Kind::Integer:
        case Kind::SmallInt:
            if (!is_integer_text(text)) {
                refuse_value(column, pg_type, text, "is not an integer");
            }
            return std::string{text};
        case Kind::Double:
        case Kind::Real: {
            if (text == "NaN" || text == "Infinity" || text == "-Infinity") {
                refuse_value(column,
                             pg_type,
                             text,
                             "is not a finite number, which the engine's row values cannot hold");
            }
            // PostgreSQL's float output is a JSON number already ("1.5",
            // "-0", "1e+30"); check its characters rather than trust it.
            const bool plausible =
                !text.empty() && std::all_of(text.begin(), text.end(), [](char c) {
                    return (std::isdigit(static_cast<unsigned char>(c)) != 0) || c == '-' ||
                           c == '+' || c == '.' || c == 'e' || c == 'E';
                });
            if (!plausible) {
                refuse_value(column, pg_type, text, "is not a number");
            }
            return std::string{text};
        }
        case Kind::Decimal: {
            if (text == "NaN" || text == "Infinity" || text == "-Infinity") {
                refuse_value(
                    column, pg_type, text, "is not a finite number, which DECIMAL cannot hold");
            }
            const auto shape = decimal_shape(text);
            if (!shape) {
                refuse_value(column, pg_type, text, "is not a decimal numeral");
            }
            const auto int_room = static_cast<std::size_t>(column.precision - column.scale);
            if (shape->int_digits > int_room ||
                shape->frac_digits > static_cast<std::size_t>(column.scale)) {
                refuse_value(column,
                             pg_type,
                             text,
                             "does not fit " + column.sql_type +
                                 " without rounding; widen the "
                                 "declared precision or scale");
            }
            return std::string{text};
        }
        case Kind::Boolean:
            if (text == "t") {
                return "true";
            }
            if (text == "f") {
                return "false";
            }
            refuse_value(column, pg_type, text, "is not a boolean");
        case Kind::Date: {
            std::size_t pos = 0;
            const auto d = parse_date_prefix(text, pos);
            if (!d || pos != text.size()) {
                refuse_value(column,
                             pg_type,
                             text,
                             "is not a date of the common era in the form YYYY-MM-DD (infinite "
                             "and BC dates cannot be held; is DateStyle set to ISO?)");
            }
            std::string out;
            append_json_string(out, text);
            return out;
        }
        case Kind::Timestamp:
        case Kind::TimestampTz:
            try {
                return std::to_string(
                    timestamp_to_epoch_ms(text, column.kind == Kind::TimestampTz));
            } catch (const EncodeError& e) {
                throw EncodeError("column " + column.name + " (" + std::string{pg_type} + " into " +
                                  column.sql_type + "): " + e.what());
            }
        case Kind::Varchar: {
            std::string out;
            append_json_string(out, text);
            return out;
        }
        default:
            break;
    }
    throw EncodeError("column " + column.name + " is not a data column");
}

RowEncoder::RowEncoder(std::vector<ColumnPlan> columns, ChangelogMode mode)
    : columns_(std::move(columns)), mode_(mode) {}

std::optional<std::string> RowEncoder::encode(const CdcEvent& ev, bool snapshot_row) const {
    if (ev.op != CdcEvent::Op::Insert && ev.op != CdcEvent::Op::Update &&
        ev.op != CdcEvent::Op::Delete) {
        return std::nullopt;
    }
    const bool is_delete = ev.op == CdcEvent::Op::Delete;
    std::string out = "{";
    bool first = true;
    const auto key = [&](std::string_view name) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        append_json_string(out, name);
        out.push_back(':');
    };
    for (const auto& col : columns_) {
        key(col.name);
        switch (col.kind) {
            case Kind::MetaOp:
                append_json_string(out,
                                   snapshot_row                    ? "snapshot"
                                   : ev.op == CdcEvent::Op::Insert ? "insert"
                                   : ev.op == CdcEvent::Op::Update ? "update"
                                                                   : "delete");
                continue;
            case Kind::MetaLsn:
                append_json_string(out, ev.lsn);
                continue;
            case Kind::MetaVersion: {
                std::uint64_t v = 0;
                if (!snapshot_row) {
                    const auto lsn = parse_lsn(ev.lsn);
                    if (!lsn) {
                        throw EncodeError("change at '" + ev.lsn + "' has no readable LSN for " +
                                          col.name);
                    }
                    v = *lsn;
                }
                out += std::to_string(v);
                continue;
            }
            case Kind::MetaDeleted:
                out += is_delete ? "true" : "false";
                continue;
            case Kind::MetaTable:
                append_json_string(out, ev.table);
                continue;
            case Kind::MetaXid:
                out += std::to_string(snapshot_row ? 0 : ev.xid);
                continue;
            default:
                break;
        }
        const CdcField* field = nullptr;
        for (const auto& f : ev.values) {
            if (f.name == col.name) {
                field = &f;
                break;
            }
        }
        if (field == nullptr) {
            throw EncodeError("column " + col.name + " is not in the change to " + ev.table +
                              " at " + ev.lsn + "; it may have been dropped or renamed");
        }
        if (field->is_null) {
            out += "null";
            continue;
        }
        try {
            out += convert_value(field->type, field->value, col);
        } catch (const EncodeError& e) {
            throw EncodeError(std::string{e.what()} + " (change to " + ev.table + " at " + ev.lsn +
                              ")");
        }
    }
    if (mode_ == ChangelogMode::Upsert) {
        key("__row_kind");
        append_json_string(out,
                           is_delete                       ? "delete"
                           : ev.op == CdcEvent::Op::Update ? "update_after"
                                                           : "insert");
    }
    out.push_back('}');
    return out;
}

}  // namespace clink::postgres::cdc
