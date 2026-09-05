#pragma once
//
// Shared value conversions for the registry formats: civil dates, the
// timestamp shapes accepted on the encode side, and decimals as two's
// complement big-endian bytes (the Avro decimal logical type's carrier).
// Header-only, library-internal.

#include <charconv>
#include <cmath>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/util/decimal.h>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"

namespace clink::schema_registry::detail {

// Howard Hinnant's civil-from-days / days-from-civil (proleptic Gregorian).
inline void civil_from_days(std::int64_t z, int& y, unsigned& m, unsigned& d) {
    z += 719468;
    const std::int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const auto doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const std::int64_t yy = static_cast<std::int64_t>(yoe) + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    d = doy - (153 * mp + 2) / 5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    y = static_cast<int>(yy + (m <= 2 ? 1 : 0));
}

inline std::int64_t days_from_civil(int y, unsigned m, unsigned d) {
    y -= m <= 2 ? 1 : 0;
    const std::int64_t era = (y >= 0 ? y : y - 399) / 400;
    const auto yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<std::int64_t>(doe) - 719468;
}

inline std::string two(unsigned v) {
    return (v < 10 ? "0" : "") + std::to_string(v);
}

inline std::string date_string(std::int64_t days) {
    int y = 0;
    unsigned m = 0;
    unsigned d = 0;
    civil_from_days(days, y, m, d);
    std::string out = std::to_string(y);
    while (out.size() < 4) {
        out.insert(0, "0");
    }
    return out + "-" + two(m) + "-" + two(d);
}

// "HH:MM:SS.fff" for millis, "HH:MM:SS.ffffff" for micros.
inline std::string time_string(std::int64_t value, int fraction_digits) {
    const std::int64_t per_second = fraction_digits == 3 ? 1000 : 1000000;
    std::int64_t secs = value / per_second;
    std::int64_t frac = value % per_second;
    if (frac < 0) {
        frac += per_second;
        secs -= 1;
    }
    const auto h = static_cast<unsigned>((secs / 3600) % 24);
    const auto mi = static_cast<unsigned>((secs / 60) % 60);
    const auto s = static_cast<unsigned>(secs % 60);
    std::string f = std::to_string(frac);
    while (static_cast<int>(f.size()) < fraction_digits) {
        f.insert(0, "0");
    }
    return two(h) + ":" + two(mi) + ":" + two(s) + "." + f;
}

inline bool read_uint(std::string_view& s, std::size_t digits, unsigned& out) {
    if (s.size() < digits) {
        return false;
    }
    unsigned v = 0;
    for (std::size_t i = 0; i < digits; ++i) {
        const char c = s[i];
        if (c < '0' || c > '9') {
            return false;
        }
        v = v * 10 + static_cast<unsigned>(c - '0');
    }
    s.remove_prefix(digits);
    out = v;
    return true;
}

// "YYYY-MM-DD" -> days since the epoch.
inline std::optional<std::int64_t> parse_date(std::string_view s) {
    unsigned y = 0;
    unsigned m = 0;
    unsigned d = 0;
    if (!read_uint(s, 4, y) || s.empty() || s[0] != '-') {
        return std::nullopt;
    }
    s.remove_prefix(1);
    if (!read_uint(s, 2, m) || s.empty() || s[0] != '-') {
        return std::nullopt;
    }
    s.remove_prefix(1);
    if (!read_uint(s, 2, d) || !s.empty() || m < 1 || m > 12 || d < 1 || d > 31) {
        return std::nullopt;
    }
    return days_from_civil(static_cast<int>(y), m, d);
}

// "HH:MM[:SS[.fraction]]" -> microseconds since midnight.
inline std::optional<std::int64_t> parse_time_micros(std::string_view s) {
    unsigned h = 0;
    unsigned mi = 0;
    unsigned sec = 0;
    if (!read_uint(s, 2, h) || s.empty() || s[0] != ':') {
        return std::nullopt;
    }
    s.remove_prefix(1);
    if (!read_uint(s, 2, mi)) {
        return std::nullopt;
    }
    std::int64_t micros = 0;
    if (!s.empty() && s[0] == ':') {
        s.remove_prefix(1);
        if (!read_uint(s, 2, sec)) {
            return std::nullopt;
        }
        if (!s.empty() && s[0] == '.') {
            s.remove_prefix(1);
            std::int64_t scale = 100000;
            std::size_t n = 0;
            while (n < s.size() && s[n] >= '0' && s[n] <= '9') {
                if (scale > 0) {
                    micros += (s[n] - '0') * scale;
                    scale /= 10;
                }
                ++n;
            }
            if (n == 0) {
                return std::nullopt;
            }
            s.remove_prefix(n);
        }
    }
    if (!s.empty() || h > 23 || mi > 59 || sec > 60) {
        return std::nullopt;
    }
    return ((static_cast<std::int64_t>(h) * 60 + mi) * 60 + sec) * 1000000 + micros;
}

// "YYYY-MM-DD[T ]HH:MM[:SS[.fraction]][Z|+HH:MM|-HH:MM]" -> epoch microseconds.
// A missing offset means UTC.
inline std::optional<std::int64_t> parse_timestamp_micros(std::string_view s) {
    if (s.size() < 10) {
        return std::nullopt;
    }
    const auto days = parse_date(s.substr(0, 10));
    if (!days.has_value()) {
        return std::nullopt;
    }
    s.remove_prefix(10);
    std::int64_t micros_of_day = 0;
    std::int64_t offset_micros = 0;
    if (!s.empty()) {
        if (s[0] != 'T' && s[0] != ' ') {
            return std::nullopt;
        }
        s.remove_prefix(1);
        std::size_t end = s.size();
        for (std::size_t i = 0; i < s.size(); ++i) {
            if (s[i] == 'Z' || s[i] == '+' || s[i] == '-') {
                end = i;
                break;
            }
        }
        const auto t = parse_time_micros(s.substr(0, end));
        if (!t.has_value()) {
            return std::nullopt;
        }
        micros_of_day = *t;
        s.remove_prefix(end);
        if (!s.empty() && s[0] != 'Z') {
            const int sign = s[0] == '-' ? -1 : 1;
            s.remove_prefix(1);
            unsigned oh = 0;
            unsigned om = 0;
            if (!read_uint(s, 2, oh)) {
                return std::nullopt;
            }
            if (!s.empty() && s[0] == ':') {
                s.remove_prefix(1);
            }
            if (!read_uint(s, 2, om) || !s.empty()) {
                return std::nullopt;
            }
            offset_micros = sign * (static_cast<std::int64_t>(oh) * 3600 + om * 60) * 1000000;
        } else if (!s.empty()) {
            s.remove_prefix(1);
            if (!s.empty()) {
                return std::nullopt;
            }
        }
    }
    return *days * 86400LL * 1000000LL + micros_of_day - offset_micros;
}

inline std::int64_t floor_div(std::int64_t a, std::int64_t b) {
    const std::int64_t q = a / b;
    return (a % b != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
}

// The registry formats' decimal carrier: two's complement big-endian bytes
// of the unscaled value, minimal length (or padded to `fixed_size`).
inline std::vector<std::uint8_t> decimal_to_bytes(const arrow::Decimal128& v,
                                                  std::optional<std::size_t> fixed_size) {
    std::vector<std::uint8_t> be(16);
    const auto hi = static_cast<std::uint64_t>(v.high_bits());
    const auto lo = v.low_bits();
    for (int i = 0; i < 8; ++i) {
        be[static_cast<std::size_t>(i)] = static_cast<std::uint8_t>((hi >> (56 - 8 * i)) & 0xff);
        be[static_cast<std::size_t>(8 + i)] =
            static_cast<std::uint8_t>((lo >> (56 - 8 * i)) & 0xff);
    }
    // Strip redundant sign bytes.
    while (be.size() > 1 &&
           ((be[0] == 0x00 && (be[1] & 0x80) == 0) || (be[0] == 0xff && (be[1] & 0x80) != 0))) {
        be.erase(be.begin());
    }
    if (fixed_size.has_value()) {
        if (be.size() > *fixed_size) {
            throw std::runtime_error("decimal does not fit the schema's fixed(" +
                                     std::to_string(*fixed_size) + ")");
        }
        const std::uint8_t pad = (be[0] & 0x80) != 0 ? 0xff : 0x00;
        be.insert(be.begin(), *fixed_size - be.size(), pad);
    }
    return be;
}

inline std::optional<arrow::Decimal128> decimal_from_bytes(const std::uint8_t* data,
                                                           std::size_t n) {
    if (n == 0 || n > 16) {
        return std::nullopt;
    }
    auto r = arrow::Decimal128::FromBigEndian(data, static_cast<std::int32_t>(n));
    if (!r.ok()) {
        return std::nullopt;
    }
    return *r;
}

// A JSON value (or the exact numeral text the JSON carried) as a decimal at
// exactly `scale`. Strings and numbers are accepted; a value with more
// fractional digits than the scale is refused rather than rounded silently.
inline std::optional<arrow::Decimal128> decimal_at_scale(const clink::config::JsonValue& v,
                                                         const std::string* raw_token,
                                                         int scale) {
    std::optional<clink::config::Decimal> d;
    if (raw_token != nullptr) {
        d = clink::config::dec_parse(*raw_token);
    } else if (v.is_string()) {
        d = clink::config::dec_parse(v.as_string());
    } else if (v.is_number()) {
        d = clink::config::dec_parse(v.serialize());
    }
    if (!d.has_value()) {
        return std::nullopt;
    }
    if (d->scale == scale) {
        return d->unscaled;
    }
    auto r = d->unscaled.Rescale(d->scale, scale);
    if (!r.ok()) {
        return std::nullopt;
    }
    return *r;
}

inline std::string decimal_string(const arrow::Decimal128& unscaled, int scale) {
    return unscaled.ToString(scale);
}

inline bool is_integral_json(const clink::config::JsonValue& v) {
    if (v.is_integral_number()) {
        return true;
    }
    if (!v.is_number()) {
        return false;
    }
    const double d = v.as_number();
    return std::isfinite(d) && std::floor(d) == d && std::fabs(d) < 9.2e18;
}

}  // namespace clink::schema_registry::detail
