#pragma once

// What the cases need to read a store back: every cell as text, through
// Relate's C library, on a connection of their own. Reading goes through
// Store::open, so a case can look at a store while a sink still holds it (the
// process shares one handle), which is also how it sees what is committed and
// what is not.

#include <array>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <relate.h>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "store.hpp"

namespace clink::relate::testing {

inline std::string padded(std::int64_t v, std::size_t width) {
    std::string s = std::to_string(v);
    while (s.size() < width) {
        s.insert(s.begin(), '0');
    }
    return s;
}

inline std::string cell_text(relate_value v) {
    switch (relate_value_kind(v)) {
        case RELATE_V_NULL:
            return "null";
        case RELATE_V_BOOL: {
            std::uint8_t b = 0;
            relate_value_bool(v, &b);
            return b != 0 ? "true" : "false";
        }
        case RELATE_V_INT: {
            std::int64_t i = 0;
            relate_value_int(v, &i);
            return std::to_string(i);
        }
        case RELATE_V_FLOAT: {
            double d = 0;
            relate_value_float(v, &d);
            if (std::isnan(d)) {
                return "NaN";
            }
            if (std::isinf(d)) {
                return d > 0 ? "Infinity" : "-Infinity";
            }
            std::array<char, 32> buf{};
            const auto res = std::to_chars(buf.data(), buf.data() + buf.size(), d);
            return {buf.data(), res.ptr};
        }
        case RELATE_V_STRING: {
            std::size_t len = 0;
            const char* s = relate_value_string(v, &len);
            return {s, len};
        }
        case RELATE_V_LIST: {
            std::string out = "[";
            for (long long i = 0; i < relate_value_list_len(v); ++i) {
                out += (i > 0 ? ", " : "") + cell_text(relate_value_list_get(v, i));
            }
            return out + "]";
        }
        case RELATE_V_MAP: {
            std::string out = "{";
            for (long long i = 0; i < relate_value_map_len(v); ++i) {
                const std::string key = relate_value_map_key(v, i, nullptr);
                out += (i > 0 ? ", " : "") + key + ": " +
                       cell_text(relate_value_map_get(v, key.c_str()));
            }
            return out + "}";
        }
        case RELATE_V_DATE:
        case RELATE_V_LOCAL_DATETIME:
        case RELATE_V_DATETIME: {
            std::int64_t y = 0;
            std::int64_t mo = 0;
            std::int64_t d = 0;
            relate_value_year(v, &y);
            relate_value_month(v, &mo);
            relate_value_day(v, &d);
            std::string out = padded(y, 4) + "-" + padded(mo, 2) + "-" + padded(d, 2);
            if (relate_value_kind(v) == RELATE_V_DATE) {
                return out;
            }
            std::int64_t h = 0;
            std::int64_t mi = 0;
            std::int64_t s = 0;
            std::int64_t ns = 0;
            relate_value_hour(v, &h);
            relate_value_minute(v, &mi);
            relate_value_second(v, &s);
            relate_value_nanosecond(v, &ns);
            out += "T" + padded(h, 2) + ":" + padded(mi, 2) + ":" + padded(s, 2) + "." +
                   padded(ns / 1'000'000, 3);
            if (relate_value_kind(v) == RELATE_V_DATETIME) {
                std::int64_t offset = -1;
                relate_value_offset_seconds(v, &offset);
                out += offset == 0 ? "Z" : "+" + std::to_string(offset) + "s";
            }
            return out;
        }
        default:
            return "<kind " + std::to_string(relate_value_kind(v)) + ">";
    }
}

// Runs `cypher` on a connection of its own and returns each row's cells as
// text. Throws when the statement fails.
inline std::vector<std::vector<std::string>> query(Store& store, const std::string& cypher) {
    relate_connection conn = nullptr;
    if (relate_connect_namespace(store.handle(), nullptr, &conn, nullptr) != RELATE_OK) {
        throw std::runtime_error("the case could not connect to " + store.path());
    }
    relate_query_result result = nullptr;
    if (relate_query_params(conn, cypher.c_str(), nullptr, &result, nullptr) != RELATE_OK) {
        relate_disconnect(conn);
        throw std::runtime_error("the case's connection failed running: " + cypher);
    }
    if (relate_result_is_error(result) != 0) {
        const std::string why = relate_result_error_json(result);
        relate_destroy_result(result);
        relate_disconnect(conn);
        throw std::runtime_error("the case's query failed: " + why + ": " + cypher);
    }
    std::vector<std::vector<std::string>> rows;
    const long long columns = relate_column_count(result);
    for (long long r = 0; r < relate_row_count(result); ++r) {
        std::vector<std::string> row;
        for (long long c = 0; c < columns; ++c) {
            row.push_back(cell_text(relate_value_at(result, r, c)));
        }
        rows.push_back(std::move(row));
    }
    relate_destroy_result(result);
    relate_disconnect(conn);
    return rows;
}

inline std::vector<std::vector<std::string>> query(const std::string& path,
                                                   const std::string& cypher) {
    const auto store = Store::open(path);
    return query(*store, cypher);
}

// The single number a counting query returns.
inline std::int64_t count(const std::string& path, const std::string& cypher) {
    const auto rows = query(path, cypher);
    if (rows.size() != 1 || rows[0].size() != 1) {
        throw std::runtime_error("not a single number: " + cypher);
    }
    return std::stoll(rows[0][0]);
}

// A directory of this case's own, named for the process and the case so that
// cases run side by side never share a store, and removed when it ends.
class TempDir {
public:
    TempDir() {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        dir_ = std::filesystem::temp_directory_path() /
               ("clink-relate-" + std::to_string(::getpid()) + "-" +
                (info != nullptr ? std::string(info->test_suite_name()) + "-" + info->name()
                                 : std::string("case")));
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] std::string path(const std::string& name) const { return (dir_ / name).string(); }

private:
    std::filesystem::path dir_;
};

}  // namespace clink::relate::testing
