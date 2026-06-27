#pragma once

// Minimal RAII wrapper over the mariadb-connector-c synchronous C API, shared by
// the MySQL source and sink. Two concerns it centralises:
//   1. result lifecycle - every MYSQL_RES* must be mysql_free_result'd exactly
//      once; Result is the RAII owner.
//   2. connection lifecycle - the MYSQL* is owned, charset/timeout options + the
//      connect are done in the ctor (throws on failure so a constructed
//      Connection is always usable), and query errors throw so the caller can
//      surface them (sink rethrows -> job replays = at-least-once).
//
// Compiled only where mariadb-connector-c is found (CLINK_HAS_MYSQL); the whole
// impls/mysql module is dep-gated on it. The include is <mariadb/mysql.h>.

#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

#include <mariadb/mysql.h>

namespace clink::mysql {

struct ConnectOptions {
    std::string host{"localhost"};
    std::uint16_t port{3306};
    std::string user;
    std::string password;
    std::string database;  // selected in-band at connect
    std::chrono::milliseconds connect_timeout{5000};
};

// Owns a MYSQL_RES*. Move-only; frees exactly once. Row access is by the
// underlying char** (text protocol: every cell is a string or NULL).
class Result {
public:
    Result() = default;
    explicit Result(MYSQL_RES* res) : res_(res) {}
    Result(const Result&) = delete;
    Result& operator=(const Result&) = delete;
    Result(Result&& o) noexcept : res_(o.res_) { o.res_ = nullptr; }
    Result& operator=(Result&& o) noexcept {
        if (this != &o) {
            reset();
            res_ = o.res_;
            o.res_ = nullptr;
        }
        return *this;
    }
    ~Result() { reset(); }

    void reset() {
        if (res_ != nullptr) {
            mysql_free_result(res_);
            res_ = nullptr;
        }
    }
    explicit operator bool() const noexcept { return res_ != nullptr; }

    unsigned int num_fields() const { return mysql_num_fields(res_); }
    MYSQL_FIELD* fields() const { return mysql_fetch_fields(res_); }
    MYSQL_ROW fetch_row() { return mysql_fetch_row(res_); }         // NULL = end of rows
    unsigned long* lengths() { return mysql_fetch_lengths(res_); }  // valid after fetch_row

private:
    MYSQL_RES* res_{nullptr};
};

// Owns a MYSQL*. Move-only. Connects (+ charset/timeout) in the ctor; throws on
// any failure.
class Connection {
public:
    explicit Connection(const ConnectOptions& o) {
        h_ = mysql_init(nullptr);
        if (h_ == nullptr) {
            throw std::runtime_error("mysql: mysql_init returned null (out of memory)");
        }
        // Only set a finite connect timeout; a 0s value means "no timeout"
        // (infinite block), which we never want. <=0 falls back to the lib default.
        if (o.connect_timeout.count() > 0) {
            const unsigned int timeout_s =
                static_cast<unsigned int>((o.connect_timeout.count() + 999) / 1000);
            mysql_options(h_, MYSQL_OPT_CONNECT_TIMEOUT, &timeout_s);
        }
        mysql_options(h_, MYSQL_SET_CHARSET_NAME, "utf8mb4");
        if (mysql_real_connect(h_,
                               o.host.c_str(),
                               o.user.c_str(),
                               o.password.empty() ? nullptr : o.password.c_str(),
                               o.database.empty() ? nullptr : o.database.c_str(),
                               o.port,
                               nullptr,
                               0) == nullptr) {
            const std::string err = mysql_error(h_);
            mysql_close(h_);
            h_ = nullptr;
            throw std::runtime_error("mysql: connect to " + o.host + ":" + std::to_string(o.port) +
                                     " failed: " + err);
        }
    }

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&& o) noexcept : h_(o.h_) { o.h_ = nullptr; }
    Connection& operator=(Connection&& o) noexcept {
        if (this != &o) {
            free_();
            h_ = o.h_;
            o.h_ = nullptr;
        }
        return *this;
    }
    ~Connection() { free_(); }

    // Run a statement with no result set (INSERT/UPDATE/DDL). Throws on error.
    void exec(std::string_view sql) {
        if (h_ == nullptr) {
            throw std::runtime_error("mysql: exec on a closed connection");
        }
        if (mysql_real_query(h_, sql.data(), sql.size()) != 0) {
            throw std::runtime_error("mysql: query failed (errno " +
                                     std::to_string(mysql_errno(h_)) + "): " + mysql_error(h_));
        }
    }

    // Run a SELECT and buffer the full result set. Throws on error.
    Result query(std::string_view sql) {
        if (h_ == nullptr) {
            throw std::runtime_error("mysql: query on a closed connection");
        }
        if (mysql_real_query(h_, sql.data(), sql.size()) != 0) {
            throw std::runtime_error("mysql: query failed (errno " +
                                     std::to_string(mysql_errno(h_)) + "): " + mysql_error(h_));
        }
        MYSQL_RES* res = mysql_store_result(h_);
        if (res == nullptr && mysql_errno(h_) != 0) {
            throw std::runtime_error("mysql: store_result failed (errno " +
                                     std::to_string(mysql_errno(h_)) + "): " + mysql_error(h_));
        }
        return Result{res};
    }

    // Charset-correct escaping of a string VALUE (needs the live handle). NOT for
    // identifiers (use quote_ident in mysql_sql.hpp for those).
    std::string escape(std::string_view in) {
        if (h_ == nullptr) {
            throw std::runtime_error("mysql: escape on a closed connection");
        }
        std::string out(in.size() * 2 + 1, '\0');
        const unsigned long n = mysql_real_escape_string(
            h_, out.data(), in.data(), static_cast<unsigned long>(in.size()));
        out.resize(n);
        return out;
    }

    bool ok() const noexcept { return h_ != nullptr; }

private:
    void free_() {
        if (h_ != nullptr) {
            mysql_close(h_);
            h_ = nullptr;
        }
    }
    MYSQL* h_{nullptr};
};

}  // namespace clink::mysql
