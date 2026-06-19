// Fluent builder for the clickhouse_sink factory. Lives at
// include/clink/api/ during Phase 1 of the impls split; in Phase 2
// this header moves to impls/clickhouse/include/clink/api/.

#pragma once

#include <cstdint>
#include <string>
#include <utility>

#include "clink/api/descriptors.hpp"

namespace clink::api {

// Inserts std::string records as rows into a ClickHouse table.
// Available only when the cluster build linked clickhouse-cpp; if not,
// the underlying connector throws on construction.
class ClickHouseSink {
public:
    class Builder {
    public:
        Builder& host(std::string h) {
            host_ = std::move(h);
            return *this;
        }
        Builder& port(std::uint16_t p) {
            port_ = p;
            return *this;
        }
        Builder& database(std::string d) {
            database_ = std::move(d);
            return *this;
        }
        Builder& table(std::string t) {
            table_ = std::move(t);
            return *this;
        }
        Builder& user(std::string u) {
            user_ = std::move(u);
            return *this;
        }
        Builder& password(std::string p) {
            password_ = std::move(p);
            return *this;
        }
        Builder& format(std::string f) {
            format_ = std::move(f);
            return *this;
        }
        Builder& batch_rows(std::int64_t n) {
            batch_rows_ = n;
            return *this;
        }
        Builder& batch_interval_ms(std::int64_t ms) {
            batch_interval_ms_ = ms;
            return *this;
        }

        SinkDescriptor build() const {
            SinkDescriptor d;
            d.op_type = "clickhouse_sink";
            d.channel_type = "string";
            if (!host_.empty()) {
                d.params["host"] = host_;
            }
            if (port_ != 0) {
                d.params["port"] = std::to_string(port_);
            }
            if (!database_.empty()) {
                d.params["database"] = database_;
            }
            d.params["table"] = table_;
            if (!user_.empty()) {
                d.params["user"] = user_;
            }
            if (!password_.empty()) {
                d.params["password"] = password_;
            }
            if (!format_.empty()) {
                d.params["format"] = format_;
            }
            if (batch_rows_ > 0) {
                d.params["batch_rows"] = std::to_string(batch_rows_);
            }
            if (batch_interval_ms_ > 0) {
                d.params["batch_interval_ms"] = std::to_string(batch_interval_ms_);
            }
            return d;
        }

    private:
        std::string host_;
        std::uint16_t port_{0};
        std::string database_;
        std::string table_;
        std::string user_;
        std::string password_;
        std::string format_;
        std::int64_t batch_rows_{0};
        std::int64_t batch_interval_ms_{0};
    };

    static Builder builder() { return Builder{}; }
};

}  // namespace clink::api
