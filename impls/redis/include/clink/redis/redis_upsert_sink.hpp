#pragma once

// RedisUpsertSink - changelog-aware key-value Redis sink for mode='upsert'.
//
// Distinct from the append-only Streams sink (redis_sink, XADD): this one keys
// each record by its PRIMARY KEY and maintains a key-value view:
//   insert / update_after  -> SET <key> <json-value>
//   delete / update_before -> DEL <key>
//   (a row with no __row_kind is an implicit insert)
// where <key> = "<key_prefix><pk1>\x1f<pk2>..." (the primary-key tuple, prefixed).
//
// Within a flush the changelog is netted by primary key (last op wins), then the
// surviving SET/DEL commands are pipelined. EFFECTIVELY-ONCE on the keyspace for
// a stable primary key and a deterministic defining query: SET and DEL are keyed
// and idempotent, so a replay converges the keyspace to the same final state. It
// is NOT two-phase commit.
//
// Lets a retracting SQL query (GROUP BY, TOP-N, outer join) maintain a Redis
// key-value view; the Streams sink can only append.
//
// The stored value is the row's JSON with the synthetic "__row_kind" field
// removed, so a reader sees a clean object.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/redis/redis_client.hpp"

namespace clink::redis {

struct RedisUpsertSinkOptions {
    ConnectOptions conn;
    std::vector<std::string> key_columns;  // PRIMARY KEY (required)
    std::string key_prefix;                // namespace prefix on the Redis key (optional)
    std::size_t batch_records{1000};
    std::string name{"redis_upsert_sink"};
};

class RedisUpsertSink : public Sink<std::string> {
public:
    explicit RedisUpsertSink(RedisUpsertSinkOptions opts) : opts_(std::move(opts)) {
        if (opts_.key_columns.empty()) {
            throw std::runtime_error(opts_.name + ": 'key_columns' (the primary key) is required");
        }
        if (opts_.batch_records == 0) {
            opts_.batch_records = 1;
        }
    }

    void open() override { conn_ = std::make_unique<Connection>(opts_.conn); }

    void on_data(const Batch<std::string>& batch) override {
        for (const auto& rec : batch) {
            net_(rec.value());
        }
        if (netted_.size() >= opts_.batch_records) {
            flush();
        }
    }

    void on_barrier(CheckpointBarrier /*b*/) override { flush(); }

    void flush() override {
        if (netted_.empty()) {
            return;
        }
        if (conn_ == nullptr) {
            throw std::runtime_error(opts_.name + ": flush() before open()");
        }
        const std::size_t n = netted_.size();
        const auto t0 = std::chrono::steady_clock::now();
        try {
            // Pipeline every SET/DEL, then drain the replies in order: one network
            // round-trip for the whole batch.
            for (auto& [key, entry] : netted_) {
                if (entry.is_delete) {
                    conn_->append({"DEL", key});
                } else {
                    conn_->append({"SET", key, entry.value});
                }
            }
            for (std::size_t i = 0; i < n; ++i) {
                Reply r = conn_->get_reply();
                if (r.is_error()) {
                    throw std::runtime_error(opts_.name + ": command failed: " + r.error_text());
                }
            }
        } catch (...) {
            clink::metrics::connector::error_inc("redis", "sink");
            netted_.clear();
            conn_.reset();  // drop a possibly-desynced connection; re-open reconnects
            throw;          // job replays from the last checkpoint (keyed ops are idempotent)
        }
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::records_out_inc("redis", n);
        clink::metrics::connector::commit_latency_observe("redis", static_cast<std::uint64_t>(dt));
        netted_.clear();
    }

    void close() override {
        flush();
        conn_.reset();
    }

    std::string name() const override { return opts_.name; }

private:
    struct Entry {
        bool is_delete{false};
        std::string value;  // clean JSON (no __row_kind); empty for a delete
    };

    void net_(const std::string& json) {
        auto j = clink::config::parse(json);
        if (!j.is_object()) {
            throw std::runtime_error(opts_.name + ": sink row is not a JSON object: " + json);
        }
        auto& obj = j.as_object();

        bool is_delete = false;
        if (auto it = obj.find("__row_kind"); it != obj.end() && it->second.is_string()) {
            const std::string& k = it->second.as_string();
            is_delete = (k == "delete" || k == "update_before");
        }

        std::string key = opts_.key_prefix;
        for (const auto& kc : opts_.key_columns) {
            auto it = obj.find(kc);
            if (it == obj.end() || it->second.is_null()) {
                throw std::runtime_error(opts_.name + ": changelog row missing primary key '" + kc +
                                         "': " + json);
            }
            key += it->second.serialize(0);
            key.push_back('\x1f');
        }

        // Store a clean value (drop the synthetic changelog marker).
        obj.erase("__row_kind");
        std::string value = is_delete ? std::string{} : clink::config::JsonValue{obj}.serialize(0);
        netted_[std::move(key)] = Entry{is_delete, std::move(value)};
    }

    RedisUpsertSinkOptions opts_;
    std::unique_ptr<Connection> conn_;
    std::map<std::string, Entry> netted_;
};

}  // namespace clink::redis
