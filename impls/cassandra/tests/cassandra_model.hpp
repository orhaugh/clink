#pragma once

// A model of a Cassandra table behind the sinks' CassandraWriteService, for
// deterministic tests of the production sinks: it reports a schema, records
// every statement and write, scripts each write's outcome, can hold writes until
// the test lets them finish (in order or reversed), and keeps the table by
// last-write-wins on the writes' timestamps, as the server does: a row's insert
// counts only when its timestamp is above the row's latest insert and delete.

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "clink/cassandra/cassandra_service.hpp"
#include "clink/config/json.hpp"

namespace clink::cassandra::testing {

class CassandraModel final : public CassandraWriteService {
public:
    // What one write does: whether the table takes it, what the sink is told,
    // and whether it stays unresolved until release_held().
    struct Plan {
        bool apply{true};
        CassandraOutcome outcome;
        bool hold{false};
    };

    struct Row {
        std::string json;
        std::int64_t live_ts{-1};
        std::int64_t tombstone_ts{-1};
        [[nodiscard]] bool visible() const { return live_ts > tombstone_ts; }
    };

    CassandraModel() {
        meta_.keyspace_found = true;
        meta_.table_found = true;
        set_key({{"id", "int"}}, {});
    }

    // ---- set up by the test ----

    void set_key(std::vector<CassandraColumnMeta> partition,
                 std::vector<CassandraColumnMeta> clustering) {
        const std::lock_guard<std::mutex> lock(mu_);
        meta_.partition_key = std::move(partition);
        meta_.clustering_key = std::move(clustering);
        meta_.columns = meta_.partition_key;
        meta_.columns.insert(
            meta_.columns.end(), meta_.clustering_key.begin(), meta_.clustering_key.end());
        meta_.columns.push_back({"v", "text"});
    }
    void set_missing(bool keyspace, bool table) {
        const std::lock_guard<std::mutex> lock(mu_);
        meta_.keyspace_found = !keyspace;
        meta_.table_found = !table && !keyspace;
    }
    void fail_connect(CassandraOutcome o) {
        const std::lock_guard<std::mutex> lock(mu_);
        connect_outcome_ = std::move(o);
    }
    void fail_prepare(CassandraOutcome o) {
        const std::lock_guard<std::mutex> lock(mu_);
        prepare_outcome_ = std::move(o);
    }
    // Decides each write's plan from the write and its position (0-based).
    void script(std::function<Plan(const CassandraWrite&, std::size_t)> f) {
        const std::lock_guard<std::mutex> lock(mu_);
        script_ = std::move(f);
    }
    // Every write from now on is held until release_held().
    void hold_all(bool on) {
        script([on](const CassandraWrite&, std::size_t) {
            Plan p;
            p.hold = on;
            return p;
        });
    }

    // Resolves every held write, applying the ones planned to apply, in the
    // order they were submitted or reversed.
    void release_held(bool reversed = false) {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            std::vector<std::size_t> held;
            for (std::size_t i = 0; i < writes_.size(); ++i) {
                if (!writes_[i].done) {
                    held.push_back(i);
                }
            }
            if (reversed) {
                std::reverse(held.begin(), held.end());
            }
            for (const auto i : held) {
                finish(writes_[i]);
            }
        }
        cv_.notify_all();
    }

    // ---- observations ----

    [[nodiscard]] std::vector<CassandraWrite> submitted() const {
        const std::lock_guard<std::mutex> lock(mu_);
        std::vector<CassandraWrite> out;
        for (const auto& w : writes_) {
            out.push_back(w.write);
        }
        return out;
    }
    [[nodiscard]] std::size_t submitted_count() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return writes_.size();
    }
    // Writes the sink holds tickets for: submitted and not yet released.
    [[nodiscard]] std::size_t outstanding() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return outstanding_;
    }
    [[nodiscard]] std::size_t max_outstanding() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return max_outstanding_;
    }
    // The tickets outstanding when each write was submitted, itself excluded.
    [[nodiscard]] std::vector<std::size_t> outstanding_at_submit() const {
        const std::lock_guard<std::mutex> lock(mu_);
        std::vector<std::size_t> out;
        for (const auto& w : writes_) {
            out.push_back(w.outstanding_before);
        }
        return out;
    }
    [[nodiscard]] std::map<std::string, Row> rows() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return rows_;
    }
    // The visible row for a key (the key values' JSON, joined by '|'), if any.
    [[nodiscard]] std::optional<std::string> row(const std::string& key) const {
        const std::lock_guard<std::mutex> lock(mu_);
        const auto it = rows_.find(key);
        if (it == rows_.end() || !it->second.visible()) {
            return std::nullopt;
        }
        return it->second.json;
    }
    [[nodiscard]] std::size_t visible_rows() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return static_cast<std::size_t>(std::count_if(
            rows_.begin(), rows_.end(), [](const auto& kv) { return kv.second.visible(); }));
    }
    [[nodiscard]] std::string insert_cql() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return insert_cql_;
    }
    [[nodiscard]] std::string delete_cql() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return delete_cql_;
    }
    [[nodiscard]] CassandraConnParams last_conn() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return last_conn_;
    }
    [[nodiscard]] int connects() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return connects_;
    }
    [[nodiscard]] int closes() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return closes_;
    }

    // ---- CassandraWriteService ----

    CassandraOutcome connect(const CassandraConnParams& conn, const CancelCheck&) override {
        const std::lock_guard<std::mutex> lock(mu_);
        ++connects_;
        last_conn_ = conn;
        return connect_outcome_;
    }

    CassandraTableMeta describe_table(const std::string&, const std::string&) override {
        const std::lock_guard<std::mutex> lock(mu_);
        return meta_;
    }

    CassandraOutcome prepare(const std::string& insert_cql,
                             const std::string& delete_cql,
                             const CancelCheck&) override {
        const std::lock_guard<std::mutex> lock(mu_);
        insert_cql_ = insert_cql;
        delete_cql_ = delete_cql;
        return prepare_outcome_;
    }

    std::uint64_t submit(const CassandraWrite& write) override {
        std::uint64_t ticket = 0;
        {
            const std::lock_guard<std::mutex> lock(mu_);
            Pending p;
            p.write = write;
            p.plan = script_ ? script_(write, writes_.size()) : Plan{};
            p.outstanding_before = outstanding_;
            ticket = writes_.size() + 1;
            writes_.push_back(std::move(p));
            ++outstanding_;
            max_outstanding_ = std::max(max_outstanding_, outstanding_);
            if (!writes_.back().plan.hold) {
                finish(writes_.back());
            }
        }
        cv_.notify_all();
        return ticket;
    }

    std::optional<CassandraOutcome> poll(std::uint64_t ticket,
                                         std::chrono::microseconds wait) override {
        std::unique_lock<std::mutex> lock(mu_);
        auto& w = writes_.at(ticket - 1);
        cv_.wait_for(lock, wait, [&w] { return w.done; });
        if (!w.done) {
            return std::nullopt;
        }
        return w.plan.outcome;
    }

    void release(std::uint64_t ticket) noexcept override {
        const std::lock_guard<std::mutex> lock(mu_);
        auto& w = writes_.at(ticket - 1);
        if (!w.released) {
            w.released = true;
            --outstanding_;
        }
    }

    void close() noexcept override {
        const std::lock_guard<std::mutex> lock(mu_);
        ++closes_;
        for (auto& w : writes_) {
            if (!w.released) {
                w.released = true;
                --outstanding_;
            }
        }
    }

private:
    struct Pending {
        CassandraWrite write;
        Plan plan;
        bool done{false};
        bool released{false};
        std::size_t outstanding_before{0};
    };

    // The key the server would see: each key column's value read as JSON and
    // written back, so two spellings of one value name one row.
    std::string key_of(const CassandraWrite& w) const {
        std::vector<std::string> names;
        for (const auto& c : meta_.partition_key) {
            names.push_back(c.name);
        }
        for (const auto& c : meta_.clustering_key) {
            names.push_back(c.name);
        }
        std::string key;
        if (w.kind == CassandraWrite::Kind::Delete) {
            for (std::size_t i = 0; i < w.key_json.size(); ++i) {
                key += (i == 0 ? "" : "|") + clink::config::parse(w.key_json[i]).serialize(0);
            }
            return key;
        }
        const auto obj = clink::config::parse(w.json);
        for (std::size_t i = 0; i < names.size(); ++i) {
            std::string value = "<missing>";
            for (const auto& [k, v] : obj.as_object()) {
                std::string lower = k;
                std::transform(lower.begin(), lower.end(), lower.begin(), [](char c) {
                    return (c >= 'A' && c <= 'Z') ? c - 'A' + 'a' : c;
                });
                if (lower == names[i]) {
                    value = v.serialize(0);
                }
            }
            key += (i == 0 ? "" : "|") + value;
        }
        return key;
    }

    void finish(Pending& p) {
        p.done = true;
        if (!p.plan.apply) {
            return;
        }
        auto& row = rows_[key_of(p.write)];
        const auto ts = p.write.timestamp_us;
        if (p.write.kind == CassandraWrite::Kind::Delete) {
            row.tombstone_ts = std::max(row.tombstone_ts, ts);
        } else if (ts > row.live_ts) {
            row.live_ts = ts;
            row.json = p.write.json;
        }
    }

    mutable std::mutex mu_;
    std::condition_variable cv_;
    CassandraTableMeta meta_;
    CassandraOutcome connect_outcome_;
    CassandraOutcome prepare_outcome_;
    std::function<Plan(const CassandraWrite&, std::size_t)> script_;
    std::deque<Pending> writes_;  // a deque: poll() holds a reference while waiting
    std::size_t outstanding_{0};
    std::size_t max_outstanding_{0};
    std::map<std::string, Row> rows_;
    std::string insert_cql_;
    std::string delete_cql_;
    CassandraConnParams last_conn_;
    int connects_{0};
    int closes_{0};
};

inline CassandraOutcome outcome(CassandraFault f, std::string error, std::string message = {}) {
    CassandraOutcome o;
    o.fault = f;
    o.error = std::move(error);
    o.message = std::move(message);
    return o;
}

}  // namespace clink::cassandra::testing
