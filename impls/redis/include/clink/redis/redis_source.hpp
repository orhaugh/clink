#pragma once

// Redis Streams source: reads a stream via a consumer group (XREADGROUP). Each
// subtask joins the group as a distinct consumer ("<prefix>-<subtask_idx>"), so
// the group hands disjoint entries to the parallel subtasks - this, not modulo
// partitioning, is how Redis Streams parallelises. Each entry is emitted as one
// std::string: when it has the single field the redis_sink writes (default "v")
// its value is emitted verbatim (clean round-trip); otherwise the whole
// field/value map is emitted as a JSON object.
//
// DELIVERY = AT-LEAST-ONCE. The group's per-consumer pending-entries list (PEL)
// is the durable cursor: on open() each consumer first re-reads its PEL (id "0"),
// re-delivering anything delivered-but-not-acked before a crash, then switches to
// new entries (">"). XACK is the offset commit and rides snapshot_offset() (which
// the runner calls on the source thread, never concurrently with produce()), so
// entries delivered since the last checkpoint are acknowledged when that
// checkpoint is taken.
//
// CAVEAT (honest, same class as the Postgres CDC standby ack): XACK is an
// external side effect not transactional with the global checkpoint. If a
// checkpoint's snapshot_offset XACKs a batch and the GLOBAL checkpoint then fails,
// those entries are gone from the PEL and not replayed - at-most-once for that
// one batch. Strict exactly-once would need a source on_commit hook (deferred).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/redis/redis_client.hpp"
#include "clink/state/state_backend.hpp"

namespace clink::redis {

struct RedisSourceOptions {
    ConnectOptions conn;
    std::string stream;                    // stream key (required)
    std::string group;                     // consumer-group name (required)
    std::string consumer_prefix{"clink"};  // consumer = "<prefix>-<subtask_idx>"
    std::string field{"v"};  // single-field round-trip: emit this field's value verbatim
    int count{500};          // XREADGROUP COUNT per call
    std::chrono::milliseconds block{500};  // XREADGROUP BLOCK (bounds cancel latency)
    std::string start_id{"$"};             // group create position: "$" new-only | "0" all history
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};
    std::string name{"redis_source"};
};

class RedisSource : public Source<std::string> {
public:
    explicit RedisSource(RedisSourceOptions opts) : opts_(std::move(opts)) {
        if (opts_.stream.empty()) {
            throw std::runtime_error(opts_.name + ": 'stream' is required");
        }
        if (opts_.group.empty()) {
            throw std::runtime_error(opts_.name + ": 'group' is required");
        }
        if (opts_.count < 1) {
            opts_.count = 1;
        }
    }

    void open() override {
        consumer_ = opts_.consumer_prefix + "-" + std::to_string(opts_.subtask_idx);
        // The socket command timeout must exceed BLOCK so a legitimate long-poll
        // (server returns NIL after BLOCK ms) never trips the client timeout.
        opts_.conn.command_timeout = opts_.block * 2 + std::chrono::milliseconds{2000};
        conn_ = std::make_unique<Connection>(opts_.conn);
        ensure_group_();
        // Re-drain this consumer's PEL first (recovers un-acked deliveries from a
        // prior run), then switch to new entries.
        pel_cursor_ = "0";
        pel_drained_ = false;
    }

    bool produce(Emitter<std::string>& out) override {
        if (this->cancelled() || conn_ == nullptr) {
            return false;
        }
        const std::string read_id = pel_drained_ ? ">" : pel_cursor_;
        Reply reply = conn_->command(xreadgroup_args_(read_id));
        if (!reply || reply.is_nil()) {
            // BLOCK timeout (no new entries) or empty PEL.
            if (!pel_drained_) {
                pel_drained_ = true;
            }
            return !this->cancelled();
        }
        if (reply->type != REDIS_REPLY_ARRAY) {
            return !this->cancelled();
        }
        Batch<std::string> batch;
        std::uint64_t bytes = 0;
        std::size_t entries_seen = 0;
        std::string last_id_this_call;
        for (std::size_t s = 0; s < reply->elements; ++s) {
            redisReply* stream = reply->element[s];
            if (stream == nullptr || stream->type != REDIS_REPLY_ARRAY || stream->elements < 2) {
                continue;
            }
            redisReply* entries = stream->element[1];
            if (entries == nullptr || entries->type != REDIS_REPLY_ARRAY) {
                continue;
            }
            for (std::size_t e = 0; e < entries->elements; ++e) {
                redisReply* entry = entries->element[e];
                if (entry == nullptr || entry->type != REDIS_REPLY_ARRAY || entry->elements < 2) {
                    continue;
                }
                redisReply* id = entry->element[0];
                redisReply* fields = entry->element[1];
                if (id == nullptr || id->type != REDIS_REPLY_STRING) {
                    continue;
                }
                std::string entry_id(id->str, id->len);
                std::string payload = render_entry_(fields);
                bytes += payload.size();
                batch.emplace(std::move(payload));
                unacked_ids_.push_back(entry_id);
                last_id_this_call = entry_id;
                last_delivered_id_ = entry_id;
                ++entries_seen;
            }
        }
        if (!pel_drained_) {
            if (entries_seen == 0) {
                pel_drained_ = true;  // PEL fully replayed; new entries from here
            } else {
                pel_cursor_ = last_id_this_call;  // page the PEL
            }
        }
        if (!batch.empty()) {
            const auto n = batch.size();
            clink::metrics::connector::records_in_inc("redis", n);
            clink::metrics::connector::bytes_in_inc("redis", bytes);
            out.emit_data(std::move(batch));
        }
        return !this->cancelled();
    }

    void cancel() override { Source<std::string>::cancel(); }

    void close() override { conn_.reset(); }

    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    std::string name() const override { return opts_.name; }

    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId /*ckpt*/) override {
        // XACK everything delivered since the last checkpoint: the offset commit.
        if (conn_ != nullptr && !unacked_ids_.empty()) {
            std::vector<std::string> args;
            args.reserve(unacked_ids_.size() + 3);
            args.emplace_back("XACK");
            args.push_back(opts_.stream);
            args.push_back(opts_.group);
            for (const auto& id : unacked_ids_) {
                args.push_back(id);
            }
            try {
                conn_->command(args);
            } catch (...) {
                clink::metrics::connector::error_inc("redis", "source");
                throw;
            }
            unacked_ids_.clear();
        }
        if (!last_delivered_id_.empty()) {
            const std::string key = std::string(kIdPrefix) + opts_.stream;
            backend.put_operator_state(
                op_id,
                key,
                StateBackend::ValueView{last_delivered_id_.data(), last_delivered_id_.size()});
        }
    }

    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        bool found = false;
        const std::string_view prefix{kIdPrefix};
        backend.scan_operator_state(
            op_id, [&](StateBackend::KeyView key, StateBackend::ValueView value) {
                if (key.size() <= prefix.size() || key.substr(0, prefix.size()) != prefix) {
                    return;
                }
                restored_last_id_ = std::string{value};
                found = true;
            });
        // The PEL (re-drained in open()) is the real recovery mechanism; the
        // persisted id is a checkpoint marker / observability handle.
        return found;
    }

    // Observability/test accessor: the id of the last entry this source emitted.
    [[nodiscard]] const std::string& last_delivered_id() const noexcept {
        return last_delivered_id_;
    }
    [[nodiscard]] const std::string& consumer_name() const noexcept { return consumer_; }

private:
    static constexpr const char* kIdPrefix = "__redis_last_id__:";

    void ensure_group_() {
        Reply r = conn_->command(
            {"XGROUP", "CREATE", opts_.stream, opts_.group, opts_.start_id, "MKSTREAM"});
        if (r.is_error()) {
            const std::string e = r.error_text();
            if (e.rfind("BUSYGROUP", 0) != 0) {  // group already exists is fine
                throw std::runtime_error(opts_.name + ": XGROUP CREATE failed: " + e);
            }
        }
    }

    std::vector<std::string> xreadgroup_args_(const std::string& read_id) const {
        return {"XREADGROUP",
                "GROUP",
                opts_.group,
                consumer_,
                "COUNT",
                std::to_string(opts_.count),
                "BLOCK",
                std::to_string(opts_.block.count()),
                "STREAMS",
                opts_.stream,
                read_id};
    }

    // Render one entry's field list to a payload string. Fast path: a single
    // field equal to opts_.field => emit its value verbatim (the redis_sink
    // round-trip). Otherwise serialise the whole field/value map as a JSON object
    // (values are Redis bulk strings, so all string-typed).
    std::string render_entry_(redisReply* fields) const {
        if (fields == nullptr || fields->type != REDIS_REPLY_ARRAY || fields->elements == 0) {
            return "{}";
        }
        if (fields->elements == 2) {
            redisReply* f = fields->element[0];
            redisReply* v = fields->element[1];
            if (f != nullptr && v != nullptr && f->type == REDIS_REPLY_STRING &&
                v->type == REDIS_REPLY_STRING && std::string_view(f->str, f->len) == opts_.field) {
                return std::string(v->str, v->len);
            }
        }
        clink::config::JsonObject obj;
        for (std::size_t i = 0; i + 1 < fields->elements; i += 2) {
            redisReply* f = fields->element[i];
            redisReply* v = fields->element[i + 1];
            if (f == nullptr || f->type != REDIS_REPLY_STRING) {
                continue;
            }
            std::string key(f->str, f->len);
            std::string val = (v != nullptr && v->type == REDIS_REPLY_STRING)
                                  ? std::string(v->str, v->len)
                                  : std::string{};
            obj[key] = clink::config::JsonValue{std::move(val)};
        }
        return clink::config::JsonValue{std::move(obj)}.serialize(0);
    }

    RedisSourceOptions opts_;
    std::unique_ptr<Connection> conn_;
    std::string consumer_;
    std::string pel_cursor_{"0"};
    bool pel_drained_{false};
    std::vector<std::string> unacked_ids_;  // delivered since the last checkpoint
    std::string last_delivered_id_;
    std::string restored_last_id_;
};

}  // namespace clink::redis
