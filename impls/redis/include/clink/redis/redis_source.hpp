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
// new entries (">"). XACK is deferred to CHECKPOINT COMPLETION: snapshot_offset()
// seals the entries delivered since the last barrier as that checkpoint's, and
// notify_checkpoint_complete() makes every interval at or below the completed id
// ready; the next produce() turn (or close()) XACKs them, so every Redis call
// stays on the source thread. An aborted or never-completed checkpoint leaves
// its entries in the PEL, and the restart reads them again. Duplicates follow
// whenever a checkpoint completes and the process dies before its XACK lands.
// The completion is not the commit confirmation: a job whose restore selects an
// older checkpoint than one this source acknowledged (the commit-confirmed
// protocol, or an operator restoring a savepoint) does not see those entries
// again.
//
// BOUND: at most max_pending entries are held unacknowledged; at the bound the
// source stops reading until a checkpoint completes. A job that never completes
// a checkpoint (no checkpointing) therefore reads max_pending entries and waits.
//
// RESCALE: a consumer is named "<prefix>-<subtask_idx>" and a restart re-reads
// only its OWN PEL. A scale-down would leave the removed subtasks' consumers
// holding pending entries nothing reads again, and the source cannot claim them
// safely: Redis keeps no lease on a consumer, so a consumer that has gone looks
// the same as one whose process is still reading (a zombie worker, another job
// on the same group and prefix). So the capability record refuses a rescale to
// fewer subtasks, and open() refuses to start when a consumer outside this
// job's layout holds pending entries. Same-parallelism and scale-up restarts are
// unaffected.

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/connectors/checkpoint_ack_ledger.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/redis/redis_client.hpp"
#include "clink/runtime/log_buffer.hpp"
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
    // Most entries held delivered but unacknowledged, each waiting for the
    // checkpoint that covers it to complete. At the bound the source stops
    // reading until one does.
    std::size_t max_pending{100000};
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};
    std::string name{"redis_source"};
};

// A consumer of the group, as XINFO CONSUMERS reports it.
struct GroupConsumer {
    std::string name;
    long long pending{0};
};

// The consumers named for a subtask this job does not run ("<prefix>-<n>" with
// n >= parallelism) that still hold pending entries: what a run at a higher
// parallelism left behind. Other names belong to no layout of this job's.
inline std::vector<GroupConsumer> stranded_consumers(std::string_view prefix,
                                                     std::uint32_t parallelism,
                                                     const std::vector<GroupConsumer>& consumers) {
    std::vector<GroupConsumer> out;
    for (const auto& c : consumers) {
        const std::string_view name{c.name};
        if (c.pending <= 0 || name.size() <= prefix.size() + 1 ||
            name.substr(0, prefix.size()) != prefix || name[prefix.size()] != '-') {
            continue;
        }
        const std::string_view digits = name.substr(prefix.size() + 1);
        if (digits.size() > 9 || !std::all_of(digits.begin(), digits.end(), [](char ch) {
                return ch >= '0' && ch <= '9';
            })) {
            continue;
        }
        if (std::stoul(std::string{digits}) >= parallelism) {
            out.push_back(c);
        }
    }
    return out;
}

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
        // A non-positive block is normalised to the default rather than issued as
        // BLOCK 0: the server would then block FOREVER, which both trips the
        // finite client command timeout (a crash-loop on any idle stream) AND
        // wedges the runner thread inside produce() so cancel/shutdown can never
        // be observed. A finite block keeps produce() returning so cancel works.
        if (opts_.block.count() <= 0) {
            opts_.block = std::chrono::milliseconds{500};
        }
        if (opts_.max_pending < 1) {
            opts_.max_pending = 1;
        }
    }

    void open() override {
        consumer_ = opts_.consumer_prefix + "-" + std::to_string(opts_.subtask_idx);
        // The socket command timeout must exceed BLOCK so a legitimate long-poll
        // (server returns NIL after BLOCK ms) never trips the client timeout. With
        // block normalised to >0 in the ctor this invariant always holds.
        opts_.conn.command_timeout = opts_.block * 2 + std::chrono::milliseconds{2000};
        conn_ = std::make_unique<Connection>(opts_.conn);
        ensure_group_();
        refuse_stranded_pending_();
        // Re-drain this consumer's PEL first (recovers un-acked deliveries from a
        // prior run), then switch to new entries.
        pel_cursor_ = "0";
        pel_drained_ = false;
    }

    bool produce(Emitter<std::string>& out) override {
        if (this->cancelled() || conn_ == nullptr) {
            return false;
        }
        acknowledge_completed_();
        const std::size_t held = ledger_.outstanding();
        if (held >= opts_.max_pending) {
            if (!paused_) {
                paused_ = true;
                clink::log::warn("redis.source",
                                 opts_.name + " (" + consumer_ + "): " + std::to_string(held) +
                                     " entries wait for a completed checkpoint before they are "
                                     "acknowledged; reading paused until one completes "
                                     "(max_pending " +
                                     std::to_string(opts_.max_pending) + ")");
            }
            std::this_thread::sleep_for(std::min(opts_.block, std::chrono::milliseconds{50}));
            return !this->cancelled();
        }
        paused_ = false;
        const std::size_t room =
            std::min<std::size_t>(opts_.max_pending - held, static_cast<std::size_t>(opts_.count));
        const std::string read_id = pel_drained_ ? ">" : pel_cursor_;
        Reply reply = conn_->command(xreadgroup_args_(read_id, room));
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
                // An entry that has been XDEL'd while still pending comes back with
                // NIL fields: ack + page past it, but do NOT emit a phantom record.
                const bool tombstone = (fields == nullptr || fields->type == REDIS_REPLY_NIL);
                ledger_.delivered(entry_id);
                last_id_this_call = entry_id;
                last_delivered_id_ = entry_id;
                ++entries_seen;
                if (tombstone) {
                    continue;
                }
                std::string payload = render_entry_(fields);
                bytes += payload.size();
                batch.emplace(std::move(payload));
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

    // Acknowledges what completed checkpoints cover, and nothing else: the
    // entries of an interval no completed checkpoint covers stay pending for the
    // restart to read again, whether the source is closing at a cancel, a
    // failure or the end of a stop.
    void close() override {
        if (conn_ != nullptr) {
            try {
                acknowledge_completed_();
            } catch (const std::exception& e) {
                clink::log::warn("redis.source",
                                 opts_.name + " (" + consumer_ +
                                     "): entries a completed checkpoint covers were not "
                                     "acknowledged at close, and a restart reads them again: " +
                                     e.what());
            }
        }
        conn_.reset();
    }

    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    std::string name() const override { return opts_.name; }

    // Seals the entries delivered since the last barrier as this checkpoint's;
    // none is acknowledged until the checkpoint completes.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt) override {
        ledger_.seal(ckpt.value());
        if (!last_delivered_id_.empty()) {
            const std::string key = std::string(kIdPrefix) + opts_.stream;
            backend.put_operator_state(
                op_id,
                key,
                StateBackend::ValueView{last_delivered_id_.data(), last_delivered_id_.size()});
        }
    }

    // Commit-dispatch thread: only moves entries between the ledger's lists. The
    // XACK runs on the source thread.
    void notify_checkpoint_complete(CheckpointId ckpt) override { ledger_.complete(ckpt.value()); }
    void notify_checkpoint_aborted(CheckpointId ckpt) override { ledger_.abort(ckpt.value()); }

    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        bool found = false;
        const std::string_view prefix{kIdPrefix};
        backend.scan_operator_state(
            op_id, [&](StateBackend::KeyView key, StateBackend::ValueView /*value*/) {
                if (key.size() <= prefix.size() || key.substr(0, prefix.size()) != prefix) {
                    return;
                }
                found = true;
            });
        // The PEL (re-drained in open()) is the real recovery mechanism; the
        // persisted id is a checkpoint marker so this returns true iff a prior
        // checkpoint recorded progress for this stream.
        return found;
    }

    // Observability/test accessor: the id of the last entry this source emitted.
    [[nodiscard]] const std::string& last_delivered_id() const noexcept {
        return last_delivered_id_;
    }
    [[nodiscard]] const std::string& consumer_name() const noexcept { return consumer_; }
    // Entries delivered and not yet acknowledged, and entries acknowledged.
    [[nodiscard]] std::size_t unacknowledged() const { return ledger_.outstanding(); }
    [[nodiscard]] std::uint64_t acknowledged_total() const noexcept { return acked_total_; }

private:
    static constexpr const char* kIdPrefix = "__redis_last_id__:";
    static constexpr std::size_t kAckChunk = 1000;  // ids per XACK

    // XACK what completed checkpoints cover. A failure throws: the entries stay
    // pending (or were acknowledged, when the reply was lost), and either way the
    // restart's PEL re-read delivers again only what no completed checkpoint
    // acknowledged. Released entries (an aborted checkpoint's) need nothing: they
    // stay pending for the restart.
    void acknowledge_completed_() {
        auto handover = ledger_.take();
        const auto& ids = handover.ready;
        for (std::size_t off = 0; off < ids.size(); off += kAckChunk) {
            const std::size_t end = std::min(off + kAckChunk, ids.size());
            std::vector<std::string> args;
            args.reserve(end - off + 3);
            args.emplace_back("XACK");
            args.push_back(opts_.stream);
            args.push_back(opts_.group);
            args.insert(args.end(),
                        ids.begin() + static_cast<long>(off),
                        ids.begin() + static_cast<long>(end));
            Reply r;
            try {
                r = conn_->command(args);
            } catch (...) {
                clink::metrics::connector::error_inc("redis", "source");
                throw;
            }
            if (r.is_error()) {
                clink::metrics::connector::error_inc("redis", "source");
                throw std::runtime_error(opts_.name + ": XACK failed: " + r.error_text());
            }
            acked_total_ += end - off;
        }
    }

    // Refuse to start beside pending entries no subtask of this layout reads.
    void refuse_stranded_pending_() {
        Reply r = conn_->command({"XINFO", "CONSUMERS", opts_.stream, opts_.group});
        if (!r || r.is_error() || r->type != REDIS_REPLY_ARRAY) {
            return;
        }
        std::vector<GroupConsumer> consumers;
        for (std::size_t i = 0; i < r->elements; ++i) {
            const redisReply* c = r->element[i];
            if (c == nullptr || c->type != REDIS_REPLY_ARRAY) {  // RESP2: name, value, ...
                continue;
            }
            GroupConsumer gc;
            for (std::size_t k = 0; k + 1 < c->elements; k += 2) {
                const redisReply* key = c->element[k];
                const redisReply* val = c->element[k + 1];
                if (key == nullptr || val == nullptr || key->type != REDIS_REPLY_STRING) {
                    continue;
                }
                const std::string_view field{key->str, key->len};
                if (field == "name" && val->type == REDIS_REPLY_STRING) {
                    gc.name.assign(val->str, val->len);
                } else if (field == "pending" && val->type == REDIS_REPLY_INTEGER) {
                    gc.pending = val->integer;
                }
            }
            consumers.push_back(std::move(gc));
        }
        const auto stranded =
            stranded_consumers(opts_.consumer_prefix, opts_.parallelism, consumers);
        if (stranded.empty()) {
            return;
        }
        std::string held;
        std::uint32_t needed = opts_.parallelism;
        for (const auto& c : stranded) {
            held += (held.empty() ? "" : ", ") + c.name + " (" + std::to_string(c.pending) +
                    " pending)";
            needed = std::max<std::uint32_t>(needed,
                                             static_cast<std::uint32_t>(std::stoul(
                                                 c.name.substr(opts_.consumer_prefix.size() + 1))) +
                                                 1);
        }
        throw std::runtime_error(
            opts_.name + ": consumer group '" + opts_.group + "' on stream '" + opts_.stream +
            "' has entries pending on " + held +
            ", a subtask this job does not run at parallelism " +
            std::to_string(opts_.parallelism) +
            ". They were delivered to a subtask a lower parallelism removed and were never "
            "acknowledged, and nothing would read them again; the source does not claim another "
            "consumer's entries. Run at parallelism " +
            std::to_string(needed) +
            " or more, or move them to a consumer this job runs (XCLAIM) or drop them (XGROUP "
            "DELCONSUMER), then start the job again");
    }

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

    std::vector<std::string> xreadgroup_args_(const std::string& read_id, std::size_t count) const {
        return {"XREADGROUP",
                "GROUP",
                opts_.group,
                consumer_,
                "COUNT",
                std::to_string(count),
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
    clink::connectors::CheckpointAckLedger<std::string> ledger_;  // entry ids awaiting XACK
    std::uint64_t acked_total_{0};
    bool paused_{false};  // at max_pending, waiting for a checkpoint to complete
    std::string last_delivered_id_;
};

}  // namespace clink::redis
