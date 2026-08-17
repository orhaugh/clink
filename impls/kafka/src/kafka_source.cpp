#include "clink/connectors/kafka_source.hpp"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/runtime_context.hpp"

#ifdef CLINK_HAS_KAFKA
// Both APIs: the C++ wrapper for configuration, subscription and the rebalance
// callback, and the C API for the batched fetch in produce(). rdkafkacpp.h does
// not pull in rdkafka.h, and the C++ wrapper has no batch-consume call - the one
// that turns a per-record queue lock and wrapper allocation into one per batch.
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#endif

namespace clink {

std::string KafkaSource::stable_group_instance_id(std::string_view topic,
                                                  std::uint32_t subtask_idx) {
    // Kafka group.instance.id is scoped by group.id. Include the topic so
    // distinct topic subscriptions using that group do not fence each other
    // merely because both have subtask zero. FNV-1a keeps the identity stable
    // across processes and standard-library implementations.
    std::uint64_t hash = 1469598103934665603ULL;
    for (const auto ch : topic) {
        hash ^= static_cast<unsigned char>(ch);
        hash *= 1099511628211ULL;
    }
    return "clink-" + std::to_string(hash) + "-" + std::to_string(subtask_idx);
}

// Offset-map (partition -> next offset) serialization. Pure and
// broker-independent (defined in both the real and stub builds) so it can be
// unit-tested without a Kafka client: count(u32 LE) then repeated
// (partition i32 LE, offset i64 LE).
std::string KafkaSource::encode_offsets(const std::map<std::int32_t, std::int64_t>& offsets) {
    std::string out;
    out.reserve(4 + offsets.size() * 12);
    auto put_u32 = [&out](std::uint32_t v) {
        for (int i = 0; i < 4; ++i) {
            out.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
        }
    };
    auto put_i64 = [&out](std::int64_t v) {
        const auto u = static_cast<std::uint64_t>(v);
        for (int i = 0; i < 8; ++i) {
            out.push_back(static_cast<char>((u >> (i * 8)) & 0xFF));
        }
    };
    put_u32(static_cast<std::uint32_t>(offsets.size()));
    for (const auto& [partition, offset] : offsets) {
        put_u32(static_cast<std::uint32_t>(partition));
        put_i64(offset);
    }
    return out;
}

std::map<std::int32_t, std::int64_t> KafkaSource::decode_offsets(std::string_view bytes) {
    std::map<std::int32_t, std::int64_t> out;
    std::size_t pos = 0;
    auto get_u32 = [&](std::uint32_t& v) -> bool {
        if (pos + 4 > bytes.size()) {
            return false;
        }
        v = 0;
        for (int i = 0; i < 4; ++i) {
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[pos++])) << (i * 8);
        }
        return true;
    };
    auto get_i64 = [&](std::int64_t& v) -> bool {
        if (pos + 8 > bytes.size()) {
            return false;
        }
        std::uint64_t u = 0;
        for (int i = 0; i < 8; ++i) {
            u |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(bytes[pos++])) << (i * 8);
        }
        v = static_cast<std::int64_t>(u);
        return true;
    };
    std::uint32_t n = 0;
    if (!get_u32(n)) {
        return out;
    }
    for (std::uint32_t i = 0; i < n; ++i) {
        std::uint32_t partition = 0;
        std::int64_t offset = 0;
        if (!get_u32(partition) || !get_i64(offset)) {
            break;
        }
        out[static_cast<std::int32_t>(partition)] = offset;
    }
    return out;
}

#ifdef CLINK_HAS_KAFKA

namespace {

// Headers come back from librdkafka owned by the message; they are copied into
// KafkaHeader by value so the caller owns the lifetime. Reads them through the C
// API because produce() fetches in batches, and the batch call is C-only.
std::vector<KafkaHeader> copy_headers_c(const rd_kafka_message_t* msg) {
    std::vector<KafkaHeader> out;
    rd_kafka_headers_t* hdrs = nullptr;
    // Returns NO_ENT for the overwhelmingly common no-headers case, and this
    // stays allocation-free on that path.
    if (rd_kafka_message_headers(msg, &hdrs) != RD_KAFKA_RESP_ERR_NO_ERROR || hdrs == nullptr) {
        return out;
    }
    const std::size_t n = rd_kafka_header_cnt(hdrs);
    out.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        const char* name = nullptr;
        const void* value = nullptr;
        std::size_t size = 0;
        if (rd_kafka_header_get_all(hdrs, i, &name, &value, &size) != RD_KAFKA_RESP_ERR_NO_ERROR) {
            break;
        }
        out.push_back(KafkaHeader{.key = name != nullptr ? std::string{name} : std::string{},
                                  .value = value != nullptr
                                               ? std::string(static_cast<const char*>(value), size)
                                               : std::string{}});
    }
    return out;
}

// Record "partition -> next offset" in a short scratch vector, overwriting any
// earlier entry for the partition. Linear scan on purpose: a subtask owns a
// handful of partitions, so this is one or two hot cache lines, where the
// std::map it replaces cost a tree descent on every record for a value only the
// checkpoint reads.
void note_next_offset(std::vector<std::pair<std::int32_t, std::int64_t>>& scratch,
                      std::int32_t partition,
                      std::int64_t next) {
    for (auto& [p, off] : scratch) {
        if (p == partition) {
            off = next;
            return;
        }
    }
    scratch.emplace_back(partition, next);
}

// Legacy whole-map operator-state key (pre per-partition rows). Still read
// on restore as a fallback so checkpoints from #52 / Gap A keep loading.
constexpr const char* kOffsetKey = "__kafka_source_offsets__";
// Per-partition operator-state key prefix (#54 Gap B): one row per
// partition (key = prefix + decimal partition, value = i64 LE next-offset).
// Distinct keys mean a multi-parent rescale merge unions partitions instead
// of colliding on a single whole-map key.
constexpr const char* kOffsetPartPrefix = "__kafka_off__:";

// Rebalance callback that makes the clink checkpoint the source of truth on
// restore. On partition assignment it seeks each partition to the restored
// offset (if any), then assigns; restored entries are applied at most once
// (erased as consumed) so a later mid-run rebalance never rewinds. With no
// restored offsets it behaves exactly like the default (assign / unassign),
// so first-run behaviour and auto_offset_reset are unchanged.
class SeekRebalanceCb final : public RdKafka::RebalanceCb {
public:
    SeekRebalanceCb(std::map<std::int32_t, std::int64_t>* restored,
                    std::map<std::int32_t, std::int64_t>* next_offsets,
                    bool* assigned_seen)
        : restored_(restored), next_offsets_(next_offsets), assigned_seen_(assigned_seen) {}

    void rebalance_cb(RdKafka::KafkaConsumer* consumer,
                      RdKafka::ErrorCode err,
                      std::vector<RdKafka::TopicPartition*>& partitions) override {
        if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
            for (auto* tp : partitions) {
                if (restored_ == nullptr) {
                    break;
                }
                auto it = restored_->find(tp->partition());
                if (it != restored_->end()) {
                    tp->set_offset(it->second);
                    // Seed next_offsets for this OWNED partition so a snapshot
                    // re-persists its restored position and the snapshot prune
                    // keeps it (rather than treating it as non-owned).
                    if (next_offsets_ != nullptr) {
                        (*next_offsets_)[tp->partition()] = it->second;
                    }
                    restored_->erase(it);  // apply once; never rewind later
                }
            }
            // Assignment has happened: the source now knows which partitions
            // it owns, so snapshot may prune non-owned operator-state rows.
            if (assigned_seen_ != nullptr) {
                *assigned_seen_ = true;
            }
            consumer->assign(partitions);
        } else {
            // Revoked: stop persisting partitions this subtask no longer owns
            // so they don't linger as stale offset rows in future checkpoints.
            if (next_offsets_ != nullptr && err == RdKafka::ERR__REVOKE_PARTITIONS) {
                for (auto* tp : partitions) {
                    next_offsets_->erase(tp->partition());
                }
            }
            consumer->unassign();
        }
    }

private:
    std::map<std::int32_t, std::int64_t>* restored_;
    std::map<std::int32_t, std::int64_t>* next_offsets_;
    bool* assigned_seen_;
};

}  // namespace

struct KafkaSource::Impl {
    Options opts;
    std::unique_ptr<RdKafka::KafkaConsumer> consumer;
    std::atomic<bool> cancelled{false};
    Counter* consumed{nullptr};
    Counter* consume_errors{nullptr};
    // The consumer's own queue - the one rd_kafka_consumer_poll() serves - held
    // so produce() can fetch a WHOLE BATCH per call via
    // rd_kafka_consume_batch_queue() instead of one message per call. The C++
    // consume() wrapper takes the queue lock, dequeues one op and heap-allocates
    // a MessageImpl every record; on the cloud rig the Kafka source was the
    // largest single cost in every query measured (27-38% of all worker CPU,
    // ~0.9us per record, against 0.05us for the projection the query actually
    // asked for). One batched fetch amortises the lock and the wrapper away.
    //
    // MUST be destroyed before consumer->close() (librdkafka requires the
    // reference released prior to close), which close() and the destructor do.
    rd_kafka_queue_t* cqueue{nullptr};
    // Reused across produce() calls: sized once to max_batch_size so the fetch
    // never allocates.
    std::vector<rd_kafka_message_t*> fetch_buf;
    // Per-batch offset scratch. A subtask owns a handful of partitions, so a
    // linear scan of a hot vector beats the red-black tree lookup that
    // next_offsets[partition] did on EVERY record.
    std::vector<std::pair<std::int32_t, std::int64_t>> offset_scratch;
    // partition -> next offset to read; advanced as records are emitted,
    // persisted at each checkpoint.
    std::map<std::int32_t, std::int64_t> next_offsets;
    // partition -> offset to seek to on assignment after a restore; drained
    // by the rebalance callback as it applies each.
    std::map<std::int32_t, std::int64_t> restored_offsets;
    // Set true once the rebalance callback has seen an assignment, so the
    // source knows which partitions it owns and snapshot may prune the rest.
    bool assigned_seen{false};
    std::unique_ptr<SeekRebalanceCb> rebalance_cb;
};

bool KafkaSource::is_real_implementation() {
    return true;
}

KafkaSource::KafkaSource(Options opts) : impl_(std::make_unique<Impl>()) {
    if (opts.brokers.empty() || opts.topic.empty()) {
        throw std::invalid_argument("KafkaSource: Options.brokers and Options.topic are required");
    }
    impl_->opts = std::move(opts);
}

KafkaSource::~KafkaSource() {
    if (impl_ && impl_->consumer) {
        // The queue reference MUST go before close(): librdkafka documents that
        // rd_kafka_queue_destroy() has to be called on the consumer queue prior
        // to rd_kafka_consumer_close(). Nulling it keeps this idempotent, since
        // close() may already have run.
        if (impl_->cqueue != nullptr) {
            rd_kafka_queue_destroy(impl_->cqueue);
            impl_->cqueue = nullptr;
        }
        impl_->consumer->close();
    }
}

void KafkaSource::open() {
    std::string err;
    auto cfg = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

    auto set_or_throw = [&](const std::string& k, const std::string& v) {
        if (cfg->set(k, v, err) != RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("KafkaSource: config '" + k + "': " + err);
        }
    };

    set_or_throw("bootstrap.servers", impl_->opts.brokers);
    set_or_throw("group.id", impl_->opts.group_id);
    set_or_throw("client.id", impl_->opts.client_id);
    set_or_throw("auto.offset.reset", impl_->opts.auto_offset_reset);
    set_or_throw("enable.partition.eof", "false");
    set_or_throw("enable.auto.commit",
                 impl_->opts.commit_mode == CommitMode::Auto ? "true" : "false");
    if (impl_->opts.enable_debug) {
        set_or_throw("debug", "consumer,cgrp,topic,fetch");
    }
    // Extra librdkafka properties (security.protocol, sasl.*, ssl.*, ...) applied
    // verbatim. librdkafka validates each key/value here and throws on a bad one.
    for (const auto& [k, v] : impl_->opts.conf) {
        set_or_throw(k, v);
    }

    // Seek-on-assignment rebalance callback: applies any restored offsets so
    // the consumer resumes from the clink checkpoint rather than Kafka's
    // committed offset. The callback points at restored_offsets (a stable
    // Impl member, populated by restore_offset before open()); with none set
    // it assigns/unassigns exactly like the default.
    impl_->rebalance_cb = std::make_unique<SeekRebalanceCb>(
        &impl_->restored_offsets, &impl_->next_offsets, &impl_->assigned_seen);
    if (cfg->set("rebalance_cb", impl_->rebalance_cb.get(), err) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("KafkaSource: config 'rebalance_cb': " + err);
    }

    auto* consumer = RdKafka::KafkaConsumer::create(cfg.get(), err);
    if (consumer == nullptr) {
        throw std::runtime_error("KafkaSource: create consumer failed: " + err);
    }
    impl_->consumer.reset(consumer);

    auto rc = impl_->consumer->subscribe({impl_->opts.topic});
    if (rc != RdKafka::ERR_NO_ERROR) {
        throw std::runtime_error("KafkaSource: subscribe failed: " + RdKafka::err2str(rc));
    }

    // Hold the consumer queue for the batched fetch path. Polling this queue
    // counts as a consumer poll (it is the queue rd_kafka_consumer_poll serves),
    // so max.poll.interval.ms keeps being reset and group membership is
    // maintained exactly as before; rebalance and error ops travel on the same
    // queue, so the seek-on-assignment callback still fires. The suite's
    // SourceReplayResumesFromCheckpoint case is the canary for that: without a
    // firing rebalance callback the restored source resumes at payload-0 rather
    // than payload-6 and the test fails.
    impl_->cqueue = rd_kafka_queue_get_consumer(impl_->consumer->c_ptr());
    if (impl_->cqueue == nullptr) {
        throw std::runtime_error("KafkaSource: rd_kafka_queue_get_consumer returned null");
    }
    impl_->fetch_buf.resize(std::max<std::size_t>(1, impl_->opts.max_batch_size));
    impl_->offset_scratch.reserve(8);

    if (auto* ctx = this->runtime();
        ctx != nullptr && ctx->metrics() != nullptr && !impl_->opts.metric_prefix.empty()) {
        const std::string prefix = "kafka_source." + impl_->opts.metric_prefix + ".";
        impl_->consumed = &ctx->metrics()->counter(prefix + "consumed");
        impl_->consume_errors = &ctx->metrics()->counter(prefix + "consume_errors");
    }
}

bool KafkaSource::produce(Emitter<KafkaMessage>& out) {
    if (this->cancelled() || impl_->cancelled.load(std::memory_order_acquire)) {
        return false;
    }
    Batch<KafkaMessage> batch;
    // The batch never exceeds max_batch_size, and its size is known here, so one
    // allocation instead of the ~8 reallocations a geometric grow costs per batch.
    batch.reserve(impl_->opts.max_batch_size);
    std::uint64_t bytes_read = 0;
    // batch_max_wait bounds TOTAL fill time: the first record may block up
    // to poll_timeout (idle stays cheap), but once the batch is non-empty
    // the remaining fill window shrinks to the deadline, so a paced or
    // trickling input gets a prompt partial batch instead of waiting for
    // max_batch_size records to accumulate.
    const auto wait_bound = impl_->opts.batch_max_wait;
    const auto fill_deadline = std::chrono::steady_clock::now() + wait_bound;
    // Fill in BATCHED FETCHES rather than one message per call. Each
    // rd_kafka_consume_batch_queue() takes the queue lock once and returns up to
    // the remaining batch capacity, where the per-message path took the lock,
    // dequeued one op and heap-allocated a wrapper object for every single
    // record. The loop repeats only while the batch is still short and the fill
    // deadline has not passed, so a full batch is normally ONE fetch call.
    std::size_t filled = 0;
    std::uint64_t consumed_ok = 0;
    std::uint64_t error_count = 0;
    impl_->offset_scratch.clear();
    while (filled < impl_->opts.max_batch_size) {
        if (impl_->cancelled.load(std::memory_order_acquire)) {
            break;
        }
        int timeout_ms = static_cast<int>(impl_->opts.poll_timeout.count());
        if (filled > 0 && wait_bound > std::chrono::milliseconds::zero()) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= fill_deadline) {
                break;
            }
            const auto remaining =
                std::chrono::duration_cast<std::chrono::milliseconds>(fill_deadline - now);
            timeout_ms = std::min(timeout_ms, static_cast<int>(remaining.count()) + 1);
        }
        const ssize_t got = rd_kafka_consume_batch_queue(impl_->cqueue,
                                                         timeout_ms,
                                                         impl_->fetch_buf.data() + filled,
                                                         impl_->opts.max_batch_size - filled);
        if (got <= 0) {
            // 0 = the timeout elapsed with nothing available; <0 = a queue-level
            // error, which the per-message path reached as ERR__TIMED_OUT too.
            // Either way stop filling and emit whatever is already in hand.
            break;
        }
        for (ssize_t k = 0; k < got; ++k) {
            rd_kafka_message_t* msg = impl_->fetch_buf[filled + static_cast<std::size_t>(k)];
            if (msg->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                KafkaMessage m;
                if (msg->payload != nullptr) {
                    m.payload.assign(static_cast<const char*>(msg->payload), msg->len);
                    bytes_read += msg->len;
                }
                if (msg->key != nullptr) {
                    m.key = std::string(static_cast<const char*>(msg->key), msg->key_len);
                }
                m.headers = copy_headers_c(msg);
                m.offset = msg->offset;
                m.partition = msg->partition;
                // librdkafka tags broker timestamps CreateTime / LogAppendTime /
                // NotAvailable; the not-available case surfaces as -1, which is
                // the convention the rest of the engine already reads.
                rd_kafka_timestamp_type_t ts_type = RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
                const int64_t ts = rd_kafka_message_timestamp(msg, &ts_type);
                m.timestamp_ms = ts_type == RD_KAFKA_TIMESTAMP_NOT_AVAILABLE ? -1 : ts;

                // Resume point for this partition is the offset AFTER the one
                // just emitted. Held in a short scratch vector and merged into
                // next_offsets once per batch: the map form did a red-black tree
                // lookup per record for a value only the checkpoint ever reads.
                note_next_offset(impl_->offset_scratch, msg->partition, msg->offset + 1);

                batch.emplace(std::move(m));
                ++consumed_ok;
            } else {
                // Real error - record it and continue. A single broken record
                // must not tear down the whole pipeline.
                ++error_count;
                clink::metrics::connector::error_inc("kafka", "source");
            }
            rd_kafka_message_destroy(msg);
        }
        filled += static_cast<std::size_t>(got);
    }
    // Counters once per batch, not once per record: these are atomics, and the
    // batch total is the same number.
    if (impl_->consumed != nullptr && consumed_ok > 0) {
        impl_->consumed->increment(consumed_ok);
    }
    if (impl_->consume_errors != nullptr && error_count > 0) {
        impl_->consume_errors->increment(error_count);
    }
    for (const auto& [partition, next] : impl_->offset_scratch) {
        impl_->next_offsets[partition] = next;
    }
    if (!batch.empty()) {
        clink::metrics::connector::records_in_inc("kafka", batch.size());
        clink::metrics::connector::bytes_in_inc("kafka", bytes_read);
        out.emit_data(std::move(batch));
    }
    // Unbounded source - only stops on cancel().
    return !impl_->cancelled.load(std::memory_order_acquire) && !this->cancelled();
}

void KafkaSource::cancel() {
    impl_->cancelled.store(true, std::memory_order_release);
    Source<KafkaMessage>::cancel();
}

void KafkaSource::close() {
    if (impl_ && impl_->consumer) {
        // Queue reference first - librdkafka requires it released before
        // rd_kafka_consumer_close().
        if (impl_->cqueue != nullptr) {
            rd_kafka_queue_destroy(impl_->cqueue);
            impl_->cqueue = nullptr;
        }
        impl_->consumer->close();
        impl_->consumer.reset();
    }
}

bool KafkaSource::commit_current() {
    if (impl_->opts.commit_mode != CommitMode::Manual) {
        return false;
    }
    if (!impl_->consumer) {
        return false;
    }
    const auto rc = impl_->consumer->commitSync();
    return rc == RdKafka::ERR_NO_ERROR;
}

void KafkaSource::snapshot_offset(StateBackend& backend,
                                  OperatorId op_id,
                                  CheckpointId /*ckpt_id*/) {
    // Runs on the source-runner thread between produce() calls, so
    // next_offsets is stable. Persist ONE operator-state row per partition
    // (key = prefix + partition, value = i64 LE next-offset). The
    // operator-state path exempts these from the rescale key-group filter,
    // and the distinct per-partition keys let a multi-parent rescale merge
    // UNION the partitions rather than collide on a single whole-map key.
    // The apply-once rebalance callback then keeps only the partitions the
    // broker assigns this subtask.
    for (const auto& [partition, offset] : impl_->next_offsets) {
        std::array<std::byte, 8> bytes{};
        const auto u = static_cast<std::uint64_t>(offset);
        for (int i = 0; i < 8; ++i) {
            bytes[static_cast<std::size_t>(i)] = static_cast<std::byte>((u >> (i * 8)) & 0xFF);
        }
        const std::string key = std::string{kOffsetPartPrefix} + std::to_string(partition);
        backend.put_operator_state(
            op_id,
            key,
            StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
    }

    // Once assignment is known, this subtask owns exactly next_offsets's
    // partitions, so drop any per-partition row it does NOT own - the
    // restored union it inherited on a rescale, or a partition revoked
    // mid-run. This converges each subtask's checkpoint to owned-only
    // (no bloat, no stale rows). Before assignment we keep everything so a
    // checkpoint in the restore->assignment window loses no offset; max-wins
    // on restore is the order-independent backstop for any row that lingers.
    if (impl_->assigned_seen) {
        const std::string_view part_prefix{kOffsetPartPrefix};
        std::vector<std::string> stale;
        backend.scan_operator_state(op_id, [&](StateBackend::KeyView key, StateBackend::ValueView) {
            if (key.size() <= part_prefix.size() ||
                key.substr(0, part_prefix.size()) != part_prefix) {
                return;
            }
            std::int32_t partition = 0;
            try {
                partition = static_cast<std::int32_t>(
                    std::stol(std::string{key.substr(part_prefix.size())}));
            } catch (const std::exception&) {
                return;
            }
            if (impl_->next_offsets.find(partition) == impl_->next_offsets.end()) {
                stale.emplace_back(key);
            }
        });
        for (const auto& key : stale) {
            backend.erase_operator_state(op_id, key);
        }
    }
}

bool KafkaSource::restore_offset(StateBackend& backend, OperatorId op_id) {
    std::map<std::int32_t, std::int64_t> restored;
    const std::string_view part_prefix{kOffsetPartPrefix};
    backend.scan_operator_state(
        op_id, [&](StateBackend::KeyView key, StateBackend::ValueView value) {
            if (key.size() <= part_prefix.size() ||
                key.substr(0, part_prefix.size()) != part_prefix || value.size() < 8) {
                return;
            }
            std::int32_t partition = 0;
            const auto suffix = std::string{key.substr(part_prefix.size())};
            try {
                partition = static_cast<std::int32_t>(std::stol(suffix));
            } catch (const std::exception&) {
                return;  // malformed partition suffix; skip
            }
            if (partition < 0) {
                return;  // partitions are non-negative; a negative id is bogus
            }
            std::uint64_t u = 0;
            for (int i = 0; i < 8; ++i) {
                u |= static_cast<std::uint64_t>(
                         static_cast<std::uint8_t>(value[static_cast<std::size_t>(i)]))
                     << (i * 8);
            }
            restored[partition] = static_cast<std::int64_t>(u);
        });

    // Fallback for checkpoints written before per-partition rows (the #52 /
    // Gap A whole-map format). get_operator_state also handles the legacy
    // raw (unprefixed) key.
    if (restored.empty()) {
        auto v = backend.get_operator_state(
            op_id, StateBackend::KeyView{kOffsetKey, std::strlen(kOffsetKey)});
        if (v.has_value()) {
            restored = decode_offsets(
                std::string_view{reinterpret_cast<const char*>(v->data()), v->size()});
        }
    }
    if (restored.empty()) {
        return false;
    }
    impl_->restored_offsets = std::move(restored);
    // Do NOT seed next_offsets with the full restored union: that would make
    // this subtask re-persist every partition (bloat) and freeze offsets for
    // partitions it does not own. The rebalance callback seeds next_offsets
    // for the partitions it is actually assigned (so an immediate post-
    // assignment checkpoint still re-persists them); until assignment, the
    // backend's restored rows are preserved (snapshot does not prune yet), so
    // no offset is lost in the restore->assignment window.
    return true;
}

#else

struct KafkaSource::Impl {};

bool KafkaSource::is_real_implementation() {
    return false;
}

KafkaSource::KafkaSource(Options /*opts*/) {
    throw std::runtime_error(
        "KafkaSource: built without librdkafka. Install it (e.g. "
        "`brew install librdkafka`) and reconfigure cmake.");
}

KafkaSource::~KafkaSource() = default;
void KafkaSource::open() {}
bool KafkaSource::produce(Emitter<KafkaMessage>& /*out*/) {
    return false;
}
void KafkaSource::cancel() {}
void KafkaSource::close() {}
bool KafkaSource::commit_current() {
    return false;
}
void KafkaSource::snapshot_offset(StateBackend& /*backend*/,
                                  OperatorId /*op_id*/,
                                  CheckpointId /*ckpt_id*/) {}
bool KafkaSource::restore_offset(StateBackend& /*backend*/, OperatorId /*op_id*/) {
    return false;
}

#endif

}  // namespace clink
