#include "clink/connectors/kafka_source.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/logging.hpp"
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

bool KafkaSource::owns_partition(std::int32_t partition,
                                 std::uint32_t source_parallelism,
                                 std::uint32_t subtask_index) noexcept {
    // Deterministic, restart-stable, rescale-stable: partition p belongs to
    // subtask p % parallelism. Ownership decided inside the engine is what
    // lets the checkpointed offset rows and the consumed positions describe
    // one cut; anything decided by a consumer-group coordinator is not
    // stable across restores and cannot be.
    if (partition < 0 || source_parallelism == 0) {
        return false;
    }
    return static_cast<std::uint32_t>(partition) % source_parallelism == subtask_index;
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

// The partition count of one topic, or 0 when the topic is not (yet) known
// to the cluster. Manual-assignment sources derive their owned set from
// this rather than from a consumer-group assignment.
std::int32_t partition_count_for(RdKafka::KafkaConsumer& consumer,
                                 const std::string& topic,
                                 std::chrono::milliseconds timeout) {
    std::string err;
    const std::unique_ptr<RdKafka::Topic> handle(
        RdKafka::Topic::create(&consumer, topic, nullptr, err));
    if (handle == nullptr) {
        throw std::runtime_error("KafkaSource: topic handle for metadata failed: " + err);
    }
    RdKafka::Metadata* md_raw = nullptr;
    const auto rc =
        consumer.metadata(false, handle.get(), &md_raw, static_cast<int>(timeout.count()));
    if (rc != RdKafka::ERR_NO_ERROR) {
        return 0;  // transient (broker starting, metadata propagating): caller retries
    }
    const std::unique_ptr<RdKafka::Metadata> md(md_raw);
    for (const auto* tmd : *md->topics()) {
        if (tmd->topic() == topic && tmd->err() == RdKafka::ERR_NO_ERROR) {
            return static_cast<std::int32_t>(tmd->partitions()->size());
        }
    }
    return 0;
}

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
    // persisted at each checkpoint. Seeded at restore with the OWNED
    // partitions' restored positions, so a checkpoint taken before the
    // first record still re-persists them.
    std::map<std::int32_t, std::int64_t> next_offsets;
    // partition -> start offset for open()'s manual assignment, already
    // narrowed to owned partitions by restore_offset. Drained as applied.
    std::map<std::int32_t, std::int64_t> restored_offsets;
    // The owned partitions currently assigned; grows when discovery sees a
    // repartitioned topic.
    std::vector<std::int32_t> owned_partitions;
    std::chrono::steady_clock::time_point last_discovery{};
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

    auto* consumer = RdKafka::KafkaConsumer::create(cfg.get(), err);
    if (consumer == nullptr) {
        throw std::runtime_error("KafkaSource: create consumer failed: " + err);
    }
    impl_->consumer.reset(consumer);

    // Manual, deterministic partition assignment: this subtask owns exactly
    // the partitions p with p % source_parallelism == subtask_index, and
    // consumes them via assign() - no consumer-group rebalancing, no group
    // join, no membership races. Group-decided ownership is external state
    // and cannot be held to one checkpoint cut: after a restore, a partition
    // handed to a different subtask either has no restored offset there or a
    // stale one, and both silently break exactly-once (QUAL-01 run C
    // measured the stale form as 107,884 inflated window aggregates while
    // every subtask held the restored union of all offset rows). Ownership
    // decided here, from the same subtask index the offset rows are scoped
    // to, keeps source position and downstream state on one cut through any
    // sequence of restores.
    //
    // The topic may not be visible yet (broker start, metadata propagation),
    // so poll metadata briefly rather than failing the first attempt.
    std::int32_t partitions = 0;
    const auto metadata_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{30};
    while ((partitions = partition_count_for(
                *impl_->consumer, impl_->opts.topic, std::chrono::milliseconds{2'000})) == 0 &&
           std::chrono::steady_clock::now() < metadata_deadline &&
           !impl_->cancelled.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds{250});
    }
    if (partitions == 0) {
        impl_->consumer->close();
        impl_->consumer.reset();
        throw std::runtime_error("KafkaSource: topic '" + impl_->opts.topic +
                                 "' has no visible partitions after 30s; cannot assign");
    }
    assign_owned_(partitions);
    impl_->last_discovery = std::chrono::steady_clock::now();

    // Hold the consumer queue for the batched fetch path. Polling this queue
    // counts as a consumer poll (it is the queue rd_kafka_consumer_poll
    // serves), and with manual assignment it carries fetch data and error ops
    // only - there is no group membership to maintain and no rebalance events
    // to dispatch. The suite's SourceReplaysFromSnapshottedOffset case is the
    // canary for restore: a restored source must resume at payload-6 rather
    // than payload-0.
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

void KafkaSource::assign_owned_(std::int32_t partition_count) {
    const auto& opts = impl_->opts;
    std::vector<std::int32_t> owned;
    for (std::int32_t p = 0; p < partition_count; ++p) {
        if (owns_partition(p, opts.source_parallelism, opts.subtask_index)) {
            owned.push_back(p);
        }
    }
    // The owned set only ever GROWS. Kafka partitions cannot be removed, so
    // a metadata response listing fewer than currently owned is a degraded
    // snapshot (a broker mid-restart), not a repartition; acting on it would
    // drop in-flight partitions and their tracked offsets, and the eventual
    // re-add would fall back to auto.offset.reset - a silent rewind to the
    // start of the partition under exactly the broker chaos a qualification
    // campaign applies.
    for (const auto p : impl_->owned_partitions) {
        if (std::find(owned.begin(), owned.end(), p) == owned.end()) {
            owned.push_back(p);
        }
    }
    std::sort(owned.begin(), owned.end());
    if (owned == impl_->owned_partitions) {
        return;  // discovery found nothing new for this subtask
    }

    // Current fetch positions of the partitions already assigned, so a
    // repartition-driven reassign never moves an in-flight partition.
    std::map<std::int32_t, std::int64_t> current;
    if (!impl_->owned_partitions.empty()) {
        std::vector<RdKafka::TopicPartition*> held;
        held.reserve(impl_->owned_partitions.size());
        for (const auto p : impl_->owned_partitions) {
            held.push_back(RdKafka::TopicPartition::create(opts.topic, p));
        }
        if (impl_->consumer->position(held) == RdKafka::ERR_NO_ERROR) {
            for (const auto* tp : held) {
                if (tp->offset() >= 0) {
                    current[tp->partition()] = tp->offset();
                }
            }
        }
        RdKafka::TopicPartition::destroy(held);
    }

    std::vector<RdKafka::TopicPartition*> assignment;
    assignment.reserve(owned.size());
    std::string described;
    for (const auto p : owned) {
        std::int64_t offset = 0;
        std::string origin;
        if (const auto held = current.find(p); held != current.end()) {
            offset = held->second;
            origin = "held";
        } else if (const auto next = impl_->next_offsets.find(p);
                   next != impl_->next_offsets.end()) {
            // Consumed (or restored) position tracked but not yet queryable
            // from the consumer - the authoritative resume point.
            offset = next->second;
            origin = "tracked";
        } else if (const auto restored = impl_->restored_offsets.find(p);
                   restored != impl_->restored_offsets.end()) {
            offset = restored->second;
            origin = "restored";
        } else {
            // No clink-side position: resolve the group's committed offset,
            // falling back to auto.offset.reset - the same semantics
            // subscription-based consumption had for a partition without
            // engine state.
            offset = RdKafka::Topic::OFFSET_STORED;
            origin = "stored/" + opts.auto_offset_reset;
        }
        impl_->restored_offsets.erase(p);  // applied; a reassign resumes, never rewinds
        assignment.push_back(RdKafka::TopicPartition::create(opts.topic, p, offset));
        described += (described.empty() ? "" : ",") + std::to_string(p) + ":" + origin +
                     (offset >= 0 ? "@" + std::to_string(offset) : "");
    }

    const auto rc = impl_->consumer->assign(assignment);
    RdKafka::TopicPartition::destroy(assignment);
    if (rc != RdKafka::ERR_NO_ERROR) {
        throw std::runtime_error("KafkaSource: assign failed: " + RdKafka::err2str(rc));
    }
    impl_->owned_partitions = std::move(owned);
    // Every owned partition gets a CONCRETE numeric position in
    // next_offsets from the moment it is assigned, so the very first
    // checkpoint already carries a resume row for it. A partition whose
    // row is absent at restore falls back to group offsets or the reset
    // policy - external state the engine's cut knows nothing about - and
    // the window between assignment and the first consumed record is
    // exactly where a fresh job's early checkpoints used to have no rows.
    // Logical starts resolve here: BEGINNING/END via the broker's
    // watermarks, STORED via the group's committed offset when one exists
    // (falling back to the reset policy's watermark). Best-effort per
    // partition: an unresolved one just stays unseeded until its first
    // record, which is no worse than before.
    for (const auto p : impl_->owned_partitions) {
        if (impl_->next_offsets.find(p) != impl_->next_offsets.end()) {
            continue;
        }
        std::int64_t low = 0;
        std::int64_t high = 0;
        if (impl_->consumer->query_watermark_offsets(opts.topic, p, &low, &high, 2'000) !=
            RdKafka::ERR_NO_ERROR) {
            continue;
        }
        std::int64_t start = opts.auto_offset_reset == "latest" ? high : low;
        std::vector<RdKafka::TopicPartition*> committed;
        committed.push_back(RdKafka::TopicPartition::create(opts.topic, p));
        if (impl_->consumer->committed(committed, 2'000) == RdKafka::ERR_NO_ERROR &&
            committed[0]->offset() >= 0) {
            start = committed[0]->offset();
        }
        RdKafka::TopicPartition::destroy(committed);
        impl_->next_offsets[p] = start;
    }
    // Drop tracked positions for partitions outside the owned set - restored
    // rows naming partitions the topic does not (or no longer) has. Left in
    // place they would re-persist forever as garbage offset rows.
    for (auto it = impl_->next_offsets.begin(); it != impl_->next_offsets.end();) {
        const bool still_owned =
            std::find(impl_->owned_partitions.begin(), impl_->owned_partitions.end(), it->first) !=
            impl_->owned_partitions.end();
        it = still_owned ? std::next(it) : impl_->next_offsets.erase(it);
    }
    // The forensic record QUAL-01 lacked: which subtask owns which partitions
    // from which offsets. One line per (re)assignment, not per record.
    clink::log::info("kafka.source",
                     "topic '" + opts.topic + "' subtask " + std::to_string(opts.subtask_index) +
                         "/" + std::to_string(opts.source_parallelism) + " assigned " +
                         std::to_string(impl_->owned_partitions.size()) + " of " +
                         std::to_string(partition_count) + " partitions [" + described + "]");
}

bool KafkaSource::produce(Emitter<KafkaMessage>& out) {
    if (this->cancelled() || impl_->cancelled.load(std::memory_order_acquire)) {
        return false;
    }
    // Partition discovery: a repartitioned topic grows new partitions whose
    // owner (by the deterministic rule) is this subtask. Metadata is polled
    // on an interval rather than learned from rebalance callbacks, because
    // there is no group protocol here. Failures are transient by nature and
    // retried on the next tick.
    if (impl_->opts.partition_discovery_interval > std::chrono::milliseconds::zero()) {
        const auto now = std::chrono::steady_clock::now();
        if (now - impl_->last_discovery >= impl_->opts.partition_discovery_interval) {
            impl_->last_discovery = now;
            try {
                const auto partitions = partition_count_for(
                    *impl_->consumer, impl_->opts.topic, std::chrono::milliseconds{1'000});
                if (partitions > 0) {
                    assign_owned_(partitions);
                }
            } catch (const std::exception& e) {
                clink::log::warn("kafka.source",
                                 "partition discovery for topic '" + impl_->opts.topic +
                                     "' failed (" + e.what() + "); retrying on the next tick");
            }
        }
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
    // restore_offset then narrows the union to the partitions the
    // deterministic ownership rule gives the restoring subtask.
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

    // This subtask owns exactly the partitions the deterministic rule gives
    // it, so drop any per-partition row it does NOT track - the restored
    // union it inherited (which a rescale legitimately hands it), or a row
    // for a partition that no longer exists. Ownership is static, so this
    // needs no assignment to have happened: restore_offset seeds
    // next_offsets with the owned partitions' restored positions, meaning a
    // checkpoint taken in the restore->consumption window persists owned
    // rows and nothing else. QUAL-01 run C's checkpoint 246 kept all four
    // subtasks' rows in every subtask, which is how stale foreign offsets
    // survived to be seeked after the partitions moved.
    {
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
    // Narrow the union to OWNED partitions here, before any consumption. A
    // restore hands every subtask all subtasks' rows (that is what a rescale
    // needs); a foreign partition's restored offset is stale the moment its
    // real owner consumes past it, so holding it as a seek target is the
    // QUAL-01 run C corruption waiting to fire. Ownership is deterministic,
    // so the narrowing needs no broker and no assignment.
    for (auto it = restored.begin(); it != restored.end();) {
        if (owns_partition(it->first, impl_->opts.source_parallelism, impl_->opts.subtask_index)) {
            ++it;
        } else {
            it = restored.erase(it);
        }
    }
    if (restored.empty()) {
        return false;  // nothing this subtask owns; fresh partitions use auto_offset_reset
    }
    // Seed next_offsets with the owned restored positions so a checkpoint
    // taken before the first record re-persists them and the snapshot prune
    // (owned-only) keeps them.
    for (const auto& [partition, offset] : restored) {
        impl_->next_offsets[partition] = offset;
    }
    impl_->restored_offsets = std::move(restored);
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
