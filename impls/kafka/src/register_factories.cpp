// Kafka factory registrations.
//
// Contains:
//   * StringKafkaSource / StringKafkaSink - adapters that turn the
//     KafkaMessage-typed connectors into Source<std::string> /
//     Sink<std::string> so they're addressable on the built-in
//     "string" channel.
//   * clink::kafka::install() - registers kafka_text_source and
//     kafka_text_sink with the supplied PluginRegistry. Callers
//     invoke explicitly after ensure_built_ins_registered() to make
//     the Kafka text factories reachable through that registry.
//
// Plugins that need the full KafkaMessage (key/headers/partition) can
// register their own KafkaMessage-typed sources/sinks via the same API.

#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/connectors/kafka_message.hpp"
#include "clink/connectors/kafka_sink.hpp"
#include "clink/connectors/kafka_source.hpp"
#include "clink/core/record.hpp"
#include "clink/kafka/install.hpp"
#include "clink/kafka/kafka_message_codec.hpp"
#include "clink/kafka/kafka_security.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::kafka {

namespace {

// Optional millisecond param -> a chrono field on an Options struct
// (e.g. 'linger_ms' -> sink linger, 'batch_max_wait_ms' -> source batch
// formation bound). Absent keeps the Options default; garbage throws.
void apply_ms_param(const plugin::BuildContext& ctx,
                    const char* key,
                    std::chrono::milliseconds& target) {
    const auto v = ctx.param_or(key, "");
    if (v.empty()) {
        return;
    }
    try {
        const auto ms = std::stoll(v);
        if (ms < 0) {
            throw std::invalid_argument("negative");
        }
        target = std::chrono::milliseconds{ms};
    } catch (const std::exception&) {
        throw std::runtime_error(std::string{"kafka: invalid '"} + key + "' value '" + v +
                                 "' (want a non-negative integer of milliseconds)");
    }
}

void apply_linger_ms(const plugin::BuildContext& ctx, KafkaSink::Options& opts) {
    apply_ms_param(ctx, "linger_ms", opts.linger_ms);
}

void apply_batch_max_wait(const plugin::BuildContext& ctx, KafkaSource::Options& opts) {
    apply_ms_param(ctx, "batch_max_wait_ms", opts.batch_max_wait);
}

// The rest of the source's batch-formation surface, so a latency-tuned
// table can shrink batches end to end (WITH max_batch_size='32',
// batch_max_wait_ms='1') without touching code. Absent keeps defaults.
void apply_batch_shape(const plugin::BuildContext& ctx, KafkaSource::Options& opts) {
    apply_ms_param(ctx, "poll_timeout_ms", opts.poll_timeout);
    const auto v = ctx.param_or("max_batch_size", "");
    if (v.empty()) {
        return;
    }
    try {
        const auto n = std::stoll(v);
        if (n <= 0) {
            throw std::invalid_argument("non-positive");
        }
        opts.max_batch_size = static_cast<std::size_t>(n);
    } catch (const std::exception&) {
        throw std::runtime_error("kafka: invalid 'max_batch_size' value '" + v +
                                 "' (want a positive integer of records)");
    }
}

// Forwarding emitter: convert KafkaMessage batches to string batches;
// pass watermarks/barriers through.
class StringKafkaSource final : public Source<std::string> {
public:
    explicit StringKafkaSource(KafkaSource::Options opts) : inner_(std::move(opts)) {}

    void open() override { inner_.open(); }
    void close() override { inner_.close(); }
    void cancel() override {
        Source<std::string>::cancel();
        inner_.cancel();
    }

    bool produce(Emitter<std::string>& out) override {
        Emitter<KafkaMessage> forwarder(
            Emitter<KafkaMessage>::Forward([&out](StreamElement<KafkaMessage> e) -> bool {
                if (e.is_data()) {
                    Batch<std::string> b;
                    for (const auto& r : e.as_data()) {
                        Record<std::string> rec(r.value().payload);
                        // Carry the Kafka partition as engine-only metadata so a
                        // downstream watermark assigner can track event time per
                        // partition (min across partitions) instead of one global
                        // watermark that races to the fastest partition.
                        if (r.value().partition >= 0) {
                            rec.set_source_partition(r.value().partition);
                        }
                        b.push(std::move(rec));
                    }
                    return out.emit_data(std::move(b));
                }
                if (e.is_watermark()) {
                    return out.emit_watermark(e.as_watermark());
                }
                return out.emit_barrier(e.as_barrier());
            }));
        return inner_.produce(forwarder);
    }

    // #57: delegate source-replay to the inner KafkaSource. Without this the
    // string/SQL Kafka path (kafka_source_string) would silently lose the
    // offset replay the inner source implements (#52) - the wrapper's default
    // no-op hooks would run instead, breaking exactly-once on restart.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override {
        inner_.snapshot_offset(backend, op_id, ckpt_id);
    }
    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        return inner_.restore_offset(backend, op_id);
    }

    std::string name() const override { return "kafka_text_source"; }

private:
    KafkaSource inner_;
};

// Adapter that turns a KafkaSink into a Sink<std::string>. Each
// incoming string is wrapped in a KafkaMessage{payload} (no key, no
// headers, partition unset) and forwarded to the inner sink.
class StringKafkaSink final : public Sink<std::string> {
public:
    explicit StringKafkaSink(KafkaSink::Options opts) : inner_(std::move(opts)) {}

    void open() override { inner_.open(); }
    void on_data(const Batch<std::string>& b) override {
        Batch<KafkaMessage> out_b;
        for (const auto& r : b) {
            out_b.emplace(KafkaMessage{r.value()});
        }
        inner_.on_data(out_b);
    }
    void on_watermark(Watermark wm) override { inner_.on_watermark(wm); }
    void on_barrier(CheckpointBarrier b) override { inner_.on_barrier(b); }
    void flush() override { inner_.flush(); }
    void close() override { inner_.close(); }
    std::string name() const override { return "kafka_text_sink"; }

private:
    KafkaSink inner_;
};

// 2PC-aware Kafka sink. Holds a transactional KafkaSink
// internally; treats on_data as plain forwarding (records produce
// inside the open transaction), on_barrier as a flush + record of
// the pending checkpoint id, on_commit as commitTransaction +
// begin a fresh transaction, and close as abortTransaction +
// inner.close().
class TwoPhaseCommitStringKafkaSink final : public Sink<std::string> {
public:
    explicit TwoPhaseCommitStringKafkaSink(KafkaSink::Options opts) : inner_(std::move(opts)) {}

    void open() override { inner_.open(); }

    void on_data(const Batch<std::string>& b) override {
        Batch<KafkaMessage> out_b;
        for (const auto& r : b) {
            out_b.emplace(KafkaMessage{r.value()});
        }
        if (!out_b.empty()) {
            inner_.on_data(out_b);
        }
    }

    void on_watermark(Watermark wm) override { inner_.on_watermark(wm); }
    void on_barrier(CheckpointBarrier b) override {
        // Pre-commit: flush in-flight records into the transaction.
        // The actual broker-side commit happens when the coordinator marks
        // the checkpoint globally durable via on_commit().
        inner_.flush();
        pending_checkpoint_ = b.id().value();
        inner_.on_barrier(b);
    }
    void on_commit(std::uint64_t checkpoint_id) override {
        if (!pending_checkpoint_.has_value() || *pending_checkpoint_ != checkpoint_id) {
            return;
        }
        inner_.commit_transaction();
        pending_checkpoint_.reset();
    }
    // Abort the prepared transaction. Mirrors on_commit
    // but calls abort_transaction so the broker discards the
    // PREPARED records. Idempotent against same checkpoint id.
    void on_abort(std::uint64_t checkpoint_id) override {
        if (!pending_checkpoint_.has_value() || *pending_checkpoint_ != checkpoint_id) {
            return;
        }
        inner_.abort_transaction();
        pending_checkpoint_.reset();
    }
    void flush() override { inner_.flush(); }
    void close() override {
        // If a transaction is still open at shutdown - meaning the
        // last barrier wasn't followed by an on_commit - abort it so
        // the broker doesn't leave records lingering in PREPARED.
        inner_.abort_transaction();
        inner_.close();
    }

    std::string name() const override { return "kafka_2pc_sink_string"; }

private:
    KafkaSink inner_;
    std::optional<std::uint64_t> pending_checkpoint_;
};

// Upsert-shaped Kafka sink. Takes JSON rows (each row is
// a complete JSON object) and emits Kafka records keyed by the
// configured primary_key columns. Rows tagged with
// `__row_kind == "delete"` are emitted with an empty payload, the
// log-compaction tombstone convention. Inserts (default kind) emit
// the row JSON minus the privileged `__row_kind` field as the
// payload. Watermarks / barriers / flush / close pass through to
// the inner KafkaSink unchanged.
class UpsertKafkaSink final : public Sink<std::string> {
public:
    UpsertKafkaSink(KafkaSink::Options opts, std::vector<std::string> primary_key)
        : inner_(std::move(opts)), primary_key_(std::move(primary_key)) {
        if (primary_key_.empty()) {
            throw std::runtime_error("kafka_upsert_sink_string: 'primary_key' is required");
        }
    }

    void open() override { inner_.open(); }
    void on_data(const Batch<std::string>& b) override {
        Batch<KafkaMessage> out_b;
        for (const auto& r : b) {
            auto built = build_message_(r.value());
            if (built.has_value()) {
                out_b.emplace(std::move(*built));
            }
        }
        if (!out_b.empty()) {
            inner_.on_data(out_b);
        }
    }
    void on_watermark(Watermark wm) override { inner_.on_watermark(wm); }
    void on_barrier(CheckpointBarrier b) override { inner_.on_barrier(b); }
    void flush() override { inner_.flush(); }
    void close() override { inner_.close(); }
    std::string name() const override { return "kafka_upsert_sink_string"; }

private:
    std::optional<KafkaMessage> build_message_(const std::string& row_json) {
        clink::config::JsonValue parsed;
        try {
            parsed = clink::config::parse(row_json);
        } catch (...) {
            return std::nullopt;
        }
        if (!parsed.is_object()) {
            return std::nullopt;
        }
        const auto& obj = parsed.as_object();
        std::string key;
        for (std::size_t i = 0; i < primary_key_.size(); ++i) {
            if (i > 0)
                key += '\x1f';
            auto it = obj.find(primary_key_[i]);
            if (it != obj.end() && !it->second.is_null()) {
                it->second.serialize_into(key);
            }
        }
        // Row kinds:
        //   delete         -> tombstone (empty payload)
        //   update_before  -> drop on the floor; the matching
        //                     update_after will overwrite by key
        //                     and Kafka log compaction handles the
        //                     replacement
        //   insert / update_after -> payload = row JSON
        auto rk_it = obj.find("__row_kind");
        std::string_view kind;
        if (rk_it != obj.end() && rk_it->second.is_string()) {
            kind = rk_it->second.as_string();
        }
        if (kind == "delete") {
            return KafkaMessage{std::string{}, std::move(key)};
        }
        if (kind == "update_before") {
            return std::nullopt;
        }
        clink::config::JsonObject payload_obj;
        for (const auto& [k, v] : obj) {
            if (k != "__row_kind") {
                payload_obj.emplace(k, v);
            }
        }
        std::string payload = clink::config::JsonValue{std::move(payload_obj)}.serialize(0);
        return KafkaMessage{std::move(payload), std::move(key)};
    }

    KafkaSink inner_;
    std::vector<std::string> primary_key_;
};

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // Register the typed channel for KafkaMessage so pipelines can carry
    // the full broker record (payload + key + headers + offset + partition
    // + timestamp) through the cluster without flattening to std::string.
    // Idempotent: register_type<T> is last-write-wins on the channel name.
    reg.register_type<KafkaMessage>(std::string{kChannelKafkaMessage}, kafka_message_codec());

    // kafka_message_source / kafka_message_sink: the typed Kafka I/O ops.
    // Source emits each broker record as a KafkaMessage (no information
    // lost). Sink takes KafkaMessage records and honours key, headers,
    // and partition if non-negative.
    reg.register_source<KafkaMessage>(
        "kafka_message_source",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<KafkaMessage>> {
            KafkaSource::Options opts;
            opts.brokers = ctx.param_or("brokers");
            opts.topic = ctx.param_or("topic");
            opts.group_id = ctx.param_or("group_id", "clink");
            opts.client_id = ctx.param_or("client_id", "clink-source");
            opts.auto_offset_reset = ctx.param_or("auto_offset_reset", "earliest");
            apply_batch_max_wait(ctx, opts);
            apply_batch_shape(ctx, opts);
            populate_kafka_security_conf(ctx, opts.conf);
            if (opts.brokers.empty()) {
                throw std::runtime_error("kafka_message_source: 'brokers' is required");
            }
            if (opts.topic.empty()) {
                throw std::runtime_error("kafka_message_source: 'topic' is required");
            }
            return std::make_shared<KafkaSource>(std::move(opts));
        });

    reg.register_sink<KafkaMessage>(
        "kafka_message_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<KafkaMessage>> {
            KafkaSink::Options opts;
            opts.brokers = ctx.param_or("brokers");
            opts.topic = ctx.param_or("topic");
            opts.client_id = ctx.param_or("client_id", "clink-sink");
            opts.acks = ctx.param_or("acks", "all");
            opts.compression_type = ctx.param_or("compression", "none");
            apply_linger_ms(ctx, opts);
            populate_kafka_security_conf(ctx, opts.conf);
            if (opts.brokers.empty()) {
                throw std::runtime_error("kafka_message_sink: 'brokers' is required");
            }
            if (opts.topic.empty()) {
                throw std::runtime_error("kafka_message_sink: 'topic' is required");
            }
            return std::make_shared<KafkaSink>(std::move(opts));
        });

    // kafka_text_source / kafka_text_sink: text-payload Kafka I/O.
    // Source emits each message's payload as std::string; sink wraps each
    // incoming std::string as KafkaMessage{payload}. Retained for
    // back-compat with pipelines that only need the payload. Plugins
    // wanting key/header/partition should prefer the typed
    // kafka_message_* variants above.
    //
    // The SQL physical planner emits op.type 'kafka_source_string' /
    // 'kafka_sink_string' for connector='kafka' (plain, at-least-once), so the
    // same builders are registered under those names too - otherwise the SQL
    // Kafka path compiles but fails at runtime with "unknown operator". (The
    // 2pc / upsert SQL sink variants are registered separately below.)
    auto text_source_builder = [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
        KafkaSource::Options opts;
        opts.brokers = ctx.param_or("brokers");
        opts.topic = ctx.param_or("topic");
        opts.group_id = ctx.param_or("group_id", "clink");
        opts.client_id = ctx.param_or("client_id", "clink-source");
        opts.auto_offset_reset = ctx.param_or("auto_offset_reset", "earliest");
        apply_batch_max_wait(ctx, opts);
        apply_batch_shape(ctx, opts);
        populate_kafka_security_conf(ctx, opts.conf);
        if (opts.brokers.empty()) {
            throw std::runtime_error("kafka source: 'brokers' is required");
        }
        if (opts.topic.empty()) {
            throw std::runtime_error("kafka source: 'topic' is required");
        }
        return std::make_shared<StringKafkaSource>(std::move(opts));
    };
    reg.register_source<std::string>("kafka_text_source", text_source_builder);
    reg.register_source<std::string>("kafka_source_string", text_source_builder);

    auto text_sink_builder = [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
        KafkaSink::Options opts;
        opts.brokers = ctx.param_or("brokers");
        opts.topic = ctx.param_or("topic");
        opts.client_id = ctx.param_or("client_id", "clink-sink");
        opts.acks = ctx.param_or("acks", "all");
        opts.compression_type = ctx.param_or("compression", "none");
        apply_linger_ms(ctx, opts);
        populate_kafka_security_conf(ctx, opts.conf);
        if (opts.brokers.empty()) {
            throw std::runtime_error("kafka sink: 'brokers' is required");
        }
        if (opts.topic.empty()) {
            throw std::runtime_error("kafka sink: 'topic' is required");
        }
        return std::make_shared<StringKafkaSink>(std::move(opts));
    };
    reg.register_sink<std::string>("kafka_text_sink", text_sink_builder);
    reg.register_sink<std::string>("kafka_sink_string", text_sink_builder);

    // kafka_2pc_sink_string. Transactional producer mode;
    // records are produced inside an open transaction. Barriers
    // flush; on_commit issues a commitTransaction call to the broker.
    //   brokers, topic, client_id, acks, compression - same as
    //       kafka_text_sink
    //   transactional_id (required) - librdkafka transactional.id
    //       config. Must be unique per producer instance; the SQL
    //       planner can append a subtask suffix when running with
    //       parallelism > 1 (caller's responsibility for now).
    reg.register_sink<std::string>(
        "kafka_2pc_sink_string", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            KafkaSink::Options opts;
            opts.brokers = ctx.param_or("brokers");
            opts.topic = ctx.param_or("topic");
            opts.client_id = ctx.param_or("client_id", "clink-sink-2pc");
            opts.compression_type = ctx.param_or("compression", "none");
            opts.transactional_id = ctx.param_or("transactional_id", "");
            apply_linger_ms(ctx, opts);
            populate_kafka_security_conf(ctx, opts.conf);
            if (opts.brokers.empty()) {
                throw std::runtime_error("kafka_2pc_sink_string: 'brokers' is required");
            }
            if (opts.topic.empty()) {
                throw std::runtime_error("kafka_2pc_sink_string: 'topic' is required");
            }
            if (opts.transactional_id.empty()) {
                throw std::runtime_error(
                    "kafka_2pc_sink_string: 'transactional_id' is required for 2PC");
            }
            if (ctx.parallelism > 1) {
                opts.transactional_id += "-" + std::to_string(ctx.subtask_idx);
            }
            auto sink = std::make_shared<TwoPhaseCommitStringKafkaSink>(std::move(opts));
            // Declare commit-group membership so the coordinator can
            // gate this sink's CommitCheckpoint on its group peers.
            if (auto cg = ctx.param_or("commit_group", ""); !cg.empty()) {
                sink->set_commit_group(cg);
            }
            return sink;
        });

    // kafka_upsert_sink_string. Takes JSON rows on the
    // string channel and emits keyed Kafka records. The SQL planner
    // chains this behind row_to_json_string when a sink table has
    // mode='upsert' and connector='kafka'.
    //   brokers, topic, client_id, acks, compression - same as
    //       kafka_text_sink
    //   primary_key (required, CSV) - columns to extract as the
    //       Kafka message key
    reg.register_sink<std::string>(
        "kafka_upsert_sink_string",
        [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            KafkaSink::Options opts;
            opts.brokers = ctx.param_or("brokers");
            opts.topic = ctx.param_or("topic");
            opts.client_id = ctx.param_or("client_id", "clink-sink");
            opts.acks = ctx.param_or("acks", "all");
            opts.compression_type = ctx.param_or("compression", "none");
            apply_linger_ms(ctx, opts);
            populate_kafka_security_conf(ctx, opts.conf);
            if (opts.brokers.empty()) {
                throw std::runtime_error("kafka_upsert_sink_string: 'brokers' is required");
            }
            if (opts.topic.empty()) {
                throw std::runtime_error("kafka_upsert_sink_string: 'topic' is required");
            }
            auto pk_csv = ctx.param_or("primary_key", "");
            if (pk_csv.empty()) {
                throw std::runtime_error(
                    "kafka_upsert_sink_string: 'primary_key' param is required");
            }
            std::vector<std::string> pk;
            std::size_t pos = 0;
            while (pos <= pk_csv.size()) {
                auto end = pk_csv.find(',', pos);
                if (end == std::string::npos)
                    end = pk_csv.size();
                auto k = pk_csv.substr(pos, end - pos);
                auto a = k.find_first_not_of(" \t");
                auto b = k.find_last_not_of(" \t");
                if (a != std::string::npos)
                    pk.push_back(k.substr(a, b - a + 1));
                if (end == pk_csv.size())
                    break;
                pos = end + 1;
            }
            return std::make_shared<UpsertKafkaSink>(std::move(opts), std::move(pk));
        });
}

}  // namespace clink::kafka
