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
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/connectors/kafka_message.hpp"
#include "clink/connectors/kafka_sink.hpp"
#include "clink/connectors/kafka_source.hpp"
#include "clink/connectors/txn_resume_registry.hpp"
#include "clink/core/record.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/kafka/install.hpp"
#include "clink/kafka/kafka_message_codec.hpp"
#include "clink/kafka/kafka_security.hpp"
#include "clink/kafka/txn_resume.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/network/connection.hpp"
#include "clink/state/state_backend.hpp"

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
                    // The batch is ours - the emitter moved it here and nothing
                    // else observes it - so take each payload rather than copying
                    // it. Copying was a second full copy of every record's bytes,
                    // on top of the one kafka_source already makes out of
                    // librdkafka's buffer.
                    Batch<KafkaMessage>& in_batch = e.as_data();  // e is by value, so mutable
                    b.reserve(in_batch.size());
                    for (auto& r : in_batch) {
                        Record<std::string> rec(std::move(r.value().payload));
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

// 2PC-aware Kafka sink. Holds a transactional KafkaSink internally. The open
// broker transaction always carries EXACTLY ONE checkpoint interval's
// records, and - the load-bearing rule - a checkpoint can only be ACKED once
// its interval's records are inside that transaction. Invariants, each
// earned against the Kafka exactly-once integration suite:
//
// 1. SERIALISATION. on_commit/on_abort dispatch on the worker's reader
//    thread while on_data/on_barrier/close run on the task thread, and
//    librdkafka's transactional API is a single-caller state machine - a
//    close()-time abort_transaction overlapping an in-flight
//    commit_transaction failed an assertion inside rdkafka_txnmgr.c and
//    SIGABRTed the whole worker on a CLEAN run's EOS. Every method takes
//    mu_.
//
// 2. COMMIT-BOUNDARY INTEGRITY. Records arriving after a barrier must not
//    ride that checkpoint's transaction: the checkpoint's source offset
//    sits at the barrier, so a worker lost right after the broker commit
//    replays them - as duplicates, had they been inside the committed
//    transaction. While a commit is outstanding, on_data buffers into
//    tail_; the commit produces the buffer into the fresh transaction it
//    begins.
//
// 3. AT MOST ONE UNCOMMITTED CHECKPOINT. on_barrier while the previous
//    commit is outstanding WAITS (bounded, loud on timeout). A non-blocking
//    revision queued the interval app-side instead and let its checkpoint
//    complete - which converted completed checkpoints into memory-only
//    data: a kill lost every queued interval, because a restore may pick
//    any completed checkpoint while a broker transaction cannot be resumed
//    by a new producer. Blocking the barrier blocks the ack, which caps
//    exposure at the single open transaction. The wait is deliberately
//    SHORT: a commit round-trip is milliseconds, so fifteen seconds means
//    the control plane is already broken, and failing the subtask loudly
//    both surfaces that and frees a cancelled incarnation within the bound
//    (an earlier 60s wait stalled every post-restart checkpoint behind a
//    cancelled predecessor).
//
// The residual window - a checkpoint that COMPLETED whose broker commit
// had not yet executed when the worker died - now has a recovery path: the
// barrier stages a resume handle (transactional.id + the producer identity
// captured from librdkafka's stats) into operator state, and on HA
// recovery the coordinator's in-doubt resolution commits the orphan over
// the wire BEFORE choosing the restore point (clink/kafka/txn_resume.hpp,
// clink/cluster/in_doubt_resolution.hpp). librdkafka itself still cannot
// resume a transaction, so when resolution cannot run (in-incarnation
// restart, SASL listener, identity never captured, broker refused) the
// commit-confirmed contract holds as before: restore from the last
// CONFIRMED checkpoint, duplicates bounded to one interval. See the
// connector doc's exactly-once caveat.
class TwoPhaseCommitStringKafkaSink final : public Sink<std::string> {
public:
    explicit TwoPhaseCommitStringKafkaSink(KafkaSink::Options opts, std::uint32_t subtask_idx = 0)
        : brokers_(opts.brokers),
          transactional_id_(opts.transactional_id),
          subtask_idx_(subtask_idx),
          inner_(std::move(opts)) {}

    void open() override { inner_.open(); }

    // The resume handle staged in on_barrier is state the checkpoint must
    // capture - same contract as CommittingSink's prepared handles.
    [[nodiscard]] bool stages_state_at_barrier() const noexcept override { return true; }

    void on_data(const Batch<std::string>& b) override {
        std::lock_guard lk(mu_);
        if (open_txn_ckpt_.has_value()) {
            // A commit is outstanding: these records belong to the next
            // interval and must not enter the prepared transaction.
            for (const auto& r : b) {
                tail_.push_back(r.value());
            }
            return;
        }
        forward_(b);
    }

    void on_watermark(Watermark wm) override {
        std::lock_guard lk(mu_);
        inner_.on_watermark(wm);
    }

    void on_barrier(CheckpointBarrier b) override {
        std::unique_lock lk(mu_);
        if (!pending_cv_.wait_for(
                lk, std::chrono::seconds{15}, [&] { return !open_txn_ckpt_.has_value(); })) {
            throw std::runtime_error(
                "kafka_2pc_sink_string: the previous checkpoint's commit did not arrive "
                "within the bound; failing rather than acking a checkpoint whose records "
                "are not yet inside a broker transaction");
        }
        // The live interval's records (tail_ drained by the last commit, or
        // forwarded directly) are in the open transaction; flush and mark it
        // as awaiting this checkpoint's commit.
        inner_.flush();
        inner_.on_barrier(b);
        open_txn_ckpt_ = b.id().value();
        // Stage the resume handle INSIDE this checkpoint: if the worker
        // dies after this checkpoint completes but before the commit
        // executes, recovery reads the handle out of the snapshot and can
        // finalise the orphaned transaction (txn_resume.hpp) - the one
        // window the design comment above calls residual. Staged even when
        // the producer identity has not been captured yet: the resolver
        // then refuses with a message naming the missing capture, which
        // beats a silently absent handle.
        stage_resume_handle_(b.id().value());
    }

    void on_commit(std::uint64_t checkpoint_id) override {
        // The same fault windows CommittingSink's finalise path exposes, so
        // the exactly-once suites can hold a broker commit open and kill a
        // worker immediately after one - the two moments where a 2PC sink's
        // claim actually gets decided.
        CLINK_FAULT_POINT(clink::fault::points::kSinkBeforeCommit);
        std::lock_guard lk(mu_);
        if (closed_) {
            // The prepared transaction died with close()'s abort. Executing
            // "successfully" here would confirm a checkpoint whose records
            // the broker has already discarded - the restore would then
            // select it and replay PAST them. Refusing keeps the worker
            // from sending CommitConfirmed, so the restore falls back to
            // the last checkpoint whose commit genuinely executed and the
            // replay re-produces these records.
            throw std::runtime_error(
                "kafka_2pc_sink_string: commit for checkpoint " + std::to_string(checkpoint_id) +
                " arrived after the sink closed; its transaction was aborted at teardown and "
                "must not be confirmed");
        }
        if (!open_txn_ckpt_.has_value() || *open_txn_ckpt_ != checkpoint_id) {
            if (checkpoint_id <= last_committed_ckpt_) {
                // Idempotent re-delivery of a commit this sink already
                // executed (e.g. the terminal barrier's local commit racing
                // the coordinator broadcast). Nothing left to do.
                return;
            }
            throw std::runtime_error("kafka_2pc_sink_string: commit for checkpoint " +
                                     std::to_string(checkpoint_id) +
                                     " does not match any prepared transaction (open: " +
                                     (open_txn_ckpt_.has_value() ? std::to_string(*open_txn_ckpt_)
                                                                 : std::string{"none"}) +
                                     "); refusing to confirm a commit that did not execute");
        }
        inner_.commit_transaction();  // commits, then begins the next txn
        last_committed_ckpt_ = checkpoint_id;
        CLINK_FAULT_POINT(clink::fault::points::kSinkAfterExternalCommit);
        // The commit provably executed: the staged handle must not outlive
        // it, or a later recovery could try to finalise a transaction that
        // no longer exists (harmless - the broker refuses - but noisy and
        // wrong on principle).
        erase_resume_handle_();
        resolve_open_();
    }

    // Abort the prepared transaction. Mirrors on_commit but calls
    // abort_transaction so the broker discards the PREPARED records.
    // Idempotent against same checkpoint id.
    void on_abort(std::uint64_t checkpoint_id) override {
        std::lock_guard lk(mu_);
        if (!open_txn_ckpt_.has_value() || *open_txn_ckpt_ != checkpoint_id) {
            return;
        }
        inner_.abort_transaction();  // aborts, then begins the next txn
        // The buffered records were emitted after the aborted checkpoint's
        // barrier. If the job restarts, this sink is torn down and the
        // transaction they enter here dies with close()'s abort; if the job
        // continues, the next checkpoint covers them and its commit
        // publishes them. Either way releasing them is the correct move -
        // dropping them would lose data in the continue case.
        resolve_open_();
    }

    void flush() override {
        std::lock_guard lk(mu_);
        inner_.flush();
    }

    void close() override {
        std::lock_guard lk(mu_);
        // If a transaction is still open at shutdown - meaning the last
        // barrier wasn't followed by an on_commit - abort it so the broker
        // doesn't leave records lingering in PREPARED. Buffered records die
        // with it; the checkpoint they follow was never committed, so the
        // restore replays them. closed_ makes any LATER on_commit refuse
        // loudly: teardown can race the coordinator's commit broadcast, and
        // a commit dispatched after this abort must not read as executed.
        closed_ = true;
        tail_.clear();
        inner_.abort_transaction();
        inner_.close();
        pending_cv_.notify_all();
    }

    std::string name() const override { return "kafka_2pc_sink_string"; }

private:
    void forward_(const Batch<std::string>& b) {
        Batch<KafkaMessage> out_b;
        for (const auto& r : b) {
            out_b.emplace(KafkaMessage{r.value()});
        }
        if (!out_b.empty()) {
            inner_.on_data(out_b);
        }
    }

    // After a commit or abort resolved the open interval: the buffered tail
    // enters the fresh transaction and normal streaming resumes.
    void resolve_open_() {
        open_txn_ckpt_.reset();
        if (!tail_.empty()) {
            Batch<KafkaMessage> out_b;
            for (auto& v : tail_) {
                out_b.emplace(KafkaMessage{std::move(v)});
            }
            tail_.clear();
            inner_.on_data(out_b);
        }
        pending_cv_.notify_all();
    }

    // --- prepared-transaction resume handle ------------------------------
    //
    // The identity a successor process needs to commit this transaction
    // via the wire protocol (clink/kafka/txn_resume.hpp), staged as
    // operator state so it lives and dies with the checkpoint. The 64-bit
    // producer_id travels as a STRING: the handle is read back through the
    // engine's JSON parser whose numbers are doubles, and a pid above 2^53
    // must not round-trip lossily.
    std::string resume_state_key_() const {
        return std::string(clink::connectors::kTxnResumeStateKeyPrefix) + "sub" +
               std::to_string(subtask_idx_);
    }

    clink::StateBackend* state_backend_() const noexcept {
        return this->runtime() != nullptr ? this->runtime()->state_backend() : nullptr;
    }

    void stage_resume_handle_(std::uint64_t ckpt) {
        auto* state = state_backend_();
        if (state == nullptr) {
            return;
        }
        const auto ident = inner_.producer_identity();
        std::string j = "{\"v\":1,\"resolver\":\"kafka_2pc\"";
        j += ",\"bootstrap\":\"" + brokers_ + "\"";
        j += ",\"transactional_id\":\"" + transactional_id_ + "\"";
        j += ",\"producer_id\":\"" + std::to_string(ident.has_value() ? ident->producer_id : -1) +
             "\"";
        j += ",\"producer_epoch\":\"" +
             std::to_string(ident.has_value() ? ident->producer_epoch : -1) + "\"";
        j += ",\"ckpt\":\"" + std::to_string(ckpt) + "\"}";
        const auto key = resume_state_key_();
        state->put_operator_state(this->id(),
                                  clink::StateBackend::KeyView{key.data(), key.size()},
                                  clink::StateBackend::ValueView{j.data(), j.size()});
    }

    void erase_resume_handle_() {
        auto* state = state_backend_();
        if (state == nullptr) {
            return;
        }
        const auto key = resume_state_key_();
        state->erase_operator_state(this->id(),
                                    clink::StateBackend::KeyView{key.data(), key.size()});
    }

    std::string brokers_;
    std::string transactional_id_;
    std::uint32_t subtask_idx_{0};
    KafkaSink inner_;
    std::mutex mu_;
    std::condition_variable pending_cv_;
    std::optional<std::uint64_t> open_txn_ckpt_;
    std::vector<std::string> tail_;
    // Highest checkpoint whose commit this instance EXECUTED. A re-delivered
    // commit at or below it is idempotent; anything else that misses the
    // open transaction is a commit that did not happen and must throw.
    std::uint64_t last_committed_ckpt_{0};
    // Set by close(). A commit arriving after teardown aborted the prepared
    // transaction must refuse rather than read as executed.
    bool closed_{false};
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

// In-doubt resolver for kafka_2pc handles (see txn_resume_registry.hpp for
// the coordinator-side contract). Parses the handle the 2PC sink staged,
// then walks the bootstrap list attempting the wire-level EndTxn(commit);
// a broker VERDICT (refused / unsupported) is final at the first broker
// that gives one, a transport failure tries the next bootstrap entry.
clink::connectors::InDoubtResolution resolve_kafka_2pc_handle(const std::string& handle_json) {
    clink::kafka::TxnIdentity txn;
    std::string bootstrap;
    try {
        const auto j = clink::config::parse(handle_json);
        bootstrap = j.at("bootstrap").as_string();
        txn.transactional_id = j.at("transactional_id").as_string();
        txn.producer_id = std::stoll(j.at("producer_id").as_string());
        txn.producer_epoch =
            static_cast<std::int16_t>(std::stoi(j.at("producer_epoch").as_string()));
    } catch (const std::exception& e) {
        return {false, std::string("kafka_2pc handle did not parse: ") + e.what()};
    }
    if (!txn.complete()) {
        return {false,
                "kafka_2pc handle for '" + txn.transactional_id +
                    "' carries no producer identity (the stats capture had not fired before "
                    "the barrier); nothing can be resumed"};
    }
    const auto connect = [](const std::string& host, std::uint16_t port) {
        return clink::network::connect_plain(host, port);
    };
    // SASL credentials come from the RESOLVING process's environment at
    // resolution time - never from the staged handle, because durable
    // checkpoint state must not carry secrets. Unset = unauthenticated,
    // the pre-SASL behaviour; a mechanism the resume path does not speak
    // is refused loudly inside resume_commit rather than downgraded.
    clink::kafka::ResumeAuth auth;
    if (const char* m = std::getenv("CLINK_KAFKA_RESUME_SASL_MECHANISM"); m != nullptr) {
        auth.mechanism = m;
    }
    if (const char* u = std::getenv("CLINK_KAFKA_RESUME_SASL_USERNAME"); u != nullptr) {
        auth.username = u;
    }
    if (const char* p = std::getenv("CLINK_KAFKA_RESUME_SASL_PASSWORD"); p != nullptr) {
        auth.password = p;
    }
    std::string last;
    std::size_t pos = 0;
    while (pos <= bootstrap.size()) {
        const auto comma = bootstrap.find(',', pos);
        const auto entry =
            bootstrap.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
        pos = comma == std::string::npos ? bootstrap.size() + 1 : comma + 1;
        if (entry.empty()) {
            continue;
        }
        const auto colon = entry.rfind(':');
        const std::string host = colon == std::string::npos ? entry : entry.substr(0, colon);
        const std::uint16_t port =
            colon == std::string::npos
                ? std::uint16_t{9092}
                : static_cast<std::uint16_t>(std::stoi(entry.substr(colon + 1)));
        const auto outcome = clink::kafka::resume_commit(host, port, txn, connect, auth);
        if (outcome.status != clink::kafka::ResumeOutcome::Status::TransportError) {
            return {outcome.committed(), outcome.detail};
        }
        last = outcome.detail;
    }
    return {false, "no bootstrap broker reachable: " + last};
}

void install(clink::plugin::PluginRegistry& reg) {
    // The resolver the coordinator's restore-point selection dispatches to
    // for handles staged by the 2PC sink below.
    clink::connectors::TxnResumeRegistry::instance().register_resolver("kafka_2pc",
                                                                       resolve_kafka_2pc_handle);
    // Capability declaration. Read out of this file and kafka_source.hpp,
    // not out of what Kafka is capable of in general.
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "kafka",
        .version = "1",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {"librdkafka"},
        .runtime_dependencies = {"kafka broker >= 0.11 for transactions"},
        .formats = {"text", "json", "bytes"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        // KafkaSource::snapshot_offset / restore_offset persist the
        // per-partition next-offset map into operator state, so the source
        // replays from clink's checkpoint rather than the broker's
        // committed offset.
        .replayable = true,
        .offset_model = clink::connectors::OffsetModel::LogOffset,
        .checkpoint_integrated = true,
        // The DEFAULT sink is a plain producer: at-least-once. The
        // transactional variant is a separate factory (kafka_2pc_sink_*)
        // and is what earns the exactly-once label below.
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .schema_evolution = false,
        .partition_discovery = true,
        .auth_methods = {"none", "sasl_plaintext", "sasl_ssl", "ssl"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"poll_timeout_ms"},
        .available_in_sql = true,
        .limitations = {"the plain sink is at-least-once; use the kafka_2pc connector for "
                        "transactional output"},
    });

    // The transactional sink is a DISTINCT connector for capability
    // purposes: same broker, different guarantee, different required
    // options. Conflating them is how a job ends up believing it has
    // exactly-once because it named a Kafka sink.
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "kafka_2pc",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"librdkafka"},
        .runtime_dependencies = {"kafka broker >= 0.11"},
        .formats = {"text", "json", "bytes"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::ExactlyOnceTwoPhaseCommit,
        .transactional = true,
        // A broker transaction cannot be resumed by a new producer: the
        // commit either executed before the crash or the records are gone
        // from the transaction. The commit-confirmed restore protocol
        // (CONFIRMED-N markers) turns that from silent loss into a bounded
        // duplicate window.
        .commit_recoverable = false,
        .schema_evolution = false,
        .partition_discovery = true,
        .auth_methods = {"none", "sasl_plaintext", "sasl_ssl", "ssl"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"poll_timeout_ms", "transaction_timeout_ms"},
        .available_in_sql = true,
        .limitations = {"transactional_id must be unique per job AND stable across restarts; "
                        "the factory suffixes the subtask index onto it",
                        "consumers must read with isolation.level=read_committed or they will "
                        "see aborted records",
                        "die-after-commit-before-confirmation is bounded to one duplicated "
                        "interval; HA recovery additionally resumes orphaned prepared "
                        "transactions over the wire (in-doubt resolution), best-effort - a "
                        "SASL-only listener or an unsupported broker version falls back to "
                        "the bounded contract"},
        .required_options_for_exactly_once = {"transactional_id"},
    });

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
            auto sink =
                std::make_shared<TwoPhaseCommitStringKafkaSink>(std::move(opts), ctx.subtask_idx);
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
