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

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/cluster/protocol_trace.hpp"
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
#include "clink/kafka/string_channel.hpp"
#include "clink/kafka/txn_resume.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/network/connection.hpp"
#include "clink/state/durable_file_write.hpp"
#include "clink/state/state_backend.hpp"
#include "clink/time/event_time.hpp"
#ifdef CLINK_HAS_SCHEMA_REGISTRY
#include "clink/schema_registry/formats.hpp"
#endif

#ifdef CLINK_KAFKA_RESUME_TLS
#include "clink/runtime/network/tls_connection.hpp"
#include "clink/runtime/network/tls_socket.hpp"
#endif

namespace clink::kafka {

namespace {

using clink::plugin::BuildContext;

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

void apply_deterministic_ownership(const plugin::BuildContext& ctx, KafkaSource::Options& opts) {
    // Partition ownership is decided by the engine, not by consumer-group
    // rebalancing: this subtask consumes exactly the partitions p with
    // p % parallelism == subtask_idx, assigned manually inside
    // KafkaSource::open(). Ownership therefore survives restores, worker
    // moves and coordinator failovers on the same cut as the checkpointed
    // per-partition offset rows.
    opts.subtask_index = ctx.subtask_idx;
    opts.source_parallelism = std::max<std::uint32_t>(1, ctx.parallelism);
    // The engine's checkpoints are the only resume authority, so the
    // engine-managed source must not WRITE group offsets: librdkafka's
    // auto-commit records consumed positions the checkpoint never
    // completed, and a restore whose partition lacks an offset row then
    // resumes from that group offset via OFFSET_STORED - past records
    // whose effects died with the rewound attempt. Measured as silent
    // loss of the whole pre-restart span when a job's first checkpoint
    // failed. Manual mode writes nothing unless commit_current() is
    // called, which the engine never does.
    opts.commit_mode = KafkaSource::CommitMode::Manual;
}

// Registry-framed value formats on the string channel (format='avro' |
// 'protobuf' | 'json-schema'): a source decodes each message into the JSON
// object text the channel carries, a sink encodes the channel's JSON rows
// into framed messages, so the planner's json_string_to_row and
// row_to_json_string bridges are unchanged either side. Both are built from
// the table's WITH options at build time, where a format this build lacks or
// one missing its registry URL is refused by name; the encoder also talks to
// the registry there (register or read the subject), so a misconfigured sink
// fails at deploy, not on its first record. A plain (json / text) table
// builds neither and pays nothing.
#ifdef CLINK_HAS_SCHEMA_REGISTRY
using ValueDecoderPtr = std::shared_ptr<clink::schema_registry::ValueDecoder>;
using ValueEncoderPtr = std::shared_ptr<clink::schema_registry::ValueEncoder>;

clink::schema_registry::ParsedFormatOptions parse_value_format(const BuildContext& ctx,
                                                               const char* who) {
    return clink::schema_registry::parse_format_options(
        [&ctx](const std::string& key, const std::string& fallback) {
            return ctx.param_or(key, fallback);
        },
        ctx.param_or("topic"),
        who);
}

ValueDecoderPtr build_value_decoder(const BuildContext& ctx, const char* who) {
    auto parsed = parse_value_format(ctx, who);
    if (!parsed.error.empty()) {
        throw std::runtime_error(parsed.error);
    }
    if (!parsed.options.has_value()) {
        return nullptr;
    }
    auto client = std::make_shared<clink::schema_registry::Client>(parsed.options->client);
    return clink::schema_registry::make_decoder(*parsed.options, std::move(client));
}

ValueEncoderPtr build_value_encoder(const BuildContext& ctx, const char* who) {
    auto parsed = parse_value_format(ctx, who);
    if (!parsed.error.empty()) {
        throw std::runtime_error(parsed.error);
    }
    if (!parsed.options.has_value()) {
        return nullptr;
    }
    auto client = std::make_shared<clink::schema_registry::Client>(parsed.options->client);
    try {
        return clink::schema_registry::make_encoder(*parsed.options, std::move(client));
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string{who} + ": " + e.what());
    }
}
#else
// Built without impls/schema_registry: the registry formats are refused by
// name, everything else is untouched.
struct NoValueDecoder {
    std::string to_json(std::string_view) { return {}; }
};
struct NoValueEncoder {
    std::string from_json(std::string_view) { return {}; }
};
using ValueDecoderPtr = std::shared_ptr<NoValueDecoder>;
using ValueEncoderPtr = std::shared_ptr<NoValueEncoder>;

void refuse_registry_format(const BuildContext& ctx, const char* who) {
    const auto fmt = ctx.param_or("format", "");
    if (fmt == "avro" || fmt == "protobuf" || fmt == "json-schema" || fmt == "json_schema" ||
        fmt == "jsonschema") {
        throw std::runtime_error(
            std::string{who} + ": format='" + fmt +
            "' needs the schema registry library, which this clink build "
            "does not include (impls/schema_registry needs clink::http_connector)");
    }
}
ValueDecoderPtr build_value_decoder(const BuildContext& ctx, const char* who) {
    refuse_registry_format(ctx, who);
    return nullptr;
}
ValueEncoderPtr build_value_encoder(const BuildContext& ctx, const char* who) {
    refuse_registry_format(ctx, who);
    return nullptr;
}
#endif

// decode_error='fail' (default) | 'skip': what a source does with a message
// its format cannot decode. fail stops the job with the reason, partition and
// offset; skip drops the record, logs it (the first, then every 1000th) and
// carries on - the poison-pill policy for a topic known to carry strays.
bool skip_undecodable(const BuildContext& ctx, const char* who) {
    const auto policy = ctx.param_or("decode_error", "fail");
    if (policy == "fail") {
        return false;
    }
    if (policy == "skip") {
        return true;
    }
    throw std::runtime_error(std::string{who} + ": decode_error must be 'fail' or 'skip' (got '" +
                             policy + "')");
}

// Forwarding emitter: convert KafkaMessage batches to string batches;
// pass watermarks/barriers through. With a value decoder, each payload is
// translated to the channel's JSON text on the way.
class StringKafkaSource final : public Source<std::string> {
public:
    explicit StringKafkaSource(KafkaSource::Options opts,
                               ValueDecoderPtr decoder = nullptr,
                               bool skip_undecodable = false)
        : topic_(opts.topic),
          inner_(std::move(opts)),
          decoder_(std::move(decoder)),
          skip_undecodable_(skip_undecodable) {}

    void open() override { inner_.open(); }
    void close() override { inner_.close(); }
    void cancel() override {
        Source<std::string>::cancel();
        inner_.cancel();
    }

    bool produce(Emitter<std::string>& out) override {
        Emitter<KafkaMessage> forwarder(
            Emitter<KafkaMessage>::Forward([this, &out](StreamElement<KafkaMessage> e) -> bool {
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
                        if (decoder_ && !decode_(r.value())) {
                            continue;  // skipped under decode_error='skip'
                        }
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
    // Replace the message's payload with its JSON text. false = drop it.
    bool decode_(KafkaMessage& m) {
        try {
            m.payload = decoder_->to_json(m.payload);
            return true;
        } catch (const std::exception& e) {
            const std::string where = "kafka source: topic '" + topic_ + "' partition " +
                                      std::to_string(m.partition) + " offset " +
                                      std::to_string(m.offset) + ": " + e.what();
            if (!skip_undecodable_) {
                throw std::runtime_error(where);
            }
            const auto n = ++skipped_;
            if (n == 1 || n % 1000 == 0) {
                if (auto* rc = this->runtime(); rc != nullptr) {
                    rc->log_warn("skipping undecodable record (" + std::to_string(n) +
                                 " so far): " + where);
                }
            }
            return false;
        }
    }

    std::string topic_;
    KafkaSource inner_;
    ValueDecoderPtr decoder_;
    bool skip_undecodable_{false};
    std::uint64_t skipped_{0};
};

// Adapter that turns a KafkaSink into a Sink<std::string>. Each
// incoming string is wrapped in a KafkaMessage{payload} (no key, no
// headers, partition unset) and forwarded to the inner sink.
class StringKafkaSink final : public Sink<std::string> {
public:
    explicit StringKafkaSink(KafkaSink::Options opts, ValueEncoderPtr encoder = nullptr)
        : inner_(std::move(opts)), encoder_(std::move(encoder)) {}

    void open() override { inner_.open(); }
    void on_data(const Batch<std::string>& b) override {
        Batch<KafkaMessage> out_b;
        for (const auto& r : b) {
            out_b.emplace(KafkaMessage{encoder_ ? encoder_->from_json(r.value()) : r.value()});
        }
        inner_.on_data(out_b);
    }
    void on_watermark(Watermark wm) override { inner_.on_watermark(wm); }
    void on_barrier(CheckpointBarrier b) override { inner_.on_barrier(b); }
    // The inner sink's barrier flush is this sink's guarantee: see KafkaSink.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override {
        return inner_.gates_checkpoint_ack();
    }
    void flush() override { inner_.flush(); }
    void close() override { inner_.close(); }
    std::string name() const override { return "kafka_text_sink"; }

private:
    KafkaSink inner_;
    ValueEncoderPtr encoder_;
};

// The dialer + credentials for the resume-protocol clients (EndTxn probes
// and read-only DescribeTransactions): plaintext by default, TLS when the
// process's environment names a CA bundle, SASL from the environment. Shared
// by the coordinator-side resolver (resolve_kafka_2pc_handle) and the sink's
// pre-fence orphan resolution - transport security is always the CALLING
// process's configuration, never the staged handle's. Over TLS it also checks
// that each broker's certificate names the host dialled, unless the
// ResumeNameCheck the caller passes says `none`.
struct ResumeDialer {
    clink::kafka::ConnectFn connect;
    clink::kafka::ResumeAuth auth;
    std::string error;  // non-empty: refused (TLS misconfiguration)
};

// Whether the TLS dialer checks that a broker's certificate names the host it
// dials (the bootstrap entry, then the transaction coordinator FindCoordinator
// names), set the way librdkafka's ssl.endpoint.identification.algorithm is:
// `https` checks, `none` does not, unset is librdkafka's default, `https`.
struct ResumeNameCheck {
    std::string algorithm;  // as configured, empty when unset
    const char* setting;    // what configured it, for a refusal
    const char* opt_out;    // what turns the check off, named by a mismatch
};

// A sink's own settle checks names exactly when the sink's producer does, so
// the two connections to the same brokers cannot disagree about it.
ResumeNameCheck sink_name_check(const std::map<std::string, std::string>& conf) {
    const auto it = conf.find("ssl.endpoint.identification.algorithm");
    return {.algorithm = it == conf.end() ? std::string{} : it->second,
            .setting = "the sink's ssl_endpoint_identification_algorithm",
            .opt_out = "ssl_endpoint_identification_algorithm='none' on the sink"};
}

// The coordinator's resolver never sees a sink's options: the same setting
// comes from its environment, beside CLINK_KAFKA_RESUME_TLS_CA.
ResumeNameCheck environment_name_check() {
    const char* v = std::getenv("CLINK_KAFKA_RESUME_TLS_ENDPOINT_IDENTIFICATION_ALGORITHM");
    return {.algorithm = v == nullptr ? std::string{} : std::string{v},
            .setting = "CLINK_KAFKA_RESUME_TLS_ENDPOINT_IDENTIFICATION_ALGORITHM",
            .opt_out = "CLINK_KAFKA_RESUME_TLS_ENDPOINT_IDENTIFICATION_ALGORITHM=none"};
}

ResumeDialer build_resume_dialer(const ResumeNameCheck& names = environment_name_check()) {
    ResumeDialer d;
    d.connect = [](const std::string& host, std::uint16_t port) {
        return clink::network::connect_plain(host, port);
    };
    if (const char* tls_ca = std::getenv("CLINK_KAFKA_RESUME_TLS_CA");
        tls_ca != nullptr && *tls_ca != '\0') {
#ifdef CLINK_KAFKA_RESUME_TLS
        std::string algorithm = names.algorithm;
        std::transform(algorithm.begin(), algorithm.end(), algorithm.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });
        if (!algorithm.empty() && algorithm != "https" && algorithm != "none") {
            d.error = std::string{"TLS requested (CLINK_KAFKA_RESUME_TLS_CA) but "} +
                      names.setting + " is '" + names.algorithm +
                      "', neither 'https' nor 'none'; refusing rather than guessing whether "
                      "to check the broker's certificate names its host";
            return d;
        }
        try {
            auto ctx = std::make_shared<clink::network::TlsClientContext>(tls_ca);
            const char* cert = std::getenv("CLINK_KAFKA_RESUME_TLS_CERT");
            const char* key = std::getenv("CLINK_KAFKA_RESUME_TLS_KEY");
            if (cert != nullptr && *cert != '\0' && key != nullptr && *key != '\0') {
                ctx->set_client_cert(cert, key);
            }
            // The same bound as each read of the resume (5 s): a restore
            // waits on this, and a broker whose TCP connection completes and
            // that never answers the ClientHello (frozen, or behind a stalled
            // middlebox) would otherwise hold it for ever.
            const clink::network::TlsConnectOptions resume_connect{
                .connect_timeout = std::chrono::milliseconds{5000},
                .verify_hostname = algorithm != "none",
                .verify_hostname_opt_out = names.opt_out};
            d.connect = [ctx, resume_connect](const std::string& host, std::uint16_t port) {
                // A failed handshake throws, and the resume takes it as a
                // transport failure, as it does an unreachable broker: bounded
                // retries and the honest transport fallback, with the reason
                // (a certificate that does not name the broker, say) in the
                // outcome's detail.
                return clink::network::connect_tls_connection(host, port, ctx, resume_connect);
            };
        } catch (const std::exception& e) {
            d.error =
                std::string{
                    "TLS requested (CLINK_KAFKA_RESUME_TLS_CA) but the client "
                    "context could not be built: "} +
                e.what();
            return d;
        }
#else
        (void)names;
        d.error =
            "TLS requested (CLINK_KAFKA_RESUME_TLS_CA) but this build carries no "
            "clink::tls; refusing rather than downgrading to plaintext";
        return d;
#endif
    }
    if (const char* m = std::getenv("CLINK_KAFKA_RESUME_SASL_MECHANISM"); m != nullptr) {
        d.auth.mechanism = m;
    }
    if (const char* u = std::getenv("CLINK_KAFKA_RESUME_SASL_USERNAME"); u != nullptr) {
        d.auth.username = u;
    }
    if (const char* p = std::getenv("CLINK_KAFKA_RESUME_SASL_PASSWORD"); p != nullptr) {
        d.auth.password = p;
    }
    return d;
}

std::vector<std::pair<std::string, std::uint16_t>> parse_bootstrap_entries(
    const std::string& bootstrap) {
    std::vector<std::pair<std::string, std::uint16_t>> entries;
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
        const auto port = colon == std::string::npos
                              ? std::uint16_t{9092}
                              : static_cast<std::uint16_t>(std::stoi(entry.substr(colon + 1)));
        entries.emplace_back(host, port);
    }
    return entries;
}

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
    explicit TwoPhaseCommitStringKafkaSink(KafkaSink::Options opts,
                                           std::uint32_t subtask_idx = 0,
                                           bool replay_suppression = true,
                                           ValueEncoderPtr encoder = nullptr)
        : brokers_(opts.brokers),
          transactional_id_(opts.transactional_id),
          subtask_idx_(subtask_idx),
          replay_suppression_(replay_suppression),
          resume_names_(sink_name_check(opts.conf)),
          inner_(std::move(opts)),
          encoder_(std::move(encoder)) {}

    // The subtask index the protocol trace names this sink by: the job-global
    // one the coordinator tracks (RuntimeContext's runner identity), else the
    // per-operator index a bare test constructs it with.
    std::uint64_t trace_subtask_() const noexcept {
        return this->runtime() != nullptr ? this->runtime()->runner_subtask_idx() : subtask_idx_;
    }
    // A protocol trace event naming this sink: its job, when a coordinator
    // assigned one, and its job-global subtask (see CommittingSink).
    protocol_trace::Event trace_sink_event_(protocol_trace::Event ev) const {
        if (this->runtime() != nullptr && this->runtime()->runner_job_id() != 0) {
            ev.u("job", this->runtime()->runner_job_id());
        }
        ev.u("sub", trace_subtask_());
        return ev;
    }

    void open() override {
        // BEFORE inner_.open(): opening the transactional producer fences
        // the previous incarnation and erases the broker state that names
        // how its last transaction ended. Anything this subtask must still
        // decide or learn about its predecessor has to be done first, and
        // the receipt a proven commit earns must be durable before replay
        // suppression arms from the receipts below.
        resolve_unresolved_orphan_();
        inner_.open();
        std::lock_guard lk(mu_);
        arm_replay_suppression_();
        if (protocol_trace::enabled()) {
            auto ev = trace_sink_event_(protocol_trace::Event("SinkOpens"));
            ev.s("family", "kafka");
            // Present only when an unresolved marker above the restore point
            // was found and settled: which checkpoint, and how it ended.
            if (settled_verdict_ != nullptr) {
                ev.u("orphan", settled_orphan_ckpt_).s("settled", settled_verdict_);
            }
            ev.emit();
        }
    }

    // The resume handle staged in on_barrier is state the checkpoint must
    // capture - same contract as CommittingSink's prepared handles.
    [[nodiscard]] bool stages_state_at_barrier() const noexcept override { return true; }

    // The sealed transaction awaiting its CommitCheckpoint: 1 from the barrier
    // that prepares it until the commit or abort that resolves it, else 0. The
    // runner's end-of-stream wait reads it and holds the teardown open while it
    // is 1, so the last checkpoint's commit, arriving on the dispatch thread
    // after the input ended, runs instead of being refused and left for
    // in-doubt resolution. A terminal barrier's local commit (or its abort on a
    // stopping task) resolves its transaction before the wait starts, and an
    // AbortCheckpoint resolves it as a commit does, so neither leaves the wait
    // holding. Read on the runner's thread without mu_, which on_commit holds
    // across the broker's commit: a mirror of open_txn_ckpt_, written under mu_
    // wherever that is.
    [[nodiscard]] std::size_t staged_commits_outstanding() const override {
        return awaiting_commit_.load(std::memory_order_acquire) ? 1 : 0;
    }

    void on_data(const Batch<std::string>& b) override {
        std::lock_guard lk(mu_);
        if (suppress_armed_) {
            // Replay suppression: the receipts this instance armed from
            // prove its commits up to suppress_source_ckpt_ executed, so the
            // restored run's re-emissions of that span are already published.
            // A fired pane is committed iff its event time (window end - 1)
            // is at or below the watermark the sealing barrier saw - exact
            // for watermark-monotone feeds (windowed/aggregated emissions),
            // which is the premise this option documents.
            Batch<std::string> kept;
            for (const auto& r : b) {
                if (!suppress_record_(r)) {
                    kept.push(r);
                }
            }
            if (kept.empty()) {
                return;
            }
            if (open_txn_ckpt_.has_value()) {
                for (const auto& r : kept) {
                    tail_.push_back(r.value());
                }
                return;
            }
            forward_(kept);
            return;
        }
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
        // Track the highest non-idle watermark: it stamps each prepared
        // transaction's receipt (the horizon its records are covered by) and
        // retires replay suppression once the replay has passed the horizon.
        if (!wm.is_idle() && wm.timestamp().millis() > cur_wm_) {
            cur_wm_ = wm.timestamp().millis();
            if (suppress_armed_ && cur_wm_ > suppress_horizon_) {
                disarm_suppression_();
            }
        }
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
        // as awaiting this checkpoint's commit. This IS the kafka 2PC
        // prepare: the named fault points bracket it so a qualification
        // campaign arming sink.before_prepare / sink.after_prepare on this
        // sink family genuinely dies inside the prepare window. They used to
        // exist only in the CommittingSink family, so arming them against a
        // kafka exactly-once pipeline tested nothing but an ordinary worker
        // restart while the evidence said otherwise.
        CLINK_FAULT_POINT(clink::fault::points::kSinkBeforePrepare);
        inner_.flush();
        inner_.on_barrier(b);
        open_txn_ckpt_ = b.id().value();
        awaiting_commit_.store(true, std::memory_order_release);
        // The watermark horizon this transaction's records are covered by:
        // stamped into the commit receipt so a later restore knows how far
        // its replayed re-emissions are already published.
        open_txn_wm_ = cur_wm_;
        // Stage the resume handle INSIDE this checkpoint: if the worker
        // dies after this checkpoint completes but before the commit
        // executes, recovery reads the handle out of the snapshot and can
        // finalise the orphaned transaction (txn_resume.hpp) - the one
        // window the design comment above calls residual. Staged even when
        // the producer identity has not been captured yet: the resolver
        // then refuses with a message naming the missing capture, which
        // beats a silently absent handle.
        stage_resume_handle_(b.id().value());
        CLINK_FAULT_POINT(clink::fault::points::kSinkAfterPrepare);
        if (protocol_trace::enabled()) {
            trace_sink_event_(protocol_trace::Event("SinkPrepare"))
                .u("ckpt", b.id().value())
                .s("family", "kafka")
                .b("staged", true)
                .emit();
        }
    }

    void on_commit(std::uint64_t checkpoint_id) override {
        // The same fault windows CommittingSink's finalise path exposes, so
        // the exactly-once suites can hold a broker commit open and kill a
        // worker immediately after one - the two moments where a 2PC sink's
        // claim actually gets decided.
        CLINK_FAULT_POINT(clink::fault::points::kSinkBeforeCommit);
        std::lock_guard lk(mu_);
        // The runner has begun this sink's teardown and waits for this
        // dispatch: a commit_transaction started now would hold the teardown
        // on the broker, indefinitely while the broker is unreachable, and the
        // restart drain with it. Declined like a commit after close().
        const bool tearing_down = this->teardown_begun();
        const bool accepted = !closed_ && !tearing_down && open_txn_ckpt_.has_value() &&
                              *open_txn_ckpt_ == checkpoint_id;
        if (protocol_trace::enabled()) {
            trace_sink_event_(protocol_trace::Event("DeliverCommit"))
                .u("ckpt", checkpoint_id)
                .b("accepted", accepted)
                .emit();
        }
        if (closed_) {
            // The producer was destroyed at close(), so this process can no
            // longer execute the commit. The PREPARED transaction survives
            // teardown (close leaves it for the checkpoint protocol), and
            // the in-doubt resolver finalises it at the held restart.
            // Confirming here would claim an execution that did not happen.
            throw std::runtime_error(
                "kafka_2pc_sink_string: commit for checkpoint " + std::to_string(checkpoint_id) +
                " arrived after the sink closed; the prepared transaction is left for "
                "in-doubt resolution and must not be confirmed by this process");
        }
        if (tearing_down) {
            throw std::runtime_error(
                "kafka_2pc_sink_string: commit for checkpoint " + std::to_string(checkpoint_id) +
                " arrived as the sink's teardown began; the prepared transaction is left for "
                "in-doubt resolution and must not be confirmed by this process");
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
        // The receipt is written BETWEEN the broker's commit and the next
        // begin_transaction. Order is the contract: while no successor
        // transaction is Ongoing, the transaction coordinator still reads
        // CompleteCommit for this commit, so a death in the ack window
        // leaves either the receipt or a coordinator state that names the
        // commit - which the pre-fence resolution in open() maps back to
        // "committed". With begin first (the old order), a death here left
        // Ongoing, indistinguishable from an undecided prepared
        // transaction, and a restore below this checkpoint replayed panes
        // the broker had already published (the rig-night duplicate).
        inner_.commit_transaction([&] {
            if (protocol_trace::enabled()) {
                trace_sink_event_(protocol_trace::Event("SinkCommit"))
                    .u("ckpt", checkpoint_id)
                    .emit();
            }
            CLINK_FAULT_POINT(clink::fault::points::kSinkBetweenCommitAndReceipt);
            write_commit_receipt_(checkpoint_id, open_txn_wm_);
            if (protocol_trace::enabled()) {
                trace_sink_event_(protocol_trace::Event("SinkReceipt"))
                    .u("ckpt", checkpoint_id)
                    .emit();
            }
        });
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
        // As on_commit: once the teardown has begun the broker call is not
        // started, and the prepared transaction is left as close() leaves it.
        const bool accepted = !this->teardown_begun() && open_txn_ckpt_.has_value() &&
                              *open_txn_ckpt_ == checkpoint_id;
        if (protocol_trace::enabled()) {
            trace_sink_event_(protocol_trace::Event("DeliverAbort"))
                .u("ckpt", checkpoint_id)
                .b("accepted", accepted)
                .emit();
        }
        if (!accepted) {
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
        closed_ = true;
        tail_.clear();
        if (!open_txn_ckpt_.has_value()) {
            // Only the OPEN TAIL transaction is teardown's to abort: no
            // barrier sealed it, no checkpoint covers its records, and the
            // restore replays them.
            inner_.abort_transaction();
        }
        // A barrier-sealed PREPARED transaction is the checkpoint
        // protocol's to finalise - commit broadcast, abort broadcast, or
        // the in-doubt resolver - never teardown's. This used to abort it
        // on the reasoning that "the checkpoint was never committed", but a
        // checkpoint CAN be completed with its commit broadcast racing a
        // restart drain: some subtasks commit, the torn-down ones aborted
        // here, and resolution then found fenced handles and fell back to
        // a replay that re-emitted the committed slices
        // (qual01-20260818a: 13,519 identical-value duplicates in one
        // window). Left pending, the transaction is finalised by the held
        // restart's resolution, or expires at transaction.timeout.ms if
        // its checkpoint genuinely never completed - a bounded
        // read_committed latency cost on teardown, never a correctness
        // cost. closed_ still makes any LATER on_commit refuse loudly: the
        // producer below is destroyed, so this process can no longer
        // execute the commit, and confirming it would be a lie the
        // resolver exists to avoid.
        inner_.close();
        pending_cv_.notify_all();
    }

    std::string name() const override { return "kafka_2pc_sink_string"; }

private:
    // Rows reach the producer as the channel's JSON text unless a value
    // format encodes them; suppression above reads the JSON, so encoding
    // happens here, at the last step before the producer.
    std::string encode_(std::string row) const {
        return encoder_ ? encoder_->from_json(row) : std::move(row);
    }

    void forward_(const Batch<std::string>& b) {
        Batch<KafkaMessage> out_b;
        for (const auto& r : b) {
            out_b.emplace(KafkaMessage{encode_(r.value())});
        }
        if (!out_b.empty()) {
            inner_.on_data(out_b);
        }
    }

    // After a commit or abort resolved the open interval: the buffered tail
    // enters the fresh transaction and normal streaming resumes.
    void resolve_open_() {
        open_txn_ckpt_.reset();
        awaiting_commit_.store(false, std::memory_order_release);
        if (!tail_.empty()) {
            Batch<KafkaMessage> out_b;
            for (auto& v : tail_) {
                out_b.emplace(KafkaMessage{encode_(std::move(v))});
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
        j += ",\"ckpt\":\"" + std::to_string(ckpt) + "\"";
        // The watermark horizon this transaction's records are covered by -
        // the exact value the commit receipt would carry. When a kill lands
        // in the ack window (committed, no receipt) and resolution proves
        // the commit over the wire, the walk materialises the missing
        // receipt FROM THIS FIELD, so a restore below this checkpoint still
        // arms replay suppression for the interval. Without it, a mixed
        // verdict replayed the committed slice as duplicates
        // (qual01-20260819f: one subtask's whole pane, twice).
        j += ",\"wm\":\"" + std::to_string(open_txn_wm_) + "\"}";
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

    // --- commit receipts + replay suppression -----------------------------
    //
    // The receipt (sub<K>-<N>, body "wm=<horizon>") is this subtask's
    // durable record that its external commit for checkpoint N executed.
    // Two readers: the coordinator's in-doubt resolution (a receipted
    // handle is COMMITTED with no wire call), and this class's own restore
    // path. When the restore point could NOT advance to a receipted
    // checkpoint (a sibling subtask's transaction was lost), the replay
    // re-produces intervals this subtask already published; suppression
    // swallows exactly those. The cut is the receipt's watermark horizon:
    // a fired pane is inside a committed transaction iff its event time is
    // at or below the watermark the sealing barrier saw - exact when the
    // feed's event times are watermark-monotone (windowed / aggregated
    // emissions), which is the premise the connector page documents for
    // the replay_suppression option.

    // True when the receipt is durable. Commit-path callers ignore the
    // answer (see the catch); the open's settle needs it.
    bool write_commit_receipt_(std::uint64_t ckpt, std::int64_t wm_horizon) {
        const auto* rt = this->runtime();
        if (!replay_suppression_ || rt == nullptr || rt->commit_receipt_dir().empty()) {
            return false;
        }
        try {
            const std::filesystem::path dir{rt->commit_receipt_dir()};
            std::filesystem::create_directories(dir);
            clink::state::detail::write_string_fsync_rename(
                dir / clink::connectors::commit_receipt_file_name(subtask_idx_, ckpt),
                "wm=" + std::to_string(wm_horizon) + "\n");
            return true;
        } catch (const std::exception& e) {
            // Best-effort by design: the commit DID execute, and confirming
            // it stays truthful. Losing the receipt only widens this
            // recovery's fallback to the bounded-replay contract - but
            // never silently.
            if (this->runtime() != nullptr) {
                this->runtime()->log_error(
                    std::string{"kafka_2pc_sink_string: commit receipt for checkpoint "} +
                    std::to_string(ckpt) + " could not be written (" + e.what() +
                    "); a recovery crossing this checkpoint falls back to bounded replay");
            }
        }
        return false;
    }

    // Called from open() BEFORE the transactional producer opens. When the
    // coordinator's in-doubt resolution could not settle this subtask's
    // prepared transaction (broker outage mid-walk, walk cancelled), it
    // leaves a sub<K>-<N>.unresolved marker next to the receipts, carrying
    // the staged handle. This is the LAST moment the orphan's fate can be
    // decided and known: nothing has re-initialised the producer yet, so the
    // broker's transaction coordinator still names it. init_transactions
    // would fence the identity, and after it no request reports how the
    // orphan ended.
    //
    // The open decides the orphan itself (txn_resume.hpp, settle_orphan).
    // A commit the broker names gets its receipt written here, durably,
    // before the marker goes and before replay suppression arms from it. An
    // undecided orphan is ended with EndTxn(abort) under the identity the
    // describe just reported, so the restore's replay is the interval's only
    // copy. Leaving it for the init to abort is not enough: the worker that
    // sealed it may have been declared lost while its process still runs,
    // with the orphan's CommitCheckpoint on its wire, and its commit can land
    // between a describe and the init (formal/ExactlyOnce.tla, mutant
    // orphan_left_to_init). The broker takes the abort or that commit first;
    // an abort refused because the commit won is followed by a describe that
    // proves the commit. An unreachable broker is a refusal to fence blind:
    // throw, let the restart cycle retry, converge when the broker returns.
    // Order: settle, receipt (only when committed), retire the marker, then
    // init_transactions in open(), then arm. A crash before the marker is
    // retired leaves it, and the next open settles again; that is idempotent,
    // since no init has run since the marker was written.
    void resolve_unresolved_orphan_() {
        settled_verdict_ = nullptr;
        const auto above = receipts_above_();
        if (!above.has_value()) {
            return;
        }
        const auto* rt = this->runtime();
        const std::string prefix = "sub" + std::to_string(subtask_idx_) + "-";
        const std::string suffix = ".unresolved";
        std::uint64_t marker_ckpt = 0;
        std::error_code ec;
        std::filesystem::directory_iterator it{rt->commit_receipt_dir(), ec};
        if (ec) {
            return;
        }
        for (const auto& ent : it) {
            const auto name = ent.path().filename().string();
            if (name.rfind(prefix, 0) != 0 || name.size() <= suffix.size() ||
                name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) {
                continue;
            }
            const auto id = std::strtoull(name.c_str() + prefix.size(), nullptr, 10);
            if (id > *above && id > marker_ckpt) {
                marker_ckpt = id;
            }
        }
        if (marker_ckpt == 0) {
            return;
        }
        const auto marker_path =
            std::filesystem::path{rt->commit_receipt_dir()} /
            (clink::connectors::commit_receipt_file_name(subtask_idx_, marker_ckpt) + suffix);
        const auto receipt_path =
            std::filesystem::path{rt->commit_receipt_dir()} /
            clink::connectors::commit_receipt_file_name(subtask_idx_, marker_ckpt);
        const auto retire = [&] {
            std::error_code rec;
            std::filesystem::remove(marker_path, rec);
        };
        const auto has_receipt = [&] {
            std::error_code rec;
            return std::filesystem::exists(receipt_path, rec);
        };
        // A receipt for the marker's checkpoint outranks it (the walk, a
        // prior incarnation, or the old incarnation itself resolved it after
        // the marker was written).
        if (has_receipt()) {
            retire();
            settled_orphan_ckpt_ = marker_ckpt;
            settled_verdict_ = "receipt";
            return;
        }
        std::string handle;
        {
            std::ifstream in{marker_path};
            std::stringstream ss;
            ss << in.rdbuf();
            handle = ss.str();
        }
        std::int64_t handle_pid = -1;
        std::int16_t handle_epoch = -1;
        std::string wm;
        std::string handle_txn_id;
        try {
            const auto j = clink::config::parse(handle);
            handle_txn_id = j.at("transactional_id").as_string();
            handle_pid = std::stoll(j.at("producer_id").as_string());
            handle_epoch = static_cast<std::int16_t>(std::stoi(j.at("producer_epoch").as_string()));
            if (j.contains("wm")) {
                wm = j.at("wm").as_string();
            }
        } catch (const std::exception& e) {
            this->runtime()->log_error(
                std::string{"kafka_2pc_sink_string: unresolved orphan marker for checkpoint "} +
                std::to_string(marker_ckpt) + " did not parse (" + e.what() +
                "); proceeding under the bounded-replay contract");
            retire();
            return;
        }
        if (handle_txn_id != transactional_id_ || handle_pid < 0 || handle_epoch < 0) {
            this->runtime()->log_error(
                std::string{"kafka_2pc_sink_string: unresolved orphan marker for checkpoint "} +
                std::to_string(marker_ckpt) + " names '" + handle_txn_id + "' pid=" +
                std::to_string(handle_pid) + " which is not this sink's transaction lineage ('" +
                transactional_id_ + "'); ignoring it under the bounded-replay contract");
            retire();
            return;
        }
        const auto dialer = build_resume_dialer(resume_names_);
        if (!dialer.error.empty()) {
            // Cannot ask AND must not fence blind: fail the open loudly.
            throw std::runtime_error("kafka_2pc_sink_string: unresolved orphan for checkpoint " +
                                     std::to_string(marker_ckpt) +
                                     " cannot be settled: " + dialer.error);
        }
        // Bounded both ways: by attempts (a fast-refusing dead port must not
        // spin) and by wall clock (a raw blocking connect against a black
        // hole can eat the OS connect timeout per attempt). Either bound
        // expiring means throw below - the restart cycle is the outer retry.
        // The 90s must stay BELOW the coordinator's restart drain deadline
        // (120s): a restart cancel arriving mid-settle is only observed
        // when this loop exits, and a drain deadline that undercuts it
        // reads the sink as a wedged survivor and fails the whole job
        // (exactly how the orphaned-commit gate first died).
        constexpr int kSettleAttempts = 6;
        const auto settle_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{90};
        clink::kafka::TxnIdentity staged{.transactional_id = transactional_id_,
                                         .producer_id = handle_pid,
                                         .producer_epoch = handle_epoch};
        std::optional<clink::kafka::OrphanSettlement> verdict;
        std::string last_detail;
        for (int attempt = 0; attempt < kSettleAttempts && !verdict.has_value() &&
                              std::chrono::steady_clock::now() < settle_deadline;
             ++attempt) {
            for (const auto& [host, port] : parse_bootstrap_entries(brokers_)) {
                auto settled = clink::kafka::settle_orphan(
                    host, port, staged, dialer.connect, dialer.auth, [] {
                        CLINK_FAULT_POINT(clink::fault::points::kSinkOrphanDescribed);
                    });
                if (settled.status != clink::kafka::OrphanSettlement::Status::TransportError) {
                    verdict = std::move(settled);
                    break;
                }
                last_detail = settled.detail;
                // The epoch a call saw Ongoing anchors the next call's proof.
                staged.producer_epoch = settled.floor_epoch;
            }
        }
        if (!verdict.has_value()) {
            // Fencing now would abort an undecided transaction OR erase the
            // evidence of a committed one - and pick which blindly. The
            // restart cycle retries this open; it converges when the broker
            // answers.
            throw std::runtime_error(
                "kafka_2pc_sink_string: checkpoint " + std::to_string(marker_ckpt) +
                " left an unresolved prepared transaction and no broker settled it (" +
                last_detail +
                "); refusing to fence blind (a blind fence turns \"committed in the ack "
                "window\" into replayed duplicates)");
        }
        const std::string seen = " (" + verdict->state +
                                 " pid=" + std::to_string(verdict->producer_id) +
                                 " epoch=" + std::to_string(verdict->producer_epoch) + ")";
        using Status = clink::kafka::OrphanSettlement::Status;
        if (verdict->status == Status::Committed) {
            if (wm.empty()) {
                this->runtime()->log_error(
                    std::string{"kafka_2pc_sink_string: orphaned transaction for checkpoint "} +
                    std::to_string(marker_ckpt) + " proved committed" + seen +
                    " but its handle carries no watermark horizon (older binary); no receipt "
                    "can be written and the replay of its interval will NOT be suppressed");
                retire();
                return;
            }
            // The receipt must be durable before the marker goes: a marker
            // retired over a lost receipt would leave the replay unsuppressed
            // for an interval the broker published. Throwing keeps the
            // marker, and the next open settles again and finds the commit.
            if (!write_commit_receipt_(marker_ckpt, std::strtoll(wm.c_str(), nullptr, 10))) {
                throw std::runtime_error(
                    "kafka_2pc_sink_string: orphaned transaction for checkpoint " +
                    std::to_string(marker_ckpt) +
                    " proved committed but its receipt could not be written; the marker stays "
                    "and the next open settles it again");
            }
            retire();
            settled_orphan_ckpt_ = marker_ckpt;
            settled_verdict_ = "committed";
            this->runtime()->log_info(
                std::string{"kafka_2pc_sink_string: orphaned transaction for checkpoint "} +
                std::to_string(marker_ckpt) + " resolved COMMITTED before fencing" + seen +
                "; receipt written, replay suppression arms from it");
            return;
        }
        retire();
        settled_orphan_ckpt_ = marker_ckpt;
        // A receipt that appeared while the settle ran answers for the
        // orphan: the old incarnation committed it and wrote the receipt
        // before the describe, which then saw whatever followed on that
        // producer (a successor transaction this open may have ended).
        settled_verdict_ = has_receipt()                        ? "receipt"
                           : verdict->status == Status::Aborted ? "aborted"
                                                                : "not_committed";
        this->runtime()->log_info(
            std::string{"kafka_2pc_sink_string: orphaned transaction for checkpoint "} +
            std::to_string(marker_ckpt) + " resolved " + settled_verdict_ + " before fencing" +
            seen + ": " + verdict->detail +
            (std::string_view{settled_verdict_} == "receipt"
                 ? "; replay suppression arms from its receipt"
                 : "; the restore's replay is the interval's only copy"));
    }

    // The id above which this run's receipts and unresolved markers lie, for
    // the open: max(restore point, run floor). A fresh start's floor sits above
    // everything an earlier run of this job left in the directory, so it
    // (restore point 0, nothing of its own above its floor yet) reads none of
    // it and owes the user its full output. A run that restores from a
    // checkpoint continues the run that took it and reads that run's receipts
    // above the restore point, output already published. A restart that
    // restores from nothing (no checkpoint of this run confirmed yet) still
    // reads this run's receipts and markers. nullopt when
    // the open reads neither: receipts off, or a deploy that carried no floor
    // (an older coordinator, a takeover of a job persisted without one) and
    // restores from nothing, which this sink takes for a fresh start, as it
    // did before the floor existed.
    std::optional<std::uint64_t> receipts_above_() const {
        const auto* rt = this->runtime();
        if (!replay_suppression_ || rt == nullptr || rt->commit_receipt_dir().empty()) {
            return std::nullopt;
        }
        const auto restore_from = rt->restore_from_checkpoint_id();
        const auto floor = rt->run_checkpoint_floor();
        if (!floor.has_value()) {
            if (restore_from == 0) {
                return std::nullopt;
            }
            return restore_from;
        }
        return std::max(restore_from, *floor);
    }

    // Called from open() (under mu_): arm suppression from this run's receipts
    // newer than the checkpoint it restored from (receipts_above_).
    void arm_replay_suppression_() {
        const auto above = receipts_above_();
        if (!above.has_value()) {
            return;
        }
        const auto* rt = this->runtime();
        const auto restore_from = rt->restore_from_checkpoint_id();
        const std::string prefix = "sub" + std::to_string(subtask_idx_) + "-";
        std::uint64_t best_ckpt = 0;
        std::error_code ec;
        std::filesystem::directory_iterator it{rt->commit_receipt_dir(), ec};
        if (ec) {
            return;  // no receipts directory: nothing was ever committed here
        }
        for (const auto& ent : it) {
            const auto name = ent.path().filename().string();
            if (name.rfind(prefix, 0) != 0) {
                continue;
            }
            const auto id = std::strtoull(name.c_str() + prefix.size(), nullptr, 10);
            if (id > *above && id > best_ckpt) {
                best_ckpt = id;
            }
        }
        if (best_ckpt == 0) {
            return;
        }
        std::ifstream in{std::filesystem::path{rt->commit_receipt_dir()} /
                         clink::connectors::commit_receipt_file_name(subtask_idx_, best_ckpt)};
        std::string line;
        if (!in.is_open() || !std::getline(in, line) || line.rfind("wm=", 0) != 0) {
            this->runtime()->log_warn(
                std::string{"kafka_2pc_sink_string: unreadable commit receipt for checkpoint "} +
                std::to_string(best_ckpt) + "; replay suppression stays off (bounded replay)");
            return;
        }
        const auto horizon = std::strtoll(line.c_str() + 3, nullptr, 10);
        if (horizon == EventTime::min().millis()) {
            return;  // no watermark had been seen by that barrier: nothing to cut on
        }
        suppress_armed_ = true;
        suppress_horizon_ = horizon;
        suppress_source_ckpt_ = best_ckpt;
        this->runtime()->log_info(
            std::string{"kafka_2pc_sink_string: replay suppression armed from receipt for "
                        "checkpoint "} +
            std::to_string(best_ckpt) + " (restored from " + std::to_string(restore_from) +
            "): re-emissions with event time <= " + std::to_string(horizon) +
            " are already published and will be swallowed");
    }

    // True = swallow. Only called while armed, under mu_.
    bool suppress_record_(const Record<std::string>& r) {
        if (!r.event_time().has_value()) {
            if (!warned_ts_less_) {
                warned_ts_less_ = true;
                if (this->runtime() != nullptr) {
                    this->runtime()->log_error(
                        "kafka_2pc_sink_string: replay suppression is armed but a record "
                        "carries no event time, so it cannot be matched against the committed "
                        "horizon; passing it through (bounded-replay contract). Feed this sink "
                        "watermark-monotone, event-timed records - windowed or aggregated "
                        "emissions - or set replay_suppression='false'.");
                }
            }
            return false;
        }
        if (r.event_time()->millis() <= suppress_horizon_) {
            ++suppressed_records_;
            return true;
        }
        return false;
    }

    void disarm_suppression_() {
        suppress_armed_ = false;
        if (this->runtime() != nullptr) {
            this->runtime()->log_info(
                std::string{"kafka_2pc_sink_string: replay suppression retired at watermark "} +
                std::to_string(cur_wm_) + ": swallowed " + std::to_string(suppressed_records_) +
                " already-published record(s) from the replay of checkpoints <= " +
                std::to_string(suppress_source_ckpt_));
        }
    }

    std::string brokers_;
    std::string transactional_id_;
    std::uint32_t subtask_idx_{0};
    bool replay_suppression_{true};
    // How the settle's TLS dialer checks broker names: as the producer does.
    ResumeNameCheck resume_names_;
    KafkaSink inner_;
    ValueEncoderPtr encoder_;
    std::mutex mu_;
    std::condition_variable pending_cv_;
    std::optional<std::uint64_t> open_txn_ckpt_;
    // open_txn_ckpt_.has_value(), for staged_commits_outstanding().
    std::atomic<bool> awaiting_commit_{false};
    std::vector<std::string> tail_;
    // Highest non-idle watermark seen, and its value when the open
    // transaction's barrier sealed it (the receipt's horizon).
    std::int64_t cur_wm_{EventTime::min().millis()};
    std::int64_t open_txn_wm_{EventTime::min().millis()};
    // Replay suppression (armed at open() from receipts newer than the
    // restore point; retired once the replay's watermark passes the horizon).
    bool suppress_armed_{false};
    std::int64_t suppress_horizon_{EventTime::min().millis()};
    std::uint64_t suppress_source_ckpt_{0};
    std::uint64_t suppressed_records_{0};
    bool warned_ts_less_{false};
    // Highest checkpoint whose commit this instance EXECUTED. A re-delivered
    // commit at or below it is idempotent; anything else that misses the
    // open transaction is a commit that did not happen and must throw.
    std::uint64_t last_committed_ckpt_{0};
    // Set by close(). A commit arriving after teardown aborted the prepared
    // transaction must refuse rather than read as executed.
    bool closed_{false};
    // What this open's settle decided about an unresolved orphan above the
    // restore point, for the SinkOpens trace line: the marker's checkpoint and
    // "receipt", "committed", "aborted" or "not_committed". Null when no
    // marker was found, or when the open could not act on the one it found
    // (unparseable, another lineage, a proven commit with no horizon).
    std::uint64_t settled_orphan_ckpt_{0};
    const char* settled_verdict_{nullptr};
};

// Upsert-shaped Kafka sink. Takes JSON rows (each row is
// a complete JSON object) and emits Kafka records keyed by the
// configured primary_key columns. Rows tagged with
// `__row_kind == "delete"` are emitted with a NULL value, the
// log-compaction tombstone. Inserts (default kind) emit
// the row JSON minus the privileged `__row_kind` field as the
// payload. Watermarks / barriers / flush / close pass through to
// the inner KafkaSink unchanged.
class UpsertKafkaSink final : public Sink<std::string> {
public:
    UpsertKafkaSink(KafkaSink::Options opts,
                    std::vector<std::string> primary_key,
                    ValueEncoderPtr encoder = nullptr)
        : inner_(tombstoning_(std::move(opts))),
          primary_key_(std::move(primary_key)),
          encoder_(std::move(encoder)) {
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
    // The inner sink's barrier flush is this sink's guarantee: see KafkaSink.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override {
        return inner_.gates_checkpoint_ack();
    }
    void flush() override { inner_.flush(); }
    void close() override { inner_.close(); }
    std::string name() const override { return "kafka_upsert_sink_string"; }

private:
    static KafkaSink::Options tombstoning_(KafkaSink::Options opts) {
        opts.empty_payload_is_tombstone = true;
        return opts;
    }

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
        //   delete         -> tombstone (no payload, produced as a NULL value)
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
        // The key stays the JSON-derived primary key (compaction keys on it);
        // only the value takes the registry framing.
        if (encoder_) {
            payload = encoder_->from_json(payload);
        }
        return KafkaMessage{std::move(payload), std::move(key)};
    }

    KafkaSink inner_;
    std::vector<std::string> primary_key_;
    ValueEncoderPtr encoder_;
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
    // Dialer + SASL from the resolving process's environment (see
    // build_resume_dialer), the broker name check included, which this
    // process cannot take from the sink's options: transport security and
    // credentials are never the staged handle's to carry. TLS requested but
    // unusable is refused LOUDLY; silently dialing plaintext would leak the
    // SASL password the operator configured TLS to protect.
    const auto dialer = build_resume_dialer();
    if (!dialer.error.empty()) {
        return {false, dialer.error};
    }
    std::string last;
    for (const auto& [host, port] : parse_bootstrap_entries(bootstrap)) {
        const auto outcome =
            clink::kafka::resume_commit(host, port, txn, dialer.connect, dialer.auth);
        if (outcome.status != clink::kafka::ResumeOutcome::Status::TransportError) {
            return {outcome.committed(), outcome.detail};
        }
        last = outcome.detail;
    }
    // Transport-only failure: no broker gave a verdict. Marked so the
    // resolution walk retries in place instead of treating unreachability
    // as "not committed" - which, after earlier handles of the same
    // checkpoint already committed, would replay their intervals.
    return {.committed = false,
            .detail = "no bootstrap broker reachable: " + last,
            .transport_inconclusive = true};
}

std::shared_ptr<Source<std::string>> build_string_source(const BuildContext& ctx) {
    KafkaSource::Options opts;
    opts.brokers = ctx.param_or("brokers");
    opts.topic = ctx.param_or("topic");
    opts.group_id = ctx.param_or("group_id", "clink");
    opts.client_id = ctx.param_or("client_id", "clink-source");
    opts.auto_offset_reset = ctx.param_or("auto_offset_reset", "earliest");
    apply_batch_max_wait(ctx, opts);
    apply_batch_shape(ctx, opts);
    populate_kafka_security_conf(ctx, opts.conf);
    apply_deterministic_ownership(ctx, opts);
    if (opts.brokers.empty()) {
        throw std::runtime_error("kafka source: 'brokers' is required");
    }
    if (opts.topic.empty()) {
        throw std::runtime_error("kafka source: 'topic' is required");
    }
    auto decoder = build_value_decoder(ctx, "kafka source");
    const bool skip = skip_undecodable(ctx, "kafka source");
    return std::make_shared<StringKafkaSource>(std::move(opts), std::move(decoder), skip);
}

std::shared_ptr<Sink<std::string>> build_string_sink(const BuildContext& ctx) {
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
    auto encoder = build_value_encoder(ctx, "kafka sink");
    return std::make_shared<StringKafkaSink>(std::move(opts), std::move(encoder));
}

std::vector<std::string> string_channel_formats() {
    std::vector<std::string> formats{"text", "json", "bytes"};
#ifdef CLINK_HAS_SCHEMA_REGISTRY
    for (auto& f : clink::schema_registry::supported_format_names()) {
        formats.push_back(std::move(f));
    }
#endif
    return formats;
}

void install(clink::plugin::PluginRegistry& reg) {
    const auto formats = string_channel_formats();
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
        .formats = formats,
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
                        "transactional output",
                        "at-least-once needs acks=all, the default: with acks=1 or 0 a record "
                        "the brokers acknowledged can still be lost",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
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
        .formats = formats,
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
                        "the bounded contract",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
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
            apply_deterministic_ownership(ctx, opts);
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
    auto text_source_builder = [](const BuildContext& ctx) { return build_string_source(ctx); };
    reg.register_source<std::string>("kafka_text_source", text_source_builder);
    reg.register_source<std::string>("kafka_source_string", text_source_builder);

    auto text_sink_builder = [](const BuildContext& ctx) { return build_string_sink(ctx); };
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
            // transaction_timeout_ms -> librdkafka transaction.timeout.ms:
            // how long the broker keeps an abandoned prepared transaction
            // before aborting it. Declared in this connector's capability
            // metadata since the committer wave but consumed only now; the
            // mixed-verdict recovery gate needs a short expiry to model an
            // orphaned sibling transaction without waiting the 60s default.
            // message.timeout.ms rides along: librdkafka requires it <= the
            // transaction timeout, and in a transactional producer a
            // message cannot meaningfully outlive its transaction anyway.
            if (const auto t = ctx.param_or("transaction_timeout_ms", ""); !t.empty()) {
                opts.conf["transaction.timeout.ms"] = t;
                opts.conf["message.timeout.ms"] = t;
            }
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
            // replay_suppression (default on): swallow re-emissions of
            // intervals this subtask's receipts prove are already published
            // when a recovery could not advance the restore point past them.
            // Exact for watermark-monotone feeds (windowed / aggregated
            // emissions); a feed that delivers records at or below the
            // current watermark should set 'false' (see the connector page).
            const bool replay_suppression = ctx.param_or("replay_suppression", "true") != "false";
            auto encoder = build_value_encoder(ctx, "kafka_2pc_sink_string");
            auto sink = std::make_shared<TwoPhaseCommitStringKafkaSink>(
                std::move(opts), ctx.subtask_idx, replay_suppression, std::move(encoder));
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
            auto encoder = build_value_encoder(ctx, "kafka_upsert_sink_string");
            return std::make_shared<UpsertKafkaSink>(
                std::move(opts), std::move(pk), std::move(encoder));
        });
}

}  // namespace clink::kafka
