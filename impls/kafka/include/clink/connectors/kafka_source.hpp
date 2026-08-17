#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>

#include "clink/connectors/kafka_message.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

// Concrete Source<KafkaMessage>. Consumes from a Kafka topic and emits
// fully-populated records (payload + key + headers + offset/partition/
// timestamp). Users that only care about payloads insert a downstream
// MapOperator<KafkaMessage, std::string>.
//
// Implementation lives in src/connectors/kafka_source.cpp. When CMake
// finds librdkafka, the .cpp is compiled with `CLINK_HAS_KAFKA` and
// links the real client. Without that, the .cpp builds a stub that
// throws on construction. ABI is stable either way.
//
// Threading: produce() must be called from a single thread (the operator
// runner). cancel() is safe to call from any thread.
class KafkaSource final : public Source<KafkaMessage> {
public:
    enum class CommitMode {
        // librdkafka commits offsets periodically based on
        // `auto.commit.interval.ms`. Simplest, at-most-once on crash
        // (offsets may have advanced past records that weren't fully
        // processed downstream).
        Auto,
        // Caller is responsible for invoking commit_current() at safe
        // points. Pairs with checkpoint barriers in clink.
        Manual,
    };

    struct Options {
        std::string brokers;  // e.g. "localhost:9092"
        std::string topic;
        std::string group_id = "clink";
        std::string client_id = "clink-source";
        // "earliest" | "latest" | "none"
        std::string auto_offset_reset = "earliest";
        std::chrono::milliseconds poll_timeout = std::chrono::milliseconds{100};
        // Records per Batch<KafkaMessage> handed downstream. 2048, not the 256
        // this was: 256 records of a ~124-byte nexmark bid is about 32 KB, chosen
        // as a network-buffer analogue, and on a saturated consumer it costs 4.6x
        // the throughput.
        //
        // Measured on nexmark q0, 22,080,000 records, one freshly composed stack
        // per variant, each drain over a multi-second window:
        //
        //   max_batch_size  batch_max_wait   drain      ev/CPU-s   anon
        //   256 (was)       5ms              1.16M/s      632k     246 MB
        //   2048 (now)      5ms              5.33M/s     1082k     184 MB
        //   2048            0                3.14M/s     1037k     181 MB
        //
        // Note the third row: REMOVING batch_max_wait is worse than keeping it at
        // the same batch size. The bound is not a throughput tax that truncates
        // batches - it is a stall guard. With no bound the fill loop blocks
        // whenever the local queue is momentarily short, and that wait costs more
        // than the smaller batch would have. So the default raises the SIZE and
        // leaves the bound alone, which also means per-record latency on a
        // trickling input is unchanged: the bound still fires first when records
        // are sparse, exactly as before.
        //
        // Larger batches also used LESS memory here, so the obvious objection does
        // not hold. Measured on ARM; the x86 rig has to confirm the magnitude.
        std::size_t max_batch_size = 2048;
        // Bounds TOTAL batch formation time in produce(). Waiting for the
        // FIRST record of a batch still blocks up to poll_timeout (idle
        // stays cheap); once a batch has begun, the fill loop stops when
        // this bound elapses, emitting a partial batch instead of waiting
        // to reach max_batch_size. Keeps per-record latency on a paced or
        // trickling input proportional to this bound rather than
        // max_batch_size / input-rate, and costs a saturated consumer
        // nothing (a full local queue fills max_batch_size well inside
        // the bound). 0 disables the bound (fill until max_batch_size or
        // a poll_timeout-quiet break).
        std::chrono::milliseconds batch_max_wait = std::chrono::milliseconds{5};
        CommitMode commit_mode = CommitMode::Auto;
        // When true, librdkafka's debug log channel is enabled - verbose
        // but useful when triaging connection issues.
        bool enable_debug = false;
        // Optional name for the metrics counter prefix. Counters created:
        //   kafka_source.<metric_prefix>.consumed
        //   kafka_source.<metric_prefix>.consume_errors
        // Empty disables the metric registration.
        std::string metric_prefix = "default";
        // Extra librdkafka config properties applied verbatim after the fields
        // above, e.g. {"security.protocol":"sasl_ssl", "sasl.mechanism":"PLAIN",
        // "sasl.username":"u", "sasl.password":"p", "ssl.ca.location":"/ca.pem"}.
        // The factory populates these from the SASL/SSL WITH-options.
        std::map<std::string, std::string> conf;
    };

    explicit KafkaSource(Options opts);
    ~KafkaSource() override;

    KafkaSource(const KafkaSource&) = delete;
    KafkaSource& operator=(const KafkaSource&) = delete;
    KafkaSource(KafkaSource&&) = delete;
    KafkaSource& operator=(KafkaSource&&) = delete;

    void open() override;
    bool produce(Emitter<KafkaMessage>& out) override;
    void cancel() override;
    void close() override;

    // A Kafka topic is an endless stream: unbounded by nature, so it never
    // triggers the end-of-input drain or the batch execution path (BATCH-1).
    // This matches the Source default; stated explicitly for the contract.
    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    // Manually commit the current consumer offsets. Only meaningful
    // when commit_mode == Manual. Returns true on success.
    bool commit_current();

    // Source replay: bind the consumer position to clink checkpoints rather
    // than Kafka's own committed offset. snapshot_offset persists the
    // per-partition next-offset map captured so far; restore_offset loads it
    // and the consumer seeks each partition there on assignment (via a
    // rebalance callback), making the clink checkpoint the source of truth on
    // recovery. Runs on the source-runner thread between produce() calls.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override;
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;

    // Serialization of the per-partition offset map (partition -> next offset
    // to read) used in the checkpoint slot. Exposed (and broker-independent)
    // so the encoding can be unit-tested without a Kafka client.
    static std::string encode_offsets(const std::map<std::int32_t, std::int64_t>& offsets);
    static std::map<std::int32_t, std::int64_t> decode_offsets(std::string_view bytes);

    // Stable Kafka group identity for one parallel source subtask. Kafka's
    // static-membership protocol maps this identity back to the same member
    // across a process or coordinator recovery, preserving partition
    // ownership so a subtask's checkpointed offset map remains authoritative.
    // Pure and broker-independent for unit testing.
    static std::string stable_group_instance_id(std::string_view topic, std::uint32_t subtask_idx);

    std::string name() const override { return "kafka_source"; }

    static bool is_real_implementation();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
