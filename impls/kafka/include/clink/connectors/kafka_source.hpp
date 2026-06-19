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
        std::size_t max_batch_size = 256;
        CommitMode commit_mode = CommitMode::Auto;
        // When true, librdkafka's debug log channel is enabled - verbose
        // but useful when triaging connection issues.
        bool enable_debug = false;
        // Optional name for the metrics counter prefix. Counters created:
        //   kafka_source.<metric_prefix>.consumed
        //   kafka_source.<metric_prefix>.consume_errors
        // Empty disables the metric registration.
        std::string metric_prefix = "default";
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

    std::string name() const override { return "kafka_source"; }

    static bool is_real_implementation();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
