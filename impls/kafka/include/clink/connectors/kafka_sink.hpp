#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>

#include "clink/connectors/kafka_message.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

// Concrete Sink<KafkaMessage>. Produces to a Kafka topic with full
// fidelity (payload + optional key + headers + per-record partition
// override).
//
// Implementation: src/connectors/kafka_sink.cpp. CLINK_HAS_KAFKA gated.
//
// Delivery accounting:
//   * On every successful broker ack, `delivered` counter increments.
//   * On every delivery failure (post-retry), `delivery_errors` counter
//     increments and the last error message is captured (recoverable
//     via last_error()).
//
// Backpressure:
//   * If the local producer queue is full, on_data() polls and retries
//     until the queue drains or `produce_timeout` elapses, then throws.
//     Throwing surfaces via the LocalExecutor's per-operator error
//     capture, so the operator runner exits cleanly without taking the
//     process down.
class KafkaSink final : public Sink<KafkaMessage> {
public:
    struct Options {
        std::string brokers;
        std::string topic;
        std::string client_id = "clink-sink";

        // Acks mode: "all" (default, durable), "1", "0".
        std::string acks = "all";

        // Compression: "none" | "gzip" | "snappy" | "lz4" | "zstd".
        std::string compression_type = "none";

        // Producer batching knob - pass-through to librdkafka.
        std::chrono::milliseconds linger_ms = std::chrono::milliseconds{5};

        // When the local librdkafka queue is full (back-pressure from a
        // slow broker or downed cluster), on_data() will retry up to
        // this long before throwing.
        std::chrono::milliseconds produce_timeout = std::chrono::milliseconds{30000};

        // Blocking flush deadline used by flush() and close().
        std::chrono::milliseconds flush_timeout = std::chrono::milliseconds{30000};

        // Optional fixed partition. If set AND the per-record
        // KafkaMessage.partition is < 0, all records land here. Per-record
        // partition (>= 0) always wins.
        std::optional<std::int32_t> fixed_partition{};

        // Counter prefix in the metrics registry:
        //   kafka_sink.<metric_prefix>.delivered
        //   kafka_sink.<metric_prefix>.delivery_errors
        //   kafka_sink.<metric_prefix>.queue_full_retries
        // Empty disables registration.
        std::string metric_prefix = "default";

        // When non-empty, the producer is configured with
        // `transactional.id` + `enable.idempotence=true`, and open()
        // runs init_transactions() + begin_transaction() so that
        // every produced record lands inside the current transaction.
        // The 2PC adapter calls commit_transaction() / abort_transaction()
        // through the new public methods below.
        std::string transactional_id;

        // Extra librdkafka config properties applied verbatim after the fields
        // above, e.g. {"security.protocol":"sasl_ssl", "sasl.mechanism":"PLAIN",
        // "sasl.username":"u", "sasl.password":"p", "ssl.ca.location":"/ca.pem"}.
        // The factory populates these from the SASL/SSL WITH-options.
        std::map<std::string, std::string> conf;
    };

    explicit KafkaSink(Options opts);
    ~KafkaSink() override;

    KafkaSink(const KafkaSink&) = delete;
    KafkaSink& operator=(const KafkaSink&) = delete;
    KafkaSink(KafkaSink&&) = delete;
    KafkaSink& operator=(KafkaSink&&) = delete;

    void open() override;
    void on_data(const Batch<KafkaMessage>& batch) override;
    void flush() override;
    void close() override;

    std::string name() const override { return "kafka_sink"; }

    // Read accessors for delivery accounting. Useful in tests and for
    // monitoring without going through MetricsRegistry.
    std::uint64_t delivered_count() const noexcept;
    std::uint64_t delivery_error_count() const noexcept;
    // Last delivery error message; empty if none seen.
    std::string last_error() const;

    // Transactional API. Callable only when Options.
    // transactional_id was non-empty. commit_transaction() flushes
    // in-flight records, then commits the current transaction and
    // begins a new one. abort_transaction() rolls back AND likewise
    // begins a new transaction (best-effort) so the sink stays ready
    // for the next checkpoint - without it the post-abort on_data()
    // would have no open transaction to produce into. Both are no-ops
    // when transactions weren't enabled.
    void commit_transaction();
    void abort_transaction();

    static bool is_real_implementation();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
