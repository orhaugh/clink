// Canonical Kafka pipeline for the clink-vs-Flink benchmark harness.
//
// Pipeline (identical semantics to the Flink job in
// ../flink-job/src/main/java/com/clink/bench/CanonicalPipeline.java):
//
//   Kafka(in, 24-byte records: { ts_ms, key, value } little-endian)
//     -> decode bytes -> InRecord
//     -> assign_timestamps_monotonic(ts_ms)
//     -> key_by(key)
//     -> tumbling_window(1000 ms event-time)
//     -> aggregate<int64_t, OutRecord>(
//          sum init/combine,
//          (key, window, sum) -> OutRecord{window.end, key, sum})
//     -> encode 24 bytes (window_end, key, sum)
//     -> Kafka(out)
//
// The emit-form aggregate exposes the key and window-end at emit time
// (see KeyedTumblingWindowEmitOperator) so both engines carry the full
// (window_end, key, sum) triple on the wire.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "clink/api/stream_execution_environment.hpp"
#include "clink/connectors/kafka_message.hpp"
#include "clink/core/codec.hpp"
#include "clink/job/register_job.hpp"
#include "clink/kafka/install.hpp"
#include "clink/kafka/typed_sink.hpp"
#include "clink/kafka/typed_source.hpp"
#include "clink/time/event_time.hpp"

namespace bench {

struct InRecord {
    std::int64_t ts_ms{0};
    std::int64_t key{0};
    std::int64_t value{0};
};

struct OutRecord {
    std::int64_t window_end_ms{0};
    std::int64_t key{0};
    std::int64_t sum_value{0};
};

inline void put_i64_le(std::vector<std::byte>& out, std::int64_t v) {
    const auto u = static_cast<std::uint64_t>(v);
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
    }
}

inline std::int64_t read_i64_le(std::span<const std::byte> b, std::size_t pos) {
    std::uint64_t u = 0;
    for (int i = 0; i < 8; ++i) {
        u |= static_cast<std::uint64_t>(static_cast<unsigned char>(b[pos + i])) << (i * 8);
    }
    return static_cast<std::int64_t>(u);
}

inline clink::Codec<InRecord> in_codec() {
    return clink::Codec<InRecord>{
        .encode =
            [](const InRecord& r) {
                std::vector<std::byte> out;
                out.reserve(24);
                put_i64_le(out, r.ts_ms);
                put_i64_le(out, r.key);
                put_i64_le(out, r.value);
                return out;
            },
        .decode = [](std::span<const std::byte> b) -> std::optional<InRecord> {
            if (b.size() < 24) {
                return std::nullopt;
            }
            InRecord r;
            r.ts_ms = read_i64_le(b, 0);
            r.key = read_i64_le(b, 8);
            r.value = read_i64_le(b, 16);
            return r;
        }};
}

inline clink::Codec<OutRecord> out_codec() {
    return clink::Codec<OutRecord>{
        .encode =
            [](const OutRecord& r) {
                std::vector<std::byte> out;
                out.reserve(24);
                put_i64_le(out, r.window_end_ms);
                put_i64_le(out, r.key);
                put_i64_le(out, r.sum_value);
                return out;
            },
        .decode = [](std::span<const std::byte> b) -> std::optional<OutRecord> {
            if (b.size() < 24) {
                return std::nullopt;
            }
            OutRecord r;
            r.window_end_ms = read_i64_le(b, 0);
            r.key = read_i64_le(b, 8);
            r.sum_value = read_i64_le(b, 16);
            return r;
        }};
}

inline std::string env_str(const char* name, const char* fallback) {
    if (const char* p = std::getenv(name); p != nullptr && *p != '\0') {
        return p;
    }
    return fallback;
}

inline std::int64_t env_i64(const char* name, std::int64_t fallback) {
    if (const char* p = std::getenv(name); p != nullptr && *p != '\0') {
        try {
            return std::stoll(p);
        } catch (...) {
        }
    }
    return fallback;
}

inline void define_job(clink::api::StreamExecutionEnvironment& env) {
    using namespace std::chrono_literals;

    // Register built-in channel types into THIS env's bundle. The
    // global ensure_built_ins_registered() writes to default-instance
    // singletons which env.registry() doesn't read from; kafka::install
    // below needs "string" and "int64" already in env.registry()
    // because it registers source/sink factories for std::string-typed
    // text Kafka I/O.
    env.registry().register_type<std::int64_t>("int64", clink::int64_codec());
    env.registry().register_type<std::string>("string", clink::string_codec());

    // Install the kafka impl's typed channel + factories into this
    // env's registry. Required so env.source<KafkaMessage>(...) can
    // look up the "kafka.message" channel and the kafka_message_*
    // source/sink factories during graph construction.
    clink::kafka::install(env.registry());

    env.registry().register_type<InRecord>("bench.in", in_codec());
    env.registry().register_type<OutRecord>("bench.out", out_codec());

    const auto brokers = env_str("BENCH_KAFKA_BROKERS", "kafka:29092");
    const auto in_topic = env_str("BENCH_INPUT_TOPIC", "bench-in");
    const auto out_topic = env_str("BENCH_OUTPUT_TOPIC", "bench-out");
    const auto group_id = env_str("BENCH_GROUP_ID", "clink-canonical");
    const auto par = env_i64("BENCH_PARALLELISM", 4);
    // Set the env-wide default parallelism so every operator the
    // fluent API builds below (source / flat_map / assign_timestamps /
    // window / sink) runs at `par` subtasks, matching Flink's
    // env.setParallelism().
    env.set_parallelism(static_cast<std::uint32_t>(par));

    clink::kafka::KafkaSourceOptions src_opts{
        .brokers = brokers,
        .topic = in_topic,
        .group_id = group_id,
        .client_id = "clink-bench",
        .auto_offset_reset = "earliest",
    };
    clink::kafka::KafkaSinkOptions sink_opts{
        .brokers = brokers,
        .topic = out_topic,
        .client_id = "clink-bench",
        .acks = "1",
    };

    auto kafka_in = clink::kafka::message_source(env, src_opts);

    auto records = kafka_in.flat_map<InRecord>([](const clink::KafkaMessage& m) {
        std::vector<InRecord> out;
        if (m.payload.size() < 24) {
            return out;
        }
        const auto* p = reinterpret_cast<const std::byte*>(m.payload.data());
        auto decoded = in_codec().decode(std::span<const std::byte>{p, m.payload.size()});
        if (decoded.has_value()) {
            out.push_back(*decoded);
        }
        return out;
    });

    auto outs = records
                    .assign_timestamps_monotonic(
                        [](const InRecord& r) { return clink::EventTime{r.ts_ms}; })
                    .key_by([](const InRecord& r) { return r.key; })
                    .tumbling_window(1000ms)
                    .aggregate<std::int64_t, OutRecord>(
                        []() -> std::int64_t { return 0; },
                        [](const std::int64_t& acc, const InRecord& r) { return acc + r.value; },
                        [](std::int64_t k, const clink::TimeWindow& w, const std::int64_t& sum) {
                            return OutRecord{w.end, k, sum};
                        });

    clink::kafka::text_sink<OutRecord>(outs, sink_opts, [](const OutRecord& r) {
        auto bytes = out_codec().encode(r);
        return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    });

}

}  // namespace bench

CLINK_REGISTER_JOB("bench-canonical-pipeline",
                   "1.0",
                   "Kafka -> key_by -> tumbling window 1s -> sum -> Kafka",
                   bench::define_job);
