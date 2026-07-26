// Kafka source read-rate bench: how fast can ONE KafkaSource subtask turn broker
// bytes into a Batch<KafkaMessage>?
//
// WHY. On the split cloud rig (benchmarks/nexmark_compare/cloud/README.md) the
// Kafka source was the largest single consumer of CPU in every query measured -
// 37.8% of all worker CPU on q0 and 27.3% on q12, roughly 0.9us per record, while
// the projection the query actually asked for cost 0.05us. That made the source,
// not the operators, the first thing worth fixing, and this bench is how the fix
// is measured in isolation rather than inferred from a whole-pipeline number.
//
// WHAT IT MEASURES. produce() calls only: fetch from the broker, build
// KafkaMessage, emit a batch. No decode, no operators, no channels. A pipeline
// number would fold in the JSON decode and three operator boundaries and could not
// attribute a change to the source.
//
// CPU PER RECORD IS THE HEADLINE, not wall-clock rate. A single reader against a
// containerised broker saturates the network pipe long before it saturates a core
// (measured here at ~135 MB/s, ~1.1M rec/s, on both the old and new fetch paths),
// so wall-clock cannot see a change in per-record cost at all - it reports the
// pipe. CPU seconds per record can, and CPU is what the cloud rig found the source
// spending: 37.8% of a four-core worker. rusage covers the whole process, so
// librdkafka's own broker threads are counted too, which is correct - they are part
// of what ingesting a record costs.
//
// The records are drained from a topic that must already exist and be populated
// (the bench does not produce them; a source bench that also writes the data would
// charge the producer's CPU to the reader). Populate with the nexmark harness or
// kafka-producer-perf-test.
//
//   CLINK_KAFKA_BROKERS=localhost:9092 CLINK_KAFKA_TOPIC=nx-bid \
//   CLINK_KAFKA_RECORDS=2000000 ./build/benchmarks/clink_kafka_source_bench
//
// Knobs: CLINK_KAFKA_BATCH (max_batch_size, default 1024), CLINK_KAFKA_TRIALS
// (default 3, each with a fresh consumer group so every trial re-reads from the
// beginning rather than resuming at a committed offset and measuring nothing).
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <unistd.h>
#include <vector>

#include <sys/resource.h>

#include "clink/connectors/kafka_source.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace {

std::string env_or(const char* k, const std::string& d) {
    const char* v = std::getenv(k);
    return (v != nullptr && *v != '\0') ? std::string{v} : d;
}

// Process CPU (user + system) in seconds. RUSAGE_SELF spans every thread,
// deliberately: librdkafka's broker threads do a real share of the ingest work and
// leaving them out would flatter whichever path pushes more work into them.
double cpu_seconds() {
    struct rusage ru{};
    if (getrusage(RUSAGE_SELF, &ru) != 0) {
        return 0.0;
    }
    const auto to_s = [](const struct timeval& tv) {
        return static_cast<double>(tv.tv_sec) + static_cast<double>(tv.tv_usec) / 1e6;
    };
    return to_s(ru.ru_utime) + to_s(ru.ru_stime);
}

std::uint64_t env_u64(const char* k, std::uint64_t d) {
    const char* v = std::getenv(k);
    if (v == nullptr || *v == '\0') {
        return d;
    }
    return std::strtoull(v, nullptr, 10);
}

// Counts what the source emits without doing anything with it, so the measurement
// is the source's cost and not a consumer's. Emitter is a concrete class taking a
// forward callback rather than an interface to override, so this is a plain
// counter the callback writes into. Records AND bytes are counted so a
// constant-time result - the classic "one side is quietly no-op-ing" failure -
// shows up rather than passing silently.
struct Tally {
    std::uint64_t records{0};
    std::uint64_t bytes{0};
    std::uint64_t batches{0};
};

}  // namespace

int main() {
    const std::string brokers = env_or("CLINK_KAFKA_BROKERS", "localhost:9092");
    const std::string topic = env_or("CLINK_KAFKA_TOPIC", "nx-bid");
    const std::uint64_t target = env_u64("CLINK_KAFKA_RECORDS", 1'000'000);
    const std::uint64_t batch_size = env_u64("CLINK_KAFKA_BATCH", 1024);
    const std::uint64_t trials = env_u64("CLINK_KAFKA_TRIALS", 3);

    if (!clink::KafkaSource::is_real_implementation()) {
        std::cerr << "built without librdkafka - nothing to measure\n";
        return 77;
    }

    std::cout << "kafka source bench: brokers=" << brokers << " topic=" << topic
              << " records=" << target << " max_batch_size=" << batch_size << " trials=" << trials
              << "\n\n";

    double best_rate = 0.0;
    double best_cpu_ns = 0.0;
    for (std::uint64_t t = 1; t <= trials; ++t) {
        clink::KafkaSource::Options opts;
        opts.brokers = brokers;
        opts.topic = topic;
        // Fresh group per trial: a reused group resumes at the committed offset
        // and the second trial would measure an empty read.
        opts.group_id = "clink-src-bench-" + std::to_string(static_cast<long>(::getpid())) + "-" +
                        std::to_string(t);
        opts.auto_offset_reset = "earliest";
        opts.commit_mode = clink::KafkaSource::CommitMode::Manual;
        opts.max_batch_size = static_cast<std::size_t>(batch_size);
        opts.metric_prefix = "bench";

        clink::MetricsRegistry metrics;
        clink::RuntimeContext ctx{clink::OperatorId{1}, "kafka_source", nullptr, &metrics};
        clink::KafkaSource src(opts);
        src.attach_runtime(&ctx);
        src.open();

        Tally sink;
        clink::Emitter<clink::KafkaMessage> em(
            [&sink](clink::StreamElement<clink::KafkaMessage> e) -> bool {
                if (e.is_data()) {
                    auto& batch = e.as_data();
                    sink.records += batch.size();
                    for (const auto& rec : batch) {
                        sink.bytes += rec.value().payload.size();
                    }
                    ++sink.batches;
                }
                return true;
            });
        // Warm up past group join and the first fetch, and start the clock only
        // once records are actually flowing: otherwise the join handshake (tens of
        // ms) lands inside the measured window and dominates a short run.
        const auto join_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (sink.records == 0 && std::chrono::steady_clock::now() < join_deadline) {
            src.produce(em);
        }
        if (sink.records == 0) {
            std::cerr << "trial " << t << ": no records - is " << topic << " populated?\n";
            src.close();
            return 1;
        }

        const std::uint64_t warm_records = sink.records;
        const std::uint64_t warm_bytes = sink.bytes;
        const std::uint64_t warm_batches = sink.batches;
        const double cpu_start = cpu_seconds();
        const auto start = std::chrono::steady_clock::now();
        const auto hard_deadline = start + std::chrono::seconds(120);
        while (sink.records - warm_records < target &&
               std::chrono::steady_clock::now() < hard_deadline) {
            src.produce(em);
        }
        const auto end = std::chrono::steady_clock::now();
        const double cpu_end = cpu_seconds();
        src.close();

        const double secs = std::chrono::duration<double>(end - start).count();
        const std::uint64_t recs = sink.records - warm_records;
        const std::uint64_t bytes = sink.bytes - warm_bytes;
        const std::uint64_t batches = sink.batches - warm_batches;
        const double rate = secs > 0 ? static_cast<double>(recs) / secs : 0.0;
        const double cpu = cpu_end - cpu_start;
        const double cpu_ns = recs > 0 ? cpu / static_cast<double>(recs) * 1e9 : 0.0;
        best_rate = std::max(best_rate, rate);
        best_cpu_ns = best_cpu_ns > 0 ? std::min(best_cpu_ns, cpu_ns) : cpu_ns;

        std::cout << "trial " << t << ": " << recs << " records in " << secs
                  << "s = " << static_cast<std::uint64_t>(rate) << " rec/s, "
                  << static_cast<std::uint64_t>(static_cast<double>(bytes) / secs / 1e6)
                  << " MB/s, " << batches << " batches (mean " << (batches > 0 ? recs / batches : 0)
                  << " rec/batch), "
                  << static_cast<std::uint64_t>(secs / static_cast<double>(recs) * 1e9)
                  << " ns/record wall, " << static_cast<std::uint64_t>(cpu_ns) << " ns/record CPU ("
                  << cpu << "s CPU, " << (cpu / secs) << " cores)\n";
    }

    std::cout << "\nbest: " << static_cast<std::uint64_t>(best_rate) << " rec/s wall, "
              << static_cast<std::uint64_t>(best_cpu_ns) << " ns/record CPU\n";
    return 0;
}
