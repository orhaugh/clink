// kafka_2pc_job - bounded slow source piped to kafka_2pc_sink_string.
//
// The Kafka arm of the multi-connector exactly-once coverage: the shared
// bounded source (offset checkpointing included) producing into a broker
// transaction that commits when the coordinator confirms the checkpoint. The
// job CARRIES the connector: clink_node links no connector impls, so this
// module links clink::kafka and installs its factories into the job bundle's
// registry.
//
// Environment contract (set by the test, read at define_job time in every
// process that deploys the job):
//   CLINK_KAFKA_BROKERS - bootstrap servers (required)
//   CLINK_KAFKA_TOPIC   - target topic, default clink_xo
//   CLINK_2PC_TOTAL / CLINK_2PC_TICK_MS - shared source knobs

#include <cstdlib>
#include <memory>
#include <string>

#include "clink/api/pipeline.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/job/register_job.hpp"
#include "clink/kafka/install.hpp"
#include "clink/plugin/plugin.hpp"

#include "bounded_slow_source.hpp"

namespace kafka2pc_test {

std::string env_or(const char* name, const std::string& fallback) {
    if (const char* p = std::getenv(name); p != nullptr && *p != '\0') {
        return std::string{p};
    }
    return fallback;
}

void define_job(clink::api::Pipeline& pipeline) {
    clink::cluster::ensure_built_ins_registered();
    clink::kafka::install(pipeline.registry());

    const auto total = clink_examples::total_from_env();
    const auto tick = clink_examples::tick_from_env();
    const auto brokers = env_or("CLINK_KAFKA_BROKERS", "");
    const auto topic = env_or("CLINK_KAFKA_TOPIC", "clink_xo");

    pipeline.registry().register_source<std::string>(
        "kafka2pc_test.bounded_slow_source", [total, tick](const clink::plugin::BuildContext&) {
            return std::make_shared<clink_examples::BoundedSlowStringSource>(total, tick);
        });

    clink::api::SourceDescriptor src;
    src.op_type = "kafka2pc_test.bounded_slow_source";
    src.channel_type = "string";

    clink::api::SinkDescriptor sink;
    sink.op_type = "kafka_2pc_sink_string";
    sink.channel_type = "string";
    sink.params["brokers"] = brokers;
    sink.params["topic"] = topic;
    sink.params["transactional_id"] = "clink-xo-" + topic;
    // A killed incarnation leaves a zombie transaction; until the broker
    // expires or fences it, its first record pins the last stable offset and
    // read_committed consumers (and so the whole recovery) wait. The default
    // expiry is 60 seconds - far longer than these bounded test runs - so
    // shorten it. Commits land every checkpoint interval (~150ms), two
    // orders of magnitude inside this bound.
    //
    // Env-overridable because the prepared-transaction-resume e2e needs the
    // OPPOSITE: its orphaned transaction must survive a coordinator
    // failover long enough for the recovery's resolution to commit it, and
    // a 5s broker expiry races that whole choreography.
    const auto txn_timeout = env_or("CLINK_2PC_KAFKA_TXN_TIMEOUT_MS", "5000");
    sink.params["kafka.transaction.timeout.ms"] = txn_timeout;
    // librdkafka refuses a producer whose delivery timeout exceeds the
    // transaction timeout, so shorten it in step.
    sink.params["kafka.message.timeout.ms"] = txn_timeout;

    pipeline.source<std::string>(src).sink(sink);
}

}  // namespace kafka2pc_test

CLINK_REGISTER_JOB("kafka-2pc-test",
                   "1.0",
                   "bounded slow source piped to the transactional Kafka sink",
                   kafka2pc_test::define_job);
