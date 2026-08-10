// postgres_2pc_job - bounded slow source piped to postgres_2pc_sink.
//
// The Postgres arm of the multi-connector exactly-once coverage: the same
// bounded source the file 2PC job uses (shared offset checkpointing), each
// record shaped as the JSON object the Postgres JSON sink projects
// ({"v":"record-N"} with columns=v), committed via PREPARE TRANSACTION /
// COMMIT PREPARED. The job CARRIES the connector: clink_node links no
// connector impls, so this module links clink::postgres and installs its
// factories into the job bundle's registry.
//
// Environment contract (set by the test, read at define_job time in every
// process that deploys the job):
//   CLINK_PG_DSN    - libpq conninfo for the target server (required)
//   CLINK_PG_TABLE  - target table, default clink_xo; must have column v
//   CLINK_2PC_TOTAL / CLINK_2PC_TICK_MS - shared source knobs

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>

#include "clink/api/pipeline.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/job/register_job.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/postgres/install.hpp"

#include "bounded_slow_source.hpp"

namespace pg2pc_test {

std::string env_or(const char* name, const std::string& fallback) {
    if (const char* p = std::getenv(name); p != nullptr && *p != '\0') {
        return std::string{p};
    }
    return fallback;
}

void define_job(clink::api::Pipeline& pipeline) {
    clink::cluster::ensure_built_ins_registered();
    clink::postgres::install(pipeline.registry());

    const auto total = clink_examples::total_from_env();
    const auto tick = clink_examples::tick_from_env();
    const auto dsn = env_or("CLINK_PG_DSN", "");
    const auto table = env_or("CLINK_PG_TABLE", "clink_xo");

    pipeline.registry().register_source<std::string>(
        "pg2pc_test.bounded_slow_source", [total, tick](const clink::plugin::BuildContext&) {
            // The record IS the JSON object the sink projects: column v
            // carries the payload the verifier compares against.
            return std::make_shared<clink_examples::BoundedSlowStringSource>(
                total, tick, [](std::int64_t i) {
                    return "{\"v\":\"record-" + std::to_string(i) + "\"}";
                });
        });

    clink::api::SourceDescriptor src;
    src.op_type = "pg2pc_test.bounded_slow_source";
    src.channel_type = "string";

    clink::api::SinkDescriptor sink;
    sink.op_type = "postgres_2pc_sink";
    sink.channel_type = "string";
    sink.params["conninfo"] = dsn;
    sink.params["table"] = table;
    sink.params["columns"] = "v";

    pipeline.source<std::string>(src).sink(sink);
}

}  // namespace pg2pc_test

CLINK_REGISTER_JOB("postgres-2pc-test",
                   "1.0",
                   "bounded slow source piped to the Postgres 2PC sink",
                   pg2pc_test::define_job);
