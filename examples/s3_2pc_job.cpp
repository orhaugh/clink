// s3_2pc_job - bounded slow source piped to s3_2pc_string_sink.
//
// The S3 arm of the multi-connector exactly-once coverage: the shared bounded
// source writing NDJSON lines into one object per (subtask, checkpoint),
// staged as a multipart upload at the barrier and made visible atomically by
// CompleteMultipartUpload once the coordinator confirms the checkpoint. The
// job CARRIES the connector: clink_node links no connector impls, so this
// module links clink::s3 and installs its factories into the job bundle's
// registry.
//
// Environment contract (set by the test, read at define_job time in every
// process that deploys the job):
//   CLINK_S3_BUCKET   - target bucket (required)
//   CLINK_S3_ENDPOINT - endpoint override for LocalStack / MinIO (required
//                       for the tests; empty means real AWS)
//   CLINK_S3_PREFIX   - key prefix, default clink-xo
//   CLINK_2PC_TOTAL / CLINK_2PC_TICK_MS - shared source knobs
//   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY - the standard chain

#include <cstdlib>
#include <memory>
#include <string>

#include "clink/api/pipeline.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/job/register_job.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/s3/install.hpp"

#include "bounded_slow_source.hpp"

namespace s32pc_test {

std::string env_or(const char* name, const std::string& fallback) {
    if (const char* p = std::getenv(name); p != nullptr && *p != '\0') {
        return std::string{p};
    }
    return fallback;
}

void define_job(clink::api::Pipeline& pipeline) {
    clink::cluster::ensure_built_ins_registered();
    clink::s3::install(pipeline.registry());

    const auto total = clink_examples::total_from_env();
    const auto tick = clink_examples::tick_from_env();

    pipeline.registry().register_source<std::string>(
        "s32pc_test.bounded_slow_source", [total, tick](const clink::plugin::BuildContext&) {
            return std::make_shared<clink_examples::BoundedSlowStringSource>(total, tick);
        });

    clink::api::SourceDescriptor src;
    src.op_type = "s32pc_test.bounded_slow_source";
    src.channel_type = "string";

    clink::api::SinkDescriptor sink;
    sink.op_type = "s3_2pc_string_sink";
    sink.channel_type = "string";
    sink.params["bucket"] = env_or("CLINK_S3_BUCKET", "");
    sink.params["key_prefix"] = env_or("CLINK_S3_PREFIX", "clink-xo");
    sink.params["endpoint_override"] = env_or("CLINK_S3_ENDPOINT", "");
    sink.params["region"] = "us-east-1";

    pipeline.source<std::string>(src).sink(sink);
}

}  // namespace s32pc_test

CLINK_REGISTER_JOB("s3-2pc-test",
                   "1.0",
                   "bounded slow source piped to the S3 multipart 2PC sink",
                   s32pc_test::define_job);
