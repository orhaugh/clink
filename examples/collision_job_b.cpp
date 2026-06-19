// Cross-job-collision test job B.
//
// Companion of collision_job_a.cpp. Same inline-op name minting
// pattern (env mints _inline_from_elements_0, _inline_map_1) but a
// different pipeline. With per-job bundles the two jobs' registrations
// stay isolated and both produce correct output when submitted
// concurrently.
//
// Pipeline: from_elements({100,200,300}).map(v -> v+1).sink(out_path)
// Output (one per line): 101, 201, 301
//
// Output path: $CLINK_COLLISION_OUT_B (defaulting to /tmp/clink_collision_b.txt).

#include <cstdint>
#include <cstdlib>
#include <string>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/stream_execution_environment.hpp"
#include "clink/job/register_job.hpp"

namespace {

std::string output_path() {
    if (const char* p = std::getenv("CLINK_COLLISION_OUT_B"); p != nullptr && *p != '\0') {
        return p;
    }
    return "/tmp/clink_collision_b.txt";
}

void define_job(clink::api::StreamExecutionEnvironment& env) {
    env.from_elements<std::int64_t>({100, 200, 300})
        .map<std::int64_t>([](const std::int64_t& v) { return v + 1; })
        .sink(clink::api::FileInt64Sink::builder().path(output_path()).build());
}

}  // namespace

CLINK_REGISTER_JOB("collision-job-b",
                   "1.0",
                   "second job in the bundle-collision pair (adds 1)",
                   define_job);
