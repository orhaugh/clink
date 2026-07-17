// Cross-job-collision test job A.
//
// Defines a simple pipeline that mints inline-op names
// _inline_from_elements_0, _inline_map_1. Job B (collision_job_b.cpp)
// mints the SAME names against a different pipeline. Submitting both
// concurrently exercises per-job-bundle isolation: each job's bundle
// holds its own _inline_<kind>_<n> entries, so the two jobs don't
// trample each other.
//
// Pipeline: from_elements({1,2,3,4,5}).map(v -> v*10).sink(out_path)
// Output (one per line): 10, 20, 30, 40, 50
//
// Output path: $CLINK_COLLISION_OUT_A (defaulting to /tmp/clink_collision_a.txt).

#include <cstdint>
#include <cstdlib>
#include <string>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/job/register_job.hpp"

namespace {

std::string output_path() {
    if (const char* p = std::getenv("CLINK_COLLISION_OUT_A"); p != nullptr && *p != '\0') {
        return p;
    }
    return "/tmp/clink_collision_a.txt";
}

void define_job(clink::api::Pipeline& pipeline) {
    pipeline.from_elements<std::int64_t>({1, 2, 3, 4, 5})
        .map<std::int64_t>([](const std::int64_t& v) { return v * 10; })
        .sink(clink::api::FileInt64Sink::builder().path(output_path()).build());
}

}  // namespace

CLINK_REGISTER_JOB("collision-job-a",
                   "1.0",
                   "first job in the bundle-collision pair (multiplies by 10)",
                   define_job);
