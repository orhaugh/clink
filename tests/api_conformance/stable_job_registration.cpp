// Stable tier: packaging a pipeline as a job module.
// Compile-only; frozen (see README.md). Additions only.
//
// One authoring style per translation unit: CLINK_REGISTER_JOB here, the
// plugin macros in stable_plugin_registration.cpp.

#include <cstdint>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/job/register_job.hpp"

namespace {

void define_job(clink::api::Pipeline& pipeline) {
    pipeline.from_elements<std::int64_t>({1, 2, 3})
        .map<std::int64_t>([](const std::int64_t& v) { return v * 10; })
        .uid("conformance-map")
        .sink(clink::api::FileInt64Sink::builder().path("/tmp/conformance-job.txt").build());
}

}  // namespace

CLINK_REGISTER_JOB("api-conformance-job", "1.0", "frozen conformance job", define_job);
