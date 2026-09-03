// A job whose SOURCE FACTORY throws clink::state::CheckpointIntegrityError.
// Fixture for the cross-boundary typed-catch contract (item 19): the factory
// closure is compiled into this .so and invoked by the WORKER (host code)
// while building the subtask chain, so the exception propagates raw across
// the dlopen seam to the worker's catch-by-type, which must mark the failure
// fatal - the coordinator then fails the job with this cause instead of
// retrying a deterministic refusal. (A throw from inside process() would not
// do: the per-record operator wrapper catches and re-wraps it as a plain
// runtime_error before the worker's typed catch can see it.)
//
// That catch working depends on plugin modules keeping default symbol
// visibility, which is exactly why clink_add_job_module's HIDDEN_VISIBILITY
// stays experimental until this fixture also passes under it.

#include <cstdint>
#include <memory>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/job/register_job.hpp"
#include "clink/state/checkpoint_integrity.hpp"

namespace {

void define_job(clink::api::Pipeline& pipeline) {
    // register_source validates the channel type; the "int64" built-in must
    // be present first (the fluent helpers do this themselves).
    clink::cluster::ensure_built_ins_registered();
    pipeline.registry().register_source<std::int64_t>(
        "integrity_throw_source",
        [](const clink::plugin::BuildContext&) -> std::shared_ptr<clink::Source<std::int64_t>> {
            throw clink::state::CheckpointIntegrityError(
                clink::state::CheckpointStatus::Corrupt,
                "integrity_throw_job: deliberately damaged restore point (fixture)");
        });
    pipeline
        .source<std::int64_t>(clink::api::SourceDescriptor{.op_type = "integrity_throw_source"},
                              "src")
        .sink(clink::api::FileInt64Sink::builder()
                  .path("/tmp/clink_integrity_throw_job_never_written.txt")
                  .build());
}

}  // namespace

CLINK_REGISTER_JOB("integrity-throw",
                   "1.0",
                   "source factory throws CheckpointIntegrityError (typed-catch fixture)",
                   define_job);
