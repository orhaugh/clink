// Schema-evolution test fixture packaged as a job .so.
//
// Exists to exercise the .so-side pre-deploy compatibility check (D):
// clink_job_check_restore_compatibility, emitted by CLINK_REGISTER_JOB.
//
// build_fn does two schema-evolution-relevant things:
//   1. Registers a migration chain for state_type "counter" (1->2, 2->3)
//      into THIS .so's StateMigrationRegistry::global(). Because
//      clink_core is statically linked and the .so is dlopened
//      RTLD_LOCAL, that global() is a distinct instance from the host's -
//      so the only way the host's check can see these migrations is by
//      asking the .so itself. That is exactly what the export does.
//   2. Declares expect_state_version("counter-op", "counter", 3), so the
//      job's expected map carries (operator_id_from_uid("counter-op"),
//      "counter", 3).
//
// A test dlopens this .so, calls the export with a crafted stored map,
// and asserts: stored v1 (or absent) -> compatible (chain 1->2->3
// reachable); stored v5 -> incompatible (no downgrade path to v3).
//
// The pipeline body is intentionally trivial - the check only needs
// build_fn to succeed and the expected map + migrations to be populated.

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/stream_execution_environment.hpp"
#include "clink/job/register_job.hpp"
#include "clink/state/schema_version.hpp"

namespace {

void define_job(clink::api::StreamExecutionEnvironment& env) {
    // Identity migrations - the check only consults path existence, not
    // the transform itself.
    const auto identity = [](std::span<const std::byte> in) {
        return std::vector<std::byte>(in.begin(), in.end());
    };
    clink::StateMigrationRegistry::global().register_migration("counter", 1, 2, identity);
    clink::StateMigrationRegistry::global().register_migration("counter", 2, 3, identity);

    env.from_elements<std::int64_t>({1, 2, 3})
        .map<std::int64_t>([](const std::int64_t& v) { return v; })
        .sink(clink::api::FileInt64Sink::builder().path("/tmp/schema_evo_test_out.txt").build());

    env.expect_state_version("counter-op", "counter", 3);
}

}  // namespace

CLINK_REGISTER_JOB("schema-evo-test",
                   "1.0",
                   "schema-evolution compatibility-check fixture",
                   define_job);
