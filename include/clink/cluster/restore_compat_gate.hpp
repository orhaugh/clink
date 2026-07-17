#pragma once

#include <cstdint>
#include <string>
#include <vector>

// Pre-deploy restore-compatibility gate (schema evolution D, part 2).
//
// When a job is submitted (or HA-recovered) with a restore configured,
// the coordinator can fail fast if the savepoint's stored state versions cannot be
// migrated to the versions the job binary expects - instead of letting
// the job deploy and then throw at worker start (which C, the restore-time
// migrator, would do anyway). This is the coordinator-side automation of
// `clink check-savepoint --expected=<job.so>`.
//
// BEST-EFFORT by design: it only ever BLOCKS on a definitive
// incompatibility verdict. If it cannot read the savepoint (remote/shared
// storage the coordinator doesn't mount, missing file), cannot load a .so, or no
// .so exports the check, it returns "" and lets the deploy proceed - C
// still guarantees correctness at restore time. The gate buys an earlier,
// clearer error when the coordinator happens to have what it needs.

namespace clink::cluster {

// Read the restore savepoint's stored version map (subtask 0's snapshot
// under <restore_from_dir>/0/checkpoint-<id>.snap) and ask each job .so,
// .so-side (where its StateMigrationRegistry lives), whether it can
// migrate that map to its expected versions.
//
// Returns a human-readable reject reason if a .so reports a DEFINITE
// incompatibility; "" if compatible, no restore is configured
// (restore_from_dir empty), the savepoint is unreadable, or no .so
// exports clink_job_check_restore_compatibility.
[[nodiscard]] std::string check_restore_compatibility_via_plugins(
    const std::vector<std::string>& plugin_so_paths,
    const std::string& restore_from_dir,
    std::uint64_t restore_checkpoint_id);

}  // namespace clink::cluster
