#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "clink/core/types.hpp"
#include "clink/state/schema_version.hpp"
#include "clink/state/state_backend.hpp"

// State schema evolution: the decision + transform shared by the
// pre-deploy compatibility checker (D) and the restore-time migration
// (C). Both reduce to the same per-(op, state_type) comparison of the
// snapshot's stamped versions against the versions the live job
// expects, plus the StateMigrationRegistry's reachability/transform.
// Keeping that decision in one place is the single-source-of-truth that
// stops the checker and the migrator from drifting.

namespace clink {

// One (operator, state_type) pair whose stored version cannot be
// brought to the expected version because the registry has no migration
// path. The pre-deploy checker returns these so a deploy can fail fast
// with a precise list instead of corrupting state at restore.
struct StateIncompatibility {
    OperatorId op_id{};
    std::string state_type;
    std::uint32_t from_version{};
    std::uint32_t to_version{};
};

// Compare the version map recovered from a snapshot (`stored`) against
// the versions the live job expects (`expected`); return every
// (op, state_type) the registry cannot bridge. An empty result means
// the restore is compatible: equal versions need no migration, and an
// entry absent from `stored` is treated as version 1 (matching the
// SchemaVersionTrait default). Pure - touches no backend.
[[nodiscard]] std::vector<StateIncompatibility> check_restore_compatibility(
    const StateVersionMap& stored,
    const StateVersionMap& expected,
    const StateMigrationRegistry& reg = StateMigrationRegistry::global());

// Pack / unpack a compatibility result (the output of
// check_restore_compatibility) to the line-oriented string form crossing
// the job-.so C-ABI boundary: one "<op_id>|<state_type>|<from>|<to>" per
// line, lines separated by '\n'. Empty input/output means "compatible"
// (no incompatibilities). state_type must not contain '|' or '\n' (same
// constraint StateVersionMap enforces). unpack throws std::runtime_error
// on a malformed line. These mirror StateVersionMap::pack/unpack so the
// .so-side packer and the host-side (CLI / JM) unpacker share one format.
[[nodiscard]] std::string pack_incompatibilities(const std::vector<StateIncompatibility>& incompat);
[[nodiscard]] std::vector<StateIncompatibility> unpack_incompatibilities(std::string_view packed);

// Migrate a freshly-restored backend's stored state up to the expected
// versions, in place, BEFORE any operator reads it. For each expected
// (op, state_type) whose stored version differs, the values of that
// entry's keyed-state slot are run through the registry's migration
// chain. The backend contract forbids mutating during a scan, so values
// are collected first and written back after. Throws std::runtime_error
// if a needed path is missing (defence in depth: the pre-deploy checker
// should have caught it, but the HA auto-restart path can restore without
// that gate). On success the backend's version map is re-stamped to
// `expected` so the next snapshot records the migrated versions (and a
// re-restore from that snapshot is a no-op).
//
// Slot-aware: an operator may declare more than one keyed-state slot
// (e.g. interval_join's "left_buf" + "right_buf"). Each StateVersionEntry
// names the slot it applies to; the migrator filters the backend scan by
// that slot's key prefix so bumping one slot's version leaves the others
// byte-identical. An entry with an empty slot migrates every value under
// the operator - the single-slot / legacy behaviour. (One typed slot per
// (op, state_type) still holds; two slots on one op use distinct
// state_type tags.)
//
// Stability across generations: the from-version is looked up by
// (op, state_type), and the slot filter matches by slot name, so BOTH the
// state_type tag and the slot name must stay stable from the generation
// that wrote a savepoint to the one that restores it. A snapshot stamp
// absent under the expected (op, state_type) is assumed v1 - safe only
// when the tag is genuinely new, NOT when a slot/tag was renamed (a rename
// would silently re-migrate already-current data). Renaming a state_type
// or slot across a restore is unsupported here; it needs an explicit
// rename/transform step before restore.
//
// TTL slots: a TTL-enabled slot stores values as [8B expire-at][user
// bytes]; the migrator hands the FULL stored value (TTL header included)
// to the migration function, which expects raw user bytes. Migrating a
// TTL-enabled slot is therefore unsupported in v1 (the migrator has no
// per-slot TtlConfig to strip/re-attach the header) - do not bump the
// schema version of a TTL slot.
void migrate_restored_state(StateBackend& backend,
                            const StateVersionMap& expected,
                            const StateMigrationRegistry& reg = StateMigrationRegistry::global());

}  // namespace clink
