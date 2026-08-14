#pragma once

// In-doubt commit resolution at restore-point selection.
//
// A checkpoint that COMPLETED but was never CONFIRMED may hold external
// transactions its dead worker prepared and never committed. Some of those
// are finalisable with connector knowledge (a resolver registered in
// TxnResumeRegistry - Kafka commits the orphan over the wire with the dead
// producer's identity, clink/kafka/txn_resume.hpp). Resolution and
// restore-point selection are ONE decision: only when every staged handle
// of a checkpoint resolves as committed may CONFIRMED advance to it -
// finalising an orphan while still restoring from before it would replay
// its interval as duplicates.
//
// Called from recovery paths that hold no coordinator lock (resolvers do
// network round trips); deliberately NOT called from restart_job_locked_,
// whose in-incarnation window keeps the commit-confirmed contract until a
// lock-free restart phase exists. Conservative on every uncertainty: an
// unreadable marker or snapshot, a checkpoint with no visible handle, a
// missing resolver, or any resolver failure stops the walk and leaves the
// bounded-replay contract in force.

#include <cstdint>
#include <filesystem>
#include <string>

namespace clink::cluster {

using JobId = std::uint64_t;  // matches protocol.hpp without dragging it in

// <checkpoint_dir>/_jobs/<job_id>: where the coordinator keeps this job's
// COMPLETED-/CONFIRMED- markers. The layout fact shared by the marker
// writers, the restore-point readers, and the resolution walk.
[[nodiscard]] std::filesystem::path completed_marker_dir_for(const std::string& checkpoint_dir,
                                                             JobId job_id);

// Walk (confirmed, completed], resolving each completed checkpoint's staged
// handles via TxnResumeRegistry and durably writing CONFIRMED-<id> on full
// success. Returns the new confirmed id (== `confirmed` when nothing
// advanced).
[[nodiscard]] std::uint64_t resolve_in_doubt_commits(const std::string& checkpoint_dir,
                                                     JobId job_id,
                                                     std::uint64_t confirmed,
                                                     std::uint64_t completed);

}  // namespace clink::cluster
