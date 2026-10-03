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

#include <atomic>
#include <chrono>
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

// <checkpoint_dir>/_jobs/<job_id>/receipts: per-subtask commit receipts.
// A 2PC sink writes `sub<K>-<N>` here immediately after its external commit
// for checkpoint N provably executed (see kCommitReceiptFileName in
// txn_resume_registry.hpp for the file-name convention). The resolution walk
// treats a receipted handle as COMMITTED without a wire call - the receipt
// is this process's own durable record of the broker's acknowledgement, so
// it outlives producer fencing, broker restarts, and transaction timeouts,
// none of which can retract a commit that already happened. Workers prune
// receipts alongside the checkpoints their retention sweep purges.
[[nodiscard]] std::filesystem::path commit_receipt_dir_for(const std::string& checkpoint_dir,
                                                           JobId job_id);

// Walk (confirmed, completed], resolving each completed checkpoint's staged
// handles via TxnResumeRegistry and durably writing CONFIRMED-<id> on full
// success. Returns the new confirmed id (== `confirmed` when nothing
// advanced).
//
// Resolution EXECUTES commits handle by handle - EndTxn is the resolution,
// there is no read-only probe - so a handle whose broker is merely
// UNREACHABLE (transport_inconclusive) is retried in place rather than
// treated as a verdict: a fallback taken after some handles committed
// would restore below intervals this walk just published and replay them
// as duplicates. Broker chaos overlapping a recovery reaches exactly that
// interleaving. `transport_retry_backoff` spaces the bounded retries; the
// held restart the callers run under is already waiting on this answer.
// The highest checkpoint id named by ANY snapshot file under this
// checkpoint directory - every generation, every subtask, INCLUDING files
// no marker vouches for. Recovered jobs number their new checkpoints above
// this as well as above the durable markers: a seconds-lived incarnation
// dies holding snapshot files whose checkpoints never completed, no marker
// records them, and a successor numbering above markers alone REUSES those
// ids - its files then interleave with the dead incarnation's, and a later
// restore can assemble one checkpoint from two vintages (qual01-20260819g:
// window state of one vintage, source offsets of another, one nominal id;
// ten windows re-published identically). Ids are cheap; never reuse one
// that ANY durable artefact names.
[[nodiscard]] std::uint64_t latest_snapshot_id_on_disk(const std::string& checkpoint_dir);

// <checkpoint_dir>/_jobs/<job_id>/TRIGGERED, in decimal: the highest
// checkpoint id any coordinator of the job has claimed, and a recovered job
// numbers above it too. A coordinator claims an id here before any frame
// naming it leaves (a TriggerCheckpoint, the reply carrying an end-of-input
// final id, a hot cutover's arm), so the record covers every barrier that can
// have reached a worker. The snapshot files cannot: a capture still being
// written has no file yet, a worker that outlives the dead coordinator can
// finish it after the new leader has looked, and with no rescale between them
// both incarnations write under one generation directory, so a reused id puts
// that file where the new run's capture of the id goes. Trace validation
// caught a takeover reusing such an id.
[[nodiscard]] std::string triggered_record_key(JobId job_id);

// The id the record holds; 0 when there is none or it does not parse.
[[nodiscard]] std::uint64_t latest_triggered_id_on_disk(const std::string& checkpoint_dir,
                                                        JobId job_id);

// What a claim on one checkpoint id found.
struct TriggeredIdClaim {
    // True when this call put the id on record, which makes it the caller's
    // alone: no coordinator can claim it after.
    bool claimed{false};
    // What the record holds afterwards: the id itself when claimed, else the
    // id at or above it that refused the claim.
    std::uint64_t on_record{0};
};

// Claim `id` on the record. The claim lands only over a record holding less
// than the id, so the record only rises and an id is claimed once across every
// coordinator of the job. That is what keeps a leader off the ids of a
// superseded coordinator still triggering under its old epoch: workers that
// have bound the new epoch refuse that coordinator's barriers, but one that
// has not yet re-registered with the leader still accepts them, and its
// capture of such an id writes the path the leader's capture of the same id
// would. A claim refused because the record already holds the id or more is
// an ordinary answer, not an error, and logs nothing: the caller must not send
// the id and numbers above what the record holds. Throws, logging nothing
// either, when the record could not be written, and the caller must not send
// the id then. Writers in one process serialise per job (the coordinator's
// per-job record lock); the compare-and-set orders them across processes.
[[nodiscard]] TriggeredIdClaim record_triggered_id(const std::string& checkpoint_dir,
                                                   JobId job_id,
                                                   std::uint64_t id);

// `cancel` (optional): cooperative cancellation, checked before every wire
// probe and before every store effect (receipt materialisation, CONFIRMED
// markers) - and between reading a probe's answer and ACTING on it. The
// coordinator's watchdog sets it when the walk outruns its deadline; the
// walk must then stop MUTATING, because its EndTxn probes execute commits
// and its store writes steer every later recovery. The rig-night composite
// caught the alternative live: a timed-out walk kept committing
// transactions and wrote CONFIRMED for a job the coordinator had already
// failed. A cancelled walk returns its progress so far, exactly like a
// refusal.
[[nodiscard]] std::uint64_t resolve_in_doubt_commits(
    const std::string& checkpoint_dir,
    JobId job_id,
    std::uint64_t confirmed,
    std::uint64_t completed,
    std::chrono::milliseconds transport_retry_backoff = std::chrono::seconds{2},
    const std::atomic<bool>* cancel = nullptr);

}  // namespace clink::cluster
