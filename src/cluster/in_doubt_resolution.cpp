#include "clink/cluster/in_doubt_resolution.hpp"

#include <algorithm>
#include <cstdlib>
#include <optional>
#include <sstream>
#include <thread>
#include <vector>

#include "clink/cluster/coordination_store.hpp"
#include "clink/config/json.hpp"
#include "clink/connectors/txn_resume_registry.hpp"
#include "clink/metrics/orchestration_metrics.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/state/durable_file_write.hpp"
#include "clink/state/state_backend_factory.hpp"
#include "clink/state_processor/savepoint.hpp"
#include "clink/state_processor/state_diff.hpp"

namespace clink::cluster {

std::filesystem::path completed_marker_dir_for(const std::string& checkpoint_dir, JobId job_id) {
    return std::filesystem::path{checkpoint_dir} / "_jobs" / std::to_string(job_id);
}

std::filesystem::path commit_receipt_dir_for(const std::string& checkpoint_dir, JobId job_id) {
    return completed_marker_dir_for(checkpoint_dir, job_id) / "receipts";
}

std::uint64_t latest_snapshot_id_on_disk(const std::string& checkpoint_dir) {
    if (checkpoint_dir.empty()) {
        return 0;
    }
    // Layout: <checkpoint_dir>/v<generation>/<subtask>/checkpoint-<id>.snap.
    // error_code iteration throughout: this scans a directory the live job
    // writes and retention prunes.
    std::uint64_t latest = 0;
    std::error_code ec;
    for (const auto& gen : std::filesystem::directory_iterator(checkpoint_dir, ec)) {
        if (ec) {
            break;
        }
        std::error_code tec;
        if (!gen.is_directory(tec) || tec) {
            continue;
        }
        const auto gname = gen.path().filename().string();
        if (gname.size() < 2 || gname.front() != 'v' ||
            gname.find_first_not_of("0123456789", 1) != std::string::npos) {
            continue;
        }
        std::error_code sec;
        for (const auto& sub : std::filesystem::directory_iterator(gen.path(), sec)) {
            if (sec) {
                break;
            }
            std::error_code stec;
            if (!sub.is_directory(stec) || stec) {
                continue;
            }
            std::error_code fec;
            for (const auto& file : std::filesystem::directory_iterator(sub.path(), fec)) {
                if (fec) {
                    break;
                }
                const auto name = file.path().filename().string();
                constexpr std::string_view kPrefix = "checkpoint-";
                constexpr std::string_view kSuffix = ".snap";
                if (name.rfind(kPrefix, 0) != 0 || name.size() <= kPrefix.size() + kSuffix.size() ||
                    name.compare(name.size() - kSuffix.size(), kSuffix.size(), kSuffix) != 0) {
                    continue;
                }
                const auto digits =
                    name.substr(kPrefix.size(), name.size() - kPrefix.size() - kSuffix.size());
                if (digits.empty() || digits.find_first_not_of("0123456789") != std::string::npos) {
                    continue;
                }
                try {
                    // Explicit width: stoull returns unsigned long long,
                    // which is NOT std::uint64_t on Linux/LP64 (that is
                    // unsigned long), and the mismatched std::max fails to
                    // compile there while building clean on macOS.
                    latest = std::max(latest, static_cast<std::uint64_t>(std::stoull(digits)));
                } catch (const std::exception&) {
                    // an overlong digit run; not an id this engine wrote
                }
            }
        }
    }
    return latest;
}

namespace {
//
// A checkpoint that COMPLETED but was never CONFIRMED may hold external
// transactions its dead worker prepared and never committed. Some of those
// are finalisable with connector knowledge (a resolver registered in
// TxnResumeRegistry - Kafka commits the orphan over the wire with the dead
// producer's identity). Resolution and restore-point selection are ONE
// decision: only when every staged handle of a checkpoint resolves as
// committed may CONFIRMED advance to it - finalising an orphan while still
// restoring from before it would replay its interval as duplicates.
//
// Runs on recovery paths that hold no coordinator lock (the resolvers do
// network round trips). Conservative on every uncertainty: an unreadable
// marker or snapshot, a checkpoint with no visible handle, a missing
// resolver, or any resolver failure stops the walk and leaves the
// commit-confirmed contract (bounded replay) in force.

// generation + acking subtasks recorded in a COMPLETED-<id> marker.
struct CompletedMarkerInfo {
    std::uint32_t generation{1};
    std::vector<std::uint32_t> subtasks;
};

std::optional<CompletedMarkerInfo> read_completed_marker(const std::string& body) {
    CompletedMarkerInfo out;
    std::istringstream in(body);
    std::string line;
    bool saw_subtasks = false;
    while (std::getline(in, line)) {
        if (line.rfind("generation=", 0) == 0) {
            out.generation =
                static_cast<std::uint32_t>(std::strtoul(line.c_str() + 11, nullptr, 10));
        } else if (line.rfind("subtasks=", 0) == 0) {
            saw_subtasks = true;
            std::size_t pos = 9;
            while (pos < line.size()) {
                const auto comma = line.find(',', pos);
                const auto tok =
                    line.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
                if (!tok.empty()) {
                    out.subtasks.push_back(
                        static_cast<std::uint32_t>(std::strtoul(tok.c_str(), nullptr, 10)));
                }
                pos = comma == std::string::npos ? line.size() : comma + 1;
            }
        }
    }
    if (!saw_subtasks) {
        return std::nullopt;  // pre-participant-set marker; nothing to walk
    }
    return out;
}

// One staged resume handle plus the sink subtask that staged it. The
// subtask index comes from the stored key's suffix (the sink stages under
// kTxnResumeStateKeyPrefix + "sub<K>"), and is what pairs the handle with
// its on-disk commit receipt.
struct StagedHandle {
    std::uint32_t subtask{0};
    std::string handle;
};

// W3 (coordination-store plan): these snapshot loads are the one read the
// store does not yet cover - they go through the state layout directly and
// only exist under a filesystem/legacy backend rooted at checkpoint_dir.
// The staged resume handles inside one checkpoint's snapshots: operator-state
// rows (0xFF-prefixed stored keys) whose logical key starts with
// kTxnResumeStateKeyPrefix. nullopt = a snapshot could not be read (or a
// staged key did not parse), which is different from "no handles" and must
// stop resolution.
std::optional<std::vector<StagedHandle>> read_resume_handles(const std::string& checkpoint_dir,
                                                             const CompletedMarkerInfo& info,
                                                             std::uint64_t ckpt_id) {
    std::vector<StagedHandle> handles;
    for (const auto sub : info.subtasks) {
        const auto snap =
            std::filesystem::path(clink::state_dir_for(checkpoint_dir, info.generation, sub)) /
            ("checkpoint-" + std::to_string(ckpt_id) + ".snap");
        std::error_code ec;
        if (!std::filesystem::exists(snap, ec)) {
            // A subtask that acked but kept no state writes no snapshot;
            // that is a normal shape, not an error.
            continue;
        }
        try {
            auto sp = clink::state_processor::Savepoint::load_from_file(snap);
            const auto entries = clink::state_processor::collect_entries(sp);
            for (const auto& [op, slots] : entries) {
                for (const auto& [slot, rows] : slots) {
                    for (const auto& [key, entry] : rows) {
                        if (key.size() > 1 &&
                            static_cast<std::uint8_t>(key.front()) ==
                                clink::kOperatorStateKeyPrefix &&
                            key.compare(1,
                                        clink::connectors::kTxnResumeStateKeyPrefix.size(),
                                        clink::connectors::kTxnResumeStateKeyPrefix) == 0) {
                            // Suffix after the prefix is "sub<K>". An
                            // unparsable suffix is corrupt state: stop, as
                            // an unreadable snapshot would.
                            const auto suffix =
                                key.substr(1 + clink::connectors::kTxnResumeStateKeyPrefix.size());
                            if (suffix.size() < 4 || suffix.compare(0, 3, "sub") != 0) {
                                clink::log::warn("coordinator.recovery",
                                                 "in-doubt resolution: staged handle key '" +
                                                     suffix + "' in " + snap.string() +
                                                     " names no subtask; stopping resolution");
                                return std::nullopt;
                            }
                            const auto owner = static_cast<std::uint32_t>(
                                std::strtoul(suffix.c_str() + 3, nullptr, 10));
                            // A handle is authoritative for THIS walk id
                            // only if it was staged AT this checkpoint -
                            // the handle records its own "ckpt". Union
                            // operator-state restore replicates every
                            // sink's row into every subtask and later
                            // snapshots re-persist the copies verbatim, so
                            // stale copies carry an OLDER ckpt by
                            // construction (only the owning sink ever
                            // re-stages its key). qual01-20260818d's walk
                            // saw 64 handles for 4 sinks and aborted on a
                            // fenced stale copy without trying the live
                            // ones. The ckpt field discriminates without
                            // assuming anything about subtask numbering -
                            // the key's index is the SINK-LOCAL one and the
                            // marker's subtasks are job-global, which a
                            // first cut of this filter conflated, dropping
                            // every live handle instead.
                            const auto ckpt_tag = "\"ckpt\":\"" + std::to_string(ckpt_id) + "\"";
                            if (entry.value.find(ckpt_tag) == std::string::npos) {
                                continue;
                            }
                            handles.push_back({owner, entry.value});
                        }
                    }
                }
            }
        } catch (const std::exception& e) {
            clink::log::warn("coordinator.recovery",
                             "in-doubt resolution: snapshot " + snap.string() +
                                 " could not be read (" + e.what() + "); stopping resolution");
            return std::nullopt;
        }
    }
    return handles;
}

// Walk (confirmed, completed], resolving each completed checkpoint's staged
// handles and durably advancing CONFIRMED on full success. Returns the new
// confirmed id (== `confirmed` when nothing advanced).
}  // namespace

std::uint64_t resolve_in_doubt_commits(const std::string& checkpoint_dir,
                                       JobId job_id,
                                       std::uint64_t confirmed,
                                       std::uint64_t completed,
                                       std::chrono::milliseconds transport_retry_backoff,
                                       const std::atomic<bool>* cancel) {
    if (checkpoint_dir.empty() || completed <= confirmed) {
        return confirmed;
    }
    // Cooperative cancellation: consulted before every wire probe and every
    // store effect, and between a probe's answer and acting on it. A probe
    // is an EndTxn - it EXECUTES commits - and the store writes steer every
    // later recovery, so a walk that outran its deadline must stop
    // mutating, not merely stop restarting (the rig-night composite caught
    // a timed-out walk committing transactions and writing CONFIRMED for a
    // job the coordinator had already failed). One mandated exception: a
    // cancelled walk still persists .unresolved markers for the handles it
    // never settled (a local store write, no wire side) - see
    // persist_unresolved_markers below.
    const auto cancelled = [cancel, job_id, &confirmed]() {
        if (cancel != nullptr && cancel->load(std::memory_order_acquire)) {
            clink::log::info("coordinator.recovery",
                             "in-doubt resolution: cancelled for job " + std::to_string(job_id) +
                                 "; stopping before further wire or store effects, the restart "
                                 "proceeds on the bounded contract at confirmed=" +
                                 std::to_string(confirmed));
            return true;
        }
        return false;
    };
    const auto store = make_coordination_store(checkpoint_dir);
    const auto job_prefix = "_jobs/" + std::to_string(job_id) + "/";
    // The refusal wall. Found by the exactly-once model (formal/ExactlyOnce.tla,
    // design record 012), not by a rig. A walk that stops at checkpoint k -
    // a final refusal, exhausted transport, a cancel, an unreadable marker -
    // returns without looking at any completed checkpoint ABOVE k. A commit
    // that executed there without its receipt (a kill in the ack window) is
    // then fenced blind when the job redeploys below k, and its interval
    // replays as duplicates; and because k stays completed-but-refused on
    // disk, every later walk stops at the same k until a higher CONFIRMED
    // marker lands, so the hole does not close by itself. The counterexample
    // is fourteen protocol steps long and needs no fault the campaigns do
    // not already inject: a completed checkpoint whose broadcast was withheld
    // and whose transactions a broker outage left unresolved (aborted at the
    // sinks' next open), then an ack-window kill one checkpoint later.
    //
    // So every early stop also leaves an .unresolved marker for each
    // unreceipted handle staged in a completed checkpoint above the stop,
    // exactly as it does for the stopped checkpoint's own unsettled handles:
    // the owning sink's pre-fence DescribeTransactions settles each one
    // before anything can fence it. A receipt outranks a marker, so handles
    // whose commit is already on record are left alone.
    const auto mark_later_unreceipted = [&](std::uint64_t stopped_at) {
        for (std::uint64_t later = stopped_at + 1; later <= completed; ++later) {
            const auto later_body = store->get(job_prefix + "COMPLETED-" + std::to_string(later));
            if (!later_body.has_value()) {
                continue;  // never completed; nothing staged there can have committed
            }
            const auto later_info = read_completed_marker(*later_body);
            if (!later_info.has_value()) {
                continue;
            }
            const auto later_handles = read_resume_handles(checkpoint_dir, *later_info, later);
            if (!later_handles.has_value()) {
                continue;  // logged by the reader; an unreadable snapshot cannot be marked
            }
            for (const auto& h : *later_handles) {
                const auto receipt = job_prefix + "receipts/" +
                                     clink::connectors::commit_receipt_file_name(h.subtask, later);
                if (store->exists(receipt)) {
                    continue;
                }
                try {
                    store->put(receipt + ".unresolved", h.handle);
                    clink::log::info(
                        "coordinator.recovery",
                        "in-doubt resolution: checkpoint " + std::to_string(later) + " subtask " +
                            std::to_string(h.subtask) +
                            ": unreceipted handle above the stopped checkpoint " +
                            std::to_string(stopped_at) +
                            " marked unresolved; the owning sink describes it before fencing");
                } catch (const std::exception& e) {
                    clink::log::error(
                        "coordinator.recovery",
                        "in-doubt resolution: checkpoint " + std::to_string(later) + " subtask " +
                            std::to_string(h.subtask) +
                            ": unresolved marker could not be written (" + std::string(e.what()) +
                            "); a restore below may replay an interval whose commit executed");
                }
            }
        }
    };
    for (std::uint64_t id = confirmed + 1; id <= completed; ++id) {
        // No cancel return here: even a walk cancelled between checkpoints
        // must first read this id's handles and leave unresolved markers
        // (below) - returning with nothing written is what lets the next
        // deploy fence blind.
        const auto marker_body = store->get(job_prefix + "COMPLETED-" + std::to_string(id));
        if (!marker_body.has_value()) {
            continue;  // this id never completed; its transaction was aborted
        }
        const auto info = read_completed_marker(*marker_body);
        if (!info.has_value()) {
            clink::log::info("coordinator.recovery",
                             "in-doubt resolution: COMPLETED-" + std::to_string(id) +
                                 " carries no participant set; stopping at confirmed=" +
                                 std::to_string(confirmed));
            clink::metrics::orch::in_doubt_unresolved();
            mark_later_unreceipted(id);
            return confirmed;
        }
        const auto handles = read_resume_handles(checkpoint_dir, *info, id);
        if (!handles.has_value() || handles->empty()) {
            // Unreadable snapshots, or a checkpoint whose sinks staged no
            // handle (older binary, different sink family): nothing here
            // can be PROVEN committed, so the restore point stays put.
            if (handles.has_value()) {
                clink::log::info("coordinator.recovery",
                                 "in-doubt resolution: checkpoint " + std::to_string(id) +
                                     " staged no resume handles; stopping at confirmed=" +
                                     std::to_string(confirmed));
            }
            clink::metrics::orch::in_doubt_unresolved();
            mark_later_unreceipted(id);
            return confirmed;
        }
        // EndTxn IS the resolution - each success EXECUTES a commit - so a
        // checkpoint's handles are walked with per-handle memory and
        // bounded retries on transport failure. A broker that is merely
        // unreachable gives no verdict, and falling back after SOME handles
        // committed would restore below intervals this walk just published
        // and replay them as duplicates; broker chaos overlapping a
        // recovery reaches exactly that interleaving. A broker that
        // ANSWERS "not committed" (fenced, timed out, refused) is final.
        constexpr int kTransportAttempts = 5;
        std::vector<bool> handle_committed(handles->size(), false);
        // A handle the broker finally REFUSED (fenced, wrong state) is
        // settled - not committed - and must not be re-marked unresolved:
        // the sink's pre-fence describe cannot out-know a broker verdict,
        // and re-judging it against later transaction generations could
        // invent a commit that never covered this checkpoint.
        std::vector<bool> handle_refused(handles->size(), false);
        // A commit receipt on disk is the sink's own durable record that the
        // broker acknowledged this subtask's commit for this checkpoint. It
        // is taken over the wire outright: the wire path can be fenced by a
        // successor producer, time out, or answer for a transaction that no
        // longer exists - none of which can retract a commit that already
        // happened (qual01-20260818b hit exactly that inversion, replaying
        // a committed interval on a fenced verdict).
        const auto marker_key = [&](std::size_t i) {
            return job_prefix + "receipts/" +
                   clink::connectors::commit_receipt_file_name((*handles)[i].subtask, id) +
                   ".unresolved";
        };
        const auto retire_marker = [&](std::size_t i) {
            // A resolved handle's unresolved marker (left by an earlier
            // failed episode of this walk) is stale the moment a verdict
            // lands; a lingering marker would make a LATER sink open
            // describe a transaction generations past the one it names.
            try {
                store->remove(marker_key(i));
            } catch (const std::exception&) {
                // Best-effort: the sink also deletes its marker after
                // acting on it, and a receipt outranks a marker.
            }
        };
        for (std::size_t i = 0; i < handles->size(); ++i) {
            if (store->exists(
                    job_prefix + "receipts/" +
                    clink::connectors::commit_receipt_file_name((*handles)[i].subtask, id))) {
                handle_committed[i] = true;
                retire_marker(i);
                clink::log::info("coordinator.recovery",
                                 "in-doubt resolution: checkpoint " + std::to_string(id) +
                                     " subtask " + std::to_string((*handles)[i].subtask) +
                                     ": commit receipt on disk; COMMITTED without a wire call");
            }
        }
        // Every handle still unresolved is persisted as an .unresolved
        // marker (body = the staged handle) next to the receipts. The
        // owning sink consumes it at its next open, BEFORE it fences:
        // fencing re-initialises the producer, which aborts an undecided
        // transaction and erases the coordinator state that could have
        // named a committed one - after that, nobody can ever know. The
        // marker is what lets the sink ask first (a read-only
        // DescribeTransactions) and turn "committed in the ack window"
        // into a receipt instead of a duplicate. Written even when the
        // walk was CANCELLED: it is the walk's mandated final act, a
        // local store write with no wire side, and skipping it is what
        // converts a bounded walk into an exactly-once hole
        // (qual01 rig-night composite: 3 keys' windows published twice).
        const auto persist_unresolved_markers = [&] {
            for (std::size_t i = 0; i < handles->size(); ++i) {
                if (handle_committed[i] || handle_refused[i]) {
                    continue;
                }
                try {
                    store->put(marker_key(i), (*handles)[i].handle);
                    clink::log::info(
                        "coordinator.recovery",
                        "in-doubt resolution: checkpoint " + std::to_string(id) + " subtask " +
                            std::to_string((*handles)[i].subtask) +
                            ": unresolved orphan marker written; the owning sink resolves it "
                            "before fencing");
                } catch (const std::exception& e) {
                    clink::log::error("coordinator.recovery",
                                      "in-doubt resolution: checkpoint " + std::to_string(id) +
                                          " subtask " + std::to_string((*handles)[i].subtask) +
                                          ": unresolved orphan marker could not be written (" +
                                          std::string(e.what()) +
                                          "); a restore below this checkpoint may replay an "
                                          "interval whose commit executed");
                }
            }
        };
        if (cancelled()) {
            persist_unresolved_markers();
            clink::metrics::orch::in_doubt_unresolved();
            return confirmed;
        }
        bool all_committed = false;
        bool verdict_failure = false;
        for (int attempt = 0; attempt < kTransportAttempts && !verdict_failure; ++attempt) {
            bool transport_hit = false;
            bool everything_done = true;
            // Probe EVERY handle, even after a final not-committed verdict.
            // The first cut broke out of this loop on the first refusal,
            // which left every handle after it unproven - and an unproven
            // commit gets no materialised receipt, so whether its interval
            // replayed as duplicates depended on the ORDER the handles came
            // back from the snapshot listing (qual01-20260819f's corner, at
            // one remove). A refusal still stops the checkpoint from
            // confirming; it must not stop the remaining commits from being
            // proven and receipted.
            for (std::size_t i = 0; i < handles->size(); ++i) {
                if (handle_committed[i]) {
                    continue;  // already executed; never re-resolve
                }
                if (cancelled()) {
                    verdict_failure = true;
                    everything_done = false;
                    break;  // no further probes; a probe EXECUTES a commit
                }
                const auto& handle = (*handles)[i].handle;
                std::string resolver_name;
                try {
                    resolver_name = clink::config::parse(handle).at("resolver").as_string();
                } catch (const std::exception& e) {
                    clink::log::warn(
                        "coordinator.recovery",
                        std::string("in-doubt resolution: handle did not parse: ") + e.what());
                    verdict_failure = true;
                    everything_done = false;
                    continue;
                }
                const auto resolver =
                    clink::connectors::TxnResumeRegistry::instance().find(resolver_name);
                if (!resolver.has_value()) {
                    clink::log::info("coordinator.recovery",
                                     "in-doubt resolution: no resolver registered for '" +
                                         resolver_name + "' (plugin not loaded in this process?)");
                    verdict_failure = true;
                    everything_done = false;
                    continue;
                }
                const auto result = (*resolver)(handle);
                if (cancelled()) {
                    // The probe was in flight when the deadline tripped; its
                    // answer must not be ACTED on - marking it committed or
                    // materialising its receipt is a store effect.
                    verdict_failure = true;
                    everything_done = false;
                    break;
                }
                clink::log::info("coordinator.recovery",
                                 "in-doubt resolution: checkpoint " + std::to_string(id) +
                                     " via '" + resolver_name + "': " +
                                     (result.committed                ? "COMMITTED"
                                      : result.transport_inconclusive ? "transport-inconclusive"
                                                                      : "not committed") +
                                     " (" + result.detail + ")");
                if (result.committed) {
                    handle_committed[i] = true;
                    // Materialise the receipt this commit never got to write.
                    // A commit proven over the wire has, by construction, no
                    // receipt on disk (the receipted case short-circuited
                    // above) - the ack window, or a commit this walk itself
                    // just executed. If resolution later stops on a SIBLING
                    // handle (the mixed verdict), the restore replays this
                    // subtask's interval, and replay suppression arms ONLY
                    // from receipts - so without this write the committed
                    // slice re-publishes as duplicates (qual01-20260819f:
                    // one subtask's whole window pane, twice). The horizon
                    // comes from the handle, staged at the sealing barrier
                    // with the exact value the sink's own receipt carries.
                    std::string wm;
                    try {
                        const auto j = clink::config::parse(handle);
                        if (j.contains("wm")) {
                            wm = j.at("wm").as_string();
                        }
                    } catch (const std::exception&) {
                        // fall through to the no-horizon path below
                    }
                    if (wm.empty()) {
                        clink::log::warn(
                            "coordinator.recovery",
                            "in-doubt resolution: checkpoint " + std::to_string(id) + " subtask " +
                                std::to_string((*handles)[i].subtask) +
                                ": handle carries no watermark horizon (staged by an older "
                                "binary); no receipt can be materialised, and a restore below "
                                "this checkpoint replays its interval under the bounded-replay "
                                "contract");
                    } else {
                        try {
                            store->put(job_prefix + "receipts/" +
                                           clink::connectors::commit_receipt_file_name(
                                               (*handles)[i].subtask, id),
                                       "wm=" + wm + "\n");
                            clink::log::info(
                                "coordinator.recovery",
                                "in-doubt resolution: checkpoint " + std::to_string(id) +
                                    " subtask " + std::to_string((*handles)[i].subtask) +
                                    ": receipt materialised by in-doubt resolution (wm=" + wm +
                                    ")");
                        } catch (const std::exception& e) {
                            clink::log::error(
                                "coordinator.recovery",
                                "in-doubt resolution: checkpoint " + std::to_string(id) +
                                    " subtask " + std::to_string((*handles)[i].subtask) +
                                    ": receipt could not be materialised (" +
                                    std::string(e.what()) +
                                    "); if resolution stops below this checkpoint, the replay "
                                    "of this interval will NOT be suppressed");
                        }
                    }
                    retire_marker(i);
                    continue;
                }
                if (!result.transport_inconclusive) {
                    // A FINAL wire refusal supersedes any stale marker: the
                    // sink's pre-fence describe could not out-know the
                    // broker's own verdict, and a marker surviving past it
                    // would be re-judged against a transaction coordinator
                    // state that later generations have moved on.
                    handle_refused[i] = true;
                    retire_marker(i);
                }
                everything_done = false;
                if (result.transport_inconclusive) {
                    transport_hit = true;
                    continue;  // other handles may reach different brokers
                }
                verdict_failure = true;
                continue;  // final refusal: keep proving the remaining handles
            }
            if (everything_done) {
                all_committed = true;
                break;
            }
            if (cancelled()) {
                break;
            }
            if (!verdict_failure && transport_hit && attempt + 1 < kTransportAttempts) {
                clink::log::info("coordinator.recovery",
                                 "in-doubt resolution: checkpoint " + std::to_string(id) +
                                     " has unreachable broker(s); retrying (attempt " +
                                     std::to_string(attempt + 2) + " of " +
                                     std::to_string(kTransportAttempts) + ")");
                std::this_thread::sleep_for(transport_retry_backoff);
            }
        }
        if (!all_committed) {
            // The job now falls back to the commit-confirmed contract: a
            // bounded replay rather than data loss. Counted, because that
            // is a DIFFERENT guarantee from the resolved path and nothing
            // else distinguishes them at the metrics layer.
            const auto committed_count = static_cast<std::size_t>(
                std::count(handle_committed.begin(), handle_committed.end(), true));
            if (committed_count > 0) {
                // The mixed-verdict corner, made loud: some of this
                // checkpoint's transactions are committed and the restore
                // below replays their intervals. Every commit resolved above
                // carries a receipt - written by the sink, or materialised
                // from the handle's watermark horizon at the moment it was
                // proven - so the replayed re-emissions are swallowed by the
                // restored sinks' replay suppression. Only a handle staged by
                // a pre-horizon binary (warned above) still reaches the
                // bounded-replay contract. Reaching this branch needs a
                // genuine mixed verdict or retries exhausted mid-outage -
                // both narrowed hard by the transport retries and by
                // teardown preserving prepared transactions.
                clink::log::error(
                    "coordinator.recovery",
                    "in-doubt resolution: checkpoint " + std::to_string(id) + ": " +
                        std::to_string(committed_count) + " of " + std::to_string(handles->size()) +
                        " handles committed before resolution failed; the restore below replays "
                        "the committed intervals, suppressed downstream by their (written or "
                        "materialised) receipts");
            }
            persist_unresolved_markers();
            mark_later_unreceipted(id);
            clink::metrics::orch::in_doubt_unresolved();
            return confirmed;
        }
        // Every handle of this checkpoint provably committed: publish the
        // confirmation durably, exactly as handle_commit_confirmed_ does,
        // so THIS and every later recovery selects it.
        if (cancelled()) {
            clink::metrics::orch::in_doubt_unresolved();
            mark_later_unreceipted(id);
            return confirmed;
        }
        try {
            store->put(job_prefix + "CONFIRMED-" + std::to_string(id),
                       "job=" + std::to_string(job_id) + "\ncheckpoint=" + std::to_string(id) +
                           "\nresolved=in-doubt\n");
        } catch (const std::exception& e) {
            clink::log::error("coordinator.recovery",
                              "in-doubt resolution: checkpoint " + std::to_string(id) +
                                  " committed but the CONFIRMED marker could not be written (" +
                                  std::string(e.what()) +
                                  "); stopping so the restore point never outruns its record");
            mark_later_unreceipted(id);
            return confirmed;
        }
        clink::metrics::orch::in_doubt_resolved();
        clink::log::info("coordinator.recovery",
                         "job_id=" + std::to_string(job_id) + " checkpoint " + std::to_string(id) +
                             " commit-CONFIRMED by in-doubt resolution");
        confirmed = id;
    }
    return confirmed;
}

}  // namespace clink::cluster
