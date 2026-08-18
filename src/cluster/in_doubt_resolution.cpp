#include "clink/cluster/in_doubt_resolution.hpp"

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <optional>
#include <thread>
#include <vector>

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

std::optional<CompletedMarkerInfo> read_completed_marker(const std::filesystem::path& marker) {
    std::ifstream in(marker);
    if (!in.is_open()) {
        return std::nullopt;
    }
    CompletedMarkerInfo out;
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

// The staged resume handles inside one checkpoint's snapshots: operator-state
// rows (0xFF-prefixed stored keys) whose logical key starts with
// kTxnResumeStateKeyPrefix. nullopt = a snapshot could not be read, which is
// different from "no handles" and must stop resolution.
std::optional<std::vector<std::string>> read_resume_handles(const std::string& checkpoint_dir,
                                                            const CompletedMarkerInfo& info,
                                                            std::uint64_t ckpt_id) {
    std::vector<std::string> handles;
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
                            handles.push_back(entry.value);
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
                                       std::chrono::milliseconds transport_retry_backoff) {
    if (checkpoint_dir.empty() || completed <= confirmed) {
        return confirmed;
    }
    const auto job_dir = completed_marker_dir_for(checkpoint_dir, job_id);
    for (std::uint64_t id = confirmed + 1; id <= completed; ++id) {
        const auto marker = job_dir / ("COMPLETED-" + std::to_string(id));
        std::error_code ec;
        if (!std::filesystem::exists(marker, ec)) {
            continue;  // this id never completed; its transaction was aborted
        }
        const auto info = read_completed_marker(marker);
        if (!info.has_value()) {
            clink::log::info("coordinator.recovery",
                             "in-doubt resolution: COMPLETED-" + std::to_string(id) +
                                 " carries no participant set; stopping at confirmed=" +
                                 std::to_string(confirmed));
            clink::metrics::orch::in_doubt_unresolved();
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
        bool all_committed = false;
        bool verdict_failure = false;
        for (int attempt = 0; attempt < kTransportAttempts && !verdict_failure; ++attempt) {
            bool transport_hit = false;
            bool everything_done = true;
            for (std::size_t i = 0; i < handles->size(); ++i) {
                if (handle_committed[i]) {
                    continue;  // already executed; never re-resolve
                }
                const auto& handle = (*handles)[i];
                std::string resolver_name;
                try {
                    resolver_name = clink::config::parse(handle).at("resolver").as_string();
                } catch (const std::exception& e) {
                    clink::log::warn(
                        "coordinator.recovery",
                        std::string("in-doubt resolution: handle did not parse: ") + e.what());
                    verdict_failure = true;
                    everything_done = false;
                    break;
                }
                const auto resolver =
                    clink::connectors::TxnResumeRegistry::instance().find(resolver_name);
                if (!resolver.has_value()) {
                    clink::log::info("coordinator.recovery",
                                     "in-doubt resolution: no resolver registered for '" +
                                         resolver_name + "' (plugin not loaded in this process?)");
                    verdict_failure = true;
                    everything_done = false;
                    break;
                }
                const auto result = (*resolver)(handle);
                clink::log::info("coordinator.recovery",
                                 "in-doubt resolution: checkpoint " + std::to_string(id) +
                                     " via '" + resolver_name + "': " +
                                     (result.committed                ? "COMMITTED"
                                      : result.transport_inconclusive ? "transport-inconclusive"
                                                                      : "not committed") +
                                     " (" + result.detail + ")");
                if (result.committed) {
                    handle_committed[i] = true;
                    continue;
                }
                everything_done = false;
                if (result.transport_inconclusive) {
                    transport_hit = true;
                    continue;  // other handles may reach different brokers
                }
                verdict_failure = true;
                break;
            }
            if (everything_done) {
                all_committed = true;
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
                // The bad corner, made loud: some of this checkpoint's
                // transactions are now committed and the restore below will
                // replay their intervals as duplicates. Reaching this needs
                // a genuine mixed verdict or retries exhausted mid-outage -
                // both narrowed hard by the transport retries and by
                // teardown preserving prepared transactions.
                clink::log::error(
                    "coordinator.recovery",
                    "in-doubt resolution: checkpoint " + std::to_string(id) + ": " +
                        std::to_string(committed_count) + " of " + std::to_string(handles->size()) +
                        " handles committed before resolution failed; the restore below WILL "
                        "replay the committed intervals as duplicates (bounded by this one "
                        "checkpoint)");
            }
            clink::metrics::orch::in_doubt_unresolved();
            return confirmed;
        }
        // Every handle of this checkpoint provably committed: publish the
        // confirmation durably, exactly as handle_commit_confirmed_ does,
        // so THIS and every later recovery selects it.
        try {
            clink::state::detail::write_string_fsync_rename(
                job_dir / ("CONFIRMED-" + std::to_string(id)),
                "job=" + std::to_string(job_id) + "\ncheckpoint=" + std::to_string(id) +
                    "\nresolved=in-doubt\n");
        } catch (const std::exception& e) {
            clink::log::error("coordinator.recovery",
                              "in-doubt resolution: checkpoint " + std::to_string(id) +
                                  " committed but the CONFIRMED marker could not be written (" +
                                  std::string(e.what()) +
                                  "); stopping so the restore point never outruns its record");
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
