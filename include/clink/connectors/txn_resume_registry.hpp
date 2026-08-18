#pragma once

// In-doubt transaction resolution: the seam between the coordinator's
// restore-point selection and connector-specific commit recovery.
//
// A sink on the commit-confirmed protocol (ConnectorCapabilities::
// commit_recoverable == false) can die between a checkpoint COMPLETING and
// its external commit executing. The coordinator then restores from the
// last CONFIRMED checkpoint and replays the interval - correct, but a
// bounded duplicate. Some of those orphaned transactions ARE finalisable
// with connector knowledge the coordinator does not have (Kafka: an EndTxn
// sent with the dead producer's identity BEFORE any successor initialises).
//
// The contract:
//   * The SINK stages a self-describing handle into operator state at
//     every barrier, under kTxnResumeStateKeyPrefix + its subtask, and
//     erases it once the commit provably executed. The handle therefore
//     travels inside the checkpoint snapshot - durable exactly when the
//     checkpoint is, with no extra wire or file format.
//   * The handle is JSON whose "resolver" field names the registered
//     resolver; everything else in it belongs to the connector.
//   * At recovery, BEFORE choosing the restore point, the coordinator
//     reads the handles out of each completed-but-unconfirmed checkpoint
//     (oldest first) and calls the resolver. Only if EVERY handle of a
//     checkpoint reports committed=true does the coordinator write
//     CONFIRMED for it and let the restore point advance - committing an
//     orphan while still restoring from before it would DUPLICATE its
//     interval, which is why resolution and restore-point selection are
//     one decision, not a sink-side afterthought.
//   * Any failure, missing resolver, or missing handle stops resolution
//     cold and leaves the current contract (bounded replay) in force.
//     committed=true on a transaction that did not commit recreates the
//     false-confirm defect of F-series memory; resolvers must treat
//     anything but an explicit broker acknowledgement as failure.
//
// Resolvers register at connector install() (the plugin model runs
// install in every process, so a coordinator that loaded the job's
// plugins has them).

#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

namespace clink::connectors {

// Logical operator-state key prefix (the state backend adds its own
// reserved 0xFF operator-state byte in front). The sink appends its
// subtask index so parallel sinks stage distinct keys.
inline constexpr std::string_view kTxnResumeStateKeyPrefix = "_clink_txn_resume_";

// Commit-receipt file name inside the job's receipts directory
// (commit_receipt_dir_for in clink/cluster/in_doubt_resolution.hpp): the
// sink's durable record that ITS external commit for checkpoint `ckpt`
// executed. Written by the sink right after the broker acknowledged the
// commit, read by the resolution walk (a receipted handle is COMMITTED with
// no wire call) and by the sink's own restore path (receipts newer than the
// restore point mean the replayed interval is already published, so its
// re-emissions are suppressed). One 2PC sink per subtask - the same
// constraint the staged-handle key above already imposes.
inline std::string commit_receipt_file_name(std::uint32_t subtask_idx, std::uint64_t ckpt) {
    return "sub" + std::to_string(subtask_idx) + "-" + std::to_string(ckpt);
}

struct InDoubtResolution {
    bool committed{false};
    std::string detail;
    // True when the resolver could not reach a broker AT ALL: no verdict
    // exists, as opposed to a broker answering "no" (fenced, timed out,
    // refused). The distinction matters because resolution EXECUTES commits
    // handle by handle - a transport failure part-way through a checkpoint,
    // treated as a verdict, would restore below intervals the walk just
    // committed and replay them as duplicates. Transport failures are
    // retried in place; verdicts are final.
    bool transport_inconclusive{false};
};

// handle_json is the staged handle verbatim.
using InDoubtResolver = std::function<InDoubtResolution(const std::string& handle_json)>;

class TxnResumeRegistry {
public:
    static TxnResumeRegistry& instance() {
        static TxnResumeRegistry r;
        return r;
    }

    // Latest registration wins, matching the factory registries: a
    // re-installed plugin replaces its own resolver.
    void register_resolver(std::string name, InDoubtResolver fn) {
        std::lock_guard lock(mu_);
        resolvers_[std::move(name)] = std::move(fn);
    }

    [[nodiscard]] std::optional<InDoubtResolver> find(const std::string& name) const {
        std::lock_guard lock(mu_);
        const auto it = resolvers_.find(name);
        if (it == resolvers_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

private:
    mutable std::mutex mu_;
    std::map<std::string, InDoubtResolver> resolvers_;
};

}  // namespace clink::connectors
