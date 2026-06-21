#pragma once

#include <coroutine>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "clink/async/task.hpp"
#include "clink/core/types.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/state/schema_version.hpp"

namespace clink {

// A snapshot is the persisted form of a state backend at a given checkpoint.
// For the in-memory backend a snapshot is just an opaque byte blob; for a
// disk-backed backend it is a path/handle. The interface is intentionally
// pretty thin so multiple implementations are easy to support.
struct Snapshot {
    CheckpointId checkpoint_id{};
    std::vector<std::byte> bytes{};
};

// A CaptureHandle is the operator-thread half of an asynchronous
// checkpoint: a detached, point-in-time copy of a backend's state for one
// checkpoint id, produced by capture() WITHOUT touching durable storage.
// The snapshot worker later hands it to persist(), which performs the slow
// durable write off the operator thread. Only meaningful for backends
// whose supports_async_persist() returns true.
struct CaptureHandle {
    CheckpointId checkpoint_id{};
    std::vector<std::byte> bytes{};
};

// A half-open key-group range [first, last). Used by restore() to load
// only the subset of keys assigned to a new subtask after a rescale.
// The default {0, kNumKeyGroups} covers every group, which is what every
// restore-without-rescale wants. `covers_all()` lets backends short-
// circuit the filter check when the caller doesn't care.
struct KeyGroupRange {
    KeyGroup first{0};
    KeyGroup last{kNumKeyGroups};

    [[nodiscard]] bool covers_all() const noexcept { return first == 0 && last == kNumKeyGroups; }
    [[nodiscard]] bool contains(KeyGroup g) const noexcept { return g >= first && g < last; }
};

// StateBackend is the abstraction over keyed state storage.
//
// Keyed state is the primary form of state in a stream engine: every keyed
// operator (window, join, aggregate, custom) reads and writes per-key values.
// Operator state (broadcast, list) can be added later via separate methods.
//
// We model values as serialized byte strings; per-operator code is responsible
// for (de)serialization. This keeps the backend interface independent of the
// payload schema and matches the interface of RocksDB and other KV stores we
// expect to support.
class StateBackend {
public:
    using KeyView = std::string_view;
    using ValueView = std::string_view;
    using Value = std::vector<std::byte>;

    virtual ~StateBackend() = default;

    // Per-(operator, key) put/get/delete.
    virtual void put(OperatorId op, KeyView key, ValueView value) = 0;
    virtual std::optional<Value> get(OperatorId op, KeyView key) const = 0;
    virtual void erase(OperatorId op, KeyView key) = 0;

    // --- Asynchronous read (opt-in) -------------------------------------
    //
    // get_async is the non-blocking twin of get(): a lazy Task that, when
    // driven to completion, yields the same optional<Value> get() would.
    // The base default is `co_return get(op, key)`, so EVERY backend is
    // async-correct with no override and completes in a single resume (one
    // synchronous step, no suspension). A backend whose read can genuinely
    // block - a remote/disaggregated store - overrides this to suspend
    // while the I/O is outstanding and resume on completion, and returns
    // true from supports_async_get() so the runtime routes reads through
    // the asynchronous execution path instead of calling get() inline.
    //
    // Contract for a deferring override: the completion may resume on a
    // DIFFERENT thread than the caller, and the Task may outlive the
    // caller's `key` buffer (on the keyed-state path KeyView is a
    // non-owning view into thread_local scratch valid only for the call).
    // An override that suspends MUST copy the key bytes (and any value or
    // scratch it needs across the suspension) into the coroutine frame
    // before the first suspension point. The base default does NOT suspend,
    // so it uses `key` in place exactly like get().
    [[nodiscard]] virtual bool supports_async_get() const noexcept { return false; }
    virtual async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const {
        co_return get(op, key);
    }

    // Batched non-blocking read (ASYNC-10): yields one optional<Value> per
    // input key, positionally. The base default loops get_async, so every
    // backend is correct unchanged. A remote/disaggregated backend overrides
    // it to coalesce the COLD misses into ONE batched fetch and a SINGLE
    // suspension (cutting N remote round-trips toward one, and de-duplicating
    // identical-content objects), then scatters the results back. Keys are
    // owned std::strings (not KeyView): a batch must hold every key across the
    // fetch, so owning the bytes sidesteps the borrowed-view-across-suspension
    // hazard get_async carries. The caller still owns the `keys` vector for the
    // duration of the returned Task.
    virtual async::Task<std::vector<std::optional<Value>>> get_many_async(
        OperatorId op, const std::vector<std::string>& keys) const {
        std::vector<std::optional<Value>> out;
        out.reserve(keys.size());
        for (const auto& k : keys) {
            out.push_back(co_await get_async(op, KeyView{k}));
        }
        co_return out;
    }

    // Wired by the runner when it routes this backend through the async
    // execution path (it does so iff supports_async_get()). A deferring
    // backend posts a completed async read's suspended coroutine handle to
    // `schedule`, and the runner resumes it on the runner thread (via
    // AsyncExecutionController::schedule_resume). The default is a no-op:
    // the base get_async never suspends, so it never calls it. WITHOUT this
    // wiring a deferring get_async has no way to hand a completion back and
    // must fall back to an inline blocking load - so this is the link that
    // makes a disaggregated backend actually suspend in production. Set once
    // at wire-up, before any read; the runner clears it (passes {}) at
    // teardown before the controller is destroyed.
    using AsyncResumeScheduler = std::function<void(std::coroutine_handle<>)>;
    virtual void set_async_resume_scheduler(AsyncResumeScheduler /*schedule*/) {}

    // ASYNC-10 coalescing hook. A read-coalescing decorator (CoalescingBackend)
    // accumulates get_async reads into a pending batch instead of issuing them;
    // the runner calls this when the controller is otherwise stuck (and after a
    // process_async batch) to issue ONE get_many_async for the whole batch.
    // Returns true if it had reads to flush. The default is a no-op false: a
    // plain backend issues each get_async immediately and has nothing pending,
    // so the runner's call is harmless when the backend is not a coalescer.
    virtual bool flush_pending_reads() { return false; }

    // Operator (non-keyed) state: source offsets, broadcast slots - state
    // that has no key and therefore no key group. It is stored under the
    // same per-(operator, key) primitives but with a reserved leading byte
    // (kOperatorStateKeyPrefix) so the rescale restore filter never narrows
    // it: every subtask restores the whole value (broadcast/union
    // semantics), which is required for correctness - the subtask that
    // wrote a Kafka partition's offset need not be the one that owns it
    // after a rescale. Built on put/get/erase; backends need not override.
    void put_operator_state(OperatorId op, KeyView key, ValueView value) {
        put(op, prefix_operator_key_(key), value);
    }
    std::optional<Value> get_operator_state(OperatorId op, KeyView key) const {
        if (auto v = get(op, prefix_operator_key_(key)); v.has_value()) {
            return v;
        }
        // Migration: a checkpoint taken before operator-state prefixing
        // stored the raw key. Fall back so pre-existing checkpoints (and a
        // same-parallelism restore of them) still load.
        return get(op, key);
    }
    void erase_operator_state(OperatorId op, KeyView key) {
        erase(op, prefix_operator_key_(key));
        erase(op, key);  // also clear any pre-prefix (legacy) row
    }
    // Visit every operator-state entry under `op` (rows written via
    // put_operator_state), yielding the LOGICAL key (reserved prefix byte
    // stripped) and its value; keyed-state rows are skipped. Lets a source
    // that spreads its state across many operator-state keys (one row per
    // Kafka partition) reassemble it on restore.
    void scan_operator_state(OperatorId op,
                             const std::function<void(KeyView, ValueView)>& visit) const {
        scan(op, [&](KeyView key, ValueView value) {
            if (!key.empty() && static_cast<std::uint8_t>(key.front()) == kOperatorStateKeyPrefix) {
                visit(key.substr(1), value);
            }
        });
    }

    // Visit every (key, value) pair stored under `op`. Iteration order is
    // unspecified; backends may choose lexicographic, hash, or insertion
    // order. The visitor must not mutate the backend mid-iteration - make a
    // copy of any keys you intend to delete and erase them after the scan.
    using ScanVisitor = std::function<void(KeyView key, ValueView value)>;
    virtual void scan(OperatorId op, const ScanVisitor& visit) const = 0;

    // Snapshot the entire state backend at the given checkpoint id.
    // Implementations should make a consistent point-in-time copy.
    virtual Snapshot snapshot(CheckpointId id) = 0;

    // --- Asynchronous checkpoint split (opt-in) -------------------------
    //
    // A backend that returns true from supports_async_persist() may have
    // its snapshot() decomposed by the runtime into two phases:
    //   capture(id)  - cheap, runs on the operator thread, produces a
    //                  detached point-in-time blob (no durable I/O);
    //   persist(h)   - slow, runs off-thread on the snapshot worker, does
    //                  the durable write and returns what snapshot() would.
    // snapshot() must keep working as persist(capture(id)) so non-worker
    // call sites are unaffected. The non-negotiable invariant: a checkpoint
    // is only ever ack'd as durable AFTER persist() returns, never after
    // capture() alone. The default is false: snapshot() stays fully
    // synchronous on the operator thread.
    [[nodiscard]] virtual bool supports_async_persist() const noexcept { return false; }

    // Operator-thread phase: make a detached point-in-time copy of the
    // state for `id` without touching durable storage. Continued
    // put/get/erase on the live backend after this returns must not alter
    // the captured bytes. Only called when supports_async_persist() is true.
    virtual CaptureHandle capture(CheckpointId /*id*/) {
        throw std::runtime_error("StateBackend::capture: async persist not supported");
    }

    // Worker-thread phase: durably persist a handle produced by capture().
    // Returns the Snapshot that snapshot() would have. Must be safe to run
    // off the operator thread concurrently with put/get/erase on the live
    // backend (it touches only the captured bytes and its own storage).
    virtual Snapshot persist(CaptureHandle /*handle*/) {
        throw std::runtime_error("StateBackend::persist: async persist not supported");
    }

    // Restore from a snapshot produced by a previous run. Only required at
    // startup; ongoing checkpoints do not call this.
    //
    // `kg_filter` controls which key groups are loaded into this backend
    // instance. The default {0, kNumKeyGroups} accepts every group, which
    // is what plain restore-on-resubmit wants. Rescale sets a narrower
    // range so each new subtask reads only its assigned slice of the
    // parent snapshot. Backends MAY honour the filter as an optimisation
    // (skipping out-of-range keys at decode time) or post-filter after
    // load; semantically the result must be: only keys whose key-group
    // prefix byte falls inside [first, last) are present.
    virtual void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) = 0;

    // Combine N snapshots produced by THIS backend's snapshot() into one
    // its own restore() can consume. Used by the changelog scale-down path
    // to fold several parent subtasks' materializations into one new
    // subtask. The default supports only the trivial single-part case;
    // backends that support multi-source restore (InMemory merges the IPC
    // streams, RocksDB joins the checkpoint dirs) override this.
    virtual Snapshot combine_snapshots(std::vector<Snapshot> parts) const {
        if (parts.size() == 1) {
            return std::move(parts.front());
        }
        throw std::runtime_error(
            "StateBackend::combine_snapshots: this backend does not support "
            "multi-source (scale-down) restore");
    }

    // Drop the on-disk artefacts of a previously-taken checkpoint (a
    // checkpoint dir, a snapshot file). Called by the retention manager
    // once a newer checkpoint has been globally completed and the old one
    // is no longer needed for recovery, so checkpoint storage stays
    // bounded. Must be safe to call with an unknown id (no-op) and must
    // not touch the live working state. The default is a no-op for
    // in-memory backends that keep no per-checkpoint artefacts.
    virtual void purge_checkpoint(CheckpointId /*id*/) {}

    // Phase 27b: record the codec version for each (operator, state_type)
    // before the next snapshot fires. The default is to ignore - backends
    // that support versioned state override these. snapshot() writes the
    // map into its serialized format; restore() repopulates it so the
    // job-control plane can compare savepoint versions against live
    // expectations and route bytes through migrations when they differ.
    virtual void set_state_versions(StateVersionMap /*versions*/) {}
    virtual StateVersionMap restored_state_versions() const { return {}; }

    // FOUND-3 (relocatable savepoints): tell the backend where the savepoint's
    // artefacts now live, so restore() can rebase references that embed a
    // capture-time absolute path. Default ignore - backends whose handles are
    // already location-independent (in-memory, object-store) need nothing.
    virtual void set_restore_base(const std::string& /*dir*/) {}

    virtual std::string description() const = 0;

private:
    // Build the storage key for an operator-state entry: the reserved
    // prefix byte followed by the caller's raw key. Returned by value; the
    // temporary outlives the put/get call it is passed to.
    static std::string prefix_operator_key_(KeyView key) {
        std::string out;
        out.reserve(key.size() + 1);
        out.push_back(static_cast<char>(kOperatorStateKeyPrefix));
        out.append(key);
        return out;
    }
};

}  // namespace clink
