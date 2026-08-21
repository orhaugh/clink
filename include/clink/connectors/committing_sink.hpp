#pragma once

// CommittingSink<In, Committable> - the generic exactly-once sink base.
//
// It owns the two-phase-commit choreography and the durable bookkeeping that
// every exactly-once sink otherwise hand-rolls: persisting a per-checkpoint
// "committable" into operator state at the barrier, finalising it when the
// Coordinator confirms the checkpoint is globally durable, rolling it back on
// abort, and re-committing anything left prepared-but-uncommitted after a
// crash. A connector supplies only the verbs; the base supplies the protocol.
//
// The lifecycle a connector implements:
//   on_open()                    - create resources / init an external txn.
//   write(batch)                 - buffer or stage the records.
//   prepare_commit(ckpt)         - flush to a durable-but-uncommitted state and
//                                  return a Committable handle (a staging path,
//                                  a global txn id, a multipart handle, ...).
//                                  Return std::nullopt if there is nothing to
//                                  commit for this checkpoint.
//   commit(committable)          - finalise atomically. MUST be idempotent: a
//                                  recovery-time or duplicate commit may fire.
//   abort(committable)           - roll back. MUST be idempotent.
//   recover(committable)         - finalise a handle found still-pending at
//                                  startup. Defaults to commit(committable).
//   serialize/deserialize        - the codec for the persisted handle.
//
// What the base owns (sealed - a connector cannot override the choreography):
//   open()        -> on_open(); then recover_all_().
//   on_data(b)    -> write(b).
//   on_barrier(b) -> prepare_commit(); persist the handle keyed by checkpoint.
//   on_commit(id) -> load handle; commit(); erase key. Idempotent (a missing
//                    key means already committed).
//   on_abort(id)  -> load handle; abort(); erase key. Idempotent.
//
// State layout: the handle is persisted via operator state (NOT keyed state) so
// a rescale restores every subtask's pending set (broadcast/union semantics),
// under the logical key "_xo_pending_sub<N>_<ckpt>". The barrier snapshot
// captures the put made inside on_barrier, so a persisted handle survives a
// crash and is replayed by recover_all_() at the next open().
//
// Commit-group and the coordinator round-trip are unchanged: a CommittingSink still
// participates in set_commit_group() and its on_commit fires from the worker's
// CommitCheckpoint handling exactly like any other sink.
//
// THREADING - the contract a connector's verbs must survive: write(),
// prepare_commit() and close() run on the subtask's task thread, while
// commit(), abort() and recover() may run on the worker's dispatch thread
// CONCURRENTLY with the task thread preparing the NEXT interval. A connector
// whose client handle is not thread-safe must serialise access itself - a
// libpq connection shared unsynchronised between a barrier's PREPARE and a
// dispatched COMMIT PREPARED desyncs the wire protocol and blocks both
// threads forever (found by the Postgres exactly-once suite; see the
// conn_mu_ in postgres_json_sink_2pc.hpp). Filesystem-rename sinks and
// thread-safe clients (AWS SDK, librdkafka) need nothing extra.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/core/types.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

template <typename In, typename Committable>
class CommittingSink : public Sink<In> {
public:
    explicit CommittingSink(std::uint32_t subtask_idx = 0) : subtask_idx_(subtask_idx) {}

    // --- connector-supplied verbs -------------------------------------------

    // Create resources / initialise an external transaction. Runs before the
    // recovery scan so recovery has what it needs (dirs exist, txn manager is
    // connected). Default no-op.
    virtual void on_open() {}

    // Buffer or stage the incoming records. Called for every data batch.
    virtual void write(const Batch<In>& batch) = 0;

    // Flush to a durable-but-uncommitted state and return the handle to
    // finalise later, or std::nullopt when there is nothing to commit for this
    // checkpoint (e.g. no records flowed since the last barrier).
    virtual std::optional<Committable> prepare_commit(std::uint64_t checkpoint_id) = 0;

    // Finalise the prepared transaction. MUST be idempotent. Returning false
    // is reserved for future retry policy; today the base always erases the key
    // after commit() returns, so treat commit() as authoritative.
    virtual bool commit(const Committable& committable) = 0;

    // Roll back the prepared transaction. MUST be idempotent. Default no-op for
    // sinks whose prepared state expires on its own.
    virtual void abort(const Committable& /*committable*/) {}

    // Finalise a handle found still-pending at startup. The default re-commits;
    // override only if recovery differs from a normal commit.
    virtual void recover(const Committable& committable) { commit(committable); }

    // Codec for the persisted handle. serialize(x) then deserialize(...) must
    // round-trip across DIFFERENT sink instances (the producer crashes, a fresh
    // instance recovers), so keep it self-contained - no instance state.
    virtual std::string serialize(const Committable& committable) const = 0;
    virtual Committable deserialize(std::string_view bytes) const = 0;

    // --- framework choreography (sealed) ------------------------------------

    void open() final {
        on_open();
        recover_all_();
    }

    void on_data(const Batch<In>& batch) final { write(batch); }

    // This sink stages its prepared-transaction handle into operator state during
    // on_barrier, so the checkpoint must capture it. See Sink::stages_state_at_barrier.
    [[nodiscard]] bool stages_state_at_barrier() const noexcept final { return true; }

    void on_barrier(CheckpointBarrier b) final {
        const auto ckpt = b.id().value();
        // Fault windows around prepare. Dying BEFORE prepare must leave no
        // external side effect at all; dying AFTER prepare but before the
        // handle is persisted must leave a prepared transaction that
        // nothing will ever commit - which is why the persist follows
        // immediately and recover_all_() exists.
        CLINK_FAULT_POINT(clink::fault::points::kSinkBeforePrepare);
        auto committable = prepare_commit(ckpt);
        if (!committable.has_value())
            return;  // nothing to commit for this checkpoint
        CLINK_FAULT_POINT(clink::fault::points::kSinkAfterPrepare);
        auto* state = state_backend_();
        if (state == nullptr)
            return;
        const std::string blob = serialize(*committable);
        state->put_operator_state(
            this->id(), state_key_(ckpt), StateBackend::ValueView{blob.data(), blob.size()});
    }

    // The persisted-handle count, read by the sink runner's end-of-stream
    // gate. The scan runs on the task thread while a commit dispatch may be
    // finalising on the worker's dispatch thread - the same concurrency the
    // barrier path already lives with (see the threading note above).
    [[nodiscard]] std::size_t staged_commits_outstanding() const final {
        std::size_t n = 0;
        auto* state = state_backend_();
        if (state == nullptr)
            return 0;
        const std::string prefix = key_prefix_();
        state->scan_operator_state(this->id(),
                                   [&](StateBackend::KeyView k, StateBackend::ValueView) {
                                       const std::string key{k};
                                       if (key.rfind(prefix, 0) == 0)
                                           ++n;
                                   });
        return n;
    }

    void on_commit(std::uint64_t checkpoint_id) final {
        finalise_(checkpoint_id, /*is_commit=*/true);
    }

    void on_abort(std::uint64_t checkpoint_id) final {
        finalise_(checkpoint_id, /*is_commit=*/false);
    }

protected:
    std::uint32_t subtask_idx() const noexcept { return subtask_idx_; }

    // The committables currently persisted (prepared-but-unfinalised) for this
    // sink, deserialized. Intended for a connector that must reconcile an
    // external registry at on_open() - e.g. an XA sink that rolls back prepared
    // transactions NOT in this set (their checkpoint never became durable).
    // Safe to call from on_open(): recover_all_() has not yet run, so the full
    // pending set is still present.
    std::vector<Committable> pending_committables() const {
        std::vector<Committable> out;
        auto* state = state_backend_();
        if (state == nullptr)
            return out;
        const std::string prefix = key_prefix_();
        state->scan_operator_state(this->id(),
                                   [&](StateBackend::KeyView k, StateBackend::ValueView v) {
                                       const std::string key{k};
                                       if (key.rfind(prefix, 0) != 0)
                                           return;
                                       out.push_back(deserialize(v));
                                   });
        return out;
    }

    // One-time upgrade bridge for sinks migrated from the pre-framework 2PC
    // implementation, which persisted the committable via a RAW put() under
    // "<legacy_prefix>sub<N>_<ckpt>" (no operator-state reserved byte, so the
    // base's recover-at-open scan does not see it). Call from on_open(): it
    // finalises each legacy handle through the same recover() verb and clears
    // the raw key. Handles written by this framework use the operator-state
    // path and are picked up by recover_all_() instead, so the two never
    // double-process (disjoint key spaces). A no-op when there is nothing left
    // from an older binary, so it is safe to leave in place indefinitely.
    void recover_legacy_handles(std::string_view legacy_prefix) {
        auto* state = state_backend_();
        if (state == nullptr)
            return;
        const std::string prefix = std::string(legacy_prefix) + sub_prefix_(subtask_idx_) + "_";
        std::vector<std::string> keys;
        std::vector<std::string> blobs;
        state->scan(this->id(), [&](StateBackend::KeyView k, StateBackend::ValueView v) {
            const std::string key{k};
            if (key.rfind(prefix, 0) != 0)
                return;  // not a legacy handle for this subtask (skips new-format keys)
            keys.push_back(key);
            blobs.emplace_back(v);
        });
        for (std::size_t i = 0; i < keys.size(); ++i) {
            recover(deserialize(blobs[i]));
            state->erase(this->id(), keys[i]);  // raw erase (matches the legacy raw put)
        }
    }

private:
    static std::string sub_prefix_(std::uint32_t sub) { return "sub" + std::to_string(sub); }
    std::string key_prefix_() const { return "_xo_pending_" + sub_prefix_(subtask_idx_) + "_"; }
    std::string state_key_(std::uint64_t ckpt) const {
        return key_prefix_() + std::to_string(ckpt);
    }

    StateBackend* state_backend_() const noexcept {
        return this->runtime() != nullptr ? this->runtime()->state_backend() : nullptr;
    }

    static std::string blob_of_(const StateBackend::Value& v) {
        return std::string(reinterpret_cast<const char*>(v.data()), v.size());
    }

    void finalise_(std::uint64_t checkpoint_id, bool is_commit) {
        auto* state = state_backend_();
        if (state == nullptr)
            return;
        const auto key = state_key_(checkpoint_id);
        auto stored = state->get_operator_state(this->id(), key);
        if (!stored.has_value())
            return;  // idempotent: already finalised
        const Committable committable = deserialize(blob_of_(*stored));
        if (is_commit) {
            // The three windows that decide whether 2PC actually holds:
            //
            //   before_commit          the handle is persisted and nothing
            //                          external has happened. A death here
            //                          must be recovered by recover_all_().
            //   during_commit          inside the external commit. Whether
            //                          it took effect is unknown, so the
            //                          recovery commit MUST be idempotent.
            //   after_external_commit  the external system has committed
            //                          but the local handle is still
            //                          persisted. Recovery will commit
            //                          AGAIN - the case that only an
            //                          idempotent commit() survives, and
            //                          the one the brief singles out.
            CLINK_FAULT_POINT(clink::fault::points::kSinkBeforeCommit);
            commit(committable);
            CLINK_FAULT_POINT(clink::fault::points::kSinkAfterExternalCommit);
        } else {
            abort(committable);
        }
        state->erase_operator_state(this->id(), key);
    }

    // Walk operator state for "_xo_pending_<sub>_*" handles left prepared by a
    // previous run and finalise each. Collect first, then finalise + erase (the
    // scan visitor must not mutate the backend mid-iteration).
    void recover_all_() {
        auto* state = state_backend_();
        if (state == nullptr)
            return;
        const std::string prefix = key_prefix_();
        std::vector<std::string> keys;
        std::vector<std::string> blobs;
        state->scan_operator_state(this->id(),
                                   [&](StateBackend::KeyView k, StateBackend::ValueView v) {
                                       const std::string key{k};
                                       if (key.rfind(prefix, 0) != 0)
                                           return;
                                       keys.push_back(key);
                                       blobs.emplace_back(v);
                                   });
        // The count is the forensic witness for restore-time recovery: a
        // restore that should have re-committed a prepared handle but logs
        // zero here lost the handle BEFORE this sink opened. The sink's
        // name is deliberate evidence too: it fires at every open, so a
        // qualification campaign can prove the 2PC family is actually
        // deployed by grepping for it - QUAL-02's gate had been matching
        // the Postgres sink's TLS warning, a config-dependent accident
        // that the S3 sink does not reproduce.
        clink::log::info("sink.2pc",
                         "recover_all [" + this->name() + "] sub" + std::to_string(subtask_idx_) +
                             ": " + std::to_string(keys.size()) +
                             " prepared handle(s) to finalise");
        for (std::size_t i = 0; i < keys.size(); ++i) {
            clink::log::info("sink.2pc", "recovering prepared handle '" + keys[i] + "'");
            recover(deserialize(blobs[i]));
            state->erase_operator_state(this->id(), keys[i]);
        }
    }

    std::uint32_t subtask_idx_;
};

}  // namespace clink
