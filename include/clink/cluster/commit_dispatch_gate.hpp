#pragma once

// CommitDispatchGate - serialises CommitCheckpoint/AbortCheckpoint dispatch
// against subtask-runner teardown.
//
// The worker dispatches a sink's commit callback on its reader thread, outside
// any lock, while the subtask runner owns the LocalExecutor on its own thread.
// The callback weak-captures the sink, which pins the SINK object alive - but
// not the RuntimeContext and StateBackend the executor owns. A slow external
// commit (a held broker transaction, a SQL COMMIT PREPARED over a slow link)
// overlapping a cancel-for-restart therefore ran CommittingSink::finalise_
// against a freed backend and took the whole worker down with SIGSEGV (found
// by the mid-commit worker-kill integration test in test_fault_recovery.cpp).
//
// The protocol: the worker wraps every registered commit/abort callback so it
// enters the gate for the duration of the dispatch, and the runner retires the
// gate - blocking until in-flight dispatch drains - strictly BEFORE its
// executor is destroyed (CommitDispatchRetirer, constructed after the
// executor, does this on every exit path including exceptions).
//
// A dispatch arriving after retirement is REFUSED, and a refusal must be
// reported as a failure to commit - never as a commit. The refusal is safe
// only because the prepared handle is still persisted, so a later restore
// re-commits it idempotently; that argument collapses the moment the refused
// checkpoint is CONFIRMED, because the restore point then advances past the
// very transaction that never committed and nothing will ever replay it.
//
// It collapsed exactly that way. The worker's dispatch loop treated a
// silently-refused callback as success and sent CommitConfirmed for it, the
// coordinator wrote CONFIRMED-N and moved the restore point on, and the
// transactional sink's output stopped becoming visible while the job went on
// reporting RUNNING with advancing checkpoints. QUAL-01 measured that as an
// output topic frozen at 955,647 records with a generator still producing at
// 1,997 events a second.
//
// So refusal throws. A caller that dispatches through gated_dispatch cannot
// mistake it for a commit, because the only way to observe "it ran" is the
// absence of an exception. Refusing an abort is likewise equivalent to a
// crash before the abort, which the same recovery reconciles.

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

namespace clink::cluster {

// A commit/abort dispatch that arrived after its runner retired.
//
// A distinct type, not a bare runtime_error, so the dispatch loop can say
// which of the two things happened: the sink tried to commit and failed, or
// the sink was never asked. Both must prevent confirmation; only one is a
// sink problem.
class CommitDispatchRefused : public std::runtime_error {
public:
    explicit CommitDispatchRefused(std::uint64_t checkpoint_id)
        : std::runtime_error("commit/abort dispatch for checkpoint " +
                             std::to_string(checkpoint_id) + " arrived after runner retirement"),
          checkpoint_id_(checkpoint_id) {}
    [[nodiscard]] std::uint64_t checkpoint_id() const { return checkpoint_id_; }

private:
    std::uint64_t checkpoint_id_;
};

class CommitDispatchGate {
public:
    // Returns false once the gate is retired; the caller must then skip the
    // dispatch. On true, the caller owns one in-flight entry and must leave().
    bool try_enter() {
        std::lock_guard lk(mu_);
        if (retired_) {
            return false;
        }
        ++in_flight_;
        return true;
    }

    void leave() {
        {
            std::lock_guard lk(mu_);
            --in_flight_;
        }
        cv_.notify_all();
    }

    // Marks the gate retired and blocks until every in-flight dispatch has
    // left. After this returns, no callback is executing and none can start,
    // so the executor behind them is safe to destroy.
    void retire_and_drain() {
        std::unique_lock lk(mu_);
        retired_ = true;
        cv_.wait(lk, [&] { return in_flight_ == 0; });
    }

private:
    std::mutex mu_;
    std::condition_variable cv_;
    int in_flight_{0};
    bool retired_{false};
};

// RAII retirement. Construct AFTER the LocalExecutor so reverse destruction
// order retires the gate (draining in-flight dispatch) before the executor -
// and the RuntimeContext + StateBackend it owns - is destroyed. A null gate
// (in-process paths that never register commit callbacks) is a no-op.
class CommitDispatchRetirer {
public:
    explicit CommitDispatchRetirer(std::shared_ptr<CommitDispatchGate> gate)
        : gate_(std::move(gate)) {}
    ~CommitDispatchRetirer() {
        if (gate_) {
            gate_->retire_and_drain();
        }
    }
    CommitDispatchRetirer(const CommitDispatchRetirer&) = delete;
    CommitDispatchRetirer& operator=(const CommitDispatchRetirer&) = delete;
    CommitDispatchRetirer(CommitDispatchRetirer&&) = delete;
    CommitDispatchRetirer& operator=(CommitDispatchRetirer&&) = delete;

private:
    std::shared_ptr<CommitDispatchGate> gate_;
};

// Wrap a commit/abort callback so it runs under the gate.
//
// The one place the enter/leave protocol is implemented, so a caller cannot
// get it subtly wrong - and so refusal always throws rather than returning
// quietly, which is the difference between "this checkpoint did not commit"
// and "this checkpoint committed".
template <class Fn>
auto gated_dispatch(std::shared_ptr<CommitDispatchGate> gate, Fn fn) {
    return [gate = std::move(gate), fn = std::move(fn)](std::uint64_t ckpt) {
        if (!gate->try_enter()) {
            throw CommitDispatchRefused(ckpt);
        }
        struct Leave {
            CommitDispatchGate* g;
            ~Leave() { g->leave(); }
        } leave{gate.get()};
        fn(ckpt);
    };
}

}  // namespace clink::cluster
