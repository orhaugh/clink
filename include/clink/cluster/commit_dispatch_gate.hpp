#pragma once

// CommitDispatchGate - serialises CommitCheckpoint/AbortCheckpoint dispatch
// against subtask-runner teardown.
//
// The worker dispatches a sink's commit callback on its commit-dispatch thread
// (an abort on its reader thread), outside any lock, while the subtask runner
// owns the LocalExecutor on its own thread. The callback weak-captures the
// sink, which pins the SINK object alive - but not the RuntimeContext and
// StateBackend the executor owns. A slow external commit (a held broker
// transaction, a SQL COMMIT PREPARED over a slow link) overlapping a
// cancel-for-restart therefore ran CommittingSink::finalise_ against a freed
// backend and took the whole worker down with SIGSEGV (found by the mid-commit
// worker-kill integration test in test_fault_recovery.cpp).
//
// The protocol: the worker wraps every registered commit/abort callback so it
// enters the gate for the duration of the dispatch, and the gate is retired -
// blocking until in-flight dispatch drains - in two places. The code that
// registers a sink's callbacks installs the gate as the sink's commit-dispatch
// fence (Sink::set_commit_dispatch_fence), and the sink's runner retires it as
// the sink's teardown begins: after the end-of-stream wait for staged commits,
// before close() or close_cancelled(), and on an exit by exception. The drain
// waits for a dispatch that is inside its external call; one that has not
// started it sees Sink::teardown_begun() and declines, so a store that is
// down cannot hold the teardown through a commit it was never asked. Then the
// runner retires it again strictly BEFORE its executor is destroyed
// (CommitDispatchRetirer, constructed after the executor, does this on every
// exit path including exceptions), the backstop for an executor whose sink
// runner never ran.
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
//
// The same rule broke again from the other side, which is why the sink's
// teardown retires the gate. With only the executor-time retirement, the
// sink's runner closed the sink and detached its runtime while the gate
// stayed open, and a commit dispatched into that window - a CancelJob or a
// lost coordinator session racing a CommitCheckpoint - reached a sink with no
// state backend. CommittingSink::finalise_ returned without committing and
// without throwing, so the callback looked like a commit that ran and the
// worker confirmed it. A commit already running when the teardown began ran
// on against a sink being closed under it.

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

namespace clink::cluster {

// A commit/abort dispatch that arrived after its gate retired.
//
// A distinct type, not a bare runtime_error, so the dispatch loop can say
// which of the two things happened: the sink tried to commit and failed, or
// the sink was never asked. Both must prevent confirmation; only one is a
// sink problem.
class CommitDispatchRefused : public std::runtime_error {
public:
    explicit CommitDispatchRefused(std::uint64_t checkpoint_id)
        : std::runtime_error("commit/abort dispatch for checkpoint " +
                             std::to_string(checkpoint_id) +
                             " arrived after its task began teardown"),
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

    // Whether this gate has been retired. A gate never un-retires, so a
    // callback behind a retired gate can never run again and dispatching to
    // it can only ever produce a refusal - which blocks confirmation for the
    // whole subtask, including any live callback registered alongside it.
    [[nodiscard]] bool retired() {
        std::lock_guard lk(mu_);
        return retired_;
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
// and the RuntimeContext + StateBackend it owns - is destroyed. A sink's
// runner has normally retired it already, at the start of the sink's
// teardown; retiring twice is harmless. A null gate (in-process paths that
// never register commit callbacks) is a no-op.
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

// A commit/abort callback together with the gate that owns it.
//
// The pairing is what lets a dispatch DROP entries whose gate has retired.
// A gate never un-retires, so such an entry can only ever refuse - and one
// refusal blocks confirmation for its whole subtask, including a live
// callback the replacement runner registered beside it. A subtask whose
// teardown hangs never reaches the code that removes its own registrations,
// so without pruning it stops the job confirming anything, permanently.
struct GatedCallback {
    std::shared_ptr<CommitDispatchGate> gate;  // null on in-process paths
    std::function<void(std::uint64_t /*checkpoint_id*/)> fn;
    void operator()(std::uint64_t ckpt) const { fn(ckpt); }
    [[nodiscard]] bool dead() const { return gate && gate->retired(); }
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
