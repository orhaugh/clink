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
// executor, does this on every exit path including exceptions). A dispatch
// arriving after retirement is refused, which is safe by design: the prepared
// handle is still persisted in operator state, so the next restore re-commits
// it idempotently via the sink's recovery scan. Refusing an abort is likewise
// equivalent to a crash before the abort, which the same recovery reconciles.

#include <condition_variable>
#include <memory>
#include <mutex>

namespace clink::cluster {

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

}  // namespace clink::cluster
