#pragma once

// Phase 28e: io_uring-backed reactor for coroutine-driven async I/O.
//
// IoUringReactor owns one io_uring submission / completion ring pair
// and runs a single dispatch loop. Coroutines suspend on awaitables
// that submit SQEs (sleep, read, write, ...); when the kernel marks
// the corresponding CQE complete, the reactor resumes the awaiting
// coroutine handle on the loop thread.
//
// Threading model:
//   - The reactor's run() owns the loop thread.
//   - Awaitables submit SQEs from arbitrary threads (e.g., a
//     coroutine running inside AsyncLookupOperator's drive loop on
//     the operator thread). io_uring's SQ is single-producer when
//     IORING_SETUP_SINGLE_ISSUER is set; we DON'T set it, so any
//     thread can submit but the producer side is mutex-protected.
//   - Completion handling and coroutine resumption happen on the
//     reactor's loop thread. User coroutine code therefore needs to
//     be safe to resume from a different thread than it started on
//     - same constraint as any io_uring user.
//
// Linux-only. Header is unconditionally usable as a forward
// declaration but the constructor / methods are only DEFINED when
// CLINK_HAS_URING is set by CMake. On macOS / non-Linux this file
// compiles to an empty namespace.
//
// Scope of 28e:
//   - Reactor lifecycle (ctor / dtor / run / stop).
//   - sleep_for(duration) awaitable using IORING_OP_TIMEOUT.
//   - The completion bookkeeping (per-op state, SQE user_data, CQE
//     dispatch) is generic enough that adding read / write / accept
//     awaitables is "more of the same" without architecture changes.
//   - Tests cover sleep_for end-to-end; socket / file awaitables
//     land in a follow-on.

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <memory>

#if defined(__linux__) && defined(CLINK_HAS_URING)
#include <cerrno>

#include <sys/socket.h>
#endif

namespace clink::async {

#if defined(__linux__) && defined(CLINK_HAS_URING)

class IoUringReactor {
public:
    // queue_depth is the maximum number of in-flight SQEs the ring
    // can hold before submit() blocks. 256 is a sane default for
    // medium-concurrency workloads (per-request HTTP lookups,
    // streaming file reads).
    explicit IoUringReactor(unsigned queue_depth = 256);
    ~IoUringReactor();

    IoUringReactor(const IoUringReactor&) = delete;
    IoUringReactor& operator=(const IoUringReactor&) = delete;
    IoUringReactor(IoUringReactor&&) = delete;
    IoUringReactor& operator=(IoUringReactor&&) = delete;

    // Run the completion-dispatch loop. Blocks until stop() is
    // called on this reactor instance. Typical usage: spawn a
    // dedicated reactor thread that does `reactor.run()`, kick off
    // coroutines that co_await reactor.sleep_for(...) on the main
    // thread, wait for results, then call stop() + join the
    // reactor thread.
    void run();

    // Signal the loop to exit. Posts a NOP SQE so an in-flight
    // io_uring_wait_cqe wakes up and observes the stop flag. Safe
    // to call from any thread; idempotent.
    void stop();

    // Per-op state held alive across the suspend/resume gap. The
    // SQE's user_data points at one of these; on completion the
    // reactor reads cqe->res, copies it into result, and resumes
    // awaiting. The awaiter owns the allocation via unique_ptr.
    struct OperationState {
        std::coroutine_handle<> awaiting{};
        std::int32_t result{0};
        bool completed{false};
    };

    // Awaitable: suspend for `duration`. Resumes when the kernel
    // timer fires. The completion delivers the syscall result
    // (ETIME on success); the awaiter discards it - we treat any
    // completion as "duration elapsed."
    class SleepAwaiter {
    public:
        SleepAwaiter(IoUringReactor& r, std::chrono::nanoseconds d) : reactor_(r), duration_(d) {}

        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h);
        void await_resume() const noexcept {}

    private:
        IoUringReactor& reactor_;
        std::chrono::nanoseconds duration_;
        std::unique_ptr<OperationState> state_;
    };

    [[nodiscard]] SleepAwaiter sleep_for(std::chrono::milliseconds ms) {
        return SleepAwaiter{*this, std::chrono::duration_cast<std::chrono::nanoseconds>(ms)};
    }
    [[nodiscard]] SleepAwaiter sleep_for(std::chrono::nanoseconds ns) {
        return SleepAwaiter{*this, ns};
    }

    // Socket awaitables. All return an int matching the kernel's
    // semantics: non-negative on success (bytes read/written for
    // read/write, accepted fd for accept, 0 for connect), or
    // -errno on failure. Callers translate as appropriate.
    //
    // Convention: file descriptors are owned by the caller; the
    // awaiter does NOT close them on completion or destruction.
    // For read/write, buf must remain valid until the Task that
    // owns the awaiter completes (the kernel reads/writes into
    // it asynchronously). The awaitable holds a non-owning
    // pointer.
    class ReadAwaiter {
    public:
        ReadAwaiter(IoUringReactor& r, int fd, void* buf, unsigned len) noexcept
            : reactor_(r), fd_(fd), buf_(buf), len_(len) {}
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h);
        int await_resume() const noexcept { return state_ ? state_->result : -EINVAL; }

    private:
        IoUringReactor& reactor_;
        int fd_;
        void* buf_;
        unsigned len_;
        std::unique_ptr<OperationState> state_;
    };

    class WriteAwaiter {
    public:
        WriteAwaiter(IoUringReactor& r, int fd, const void* buf, unsigned len) noexcept
            : reactor_(r), fd_(fd), buf_(buf), len_(len) {}
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h);
        int await_resume() const noexcept { return state_ ? state_->result : -EINVAL; }

    private:
        IoUringReactor& reactor_;
        int fd_;
        const void* buf_;
        unsigned len_;
        std::unique_ptr<OperationState> state_;
    };

    class AcceptAwaiter {
    public:
        AcceptAwaiter(IoUringReactor& r, int listen_fd) noexcept : reactor_(r), fd_(listen_fd) {}
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h);
        int await_resume() const noexcept { return state_ ? state_->result : -EINVAL; }

    private:
        IoUringReactor& reactor_;
        int fd_;
        std::unique_ptr<OperationState> state_;
    };

    class ConnectAwaiter {
    public:
        ConnectAwaiter(IoUringReactor& r,
                       int sock_fd,
                       const sockaddr* addr,
                       socklen_t addr_len) noexcept
            : reactor_(r), fd_(sock_fd), addr_(addr), addr_len_(addr_len) {}
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h);
        int await_resume() const noexcept { return state_ ? state_->result : -EINVAL; }

    private:
        IoUringReactor& reactor_;
        int fd_;
        const sockaddr* addr_;
        socklen_t addr_len_;
        std::unique_ptr<OperationState> state_;
    };

    [[nodiscard]] ReadAwaiter read_async(int fd, void* buf, unsigned len) {
        return ReadAwaiter{*this, fd, buf, len};
    }
    [[nodiscard]] WriteAwaiter write_async(int fd, const void* buf, unsigned len) {
        return WriteAwaiter{*this, fd, buf, len};
    }
    [[nodiscard]] AcceptAwaiter accept_async(int listen_fd) {
        return AcceptAwaiter{*this, listen_fd};
    }
    [[nodiscard]] ConnectAwaiter connect_async(int sock_fd,
                                               const sockaddr* addr,
                                               socklen_t addr_len) {
        return ConnectAwaiter{*this, sock_fd, addr, addr_len};
    }

    // Diagnostic accessors for tests. completions_handled() counts
    // every CQE the loop processed (including the NOP wakeup from
    // stop()); ops_submitted() counts every SQE the reactor accepted.
    [[nodiscard]] std::uint64_t completions_handled() const noexcept {
        return completions_handled_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t ops_submitted() const noexcept {
        return ops_submitted_.load(std::memory_order_relaxed);
    }

private:
    // Pimpl: <liburing.h> stays out of the public header so callers
    // don't drag in the kernel headers and so the type compiles
    // when CLINK_HAS_URING is off (the pimpl pointer is nullptr).
    struct Impl;
    std::unique_ptr<Impl> impl_;

    std::atomic<bool> stop_{false};
    std::atomic<std::uint64_t> completions_handled_{0};
    std::atomic<std::uint64_t> ops_submitted_{0};

    // Submit a NOP SQE used by stop() to wake the loop. Public for
    // internal awaiters that need their own NOPs.
    void submit_nop_wakeup_();

    // Friend access for SleepAwaiter::await_suspend so it can
    // submit timeout SQEs without exposing the ring pointer.
    friend class SleepAwaiter;
};

#endif  // __linux__ && CLINK_HAS_URING

}  // namespace clink::async
