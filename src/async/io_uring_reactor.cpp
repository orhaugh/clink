// io_uring reactor implementation.
//
// Linux-only. Built conditionally by CMakeLists.txt when liburing is
// present; the header guards its declarations with the same macros
// so a non-Linux build sees an empty namespace and no missing
// references.

#include "clink/async/io_uring_reactor.hpp"

#if defined(__linux__) && defined(CLINK_HAS_URING)

#include <cerrno>
#include <cstring>
#include <liburing.h>
#include <mutex>
#include <stdexcept>
#include <string>

namespace clink::async {

struct IoUringReactor::Impl {
    io_uring ring{};
    // SQ submission is mutex-protected so any thread can submit; the
    // CQ side is owned by the run() loop.
    std::mutex submit_mu;
};

IoUringReactor::IoUringReactor(unsigned queue_depth) : impl_(std::make_unique<Impl>()) {
    const int rc = io_uring_queue_init(queue_depth, &impl_->ring, 0);
    if (rc < 0) {
        throw std::runtime_error("IoUringReactor: io_uring_queue_init failed: " +
                                 std::string{std::strerror(-rc)});
    }
}

IoUringReactor::~IoUringReactor() {
    if (impl_) {
        io_uring_queue_exit(&impl_->ring);
    }
}

void IoUringReactor::submit_nop_wakeup_() {
    std::lock_guard lock(impl_->submit_mu);
    io_uring_sqe* sqe = io_uring_get_sqe(&impl_->ring);
    if (!sqe) {
        // SQ is full; the loop will drain the CQ soon enough and a
        // subsequent stop() will be observable via the flag without
        // a wakeup SQE. This is a best-effort hint.
        return;
    }
    io_uring_prep_nop(sqe);
    // user_data=0 means "no awaiter to resume; just drain and
    // continue." The dispatch loop ignores zero user_data.
    io_uring_sqe_set_data(sqe, nullptr);
    io_uring_submit(&impl_->ring);
    ops_submitted_.fetch_add(1, std::memory_order_relaxed);
}

void IoUringReactor::stop() {
    stop_.store(true, std::memory_order_release);
    submit_nop_wakeup_();
}

void IoUringReactor::run() {
    while (!stop_.load(std::memory_order_acquire)) {
        io_uring_cqe* cqe = nullptr;
        const int rc = io_uring_wait_cqe(&impl_->ring, &cqe);
        if (rc < 0) {
            if (-rc == EINTR) {
                continue;  // signal woke us; retry
            }
            throw std::runtime_error("IoUringReactor::run: io_uring_wait_cqe failed: " +
                                     std::string{std::strerror(-rc)});
        }
        // Drain every available CQE in one shot before re-waiting -
        // amortizes the syscall cost when the kernel batches
        // completions.
        unsigned head;
        unsigned processed = 0;
        io_uring_for_each_cqe(&impl_->ring, head, cqe) {
            ++processed;
            void* udata = io_uring_cqe_get_data(cqe);
            if (udata != nullptr) {
                auto* state = static_cast<OperationState*>(udata);
                state->result = cqe->res;
                state->completed = true;
                // Resume on this thread. The awaiting coroutine
                // returns from co_await into whatever logic it
                // wraps; that logic must be safe to run on the
                // reactor thread.
                if (state->awaiting) {
                    state->awaiting.resume();
                }
            }
        }
        io_uring_cq_advance(&impl_->ring, processed);
        completions_handled_.fetch_add(processed, std::memory_order_relaxed);
    }
}

void IoUringReactor::SleepAwaiter::await_suspend(std::coroutine_handle<> h) {
    state_ = std::make_unique<IoUringReactor::OperationState>();
    state_->awaiting = h;
    __kernel_timespec ts{};
    const auto secs = std::chrono::duration_cast<std::chrono::seconds>(duration_);
    ts.tv_sec = secs.count();
    ts.tv_nsec = (duration_ - secs).count();

    std::lock_guard lock(reactor_.impl_->submit_mu);
    io_uring_sqe* sqe = io_uring_get_sqe(&reactor_.impl_->ring);
    if (!sqe) {
        // SQ full: schedule failure path. Resume immediately with
        // an error result so the awaiter doesn't deadlock waiting
        // for a kernel timer that was never submitted.
        state_->result = -ENOMEM;
        state_->completed = true;
        h.resume();
        return;
    }
    io_uring_prep_timeout(sqe, &ts, /*count=*/0, /*flags=*/0);
    io_uring_sqe_set_data(sqe, state_.get());
    io_uring_submit(&reactor_.impl_->ring);
    reactor_.ops_submitted_.fetch_add(1, std::memory_order_relaxed);
}

// Common submit shape for the socket awaiters. Each await_suspend
// initialises OperationState, grabs an SQE under the submit mutex,
// fills it via the op-specific io_uring_prep_*, and submits. The
// SQE-full path resumes immediately with -ENOMEM so the awaiter
// doesn't deadlock. Inlined per-op rather than factored into a
// helper because submit_mu and the ring live behind impl_ which is
// private to IoUringReactor; nested classes get access automatically
// but a free helper would need a friend dance.

void IoUringReactor::ReadAwaiter::await_suspend(std::coroutine_handle<> h) {
    state_ = std::make_unique<IoUringReactor::OperationState>();
    state_->awaiting = h;
    std::lock_guard lock(reactor_.impl_->submit_mu);
    io_uring_sqe* sqe = io_uring_get_sqe(&reactor_.impl_->ring);
    if (!sqe) {
        state_->result = -ENOMEM;
        state_->completed = true;
        h.resume();
        return;
    }
    io_uring_prep_read(sqe, fd_, buf_, len_, /*offset=*/0);
    io_uring_sqe_set_data(sqe, state_.get());
    io_uring_submit(&reactor_.impl_->ring);
    reactor_.ops_submitted_.fetch_add(1, std::memory_order_relaxed);
}

void IoUringReactor::WriteAwaiter::await_suspend(std::coroutine_handle<> h) {
    state_ = std::make_unique<IoUringReactor::OperationState>();
    state_->awaiting = h;
    std::lock_guard lock(reactor_.impl_->submit_mu);
    io_uring_sqe* sqe = io_uring_get_sqe(&reactor_.impl_->ring);
    if (!sqe) {
        state_->result = -ENOMEM;
        state_->completed = true;
        h.resume();
        return;
    }
    io_uring_prep_write(sqe, fd_, buf_, len_, /*offset=*/0);
    io_uring_sqe_set_data(sqe, state_.get());
    io_uring_submit(&reactor_.impl_->ring);
    reactor_.ops_submitted_.fetch_add(1, std::memory_order_relaxed);
}

void IoUringReactor::AcceptAwaiter::await_suspend(std::coroutine_handle<> h) {
    state_ = std::make_unique<IoUringReactor::OperationState>();
    state_->awaiting = h;
    std::lock_guard lock(reactor_.impl_->submit_mu);
    io_uring_sqe* sqe = io_uring_get_sqe(&reactor_.impl_->ring);
    if (!sqe) {
        state_->result = -ENOMEM;
        state_->completed = true;
        h.resume();
        return;
    }
    // nullptr addr / addrlen: we don't surface the peer's sockaddr.
    // Callers that need it call getpeername() on the returned fd.
    io_uring_prep_accept(sqe, fd_, nullptr, nullptr, /*flags=*/0);
    io_uring_sqe_set_data(sqe, state_.get());
    io_uring_submit(&reactor_.impl_->ring);
    reactor_.ops_submitted_.fetch_add(1, std::memory_order_relaxed);
}

void IoUringReactor::ConnectAwaiter::await_suspend(std::coroutine_handle<> h) {
    state_ = std::make_unique<IoUringReactor::OperationState>();
    state_->awaiting = h;
    std::lock_guard lock(reactor_.impl_->submit_mu);
    io_uring_sqe* sqe = io_uring_get_sqe(&reactor_.impl_->ring);
    if (!sqe) {
        state_->result = -ENOMEM;
        state_->completed = true;
        h.resume();
        return;
    }
    io_uring_prep_connect(sqe, fd_, addr_, addr_len_);
    io_uring_sqe_set_data(sqe, state_.get());
    io_uring_submit(&reactor_.impl_->ring);
    reactor_.ops_submitted_.fetch_add(1, std::memory_order_relaxed);
}

}  // namespace clink::async

#endif  // __linux__ && CLINK_HAS_URING
