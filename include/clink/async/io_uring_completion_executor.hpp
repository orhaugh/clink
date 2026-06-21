#pragma once

// IoUringCompletionExecutor (ASYNC-9B): the io_uring-backed completion source
// behind the CompletionExecutor seam.
//
// submit_read drives a file-descriptor read through the process's io_uring
// reactor: a single loop thread reaps completions and resumes the awaiting
// coroutine, so thousands of fd reads can be in flight without a thread each -
// the structural edge over the thread-pool executor for an fd-level remote tier
// (a file / block / socket store). The continuation (`done`) runs from the
// reactor loop thread on the CQE; for a state read that continuation calls
// AsyncExecutionController::schedule_resume, which marshals the coroutine back
// to the runner thread - so the cross-thread contract is identical to the
// thread-pool executor's, only the completion SOURCE differs.
//
// submit_blocking CANNOT use io_uring (an opaque blocking callable - e.g. an S3
// HTTPS GET through the AWS SDK - is not an fd op io_uring can decompose), so it
// delegates to a composed thread pool. This executor is therefore a SUPERSET of
// the thread-pool one: fd reads via io_uring, opaque work via worker threads.
//
// Linux + CLINK_HAS_URING only; on other platforms this header is an empty
// namespace (like io_uring_reactor.hpp) and callers use ThreadPoolCompletionExecutor.
//
// Scope note (ASYNC-9B): no production loader is fd-level yet (the disaggregated
// tier is S3-HTTP, which rides submit_blocking). This executor is built + tested
// so it is ready the moment an fd-level remote tier (or an io_uring-socket S3
// client) lands; until then submit_read is exercised by its own test, not the
// production read path. It is deliberately NOT wired into the state-backend
// factory yet - selecting it for an S3 backend would spin up a reactor loop
// thread that nothing uses.

#include "clink/async/completion_executor.hpp"

#if defined(__linux__) && defined(CLINK_HAS_URING)

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <thread>
#include <utility>

#include "clink/async/io_uring_reactor.hpp"

namespace clink::async {

namespace detail {

// A fire-and-forget coroutine: runs eagerly to its first suspension and
// self-destroys when it completes (both initial and final suspend are
// suspend_never). It exists to adapt the reactor's coroutine-based read_async
// into the executor's callback-based submit_read - the executor has a `done`
// callback, not a co_await site, so it wraps the await in a detached coroutine.
struct DetachedTask {
    struct promise_type {
        DetachedTask get_return_object() noexcept { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        // A detached IO continuation has no caller to propagate to; a throw here
        // is a programming error in `done` (the resume continuation must not
        // throw), so fail loud rather than silently swallow.
        void unhandled_exception() noexcept { std::terminate(); }
    };
};

}  // namespace detail

class IoUringCompletionExecutor final : public CompletionExecutor {
public:
    // blocking_threads sizes the worker pool that serves submit_blocking
    // (io_uring cannot transport opaque blocking work). queue_depth is the
    // reactor's submission-queue depth (max in-flight SQEs before submit
    // back-pressures). The reactor runs on its own loop thread for this
    // executor's lifetime.
    explicit IoUringCompletionExecutor(std::size_t blocking_threads = kDefaultIoThreads,
                                       unsigned queue_depth = 256)
        : blocking_(blocking_threads), reactor_(queue_depth) {
        loop_ = std::thread([this] { reactor_.run(); });
    }

    ~IoUringCompletionExecutor() override {
        reactor_.stop();
        if (loop_.joinable()) {
            loop_.join();
        }
    }

    IoUringCompletionExecutor(const IoUringCompletionExecutor&) = delete;
    IoUringCompletionExecutor& operator=(const IoUringCompletionExecutor&) = delete;

    void submit_blocking(std::function<void()> job) override {
        blocking_.submit_blocking(std::move(job));
    }

    // fd read via io_uring. A detached coroutine co_awaits the reactor's
    // read_async (submits an SQE from this thread, suspends) and runs `done`
    // from the reactor loop thread when the CQE arrives. `buf`/`fd` must outlive
    // the read (the caller owns them across the IO).
    void submit_read(int fd,
                     void* buf,
                     std::size_t len,
                     std::function<void(std::int64_t)> done) override {
        run_read_(reactor_, fd, buf, len, std::move(done));
    }

    [[nodiscard]] IoUringReactor& reactor() noexcept { return reactor_; }

private:
    static detail::DetachedTask run_read_(IoUringReactor& r,
                                          int fd,
                                          void* buf,
                                          std::size_t len,
                                          std::function<void(std::int64_t)> done) {
        // The reactor's read awaiter takes an unsigned length; clamp.
        const unsigned n = len > 0xffffffffu ? 0xffffffffu : static_cast<unsigned>(len);
        const int res = co_await r.read_async(fd, buf, n);  // resumes on the loop thread
        done(static_cast<std::int64_t>(res));               // >=0 bytes, or -errno
    }

    ThreadPoolCompletionExecutor blocking_;  // serves submit_blocking
    IoUringReactor reactor_;                 // serves submit_read
    std::thread loop_;                       // runs reactor_.run()
};

}  // namespace clink::async

#endif  // __linux__ && CLINK_HAS_URING
