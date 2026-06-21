#pragma once

// CompletionExecutor (ASYNC-9): the pluggable source of async-IO completions
// behind the AsyncExecutionController's schedule_resume seam.
//
// A state backend whose reads block on a slow tier suspends the record's
// coroutine and hands the IO to a CompletionExecutor. The executor does the
// work OFF the runner thread and then invokes a continuation; for a state read
// that continuation calls AsyncExecutionController::schedule_resume, which
// resumes the coroutine back on the runner thread. The executor never resumes a
// coroutine and never touches runner-thread-private state - it is purely "run
// this off-thread, then tell me it is done", exactly the cross-thread boundary
// the controller is built around.
//
// Two work kinds, because the two transports differ fundamentally:
//   - submit_blocking: opaque blocking work (the production case: an S3 HTTPS
//     GET through Arrow's S3FileSystem / the AWS SDK). Only a worker thread can
//     run it - io_uring cannot transport an HTTPS request, and the work is an
//     opaque callable it cannot decompose.
//   - submit_read: a file-descriptor read (a file / socket-backed remote tier).
//     This IS io_uring's wheelhouse: the io_uring executor drives it as an SQE
//     and posts the continuation from the reactor loop on the CQE; the portable
//     executor does a blocking read on a worker.
//
// The portable ThreadPoolCompletionExecutor below is the default everywhere.
// The io_uring-backed executor lives in io_uring_completion_executor.hpp (Linux,
// CLINK_HAS_URING) so this header drags in no kernel headers. One executor can
// be shared across the backends of a process (a single sized IO pool) or owned
// per-backend (the default).

#include <cerrno>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <vector>

namespace clink::async {

class CompletionExecutor {
public:
    virtual ~CompletionExecutor() = default;

    // Run `job` off the runner thread; `job` invokes its own continuation when
    // done (for a state read: do the blocking load, then schedule_resume).
    virtual void submit_blocking(std::function<void()> job) = 0;

    // Read up to `len` bytes from `fd` (at its current file offset) into `buf`,
    // then run done(n): n >= 0 bytes read, or -errno on failure. `buf` and `fd`
    // must stay valid until `done` runs (the caller owns them across the IO).
    virtual void submit_read(int fd,
                             void* buf,
                             std::size_t len,
                             std::function<void(std::int64_t)> done) = 0;
};

// Portable default. A fixed pool of worker threads; submit_blocking runs the
// job on a worker, submit_read does a blocking ::read on a worker. The pool
// size is the IO concurrency: the pre-ASYNC-9 backend hardcoded a single IO
// thread, which serialized every in-flight remote read through one worker even
// though the controller admits thousands. Size this to the concurrency the
// remote tier can absorb.
class ThreadPoolCompletionExecutor final : public CompletionExecutor {
public:
    explicit ThreadPoolCompletionExecutor(std::size_t threads) {
        if (threads == 0) {
            threads = 1;
        }
        workers_.reserve(threads);
        for (std::size_t i = 0; i < threads; ++i) {
            workers_.emplace_back([this] { loop_(); });
        }
    }

    ~ThreadPoolCompletionExecutor() override {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    ThreadPoolCompletionExecutor(const ThreadPoolCompletionExecutor&) = delete;
    ThreadPoolCompletionExecutor& operator=(const ThreadPoolCompletionExecutor&) = delete;

    void submit_blocking(std::function<void()> job) override { enqueue_(std::move(job)); }

    void submit_read(int fd,
                     void* buf,
                     std::size_t len,
                     std::function<void(std::int64_t)> done) override {
        enqueue_([fd, buf, len, done = std::move(done)]() {
            const ::ssize_t n = ::read(fd, buf, len);
            done(n >= 0 ? static_cast<std::int64_t>(n) : -static_cast<std::int64_t>(errno));
        });
    }

    [[nodiscard]] std::size_t thread_count() const noexcept { return workers_.size(); }

private:
    void enqueue_(std::function<void()> job) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            jobs_.push_back(std::move(job));
        }
        cv_.notify_one();
    }

    void loop_() {
        for (;;) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_.wait(lk, [this] { return stop_ || !jobs_.empty(); });
                if (stop_ && jobs_.empty()) {
                    return;
                }
                job = std::move(jobs_.front());
                jobs_.pop_front();
            }
            job();
        }
    }

    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<std::function<void()>> jobs_;
    bool stop_{false};
    std::vector<std::thread> workers_;
};

// The default IO concurrency for a backend that does not inject its own
// executor. Chosen well above 1 (the old serializing value) but modest enough
// not to explode thread counts across many per-subtask backends; raise it via
// the factory `?io_threads=` knob, or inject one shared sized executor.
inline constexpr std::size_t kDefaultIoThreads = 8;

}  // namespace clink::async
