// CompletionExecutor (ASYNC-9): the pluggable off-runner-thread IO executor
// behind the controller's schedule_resume seam. These cover the portable
// ThreadPoolCompletionExecutor: submit_blocking runs work off the caller's
// thread, submit_read reads a file descriptor, and a sized pool genuinely
// overlaps work (the pre-ASYNC-9 single thread serialized every in-flight
// read). The io_uring-backed executor is Docker-only and covered separately.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>

#include <gtest/gtest.h>

#include "clink/async/completion_executor.hpp"

using namespace clink::async;
using namespace std::chrono_literals;

namespace {

// A temp file holding `content`, removed on destruction. Returns an O_RDONLY fd
// via fd().
class TempFile {
public:
    explicit TempFile(const std::string& content) {
        char tmpl[] = "/tmp/clink_ce_XXXXXX";
        fd_ = ::mkstemp(tmpl);
        path_ = tmpl;
        if (fd_ >= 0) {
            const auto n = ::write(fd_, content.data(), content.size());
            (void)n;
            ::lseek(fd_, 0, SEEK_SET);
        }
    }
    ~TempFile() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        if (!path_.empty()) {
            ::unlink(path_.c_str());
        }
    }
    [[nodiscard]] int fd() const { return fd_; }

private:
    int fd_{-1};
    std::string path_;
};

}  // namespace

TEST(CompletionExecutor, SubmitBlockingRunsJobOffCallerThread) {
    ThreadPoolCompletionExecutor exec(2);
    std::mutex m;
    std::condition_variable cv;
    bool done = false;
    std::thread::id worker_tid;
    const std::thread::id caller_tid = std::this_thread::get_id();

    exec.submit_blocking([&] {
        worker_tid = std::this_thread::get_id();
        std::lock_guard<std::mutex> lk(m);
        done = true;
        cv.notify_one();
    });

    std::unique_lock<std::mutex> lk(m);
    ASSERT_TRUE(cv.wait_for(lk, 5s, [&] { return done; }));
    EXPECT_NE(worker_tid, caller_tid);  // ran on a pool worker, not the caller
}

TEST(CompletionExecutor, SubmitReadReturnsFileBytes) {
    ThreadPoolCompletionExecutor exec(2);
    TempFile f("hello-bytes");
    ASSERT_GE(f.fd(), 0);

    char buf[64] = {};
    std::mutex m;
    std::condition_variable cv;
    bool done = false;
    std::int64_t result = -123;

    exec.submit_read(f.fd(), buf, sizeof(buf), [&](std::int64_t n) {
        result = n;
        std::lock_guard<std::mutex> lk(m);
        done = true;
        cv.notify_one();
    });

    std::unique_lock<std::mutex> lk(m);
    ASSERT_TRUE(cv.wait_for(lk, 5s, [&] { return done; }));
    ASSERT_EQ(result, static_cast<std::int64_t>(std::strlen("hello-bytes")));
    EXPECT_EQ(std::string(buf, static_cast<std::size_t>(result)), "hello-bytes");
}

TEST(CompletionExecutor, SizedPoolOverlapsWork) {
    // Four jobs that each block until all four have started: only possible if at
    // least four run concurrently. A single-thread pool (the old behaviour)
    // would deadlock here, so the timed wait proves real concurrency.
    constexpr int kJobs = 4;
    ThreadPoolCompletionExecutor exec(kJobs);
    EXPECT_EQ(exec.thread_count(), static_cast<std::size_t>(kJobs));

    std::mutex m;
    std::condition_variable cv;
    int started = 0;
    int finished = 0;

    for (int i = 0; i < kJobs; ++i) {
        exec.submit_blocking([&] {
            {
                std::unique_lock<std::mutex> lk(m);
                ++started;
                cv.notify_all();
                // Wait until every job has started: forces genuine overlap.
                ASSERT_TRUE(cv.wait_for(lk, 5s, [&] { return started == kJobs; }));
                ++finished;
                cv.notify_all();
            }
        });
    }

    std::unique_lock<std::mutex> lk(m);
    EXPECT_TRUE(cv.wait_for(lk, 5s, [&] { return finished == kJobs; }));
}

TEST(CompletionExecutor, ZeroThreadsClampsToOne) {
    // Synchronisation state is declared BEFORE the executor so the
    // executor's destructor (which joins the worker) runs before the
    // condvar/mutex are destroyed, and the flag flips UNDER the mutex so
    // the waiter cannot satisfy its predicate while the worker is still
    // touching the condvar. The original shape (flag set outside the
    // lock, cv declared after exec) let the test body end - destroying
    // the condvar - while the worker was inside notify_one (TSan:
    // pthread_cond_signal vs pthread_cond_destroy).
    std::mutex m;
    std::condition_variable cv;
    bool ran = false;
    ThreadPoolCompletionExecutor exec(0);
    EXPECT_EQ(exec.thread_count(), 1u);
    exec.submit_blocking([&] {
        std::lock_guard<std::mutex> lk(m);
        ran = true;
        cv.notify_one();
    });
    std::unique_lock<std::mutex> lk(m);
    EXPECT_TRUE(cv.wait_for(lk, 5s, [&] { return ran; }));
}
