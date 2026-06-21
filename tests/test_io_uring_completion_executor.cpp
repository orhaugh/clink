// IoUringCompletionExecutor (ASYNC-9B): the io_uring-backed CompletionExecutor.
// Linux-only; conditionally compiled. When CLINK_HAS_URING isn't set the file
// reduces to a single skipped test so the suite still has an entry on every
// platform (mirrors test_io_uring_reactor.cpp).
//
// Proves: submit_read drives a real fd read through the reactor and returns the
// byte count; the completion (`done`) fires on the reactor LOOP thread, not the
// submitting thread (the cross-thread handoff the AsyncExecutionController is
// built around); and submit_blocking still runs opaque work off-thread.

#include <gtest/gtest.h>

#include "clink/async/io_uring_completion_executor.hpp"

#if defined(__linux__) && defined(CLINK_HAS_URING)

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>

using namespace clink::async;
using namespace std::chrono_literals;

namespace {

// Construct an executor, or nullptr if io_uring is unavailable at RUNTIME -
// typically a container seccomp profile blocking io_uring_setup, or a kernel
// older than 5.1. liburing's queue_init throws from the reactor ctor, which the
// executor ctor propagates. Tests GTEST_SKIP rather than fail in that case,
// matching test_io_uring_reactor.cpp (io_uring is compiled in but not always
// runnable). A seccomp-unconfined run exercises the real path.
[[nodiscard]] std::unique_ptr<IoUringCompletionExecutor> try_make_executor(
    std::size_t blocking_threads = 2) {
    try {
        return std::make_unique<IoUringCompletionExecutor>(blocking_threads);
    } catch (const std::runtime_error&) {
        return nullptr;
    }
}

// A temp file holding `content`, removed on destruction; exposes an fd at pos 0.
class TempFile {
public:
    explicit TempFile(const std::string& content) {
        char tmpl[] = "/tmp/clink_iouring_ce_XXXXXX";
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

TEST(IoUringCompletionExecutor, SubmitReadReadsFileViaUring) {
    auto exec = try_make_executor();
    if (!exec) {
        GTEST_SKIP() << "io_uring unavailable at runtime (seccomp/kernel)";
    }
    TempFile f("uring-bytes");
    ASSERT_GE(f.fd(), 0);

    char buf[64] = {};
    std::promise<std::int64_t> p;
    auto fut = p.get_future();
    exec->submit_read(f.fd(), buf, sizeof(buf), [&](std::int64_t n) { p.set_value(n); });

    ASSERT_EQ(fut.wait_for(5s), std::future_status::ready);
    const std::int64_t n = fut.get();
    ASSERT_EQ(n, static_cast<std::int64_t>(std::strlen("uring-bytes")));
    EXPECT_EQ(std::string(buf, static_cast<std::size_t>(n)), "uring-bytes");
}

TEST(IoUringCompletionExecutor, ReadCompletionFiresOnReactorLoopThread) {
    auto exec = try_make_executor();
    if (!exec) {
        GTEST_SKIP() << "io_uring unavailable at runtime (seccomp/kernel)";
    }
    TempFile f("x");
    ASSERT_GE(f.fd(), 0);

    const std::thread::id caller = std::this_thread::get_id();
    char buf[8] = {};
    std::promise<std::thread::id> p;
    auto fut = p.get_future();
    exec->submit_read(
        f.fd(), buf, sizeof(buf), [&](std::int64_t) { p.set_value(std::this_thread::get_id()); });

    ASSERT_EQ(fut.wait_for(5s), std::future_status::ready);
    // The completion is delivered from the reactor's loop thread, never the
    // thread that submitted the read - that is the whole point of the handoff.
    EXPECT_NE(fut.get(), caller);
}

TEST(IoUringCompletionExecutor, SubmitBlockingRunsOpaqueWorkOffThread) {
    auto exec = try_make_executor();
    if (!exec) {
        GTEST_SKIP() << "io_uring unavailable at runtime (seccomp/kernel)";
    }
    const std::thread::id caller = std::this_thread::get_id();
    std::promise<std::thread::id> p;
    auto fut = p.get_future();
    exec->submit_blocking([&] { p.set_value(std::this_thread::get_id()); });

    ASSERT_EQ(fut.wait_for(5s), std::future_status::ready);
    EXPECT_NE(fut.get(), caller);  // ran on the composed worker pool
}

#else  // !(__linux__ && CLINK_HAS_URING)

TEST(IoUringCompletionExecutor, SkippedOnThisPlatform) {
    GTEST_SKIP() << "io_uring not available (not Linux or liburing absent)";
}

#endif  // __linux__ && CLINK_HAS_URING
