// Unit tests for clink::async::Task<T> (Phase 28a coroutine primitive).
//
// Covers the foundational coroutine contract that the rest of Phase 28
// builds on:
//   - Lazy start: a constructed Task does NOT run its body until the
//     caller resumes it.
//   - Single-step resume drives the body to completion.
//   - co_return delivers the value via get().
//   - co_await chaining: one Task awaiting another resumes the parent
//     after the child finishes.
//   - Exception propagation: a throw inside the body is captured on
//     the promise and rethrown from get() / co_await's await_resume.
//   - Move semantics: Task is move-only; moves transfer the handle.
//   - Void specialisation has the same lifecycle without a value.

#include <atomic>
#include <stdexcept>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "clink/async/task.hpp"

using clink::async::Task;

namespace {

// Helpers ------------------------------------------------------------

Task<int> ready_int(int x) {
    co_return x;
}

Task<std::string> ready_string(const char* s) {
    co_return std::string{s};
}

Task<int> always_throws() {
    throw std::runtime_error("boom");
    co_return 0;  // unreachable; needed to make this a coroutine
}

Task<void> noop_void() {
    co_return;
}

Task<void> always_throws_void() {
    throw std::runtime_error("boom-void");
    co_return;  // unreachable
}

Task<int> chained_inner(int x) {
    co_return x * 10;
}

Task<int> chained_outer(int x) {
    int inner_result = co_await chained_inner(x);
    co_return inner_result + 1;
}

Task<int> chained_throw_inner() {
    throw std::runtime_error("inner-fail");
    co_return 0;
}

Task<int> chained_throw_outer() {
    int v = co_await chained_throw_inner();
    co_return v;  // unreachable; inner throws across co_await
}

}  // namespace

// --- Lazy start ----------------------------------------------------

TEST(AsyncTask, LazyStartDoesNotRunUntilResume) {
    std::atomic<bool> ran{false};
    auto factory = [&]() -> Task<int> {
        ran.store(true);
        co_return 7;
    };
    auto t = factory();
    EXPECT_FALSE(ran.load()) << "lazy Task must not begin until resume()";
    EXPECT_TRUE(t.valid());
    EXPECT_FALSE(t.done());

    t.resume();
    EXPECT_TRUE(ran.load());
    EXPECT_TRUE(t.done());
    EXPECT_EQ(t.get(), 7);
}

TEST(AsyncTask, ResumeDrivesBodyToCompletion) {
    auto t = ready_int(42);
    t.resume();
    EXPECT_TRUE(t.done());
    EXPECT_EQ(t.get(), 42);
}

// --- get() error paths ---------------------------------------------

TEST(AsyncTask, GetBeforeResumeThrowsLogicError) {
    auto t = ready_int(1);
    EXPECT_THROW(t.get(), std::logic_error);
}

TEST(AsyncTask, GetOnEmptyTaskThrowsLogicError) {
    Task<int> empty;
    EXPECT_FALSE(empty.valid());
    EXPECT_THROW(empty.get(), std::logic_error);
}

// --- Exception propagation -----------------------------------------

TEST(AsyncTask, ExceptionFromBodyIsCapturedAndRethrown) {
    auto t = always_throws();
    t.resume();
    EXPECT_TRUE(t.done());
    EXPECT_TRUE(t.has_exception());
    EXPECT_THROW(t.get(), std::runtime_error);
}

TEST(AsyncTask, VoidExceptionPropagates) {
    auto t = always_throws_void();
    t.resume();
    EXPECT_TRUE(t.has_exception());
    EXPECT_THROW(t.get(), std::runtime_error);
}

// --- Move semantics ------------------------------------------------

TEST(AsyncTask, MoveTransfersHandle) {
    auto a = ready_int(11);
    EXPECT_TRUE(a.valid());
    Task<int> b = std::move(a);
    EXPECT_FALSE(a.valid()) << "moved-from must be empty";
    EXPECT_TRUE(b.valid());
    b.resume();
    EXPECT_EQ(b.get(), 11);
}

TEST(AsyncTask, MoveAssignmentDestroysPriorHandle) {
    // Two tasks; assign the second to a slot already holding the
    // first. The first's handle must be destroyed (no resource leak,
    // and the assigned slot drives the second to completion).
    auto a = ready_int(1);
    a.resume();
    EXPECT_EQ(a.get(), 1);

    a = ready_int(2);
    EXPECT_TRUE(a.valid());
    EXPECT_FALSE(a.done());
    a.resume();
    EXPECT_EQ(a.get(), 2);
}

// --- co_await chaining --------------------------------------------

TEST(AsyncTask, CoAwaitChainsAndDeliversInnerResult) {
    auto t = chained_outer(3);
    // Resume the outer; symmetric transfer should run inner to
    // completion + resume outer, producing (3 * 10) + 1 = 31.
    t.resume();
    EXPECT_TRUE(t.done());
    EXPECT_EQ(t.get(), 31);
}

TEST(AsyncTask, CoAwaitPropagatesInnerException) {
    auto t = chained_throw_outer();
    t.resume();
    EXPECT_TRUE(t.done());
    EXPECT_TRUE(t.has_exception());
    EXPECT_THROW(t.get(), std::runtime_error);
}

// --- Void specialisation -------------------------------------------

TEST(AsyncTaskVoid, ResumeAndGet) {
    auto t = noop_void();
    EXPECT_FALSE(t.done());
    t.resume();
    EXPECT_TRUE(t.done());
    EXPECT_NO_THROW(t.get());
}

TEST(AsyncTaskVoid, GetOnEmptyThrowsLogicError) {
    Task<void> empty;
    EXPECT_THROW(empty.get(), std::logic_error);
}

// --- Non-trivial value types ---------------------------------------

TEST(AsyncTask, StringResultRoundTrips) {
    auto t = ready_string("hello world");
    t.resume();
    EXPECT_EQ(t.get(), "hello world");
}

// --- Implicit-conversion return ------------------------------------

TEST(AsyncTask, ImplicitConversionInReturnValue) {
    // return_value uses convertible_to so users can co_return an
    // integral literal where T is int64_t.
    auto t = []() -> Task<long> { co_return 17; }();
    t.resume();
    EXPECT_EQ(t.get(), 17L);
}
