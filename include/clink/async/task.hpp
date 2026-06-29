#pragma once

// Task<T> coroutine primitive.
//
// A lightweight C++20 coroutine return type. Move-only, lazy-start
// (initial_suspend returns suspend_always so the coroutine doesn't
// run until the caller resumes it), exception-safe (uncaught
// exceptions inside the coroutine body are captured on the promise
// and rethrown from get()). Designed as the building block for the
// async lookup pipeline: an async lookup function declared as
// `Task<Out> lookup(In)` returns a Task that the operator scheduler
// advances via resume(), polls with done(), and drains with get().
//
// Composition: Task<T> is itself awaitable, so a coroutine can
// `co_await another_task()` to chain async work without futures.
// The awaiter resumes the awaiting coroutine when the awaited Task
// completes (which the user-driven scheduler arranges).
//
// Scope:
// - Task<T> and Task<void> with promise_type, get_return_object,
//   initial_suspend (lazy), final_suspend, return_value/return_void,
//   unhandled_exception.
// - Move-only RAII over std::coroutine_handle (destroy on drop).
// - Public API: resume() / done() / get() / has_exception() / valid().
// - co_await support: a Task<T> is awaitable from inside another
//   coroutine; the await_suspend records the awaiting handle on the
//   awaited task's promise so the scheduler can chain resumption.
//
// Out of scope (deferred):
// - Scheduler / event loop.
// - Integration with AsyncMapOperator / SQL LATERAL LOOKUP.
// - I/O primitives (HTTP pool, io_uring backend).

#include <concepts>
#include <coroutine>
#include <exception>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace clink::async {

namespace detail {

// Common bookkeeping for Task<T>'s promise. Captures an exception_ptr
// for any uncaught throw inside the coroutine body, and a continuation
// handle so a Task awaited via co_await can resume its parent when it
// completes.
struct PromiseBase {
    std::exception_ptr exception_{};
    std::coroutine_handle<> continuation_{};

    // The final_suspend awaiter that hands control back to the
    // awaiting coroutine (if any) instead of returning to the
    // caller. This is what makes Task<T> chainable: `co_await
    // child_task()` arranges that when child completes its
    // final_suspend, the parent's coroutine resumes.
    struct FinalAwaiter {
        bool await_ready() const noexcept { return false; }
        template <typename Promise>
        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<Promise> finishing) const noexcept {
            auto cont = finishing.promise().continuation_;
            return cont ? cont : std::noop_coroutine();
        }
        void await_resume() const noexcept {}
    };

    std::suspend_always initial_suspend() noexcept { return {}; }
    FinalAwaiter final_suspend() noexcept { return {}; }
    void unhandled_exception() noexcept { exception_ = std::current_exception(); }
};

}  // namespace detail

template <typename T>
class Task;

template <typename T>
class [[nodiscard]] Task {
public:
    struct promise_type : detail::PromiseBase {
        std::optional<T> value_{};

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        template <typename U>
            requires std::convertible_to<U, T>
        void return_value(U&& v) noexcept(std::is_nothrow_constructible_v<T, U&&>) {
            value_.emplace(std::forward<U>(v));
        }
    };

    using handle_type = std::coroutine_handle<promise_type>;

    Task() = default;
    explicit Task(handle_type h) noexcept : handle_(h) {}

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            destroy_();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }
    ~Task() { destroy_(); }

    [[nodiscard]] bool valid() const noexcept { return static_cast<bool>(handle_); }
    [[nodiscard]] bool done() const noexcept { return !handle_ || handle_.done(); }
    [[nodiscard]] bool has_exception() const noexcept {
        return handle_ && handle_.done() && static_cast<bool>(handle_.promise().exception_);
    }

    // Drive the coroutine forward one step. No-op if the Task is
    // empty or already complete. Caller must check done() between
    // resumes - resuming a finished coroutine is undefined behaviour.
    void resume() {
        if (handle_ && !handle_.done()) {
            handle_.resume();
        }
    }

    // Pull the result. Rethrows the captured exception if the body
    // threw. Throws std::logic_error if the Task is empty or not
    // yet complete.
    T get() {
        if (!handle_) {
            throw std::logic_error("clink::async::Task::get(): empty task");
        }
        if (!handle_.done()) {
            throw std::logic_error("clink::async::Task::get(): not ready");
        }
        auto& promise = handle_.promise();
        if (promise.exception_) {
            std::rethrow_exception(promise.exception_);
        }
        return std::move(*promise.value_);
    }

    // co_await chaining. When a coroutine `co_await`s this Task:
    //   - await_ready: false unless the Task is already complete
    //     (suspend_always-initialised Tasks are never ready until
    //     someone has resumed them).
    //   - await_suspend: records the awaiting coroutine on this
    //     Task's promise as the continuation; the scheduler is
    //     expected to resume this Task next. When this Task hits
    //     final_suspend, the FinalAwaiter hands control back to the
    //     continuation.
    //   - await_resume: returns the value (or rethrows).
    auto operator co_await() && noexcept {
        struct Awaiter {
            handle_type self;
            bool await_ready() const noexcept { return !self || self.done(); }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting) noexcept {
                self.promise().continuation_ = awaiting;
                return self;  // Symmetric-transfer: jump to the awaited coroutine.
            }
            T await_resume() {
                auto& promise = self.promise();
                if (promise.exception_) {
                    std::rethrow_exception(promise.exception_);
                }
                return std::move(*promise.value_);
            }
        };
        return Awaiter{handle_};
    }

private:
    void destroy_() noexcept {
        if (handle_) {
            handle_.destroy();
            handle_ = {};
        }
    }

    handle_type handle_{};
};

// Void specialisation. Same lifecycle, no value to extract.
template <>
class [[nodiscard]] Task<void> {
public:
    struct promise_type : detail::PromiseBase {
        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        void return_void() noexcept {}
    };

    using handle_type = std::coroutine_handle<promise_type>;

    Task() = default;
    explicit Task(handle_type h) noexcept : handle_(h) {}

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            destroy_();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }
    ~Task() { destroy_(); }

    [[nodiscard]] bool valid() const noexcept { return static_cast<bool>(handle_); }
    [[nodiscard]] bool done() const noexcept { return !handle_ || handle_.done(); }
    [[nodiscard]] bool has_exception() const noexcept {
        return handle_ && handle_.done() && static_cast<bool>(handle_.promise().exception_);
    }

    void resume() {
        if (handle_ && !handle_.done()) {
            handle_.resume();
        }
    }

    void get() {
        if (!handle_) {
            throw std::logic_error("clink::async::Task<void>::get(): empty task");
        }
        if (!handle_.done()) {
            throw std::logic_error("clink::async::Task<void>::get(): not ready");
        }
        if (auto& ex = handle_.promise().exception_; ex) {
            std::rethrow_exception(ex);
        }
    }

    auto operator co_await() && noexcept {
        struct Awaiter {
            handle_type self;
            bool await_ready() const noexcept { return !self || self.done(); }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting) noexcept {
                self.promise().continuation_ = awaiting;
                return self;
            }
            void await_resume() {
                if (auto& ex = self.promise().exception_; ex) {
                    std::rethrow_exception(ex);
                }
            }
        };
        return Awaiter{handle_};
    }

private:
    void destroy_() noexcept {
        if (handle_) {
            handle_.destroy();
            handle_ = {};
        }
    }

    handle_type handle_{};
};

}  // namespace clink::async
