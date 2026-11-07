#pragma once

// Ends the request in flight on an HttpRequest once the task that owns it is
// cancelled, or once the request runs past a deadline, so a cancelled task
// stops at once rather than waiting out the request's read timeout, and a
// server that trickles its reply cannot hold a request open past its bound.
// One thread per client, asleep unless a request is armed; it touches nothing
// but HttpRequest::interrupt(), so it hides no error and holds no data. The
// bulk sinks arm it around each POST, and the polling source around each GET,
// with its request_timeout as the deadline. It is stopped and joined by its
// destructor, which must run before the client's.

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "clink/http_connector/http_request.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace clink::http_connector {

class CancelWatch {
public:
    explicit CancelWatch(HttpRequest& http) : http_(http), thread_([this] { run(); }) {}

    ~CancelWatch() {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    CancelWatch(const CancelWatch&) = delete;
    CancelWatch& operator=(const CancelWatch&) = delete;
    CancelWatch(CancelWatch&&) = delete;
    CancelWatch& operator=(CancelWatch&&) = delete;

    // Watch the request about to be sent for `cancel`, and for running past
    // `deadline` when one is given.
    void arm(CancelSignal cancel,
             std::chrono::steady_clock::time_point deadline =
                 std::chrono::steady_clock::time_point::max()) {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            armed_ = true;
            cancel_ = std::move(cancel);
            deadline_ = deadline;
            interrupted_ = false;
        }
        cv_.notify_all();
    }

    // Stop watching; true when the watch interrupted the request armed last,
    // for its cancel or its deadline (the caller's cancel signal tells which).
    bool disarm() {
        const std::lock_guard<std::mutex> lock(mu_);
        armed_ = false;
        return interrupted_;
    }

private:
    void run() {
        std::unique_lock<std::mutex> lock(mu_);
        while (!stop_) {
            if (!armed_) {
                cv_.wait(lock, [this] { return stop_ || armed_; });
                continue;
            }
            cv_.wait_for(lock, std::chrono::milliseconds{20});
            if (stop_ || !armed_ ||
                (!cancel_.requested() && std::chrono::steady_clock::now() < deadline_)) {
                continue;
            }
            interrupted_ = true;
            // Outside the lock: a connect under way holds the client until it
            // finishes or times out, and arm/disarm must not wait on it. While
            // still armed, the next tick interrupts again, in case this one
            // landed between two of the client's own steps.
            lock.unlock();
            http_.interrupt();
            lock.lock();
        }
    }

    HttpRequest& http_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool armed_{false};
    bool stop_{false};
    bool interrupted_{false};
    CancelSignal cancel_;
    std::chrono::steady_clock::time_point deadline_{std::chrono::steady_clock::time_point::max()};
    std::thread thread_;
};

}  // namespace clink::http_connector
