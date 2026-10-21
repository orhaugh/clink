#include "connection.hpp"

#include <algorithm>
#include <utility>

#include "clink/core/base64.hpp"

namespace clink::neo4j {

using Clock = std::chrono::steady_clock;
using clink::http_connector::HttpRequest;

std::unique_ptr<HttpRequest> make_client(const ConnectionOptions& o) {
    HttpRequest::Options h;
    h.base_url = o.endpoint;
    h.connect_timeout_ms = static_cast<int>(o.connect_timeout_ms);
    h.rw_timeout_ms = static_cast<int>(std::min<std::uint64_t>(o.request_timeout_ms, 3'600'000));
    h.verify_tls = true;
    h.ca_cert_path = o.tls_ca_file;
    h.verify_hostname = true;
    if (!o.username.empty()) {
        h.headers["Authorization"] = "Basic " + clink::base64_encode(o.username + ":" + o.password);
    } else if (!o.bearer_token.empty()) {
        h.headers["Authorization"] = "Bearer " + o.bearer_token;
    }
    return std::make_unique<HttpRequest>(std::move(h));
}

// ---- RequestWatch -----------------------------------------------------------

RequestWatch::RequestWatch(HttpRequest& http) : http_(http), thread_([this] { run(); }) {}

RequestWatch::~RequestWatch() {
    stop();
}

void RequestWatch::arm(Clock::time_point deadline, CancelSignal cancel) {
    {
        const std::lock_guard<std::mutex> lock(mu_);
        armed_ = true;
        deadline_ = deadline;
        cancel_ = std::move(cancel);
        why_ = Why::None;
    }
    cv_.notify_all();
}

RequestWatch::Why RequestWatch::disarm() {
    const std::lock_guard<std::mutex> lock(mu_);
    armed_ = false;
    return why_;
}

void RequestWatch::stop() {
    {
        const std::lock_guard<std::mutex> lock(mu_);
        stop_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

void RequestWatch::run() {
    std::unique_lock<std::mutex> lock(mu_);
    while (!stop_) {
        if (!armed_) {
            cv_.wait(lock, [this] { return stop_ || armed_; });
            continue;
        }
        cv_.wait_for(lock, std::chrono::milliseconds{20});
        if (stop_ || !armed_) {
            continue;
        }
        Why why = Why::None;
        if (cancel_.requested()) {
            why = Why::Cancelled;
        } else if (Clock::now() >= deadline_) {
            why = Why::Deadline;
        }
        if (why == Why::None) {
            continue;
        }
        if (why_ == Why::None) {
            why_ = why;
        }
        // Outside the lock: a connect under way holds the client until it
        // finishes or times out, and arm/disarm must not wait on it. While
        // still armed, the next tick interrupts again, in case this one
        // landed between two of the client's own steps.
        lock.unlock();
        http_.interrupt();
        lock.lock();
    }
}

// ---- Backoff ----------------------------------------------------------------

std::chrono::milliseconds Backoff::next() {
    const std::int64_t cap = std::min<std::int64_t>(5000, 100LL << std::min(n_, 10U));
    ++n_;
    std::uniform_int_distribution<std::int64_t> pick(0, cap);
    return std::chrono::milliseconds{pick(rng_)};
}

bool cancellable_wait(std::chrono::milliseconds d,
                      const CancelSignal& cancel,
                      const std::atomic<bool>* stop) {
    const auto until = Clock::now() + d;
    while (true) {
        if (cancel.requested() || (stop != nullptr && stop->load())) {
            return false;
        }
        const auto now = Clock::now();
        if (now >= until) {
            return true;
        }
        std::this_thread::sleep_for(
            std::min<Clock::duration>(until - now, std::chrono::milliseconds{50}));
    }
}

const char* error_code_for(ErrorClass c) {
    switch (c) {
        case ErrorClass::Authentication:
            return code::kAuthentication;
        case ErrorClass::Authorisation:
            return code::kAuthorisation;
        case ErrorClass::Statement:
            return code::kStatementRefused;
        default:
            return code::kServerRefused;
    }
}

std::string tagged(const char* metric, std::uint64_t op) {
    return std::string(metric) + "{op_id=\"" + std::to_string(op) + "\"}";
}

std::string tagged(const char* metric,
                   std::uint64_t op,
                   std::string_view key,
                   std::string_view value) {
    std::string out(metric);
    out += "{op_id=\"" + std::to_string(op) + "\",";
    out += key;
    out += "=\"";
    out += value;
    out += "\"}";
    return out;
}

}  // namespace clink::neo4j
