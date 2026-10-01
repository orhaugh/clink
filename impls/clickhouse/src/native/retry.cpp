#include "native/retry.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native {

Backoff::Backoff(std::chrono::milliseconds initial,
                 std::chrono::milliseconds cap,
                 std::uint64_t seed)
    : initial_(initial), cap_(cap), rng_(seed) {}

std::chrono::milliseconds Backoff::next() {
    not_implemented("Backoff::next");
}

void Backoff::reset() {
    attempts_ = 0;
}

RetryWindow::RetryWindow(std::chrono::milliseconds window) : window_(window) {}

void RetryWindow::start(std::chrono::steady_clock::time_point /*now*/) {
    not_implemented("RetryWindow::start");
}

bool RetryWindow::allows(std::chrono::steady_clock::time_point /*now*/,
                         std::chrono::milliseconds /*wait*/) const {
    not_implemented("RetryWindow::allows");
}

bool cancellable_wait(std::chrono::milliseconds /*d*/,
                      const CancelSignal& /*cancel*/,
                      const std::atomic<bool>& /*stop*/) {
    not_implemented("cancellable_wait");
}

}  // namespace clink::clickhouse::native
