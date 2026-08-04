#pragma once

// Wait until a spawned node is accepting on its port.
//
// Exists because the pre-harness integration tests all open the same way:
// spawn a coordinator, sleep 200-300ms "to give it time to bind", then use
// it. That sleep is a guess at how long a process takes to start, and it is
// the single largest source of noise in the suite - each of those tests
// passes alone in under two seconds and fails when 109 multi-process tests
// run back to back, or when a container build is running on the same
// machine. The failure is always reported as a defect in whatever the test
// was actually checking.
//
// The condition is available and cheap: connect to the port. This is not a
// synchronisation delay - it returns the instant the listener is up - and
// the timeout is a failure bound, not a wait.
//
// The full harness (cluster_harness.hpp) already does this and more, but
// converting these tests to it is a larger change than replacing a sleep;
// this is the small step that removes the flakiness now. New tests should
// use the harness.

#include <chrono>
#include <cstdint>
#include <thread>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace clink::itest {

[[nodiscard]] inline bool await_port_accepting(
    std::uint16_t port, std::chrono::milliseconds timeout = std::chrono::seconds{15}) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd >= 0) {
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            const bool ok = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
            ::close(fd);
            if (ok) {
                return true;
            }
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
}

}  // namespace clink::itest
