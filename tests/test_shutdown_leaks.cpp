// Shutdown releases what startup took.
//
// docs/production-hardening-plan.md W21 recorded the gap plainly: "No leak
// check. Nothing asserts that threads, file descriptors or temporary files
// are released on shutdown. A clean exit code is not evidence of a clean
// teardown."
//
// It matters most for the long-lived processes. A coordinator that leaks two
// descriptors per stopped job is invisible for a day and then hits the
// process limit, and the failure surfaces as an unrelated accept() error on
// the control plane. Nothing in the suite would have caught that: every
// existing test starts a cluster, asserts a behaviour and exits, so the
// leak goes out with the process.
//
// Cycles rather than a single start/stop, because one iteration cannot
// distinguish a leak from a one-off allocation that is legitimately kept -
// a lazily-created log sink, a cached DNS handle. A LEAK grows with the
// number of cycles; a fixture does not. The assertions are on the delta
// across the last cycles for that reason.

#include <chrono>
#include <cstdint>
#include <dirent.h>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#if defined(__APPLE__)
#include <mach/mach.h>
#endif

#include "clink/cluster/coordinator.hpp"

namespace {

using namespace clink;
using namespace clink::cluster;
using namespace std::chrono_literals;

// Open descriptors for this process. /dev/fd is present on both macOS and
// Linux and lists exactly the open descriptors, so no /proc dependency and
// no lsof subprocess.
std::size_t open_fd_count() {
    DIR* d = ::opendir("/dev/fd");
    if (d == nullptr) {
        return 0;
    }
    std::size_t n = 0;
    while (::readdir(d) != nullptr) {
        ++n;
    }
    ::closedir(d);
    // The readdir handle itself holds one; ".", ".." are counted too. All
    // three are constant across calls, so they cancel in a delta.
    return n;
}

// Live threads in this process. Returns 0 where unsupported, and the test
// skips its thread assertion rather than asserting on a fabricated number.
std::size_t live_thread_count() {
#if defined(__APPLE__)
    thread_act_array_t threads = nullptr;
    mach_msg_type_number_t count = 0;
    if (::task_threads(::mach_task_self(), &threads, &count) != KERN_SUCCESS) {
        return 0;
    }
    for (mach_msg_type_number_t i = 0; i < count; ++i) {
        ::mach_port_deallocate(::mach_task_self(), threads[i]);
    }
    ::vm_deallocate(
        ::mach_task_self(), reinterpret_cast<vm_address_t>(threads), count * sizeof(thread_act_t));
    return static_cast<std::size_t>(count);
#elif defined(__linux__)
    DIR* d = ::opendir("/proc/self/task");
    if (d == nullptr) {
        return 0;
    }
    std::size_t n = 0;
    while (const auto* e = ::readdir(d)) {
        const std::string name = e->d_name;
        if (name != "." && name != "..") {
            ++n;
        }
    }
    ::closedir(d);
    return n;
#else
    return 0;
#endif
}

// Threads are joined asynchronously enough that an immediate read can catch
// one on its way out. Waiting for the count to come back down is the
// condition; the bound only decides how long to wait before calling it a
// leak.
std::size_t settled_thread_count(std::size_t want, std::chrono::milliseconds bound = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + bound;
    std::size_t n = live_thread_count();
    while (n > want && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(20ms);
        n = live_thread_count();
    }
    return n;
}

}  // namespace

TEST(ShutdownLeaks, StartingAndStoppingACoordinatorReleasesItsDescriptors) {
    // One cycle first, to let anything lazily created on the first start
    // exist before the baseline is taken. Without this the test measures
    // first-use allocation rather than a leak.
    {
        Coordinator warmup;
        (void)warmup.start();
        warmup.stop();
    }

    const auto fds_before = open_fd_count();
    ASSERT_GT(fds_before, 0u) << "/dev/fd is unreadable, so this test can prove nothing";

    constexpr int kCycles = 8;
    for (int i = 0; i < kCycles; ++i) {
        Coordinator c;
        const auto port = c.start();
        ASSERT_GT(port, 0) << "coordinator failed to bind on cycle " << i;
        c.stop();
    }

    const auto fds_after = open_fd_count();
    // Exact, not a tolerance. A coordinator that binds a listener and stops
    // must give the descriptor back; anything retained is retained per cycle
    // and would grow without bound on a long-lived node.
    EXPECT_LE(fds_after, fds_before)
        << "descriptors grew across " << kCycles << " coordinator start/stop cycles: " << fds_before
        << " -> " << fds_after
        << ". A leak here is invisible for a day and then surfaces as an accept() failure on the "
           "control plane.";
}

TEST(ShutdownLeaks, StoppingACoordinatorJoinsItsThreads) {
    if (live_thread_count() == 0) {
        GTEST_SKIP() << "thread counting unsupported on this platform";
    }

    {
        Coordinator warmup;
        (void)warmup.start();
        warmup.stop();
    }
    const auto threads_before = settled_thread_count(live_thread_count());

    constexpr int kCycles = 8;
    for (int i = 0; i < kCycles; ++i) {
        Coordinator c;
        (void)c.start();
        c.stop();
    }

    const auto threads_after = settled_thread_count(threads_before);
    EXPECT_LE(threads_after, threads_before)
        << "threads grew across " << kCycles << " coordinator start/stop cycles: " << threads_before
        << " -> " << threads_after
        << ". stop() returning is not the same as its threads having been joined, and an unjoined "
           "thread holding a socket is how a 'clean' shutdown still loses a port.";
}

TEST(ShutdownLeaks, ACoordinatorThatWasNeverStartedStopsCleanly) {
    // The teardown path most likely to be wrong, because it is the one
    // nobody runs on purpose: a process that fails during configuration and
    // unwinds. It must not hang, crash, or leak.
    const auto fds_before = open_fd_count();
    for (int i = 0; i < 4; ++i) {
        Coordinator c;
        c.stop();  // never started
    }
    EXPECT_LE(open_fd_count(), fds_before)
        << "stopping a coordinator that was never started leaked descriptors";
}
