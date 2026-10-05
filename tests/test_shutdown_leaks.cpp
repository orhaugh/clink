// Shutdown releases what startup took.
//
// docs/history/production-hardening-2026-08.md W21 recorded the gap plainly: "No leak
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

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <dirent.h>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#if defined(__APPLE__)
#include <mach/mach.h>
#endif

#include "clink/cluster/coordinator.hpp"
#include "clink/fault/fault_injection.hpp"

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

// True while `fd` is still the listener bound to `port`: open, and not
// reissued to another socket. A woken listener counts (on Linux the wake
// shuts it down, which leaves it open and bound); a closed one does not.
bool is_listening_on(int fd, std::uint16_t port) {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0 ||
        addr.sin_family != AF_INET || ntohs(addr.sin_port) != port) {
        return false;
    }
    // Not SO_ACCEPTCONN, which Darwin does not answer. A socket bound to
    // the port with no peer is the listener: an accepted socket shares the
    // local port but has a peer.
    sockaddr_in peer{};
    socklen_t peer_len = sizeof(peer);
    return ::getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &peer_len) != 0 &&
           errno == ENOTCONN;
}

// The descriptor this process holds listening on `port`, or -1.
int listening_fd_for_port(std::uint16_t port) {
    DIR* d = ::opendir("/dev/fd");
    if (d == nullptr) {
        return -1;
    }
    int found = -1;
    while (const auto* e = ::readdir(d)) {
        const int fd = std::atoi(e->d_name);
        if (e->d_name[0] != '.' && is_listening_on(fd, port)) {
            found = fd;
            break;
        }
    }
    ::closedir(d);
    return found;
}

bool await_condition(const std::function<bool()>& cond, std::chrono::milliseconds bound = 10s) {
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while (!cond()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(5ms);
    }
    return true;
}

}  // namespace

// stop() must not close the listener while its accept thread can still be
// using it: it wakes the thread, joins it, and only then closes.
//
// Closing the listener to wake the thread is what it used to do, and on
// Darwin it wedged under load: a close() that lands while the other thread is
// entering accept() can miss it, and then both wait - accept() asleep, and
// close(), uninterruptibly, for accept() to let go of the descriptor - until
// a connection happens to arrive, which on an idle test coordinator is never.
// That was the occasional timeout of the two cycling tests below. On Linux,
// close() does not wake accept() at all, and a descriptor closed under a
// thread about to use it can be reissued to another listener in the process
// before the thread gets there.
//
// The kernel race itself is too narrow to hit on demand, so this pins the
// rule that rules it out: the accept thread is parked just before it waits,
// stop() is let run up to the point where it has woken that thread, and the
// listener must still be open and listening there.
TEST(ShutdownLeaks, StopJoinsTheAcceptThreadBeforeClosingItsListener) {
    namespace fault = clink::fault;
    // Before the fault guard, so an early exit resets the registry (releasing
    // the parked accept thread) before the coordinator's destructor joins it.
    Coordinator c;
    fault::Registry::instance().reset();
    fault::ScopedFault park{fault::Rule{.point = fault::points::kCoordinatorAcceptBeforeWait,
                                        .ordinal = 1,
                                        .action = fault::Action::Block}};
    fault::Registry::instance().arm({.point = fault::points::kCoordinatorStopAfterAcceptWake,
                                     .action = fault::Action::Observe});

    const auto port = c.start();
    ASSERT_TRUE(await_condition([] {
        return fault::Registry::instance().hits(fault::points::kCoordinatorAcceptBeforeWait) >= 1;
    })) << "the accept thread never started its first pass";
    const int listener = listening_fd_for_port(port);
    ASSERT_GE(listener, 0) << "no descriptor in this process is listening on " << port;

    std::thread stopper([&] { c.stop(); });
    const bool woke = await_condition([] {
        return fault::Registry::instance().hits(fault::points::kCoordinatorStopAfterAcceptWake) >=
               1;
    });
    const bool open_while_thread_lives = is_listening_on(listener, port);
    // Not asserting the count: the thread can be counted as a hit before it
    // has parked. The release still reaches it (reach() captures the epoch).
    fault::Registry::instance().release(fault::points::kCoordinatorAcceptBeforeWait);
    stopper.join();

    ASSERT_TRUE(woke) << "stop() never reached the point after waking the accept thread";
    EXPECT_TRUE(open_while_thread_lives)
        << "stop() closed the listener while the accept thread could still accept() on it";
    EXPECT_FALSE(is_listening_on(listener, port)) << "stop() returned with its listener open";
}

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
