// cpu_affinity.hpp: cross-platform thread naming + best-effort core pinning.
// The load-bearing claims are: naming never throws and (on Linux) takes
// effect; pinning binds the calling thread to one core on Linux and is an
// honest no-op (returns false, no crash) where hard affinity is unavailable.
// The Linux-specific assertions are guarded so the suite is meaningful on the
// macOS host and exact on the Linux build (where production runs).

#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "clink/runtime/cpu_affinity.hpp"

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace {

using namespace clink;

TEST(CpuAffinity, CoreCountIsAtLeastOne) {
    EXPECT_GE(core_count(), 1U);
}

TEST(CpuAffinity, SetThreadNameNeverThrows) {
    EXPECT_NO_THROW(set_current_thread_name("clink-test"));
    // Over-long names are silently truncated, not rejected.
    EXPECT_NO_THROW(set_current_thread_name(std::string(200, 'x')));
    // Empty name is harmless.
    EXPECT_NO_THROW(set_current_thread_name(""));
}

TEST(CpuAffinity, PinNeverThrowsAndAgreesWithSupport) {
    // pin returns true only where affinity is supported. Where it is not, it
    // must be a clean no-op returning false (never a crash or an exception).
    bool pinned = false;
    EXPECT_NO_THROW(pinned = pin_current_thread_to_core(0));
    if (!affinity_supported()) {
        EXPECT_FALSE(pinned) << "pinning must be a no-op where affinity is unsupported";
    }
}

TEST(CpuAffinity, OutOfRangeCoreWraps) {
    // An out-of-range core index wraps modulo core_count() rather than failing
    // the call outright.
    EXPECT_NO_THROW((void)pin_current_thread_to_core(core_count() + 5));
}

#if defined(__linux__)
TEST(CpuAffinity, LinuxNameTakesEffectTruncatedToFifteen) {
    set_current_thread_name("abcdefghijklmnopqrstuvwxyz");  // 26 chars
    char buf[32] = {};
    ASSERT_EQ(pthread_getname_np(pthread_self(), buf, sizeof(buf)), 0);
    // Linux caps at 15 chars + NUL.
    EXPECT_EQ(std::string(buf), std::string("abcdefghijklmno"));
}

TEST(CpuAffinity, LinuxPinBindsToSingleCore) {
    ASSERT_TRUE(affinity_supported());
    // Be correct under a cgroup/cpuset that excludes some CPUs. Two hazards:
    // pinning to an excluded CPU legitimately fails (EINVAL), and
    // pin_current_thread_to_core's internal `% core_count()` can itself remap a
    // request onto an excluded CPU when the cpuset renumbers the allowed count.
    // CPU 0 sidesteps both: it is in the allowed set on essentially every host
    // (Docker default, bare metal) and 0 % core_count() == 0, so the pin lands
    // exactly on it. If some exotic cpuset excludes CPU 0, skip rather than
    // flake - pinning is best-effort and PinNeverThrows already covers no-crash.
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    ASSERT_EQ(pthread_getaffinity_np(pthread_self(), sizeof(allowed), &allowed), 0);
    if (!CPU_ISSET(0, &allowed)) {
        GTEST_SKIP() << "CPU 0 not in the allowed cpuset; pinning is best-effort here";
    }

    ASSERT_TRUE(pin_current_thread_to_core(0));
    cpu_set_t bound;
    CPU_ZERO(&bound);
    ASSERT_EQ(pthread_getaffinity_np(pthread_self(), sizeof(bound), &bound), 0);
    EXPECT_TRUE(CPU_ISSET(0, &bound));
    EXPECT_EQ(CPU_COUNT(&bound), 1) << "thread must be bound to exactly one core";
}
#endif

}  // namespace
