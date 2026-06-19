#pragma once

// CPU affinity + thread identity primitives for the executor.
//
// These are the foundation for shard-per-core execution: a worker thread that
// owns a key-group range and its own state shard wants to stay on one core so
// that shard's working set stays hot in that core's cache. Pinning is also the
// difference between "16 threads the scheduler migrates freely" and "16 threads
// that each own a core", which is the whole point of the share-nothing model.
//
// Everything here is best-effort and cross-platform:
//
//   * Linux  - real pinning via pthread_setaffinity_np; real naming via
//              pthread_setname_np (kernel limit 15 chars + NUL).
//   * macOS  - naming works (pthread_setname_np, current thread only); there is
//              NO hard core pinning (THREAD_AFFINITY_POLICY is an advisory hint
//              on x86 and absent on Apple Silicon), so pinning is an honest
//              no-op that returns false.
//   * other  - no-op, returns false.
//
// Callers must treat pinning as advisory: pin_current_thread_to_core() returning
// false is normal on macOS and never an error. Naming never fails loudly.
//
// NOTE (host vs Linux): the host dev machine is macOS, where the pinning path is
// a no-op. The Linux path is the one that matters in production and must be
// verified in the Docker image, not just on the host.

#include <string>
#include <thread>

#if defined(__linux__)
// cpu_set_t / CPU_SET / pthread_setaffinity_np are GNU extensions exposed under
// _GNU_SOURCE, which the standard C++ toolchain (libstdc++) defines implicitly.
#include <pthread.h>
#include <sched.h>
#elif defined(__APPLE__)
#include <pthread.h>
#endif

namespace clink {

// Number of logical cores the runtime should assume, never less than 1
// (hardware_concurrency() may report 0 when it cannot detect the count).
inline unsigned core_count() noexcept {
    const unsigned n = std::thread::hardware_concurrency();
    return n == 0 ? 1U : n;
}

// True only where pin_current_thread_to_core() can actually bind a thread to a
// core. Lets callers decide whether to bother computing a placement.
constexpr bool affinity_supported() noexcept {
#if defined(__linux__)
    return true;
#else
    return false;
#endif
}

// Name the calling thread (visible in `top -H`, gdb, perf, /proc/<pid>/task).
// Pure diagnostics: helps tell apart shard threads when the executor fans one
// operator across many. Silently truncated to the platform limit; never throws.
inline void set_current_thread_name(const std::string& name) {
#if defined(__linux__)
    // Linux caps thread names at 16 bytes including the NUL terminator.
    constexpr std::size_t kMaxLen = 15;
    const std::string truncated = name.size() > kMaxLen ? name.substr(0, kMaxLen) : name;
    pthread_setname_np(pthread_self(), truncated.c_str());
#elif defined(__APPLE__)
    // macOS names the current thread only (no thread handle argument) and
    // allows up to 63 chars; truncate generously to stay within it.
    constexpr std::size_t kMaxLen = 63;
    const std::string truncated = name.size() > kMaxLen ? name.substr(0, kMaxLen) : name;
    pthread_setname_np(truncated.c_str());
#else
    (void)name;
#endif
}

// Bind the calling thread to a single logical core. Returns true if the binding
// was applied, false if pinning is unsupported on this platform or the call
// failed. The core index is taken modulo core_count() so an out-of-range index
// wraps rather than failing.
inline bool pin_current_thread_to_core(unsigned core) {
#if defined(__linux__)
    const unsigned target = core % core_count();
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(target, &set);
    return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
#else
    (void)core;
    return false;
#endif
}

}  // namespace clink
