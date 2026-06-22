#include "clink/metrics/system_metrics.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <string>
#include <unistd.h>

#include <sys/resource.h>
#include <sys/statvfs.h>

#include "clink/metrics/metrics_registry.hpp"

#if defined(__APPLE__)
#include <mach/mach.h>
#endif

namespace clink::metrics {

namespace {

// Count entries in a directory (used for /proc/self/fd, /proc/self/task,
// /dev/fd). Returns nullopt if the directory cannot be read.
std::optional<std::int64_t> count_dir_entries(const char* path) {
    std::error_code ec;
    std::filesystem::directory_iterator it(path, ec);
    if (ec) {
        return std::nullopt;
    }
    std::int64_t n = 0;
    for (const auto& entry : it) {
        (void)entry;
        ++n;
    }
    return n;
}

// Cumulative process CPU time (user + system) in milliseconds, via POSIX
// getrusage. Portable across Linux and macOS.
std::int64_t cpu_ms_total() {
    rusage ru{};
    if (getrusage(RUSAGE_SELF, &ru) != 0) {
        return 0;
    }
    const auto to_ms = [](const timeval& tv) {
        return (static_cast<std::int64_t>(tv.tv_sec) * 1000) +
               (static_cast<std::int64_t>(tv.tv_usec) / 1000);
    };
    return to_ms(ru.ru_utime) + to_ms(ru.ru_stime);
}

struct MemSample {
    std::int64_t resident_bytes{0};
    std::int64_t virtual_bytes{0};
};

#if defined(__linux__)
MemSample read_memory() {
    // /proc/self/statm: size resident shared text lib data dt (all in pages).
    MemSample m;
    std::ifstream in("/proc/self/statm");
    long long size_pages = 0;
    long long resident_pages = 0;
    if (in >> size_pages >> resident_pages) {
        const auto page = static_cast<std::int64_t>(::sysconf(_SC_PAGESIZE));
        m.virtual_bytes = static_cast<std::int64_t>(size_pages) * page;
        m.resident_bytes = static_cast<std::int64_t>(resident_pages) * page;
    }
    return m;
}

std::optional<std::int64_t> read_threads() {
    return count_dir_entries("/proc/self/task");
}

std::optional<std::int64_t> read_open_fds() {
    return count_dir_entries("/proc/self/fd");
}
#elif defined(__APPLE__)
MemSample read_memory() {
    MemSample m;
    mach_task_basic_info info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(
            mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) ==
        KERN_SUCCESS) {
        m.resident_bytes = static_cast<std::int64_t>(info.resident_size);
        m.virtual_bytes = static_cast<std::int64_t>(info.virtual_size);
    }
    return m;
}

std::optional<std::int64_t> read_threads() {
    thread_act_array_t threads = nullptr;
    mach_msg_type_number_t n = 0;
    if (task_threads(mach_task_self(), &threads, &n) != KERN_SUCCESS) {
        return std::nullopt;
    }
    const auto count = static_cast<std::int64_t>(n);
    vm_deallocate(
        mach_task_self(), reinterpret_cast<vm_address_t>(threads), n * sizeof(thread_act_t));
    return count;
}

std::optional<std::int64_t> read_open_fds() {
    // /dev/fd lists the calling process's open descriptors on macOS.
    return count_dir_entries("/dev/fd");
}
#else
MemSample read_memory() {
    return {};
}
std::optional<std::int64_t> read_threads() {
    return std::nullopt;
}
std::optional<std::int64_t> read_open_fds() {
    return std::nullopt;
}
#endif

// CPU-percent + counter state behind a function-local static (matches the
// logging module's idiom), guarded so concurrent /metrics scrapes don't race
// on the deltas.
struct CpuState {
    std::mutex mu;
    std::int64_t last_cpu_ms = 0;
    std::int64_t last_wall_ms = 0;
    std::int64_t counter_pushed = 0;
    bool primed = false;
};

CpuState& cpu_state() {
    static CpuState s;
    return s;
}

std::int64_t now_wall_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

}  // namespace

void sample_system_metrics(const std::string& disk_path) {
    auto& r = MetricsRegistry::global();

    const MemSample mem = read_memory();
    r.gauge(kProcResidentMemoryBytes).set(mem.resident_bytes);
    r.gauge(kProcVirtualMemoryBytes).set(mem.virtual_bytes);

    if (const auto fds = read_open_fds()) {
        r.gauge(kProcOpenFds).set(*fds);
    }
    if (const auto threads = read_threads()) {
        r.gauge(kProcThreads).set(*threads);
    }

    // CPU: advance the cumulative-ms counter by the delta since last sample,
    // and compute an instantaneous percent over the scrape interval.
    const std::int64_t cpu_ms = cpu_ms_total();
    const std::int64_t wall_ms = now_wall_ms();
    {
        auto& st = cpu_state();
        std::lock_guard lock(st.mu);
        if (cpu_ms > st.counter_pushed) {
            r.counter(kProcCpuMsTotal)
                .increment(static_cast<std::uint64_t>(cpu_ms - st.counter_pushed));
            st.counter_pushed = cpu_ms;
        }
        if (st.primed) {
            const std::int64_t d_cpu = cpu_ms - st.last_cpu_ms;
            const std::int64_t d_wall = wall_ms - st.last_wall_ms;
            if (d_wall > 0 && d_cpu >= 0) {
                r.gauge(kProcCpuPercent).set((d_cpu * 100) / d_wall);
            }
        }
        st.last_cpu_ms = cpu_ms;
        st.last_wall_ms = wall_ms;
        st.primed = true;
    }

    // Disk: capacity of the filesystem backing the node's working/data path.
    struct statvfs vfs{};
    if (::statvfs(disk_path.c_str(), &vfs) == 0) {
        const auto frsize = static_cast<std::int64_t>(vfs.f_frsize);
        const std::int64_t total = static_cast<std::int64_t>(vfs.f_blocks) * frsize;
        const std::int64_t avail = static_cast<std::int64_t>(vfs.f_bavail) * frsize;
        const std::int64_t freeb = static_cast<std::int64_t>(vfs.f_bfree) * frsize;
        r.gauge(kDiskTotalBytes).set(total);
        r.gauge(kDiskFreeBytes).set(avail);
        r.gauge(kDiskUsedBytes).set(total - freeb);
    }
}

}  // namespace clink::metrics
