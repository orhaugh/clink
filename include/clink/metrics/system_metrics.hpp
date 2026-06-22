#pragma once

// Process + host resource metrics (CPU, memory, disk, file descriptors,
// threads) for a clink_node. Names follow the Prometheus process-collector
// convention under the clink_ prefix so the existing /metrics endpoint and the
// console's parser pick them up with no special-casing.
//
// sample_system_metrics() reads the current values from the OS and writes them
// into MetricsRegistry::global(). It is called sample-on-scrape from the
// /metrics handler, so the values are always fresh at read time. The CPU
// counter + the instantaneous CPU percent are derived from the delta since the
// previous sample (guarded internally, safe under concurrent scrapes).
//
// Cross-platform: Linux reads /proc; macOS uses mach + POSIX. Anything the
// platform does not expose is simply left unset (the gauge keeps its last
// value / zero) rather than failing the scrape.

#include <string>

namespace clink::metrics {

// Gauge: current resident set size (physical memory) in bytes.
inline constexpr const char* kProcResidentMemoryBytes = "clink_process_resident_memory_bytes";
// Gauge: current virtual memory size in bytes.
inline constexpr const char* kProcVirtualMemoryBytes = "clink_process_virtual_memory_bytes";
// Counter: cumulative CPU time (user + system) in milliseconds.
inline constexpr const char* kProcCpuMsTotal = "clink_process_cpu_ms_total";
// Gauge: CPU utilisation over the last scrape interval, in percent (can exceed
// 100 on multi-core; 250 = 2.5 cores busy).
inline constexpr const char* kProcCpuPercent = "clink_process_cpu_percent";
// Gauge: open file descriptors.
inline constexpr const char* kProcOpenFds = "clink_process_open_fds";
// Gauge: live thread count.
inline constexpr const char* kProcThreads = "clink_process_threads";
// Gauges: filesystem capacity for the node's working/data path, in bytes.
inline constexpr const char* kDiskTotalBytes = "clink_disk_total_bytes";
inline constexpr const char* kDiskFreeBytes = "clink_disk_free_bytes";
inline constexpr const char* kDiskUsedBytes = "clink_disk_used_bytes";

// Sample the current process + host resource values into
// MetricsRegistry::global(). `disk_path` selects the filesystem measured for
// the clink_disk_* gauges (defaults to the working directory).
void sample_system_metrics(const std::string& disk_path = ".");

}  // namespace clink::metrics
