#pragma once

// SQL frontend observability.
//
// Coverage:
//   - clink_sql_parses_total
//   - clink_sql_parse_errors_total
//   - clink_sql_parse_duration_ns_sum / count
//   - clink_sql_binds_total
//   - clink_sql_bind_errors_total
//   - clink_sql_bind_duration_ns_sum / count
//   - clink_sql_optimizes_total
//   - clink_sql_optimize_duration_ns_sum / count
//   - clink_sql_physical_plans_total
//   - clink_sql_physical_plan_duration_ns_sum / count
//
// Each metric is a flat name; the per-statement-type breakdown can
// be added later as a {type=...} tag once we plumb the SELECT /
// INSERT / CREATE-TABLE / ... distinction through the helpers.

#include <cstdint>

#include "clink/metrics/metrics_registry.hpp"

namespace clink::metrics {

inline constexpr const char* kSqlParses = "clink_sql_parses_total";
inline constexpr const char* kSqlParseErrors = "clink_sql_parse_errors_total";
inline constexpr const char* kSqlBinds = "clink_sql_binds_total";
inline constexpr const char* kSqlBindErrors = "clink_sql_bind_errors_total";
inline constexpr const char* kSqlOptimizes = "clink_sql_optimizes_total";
inline constexpr const char* kSqlPhysicalPlans = "clink_sql_physical_plans_total";

// Per-phase duration histogram bases (OBS-1b). Each exposes
// <base>_{bucket,sum,count}, keeping the historical _sum / _count line names.
inline constexpr const char* kSqlParseNs = "clink_sql_parse_duration_ns";
inline constexpr const char* kSqlBindNs = "clink_sql_bind_duration_ns";
inline constexpr const char* kSqlOptimizeNs = "clink_sql_optimize_duration_ns";
inline constexpr const char* kSqlPhysicalNs = "clink_sql_physical_plan_duration_ns";

namespace sql {

inline void parse_completed(std::uint64_t duration_ns) {
    MetricsRegistry::global().counter(kSqlParses).increment();
    MetricsRegistry::global().histogram(kSqlParseNs).observe(static_cast<double>(duration_ns));
}
inline void parse_failed() {
    MetricsRegistry::global().counter(kSqlParseErrors).increment();
}
inline void bind_completed(std::uint64_t duration_ns) {
    MetricsRegistry::global().counter(kSqlBinds).increment();
    MetricsRegistry::global().histogram(kSqlBindNs).observe(static_cast<double>(duration_ns));
}
inline void bind_failed() {
    MetricsRegistry::global().counter(kSqlBindErrors).increment();
}
inline void optimize_completed(std::uint64_t duration_ns) {
    MetricsRegistry::global().counter(kSqlOptimizes).increment();
    MetricsRegistry::global().histogram(kSqlOptimizeNs).observe(static_cast<double>(duration_ns));
}
inline void physical_plan_completed(std::uint64_t duration_ns) {
    MetricsRegistry::global().counter(kSqlPhysicalPlans).increment();
    MetricsRegistry::global().histogram(kSqlPhysicalNs).observe(static_cast<double>(duration_ns));
}

}  // namespace sql

}  // namespace clink::metrics
