#pragma once

// Statistics for cost-based optimization (clink::sql).
//
// clink is a STREAMING engine: there are no scan-time statistics on an
// unbounded source, so table statistics are DECLARED by the user in the
// table's WITH options and read here. They drive the cardinality estimator
// (cardinality.hpp) which the cost-based join reorderer uses. For a stream,
// `row_count` is a cardinality proxy used to compare plan shapes / minimise
// intermediate state, not a literal count; per-column NDV (distinct values)
// is what estimates join-output cardinality and operator state growth.
//
// WITH-option schema (all optional; absent => unknown => 0):
//   row_count='<n>'      estimated cardinality of the table/stream
//   ndv_<col>='<n>'      number of distinct values of column <col>
//   nulls_<col>='<f>'    null fraction of column <col> in [0,1]
//
// Build-vs-buy note: the estimator + reorderer are hand-rolled on clink's own
// plan IR (no external optimizer fits a JVM-free Arrow-native streaming engine;
// see the project memory). The designs are borrowed from DuckDB's
// CardinalityEstimator + the Selinger/Moerkotte literature.

#include <map>
#include <string>

#include "clink/config/decimal.hpp"
#include "clink/sql/catalog.hpp"

namespace clink::sql {

// Per-column statistics. ndv = number of distinct values (0 = unknown);
// null_fraction in [0,1].
struct ColumnStats {
    double ndv = 0.0;
    double null_fraction = 0.0;

    [[nodiscard]] bool ndv_known() const noexcept { return ndv > 0.0; }
};

// Statistics for a relation = a plan node's output: an estimated row count and
// per-output-column stats keyed by the column's name in this relation's output.
struct RelStats {
    double row_count = 0.0;  // 0 = unknown
    std::map<std::string, ColumnStats> columns;

    [[nodiscard]] bool row_count_known() const noexcept { return row_count > 0.0; }

    // Stats for `col`, or a default (all-unknown) if absent.
    [[nodiscard]] ColumnStats column(const std::string& col) const {
        auto it = columns.find(col);
        return it == columns.end() ? ColumnStats{} : it->second;
    }
};

namespace stats_detail {

// Parse a WITH-option value as a non-negative double; nullopt if missing/bad.
inline std::optional<double> opt_number(const TableDef& t, const std::string& key) {
    auto it = t.properties.find(key);
    if (it == t.properties.end() || it->second.empty()) {
        return std::nullopt;
    }
    try {
        const double v = std::stod(it->second);
        return v >= 0.0 ? std::optional<double>{v} : std::nullopt;
    } catch (...) {
        return std::nullopt;
    }
}

}  // namespace stats_detail

// Read a table's declared statistics from its WITH options. Unknown fields stay
// 0. NDV is clamped to the row count (a column cannot have more distinct values
// than rows) when both are known.
inline RelStats table_stats_from(const TableDef& t) {
    RelStats s;
    if (auto rc = stats_detail::opt_number(t, "row_count"); rc.has_value()) {
        s.row_count = *rc;
    }
    for (const auto& c : t.columns) {
        ColumnStats cs;
        if (auto ndv = stats_detail::opt_number(t, "ndv_" + c.name); ndv.has_value()) {
            cs.ndv = (s.row_count_known() && *ndv > s.row_count) ? s.row_count : *ndv;
        }
        if (auto nf = stats_detail::opt_number(t, "nulls_" + c.name); nf.has_value()) {
            cs.null_fraction = *nf > 1.0 ? 1.0 : *nf;
        }
        if (cs.ndv != 0.0 || cs.null_fraction != 0.0) {
            s.columns[c.name] = cs;
        }
    }
    return s;
}

}  // namespace clink::sql
