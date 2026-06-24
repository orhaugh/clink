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
//   hist_<col>='b0,b1,..,bN'  equi-depth histogram boundaries of column <col>
//                             (ascending; N+1 boundaries => N equal-mass
//                             buckets). Numeric columns only; drives range
//                             selectivity (<, <=, >, >=, BETWEEN).
//   mcv_<col>='v1:f1,v2:f2,..' most-common values + their frequencies (fraction
//                             of rows, in [0,1]); drives equality/IN selectivity
//                             on skewed columns instead of uniform 1/NDV.
//
// These are DECLARED here; ANALYZE TABLE (a later increment) computes the same
// fields by scanning a bounded source and writes them back as the same WITH
// options, so the read path is unified.
//
// Build-vs-buy note: the estimator + reorderer are hand-rolled on clink's own
// plan IR (no external optimizer fits a JVM-free Arrow-native streaming engine;
// see the project memory). The designs are borrowed from DuckDB's
// CardinalityEstimator + PostgreSQL's MCV+histogram selectivity model + the
// Selinger/Moerkotte literature.

#include <algorithm>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "clink/config/decimal.hpp"
#include "clink/sql/catalog.hpp"

namespace clink::sql {

// One most-common value: its canonical-string key (numbers as their decimal
// text, strings verbatim, bools "true"/"false") and the fraction of rows equal
// to it. A predicate literal is canonicalised the same way to look it up.
struct McvEntry {
    std::string value;
    double frequency = 0.0;  // in [0,1]
};

// Per-column statistics. ndv = number of distinct values (0 = unknown);
// null_fraction in [0,1]. histogram = equi-depth boundaries (ascending; empty =
// none). mcv = most-common values (frequency-descending; empty = none).
struct ColumnStats {
    double ndv = 0.0;
    double null_fraction = 0.0;
    std::vector<double> histogram;
    std::vector<McvEntry> mcv;

    [[nodiscard]] bool ndv_known() const noexcept { return ndv > 0.0; }
    // A usable equi-depth histogram needs at least one bucket (two boundaries).
    [[nodiscard]] bool has_histogram() const noexcept { return histogram.size() >= 2; }
    // Total fraction of rows covered by the MCV list (the residual NDV carries
    // the rest).
    [[nodiscard]] double mcv_frequency_sum() const noexcept {
        double s = 0.0;
        for (const auto& e : mcv) {
            s += e.frequency;
        }
        return s > 1.0 ? 1.0 : s;
    }
    // Frequency of `key` if it is a most-common value, else nullopt.
    [[nodiscard]] std::optional<double> mcv_frequency(const std::string& key) const noexcept {
        for (const auto& e : mcv) {
            if (e.value == key) {
                return e.frequency;
            }
        }
        return std::nullopt;
    }
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

// Split `s` on `delim` into trimmed non-empty pieces.
inline std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::size_t pos = 0;
    while (pos <= s.size()) {
        auto end = s.find(delim, pos);
        if (end == std::string::npos) {
            end = s.size();
        }
        auto piece = s.substr(pos, end - pos);
        // trim ASCII whitespace
        std::size_t b = piece.find_first_not_of(" \t");
        std::size_t e = piece.find_last_not_of(" \t");
        if (b != std::string::npos) {
            out.push_back(piece.substr(b, e - b + 1));
        }
        if (end == s.size()) {
            break;
        }
        pos = end + 1;
    }
    return out;
}

// Parse `hist_<col>` = "b0,b1,..,bN" -> ascending boundaries (>=2). Returns
// empty on any parse failure or fewer than two boundaries (no usable bucket).
inline std::vector<double> opt_histogram(const TableDef& t, const std::string& key) {
    auto it = t.properties.find(key);
    if (it == t.properties.end() || it->second.empty()) {
        return {};
    }
    std::vector<double> bounds;
    for (const auto& tok : split(it->second, ',')) {
        try {
            bounds.push_back(std::stod(tok));
        } catch (...) {
            return {};  // malformed: ignore the whole declaration
        }
    }
    if (bounds.size() < 2) {
        return {};
    }
    std::sort(bounds.begin(), bounds.end());  // tolerate an unsorted declaration
    return bounds;
}

// Parse `mcv_<col>` = "v1:f1,v2:f2,.." -> most-common values. A value may itself
// contain ':' only if it is not the last colon (we split on the LAST ':' so a
// value like "a:b" with frequency works). Frequencies out of [0,1] or
// unparseable drop that entry; the list is sorted frequency-descending.
inline std::vector<McvEntry> opt_mcv(const TableDef& t, const std::string& key) {
    auto it = t.properties.find(key);
    if (it == t.properties.end() || it->second.empty()) {
        return {};
    }
    std::vector<McvEntry> out;
    for (const auto& pair : split(it->second, ',')) {
        auto colon = pair.rfind(':');
        if (colon == std::string::npos || colon == 0 || colon + 1 >= pair.size()) {
            continue;
        }
        try {
            const double f = std::stod(pair.substr(colon + 1));
            if (f >= 0.0 && f <= 1.0) {
                out.push_back(McvEntry{pair.substr(0, colon), f});
            }
        } catch (...) {
            // skip a malformed entry
        }
    }
    std::sort(out.begin(), out.end(), [](const McvEntry& a, const McvEntry& b) {
        return a.frequency > b.frequency;
    });
    return out;
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
        cs.histogram = stats_detail::opt_histogram(t, "hist_" + c.name);
        cs.mcv = stats_detail::opt_mcv(t, "mcv_" + c.name);
        if (cs.ndv != 0.0 || cs.null_fraction != 0.0 || cs.has_histogram() || !cs.mcv.empty()) {
            s.columns[c.name] = cs;
        }
    }
    return s;
}

}  // namespace clink::sql
