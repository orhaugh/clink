#pragma once

// ANALYZE TABLE statistics collection (clink::sql).
//
// Scans a bounded relation's rows and computes EXACT per-column statistics
// (row_count, NDV, null fraction, an equi-depth histogram, and most-common
// values), then emits the WITH-option strings that table_stats_from() reads
// back (statistics.hpp). The histogram values + MCV keys use the SAME
// canonicalisation as the query-time selectivity estimator (cardinality_detail::
// lit_canonical / lit_as_double), so an analyzed value matches a predicate
// literal by construction.
//
// v1 is EXACT over a bounded scan: per-column it holds a value-frequency map (for
// NDV + MCV) and, for numeric columns, the observed values (for the histogram).
// Memory is therefore O(rows) for numeric columns and O(distinct) otherwise;
// reservoir sampling for large tables is a deferred follow-on. ANALYZE is
// rejected on unbounded sources before any scan, so the bound is finite.

#include <algorithm>
#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/sql/cardinality.hpp"  // lit_canonical / lit_as_double
#include "clink/sql/row.hpp"

namespace clink::sql {

namespace analyze_detail {

// Compact decimal text for a fraction/double: trims trailing zeros so 0.8 ->
// "0.8" (not "0.800000"), round-tripping through std::stod in opt_* parsers.
inline std::string fmt_double(double d) {
    std::string s = std::to_string(d);
    if (auto dot = s.find('.'); dot != std::string::npos) {
        auto last = s.find_last_not_of('0');
        if (last == dot) {
            --last;  // drop the now-bare '.'
        }
        s.erase(last + 1);
    }
    return s;
}

inline std::string join_doubles(const std::vector<double>& xs) {
    std::string out;
    for (std::size_t i = 0; i < xs.size(); ++i) {
        if (i != 0) {
            out += ',';
        }
        out += fmt_double(xs[i]);
    }
    return out;
}

}  // namespace analyze_detail

// Accumulates exact per-column statistics over a scanned bounded relation, then
// produces the WITH-option map to merge into the analyzed table's TableDef.
class StatsCollector {
public:
    // `columns` are the columns to analyze (typically all of the table's).
    // hist_buckets bounds the equi-depth histogram resolution; max_mcv bounds the
    // most-common-values list.
    explicit StatsCollector(std::vector<std::string> columns,
                            std::size_t hist_buckets = 64,
                            std::size_t max_mcv = 16)
        : columns_(std::move(columns)),
          hist_buckets_(hist_buckets == 0 ? 1 : hist_buckets),
          max_mcv_(max_mcv) {
        per_.resize(columns_.size());
    }

    // Observe one scanned row.
    void observe(const Row& row) {
        ++row_count_;
        for (std::size_t i = 0; i < columns_.size(); ++i) {
            Col& c = per_[i];
            auto it = row.values.find(columns_[i]);
            if (it == row.values.end() || it->second.is_null()) {
                ++c.nulls;
                continue;
            }
            if (auto key = cardinality_detail::lit_canonical(it->second)) {
                ++c.freq[*key];
            }
            if (auto d = cardinality_detail::lit_as_double(it->second)) {
                c.numeric.push_back(*d);
            }
        }
    }

    [[nodiscard]] std::uint64_t row_count() const noexcept { return row_count_; }

    // The computed statistics as WITH-option key->value strings. Empty when no
    // rows were observed (nothing to assert). row_count + per-column ndv_/nulls_
    // / hist_ / mcv_, in the exact format statistics.hpp parses.
    [[nodiscard]] std::map<std::string, std::string> to_with_options() const {
        using analyze_detail::fmt_double;
        std::map<std::string, std::string> out;
        if (row_count_ == 0) {
            return out;
        }
        out["row_count"] = std::to_string(row_count_);
        const double rows = static_cast<double>(row_count_);
        for (std::size_t i = 0; i < columns_.size(); ++i) {
            const Col& c = per_[i];
            const std::string& name = columns_[i];
            out["ndv_" + name] = std::to_string(c.freq.size());
            if (c.nulls > 0) {
                out["nulls_" + name] = fmt_double(static_cast<double>(c.nulls) / rows);
            }
            if (auto h = histogram_csv(c)) {
                out["hist_" + name] = *h;
            }
            if (auto m = mcv_csv(c, rows)) {
                out["mcv_" + name] = *m;
            }
        }
        return out;
    }

private:
    struct Col {
        std::unordered_map<std::string, std::uint64_t> freq;  // canonical value -> count
        std::vector<double> numeric;                          // numeric values, for the histogram
        std::uint64_t nulls = 0;
    };

    // Equi-depth boundaries over the sorted numeric values: B = min(hist_buckets,
    // n-1) buckets, B+1 boundaries at quantile positions (each bucket ~equal
    // count). nullopt when fewer than two numeric values (no usable bucket) or
    // the column is non-numeric.
    [[nodiscard]] std::optional<std::string> histogram_csv(const Col& c) const {
        if (c.numeric.size() < 2) {
            return std::nullopt;
        }
        std::vector<double> v = c.numeric;
        std::sort(v.begin(), v.end());
        const std::size_t n = v.size();
        const std::size_t B = std::min(hist_buckets_, n - 1);
        std::vector<double> bounds;
        bounds.reserve(B + 1);
        for (std::size_t j = 0; j <= B; ++j) {
            const std::size_t idx = static_cast<std::size_t>(
                (static_cast<double>(j) * static_cast<double>(n - 1)) / static_cast<double>(B));
            bounds.push_back(v[idx]);
        }
        return analyze_detail::join_doubles(bounds);
    }

    // Top-max_mcv values by count, as "value:freq" pairs. Skips values whose
    // canonical text contains ',' or ':' (cannot round-trip the CSV format).
    [[nodiscard]] std::optional<std::string> mcv_csv(const Col& c, double rows) const {
        std::vector<std::pair<std::string, std::uint64_t>> items(c.freq.begin(), c.freq.end());
        std::sort(items.begin(), items.end(), [](const auto& a, const auto& b) {
            return a.second > b.second;
        });
        std::string out;
        std::size_t kept = 0;
        for (const auto& [value, count] : items) {
            if (kept >= max_mcv_) {
                break;
            }
            if (value.find(',') != std::string::npos || value.find(':') != std::string::npos) {
                continue;  // unrepresentable in the CSV WITH-option format
            }
            if (!out.empty()) {
                out += ',';
            }
            out += value + ':' + analyze_detail::fmt_double(static_cast<double>(count) / rows);
            ++kept;
        }
        if (out.empty()) {
            return std::nullopt;
        }
        return out;
    }

    std::vector<std::string> columns_;
    std::size_t hist_buckets_;
    std::size_t max_mcv_;
    std::uint64_t row_count_ = 0;
    std::vector<Col> per_;
};

class Catalog;

// Execute ANALYZE: scan the bounded table `name` (its Row source, built in
// process via OperatorRegistry::default_instance() + LocalExecutor), compute
// exact column statistics with a StatsCollector, and write them into `catalog`
// so the optimizer's selectivity estimator picks them up. `columns` empty means
// all of the table's columns. Throws TranslationError when the table is unknown,
// is not a bounded Row source (e.g. a Kafka string stream), or its source
// factory is not registered (connector impl not linked).
void analyze_table(Catalog& catalog,
                   const std::string& name,
                   const std::vector<std::string>& columns = {});

}  // namespace clink::sql
