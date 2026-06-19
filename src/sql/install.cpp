#include "clink/sql/install.hpp"

#include <algorithm>
#include <cmath>
#include <deque>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "clink/cep/cep_operator.hpp"
#include "clink/cep/pattern.hpp"
#include "clink/config/json.hpp"
#include "clink/connectors/file_2pc_sink.hpp"
#include "clink/connectors/file_sink.hpp"
#include "clink/connectors/file_source.hpp"
#include "clink/operators/agg_function_registry.hpp"
#include "clink/operators/async_lookup_operator.hpp"
#include "clink/operators/filter_operator.hpp"
#include "clink/operators/json_predicate.hpp"
#include "clink/operators/json_value_expr.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/watermark_assigner_operator.hpp"
#include "clink/sql/async_function_registry.hpp"
#include "clink/sql/ptf_registry.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_kind.hpp"
#include "clink/time/watermark_strategy.hpp"

namespace clink::sql {

using clink::plugin::BuildContext;

namespace {

// Aggregate-function dispatch over a single value at a time, with
// per-(group, window) state stored alongside each output column.
struct AggSpec {
    std::string output_name;
    std::string fn;
    std::string input_column;  // "" for COUNT(*)
    bool distinct = false;     // COUNT(DISTINCT x) / STRING_AGG(DISTINCT x)
    std::string separator;     // STRING_AGG element separator
    double percentile = 0.0;   // PERCENTILE / APPROX_PERCENTILE fraction in [0,1]
    // When true, MIN / MAX keep a type-ordered multiset of live values so
    // a retraction (changelog delete) can recompute the extreme exactly.
    // Set only by the changelog GROUP BY, whose input may carry deletes;
    // the append-only window / OVER operators leave it false and use the
    // single-value fast path (no per-value memory).
    bool retractable = false;
    // UDAF (SQLOPT-3): set when `fn` names a registered aggregate UDF. `udaf`
    // holds the resolved accumulator closures (shared so copying an AggSpec is
    // cheap). When set, update/retract/finalize/merge dispatch to the closures
    // and the built-in AggState fields are untouched.
    bool is_udaf = false;
    std::shared_ptr<const clink::AggFunctionRegistry::Entry> udaf;
};

// Strict-weak ordering over JsonValue for the MIN / MAX multiset. Within
// a SQL column the values are homogeneous (all numbers or all strings),
// but the comparator is total over every JSON type so std::map stays
// well-formed: order by a type rank, then numerically / lexicographically
// within number / string (other types compare equal - MIN / MAX over them
// is not meaningful).
struct JsonValueLess {
    static int rank_(const clink::config::JsonValue& v) {
        if (v.is_null())
            return 0;
        if (v.is_bool())
            return 1;
        // #56: decimals (dec-strings) sort WITH numbers by value, not as plain
        // strings (a lexical sentinel-string compare would order MIN/MAX wrong).
        if (v.is_number() || clink::config::is_dec_string(v))
            return 2;
        if (v.is_string())
            return 3;
        return 4;  // array / object
    }
    static double as_double_(const clink::config::JsonValue& v) {
        if (clink::config::is_dec_string(v)) {
            auto d = clink::config::dec_parse(v.as_string());
            return d ? clink::config::dec_to_double(*d) : 0.0;
        }
        return v.as_number();
    }
    bool operator()(const clink::config::JsonValue& a, const clink::config::JsonValue& b) const {
        const int ra = rank_(a);
        const int rb = rank_(b);
        if (ra != rb)
            return ra < rb;
        if (ra == 2) {
            if (clink::config::is_dec_string(a) || clink::config::is_dec_string(b)) {
                auto da = clink::config::as_decimal(a);
                auto db = clink::config::as_decimal(b);
                if (da && db)
                    return clink::config::dec_compare(*da, *db) < 0;  // exact
                return as_double_(a) < as_double_(b);  // decimal vs non-integral double
            }
            return a.as_number() < b.as_number();
        }
        if (ra == 3)
            return a.as_string() < b.as_string();
        if (ra == 1)
            return static_cast<int>(a.as_bool()) < static_cast<int>(b.as_bool());
        return false;  // null / array / object: equal under this order
    }
};

struct AggState {
    // Tagged accumulators: SUM / AVG keep running double + count; the
    // variance/stddev family also keeps sum-of-squares. COUNT(DISTINCT)
    // and STRING_AGG keep a value -> multiplicity map (sorted, so
    // STRING_AGG output is deterministic and retraction just decrements).
    // MIN / MAX keep a single running value on the append-only path, or
    // (when AggSpec.retractable) a type-ordered value -> multiplicity map
    // so a delete recomputes the new extreme.
    double running_sum = 0.0;
    double running_sum_sq = 0.0;
    std::int64_t running_count = 0;
    clink::config::JsonValue running_min{nullptr};
    clink::config::JsonValue running_max{nullptr};
    bool initialised = false;
    std::map<std::string, int> value_counts;
    std::map<clink::config::JsonValue, int, JsonValueLess> minmax_counts;
    // #56: exact decimal SUM. running_sum (double, above) is kept in parallel
    // for AVG/variance; running_sum_dec accumulates every value exactly (a
    // dec-string or an integral number). The SUM result is emitted as a decimal
    // iff a dec-string was summed AND the exact accumulation stayed complete (no
    // non-integral double and no overflow broke it).
    clink::config::Decimal running_sum_dec{};
    bool sum_dec_started = false;
    bool sum_saw_decimal = false;
    bool sum_dec_complete = true;
    // PERCENTILE / APPROX_PERCENTILE: live numeric values, sorted lazily at
    // finalize. Retraction removes one occurrence (a late delete forces a
    // recompute of the extreme from the remaining values).
    std::vector<double> percentile_values;
    // ARRAY_AGG: live values in insertion order. NULLs are skipped (the
    // default IGNORE NULLS behaviour). Retraction erases the first matching occurrence;
    // finalize emits a JSON array (deduplicated for ARRAY_AGG(DISTINCT)).
    std::vector<clink::config::JsonValue> array_values;
    // UDAF (SQLOPT-3): the opaque accumulator a registered aggregate UDF
    // manages. Kept a JsonValue (not std::any / a closure) so it serialises on
    // the Row wire and would ride any keyed-state snapshot exactly like the
    // built-in AggState fields - i.e. at parity with them (these SQL aggregate
    // operators hold per-group state in-process and do not currently persist it
    // through a StateBackend, for built-ins or UDAFs alike). Lazily init()'d on
    // first touch so finalize/merge over an empty group still see a well-formed
    // value.
    clink::config::JsonValue udaf_acc{nullptr};
    bool udaf_initialised = false;
};

// Key a value for the multiplicity map. STRING_AGG joins the raw text
// (strings unquoted); COUNT(DISTINCT) keys on the canonical
// serialization so 1 and "1" stay distinct.
inline std::string agg_value_key(const clink::config::JsonValue& v, const AggSpec& spec) {
    // #56: a decimal keys on its scale-invariant canonical text (1.10 == 1.1),
    // and string_agg joins that clean text form (sentinel stripped).
    if (clink::config::is_dec_string(v)) {
        if (auto d = clink::config::as_decimal(v))
            return clink::config::dec_canonical_key(*d);
    }
    if (spec.fn == "string_agg")
        return v.is_string() ? v.as_string() : v.serialize(0);
    return v.serialize(0);
}

// #56: numeric value as a double for the double-accumulating aggregates
// (AVG / variance), tolerating a dec-string operand (would otherwise crash on
// as_number). Returns nullopt for non-numeric / null.
inline std::optional<double> agg_numeric_double(const clink::config::JsonValue& v) {
    return clink::operators::value_expr_detail::numeric_as_double(v);
}

// #56: parse a "name:scale,name:scale,..." param into a column -> scale map
// (used by the schema-aware Row source + sinks to tag/quantise DECIMAL columns).
inline std::map<std::string, int> parse_decimal_columns(const std::string& csv) {
    std::map<std::string, int> out;
    std::size_t pos = 0;
    while (pos < csv.size()) {
        const std::size_t comma = csv.find(',', pos);
        const std::string entry = csv.substr(pos, comma - pos);
        const std::size_t colon = entry.rfind(':');
        if (colon != std::string::npos) {
            try {
                out[entry.substr(0, colon)] = std::stoi(entry.substr(colon + 1));
            } catch (...) {
            }
        }
        if (comma == std::string::npos)
            break;
        pos = comma + 1;
    }
    return out;
}

// Decode the optional distinct / separator fields shared by every
// aggregate-bearing operator factory (group-by, tumbling / hopping /
// session / cumulate windows).
// Built-in aggregate names. A UDAF must never shadow one (else dispatch would
// route a built-in through the UDAF path), so decode_agg_extras only resolves a
// UDAF for a name NOT in this set. Mirrors the binder's is_aggregate_fn_name
// literal set.
inline bool is_builtin_agg_fn(const std::string& fn) {
    return fn == "sum" || fn == "count" || fn == "min" || fn == "max" || fn == "avg" ||
           fn == "stddev" || fn == "stddev_pop" || fn == "stddev_samp" || fn == "variance" ||
           fn == "var_pop" || fn == "var_samp" || fn == "string_agg" || fn == "listagg" ||
           fn == "percentile" || fn == "approx_percentile" || fn == "array_agg";
}

inline void decode_agg_extras(const clink::config::JsonValue& entry, AggSpec& spec) {
    if (entry.contains("distinct") && entry.at("distinct").is_bool())
        spec.distinct = entry.at("distinct").as_bool();
    if (entry.contains("separator") && entry.at("separator").is_string())
        spec.separator = entry.at("separator").as_string();
    if (entry.contains("percentile") && entry.at("percentile").is_number())
        spec.percentile = entry.at("percentile").as_number();
    // UDAF (SQLOPT-3): resolve a registered aggregate UDF by name (single seam;
    // every aggregate factory calls this right after setting spec.fn). Built-in
    // names are never treated as UDAFs.
    if (!is_builtin_agg_fn(spec.fn)) {
        if (auto e = clink::AggFunctionRegistry::global().lookup(spec.fn)) {
            spec.is_udaf = true;
            spec.udaf = std::make_shared<const clink::AggFunctionRegistry::Entry>(std::move(*e));
        }
    }
}

// Lazily initialise a UDAF accumulator the first time a group/window touches it,
// so finalize/merge over an empty group still see a well-formed init() value.
inline void ensure_udaf_init(AggState& st, const clink::AggFunctionRegistry::Entry& e) {
    if (!st.udaf_initialised) {
        st.udaf_acc = e.init();
        st.udaf_initialised = true;
    }
}

inline bool is_percentile_fn(const std::string& fn) {
    return fn == "percentile" || fn == "approx_percentile";
}

// The sample-variance family (stddev / variance / *_samp) divides by
// n-1; the population family (*_pop) divides by n.
inline bool is_variance_fn(const std::string& fn) {
    return fn == "stddev" || fn == "stddev_pop" || fn == "stddev_samp" || fn == "variance" ||
           fn == "var_pop" || fn == "var_samp";
}

// Phase 24b: subtract one input row's column value from the running
// accumulator. Mirrors update_agg but inverted for SUM / COUNT / AVG.
// MIN and MAX retraction needs a multiset of seen values, which we
// don't track; for those functions retraction is a no-op (the
// running min/max may be stale until the next non-retracting input).
void retract_agg(AggState& st, const AggSpec& spec, const Row& row) {
    if (spec.is_udaf) {
        // A UDAF can only retract if it supplied a retract closure. Receiving a
        // changelog delete without one would silently corrupt the accumulator,
        // so reject clearly (retract_agg is reached only from the changelog
        // GROUP BY; window/OVER ops never call it).
        if (!spec.udaf->has_retract()) {
            throw std::runtime_error(
                "UDAF '" + spec.fn +
                "' received a changelog delete but has no retract closure; register a retract "
                "closure or use it only on append-only / windowed input");
        }
        ensure_udaf_init(st, *spec.udaf);
        auto it = row.values.find(spec.input_column);
        if (it == row.values.end() || it->second.is_null())
            return;
        st.udaf_acc = spec.udaf->retract(std::move(st.udaf_acc), {it->second});
        return;
    }
    // Decrement the multiplicity map and erase a value at zero (used by
    // COUNT(DISTINCT) and STRING_AGG).
    auto retract_value = [&st](const std::string& key) {
        auto vc = st.value_counts.find(key);
        if (vc != st.value_counts.end() && --vc->second <= 0) {
            st.value_counts.erase(vc);
        }
    };
    if (spec.fn == "count") {
        if (spec.input_column.empty()) {
            if (st.running_count > 0)
                --st.running_count;
            return;
        }
        auto it = row.values.find(spec.input_column);
        if (it != row.values.end() && !it->second.is_null()) {
            if (spec.distinct) {
                retract_value(agg_value_key(it->second, spec));
            } else if (st.running_count > 0) {
                --st.running_count;
            }
        }
        return;
    }
    auto it = row.values.find(spec.input_column);
    if (it == row.values.end() || it->second.is_null())
        return;
    const auto& v = it->second;
    if (spec.fn == "string_agg") {
        retract_value(agg_value_key(v, spec));
        return;
    }
    if (is_percentile_fn(spec.fn)) {
        if (v.is_number()) {
            auto& pv = st.percentile_values;
            auto found = std::find(pv.begin(), pv.end(), v.as_number());
            if (found != pv.end())
                pv.erase(found);
        }
        return;
    }
    if (spec.fn == "array_agg") {
        // Remove one occurrence matching the deleted value (serialized
        // equality, so 1 and "1" stay distinct). Insertion order of the
        // survivors is preserved.
        auto& av = st.array_values;
        const std::string key = v.serialize(0);
        auto found = std::find_if(av.begin(), av.end(), [&key](const clink::config::JsonValue& x) {
            return x.serialize(0) == key;
        });
        if (found != av.end())
            av.erase(found);
        return;
    }
    if (spec.fn == "sum" || spec.fn == "avg") {
        if (auto dv = clink::config::as_decimal(v)) {  // #56: mirror the exact decimal sum
            if (st.sum_dec_started) {
                if (auto s = clink::config::dec_sub(st.running_sum_dec, *dv))
                    st.running_sum_dec = *s;
                else
                    st.sum_dec_complete = false;
            }
            st.running_sum -= clink::config::dec_to_double(*dv);
        } else if (v.is_number()) {
            st.running_sum -= v.as_number();
        }
        if (st.running_count > 0)
            --st.running_count;
        return;
    }
    if (is_variance_fn(spec.fn)) {
        if (auto d = agg_numeric_double(v)) {
            st.running_sum -= *d;
            st.running_sum_sq -= *d * *d;
        }
        if (st.running_count > 0)
            --st.running_count;
        return;
    }
    if (spec.fn == "min" || spec.fn == "max") {
        // Retractable MIN / MAX: drop one occurrence of the deleted value
        // from the multiset; finalize then reads the new extreme from the
        // remaining live values. (On the non-retractable path retract_agg
        // is never called - only the changelog GROUP BY retracts, and it
        // sets retractable.)
        if (spec.retractable) {
            auto mc = st.minmax_counts.find(v);
            if (mc != st.minmax_counts.end() && --mc->second <= 0) {
                st.minmax_counts.erase(mc);
            }
        }
        return;
    }
}

// Update accumulator state with one input row's column value.
void update_agg(AggState& st, const AggSpec& spec, const Row& row) {
    if (spec.is_udaf) {
        ensure_udaf_init(st, *spec.udaf);
        auto it = row.values.find(spec.input_column);
        if (it == row.values.end() || it->second.is_null())
            return;  // NULL-skip, matching the built-in convention
        st.udaf_acc = spec.udaf->accumulate(std::move(st.udaf_acc), {it->second});
        return;
    }
    if (spec.fn == "count") {
        if (spec.input_column.empty()) {
            ++st.running_count;
            return;
        }
        // COUNT(col): count only when col is non-null. COUNT(DISTINCT
        // col) tracks per-value multiplicity instead.
        auto it = row.values.find(spec.input_column);
        if (it != row.values.end() && !it->second.is_null()) {
            if (spec.distinct) {
                ++st.value_counts[agg_value_key(it->second, spec)];
            } else {
                ++st.running_count;
            }
        }
        return;
    }
    auto it = row.values.find(spec.input_column);
    if (it == row.values.end() || it->second.is_null())
        return;
    const auto& v = it->second;
    if (spec.fn == "string_agg") {
        ++st.value_counts[agg_value_key(v, spec)];
        return;
    }
    if (is_percentile_fn(spec.fn)) {
        if (v.is_number())
            st.percentile_values.push_back(v.as_number());
        return;
    }
    if (spec.fn == "array_agg") {
        // Append in arrival order; DISTINCT dedup happens at finalize so
        // retraction stays a simple erase-one-occurrence.
        st.array_values.push_back(v);
        return;
    }
    if (spec.fn == "sum" || spec.fn == "avg") {
        // #56: accumulate an EXACT decimal sum (covers decimals and integers)
        // alongside the double running_sum used by AVG.
        if (auto dv = clink::config::as_decimal(v)) {
            if (st.sum_dec_started) {
                if (auto s = clink::config::dec_add(st.running_sum_dec, *dv))
                    st.running_sum_dec = *s;
                else
                    st.sum_dec_complete = false;  // overflow -> fall back to double for the result
            } else {
                st.running_sum_dec = *dv;
                st.sum_dec_started = true;
            }
            if (clink::config::is_dec_string(v))
                st.sum_saw_decimal = true;
            st.running_sum += clink::config::dec_to_double(*dv);
        } else if (v.is_number()) {
            st.running_sum += v.as_number();
            st.sum_dec_complete = false;  // a non-integral double -> decimal sum is not exact
        }
        ++st.running_count;
        return;
    }
    if (is_variance_fn(spec.fn)) {
        if (auto d = agg_numeric_double(v)) {
            st.running_sum += *d;
            st.running_sum_sq += *d * *d;
        }
        ++st.running_count;
        return;
    }
    if (spec.fn == "min" || spec.fn == "max") {
        if (spec.retractable) {
            // Retractable path: track every live value so a later delete
            // can recompute the extreme. finalize reads begin()/rbegin().
            ++st.minmax_counts[v];
            return;
        }
        const bool is_min = spec.fn == "min";
        auto& running = is_min ? st.running_min : st.running_max;
        bool replace = !st.initialised;
        if (!replace) {
            // #56: decimal-aware comparison (a lexical compare of dec-strings
            // would order MIN/MAX wrong).
            if (clink::config::is_dec_string(v) || clink::config::is_dec_string(running)) {
                auto dv = clink::config::as_decimal(v);
                auto dr = clink::config::as_decimal(running);
                if (dv && dr) {
                    const int c = clink::config::dec_compare(*dv, *dr);
                    replace = is_min ? c < 0 : c > 0;
                }
            } else if (v.is_number() && running.is_number()) {
                replace = is_min ? v.as_number() < running.as_number()
                                 : v.as_number() > running.as_number();
            } else if (v.is_string() && running.is_string()) {
                replace = is_min ? v.as_string() < running.as_string()
                                 : v.as_string() > running.as_string();
            }
        }
        if (replace) {
            running = v;
            st.initialised = true;
        }
        return;
    }
}

// Finalise a per-(group, window) AggState into a JsonValue for the
// final output Row.
clink::config::JsonValue finalize_agg(const AggState& st, const AggSpec& spec) {
    if (spec.is_udaf) {
        // Empty group/window (never accumulated): result over a fresh init().
        if (!st.udaf_initialised) {
            return spec.udaf->result(spec.udaf->init());
        }
        return spec.udaf->result(st.udaf_acc);
    }
    if (spec.fn == "count") {
        if (spec.distinct) {
            return clink::config::JsonValue{static_cast<std::int64_t>(st.value_counts.size())};
        }
        return clink::config::JsonValue{static_cast<std::int64_t>(st.running_count)};
    }
    if (is_percentile_fn(spec.fn)) {
        if (st.percentile_values.empty())
            return clink::config::JsonValue{nullptr};
        std::vector<double> sorted = st.percentile_values;
        std::sort(sorted.begin(), sorted.end());
        // PERCENTILE_CONT semantics: linear interpolation between the two
        // closest ranks. approx_percentile uses the same exact path here
        // (the "approx" affordance is reserved for a future sketch backend
        // on very large groups; for streaming-correctness it returns the
        // exact value).
        const double idx = spec.percentile * static_cast<double>(sorted.size() - 1);
        const auto lo = static_cast<std::size_t>(std::floor(idx));
        const auto hi = static_cast<std::size_t>(std::ceil(idx));
        const double w = idx - static_cast<double>(lo);
        return clink::config::JsonValue{sorted[lo] + (sorted[hi] - sorted[lo]) * w};
    }
    if (spec.fn == "array_agg") {
        // SQL ARRAY_AGG over zero (non-null) rows is NULL, matching
        // PostgreSQL. DISTINCT dedups on serialized equality, keeping the
        // first occurrence so order stays deterministic.
        if (st.array_values.empty())
            return clink::config::JsonValue{nullptr};
        clink::config::JsonArray arr;
        if (spec.distinct) {
            std::set<std::string> seen;
            for (const auto& val : st.array_values) {
                if (seen.insert(val.serialize(0)).second)
                    arr.push_back(val);
            }
        } else {
            arr.assign(st.array_values.begin(), st.array_values.end());
        }
        return clink::config::JsonValue{std::move(arr)};
    }
    if (spec.fn == "string_agg") {
        if (st.value_counts.empty())
            return clink::config::JsonValue{nullptr};
        std::string out;
        bool first = true;
        for (const auto& [key, count] : st.value_counts) {
            const int reps = spec.distinct ? 1 : count;
            for (int i = 0; i < reps; ++i) {
                if (!first)
                    out += spec.separator;
                out += key;
                first = false;
            }
        }
        return clink::config::JsonValue{out};
    }
    if (spec.fn == "sum") {
        // #56: emit an EXACT decimal sum when a decimal was summed and the
        // exact accumulation stayed complete; otherwise the running double
        // (downstream coercion renders integers cleanly at the sink).
        if (st.sum_saw_decimal && st.sum_dec_complete && st.sum_dec_started)
            return clink::config::make_dec_value(st.running_sum_dec);
        return clink::config::JsonValue{st.running_sum};
    }
    if (spec.fn == "avg") {
        if (st.running_count == 0)
            return clink::config::JsonValue{nullptr};
        return clink::config::JsonValue{st.running_sum / static_cast<double>(st.running_count)};
    }
    if (spec.fn == "min") {
        if (spec.retractable) {
            return st.minmax_counts.empty() ? clink::config::JsonValue{nullptr}
                                            : st.minmax_counts.begin()->first;
        }
        return st.running_min;
    }
    if (spec.fn == "max") {
        if (spec.retractable) {
            return st.minmax_counts.empty() ? clink::config::JsonValue{nullptr}
                                            : st.minmax_counts.rbegin()->first;
        }
        return st.running_max;
    }
    if (is_variance_fn(spec.fn)) {
        const std::int64_t n = st.running_count;
        const bool sample = spec.fn == "stddev" || spec.fn == "stddev_samp" ||
                            spec.fn == "variance" || spec.fn == "var_samp";
        if (n == 0 || (sample && n < 2))
            return clink::config::JsonValue{nullptr};
        const double mean = st.running_sum / static_cast<double>(n);
        // sum of squared deviations = sum_sq - n*mean^2 (guard tiny
        // negative from floating-point cancellation).
        double ss = st.running_sum_sq - static_cast<double>(n) * mean * mean;
        if (ss < 0.0)
            ss = 0.0;
        const double denom = sample ? static_cast<double>(n - 1) : static_cast<double>(n);
        const double var = ss / denom;
        const bool is_stddev =
            spec.fn == "stddev" || spec.fn == "stddev_pop" || spec.fn == "stddev_samp";
        return clink::config::JsonValue{is_stddev ? std::sqrt(var) : var};
    }
    return clink::config::JsonValue{nullptr};
}

// Per-record state for one (group_key, window) bucket.
struct WindowBucket {
    std::vector<AggState> agg_states;
    Row group_values;  // Captured at first record so the emit Row has the group columns.
    // Stored explicitly because the inner state map is keyed by
    // window_end (CUMULATE slices share a start but differ by end).
    std::int64_t window_start = 0;
};

// Tumbling / hopping / cumulate window aggregate over Row records.
// State shape: group_key_string -> window_end_ms -> bucket (keyed by
// end so CUMULATE's shared-start slices coexist).
// TUMBLE:   each event lands in exactly one bucket
// HOP:      each event lands in size/slide buckets simultaneously
// CUMULATE: each event lands in every slice [anchor, anchor + k*step)
//           that still contains it; slices share the size-aligned anchor
// Watermarks fire windows whose end <= watermark.
class WindowRowOp final : public Operator<Row, Row> {
public:
    enum class Kind { Tumble, Hop, Cumulate };

    WindowRowOp(Kind kind,
                std::string time_column,
                std::int64_t size_ms,
                std::int64_t slide_ms,
                std::vector<std::string> group_keys,
                std::vector<AggSpec> aggregates,
                std::vector<std::string> group_key_outputs = {})
        : kind_(kind),
          time_column_(std::move(time_column)),
          size_ms_(size_ms),
          slide_ms_(slide_ms),
          group_keys_(std::move(group_keys)),
          aggregates_(std::move(aggregates)),
          group_key_outputs_(std::move(group_key_outputs)) {
        if (group_key_outputs_.size() != group_keys_.size()) {
            group_key_outputs_ = group_keys_;  // default: emit each key under its raw name
        }
        if (size_ms_ <= 0)
            throw std::runtime_error("window_row: size_ms must be > 0");
        if (kind_ == Kind::Hop && (slide_ms_ <= 0 || slide_ms_ > size_ms_)) {
            throw std::runtime_error("window_row: HOP needs 0 < slide_ms <= size_ms");
        }
        // For CUMULATE, slide_ms_ carries the step.
        if (kind_ == Kind::Cumulate && (slide_ms_ <= 0 || size_ms_ % slide_ms_ != 0)) {
            throw std::runtime_error(
                "window_row: CUMULATE needs step > 0 and size divisible "
                "by step");
        }
    }

    void process(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (element.is_data()) {
            const auto& batch = element.as_data();
            for (const auto& rec : batch) {
                handle_record_(rec.value());
            }
        } else if (element.is_watermark()) {
            auto wm = element.as_watermark();
            if (!wm.is_idle())
                fire_due_(wm.timestamp(), out);
            this->on_watermark(wm, out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    std::string name() const override {
        switch (kind_) {
            case Kind::Hop:
                return "hopping_window_row";
            case Kind::Cumulate:
                return "cumulate_window_row";
            case Kind::Tumble:
                break;
        }
        return "tumbling_window_row";
    }

private:
    void handle_record_(const Row& row) {
        auto it = row.values.find(time_column_);
        if (it == row.values.end() || !it->second.is_number())
            return;
        auto ts = static_cast<std::int64_t>(it->second.as_number());

        // Build group key.
        std::string key;
        for (std::size_t i = 0; i < group_keys_.size(); ++i) {
            if (i > 0)
                key += '\x1f';
            auto vit = row.values.find(group_keys_[i]);
            if (vit != row.values.end() && !vit->second.is_null()) {
                key += vit->second.serialize(0);
            }
        }

        // Compute the (window_start, window_end) slices this event
        // belongs to. The inner state map is keyed by window_end.
        std::vector<std::pair<std::int64_t, std::int64_t>> windows;
        if (kind_ == Kind::Tumble) {
            std::int64_t start = (ts / size_ms_) * size_ms_;
            windows.emplace_back(start, start + size_ms_);
        } else if (kind_ == Kind::Hop) {
            // HOP: windows start at multiples of slide_ms. An event at
            // ts is in windows starting from
            //   first = floor((ts - size + slide) / slide) * slide
            // up to floor(ts / slide) * slide.
            std::int64_t last = (ts / slide_ms_) * slide_ms_;
            std::int64_t first = ((ts - size_ms_ + slide_ms_) / slide_ms_) * slide_ms_;
            if (first < 0)
                first = 0;
            for (std::int64_t s = first; s <= last; s += slide_ms_) {
                if (ts >= s && ts < s + size_ms_)
                    windows.emplace_back(s, s + size_ms_);
            }
        } else {
            // CUMULATE: slices share the size-aligned anchor and grow by
            // step (carried in slide_ms_) up to size. The event is in
            // every slice [anchor, anchor + k*step) whose end exceeds ts,
            // for k = 1 .. size/step.
            std::int64_t anchor = (ts / size_ms_) * size_ms_;
            for (std::int64_t end = anchor + slide_ms_; end <= anchor + size_ms_;
                 end += slide_ms_) {
                if (ts < end)
                    windows.emplace_back(anchor, end);
            }
        }
        auto& by_window = state_[key];
        for (auto [win_start, win_end] : windows) {
            auto wit = by_window.find(win_end);
            if (wit == by_window.end()) {
                WindowBucket b;
                b.window_start = win_start;
                b.agg_states.resize(aggregates_.size());
                for (std::size_t i = 0; i < group_keys_.size(); ++i) {
                    auto v = row.values.find(group_keys_[i]);
                    if (v != row.values.end())
                        b.group_values.values[group_key_outputs_[i]] = v->second;
                }
                wit = by_window.emplace(win_end, std::move(b)).first;
            }
            for (std::size_t i = 0; i < aggregates_.size(); ++i) {
                update_agg(wit->second.agg_states[i], aggregates_[i], row);
            }
        }
    }

    void fire_due_(EventTime wm, Emitter<Row>& out) {
        const auto wm_value = wm.millis();
        Batch<Row> emit_batch;
        for (auto& [k, by_window] : state_) {
            (void)k;
            for (auto it = by_window.begin(); it != by_window.end();) {
                std::int64_t win_end = it->first;
                std::int64_t win_start = it->second.window_start;
                if (win_end > wm_value) {
                    ++it;
                    continue;
                }
                Row out_row = it->second.group_values;
                for (std::size_t i = 0; i < aggregates_.size(); ++i) {
                    out_row.values[aggregates_[i].output_name] =
                        finalize_agg(it->second.agg_states[i], aggregates_[i]);
                }
                out_row.values["window_start"] =
                    clink::config::JsonValue{static_cast<std::int64_t>(win_start)};
                out_row.values["window_end"] =
                    clink::config::JsonValue{static_cast<std::int64_t>(win_end)};
                emit_batch.push(Record<Row>{std::move(out_row)});
                it = by_window.erase(it);
            }
        }
        if (!emit_batch.empty())
            out.emit_data(std::move(emit_batch));
    }

    Kind kind_;
    std::string time_column_;
    std::int64_t size_ms_;
    std::int64_t slide_ms_;
    std::vector<std::string> group_keys_;
    std::vector<AggSpec> aggregates_;
    std::vector<std::string> group_key_outputs_;
    std::unordered_map<std::string, std::map<std::int64_t, WindowBucket>> state_;
};

// Session-window aggregate. Per-group state is an ordered map keyed
// by session start. Each new event either extends an existing session,
// merges multiple sessions whose [start - gap, end + gap] intervals
// the event bridges, or opens a new session. Watermarks fire sessions
// whose end + gap <= watermark (no further event can extend them).
class SessionWindowRowOp final : public Operator<Row, Row> {
public:
    SessionWindowRowOp(std::string time_column,
                       std::int64_t gap_ms,
                       std::vector<std::string> group_keys,
                       std::vector<AggSpec> aggregates,
                       std::vector<std::string> group_key_outputs = {})
        : time_column_(std::move(time_column)),
          gap_ms_(gap_ms),
          group_keys_(std::move(group_keys)),
          aggregates_(std::move(aggregates)),
          group_key_outputs_(std::move(group_key_outputs)) {
        if (group_key_outputs_.size() != group_keys_.size()) {
            group_key_outputs_ = group_keys_;  // default: emit each key under its raw name
        }
        if (gap_ms_ <= 0)
            throw std::runtime_error("session_window_row: gap_ms must be > 0");
    }

    void process(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (element.is_data()) {
            for (const auto& rec : element.as_data())
                handle_record_(rec.value());
        } else if (element.is_watermark()) {
            auto wm = element.as_watermark();
            if (!wm.is_idle())
                fire_due_(wm.timestamp(), out);
            this->on_watermark(wm, out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    std::string name() const override { return "session_window_row"; }

private:
    struct Session {
        std::int64_t start;
        std::int64_t end;  // last event timestamp seen
        std::vector<AggState> agg_states;
        Row group_values;
    };

    static void merge_into(Session& dst, Session& src, const std::vector<AggSpec>& specs) {
        dst.start = std::min(dst.start, src.start);
        dst.end = std::max(dst.end, src.end);
        for (std::size_t i = 0; i < specs.size(); ++i) {
            const auto& spec = specs[i];
            auto& a = dst.agg_states[i];
            const auto& b = src.agg_states[i];
            // UDAF (SQLOPT-3): merge the opaque accumulators. A merge-less UDAF
            // cannot combine two sessions, so reject clearly (silently dropping
            // src's state was the exact prior bug fixed for the built-in
            // families). Skip the built-in field merges below for a UDAF spec.
            if (spec.is_udaf) {
                if (b.udaf_initialised) {
                    ensure_udaf_init(a, *spec.udaf);
                    if (!spec.udaf->has_merge()) {
                        throw std::runtime_error(
                            "UDAF '" + spec.fn +
                            "' used in a SESSION window but has no merge closure; register a merge "
                            "closure or use it only with TUMBLE/HOP/CUMULATE/GROUP BY");
                    }
                    a.udaf_acc = spec.udaf->merge(std::move(a.udaf_acc), b.udaf_acc);
                }
                continue;
            }
            // Generic accumulator merge. Every aggregate reads only its own
            // AggState fields at finalize, so merging all of them when two
            // sessions combine is safe and keeps SUM/AVG/COUNT, the variance
            // family, STRING_AGG, PERCENTILE and ARRAY_AGG correct across a
            // merge (previously only SUM/AVG/COUNT were merged, silently
            // dropping the others' buffered state).
            a.running_sum += b.running_sum;
            a.running_sum_sq += b.running_sum_sq;
            a.running_count += b.running_count;
            // #56: merge the exact decimal sum across the combined sessions.
            if (b.sum_dec_started) {
                if (a.sum_dec_started) {
                    if (auto s = clink::config::dec_add(a.running_sum_dec, b.running_sum_dec))
                        a.running_sum_dec = *s;
                    else
                        a.sum_dec_complete = false;
                } else {
                    a.running_sum_dec = b.running_sum_dec;
                    a.sum_dec_started = true;
                }
            }
            a.sum_saw_decimal = a.sum_saw_decimal || b.sum_saw_decimal;
            a.sum_dec_complete = a.sum_dec_complete && b.sum_dec_complete;
            for (const auto& [k, c] : b.value_counts)
                a.value_counts[k] += c;
            for (const auto& [k, c] : b.minmax_counts)
                a.minmax_counts[k] += c;
            a.percentile_values.insert(
                a.percentile_values.end(), b.percentile_values.begin(), b.percentile_values.end());
            a.array_values.insert(
                a.array_values.end(), b.array_values.begin(), b.array_values.end());
            // Running MIN / MAX hold a single value (the append-only window
            // path); merge by comparison rather than addition.
            if (spec.fn == "min") {
                if (!a.initialised) {
                    a.running_min = b.running_min;
                    a.initialised = b.initialised;
                } else if (b.initialised) {
                    // #56: decimal-aware order BEFORE the lexical string branch.
                    if (clink::config::is_dec_string(a.running_min) ||
                        clink::config::is_dec_string(b.running_min)) {
                        auto da = clink::config::as_decimal(a.running_min);
                        auto db = clink::config::as_decimal(b.running_min);
                        if (da && db && clink::config::dec_compare(*db, *da) < 0)
                            a.running_min = b.running_min;
                    } else if (a.running_min.is_number() && b.running_min.is_number()) {
                        if (b.running_min.as_number() < a.running_min.as_number())
                            a.running_min = b.running_min;
                    } else if (a.running_min.is_string() && b.running_min.is_string()) {
                        if (b.running_min.as_string() < a.running_min.as_string())
                            a.running_min = b.running_min;
                    }
                }
            } else if (spec.fn == "max") {
                if (!a.initialised) {
                    a.running_max = b.running_max;
                    a.initialised = b.initialised;
                } else if (b.initialised) {
                    // #56: decimal-aware order BEFORE the lexical string branch.
                    if (clink::config::is_dec_string(a.running_max) ||
                        clink::config::is_dec_string(b.running_max)) {
                        auto da = clink::config::as_decimal(a.running_max);
                        auto db = clink::config::as_decimal(b.running_max);
                        if (da && db && clink::config::dec_compare(*db, *da) > 0)
                            a.running_max = b.running_max;
                    } else if (a.running_max.is_number() && b.running_max.is_number()) {
                        if (b.running_max.as_number() > a.running_max.as_number())
                            a.running_max = b.running_max;
                    } else if (a.running_max.is_string() && b.running_max.is_string()) {
                        if (b.running_max.as_string() > a.running_max.as_string())
                            a.running_max = b.running_max;
                    }
                }
            }
        }
    }

    void handle_record_(const Row& row) {
        auto tit = row.values.find(time_column_);
        if (tit == row.values.end() || !tit->second.is_number())
            return;
        auto ts = static_cast<std::int64_t>(tit->second.as_number());

        std::string key;
        for (std::size_t i = 0; i < group_keys_.size(); ++i) {
            if (i > 0)
                key += '\x1f';
            auto vit = row.values.find(group_keys_[i]);
            if (vit != row.values.end() && !vit->second.is_null()) {
                key += vit->second.serialize(0);
            }
        }

        auto& by_session = state_[key];

        // Find sessions that the new event extends or merges. Iterate
        // upper-bound back: any session with end + gap >= ts and
        // start - gap <= ts overlaps.
        std::vector<std::int64_t> overlap_starts;
        for (auto it = by_session.begin(); it != by_session.end(); ++it) {
            const auto& s = it->second;
            if (s.end + gap_ms_ < ts)
                continue;  // session already ended
            if (s.start > ts + gap_ms_)
                break;  // future session, no overlap
            overlap_starts.push_back(it->first);
        }

        if (overlap_starts.empty()) {
            // New session.
            Session s;
            s.start = ts;
            s.end = ts;
            s.agg_states.resize(aggregates_.size());
            for (std::size_t i = 0; i < group_keys_.size(); ++i) {
                auto v = row.values.find(group_keys_[i]);
                if (v != row.values.end())
                    s.group_values.values[group_key_outputs_[i]] = v->second;
            }
            for (std::size_t i = 0; i < aggregates_.size(); ++i) {
                update_agg(s.agg_states[i], aggregates_[i], row);
            }
            by_session.emplace(ts, std::move(s));
            return;
        }

        // Merge all overlapping sessions plus the new event into the
        // earliest session. Remove the others from the map.
        Session merged = std::move(by_session[overlap_starts.front()]);
        for (std::size_t i = 1; i < overlap_starts.size(); ++i) {
            auto& s = by_session[overlap_starts[i]];
            merge_into(merged, s, aggregates_);
            by_session.erase(overlap_starts[i]);
        }
        by_session.erase(overlap_starts.front());

        // Update with the new event.
        merged.start = std::min(merged.start, ts);
        merged.end = std::max(merged.end, ts);
        for (std::size_t i = 0; i < aggregates_.size(); ++i) {
            update_agg(merged.agg_states[i], aggregates_[i], row);
        }
        // group_values stay the same (group key unchanged within group).
        std::int64_t new_start = merged.start;
        by_session.emplace(new_start, std::move(merged));
    }

    void fire_due_(EventTime wm, Emitter<Row>& out) {
        const auto wm_value = wm.millis();
        Batch<Row> emit_batch;
        for (auto& [k, by_session] : state_) {
            (void)k;
            for (auto it = by_session.begin(); it != by_session.end();) {
                if (it->second.end + gap_ms_ > wm_value) {
                    ++it;
                    continue;
                }
                Row out_row = it->second.group_values;
                for (std::size_t i = 0; i < aggregates_.size(); ++i) {
                    out_row.values[aggregates_[i].output_name] =
                        finalize_agg(it->second.agg_states[i], aggregates_[i]);
                }
                out_row.values["window_start"] =
                    clink::config::JsonValue{static_cast<std::int64_t>(it->second.start)};
                out_row.values["window_end"] =
                    clink::config::JsonValue{static_cast<std::int64_t>(it->second.end + gap_ms_)};
                emit_batch.push(Record<Row>{std::move(out_row)});
                it = by_session.erase(it);
            }
        }
        if (!emit_batch.empty())
            out.emit_data(std::move(emit_batch));
    }

    std::string time_column_;
    std::int64_t gap_ms_;
    std::vector<std::string> group_keys_;
    std::vector<AggSpec> aggregates_;
    std::vector<std::string> group_key_outputs_;
    std::unordered_map<std::string, std::map<std::int64_t, Session>> state_;
};

// One OVER output column. fn is one of sum/count/avg/min/max (running
// aggregate) or first_value/last_value/lag (navigation).
struct OverSpec {
    std::string output_name;
    std::string fn;
    std::string input_column;     // "" for COUNT(*)
    std::int64_t lag_offset = 1;  // LAG only
    // Window frame (Wave 7): 0 = running (UNBOUNDED PRECEDING ... CURRENT
    // ROW); 1 = ROWS <n> PRECEDING; 2 = RANGE <n> PRECEDING. frame_start is
    // the row count (ROWS) or ms span (RANGE).
    int frame_mode = 0;
    std::int64_t frame_start = 0;
    [[nodiscard]] bool bounded() const { return frame_mode != 0; }
};

// OVER (running) aggregate over Row records. Emits one append-only Row
// per input, carrying the original columns plus the OVER outputs,
// finalised once the watermark passes the row's event time so the
// running frame (UNBOUNDED PRECEDING ... CURRENT ROW) up to and
// including it is complete. No retraction: the result for a row never
// changes after it is emitted.
//
// Per-partition state: a running AggState per aggregate output, the
// first folded row (FIRST_VALUE), a small ring of the most recent
// folded rows (LAG), and a (ts, seq)-ordered buffer of rows not yet
// folded. seq is a monotonic counter giving deterministic order among
// equal timestamps (which would otherwise be nondeterministic).
class OverAggregateRowOp final : public Operator<Row, Row> {
public:
    OverAggregateRowOp(std::string time_column,
                       std::vector<std::string> partition_columns,
                       std::vector<OverSpec> specs)
        : time_column_(std::move(time_column)),
          partition_columns_(std::move(partition_columns)),
          specs_(std::move(specs)) {
        for (const auto& s : specs_) {
            if (s.fn == "lag")
                max_lag_ = std::max(max_lag_, s.lag_offset);
            AggSpec a;
            a.output_name = s.output_name;
            a.fn = s.fn;
            a.input_column = s.input_column;
            agg_specs_.push_back(std::move(a));
            // Bounded frames need a per-partition buffer of folded rows so
            // each row's frame can be recomputed. Track the widest reach so
            // the buffer can be trimmed.
            if (s.frame_mode == 1) {  // ROWS <n> PRECEDING
                needs_buffer_ = true;
                max_rows_back_ = std::max(max_rows_back_, s.frame_start);
            } else if (s.frame_mode == 2) {  // RANGE <n> PRECEDING
                needs_buffer_ = true;
                max_range_back_ = std::max(max_range_back_, s.frame_start);
            }
        }
    }

    void process(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (element.is_data()) {
            for (const auto& rec : element.as_data()) {
                handle_record_(rec.value());
            }
        } else if (element.is_watermark()) {
            auto wm = element.as_watermark();
            if (!wm.is_idle())
                fire_due_(wm.timestamp(), out);
            this->on_watermark(wm, out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    std::string name() const override { return "over_aggregate_row"; }

private:
    struct PartState {
        std::vector<AggState> agg;     // parallel to specs_ (used for aggregate fns)
        std::optional<Row> first_row;  // FIRST_VALUE
        std::deque<Row> recent;        // last max_lag_ folded rows; back = most recent
        std::map<std::pair<std::int64_t, std::uint64_t>, Row> pending;  // (ts, seq) -> not folded
        // Bounded-frame buffer (Wave 7): folded rows in fire (ts, seq) order
        // with their event time. Only maintained when a spec has a ROWS /
        // RANGE frame; trimmed to the widest frame reach.
        std::deque<std::pair<std::int64_t, Row>> folded;
    };

    std::optional<std::int64_t> ts_of_(const Row& row) const {
        auto it = row.values.find(time_column_);
        if (it == row.values.end() || !it->second.is_number())
            return std::nullopt;
        return static_cast<std::int64_t>(it->second.as_number());
    }

    std::string partition_key_(const Row& row) const {
        std::string key;
        for (std::size_t i = 0; i < partition_columns_.size(); ++i) {
            if (i > 0)
                key += '\x1f';
            auto it = row.values.find(partition_columns_[i]);
            if (it != row.values.end() && !it->second.is_null())
                key += it->second.serialize(0);
        }
        return key;
    }

    void handle_record_(const Row& row) {
        auto ts_opt = ts_of_(row);
        if (!ts_opt)
            return;  // rows without a usable event time are dropped
        if (have_wm_ && *ts_opt <= current_wm_)
            return;  // late: the running value at this point already emitted
        auto& st = state_[partition_key_(row)];
        if (st.agg.empty())
            st.agg.resize(specs_.size());
        st.pending.emplace(std::make_pair(*ts_opt, seq_++), row);
    }

    void fire_due_(EventTime wm, Emitter<Row>& out) {
        const auto wm_value = wm.millis();
        have_wm_ = true;
        if (wm_value > current_wm_)
            current_wm_ = wm_value;
        Batch<Row> emit_batch;
        for (auto& [key, st] : state_) {
            (void)key;
            while (!st.pending.empty() && st.pending.begin()->first.first <= wm_value) {
                Row r = std::move(st.pending.begin()->second);
                st.pending.erase(st.pending.begin());
                emit_one_(st, r, emit_batch);
            }
        }
        if (!emit_batch.empty())
            out.emit_data(std::move(emit_batch));
    }

    // Fold row r (the current row, in (ts, seq) order) into the running
    // state and emit its output Row.
    void emit_one_(PartState& st, const Row& r, Batch<Row>& emit_batch) {
        Row out_row = r;
        out_row.values.erase("__key");  // drop the synthetic routing key

        // LAG reads rows folded BEFORE r (the recent ring).
        for (std::size_t i = 0; i < specs_.size(); ++i) {
            const auto& s = specs_[i];
            if (s.fn != "lag")
                continue;
            clink::config::JsonValue v{nullptr};
            if (static_cast<std::int64_t>(st.recent.size()) >= s.lag_offset) {
                const Row& prev =
                    st.recent[st.recent.size() - static_cast<std::size_t>(s.lag_offset)];
                auto it = prev.values.find(s.input_column);
                if (it != prev.values.end())
                    v = it->second;
            }
            out_row.values[s.output_name] = std::move(v);
        }

        // Append r to the bounded-frame buffer (fire order) and trim it to
        // the widest frame reach.
        if (needs_buffer_) {
            const std::int64_t ts = ts_of_(r).value_or(0);
            st.folded.emplace_back(ts, r);
            while (st.folded.size() > static_cast<std::size_t>(max_rows_back_) + 1 &&
                   ts - st.folded.front().first > max_range_back_) {
                st.folded.pop_front();
            }
        }

        // Fold r into the running aggregates (running frame only) and
        // capture the first row.
        for (std::size_t i = 0; i < specs_.size(); ++i) {
            if (specs_[i].frame_mode == 0 && is_running_agg_(specs_[i].fn))
                update_agg(st.agg[i], agg_specs_[i], r);
        }
        if (!st.first_row.has_value())
            st.first_row = r;

        // Aggregate / first_value / last_value read state including r.
        for (std::size_t i = 0; i < specs_.size(); ++i) {
            const auto& s = specs_[i];
            if (is_running_agg_(s.fn)) {
                out_row.values[s.output_name] = s.frame_mode == 0
                                                    ? finalize_agg(st.agg[i], agg_specs_[i])
                                                    : bounded_frame_agg_(st, s, agg_specs_[i]);
            } else if (s.fn == "first_value") {
                clink::config::JsonValue v{nullptr};
                if (st.first_row.has_value()) {
                    auto it = st.first_row->values.find(s.input_column);
                    if (it != st.first_row->values.end())
                        v = it->second;
                }
                out_row.values[s.output_name] = std::move(v);
            } else if (s.fn == "last_value") {
                clink::config::JsonValue v{nullptr};
                auto it = r.values.find(s.input_column);
                if (it != r.values.end())
                    v = it->second;
                out_row.values[s.output_name] = std::move(v);
            }
        }
        emit_batch.push(Record<Row>{std::move(out_row)});

        if (max_lag_ > 0) {
            st.recent.push_back(r);
            while (static_cast<std::int64_t>(st.recent.size()) > max_lag_)
                st.recent.pop_front();
        }
    }

    // Recompute an aggregate over a bounded frame from the folded buffer.
    // ROWS <n> PRECEDING = the last (n+1) folded rows including the current;
    // RANGE <n> PRECEDING = folded rows with ts in [cur_ts - n, cur_ts]. The
    // buffer is in fire (ts, seq) order; same-timestamp peers follow that
    // deterministic order (consistent with the running frame), not strict
    // SQL RANGE-peer equivalence.
    clink::config::JsonValue bounded_frame_agg_(const PartState& st,
                                                const OverSpec& s,
                                                const AggSpec& aspec) const {
        const auto& folded = st.folded;
        if (folded.empty())
            return clink::config::JsonValue{nullptr};
        std::size_t start = 0;
        if (s.frame_mode == 1) {  // ROWS
            const std::size_t span = static_cast<std::size_t>(s.frame_start) + 1;
            start = folded.size() > span ? folded.size() - span : 0;
        } else {  // RANGE
            const std::int64_t lo = folded.back().first - s.frame_start;
            while (start < folded.size() && folded[start].first < lo)
                ++start;
        }
        AggState acc;
        for (std::size_t k = start; k < folded.size(); ++k)
            update_agg(acc, aspec, folded[k].second);
        return finalize_agg(acc, aspec);
    }

    static bool is_running_agg_(const std::string& fn) {
        return fn == "sum" || fn == "count" || fn == "avg" || fn == "min" || fn == "max" ||
               fn == "array_agg";
    }

    std::string time_column_;
    std::vector<std::string> partition_columns_;
    std::vector<OverSpec> specs_;
    std::vector<AggSpec> agg_specs_;  // parallel to specs_; used for aggregate fns
    std::int64_t max_lag_ = 0;
    std::uint64_t seq_ = 0;
    bool have_wm_ = false;
    std::int64_t current_wm_ = 0;
    // Bounded-frame bookkeeping (Wave 7).
    bool needs_buffer_ = false;
    std::int64_t max_rows_back_ = 0;   // widest ROWS <n> PRECEDING
    std::int64_t max_range_back_ = 0;  // widest RANGE <n> PRECEDING (ms)
    std::unordered_map<std::string, PartState> state_;
};

// Phase 8: unbounded GROUP BY aggregator (no window TVF).
//
// Per-group running state kept in an unordered_map keyed by the
// concatenated group-key values. Each input record updates the
// matching group's aggregate state and emits one Row carrying the
// group columns plus the latest finalised aggregate values
// (upsert-style stream). Watermarks and barriers are forwarded
// unchanged; state never expires.
class AggregateRowOp final : public Operator<Row, Row> {
public:
    AggregateRowOp(std::vector<std::string> group_keys,
                   std::vector<AggSpec> aggregates,
                   std::vector<std::string> group_key_outputs = {})
        : group_keys_(std::move(group_keys)),
          aggregates_(std::move(aggregates)),
          group_key_outputs_(std::move(group_key_outputs)) {
        if (group_key_outputs_.size() != group_keys_.size()) {
            group_key_outputs_ = group_keys_;  // default: emit each key under its raw name
        }
    }

    void process(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (element.is_data()) {
            Batch<Row> emit_batch;
            for (const auto& rec : element.as_data()) {
                if (auto out_row = handle_record_(rec.value())) {
                    emit_batch.push(Record<Row>{std::move(*out_row)});
                }
            }
            if (!emit_batch.empty())
                out.emit_data(std::move(emit_batch));
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    std::string name() const override { return "aggregate_row"; }

private:
    struct Bucket {
        Row group_values;
        std::vector<AggState> agg_states;
    };

    std::optional<Row> handle_record_(const Row& row) {
        std::string key;
        for (std::size_t i = 0; i < group_keys_.size(); ++i) {
            if (i > 0)
                key += '\x1f';
            auto vit = row.values.find(group_keys_[i]);
            if (vit != row.values.end() && !vit->second.is_null()) {
                key += vit->second.serialize(0);
            }
        }
        auto it = state_.find(key);
        if (it == state_.end()) {
            Bucket b;
            b.agg_states.resize(aggregates_.size());
            for (std::size_t i = 0; i < group_keys_.size(); ++i) {
                auto v = row.values.find(group_keys_[i]);
                if (v != row.values.end())
                    b.group_values.values[group_key_outputs_[i]] = v->second;
            }
            it = state_.emplace(key, std::move(b)).first;
        }
        // Phase 24b: retraction-aware aggregation. Input rows tagged
        // delete or update_before subtract from the running state;
        // insert / update_after / untagged add. When the input
        // stream is a changelog (any __row_kind present), tag the
        // output as update_after so downstream upsert sinks treat
        // each emission as the latest snapshot of the group key.
        const bool input_has_kind = has_row_kind(row);
        const auto kind = input_has_kind ? row_kind_of(row) : std::string{};
        const bool retract = input_has_kind && is_delete_like(kind);
        for (std::size_t i = 0; i < aggregates_.size(); ++i) {
            if (retract) {
                retract_agg(it->second.agg_states[i], aggregates_[i], row);
            } else {
                update_agg(it->second.agg_states[i], aggregates_[i], row);
            }
        }
        Row out_row = it->second.group_values;
        for (std::size_t i = 0; i < aggregates_.size(); ++i) {
            out_row.values[aggregates_[i].output_name] =
                finalize_agg(it->second.agg_states[i], aggregates_[i]);
        }
        if (input_has_kind) {
            set_row_kind(out_row, kRowKindUpdateAfter);
        }
        return out_row;
    }

    std::vector<std::string> group_keys_;
    std::vector<AggSpec> aggregates_;
    std::vector<std::string> group_key_outputs_;
    std::unordered_map<std::string, Bucket> state_;
};

enum class EquiJoinKind { Inner, LeftOuter, RightOuter, FullOuter };

// Stream-stream equi-join (Inner / Left / Right / Full outer). Per-side
// state is keyed by the serialised join key. INNER emits matched pairs
// only (plain rows). The outer variants emit a null-padded row for an
// unmatched row on the kept side and RETRACT it (changelog delete) when
// a match later arrives, then emit the paired row. Output columns are
// <alias>_<col> for every column on each side (built from the column
// lists), with the absent side filled with nulls. State is unbounded;
// bound the inputs or add TTL for production.
class EquiJoinRowOp final : public CoOperator<Row, Row, Row> {
public:
    EquiJoinRowOp(std::string left_key_column,
                  std::string right_key_column,
                  std::string left_alias,
                  std::string right_alias,
                  EquiJoinKind kind,
                  std::vector<std::string> left_columns,
                  std::vector<std::string> right_columns)
        : left_key_column_(std::move(left_key_column)),
          right_key_column_(std::move(right_key_column)),
          left_alias_(std::move(left_alias)),
          right_alias_(std::move(right_alias)),
          kind_(kind),
          left_columns_(std::move(left_columns)),
          right_columns_(std::move(right_columns)) {}

    void process_element1(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (!element.is_data())
            return;
        Batch<Row> batch;
        for (const auto& rec : element.as_data())
            handle_(rec.value(), /*is_left=*/true, batch);
        if (!batch.empty())
            out.emit_data(std::move(batch));
    }
    void process_element2(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (!element.is_data())
            return;
        Batch<Row> batch;
        for (const auto& rec : element.as_data())
            handle_(rec.value(), /*is_left=*/false, batch);
        if (!batch.empty())
            out.emit_data(std::move(batch));
    }

    std::string name() const override { return "equi_join_row"; }

private:
    struct Entry {
        Row row;
        bool null_emitted = false;  // an outer null-padded row is currently live for this entry
    };

    static std::optional<std::string> key_of_(const Row& row, const std::string& column) {
        auto it = row.values.find(column);
        if (it == row.values.end() || it->second.is_null())
            return std::nullopt;  // NULL key never matches
        return it->second.serialize(0);
    }

    bool left_keeps_unmatched_() const {
        return kind_ == EquiJoinKind::LeftOuter || kind_ == EquiJoinKind::FullOuter;
    }
    bool right_keeps_unmatched_() const {
        return kind_ == EquiJoinKind::RightOuter || kind_ == EquiJoinKind::FullOuter;
    }

    // Build a joined Row from the column lists; nulls for the absent side.
    Row build_(const Row* left, const Row* right) const {
        Row out;
        auto fill =
            [&](const std::vector<std::string>& cols, const std::string& alias, const Row* src) {
                for (const auto& c : cols) {
                    clink::config::JsonValue v{nullptr};
                    if (src != nullptr) {
                        auto it = src->values.find(c);
                        if (it != src->values.end())
                            v = it->second;
                    }
                    out.values[alias + "_" + c] = std::move(v);
                }
            };
        fill(left_columns_, left_alias_, left);
        fill(right_columns_, right_alias_, right);
        return out;
    }

    void emit_pair_(const Row& left, const Row& right, Batch<Row>& batch) {
        Row r = build_(&left, &right);
        if (kind_ != EquiJoinKind::Inner)
            set_row_kind(r, kRowKindInsert);
        batch.push(Record<Row>{std::move(r)});
    }
    // Emit / retract a null-padded row for `present` on its own side.
    void emit_outer_(const Row& present,
                     bool present_is_left,
                     std::string_view kind,
                     Batch<Row>& batch) {
        Row r = present_is_left ? build_(&present, nullptr) : build_(nullptr, &present);
        set_row_kind(r, kind);
        batch.push(Record<Row>{std::move(r)});
    }

    void handle_(const Row& row, bool is_left, Batch<Row>& batch) {
        const auto& key_col = is_left ? left_key_column_ : right_key_column_;
        auto key = key_of_(row, key_col);
        const bool this_outer = is_left ? left_keeps_unmatched_() : right_keeps_unmatched_();
        const bool other_outer = is_left ? right_keeps_unmatched_() : left_keeps_unmatched_();

        if (!key.has_value()) {
            // NULL join key never matches; emit a null-padded row if this
            // side is kept, and don't store it (it can never join).
            if (this_outer)
                emit_outer_(row, is_left, kRowKindInsert, batch);
            return;
        }
        auto& self = is_left ? left_state_ : right_state_;
        auto& other = is_left ? right_state_ : left_state_;
        self[*key].push_back(Entry{row, false});
        Entry& me = self[*key].back();

        auto oit = other.find(*key);
        if (oit == other.end() || oit->second.empty()) {
            if (this_outer) {
                emit_outer_(row, is_left, kRowKindInsert, batch);
                me.null_emitted = true;
            }
            return;
        }
        for (auto& oe : oit->second) {
            if (other_outer && oe.null_emitted) {
                // The matched other-side row was outer-null-padded; the
                // match means it is no longer unmatched, so retract it.
                emit_outer_(oe.row, /*present_is_left=*/!is_left, kRowKindDelete, batch);
                oe.null_emitted = false;
            }
            if (is_left)
                emit_pair_(row, oe.row, batch);
            else
                emit_pair_(oe.row, row, batch);
        }
    }

    std::string left_key_column_;
    std::string right_key_column_;
    std::string left_alias_;
    std::string right_alias_;
    EquiJoinKind kind_;
    std::vector<std::string> left_columns_;
    std::vector<std::string> right_columns_;
    std::unordered_map<std::string, std::vector<Entry>> left_state_;
    std::unordered_map<std::string, std::vector<Entry>> right_state_;
};

// Inc 4: semi / anti join over Row - the runtime for IN / NOT IN and
// equality-correlated EXISTS / NOT EXISTS. Output is the LEFT row only
// (a filter), tagged via __row_kind. Inputs are append-only: semi emits
// a left row once its key appears on the right; anti emits a left row
// while its key is absent and retracts it (delete) when the key, or any
// NULL, appears on the right. NOT IN follows the SQL standard - a NULL
// anywhere on the right makes every probe UNKNOWN, so no left row
// qualifies.
class SemiAntiJoinRowOp final : public CoOperator<Row, Row, Row> {
public:
    // null_aware distinguishes the two anti flavors (ignored for semi):
    //  - true  = NOT IN: SQL 3VL poison - a NULL on the right makes every
    //            probe UNKNOWN, and a NULL-component probe never qualifies.
    //  - false = NOT EXISTS: plain anti - NULL keys simply don't match, so a
    //            NULL-component probe IS included and a NULL right is ignored.
    SemiAntiJoinRowOp(std::vector<std::string> left_key_columns,
                      std::vector<std::string> right_key_columns,
                      bool anti,
                      bool null_aware)
        : left_key_columns_(std::move(left_key_columns)),
          right_key_columns_(std::move(right_key_columns)),
          anti_(anti),
          null_aware_(null_aware) {}

    void process_element1(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (!element.is_data())
            return;
        Batch<Row> batch;
        for (const auto& rec : element.as_data())
            handle_left_(rec.value(), batch);
        if (!batch.empty())
            out.emit_data(std::move(batch));
    }
    void process_element2(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (!element.is_data())
            return;
        Batch<Row> batch;
        for (const auto& rec : element.as_data())
            handle_right_(rec.value(), batch);
        if (!batch.empty())
            out.emit_data(std::move(batch));
    }
    std::string name() const override { return "semi_join_row"; }

private:
    struct LeftEntry {
        Row row;
        bool emitted = false;
    };

    // #49: per-position key for the null-aware NOT IN path. Each element is
    // the serialized value of one key position, or nullopt when that
    // position is NULL. A NULL position is a WILDCARD in potential_match_
    // (it can equal any value), which is exactly the SQL 3VL row-comparison
    // semantics: (p1..pk) and (t1..tk) "potentially match" iff they agree on
    // every position where BOTH are non-null. The single scalar poison used
    // for single-column NOT IN cannot express this (it records neither the
    // null position nor the tuple's non-null values), so the multi-column
    // path keeps null-bearing tuples and matches them position-wise.
    using MaskedKey = std::vector<std::optional<std::string>>;
    struct NaProbe {
        Row row;
        MaskedKey key;  // has >= 1 nullopt component
        bool emitted = false;
    };

    // Composite key: the serialized tuple of the key columns' values. Any
    // NULL component makes the tuple-equality UNKNOWN (never a definite
    // match), so the whole key is nullopt - a probe with a NULL component
    // never matches (IN) and a right tuple with a NULL component matches
    // nothing (and, for plain anti / NOT EXISTS, is ignored). The null-aware
    // NOT IN path handles a nullopt key via the per-position MaskedKey model
    // below rather than this whole-tuple collapse.
    static std::optional<std::string> key_of_(const Row& row,
                                              const std::vector<std::string>& cols) {
        clink::config::JsonArray arr;
        arr.reserve(cols.size());
        for (const auto& col : cols) {
            auto it = row.values.find(col);
            if (it == row.values.end() || it->second.is_null())
                return std::nullopt;  // NULL component -> UNKNOWN
            arr.push_back(it->second);
        }
        return clink::config::JsonValue{std::move(arr)}.serialize(0);
    }

    static void emit_(const Row& row, std::string_view kind, Batch<Row>& batch) {
        Row r = row;
        r.values.erase("__key");  // drop the synthetic routing key
        set_row_kind(r, kind);
        batch.push(Record<Row>{std::move(r)});
    }

    void handle_left_(const Row& row, Batch<Row>& batch) {
        auto key = key_of_(row, left_key_columns_);
        if (!key.has_value()) {
            // NULL-component probe.
            if (anti_ && !null_aware_) {
                // NOT EXISTS (plain anti): a NULL probe matches nothing, so it
                // qualifies and can never be retracted.
                emit_(row, kRowKindInsert, batch);
                return;
            }
            if (anti_ && null_aware_) {
                // NOT IN (null-aware): per-position 3VL. The probe qualifies
                // iff NO right tuple potentially-matches it. Its NULL positions
                // are wildcards, so any right tuple agreeing on the probe's
                // non-null positions poisons it. Track it for later poisoning.
                MaskedKey pk = masked_key_of_(row, left_key_columns_);
                const bool matched = na_probe_has_right_match_(pk);
                na_null_probes_.push_back(NaProbe{row, pk, !matched});
                if (!matched)
                    emit_(row, kRowKindInsert, batch);
            }
            // semi (IN) with a NULL probe: never matches -> not emitted.
            return;
        }
        auto& entries = left_state_[*key];
        entries.push_back(LeftEntry{row, false});
        LeftEntry& e = entries.back();
        if (!anti_) {
            if (right_count_[*key] > 0) {  // semi: key present -> matched
                e.emitted = true;
                emit_(e.row, kRowKindInsert, batch);
            }
        } else if (right_count_[*key] == 0) {
            // anti: no exact (no-null) right match. For null-aware NOT IN also
            // require that no null-bearing right tuple poisons this probe (a
            // right tuple agreeing on this probe's positions where both are
            // non-null). For plain anti the null-right check is skipped.
            if (!null_aware_ || !na_null_right_matches_(masked_key_of_(row, left_key_columns_))) {
                e.emitted = true;
                emit_(e.row, kRowKindInsert, batch);
            }
        }
    }

    void handle_right_(const Row& row, Batch<Row>& batch) {
        auto key = key_of_(row, right_key_columns_);
        if (!key.has_value()) {
            // NULL-component right tuple. For IN (semi) and plain NOT EXISTS a
            // NULL right contributes no match: ignore it.
            if (!null_aware_)
                return;
            // Null-aware NOT IN: this right tuple poisons exactly the probes
            // that agree with it on its non-null positions (per-position 3VL),
            // not every probe. Record it (deduplicated; multiplicity is
            // irrelevant to the 0-vs-present qualification boundary) and, on a
            // genuinely new distinct tuple, retract the emitted probes it now
            // poisons.
            MaskedKey rk = masked_key_of_(row, right_key_columns_);
            bool is_new = true;
            for (const auto& existing : na_null_rights_) {
                if (existing == rk) {
                    is_new = false;
                    break;
                }
            }
            if (is_new) {
                na_null_rights_.push_back(rk);
                if (anti_)
                    retract_poisoned_(rk, batch);
            }
            return;
        }
        const int before = right_count_[*key];
        right_count_[*key] = before + 1;
        if (before != 0)
            return;  // key already present; no transition (poison already applied)
        // Exact-match transition: emit (semi) / retract (anti) the no-null
        // probes with this exact key.
        auto it = left_state_.find(*key);
        if (it != left_state_.end()) {
            for (auto& e : it->second) {
                if (!anti_) {
                    if (!e.emitted) {  // semi: newly matched
                        e.emitted = true;
                        emit_(e.row, kRowKindInsert, batch);
                    }
                } else if (e.emitted) {  // anti: now matched -> retract
                    e.emitted = false;
                    emit_(e.row, kRowKindDelete, batch);
                }
            }
        }
        // Null-aware NOT IN: a no-null right tuple also poisons null-bearing
        // probes that agree on their non-null positions.
        if (anti_ && null_aware_ && !na_null_probes_.empty()) {
            MaskedKey rk = masked_key_of_(row, right_key_columns_);
            for (auto& p : na_null_probes_) {
                if (p.emitted && potential_match_(p.key, rk)) {
                    p.emitted = false;
                    emit_(p.row, kRowKindDelete, batch);
                }
            }
        }
    }

    // --- #49 per-position NULL-poison helpers (null-aware NOT IN only) ---

    // Build the per-position masked key for a row (nullopt for NULL / missing
    // positions). Unlike key_of_ this never collapses to a single nullopt.
    static MaskedKey masked_key_of_(const Row& row, const std::vector<std::string>& cols) {
        MaskedKey mk;
        mk.reserve(cols.size());
        for (const auto& col : cols) {
            auto it = row.values.find(col);
            if (it == row.values.end() || it->second.is_null())
                mk.push_back(std::nullopt);
            else
                mk.push_back(it->second.serialize(0));
        }
        return mk;
    }

    // Reconstruct a masked key (all positions present) from a right_count_
    // exact key (a serialized JSON array of the no-null component values).
    static MaskedKey masked_key_from_exact_(const std::string& serialized) {
        MaskedKey mk;
        auto v = clink::config::parse(serialized);
        if (v.is_array()) {
            for (const auto& el : v.as_array())
                mk.push_back(el.is_null() ? std::optional<std::string>{}
                                          : std::optional<std::string>{el.serialize(0)});
        }
        return mk;
    }

    // Two tuples potentially-match iff they agree on every position where BOTH
    // are non-null (a NULL position is a wildcard). Equivalent to the SQL
    // row-comparison being not-FALSE (TRUE or UNKNOWN).
    static bool potential_match_(const MaskedKey& a, const MaskedKey& b) {
        const std::size_t n = std::min(a.size(), b.size());
        for (std::size_t i = 0; i < n; ++i) {
            if (a[i].has_value() && b[i].has_value() && *a[i] != *b[i])
                return false;
        }
        return true;
    }

    // Does any recorded null-bearing right tuple potentially-match this probe?
    bool na_null_right_matches_(const MaskedKey& pk) const {
        for (const auto& rk : na_null_rights_) {
            if (potential_match_(pk, rk))
                return true;
        }
        return false;
    }

    // Does any right tuple (exact no-null OR null-bearing) potentially-match
    // this (null-bearing) probe? Used for the probe's initial qualification.
    bool na_probe_has_right_match_(const MaskedKey& pk) const {
        for (const auto& [rkey, cnt] : right_count_) {
            if (cnt > 0 && potential_match_(pk, masked_key_from_exact_(rkey)))
                return true;
        }
        return na_null_right_matches_(pk);
    }

    // A new null-bearing right tuple arrived: retract every currently-emitted
    // probe (no-null in left_state_ and null-bearing in na_null_probes_) it now
    // poisons. The `emitted` guard makes this idempotent (retract at most once).
    void retract_poisoned_(const MaskedKey& rk, Batch<Row>& batch) {
        for (auto& [k, entries] : left_state_) {
            (void)k;
            for (auto& e : entries) {
                if (e.emitted && potential_match_(masked_key_of_(e.row, left_key_columns_), rk)) {
                    e.emitted = false;
                    emit_(e.row, kRowKindDelete, batch);
                }
            }
        }
        for (auto& p : na_null_probes_) {
            if (p.emitted && potential_match_(p.key, rk)) {
                p.emitted = false;
                emit_(p.row, kRowKindDelete, batch);
            }
        }
    }

    std::vector<std::string> left_key_columns_;
    std::vector<std::string> right_key_columns_;
    bool anti_;
    bool null_aware_;
    std::unordered_map<std::string, std::vector<LeftEntry>> left_state_;
    std::unordered_map<std::string, int> right_count_;
    // #49 null-aware NOT IN only: probes with >= 1 NULL key component (never
    // entered left_state_), and the distinct null-bearing right tuples that
    // poison probes position-wise. Empty unless NULLs actually appear, so the
    // no-null exact-hash path above stays the unchanged fast path.
    //
    // Correctness depends on this operator running at parallelism 1 (the
    // OperatorSpec default the SQL planner leaves in place): a probe and a
    // null-bearing right tuple that potentially-match hash to DIFFERENT
    // row_compute_key partitions (their full tuples differ), so only a single
    // instance sees both. The pre-existing global single-column poison had the
    // same parallelism-1 invariant. Raising semi_join_row parallelism would
    // require broadcasting null-bearing right tuples to every instance.
    std::vector<NaProbe> na_null_probes_;
    std::vector<MaskedKey> na_null_rights_;
};

// Set operation INTERSECT / EXCEPT (distinct) over two union-compatible
// Row streams. Unlike a semi/anti join, set ops match on the WHOLE row
// by position (not a single column) and treat NULL = NULL, so the key
// is the canonical serialization of the per-side column values in
// order. Output is distinct (each qualifying row at most once).
//   INTERSECT: emit a row once both sides have produced it (insert
//     only - monotonic over append-only inputs).
//   EXCEPT: emit a left row when the right has not produced it, and
//     RETRACT it (changelog delete) when the right later does. Always
//     emits a left-side row so the output carries the left's columns
//     (set-op output takes its names from the left branch).
class SetOpRowOp final : public CoOperator<Row, Row, Row> {
public:
    SetOpRowOp(std::vector<std::string> left_columns,
               std::vector<std::string> right_columns,
               bool is_except,
               bool all)
        : left_columns_(std::move(left_columns)),
          right_columns_(std::move(right_columns)),
          is_except_(is_except),
          all_(all) {}

    void process_element1(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (!element.is_data())
            return;
        Batch<Row> batch;
        for (const auto& rec : element.as_data())
            handle_(rec.value(), /*left=*/true, batch);
        if (!batch.empty())
            out.emit_data(std::move(batch));
    }
    void process_element2(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (!element.is_data())
            return;
        Batch<Row> batch;
        for (const auto& rec : element.as_data())
            handle_(rec.value(), /*left=*/false, batch);
        if (!batch.empty())
            out.emit_data(std::move(batch));
    }
    std::string name() const override { return "set_op_row"; }

private:
    static std::string key_of_(const Row& row, const std::vector<std::string>& cols) {
        clink::config::JsonArray arr;
        arr.reserve(cols.size());
        for (const auto& c : cols) {
            auto it = row.values.find(c);
            arr.push_back(it != row.values.end() ? it->second : clink::config::JsonValue{});
        }
        return clink::config::JsonValue{std::move(arr)}.serialize(0);
    }
    static void emit_(const Row& row, std::string_view kind, Batch<Row>& batch) {
        Row r = row;
        r.values.erase("__key");  // drop the synthetic routing key
        set_row_kind(r, kind);
        batch.push(Record<Row>{std::move(r)});
    }

    // Both sides feed a per-key multiplicity. INTERSECT wants min(L, R)
    // copies of each key; EXCEPT wants max(L - R, 0); the distinct (set)
    // variants clamp the same formulas to 0/1. After each arrival we
    // recompute the desired multiplicity and emit the changelog delta
    // (inserts when it grows, deletes when it shrinks), so the model is
    // identical for the set and multiset cases. Input row-kind is honored:
    // a delete-tagged input decrements its side (correct for changelog
    // inputs); plain sources are all inserts.
    void handle_(const Row& row, bool left, Batch<Row>& batch) {
        const auto& cols = left ? left_columns_ : right_columns_;
        std::string key = key_of_(row, cols);
        const int delta = is_delete_like(row_kind_of(row)) ? -1 : 1;
        auto& counts = left ? left_count_ : right_count_;
        auto& reps = left ? left_rep_ : right_rep_;
        const int newc = counts[key] + delta;
        if (newc <= 0) {
            counts.erase(key);
            // Keep the representative row: recompute_ still needs it to emit
            // the retraction that the count dropping to zero owes. It is
            // cleaned up there once both sides are empty. (Erasing it here
            // dropped the owed delete -> a phantom row leaked downstream.)
        } else {
            counts[key] = newc;
            if (delta > 0)
                reps.try_emplace(key, row);
        }
        recompute_(key, batch);
    }

    void recompute_(const std::string& key, Batch<Row>& batch) {
        const auto lc = left_count_.find(key);
        const auto rc = right_count_.find(key);
        const int L = lc != left_count_.end() ? lc->second : 0;
        const int R = rc != right_count_.end() ? rc->second : 0;
        // Multiset: INTERSECT wants min(L,R); EXCEPT wants max(L-R,0).
        // Distinct collapses to presence: INTERSECT iff both sides have the
        // key; EXCEPT iff the left has it and the right does NOT (note this
        // is NOT max(L-R,0)>0, which would wrongly keep a key the right also
        // holds when L>R).
        int want;
        if (all_) {
            want = is_except_ ? std::max(L - R, 0) : std::min(L, R);
        } else {
            want = is_except_ ? (L > 0 && R == 0 ? 1 : 0) : (L > 0 && R > 0 ? 1 : 0);
        }
        const auto em = emitted_.find(key);
        int cur = em != emitted_.end() ? em->second : 0;
        if (cur == want)
            return;
        // A representative row exists whenever want > 0 (which requires
        // L > 0). Prefer the left row; fall back to the right.
        const Row* rep = nullptr;
        if (auto lr = left_rep_.find(key); lr != left_rep_.end())
            rep = &lr->second;
        else if (auto rr = right_rep_.find(key); rr != right_rep_.end())
            rep = &rr->second;
        if (rep == nullptr)
            return;  // defensive: nothing to emit without a representative
        while (cur < want) {
            emit_(*rep, kRowKindInsert, batch);
            ++cur;
        }
        while (cur > want) {
            emit_(*rep, kRowKindDelete, batch);
            --cur;
        }
        if (cur == 0)
            emitted_.erase(key);
        else
            emitted_[key] = cur;
        // Once both sides are empty and nothing is owed, the key is gone for
        // good (counts only ever rise from a fresh insert, which re-seeds its
        // own rep). Drop the representatives to bound memory.
        if (L == 0 && R == 0)
            erase_reps_(key);
    }

    void erase_reps_(const std::string& key) {
        left_rep_.erase(key);
        right_rep_.erase(key);
    }

    std::vector<std::string> left_columns_;
    std::vector<std::string> right_columns_;
    bool is_except_;
    bool all_;
    std::unordered_map<std::string, int> left_count_;   // key -> live left multiplicity
    std::unordered_map<std::string, int> right_count_;  // key -> live right multiplicity
    std::unordered_map<std::string, Row> left_rep_;     // key -> representative left row
    std::unordered_map<std::string, Row> right_rep_;    // key -> representative right row
    std::unordered_map<std::string, int> emitted_;      // key -> currently emitted multiplicity
};

// Inc 4: uncorrelated scalar-subquery filter. The scalar (right) side is
// a single-value aggregate; main (left) rows are buffered and, at EOS,
// compared against the settled scalar value (v1 semantics: the scalar is
// a moving aggregate, so we compare against its final value rather than
// retracting as it changes). Output is the matching main rows.
class ScalarBroadcastFilterRowOp final : public CoOperator<Row, Row, Row> {
public:
    ScalarBroadcastFilterRowOp(std::string test_column,
                               std::string comparison_op,
                               std::string scalar_column)
        : test_column_(std::move(test_column)),
          comparison_op_(std::move(comparison_op)),
          scalar_column_(std::move(scalar_column)) {}

    void process_element1(const StreamElement<Row>& element, Emitter<Row>& /*out*/) override {
        if (!element.is_data())
            return;
        for (const auto& rec : element.as_data())
            main_buffer_.push_back(rec.value());
    }
    void process_element2(const StreamElement<Row>& element, Emitter<Row>& /*out*/) override {
        if (!element.is_data())
            return;
        for (const auto& rec : element.as_data()) {
            if (!is_insert_like(row_kind_of(rec.value())))
                continue;
            auto it = rec.value().values.find(scalar_column_);
            if (it != rec.value().values.end() && !it->second.is_null()) {
                scalar_value_ = it->second;
                scalar_set_ = true;
            }
        }
    }
    void flush(Emitter<Row>& out) override {
        if (!scalar_set_)
            return;  // no scalar value -> comparison is UNKNOWN -> no rows
        Batch<Row> batch;
        for (auto& row : main_buffer_) {
            auto it = row.values.find(test_column_);
            if (it == row.values.end() || it->second.is_null())
                continue;  // NULL test value -> UNKNOWN
            if (compare_(it->second, scalar_value_, comparison_op_)) {
                Row r = row;
                r.values.erase("__key");
                set_row_kind(r, kRowKindInsert);
                batch.push(Record<Row>{std::move(r)});
            }
        }
        if (!batch.empty())
            out.emit_data(std::move(batch));
    }
    std::string name() const override { return "scalar_broadcast_filter_row"; }

private:
    static bool compare_(const clink::config::JsonValue& a,
                         const clink::config::JsonValue& b,
                         const std::string& op) {
        int cmp = 0;
        if (a.is_number() && b.is_number()) {
            double ad = a.as_number();
            double bd = b.as_number();
            cmp = (ad < bd) ? -1 : (ad > bd ? 1 : 0);
        } else if (a.is_string() && b.is_string()) {
            cmp = a.as_string().compare(b.as_string());
        } else {
            cmp = a.serialize(0).compare(b.serialize(0));
        }
        if (op == "eq")
            return cmp == 0;
        if (op == "ne")
            return cmp != 0;
        if (op == "lt")
            return cmp < 0;
        if (op == "le")
            return cmp <= 0;
        if (op == "gt")
            return cmp > 0;
        if (op == "ge")
            return cmp >= 0;
        return false;
    }

    std::string test_column_;
    std::string comparison_op_;
    std::string scalar_column_;
    std::vector<Row> main_buffer_;
    clink::config::JsonValue scalar_value_{nullptr};
    bool scalar_set_ = false;
};

// Scalar subquery in the SELECT list: append the broadcast scalar (right
// side, a single-value aggregate) to every main (left) row as a new column.
// Buffers main rows, caches the latest insert-like scalar value (v1: a moving
// aggregate, so its final value is used), and at EOS emits each main row with
// the column appended. Differs from ScalarBroadcastFilterRowOp in two ways:
// (1) it does NOT drop rows when the scalar is unset - an empty/NULL subquery
// yields a NULL appended column, the main rows still flow; (2) it preserves
// each main row's changelog kind rather than forcing insert, so a changelog
// main stream keeps its delete/update semantics.
class ScalarProjectRowOp final : public CoOperator<Row, Row, Row> {
public:
    ScalarProjectRowOp(std::string output_column, std::string scalar_column)
        : output_column_(std::move(output_column)), scalar_column_(std::move(scalar_column)) {}

    void process_element1(const StreamElement<Row>& element, Emitter<Row>& /*out*/) override {
        if (!element.is_data())
            return;
        for (const auto& rec : element.as_data())
            main_buffer_.push_back(rec.value());
    }
    void process_element2(const StreamElement<Row>& element, Emitter<Row>& /*out*/) override {
        if (!element.is_data())
            return;
        for (const auto& rec : element.as_data()) {
            if (!is_insert_like(row_kind_of(rec.value())))
                continue;  // append-only: track the latest insert-like value
            auto it = rec.value().values.find(scalar_column_);
            scalar_value_ =
                (it != rec.value().values.end()) ? it->second : clink::config::JsonValue{nullptr};
        }
    }
    void flush(Emitter<Row>& out) override {
        Batch<Row> batch;
        for (auto& row : main_buffer_) {
            Row r = row;
            r.values.erase("__key");
            r.values[output_column_] = scalar_value_;  // NULL if subquery produced nothing
            batch.push(Record<Row>{std::move(r)});     // keep the main row's __row_kind
        }
        if (!batch.empty())
            out.emit_data(std::move(batch));
    }
    std::string name() const override { return "scalar_project_row"; }

private:
    std::string output_column_;
    std::string scalar_column_;
    std::vector<Row> main_buffer_;
    clink::config::JsonValue scalar_value_{nullptr};
};

// Phase 22b: in-memory upsert sink that maintains the current value
// for each primary key and writes the final state to an NDJSON file
// on EOS (flush()). Atomic via write-tmp-then-rename. State is
// unbounded (one entry per surviving PK); typical use is downstream
// of a bounded-N producer like top_n_per_key_row, which caps the
// live set size by construction.
class FileJsonUpsertSink final : public Sink<Row> {
public:
    FileJsonUpsertSink(std::string path,
                       std::vector<std::string> primary_key,
                       std::map<std::string, int> decimal_scales = {})
        : path_(std::move(path)),
          primary_key_(std::move(primary_key)),
          decimal_scales_(std::move(decimal_scales)) {
        if (primary_key_.empty()) {
            throw std::runtime_error("file_json_upsert_sink: 'primary_key' is required");
        }
    }

    void on_data(const Batch<Row>& batch) override {
        for (const auto& rec : batch) {
            const auto& row = rec.value();
            auto key = make_key_(row);
            const auto kind = row_kind_of(row);
            // Phase 24a: delete erases; update_before is dropped on
            // the floor because the matching update_after will
            // overwrite by PK on the same logical change. insert and
            // update_after both materialise the row.
            if (kind == kRowKindDelete) {
                state_.erase(key);
                continue;
            }
            if (kind == kRowKindUpdateBefore) {
                continue;
            }
            // Strip the __row_kind marker before storing so the final
            // file contains clean rows (the kind isn't part of the
            // logical table).
            Row stored = row;
            stored.values.erase(std::string{kRowKindField});
            state_[key] = std::move(stored);
        }
    }

    void flush() override {
        if (flushed_)
            return;
        flushed_ = true;
        const std::string tmp_path = path_ + ".tmp";
        {
            std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
            if (!out) {
                throw std::runtime_error("file_json_upsert_sink: cannot open " + tmp_path);
            }
            for (const auto& [_k, row] : state_) {
                Row q = row;  // #56: quantise DECIMAL columns to declared scale + clean render
                requantise_row_decimals(q, decimal_scales_);
                clink::config::JsonValue v{clink::config::JsonObject{q.values}};
                auto s = clink::config::serialize_output(v);
                out.write(s.data(), static_cast<std::streamsize>(s.size()));
                out.put('\n');
            }
        }
        std::filesystem::rename(tmp_path, path_);
    }

    std::string name() const override { return "file_json_upsert_sink"; }

private:
    std::string make_key_(const Row& row) const {
        std::string key;
        for (std::size_t i = 0; i < primary_key_.size(); ++i) {
            if (i > 0)
                key += '\x1f';
            auto it = row.values.find(primary_key_[i]);
            if (it != row.values.end() && !it->second.is_null()) {
                key += it->second.serialize(0);
            }
        }
        return key;
    }

    std::string path_;
    std::vector<std::string> primary_key_;
    std::map<std::string, int> decimal_scales_;  // #56: column -> declared scale
    std::unordered_map<std::string, Row> state_;
    bool flushed_{false};
};

// Phase 21c: TOP-N-per-partition. Single-input op that maintains a
// per-partition sorted buffer of at most `count_` records. On each
// arriving record:
//   1. Compute the partition key (concatenated values of the
//      configured partition columns).
//   2. Find the position where the record sorts in the partition's
//      buffer using the configured (column, direction) comparator.
//   3. If the position is outside top-N, drop the record.
//   4. Otherwise insert; if the buffer was already at capacity, the
//      worst-ranked record is evicted - emit a `delete` for it.
//   5. Emit an `insert` for the new record.
// Records carry __row_kind in the emitted Row.values; downstream
// sinks pass it through as-is for now (upsert-aware sinks land in
// a later phase). Watermarks and barriers forward unchanged.
// Ranking semantics for top_n_per_key_row. ROW_NUMBER keeps exactly
// `count` rows per partition (ties broken by arrival order). RANK and
// DENSE_RANK keep every row whose rank is <= count, so tied rows are
// retained together and a partition may hold more than `count` rows.
// RANK leaves a gap after a tie group (rank = 1 + number of strictly-
// better rows); DENSE_RANK leaves no gap (rank = 1 + number of
// strictly-better distinct sort keys).
enum class RankKind { RowNumber, Rank, DenseRank };

class TopNPerKeyRowOp final : public Operator<Row, Row> {
public:
    TopNPerKeyRowOp(std::vector<std::string> partition_columns,
                    std::vector<std::string> sort_columns,
                    std::vector<bool> sort_descending,
                    std::int64_t count,
                    RankKind rank_kind = RankKind::RowNumber)
        : partition_columns_(std::move(partition_columns)),
          sort_columns_(std::move(sort_columns)),
          sort_descending_(std::move(sort_descending)),
          count_(count),
          rank_kind_(rank_kind) {}

    void process(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (element.is_data()) {
            Batch<Row> emit_batch;
            for (const auto& rec : element.as_data()) {
                handle_(rec.value(), emit_batch);
            }
            if (!emit_batch.empty())
                out.emit_data(std::move(emit_batch));
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    std::string name() const override { return "top_n_per_key_row"; }

private:
    std::string partition_key_(const Row& row) const {
        std::string key;
        for (std::size_t i = 0; i < partition_columns_.size(); ++i) {
            if (i > 0)
                key += '\x1f';
            auto it = row.values.find(partition_columns_[i]);
            if (it != row.values.end() && !it->second.is_null()) {
                key += it->second.serialize(0);
            }
        }
        return key;
    }

    // Returns true if `a` should sort before `b` under the configured
    // (column, direction) tuple list.
    bool better_(const Row& a, const Row& b) const {
        for (std::size_t i = 0; i < sort_columns_.size(); ++i) {
            const auto& col = sort_columns_[i];
            const auto ait = a.values.find(col);
            const auto bit = b.values.find(col);
            const auto& av =
                ait != a.values.end() ? ait->second : clink::config::JsonValue{nullptr};
            const auto& bv =
                bit != b.values.end() ? bit->second : clink::config::JsonValue{nullptr};
            int cmp;
            if (av.is_null() && bv.is_null())
                cmp = 0;
            else if (av.is_null())
                cmp = -1;
            else if (bv.is_null())
                cmp = 1;
            else if (av.is_number() && bv.is_number()) {
                double ad = av.as_number(), bd = bv.as_number();
                cmp = (ad < bd) ? -1 : (ad > bd ? 1 : 0);
            } else if (av.is_string() && bv.is_string()) {
                cmp = av.as_string().compare(bv.as_string());
            } else {
                cmp = av.serialize(0).compare(bv.serialize(0));
            }
            if (cmp == 0)
                continue;
            return sort_descending_[i] ? cmp > 0 : cmp < 0;
        }
        return false;
    }

    // Two rows are tied when neither sorts before the other under the
    // configured (column, direction) tuple list - i.e. equal on every
    // sort column. This is the equality predicate RANK / DENSE_RANK
    // need; ROW_NUMBER ignores it and breaks ties by arrival order.
    bool tied_(const Row& a, const Row& b) const { return !better_(a, b) && !better_(b, a); }

    // Given a partition buffer sorted best-first, return the index of
    // the first row whose rank exceeds count_ (the cut point). Rows
    // before the cut stay in the top-N; rows from the cut onward are
    // out. Ranks are monotonic non-decreasing down the buffer, so a
    // single cut point exists.
    std::size_t compute_cut_(const std::vector<Row>& part) const {
        std::size_t group_start = 0;
        std::int64_t group_idx = 0;
        for (std::size_t i = 0; i < part.size(); ++i) {
            if (i > 0 && !tied_(part[i], part[i - 1])) {
                group_start = i;
                ++group_idx;
            }
            std::int64_t rank = 0;
            switch (rank_kind_) {
                case RankKind::RowNumber:
                    rank = static_cast<std::int64_t>(i) + 1;
                    break;
                case RankKind::Rank:
                    rank = static_cast<std::int64_t>(group_start) + 1;
                    break;
                case RankKind::DenseRank:
                    rank = group_idx + 1;
                    break;
            }
            if (rank > count_)
                return i;
        }
        return part.size();
    }

    void handle_(const Row& row, Batch<Row>& emit_batch) {
        if (count_ <= 0)
            return;
        auto key = partition_key_(row);
        auto& part = state_[key];
        // Insertion position = number of strictly-better rows. The new
        // row sorts ahead of any existing rows it ties with; that
        // arrival-order tiebreak is irrelevant for RANK / DENSE_RANK,
        // which treat a tie group as a unit.
        std::size_t pos = 0;
        while (pos < part.size() && better_(part[pos], row))
            ++pos;
        part.insert(part.begin() + static_cast<std::ptrdiff_t>(pos), row);
        const std::size_t cut = compute_cut_(part);
        // Rows from the cut onward are no longer in the top-N. Existing
        // rows there were previously emitted as inserts and must be
        // retracted; the just-inserted row (at index pos) is brand new
        // and is simply dropped without a delete. TopN-per-key emits
        // delete+insert (not update_before/update_after) because the
        // displaced and incoming records have different primary keys -
        // the upsert sink keys by PK and would otherwise leave the
        // displaced row in state when it dropped the update_before
        // half. update_before/update_after is reserved for PK-stable
        // changes (CDC sources, retracting aggregates).
        for (std::size_t i = cut; i < part.size(); ++i) {
            if (i == pos)
                continue;
            Row evicted = part[i];
            set_row_kind(evicted, kRowKindDelete);
            emit_batch.push(Record<Row>{std::move(evicted)});
        }
        const bool admitted = pos < cut;
        part.erase(part.begin() + static_cast<std::ptrdiff_t>(cut), part.end());
        // The new row is emitted only when it lands inside the kept
        // prefix; otherwise it was tentatively inserted past the cut
        // and has now been erased.
        if (admitted) {
            Row inserted = row;
            set_row_kind(inserted, kRowKindInsert);
            emit_batch.push(Record<Row>{std::move(inserted)});
        }
    }

    std::vector<std::string> partition_columns_;
    std::vector<std::string> sort_columns_;
    std::vector<bool> sort_descending_;
    std::int64_t count_;
    RankKind rank_kind_;
    std::unordered_map<std::string, std::vector<Row>> state_;
};

// Phase 13: UNION ALL. Single-input identity map; the OperatorSpec
// carries multiple upstreams in `inputs` and the runtime merges
// them into one Row stream via build_typed_input_stage<Row>. We
// don't model this as a CoOperator because that path requires
// In1 != In2 channel types - same-type two-input ops fall out of
// the channel-partitioning logic in PluginRegistry.
//
// Order across upstreams isn't guaranteed; that matches SQL UNION
// ALL semantics where row order across the two branches is
// unspecified.

// Phase 17: ORDER BY + LIMIT n (TOP-N). Holds up to N records in a
// reverse-priority-queue so the worst element by sort key sits at
// the top and is evicted when a better record arrives. Watermarks
// and barriers pass through; the buffered top-n is emitted at the
// final flush hook (end-of-stream).
class TopNRowOp final : public Operator<Row, Row> {
public:
    TopNRowOp(std::vector<std::string> sort_columns,
              std::vector<bool> sort_descending,
              std::int64_t count,
              std::int64_t offset)
        : sort_columns_(std::move(sort_columns)),
          sort_descending_(std::move(sort_descending)),
          count_(count),
          offset_(offset) {}

    void process(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (element.is_data()) {
            for (const auto& rec : element.as_data())
                consider_(rec.value());
        } else if (element.is_watermark()) {
            auto wm = element.as_watermark();
            if (wm.timestamp() == clink::EventTime::max()) {
                flush(out);
            }
            this->on_watermark(wm, out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    void flush(Emitter<Row>& out) override {
        if (flushed_)
            return;
        flushed_ = true;
        std::vector<Row> sorted;
        sorted.reserve(heap_.size());
        for (auto& [_key, row] : heap_)
            sorted.push_back(std::move(row));
        // sort ascending by the same comparator the heap uses so the
        // emitted order matches the SQL ORDER BY direction.
        std::sort(sorted.begin(), sorted.end(), [this](const Row& a, const Row& b) {
            return compare_(a, b);
        });
        // OFFSET: skip the first `offset_` rows. The heap held the
        // best count+offset rows, so after sorting indices
        // [offset_, offset_+count_) are the right ones.
        Batch<Row> batch;
        std::size_t skip = std::min<std::size_t>(static_cast<std::size_t>(offset_), sorted.size());
        for (std::size_t i = skip; i < sorted.size(); ++i)
            batch.push(Record<Row>{std::move(sorted[i])});
        if (!batch.empty())
            out.emit_data(std::move(batch));
        heap_.clear();
    }

    std::string name() const override { return "top_n_row"; }

private:
    // Returns true if `a` should sort before `b` under the configured
    // (column, direction) tuple list.
    bool compare_(const Row& a, const Row& b) const {
        for (std::size_t i = 0; i < sort_columns_.size(); ++i) {
            const auto& col = sort_columns_[i];
            const auto ait = a.values.find(col);
            const auto bit = b.values.find(col);
            const auto& av =
                ait != a.values.end() ? ait->second : clink::config::JsonValue{nullptr};
            const auto& bv =
                bit != b.values.end() ? bit->second : clink::config::JsonValue{nullptr};
            int cmp = compare_values_(av, bv);
            if (cmp == 0)
                continue;
            const bool desc = sort_descending_[i];
            return desc ? cmp > 0 : cmp < 0;
        }
        return false;
    }

    static int compare_values_(const clink::config::JsonValue& a,
                               const clink::config::JsonValue& b) {
        if (a.is_null() && b.is_null())
            return 0;
        if (a.is_null())
            return -1;
        if (b.is_null())
            return 1;
        if (a.is_number() && b.is_number()) {
            if (a.as_number() < b.as_number())
                return -1;
            if (a.as_number() > b.as_number())
                return 1;
            return 0;
        }
        if (a.is_string() && b.is_string()) {
            return a.as_string().compare(b.as_string());
        }
        return a.serialize(0).compare(b.serialize(0));
    }

    void consider_(const Row& row) {
        // Linear scan: heap stays small (count+offset for TOP-N is
        // typically tiny). When at capacity, replace the worst entry
        // only if `row` sorts strictly before it. OFFSET widens the
        // buffer so flush() can drop the first `offset_` rows after
        // sorting and still emit `count_` of them.
        const std::int64_t capacity = count_ + offset_;
        if (capacity <= 0)
            return;
        if (static_cast<std::int64_t>(heap_.size()) < capacity) {
            heap_.emplace_back(0, row);
            return;
        }
        std::size_t worst = 0;
        for (std::size_t i = 1; i < heap_.size(); ++i) {
            if (compare_(heap_[worst].second, heap_[i].second))
                worst = i;
        }
        if (compare_(row, heap_[worst].second)) {
            heap_[worst].second = row;
        }
    }

    std::vector<std::string> sort_columns_;
    std::vector<bool> sort_descending_;
    std::int64_t count_;
    std::int64_t offset_;
    // (unused-int-key, row). Keeps the structure trivially swappable.
    std::vector<std::pair<int, Row>> heap_;
    bool flushed_{false};
};

// Phase 11: LIMIT n. Stateful counter, no per-key state. Forwards
// the first `count` records and drops the rest. Watermarks and
// barriers always pass through so the rest of the graph still sees
// the lifecycle signals.
class LimitRowOp final : public Operator<Row, Row> {
public:
    LimitRowOp(std::int64_t count, std::int64_t offset) : remaining_(count), to_skip_(offset) {}

    void process(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (element.is_data()) {
            if (remaining_ <= 0)
                return;
            Batch<Row> emit_batch;
            for (const auto& rec : element.as_data()) {
                if (remaining_ <= 0)
                    break;
                if (to_skip_ > 0) {
                    --to_skip_;
                    continue;
                }
                emit_batch.push(rec);
                --remaining_;
            }
            if (!emit_batch.empty())
                out.emit_data(std::move(emit_batch));
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    std::string name() const override { return "limit_row"; }

private:
    std::int64_t remaining_;
    std::int64_t to_skip_;
};

// Phase 10: dedupe operator. Maintains an unordered_set keyed by the
// canonical serialization of the input Row's value map. Emits each
// unique row at most once. State grows unbounded; pair with a
// row_compute_key upstream so each subtask sees only the records
// hashing to it.
class DistinctRowOp final : public Operator<Row, Row> {
public:
    void process(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (element.is_data()) {
            Batch<Row> emit_batch;
            for (const auto& rec : element.as_data()) {
                std::string key = render_key_(rec.value());
                if (seen_.insert(std::move(key)).second) {
                    emit_batch.push(rec);
                }
            }
            if (!emit_batch.empty())
                out.emit_data(std::move(emit_batch));
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    std::string name() const override { return "distinct_row"; }

private:
    static std::string render_key_(const Row& row) {
        // Order-stable: row.values is a std::map (sorted by key), so
        // serialize(0) produces a deterministic string per row.
        clink::config::JsonObject obj = row.values;
        return clink::config::JsonValue{std::move(obj)}.serialize(0);
    }

    std::unordered_set<std::string> seen_;
};

// Stream-stream interval join over Row records.
//
// Match condition:
//   left.ts BETWEEN right.ts + lower_offset_ms AND right.ts + upper_offset_ms
// equivalently
//   right.ts in [left.ts - upper, left.ts - lower].
//
// Per-side state is keyed by the string-rendered join key. Records
// are stored in arrival order; the watermark prunes records that can
// no longer match. Emitted Rows carry every column from both sides
// prefixed by the aliases declared in the SELECT (`<alias>_<col>`).
class IntervalJoinRowOp final : public CoOperator<Row, Row, Row> {
public:
    IntervalJoinRowOp(std::string left_key_column,
                      std::string right_key_column,
                      std::string left_ts_column,
                      std::string right_ts_column,
                      std::string left_alias,
                      std::string right_alias,
                      std::int64_t lower_offset_ms,
                      std::int64_t upper_offset_ms,
                      EquiJoinKind kind = EquiJoinKind::Inner,
                      std::vector<std::string> left_columns = {},
                      std::vector<std::string> right_columns = {})
        : left_key_column_(std::move(left_key_column)),
          right_key_column_(std::move(right_key_column)),
          left_ts_column_(std::move(left_ts_column)),
          right_ts_column_(std::move(right_ts_column)),
          left_alias_(std::move(left_alias)),
          right_alias_(std::move(right_alias)),
          lower_offset_ms_(lower_offset_ms),
          upper_offset_ms_(upper_offset_ms),
          kind_(kind),
          left_columns_(std::move(left_columns)),
          right_columns_(std::move(right_columns)) {
        if (lower_offset_ms_ > upper_offset_ms_) {
            throw std::runtime_error(
                "interval_join_row: lower_offset_ms must be <= upper_offset_ms");
        }
    }

    void process_element1(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (!element.is_data())
            return;  // watermark/barrier go through on_watermark/on_barrier
        for (const auto& rec : element.as_data())
            handle_left_(rec.value(), out);
    }

    void process_element2(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (!element.is_data())
            return;
        for (const auto& rec : element.as_data())
            handle_right_(rec.value(), out);
    }

    void on_watermark(Watermark wm, Emitter<Row>& out) override {
        if (!wm.is_idle()) {
            const auto wm_ms = wm.timestamp().millis();
            // OUTER: null-pad unmatched rows as they are evicted, emitting them
            // BEFORE the watermark is forwarded (their event time is <= wm).
            Batch<Row> null_pads;
            // Drop left rows whose left.ts + lower_offset_ms < wm
            // (no future right.ts >= wm can produce a match). Saturating
            // subtraction: the end-of-stream watermark is INT64_MAX and a
            // negative offset would overflow wm_ms - offset (signed-overflow
            // UB); saturating to INT64_MAX prunes everything, which is the
            // intended end-of-stream behavior.
            prune_(left_state_,
                   sat_sub_(wm_ms, lower_offset_ms_),
                   left_keeps_unmatched_(),
                   /*present_is_left=*/true,
                   null_pads);
            // Drop right rows whose right.ts + upper_offset_ms < wm
            // (no future left.ts >= wm can match).
            prune_(right_state_,
                   sat_sub_(wm_ms, upper_offset_ms_),
                   right_keeps_unmatched_(),
                   /*present_is_left=*/false,
                   null_pads);
            if (!null_pads.empty()) {
                out.emit_data(std::move(null_pads));
            }
        }
        CoOperator<Row, Row, Row>::on_watermark(wm, out);
    }

    std::string name() const override { return "interval_join_row"; }

private:
    struct Buffered {
        std::int64_t ts;
        Row row;
        bool matched = false;  // OUTER: was this row ever part of a join match?
    };

    bool left_keeps_unmatched_() const {
        return kind_ == EquiJoinKind::LeftOuter || kind_ == EquiJoinKind::FullOuter;
    }
    bool right_keeps_unmatched_() const {
        return kind_ == EquiJoinKind::RightOuter || kind_ == EquiJoinKind::FullOuter;
    }

    // Null-padded output for an unmatched kept-side row: the present side's
    // columns prefixed by its alias, and the absent side's columns (from the
    // column list) filled with null. Mirrors the equi-join's build_().
    Row build_outer_(const Row& present, bool present_is_left) const {
        Row out;
        const std::string& present_alias = present_is_left ? left_alias_ : right_alias_;
        for (const auto& [k, v] : present.values) {
            out.values[present_alias + "_" + k] = v;
        }
        const auto& absent_cols = present_is_left ? right_columns_ : left_columns_;
        const std::string& absent_alias = present_is_left ? right_alias_ : left_alias_;
        for (const auto& c : absent_cols) {
            out.values[absent_alias + "_" + c] = clink::config::JsonValue{nullptr};
        }
        return out;
    }

    // a - b clamped to the int64 range. Guards the watermark-minus-offset
    // prune thresholds against signed-overflow UB when the end-of-stream
    // watermark (INT64_MAX) meets a negative offset.
    static std::int64_t sat_sub_(std::int64_t a, std::int64_t b) {
        if (b < 0 && a > INT64_MAX + b) {
            return INT64_MAX;
        }
        if (b > 0 && a < INT64_MIN + b) {
            return INT64_MIN;
        }
        return a - b;
    }

    static std::string key_string(const Row& row, const std::string& key_column) {
        auto it = row.values.find(key_column);
        if (it == row.values.end() || it->second.is_null())
            return {};
        return it->second.serialize(0);
    }

    static std::optional<std::int64_t> ts_value(const Row& row, const std::string& ts_column) {
        auto it = row.values.find(ts_column);
        if (it == row.values.end() || !it->second.is_number())
            return std::nullopt;
        return static_cast<std::int64_t>(it->second.as_number());
    }

    Row build_joined_(const Row& l, const Row& r) const {
        Row out;
        for (const auto& [k, v] : l.values)
            out.values[left_alias_ + "_" + k] = v;
        for (const auto& [k, v] : r.values)
            out.values[right_alias_ + "_" + k] = v;
        return out;
    }

    void handle_left_(const Row& l, Emitter<Row>& out) {
        auto ts_opt = ts_value(l, left_ts_column_);
        if (!ts_opt.has_value())
            return;
        auto ts = *ts_opt;
        auto key = key_string(l, left_key_column_);
        // Match right rows whose right.ts in [ts - upper, ts - lower].
        const auto rlow = ts - upper_offset_ms_;
        const auto rhigh = ts - lower_offset_ms_;
        Batch<Row> emit;
        bool any_match = false;
        auto it = right_state_.find(key);
        if (it != right_state_.end()) {
            for (auto& r : it->second) {
                if (r.ts >= rlow && r.ts <= rhigh) {
                    emit.push(Record<Row>{build_joined_(l, r.row)});
                    r.matched = true;  // OUTER: this right row found a partner
                    any_match = true;
                }
            }
        }
        if (!emit.empty())
            out.emit_data(std::move(emit));
        left_state_[key].push_back(Buffered{ts, l, any_match});
    }

    void handle_right_(const Row& r, Emitter<Row>& out) {
        auto ts_opt = ts_value(r, right_ts_column_);
        if (!ts_opt.has_value())
            return;
        auto ts = *ts_opt;
        auto key = key_string(r, right_key_column_);
        // Match left rows whose left.ts in [ts + lower, ts + upper].
        const auto llow = ts + lower_offset_ms_;
        const auto lhigh = ts + upper_offset_ms_;
        Batch<Row> emit;
        bool any_match = false;
        auto it = left_state_.find(key);
        if (it != left_state_.end()) {
            for (auto& l : it->second) {
                if (l.ts >= llow && l.ts <= lhigh) {
                    emit.push(Record<Row>{build_joined_(l.row, r)});
                    l.matched = true;  // OUTER: this left row found a partner
                    any_match = true;
                }
            }
        }
        if (!emit.empty())
            out.emit_data(std::move(emit));
        right_state_[key].push_back(Buffered{ts, r, any_match});
    }

    // Evict rows whose ts < cutoff (no future partner can fall in their window).
    // For an OUTER kept side, a row evicted while still unmatched gets a final
    // null-padded emission into `null_pads` - the window is closed, so the
    // unmatched verdict is final and needs no later retraction.
    void prune_(std::unordered_map<std::string, std::vector<Buffered>>& side,
                std::int64_t cutoff,
                bool emit_unmatched,
                bool present_is_left,
                Batch<Row>& null_pads) {
        for (auto it = side.begin(); it != side.end();) {
            auto& vec = it->second;
            std::size_t w = 0;
            for (std::size_t r = 0; r < vec.size(); ++r) {
                if (vec[r].ts < cutoff) {
                    if (emit_unmatched && !vec[r].matched) {
                        null_pads.push(Record<Row>{build_outer_(vec[r].row, present_is_left)});
                    }
                    continue;  // evicted: do not keep
                }
                if (w != r) {
                    vec[w] = std::move(vec[r]);
                }
                ++w;
            }
            vec.resize(w);
            if (vec.empty())
                it = side.erase(it);
            else
                ++it;
        }
    }

    std::string left_key_column_;
    std::string right_key_column_;
    std::string left_ts_column_;
    std::string right_ts_column_;
    std::string left_alias_;
    std::string right_alias_;
    std::int64_t lower_offset_ms_;
    std::int64_t upper_offset_ms_;
    EquiJoinKind kind_;
    std::vector<std::string> left_columns_;
    std::vector<std::string> right_columns_;
    std::unordered_map<std::string, std::vector<Buffered>> left_state_;
    std::unordered_map<std::string, std::vector<Buffered>> right_state_;
};

}  // namespace

// Hash one JsonValue stably. Used by row_compute_key.
std::int64_t hash_json_value(const clink::config::JsonValue& v) {
    std::string s;
    if (v.is_string()) {
        s = v.as_string();
    } else if (v.is_number()) {
        s = std::to_string(static_cast<std::int64_t>(v.as_number()));
    } else if (v.is_bool()) {
        s = v.as_bool() ? "1" : "0";
    } else if (v.is_null()) {
        return 0;
    } else {
        s = v.serialize(0);
    }
    // FNV-1a 64. Accumulate in unsigned so the multiply/xor wrap
    // (defined behavior); the final bit pattern is reinterpreted as
    // int64 for key routing - identical bits to the previous signed
    // form, just without the signed-overflow UB.
    std::uint64_t h = 0xcbf29ce484222325ULL;
    for (char c : s) {
        h ^= static_cast<unsigned char>(c);
        h *= 0x100000001b3ULL;
    }
    return static_cast<std::int64_t>(h);
}

constexpr const char* kRowKeyField = "__key";

void install(clink::plugin::PluginRegistry& reg) {
    // ---- Channel type ----
    reg.register_type<Row>(std::string{kChannelRow}, row_json_codec());

    // Row key extractor for parallelism > 1. Reads the synthetic
    // __key field that row_compute_key wrote, hashes it as int64.
    // The routing layer reduces modulo subtask count, so same-key
    // records land on the same subtask of a downstream keyed op
    // (window aggregate / interval join).
    reg.register_key_extractor<Row>("row_key", [](const Row& r) -> std::int64_t {
        auto it = r.values.find(kRowKeyField);
        if (it == r.values.end())
            return 0;
        if (it->second.is_number())
            return static_cast<std::int64_t>(it->second.as_number());
        return hash_json_value(it->second);
    });

    // ---- Sources ----

    // file_json_source: read NDJSON file, emit one Row per line.
    //   path (required): filesystem path
    //   batch_size (default 256): lines per produce() batch
    reg.register_source<Row>(
        "file_json_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<Row>> {
            auto path = ctx.param_or("path");
            if (path.empty()) {
                throw std::runtime_error("file_json_source: 'path' param is required");
            }
            auto batch_size = static_cast<std::size_t>(ctx.param_int64_or("batch_size", 256));
            // SQLOPT-4: the optimizer's projection pushdown narrows the read to
            // these columns (empty => keep all). Drop unprojected columns at
            // decode so a narrow query carries only what it needs downstream.
            std::vector<std::string> projected;
            {
                const auto csv = ctx.param_or("projected_columns");
                std::size_t pos = 0;
                while (pos <= csv.size()) {
                    auto end = csv.find(',', pos);
                    if (end == std::string::npos)
                        end = csv.size();
                    auto c = csv.substr(pos, end - pos);
                    if (!c.empty())
                        projected.push_back(std::move(c));
                    if (end == csv.size())
                        break;
                    pos = end + 1;
                }
            }
            // #56: DECIMAL columns are tagged exact at ingestion.
            return std::make_shared<FileSource<Row>>(
                path,
                row_json_text_format_projected(
                    parse_decimal_columns(ctx.param_or("decimal_columns")), std::move(projected)),
                batch_size,
                "file_json_source");
        });

    // ---- Sinks ----

    // file_json_sink: write one Row per line as a JSON object.
    //   path (required): filesystem path
    //   append (default "false"): open in append mode if "true"
    reg.register_sink<Row>(
        "file_json_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<Row>> {
            auto path = ctx.param_or("path");
            if (path.empty()) {
                throw std::runtime_error("file_json_sink: 'path' param is required");
            }
            if (ctx.parallelism > 1) {
                path += "." + std::to_string(ctx.subtask_idx);
            }
            const bool append = ctx.param_or("append", "false") == "true";
            return std::make_shared<FileSink<Row>>(
                path,
                row_json_text_format_with_decimals(
                    parse_decimal_columns(ctx.param_or("decimal_columns"))),
                append,
                "file_json_sink");
        });

    // file_2pc_sink_row: two-phase-commit file sink for the Row
    // channel. Wraps the general-purpose FileSink2PC<T> with the
    // row_json text format. Activated by mode='exactly_once' (Phase
    // 23). Records are staged per-checkpoint and atomically renamed
    // into a committed/ subdirectory once the JM confirms the
    // checkpoint is globally durable. Params:
    //   dir (required) - output directory; both staging/ and
    //                    committed/ subdirectories are created here.
    reg.register_sink<Row>(
        "file_2pc_sink_row", [](const BuildContext& ctx) -> std::shared_ptr<Sink<Row>> {
            const auto dir = ctx.param_or("dir", ctx.param_or("path", ""));
            if (dir.empty()) {
                throw std::runtime_error("file_2pc_sink_row: 'dir' or 'path' is required");
            }
            auto sink = std::make_shared<FileSink2PC<Row>>(
                dir,
                row_json_text_format_with_decimals(
                    parse_decimal_columns(ctx.param_or("decimal_columns"))),
                ctx.subtask_idx,
                "file_2pc_sink_row");
            // Phase 30a: declare commit-group membership so the JM can
            // gate this sink's CommitCheckpoint on its group peers.
            if (auto cg = ctx.param_or("commit_group", ""); !cg.empty()) {
                sink->set_commit_group(cg);
            }
            return sink;
        });

    // file_json_upsert_sink: in-memory upsert table flushed to an
    // NDJSON file on EOS. Reads __row_kind from each Row; 'insert'
    // (default) overwrites by primary key, 'delete' erases.
    //   path (required)
    //   primary_key (required, CSV)
    reg.register_sink<Row>(
        "file_json_upsert_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<Row>> {
            auto path = ctx.param_or("path");
            if (path.empty()) {
                throw std::runtime_error("file_json_upsert_sink: 'path' param is required");
            }
            if (ctx.parallelism > 1) {
                path += "." + std::to_string(ctx.subtask_idx);
            }
            auto pk_csv = ctx.param_or("primary_key", "");
            if (pk_csv.empty()) {
                throw std::runtime_error("file_json_upsert_sink: 'primary_key' is required");
            }
            std::vector<std::string> pk;
            std::size_t pos = 0;
            while (pos <= pk_csv.size()) {
                auto end = pk_csv.find(',', pos);
                if (end == std::string::npos)
                    end = pk_csv.size();
                auto k = pk_csv.substr(pos, end - pos);
                auto a = k.find_first_not_of(" \t");
                auto b = k.find_last_not_of(" \t");
                if (a != std::string::npos)
                    pk.push_back(k.substr(a, b - a + 1));
                if (end == pk_csv.size())
                    break;
                pos = end + 1;
            }
            return std::make_shared<FileJsonUpsertSink>(
                std::move(path),
                std::move(pk),
                parse_decimal_columns(ctx.param_or("decimal_columns")));
        });

    // ---- Operators ----

    // assign_timestamps_row: extract the event-time column from each
    // Row and emit watermarks. Params:
    //   column (required): name of the timestamp column. The column
    //                      value must be a JSON integer carrying
    //                      milliseconds-since-epoch (Phase 4.1 scope;
    //                      ISO-string parsing follows in a later phase).
    //   out_of_order_ms (default 0): bounded out-of-orderness lag in
    //                      milliseconds. 0 -> monotonic watermarks.
    //
    // The op is a pure pass-through for record values - the only
    // observable effect downstream is watermark emission.
    reg.register_operator<Row, Row>(
        "assign_timestamps_row",
        [](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            const auto column = ctx.param_or("column", "");
            if (column.empty()) {
                throw std::runtime_error("assign_timestamps_row: 'column' param is required");
            }
            const auto bound_ms = ctx.param_int64_or("out_of_order_ms", 0);
            auto extractor = [column](const Row& r) -> EventTime {
                auto it = r.values.find(column);
                if (it == r.values.end() || it->second.is_null()) {
                    return EventTime::min();
                }
                if (it->second.is_number()) {
                    return EventTime{static_cast<std::int64_t>(it->second.as_number())};
                }
                if (it->second.is_string()) {
                    try {
                        return EventTime{
                            static_cast<std::int64_t>(std::stoll(it->second.as_string()))};
                    } catch (...) {
                        return EventTime::min();
                    }
                }
                return EventTime::min();
            };
            std::unique_ptr<WatermarkStrategy<Row>> strategy;
            if (bound_ms <= 0) {
                strategy = std::make_unique<MonotonicWatermarkStrategy<Row>>();
            } else {
                strategy = std::make_unique<BoundedOutOfOrdernessStrategy<Row>>(
                    std::chrono::milliseconds(bound_ms));
            }
            return std::make_shared<WatermarkAssignerOperator<Row>>(
                std::move(extractor), std::move(strategy), "assign_timestamps_row");
        });

    // json_string_to_row: bridge from the string channel (raw NDJSON
    // payload) to the Row channel. Used by the SQL planner when a
    // Row-typed table is backed by a string-typed connector (e.g.
    // kafka_source_string). Lines that don't parse as JSON objects
    // are dropped silently (matches file_json_source's tolerant
    // decoder behaviour).
    reg.register_operator<std::string, Row>(
        "json_string_to_row",
        [](const BuildContext&) -> std::shared_ptr<Operator<std::string, Row>> {
            auto fmt = std::make_shared<clink::TextFormat<Row>>(row_json_text_format());
            return std::make_shared<MapOperator<std::string, Row>>(
                [fmt](const std::string& line) -> Row {
                    auto decoded = fmt->decode(line);
                    return decoded.value_or(Row{});
                },
                "json_string_to_row");
        });

    // row_to_json_string: bridge from the Row channel back to the
    // string channel for sinks that consume std::string (kafka_sink_
    // string). Encodes each Row as a single-line JSON object.
    reg.register_operator<Row, std::string>(
        "row_to_json_string",
        [](const BuildContext&) -> std::shared_ptr<Operator<Row, std::string>> {
            auto fmt = std::make_shared<clink::TextFormat<Row>>(row_json_text_format());
            return std::make_shared<MapOperator<Row, std::string>>(
                [fmt](const Row& r) -> std::string { return fmt->encode(r); },
                "row_to_json_string");
        });

    // row_compute_key: writes a hash of the named columns into the
    // synthetic __key field that the "row_key" extractor reads. The
    // SQL planner emits this Map immediately upstream of every keyed
    // op (window aggregate, interval join) so hash routing partitions
    // by the SQL key columns at parallelism > 1.
    //
    // Params:
    //   columns - CSV list of source column names to hash (multi-key
    //             grouping concatenates and FNV-hashes each in turn).
    //   column  - single-column shortcut for the common case; equivalent
    //             to columns=<col>.
    reg.register_operator<Row, Row>(
        "row_compute_key", [](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            std::vector<std::string> columns;
            auto csv = ctx.param_or("columns", "");
            if (csv.empty()) {
                csv = ctx.param_or("column", "");
            }
            std::size_t pos = 0;
            while (pos <= csv.size()) {
                auto end = csv.find(',', pos);
                if (end == std::string::npos)
                    end = csv.size();
                auto k = csv.substr(pos, end - pos);
                if (!k.empty())
                    columns.push_back(std::move(k));
                if (end == csv.size())
                    break;
                pos = end + 1;
            }
            if (columns.empty()) {
                throw std::runtime_error(
                    "row_compute_key: 'columns' (or 'column') param is required");
            }
            return std::make_shared<MapOperator<Row, Row>>(
                [columns](const Row& r) -> Row {
                    Row out = r;
                    // FNV-1a fold over the named columns so column order
                    // matters. Accumulate in unsigned (defined wraparound);
                    // a missing column folds in a 0 so a present-vs-missing
                    // mismatch routes to a different slot. Final bits are
                    // reinterpreted as int64 for routing.
                    std::uint64_t h = 0xcbf29ce484222325ULL;
                    for (const auto& col : columns) {
                        auto it = r.values.find(col);
                        std::uint64_t v =
                            it != r.values.end()
                                ? static_cast<std::uint64_t>(hash_json_value(it->second))
                                : 0ULL;
                        h ^= v;
                        h *= 0x100000001b3ULL;
                    }
                    out.values[kRowKeyField] =
                        clink::config::JsonValue{static_cast<std::int64_t>(h)};
                    return out;
                },
                "row_compute_key");
        });

    // identity_row: passthrough. SQL planner emits this for SELECT *
    // when no real projection is needed.
    reg.register_operator<Row, Row>("identity_row",
                                    [](const BuildContext&) -> std::shared_ptr<Operator<Row, Row>> {
                                        return std::make_shared<MapOperator<Row, Row>>(
                                            [](const Row& r) { return r; }, "identity_row");
                                    });

    // Phase 28c (runtime slice): async_lookup_row. Drives a registered
    // AsyncLookupFn against Row input via AsyncLookupOperator<Row, Row>.
    // Params:
    //   function_name (required) - key in AsyncFunctionRegistry::global()
    //   max_in_flight (optional)  - default 64
    //   ordered (optional)        - default "true"
    // SQL-frontend wiring is shipped: the binder lowers an async UDF
    // call to a LogicalAsyncMap (binder.cpp detect_async_lookup) and the
    // physical planner emits this op (physical_plan.cpp). Users may also
    // construct the JobGraphSpec entry directly.
    reg.register_operator<Row, Row>(
        "async_lookup_row", [](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            const auto fn_name = ctx.param_or("function_name", "");
            if (fn_name.empty()) {
                throw std::runtime_error("async_lookup_row: 'function_name' param is required");
            }
            auto fn = AsyncFunctionRegistry::global().lookup(fn_name);
            if (!fn) {
                throw std::runtime_error("async_lookup_row: function '" + fn_name +
                                         "' is not registered in AsyncFunctionRegistry::global()");
            }
            const auto max_in_flight_str = ctx.param_or("max_in_flight", "64");
            const std::size_t max_in_flight =
                static_cast<std::size_t>(std::stoull(max_in_flight_str));
            const auto ordered_str = ctx.param_or("ordered", "true");
            const bool ordered = (ordered_str != "false" && ordered_str != "0");
            return std::make_shared<AsyncLookupOperator<Row, Row>>(
                std::move(fn), max_in_flight, ordered, "async_lookup_row");
        });

    // SQLOPT PTF: process_table_function_row. Resolves a registered keyed
    // Row->Rows KeyedProcessFunction by name and wraps it in the keyed adapter.
    // PARTITION BY columns build the per-key string key (\x1f-joined serialised
    // values, the engine-wide SQL Row key convention); cross-subtask routing is
    // handled by the physical row_compute_key + key_by=row_key (set in the
    // planner). v1 is timerless (no timer-key decoder).
    reg.register_operator<Row, Row>(
        "process_table_function_row",
        [](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            const auto fn_name = ctx.param_or("function_name", "");
            if (fn_name.empty()) {
                throw std::runtime_error(
                    "process_table_function_row: 'function_name' param is required");
            }
            auto entry = PtfRegistry::global().lookup(fn_name);
            if (!entry) {
                throw std::runtime_error("process_table_function_row: function '" + fn_name +
                                         "' is not registered in PtfRegistry::global()");
            }
            auto fn = entry->factory();
            auto split_csv = [](const std::string& csv) {
                std::vector<std::string> out;
                std::size_t start = 0;
                while (start <= csv.size()) {
                    auto comma = csv.find(',', start);
                    if (comma == std::string::npos) {
                        if (start < csv.size()) {
                            out.push_back(csv.substr(start));
                        }
                        break;
                    }
                    out.push_back(csv.substr(start, comma - start));
                    start = comma + 1;
                }
                return out;
            };
            const auto cols = split_csv(ctx.param_or("partition_keys", ""));
            auto key_fn = [cols](const Row& row) -> std::string {
                std::string key;
                for (std::size_t i = 0; i < cols.size(); ++i) {
                    if (i > 0) {
                        key += '\x1f';
                    }
                    auto vit = row.values.find(cols[i]);
                    if (vit != row.values.end() && !vit->second.is_null()) {
                        key += vit->second.serialize(0);
                    }
                }
                return key;  // "" when unpartitioned (single per-subtask key)
            };
            return std::make_shared<detail::KeyedProcessFunctionAdapter<std::string, Row, Row>>(
                std::move(fn),
                std::move(key_fn),
                /*timer_key_fn=*/nullptr,
                "process_table_function_row");
        });

    // async_lookup_join_row: SQL lookup (enrichment) join. The probe
    // stream is enriched per row against a connector='lookup' dim table
    // whose registered Row -> async::Task<Row> function returns the dim
    // columns for the probe's key (an empty/missing column set on a
    // miss). This wraps the registered dim function in a merge step and
    // drives it through AsyncLookupOperator<Row, Row>, so cascading
    // co_await I/O (HTTP pool, io_uring) composes exactly as for
    // async_lookup_row. The wrapper builds the join output row: every
    // probe column aliased <probe_alias>_<col>, then every dim column
    // aliased <dim_alias>_<col> (null when the dim row omits it). INNER
    // vs LEFT is handled upstream by the physical plan (INNER appends an
    // IS NOT NULL filter on the dim key column); this op is the same for
    // both. Params: function_name, probe_alias, dim_alias,
    // probe_columns (csv), dim_columns (csv), max_in_flight, ordered.
    reg.register_operator<Row, Row>(
        "async_lookup_join_row",
        [](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            const auto fn_name = ctx.param_or("function_name", "");
            if (fn_name.empty()) {
                throw std::runtime_error(
                    "async_lookup_join_row: 'function_name' param is required");
            }
            auto dim_fn = AsyncFunctionRegistry::global().lookup(fn_name);
            if (!dim_fn) {
                throw std::runtime_error("async_lookup_join_row: function '" + fn_name +
                                         "' is not registered in AsyncFunctionRegistry::global()");
            }
            auto split_csv = [](const std::string& csv) {
                std::vector<std::string> out;
                std::size_t start = 0;
                while (start <= csv.size()) {
                    auto comma = csv.find(',', start);
                    if (comma == std::string::npos) {
                        if (start < csv.size()) {
                            out.push_back(csv.substr(start));
                        }
                        break;
                    }
                    out.push_back(csv.substr(start, comma - start));
                    start = comma + 1;
                }
                return out;
            };
            const std::string probe_alias = ctx.param_or("probe_alias", "");
            const std::string dim_alias = ctx.param_or("dim_alias", "");
            auto probe_cols = split_csv(ctx.param_or("probe_columns", ""));
            auto dim_cols = split_csv(ctx.param_or("dim_columns", ""));
            const std::size_t max_in_flight =
                static_cast<std::size_t>(std::stoull(ctx.param_or("max_in_flight", "64")));
            const auto ordered_str = ctx.param_or("ordered", "true");
            const bool ordered = (ordered_str != "false" && ordered_str != "0");

            AsyncLookupFn wrapper =
                [dim_fn = std::move(dim_fn), probe_alias, dim_alias, probe_cols, dim_cols](
                    const Row& probe) -> clink::async::Task<Row> {
                Row dim = co_await dim_fn(probe);
                Row out;
                for (const auto& c : probe_cols) {
                    auto it = probe.values.find(c);
                    out.values[probe_alias + "_" + c] =
                        it != probe.values.end() ? it->second : clink::config::JsonValue{};
                }
                for (const auto& c : dim_cols) {
                    auto it = dim.values.find(c);
                    out.values[dim_alias + "_" + c] =
                        it != dim.values.end() ? it->second : clink::config::JsonValue{};
                }
                co_return out;
            };
            return std::make_shared<AsyncLookupOperator<Row, Row>>(
                std::move(wrapper), max_in_flight, ordered, "async_lookup_join_row");
        });

    // filter_row_predicate: WHERE clause on multi-column rows. The
    // 'predicate' param is the JSON predicate format from
    // include/clink/operators/json_predicate.hpp. The column resolver
    // looks up names in the Row's JsonObject and returns string-
    // rendered values (numeric / bool get stringified per Row::get_string).
    reg.register_operator<Row, Row>(
        "filter_row_predicate", [](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            const auto pred_text = ctx.param_or("predicate", "");
            if (pred_text.empty()) {
                throw std::runtime_error("filter_row_predicate: 'predicate' param is required");
            }
            auto pred_json =
                std::make_shared<clink::config::JsonValue>(clink::config::parse(pred_text));
            return std::make_shared<clink::FilterOperator<Row>>(
                [pred_json](const Row& r) -> bool {
                    // Row resolver returns the raw JsonValue from the row (or
                    // JsonValue{nullptr} for missing/null columns) so the
                    // evaluator can do type-aware comparisons.
                    auto resolve = [&](const std::string& name) -> clink::config::JsonValue {
                        auto it = r.values.find(name);
                        if (it == r.values.end())
                            return clink::config::JsonValue{nullptr};
                        return it->second;
                    };
                    return clink::operators::evaluate_json_predicate(*pred_json, resolve);
                },
                "filter_row_predicate");
        });

    // #61 phase 2: match_recognize_row - SQL MATCH_RECOGNIZE lowered onto the
    // CEP engine. Rebuilds a Pattern<Row> from the params (strict-contiguity
    // steps, DEFINE predicates as json_predicate closures, greedy quantifiers)
    // and drives CepOperator<Row,Row>. ONE ROW PER MATCH; the SelectFn emits
    // partition-key columns + FIRST/LAST(var.col) measures. v1: matches over
    // per-key ARRIVAL order, so the input must be ordered by the ORDER BY
    // column (the usual time-ordered stream). Params: partition_keys (CSV),
    // order_column, pattern / defines / measures (JSON arrays).
    reg.register_operator<Row, Row>(
        "match_recognize_row", [](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            const auto pattern_text = ctx.param_or("pattern", "");
            if (pattern_text.empty()) {
                throw std::runtime_error("match_recognize_row: 'pattern' param is required");
            }
            const auto defines_text = ctx.param_or("defines", "[]");
            const auto measures_text = ctx.param_or("measures", "[]");
            const auto partition_keys = ctx.param_or("partition_keys", "");

            auto pattern_json = clink::config::parse(pattern_text);
            auto defines_json = clink::config::parse(defines_text.empty() ? "[]" : defines_text);
            auto measures_json = clink::config::parse(measures_text.empty() ? "[]" : measures_text);

            // var -> DEFINE predicate (json_predicate IR).
            std::unordered_map<std::string, std::shared_ptr<clink::config::JsonValue>> var_pred;
            for (const auto& d : defines_json.as_array()) {
                var_pred[d.at("var").as_string()] =
                    std::make_shared<clink::config::JsonValue>(d.at("predicate"));
            }

            // Build Pattern<Row>: first step = begin, rest = next (strict
            // contiguity); attach each var's predicate + greedy quantifier.
            const auto& steps = pattern_json.as_array();
            if (steps.empty()) {
                throw std::runtime_error("match_recognize_row: empty pattern");
            }
            std::optional<clink::cep::Pattern<Row>> pattern;
            for (std::size_t i = 0; i < steps.size(); ++i) {
                const std::string name = steps[i].at("name").as_string();
                const auto minc = static_cast<std::uint32_t>(steps[i].at("min").as_number());
                const auto maxc = static_cast<std::uint32_t>(steps[i].at("max").as_number());
                if (!pattern.has_value()) {
                    pattern.emplace(clink::cep::Pattern<Row>::begin(name));
                } else {
                    pattern->next(name);
                }
                if (auto it = var_pred.find(name); it != var_pred.end()) {
                    auto pj = it->second;
                    pattern->where([pj](const Row& r) -> bool {
                        auto resolve = [&](const std::string& nm) -> clink::config::JsonValue {
                            auto vit = r.values.find(nm);
                            if (vit == r.values.end()) {
                                return clink::config::JsonValue{nullptr};
                            }
                            return vit->second;
                        };
                        return clink::operators::evaluate_json_predicate(*pj, resolve);
                    });
                }
                if (minc == 0 && maxc == 1) {
                    pattern->optional();
                } else if (minc == 1 && maxc == std::numeric_limits<std::uint32_t>::max()) {
                    pattern->one_or_more();
                } else if (minc == 0 && maxc == std::numeric_limits<std::uint32_t>::max()) {
                    pattern->times(0, std::numeric_limits<std::uint32_t>::max());
                } else if (!(minc == 1 && maxc == 1)) {
                    pattern->times(minc, maxc);
                }
            }

            std::vector<std::string> part_cols;
            for (std::size_t a = 0, b = 0; a <= partition_keys.size(); ++a) {
                if (a == partition_keys.size() || partition_keys[a] == ',') {
                    if (a > b) {
                        part_cols.push_back(partition_keys.substr(b, a - b));
                    }
                    b = a + 1;
                }
            }

            struct MeasureSpec {
                std::string name;
                std::string fn;
                std::string var;
                std::string column;
            };
            std::vector<MeasureSpec> measures;
            for (const auto& m : measures_json.as_array()) {
                measures.push_back(MeasureSpec{m.at("name").as_string(),
                                               m.at("fn").as_string(),
                                               m.at("var").as_string(),
                                               m.at("column").as_string()});
            }

            auto select_fn = [part_cols,
                              measures](const clink::cep::PatternMatch<Row>& match) -> Row {
                Row out;
                // Partition keys are identical across the match; take from any
                // matched event.
                const Row* any = nullptr;
                for (const auto& [step, rows] : match) {
                    if (!rows.empty()) {
                        any = &rows.front();
                        break;
                    }
                }
                if (any != nullptr) {
                    for (const auto& pc : part_cols) {
                        if (auto it = any->values.find(pc); it != any->values.end()) {
                            out.values[pc] = it->second;
                        }
                    }
                }
                for (const auto& ms : measures) {
                    auto vit = match.find(ms.var);
                    if (vit == match.end() || vit->second.empty()) {
                        out.values[ms.name] = clink::config::JsonValue{nullptr};
                        continue;
                    }
                    const Row& src = (ms.fn == "first") ? vit->second.front() : vit->second.back();
                    auto cit = src.values.find(ms.column);
                    out.values[ms.name] =
                        (cit != src.values.end()) ? cit->second : clink::config::JsonValue{nullptr};
                }
                return out;
            };

            auto key_fn = [](const Row& r) -> std::int64_t {
                auto it = r.values.find(kRowKeyField);
                if (it == r.values.end()) {
                    return 0;
                }
                if (it->second.is_number()) {
                    return static_cast<std::int64_t>(it->second.as_number());
                }
                return hash_json_value(it->second);
            };

            return std::make_shared<clink::cep::CepOperator<Row, Row>>(
                std::move(*pattern), row_json_codec(), key_fn, select_fn, "match_recognize_row");
        });

    // tumbling_window_row / hopping_window_row: per-(group, window)
    // aggregation over Row records. Shared factory body; the
    // planner picks the right factory name per window kind.
    // Common params:
    //   time_column (required): event-time column name
    //   size_ms     (required): window size in milliseconds
    //   slide_ms    (HOP only): slide in ms; must satisfy 0 < slide_ms <= size_ms
    //   group_keys             : comma-separated group-by column names
    //                            (empty = global aggregate per window)
    //   aggregates  (required): JSON array of {name, fn, input_column}
    //                            where fn is sum/count/min/max/avg.
    //
    // Emits one Row per fired window per group, carrying group
    // columns, each aggregate output by its declared name, plus
    // synthetic window_start / window_end columns (ms-since-epoch).
    auto build_window_op = [](const BuildContext& ctx,
                              WindowRowOp::Kind kind) -> std::shared_ptr<Operator<Row, Row>> {
        auto time_column = ctx.param_or("time_column", "");
        if (time_column.empty()) {
            throw std::runtime_error("window_row: 'time_column' param is required");
        }
        auto size_ms = ctx.param_int64_or("size_ms", 0);
        if (size_ms <= 0) {
            throw std::runtime_error("window_row: 'size_ms' must be > 0");
        }
        // HOP carries the slide; CUMULATE carries the step in the same
        // WindowRowOp slot.
        auto slide_ms = kind == WindowRowOp::Kind::Cumulate ? ctx.param_int64_or("step_ms", 0)
                                                            : ctx.param_int64_or("slide_ms", 0);

        auto split_csv = [](const std::string& s) {
            std::vector<std::string> out;
            std::size_t pos = 0;
            while (pos <= s.size()) {
                auto end = s.find(',', pos);
                if (end == std::string::npos)
                    end = s.size();
                auto k = s.substr(pos, end - pos);
                if (!k.empty())
                    out.push_back(std::move(k));
                if (end == s.size())
                    break;
                pos = end + 1;
            }
            return out;
        };
        std::vector<std::string> group_keys = split_csv(ctx.param_or("group_keys", ""));
        std::vector<std::string> group_key_outputs =
            split_csv(ctx.param_or("group_key_outputs", ""));

        const auto aggregates_text = ctx.param_or("aggregates", "");
        if (aggregates_text.empty()) {
            throw std::runtime_error("window_row: 'aggregates' param is required");
        }
        auto agg_json = clink::config::parse(aggregates_text);
        if (!agg_json.is_array()) {
            throw std::runtime_error("window_row: 'aggregates' must be a JSON array");
        }
        std::vector<AggSpec> aggregates;
        for (const auto& entry : agg_json.as_array()) {
            if (!entry.is_object()) {
                throw std::runtime_error("window_row: aggregate entry must be an object");
            }
            AggSpec spec;
            spec.output_name = entry.at("name").as_string();
            spec.fn = entry.at("fn").as_string();
            if (entry.contains("input_column") && entry.at("input_column").is_string()) {
                spec.input_column = entry.at("input_column").as_string();
            }
            decode_agg_extras(entry, spec);
            aggregates.push_back(std::move(spec));
        }
        return std::make_shared<WindowRowOp>(kind,
                                             std::move(time_column),
                                             size_ms,
                                             slide_ms,
                                             std::move(group_keys),
                                             std::move(aggregates),
                                             std::move(group_key_outputs));
    };

    reg.register_operator<Row, Row>(
        "tumbling_window_row",
        [build_window_op](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            return build_window_op(ctx, WindowRowOp::Kind::Tumble);
        });
    reg.register_operator<Row, Row>(
        "hopping_window_row",
        [build_window_op](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            return build_window_op(ctx, WindowRowOp::Kind::Hop);
        });
    reg.register_operator<Row, Row>(
        "cumulate_window_row",
        [build_window_op](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            return build_window_op(ctx, WindowRowOp::Kind::Cumulate);
        });

    // over_aggregate_row: running OVER aggregate. Required params:
    //   time_column        - event-time column (must equal the source's
    //                        event_time_column so the frame closes with
    //                        the watermark)
    //   partition_columns  - CSV of PARTITION BY columns (may be empty)
    //   outputs            - JSON array of {name, fn, input_column,
    //                        lag_offset}
    reg.register_operator<Row, Row>(
        "over_aggregate_row", [](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            auto time_column = ctx.param_or("time_column", "");
            if (time_column.empty())
                throw std::runtime_error("over_aggregate_row: 'time_column' param is required");
            auto split_csv = [](const std::string& csv) {
                std::vector<std::string> out;
                std::size_t pos = 0;
                while (pos <= csv.size()) {
                    auto end = csv.find(',', pos);
                    if (end == std::string::npos)
                        end = csv.size();
                    auto k = csv.substr(pos, end - pos);
                    if (!k.empty())
                        out.push_back(std::move(k));
                    if (end == csv.size())
                        break;
                    pos = end + 1;
                }
                return out;
            };
            auto partition_columns = split_csv(ctx.param_or("partition_columns", ""));
            const auto outputs_text = ctx.param_or("outputs", "");
            if (outputs_text.empty())
                throw std::runtime_error("over_aggregate_row: 'outputs' param is required");
            auto outs_json = clink::config::parse(outputs_text);
            if (!outs_json.is_array())
                throw std::runtime_error("over_aggregate_row: 'outputs' must be a JSON array");
            std::vector<OverSpec> specs;
            for (const auto& entry : outs_json.as_array()) {
                if (!entry.is_object())
                    throw std::runtime_error("over_aggregate_row: output entry must be an object");
                OverSpec s;
                s.output_name = entry.at("name").as_string();
                s.fn = entry.at("fn").as_string();
                if (entry.contains("input_column") && entry.at("input_column").is_string())
                    s.input_column = entry.at("input_column").as_string();
                if (entry.contains("lag_offset") && entry.at("lag_offset").is_number())
                    s.lag_offset = static_cast<std::int64_t>(entry.at("lag_offset").as_number());
                if (entry.contains("frame_mode") && entry.at("frame_mode").is_number())
                    s.frame_mode = static_cast<int>(entry.at("frame_mode").as_number());
                if (entry.contains("frame_start") && entry.at("frame_start").is_number())
                    s.frame_start = static_cast<std::int64_t>(entry.at("frame_start").as_number());
                specs.push_back(std::move(s));
            }
            return std::make_shared<OverAggregateRowOp>(
                std::move(time_column), std::move(partition_columns), std::move(specs));
        });

    // interval_join_row: stream-stream interval join. Required params:
    //   left_key_column, right_key_column,
    //   left_ts_column,  right_ts_column,
    //   left_alias,      right_alias,
    //   lower_offset_ms, upper_offset_ms.
    // Output Row carries every column from each side prefixed by the
    // configured alias ("<alias>_<col>").
    reg.register_co_operator<Row, Row, Row>(
        "interval_join_row",
        [](const BuildContext& ctx) -> std::shared_ptr<CoOperator<Row, Row, Row>> {
            auto need = [&](const char* k) {
                auto v = ctx.param_or(k, "");
                if (v.empty()) {
                    throw std::runtime_error(std::string{"interval_join_row: '"} + k +
                                             "' param is required");
                }
                return v;
            };
            auto split_csv = [](const std::string& csv) {
                std::vector<std::string> out;
                std::size_t pos = 0;
                while (pos <= csv.size()) {
                    auto end = csv.find(',', pos);
                    if (end == std::string::npos)
                        end = csv.size();
                    auto k = csv.substr(pos, end - pos);
                    if (!k.empty())
                        out.push_back(std::move(k));
                    if (end == csv.size())
                        break;
                    pos = end + 1;
                }
                return out;
            };
            const auto jt_str = ctx.param_or("join_type", "inner");
            EquiJoinKind kind = EquiJoinKind::Inner;
            if (jt_str == "left_outer")
                kind = EquiJoinKind::LeftOuter;
            else if (jt_str == "right_outer")
                kind = EquiJoinKind::RightOuter;
            else if (jt_str == "full_outer")
                kind = EquiJoinKind::FullOuter;
            else if (jt_str != "inner")
                throw std::runtime_error("interval_join_row: unknown join_type '" + jt_str + "'");
            return std::make_shared<IntervalJoinRowOp>(
                need("left_key_column"),
                need("right_key_column"),
                need("left_ts_column"),
                need("right_ts_column"),
                need("left_alias"),
                need("right_alias"),
                ctx.param_int64_or("lower_offset_ms", 0),
                ctx.param_int64_or("upper_offset_ms", 0),
                kind,
                split_csv(ctx.param_or("left_columns", "")),
                split_csv(ctx.param_or("right_columns", "")));
        });

    // equi_join_row: stream-stream INNER equi-join. Required params:
    //   left_key_column, right_key_column,
    //   left_alias,      right_alias.
    // State is unbounded; pair with TTL state for production use.
    reg.register_co_operator<Row, Row, Row>(
        "equi_join_row", [](const BuildContext& ctx) -> std::shared_ptr<CoOperator<Row, Row, Row>> {
            auto need = [&](const char* k) {
                auto v = ctx.param_or(k, "");
                if (v.empty()) {
                    throw std::runtime_error(std::string{"equi_join_row: '"} + k +
                                             "' param is required");
                }
                return v;
            };
            auto split_csv = [](const std::string& csv) {
                std::vector<std::string> out;
                std::size_t pos = 0;
                while (pos <= csv.size()) {
                    auto end = csv.find(',', pos);
                    if (end == std::string::npos)
                        end = csv.size();
                    auto k = csv.substr(pos, end - pos);
                    if (!k.empty())
                        out.push_back(std::move(k));
                    if (end == csv.size())
                        break;
                    pos = end + 1;
                }
                return out;
            };
            const auto jt_str = ctx.param_or("join_type", "inner");
            EquiJoinKind kind = EquiJoinKind::Inner;
            if (jt_str == "left_outer")
                kind = EquiJoinKind::LeftOuter;
            else if (jt_str == "right_outer")
                kind = EquiJoinKind::RightOuter;
            else if (jt_str == "full_outer")
                kind = EquiJoinKind::FullOuter;
            else if (jt_str != "inner")
                throw std::runtime_error("equi_join_row: unknown join_type '" + jt_str + "'");
            return std::make_shared<EquiJoinRowOp>(need("left_key_column"),
                                                   need("right_key_column"),
                                                   need("left_alias"),
                                                   need("right_alias"),
                                                   kind,
                                                   split_csv(ctx.param_or("left_columns", "")),
                                                   split_csv(ctx.param_or("right_columns", "")));
        });

    // semi_join_row: IN / NOT IN / EXISTS lowering. Required params:
    //   left_key_column, right_key_column; anti ("1"/"0", default "0").
    // Emits left rows only, as changelog (insert / delete).
    reg.register_co_operator<Row, Row, Row>(
        "semi_join_row", [](const BuildContext& ctx) -> std::shared_ptr<CoOperator<Row, Row, Row>> {
            auto need = [&](const char* k) {
                auto v = ctx.param_or(k, "");
                if (v.empty()) {
                    throw std::runtime_error(std::string{"semi_join_row: '"} + k +
                                             "' param is required");
                }
                return v;
            };
            const auto anti_str = ctx.param_or("anti", "0");
            const bool anti = anti_str == "1" || anti_str == "true";
            // null_aware = NOT IN (SQL 3VL poison); plain anti = NOT EXISTS.
            // Defaults true so a legacy single-column NOT IN keeps its
            // semantics when the param is absent.
            const auto na_str = ctx.param_or("null_aware", "1");
            const bool null_aware = na_str == "1" || na_str == "true";
            // Key columns are CSV (one entry for single-column IN/EXISTS,
            // several for a composite multi-column IN / multi-equality EXISTS).
            auto split_csv = [](const std::string& csv) {
                std::vector<std::string> out;
                std::size_t start = 0;
                while (start <= csv.size()) {
                    auto comma = csv.find(',', start);
                    if (comma == std::string::npos) {
                        if (start < csv.size())
                            out.push_back(csv.substr(start));
                        break;
                    }
                    out.push_back(csv.substr(start, comma - start));
                    start = comma + 1;
                }
                return out;
            };
            return std::make_shared<SemiAntiJoinRowOp>(split_csv(need("left_key_column")),
                                                       split_csv(need("right_key_column")),
                                                       anti,
                                                       null_aware);
        });

    // set_op_row: INTERSECT / EXCEPT (distinct). Params:
    //   mode ("intersect" | "except"), left_columns (CSV),
    //   right_columns (CSV). Rows match by position over those columns
    //   (NULL = NULL); INTERSECT emits inserts only, EXCEPT is changelog.
    reg.register_co_operator<Row, Row, Row>(
        "set_op_row", [](const BuildContext& ctx) -> std::shared_ptr<CoOperator<Row, Row, Row>> {
            auto split_csv = [](const std::string& csv) {
                std::vector<std::string> out;
                std::size_t start = 0;
                while (start <= csv.size()) {
                    auto comma = csv.find(',', start);
                    if (comma == std::string::npos) {
                        if (start < csv.size())
                            out.push_back(csv.substr(start));
                        break;
                    }
                    out.push_back(csv.substr(start, comma - start));
                    start = comma + 1;
                }
                return out;
            };
            const auto mode = ctx.param_or("mode", "intersect");
            if (mode != "intersect" && mode != "except") {
                throw std::runtime_error("set_op_row: 'mode' must be 'intersect' or 'except'");
            }
            const bool all = ctx.param_or("all", "false") == "true";
            return std::make_shared<SetOpRowOp>(split_csv(ctx.param_or("left_columns", "")),
                                                split_csv(ctx.param_or("right_columns", "")),
                                                mode == "except",
                                                all);
        });

    // scalar_broadcast_filter_row: uncorrelated scalar-subquery filter.
    // Required params: test_column, comparison_op (eq/ne/lt/le/gt/ge),
    // scalar_column. Main rows compared against the settled scalar value.
    reg.register_co_operator<Row, Row, Row>(
        "scalar_broadcast_filter_row",
        [](const BuildContext& ctx) -> std::shared_ptr<CoOperator<Row, Row, Row>> {
            auto need = [&](const char* k) {
                auto v = ctx.param_or(k, "");
                if (v.empty()) {
                    throw std::runtime_error(std::string{"scalar_broadcast_filter_row: '"} + k +
                                             "' param is required");
                }
                return v;
            };
            return std::make_shared<ScalarBroadcastFilterRowOp>(
                need("test_column"), need("comparison_op"), need("scalar_column"));
        });

    // scalar_project_row: scalar subquery in the SELECT list. Required params:
    // output_column (appended column name), scalar_column (column the agg
    // subplan emits). Appends the settled scalar to every main row.
    reg.register_co_operator<Row, Row, Row>(
        "scalar_project_row",
        [](const BuildContext& ctx) -> std::shared_ptr<CoOperator<Row, Row, Row>> {
            auto need = [&](const char* k) {
                auto v = ctx.param_or(k, "");
                if (v.empty()) {
                    throw std::runtime_error(std::string{"scalar_project_row: '"} + k +
                                             "' param is required");
                }
                return v;
            };
            return std::make_shared<ScalarProjectRowOp>(need("output_column"),
                                                        need("scalar_column"));
        });

    // session_window_row: gap-based dynamic windows. Params:
    //   time_column (required), gap_ms (required), group_keys (CSV),
    //   aggregates (JSON array of {name, fn, input_column}).
    reg.register_operator<Row, Row>(
        "session_window_row", [](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            auto time_column = ctx.param_or("time_column", "");
            if (time_column.empty()) {
                throw std::runtime_error("session_window_row: 'time_column' param is required");
            }
            auto gap_ms = ctx.param_int64_or("gap_ms", 0);
            if (gap_ms <= 0) {
                throw std::runtime_error("session_window_row: 'gap_ms' must be > 0");
            }
            auto split_csv = [](const std::string& s) {
                std::vector<std::string> out;
                std::size_t pos = 0;
                while (pos <= s.size()) {
                    auto end = s.find(',', pos);
                    if (end == std::string::npos)
                        end = s.size();
                    auto k = s.substr(pos, end - pos);
                    if (!k.empty())
                        out.push_back(std::move(k));
                    if (end == s.size())
                        break;
                    pos = end + 1;
                }
                return out;
            };
            std::vector<std::string> group_keys = split_csv(ctx.param_or("group_keys", ""));
            std::vector<std::string> group_key_outputs =
                split_csv(ctx.param_or("group_key_outputs", ""));
            const auto aggregates_text = ctx.param_or("aggregates", "");
            if (aggregates_text.empty()) {
                throw std::runtime_error("session_window_row: 'aggregates' param is required");
            }
            auto agg_json = clink::config::parse(aggregates_text);
            if (!agg_json.is_array()) {
                throw std::runtime_error("session_window_row: 'aggregates' must be JSON array");
            }
            std::vector<AggSpec> aggregates;
            for (const auto& entry : agg_json.as_array()) {
                AggSpec spec;
                spec.output_name = entry.at("name").as_string();
                spec.fn = entry.at("fn").as_string();
                if (entry.contains("input_column") && entry.at("input_column").is_string()) {
                    spec.input_column = entry.at("input_column").as_string();
                }
                decode_agg_extras(entry, spec);
                aggregates.push_back(std::move(spec));
            }
            return std::make_shared<SessionWindowRowOp>(std::move(time_column),
                                                        gap_ms,
                                                        std::move(group_keys),
                                                        std::move(aggregates),
                                                        std::move(group_key_outputs));
        });

    // union_row: identity Map for UNION ALL. The OperatorSpec wires
    // both upstream branches as `inputs`; the runtime merges them
    // into one Row stream.
    reg.register_operator<Row, Row>("union_row",
                                    [](const BuildContext&) -> std::shared_ptr<Operator<Row, Row>> {
                                        return std::make_shared<MapOperator<Row, Row>>(
                                            [](const Row& r) { return r; }, "union_row");
                                    });

    // top_n_per_key_row: ROW_NUMBER() + WHERE rn <= N pattern.
    // Params:
    //   partition_columns (CSV, possibly empty for global TOP-N)
    //   sort_columns (CSV, non-empty)
    //   sort_descending (CSV of 0/1, same arity as sort_columns)
    //   count (int >= 1)
    // Emits changelog Rows tagged via __row_kind (insert / delete).
    reg.register_operator<Row, Row>(
        "top_n_per_key_row", [](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            auto count = ctx.param_int64_or("count", -1);
            if (count < 1) {
                throw std::runtime_error("top_n_per_key_row: 'count' must be >= 1");
            }
            auto split_csv = [](const std::string& csv) {
                std::vector<std::string> out;
                std::size_t pos = 0;
                while (pos <= csv.size()) {
                    auto end = csv.find(',', pos);
                    if (end == std::string::npos)
                        end = csv.size();
                    auto k = csv.substr(pos, end - pos);
                    if (!k.empty())
                        out.push_back(std::move(k));
                    if (end == csv.size())
                        break;
                    pos = end + 1;
                }
                return out;
            };
            auto part = split_csv(ctx.param_or("partition_columns", ""));
            auto cols = split_csv(ctx.param_or("sort_columns", ""));
            auto descs = split_csv(ctx.param_or("sort_descending", ""));
            if (cols.empty() || cols.size() != descs.size()) {
                throw std::runtime_error(
                    "top_n_per_key_row: 'sort_columns' must be non-empty and match "
                    "'sort_descending' arity");
            }
            std::vector<bool> sort_descending;
            sort_descending.reserve(descs.size());
            for (const auto& d : descs)
                sort_descending.push_back(d == "1" || d == "true");
            const auto rank_kind_str = ctx.param_or("rank_kind", "row_number");
            RankKind rank_kind = RankKind::RowNumber;
            if (rank_kind_str == "rank")
                rank_kind = RankKind::Rank;
            else if (rank_kind_str == "dense_rank")
                rank_kind = RankKind::DenseRank;
            else if (rank_kind_str != "row_number")
                throw std::runtime_error("top_n_per_key_row: unknown rank_kind '" + rank_kind_str +
                                         "'");
            return std::make_shared<TopNPerKeyRowOp>(
                std::move(part), std::move(cols), std::move(sort_descending), count, rank_kind);
        });

    // top_n_row: ORDER BY + LIMIT. Params:
    //   count (int >= 0)
    //   sort_columns (CSV) and sort_descending (CSV of 0/1, same arity)
    reg.register_operator<Row, Row>(
        "top_n_row", [](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            auto count = ctx.param_int64_or("count", -1);
            if (count < 0) {
                throw std::runtime_error("top_n_row: 'count' is required and must be >= 0");
            }
            auto split_csv = [](const std::string& csv) {
                std::vector<std::string> out;
                std::size_t pos = 0;
                while (pos <= csv.size()) {
                    auto end = csv.find(',', pos);
                    if (end == std::string::npos)
                        end = csv.size();
                    auto k = csv.substr(pos, end - pos);
                    if (!k.empty())
                        out.push_back(std::move(k));
                    if (end == csv.size())
                        break;
                    pos = end + 1;
                }
                return out;
            };
            auto cols = split_csv(ctx.param_or("sort_columns", ""));
            auto descs = split_csv(ctx.param_or("sort_descending", ""));
            if (cols.empty() || cols.size() != descs.size()) {
                throw std::runtime_error(
                    "top_n_row: 'sort_columns' and 'sort_descending' must be non-empty + "
                    "same arity");
            }
            std::vector<bool> sort_descending;
            sort_descending.reserve(descs.size());
            for (const auto& d : descs)
                sort_descending.push_back(d == "1" || d == "true");
            const auto offset = ctx.param_int64_or("offset", 0);
            if (offset < 0) {
                throw std::runtime_error("top_n_row: 'offset' must be >= 0");
            }
            return std::make_shared<TopNRowOp>(
                std::move(cols), std::move(sort_descending), count, offset);
        });

    // limit_row: forward at most 'count' records, drop the rest.
    // count >= 0; per-subtask semantics at parallelism > 1.
    reg.register_operator<Row, Row>(
        "limit_row", [](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            auto count = ctx.param_int64_or("count", -1);
            if (count < 0) {
                throw std::runtime_error("limit_row: 'count' param is required and must be >= 0");
            }
            const auto offset = ctx.param_int64_or("offset", 0);
            if (offset < 0) {
                throw std::runtime_error("limit_row: 'offset' must be >= 0");
            }
            return std::make_shared<LimitRowOp>(count, offset);
        });

    // distinct_row: dedupe each Row across the run. No params.
    reg.register_operator<Row, Row>("distinct_row",
                                    [](const BuildContext&) -> std::shared_ptr<Operator<Row, Row>> {
                                        return std::make_shared<DistinctRowOp>();
                                    });

    // aggregate_row: unbounded GROUP BY (no window TVF). Params mirror
    // the window factories minus the time/size knobs:
    //   group_keys (CSV, may be empty for global aggregate)
    //   aggregates (JSON array of {name, fn, input_column}; same shape
    //               as window factories).
    // Emits one upsert-style Row per input record: group columns plus
    // the latest finalised aggregate values.
    reg.register_operator<Row, Row>(
        "aggregate_row", [](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            auto split_csv = [](const std::string& csv) {
                std::vector<std::string> out;
                std::size_t pos = 0;
                while (pos <= csv.size()) {
                    auto end = csv.find(',', pos);
                    if (end == std::string::npos)
                        end = csv.size();
                    auto k = csv.substr(pos, end - pos);
                    if (!k.empty())
                        out.push_back(std::move(k));
                    if (end == csv.size())
                        break;
                    pos = end + 1;
                }
                return out;
            };
            std::vector<std::string> group_keys = split_csv(ctx.param_or("group_keys", ""));
            std::vector<std::string> group_key_outputs =
                split_csv(ctx.param_or("group_key_outputs", ""));
            const auto aggregates_text = ctx.param_or("aggregates", "");
            if (aggregates_text.empty()) {
                throw std::runtime_error("aggregate_row: 'aggregates' param is required");
            }
            auto agg_json = clink::config::parse(aggregates_text);
            if (!agg_json.is_array()) {
                throw std::runtime_error("aggregate_row: 'aggregates' must be a JSON array");
            }
            std::vector<AggSpec> aggregates;
            for (const auto& entry : agg_json.as_array()) {
                if (!entry.is_object()) {
                    throw std::runtime_error("aggregate_row: aggregate entry must be an object");
                }
                AggSpec spec;
                spec.output_name = entry.at("name").as_string();
                spec.fn = entry.at("fn").as_string();
                if (entry.contains("input_column") && entry.at("input_column").is_string()) {
                    spec.input_column = entry.at("input_column").as_string();
                }
                decode_agg_extras(entry, spec);
                // The GROUP BY operator's input may be a changelog (deletes
                // from an upstream retraction), so MIN / MAX must track a
                // live-value multiset to recompute the extreme on retract.
                spec.retractable = true;
                aggregates.push_back(std::move(spec));
            }
            return std::make_shared<AggregateRowOp>(
                std::move(group_keys), std::move(aggregates), std::move(group_key_outputs));
        });

    // project_row: per-row expression evaluation. The 'outputs' param
    // is a JSON array of {name, expr} where expr is in the value-
    // expression format defined by
    // include/clink/operators/json_value_expr.hpp. Each input Row is
    // mapped to a new Row containing one field per output, the field
    // value being the expression's evaluation against the input.
    reg.register_operator<Row, Row>(
        "project_row", [](const BuildContext& ctx) -> std::shared_ptr<Operator<Row, Row>> {
            const auto outputs_text = ctx.param_or("outputs", "");
            if (outputs_text.empty()) {
                throw std::runtime_error("project_row: 'outputs' param is required");
            }
            auto outputs_json =
                std::make_shared<clink::config::JsonValue>(clink::config::parse(outputs_text));
            if (!outputs_json->is_array()) {
                throw std::runtime_error("project_row: 'outputs' must be a JSON array");
            }
            // Pre-extract (name, expr) pairs into shared_ptr to avoid
            // re-walking JSON on every record.
            struct Plan {
                std::vector<std::pair<std::string, clink::config::JsonValue>> outs;
            };
            auto plan = std::make_shared<Plan>();
            for (const auto& entry : outputs_json->as_array()) {
                if (!entry.is_object() || !entry.contains("name") || !entry.contains("expr")) {
                    throw std::runtime_error("project_row: each output must be {name, expr}");
                }
                plan->outs.emplace_back(entry.at("name").as_string(), entry.at("expr"));
            }
            return std::make_shared<MapOperator<Row, Row>>(
                [plan](const Row& r) -> Row {
                    Row out;
                    auto resolve = [&](const std::string& name) -> clink::config::JsonValue {
                        auto it = r.values.find(name);
                        if (it == r.values.end()) {
                            return clink::config::JsonValue{nullptr};
                        }
                        return it->second;
                    };
                    for (const auto& [name, expr] : plan->outs) {
                        out.values[name] =
                            clink::operators::evaluate_json_value_expr(expr, resolve);
                    }
                    // Phase 21a: privileged synthetic fields ride through
                    // project_row even when they aren't in the declared
                    // outputs. __row_kind carries changelog semantics
                    // (insert / delete); dropping it here would silently
                    // turn a retract stream into an append stream.
                    copy_row_kind(r, out);
                    return out;
                },
                "project_row");
        });
}

}  // namespace clink::sql
