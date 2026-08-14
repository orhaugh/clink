// Differential SQL oracle: the same query, the same data, clink versus
// DuckDB, and the answers must agree.
//
// Every other SQL test in this tree is self-referential - it asserts what
// we BELIEVE the semantics are, so a misunderstanding shared by the
// implementation and its author passes cleanly. This suite removes that
// blind spot by comparing against an independent implementation. It found
// its first defect while it was still a shell probe: SUM over a group
// whose every input is NULL returned 0 where the standard (and DuckDB,
// Postgres, SQLite) says NULL.
//
// Shape. A fixed, seeded dataset is written once as NDJSON. Each query
// runs twice: through clink's EmbeddedEngine (file/json source and sink -
// the same path `clink run` drives) and through the duckdb CLI (read_json
// with PINNED column types, so schema inference cannot diverge; COPY ...
// (FORMAT JSON) out). Both outputs are parsed, canonicalised and compared
// as multisets - column order and row order carry no meaning.
//
// The one semantic bridge: clink's unbounded GROUP BY emits RUNNING
// results (one row per input row), while DuckDB emits finals. For grouped
// queries the harness reduces clink's stream to the last emission per
// group key before comparing - per-key order is preserved at
// parallelism 1, so "last" is well defined. Everything else is compared
// directly.
//
// The oracle binary is optional: no duckdb on PATH (or CLINK_DUCKDB) means
// the suite SKIPS, mirroring the docker-gated suites. Determinism: data
// and generated queries use fixed seeds; CLINK_ORACLE_SEED and
// CLINK_ORACLE_N widen the generated families for exploratory runs, and
// every failure prints the seed and the full query so it reproduces.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/embed/embedded_engine.hpp"

namespace {

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Locating the oracle.

std::string find_duckdb() {
    if (const char* env = std::getenv("CLINK_DUCKDB"); env && *env) {
        return env;
    }
    const char* path = std::getenv("PATH");
    if (path == nullptr) {
        return {};
    }
    std::istringstream dirs{std::string{path}};
    std::string dir;
    while (std::getline(dirs, dir, ':')) {
        if (dir.empty()) {
            continue;
        }
        const fs::path candidate = fs::path(dir) / "duckdb";
        std::error_code ec;
        if (fs::exists(candidate, ec) && ::access(candidate.c_str(), X_OK) == 0) {
            return candidate.string();
        }
    }
    return {};
}

// ---------------------------------------------------------------------------
// Canonical cells. Each output cell becomes a tagged string so rows can be
// sorted and diffed exactly. Integral doubles unify with integers ("4.0"
// from DuckDB's DOUBLE must equal clink's BIGINT 4), and non-integral
// doubles are rounded to 9 significant digits, which absorbs
// summation-order noise while still distinguishing genuinely different
// answers.

std::string canon_cell(const clink::config::JsonValue& v) {
    if (v.is_null()) {
        return "@null";
    }
    if (v.is_bool()) {
        return v.as_bool() ? "b:1" : "b:0";
    }
    if (v.is_number()) {
        const double d = v.as_number();
        const double integral_probe = std::floor(d);
        if (d == integral_probe && std::abs(d) < 9.0e15) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "i:%lld", static_cast<long long>(d));
            return buf;
        }
        char buf[48];
        std::snprintf(buf, sizeof(buf), "d:%.9g", d == 0.0 ? 0.0 : d);
        return buf;
    }
    if (v.is_string()) {
        return "s:" + v.as_string();
    }
    return "@unsupported";
}

// ---------------------------------------------------------------------------
// Query descriptions.

struct OutCol {
    std::string name;
    std::string clink_type;  // the sink DDL type on the clink side
};

struct QuerySpec {
    std::string name;
    std::string select_sql;               // identical text on both sides
    std::vector<OutCol> out_cols;         // canonical column order + sink DDL
    std::vector<std::string> group_keys;  // non-empty => last-emission-per-key
};

struct RunResult {
    bool ok{false};
    std::string error;
    std::vector<clink::config::JsonValue> rows;  // objects, file order
};

std::vector<std::string> read_lines(const fs::path& path) {
    std::vector<std::string> lines;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) {
            lines.push_back(line);
        }
    }
    return lines;
}

RunResult parse_ndjson(const fs::path& path) {
    RunResult r;
    r.ok = true;
    std::error_code ec;
    if (!fs::exists(path, ec)) {
        return r;  // zero output rows is a legal answer, not an error
    }
    for (const auto& l : read_lines(path)) {
        auto js = clink::config::parse(l);
        if (!js.is_object()) {
            r.ok = false;
            r.error = "non-object output line: " + l;
            return r;
        }
        r.rows.push_back(std::move(js));
    }
    return r;
}

// ---------------------------------------------------------------------------
// The fixture: dataset on disk, both runners, the comparator.

class SqlOracle : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        duckdb_ = find_duckdb();
        dir_ = fs::temp_directory_path() / ("clink_sql_oracle_" + std::to_string(::getpid()));
        fs::remove_all(dir_);
        fs::create_directories(dir_);
        write_dataset_();
    }

    static void TearDownTestSuite() {
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    void SetUp() override {
        if (duckdb_.empty()) {
            GTEST_SKIP() << "no duckdb binary on PATH (or CLINK_DUCKDB); "
                            "the differential oracle needs one - `brew install duckdb`";
        }
    }

    // --- dataset -----------------------------------------------------------
    //
    // t: the workhorse. Small key range so groups collide, ~20% NULLs so
    // three-valued logic is exercised everywhere, values small enough that
    // no expression the generator builds can overflow or lose double
    // precision in the comparator.
    // u: the join peer. t2: hand-written rows for the aggregate NULL edges
    // (a group that is entirely NULL, a lone-row group, a sum that lands
    // exactly on zero) that random data hits too rarely to rely on.

    static constexpr std::uint64_t kDataSeed = 7;

    static void write_dataset_() {
        std::mt19937_64 rng{kDataSeed};
        auto pick = [&rng](int lo, int hi) {
            return std::uniform_int_distribution<int>(lo, hi)(rng);
        };
        auto null_at = [&](int pct) { return pick(1, 100) <= pct; };
        static const std::vector<std::string> kStrings{"", "a", "b", "ab", "BA", "x y", "A"};

        {
            std::ofstream out(dir_ / "t.ndjson", std::ios::trunc);
            for (int i = 0; i < 200; ++i) {
                out << "{\"k\":" << (null_at(8) ? "null" : std::to_string(pick(0, 7)))
                    << ",\"v\":" << (null_at(20) ? "null" : std::to_string(pick(-50, 50)))
                    << ",\"w\":" << (null_at(20) ? "null" : std::to_string(pick(-5, 5)))
                    << ",\"s\":"
                    << (null_at(15) ? "null"
                                    : "\"" +
                                          kStrings[static_cast<std::size_t>(
                                              pick(0, static_cast<int>(kStrings.size()) - 1))] +
                                          "\"")
                    << "}\n";
            }
        }
        {
            std::ofstream out(dir_ / "u.ndjson", std::ios::trunc);
            for (int i = 0; i < 40; ++i) {
                out << "{\"k\":" << (null_at(10) ? "null" : std::to_string(pick(0, 9)))
                    << ",\"x\":" << (null_at(15) ? "null" : std::to_string(pick(-10, 10))) << "}\n";
            }
        }
        {
            std::ofstream out(dir_ / "t2.ndjson", std::ios::trunc);
            out << R"({"k":1,"v":null})" << "\n"
                << R"({"k":1,"v":null})" << "\n"
                << R"({"k":2,"v":5})" << "\n"
                << R"({"k":2,"v":null})" << "\n"
                << R"({"k":3,"v":-7})" << "\n"
                << R"({"k":3,"v":7})" << "\n"
                << R"({"k":4,"v":0})" << "\n";
        }
    }

    // --- the clink side ----------------------------------------------------

    static RunResult run_clink(const QuerySpec& q, const std::string& select_sql) {
        const fs::path out_path =
            dir_ / (q.name + "_clink_" + std::to_string(counter_++) + ".ndjson");
        std::string ddl_cols;
        for (const auto& c : q.out_cols) {
            if (!ddl_cols.empty()) {
                ddl_cols += ", ";
            }
            ddl_cols += c.name + " " + c.clink_type;
        }
        const std::string script =
            "CREATE TABLE t (k BIGINT, v BIGINT, w BIGINT, s VARCHAR) "
            "WITH (connector='file', format='json', path='" +
            (dir_ / "t.ndjson").string() +
            "');"
            "CREATE TABLE u (k BIGINT, x BIGINT) "
            "WITH (connector='file', format='json', path='" +
            (dir_ / "u.ndjson").string() +
            "');"
            "CREATE TABLE t2 (k BIGINT, v BIGINT) "
            "WITH (connector='file', format='json', path='" +
            (dir_ / "t2.ndjson").string() +
            "');"
            "CREATE TABLE oracle_out (" +
            ddl_cols +
            ") "
            "WITH (connector='file', format='json', path='" +
            out_path.string() +
            "');"
            "INSERT INTO oracle_out " +
            select_sql;

        clink::embed::EngineOptions opts;
        std::ostringstream err;
        opts.err = &err;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        if (engine.execute_script(script) != 0) {
            return {.ok = false, .error = "clink rejected the script: " + err.str(), .rows = {}};
        }
        if (!engine.await_all()) {
            return {.ok = false, .error = "clink job failed: " + err.str(), .rows = {}};
        }
        return parse_ndjson(out_path);
    }

    // --- the duckdb side ---------------------------------------------------

    static RunResult run_duckdb(const QuerySpec& q, const std::string& select_sql) {
        const std::string idx = std::to_string(counter_++);
        const fs::path out_path = dir_ / (q.name + "_duck_" + idx + ".ndjson");
        const fs::path script_path = dir_ / (q.name + "_duck_" + idx + ".sql");
        const fs::path err_path = dir_ / (q.name + "_duck_" + idx + ".err");
        {
            std::ofstream script(script_path, std::ios::trunc);
            script << "CREATE TABLE t AS SELECT * FROM read_json('" << (dir_ / "t.ndjson").string()
                   << "', format='newline_delimited', "
                      "columns={k: 'BIGINT', v: 'BIGINT', w: 'BIGINT', s: 'VARCHAR'});\n"
                   << "CREATE TABLE u AS SELECT * FROM read_json('" << (dir_ / "u.ndjson").string()
                   << "', format='newline_delimited', columns={k: 'BIGINT', x: 'BIGINT'});\n"
                   << "CREATE TABLE t2 AS SELECT * FROM read_json('"
                   << (dir_ / "t2.ndjson").string()
                   << "', format='newline_delimited', columns={k: 'BIGINT', v: 'BIGINT'});\n"
                   << "COPY (" << select_sql << ") TO '" << out_path.string()
                   << "' (FORMAT JSON);\n";
        }
        const std::string cmd = "'" + duckdb_ + "' -batch :memory: < '" + script_path.string() +
                                "' > /dev/null 2> '" + err_path.string() + "'";
        if (std::system(cmd.c_str()) != 0) {
            std::string err;
            for (const auto& l : read_lines(err_path)) {
                err += l + "\n";
            }
            return {.ok = false, .error = "duckdb rejected the script: " + err, .rows = {}};
        }
        return parse_ndjson(out_path);
    }

    // --- comparison --------------------------------------------------------

    static std::vector<std::string> canon_rows(const QuerySpec& q,
                                               const std::vector<clink::config::JsonValue>& rows,
                                               bool reduce_to_last_per_key) {
        // A sink column absent from an output object reads as NULL: the two
        // engines are free to omit-vs-write null fields differently, and
        // that difference carries no meaning.
        static const clink::config::JsonValue kNull{};
        auto cell = [](const clink::config::JsonValue& row,
                       const std::string& col) -> const clink::config::JsonValue& {
            const auto& obj = row.as_object();
            const auto it = obj.find(col);
            return it == obj.end() ? kNull : it->second;
        };

        if (reduce_to_last_per_key && !q.group_keys.empty()) {
            // Streaming GROUP BY emits the running value per input row; the
            // last emission per key is the final answer. File order IS
            // emission order at parallelism 1.
            std::map<std::string, std::string> last;
            for (const auto& row : rows) {
                std::string key;
                for (const auto& g : q.group_keys) {
                    key += canon_cell(cell(row, g)) + "\x1f";
                }
                std::string full;
                for (const auto& c : q.out_cols) {
                    full += canon_cell(cell(row, c.name)) + "\x1f";
                }
                last[key] = std::move(full);
            }
            std::vector<std::string> out;
            out.reserve(last.size());
            for (auto& [_, v] : last) {
                out.push_back(std::move(v));
            }
            std::sort(out.begin(), out.end());
            return out;
        }

        std::vector<std::string> out;
        out.reserve(rows.size());
        for (const auto& row : rows) {
            std::string full;
            for (const auto& c : q.out_cols) {
                full += canon_cell(cell(row, c.name)) + "\x1f";
            }
            out.push_back(std::move(full));
        }
        std::sort(out.begin(), out.end());
        return out;
    }

    struct Divergence {
        bool diverged{false};
        std::string report;
    };

    static Divergence diff(const QuerySpec& q,
                           const std::vector<std::string>& clink_rows,
                           const std::vector<std::string>& duck_rows,
                           std::size_t clink_raw_count) {
        std::vector<std::string> only_clink;
        std::vector<std::string> only_duck;
        std::size_t i = 0;
        std::size_t j = 0;
        while (i < clink_rows.size() && j < duck_rows.size()) {
            if (clink_rows[i] == duck_rows[j]) {
                ++i;
                ++j;
            } else if (clink_rows[i] < duck_rows[j]) {
                only_clink.push_back(clink_rows[i++]);
            } else {
                only_duck.push_back(duck_rows[j++]);
            }
        }
        only_clink.insert(
            only_clink.end(), clink_rows.begin() + static_cast<long>(i), clink_rows.end());
        only_duck.insert(
            only_duck.end(), duck_rows.begin() + static_cast<long>(j), duck_rows.end());
        if (only_clink.empty() && only_duck.empty()) {
            return {};
        }
        std::ostringstream os;
        auto printable = [](std::string s) {
            for (auto& ch : s) {
                if (ch == '\x1f') {
                    ch = '|';
                }
            }
            return s;
        };
        os << "DIVERGENCE on query '" << q.name << "':\n  " << q.select_sql
           << "\n  rows: clink=" << clink_rows.size() << " (raw " << clink_raw_count
           << "), duckdb=" << duck_rows.size() << "\n";
        os << "  only clink produced (" << only_clink.size() << ", first 5):\n";
        for (std::size_t n = 0; n < only_clink.size() && n < 5; ++n) {
            os << "    " << printable(only_clink[n]) << "\n";
        }
        os << "  only duckdb produced (" << only_duck.size() << ", first 5):\n";
        for (std::size_t n = 0; n < only_duck.size() && n < 5; ++n) {
            os << "    " << printable(only_duck[n]) << "\n";
        }
        return {.diverged = true, .report = os.str()};
    }

    // Run one spec on both engines. `duck_sql_override` exists for the
    // vacuity test ONLY - the whole point of the oracle is that both sides
    // execute the same text.
    static Divergence run_pair(const QuerySpec& q, const std::string& duck_sql_override = {}) {
        auto clink_r = run_clink(q, q.select_sql);
        if (!clink_r.ok) {
            return {.diverged = true,
                    .report = "query '" + q.name + "': " + clink_r.error + "\n  " + q.select_sql};
        }
        auto duck_r = run_duckdb(q, duck_sql_override.empty() ? q.select_sql : duck_sql_override);
        if (!duck_r.ok) {
            return {.diverged = true,
                    .report = "query '" + q.name + "': " + duck_r.error + "\n  " + q.select_sql};
        }
        const auto clink_canon = canon_rows(q, clink_r.rows, /*reduce_to_last_per_key=*/true);
        const auto duck_canon = canon_rows(q, duck_r.rows, /*reduce_to_last_per_key=*/false);
        return diff(q, clink_canon, duck_canon, clink_r.rows.size());
    }

    static std::string duckdb_;
    static fs::path dir_;
    static int counter_;
};

std::string SqlOracle::duckdb_;
fs::path SqlOracle::dir_;
int SqlOracle::counter_ = 0;

// ---------------------------------------------------------------------------
// Vacuity first: a comparator that cannot see a difference would make every
// agreement below meaningless. This drives a real one-cell difference
// through the FULL pipeline - both engines, files, parsing, reduction,
// diff - and requires it to be reported.

TEST_F(SqlOracle, PipelineDetectsAnInjectedOneCellDivergence) {
    QuerySpec q{.name = "vacuity",
                .select_sql = "SELECT k AS c0, v AS c1 FROM t WHERE v IS NOT NULL",
                .out_cols = {{"c0", "BIGINT"}, {"c1", "BIGINT"}},
                .group_keys = {}};
    const auto d = run_pair(q, "SELECT k AS c0, v + 1 AS c1 FROM t WHERE v IS NOT NULL");
    EXPECT_TRUE(d.diverged)
        << "the oracle compared clink's `v` against duckdb's `v + 1` and saw no difference - "
           "the comparator is blind and every agreement this suite reports is vacuous";
}

// ---------------------------------------------------------------------------
// Curated corpus: the semantic edges worth naming individually.

TEST_F(SqlOracle, ProjectionArithmeticAndNullPropagation) {
    const auto d = run_pair(
        {.name = "proj_arith",
         .select_sql = "SELECT k AS c0, v + w AS c1, v - w AS c2, ABS(v) AS c3, -v AS c4 FROM t",
         .out_cols = {{"c0", "BIGINT"},
                      {"c1", "BIGINT"},
                      {"c2", "BIGINT"},
                      {"c3", "BIGINT"},
                      {"c4", "BIGINT"}},
         .group_keys = {}});
    EXPECT_FALSE(d.diverged) << d.report;
}

TEST_F(SqlOracle, CaseCoalesceNullif) {
    const auto d = run_pair(
        {.name = "proj_case",
         .select_sql = "SELECT k AS c0, CASE WHEN v > 0 THEN v ELSE w END AS c1, "
                       "COALESCE(v, -99) AS c2, NULLIF(w, 0) AS c3 FROM t",
         .out_cols = {{"c0", "BIGINT"}, {"c1", "BIGINT"}, {"c2", "BIGINT"}, {"c3", "BIGINT"}},
         .group_keys = {}});
    EXPECT_FALSE(d.diverged) << d.report;
}

TEST_F(SqlOracle, ThreeValuedLogicDropsUnknownRows) {
    const auto d = run_pair({.name = "filter_3vl",
                             .select_sql = "SELECT k AS c0, v AS c1, w AS c2 FROM t "
                                           "WHERE v > 0 OR w < -2",
                             .out_cols = {{"c0", "BIGINT"}, {"c1", "BIGINT"}, {"c2", "BIGINT"}},
                             .group_keys = {}});
    EXPECT_FALSE(d.diverged) << d.report;
}

TEST_F(SqlOracle, IsNullPredicates) {
    const auto d = run_pair({.name = "filter_is_null",
                             .select_sql = "SELECT k AS c0, w AS c1 FROM t "
                                           "WHERE v IS NULL AND s IS NOT NULL",
                             .out_cols = {{"c0", "BIGINT"}, {"c1", "BIGINT"}},
                             .group_keys = {}});
    EXPECT_FALSE(d.diverged) << d.report;
}

TEST_F(SqlOracle, StringFunctions) {
    const auto d =
        run_pair({.name = "strings",
                  .select_sql = "SELECT k AS c0, UPPER(s) AS c1, LOWER(s) AS c2, LENGTH(s) AS c3, "
                                "COALESCE(s, '~') AS c4 FROM t",
                  .out_cols = {{"c0", "BIGINT"},
                               {"c1", "VARCHAR"},
                               {"c2", "VARCHAR"},
                               {"c3", "BIGINT"},
                               {"c4", "VARCHAR"}},
                  .group_keys = {}});
    EXPECT_FALSE(d.diverged) << d.report;
}

TEST_F(SqlOracle, DistinctPairs) {
    const auto d = run_pair({.name = "distinct_pair",
                             .select_sql = "SELECT DISTINCT k AS c0, s AS c1 FROM t",
                             .out_cols = {{"c0", "BIGINT"}, {"c1", "VARCHAR"}},
                             .group_keys = {}});
    EXPECT_FALSE(d.diverged) << d.report;
}

TEST_F(SqlOracle, GroupByCountsAndSum) {
    const auto d = run_pair(
        {.name = "group_counts",
         .select_sql = "SELECT k AS g0, COUNT(*) AS c0, COUNT(v) AS c1, SUM(v) AS c2 "
                       "FROM t GROUP BY k",
         .out_cols = {{"g0", "BIGINT"}, {"c0", "BIGINT"}, {"c1", "BIGINT"}, {"c2", "BIGINT"}},
         .group_keys = {"g0"}});
    EXPECT_FALSE(d.diverged) << d.report;
}

TEST_F(SqlOracle, AggregatesOverAnAllNullGroupAreNullNotZero) {
    // The hand-built rows in t2 exist for this: group 1 is entirely NULL,
    // so the standard answer is SUM=NULL, MIN=NULL, MAX=NULL, COUNT(v)=0.
    // An accumulator seeded with 0 instead of "no value yet" passes every
    // self-referential test and fails here.
    const auto d =
        run_pair({.name = "group_allnull",
                  .select_sql = "SELECT k AS g0, SUM(v) AS c0, MIN(v) AS c1, MAX(v) AS c2, "
                                "COUNT(v) AS c3 FROM t2 GROUP BY k",
                  .out_cols = {{"g0", "BIGINT"},
                               {"c0", "BIGINT"},
                               {"c1", "BIGINT"},
                               {"c2", "BIGINT"},
                               {"c3", "BIGINT"}},
                  .group_keys = {"g0"}});
    EXPECT_FALSE(d.diverged) << d.report;
}

TEST_F(SqlOracle, GroupByStringKeyIncludingTheNullGroup) {
    const auto d = run_pair({.name = "group_by_s",
                             .select_sql = "SELECT s AS g0, COUNT(*) AS c0, SUM(v) AS c1 "
                                           "FROM t GROUP BY s",
                             .out_cols = {{"g0", "VARCHAR"}, {"c0", "BIGINT"}, {"c1", "BIGINT"}},
                             .group_keys = {"g0"}});
    EXPECT_FALSE(d.diverged) << d.report;
}

TEST_F(SqlOracle, GroupByBigintKeyIncludingTheNullGroup) {
    // k is nullable: the standard collects every NULL key into ONE group.
    const auto d = run_pair(
        {.name = "group_null_key",
         .select_sql = "SELECT k AS g0, COUNT(*) AS c0, MIN(w) AS c1, "
                       "MAX(w) AS c2 FROM t GROUP BY k",
         .out_cols = {{"g0", "BIGINT"}, {"c0", "BIGINT"}, {"c1", "BIGINT"}, {"c2", "BIGINT"}},
         .group_keys = {"g0"}});
    EXPECT_FALSE(d.diverged) << d.report;
}

TEST_F(SqlOracle, GroupByWithAFilterUnderneath) {
    const auto d = run_pair({.name = "group_where",
                             .select_sql = "SELECT k AS g0, SUM(w) AS c0 FROM t "
                                           "WHERE v IS NOT NULL GROUP BY k",
                             .out_cols = {{"g0", "BIGINT"}, {"c0", "BIGINT"}},
                             .group_keys = {"g0"}});
    EXPECT_FALSE(d.diverged) << d.report;
}

TEST_F(SqlOracle, AvgIsADouble) {
    const auto d = run_pair({.name = "avg_double",
                             .select_sql = "SELECT k AS g0, AVG(v) AS c0 FROM t GROUP BY k",
                             .out_cols = {{"g0", "BIGINT"}, {"c0", "DOUBLE"}},
                             .group_keys = {"g0"}});
    EXPECT_FALSE(d.diverged) << d.report;
}

TEST_F(SqlOracle, ModSignFollowsTheDividend) {
    const auto d = run_pair({.name = "mod_sign",
                             .select_sql = "SELECT k AS c0, MOD(v, 7) AS c1 FROM t "
                                           "WHERE v IS NOT NULL",
                             .out_cols = {{"c0", "BIGINT"}, {"c1", "BIGINT"}},
                             .group_keys = {}});
    EXPECT_FALSE(d.diverged) << d.report;
}

TEST_F(SqlOracle, InnerJoinOnEquality) {
    // NULL keys match nothing on either side - the join predicate is an
    // equality, and NULL = NULL is UNKNOWN.
    const auto d = run_pair({.name = "join_inner",
                             .select_sql = "SELECT t.k AS c0, t.v AS c1, u.x AS c2 FROM t "
                                           "INNER JOIN u ON t.k = u.k",
                             .out_cols = {{"c0", "BIGINT"}, {"c1", "BIGINT"}, {"c2", "BIGINT"}},
                             .group_keys = {}});
    EXPECT_FALSE(d.diverged) << d.report;
}

TEST_F(SqlOracle, InnerJoinWithAPostFilter) {
    const auto d = run_pair({.name = "join_filtered",
                             .select_sql = "SELECT t.k AS c0, u.x AS c1 FROM t "
                                           "INNER JOIN u ON t.k = u.k WHERE u.x > 0",
                             .out_cols = {{"c0", "BIGINT"}, {"c1", "BIGINT"}},
                             .group_keys = {}});
    EXPECT_FALSE(d.diverged) << d.report;
}

// ---------------------------------------------------------------------------
// Generated families. Deterministic by default (fixed seed, so CI failures
// reproduce); CLINK_ORACLE_SEED and CLINK_ORACLE_N are the exploration
// knobs, and the failure message always carries the seed and full query.

namespace gen {

std::uint64_t seed() {
    if (const char* s = std::getenv("CLINK_ORACLE_SEED"); s && *s) {
        return std::strtoull(s, nullptr, 10);
    }
    return 20260814ULL;
}

int per_family() {
    if (const char* s = std::getenv("CLINK_ORACLE_N"); s && *s) {
        return std::max(1, std::atoi(s));
    }
    return 6;
}

struct Rng {
    std::mt19937_64 e;
    int pick(int lo, int hi) { return std::uniform_int_distribution<int>(lo, hi)(e); }
    std::string constant() { return std::to_string(pick(-9, 9)); }
};

std::string pred(Rng& r, int depth);

// Integer-typed expressions over t's columns. No division (dialects
// disagree on `/` for integers), no unbounded growth (inputs are <= 50, so
// nothing here can leave exact-double range).
std::string int_expr(Rng& r, int depth = 0) {
    const int choice = r.pick(0, depth > 1 ? 6 : 9);
    switch (choice) {
        case 0:
            return "v";
        case 1:
            return "w";
        case 2:
            return r.constant();
        case 3:
            return "ABS(" + int_expr(r, depth + 1) + ")";
        case 4:
            return "COALESCE(v, " + r.constant() + ")";
        case 5:
            return "NULLIF(w, " + r.constant() + ")";
        case 6:
            return "-(" + int_expr(r, depth + 1) + ")";
        case 7:
            return "(" + int_expr(r, depth + 1) + " + " + int_expr(r, depth + 1) + ")";
        case 8:
            return "(" + int_expr(r, depth + 1) + " - " + int_expr(r, depth + 1) + ")";
        default:
            return "CASE WHEN " + pred(r, depth + 1) + " THEN " + int_expr(r, depth + 1) +
                   " ELSE " + int_expr(r, depth + 1) + " END";
    }
}

std::string pred(Rng& r, int depth) {
    const int choice = r.pick(0, depth > 1 ? 5 : 8);
    static const char* kCmp[] = {" > ", " < ", " >= ", " <= ", " = ", " <> "};
    switch (choice) {
        case 0:
            return "v" + std::string{kCmp[r.pick(0, 5)]} + r.constant();
        case 1:
            return "w" + std::string{kCmp[r.pick(0, 5)]} + r.constant();
        case 2:
            return "k" + std::string{kCmp[r.pick(0, 5)]} + std::to_string(r.pick(0, 7));
        case 3:
            return "v IS NULL";
        case 4:
            return "s IS NOT NULL";
        case 5:
            return "s = '" + std::string{static_cast<char>('a' + r.pick(0, 1))} + "'";
        case 6:
            return "(" + pred(r, depth + 1) + " AND " + pred(r, depth + 1) + ")";
        case 7:
            return "(" + pred(r, depth + 1) + " OR " + pred(r, depth + 1) + ")";
        default:
            return "NOT (" + pred(r, depth + 1) + ")";
    }
}

}  // namespace gen

TEST_F(SqlOracle, GeneratedProjectionsAndFiltersAgree) {
    gen::Rng r{std::mt19937_64{gen::seed() ^ 0xA1}};
    for (int i = 0; i < gen::per_family(); ++i) {
        const std::string sql = "SELECT k AS c0, " + gen::int_expr(r) + " AS c1, " +
                                gen::int_expr(r) + " AS c2 FROM t WHERE " + gen::pred(r, 0);
        const auto d = run_pair({.name = "gen_proj_" + std::to_string(i),
                                 .select_sql = sql,
                                 .out_cols = {{"c0", "BIGINT"}, {"c1", "BIGINT"}, {"c2", "BIGINT"}},
                                 .group_keys = {}});
        EXPECT_FALSE(d.diverged) << "seed=" << gen::seed() << "\n" << d.report;
    }
}

TEST_F(SqlOracle, GeneratedGroupedAggregatesAgree) {
    gen::Rng r{std::mt19937_64{gen::seed() ^ 0xB2}};
    static const char* kAggs[] = {
        "COUNT(*)", "COUNT(v)", "SUM(v)", "MIN(v)", "MAX(v)", "SUM(w)", "MIN(w)", "MAX(w)"};
    for (int i = 0; i < gen::per_family(); ++i) {
        const std::string a0 = kAggs[r.pick(0, 7)];
        const std::string a1 = kAggs[r.pick(0, 7)];
        std::string sql = "SELECT k AS g0, " + a0 + " AS c0, " + a1 + " AS c1 FROM t";
        if (r.pick(0, 1) == 1) {
            sql += " WHERE " + gen::pred(r, 0);
        }
        sql += " GROUP BY k";
        const auto d = run_pair({.name = "gen_agg_" + std::to_string(i),
                                 .select_sql = sql,
                                 .out_cols = {{"g0", "BIGINT"}, {"c0", "BIGINT"}, {"c1", "BIGINT"}},
                                 .group_keys = {"g0"}});
        EXPECT_FALSE(d.diverged) << "seed=" << gen::seed() << "\n" << d.report;
    }
}

TEST_F(SqlOracle, GeneratedJoinsAgree) {
    gen::Rng r{std::mt19937_64{gen::seed() ^ 0xC3}};
    for (int i = 0; i < gen::per_family(); ++i) {
        std::string sql =
            "SELECT t.k AS c0, t.v AS c1, u.x AS c2 FROM t "
            "INNER JOIN u ON t.k = u.k";
        switch (r.pick(0, 2)) {
            case 0:
                sql += " WHERE t.v > " + r.constant();
                break;
            case 1:
                sql += " WHERE u.x <= " + r.constant();
                break;
            default:
                break;
        }
        const auto d = run_pair({.name = "gen_join_" + std::to_string(i),
                                 .select_sql = sql,
                                 .out_cols = {{"c0", "BIGINT"}, {"c1", "BIGINT"}, {"c2", "BIGINT"}},
                                 .group_keys = {}});
        EXPECT_FALSE(d.diverged) << "seed=" << gen::seed() << "\n" << d.report;
    }
}

TEST_F(SqlOracle, GeneratedDistinctsAgree) {
    gen::Rng r{std::mt19937_64{gen::seed() ^ 0xD4}};
    for (int i = 0; i < gen::per_family(); ++i) {
        const std::string sql = "SELECT DISTINCT k AS c0, " + gen::int_expr(r) +
                                " AS c1 FROM t WHERE " + gen::pred(r, 0);
        const auto d = run_pair({.name = "gen_distinct_" + std::to_string(i),
                                 .select_sql = sql,
                                 .out_cols = {{"c0", "BIGINT"}, {"c1", "BIGINT"}},
                                 .group_keys = {}});
        EXPECT_FALSE(d.diverged) << "seed=" << gen::seed() << "\n" << d.report;
    }
}

}  // namespace
