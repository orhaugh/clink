// Born-columnar output against the row carrier, end to end through the real
// planner and the embedded engine.
//
// An inner join or a window whose consumer ingests columnar appends its output
// cells straight into typed Arrow builders (RowColumnarOutput) instead of
// building a Row each. The sidecar layout keeps BIGINT, INTEGER, DOUBLE, REAL,
// BOOLEAN, VARCHAR, DECIMAL and REAL[] as typed columns and stores everything
// else as text, so a TIMESTAMP, SMALLINT or DATE number, or an ARRAY, MAP or ROW
// value, could come back out of that batch as a JSON string where the row
// carrier kept the number, array or object.
//
// The oracle is the row carrier: the same script, the same input, compiled
// with CLINK_DISABLE_COLUMNAR_OUTPUT=1 (read per compile). Each case also
// checks which carrier the first run took: the producer carries
// `columnar_output` in its plan, and the bail counter says whether any
// emission fell back to rows. The materialisation counter alone cannot say: a
// bail decodes what it had accumulated, as a consumer of a batch that stayed
// columnar does.

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/embed/embedded_engine.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/row_columnar_batcher.hpp"
#include "clink/sql/row_columnar_output.hpp"
#include "clink/sql/script_runner.hpp"

namespace fs = std::filesystem;

namespace {

fs::path parity_scratch(const std::string& name) {
    const auto dir = fs::temp_directory_path() /
                     ("clink_born_columnar_" + name + "_" + std::to_string(::getpid()));
    fs::remove_all(dir);
    fs::create_directories(dir);
    return dir;
}

void parity_write_lines(const fs::path& path, const std::vector<std::string>& lines) {
    std::ofstream out(path, std::ios::trunc);
    for (const auto& l : lines) {
        out << l << "\n";
    }
}

std::vector<std::string> parity_read_sorted(const fs::path& path) {
    std::vector<std::string> lines;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) {
            lines.push_back(line);
        }
    }
    // The emission order is not what is being compared here: a join's two file
    // sources interleave as the scheduler runs them, and a window fire visits
    // its groups in hash order. Sorting makes the comparison about the rows
    // alone. That a bail keeps the emission order is pinned at the Dag level, in
    // test_sql_runtime.cpp, where the order is deterministic.
    std::sort(lines.begin(), lines.end());
    return lines;
}

// Every spec `ddl` then `sql` compiles, as the script runner hands them over.
std::vector<clink::cluster::JobGraphSpec> parity_compile(const std::string& ddl,
                                                         const std::string& sql) {
    clink::sql::Catalog catalog;
    clink::sql::ScriptRunOptions opts;
    opts.parallelism = 1;
    std::ostringstream out;
    std::ostringstream err;
    const clink::sql::ScriptIO io{&out, &err};
    std::vector<clink::cluster::JobGraphSpec> specs;
    const auto submit = [&specs](const clink::cluster::JobGraphSpec& spec,
                                 const std::string& /*name*/) {
        specs.push_back(spec);
        return 0;
    };
    if (clink::sql::run_script(ddl, catalog, opts, io, submit) != 0) {
        throw std::runtime_error("the DDL did not compile: " + err.str());
    }
    if (clink::sql::run_script(sql, catalog, opts, io, submit) != 0) {
        throw std::runtime_error("the script did not compile: " + err.str());
    }
    return specs;
}

const clink::cluster::OperatorSpec* parity_only_op(const clink::cluster::JobGraphSpec& spec,
                                                   const std::string& type) {
    const clink::cluster::OperatorSpec* found = nullptr;
    for (const auto& op : spec.ops) {
        if (op.type == type) {
            if (found != nullptr) {
                return nullptr;
            }
            found = &op;
        }
    }
    return found;
}

std::string parity_plan_types(const clink::cluster::JobGraphSpec& spec) {
    std::string out;
    for (const auto& op : spec.ops) {
        out += (out.empty() ? "" : ", ") + op.type;
    }
    return out;
}

struct ParityRun {
    std::vector<std::string> lines;
    std::uint64_t decoded{0};  // columnar batches materialised during the run
    std::uint64_t bails{0};    // emissions begun columnar and finished in row form
};

// One engine, `ddl` then `insert`, to completion; the output file's lines.
ParityRun parity_run(const std::string& ddl, const std::string& insert, const fs::path& out) {
    fs::remove(out);
    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    opts.out = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    const auto before = clink::detail::batch_materialize_counter().load();
    const auto bails_before = clink::detail::columnar_output_bail_counter().load();
    EXPECT_EQ(engine.execute_script(ddl), 0) << err.str();
    EXPECT_EQ(engine.execute_script(insert), 0) << err.str();
    EXPECT_TRUE(engine.await_all()) << err.str();
    ParityRun run;
    run.decoded = clink::detail::batch_materialize_counter().load() - before;
    run.bails = clink::detail::columnar_output_bail_counter().load() - bails_before;
    run.lines = parity_read_sorted(out);
    return run;
}

// The same script on the row carrier: columnar output off for every compile in
// the scope.
template <typename F>
auto with_columnar_output_off(F&& f) {
    struct Restore {
        ~Restore() { ::unsetenv("CLINK_DISABLE_COLUMNAR_OUTPUT"); }
    } restore;
    ::setenv("CLINK_DISABLE_COLUMNAR_OUTPUT", "1", 1);
    return f();
}

// Rows per input file. Well past kColumnarOutputMinRows (64), so one emission
// of either producer is big enough for the carrier to be kept.
constexpr int kParityRows = 512;

constexpr std::int64_t kBaseMs = 1700000000000;  // 2023-11-14 22:13:20 UTC
constexpr std::int64_t kBaseDay = 19000;         // 2022-01-08

// One source row carrying every type the sidecar stores as text, with the
// values the row carrier keeps as numbers, arrays and objects.
std::string parity_source_line(int i) {
    std::ostringstream l;
    l << R"({"id":)" << i << R"(,"t":)" << (kBaseMs + (static_cast<std::int64_t>(i % 37) * 1000))
      << R"(,"s":)" << (i % 300) << R"(,"d":)" << (kBaseDay + (i % 50)) << R"(,"arr":[)" << i << ","
      << (i + 1) << R"(],"m":{"x":"v)" << i << R"("},"r":{"a":)" << i << R"(,"b":"b)" << i
      << R"("}})";
    return l.str();
}

std::string parity_join_ddl(const fs::path& dir) {
    std::vector<std::string> src;
    std::vector<std::string> keys;
    for (int i = 1; i <= kParityRows; ++i) {
        src.push_back(parity_source_line(i));
        keys.push_back(R"({"k":)" + std::to_string(i) + "}");
    }
    parity_write_lines(dir / "src.ndjson", src);
    parity_write_lines(dir / "keys.ndjson", keys);
    return "CREATE TABLE src (id BIGINT, t TIMESTAMP(3), s SMALLINT, d DATE, arr BIGINT[], "
           "m MAP<VARCHAR, VARCHAR>, r ROW<a BIGINT, b VARCHAR>) WITH (connector='file', "
           "format='json', path='" +
           (dir / "src.ndjson").string() +
           "');"
           "CREATE TABLE keys (k BIGINT) WITH (connector='file', format='json', path='" +
           (dir / "keys.ndjson").string() + "');";
}

// Which carrier the planned run must have taken through the producer.
enum class Carrier {
    // Every emission rode the sidecar: a columnar batch was decoded and no
    // emission bailed.
    Columnar,
    // Every emission went out in row form because a cell bailed it: no
    // columnar batch was decoded, and the bail counter moved.
    Rows,
};

// The plan for `insert` carries `columnar_output` on `producer`, and the same
// compile with columnar output off does not.
void expect_columnar_plan(const std::string& ddl,
                          const std::string& insert,
                          const std::string& producer) {
    const auto plans = parity_compile(ddl, insert);
    ASSERT_EQ(plans.size(), 1U);
    const auto* op = parity_only_op(plans[0], producer);
    ASSERT_NE(op, nullptr) << "no single " << producer
                           << " in the plan: " << parity_plan_types(plans[0]);
    ASSERT_EQ(op->params.count("columnar_output"), 1U)
        << producer << " no longer emits columnar output, so this case tests the row path: "
        << parity_plan_types(plans[0]);
    const auto row_plans = with_columnar_output_off([&] { return parity_compile(ddl, insert); });
    ASSERT_EQ(row_plans.size(), 1U);
    const auto* row_op = parity_only_op(row_plans[0], producer);
    ASSERT_NE(row_op, nullptr);
    ASSERT_EQ(row_op->params.count("columnar_output"), 0U);
}

void expect_same_lines(const std::vector<std::string>& columnar,
                       const std::vector<std::string>& rows) {
    ASSERT_FALSE(rows.empty()) << "the fixture must produce output";
    ASSERT_EQ(columnar.size(), rows.size());
    std::size_t shown = 0;
    for (std::size_t i = 0; i < rows.size() && shown < 3; ++i) {
        if (columnar[i] != rows[i]) {
            ++shown;
            ADD_FAILURE() << "line " << i << " differs\n  columnar: " << columnar[i]
                          << "\n  rows:     " << rows[i];
        }
    }
    EXPECT_EQ(columnar, rows);
}

// Runs the script as planned and on the row carrier: the planned run took
// `carrier` through `producer`, and both wrote the same lines.
void expect_parity(const std::string& ddl,
                   const std::string& insert,
                   const std::string& producer,
                   const fs::path& dir,
                   Carrier carrier) {
    expect_columnar_plan(ddl, insert, producer);
    if (::testing::Test::HasFatalFailure()) {
        return;
    }
    const auto columnar = parity_run(ddl, insert, dir / "out.ndjson");
    const auto rows =
        with_columnar_output_off([&] { return parity_run(ddl, insert, dir / "out.ndjson"); });
    EXPECT_EQ(rows.decoded, 0U) << "the row-carrier run decoded a columnar batch";
    EXPECT_EQ(rows.bails, 0U) << "the row-carrier run began an emission columnar";
    if (carrier == Carrier::Columnar) {
        EXPECT_GT(columnar.decoded, 0U)
            << "no columnar batch was decoded, so the producer never took the columnar carrier";
        EXPECT_EQ(columnar.bails, 0U)
            << "an emission of values the sidecar holds exactly bailed to row form";
    } else {
        EXPECT_EQ(columnar.decoded, 0U)
            << "a columnar batch was decoded, so an emission the sidecar cannot hold exactly "
               "rode it";
        EXPECT_GT(columnar.bails, 0U)
            << "nothing bailed, so the row form came from something other than the cell check";
    }
    expect_same_lines(columnar.lines, rows.lines);
}

std::string parity_file_table(const std::string& name,
                              const std::string& columns,
                              const fs::path& path) {
    return "CREATE TABLE " + name + " " + columns +
           " WITH (connector='file', format='json', path='" + path.string() + "');";
}

// The shared-type columns: every one has a typed column in the sidecar, which
// holds its values exactly.
constexpr const char* kSharedColumns = "(id BIGINT, i INTEGER, x DOUBLE, ok BOOLEAN, v VARCHAR)";

std::string parity_shared_line(int i, bool v_as_number = false) {
    std::ostringstream l;
    l << R"({"id":)" << i << R"(,"i":)" << (i * 7) << R"(,"x":)" << i << ".25"
      << R"(,"ok":)" << (i % 2 == 0 ? "true" : "false") << R"(,"v":)";
    if (v_as_number) {
        l << i;
    } else {
        l << R"("v)" << i << R"(")";
    }
    l << "}";
    return l.str();
}

std::string parity_shared_join_ddl(const fs::path& dir, int number_in_varchar_at = 0) {
    std::vector<std::string> src;
    std::vector<std::string> keys;
    for (int i = 1; i <= kParityRows; ++i) {
        src.push_back(parity_shared_line(i, i == number_in_varchar_at));
        keys.push_back(R"({"k":)" + std::to_string(i) + "}");
    }
    parity_write_lines(dir / "src.ndjson", src);
    parity_write_lines(dir / "keys.ndjson", keys);
    return parity_file_table("src", kSharedColumns, dir / "src.ndjson") +
           parity_file_table("keys", "(k BIGINT)", dir / "keys.ndjson") +
           parity_file_table("out", kSharedColumns, dir / "out.ndjson");
}

const std::string kSharedJoin =
    "INSERT INTO out SELECT a.id, a.i, a.x, a.ok, a.v FROM src a JOIN keys b ON a.id = b.k;";

// A window source: kWindows tumbling windows of 10 s, kGroups groups in each,
// two records per group, then one record past the last window so the
// watermark fires them all. `key_of(g)` renders group g's key fields.
std::string parity_window_source(const fs::path& path,
                                 const std::function<std::string(int)>& key_of,
                                 const std::string& tail_keys) {
    constexpr int kWindows = 4;
    constexpr int kGroups = 96;
    std::vector<std::string> lines;
    for (int w = 0; w < kWindows; ++w) {
        for (int g = 0; g < kGroups; ++g) {
            for (int n = 0; n < 2; ++n) {
                lines.push_back(R"({"ts":)" + std::to_string((w * 10000) + 1000 + g) + "," +
                                key_of(g) + "}");
            }
        }
    }
    lines.push_back(R"({"ts":100000,)" + tail_keys + "}");
    parity_write_lines(path, lines);
    return path.string();
}

// Final value per group from a changelog the file sink appended in order: the
// last line written for each group key.
std::map<std::string, std::string> parity_last_per_group(const fs::path& path,
                                                         const std::string& key) {
    std::map<std::string, std::string> out;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        const auto v = clink::config::parse(line);
        out[v.as_object().at(key).serialize(0)] = line;
    }
    return out;
}

}  // namespace

// --- Every type the sidecar stores as text ------------------------------------

// An inner equi-join whose output carries TIMESTAMP(3), SMALLINT, DATE, BIGINT
// ARRAY, MAP and ROW columns, then the projection the SELECT list puts after
// it, which ingests columnar, into a JSON file. None of those values has an
// exact cell in the sidecar, so every emission must take the row form.
TEST(BornColumnarParity, JoinOutputOfTextStoredTypesMatchesTheRowCarrier) {
    const auto dir = parity_scratch("join");
    const std::string ddl =
        parity_join_ddl(dir) +
        parity_file_table("out",
                          "(id BIGINT, t TIMESTAMP(3), s SMALLINT, d DATE, arr BIGINT[], "
                          "m MAP<VARCHAR, VARCHAR>, r ROW<a BIGINT, b VARCHAR>)",
                          dir / "out.ndjson");
    expect_parity(ddl,
                  "INSERT INTO out SELECT a.id, a.t, a.s, a.d, a.arr, a.m, a.r "
                  "FROM src a JOIN keys b ON a.id = b.k;",
                  "equi_join_row",
                  dir,
                  Carrier::Rows);
    fs::remove_all(dir);
}

// The join's output grouped on its TIMESTAMP column. An aggregate keyed on the
// text form emits the key as a string where one keyed on the number emits the
// number. The aggregate folds a columnar batch without decoding it and emits
// per batch rather than per row, so the comparison is the final value of each
// group, not the changelog.
TEST(BornColumnarParity, JoinOutputGroupedOnATimestampMatchesTheRowCarrier) {
    const auto dir = parity_scratch("join_group");
    const std::string ddl =
        parity_join_ddl(dir) +
        parity_file_table("out", "(t TIMESTAMP(3), c BIGINT)", dir / "out.ndjson");
    const std::string insert =
        "INSERT INTO out SELECT a.t, COUNT(*) AS c FROM src a JOIN keys b ON a.id = b.k "
        "GROUP BY a.t;";
    expect_columnar_plan(ddl, insert, "equi_join_row");
    if (HasFatalFailure()) {
        return;
    }
    (void)parity_run(ddl, insert, dir / "out.ndjson");
    const auto columnar = parity_last_per_group(dir / "out.ndjson", "t");
    (void)with_columnar_output_off([&] { return parity_run(ddl, insert, dir / "out.ndjson"); });
    const auto rows = parity_last_per_group(dir / "out.ndjson", "t");
    ASSERT_EQ(rows.size(), 37U) << "one group per distinct t in the fixture";
    EXPECT_EQ(columnar.size(), rows.size());
    EXPECT_EQ(columnar, rows);
    fs::remove_all(dir);
}

// A tumbling window grouped on TIMESTAMP(3), SMALLINT and DATE keys, with more
// than 64 groups in every window so the fire is big enough to keep the
// columnar carrier, then the HAVING filter, which ingests columnar, into a JSON
// file. No key has an exact cell in the sidecar, so every fire takes the row
// form.
TEST(BornColumnarParity, WindowKeyedOnTextStoredTypesMatchesTheRowCarrier) {
    const auto dir = parity_scratch("window");
    const auto src = parity_window_source(
        dir / "src.ndjson",
        [](int g) {
            return R"("t":)" + std::to_string(kBaseMs + (static_cast<std::int64_t>(g) * 1000)) +
                   R"(,"s":)" + std::to_string(g) + R"(,"d":)" + std::to_string(kBaseDay + g);
        },
        R"("t":0,"s":0,"d":0)");
    const std::string ddl =
        "CREATE TABLE src (ts BIGINT, t TIMESTAMP(3), s SMALLINT, d DATE) WITH ("
        "connector='file', format='json', path='" +
        src + "', event_time_column='ts', watermark_lag_ms='0');" +
        parity_file_table(
            "out", "(t TIMESTAMP(3), s SMALLINT, d DATE, c BIGINT)", dir / "out.ndjson");
    expect_parity(ddl,
                  "INSERT INTO out SELECT t, s, d, COUNT(*) AS c FROM src "
                  "GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), t, s, d HAVING COUNT(*) > 1;",
                  "tumbling_window_row",
                  dir,
                  Carrier::Rows);
    fs::remove_all(dir);
}

// --- Types the sidecar holds exactly ------------------------------------------

// The same join over BIGINT, INTEGER, DOUBLE, BOOLEAN and VARCHAR, whose values
// the sidecar holds exactly: the columnar carrier stays, and agrees.
TEST(BornColumnarParity, JoinOutputOfSharedTypesStaysColumnarAndMatches) {
    const auto dir = parity_scratch("join_shared");
    expect_parity(
        parity_shared_join_ddl(dir), kSharedJoin, "equi_join_row", dir, Carrier::Columnar);
    fs::remove_all(dir);
}

// The same window keyed on BIGINT and VARCHAR: the columnar carrier stays.
TEST(BornColumnarParity, WindowKeyedOnSharedTypesStaysColumnarAndMatches) {
    const auto dir = parity_scratch("window_shared");
    const auto src = parity_window_source(
        dir / "src.ndjson",
        [](int g) {
            return R"("k":)" + std::to_string(g * 3) + R"(,"v":"v)" + std::to_string(g) + R"(")";
        },
        R"("k":0,"v":"v0")");
    const std::string ddl =
        "CREATE TABLE src (ts BIGINT, k BIGINT, v VARCHAR) WITH (connector='file', "
        "format='json', path='" +
        src + "', event_time_column='ts', watermark_lag_ms='0');" +
        parity_file_table("out", "(k BIGINT, v VARCHAR, c BIGINT)", dir / "out.ndjson");
    expect_parity(ddl,
                  "INSERT INTO out SELECT k, v, COUNT(*) AS c FROM src "
                  "GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k, v HAVING COUNT(*) > 1;",
                  "tumbling_window_row",
                  dir,
                  Carrier::Columnar);
    fs::remove_all(dir);
}

// --- One inexact cell among exact ones -----------------------------------------

// A VARCHAR column holding a JSON number in one row. The row carrier keeps the
// number; the sidecar's text column would turn it into a string. That one cell
// sends its emission to row form, and the output still agrees.
TEST(BornColumnarParity, JoinRowWithANumberInAVarcharColumnMatchesTheRowCarrier) {
    const auto dir = parity_scratch("join_mixed");
    const std::string ddl = parity_shared_join_ddl(dir, kParityRows / 2);
    expect_columnar_plan(ddl, kSharedJoin, "equi_join_row");
    if (HasFatalFailure()) {
        return;
    }
    const auto columnar = parity_run(ddl, kSharedJoin, dir / "out.ndjson");
    const auto rows =
        with_columnar_output_off([&] { return parity_run(ddl, kSharedJoin, dir / "out.ndjson"); });
    EXPECT_GT(columnar.bails, 0U) << "the number in the VARCHAR column did not bail the join";
    expect_same_lines(columnar.lines, rows.lines);
    fs::remove_all(dir);
}

// The window's version: one group's VARCHAR key is a JSON number.
TEST(BornColumnarParity, WindowGroupWithANumberInAVarcharKeyMatchesTheRowCarrier) {
    const auto dir = parity_scratch("window_mixed");
    const auto src = parity_window_source(
        dir / "src.ndjson",
        [](int g) {
            if (g == 40) {
                return std::string(R"("k":1,"v":40)");
            }
            return R"("k":1,"v":"v)" + std::to_string(g) + R"(")";
        },
        R"("k":1,"v":"v0")");
    const std::string ddl =
        "CREATE TABLE src (ts BIGINT, k BIGINT, v VARCHAR) WITH (connector='file', "
        "format='json', path='" +
        src + "', event_time_column='ts', watermark_lag_ms='0');" +
        parity_file_table("out", "(k BIGINT, v VARCHAR, c BIGINT)", dir / "out.ndjson");
    const std::string insert =
        "INSERT INTO out SELECT k, v, COUNT(*) AS c FROM src "
        "GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k, v HAVING COUNT(*) > 1;";
    expect_columnar_plan(ddl, insert, "tumbling_window_row");
    if (HasFatalFailure()) {
        return;
    }
    const auto columnar = parity_run(ddl, insert, dir / "out.ndjson");
    const auto rows =
        with_columnar_output_off([&] { return parity_run(ddl, insert, dir / "out.ndjson"); });
    EXPECT_GT(columnar.bails, 0U) << "the number in the VARCHAR key did not bail the fire";
    expect_same_lines(columnar.lines, rows.lines);
    fs::remove_all(dir);
}

// --- The check itself -----------------------------------------------------------

namespace {

using clink::config::JsonValue;
using clink::sql::row_columnar_detail::cell_is_exact;
using clink::sql::row_columnar_detail::effective_type;

// What a row consumer reads back for `v` from a column of declared type
// `declared`: append_json_cell, then read_cell.
JsonValue parity_round_trip(const std::shared_ptr<arrow::DataType>& declared, const JsonValue& v) {
    const auto eff = effective_type(declared);
    auto b = clink::sql::row_columnar_detail::make_cell_builder(eff);
    clink::sql::row_columnar_detail::append_json_cell(*b, *eff, &v);
    std::shared_ptr<arrow::Array> arr;
    EXPECT_TRUE(b->Finish(&arr).ok());
    return clink::sql::row_columnar_detail::read_cell(eff, *arr, 0);
}

JsonValue parity_json(const std::string& text) {
    return clink::config::parse(text);
}

}  // namespace

// Every value the check calls exact reads back as itself, kind and text; every
// value it calls inexact does not, so the check is neither too strict nor too
// loose on these.
TEST(BornColumnarParity, CellIsExactAgreesWithTheRoundTrip) {
    struct Case {
        std::shared_ptr<arrow::DataType> declared;
        JsonValue value;
        bool exact;
    };
    const auto dec = arrow::decimal128(10, 2);
    const std::vector<Case> cases = {
        // Typed columns.
        {arrow::int64(), JsonValue{std::int64_t{9007199254740993}}, true},
        {arrow::int64(), JsonValue{5.0}, true},
        {arrow::int64(), JsonValue{5.5}, false},
        {arrow::int64(), JsonValue{std::string("5")}, false},
        {arrow::int32(), JsonValue{std::int64_t{2147483647}}, true},
        {arrow::int32(), JsonValue{std::int64_t{2147483648}}, false},
        {arrow::float64(), JsonValue{0.1}, true},
        {arrow::float64(), JsonValue{std::int64_t{9007199254740992}}, true},
        {arrow::float64(), JsonValue{std::int64_t{9007199254740993}}, false},
        {arrow::float32(), JsonValue{0.5}, true},
        {arrow::float32(), JsonValue{0.1}, false},
        {arrow::boolean(), JsonValue{true}, true},
        {arrow::boolean(), JsonValue{std::int64_t{1}}, false},
        {dec, clink::config::make_dec_value(*clink::config::dec_parse("12.34")), true},
        {dec, clink::config::make_dec_value(*clink::config::dec_parse("12.3")), false},
        {dec, JsonValue{12.34}, false},
        {arrow::list(arrow::float32()), parity_json("[0.5, 1, 2.25]"), true},
        {arrow::list(arrow::float32()), parity_json("[0.1]"), false},
        // Stored as text.
        {arrow::utf8(), JsonValue{std::string("v1")}, true},
        {arrow::utf8(), JsonValue{std::int64_t{7}}, false},
        {arrow::utf8(), clink::config::make_dec_value(*clink::config::dec_parse("1.5")), false},
        {arrow::timestamp(arrow::TimeUnit::MILLI), JsonValue{std::int64_t{kBaseMs}}, false},
        {arrow::timestamp(arrow::TimeUnit::MILLI),
         JsonValue{std::string("2023-11-14 22:13:20.000")},
         true},
        {arrow::int16(), JsonValue{std::int64_t{3}}, false},
        {arrow::date32(), JsonValue{std::int64_t{kBaseDay}}, false},
        {arrow::list(arrow::int64()), parity_json("[1, 2]"), false},
        {arrow::map(arrow::utf8(), arrow::utf8()), parity_json(R"({"x":"v"})"), false},
        {arrow::utf8(), JsonValue{false}, false},
    };
    for (const auto& c : cases) {
        const auto eff = effective_type(c.declared);
        EXPECT_EQ(cell_is_exact(*eff, &c.value), c.exact)
            << c.declared->ToString() << " " << c.value.serialize(0);
        const auto back = parity_round_trip(c.declared, c.value);
        const bool same =
            back.type() == c.value.type() || (back.is_number() && c.value.is_number());
        EXPECT_EQ(same && back.serialize(0) == c.value.serialize(0), c.exact)
            << c.declared->ToString() << " " << c.value.serialize(0) << " reads back as "
            << back.serialize(0);
    }
    // A null, absent or present, is exact in every layout.
    const JsonValue null_value;
    EXPECT_TRUE(cell_is_exact(*arrow::utf8(), nullptr));
    EXPECT_TRUE(cell_is_exact(*arrow::int64(), &null_value));
}

// try_append_row checks the whole row before it appends any of it: a row with
// one inexact cell leaves the builder exactly as it was, and the rows around it
// still make a well-formed batch.
TEST(BornColumnarParity, TryAppendRowAppendsAllOrNothing) {
    clink::sql::RowColumnarOutput out({{"id", arrow::int64()}, {"t", arrow::utf8()}});
    const JsonValue id1{std::int64_t{1}};
    const JsonValue id2{std::int64_t{2}};
    const JsonValue text{std::string("2023-11-14")};
    const JsonValue millis{std::int64_t{kBaseMs}};

    EXPECT_TRUE(out.try_append_row({&id1, &text}));
    EXPECT_FALSE(out.try_append_row({&id2, &millis}))
        << "a number in a text column reads back as a string";
    EXPECT_EQ(out.rows(), 1U);
    EXPECT_FALSE(out.try_append_row({&id2})) << "a row of the wrong width";
    EXPECT_TRUE(out.try_append_row({&id2, nullptr}));
    EXPECT_EQ(out.rows(), 2U);

    const auto rb = out.finish();
    ASSERT_NE(rb, nullptr) << "a refused row must not shear the columns";
    ASSERT_EQ(rb->num_rows(), 2);
    const auto& ids = static_cast<const arrow::Int64Array&>(*rb->GetColumnByName("id"));
    EXPECT_EQ(ids.Value(0), 1);
    EXPECT_EQ(ids.Value(1), 2);
    EXPECT_TRUE(rb->GetColumnByName("t")->IsNull(1));
}

// A REAL the source did not round to float precision: the file source keeps
// 0.1 as the double it parsed, which a float32 column would hand back as
// 0.10000000149011612. Every row has one, so every pair takes the row path.
TEST(BornColumnarParity, JoinOutputOfARealOffFloatPrecisionMatchesTheRowCarrier) {
    const auto dir = parity_scratch("join_real");
    std::vector<std::string> src;
    std::vector<std::string> keys;
    for (int i = 1; i <= kParityRows; ++i) {
        src.push_back(R"({"id":)" + std::to_string(i) + R"(,"r":)" + std::to_string(i) + ".1}");
        keys.push_back(R"({"k":)" + std::to_string(i) + "}");
    }
    parity_write_lines(dir / "src.ndjson", src);
    parity_write_lines(dir / "keys.ndjson", keys);
    const std::string ddl = parity_file_table("src", "(id BIGINT, r REAL)", dir / "src.ndjson") +
                            parity_file_table("keys", "(k BIGINT)", dir / "keys.ndjson") +
                            parity_file_table("out", "(id BIGINT, r REAL)", dir / "out.ndjson");
    expect_parity(ddl,
                  "INSERT INTO out SELECT a.id, a.r FROM src a JOIN keys b ON a.id = b.k;",
                  "equi_join_row",
                  dir,
                  Carrier::Rows);
    fs::remove_all(dir);
}
