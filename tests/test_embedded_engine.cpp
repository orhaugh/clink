// EmbeddedEngine end-to-end: the whole runtime in one process (in-process
// coordinator + worker over loopback) driving the shared SQL script runner - the
// execution core behind `clink run <file>.sql`. Also covers the
// script-runner's bare-SELECT-to-print synthesis and the print sink's
// stdout output (captured at the fd level, since sink subtasks write from
// runner threads).

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include "clink/config/json.hpp"
#include "clink/embed/embedded_engine.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/script_runner.hpp"

namespace {

namespace fs = std::filesystem;

void write_lines(const fs::path& path, const std::vector<std::string>& lines) {
    std::ofstream out(path, std::ios::trunc);
    for (const auto& l : lines) {
        out << l << "\n";
    }
}

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

// Redirect fd 1 to a file for the scope, restoring after. fd-level (not
// std::cout rdbuf) because the print sink fwrite()s to stdout from runner
// threads. Keep gtest assertions OUT of the captured window - a failure
// message would land in the capture file.
class CaptureStdoutToFile {
public:
    explicit CaptureStdoutToFile(const fs::path& path) {
        std::fflush(stdout);
        saved_ = dup(STDOUT_FILENO);
        const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        dup2(fd, STDOUT_FILENO);
        ::close(fd);
    }
    ~CaptureStdoutToFile() { restore(); }
    void restore() {
        if (saved_ < 0) {
            return;
        }
        std::fflush(stdout);
        dup2(saved_, STDOUT_FILENO);
        ::close(saved_);
        saved_ = -1;
    }

private:
    int saved_{-1};
};

std::string orders_ddl(const fs::path& in_path) {
    return std::string{
               "CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
               "WITH (connector='file', format='json', path='"} +
           in_path.string() + "');";
}

void write_orders(const fs::path& in_path) {
    write_lines(in_path,
                {R"({"user_id":1,"amount":10})",
                 R"({"user_id":2,"amount":20})",
                 R"({"user_id":1,"amount":30})",
                 R"({"user_id":2,"amount":5})",
                 R"({"user_id":1,"amount":7})"});
}

TEST(EmbeddedEngine, BoundedFileToFilePipelineRuns) {
    const auto in_path = fs::temp_directory_path() / "clink_embed_gb_in.ndjson";
    const auto out_path = fs::temp_directory_path() / "clink_embed_gb_out.ndjson";
    fs::remove(in_path);
    fs::remove(out_path);
    write_orders(in_path);

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    const std::string script =
        orders_ddl(in_path) +
        "CREATE TABLE out_t (uid BIGINT, total BIGINT) "
        "WITH (connector='file', format='json', path='" +
        out_path.string() +
        "');"
        "INSERT INTO out_t SELECT user_id AS uid, SUM(amount) AS total FROM orders "
        "GROUP BY user_id";
    ASSERT_EQ(engine.execute_script(script), 0) << err.str();
    ASSERT_EQ(engine.job_count(), 1u);
    ASSERT_TRUE(engine.await_all()) << err.str();

    // Unbounded GROUP BY emits the running total per input row; the last
    // emit per key is the final answer.
    std::map<std::int64_t, std::int64_t> final_by_uid;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        ASSERT_TRUE(js.is_object()) << l;
        final_by_uid[static_cast<std::int64_t>(js.at("uid").as_number())] =
            static_cast<std::int64_t>(js.at("total").as_number());
    }
    EXPECT_EQ(final_by_uid[1], 47);
    EXPECT_EQ(final_by_uid[2], 25);
    fs::remove(in_path);
    fs::remove(out_path);
}

TEST(EmbeddedEngine, BareSelectPrintsRowsToStdout) {
    const auto in_path = fs::temp_directory_path() / "clink_embed_sel_in.ndjson";
    const auto cap_path = fs::temp_directory_path() / "clink_embed_sel_stdout.txt";
    fs::remove(in_path);
    fs::remove(cap_path);
    write_orders(in_path);

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    const std::string script = orders_ddl(in_path) + "SELECT user_id, amount FROM orders";
    int rc = -1;
    bool ok = false;
    {
        CaptureStdoutToFile cap(cap_path);
        rc = engine.execute_script(script);
        ok = (rc == 0) && engine.await_all();
    }
    ASSERT_EQ(rc, 0) << err.str();
    ASSERT_TRUE(ok) << err.str();

    const auto lines = read_lines(cap_path);
    ASSERT_EQ(lines.size(), 5u);
    std::int64_t amount_sum = 0;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        ASSERT_TRUE(js.is_object()) << l;
        amount_sum += static_cast<std::int64_t>(js.at("amount").as_number());
    }
    EXPECT_EQ(amount_sum, 72);
    fs::remove(in_path);
    fs::remove(cap_path);
}

TEST(EmbeddedEngine, BareSelectSingleColumnRunsOnRowChannel) {
    // A one-column projection must not be mistaken for the single-TEXT-column
    // string channel: the synthesised sink follows the plan's channel (it
    // carries format='json'), so `SELECT amount FROM orders` runs rather than
    // failing with a source/sink channel mismatch.
    const auto in_path = fs::temp_directory_path() / "clink_embed_onecol_in.ndjson";
    const auto cap_path = fs::temp_directory_path() / "clink_embed_onecol_stdout.txt";
    fs::remove(in_path);
    fs::remove(cap_path);
    write_orders(in_path);

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    const std::string script = orders_ddl(in_path) + "SELECT amount FROM orders";
    int rc = -1;
    bool ok = false;
    {
        CaptureStdoutToFile cap(cap_path);
        rc = engine.execute_script(script);
        ok = (rc == 0) && engine.await_all();
    }
    ASSERT_EQ(rc, 0) << err.str();
    ASSERT_TRUE(ok) << err.str();

    const auto lines = read_lines(cap_path);
    ASSERT_EQ(lines.size(), 5u);
    std::int64_t amount_sum = 0;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        ASSERT_TRUE(js.is_object()) << l;
        amount_sum += static_cast<std::int64_t>(js.at("amount").as_number());
    }
    EXPECT_EQ(amount_sum, 72);
    fs::remove(in_path);
    fs::remove(cap_path);
}

TEST(EmbeddedEngine, ChangelogSelectPrintsKindPrefixes) {
    // TOP-1 per user via ROW_NUMBER produces a changelog (displaced rows
    // retract); the binder admits connector='print' for changelog SELECTs
    // and the sink prefixes non-insert kinds. user 1's top amount improves
    // 10 -> 30, so at least one retraction must appear alongside plain
    // insert lines.
    const auto in_path = fs::temp_directory_path() / "clink_embed_topn_in.ndjson";
    const auto cap_path = fs::temp_directory_path() / "clink_embed_topn_stdout.txt";
    fs::remove(in_path);
    fs::remove(cap_path);
    write_orders(in_path);

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    const std::string script =
        orders_ddl(in_path) +
        "SELECT * FROM ("
        "  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rn "
        "  FROM orders) ranked WHERE rn <= 1";
    int rc = -1;
    bool ok = false;
    {
        CaptureStdoutToFile cap(cap_path);
        rc = engine.execute_script(script);
        ok = (rc == 0) && engine.await_all();
    }
    ASSERT_EQ(rc, 0) << err.str();
    ASSERT_TRUE(ok) << err.str();

    const auto lines = read_lines(cap_path);
    ASSERT_FALSE(lines.empty());
    bool saw_retraction = false;
    bool saw_plain_insert = false;
    for (const auto& l : lines) {
        if (l.starts_with("-D ") || l.starts_with("-U ") || l.starts_with("+U ")) {
            saw_retraction = true;
            // The prefixed remainder must still be a JSON object with the
            // marker field stripped.
            auto js = clink::config::parse(l.substr(3));
            EXPECT_TRUE(js.is_object()) << l;
            EXPECT_EQ(js.as_object().count("__row_kind"), 0u) << l;
        } else {
            saw_plain_insert = true;
            auto js = clink::config::parse(l);
            EXPECT_TRUE(js.is_object()) << l;
            EXPECT_EQ(js.as_object().count("__row_kind"), 0u) << l;
        }
    }
    EXPECT_TRUE(saw_retraction);
    EXPECT_TRUE(saw_plain_insert);
    fs::remove(in_path);
    fs::remove(cap_path);
}

TEST(EmbeddedEngine, CollectReaderDeliversTypedBatchesAndEos) {
    const auto in_path = fs::temp_directory_path() / "clink_embed_collect_in.ndjson";
    fs::remove(in_path);
    write_orders(in_path);

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    // DDL first so the collect table is in the catalog...
    ASSERT_EQ(engine.execute_script(orders_ddl(in_path) +
                                    "CREATE TABLE results (user_id BIGINT, amount BIGINT) "
                                    "WITH (connector='collect')"),
              0)
        << err.str();

    // ...then the reader, requested BEFORE the producing job exists: valid,
    // blocks until batches arrive.
    auto reader_r = engine.collect_reader("results");
    ASSERT_TRUE(reader_r.ok()) << reader_r.status().ToString();
    auto reader = *reader_r;
    ASSERT_EQ(reader->schema()->num_fields(), 2);
    EXPECT_EQ(reader->schema()->field(0)->name(), "user_id");
    EXPECT_TRUE(reader->schema()->field(0)->type()->Equals(arrow::int64()));
    EXPECT_TRUE(reader->schema()->field(1)->type()->Equals(arrow::int64()));

    // One consumer per table.
    auto second = engine.collect_reader("results");
    EXPECT_FALSE(second.ok());

    ASSERT_EQ(engine.execute_script("INSERT INTO results SELECT user_id, amount FROM orders"), 0)
        << err.str();

    std::int64_t rows = 0;
    std::int64_t amount_sum = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto st = reader->ReadNext(&batch);
        ASSERT_TRUE(st.ok()) << st.ToString();
        if (!batch) {
            break;  // end of stream: the bounded job's sinks closed
        }
        ASSERT_TRUE(batch->schema()->Equals(*reader->schema()));
        rows += batch->num_rows();
        const auto& amounts = static_cast<const arrow::Int64Array&>(*batch->column(1));
        for (std::int64_t i = 0; i < amounts.length(); ++i) {
            amount_sum += amounts.Value(i);
        }
    }
    EXPECT_EQ(rows, 5);
    EXPECT_EQ(amount_sum, 72);
    EXPECT_TRUE(engine.await_all()) << err.str();
    fs::remove(in_path);
}

// The collect reader's schema is built from the declared columns, the collect
// sink's batches from the planned `schema_columns`. For every SQL type the two
// must agree, or a host reading the stream sees batches whose schema is not the
// one it was promised. Both go through the V1 row layout, so TIMESTAMP and
// TIMESTAMPTZ are text on both sides whatever the reader learns to carry.
TEST(EmbeddedEngine, CollectReaderSchemaMatchesTheSinksBatchesForEverySqlType) {
    const auto in_path = fs::temp_directory_path() /
                         ("clink_embed_collect_types_" + std::to_string(::getpid()) + ".ndjson");
    write_lines(in_path,
                {R"({"b":1,"i":2,"s":3,"o":true,"r":1.5,"f":2.5,"v":"x","t":"y",)"
                 R"("ts0":1700000000000,"ts3":1700000000123,"ts6":"2023-11-14 22:13:20",)"
                 R"("ts9":0,"tsd":1,"tz":1700000000123,"d":"2023-11-14","tm":"22:13:20",)"
                 R"("dec":"12.34","raw":"AQI=","emb":[0.5,1.5],"arr":[1,2],"m":{"k":1},)"
                 R"("w":{"x":5,"y":"q"}})"});
    const std::string columns =
        "(b BIGINT, i INT, s SMALLINT, o BOOLEAN, r REAL, f DOUBLE, v VARCHAR, t TEXT, "
        "ts0 TIMESTAMP(0), ts3 TIMESTAMP(3), ts6 TIMESTAMP(6), ts9 TIMESTAMP(9), tsd TIMESTAMP, "
        "tz TIMESTAMP(3) WITH TIME ZONE, d DATE, tm TIME, dec DECIMAL(10, 2), raw BYTEA, "
        "emb REAL[], arr BIGINT[], m MAP<VARCHAR, BIGINT>, w ROW<x BIGINT, y VARCHAR>)";

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    ASSERT_EQ(engine.execute_script("CREATE TABLE src_all " + columns +
                                    " WITH (connector='file', format='json', path='" +
                                    in_path.string() + "');" + "CREATE TABLE all_types " + columns +
                                    " WITH (connector='collect')"),
              0)
        << err.str();
    auto reader_r = engine.collect_reader("all_types");
    ASSERT_TRUE(reader_r.ok()) << reader_r.status().ToString();
    auto reader = *reader_r;
    ASSERT_EQ(reader->schema()->num_fields(), 22);
    // The text fallback for the types the layout does not carry.
    for (const char* text :
         {"s", "ts0", "ts3", "ts6", "ts9", "tsd", "tz", "d", "tm", "raw", "arr"}) {
        EXPECT_EQ(reader->schema()->GetFieldByName(text)->type()->id(), arrow::Type::STRING)
            << text;
    }

    ASSERT_EQ(engine.execute_script("INSERT INTO all_types SELECT * FROM src_all"), 0) << err.str();
    std::int64_t rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto st = reader->ReadNext(&batch);
        ASSERT_TRUE(st.ok()) << st.ToString();
        if (!batch) {
            break;
        }
        EXPECT_TRUE(batch->schema()->Equals(*reader->schema()))
            << "batch:\n"
            << batch->schema()->ToString() << "\nreader:\n"
            << reader->schema()->ToString();
        rows += batch->num_rows();
    }
    EXPECT_EQ(rows, 1);
    EXPECT_TRUE(engine.await_all()) << err.str();
    fs::remove(in_path);
}

TEST(EmbeddedEngine, CollectChangelogStreamsRowKinds) {
    // connector='collect' with changelog='true' accepts a retracting SELECT
    // and prepends a row_kind utf8 column to the Arrow stream. TOP-1 per
    // user: user 1's best improves 10 -> 30, so retractions must appear,
    // and applying the changelog reconstructs the final TOP-1 relation.
    const auto in_path = fs::temp_directory_path() / "clink_embed_collect_clog_in.ndjson";
    fs::remove(in_path);
    write_orders(in_path);

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    ASSERT_EQ(engine.execute_script(orders_ddl(in_path) +
                                    "CREATE TABLE topn (user_id BIGINT, amount BIGINT) "
                                    "WITH (connector='collect', changelog='true')"),
              0)
        << err.str();

    auto reader_r = engine.collect_reader("topn");
    ASSERT_TRUE(reader_r.ok()) << reader_r.status().ToString();
    auto reader = *reader_r;
    ASSERT_EQ(reader->schema()->num_fields(), 3);
    EXPECT_EQ(reader->schema()->field(0)->name(), "row_kind");
    EXPECT_TRUE(reader->schema()->field(0)->type()->Equals(arrow::utf8()));
    EXPECT_EQ(reader->schema()->field(1)->name(), "user_id");

    ASSERT_EQ(engine.execute_script(
                  "INSERT INTO topn SELECT user_id, amount FROM ("
                  "  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) AS "
                  "rn FROM orders) ranked WHERE rn <= 1"),
              0)
        << err.str();

    // Apply the changelog: add on insert/update_after, remove on
    // delete/update_before. The surviving relation is the final TOP-1.
    std::map<std::pair<std::int64_t, std::int64_t>, int> relation;  // (user, amount) -> count
    bool saw_retraction = false;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto st = reader->ReadNext(&batch);
        ASSERT_TRUE(st.ok()) << st.ToString();
        if (!batch) {
            break;
        }
        ASSERT_TRUE(batch->schema()->Equals(*reader->schema()));
        const auto& kinds = static_cast<const arrow::StringArray&>(*batch->column(0));
        const auto& users = static_cast<const arrow::Int64Array&>(*batch->column(1));
        const auto& amounts = static_cast<const arrow::Int64Array&>(*batch->column(2));
        for (std::int64_t i = 0; i < batch->num_rows(); ++i) {
            const auto kind = kinds.GetString(i);
            const auto key = std::make_pair(users.Value(i), amounts.Value(i));
            if (kind == "insert" || kind == "update_after") {
                ++relation[key];
            } else {
                ASSERT_TRUE(kind == "delete" || kind == "update_before") << kind;
                saw_retraction = true;
                if (--relation[key] == 0) {
                    relation.erase(key);
                }
            }
        }
    }
    EXPECT_TRUE(saw_retraction);
    const std::map<std::pair<std::int64_t, std::int64_t>, int> expected{{{1, 30}, 1}, {{2, 20}, 1}};
    EXPECT_EQ(relation, expected);
    EXPECT_TRUE(engine.await_all()) << err.str();
    fs::remove(in_path);
}

TEST(EmbeddedEngine, CollectRejectsChangelogSelect) {
    // connector='collect' WITHOUT changelog='true' stays append-only: the
    // typed batches carry no changelog kind, so a retracting SELECT must be
    // rejected at bind, not silently flattened into inserts.
    const auto in_path = fs::temp_directory_path() / "clink_embed_collect_cl_in.ndjson";
    fs::remove(in_path);
    write_orders(in_path);

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    const std::string script =
        orders_ddl(in_path) +
        "CREATE TABLE topn (user_id BIGINT, amount BIGINT) "
        "WITH (connector='collect');"
        "INSERT INTO topn SELECT * FROM ("
        "  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rn "
        "  FROM orders) ranked WHERE rn <= 1";
    EXPECT_NE(engine.execute_script(script), 0);
    EXPECT_NE(err.str().find("append-only"), std::string::npos) << err.str();
    fs::remove(in_path);
}

TEST(EmbeddedEngine, PureDdlScriptSubmitsNothing) {
    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    ASSERT_EQ(engine.execute_script("CREATE TABLE t (a BIGINT) "
                                    "WITH (connector='file', format='json', path='/tmp/x')"),
              0)
        << err.str();
    EXPECT_EQ(engine.job_count(), 0u);
    EXPECT_TRUE(engine.await_all());
}

TEST(EmbeddedEngine, CancelWhileRunningReturnsCleanly) {
    // A cancel request racing a bounded job must end cleanly either way:
    // completed before the cancel landed, or cancelled and drained.
    const auto in_path = fs::temp_directory_path() / "clink_embed_cancel_in.ndjson";
    fs::remove(in_path);
    {
        std::ofstream out(in_path, std::ios::trunc);
        for (int i = 0; i < 50'000; ++i) {
            out << R"({"user_id":)" << (i % 100) << R"(,"amount":)" << i << "}\n";
        }
    }

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    const std::string script =
        orders_ddl(in_path) +
        "CREATE TABLE sink_bh (user_id BIGINT, amount BIGINT) WITH (connector='blackhole');"
        "INSERT INTO sink_bh SELECT user_id, amount FROM orders";
    ASSERT_EQ(engine.execute_script(script), 0) << err.str();
    EXPECT_TRUE(engine.await_all([] { return true; })) << err.str();
    fs::remove(in_path);
}

TEST(ScriptRunner, ExplainReportsReplayDeterminism) {
    // The verdict a user needs BEFORE submitting: does a replay reproduce
    // this statement's output? LIMIT without ORDER BY keeps arrival-order
    // rows and must say so; a plain projection and an ORDER BY ... LIMIT
    // (sort-pinned) must read deterministic rather than crying wolf.
    clink::sql::Catalog catalog;
    clink::sql::ScriptRunOptions opts;
    std::ostringstream out;
    std::ostringstream err;
    clink::sql::ScriptIO io{&out, &err};
    auto submit = [](const clink::cluster::JobGraphSpec&, const std::string&) -> int {
        ADD_FAILURE() << "EXPLAIN must not submit";
        return 1;
    };
    const std::string script =
        "CREATE TABLE det_src (k BIGINT, v BIGINT) WITH (connector='kafka', format='json', "
        "brokers='b', topic='t', group_id='g');"
        "CREATE TABLE det_out (k BIGINT, v BIGINT) WITH (connector='blackhole');"
        "EXPLAIN INSERT INTO det_out SELECT k, v FROM det_src LIMIT 5;"
        "EXPLAIN INSERT INTO det_out SELECT k, v FROM det_src;"
        "EXPLAIN INSERT INTO det_out SELECT k, v FROM det_src ORDER BY v LIMIT 3";
    ASSERT_EQ(clink::sql::run_script(script, catalog, opts, io, submit), 0) << err.str();

    const auto text = out.str();
    EXPECT_NE(text.find("replay determinism: NONDETERMINISTIC"), std::string::npos) << text;
    EXPECT_NE(text.find("LIMIT without ORDER BY"), std::string::npos) << text;
    // Exactly one of the three statements is nondeterministic; the other
    // two must both render the clean verdict.
    std::size_t clean = 0;
    for (std::size_t pos = text.find("replay determinism: deterministic"); pos != std::string::npos;
         pos = text.find("replay determinism: deterministic", pos + 1)) {
        ++clean;
    }
    EXPECT_EQ(clean, 2u) << text;
}

TEST(ScriptRunner, BareSelectSynthesisesPrintSinkSpec) {
    const auto in_path = fs::temp_directory_path() / "clink_runner_sel_in.ndjson";
    fs::remove(in_path);
    write_orders(in_path);

    clink::sql::Catalog catalog;
    clink::sql::ScriptRunOptions opts;
    opts.bare_select_to_print = true;
    std::ostringstream out;
    std::ostringstream err;
    clink::sql::ScriptIO io{&out, &err};
    std::vector<clink::cluster::JobGraphSpec> specs;
    auto submit = [&](const clink::cluster::JobGraphSpec& spec, const std::string&) -> int {
        specs.push_back(spec);
        return 0;
    };
    const std::string script = orders_ddl(in_path) + "SELECT user_id, amount FROM orders";
    ASSERT_EQ(clink::sql::run_script(script, catalog, opts, io, submit), 0) << err.str();

    ASSERT_EQ(specs.size(), 1u);
    bool has_print_sink = false;
    for (const auto& op : specs[0].ops) {
        if (op.type == "print_sink_row") {
            has_print_sink = true;
        }
    }
    EXPECT_TRUE(has_print_sink);
    // The synthesised sink table carries the SELECT's output schema.
    const auto* def = catalog.get_table("__stdout_0");
    ASSERT_NE(def, nullptr);
    EXPECT_EQ(def->properties.at("connector"), "print");
    ASSERT_EQ(def->columns.size(), 2u);
    EXPECT_EQ(def->columns[0].name, "user_id");
    EXPECT_EQ(def->columns[1].name, "amount");
    fs::remove(in_path);
}

TEST(ScriptRunner, BareSelectStillRejectedWhenSugarOff) {
    clink::sql::Catalog catalog;
    clink::sql::ScriptRunOptions opts;  // bare_select_to_print defaults false
    std::ostringstream out;
    std::ostringstream err;
    clink::sql::ScriptIO io{&out, &err};
    auto submit = [&](const clink::cluster::JobGraphSpec&, const std::string&) -> int {
        ADD_FAILURE() << "bare SELECT must not compile when the sugar is off";
        return 1;
    };
    const std::string script =
        "CREATE TABLE t (a BIGINT) WITH (connector='file', format='json', path='/tmp/x');"
        "SELECT a FROM t";
    EXPECT_EQ(clink::sql::run_script(script, catalog, opts, io, submit), 1);
    EXPECT_NE(err.str().find("bare SELECT"), std::string::npos) << err.str();
}

}  // namespace

TEST(EmbeddedEngine, ParquetProjectedReadEndToEnd) {
    // json -> parquet (3 columns), then a one-column SELECT back out of
    // the parquet table: the optimizer's projected-columns hint narrows
    // the parquet read to that column (plus nothing else), and the values
    // survive the round trip.
    const auto in_path = fs::temp_directory_path() / "clink_embed_pq_in.ndjson";
    const auto pq_path = fs::temp_directory_path() / "clink_embed_pq.parquet";
    fs::remove_all(in_path);
    fs::remove_all(pq_path);
    write_lines(in_path,
                {R"({"user_id":1,"name":"a","amount":10})",
                 R"({"user_id":2,"name":"b","amount":20})",
                 R"({"user_id":3,"name":"c","amount":30})"});

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    ASSERT_EQ(engine.execute_script("CREATE TABLE evt (user_id BIGINT, name TEXT, amount BIGINT) "
                                    "WITH (connector='file', format='json', path='" +
                                    in_path.string() +
                                    "');"
                                    "CREATE TABLE pq (user_id BIGINT, name TEXT, amount BIGINT) "
                                    "WITH (connector='parquet', path='" +
                                    pq_path.string() +
                                    "');"
                                    "INSERT INTO pq SELECT user_id, name, amount FROM evt"),
              0)
        << err.str();
    ASSERT_TRUE(engine.await_all()) << err.str();

    ASSERT_EQ(engine.execute_script("CREATE TABLE out_amounts (user_id BIGINT, amount BIGINT) "
                                    "WITH (connector='collect');"
                                    "INSERT INTO out_amounts SELECT user_id, amount FROM pq"),
              0)
        << err.str();
    auto reader = engine.collect_reader("out_amounts").ValueOrDie();
    std::int64_t sum = 0;
    std::int64_t rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_TRUE(reader->ReadNext(&batch).ok());
        if (!batch) {
            break;
        }
        const auto& amounts = static_cast<const arrow::Int64Array&>(*batch->column(1));
        for (std::int64_t i = 0; i < amounts.length(); ++i) {
            sum += amounts.Value(i);
            ++rows;
        }
    }
    EXPECT_EQ(rows, 3);
    EXPECT_EQ(sum, 60);
    EXPECT_TRUE(engine.await_all()) << err.str();
    fs::remove_all(in_path);
    fs::remove_all(pq_path);
}

TEST(EmbeddedEngine, CreateFunctionLanguageSqlRunsEndToEnd) {
    // Expression-bodied scalar UDF: declared in SQL, interpreted by the
    // engine's own expression evaluator, usable like any function.
    const auto in_path = fs::temp_directory_path() / "clink_embed_udf_in.ndjson";
    const auto out_path = fs::temp_directory_path() / "clink_embed_udf_out.ndjson";
    fs::remove(in_path);
    fs::remove(out_path);
    write_orders(in_path);

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    ASSERT_EQ(engine.execute_script(
                  "CREATE OR REPLACE FUNCTION with_tax(amount BIGINT) RETURNS BIGINT "
                  "AS 'amount + amount / 10' LANGUAGE SQL;" +
                  orders_ddl(in_path) +
                  "CREATE TABLE taxed (user_id BIGINT, total BIGINT) "
                  "WITH (connector='file', format='json', path='" +
                  out_path.string() +
                  "');"
                  "INSERT INTO taxed SELECT user_id, with_tax(amount) AS total FROM orders"),
              0)
        << err.str();
    ASSERT_TRUE(engine.await_all()) << err.str();

    const auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 5u);
    std::int64_t sum = 0;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        sum += static_cast<std::int64_t>(js.at("total").as_number());
    }
    // amounts 10,20,30,5,7 -> with_tax: 11,22,33,5,7 (integer division) = 78
    EXPECT_EQ(sum, 78);

    // A body referencing an unnamed/unknown parameter fails loudly.
    std::ostringstream err2;
    clink::embed::EngineOptions opts2;
    opts2.err = &err2;
    clink::embed::EmbeddedEngine engine2{std::move(opts2)};
    EXPECT_NE(engine2.execute_script("CREATE FUNCTION broken(x BIGINT) RETURNS BIGINT "
                                     "AS 'y * 2' LANGUAGE SQL"),
              0);
    EXPECT_FALSE(err2.str().empty());

    fs::remove(in_path);
    fs::remove(out_path);
}

// D4 inc3: born-columnar operator output, end to end through the real planner.
//
// A HAVING filter sits above the windowed aggregate, and filter_row_predicate
// ingests columnar, so the planner enables born-columnar output on the window
// (see SqlPhysical.ColumnarOutputOnWhenTheConsumerIngestsColumnar). The window
// then appends fired panes straight into typed Arrow builders instead of building
// an output Row each. The oracle is the row path: same query, same input,
// CLINK_DISABLE_COLUMNAR_OUTPUT=1, byte-identical output lines.
TEST(EmbeddedEngine, ColumnarWindowOutputMatchesRowOutput) {
    const auto in_path = fs::temp_directory_path() / "clink_embed_cwo_in.ndjson";
    fs::remove(in_path);
    {
        std::vector<std::string> lines;
        // Three windows' worth of events over four keys, with per-key counts that
        // straddle the HAVING threshold so some panes are filtered out.
        for (int w = 0; w < 3; ++w) {
            for (int k = 1; k <= 4; ++k) {
                for (int n = 0; n < k; ++n) {
                    lines.push_back("{\"k\":" + std::to_string(k) +
                                    ",\"ts\":" + std::to_string((w * 10000) + 1000 + n) + "}");
                }
            }
        }
        // A trailing event well past the last window so the watermark fires them.
        lines.push_back("{\"k\":1,\"ts\":100000}");
        write_lines(in_path, lines);
    }

    auto run = [&](const fs::path& out_path) {
        fs::remove(out_path);
        clink::embed::EngineOptions opts;
        std::ostringstream err;
        opts.err = &err;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        const std::string script =
            "CREATE TABLE cwo_src (k BIGINT, ts BIGINT) WITH (connector='file', format='json', "
            "path='" +
            in_path.string() +
            "', event_time_column='ts', watermark_lag_ms='0');"
            "CREATE TABLE cwo_out (k BIGINT, c BIGINT) WITH (connector='file', format='json', "
            "path='" +
            out_path.string() +
            "');"
            "INSERT INTO cwo_out SELECT k, COUNT(*) AS c FROM cwo_src "
            "GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k HAVING COUNT(*) > 1";
        EXPECT_EQ(engine.execute_script(script), 0) << err.str();
        EXPECT_TRUE(engine.await_all()) << err.str();
        auto lines = read_lines(out_path);
        std::sort(lines.begin(), lines.end());
        return lines;
    };

    const auto columnar = run(fs::temp_directory_path() / "clink_embed_cwo_col.ndjson");
    ASSERT_FALSE(columnar.empty()) << "the fixture must emit panes that pass HAVING";

    setenv("CLINK_DISABLE_COLUMNAR_OUTPUT", "1", 1);
    const auto rows = run(fs::temp_directory_path() / "clink_embed_cwo_row.ndjson");
    unsetenv("CLINK_DISABLE_COLUMNAR_OUTPUT");

    EXPECT_EQ(columnar, rows)
        << "born-columnar window output must be indistinguishable from the row fire";
}

namespace {

struct ResumeRun {
    int execute_rc{0};
    std::int64_t rows{0};
    std::string err;
};

// One engine, one run of `insert` into a collect table over the orders file, with
// checkpointing into `ckpt`. The engine is destroyed before returning, as a
// process that exits would be.
ResumeRun run_orders_into_collect(const fs::path& in_path,
                                  const fs::path& ckpt,
                                  const std::string& insert,
                                  bool fresh) {
    ResumeRun run;
    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    opts.checkpoint_dir = ckpt.string();
    opts.checkpoint_interval_ms = 100;
    opts.fresh = fresh;
    {
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        if (engine.execute_script(orders_ddl(in_path) +
                                  "CREATE TABLE results (user_id BIGINT, amount BIGINT) "
                                  "WITH (connector='collect')") != 0) {
            run.execute_rc = -1;
            run.err = err.str();
            return run;
        }
        auto reader_r = engine.collect_reader("results");
        if (!reader_r.ok()) {
            run.execute_rc = -2;
            run.err = reader_r.status().ToString();
            return run;
        }
        auto reader = *reader_r;
        run.execute_rc = engine.execute_script(insert);
        if (run.execute_rc == 0) {
            while (true) {
                std::shared_ptr<arrow::RecordBatch> batch;
                if (!reader->ReadNext(&batch).ok() || !batch) {
                    break;
                }
                run.rows += batch->num_rows();
            }
            (void)engine.await_all();
        }
    }
    run.err = err.str();
    return run;
}

std::uint64_t highest_completed_marker(const fs::path& ckpt) {
    std::uint64_t best = 0;
    std::error_code ec;
    for (const auto& e : fs::recursive_directory_iterator(ckpt / "_jobs", ec)) {
        const auto name = e.path().filename().string();
        if (name.rfind("COMPLETED-", 0) == 0) {
            best = std::max<std::uint64_t>(best, std::stoull(name.substr(10)));
        }
    }
    return best;
}

fs::path resume_scratch(const std::string& name) {
    const auto dir = fs::temp_directory_path() /
                     ("clink_embed_resume_" + name + "_" + std::to_string(::getpid()));
    fs::remove_all(dir);
    fs::create_directories(dir);
    return dir;
}

const std::string kResumeInsert = "INSERT INTO results SELECT user_id, amount FROM orders";

// What a kill leaves behind: the run's checkpoints without the FINISHED marker
// a clean end of input writes. Removing the marker from a finished run gives the
// in-process tests a killed run's directory deterministically; the real kill is
// EmbeddedResumeKafka in the integration suite.
void as_if_killed(const fs::path& ckpt) {
    std::error_code ec;
    fs::remove(ckpt / "_jobs" / "1" / "FINISHED", ec);
}

// Runs `insert` over the orders file into a plain file sink.
int run_orders_into_file(const fs::path& in_path,
                         const fs::path& out_path,
                         const fs::path& ckpt,
                         std::string* err_out) {
    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    opts.checkpoint_dir = ckpt.string();
    opts.checkpoint_interval_ms = 100;
    int rc = 0;
    {
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        rc = engine.execute_script(orders_ddl(in_path) +
                                   "CREATE TABLE out_file (user_id BIGINT, amount BIGINT) WITH "
                                   "(connector='file', path='" +
                                   out_path.string() +
                                   "', format='json');\n"
                                   "INSERT INTO out_file SELECT user_id, amount FROM orders;");
        if (rc == 0 && !engine.await_all()) {
            rc = 1;
        }
    }
    if (err_out != nullptr) {
        *err_out = err.str();
    }
    return rc;
}

}  // namespace

// A job whose last run did not finish resumes from its checkpoints: the file
// source restores its offset at the end of the file, so nothing is read or
// emitted again. Before resume existed the rerun started from nothing and
// re-emitted every row, which into an exactly-once sink duplicates everything
// already committed.
TEST(EmbeddedEngine, AnUnfinishedJobResumesFromItsCheckpointsInsteadOfReplaying) {
    const auto dir = resume_scratch("rerun");
    write_orders(dir / "in.ndjson");

    const auto first =
        run_orders_into_collect(dir / "in.ndjson", dir / "ckpt", kResumeInsert, false);
    ASSERT_EQ(first.execute_rc, 0) << first.err;
    EXPECT_EQ(first.rows, 5);
    const auto first_marker = highest_completed_marker(dir / "ckpt");
    ASSERT_GT(first_marker, 0u) << "the first run must leave a completed checkpoint";
    as_if_killed(dir / "ckpt");

    const auto second =
        run_orders_into_collect(dir / "in.ndjson", dir / "ckpt", kResumeInsert, false);
    ASSERT_EQ(second.execute_rc, 0) << second.err;
    EXPECT_EQ(second.rows, 0) << "a resumed run must not re-emit what the first run read";
    EXPECT_GT(highest_completed_marker(dir / "ckpt"), first_marker)
        << "the resumed run numbers its checkpoints above the first run's";
    fs::remove_all(dir);
}

#ifdef CLINK_FAULT_INJECTION
namespace {

// Every line committed by the exactly-once file sink under `out`.
std::vector<std::string> committed_lines(const fs::path& out) {
    std::vector<std::string> lines;
    std::error_code ec;
    for (const auto& e : fs::directory_iterator(out / "committed", ec)) {
        std::ifstream in(e.path());
        for (std::string line; std::getline(in, line);) {
            if (!line.empty()) {
                lines.push_back(line);
            }
        }
    }
    std::sort(lines.begin(), lines.end());
    return lines;
}

bool await_hits(std::string_view point, std::uint64_t n) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (clink::fault::Registry::instance().hits(point) < n) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return true;
}

int run_orders_into_exactly_once_file(const fs::path& in_path,
                                      const fs::path& out,
                                      const fs::path& ckpt,
                                      bool cancel_at_end_of_input) {
    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    opts.checkpoint_dir = ckpt.string();
    opts.checkpoint_interval_ms = 100;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    const auto rc = engine.execute_script(
        orders_ddl(in_path) +
        "CREATE TABLE out_file (user_id BIGINT, amount BIGINT) WITH (connector='file', path='" +
        out.string() +
        "', format='json', delivery_guarantee='exactly_once');\n"
        "INSERT INTO out_file SELECT user_id, amount FROM orders;");
    if (rc != 0) {
        ADD_FAILURE() << err.str();
        return rc;
    }
    if (!cancel_at_end_of_input) {
        return engine.await_all() ? 0 : 1;
    }
    auto& faults = clink::fault::Registry::instance();
    // The source reaches the end of its input and asks for its final checkpoint.
    EXPECT_TRUE(await_hits(clink::fault::points::kCoordinatorBeforeFinalCheckpointRequest, 1));
    const auto ids = engine.job_ids();
    EXPECT_EQ(ids.size(), 1U);
    // The cancel is decided, and held before its broadcast.
    std::thread canceller([&] { engine.cancel_job(ids.front()); });
    EXPECT_TRUE(await_hits(clink::fault::points::kCoordinatorBeforeCancelBroadcast, 1));
    // The request is answered in the gap: declined, the job is stopping.
    faults.release(clink::fault::points::kCoordinatorBeforeFinalCheckpointRequest);
    (void)engine.await_all();
    faults.release(clink::fault::points::kCoordinatorBeforeCancelBroadcast);
    canceller.join();
    return 0;
}

}  // namespace

// A cancel that races end of input. cancel_job marks the job cancelling, then
// broadcasts CancelJob; a source that reached the end of its input between the
// two asked for its final checkpoint, was declined, and committed its tail
// locally, a commit that no checkpoint covers. The cancelled run leaves no
// FINISHED marker, so a rerun resumes from the last completed checkpoint and
// published the tail a second time.
TEST(EmbeddedEngine, ACancelThatRacesEndOfInputDoesNotPublishATailARerunRepublishes) {
    auto& faults = clink::fault::Registry::instance();
    faults.reset();
    const auto dir = resume_scratch("cancel_race");
    write_orders(dir / "in.ndjson");
    faults.arm({.point = clink::fault::points::kCoordinatorBeforeFinalCheckpointRequest,
                .ordinal = 1,
                .action = clink::fault::Action::Block});
    faults.arm({.point = clink::fault::points::kCoordinatorBeforeCancelBroadcast,
                .ordinal = 1,
                .action = clink::fault::Action::Block});
    ASSERT_EQ(run_orders_into_exactly_once_file(dir / "in.ndjson", dir / "out", dir / "ckpt", true),
              0);
    faults.reset();
    ASSERT_EQ(
        run_orders_into_exactly_once_file(dir / "in.ndjson", dir / "out", dir / "ckpt", false), 0);
    const std::vector<std::string> expected{
        R"({"amount":10,"user_id":1})",
        R"({"amount":20,"user_id":2})",
        R"({"amount":30,"user_id":1})",
        R"({"amount":5,"user_id":2})",
        R"({"amount":7,"user_id":1})",
    };
    EXPECT_EQ(committed_lines(dir / "out"), expected)
        << "the cancelled run published its tail and the rerun published it again";
    fs::remove_all(dir);
}
#endif  // CLINK_FAULT_INJECTION

// A job that reached the end of its input is run again by a rerun - a bounded
// load, a full-refresh view - rather than resumed at its end, where it would
// publish nothing. Its checkpoints still number above the finished run's.
TEST(EmbeddedEngine, AFinishedJobStartsOverOnARerun) {
    const auto dir = resume_scratch("finished");
    write_orders(dir / "in.ndjson");

    const auto first =
        run_orders_into_collect(dir / "in.ndjson", dir / "ckpt", kResumeInsert, false);
    ASSERT_EQ(first.execute_rc, 0) << first.err;
    ASSERT_TRUE(fs::exists(dir / "ckpt" / "_jobs" / "1" / "FINISHED"))
        << "a clean end of input records the finish";
    const auto first_marker = highest_completed_marker(dir / "ckpt");

    const auto second =
        run_orders_into_collect(dir / "in.ndjson", dir / "ckpt", kResumeInsert, false);
    ASSERT_EQ(second.execute_rc, 0) << second.err;
    EXPECT_EQ(second.rows, 5) << "a finished job is run again, not resumed at its end";
    EXPECT_GT(highest_completed_marker(dir / "ckpt"), first_marker);

    // A finished job restores nothing, so a changed script is not refused.
    const auto changed = run_orders_into_collect(
        dir / "in.ndjson",
        dir / "ckpt",
        "INSERT INTO results SELECT user_id, amount FROM orders WHERE amount > 0",
        false);
    EXPECT_EQ(changed.execute_rc, 0) << changed.err;
    EXPECT_EQ(changed.rows, 5);
    fs::remove_all(dir);
}

// Job ids restart at 1 in every process, so the markers alone cannot say which job
// wrote them. A different script resuming the same directory is refused by name
// rather than handed the first job's state.
TEST(EmbeddedEngine, ResumingADifferentScriptOnTheSameCheckpointsIsRefused) {
    const auto dir = resume_scratch("changed");
    write_orders(dir / "in.ndjson");

    const auto first =
        run_orders_into_collect(dir / "in.ndjson", dir / "ckpt", kResumeInsert, false);
    ASSERT_EQ(first.execute_rc, 0) << first.err;
    as_if_killed(dir / "ckpt");

    const auto changed = run_orders_into_collect(
        dir / "in.ndjson",
        dir / "ckpt",
        "INSERT INTO results SELECT user_id, amount FROM orders WHERE amount > 0",
        false);
    EXPECT_NE(changed.execute_rc, 0);
    EXPECT_NE(changed.err.find("refusing to resume job 1"), std::string::npos) << changed.err;
    EXPECT_NE(changed.err.find("different job graph"), std::string::npos) << changed.err;
    fs::remove_all(dir);
}

// fresh = true starts an unfinished job from empty state on the same directory,
// and its checkpoints still number above the old run's, so it overwrites none of
// that run's markers. A changed script is accepted fresh where a resume refuses it.
TEST(EmbeddedEngine, AFreshRunStartsOverAboveTheOldCheckpointIds) {
    const auto dir = resume_scratch("fresh");
    write_orders(dir / "in.ndjson");

    const auto first =
        run_orders_into_collect(dir / "in.ndjson", dir / "ckpt", kResumeInsert, false);
    ASSERT_EQ(first.execute_rc, 0) << first.err;
    const auto first_marker = highest_completed_marker(dir / "ckpt");
    as_if_killed(dir / "ckpt");

    const auto fresh =
        run_orders_into_collect(dir / "in.ndjson", dir / "ckpt", kResumeInsert, true);
    ASSERT_EQ(fresh.execute_rc, 0) << fresh.err;
    EXPECT_EQ(fresh.rows, 5) << "a fresh run reads the whole input again";
    EXPECT_GT(highest_completed_marker(dir / "ckpt"), first_marker);
    as_if_killed(dir / "ckpt");

    const auto changed_fresh = run_orders_into_collect(
        dir / "in.ndjson",
        dir / "ckpt",
        "INSERT INTO results SELECT user_id, amount FROM orders WHERE amount > 0",
        true);
    EXPECT_EQ(changed_fresh.execute_rc, 0) << changed_fresh.err;
    EXPECT_EQ(changed_fresh.rows, 5);
    fs::remove_all(dir);
}

// A resumed run continues a plain file sink's output rather than truncating it:
// the sources replay only what follows the restore point, so a truncated file
// would lose every row written before it. (At-least-once: rows after the restore
// point are written again; here the source restores at its end, so none are.)
TEST(EmbeddedEngine, AResumedRunKeepsThePlainFileSinksEarlierOutput) {
    const auto dir = resume_scratch("filesink");
    write_orders(dir / "in.ndjson");
    std::string err;

    ASSERT_EQ(run_orders_into_file(dir / "in.ndjson", dir / "out.ndjson", dir / "ckpt", &err), 0)
        << err;
    ASSERT_EQ(read_lines(dir / "out.ndjson").size(), 5u);
    as_if_killed(dir / "ckpt");

    ASSERT_EQ(run_orders_into_file(dir / "in.ndjson", dir / "out.ndjson", dir / "ckpt", &err), 0)
        << err;
    EXPECT_EQ(read_lines(dir / "out.ndjson").size(), 5u)
        << "the resumed run must keep what the first run wrote";

    // A finished job starts over and rewrites its output whole, as before resume.
    ASSERT_EQ(run_orders_into_file(dir / "in.ndjson", dir / "out.ndjson", dir / "ckpt", &err), 0)
        << err;
    EXPECT_EQ(read_lines(dir / "out.ndjson").size(), 5u);
    fs::remove_all(dir);
}

// A run that started from empty state and died before completing a checkpoint
// of its own has nothing to resume. The checkpoints on disk are the run before
// it, and resuming them would hand this run that run's state: after a clean
// finish, its sources at their end, so nothing is published; after --fresh with
// a changed script, another graph's state. The run base each fresh start records
// keeps them out of reach. The directory is built the way such a kill leaves it:
// the new run's deploy records its base above the old checkpoints and clears the
// finish, and nothing more lands before the kill.
TEST(EmbeddedEngine, ARunKilledBeforeItsFirstCheckpointDoesNotResumeTheRunBeforeIt) {
    const auto dir = resume_scratch("killedearly");
    write_orders(dir / "in.ndjson");

    const auto first =
        run_orders_into_collect(dir / "in.ndjson", dir / "ckpt", kResumeInsert, false);
    ASSERT_EQ(first.execute_rc, 0) << first.err;
    const auto first_marker = highest_completed_marker(dir / "ckpt");
    ASSERT_GT(first_marker, 0u);
    {
        std::ofstream(dir / "ckpt" / "_jobs" / "1" / "run-base") << first_marker;
    }
    as_if_killed(dir / "ckpt");

    const auto rerun =
        run_orders_into_collect(dir / "in.ndjson", dir / "ckpt", kResumeInsert, false);
    ASSERT_EQ(rerun.execute_rc, 0) << rerun.err;
    EXPECT_EQ(rerun.rows, 5) << "the killed run completed no checkpoint, so it starts over";

    // Same shape after --fresh with a changed script: its fingerprint is the new
    // graph's, the checkpoints below the base are the old graph's.
    const auto marker = highest_completed_marker(dir / "ckpt");
    {
        std::ofstream(dir / "ckpt" / "_jobs" / "1" / "run-base") << marker;
    }
    as_if_killed(dir / "ckpt");
    const auto changed = run_orders_into_collect(
        dir / "in.ndjson",
        dir / "ckpt",
        "INSERT INTO results SELECT user_id, amount FROM orders WHERE amount > 0",
        false);
    EXPECT_EQ(changed.execute_rc, 0) << changed.err;
    EXPECT_EQ(changed.rows, 5) << "nothing of its own to resume, so no refusal and no old state";
    fs::remove_all(dir);
}

namespace {

// Scripts whose job-graph fingerprint the v0.10.0 release recorded, with the
// fingerprint it recorded, taken by running each script under the v0.10.0
// runtime image with --checkpoint-dir and reading _jobs/1/graph-fingerprint.
// The fingerprint hashes every path in the script, so the inputs live at fixed
// paths, one file per case so concurrently running cases never share one.
struct ReleasedFingerprint {
    const char* label;
    std::vector<std::pair<std::string, std::vector<std::string>>> inputs;  // file -> lines
    std::string script;
    const char* recorded;
};

const char* const kGoldenDir = "/tmp/clink-fingerprint-golden";

std::vector<ReleasedFingerprint> v0100_fingerprints() {
    const std::string d = kGoldenDir;
    const std::vector<std::string> windows{
        R"({"k":1,"ts":1000})", R"({"k":2,"ts":2000})", R"({"k":1,"ts":100000})"};
    return {
        // A window straight into the bind in front of a blackhole sink. v0.10.0
        // planned it in row form; this build plans it columnar.
        {"window into the bind",
         {{"window_bind.ndjson", windows}},
         "CREATE TABLE ev (k BIGINT, ts BIGINT) WITH (connector='file', format='json', path='" + d +
             "/window_bind.ndjson', event_time_column='ts', watermark_lag_ms='0');"
             "CREATE TABLE per_window (k BIGINT, c BIGINT) WITH (connector='blackhole');"
             "INSERT INTO per_window SELECT k, COUNT(*) AS c FROM ev "
             "GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k;",
         "a3a5de713f0e26fb"},
        // A window into a HAVING filter: v0.10.0 already gave the window
        // columnar_output, and hashed it.
        {"window into a filter",
         {{"window_having.ndjson", windows}},
         "CREATE TABLE ev (k BIGINT, ts BIGINT) WITH (connector='file', format='json', path='" + d +
             "/window_having.ndjson', event_time_column='ts', watermark_lag_ms='0');"
             "CREATE TABLE per_window (k BIGINT, c BIGINT) WITH (connector='blackhole');"
             "INSERT INTO per_window SELECT k, COUNT(*) AS c FROM ev "
             "GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k HAVING COUNT(*) > 0;",
         "5bb25bd991bc57c1"},
        // An inner join into its projection: v0.10.0 already gave the join
        // columnar_output, and hashed it.
        {"inner join",
         {{"join_a.ndjson", {R"({"k":1,"x":10})", R"({"k":2,"x":20})"}},
          {"join_b.ndjson", {R"({"k":1,"y":5})", R"({"k":3,"y":7})"}}},
         "CREATE TABLE a (k BIGINT, x BIGINT) WITH (connector='file', format='json', path='" + d +
             "/join_a.ndjson');"
             "CREATE TABLE b (k BIGINT, y BIGINT) WITH (connector='file', format='json', path='" +
             d +
             "/join_b.ndjson');"
             "CREATE TABLE joined (k BIGINT, x BIGINT, y BIGINT) WITH (connector='blackhole');"
             "INSERT INTO joined SELECT a.k, a.x, b.y FROM a JOIN b ON a.k = b.k;",
         "0639e14a4d089b0d"},
        // A window whose output carries TIMESTAMP aggregates into a HAVING
        // filter: v0.10.0 hashed its columnar_output with those columns as str,
        // and this build writes ts_ms there.
        {"window with timestamp aggregates into a filter",
         {{"window_ts_having.ndjson",
           {R"({"k":1,"ts":1000})", R"({"k":2,"ts":2000})", R"({"k":1,"ts":100000})"}}},
         "CREATE TABLE ev (k BIGINT, ts TIMESTAMP(3)) WITH (connector='file', format='json', "
         "path='" +
             d +
             "/window_ts_having.ndjson', event_time_column='ts', watermark_lag_ms='0');"
             "CREATE TABLE per_window (k BIGINT, first_ts TIMESTAMP(3), last_ts TIMESTAMP(3), c "
             "BIGINT) WITH (connector='blackhole');"
             "INSERT INTO per_window SELECT k, MIN(ts) AS first_ts, MAX(ts) AS last_ts, COUNT(*) "
             "AS c FROM ev GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k HAVING COUNT(*) > 0;",
         "3aeda44484f48a79"},
        // An inner join carrying a TIMESTAMP and a TIMESTAMPTZ into its
        // projection: v0.10.0 hashed its columnar_output with both as str, and
        // this build writes ts_ms and tstz_ms there.
        {"inner join carrying timestamps",
         {{"join_ts_a.ndjson", {R"({"k":1,"t":1000,"tz":2000})", R"({"k":2,"t":3000,"tz":4000})"}},
          {"join_ts_b.ndjson", {R"({"k":1,"y":5})", R"({"k":3,"y":7})"}}},
         "CREATE TABLE a (k BIGINT, t TIMESTAMP(3), tz TIMESTAMPTZ) WITH (connector='file', "
         "format='json', path='" +
             d +
             "/join_ts_a.ndjson');"
             "CREATE TABLE b (k BIGINT, y BIGINT) WITH (connector='file', format='json', path='" +
             d +
             "/join_ts_b.ndjson');"
             "CREATE TABLE joined (k BIGINT, t TIMESTAMP(3), tz TIMESTAMPTZ, y BIGINT) WITH "
             "(connector='blackhole');"
             "INSERT INTO joined SELECT a.k, a.t, a.tz, b.y FROM a JOIN b ON a.k = b.k;",
         "a609418b8454ee76"},
    };
}

// The job spec the script runner hands the embedded engine for `script`, which
// holds one INSERT. The engine submits that spec as it is but for the per-engine
// collect_scope, which the fingerprint leaves out, so its job_graph_fingerprint
// is the one a checkpointed run of the script records.
clink::cluster::JobGraphSpec planned_spec(const std::string& script) {
    clink::sql::Catalog catalog;
    clink::sql::ScriptRunOptions opts;
    opts.parallelism = clink::embed::EngineOptions{}.parallelism;
    std::ostringstream out;
    std::ostringstream err;
    std::vector<clink::cluster::JobGraphSpec> specs;
    const int rc = clink::sql::run_script(
        script,
        catalog,
        opts,
        clink::sql::ScriptIO{&out, &err},
        [&specs](const clink::cluster::JobGraphSpec& spec, const std::string&) {
            specs.push_back(spec);
            return 0;
        });
    EXPECT_EQ(rc, 0) << err.str();
    EXPECT_EQ(specs.size(), 1U);
    return specs.empty() ? clink::cluster::JobGraphSpec{} : specs.front();
}

// One checkpointed run of `script`. Returns execute_script's code; the engine
// is destroyed before returning, as a process that exits would be.
int run_checkpointed(const std::string& script, const fs::path& ckpt, std::string* err_out) {
    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    opts.checkpoint_dir = ckpt.string();
    opts.checkpoint_interval_ms = 100;
    int rc = 0;
    {
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        rc = engine.execute_script(script);
        if (rc == 0 && !engine.await_all()) {
            rc = 1;
        }
    }
    if (err_out != nullptr) {
        *err_out = err.str();
    }
    return rc;
}

std::string read_file(const fs::path& path) {
    std::ifstream in(path);
    std::stringstream buf;
    buf << in.rdbuf();
    return buf.str();
}

}  // namespace

// A job interrupted under v0.10.0 resumes under this build. Each case runs once
// here and is left as a kill leaves it; its recorded fingerprint is then set to
// the one v0.10.0 recorded for the same script, so the rerun stands where an
// upgrade would. The window into the bind is the case this build plans
// differently (columnar where v0.10.0 planned rows); the filter and join cases
// carried columnar_output under v0.10.0 already, and their fingerprints must
// still include it. The two timestamp cases carry it too, and this build spells
// their timestamp columns there in the second Row layout's codes.
TEST(EmbeddedEngine, AJobInterruptedUnderTheLastReleaseResumes) {
    std::error_code ec;
    fs::create_directories(kGoldenDir, ec);
    for (const auto& c : v0100_fingerprints()) {
        SCOPED_TRACE(c.label);
        for (const auto& [file, lines] : c.inputs) {
            write_lines(fs::path{kGoldenDir} / file, lines);
        }
        const auto dir = resume_scratch("release_upgrade");
        std::string err;
        ASSERT_EQ(run_checkpointed(c.script, dir / "ckpt", &err), 0) << err;
        const auto first_marker = highest_completed_marker(dir / "ckpt");
        ASSERT_GT(first_marker, 0u) << "the first run must leave a completed checkpoint";
        const auto jobs = dir / "ckpt" / "_jobs" / "1";
        const auto first_base = read_file(jobs / "run-base");
        ASSERT_FALSE(first_base.empty()) << "the first run started from empty state and records it";
        EXPECT_EQ(read_file(jobs / "graph-fingerprint"), c.recorded)
            << "this build fingerprints the script differently from v0.10.0";
        as_if_killed(dir / "ckpt");
        {
            std::ofstream out(jobs / "graph-fingerprint", std::ios::trunc);
            out << c.recorded;
        }

        ASSERT_EQ(run_checkpointed(c.script, dir / "ckpt", &err), 0) << err;
        EXPECT_EQ(err.find("different job graph"), std::string::npos) << err;
        EXPECT_EQ(read_file(jobs / "run-base"), first_base)
            << "the rerun started from empty state instead of resuming the first run";
        EXPECT_GT(highest_completed_marker(dir / "ckpt"), first_marker);
        fs::remove_all(dir);
        for (const auto& [file, lines] : c.inputs) {
            fs::remove(fs::path{kGoldenDir} / file, ec);
        }
    }
}

// Jobs whose columnar carriers hold TIMESTAMP and TIMESTAMPTZ columns. This
// build carries them as epoch milliseconds, so it writes ts_ms and tstz_ms where
// v0.10.0 wrote str: in a window's or a join's columnar_output (the run-recorded
// golden cases above) and in a Kafka JSON table's columnar decode
// schema_columns. Each fingerprint must still be the one v0.10.0 recorded. A run
// of the Kafka script needs a broker, so its fingerprint is the planned one;
// v0.10.0's was recorded by running the script under the v0.10.0 runtime image
// with --checkpoint-dir, which writes the fingerprint before any operator is
// built. The run-recorded golden cases check that a planned fingerprint is the
// one a run records.
TEST(EmbeddedEngine, JobsCarryingTimestampsKeepTheFingerprintsTheLastReleaseRecorded) {
    for (const auto& c : v0100_fingerprints()) {
        SCOPED_TRACE(c.label);
        const auto spec = planned_spec(c.script);
        EXPECT_EQ(clink::cluster::job_graph_fingerprint(spec), c.recorded)
            << "the planned fingerprint is not the one a run records";
        if (std::string_view{c.label}.find("timestamp") != std::string_view::npos) {
            bool carried = false;
            for (const auto& op : spec.ops) {
                const auto it = op.params.find("columnar_output");
                carried = carried ||
                          (it != op.params.end() && it->second.find("_ms") != std::string::npos);
            }
            EXPECT_TRUE(carried) << "no columnar_output carries a timestamp as epoch "
                                    "milliseconds, so this case does not test the spelling";
        }
    }
    const std::string kafka =
        "CREATE TABLE ev (k BIGINT, ts TIMESTAMP(3), tz TIMESTAMPTZ) WITH (connector='kafka', "
        "format='json', brokers='127.0.0.1:1', topic='fingerprint_ts', event_time_column='ts', "
        "watermark_lag_ms='0');"
        "CREATE TABLE sunk (k BIGINT, ts TIMESTAMP(3), tz TIMESTAMPTZ) WITH "
        "(connector='blackhole');"
        "INSERT INTO sunk SELECT k, ts, tz FROM ev;";
    const auto spec = planned_spec(kafka);
    std::string decode_schema;
    for (const auto& op : spec.ops) {
        if (op.type == "json_string_to_row_columnar") {
            decode_schema = op.params.at("schema_columns");
        }
    }
    EXPECT_EQ(decode_schema, "k:i64;ts:ts_ms;tz:tstz_ms")
        << "the columnar decode does not carry the timestamps as epoch milliseconds, so this "
           "case does not test the spelling the fingerprint must read back";
    EXPECT_EQ(clink::cluster::job_graph_fingerprint(spec), "7bf9b02606fcea92")
        << "this build fingerprints the Kafka timestamp table differently from v0.10.0";
}

// The embedded engine admits the Row sidecar's second layout through its own
// deploy: its in-process worker registers at this build's protocol version, so
// the coordinator stamps the layout-bearing operators in the Deploy frames. The
// stamp never reaches the submitted spec, so the fingerprint a resume checks is
// the one v0.10.0 recorded for the same script.
TEST(EmbeddedEngine, AdmittingTheSecondRowLayoutLeavesTheResumeFingerprintAlone) {
    std::error_code ec;
    fs::create_directories(kGoldenDir, ec);
    const auto cases = v0100_fingerprints();
    const auto& c = cases.front();  // the window into the bind: one stamped window
    ASSERT_EQ(std::string{c.label}, "window into the bind");
    for (const auto& [file, lines] : c.inputs) {
        write_lines(fs::path{kGoldenDir} / file, lines);
    }
    const auto dir = resume_scratch("admitted_layout");
    const auto since = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count() -
                       1;
    std::string err;
    ASSERT_EQ(run_checkpointed(c.script, dir / "ckpt", &err), 0) << err;

    bool admitted = false;
    std::string seen;
    for (const auto& r : clink::LogBuffer::global().tail(1024, "info", since, "coordinator")) {
        if (r.message.find("row_layout=") == std::string::npos) {
            continue;
        }
        seen += r.message + "\n";
        const std::string key = "row_layout=2 stamped_ops=";
        if (const auto at = r.message.find(key); at != std::string::npos &&
                                                 at + key.size() < r.message.size() &&
                                                 r.message[at + key.size()] != '0') {
            admitted = true;
        }
    }
    EXPECT_TRUE(admitted) << "the embedded deploy did not admit and stamp the second layout; "
                             "coordinator said:\n"
                          << seen;
    EXPECT_EQ(read_file(dir / "ckpt" / "_jobs" / "1" / "graph-fingerprint"), c.recorded)
        << "admission moved the resume fingerprint";
    fs::remove_all(dir, ec);
    for (const auto& [file, lines] : c.inputs) {
        fs::remove(fs::path{kGoldenDir} / file, ec);
    }
}

// The plain `parquet` sink across a resume. It used to write one file per
// subtask, which a Parquet writer can only finish at close: a restored run
// started that file again from empty and dropped every row written before the
// restore point. It now writes complete parts at each barrier, and a resumed
// run keeps them.
TEST(EmbeddedEngine, AResumedRunKeepsThePlainParquetSinksEarlierOutput) {
    const auto dir = resume_scratch("parquetsink");
    write_orders(dir / "in.ndjson");
    const auto out = dir / "out_parquet";
    const auto run = [&](std::string* err_out) {
        clink::embed::EngineOptions opts;
        std::ostringstream err;
        opts.err = &err;
        opts.checkpoint_dir = (dir / "ckpt").string();
        opts.checkpoint_interval_ms = 100;
        int rc = 0;
        {
            clink::embed::EmbeddedEngine engine{std::move(opts)};
            rc = engine.execute_script(orders_ddl(dir / "in.ndjson") +
                                       "CREATE TABLE out_pq (user_id BIGINT, amount BIGINT) WITH "
                                       "(connector='parquet', path='" +
                                       out.string() +
                                       "');\n"
                                       "INSERT INTO out_pq SELECT user_id, amount FROM orders;");
            if (rc == 0 && !engine.await_all()) {
                rc = 1;
            }
        }
        *err_out = err.str();
        return rc;
    };
    const auto rows_on_disk = [&] {
        std::int64_t rows = 0;
        for (const auto& e : fs::directory_iterator(out)) {
            if (e.path().extension() != ".parquet") {
                continue;
            }
            auto in = arrow::io::ReadableFile::Open(e.path().string());
            EXPECT_TRUE(in.ok());
            auto reader = parquet::arrow::OpenFile(*in, arrow::default_memory_pool());
            EXPECT_TRUE(reader.ok());
            rows += (*reader)->parquet_reader()->metadata()->num_rows();
        }
        return rows;
    };
    std::string err;

    ASSERT_EQ(run(&err), 0) << err;
    ASSERT_EQ(rows_on_disk(), 5);
    as_if_killed(dir / "ckpt");

    ASSERT_EQ(run(&err), 0) << err;
    EXPECT_EQ(rows_on_disk(), 5) << "the resumed run must keep what the first run wrote";
    fs::remove_all(dir);
}

// A collect stream ends when the job writing it ends, and a job that failed
// ends it with the failure: the reader neither waits for producers that will
// never close nor hands back a short result as if it were complete.
TEST(EmbeddedEngine, ACollectStreamEndsWithItsJobsFailure) {
    const auto dir = resume_scratch("collectfail");
    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    ASSERT_EQ(
        engine.execute_script("CREATE TABLE src (a BIGINT, b BIGINT) "
                              "WITH (connector='parquet', path='" +
                              (dir / "missing.parquet").string() +
                              "');"
                              "CREATE TABLE got (a BIGINT, b BIGINT) WITH (connector='collect');"
                              "INSERT INTO got SELECT a, b FROM src"),
        0)
        << err.str();
    auto reader = engine.collect_reader("got").ValueOrDie();
    arrow::Status st;
    std::int64_t rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        st = reader->ReadNext(&batch);
        if (!st.ok() || !batch) {
            break;
        }
        rows += batch->num_rows();
    }
    EXPECT_EQ(rows, 0);
    ASSERT_FALSE(st.ok()) << "a failed job must end its collect stream with the failure";
    EXPECT_NE(st.ToString().find("failed"), std::string::npos) << st.ToString();
    EXPECT_FALSE(engine.await_all());
    fs::remove_all(dir);
}

// The connector page's example over files another tool wrote: plain Parquet
// with none of clink's own columns (no event_time), read through `prefix`.
TEST(EmbeddedEngine, AParquetDirectoryAnotherToolWroteIsReadBySql) {
    const auto dir = resume_scratch("pqforeign");
    const auto events = dir / "events";
    fs::create_directories(events);
    const auto schema = arrow::schema({arrow::field("user_id", arrow::int64(), false),
                                       arrow::field("amount", arrow::int64(), false)});
    for (int part = 0; part < 2; ++part) {
        arrow::Int64Builder ids;
        arrow::Int64Builder amounts;
        for (std::int64_t i = 0; i < 3; ++i) {
            ASSERT_TRUE(ids.Append(part * 3 + i + 1).ok());
            ASSERT_TRUE(amounts.Append((part * 3 + i + 1) * 10).ok());
        }
        auto table =
            arrow::Table::Make(schema, {ids.Finish().ValueOrDie(), amounts.Finish().ValueOrDie()});
        auto out = arrow::io::FileOutputStream::Open(
                       (events / ("part-" + std::to_string(part) + ".parquet")).string())
                       .ValueOrDie();
        ASSERT_TRUE(
            parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1024).ok());
        ASSERT_TRUE(out->Close().ok());
    }

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    ASSERT_EQ(engine.execute_script("CREATE TABLE events (user_id BIGINT, amount BIGINT) "
                                    "WITH (connector='parquet', prefix='" +
                                    events.string() +
                                    "');"
                                    "CREATE TABLE got (user_id BIGINT, amount BIGINT) "
                                    "WITH (connector='collect');"
                                    "INSERT INTO got SELECT user_id, amount FROM events"),
              0)
        << err.str();
    auto reader = engine.collect_reader("got").ValueOrDie();
    std::int64_t rows = 0;
    std::int64_t amount_sum = 0;
    std::int64_t id_sum = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_TRUE(reader->ReadNext(&batch).ok());
        if (!batch) {
            break;
        }
        const auto got_amounts =
            std::static_pointer_cast<arrow::Int64Array>(batch->GetColumnByName("amount"));
        const auto got_ids =
            std::static_pointer_cast<arrow::Int64Array>(batch->GetColumnByName("user_id"));
        for (std::int64_t i = 0; i < batch->num_rows(); ++i) {
            amount_sum += got_amounts->Value(i);
            id_sum += got_ids->Value(i);
        }
        rows += batch->num_rows();
    }
    EXPECT_TRUE(engine.await_all()) << err.str();
    EXPECT_EQ(rows, 6);
    EXPECT_EQ(id_sum, 21);
    EXPECT_EQ(amount_sum, 210);
    fs::remove_all(dir);
}

// A parquet table reads a directory of parts by column name, as it reads one
// file: a table declaring fewer columns, in another order, reads the parts a
// wider table wrote.
TEST(EmbeddedEngine, AParquetDirectoryIsReadByColumnName) {
    const auto dir = resume_scratch("pqbyname");
    const auto in_path = dir / "in.ndjson";
    const auto pq_path = dir / "pq";
    write_lines(in_path,
                {R"({"user_id":1,"name":"a","amount":10})",
                 R"({"user_id":2,"name":"b","amount":20})",
                 R"({"user_id":3,"name":"c","amount":30})"});
    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    ASSERT_EQ(engine.execute_script("CREATE TABLE evt (user_id BIGINT, name TEXT, amount BIGINT) "
                                    "WITH (connector='file', format='json', path='" +
                                    in_path.string() +
                                    "');"
                                    "CREATE TABLE pq_w (user_id BIGINT, name TEXT, amount BIGINT) "
                                    "WITH (connector='parquet', path='" +
                                    pq_path.string() +
                                    "');"
                                    "INSERT INTO pq_w SELECT user_id, name, amount FROM evt"),
              0)
        << err.str();
    ASSERT_TRUE(engine.await_all()) << err.str();
    ASSERT_TRUE(fs::is_directory(pq_path));

    ASSERT_EQ(engine.execute_script("CREATE TABLE pq_r (amount BIGINT, user_id BIGINT) "
                                    "WITH (connector='parquet', path='" +
                                    pq_path.string() +
                                    "');"
                                    "CREATE TABLE got (amount BIGINT, user_id BIGINT) "
                                    "WITH (connector='collect');"
                                    "INSERT INTO got SELECT amount, user_id FROM pq_r"),
              0)
        << err.str();
    auto reader = engine.collect_reader("got").ValueOrDie();
    std::int64_t rows = 0;
    std::int64_t amount_sum = 0;
    std::int64_t id_sum = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_TRUE(reader->ReadNext(&batch).ok());
        if (!batch) {
            break;
        }
        const auto amounts =
            std::static_pointer_cast<arrow::Int64Array>(batch->GetColumnByName("amount"));
        const auto ids =
            std::static_pointer_cast<arrow::Int64Array>(batch->GetColumnByName("user_id"));
        for (std::int64_t i = 0; i < batch->num_rows(); ++i) {
            amount_sum += amounts->Value(i);
            id_sum += ids->Value(i);
        }
        rows += batch->num_rows();
    }
    EXPECT_TRUE(engine.await_all()) << err.str();
    EXPECT_EQ(rows, 3);
    EXPECT_EQ(amount_sum, 60);
    EXPECT_EQ(id_sum, 6);
    fs::remove_all(dir);
}
