// EmbeddedEngine end-to-end: the whole runtime in one process (in-process
// JM + TM over loopback) driving the shared SQL script runner - the
// execution core behind `clink run <file>.sql`. Also covers the
// script-runner's bare-SELECT-to-print synthesis and the print sink's
// stdout output (captured at the fd level, since sink subtasks write from
// runner threads).

#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/embed/embedded_engine.hpp"
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
    fs::remove(in_path);
    fs::remove(pq_path);
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
    fs::remove(in_path);
    fs::remove(pq_path);
}
