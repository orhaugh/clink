// Delta Lake sink: the pure transaction-log layer (delta_log.hpp) and a local
// filesystem round-trip through DeltaRowSink - write a real Delta table, then
// verify the _delta_log actions, the data files, and that the Parquet reads back.

#ifndef CLINK_HAS_ARROW
#error "test_delta_sink requires Arrow"
#endif

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/connectors/delta_log.hpp"
#include "clink/connectors/parquet_source.hpp"
#include "clink/core/record.hpp"
#include "clink/sql/delta_row_sink.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::Emitter;
using clink::ParquetSource;
using clink::StreamElement;
using clink::sql::DeltaRowSink;
using clink::sql::DeltaRowSinkOptions;
using clink::sql::make_row_columnar_arrow_batcher;
using clink::sql::Row;
using clink::sql::RowColumn;
namespace cfg = clink::config;
namespace dl = clink::delta;

namespace {

std::vector<RowColumn> schema() {
    return {{"id", arrow::int64()}, {"name", arrow::utf8()}};
}

Row make_row(std::int64_t id, const std::string& name) {
    Row r;
    r.values["id"] = cfg::JsonValue{id};
    r.values["name"] = cfg::JsonValue{name};
    return r;
}

std::vector<std::string> read_lines(const std::filesystem::path& p) {
    std::ifstream in(p);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) {
            lines.push_back(line);
        }
    }
    return lines;
}

}  // namespace

// ---- Pure delta_log layer ----

TEST(DeltaLog, VersionFilenameIsZeroPadded) {
    EXPECT_EQ(dl::version_filename(0), "00000000000000000000.json");
    EXPECT_EQ(dl::version_filename(1), "00000000000000000001.json");
    EXPECT_EQ(dl::version_filename(42), "00000000000000000042.json");
}

TEST(DeltaLog, ArrowSchemaMapsToDeltaTypes) {
    auto s = arrow::schema({arrow::field("a", arrow::int64()),
                            arrow::field("b", arrow::utf8(), /*nullable=*/false),
                            arrow::field("c", arrow::float64()),
                            arrow::field("d", arrow::boolean()),
                            arrow::field("e", arrow::date32())});
    auto js = cfg::parse(dl::arrow_schema_to_delta_schema_json(*s));
    ASSERT_TRUE(js.is_object());
    EXPECT_EQ(js.as_object().at("type").as_string(), "struct");
    const auto& fields = js.as_object().at("fields").as_array();
    ASSERT_EQ(fields.size(), 5u);
    EXPECT_EQ(fields[0].as_object().at("name").as_string(), "a");
    EXPECT_EQ(fields[0].as_object().at("type").as_string(), "long");
    EXPECT_TRUE(fields[0].as_object().at("nullable").as_bool());
    EXPECT_EQ(fields[1].as_object().at("type").as_string(), "string");
    EXPECT_FALSE(fields[1].as_object().at("nullable").as_bool());
    EXPECT_EQ(fields[2].as_object().at("type").as_string(), "double");
    EXPECT_EQ(fields[3].as_object().at("type").as_string(), "boolean");
    EXPECT_EQ(fields[4].as_object().at("type").as_string(), "date");
}

TEST(DeltaLog, AddActionCarriesPathSizeAndStats) {
    auto js = cfg::parse(dl::add_action_json("part-0.parquet", 1234, 1751000000000, 10));
    const auto& add = js.as_object().at("add").as_object();
    EXPECT_EQ(add.at("path").as_string(), "part-0.parquet");
    EXPECT_EQ(add.at("size").as_number(), 1234);
    EXPECT_EQ(add.at("modificationTime").as_number(), 1751000000000);
    EXPECT_TRUE(add.at("dataChange").as_bool());
    // stats is a STRINGIFIED JSON blob with numRecords.
    auto stats = cfg::parse(add.at("stats").as_string());
    EXPECT_EQ(stats.as_object().at("numRecords").as_number(), 10);
}

TEST(DeltaLog, ProtocolAndMetadataActions) {
    auto p = cfg::parse(dl::protocol_action_json());
    EXPECT_EQ(p.as_object().at("protocol").as_object().at("minWriterVersion").as_number(), 2);
    auto m = cfg::parse(
        dl::metadata_action_json("tid-123", R"({"type":"struct","fields":[]})", 1751000000000));
    const auto& md = m.as_object().at("metaData").as_object();
    EXPECT_EQ(md.at("id").as_string(), "tid-123");
    EXPECT_EQ(md.at("format").as_object().at("provider").as_string(), "parquet");
    // schemaString is a stringified schema JSON.
    auto sc = cfg::parse(md.at("schemaString").as_string());
    EXPECT_EQ(sc.as_object().at("type").as_string(), "struct");
}

// ---- Local round-trip through DeltaRowSink ----

TEST(DeltaSink, WritesValidDeltaTableWithTwoCommits) {
    // CLINK_DELTA_KEEP_DIR lets an external reader (delta-rs / DuckDB) validate the
    // produced table: point it at a dir, run this test, then read that dir.
    const char* keep = std::getenv("CLINK_DELTA_KEEP_DIR");
    auto root = keep != nullptr
                    ? std::filesystem::path(keep)
                    : std::filesystem::temp_directory_path() /
                          ("clink_delta_" + std::to_string(static_cast<long>(::getpid())));
    std::filesystem::remove_all(root);

    DeltaRowSinkOptions o;
    o.table_root = root.string();
    o.batcher = make_row_columnar_arrow_batcher(schema());
    DeltaRowSink sink(std::move(o));
    sink.open();

    // Commit 0 (two rows), barrier; commit 1 (one row), close.
    Batch<Row> b0;
    b0.emplace(make_row(1, "a"));
    b0.emplace(make_row(2, "b"));
    sink.on_data(b0);
    sink.on_barrier(CheckpointBarrier{CheckpointId{1}});
    Batch<Row> b1;
    b1.emplace(make_row(3, "c"));
    sink.on_data(b1);
    sink.close();  // commits the tail interval (version 1)

    // _delta_log/0.json: protocol + metaData + commitInfo + add. 1.json: commitInfo + add.
    const auto log0 = root / "_delta_log" / "00000000000000000000.json";
    const auto log1 = root / "_delta_log" / "00000000000000000001.json";
    ASSERT_TRUE(std::filesystem::exists(log0));
    ASSERT_TRUE(std::filesystem::exists(log1));

    auto l0 = read_lines(log0);
    ASSERT_EQ(l0.size(), 4u) << "v0 = protocol+metaData+commitInfo+add";
    EXPECT_TRUE(cfg::parse(l0[0]).as_object().count("protocol"));
    EXPECT_TRUE(cfg::parse(l0[1]).as_object().count("metaData"));
    EXPECT_TRUE(cfg::parse(l0[2]).as_object().count("commitInfo"));
    EXPECT_TRUE(cfg::parse(l0[3]).as_object().count("add"));

    auto l1 = read_lines(log1);
    ASSERT_EQ(l1.size(), 2u) << "v1 = commitInfo+add (no protocol/metaData)";
    EXPECT_TRUE(cfg::parse(l1[0]).as_object().count("commitInfo"));
    EXPECT_TRUE(cfg::parse(l1[1]).as_object().count("add"));

    // The metaData schema includes the row batcher's event_time + the two columns.
    auto sc = cfg::parse(
        cfg::parse(l0[1]).as_object().at("metaData").as_object().at("schemaString").as_string());
    const auto& fields = sc.as_object().at("fields").as_array();
    ASSERT_EQ(fields.size(), 3u);
    EXPECT_EQ(fields[0].as_object().at("name").as_string(), "event_time");
    EXPECT_EQ(fields[1].as_object().at("name").as_string(), "id");
    EXPECT_EQ(fields[2].as_object().at("name").as_string(), "name");

    // Both data files exist and read back via ParquetSource<Row>.
    std::int64_t total = 0;
    for (const auto* lines : {&l0, &l1}) {
        const std::string add_path =
            cfg::parse(lines->back()).as_object().at("add").as_object().at("path").as_string();
        const auto data_file = root / add_path;
        ASSERT_TRUE(std::filesystem::exists(data_file)) << add_path;
        ParquetSource<Row> src(data_file.string(), make_row_columnar_arrow_batcher(schema()), "rb");
        src.open();
        bool more = true;
        while (more) {
            std::vector<Row> got;
            auto em = Emitter<Row>{[&](StreamElement<Row> e) {
                if (e.is_data()) {
                    for (const auto& r : e.as_data()) {
                        got.push_back(r.value());
                    }
                }
                return true;
            }};
            more = src.produce(em);
            for (const auto& r : got) {
                EXPECT_TRUE(r.values.count("id"));
                EXPECT_TRUE(r.values.count("name"));
                ++total;
            }
            if (got.empty()) {
                break;
            }
        }
        src.close();
    }
    EXPECT_EQ(total, 3) << "all 3 rows across the two commits read back";

    if (keep == nullptr) {
        std::filesystem::remove_all(root);
    }
}
