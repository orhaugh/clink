// Apache Iceberg sink: a local filesystem round-trip through make_iceberg_row_sink -
// write a real Iceberg table (SQLite catalog + typed Parquet data files + snapshots)
// to a temp warehouse, then verify the on-disk table structure (catalog DB, table
// metadata, snapshot manifests, data files). PyIceberg/Spark read-back is the external
// validation (set CLINK_ICEBERG_KEEP_DIR to keep the warehouse for it).

#ifndef CLINK_HAS_ICEBERG
#error "test_iceberg_sink requires iceberg-cpp"
#endif

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/iceberg/iceberg_row_sink.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::iceberg::IcebergRowSinkOptions;
using clink::iceberg::make_iceberg_row_sink;
using clink::sql::make_row_columnar_arrow_batcher;
using clink::sql::Row;
using clink::sql::RowColumn;
namespace cfg = clink::config;
namespace fs = std::filesystem;

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

// Recursively count files under `root` whose path contains `needle`.
int count_paths_containing(const fs::path& root, const std::string& needle) {
    int n = 0;
    if (!fs::exists(root)) {
        return 0;
    }
    for (const auto& e : fs::recursive_directory_iterator(root)) {
        if (e.is_regular_file() && e.path().string().find(needle) != std::string::npos) {
            ++n;
        }
    }
    return n;
}

}  // namespace

TEST(IcebergSink, WritesRealIcebergTableWithTwoSnapshots) {
    const char* keep = std::getenv("CLINK_ICEBERG_KEEP_DIR");
    auto wh = keep != nullptr
                  ? fs::path(keep)
                  : fs::temp_directory_path() /
                        ("clink_iceberg_" + std::to_string(static_cast<long>(::getpid())));
    fs::remove_all(wh);
    fs::create_directories(wh);

    IcebergRowSinkOptions o;
    o.warehouse = wh.string();
    o.namespace_levels = {"default"};
    o.table = "events";
    o.batcher = make_row_columnar_arrow_batcher(schema());
    auto sink = make_iceberg_row_sink(std::move(o));
    sink->open();

    // Snapshot 0 (two rows), barrier; snapshot 1 (one row), close.
    Batch<Row> b0;
    b0.emplace(make_row(1, "a"));
    b0.emplace(make_row(2, "b"));
    sink->on_data(b0);
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    Batch<Row> b1;
    b1.emplace(make_row(3, "c"));
    sink->on_data(b1);
    sink->close();  // commits the tail interval

    // The SQLite catalog DB was created.
    EXPECT_TRUE(fs::exists(wh / "catalog.db"));
    // Two data files (one per snapshot interval).
    EXPECT_EQ(count_paths_containing(wh, ".parquet"), 2);
    // Iceberg table metadata + manifests were written.
    EXPECT_GE(count_paths_containing(wh, ".metadata.json"), 1);
    EXPECT_GE(count_paths_containing(wh, ".avro"), 1) << "snapshot manifests";

    if (keep == nullptr) {
        fs::remove_all(wh);
    }
}

// Subtask != 0 is dormant (single-writer table): no warehouse files are produced.
TEST(IcebergSink, NonZeroSubtaskIsDormant) {
    auto wh = fs::temp_directory_path() /
              ("clink_iceberg_dormant_" + std::to_string(static_cast<long>(::getpid())));
    fs::remove_all(wh);
    fs::create_directories(wh);

    IcebergRowSinkOptions o;
    o.warehouse = wh.string();
    o.table = "events";
    o.subtask_idx = 1;  // not the writer
    o.batcher = make_row_columnar_arrow_batcher(schema());
    auto sink = make_iceberg_row_sink(std::move(o));
    sink->open();
    Batch<Row> b;
    b.emplace(make_row(1, "a"));
    sink->on_data(b);
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->close();

    EXPECT_FALSE(fs::exists(wh / "catalog.db"));
    EXPECT_EQ(count_paths_containing(wh, ".parquet"), 0);

    fs::remove_all(wh);
}
