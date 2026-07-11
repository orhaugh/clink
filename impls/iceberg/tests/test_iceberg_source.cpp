// Apache Iceberg source: write a real table through make_iceberg_row_sink
// (SQLite catalog, local warehouse, two snapshots), then read it back
// through make_iceberg_row_source - full scan, projected scan, replay
// offset resume, and the missing-table error.

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/iceberg/iceberg_row_sink.hpp"
#include "clink/iceberg/iceberg_row_source.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using clink::iceberg::IcebergRowSinkOptions;
using clink::iceberg::IcebergRowSourceOptions;
using clink::iceberg::make_iceberg_row_sink;
using clink::iceberg::make_iceberg_row_source;
using clink::sql::make_row_columnar_arrow_batcher;
using clink::sql::Row;
using clink::sql::RowColumn;
namespace cfg = clink::config;
namespace fs = std::filesystem;
using namespace clink;

namespace {

std::vector<RowColumn> schema3() {
    return {{"id", arrow::int64()}, {"name", arrow::utf8()}, {"amount", arrow::int64()}};
}

Row make_row(std::int64_t id, const std::string& name, std::int64_t amount) {
    Row r;
    r.values["id"] = cfg::JsonValue{id};
    r.values["name"] = cfg::JsonValue{name};
    r.values["amount"] = cfg::JsonValue{amount};
    return r;
}

// Write a two-snapshot table {1..3} + {4..5} and return the warehouse.
fs::path write_table(const std::string& tag) {
    auto wh = fs::temp_directory_path() /
              ("clink_iceberg_src_" + tag + "_" + std::to_string(static_cast<long>(::getpid())));
    fs::remove_all(wh);
    fs::create_directories(wh);
    IcebergRowSinkOptions o;
    o.warehouse = wh.string();
    o.table = "events";
    o.batcher = make_row_columnar_arrow_batcher(schema3());
    auto sink = make_iceberg_row_sink(std::move(o));
    sink->open();
    Batch<Row> b0;
    for (std::int64_t i = 1; i <= 3; ++i) {
        b0.emplace(make_row(i, "n" + std::to_string(i), i * 10));
    }
    sink->on_data(b0);
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    Batch<Row> b1;
    for (std::int64_t i = 4; i <= 5; ++i) {
        b1.emplace(make_row(i, "n" + std::to_string(i), i * 10));
    }
    sink->on_data(b1);
    sink->close();
    return wh;
}

struct Drained {
    std::vector<Row> rows;
    std::uint64_t produce_calls{0};
};

Drained drain(Source<Row>& source) {
    Drained out;
    Emitter<Row> emitter([&out](StreamElement<Row> e) {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                out.rows.push_back(rec.value());
            }
        }
        return true;
    });
    while (source.produce(emitter)) {
        ++out.produce_calls;
    }
    return out;
}

}  // namespace

TEST(IcebergSource, FullScanReadsBothSnapshots) {
    const auto wh = write_table("full");

    IcebergRowSourceOptions o;
    o.warehouse = wh.string();
    o.table = "events";
    o.columns = schema3();
    auto source = make_iceberg_row_source(std::move(o));
    ASSERT_TRUE(source->is_bounded());
    source->open();
    auto got = drain(*source);
    source->close();

    ASSERT_EQ(got.rows.size(), 5u);
    std::int64_t id_sum = 0;
    std::int64_t amount_sum = 0;
    for (const auto& r : got.rows) {
        id_sum += static_cast<std::int64_t>(r.values.at("id").as_number());
        amount_sum += static_cast<std::int64_t>(r.values.at("amount").as_number());
        EXPECT_EQ(r.values.count("name"), 1u);
    }
    EXPECT_EQ(id_sum, 15);
    EXPECT_EQ(amount_sum, 150);
    fs::remove_all(wh);
}

TEST(IcebergSource, ProjectedScanReadsOnlySelectedColumns) {
    const auto wh = write_table("proj");

    IcebergRowSourceOptions o;
    o.warehouse = wh.string();
    o.table = "events";
    o.columns = {{"amount", arrow::int64()}, {"id", arrow::int64()}};  // reordered subset
    auto source = make_iceberg_row_source(std::move(o));
    source->open();
    auto got = drain(*source);
    source->close();

    ASSERT_EQ(got.rows.size(), 5u);
    for (const auto& r : got.rows) {
        EXPECT_EQ(r.values.count("name"), 0u) << "unprojected column must not materialise";
        EXPECT_EQ(static_cast<std::int64_t>(r.values.at("amount").as_number()),
                  static_cast<std::int64_t>(r.values.at("id").as_number()) * 10);
    }
    fs::remove_all(wh);
}

TEST(IcebergSource, OffsetRoundTripResumesWhereItStopped) {
    const auto wh = write_table("offset");
    InMemoryStateBackend backend;
    const OperatorId op{77};

    std::vector<Row> first_rows;
    std::uint64_t emitted_calls = 0;
    {
        IcebergRowSourceOptions o;
        o.warehouse = wh.string();
        o.table = "events";
        o.columns = schema3();
        auto source = make_iceberg_row_source(std::move(o));
        source->open();
        Emitter<Row> emitter([&first_rows](StreamElement<Row> e) {
            if (e.is_data()) {
                for (const auto& rec : e.as_data()) {
                    first_rows.push_back(rec.value());
                }
            }
            return true;
        });
        // Emit exactly one produce() worth (the first data file's batch),
        // checkpoint the offset mid-scan, and abandon the run.
        ASSERT_TRUE(source->produce(emitter));
        ++emitted_calls;
        source->snapshot_offset(backend, op, CheckpointId{1});
        source->close();
    }
    ASSERT_FALSE(first_rows.empty());

    // Recovery: a fresh source restores the offset and emits ONLY the rest.
    IcebergRowSourceOptions o;
    o.warehouse = wh.string();
    o.table = "events";
    o.columns = schema3();
    auto resumed = make_iceberg_row_source(std::move(o));
    ASSERT_TRUE(resumed->restore_offset(backend, op));
    resumed->open();
    auto rest = drain(*resumed);
    resumed->close();

    EXPECT_EQ(first_rows.size() + rest.rows.size(), 5u);
    std::int64_t total = 0;
    for (const auto& r : first_rows) {
        total += static_cast<std::int64_t>(r.values.at("id").as_number());
    }
    for (const auto& r : rest.rows) {
        total += static_cast<std::int64_t>(r.values.at("id").as_number());
    }
    EXPECT_EQ(total, 15) << "nothing re-emitted, nothing skipped";
    fs::remove_all(wh);
}

TEST(IcebergSource, MissingTableErrorsInsteadOfCreating) {
    auto wh = fs::temp_directory_path() /
              ("clink_iceberg_src_missing_" + std::to_string(static_cast<long>(::getpid())));
    fs::remove_all(wh);
    fs::create_directories(wh);

    IcebergRowSourceOptions o;
    o.warehouse = wh.string();
    o.table = "never_created";
    o.columns = schema3();
    auto source = make_iceberg_row_source(std::move(o));
    try {
        source->open();
        FAIL() << "expected a missing-table error";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("never_created"), std::string::npos) << e.what();
    }
    // The load-only path must not have created the table.
    bool created = false;
    for (const auto& entry : fs::recursive_directory_iterator(wh)) {
        if (entry.path().string().find("never_created") != std::string::npos) {
            created = true;
        }
    }
    EXPECT_FALSE(created);
    fs::remove_all(wh);
}
