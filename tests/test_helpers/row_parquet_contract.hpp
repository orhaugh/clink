// The Row-channel Parquet contract every object-store connector keeps, run against a live store
// by each one's live test (S3, GCS and Azure; WebHDFS, which has no Arrow read path, keeps it in
// impls/webhdfs/tests/test_webhdfs_parquet.cpp against an in-process server). A SQL table's
// declared columns are written as typed Parquet columns, one each, through the
// <connector>_row_sink factory the planner names, and read back by name through
// <connector>_row_source: from the prefix the sink wrote, and from one object (key) another tool
// wrote, with its columns in another order, a column the table does not declare, and no
// event-time column.
//
// `params` carries the store's location and auth (bucket or container, endpoint, credentials).
// `fs` is the store's own Arrow filesystem and `root` the bucket or container the params name,
// used to look at what the sink wrote and to write the foreign object. `prefix` is a key prefix
// fresh for this run.

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"

namespace clink::test_support {

inline clink::sql::Row row_parquet_contract_row(std::int64_t id,
                                                clink::config::JsonValue name,
                                                double score,
                                                bool ok) {
    clink::sql::Row r;
    r.values["id"] = clink::config::JsonValue{id};
    r.values["name"] = std::move(name);
    r.values["score"] = clink::config::JsonValue{score};
    r.values["ok"] = clink::config::JsonValue{ok};
    return r;
}

inline void expect_row_parquet_contract(const std::string& connector,
                                        const std::map<std::string, std::string>& params,
                                        arrow::fs::FileSystem& fs,
                                        const std::string& root,
                                        const std::string& prefix) {
    using clink::config::JsonValue;
    using clink::sql::Row;
    const auto& reg = clink::cluster::OperatorRegistry::default_instance();
    const auto context = [&](const std::string& where, const std::string& path) {
        clink::cluster::OperatorBuildContext ctx;
        ctx.params = params;
        ctx.params["schema_columns"] = "id:i64;name:str;score:f64;ok:bool";
        ctx.params[where] = path;
        return ctx;
    };
    const auto read = [&](const std::string& where, const std::string& path) {
        std::vector<Row> rows;
        const auto* factory = reg.find_source(connector + "_row_source", "row");
        if (factory == nullptr) {
            ADD_FAILURE() << connector << "_row_source is not registered";
            return rows;
        }
        auto src =
            std::static_pointer_cast<clink::Source<Row>>(factory->build(context(where, path)));
        clink::Emitter<Row> em([&](clink::StreamElement<Row> e) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    rows.push_back(r.value());
                }
            }
            return true;
        });
        src->open();
        while (src->produce(em)) {
        }
        src->close();
        return rows;
    };

    // Two rows, one holding a null, in a part the barrier closes.
    const auto typed = prefix + "/typed";
    {
        const auto* factory = reg.find_sink(connector + "_row_sink", "row");
        ASSERT_NE(factory, nullptr) << connector << "_row_sink is not registered";
        auto sink =
            std::static_pointer_cast<clink::Sink<Row>>(factory->build(context("prefix", typed)));
        sink->open();
        clink::Batch<Row> b;
        b.emplace(row_parquet_contract_row(1, JsonValue{std::string{"a"}}, 0.5, true));
        b.emplace(row_parquet_contract_row(2, JsonValue{}, -1.25, false));
        sink->on_data(b);
        sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
        sink->close();
    }

    // The part holds the declared columns as typed Parquet columns, not one JSON text column.
    arrow::fs::FileSelector sel;
    sel.base_dir = root + "/" + typed;
    sel.recursive = true;
    std::vector<std::string> parts;
    for (const auto& info : fs.GetFileInfo(sel).ValueOrDie()) {
        if (info.type() == arrow::fs::FileType::File && info.path().ends_with(".parquet")) {
            parts.push_back(info.path());
        }
    }
    ASSERT_EQ(parts.size(), 1U) << "under " << sel.base_dir;
    auto reader = parquet::arrow::OpenFile(fs.OpenInputFile(parts[0]).ValueOrDie(),
                                           arrow::default_memory_pool())
                      .ValueOrDie();
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_TRUE(reader->GetSchema(&schema).ok());
    const std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> declared = {
        {"id", arrow::int64()},
        {"name", arrow::utf8()},
        {"score", arrow::float64()},
        {"ok", arrow::boolean()}};
    for (const auto& [name, type] : declared) {
        const auto field = schema->GetFieldByName(name);
        ASSERT_NE(field, nullptr) << name << " in " << schema->ToString();
        EXPECT_TRUE(field->type()->Equals(*type)) << name << " in " << schema->ToString();
    }
    EXPECT_EQ(schema->GetFieldByName("value"), nullptr) << "no JSON text column";

    const auto written = read("prefix", typed);
    ASSERT_EQ(written.size(), 2U);
    EXPECT_EQ(written[0].values.at("id").as_int(), 1);
    EXPECT_EQ(written[0].values.at("name").as_string(), "a");
    EXPECT_EQ(written[0].values.at("score").as_number(), 0.5);
    EXPECT_TRUE(written[0].values.at("ok").as_bool());
    EXPECT_EQ(written[1].values.at("id").as_int(), 2);
    EXPECT_TRUE(written[1].values.at("name").is_null());
    EXPECT_EQ(written[1].values.at("score").as_number(), -1.25);
    EXPECT_FALSE(written[1].values.at("ok").as_bool());

    // One object another tool wrote, read by key.
    arrow::BooleanBuilder ok;
    arrow::StringBuilder extra;
    arrow::DoubleBuilder score;
    arrow::StringBuilder name;
    arrow::Int64Builder id;
    ASSERT_TRUE(ok.Append(true).ok());
    ASSERT_TRUE(extra.Append("not declared").ok());
    ASSERT_TRUE(score.Append(9.5).ok());
    ASSERT_TRUE(name.Append("vendor").ok());
    ASSERT_TRUE(id.Append(7).ok());
    const auto foreign = arrow::Table::Make(arrow::schema({arrow::field("ok", arrow::boolean()),
                                                           arrow::field("extra", arrow::utf8()),
                                                           arrow::field("score", arrow::float64()),
                                                           arrow::field("name", arrow::utf8()),
                                                           arrow::field("id", arrow::int64())}),
                                            {ok.Finish().ValueOrDie(),
                                             extra.Finish().ValueOrDie(),
                                             score.Finish().ValueOrDie(),
                                             name.Finish().ValueOrDie(),
                                             id.Finish().ValueOrDie()});
    const auto drop = prefix + "/vendor/drop.parquet";
    {
        auto out = fs.OpenOutputStream(root + "/" + drop).ValueOrDie();
        ASSERT_TRUE(
            parquet::arrow::WriteTable(*foreign, arrow::default_memory_pool(), out, 1024).ok());
        ASSERT_TRUE(out->Close().ok());
    }
    const auto vendor = read("key", drop);
    ASSERT_EQ(vendor.size(), 1U);
    EXPECT_EQ(vendor[0].values.at("id").as_int(), 7);
    EXPECT_EQ(vendor[0].values.at("name").as_string(), "vendor");
    EXPECT_EQ(vendor[0].values.at("score").as_number(), 9.5);
    EXPECT_TRUE(vendor[0].values.at("ok").as_bool());
    EXPECT_FALSE(vendor[0].has_column("extra"));
}

}  // namespace clink::test_support
