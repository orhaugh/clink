// Frozen fixtures for the persisted SQL catalog (design record 011).
//
// A catalog directory (`--catalog-dir`) holds one JSON file per table, model
// and function, written by Catalog::to_json. Those files outlive the build
// that wrote them: a 1.0 catalog directory must load on every 1.x release.
// The fixtures under tests/fixtures/catalog-*-v1.json were written by one
// build and must stay readable by every later one - the same contract, and
// the same regeneration rule, as tests/test_format_fixtures.cpp: run with
// CLINK_REGEN_FORMAT_FIXTURES=1 only to ADD a fixture, never to make a
// failing read pass. Readers ignore unknown keys, so the format grows by
// adding keys; renaming or removing one is a break.

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/sql/catalog.hpp"

namespace {

namespace fs = std::filesystem;

fs::path fixture_path(const std::string& name) {
    return fs::path{CLINK_SQL_FIXTURE_DIR} / name;
}

bool regen() {
    const char* env = std::getenv("CLINK_REGEN_FORMAT_FIXTURES");
    return env != nullptr && *env == '1';
}

std::string read_text(const fs::path& p) {
    std::ifstream in(p);
    EXPECT_TRUE(in.good())
        << "fixture missing: " << p
        << " (regenerate ONLY to add fixtures, never to make a failing read pass)";
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

void write_text(const fs::path& p, const std::string& text) {
    std::ofstream out(p, std::ios::trunc);
    ASSERT_TRUE(out.good()) << p;
    out << text;
}

// The representative definitions the fixtures were written from. They use
// every field the v1 format carries; a new field added later gets a NEW
// fixture rather than a change to these.
clink::sql::TableDef reference_table() {
    clink::sql::TableDef def;
    def.name = "orders";
    def.columns = {
        {"id", arrow::int64()},
        {"customer", arrow::utf8()},
        {"amount", arrow::decimal128(18, 2)},
        {"weight", arrow::float64()},
        {"gift", arrow::boolean()},
    };
    def.properties = {
        {"connector", "kafka"},
        {"format", "json"},
        {"topic", "orders"},
        {"brokers", "localhost:9092"},
        {"event_time_column", "id"},
        {"watermark_lag_ms", "2000"},
        {"primary_key", "id"},
        {"mode", "upsert"},
    };
    return def;
}

clink::sql::FunctionDef reference_function() {
    clink::sql::FunctionDef def;
    def.name = "with_tax";
    def.language = "sql";
    def.arg_names = {"amount"};
    def.arg_types = {"int64"};
    def.return_type = "int64";
    def.definitions = {"amount + amount / 10"};
    def.module_b64 = "";
    def.kind = "scalar";
    return def;
}

clink::sql::ModelDef reference_model() {
    clink::sql::ModelDef def;
    def.name = "sentiment";
    def.input_columns = {{"body", arrow::utf8()}};
    def.output_columns = {{"label", arrow::utf8()}, {"score", arrow::float64()}};
    def.properties = {{"provider", "http"},
                      {"endpoint", "http://localhost:8900/score"},
                      {"task", "classification"}};
    return def;
}

void expect_columns_equal(const std::vector<clink::sql::ColumnSpec>& a,
                          const std::vector<clink::sql::ColumnSpec>& b) {
    ASSERT_EQ(a.size(), b.size());
    for (std::size_t i = 0; i < a.size(); ++i) {
        EXPECT_EQ(a[i].name, b[i].name);
        EXPECT_TRUE(a[i].type->Equals(*b[i].type))
            << a[i].type->ToString() << " vs " << b[i].type->ToString();
    }
}

TEST(SqlCatalogFixtures, TableV1StaysLoadable) {
    const auto path = fixture_path("catalog-table-v1.json");
    const auto ref = reference_table();
    if (regen()) {
        write_text(path, clink::sql::Catalog::to_json(ref));
    }
    const auto loaded = clink::sql::Catalog::from_json(read_text(path));
    EXPECT_EQ(loaded.name, ref.name);
    expect_columns_equal(loaded.columns, ref.columns);
    EXPECT_EQ(loaded.properties, ref.properties);
    EXPECT_TRUE(loaded.is_upsert());
}

TEST(SqlCatalogFixtures, FunctionV1StaysLoadable) {
    const auto path = fixture_path("catalog-function-v1.json");
    const auto ref = reference_function();
    if (regen()) {
        write_text(path, clink::sql::Catalog::to_json(ref));
    }
    const auto loaded = clink::sql::Catalog::function_from_json(read_text(path));
    EXPECT_EQ(loaded.name, ref.name);
    EXPECT_EQ(loaded.language, ref.language);
    EXPECT_EQ(loaded.arg_names, ref.arg_names);
    EXPECT_EQ(loaded.arg_types, ref.arg_types);
    EXPECT_EQ(loaded.return_type, ref.return_type);
    EXPECT_EQ(loaded.definitions, ref.definitions);
    EXPECT_EQ(loaded.module_b64, ref.module_b64);
    EXPECT_EQ(loaded.kind, ref.kind);
}

TEST(SqlCatalogFixtures, ModelV1StaysLoadable) {
    const auto path = fixture_path("catalog-model-v1.json");
    const auto ref = reference_model();
    if (regen()) {
        write_text(path, clink::sql::Catalog::to_json(ref));
    }
    const auto loaded = clink::sql::Catalog::model_from_json(read_text(path));
    EXPECT_EQ(loaded.name, ref.name);
    expect_columns_equal(loaded.input_columns, ref.input_columns);
    expect_columns_equal(loaded.output_columns, ref.output_columns);
    EXPECT_EQ(loaded.properties, ref.properties);
}

// Readers must ignore unknown keys: that is what makes adding a key a
// compatible change. A fixture from a FUTURE release carrying keys this
// build has never heard of still loads.
TEST(SqlCatalogFixtures, UnknownKeysAreIgnoredOnLoad) {
    const auto table = clink::sql::Catalog::from_json(
        R"({"name":"t","columns":[{"name":"a","type":"BIGINT"}],"properties":{"connector":"file"},"added_in_1_7":true})");
    EXPECT_EQ(table.name, "t");
    ASSERT_EQ(table.columns.size(), 1u);
}

}  // namespace
