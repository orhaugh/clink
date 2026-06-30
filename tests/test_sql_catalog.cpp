#include <atomic>
#include <chrono>
#include <filesystem>

#include <gtest/gtest.h>

#include "clink/sql/catalog.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/type.hpp"

#include "arrow/api.h"

namespace clink::sql {

namespace {

ast::TypeName tn(std::string schema, std::string name, std::vector<int> typmods = {}) {
    return ast::TypeName{std::move(schema), std::move(name), std::move(typmods), {}};
}

}  // namespace

// --- Type bridge ---------------------------------------------------

TEST(SqlType, IntegerFamily) {
    EXPECT_TRUE(sql_type_to_arrow(tn("pg_catalog", "int8"))->Equals(*arrow::int64()));
    EXPECT_TRUE(sql_type_to_arrow(tn("pg_catalog", "int4"))->Equals(*arrow::int32()));
    EXPECT_TRUE(sql_type_to_arrow(tn("pg_catalog", "int2"))->Equals(*arrow::int16()));
}

TEST(SqlType, StringFamily) {
    EXPECT_TRUE(sql_type_to_arrow(tn("", "text"))->Equals(*arrow::utf8()));
    EXPECT_TRUE(sql_type_to_arrow(tn("pg_catalog", "varchar"))->Equals(*arrow::utf8()));
}

TEST(SqlType, BoolAndFloat) {
    EXPECT_TRUE(sql_type_to_arrow(tn("pg_catalog", "bool"))->Equals(*arrow::boolean()));
    EXPECT_TRUE(sql_type_to_arrow(tn("pg_catalog", "float8"))->Equals(*arrow::float64()));
    EXPECT_TRUE(sql_type_to_arrow(tn("pg_catalog", "float4"))->Equals(*arrow::float32()));
}

TEST(SqlType, TimestampPrecisionToUnit) {
    EXPECT_TRUE(sql_type_to_arrow(tn("pg_catalog", "timestamp", {0}))
                    ->Equals(*arrow::timestamp(arrow::TimeUnit::SECOND)));
    EXPECT_TRUE(sql_type_to_arrow(tn("pg_catalog", "timestamp", {3}))
                    ->Equals(*arrow::timestamp(arrow::TimeUnit::MILLI)));
    EXPECT_TRUE(sql_type_to_arrow(tn("pg_catalog", "timestamp", {6}))
                    ->Equals(*arrow::timestamp(arrow::TimeUnit::MICRO)));
    EXPECT_TRUE(sql_type_to_arrow(tn("pg_catalog", "timestamp", {9}))
                    ->Equals(*arrow::timestamp(arrow::TimeUnit::NANO)));
    // Default (no typmod) lands at MICRO, matching PG's default of (6).
    EXPECT_TRUE(sql_type_to_arrow(tn("pg_catalog", "timestamp"))
                    ->Equals(*arrow::timestamp(arrow::TimeUnit::MICRO)));
}

TEST(SqlType, TimestampTzCarriesUtcTimezone) {
    auto t = sql_type_to_arrow(tn("pg_catalog", "timestamptz", {3}));
    ASSERT_EQ(t->id(), arrow::Type::TIMESTAMP);
    const auto& ts = static_cast<const arrow::TimestampType&>(*t);
    EXPECT_EQ(ts.unit(), arrow::TimeUnit::MILLI);
    EXPECT_EQ(ts.timezone(), "UTC");
}

TEST(SqlType, DecimalPrecisionAndScale) {
    auto t = sql_type_to_arrow(tn("pg_catalog", "numeric", {18, 4}));
    ASSERT_EQ(t->id(), arrow::Type::DECIMAL128);
    const auto& d = static_cast<const arrow::Decimal128Type&>(*t);
    EXPECT_EQ(d.precision(), 18);
    EXPECT_EQ(d.scale(), 4);
}

TEST(SqlType, RejectsUnknownTypeWithTranslationError) {
    EXPECT_THROW(sql_type_to_arrow(tn("pg_catalog", "xml")), TranslationError);
}

TEST(SqlType, RejectsForeignSchema) {
    EXPECT_THROW(sql_type_to_arrow(tn("public", "int8")), TranslationError);
}

TEST(SqlType, ArrowToSqlRoundTripsCanonicalTypes) {
    EXPECT_EQ(arrow_to_sql_type_string(*arrow::int64()), "BIGINT");
    EXPECT_EQ(arrow_to_sql_type_string(*arrow::utf8()), "VARCHAR");
    EXPECT_EQ(arrow_to_sql_type_string(*arrow::timestamp(arrow::TimeUnit::MILLI)), "TIMESTAMP(3)");
    EXPECT_EQ(arrow_to_sql_type_string(*arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")),
              "TIMESTAMP(3) WITH TIME ZONE");
    EXPECT_EQ(arrow_to_sql_type_string(*arrow::decimal128(18, 4)), "DECIMAL(18, 4)");
}

// --- Catalog -------------------------------------------------------

TEST(SqlCatalog, RegisterAndLookupFromCreateStmt) {
    auto script = parse(
        "CREATE TABLE clicks ("
        "  user_id BIGINT,"
        "  ts TIMESTAMP(3),"
        "  url TEXT"
        ") WITH (connector='kafka', topic='clicks', bootstrap='localhost:9092')");
    const auto& create = std::get<ast::CreateTableStmt>(script.statements[0]);

    Catalog cat;
    cat.register_table(create);

    const auto* def = cat.get_table("clicks");
    ASSERT_NE(def, nullptr);
    EXPECT_EQ(def->name, "clicks");
    ASSERT_EQ(def->columns.size(), 3u);
    EXPECT_EQ(def->columns[0].name, "user_id");
    EXPECT_TRUE(def->columns[0].type->Equals(*arrow::int64()));
    EXPECT_EQ(def->columns[1].name, "ts");
    EXPECT_TRUE(def->columns[1].type->Equals(*arrow::timestamp(arrow::TimeUnit::MILLI)));
    EXPECT_EQ(def->columns[2].name, "url");
    EXPECT_TRUE(def->columns[2].type->Equals(*arrow::utf8()));

    EXPECT_EQ(def->properties.at("connector"), "kafka");
    EXPECT_EQ(def->properties.at("topic"), "clicks");
    EXPECT_EQ(def->properties.at("bootstrap"), "localhost:9092");
}

TEST(SqlCatalog, RejectsDuplicateRegistration) {
    auto script = parse("CREATE TABLE t (a BIGINT) WITH (connector='kafka', topic='x')");
    const auto& create = std::get<ast::CreateTableStmt>(script.statements[0]);
    Catalog cat;
    cat.register_table(create);
    EXPECT_THROW(cat.register_table(create), TranslationError);
}

TEST(SqlCatalog, CreateIfNotExistsSkipsExistingTable) {
    auto s1 = parse("CREATE TABLE t (a BIGINT) WITH (connector='kafka', topic='x')");
    auto s2 = parse(
        "CREATE TABLE IF NOT EXISTS t (a BIGINT, b TEXT) WITH (connector='kafka', topic='y')");
    Catalog cat;
    cat.register_table(std::get<ast::CreateTableStmt>(s1.statements[0]));
    // IF NOT EXISTS on an already-registered table: no throw, and the
    // original definition is kept (not overwritten by the second DDL).
    EXPECT_NO_THROW(cat.register_table(std::get<ast::CreateTableStmt>(s2.statements[0])));
    const auto* def = cat.get_table("t");
    ASSERT_NE(def, nullptr);
    EXPECT_EQ(def->columns.size(), 1u);
    EXPECT_EQ(def->properties.at("topic"), "x");
}

TEST(SqlCatalog, DropTableRemovesEntry) {
    auto script = parse("CREATE TABLE t (a BIGINT) WITH (connector='kafka', topic='x')");
    const auto& create = std::get<ast::CreateTableStmt>(script.statements[0]);
    Catalog cat;
    cat.register_table(create);
    ASSERT_NE(cat.get_table("t"), nullptr);
    EXPECT_TRUE(cat.drop_table("t"));
    EXPECT_EQ(cat.get_table("t"), nullptr);
    EXPECT_FALSE(cat.drop_table("t"));  // already gone
}

namespace {
// Apply an ALTER TABLE statement (parsed from SQL) to the catalog.
void alter(Catalog& cat, const char* sql) {
    cat.alter_table(std::get<ast::AlterTableStmt>(parse(sql).statements[0]));
}
void make_table(Catalog& cat, const char* sql) {
    cat.register_table(std::get<ast::CreateTableStmt>(parse(sql).statements[0]));
}
std::vector<std::string> col_names(const TableDef& def) {
    std::vector<std::string> out;
    for (const auto& c : def.columns) {
        out.push_back(c.name);
    }
    return out;
}
}  // namespace

TEST(SqlCatalog, AlterTableAddAndDropColumn) {
    Catalog cat;
    make_table(cat, "CREATE TABLE t (a BIGINT, b BIGINT) WITH (connector='kafka', topic='x')");
    alter(cat, "ALTER TABLE t ADD COLUMN c TEXT");
    EXPECT_EQ(col_names(*cat.get_table("t")), (std::vector<std::string>{"a", "b", "c"}));
    alter(cat, "ALTER TABLE t DROP COLUMN b");
    EXPECT_EQ(col_names(*cat.get_table("t")), (std::vector<std::string>{"a", "c"}));
    // Multiple commands apply in order.
    alter(cat, "ALTER TABLE t ADD COLUMN d BIGINT, DROP COLUMN a");
    EXPECT_EQ(col_names(*cat.get_table("t")), (std::vector<std::string>{"c", "d"}));
}

TEST(SqlCatalog, AlterTableDuplicateAndAbsentColumnGuards) {
    Catalog cat;
    make_table(cat, "CREATE TABLE t (a BIGINT) WITH (connector='kafka', topic='x')");
    EXPECT_THROW(alter(cat, "ALTER TABLE t ADD COLUMN a BIGINT"), TranslationError);
    EXPECT_THROW(alter(cat, "ALTER TABLE t DROP COLUMN nope"), TranslationError);
    // IF (NOT) EXISTS makes both a no-op.
    EXPECT_NO_THROW(alter(cat, "ALTER TABLE t ADD COLUMN IF NOT EXISTS a BIGINT"));
    EXPECT_NO_THROW(alter(cat, "ALTER TABLE t DROP COLUMN IF EXISTS nope"));
    EXPECT_EQ(col_names(*cat.get_table("t")), (std::vector<std::string>{"a"}));
}

TEST(SqlCatalog, AlterTableProtectsEventTimeAndPrimaryKeyColumns) {
    Catalog cat;
    make_table(cat,
               "CREATE TABLE t (id BIGINT, ts BIGINT, v BIGINT) "
               "WITH (connector='kafka', topic='x', event_time_column='ts', primary_key='id')");
    EXPECT_THROW(alter(cat, "ALTER TABLE t DROP COLUMN ts"), TranslationError);  // event time
    EXPECT_THROW(alter(cat, "ALTER TABLE t DROP COLUMN id"), TranslationError);  // primary key
    EXPECT_NO_THROW(alter(cat, "ALTER TABLE t DROP COLUMN v"));                  // a plain column
    EXPECT_EQ(col_names(*cat.get_table("t")), (std::vector<std::string>{"id", "ts"}));
}

TEST(SqlCatalog, AlterTableRejectsNonTableAndUnknown) {
    Catalog cat;
    // A materialized-view-kind TableDef is not ALTER TABLE-able.
    TableDef mv;
    mv.name = "mv";
    mv.properties["view_kind"] = "materialized";
    mv.columns.push_back(ColumnSpec{"a", arrow::int64()});
    cat.register_table(std::move(mv));
    EXPECT_THROW(alter(cat, "ALTER TABLE mv ADD COLUMN b BIGINT"), TranslationError);
    // Unknown table: throws, unless IF EXISTS.
    EXPECT_THROW(alter(cat, "ALTER TABLE nope ADD COLUMN b BIGINT"), TranslationError);
    EXPECT_NO_THROW(alter(cat, "ALTER TABLE IF EXISTS nope ADD COLUMN b BIGINT"));
}

// A multi-command ALTER is atomic: a failing later command rolls back the whole
// statement (the earlier ADD is not left applied).
TEST(SqlCatalog, AlterTableIsAtomic) {
    Catalog cat;
    make_table(cat, "CREATE TABLE t (a BIGINT) WITH (connector='kafka', topic='x')");
    EXPECT_THROW(alter(cat, "ALTER TABLE t ADD COLUMN c BIGINT, DROP COLUMN nope"),
                 TranslationError);
    // c must NOT have been added (the statement rolled back).
    EXPECT_EQ(col_names(*cat.get_table("t")), (std::vector<std::string>{"a"}));
}

TEST(SqlCatalog, AlterTableRejectsDroppingLastColumn) {
    Catalog cat;
    make_table(cat, "CREATE TABLE t (a BIGINT) WITH (connector='kafka', topic='x')");
    EXPECT_THROW(alter(cat, "ALTER TABLE t DROP COLUMN a"), TranslationError);
}

// drop_object enforces Postgres object-kind matching: DROP TABLE rejects a
// materialized view and DROP MATERIALIZED VIEW rejects a plain table, neither
// removing anything; the matching kind drops.
TEST(SqlCatalog, DropObjectEnforcesObjectKind) {
    Catalog cat;
    cat.register_table(std::get<ast::CreateTableStmt>(
        parse("CREATE TABLE t (a BIGINT) WITH (connector='kafka', topic='x')").statements[0]));
    // A materialized view is a backing TableDef tagged view_kind='materialized'.
    TableDef mv;
    mv.name = "mv";
    mv.properties["view_kind"] = "materialized";
    mv.properties["connector"] = "blackhole";
    cat.register_table(std::move(mv));

    using DR = Catalog::DropResult;
    // Mismatched kinds are rejected and nothing is removed.
    EXPECT_EQ(cat.drop_object("t", ast::DropKind::MaterializedView), DR::KindMismatch);
    EXPECT_EQ(cat.drop_object("mv", ast::DropKind::Table), DR::KindMismatch);
    EXPECT_NE(cat.get_table("t"), nullptr);
    EXPECT_NE(cat.get_table("mv"), nullptr);
    // Unknown name.
    EXPECT_EQ(cat.drop_object("nope", ast::DropKind::Table), DR::NotFound);
    // Matching kinds drop.
    EXPECT_EQ(cat.drop_object("mv", ast::DropKind::MaterializedView), DR::Dropped);
    EXPECT_EQ(cat.get_table("mv"), nullptr);
    EXPECT_EQ(cat.drop_object("t", ast::DropKind::Table), DR::Dropped);
    EXPECT_EQ(cat.get_table("t"), nullptr);
    // Gone now.
    EXPECT_EQ(cat.drop_object("mv", ast::DropKind::MaterializedView), DR::NotFound);
}

TEST(SqlCatalog, ListTablesPreservesRegistrationOrder) {
    Catalog cat;
    auto reg = [&](const char* sql) {
        auto s = parse(sql);
        cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    };
    reg("CREATE TABLE z (a BIGINT) WITH (connector='kafka', topic='x')");
    reg("CREATE TABLE a (a BIGINT) WITH (connector='kafka', topic='x')");
    reg("CREATE TABLE m (a BIGINT) WITH (connector='kafka', topic='x')");
    auto names = cat.list_tables();
    ASSERT_EQ(names.size(), 3u);
    EXPECT_EQ(names[0], "z");
    EXPECT_EQ(names[1], "a");
    EXPECT_EQ(names[2], "m");
}

TEST(SqlCatalog, TypeErrorInColumnPropagatesAsTranslationError) {
    auto script = parse("CREATE TABLE t (a XML) WITH (connector='kafka', topic='x')");
    const auto& create = std::get<ast::CreateTableStmt>(script.statements[0]);
    Catalog cat;
    EXPECT_THROW(cat.register_table(create), TranslationError);
}

// --- Persistence ---------------------------------------------------

namespace {

std::filesystem::path make_temp_catalog_dir() {
    static std::atomic<int> counter{0};
    auto base = std::filesystem::temp_directory_path() /
                ("clink_catalog_test_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "_" +
                 std::to_string(counter.fetch_add(1)));
    std::filesystem::remove_all(base);
    return base;
}

}  // namespace

TEST(SqlCatalog, JsonRoundTripPreservesTableDef) {
    auto script = parse(
        "CREATE TABLE clicks (user_id BIGINT, ts TIMESTAMP(3), url TEXT) "
        "WITH (connector='kafka', topic='clicks', bootstrap='localhost:9092')");
    const auto& create = std::get<ast::CreateTableStmt>(script.statements[0]);
    Catalog src;
    src.register_table(create);

    auto json = Catalog::to_json(*src.get_table("clicks"));
    auto roundtripped = Catalog::from_json(json);

    EXPECT_EQ(roundtripped.name, "clicks");
    ASSERT_EQ(roundtripped.columns.size(), 3u);
    EXPECT_EQ(roundtripped.columns[0].name, "user_id");
    EXPECT_TRUE(roundtripped.columns[0].type->Equals(*arrow::int64()));
    EXPECT_EQ(roundtripped.columns[1].name, "ts");
    EXPECT_TRUE(roundtripped.columns[1].type->Equals(*arrow::timestamp(arrow::TimeUnit::MILLI)));
    EXPECT_EQ(roundtripped.columns[2].name, "url");
    EXPECT_TRUE(roundtripped.columns[2].type->Equals(*arrow::utf8()));
    EXPECT_EQ(roundtripped.properties.at("topic"), "clicks");
    EXPECT_EQ(roundtripped.properties.at("bootstrap"), "localhost:9092");
}

TEST(SqlCatalog, PersistenceDirAutoSavesAndReloads) {
    auto dir = make_temp_catalog_dir();
    // Write, reload, see same entries.
    {
        Catalog cat;
        cat.set_persistence_dir(dir.string());
        cat.register_table(std::get<ast::CreateTableStmt>(
            parse("CREATE TABLE t1 (a BIGINT) WITH (connector='kafka', topic='x')").statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(
            parse("CREATE TABLE t2 (b TEXT) WITH (connector='file', path='/tmp/y')")
                .statements[0]));

        EXPECT_TRUE(std::filesystem::exists(dir / "t1.json"));
        EXPECT_TRUE(std::filesystem::exists(dir / "t2.json"));
    }
    // Reload in a fresh Catalog
    {
        Catalog cat;
        cat.load_from_dir(dir.string());
        auto names = cat.list_tables();
        ASSERT_EQ(names.size(), 2u);
        // Order is alphabetical by file
        EXPECT_EQ(names[0], "t1");
        EXPECT_EQ(names[1], "t2");

        const auto* t1 = cat.get_table("t1");
        ASSERT_NE(t1, nullptr);
        EXPECT_TRUE(t1->columns[0].type->Equals(*arrow::int64()));
        EXPECT_EQ(t1->properties.at("topic"), "x");
    }
    std::filesystem::remove_all(dir);
}

TEST(SqlCatalog, DropAlsoRemovesPersistedFile) {
    auto dir = make_temp_catalog_dir();
    Catalog cat;
    cat.set_persistence_dir(dir.string());
    cat.register_table(std::get<ast::CreateTableStmt>(
        parse("CREATE TABLE t (a BIGINT) WITH (connector='kafka', topic='x')").statements[0]));
    EXPECT_TRUE(std::filesystem::exists(dir / "t.json"));

    EXPECT_TRUE(cat.drop_table("t"));
    EXPECT_FALSE(std::filesystem::exists(dir / "t.json"));
    std::filesystem::remove_all(dir);
}

TEST(SqlCatalog, LoadFromMissingDirLeavesCatalogEmpty) {
    Catalog cat;
    EXPECT_NO_THROW(cat.load_from_dir("/tmp/clink_definitely_does_not_exist_xyz"));
    EXPECT_TRUE(cat.list_tables().empty());
}

TEST(SqlCatalog, JsonRoundTripsAcrossAllSupportedTypes) {
    auto script = parse(
        "CREATE TABLE every_type (a BIGINT, b INTEGER, c SMALLINT, d BOOLEAN, "
        "e DOUBLE PRECISION, f REAL, g TEXT, h TIMESTAMP(3), "
        "i TIMESTAMP(6), j DATE, k TIME, l DECIMAL(18, 4)) "
        "WITH (connector='file', path='/tmp/x')");
    const auto& create = std::get<ast::CreateTableStmt>(script.statements[0]);
    Catalog src;
    src.register_table(create);

    auto json = Catalog::to_json(*src.get_table("every_type"));
    auto round = Catalog::from_json(json);
    ASSERT_EQ(round.columns.size(), src.get_table("every_type")->columns.size());
    for (std::size_t i = 0; i < round.columns.size(); ++i) {
        EXPECT_TRUE(round.columns[i].type->Equals(*src.get_table("every_type")->columns[i].type))
            << "column " << i << " (" << round.columns[i].name << ") differs";
    }
}

}  // namespace clink::sql
