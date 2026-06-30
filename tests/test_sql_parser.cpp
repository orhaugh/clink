#include <variant>

#include <gtest/gtest.h>

#include "clink/sql/parser.hpp"

namespace clink::sql {

TEST(SqlParser, ParsesTrivialSelect) {
    auto script = parse("SELECT 1");
    ASSERT_EQ(script.statements.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<ast::SelectStmt>(script.statements[0]));
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_EQ(sel.target_list.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<ast::IntLiteral>(sel.target_list[0].expr));
    EXPECT_EQ(std::get<ast::IntLiteral>(sel.target_list[0].expr).value, 1);
    EXPECT_TRUE(sel.from_clause.empty());
}

TEST(SqlParser, ParsesSelectColumnList) {
    auto script = parse("SELECT a, b, c FROM t");
    ASSERT_EQ(script.statements.size(), 1u);
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_EQ(sel.target_list.size(), 3u);
    const auto& a = std::get<ast::ColumnRef>(sel.target_list[0].expr);
    EXPECT_EQ(a.parts, std::vector<std::string>{"a"});
    EXPECT_FALSE(a.is_star);
    EXPECT_EQ(std::get<ast::ColumnRef>(sel.target_list[1].expr).parts.front(), "b");
    EXPECT_EQ(std::get<ast::ColumnRef>(sel.target_list[2].expr).parts.front(), "c");
    ASSERT_EQ(sel.from_clause.size(), 1u);
    EXPECT_EQ(sel.from_clause[0].name, "t");
    EXPECT_FALSE(sel.from_clause[0].alias.has_value());
}

TEST(SqlParser, ParsesQualifiedColumnRef) {
    auto script = parse("SELECT t.a FROM t");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    const auto& ref = std::get<ast::ColumnRef>(sel.target_list[0].expr);
    ASSERT_EQ(ref.parts.size(), 2u);
    EXPECT_EQ(ref.parts[0], "t");
    EXPECT_EQ(ref.parts[1], "a");
}

TEST(SqlParser, ParsesSelectStar) {
    auto script = parse("SELECT * FROM t");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_EQ(sel.target_list.size(), 1u);
    const auto& ref = std::get<ast::ColumnRef>(sel.target_list[0].expr);
    EXPECT_TRUE(ref.is_star);
}

TEST(SqlParser, ParsesSelectItemAlias) {
    auto script = parse("SELECT a AS aa FROM t");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_TRUE(sel.target_list[0].alias.has_value());
    EXPECT_EQ(*sel.target_list[0].alias, "aa");
}

TEST(SqlParser, ParsesStringAndBoolLiterals) {
    auto script = parse("SELECT 'hi', TRUE, FALSE, NULL");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_EQ(sel.target_list.size(), 4u);
    EXPECT_EQ(std::get<ast::StringLiteral>(sel.target_list[0].expr).value, "hi");
    EXPECT_TRUE(std::get<ast::BoolLiteral>(sel.target_list[1].expr).value);
    EXPECT_FALSE(std::get<ast::BoolLiteral>(sel.target_list[2].expr).value);
    EXPECT_TRUE(std::holds_alternative<ast::NullLiteral>(sel.target_list[3].expr));
}

TEST(SqlParser, ParsesCreateTableWithStorageParams) {
    const auto* sql =
        "CREATE TABLE clicks ("
        "  user_id BIGINT,"
        "  ts TIMESTAMP(3),"
        "  url TEXT"
        ") WITH (connector='kafka', topic='clicks', bootstrap='localhost:9092')";
    auto script = parse(sql);
    ASSERT_EQ(script.statements.size(), 1u);
    const auto& ct = std::get<ast::CreateTableStmt>(script.statements[0]);
    EXPECT_EQ(ct.table_name, "clicks");
    ASSERT_EQ(ct.columns.size(), 3u);
    EXPECT_EQ(ct.columns[0].name, "user_id");
    EXPECT_EQ(ct.columns[0].type.name, "int8");
    EXPECT_EQ(ct.columns[0].type.schema, "pg_catalog");
    EXPECT_EQ(ct.columns[1].name, "ts");
    EXPECT_EQ(ct.columns[1].type.name, "timestamp");
    ASSERT_EQ(ct.columns[1].type.typmods.size(), 1u);
    EXPECT_EQ(ct.columns[1].type.typmods[0], 3);
    EXPECT_EQ(ct.columns[2].name, "url");
    EXPECT_EQ(ct.columns[2].type.name, "text");
    ASSERT_EQ(ct.options.size(), 3u);
    EXPECT_EQ(ct.options[0].key, "connector");
    EXPECT_EQ(ct.options[0].value, "kafka");
    EXPECT_EQ(ct.options[1].key, "topic");
    EXPECT_EQ(ct.options[1].value, "clicks");
    EXPECT_EQ(ct.options[2].key, "bootstrap");
    EXPECT_EQ(ct.options[2].value, "localhost:9092");
}

TEST(SqlParser, ParsesInsertIntoSelect) {
    auto script = parse("INSERT INTO sink SELECT user_id, url FROM clicks");
    ASSERT_EQ(script.statements.size(), 1u);
    const auto& ins = std::get<ast::InsertStmt>(script.statements[0]);
    EXPECT_EQ(ins.target.name, "sink");
    ASSERT_EQ(ins.select.target_list.size(), 2u);
    EXPECT_EQ(std::get<ast::ColumnRef>(ins.select.target_list[0].expr).parts.front(), "user_id");
    EXPECT_EQ(std::get<ast::ColumnRef>(ins.select.target_list[1].expr).parts.front(), "url");
    ASSERT_EQ(ins.select.from_clause.size(), 1u);
    EXPECT_EQ(ins.select.from_clause[0].name, "clicks");
}

TEST(SqlParser, ParsesArrayLiteral) {
    auto script = parse("SELECT ARRAY[10, 20, 30] AS a");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_EQ(sel.target_list.size(), 1u);
    ASSERT_TRUE(
        std::holds_alternative<std::unique_ptr<ast::ArrayLiteral>>(sel.target_list[0].expr));
    const auto& arr = *std::get<std::unique_ptr<ast::ArrayLiteral>>(sel.target_list[0].expr);
    ASSERT_EQ(arr.elements.size(), 3u);
    EXPECT_EQ(std::get<ast::IntLiteral>(arr.elements[0]).value, 10);
    EXPECT_EQ(std::get<ast::IntLiteral>(arr.elements[2]).value, 30);
}

TEST(SqlParser, ParsesEmptyArrayLiteral) {
    auto script = parse("SELECT ARRAY[] AS a");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    const auto& arr = *std::get<std::unique_ptr<ast::ArrayLiteral>>(sel.target_list[0].expr);
    EXPECT_TRUE(arr.elements.empty());
}

TEST(SqlParser, ParsesArraySubscript) {
    auto script = parse("SELECT (ARRAY[10, 20, 30])[2] AS second");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<ast::Subscript>>(sel.target_list[0].expr));
    const auto& sub = *std::get<std::unique_ptr<ast::Subscript>>(sel.target_list[0].expr);
    EXPECT_TRUE(std::holds_alternative<std::unique_ptr<ast::ArrayLiteral>>(sub.base));
    EXPECT_EQ(std::get<ast::IntLiteral>(sub.index).value, 2);
}

TEST(SqlParser, ParsesChainedSubscriptLeftToRight) {
    auto script = parse("SELECT a[1][2] FROM t");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    // a[1][2] nests as Subscript(Subscript(a, 1), 2).
    const auto& outer = *std::get<std::unique_ptr<ast::Subscript>>(sel.target_list[0].expr);
    EXPECT_EQ(std::get<ast::IntLiteral>(outer.index).value, 2);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<ast::Subscript>>(outer.base));
    const auto& inner = *std::get<std::unique_ptr<ast::Subscript>>(outer.base);
    EXPECT_EQ(std::get<ast::IntLiteral>(inner.index).value, 1);
}

TEST(SqlParser, RejectsArraySlice) {
    // libpg_query parses a[1:2] fine; the ast_builder rejects the slice.
    EXPECT_THROW(parse("SELECT a[1:2] FROM t"), TranslationError);
}

TEST(SqlParser, ParsesArrayColumnType) {
    auto script = parse("CREATE TABLE t (tags INT ARRAY, names TEXT[])");
    const auto& ct = std::get<ast::CreateTableStmt>(script.statements[0]);
    ASSERT_EQ(ct.columns.size(), 2u);
    EXPECT_EQ(ct.columns[0].type.name, "int4");
    EXPECT_EQ(ct.columns[0].type.array_ndims, 1);
    EXPECT_EQ(ct.columns[1].type.name, "text");
    EXPECT_EQ(ct.columns[1].type.array_ndims, 1);
}

TEST(SqlParser, ParsesRowConstructor) {
    auto script = parse("SELECT ROW(1, 'a', 2) AS r");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_TRUE(
        std::holds_alternative<std::unique_ptr<ast::RowConstructor>>(sel.target_list[0].expr));
    const auto& rc = *std::get<std::unique_ptr<ast::RowConstructor>>(sel.target_list[0].expr);
    ASSERT_EQ(rc.fields.size(), 3u);
    EXPECT_EQ(std::get<ast::IntLiteral>(rc.fields[0]).value, 1);
    EXPECT_EQ(std::get<ast::StringLiteral>(rc.fields[1]).value, "a");
}

TEST(SqlParser, ParsesFieldAccess) {
    auto script = parse("SELECT (r).f FROM t");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<ast::FieldAccess>>(sel.target_list[0].expr));
    const auto& fa = *std::get<std::unique_ptr<ast::FieldAccess>>(sel.target_list[0].expr);
    EXPECT_EQ(fa.field, "f");
    EXPECT_TRUE(std::holds_alternative<ast::ColumnRef>(fa.base));
}

TEST(SqlParser, ParsesChainedFieldAccess) {
    auto script = parse("SELECT (r).a.b FROM t");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    // (r).a.b nests as FieldAccess(FieldAccess(r, a), b).
    const auto& outer = *std::get<std::unique_ptr<ast::FieldAccess>>(sel.target_list[0].expr);
    EXPECT_EQ(outer.field, "b");
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<ast::FieldAccess>>(outer.base));
    EXPECT_EQ(std::get<std::unique_ptr<ast::FieldAccess>>(outer.base)->field, "a");
}

TEST(SqlParser, ParsesMixedFieldAndSubscript) {
    auto script = parse("SELECT (r).a[1] FROM t");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    // (r).a[1] nests as Subscript(FieldAccess(r, a), 1).
    const auto& sub = *std::get<std::unique_ptr<ast::Subscript>>(sel.target_list[0].expr);
    EXPECT_EQ(std::get<ast::IntLiteral>(sub.index).value, 1);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<ast::FieldAccess>>(sub.base));
    EXPECT_EQ(std::get<std::unique_ptr<ast::FieldAccess>>(sub.base)->field, "a");
}

TEST(SqlParser, RejectsRowFieldStar) {
    // (r).* field expansion is rejected (we have no row type to expand).
    EXPECT_THROW(parse("SELECT (r).* FROM t"), TranslationError);
}

TEST(SqlParser, RejectsImplicitRowConstructor) {
    // (a, b) outside an IN-subquery collides with the multi-column IN
    // paren-list; require explicit ROW(...) instead.
    EXPECT_THROW(parse("SELECT (1, 2) AS r"), TranslationError);
}

TEST(SqlParser, RejectsAggregateOrderBy) {
    // Within-aggregate ORDER BY is not implemented; reject rather than
    // silently drop the sort.
    EXPECT_THROW(parse("SELECT array_agg(x ORDER BY y) FROM t"), TranslationError);
}

TEST(SqlParser, ParsesNestedArrayColumnType) {
    auto script = parse("CREATE TABLE t (grid INT[][])");
    const auto& ct = std::get<ast::CreateTableStmt>(script.statements[0]);
    ASSERT_EQ(ct.columns.size(), 1u);
    EXPECT_EQ(ct.columns[0].type.name, "int4");
    EXPECT_EQ(ct.columns[0].type.array_ndims, 2);
}

TEST(SqlParser, RejectsSyntaxError) {
    EXPECT_THROW(parse("SELECT FROM WHERE"), ParseError);
}

TEST(SqlParser, ParseErrorCarriesCursorPosition) {
    try {
        parse("SELECT * FROOM t");
        FAIL() << "expected ParseError";
    } catch (const ParseError& e) {
        EXPECT_GT(e.cursor_position(), 0);
    }
}

TEST(SqlParser, ParsesWhereClauseAsBinaryOp) {
    auto script = parse("SELECT a FROM t WHERE a = 'x'");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_TRUE(sel.where_clause.has_value());
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<ast::BinaryOp>>(*sel.where_clause));
    const auto& bin = *std::get<std::unique_ptr<ast::BinaryOp>>(*sel.where_clause);
    EXPECT_EQ(bin.op, ast::BinOp::Eq);
    EXPECT_TRUE(std::holds_alternative<ast::ColumnRef>(bin.left));
    EXPECT_TRUE(std::holds_alternative<ast::StringLiteral>(bin.right));
}

TEST(SqlParser, ParsesWhereClauseWithAndOrNot) {
    auto script = parse("SELECT a FROM t WHERE a = 'x' AND a != 'y' OR NOT (a = 'z')");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_TRUE(sel.where_clause.has_value());
    // Top-level should be OR (left-associative parse: (AND ... AND ...) OR (NOT ...)).
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<ast::LogicalOp>>(*sel.where_clause));
    EXPECT_EQ(std::get<std::unique_ptr<ast::LogicalOp>>(*sel.where_clause)->op,
              ast::LogicalKind::Or);
}

TEST(SqlParser, ParsesIsNullAndIsNotNull) {
    auto script = parse(
        "SELECT a FROM t WHERE a IS NULL;"
        "SELECT a FROM t WHERE a IS NOT NULL");
    ASSERT_EQ(script.statements.size(), 2u);
    {
        const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
        ASSERT_TRUE(sel.where_clause.has_value());
        const auto& n = *std::get<std::unique_ptr<ast::IsNullOp>>(*sel.where_clause);
        EXPECT_FALSE(n.negated);
    }
    {
        const auto& sel = std::get<ast::SelectStmt>(script.statements[1]);
        const auto& n = *std::get<std::unique_ptr<ast::IsNullOp>>(*sel.where_clause);
        EXPECT_TRUE(n.negated);
    }
}

TEST(SqlParser, ParsesAllComparisonOperators) {
    for (auto* expr :
         {"a = 'x'", "a <> 'x'", "a != 'x'", "a < 'x'", "a > 'x'", "a <= 'x'", "a >= 'x'"}) {
        std::string sql = std::string("SELECT a FROM t WHERE ") + expr;
        auto script = parse(sql);
        const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
        ASSERT_TRUE(sel.where_clause.has_value()) << "for " << expr;
        ASSERT_TRUE(std::holds_alternative<std::unique_ptr<ast::BinaryOp>>(*sel.where_clause))
            << "for " << expr;
    }
}

TEST(SqlParser, ParsesDropTable) {
    auto script = parse("DROP TABLE foo");
    ASSERT_EQ(script.statements.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<ast::DropTableStmt>(script.statements[0]));
    const auto& drop = std::get<ast::DropTableStmt>(script.statements[0]);
    ASSERT_EQ(drop.table_names.size(), 1u);
    EXPECT_EQ(drop.table_names[0], "foo");
    EXPECT_FALSE(drop.if_exists);
}

TEST(SqlParser, ParsesDropTableIfExists) {
    auto script = parse("DROP TABLE IF EXISTS foo");
    const auto& drop = std::get<ast::DropTableStmt>(script.statements[0]);
    ASSERT_EQ(drop.table_names.size(), 1u);
    EXPECT_EQ(drop.table_names[0], "foo");
    EXPECT_TRUE(drop.if_exists);
}

TEST(SqlParser, ParsesDropTableMultiple) {
    auto script = parse("DROP TABLE a, b, c");
    const auto& drop = std::get<ast::DropTableStmt>(script.statements[0]);
    EXPECT_EQ(drop.table_names, (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(drop.object_kind, ast::DropKind::Table);
}

TEST(SqlParser, ParsesDropMaterializedView) {
    auto script = parse("DROP MATERIALIZED VIEW mv");
    ASSERT_EQ(script.statements.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<ast::DropTableStmt>(script.statements[0]));
    const auto& drop = std::get<ast::DropTableStmt>(script.statements[0]);
    ASSERT_EQ(drop.table_names.size(), 1u);
    EXPECT_EQ(drop.table_names[0], "mv");
    EXPECT_EQ(drop.object_kind, ast::DropKind::MaterializedView);
    EXPECT_FALSE(drop.if_exists);
}

TEST(SqlParser, ParsesDropMaterializedViewIfExistsMultiple) {
    auto script = parse("DROP MATERIALIZED VIEW IF EXISTS a, b");
    const auto& drop = std::get<ast::DropTableStmt>(script.statements[0]);
    EXPECT_EQ(drop.table_names, (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(drop.object_kind, ast::DropKind::MaterializedView);
    EXPECT_TRUE(drop.if_exists);
}

TEST(SqlParser, ParsesDropView) {
    auto script = parse("DROP VIEW v");
    ASSERT_TRUE(std::holds_alternative<ast::DropTableStmt>(script.statements[0]));
    const auto& drop = std::get<ast::DropTableStmt>(script.statements[0]);
    EXPECT_EQ(drop.table_names, (std::vector<std::string>{"v"}));
    EXPECT_EQ(drop.object_kind, ast::DropKind::View);
}

TEST(SqlParser, ParsesCreateView) {
    auto script = parse("CREATE VIEW v AS SELECT a, b FROM t");
    ASSERT_EQ(script.statements.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<ast::CreateViewStmt>(script.statements[0]));
    const auto& cv = std::get<ast::CreateViewStmt>(script.statements[0]);
    EXPECT_EQ(cv.view_name, "v");
    EXPECT_FALSE(cv.or_replace);
}

TEST(SqlParser, ParsesCreateOrReplaceView) {
    auto script = parse("CREATE OR REPLACE VIEW v AS SELECT a FROM t");
    ASSERT_TRUE(std::holds_alternative<ast::CreateViewStmt>(script.statements[0]));
    EXPECT_TRUE(std::get<ast::CreateViewStmt>(script.statements[0]).or_replace);
}

// v1 does not support a column-alias list; the columns must be named in the SELECT.
TEST(SqlParser, RejectsCreateViewColumnAliases) {
    EXPECT_THROW(parse("CREATE VIEW v (x, y) AS SELECT a, b FROM t"), TranslationError);
}

TEST(SqlParser, ParsesAlterTableAddColumn) {
    auto script = parse("ALTER TABLE t ADD COLUMN c BIGINT");
    ASSERT_EQ(script.statements.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<ast::AlterTableStmt>(script.statements[0]));
    const auto& alt = std::get<ast::AlterTableStmt>(script.statements[0]);
    EXPECT_EQ(alt.table_name, "t");
    EXPECT_FALSE(alt.if_exists);
    ASSERT_EQ(alt.cmds.size(), 1u);
    EXPECT_EQ(alt.cmds[0].kind, ast::AlterTableCmd::Kind::AddColumn);
    EXPECT_EQ(alt.cmds[0].column_name, "c");
    EXPECT_FALSE(alt.cmds[0].missing_ok);
}

TEST(SqlParser, ParsesAlterTableDropColumn) {
    auto script = parse("ALTER TABLE t DROP COLUMN c");
    const auto& alt = std::get<ast::AlterTableStmt>(script.statements[0]);
    ASSERT_EQ(alt.cmds.size(), 1u);
    EXPECT_EQ(alt.cmds[0].kind, ast::AlterTableCmd::Kind::DropColumn);
    EXPECT_EQ(alt.cmds[0].column_name, "c");
}

TEST(SqlParser, ParsesAlterTableMultipleCommands) {
    auto script = parse("ALTER TABLE t ADD COLUMN a BIGINT, DROP COLUMN b");
    const auto& alt = std::get<ast::AlterTableStmt>(script.statements[0]);
    ASSERT_EQ(alt.cmds.size(), 2u);
    EXPECT_EQ(alt.cmds[0].kind, ast::AlterTableCmd::Kind::AddColumn);
    EXPECT_EQ(alt.cmds[0].column_name, "a");
    EXPECT_EQ(alt.cmds[1].kind, ast::AlterTableCmd::Kind::DropColumn);
    EXPECT_EQ(alt.cmds[1].column_name, "b");
}

TEST(SqlParser, ParsesAlterTableIfExistsAndIfNotExists) {
    auto s1 = parse("ALTER TABLE IF EXISTS t DROP COLUMN c");
    EXPECT_TRUE(std::get<ast::AlterTableStmt>(s1.statements[0]).if_exists);
    auto s2 = parse("ALTER TABLE t ADD COLUMN IF NOT EXISTS c BIGINT");
    EXPECT_TRUE(std::get<ast::AlterTableStmt>(s2.statements[0]).cmds[0].missing_ok);
}

// Other AlterTableCmd subtypes are rejected in v1.
TEST(SqlParser, RejectsUnsupportedAlterTableActions) {
    EXPECT_THROW(parse("ALTER TABLE t ALTER COLUMN c TYPE TEXT"), TranslationError);
    EXPECT_THROW(parse("ALTER TABLE t ADD PRIMARY KEY (c)"), TranslationError);
}

TEST(SqlParser, ParsesAlterTableRenameTo) {
    auto script = parse("ALTER TABLE t RENAME TO t2");
    ASSERT_EQ(script.statements.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<ast::RenameStmt>(script.statements[0]));
    const auto& rn = std::get<ast::RenameStmt>(script.statements[0]);
    EXPECT_EQ(rn.kind, ast::RenameStmt::Kind::Table);
    EXPECT_EQ(rn.table_name, "t");
    EXPECT_EQ(rn.new_name, "t2");
    EXPECT_FALSE(rn.if_exists);
}

TEST(SqlParser, ParsesAlterTableRenameColumn) {
    auto script = parse("ALTER TABLE t RENAME COLUMN a TO b");
    const auto& rn = std::get<ast::RenameStmt>(script.statements[0]);
    EXPECT_EQ(rn.kind, ast::RenameStmt::Kind::Column);
    EXPECT_EQ(rn.table_name, "t");
    EXPECT_EQ(rn.old_column, "a");
    EXPECT_EQ(rn.new_name, "b");
}

TEST(SqlParser, ParsesAlterTableRenameIfExists) {
    auto script = parse("ALTER TABLE IF EXISTS t RENAME TO t2");
    EXPECT_TRUE(std::get<ast::RenameStmt>(script.statements[0]).if_exists);
}

// v1 supports ALTER TABLE renames only; ALTER VIEW / MATERIALIZED VIEW are rejected.
TEST(SqlParser, RejectsAlterViewRename) {
    EXPECT_THROW(parse("ALTER MATERIALIZED VIEW m RENAME TO m2"), TranslationError);
    EXPECT_THROW(parse("ALTER VIEW v RENAME COLUMN a TO b"), TranslationError);
}

TEST(SqlParser, ParsesCreateTableIfNotExists) {
    auto script = parse(
        "CREATE TABLE IF NOT EXISTS t (a BIGINT) WITH (connector='file', format='json', "
        "path='/tmp/x')");
    const auto& ct = std::get<ast::CreateTableStmt>(script.statements[0]);
    EXPECT_TRUE(ct.if_not_exists);
    EXPECT_EQ(ct.table_name, "t");
}

TEST(SqlParser, ParsesShowTables) {
    auto script = parse("SHOW TABLES");
    ASSERT_EQ(script.statements.size(), 1u);
    EXPECT_TRUE(std::holds_alternative<ast::ShowTablesStmt>(script.statements[0]));
}

TEST(SqlParser, RejectsOtherShowVariants) {
    EXPECT_THROW(parse("SHOW search_path"), TranslationError);
}

TEST(SqlParser, RejectsDropIndex) {
    EXPECT_THROW(parse("DROP INDEX foo"), TranslationError);
}

TEST(SqlParser, ParsesExplainSelect) {
    auto script = parse("EXPLAIN SELECT a FROM t");
    ASSERT_EQ(script.statements.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<ast::ExplainStmt>>(script.statements[0]));
    const auto& exp = *std::get<std::unique_ptr<ast::ExplainStmt>>(script.statements[0]);
    EXPECT_TRUE(std::holds_alternative<ast::SelectStmt>(exp.query));
}

TEST(SqlParser, ParsesExplainInsert) {
    auto script = parse("EXPLAIN INSERT INTO t SELECT a FROM s");
    const auto& exp = *std::get<std::unique_ptr<ast::ExplainStmt>>(script.statements[0]);
    EXPECT_TRUE(std::holds_alternative<ast::InsertStmt>(exp.query));
}

TEST(SqlParser, MultipleStatementsParseSeparately) {
    auto script = parse(
        "CREATE TABLE t (a BIGINT) WITH (connector='kafka', topic='x');"
        "SELECT a FROM t");
    ASSERT_EQ(script.statements.size(), 2u);
    EXPECT_TRUE(std::holds_alternative<ast::CreateTableStmt>(script.statements[0]));
    EXPECT_TRUE(std::holds_alternative<ast::SelectStmt>(script.statements[1]));
}

TEST(SqlParser, SetOpsParseToSelectSetOp) {
    auto set_op_of = [](const char* sql) {
        auto s = parse(sql);
        return std::get<ast::SelectStmt>(s.statements[0]).set_op;
    };
    EXPECT_EQ(set_op_of("SELECT v FROM t UNION ALL SELECT v FROM u"), ast::SelectSetOp::UnionAll);
    EXPECT_EQ(set_op_of("SELECT v FROM t UNION SELECT v FROM u"), ast::SelectSetOp::UnionDistinct);
    EXPECT_EQ(set_op_of("SELECT v FROM t INTERSECT SELECT v FROM u"), ast::SelectSetOp::Intersect);
    EXPECT_EQ(set_op_of("SELECT v FROM t EXCEPT SELECT v FROM u"), ast::SelectSetOp::Except);
    // The multiset ALL forms map to the dedicated IntersectAll / ExceptAll.
    EXPECT_EQ(set_op_of("SELECT v FROM t INTERSECT ALL SELECT v FROM u"),
              ast::SelectSetOp::IntersectAll);
    EXPECT_EQ(set_op_of("SELECT v FROM t EXCEPT ALL SELECT v FROM u"), ast::SelectSetOp::ExceptAll);
}

}  // namespace clink::sql
