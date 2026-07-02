// SQL-native AI frontend tests: CREATE MODEL, ML_PREDICT, VECTOR_SEARCH across the
// parse -> catalog -> bind -> physical layers. These are frontend-only (no runtime
// operators): they assert the AST round-trip, catalog registration, the bound
// derived-table schema + validation errors, and the compiled JobGraphSpec op params.

#include <string>
#include <variant>

#include <gtest/gtest.h>

#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/physical_plan.hpp"

namespace clink::sql {

namespace {

ast::Script parse_ok(const std::string& sql) {
    return parse(sql);
}

// Register a JSON (Row-channel) table via CREATE TABLE.
void register_json(Catalog& cat, const std::string& name, const std::string& cols) {
    const std::string sql = "CREATE TABLE " + name + " " + cols +
                            " WITH (connector='file', format='json', path='/tmp/" + name +
                            ".ndjson')";
    cat.register_table(std::get<ast::CreateTableStmt>(parse(sql).statements[0]));
}

void register_model(Catalog& cat, const std::string& create_model_sql) {
    cat.register_model(std::get<ast::CreateModelStmt>(parse(create_model_sql).statements[0]));
}

std::unique_ptr<LogicalPlan> bind_select(const Catalog& cat, const std::string& sql) {
    Binder b(cat);
    return b.bind_select(std::get<ast::SelectStmt>(parse(sql).statements[0]));
}

std::unique_ptr<LogicalPlan> bind_insert(const Catalog& cat, const std::string& sql) {
    Binder b(cat);
    return b.bind_insert(std::get<ast::InsertStmt>(parse(sql).statements[0]));
}

const cluster::OperatorSpec* find_op(const cluster::JobGraphSpec& spec, const std::string& type) {
    for (const auto& op : spec.ops) {
        if (op.type == type) {
            return &op;
        }
    }
    return nullptr;
}

std::vector<std::string> field_names(const std::shared_ptr<arrow::Schema>& s) {
    std::vector<std::string> out;
    for (const auto& f : s->fields()) {
        out.push_back(f->name());
    }
    return out;
}

}  // namespace

// --- Parse / preparse -------------------------------------------------------

TEST(SqlAiParse, CreateModelParsesToStatement) {
    auto script = parse_ok(
        "CREATE MODEL sentiment INPUT (body TEXT) OUTPUT (label TEXT, score DOUBLE PRECISION) "
        "WITH ('provider'='http', 'endpoint'='https://x/infer')");
    ASSERT_EQ(script.statements.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<ast::CreateModelStmt>(script.statements[0]));
    const auto& m = std::get<ast::CreateModelStmt>(script.statements[0]);
    EXPECT_EQ(m.model_name, "sentiment");
    ASSERT_EQ(m.input_columns.size(), 1u);
    EXPECT_EQ(m.input_columns[0].name, "body");
    ASSERT_EQ(m.output_columns.size(), 2u);
    EXPECT_EQ(m.output_columns[0].name, "label");
    EXPECT_EQ(m.output_columns[1].name, "score");
    ASSERT_EQ(m.options.size(), 2u);
    EXPECT_EQ(m.options[0].key, "provider");
    EXPECT_EQ(m.options[0].value, "http");
}

TEST(SqlAiParse, CreateModelIfNotExists) {
    auto script = parse_ok("CREATE MODEL IF NOT EXISTS m INPUT (x BIGINT) OUTPUT (y BIGINT)");
    ASSERT_TRUE(std::holds_alternative<ast::CreateModelStmt>(script.statements[0]));
    EXPECT_TRUE(std::get<ast::CreateModelStmt>(script.statements[0]).if_not_exists);
}

TEST(SqlAiParse, CreateModelPlainCreateTableUnaffected) {
    // The CREATE MODEL rewrite must not swallow an ordinary CREATE TABLE.
    auto script = parse_ok("CREATE TABLE t (a BIGINT) WITH (connector='file', path='/tmp/t')");
    EXPECT_TRUE(std::holds_alternative<ast::CreateTableStmt>(script.statements[0]));
}

TEST(SqlAiParse, MlPredictIslandRoundTrips) {
    auto script =
        parse_ok("SELECT * FROM ML_PREDICT(TABLE reviews, MODEL sentiment, DESCRIPTOR(a, b))");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_EQ(sel.from_items.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<ast::MlPredictClause>>(sel.from_items[0]));
    const auto& mlp = *std::get<std::unique_ptr<ast::MlPredictClause>>(sel.from_items[0]);
    EXPECT_EQ(mlp.input.name, "reviews");
    EXPECT_EQ(mlp.model_name, "sentiment");
    ASSERT_EQ(mlp.feature_columns.size(), 2u);
    EXPECT_EQ(mlp.feature_columns[0], "a");
    EXPECT_EQ(mlp.feature_columns[1], "b");
}

TEST(SqlAiParse, MlPredictCarriesAlias) {
    auto script = parse_ok("SELECT * FROM ML_PREDICT(TABLE t, MODEL m, DESCRIPTOR(x)) AS p");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    const auto& mlp = *std::get<std::unique_ptr<ast::MlPredictClause>>(sel.from_items[0]);
    ASSERT_TRUE(mlp.alias.has_value());
    EXPECT_EQ(*mlp.alias, "p");
}

TEST(SqlAiParse, VectorSearchIslandRoundTrips) {
    auto script = parse_ok(
        "SELECT * FROM VECTOR_SEARCH(TABLE q, emb, docs, DESCRIPTOR(vec), 5, metric='cosine')");
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_EQ(sel.from_items.size(), 1u);
    ASSERT_TRUE(
        std::holds_alternative<std::unique_ptr<ast::VectorSearchClause>>(sel.from_items[0]));
    const auto& vs = *std::get<std::unique_ptr<ast::VectorSearchClause>>(sel.from_items[0]);
    EXPECT_EQ(vs.input.name, "q");
    EXPECT_EQ(vs.query_vector_column, "emb");
    EXPECT_EQ(vs.vector_table, "docs");
    EXPECT_EQ(vs.index_column, "vec");
    EXPECT_EQ(vs.top_k, 5);
    ASSERT_EQ(vs.options.size(), 1u);
    EXPECT_EQ(vs.options[0].key, "metric");
    EXPECT_EQ(vs.options[0].value, "cosine");
}

TEST(SqlAiParse, VectorSearchNonIntegerTopKRejected) {
    EXPECT_THROW(parse_ok("SELECT * FROM VECTOR_SEARCH(TABLE q, emb, docs, DESCRIPTOR(vec), five)"),
                 TranslationError);
}

// --- Catalog ----------------------------------------------------------------

TEST(SqlAiCatalog, RegisterModelResolvesSchemas) {
    Catalog cat;
    register_model(cat,
                   "CREATE MODEL m INPUT (a BIGINT, b TEXT) OUTPUT (label TEXT) "
                   "WITH ('provider'='http')");
    const ModelDef* m = cat.get_model("m");
    ASSERT_NE(m, nullptr);
    EXPECT_EQ(m->provider(), "http");
    ASSERT_EQ(m->input_columns.size(), 2u);
    EXPECT_EQ(m->output_columns.size(), 1u);
    EXPECT_EQ(m->output_schema()->num_fields(), 1);
    EXPECT_EQ(m->output_schema()->field(0)->name(), "label");
    auto names = cat.list_models();
    ASSERT_EQ(names.size(), 1u);
    EXPECT_EQ(names[0], "m");
}

TEST(SqlAiCatalog, DuplicateModelRejected) {
    Catalog cat;
    register_model(cat, "CREATE MODEL m INPUT (a BIGINT) OUTPUT (y BIGINT)");
    EXPECT_THROW(register_model(cat, "CREATE MODEL m INPUT (a BIGINT) OUTPUT (y BIGINT)"),
                 TranslationError);
}

TEST(SqlAiCatalog, ModelNameCollidesWithTableRejected) {
    Catalog cat;
    register_json(cat, "shared", "(a BIGINT)");
    EXPECT_THROW(register_model(cat, "CREATE MODEL shared INPUT (a BIGINT) OUTPUT (y BIGINT)"),
                 TranslationError);
}

TEST(SqlAiCatalog, DropModel) {
    Catalog cat;
    register_model(cat, "CREATE MODEL m INPUT (a BIGINT) OUTPUT (y BIGINT)");
    EXPECT_TRUE(cat.drop_model("m"));
    EXPECT_EQ(cat.get_model("m"), nullptr);
    EXPECT_FALSE(cat.drop_model("m"));
}

// --- Bind: ML_PREDICT -------------------------------------------------------

TEST(SqlAiBind, MlPredictDerivedSchemaIsInputPlusOutput) {
    Catalog cat;
    register_json(cat, "reviews", "(id BIGINT, body TEXT)");
    register_model(cat,
                   "CREATE MODEL sentiment INPUT (body TEXT) OUTPUT (label TEXT, score DOUBLE "
                   "PRECISION) WITH ('provider'='http')");
    auto plan = bind_select(
        cat, "SELECT * FROM ML_PREDICT(TABLE reviews, MODEL sentiment, DESCRIPTOR(body))");
    EXPECT_EQ(field_names(plan->schema()),
              (std::vector<std::string>{"id", "body", "label", "score"}));
}

TEST(SqlAiBind, MlPredictUnknownModelRejected) {
    Catalog cat;
    register_json(cat, "reviews", "(id BIGINT, body TEXT)");
    EXPECT_THROW(
        bind_select(cat, "SELECT * FROM ML_PREDICT(TABLE reviews, MODEL nope, DESCRIPTOR(body))"),
        TranslationError);
}

TEST(SqlAiBind, MlPredictDescriptorArityMismatchRejected) {
    Catalog cat;
    register_json(cat, "reviews", "(id BIGINT, body TEXT)");
    register_model(cat, "CREATE MODEL m INPUT (a TEXT, b TEXT) OUTPUT (y TEXT)");
    EXPECT_THROW(
        bind_select(cat, "SELECT * FROM ML_PREDICT(TABLE reviews, MODEL m, DESCRIPTOR(body))"),
        TranslationError);
}

TEST(SqlAiBind, MlPredictUnknownFeatureColumnRejected) {
    Catalog cat;
    register_json(cat, "reviews", "(id BIGINT, body TEXT)");
    register_model(cat, "CREATE MODEL m INPUT (t TEXT) OUTPUT (y TEXT)");
    EXPECT_THROW(
        bind_select(cat, "SELECT * FROM ML_PREDICT(TABLE reviews, MODEL m, DESCRIPTOR(missing))"),
        TranslationError);
}

TEST(SqlAiBind, MlPredictOutputCollisionRejected) {
    Catalog cat;
    register_json(cat, "reviews", "(id BIGINT, body TEXT)");
    // OUTPUT column 'body' collides with an input column.
    register_model(cat, "CREATE MODEL m INPUT (body TEXT) OUTPUT (body TEXT)");
    EXPECT_THROW(
        bind_select(cat, "SELECT * FROM ML_PREDICT(TABLE reviews, MODEL m, DESCRIPTOR(body))"),
        TranslationError);
}

// --- Bind: VECTOR_SEARCH ----------------------------------------------------

TEST(SqlAiBind, VectorSearchDerivedSchema) {
    Catalog cat;
    register_json(cat, "queries", "(id BIGINT, emb DOUBLE PRECISION ARRAY)");
    register_json(cat, "docs", "(doc_id BIGINT, vec DOUBLE PRECISION ARRAY, title TEXT)");
    auto plan = bind_select(
        cat, "SELECT * FROM VECTOR_SEARCH(TABLE queries, emb, docs, DESCRIPTOR(vec), 3)");
    EXPECT_EQ(field_names(plan->schema()),
              (std::vector<std::string>{"id", "emb", "doc_id", "vec", "title", "score"}));
}

TEST(SqlAiBind, VectorSearchUnknownTableRejected) {
    Catalog cat;
    register_json(cat, "queries", "(id BIGINT, emb DOUBLE PRECISION ARRAY)");
    EXPECT_THROW(
        bind_select(cat,
                    "SELECT * FROM VECTOR_SEARCH(TABLE queries, emb, nope, DESCRIPTOR(vec), 3)"),
        TranslationError);
}

TEST(SqlAiBind, VectorSearchNonArrayQueryColumnRejected) {
    Catalog cat;
    register_json(cat, "queries", "(id BIGINT, emb TEXT)");
    register_json(cat, "docs", "(doc_id BIGINT, vec DOUBLE PRECISION ARRAY)");
    EXPECT_THROW(
        bind_select(cat,
                    "SELECT * FROM VECTOR_SEARCH(TABLE queries, emb, docs, DESCRIPTOR(vec), 3)"),
        TranslationError);
}

TEST(SqlAiBind, VectorSearchUnknownMetricRejected) {
    Catalog cat;
    register_json(cat, "queries", "(id BIGINT, emb DOUBLE PRECISION ARRAY)");
    register_json(cat, "docs", "(doc_id BIGINT, vec DOUBLE PRECISION ARRAY)");
    EXPECT_THROW(bind_select(cat,
                             "SELECT * FROM VECTOR_SEARCH(TABLE queries, emb, docs, "
                             "DESCRIPTOR(vec), 3, metric='hamming')"),
                 TranslationError);
}

TEST(SqlAiBind, VectorSearchColumnCollisionRejected) {
    Catalog cat;
    // Both tables have an 'id'/'doc_id'... make them collide via a shared column name.
    register_json(cat, "queries", "(id BIGINT, emb DOUBLE PRECISION ARRAY)");
    register_json(cat, "docs", "(id BIGINT, vec DOUBLE PRECISION ARRAY)");
    EXPECT_THROW(
        bind_select(cat,
                    "SELECT * FROM VECTOR_SEARCH(TABLE queries, emb, docs, DESCRIPTOR(vec), 3)"),
        TranslationError);
}

// --- Physical ---------------------------------------------------------------

TEST(SqlAiPhysical, MlPredictLowersToMlPredictRow) {
    Catalog cat;
    register_json(cat, "reviews", "(id BIGINT, body TEXT)");
    register_model(cat,
                   "CREATE MODEL sentiment INPUT (body TEXT) OUTPUT (label TEXT, score DOUBLE "
                   "PRECISION) WITH ('provider'='http', 'endpoint'='https://x/infer')");
    register_json(cat, "out", "(id BIGINT, body TEXT, label TEXT, score DOUBLE PRECISION)");
    auto plan = bind_insert(cat,
                            "INSERT INTO out SELECT * FROM ML_PREDICT(TABLE reviews, MODEL "
                            "sentiment, DESCRIPTOR(body))");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* op = find_op(spec, "ml_predict_row");
    ASSERT_NE(op, nullptr);
    EXPECT_EQ(op->params.at("model_name"), "sentiment");
    EXPECT_EQ(op->params.at("feature_columns"), "body");
    EXPECT_EQ(op->params.at("output_columns"), "label,score");
    EXPECT_EQ(op->params.at("model.provider"), "http");
    EXPECT_EQ(op->params.at("model.endpoint"), "https://x/infer");
    EXPECT_FALSE(op->params.at("output_schema").empty());
    // The provider config must be namespaced, never bare.
    EXPECT_EQ(op->params.count("provider"), 0u);
}

TEST(SqlAiPhysical, VectorSearchLowersToVectorSearchRow) {
    Catalog cat;
    register_json(cat, "queries", "(id BIGINT, emb DOUBLE PRECISION ARRAY)");
    register_json(cat, "docs", "(doc_id BIGINT, vec DOUBLE PRECISION ARRAY, title TEXT)");
    register_json(cat,
                  "out",
                  "(id BIGINT, emb DOUBLE PRECISION ARRAY, doc_id BIGINT, vec DOUBLE "
                  "PRECISION ARRAY, title TEXT, score DOUBLE PRECISION)");
    auto plan = bind_insert(cat,
                            "INSERT INTO out SELECT * FROM VECTOR_SEARCH(TABLE queries, emb, docs, "
                            "DESCRIPTOR(vec), 7, metric='l2')");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* op = find_op(spec, "vector_search_row");
    ASSERT_NE(op, nullptr);
    EXPECT_EQ(op->params.at("query_column"), "emb");
    EXPECT_EQ(op->params.at("index_column"), "vec");
    EXPECT_EQ(op->params.at("top_k"), "7");
    EXPECT_EQ(op->params.at("metric"), "l2");
    EXPECT_EQ(op->params.at("vector_columns"), "doc_id,vec,title");
    // The corpus is resolved to a bounded Row source factory + namespaced build params.
    EXPECT_EQ(op->params.at("vector_source_factory"), "file_json_source");
    EXPECT_EQ(op->params.at("vector_table.path"), "/tmp/docs.ndjson");
    EXPECT_FALSE(op->params.at("vector_table.schema_columns").empty());
    // The raw connector name is not leaked as a bare param.
    EXPECT_EQ(op->params.count("vector_table_connector"), 0u);
    // No corpus refresh requested -> the param is absent (fixed corpus at open).
    EXPECT_EQ(op->params.count("corpus_refresh_ms"), 0u);
}

TEST(SqlAiPhysical, VectorSearchThreadsCorpusRefreshOption) {
    Catalog cat;
    register_json(cat, "queries", "(id BIGINT, emb DOUBLE PRECISION ARRAY)");
    register_json(cat, "docs", "(doc_id BIGINT, vec DOUBLE PRECISION ARRAY)");
    register_json(cat,
                  "out",
                  "(id BIGINT, emb DOUBLE PRECISION ARRAY, doc_id BIGINT, vec DOUBLE "
                  "PRECISION ARRAY, score DOUBLE PRECISION)");
    auto plan = bind_insert(cat,
                            "INSERT INTO out SELECT * FROM VECTOR_SEARCH(TABLE queries, emb, docs, "
                            "DESCRIPTOR(vec), 5, corpus_refresh_ms='60000')");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* op = find_op(spec, "vector_search_row");
    ASSERT_NE(op, nullptr);
    EXPECT_EQ(op->params.at("corpus_refresh_ms"), "60000");
}

TEST(SqlAiBind, VectorSearchRejectsNegativeCorpusRefresh) {
    Catalog cat;
    register_json(cat, "queries", "(id BIGINT, emb DOUBLE PRECISION ARRAY)");
    register_json(cat, "docs", "(doc_id BIGINT, vec DOUBLE PRECISION ARRAY)");
    register_json(cat,
                  "out",
                  "(id BIGINT, emb DOUBLE PRECISION ARRAY, doc_id BIGINT, vec DOUBLE "
                  "PRECISION ARRAY, score DOUBLE PRECISION)");
    EXPECT_THROW((void)bind_insert(cat,
                                   "INSERT INTO out SELECT * FROM VECTOR_SEARCH("
                                   "TABLE queries, emb, docs, DESCRIPTOR(vec), 5, "
                                   "corpus_refresh_ms='-1')"),
                 TranslationError);
}

}  // namespace clink::sql
