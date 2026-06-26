#include <algorithm>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/join_reorder.hpp"
#include "clink/sql/optimizer.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/physical_plan.hpp"

namespace clink::sql {

namespace {

void register_text(Catalog& cat,
                   const char* name,
                   const std::string& connector,
                   const std::string& path) {
    std::string sql = std::string("CREATE TABLE ") + name + " (line TEXT) WITH (connector='" +
                      connector + "', path='" + path + "')";
    auto s = parse(sql);
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
}

std::unique_ptr<LogicalPlan> bind_insert(const Catalog& cat, const char* sql) {
    Binder b(cat);
    return b.bind_insert(std::get<ast::InsertStmt>(parse(sql).statements[0]));
}

void register_json(Catalog& cat,
                   const char* name,
                   const std::string& cols,
                   const std::string& opts) {
    std::string sql = "CREATE TABLE " + std::string(name) + " " + cols +
                      " WITH (connector='file', format='json', path='/tmp/" + name + ".ndjson'" +
                      (opts.empty() ? "" : ", " + opts) + ")";
    auto s = parse(sql);
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
}

const cluster::OperatorSpec* find_op(const cluster::JobGraphSpec& spec, const std::string& type) {
    for (const auto& op : spec.ops) {
        if (op.type == type)
            return &op;
    }
    return nullptr;
}

}  // namespace

TEST(SqlPhysical, FileSourceFileSinkCompilesToThreeOps) {
    Catalog cat;
    register_text(cat, "src_t", "file", "/tmp/in.txt");
    register_text(cat, "dst_t", "file", "/tmp/out.txt");
    auto plan = bind_insert(&cat == &cat ? cat : cat, "INSERT INTO dst_t SELECT line FROM src_t");
    auto& sink = static_cast<const LogicalSink&>(*plan);

    PhysicalPlanner pp;
    auto spec = pp.compile(sink);
    ASSERT_EQ(spec.ops.size(), 3u);

    const auto* src = find_op(spec, "file_text_source");
    const auto* proj = find_op(spec, "identity_string");
    const auto* snk = find_op(spec, "file_text_sink");
    ASSERT_NE(src, nullptr);
    ASSERT_NE(proj, nullptr);
    ASSERT_NE(snk, nullptr);

    EXPECT_EQ(src->params.at("path"), "/tmp/in.txt");
    EXPECT_EQ(snk->params.at("path"), "/tmp/out.txt");
    // connector= must NOT survive into op params (it was the factory selector).
    EXPECT_EQ(src->params.count("connector"), 0u);
    EXPECT_EQ(snk->params.count("connector"), 0u);

    // The proj's input is the src, and the snk's input is the proj.
    ASSERT_EQ(proj->inputs.size(), 1u);
    EXPECT_EQ(proj->inputs[0], src->id);
    ASSERT_EQ(snk->inputs.size(), 1u);
    EXPECT_EQ(snk->inputs[0], proj->id);
    EXPECT_EQ(src->out_channel, "string");
}

TEST(SqlPhysical, KafkaConnectorMapsToKafkaFactories) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE k_in (msg TEXT) "
        "WITH (connector='kafka', topic='clicks', bootstrap='localhost:9092');"
        "CREATE TABLE k_out (msg TEXT) "
        "WITH (connector='kafka', topic='out', bootstrap='localhost:9092');");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO k_out SELECT msg FROM k_in");

    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    EXPECT_NE(find_op(spec, "kafka_source_string"), nullptr);
    EXPECT_NE(find_op(spec, "kafka_sink_string"), nullptr);

    const auto* src = find_op(spec, "kafka_source_string");
    ASSERT_NE(src, nullptr);
    EXPECT_EQ(src->params.at("topic"), "clicks");
    EXPECT_EQ(src->params.at("bootstrap"), "localhost:9092");
}

// Wave 2 inc1: a kafka json table with columnar_decode='true' swaps the
// row-form JSON bridge for the columnar one, and the columnar bridge must
// carry the declared schema so it can build typed Arrow columns.
TEST(SqlPhysical, KafkaColumnarDecodeOptionEmitsColumnarBridgeWithSchema) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE k_in (a BIGINT, b DOUBLE, c VARCHAR) "
        "WITH (connector='kafka', format='json', topic='t', bootstrap='localhost:9092', "
        "columnar_decode='true');"
        "CREATE TABLE f_out (a BIGINT, b DOUBLE, c VARCHAR) "
        "WITH (connector='file', format='json', path='/tmp/o.ndjson');");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO f_out SELECT a, b, c FROM k_in");

    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    const auto* bridge = find_op(spec, "json_string_to_row_columnar");
    ASSERT_NE(bridge, nullptr);
    EXPECT_EQ(find_op(spec, "json_string_to_row"), nullptr);
    ASSERT_EQ(bridge->params.count("schema_columns"), 1u);
    EXPECT_FALSE(bridge->params.at("schema_columns").empty());
}

// Without the option, the kafka json table keeps the plain row-form bridge.
TEST(SqlPhysical, KafkaJsonDefaultUsesRowFormBridge) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE k_in (a BIGINT, b DOUBLE, c VARCHAR) "
        "WITH (connector='kafka', format='json', topic='t', bootstrap='localhost:9092');"
        "CREATE TABLE f_out (a BIGINT, b DOUBLE, c VARCHAR) "
        "WITH (connector='file', format='json', path='/tmp/o.ndjson');");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO f_out SELECT a, b, c FROM k_in");

    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    EXPECT_NE(find_op(spec, "json_string_to_row"), nullptr);
    EXPECT_EQ(find_op(spec, "json_string_to_row_columnar"), nullptr);
}

TEST(SqlPhysical, JsonRoundTripPreservesSpec) {
    Catalog cat;
    register_text(cat, "src_t", "file", "/tmp/a");
    register_text(cat, "dst_t", "file", "/tmp/b");
    auto plan = bind_insert(cat, "INSERT INTO dst_t SELECT line FROM src_t");

    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    auto json = spec.to_json();
    auto round = cluster::JobGraphSpec::from_json(json);
    EXPECT_EQ(round.ops.size(), spec.ops.size());
    for (std::size_t i = 0; i < spec.ops.size(); ++i) {
        EXPECT_EQ(round.ops[i].type, spec.ops[i].type);
        EXPECT_EQ(round.ops[i].id, spec.ops[i].id);
        EXPECT_EQ(round.ops[i].inputs, spec.ops[i].inputs);
        EXPECT_EQ(round.ops[i].params, spec.ops[i].params);
    }
}

TEST(SqlPhysical, ChannelMismatchSourceRowSinkStringRejected) {
    // Phase 3.2: source and sink must agree on the channel. A Row
    // source (multi-column + format='json') feeding a string sink
    // (single TEXT, no format) is rejected at compile time.
    Catalog cat;
    auto s = parse(
        "CREATE TABLE multi (a TEXT, b TEXT) "
        "WITH (connector='file', format='json', path='/tmp/x');"
        "CREATE TABLE dst_t (line TEXT) WITH (connector='file', path='/tmp/y')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    Binder b(cat);
    auto plan =
        b.bind_select(std::get<ast::SelectStmt>(parse("SELECT a FROM multi").statements[0]));
    auto sink = std::make_unique<LogicalSink>(std::move(plan), cat.get_table("dst_t"));
    PhysicalPlanner pp;
    EXPECT_THROW(pp.compile(*sink), TranslationError);
}

TEST(SqlPhysical, PostgresSourceMapsToPostgresTextSource) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE pg_in (line TEXT) WITH (connector='postgres', dsn='postgresql://...');"
        "CREATE TABLE dst (line TEXT) WITH (connector='file', path='/tmp/out')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO dst SELECT line FROM pg_in");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    EXPECT_NE(find_op(spec, "postgres_text_source"), nullptr);
}

TEST(SqlPhysical, PostgresCdcModeMapsToCdcSource) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE pg_cdc (line TEXT) WITH (connector='postgres', mode='cdc', "
        "dsn='postgresql://...', slot='clink');"
        "CREATE TABLE dst (line TEXT) WITH (connector='file', path='/tmp/out')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO dst SELECT line FROM pg_cdc");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    EXPECT_NE(find_op(spec, "postgres_cdc_text_source"), nullptr);
}

TEST(SqlPhysical, ClickHouseSourceAndSinkMap) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE ch_in (line TEXT) WITH (connector='clickhouse', host='localhost', "
        "table='events');"
        "CREATE TABLE ch_out (line TEXT) WITH (connector='clickhouse', host='localhost', "
        "table='out')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO ch_out SELECT line FROM ch_in");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    EXPECT_NE(find_op(spec, "clickhouse_text_source"), nullptr);
    EXPECT_NE(find_op(spec, "clickhouse_sink"), nullptr);
}

TEST(SqlPhysical, ParquetSourceAndSinkMap) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE pq_in (line TEXT) WITH (connector='parquet', path='/tmp/in.parquet');"
        "CREATE TABLE pq_out (line TEXT) WITH (connector='parquet', path='/tmp/out.parquet')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO pq_out SELECT line FROM pq_in");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    EXPECT_NE(find_op(spec, "parquet_string_source"), nullptr);
    EXPECT_NE(find_op(spec, "parquet_string_sink"), nullptr);
}

// A multi-column parquet table runs on the Row channel and binds to the
// typed-columnar parquet_row factories, with the column schema threaded
// in the schema_columns param.
TEST(SqlPhysical, MultiColumnParquetMapsToRowFactoriesWithSchema) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE pq_in (id BIGINT, name VARCHAR, px DOUBLE) "
        "WITH (connector='parquet', path='/tmp/in.parquet');"
        "CREATE TABLE pq_out (id BIGINT, name VARCHAR, px DOUBLE) "
        "WITH (connector='parquet', path='/tmp/out.parquet')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO pq_out SELECT id, name, px FROM pq_in");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    const auto* src = find_op(spec, "parquet_row_source");
    const auto* snk = find_op(spec, "parquet_row_sink");
    ASSERT_NE(src, nullptr);
    ASSERT_NE(snk, nullptr);
    EXPECT_EQ(src->out_channel, "row");
    EXPECT_EQ(snk->out_channel, "row");

    // The full column schema is threaded so the factory can build a typed
    // Arrow batcher at runtime.
    const std::string expected = "id:i64;name:str;px:f64";
    EXPECT_EQ(src->params.at("schema_columns"), expected);
    EXPECT_EQ(snk->params.at("schema_columns"), expected);
}

// delivery_guarantee='exactly_once' routes the parquet row sink to the
// 2PC variant.
// The planner's async-state switch stamps async_state=true on the
// aggregate_row op (and leaves it off by default), which is what makes the
// runtime aggregate take the KeyedState-backed path.
TEST(SqlPhysical, AsyncStateFlagMarksAggregateRow) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE dst (user_id BIGINT, s BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(
        cat, "INSERT INTO dst SELECT user_id, SUM(amount) AS s FROM orders GROUP BY user_id");

    PhysicalPlanner on;
    on.set_async_state_for_aggregation(true);
    auto spec_on = on.compile(static_cast<const LogicalSink&>(*plan));
    const auto* agg_on = find_op(spec_on, "aggregate_row");
    ASSERT_NE(agg_on, nullptr);
    ASSERT_EQ(agg_on->params.count("async_state"), 1u);
    EXPECT_EQ(agg_on->params.at("async_state"), "true");

    PhysicalPlanner off;  // default: switch off
    auto spec_off = off.compile(static_cast<const LogicalSink&>(*plan));
    const auto* agg_off = find_op(spec_off, "aggregate_row");
    ASSERT_NE(agg_off, nullptr);
    EXPECT_EQ(agg_off->params.count("async_state"), 0u);
}

TEST(SqlPhysical, ExactlyOnceParquetMapsTo2pcRowSink) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE pq_in2 (id BIGINT, px DOUBLE) WITH (connector='parquet', path='/tmp/in.pq');"
        "CREATE TABLE pq_out2 (id BIGINT, px DOUBLE) "
        "WITH (connector='parquet', path='/tmp/out', delivery_guarantee='exactly_once')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO pq_out2 SELECT id, px FROM pq_in2");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    EXPECT_NE(find_op(spec, "parquet_row_2pc_sink"), nullptr);
    EXPECT_EQ(find_op(spec, "parquet_row_sink"), nullptr);
}

TEST(SqlPhysical, S3ParquetSourceAndSinkMap) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE s3_in (line TEXT) WITH (connector='s3_parquet', bucket='b', "
        "key='in.parquet');"
        "CREATE TABLE s3_out (line TEXT) WITH (connector='s3_parquet', bucket='b', "
        "key='out.parquet')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO s3_out SELECT line FROM s3_in");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    EXPECT_NE(find_op(spec, "s3_parquet_string_source"), nullptr);
    EXPECT_NE(find_op(spec, "s3_parquet_string_sink"), nullptr);
}

TEST(SqlPhysical, S3TextSinkMaps) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src (line TEXT) WITH (connector='file', path='/tmp/in');"
        "CREATE TABLE s3_out (line TEXT) WITH (connector='s3', bucket='b', key='out.txt')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO s3_out SELECT line FROM src");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    EXPECT_NE(find_op(spec, "s3_text_sink"), nullptr);
}

TEST(SqlPhysical, PostgresSinkRejectedWithUsefulMessage) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src (line TEXT) WITH (connector='file', path='/tmp/in');"
        "CREATE TABLE pg_sink (line TEXT) WITH (connector='postgres', dsn='postgresql://...')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO pg_sink SELECT line FROM src");
    PhysicalPlanner pp;
    try {
        pp.compile(static_cast<const LogicalSink&>(*plan));
        FAIL() << "expected TranslationError";
    } catch (const TranslationError& e) {
        EXPECT_NE(std::string(e.what()).find("Phase 3"), std::string::npos);
    }
}

TEST(SqlPhysical, MultiColumnJsonFlowUsesRowFactories) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE events (user_id BIGINT, url TEXT, ts TIMESTAMP(3)) "
        "WITH (connector='file', format='json', path='/tmp/events.ndjson');"
        "CREATE TABLE out_t (user_id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT user_id, url FROM events");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    EXPECT_EQ(find_op(spec, "file_text_source"), nullptr);
    EXPECT_NE(find_op(spec, "file_json_source"), nullptr);
    EXPECT_NE(find_op(spec, "project_row"), nullptr);
    EXPECT_NE(find_op(spec, "file_json_sink"), nullptr);

    const auto* proj = find_op(spec, "project_row");
    ASSERT_NE(proj, nullptr);
    const auto& out_text = proj->params.at("outputs");
    EXPECT_NE(out_text.find("\"name\":\"user_id\""), std::string::npos);
    EXPECT_NE(out_text.find("\"name\":\"url\""), std::string::npos);
    EXPECT_NE(out_text.find("\"col\":\"user_id\""), std::string::npos);
    EXPECT_NE(out_text.find("\"col\":\"url\""), std::string::npos);
    EXPECT_EQ(proj->out_channel, "row");

    const auto* src = find_op(spec, "file_json_source");
    ASSERT_NE(src, nullptr);
    EXPECT_EQ(src->params.at("path"), "/tmp/events.ndjson");
    EXPECT_EQ(src->params.count("format"), 0u);  // format= is the channel selector, not param
}

TEST(SqlPhysical, RowChannelWhereLowersToFilterRowPredicate) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE events (user_id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/events.ndjson');"
        "CREATE TABLE out_t (user_id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(
        cat, "INSERT INTO out_t SELECT user_id, url FROM events WHERE url = 'http://x'");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    const auto* filter = find_op(spec, "filter_row_predicate");
    ASSERT_NE(filter, nullptr);
    EXPECT_NE(filter->params.at("predicate").find("\"col\":\"url\""), std::string::npos);
    EXPECT_NE(filter->params.at("predicate").find("\"literal\":\"http://x\""), std::string::npos);
}

TEST(SqlPhysical, RowChannelProjectRespectsAliases) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE events (user_id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/events.ndjson');"
        "CREATE TABLE out_t (id BIGINT, link TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT user_id AS id, url AS link FROM events");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* proj = find_op(spec, "project_row");
    ASSERT_NE(proj, nullptr);
    const auto& out_text = proj->params.at("outputs");
    EXPECT_NE(out_text.find("\"name\":\"id\""), std::string::npos);
    EXPECT_NE(out_text.find("\"name\":\"link\""), std::string::npos);
    EXPECT_NE(out_text.find("\"col\":\"user_id\""), std::string::npos);
    EXPECT_NE(out_text.find("\"col\":\"url\""), std::string::npos);
}

TEST(SqlPhysical, GroupByTumbleEmitsTumblingWindowRow) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE orders (user_id BIGINT, ts BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/orders.ndjson', "
        "event_time_column='ts');"
        "CREATE TABLE out_t (user_id BIGINT, total BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat,
                            "INSERT INTO out_t SELECT user_id, SUM(amount) AS total "
                            "FROM orders GROUP BY TUMBLE(ts, 1000), user_id");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    const auto* ts = find_op(spec, "assign_timestamps_row");
    ASSERT_NE(ts, nullptr);
    const auto* agg = find_op(spec, "tumbling_window_row");
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->params.at("time_column"), "ts");
    EXPECT_EQ(agg->params.at("size_ms"), "1000");
    EXPECT_EQ(agg->params.at("group_keys"), "user_id");
    EXPECT_NE(agg->params.at("aggregates").find("\"fn\":\"sum\""), std::string::npos);
    EXPECT_NE(agg->params.at("aggregates").find("\"input_column\":\"amount\""), std::string::npos);
    EXPECT_NE(agg->params.at("aggregates").find("\"name\":\"total\""), std::string::npos);

    // chain: source -> assign_timestamps -> row_compute_key -> window -> sink
    // (Phase 7 inserts the keyer for par > 1 hash routing.)
    const auto* keyer = find_op(spec, "row_compute_key");
    ASSERT_NE(keyer, nullptr);
    EXPECT_EQ(keyer->inputs[0], ts->id);
    EXPECT_EQ(keyer->params.at("columns"), "user_id");
    ASSERT_EQ(agg->inputs.size(), 1u);
    EXPECT_EQ(agg->inputs[0], keyer->id);
    EXPECT_EQ(agg->key_by, "row_key");
}

TEST(SqlPhysical, MultiColumnKafkaSourceBridgesViaJsonStringToRow) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE clicks (user_id BIGINT, ts BIGINT, url TEXT) "
        "WITH (connector='kafka', format='json', topic='clicks', "
        "bootstrap='localhost:9092', event_time_column='ts');"
        "CREATE TABLE out_t (user_id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT user_id, url FROM clicks");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    // Source chain: kafka_source_string (string) -> json_string_to_row (row)
    const auto* src = find_op(spec, "kafka_source_string");
    ASSERT_NE(src, nullptr);
    EXPECT_EQ(src->out_channel, "string");
    EXPECT_EQ(src->params.at("topic"), "clicks");
    const auto* bridge = find_op(spec, "json_string_to_row");
    ASSERT_NE(bridge, nullptr);
    EXPECT_EQ(bridge->out_channel, "row");
    ASSERT_EQ(bridge->inputs.size(), 1u);
    EXPECT_EQ(bridge->inputs[0], src->id);

    // The downstream assign_timestamps_row consumes the bridge output.
    const auto* ts = find_op(spec, "assign_timestamps_row");
    ASSERT_NE(ts, nullptr);
    EXPECT_EQ(ts->inputs[0], bridge->id);
}

TEST(SqlPhysical, MultiColumnKafkaSinkBridgesViaRowToJsonString) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (user_id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE out_t (user_id BIGINT, url TEXT) "
        "WITH (connector='kafka', format='json', topic='out', "
        "bootstrap='localhost:9092')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT user_id, url FROM src_t");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* bridge = find_op(spec, "row_to_json_string");
    ASSERT_NE(bridge, nullptr);
    EXPECT_EQ(bridge->out_channel, "string");
    const auto* sink = find_op(spec, "kafka_sink_string");
    ASSERT_NE(sink, nullptr);
    EXPECT_EQ(sink->inputs[0], bridge->id);
}

TEST(SqlPhysical, ProjectionPushdownAnnotatesSource) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE events (a BIGINT, b BIGINT, c TEXT, d TEXT) "
        "WITH (connector='file', format='json', path='/tmp/e.ndjson');"
        "CREATE TABLE out_t (a BIGINT, c TEXT) "
        "WITH (connector='file', format='json', path='/tmp/o.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT a, c FROM events WHERE c = 'x'");
    plan = optimize(std::move(plan));
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* src = find_op(spec, "file_json_source");
    ASSERT_NE(src, nullptr);
    // Used columns: 'a' (project), 'c' (project + filter predicate).
    // 'b' and 'd' are unused -> dropped from the hint.
    ASSERT_EQ(src->params.count("projected_columns"), 1u);
    EXPECT_EQ(src->params.at("projected_columns"), "a,c");
}

namespace {
const LogicalEquiJoin* find_equi_join(const LogicalPlan* p) {
    while (p != nullptr) {
        if (const auto* j = dynamic_cast<const LogicalEquiJoin*>(p)) {
            return j;
        }
        if (p->inputs().empty()) {
            return nullptr;
        }
        p = p->inputs()[0];
    }
    return nullptr;
}
const LogicalPlan* find_kind(const LogicalPlan* p, const std::string& kind) {
    while (p != nullptr) {
        if (p->kind() == kind) {
            return p;
        }
        if (p->inputs().empty()) {
            return nullptr;
        }
        p = p->inputs()[0];
    }
    return nullptr;
}
}  // namespace

// SQLOPT-4: a single-side WHERE conjunct over an INNER join is pushed below the
// join into that side's scan (de-aliased to the raw column name), shrinking the
// join input. Both single-side conjuncts here push; the outer Filter keeps the
// vacuously-true residual.
TEST(SqlPhysical, PredicatePushdownThroughInnerJoin) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE a (k BIGINT, x BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (k BIGINT, y BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson');"
        "CREATE TABLE out_t (a_k BIGINT, a_x BIGINT, b_k BIGINT, b_y BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/o.ndjson')");
    for (const auto& st : s.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    }
    auto plan = bind_insert(
        cat, "INSERT INTO out_t SELECT * FROM a JOIN b ON a.k = b.k WHERE a_x > 5 AND b_y < 100");
    plan = optimize(std::move(plan));
    const auto* j = find_equi_join(plan.get());
    ASSERT_NE(j, nullptr);
    // Both sides wrapped in a Filter; the pushed predicate uses the raw column
    // name (a_x -> x, b_y -> y), not the join's flat alias name.
    ASSERT_EQ(j->left().kind(), "Filter");
    ASSERT_EQ(j->right().kind(), "Filter");
    const auto& lf = static_cast<const LogicalFilter&>(j->left());
    EXPECT_NE(lf.predicate_json().find("\"x\""), std::string::npos);
    EXPECT_EQ(lf.predicate_json().find("a_x"), std::string::npos);
    const auto& rf = static_cast<const LogicalFilter&>(j->right());
    EXPECT_NE(rf.predicate_json().find("\"y\""), std::string::npos);
    EXPECT_EQ(rf.predicate_json().find("b_y"), std::string::npos);
}

// SQLOPT-4: a predicate over an OUTER join must NOT be pushed (a null-padded-side
// predicate must run after the join), so the scan children stay bare.
TEST(SqlPhysical, PredicateNotPushedThroughOuterJoin) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE a (k BIGINT, x BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (k BIGINT, y BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson');"
        "CREATE TABLE out_t (a_k BIGINT, a_x BIGINT, b_k BIGINT, b_y BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/o.ndjson')");
    for (const auto& st : s.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    }
    auto plan = bind_insert(
        cat, "INSERT INTO out_t SELECT * FROM a LEFT JOIN b ON a.k = b.k WHERE b_y < 100");
    plan = optimize(std::move(plan));
    const auto* j = find_equi_join(plan.get());
    ASSERT_NE(j, nullptr);
    EXPECT_EQ(j->left().kind(), "Scan");
    EXPECT_EQ(j->right().kind(), "Scan");
}

// SQLOPT-4 (projection through joins): each equi-join side's scan is narrowed to
// the columns referenced above plus the join key (de-aliased to raw names);
// columns neither side needs are dropped from both source reads.
TEST(SqlPhysical, ProjectionPushdownThroughEquiJoinNarrowsBothScans) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE a (k BIGINT, x BIGINT, junk_a BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (k BIGINT, y BIGINT, junk_b BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson');"
        "CREATE TABLE out_t (a_x BIGINT, b_y BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/o.ndjson')");
    for (const auto& st : s.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    }
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT a_x, b_y FROM a JOIN b ON a.k = b.k");
    plan = optimize(std::move(plan));
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    std::vector<std::string> source_projections;
    for (const auto& op : spec.ops) {
        if (op.type == "file_json_source") {
            auto it = op.params.find("projected_columns");
            source_projections.push_back(it != op.params.end() ? it->second
                                                               : std::string{"(none)"});
        }
    }
    ASSERT_EQ(source_projections.size(), 2u);
    // Each scan keeps its key (k) + its one referenced column; junk dropped.
    EXPECT_EQ(source_projections[0], "k,x");
    EXPECT_EQ(source_projections[1], "k,y");
}

// SQLOPT-4: projection pushdown also narrows INNER interval-join scans, keeping
// each side's join key AND timestamp column (the BETWEEN bound), dropping junk.
TEST(SqlPhysical, ProjectionPushdownThroughIntervalJoinNarrowsBothScans) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE c (k BIGINT, ts BIGINT, page TEXT, junk_c BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/c.ndjson', event_time_column='ts');"
        "CREATE TABLE d (k BIGINT, ts BIGINT, ad TEXT, junk_d BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/d.ndjson', event_time_column='ts');"
        "CREATE TABLE out_t (c_page TEXT, d_ad TEXT) "
        "WITH (connector='file', format='json', path='/tmp/o.ndjson')");
    for (const auto& st : s.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    }
    auto plan = bind_insert(cat,
                            "INSERT INTO out_t SELECT c_page, d_ad FROM c JOIN d "
                            "ON c.k = d.k AND c.ts BETWEEN d.ts - 50 AND d.ts + 200");
    plan = optimize(std::move(plan));
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    std::vector<std::string> source_projections;
    for (const auto& op : spec.ops) {
        if (op.type == "file_json_source") {
            auto it = op.params.find("projected_columns");
            source_projections.push_back(it != op.params.end() ? it->second
                                                               : std::string{"(none)"});
        }
    }
    ASSERT_EQ(source_projections.size(), 2u);
    // Each scan keeps key (k), timestamp (ts) and its referenced payload col;
    // junk dropped. (ts is retained both as the interval bound and via the
    // event_time_column union.)
    for (const auto& pc : source_projections) {
        EXPECT_NE(pc.find("k"), std::string::npos);
        EXPECT_NE(pc.find("ts"), std::string::npos);
        EXPECT_EQ(pc.find("junk"), std::string::npos);
    }
}

// SQLOPT-4: predicate pushdown also covers INNER interval joins (the symmetric
// sibling of the equi join), de-aliasing single-side conjuncts into the scans.
TEST(SqlPhysical, PredicatePushdownThroughInnerIntervalJoin) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE c (k BIGINT, ts BIGINT, x BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/c.ndjson', event_time_column='ts');"
        "CREATE TABLE d (k BIGINT, ts BIGINT, y BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/d.ndjson', event_time_column='ts');"
        "CREATE TABLE out_t (c_k BIGINT, c_ts BIGINT, c_x BIGINT, d_k BIGINT, d_ts BIGINT, "
        "d_y BIGINT) WITH (connector='file', format='json', path='/tmp/o.ndjson')");
    for (const auto& st : s.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    }
    auto plan = bind_insert(cat,
                            "INSERT INTO out_t SELECT * FROM c JOIN d "
                            "ON c.k = d.k AND c.ts BETWEEN d.ts - 50 AND d.ts + 200 "
                            "WHERE c_x > 5 AND d_y < 100");
    plan = optimize(std::move(plan));
    const auto* j = find_kind(plan.get(), "IntervalJoin");
    ASSERT_NE(j, nullptr);
    const auto& ij = static_cast<const LogicalIntervalJoin&>(*j);
    ASSERT_EQ(ij.left().kind(), "Filter");
    ASSERT_EQ(ij.right().kind(), "Filter");
    EXPECT_NE(static_cast<const LogicalFilter&>(ij.left()).predicate_json().find("\"x\""),
              std::string::npos);
    EXPECT_EQ(static_cast<const LogicalFilter&>(ij.left()).predicate_json().find("c_x"),
              std::string::npos);
}

// SQLOPT-4: a probe-side conjunct is pushed below a lookup join into the probe
// scan (cutting async lookup calls); a dim-side conjunct stays above (the dim is
// an async function with nothing to push into).
TEST(SqlPhysical, PredicatePushdownThroughLookupJoinProbeOnly) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE orders (cust BIGINT, amt BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/o.ndjson');"
        "CREATE TABLE customers (id BIGINT, name TEXT) "
        "WITH (connector='lookup', function='cust_lookup');"
        "CREATE TABLE out_t (o_cust BIGINT, o_amt BIGINT, c_id BIGINT, c_name TEXT) "
        "WITH (connector='file', format='json', path='/tmp/oo.ndjson')");
    for (const auto& st : s.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    }
    auto plan = bind_insert(cat,
                            "INSERT INTO out_t SELECT * FROM orders o JOIN customers c "
                            "ON o.cust = c.id WHERE o_amt > 50");
    plan = optimize(std::move(plan));
    const auto* j = find_kind(plan.get(), "LookupJoin");
    ASSERT_NE(j, nullptr);
    const auto& lj = static_cast<const LogicalLookupJoin&>(*j);
    // The probe-side conjunct o_amt>50 is pushed into the probe scan (de-aliased).
    ASSERT_EQ(lj.input().kind(), "Filter");
    EXPECT_NE(static_cast<const LogicalFilter&>(lj.input()).predicate_json().find("\"amt\""),
              std::string::npos);
    EXPECT_EQ(static_cast<const LogicalFilter&>(lj.input()).predicate_json().find("o_amt"),
              std::string::npos);
}

TEST(SqlPhysical, ProjectionPushdownIncludesEventTimeColumn) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE events (a BIGINT, ts BIGINT, amount BIGINT, unused TEXT) "
        "WITH (connector='file', format='json', path='/tmp/e.ndjson', event_time_column='ts');"
        "CREATE TABLE out_t (a BIGINT, total BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/o.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat,
                            "INSERT INTO out_t SELECT a, SUM(amount) AS total FROM events "
                            "GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), a");
    plan = optimize(std::move(plan));
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* src = find_op(spec, "file_json_source");
    ASSERT_NE(src, nullptr);
    EXPECT_EQ(src->params.at("projected_columns"), "a,ts,amount");
    EXPECT_EQ(src->params.at("projected_columns").find("unused"), std::string::npos);
}

// SQLOPT-4 (arm the hint): an event-time table's ts column must be in the
// projected set EVEN WITHOUT a window, because the physical assign_timestamps_row
// op reads it on every scan. Dropping it would silently starve watermarks.
TEST(SqlPhysical, ProjectionPushdownKeepsEventTimeColumnWithoutWindow) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE t (a BIGINT, ts BIGINT, unused BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/t.ndjson', event_time_column='ts');"
        "CREATE TABLE out_t (a BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/o.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT a FROM t");
    plan = optimize(std::move(plan));
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* src = find_op(spec, "file_json_source");
    ASSERT_NE(src, nullptr);
    ASSERT_EQ(src->params.count("projected_columns"), 1u);
    // 'a' (projected) and 'ts' (event-time, injected by Fix A); 'unused' dropped.
    EXPECT_NE(src->params.at("projected_columns").find("a"), std::string::npos);
    EXPECT_NE(src->params.at("projected_columns").find("ts"), std::string::npos);
    EXPECT_EQ(src->params.at("projected_columns").find("unused"), std::string::npos);
}

// SQLOPT-4 (arm the hint): a column referenced ONLY inside a CASE expression in
// the SELECT must be in the projected set. collect_columns now recurses into
// the CASE branches; without that, arming the hint would prune flag/x/y and the
// CASE would read missing columns.
TEST(SqlPhysical, ProjectionPushdownCollectsCaseColumns) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE t (flag BIGINT, x BIGINT, y BIGINT, unused BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/t.ndjson');"
        "CREATE TABLE out_t (r BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/o.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(
        cat, "INSERT INTO out_t SELECT CASE WHEN flag = 1 THEN x ELSE y END AS r FROM t");
    plan = optimize(std::move(plan));
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* src = find_op(spec, "file_json_source");
    ASSERT_NE(src, nullptr);
    ASSERT_EQ(src->params.count("projected_columns"), 1u);
    const auto& pc = src->params.at("projected_columns");
    EXPECT_NE(pc.find("flag"), std::string::npos);
    EXPECT_NE(pc.find("x"), std::string::npos);
    EXPECT_NE(pc.find("y"), std::string::npos);
    EXPECT_EQ(pc.find("unused"), std::string::npos);
}

TEST(SqlPhysical, IntervalJoinEmitsIntervalJoinRow) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE clicks (user_id BIGINT, click_ts BIGINT, page TEXT) "
        "WITH (connector='file', format='json', path='/tmp/clicks.ndjson', "
        "event_time_column='click_ts');"
        "CREATE TABLE imps (user_id BIGINT, imp_ts BIGINT, ad TEXT) "
        "WITH (connector='file', format='json', path='/tmp/imps.ndjson', "
        "event_time_column='imp_ts');"
        "CREATE TABLE out_t (c_user_id BIGINT, c_click_ts BIGINT, c_page TEXT, "
        "                    i_user_id BIGINT, i_imp_ts BIGINT, i_ad TEXT) "
        "WITH (connector='file', format='json', path='/tmp/joined.ndjson')");
    for (const auto& stmt : s.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(stmt));
    }
    auto plan = bind_insert(cat,
                            "INSERT INTO out_t SELECT * FROM clicks c JOIN imps i "
                            "ON c.user_id = i.user_id "
                            "AND c.click_ts BETWEEN i.imp_ts - INTERVAL '5' SECOND "
                            "AND i.imp_ts + INTERVAL '10' SECOND");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* jn = find_op(spec, "interval_join_row");
    ASSERT_NE(jn, nullptr);
    EXPECT_EQ(jn->params.at("left_key_column"), "user_id");
    EXPECT_EQ(jn->params.at("right_key_column"), "user_id");
    EXPECT_EQ(jn->params.at("left_ts_column"), "click_ts");
    EXPECT_EQ(jn->params.at("right_ts_column"), "imp_ts");
    EXPECT_EQ(jn->params.at("left_alias"), "c");
    EXPECT_EQ(jn->params.at("right_alias"), "i");
    EXPECT_EQ(jn->params.at("lower_offset_ms"), "-5000");
    EXPECT_EQ(jn->params.at("upper_offset_ms"), "10000");
    ASSERT_EQ(jn->inputs.size(), 2u);

    // Both sides should have their own assign_timestamps_row.
    int ts_count = 0;
    for (const auto& op : spec.ops) {
        if (op.type == "assign_timestamps_row")
            ++ts_count;
    }
    EXPECT_EQ(ts_count, 2);
}

TEST(SqlPhysical, EquiJoinEmitsEquiJoinRowKeyedByJoinColumn) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE a (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (k BIGINT, w BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson');"
        "CREATE TABLE out_t (a_k BIGINT, a_v BIGINT, b_k BIGINT, b_w BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    for (const auto& stmt : s.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(stmt));
    }
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT * FROM a JOIN b ON a.k = b.k");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    const auto* jn = find_op(spec, "equi_join_row");
    ASSERT_NE(jn, nullptr);
    EXPECT_EQ(jn->params.at("left_key_column"), "k");
    EXPECT_EQ(jn->params.at("right_key_column"), "k");
    EXPECT_EQ(jn->params.at("left_alias"), "a");
    EXPECT_EQ(jn->params.at("right_alias"), "b");
    EXPECT_EQ(jn->key_by, "row_key");
    ASSERT_EQ(jn->inputs.size(), 2u);

    // Both sides should land on a row_compute_key partitioned on `k`.
    int key_count = 0;
    for (const auto& op : spec.ops) {
        if (op.type == "row_compute_key" && op.params.count("columns") &&
            op.params.at("columns") == "k") {
            ++key_count;
        }
    }
    EXPECT_GE(key_count, 2);

    // No interval-specific ops should appear.
    EXPECT_EQ(find_op(spec, "interval_join_row"), nullptr);
    EXPECT_EQ(find_op(spec, "assign_timestamps_row"), nullptr);
    // Inner join carries the inner join_type.
    EXPECT_EQ(jn->params.at("join_type"), "inner");
}

TEST(SqlPhysical, OuterEquiJoinEmitsJoinTypeAndColumns) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE a (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (k BIGINT, w BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson');"
        "CREATE TABLE out_t (a_k BIGINT, a_v BIGINT, b_k BIGINT, b_w BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    for (const auto& stmt : s.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(stmt));
    }
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT * FROM a LEFT JOIN b ON a.k = b.k");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* jn = find_op(spec, "equi_join_row");
    ASSERT_NE(jn, nullptr);
    EXPECT_EQ(jn->params.at("join_type"), "left_outer");
    EXPECT_EQ(jn->params.at("left_columns"), "k,v");
    EXPECT_EQ(jn->params.at("right_columns"), "k,w");
}

namespace {
Catalog lookup_join_physical_catalog() {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE orders (cust BIGINT, amt BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/o.ndjson');"
        "CREATE TABLE customers (id BIGINT, name TEXT) "
        "WITH (connector='lookup', function='cust_lookup');"
        "CREATE TABLE out_t (o_cust BIGINT, o_amt BIGINT, c_id BIGINT, c_name TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    for (const auto& stmt : s.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(stmt));
    }
    return cat;
}
}  // namespace

TEST(SqlPhysical, LookupJoinEmitsAsyncLookupJoinRowWithInnerFilter) {
    Catalog cat = lookup_join_physical_catalog();
    auto plan = bind_insert(
        cat, "INSERT INTO out_t SELECT * FROM orders o JOIN customers c ON o.cust = c.id");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* lj = find_op(spec, "async_lookup_join_row");
    ASSERT_NE(lj, nullptr);
    EXPECT_EQ(lj->params.at("function_name"), "cust_lookup");
    EXPECT_EQ(lj->params.at("probe_alias"), "o");
    EXPECT_EQ(lj->params.at("dim_alias"), "c");
    EXPECT_EQ(lj->params.at("probe_columns"), "cust,amt");
    EXPECT_EQ(lj->params.at("dim_columns"), "id,name");
    EXPECT_EQ(lj->params.at("join_type"), "inner");
    // INNER drops misses via a trailing IS NOT NULL filter on the dim key.
    const auto* filt = find_op(spec, "filter_row_predicate");
    ASSERT_NE(filt, nullptr);
    EXPECT_NE(filt->params.at("predicate").find("is_not_null"), std::string::npos);
    EXPECT_NE(filt->params.at("predicate").find("c_id"), std::string::npos);
}

TEST(SqlPhysical, LookupJoinLeftOuterHasNoMissFilter) {
    Catalog cat = lookup_join_physical_catalog();
    auto plan = bind_insert(
        cat, "INSERT INTO out_t SELECT * FROM orders o LEFT JOIN customers c ON o.cust = c.id");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* lj = find_op(spec, "async_lookup_join_row");
    ASSERT_NE(lj, nullptr);
    EXPECT_EQ(lj->params.at("join_type"), "left_outer");
    // LEFT keeps every probe row, so no miss-dropping filter is emitted.
    EXPECT_EQ(find_op(spec, "filter_row_predicate"), nullptr);
}

TEST(SqlPhysical, UnionAllEmitsUnionRowWithTwoInputs) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE a (id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson');"
        "CREATE TABLE out_t (id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[2]));
    auto plan =
        bind_insert(cat, "INSERT INTO out_t SELECT id, url FROM a UNION ALL SELECT id, url FROM b");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    const auto* un = find_op(spec, "union_row");
    ASSERT_NE(un, nullptr);
    ASSERT_EQ(un->inputs.size(), 2u);
    EXPECT_EQ(un->out_channel, "row");
}

namespace {
Catalog set_op_physical_catalog() {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE a (id BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (id BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson');"
        "CREATE TABLE out_t (id BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    for (const auto& stmt : s.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(stmt));
    }
    return cat;
}
int count_ops(const cluster::JobGraphSpec& spec, const std::string& type) {
    int n = 0;
    for (const auto& op : spec.ops) {
        if (op.type == type)
            ++n;
    }
    return n;
}
}  // namespace

TEST(SqlPhysical, IntersectEmitsSetOpRowKeyedBothSides) {
    Catalog cat = set_op_physical_catalog();
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT id FROM a INTERSECT SELECT id FROM b");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* so = find_op(spec, "set_op_row");
    ASSERT_NE(so, nullptr);
    EXPECT_EQ(so->params.at("mode"), "intersect");
    EXPECT_EQ(so->params.at("left_columns"), "id");
    EXPECT_EQ(so->params.at("right_columns"), "id");
    EXPECT_EQ(so->key_by, "row_key");
    ASSERT_EQ(so->inputs.size(), 2u);
    // Both sides keyed by their columns so equal rows co-partition.
    EXPECT_EQ(count_ops(spec, "row_compute_key"), 2);
}

TEST(SqlPhysical, ExceptEmitsSetOpRowExceptMode) {
    Catalog cat = set_op_physical_catalog();
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT id FROM a EXCEPT SELECT id FROM b");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* so = find_op(spec, "set_op_row");
    ASSERT_NE(so, nullptr);
    EXPECT_EQ(so->params.at("mode"), "except");
}

TEST(SqlPhysical, UnionDistinctEmitsDistinctOverUnion) {
    Catalog cat = set_op_physical_catalog();
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT id FROM a UNION SELECT id FROM b");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    EXPECT_NE(find_op(spec, "union_row"), nullptr);
    EXPECT_NE(find_op(spec, "distinct_row"), nullptr);
    EXPECT_EQ(find_op(spec, "set_op_row"), nullptr);
}

TEST(SqlPhysical, CountDistinctAndStringAggEncodeDistinctAndSeparator) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE t (k BIGINT, v BIGINT, s TEXT) "
        "WITH (connector='file', format='json', path='/tmp/t.ndjson');"
        "CREATE TABLE out_t (k BIGINT, cd BIGINT, sa TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    for (const auto& stmt : s.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(stmt));
    }
    auto plan =
        bind_insert(cat,
                    "INSERT INTO out_t SELECT k, COUNT(DISTINCT v) AS cd, STRING_AGG(s, '|') AS sa "
                    "FROM t GROUP BY k");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* op = find_op(spec, "aggregate_row");
    ASSERT_NE(op, nullptr);
    auto aggs = clink::config::parse(op->params.at("aggregates"));
    ASSERT_TRUE(aggs.is_array());
    bool found_cd = false;
    bool found_sa = false;
    for (const auto& e : aggs.as_array()) {
        if (e.at("name").as_string() == "cd") {
            EXPECT_TRUE(e.at("distinct").as_bool());
            found_cd = true;
        }
        if (e.at("name").as_string() == "sa") {
            EXPECT_EQ(e.at("fn").as_string(), "string_agg");
            EXPECT_EQ(e.at("separator").as_string(), "|");
            found_sa = true;
        }
    }
    EXPECT_TRUE(found_cd);
    EXPECT_TRUE(found_sa);
}

TEST(SqlPhysical, KafkaExactlyOnceSinkSelected) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE keo (k BIGINT, v BIGINT) "
        "WITH (connector='kafka', format='json', topic='out', "
        "      brokers='localhost:9092', "
        "      delivery_guarantee='exactly_once', "
        "      transactional_id='my-job-keo')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO keo SELECT k, v FROM src_t");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* sink = find_op(spec, "kafka_2pc_sink_string");
    ASSERT_NE(sink, nullptr);
    EXPECT_EQ(sink->params.at("transactional_id"), "my-job-keo");
    EXPECT_EQ(find_op(spec, "kafka_sink_string"), nullptr);
    EXPECT_EQ(find_op(spec, "kafka_upsert_sink_string"), nullptr);
    EXPECT_NE(find_op(spec, "row_to_json_string"), nullptr);
}

TEST(SqlPhysical, FileExactlyOnceSinkSelected) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE eo (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/eo_dir/', "
        "      delivery_guarantee='exactly_once')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO eo SELECT k, v FROM src_t");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* sink = find_op(spec, "file_2pc_sink_row");
    ASSERT_NE(sink, nullptr);
    EXPECT_EQ(find_op(spec, "file_json_sink"), nullptr);
    EXPECT_EQ(find_op(spec, "file_json_upsert_sink"), nullptr);
    // 'path' from the table definition flows through as a param;
    // the sink factory accepts 'dir' or 'path'.
    EXPECT_EQ(sink->params.at("path"), "/tmp/eo_dir/");
}

TEST(SqlPhysical, CommitGroupThreadsThroughToSinkParams) {
    // Phase 30a: commit_group property on the table flows through the
    // physical planner into OperatorSpec.params, where sink factories
    // pick it up and call set_commit_group on the constructed sink.
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE eo_grouped (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/grp/', "
        "      delivery_guarantee='exactly_once', commit_group='atomic-out')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO eo_grouped SELECT k, v FROM src_t");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* sink = find_op(spec, "file_2pc_sink_row");
    ASSERT_NE(sink, nullptr);
    EXPECT_EQ(sink->params.at("commit_group"), "atomic-out");
}

TEST(SqlPhysical, FileUpsertSinkSelected) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE up (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson', "
        "      mode='upsert', primary_key='k')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO up SELECT k, v FROM src_t");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* sink = find_op(spec, "file_json_upsert_sink");
    ASSERT_NE(sink, nullptr);
    EXPECT_EQ(sink->params.at("primary_key"), "k");
    EXPECT_EQ(find_op(spec, "file_json_sink"), nullptr);
}

TEST(SqlPhysical, KafkaUpsertSinkSelected) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE up (k BIGINT, v BIGINT) "
        "WITH (connector='kafka', format='json', topic='topk', "
        "      bootstrap='localhost:9092', brokers='localhost:9092', "
        "      mode='upsert', primary_key='k')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO up SELECT k, v FROM src_t");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* sink = find_op(spec, "kafka_upsert_sink_string");
    ASSERT_NE(sink, nullptr);
    EXPECT_EQ(sink->params.at("primary_key"), "k");
    // The row_to_json_string bridge sits between the source and the sink.
    EXPECT_NE(find_op(spec, "row_to_json_string"), nullptr);
}

TEST(SqlPhysical, TopNPerKeyEmitsTopNPerKeyRowOp) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE t (user_id BIGINT, ts BIGINT, page TEXT) "
        "WITH (connector='file', format='json', path='/tmp/t.ndjson');"
        "CREATE TABLE out_t (user_id BIGINT, ts BIGINT, page TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson', "
        "      mode='upsert', primary_key='user_id, ts')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat,
                            "INSERT INTO out_t SELECT * FROM "
                            "(SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts DESC)"
                            " AS rn FROM t) sub WHERE rn <= 2");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* op = find_op(spec, "top_n_per_key_row");
    ASSERT_NE(op, nullptr);
    EXPECT_EQ(op->params.at("count"), "2");
    EXPECT_EQ(op->params.at("partition_columns"), "user_id");
    EXPECT_EQ(op->params.at("sort_columns"), "ts");
    EXPECT_EQ(op->params.at("sort_descending"), "1");
    EXPECT_EQ(op->params.at("rank_kind"), "row_number");
    EXPECT_EQ(op->key_by, "row_key");
}

TEST(SqlPhysical, RankAndDenseRankTopNEmitRankKindParam) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE t (user_id BIGINT, ts BIGINT, page TEXT) "
        "WITH (connector='file', format='json', path='/tmp/t.ndjson');"
        "CREATE TABLE out_t (user_id BIGINT, ts BIGINT, page TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson', "
        "      mode='upsert', primary_key='user_id, ts')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));

    PhysicalPlanner pp;
    {
        auto plan = bind_insert(cat,
                                "INSERT INTO out_t SELECT * FROM "
                                "(SELECT *, RANK() OVER (PARTITION BY user_id ORDER BY ts DESC)"
                                " AS rn FROM t) sub WHERE rn <= 2");
        auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
        const auto* op = find_op(spec, "top_n_per_key_row");
        ASSERT_NE(op, nullptr);
        EXPECT_EQ(op->params.at("rank_kind"), "rank");
    }
    {
        auto plan =
            bind_insert(cat,
                        "INSERT INTO out_t SELECT * FROM "
                        "(SELECT *, DENSE_RANK() OVER (PARTITION BY user_id ORDER BY ts DESC)"
                        " AS rn FROM t) sub WHERE rn <= 2");
        auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
        const auto* op = find_op(spec, "top_n_per_key_row");
        ASSERT_NE(op, nullptr);
        EXPECT_EQ(op->params.at("rank_kind"), "dense_rank");
    }
}

TEST(SqlPhysical, OrderByLimitEmitsTopNRowWithParams) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE t (a BIGINT, b TEXT) "
        "WITH (connector='file', format='json', path='/tmp/t.ndjson');"
        "CREATE TABLE out_t (a BIGINT, b TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan =
        bind_insert(cat, "INSERT INTO out_t SELECT a, b FROM t ORDER BY a DESC, b ASC LIMIT 4");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    const auto* tn = find_op(spec, "top_n_row");
    ASSERT_NE(tn, nullptr);
    EXPECT_EQ(tn->params.at("count"), "4");
    EXPECT_EQ(tn->params.at("sort_columns"), "a,b");
    EXPECT_EQ(tn->params.at("sort_descending"), "1,0");
}

TEST(SqlPhysical, LimitEmitsLimitRowWithCount) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE clicks (user_id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/clicks.ndjson');"
        "CREATE TABLE out_t (user_id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT user_id, url FROM clicks LIMIT 3");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    const auto* lim = find_op(spec, "limit_row");
    ASSERT_NE(lim, nullptr);
    EXPECT_EQ(lim->params.at("count"), "3");
}

TEST(SqlPhysical, SelectDistinctEmitsDistinctRowKeyedByAllColumns) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE clicks (user_id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/clicks.ndjson');"
        "CREATE TABLE out_t (user_id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT DISTINCT user_id, url FROM clicks");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    const auto* dist = find_op(spec, "distinct_row");
    ASSERT_NE(dist, nullptr);
    EXPECT_EQ(dist->key_by, "row_key");

    const auto* keyer = find_op(spec, "row_compute_key");
    ASSERT_NE(keyer, nullptr);
    EXPECT_EQ(keyer->params.at("columns"), "user_id,url");
    ASSERT_EQ(dist->inputs.size(), 1u);
    EXPECT_EQ(dist->inputs[0], keyer->id);
}

TEST(SqlPhysical, HavingEmitsFilterRowPredicateAfterAggregate) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/orders.ndjson');"
        "CREATE TABLE out_t (user_id BIGINT, total BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat,
                            "INSERT INTO out_t SELECT user_id, SUM(amount) AS total "
                            "FROM orders GROUP BY user_id HAVING total > 100");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    const auto* agg = find_op(spec, "aggregate_row");
    ASSERT_NE(agg, nullptr);
    const auto* having = find_op(spec, "filter_row_predicate");
    ASSERT_NE(having, nullptr);
    ASSERT_EQ(having->inputs.size(), 1u);
    EXPECT_EQ(having->inputs[0], agg->id);
    EXPECT_NE(having->params.at("predicate").find("\"col\":\"total\""), std::string::npos);
}

TEST(SqlPhysical, UnboundedGroupByEmitsAggregateRow) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/orders.ndjson');"
        "CREATE TABLE out_t (user_id BIGINT, total BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat,
                            "INSERT INTO out_t SELECT user_id, SUM(amount) AS total "
                            "FROM orders GROUP BY user_id");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    const auto* agg = find_op(spec, "aggregate_row");
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->params.at("group_keys"), "user_id");
    EXPECT_NE(agg->params.at("aggregates").find("\"fn\":\"sum\""), std::string::npos);
    EXPECT_NE(agg->params.at("aggregates").find("\"name\":\"total\""), std::string::npos);
    EXPECT_EQ(agg->key_by, "row_key");

    const auto* keyer = find_op(spec, "row_compute_key");
    ASSERT_NE(keyer, nullptr);
    EXPECT_EQ(keyer->params.at("columns"), "user_id");
    ASSERT_EQ(agg->inputs.size(), 1u);
    EXPECT_EQ(agg->inputs[0], keyer->id);

    // No assign_timestamps_row: the unbounded path doesn't need a
    // watermark.
    EXPECT_EQ(find_op(spec, "assign_timestamps_row"), nullptr);
    EXPECT_EQ(find_op(spec, "tumbling_window_row"), nullptr);
}

TEST(SqlPhysical, MultiColumnGroupByPassesAllKeysToKeyer) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE orders (region TEXT, user_id BIGINT, ts BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/o.ndjson', "
        "event_time_column='ts');"
        "CREATE TABLE out_t (region TEXT, user_id BIGINT, total BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/x.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan =
        bind_insert(cat,
                    "INSERT INTO out_t SELECT region, user_id, SUM(amount) AS total "
                    "FROM orders GROUP BY TUMBLE(ts, INTERVAL '1' SECOND), region, user_id");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* keyer = find_op(spec, "row_compute_key");
    ASSERT_NE(keyer, nullptr);
    EXPECT_EQ(keyer->params.at("columns"), "region,user_id");
}

TEST(SqlPhysical, IntervalJoinEmitsRowComputeKeyOnEachSide) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE clicks (user_id BIGINT, click_ts BIGINT, page TEXT) "
        "WITH (connector='file', format='json', path='/tmp/c.ndjson', "
        "event_time_column='click_ts');"
        "CREATE TABLE imps (uid BIGINT, imp_ts BIGINT, ad TEXT) "
        "WITH (connector='file', format='json', path='/tmp/i.ndjson', "
        "event_time_column='imp_ts');"
        "CREATE TABLE out_t (c_user_id BIGINT, c_click_ts BIGINT, c_page TEXT, "
        "i_uid BIGINT, i_imp_ts BIGINT, i_ad TEXT) "
        "WITH (connector='file', format='json', path='/tmp/joined.ndjson')");
    for (const auto& stmt : s.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(stmt));
    }
    auto plan = bind_insert(cat,
                            "INSERT INTO out_t SELECT * FROM clicks c JOIN imps i "
                            "ON c.user_id = i.uid AND c.click_ts BETWEEN i.imp_ts - "
                            "INTERVAL '5' SECOND AND i.imp_ts + INTERVAL '10' SECOND");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    // Two row_compute_key ops: one for each side of the join.
    int keyers = 0;
    for (const auto& op : spec.ops) {
        if (op.type == "row_compute_key")
            ++keyers;
    }
    EXPECT_EQ(keyers, 2);

    const auto* jn = find_op(spec, "interval_join_row");
    ASSERT_NE(jn, nullptr);
    EXPECT_EQ(jn->key_by, "row_key");
    ASSERT_EQ(jn->inputs.size(), 2u);
    // Both join inputs come from row_compute_key ops.
    for (const auto& input_id : jn->inputs) {
        bool from_keyer = false;
        for (const auto& op : spec.ops) {
            if (op.id == input_id && op.type == "row_compute_key") {
                from_keyer = true;
                break;
            }
        }
        EXPECT_TRUE(from_keyer) << "join input " << input_id << " not from row_compute_key";
    }
}

TEST(SqlPhysical, GroupBySessionEmitsSessionWindowRow) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE clicks (user_id BIGINT, ts BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/c.ndjson', "
        "event_time_column='ts');"
        "CREATE TABLE out_t (user_id BIGINT, n BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat,
                            "INSERT INTO out_t SELECT user_id, COUNT(*) AS n FROM clicks "
                            "GROUP BY SESSION(ts, INTERVAL '30' SECOND), user_id");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* sess = find_op(spec, "session_window_row");
    ASSERT_NE(sess, nullptr);
    EXPECT_EQ(sess->params.at("gap_ms"), "30000");
    EXPECT_EQ(sess->params.at("group_keys"), "user_id");
}

TEST(SqlPhysical, GroupByHopEmitsHoppingWindowRow) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE orders (user_id BIGINT, ts BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/orders.ndjson', "
        "event_time_column='ts');"
        "CREATE TABLE out_t (user_id BIGINT, total BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat,
                            "INSERT INTO out_t SELECT user_id, SUM(amount) AS total "
                            "FROM orders "
                            "GROUP BY HOP(ts, INTERVAL '10' SECOND, INTERVAL '1' SECOND), user_id");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* agg = find_op(spec, "hopping_window_row");
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->params.at("size_ms"), "10000");
    EXPECT_EQ(agg->params.at("slide_ms"), "1000");
    EXPECT_EQ(agg->params.at("group_keys"), "user_id");
}

TEST(SqlPhysical, GroupByCumulateEmitsCumulateWindowRow) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE orders (user_id BIGINT, ts BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/orders.ndjson', "
        "event_time_column='ts');"
        "CREATE TABLE out_t (user_id BIGINT, total BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat,
                            "INSERT INTO out_t SELECT user_id, SUM(amount) AS total "
                            "FROM orders "
                            "GROUP BY CUMULATE(ts, 1000, 3000), user_id");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* agg = find_op(spec, "cumulate_window_row");
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->params.at("size_ms"), "3000");
    EXPECT_EQ(agg->params.at("step_ms"), "1000");
    EXPECT_EQ(agg->params.at("group_keys"), "user_id");
}

TEST(SqlPhysical, OverAggregateEmitsOverAggregateRow) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE evt (user_id BIGINT, ts BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/evt.ndjson', event_time_column='ts');"
        "CREATE TABLE out_t (user_id BIGINT, ts BIGINT, amount BIGINT, running BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat,
                            "INSERT INTO out_t SELECT *, SUM(amount) OVER "
                            "(PARTITION BY user_id ORDER BY ts) AS running FROM evt");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* op = find_op(spec, "over_aggregate_row");
    ASSERT_NE(op, nullptr);
    EXPECT_EQ(op->params.at("time_column"), "ts");
    EXPECT_EQ(op->params.at("partition_columns"), "user_id");
    EXPECT_EQ(op->key_by, "row_key");
    EXPECT_NE(op->params.at("outputs").find("\"running\""), std::string::npos);
    EXPECT_NE(op->params.at("outputs").find("\"sum\""), std::string::npos);
}

TEST(SqlPhysical, InAndNotInEmitSemiJoinRow) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE clicks (user_id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/clicks.ndjson');"
        "CREATE TABLE vips (user_id BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/vips.ndjson');"
        "CREATE TABLE out_t (user_id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[2]));
    PhysicalPlanner pp;
    {
        auto plan = bind_insert(
            cat,
            "INSERT INTO out_t SELECT * FROM clicks WHERE user_id IN (SELECT user_id FROM vips)");
        auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
        const auto* op = find_op(spec, "semi_join_row");
        ASSERT_NE(op, nullptr);
        EXPECT_EQ(op->params.at("left_key_column"), "user_id");
        EXPECT_EQ(op->params.at("right_key_column"), "user_id");
        EXPECT_EQ(op->params.at("anti"), "0");
        EXPECT_EQ(op->key_by, "row_key");
    }
    {
        auto plan = bind_insert(cat,
                                "INSERT INTO out_t SELECT * FROM clicks WHERE user_id NOT IN "
                                "(SELECT user_id FROM vips)");
        auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
        const auto* op = find_op(spec, "semi_join_row");
        ASSERT_NE(op, nullptr);
        EXPECT_EQ(op->params.at("anti"), "1");
    }
}

// #49: multi-column NOT IN lowers to a composite null-aware anti semi_join_row.
TEST(SqlPhysical, MultiColumnNotInEmitsCompositeAntiSemiJoinRow) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE t (a BIGINT, b BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/t.ndjson');"
        "CREATE TABLE s (x BIGINT, y BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/s.ndjson');"
        "CREATE TABLE out_t (a BIGINT, b BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[2]));
    auto plan = bind_insert(
        cat, "INSERT INTO out_t SELECT * FROM t WHERE (a, b) NOT IN (SELECT x, y FROM s)");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* op = find_op(spec, "semi_join_row");
    ASSERT_NE(op, nullptr);
    EXPECT_EQ(op->params.at("left_key_column"), "a,b");
    EXPECT_EQ(op->params.at("right_key_column"), "x,y");
    EXPECT_EQ(op->params.at("anti"), "1");
    EXPECT_EQ(op->params.at("null_aware"), "1");
    EXPECT_EQ(op->key_by, "row_key");
}

TEST(SqlPhysical, ScalarSubqueryEmitsBroadcastFilter) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE orders (id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/orders.ndjson');"
        "CREATE TABLE refs (amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/refs.ndjson');"
        "CREATE TABLE out_t (id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[2]));
    auto plan = bind_insert(
        cat,
        "INSERT INTO out_t SELECT * FROM orders WHERE amount > (SELECT avg(amount) FROM refs)");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* op = find_op(spec, "scalar_broadcast_filter_row");
    ASSERT_NE(op, nullptr);
    EXPECT_EQ(op->params.at("test_column"), "amount");
    EXPECT_EQ(op->params.at("comparison_op"), "gt");
    EXPECT_FALSE(op->params.at("scalar_column").empty());
    // The scalar side compiled to a global aggregate.
    EXPECT_NE(find_op(spec, "aggregate_row"), nullptr);
}

// #55: scalar subquery as a SELECT item -> scalar_project_row op fed by
// the main scan and the empty-group aggregate.
TEST(SqlPhysical, ScalarSubqueryInSelectEmitsScalarProjectRow) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE orders (id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/orders.ndjson');"
        "CREATE TABLE refs (amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/refs.ndjson');"
        "CREATE TABLE out_t (id BIGINT, a BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[2]));
    auto plan = bind_insert(
        cat, "INSERT INTO out_t SELECT id, (SELECT max(amount) FROM refs) AS a FROM orders");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* op = find_op(spec, "scalar_project_row");
    ASSERT_NE(op, nullptr);
    EXPECT_FALSE(op->params.at("output_column").empty());
    EXPECT_EQ(op->params.at("scalar_column"), op->params.at("output_column"));
    ASSERT_EQ(op->inputs.size(), 2U);  // [0]=main, [1]=scalar
    // The scalar side compiled to a global aggregate.
    EXPECT_NE(find_op(spec, "aggregate_row"), nullptr);
}

TEST(SqlPhysical, EventTimeColumnEmitsAssignTimestampsRow) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE events (user_id BIGINT, ts BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson', "
        "event_time_column='ts', watermark_lag_ms='5000');"
        "CREATE TABLE out_t (user_id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT user_id, url FROM events");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));

    const auto* ts = find_op(spec, "assign_timestamps_row");
    ASSERT_NE(ts, nullptr);
    EXPECT_EQ(ts->params.at("column"), "ts");
    EXPECT_EQ(ts->params.at("out_of_order_ms"), "5000");
    EXPECT_EQ(ts->out_channel, "row");

    // Source params must NOT carry event_time_column / watermark_lag_ms
    // (the planner consumed them to wire the timestamps op).
    const auto* src = find_op(spec, "file_json_source");
    ASSERT_NE(src, nullptr);
    EXPECT_EQ(src->params.count("event_time_column"), 0u);
    EXPECT_EQ(src->params.count("watermark_lag_ms"), 0u);
    EXPECT_EQ(src->params.at("path"), "/tmp/in.ndjson");

    // ts op's input is the source; downstream chain (project / sink)
    // builds on the ts op's id.
    ASSERT_EQ(ts->inputs.size(), 1u);
    EXPECT_EQ(ts->inputs[0], src->id);
    const auto* proj = find_op(spec, "project_row");
    ASSERT_NE(proj, nullptr);
    EXPECT_EQ(proj->inputs[0], ts->id);
}

TEST(SqlPhysical, NoEventTimeMeansNoAssignTimestamps) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE events (a BIGINT, b TEXT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE out_t (a BIGINT, b TEXT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO out_t SELECT a, b FROM events");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    EXPECT_EQ(find_op(spec, "assign_timestamps_row"), nullptr);
}

TEST(SqlPhysical, ArithmeticExpressionEmitsOpInOutputs) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE events (user_id BIGINT, age BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE out_t (uid BIGINT, plus_one DOUBLE PRECISION) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(
        cat, "INSERT INTO out_t SELECT user_id AS uid, age + 1 AS plus_one FROM events");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    const auto* proj = find_op(spec, "project_row");
    ASSERT_NE(proj, nullptr);
    const auto& out_text = proj->params.at("outputs");
    EXPECT_NE(out_text.find("\"op\":\"add\""), std::string::npos);
    EXPECT_NE(out_text.find("\"col\":\"age\""), std::string::npos);
    EXPECT_NE(out_text.find("\"lit\":1"), std::string::npos);
}

TEST(SqlPhysical, WhereLowersToFilterStringPredicate) {
    Catalog cat;
    register_text(cat, "src_t", "file", "/tmp/in");
    register_text(cat, "dst_t", "file", "/tmp/out");
    auto plan = bind_insert(cat, "INSERT INTO dst_t SELECT line FROM src_t WHERE line = 'hi'");
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    // 4 ops: scan, filter, project, sink.
    EXPECT_EQ(spec.ops.size(), 4u);
    const auto* filter = find_op(spec, "filter_string_predicate");
    ASSERT_NE(filter, nullptr);
    ASSERT_EQ(filter->params.count("predicate"), 1u);
    EXPECT_NE(filter->params.at("predicate").find("\"op\":\"eq\""), std::string::npos);
    EXPECT_NE(filter->params.at("predicate").find("\"literal\":\"hi\""), std::string::npos);
}

TEST(SqlPhysical, RejectsUnknownConnector) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE weird_src (line TEXT) WITH (connector='snowflake', path='/x');"
        "CREATE TABLE dst_t (line TEXT) WITH (connector='file', path='/tmp/y')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    auto plan = bind_insert(cat, "INSERT INTO dst_t SELECT line FROM weird_src");
    PhysicalPlanner pp;
    EXPECT_THROW(pp.compile(static_cast<const LogicalSink&>(*plan)), TranslationError);
}

// A plan carrying a null child (e.g. an optimizer pass that threw mid-rewrite and
// left a moved-out slot) must be rejected by the planner with a clean
// TranslationError, never dereferenced into a crash. Exercised end-to-end: a
// forced throw inside reorder_subtree's commit window (after move_leaves nulls a
// join's slot) is absorbed by optimize()'s guard, which returns the now-null-child
// plan; compiling it must surface the null cleanly.
TEST(SqlPhysical, MidReorderThrowYieldsNullChildRejectedCleanly) {
    Catalog cat;
    register_json(cat, "big", "(k BIGINT, bv BIGINT)", "row_count='1000000', ndv_k='1000000'");
    register_json(cat, "mid", "(k BIGINT, mv BIGINT)", "row_count='1000', ndv_k='1000'");
    register_json(cat, "small", "(k BIGINT, sv BIGINT)", "row_count='10', ndv_k='10'");
    register_json(cat, "dst", "(bv BIGINT)", "");
    auto plan = bind_insert(cat,
                            "INSERT INTO dst SELECT big_bv FROM big JOIN mid ON big.k = mid.k "
                            "JOIN small ON mid.k = small.k");

    reorder_detail::force_throw_after_move_flag() = true;
    std::unique_ptr<LogicalPlan> opt;
    EXPECT_NO_THROW({ opt = optimize(std::move(plan)); });  // guard absorbs the mid-reorder throw
    reorder_detail::force_throw_after_move_flag() = false;  // reset before any ASSERT can return

    ASSERT_NE(opt, nullptr);
    PhysicalPlanner pp;
    // The mid-reorder throw left a null child; the planner backstop rejects it
    // with a clean error instead of a null-deref crash.
    EXPECT_THROW(pp.compile(static_cast<const LogicalSink&>(*opt)), TranslationError);
}

}  // namespace clink::sql
