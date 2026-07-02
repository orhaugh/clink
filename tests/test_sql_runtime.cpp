// SQL end-to-end runtime test.
//
// Compiles a SQL job through the full frontend (parser -> binder ->
// optimizer -> physical planner), then runs the resulting
// JobGraphSpec in-process against a JobManager + TaskManager pair.
// The SQL ops are registered on the process-wide registry by
// clink::sql::install at test start.

#include <algorithm>
#include <any>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>

#include "clink/application/job_submitter.hpp"
#include "clink/async/task.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/dag_builder_registry.hpp"
#include "clink/cluster/job_manager.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/refresh_scheduler.hpp"
#include "clink/cluster/task_manager.hpp"
#include "clink/config/json.hpp"
#include "clink/operators/agg_function_registry.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/operators/scalar_function_registry.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/dead_letter.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/sql/analyze.hpp"
#include "clink/sql/async_function_registry.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/model_provider.hpp"
#ifdef CLINK_TESTS_HAVE_VECTOR_SEARCH
#include "clink/vector_search/install.hpp"
#endif
#include "clink/sql/cardinality.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/materialized_view.hpp"
#include "clink/sql/optimizer.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/physical_plan.hpp"
#include "clink/sql/ptf_registry.hpp"
#include "clink/sql/row_kind.hpp"
#include "clink/sql/table_api.hpp"
#include "clink/sql/view.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/remote_pool.hpp"
#include "clink/state/remote_read_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace clink::sql {

namespace {

void write_lines(const std::filesystem::path& path, const std::vector<std::string>& lines) {
    std::ofstream out(path);
    for (const auto& l : lines)
        out << l << "\n";
}

std::vector<std::string> read_lines(const std::filesystem::path& path) {
    std::ifstream in(path);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty())
            lines.push_back(line);
    }
    return lines;
}

// Single static install: registering SQL ops a second time on the
// global registry throws. Tests share the same process so we install
// once and let every test reuse it.
void ensure_sql_installed_once() {
    static bool done = [] {
        cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::sql::install(reg);
#ifdef CLINK_TESTS_HAVE_VECTOR_SEARCH
        // SQL-native AI: register vector_search_row after the Row channel exists.
        clink::vector_search::install(reg);
#endif
        // SQL-native AI: a deterministic in-process model provider for ML_PREDICT
        // end-to-end tests (no network). Reads the first feature value, doubles it
        // into the first OUTPUT column, and sets a second OUTPUT column to "ok".
        ModelProviderRegistry::global().register_provider(
            "test_double", [](const std::map<std::string, std::string>& opts) {
                auto split = [](const std::string& s) {
                    std::vector<std::string> out;
                    std::size_t start = 0;
                    while (start <= s.size()) {
                        auto comma = s.find(',', start);
                        auto end = comma == std::string::npos ? s.size() : comma;
                        if (end > start)
                            out.push_back(s.substr(start, end - start));
                        if (comma == std::string::npos)
                            break;
                        start = comma + 1;
                    }
                    return out;
                };
                auto feats = split(opts.count("feature_columns") ? opts.at("feature_columns") : "");
                auto outs = split(opts.count("output_columns") ? opts.at("output_columns") : "");
                return make_closure_provider("test_double", [feats, outs](const Row& f) -> Row {
                    double v = 0.0;
                    if (!feats.empty()) {
                        auto it = f.values.find(feats[0]);
                        if (it != f.values.end() && it->second.is_number())
                            v = it->second.as_number();
                    }
                    Row out;
                    if (!outs.empty())
                        out.values[outs[0]] = clink::config::JsonValue{v * 2.0};
                    if (outs.size() >= 2)
                        out.values[outs[1]] = clink::config::JsonValue{std::string("ok")};
                    return out;
                });
            });
        return true;
    }();
    (void)done;
}

cluster::JobGraphSpec compile(const Catalog& cat, const char* sql, bool async_agg = false) {
    Binder b(cat);
    auto plan = b.bind_insert(std::get<ast::InsertStmt>(parse(sql).statements[0]));
    plan = optimize(std::move(plan));
    PhysicalPlanner pp;
    pp.set_async_state_for_aggregation(async_agg);
    return pp.compile(static_cast<const LogicalSink&>(*plan));
}

struct InProcessCluster {
    cluster::JobManager jm;
    std::uint16_t jm_port{};
    std::unique_ptr<cluster::TaskManager> tm;

    InProcessCluster(const std::string& tm_id, std::size_t slots) {
        jm_port = jm.start();
        jm.expect_tms({tm_id});
        cluster::TaskManager::Config cfg;
        cfg.slot_count = slots;
        tm = std::make_unique<cluster::TaskManager>(tm_id, "127.0.0.1", cfg);
        tm->connect_to_jm("127.0.0.1", jm_port);
        std::this_thread::sleep_for(150ms);
    }
    ~InProcessCluster() {
        if (tm)
            tm->stop();
        jm.stop();
    }
};

}  // namespace

// A SELECT alias on a GROUP BY key is honoured in the emitted output column
// (previously the aggregate emitted the key under its raw name, so the alias
// was lost). user_id 1 -> 47, user_id 2 -> 25, emitted under "uid".
TEST(SqlRuntime, GroupByKeyAliasHonouredInOutput) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_gka_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_gka_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path,
                {R"({"user_id":1,"amount":10})",
                 R"({"user_id":2,"amount":20})",
                 R"({"user_id":1,"amount":30})",
                 R"({"user_id":2,"amount":5})",
                 R"({"user_id":1,"amount":7})"});
    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (uid BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(
        cat,
        "INSERT INTO out_t SELECT user_id AS uid, SUM(amount) AS total FROM orders GROUP BY "
        "user_id");

    InProcessCluster cluster("tm-sql-gka", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    ASSERT_FALSE(lines.empty());
    std::map<std::int64_t, std::int64_t> final_by_uid;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        ASSERT_TRUE(js.is_object());
        ASSERT_TRUE(js.as_object().count("uid") == 1) << "key emitted under wrong name: " << l;
        final_by_uid[static_cast<std::int64_t>(js.at("uid").as_number())] =
            static_cast<std::int64_t>(js.at("total").as_number());
    }
    EXPECT_EQ(final_by_uid[1], 47);
    EXPECT_EQ(final_by_uid[2], 25);
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// connector='blackhole' binds to the discard-sink factory registered by
// install() (blackhole_sink_row): a bounded file source -> blackhole runs to
// completion, the sink counting then dropping every row. Guards the core discard
// connector used to measure engine throughput without sink I/O distorting it -
// the job would fail to build if the factory were not registered.
TEST(SqlRuntime, BlackholeSinkRunsToCompletion) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_bh_in.ndjson";
    std::filesystem::remove(in_path);
    write_lines(in_path, {R"({"a":1,"b":10})", R"({"a":2,"b":20})", R"({"a":3,"b":30})"});
    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE src_bh (a BIGINT, b BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE sink_bh (a BIGINT, b BIGINT) WITH (connector='blackhole')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat, "INSERT INTO sink_bh SELECT a, b FROM src_bh");

    bool has_bh = false;
    for (const auto& op : spec.ops)
        if (op.type == "blackhole_sink_row")
            has_bh = true;
    EXPECT_TRUE(has_bh) << "connector='blackhole' did not map to blackhole_sink_row";

    InProcessCluster cluster("tm-sql-bh", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    std::filesystem::remove(in_path);
}

TEST(SqlRuntime, UnboundedGroupBySumRunsEndToEnd) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_orders.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_totals.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":10})",
                    R"({"user_id":2,"amount":20})",
                    R"({"user_id":1,"amount":30})",
                    R"({"user_id":2,"amount":5})",
                    R"({"user_id":1,"amount":7})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT user_id, SUM(amount) AS total "
                        "FROM orders GROUP BY user_id");

    InProcessCluster cluster("tm-sql-e2e-groupby", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Unbounded GROUP BY emits the latest aggregate row on every
    // input. The last emit for each group is the running total:
    //   user_id=1 -> 10 + 30 + 7 = 47
    //   user_id=2 -> 20 + 5      = 25
    // The output file therefore contains every intermediate state;
    // we verify the final totals by picking the last row per group.
    auto lines = read_lines(out_path);
    ASSERT_FALSE(lines.empty());

    std::int64_t final_user1 = -1;
    std::int64_t final_user2 = -1;
    for (const auto& line : lines) {
        auto js = clink::config::parse(line);
        ASSERT_TRUE(js.is_object()) << "bad output line: " << line;
        auto uid = static_cast<std::int64_t>(js.at("user_id").as_number());
        auto total = static_cast<std::int64_t>(js.at("total").as_number());
        if (uid == 1)
            final_user1 = total;
        else if (uid == 2)
            final_user2 = total;
    }
    EXPECT_EQ(final_user1, 47);
    EXPECT_EQ(final_user2, 25);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Auto-on: a GROUP BY compiled WITHOUT set_async_state_for_aggregation still
// rides the deferring async-state path when the bound backend can defer reads
// (supports_async_get()). On a non-deferring backend the byte-for-byte
// in-memory path runs and produces identical totals. This is the production
// "deferring backend is the default" behaviour - no manual opt-in - exercised
// cluster-free (VectorSource -> aggregate_row -> CollectingSink) so we can
// inject the backend instance and read its counters.
TEST(SqlRuntime, GroupByAutoActivatesAsyncOnDeferringBackend) {
    ensure_sql_installed_once();

    // Compile with async_agg=FALSE: the plan carries NO async_state param, so
    // the operator's async_state_ is false and only its backend-aware open()
    // can turn the async path on.
    Catalog cat;
    auto ddl = parse(
        std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                    "WITH (connector='file', format='json', path='/tmp/auto_on_orders.ndjson');"
                    "CREATE TABLE out_t (user_id BIGINT, total BIGINT) "
                    "WITH (connector='file', format='json', path='/tmp/auto_on_totals.ndjson')"});
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT user_id, SUM(amount) AS total "
                        "FROM orders GROUP BY user_id",
                        /*async_agg=*/false);

    std::map<std::string, std::string> agg_params;
    for (const auto& op : spec.ops) {
        if (op.type == "aggregate_row") {
            agg_params = op.params;
        }
    }
    ASSERT_FALSE(agg_params.empty()) << "no aggregate_row op in the compiled spec";
    EXPECT_EQ(agg_params.count("async_state"), 0u)
        << "async_state must NOT be in the plan (auto-on must not depend on the manual flag)";

    auto mk = [](std::int64_t uid, std::int64_t amount) {
        Row r;
        r.values["user_id"] = clink::config::JsonValue{static_cast<double>(uid)};
        r.values["amount"] = clink::config::JsonValue{static_cast<double>(amount)};
        return Record<Row>{std::move(r)};
    };
    const std::vector<Record<Row>> input = {
        mk(1, 10),
        mk(2, 20),
        mk(1, 30),
        mk(2, 5),
        mk(1, 7),
    };

    auto build_agg = [&]() {
        const auto* factory = cluster::OperatorRegistry::default_instance().find_operator(
            "aggregate_row", std::string{kChannelRow}, std::string{kChannelRow});
        EXPECT_NE(factory, nullptr);
        cluster::OperatorBuildContext octx;
        octx.params = agg_params;
        return std::static_pointer_cast<Operator<Row, Row>>(factory->build(octx));
    };

    // Drive the GROUP BY through a cluster-free Dag on `backend`; return the
    // final per-key totals (unbounded GROUP BY emits the running total per
    // input row, so the last emit per key is the answer).
    auto run_on = [&](std::shared_ptr<StateBackend> backend) {
        Dag dag;
        auto src = std::make_shared<VectorSource<Row>>(input);
        auto h_src = dag.add_source<Row>(src);
        auto h_op = dag.add_operator<Row, Row>(h_src, build_agg());
        auto sink = std::make_shared<CollectingSink<Row>>();
        dag.add_sink<Row>(h_op, sink);
        JobConfig cfg;
        cfg.state_backend = std::move(backend);
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();
        std::int64_t u1 = -1;
        std::int64_t u2 = -1;
        for (const auto& rec : sink->collected_records()) {
            const Row& row = rec.value();
            const auto uid = static_cast<std::int64_t>(row.values.at("user_id").as_number());
            const auto total = static_cast<std::int64_t>(row.values.at("total").as_number());
            if (uid == 1) {
                u1 = total;
            } else if (uid == 2) {
                u2 = total;
            }
        }
        return std::make_pair(u1, u2);
    };

    // Baseline: non-deferring in-memory backend -> in-memory map path (today's
    // default), totals 1->10+30+7=47, 2->20+5=25.
    const auto [base1, base2] = run_on(std::make_shared<InMemoryStateBackend>());
    EXPECT_EQ(base1, 47);
    EXPECT_EQ(base2, 25);

    // Auto-on: a deferring backend (RemoteReadBackend over an in-memory pool,
    // i.e. the disagg-local:// scheme) activates the async KeyedState path with
    // NO manual flag. Identical totals, and remote_loads() proves group state
    // was read THROUGH the deferring tier - the in-memory map path never reads
    // the backend, so it would leave remote_loads() == 0. Two distinct keys =>
    // at least two first-touch cold reads.
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    const auto [auto1, auto2] = run_on(rrb);
    EXPECT_EQ(auto1, 47);
    EXPECT_EQ(auto2, 25);
    EXPECT_GE(rrb->remote_loads(), 2u)
        << "auto-on did not route GROUP BY state through the deferring backend";
}

// SELECT DISTINCT dedups end-to-end AND rides the checkpointed KeyedState path,
// auto-activating the async/disaggregated path on a deferring backend. The op
// once held an in-memory FlatSet that was never snapshotted (a restore replayed
// already-emitted rows); it is now a KeyedState presence-marker per distinct
// row. Baseline (in-memory backend, sync KeyedState) and a RemoteReadBackend
// (disagg-local://) must dedup identically; remote_loads() proves the seen-set
// was read THROUGH the deferring tier - the sync path leaves it 0. Cluster-free
// (VectorSource -> distinct_row -> CollectingSink) so the backend + its counters
// are injectable. Also locks the planner contract that distinct_row is handed
// the dedup columns, so its state key routes to the record's key-group.
TEST(SqlRuntime, SelectDistinctDedupsAndAutoRidesDeferringBackend) {
    ensure_sql_installed_once();

    Catalog cat;
    auto ddl = parse(
        std::string{"CREATE TABLE clicks (user_id BIGINT, url TEXT) "
                    "WITH (connector='file', format='json', path='/tmp/distinct_clicks.ndjson');"
                    "CREATE TABLE out_t (user_id BIGINT, url TEXT) "
                    "WITH (connector='file', format='json', path='/tmp/distinct_out.ndjson')"});
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat, "INSERT INTO out_t SELECT DISTINCT user_id, url FROM clicks");

    std::map<std::string, std::string> dist_params;
    for (const auto& op : spec.ops) {
        if (op.type == "distinct_row") {
            dist_params = op.params;
        }
    }
    ASSERT_FALSE(dist_params.empty()) << "no distinct_row op in the compiled spec";
    EXPECT_EQ(dist_params.at("columns"), "user_id,url")
        << "planner must hand distinct_row the dedup columns for a routing-consistent state key";

    // (1,a) x3, (1,b), (2,a) -> 3 distinct rows.
    auto mk = [](std::int64_t uid, const char* url) {
        Row r;
        r.values["user_id"] = clink::config::JsonValue{static_cast<double>(uid)};
        r.values["url"] = clink::config::JsonValue{std::string{url}};
        return Record<Row>{std::move(r)};
    };
    const std::vector<Record<Row>> input = {
        mk(1, "a"),
        mk(1, "a"),
        mk(1, "b"),
        mk(2, "a"),
        mk(1, "a"),
    };

    auto build_distinct = [&]() {
        const auto* factory = cluster::OperatorRegistry::default_instance().find_operator(
            "distinct_row", std::string{kChannelRow}, std::string{kChannelRow});
        EXPECT_NE(factory, nullptr);
        cluster::OperatorBuildContext octx;
        octx.params = dist_params;
        return std::static_pointer_cast<Operator<Row, Row>>(factory->build(octx));
    };

    auto run_on = [&](std::shared_ptr<StateBackend> backend) {
        Dag dag;
        auto src = std::make_shared<VectorSource<Row>>(input);
        auto h_src = dag.add_source<Row>(src);
        auto h_op = dag.add_operator<Row, Row>(h_src, build_distinct());
        auto sink = std::make_shared<CollectingSink<Row>>();
        dag.add_sink<Row>(h_op, sink);
        JobConfig cfg;
        cfg.state_backend = std::move(backend);
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();
        std::set<std::pair<std::int64_t, std::string>> out;
        for (const auto& rec : sink->collected_records()) {
            const Row& row = rec.value();
            out.emplace(static_cast<std::int64_t>(row.values.at("user_id").as_number()),
                        row.values.at("url").as_string());
        }
        return out;
    };

    const std::set<std::pair<std::int64_t, std::string>> expected = {{1, "a"}, {1, "b"}, {2, "a"}};

    // Baseline: non-deferring in-memory backend -> synchronous KeyedState path.
    EXPECT_EQ(run_on(std::make_shared<InMemoryStateBackend>()), expected);

    // Auto-on: a deferring backend activates the async KeyedState path with no
    // manual flag. Identical dedup, and remote_loads() >= 3 (three distinct
    // rows => three first-touch cold reads) proves it rode the deferring tier.
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    EXPECT_EQ(run_on(rrb), expected);
    EXPECT_GE(rrb->remote_loads(), 3u)
        << "auto-on did not route DISTINCT state through the deferring backend";
}

// Regression for the coalescing async submit-retry livelock: a single batch that
// carries MORE than the in-flight cap (kDefaultMaxInFlight = 6000) distinct keys
// fills the cap mid-batch. With coalesce_reads() on (auto for GROUP BY over a
// deferring backend) every submitted record's first step is a get_async the
// coalescer PARKS until flush(); the old submit-retry spin called only poll(),
// which never flushed, so capacity never freed and the runner thread livelocked.
// poll_or_flush() in the spin issues the parked batch so it makes progress. A
// watchdog turns a regression into a bounded failure instead of a CI hang.
TEST(SqlRuntime, GroupByFatBatchDoesNotLivelockAtInFlightCap) {
    ensure_sql_installed_once();

    Catalog cat;
    auto ddl = parse(
        std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                    "WITH (connector='file', format='json', path='/tmp/fatbatch_orders.ndjson');"
                    "CREATE TABLE out_t (user_id BIGINT, total BIGINT) "
                    "WITH (connector='file', format='json', path='/tmp/fatbatch_totals.ndjson')"});
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT user_id, SUM(amount) AS total "
                        "FROM orders GROUP BY user_id",
                        /*async_agg=*/false);
    std::map<std::string, std::string> agg_params;
    for (const auto& op : spec.ops) {
        if (op.type == "aggregate_row") {
            agg_params = op.params;
        }
    }
    ASSERT_FALSE(agg_params.empty());

    // One fat batch (VectorSource ships its whole vector as a single Batch) of
    // N > 6000 distinct keys, so process_async submits past the cap before the
    // runner's post-batch flush can run.
    constexpr int kKeys = 6500;
    std::vector<Record<Row>> input;
    input.reserve(kKeys);
    for (int i = 0; i < kKeys; ++i) {
        Row r;
        r.values["user_id"] = clink::config::JsonValue{static_cast<double>(i)};
        r.values["amount"] = clink::config::JsonValue{static_cast<double>(i)};
        input.push_back(Record<Row>{std::move(r)});
    }

    auto run = [&]() -> std::size_t {
        const auto* factory = cluster::OperatorRegistry::default_instance().find_operator(
            "aggregate_row", std::string{kChannelRow}, std::string{kChannelRow});
        cluster::OperatorBuildContext octx;
        octx.params = agg_params;
        auto op = std::static_pointer_cast<Operator<Row, Row>>(factory->build(octx));

        Dag dag;
        auto src = std::make_shared<VectorSource<Row>>(input);
        auto h_src = dag.add_source<Row>(src);
        auto h_op = dag.add_operator<Row, Row>(h_src, op);
        auto sink = std::make_shared<CollectingSink<Row>>();
        dag.add_sink<Row>(h_op, sink);
        JobConfig cfg;
        cfg.state_backend = std::make_shared<RemoteReadBackend>(
            std::make_shared<InMemoryRemotePool>(), /*io_threads=*/1, /*hot_max_bytes=*/0);
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();
        return sink->collected_records().size();
    };

    std::packaged_task<std::size_t()> task(run);
    auto fut = task.get_future();
    std::thread th(std::move(task));
    if (fut.wait_for(std::chrono::seconds(60)) != std::future_status::ready) {
        th.detach();  // wedged runner thread; only reached on a real regression
        FAIL() << "GROUP BY over a >cap fat batch livelocked at the in-flight cap";
    }
    th.join();
    // Unbounded GROUP BY emits the running total per input row, so every one of
    // the kKeys rows produced an emit - none were stranded behind the cap.
    EXPECT_EQ(fut.get(), static_cast<std::size_t>(kKeys));
}

// Increment 5: a SQL stream-stream INNER join run over a deferring backend
// (disagg-local://) auto-rides EquiJoinRowOp's async/disaggregated KeyedState
// path (process_async{1,2} + the per-key row-list codec through the remote pool)
// and produces the correct join, end-to-end. The codec itself is round-trip
// tested in test_sql_row.cpp; this proves the join executes correctly when its
// per-side state lives in the disaggregated pool. (Proving the async path was
// taken vs a sync fallback needs a backend-level signal not reachable through
// the cluster; the auto-on mechanism is the same one AsyncStateGroupByMatches
// InMemory proves takes the async path.)
TEST(SqlRuntime, AsyncStateInnerJoinOverDisaggProducesCorrectOutput) {
    ensure_sql_installed_once();
    const auto l_path = std::filesystem::temp_directory_path() / "clink_sql_join_left.ndjson";
    const auto r_path = std::filesystem::temp_directory_path() / "clink_sql_join_right.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_join_out.ndjson";
    std::filesystem::remove(l_path);
    std::filesystem::remove(r_path);
    std::filesystem::remove(out_path);
    write_lines(l_path, {R"({"id":1,"lv":10})", R"({"id":2,"lv":20})", R"({"id":1,"lv":11})"});
    write_lines(r_path, {R"({"id":1,"rv":100})", R"({"id":1,"rv":101})", R"({"id":3,"rv":300})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE lt (id BIGINT, lv BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     l_path.string() +
                     "');"
                     "CREATE TABLE rt (id BIGINT, rv BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     r_path.string() +
                     "');"
                     "CREATE TABLE jout (lv BIGINT, rv BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (int i = 0; i < 3; ++i) {
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[i]));
    }
    auto spec = compile(
        cat, "INSERT INTO jout SELECT l_lv AS lv, r_rv AS rv FROM lt l JOIN rt r ON l.id = r.id");

    InProcessCluster cluster("tm-sql-join-disagg", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    // A deferring backend: EquiJoinRowOp::open() auto-enables the async KeyedState
    // path, so each side's per-key entry list lives in the disaggregated pool.
    opts.checkpoint.state_backend_uri = "disagg-local://";
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    std::vector<std::pair<std::int64_t, std::int64_t>> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        ASSERT_TRUE(js.is_object());
        got.emplace_back(static_cast<std::int64_t>(js.at("lv").as_number()),
                         static_cast<std::int64_t>(js.at("rv").as_number()));
    }
    std::sort(got.begin(), got.end());
    // id=1 matches: lv in {10,11} x rv in {100,101}; id=2 (left) / id=3 (right) unmatched.
    const std::vector<std::pair<std::int64_t, std::int64_t>> want = {
        {10, 100}, {10, 101}, {11, 100}, {11, 101}};
    EXPECT_EQ(got, want);
    std::filesystem::remove(l_path);
    std::filesystem::remove(r_path);
    std::filesystem::remove(out_path);
}

// Stronger than the SQL-cluster join e2e above: build the real EquiJoinRowOp via
// its registered Dag builder into a cluster-free Dag over an INJECTABLE deferring
// backend, so we can read remote_loads() and prove the INNER join actually rode
// the async / remote-read path (not just that the output was correct). SQL co-ops
// can't be built via OperatorRegistry::find_co_operator (none exists); the
// DagBuilderRegistry seam (populated by install()) is how a test reaches one.
TEST(SqlRuntime, AsyncStateInnerJoinRidesRemoteReadPath) {
    ensure_sql_installed_once();
    const auto* builder = cluster::DagBuilderRegistry::default_instance().find("equi_join_row");
    ASSERT_NE(builder, nullptr) << "equi_join_row Dag builder not registered";

    auto mkrow = [](std::int64_t id, const char* col, std::int64_t v) {
        Row r;
        r.values["id"] = clink::config::JsonValue{static_cast<double>(id)};
        r.values[col] = clink::config::JsonValue{static_cast<double>(v)};
        return Record<Row>{std::move(r)};
    };
    const std::vector<Record<Row>> left = {
        mkrow(1, "lv", 10), mkrow(2, "lv", 20), mkrow(1, "lv", 11)};
    const std::vector<Record<Row>> right = {
        mkrow(1, "rv", 100), mkrow(1, "rv", 101), mkrow(3, "rv", 300)};

    Dag dag;
    auto hl = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(left));
    auto hr = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(right));
    clink::plugin::BuildContext ctx;
    ctx.params["left_key_column"] = "id";
    ctx.params["right_key_column"] = "id";
    ctx.params["left_alias"] = "l";
    ctx.params["right_alias"] = "r";
    ctx.params["join_type"] = "inner";
    ctx.params["left_columns"] = "id,lv";
    ctx.params["right_columns"] = "id,rv";
    std::vector<std::any> upstream = {std::any{hl}, std::any{hr}};
    auto built = (*builder)(dag, upstream, ctx);
    auto h_join = std::any_cast<StageHandle<Row>>(built.main_handle);
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(h_join, sink);

    // Deferring backend: EquiJoinRowOp auto-enables the async KeyedState path.
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    JobConfig cfg;
    cfg.state_backend = rrb;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    std::vector<std::pair<std::int64_t, std::int64_t>> got;
    for (const auto& rec : sink->collected_records()) {
        const Row& r = rec.value();
        got.emplace_back(static_cast<std::int64_t>(r.values.at("l_lv").as_number()),
                         static_cast<std::int64_t>(r.values.at("r_rv").as_number()));
    }
    std::sort(got.begin(), got.end());
    const std::vector<std::pair<std::int64_t, std::int64_t>> want = {
        {10, 100}, {10, 101}, {11, 100}, {11, 101}};
    EXPECT_EQ(got, want);
    EXPECT_GT(rrb->remote_loads(), 0u)
        << "INNER join did not route its per-key state through the deferring backend (async path)";
}

// SET operations (INTERSECT / EXCEPT) ride the checkpointed KeyedState path and
// auto-activate the async/disaggregated path on a deferring backend. The op once
// held five in-memory maps (per-side counts, representatives, emitted
// multiplicity) that were never snapshotted, so a restore re-emitted or dropped
// rows; both sides' per-tuple state now lives in ONE KeyedState bucket. Driven
// cluster-free through the set_op_row Dag builder (two VectorSources -> co-op) on
// a RemoteReadBackend so remote_loads() proves the tuple state went through the
// deferring tier. Interleaving of the two inputs differs between the sync and
// async runners, so we assert on the settled live set (net inserts minus
// retractions), not the raw emission order. Distinct INTERSECT of {1,2,2,3} and
// {2,3,3,4} = {2,3}.
TEST(SqlRuntime, SetOpIntersectRidesRemoteReadPath) {
    ensure_sql_installed_once();
    const auto* builder = cluster::DagBuilderRegistry::default_instance().find("set_op_row");
    ASSERT_NE(builder, nullptr) << "set_op_row Dag builder not registered";

    auto mkrow = [](std::int64_t v) {
        Row r;
        r.values["v"] = clink::config::JsonValue{static_cast<double>(v)};
        return Record<Row>{std::move(r)};
    };
    const std::vector<Record<Row>> left = {mkrow(1), mkrow(2), mkrow(2), mkrow(3)};
    const std::vector<Record<Row>> right = {mkrow(2), mkrow(3), mkrow(3), mkrow(4)};

    Dag dag;
    auto hl = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(left));
    auto hr = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(right));
    clink::plugin::BuildContext ctx;
    ctx.params["mode"] = "intersect";
    ctx.params["all"] = "false";
    ctx.params["left_columns"] = "v";
    ctx.params["right_columns"] = "v";
    std::vector<std::any> upstream = {std::any{hl}, std::any{hr}};
    auto built = (*builder)(dag, upstream, ctx);
    auto h_op = std::any_cast<StageHandle<Row>>(built.main_handle);
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(h_op, sink);

    // Deferring backend: SetOpRowOp auto-enables the async KeyedState path.
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    JobConfig cfg;
    cfg.state_backend = rrb;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    std::map<std::int64_t, int> net;
    for (const auto& rec : sink->collected_records()) {
        Row r = rec.value();
        const std::string kind = row_kind_of(r);
        const auto v = static_cast<std::int64_t>(r.values.at("v").as_number());
        net[v] += is_delete_like(kind) ? -1 : 1;
    }
    std::set<std::int64_t> present;
    for (const auto& [v, n] : net) {
        if (n > 0)
            present.insert(v);
    }
    EXPECT_EQ(present, (std::set<std::int64_t>{2, 3}));
    EXPECT_GT(rrb->remote_loads(), 0u)
        << "INTERSECT did not route its per-tuple state through the deferring backend (async path)";
}

// A plain semi join (IN / EXISTS, null_aware=0) rides the checkpointed KeyedState
// path and auto-activates the async/disaggregated path on a deferring backend.
// The left entries and the right presence-count were in-memory maps that a
// restore would lose; both now live in per-join-key KeyedState slots. Null-aware
// NOT IN keeps the in-memory path (cross-key NULL poison, parallelism 1). Driven
// cluster-free through the semi_join_row Dag builder (two VectorSources -> co-op)
// on a RemoteReadBackend so remote_loads() proves the state went through the
// deferring tier. Semi(IN) of left {1,2,3} against right {2,3,3} = {2,3}.
TEST(SqlRuntime, SemiJoinInRidesRemoteReadPath) {
    ensure_sql_installed_once();
    const auto* builder = cluster::DagBuilderRegistry::default_instance().find("semi_join_row");
    ASSERT_NE(builder, nullptr) << "semi_join_row Dag builder not registered";

    auto mkrow = [](std::int64_t k) {
        Row r;
        r.values["k"] = clink::config::JsonValue{static_cast<double>(k)};
        return Record<Row>{std::move(r)};
    };
    const std::vector<Record<Row>> left = {mkrow(1), mkrow(2), mkrow(3)};
    const std::vector<Record<Row>> right = {mkrow(2), mkrow(3), mkrow(3)};

    Dag dag;
    auto hl = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(left));
    auto hr = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(right));
    clink::plugin::BuildContext ctx;
    ctx.params["left_key_column"] = "k";
    ctx.params["right_key_column"] = "k";
    ctx.params["anti"] = "0";
    ctx.params["null_aware"] = "0";
    std::vector<std::any> upstream = {std::any{hl}, std::any{hr}};
    auto built = (*builder)(dag, upstream, ctx);
    auto h_op = std::any_cast<StageHandle<Row>>(built.main_handle);
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(h_op, sink);

    // Deferring backend: SemiAntiJoinRowOp's plain path auto-enables async.
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    JobConfig cfg;
    cfg.state_backend = rrb;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    // Semi is insert-only over append inputs; net the live set to be order-safe.
    std::map<std::int64_t, int> net;
    for (const auto& rec : sink->collected_records()) {
        Row r = rec.value();
        const std::string kind = row_kind_of(r);
        const auto k = static_cast<std::int64_t>(r.values.at("k").as_number());
        net[k] += is_delete_like(kind) ? -1 : 1;
    }
    std::set<std::int64_t> present;
    for (const auto& [k, n] : net) {
        if (n > 0)
            present.insert(k);
    }
    EXPECT_EQ(present, (std::set<std::int64_t>{2, 3}));
    EXPECT_GT(rrb->remote_loads(), 0u)
        << "semi join did not route its per-key state through the deferring backend (async path)";
}

// PARITY-P1 whole-job proof: a job with MULTIPLE distinct stateful operators (an
// INNER equi-join feeding a GROUP BY, both auto-async on a deferring backend)
// SHARING ONE backend instance has every stateful op ride the async path and
// completes correctly - i.e. selecting a disaggregated backend is the single
// switch that makes the whole job async. Verified by output == the in-memory
// baseline AND remote_loads > 0 (both ops' state went through the deferring tier).
//
// Also a regression for a fixed DEADLOCK: each async op's runner used to wire ITS
// controller into the backend's SINGLE resume-scheduler slot, so the second op
// clobbered the first and the first's cold-read completions were misrouted,
// hanging the job (stuck async-persist "snapshot-worker" teardown) at io_threads
// = 1 AND 8. RemoteReadBackend now keys the resume target by runner thread, so a
// shared backend routes each completion to the op that issued the read.
TEST(SqlRuntime, WholeJobRidesAsyncOnOneSharedDeferringBackend) {
    ensure_sql_installed_once();
    const auto* join_builder =
        cluster::DagBuilderRegistry::default_instance().find("equi_join_row");
    const auto* agg_builder = cluster::DagBuilderRegistry::default_instance().find("aggregate_row");
    ASSERT_NE(join_builder, nullptr);
    ASSERT_NE(agg_builder, nullptr) << "aggregate_row Dag builder not registered";

    auto mkrow = [](std::int64_t id, const char* col, std::int64_t v) {
        Row r;
        r.values["id"] = clink::config::JsonValue{static_cast<double>(id)};
        r.values[col] = clink::config::JsonValue{static_cast<double>(v)};
        return Record<Row>{std::move(r)};
    };
    const std::vector<Record<Row>> left = {
        mkrow(1, "lv", 10), mkrow(2, "lv", 20), mkrow(1, "lv", 11)};
    const std::vector<Record<Row>> right = {
        mkrow(1, "rv", 100), mkrow(1, "rv", 101), mkrow(3, "rv", 300)};

    // Build source -> equi_join_row -> aggregate_row -> sink on the given backend
    // and return the final SUM per join-key (unbounded GROUP BY emits the running
    // total per input row, so the last value per key is the answer).
    auto run_on = [&](std::shared_ptr<StateBackend> backend) {
        Dag dag;
        auto hl = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(left));
        auto hr = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(right));

        clink::plugin::BuildContext jctx;
        jctx.params["left_key_column"] = "id";
        jctx.params["right_key_column"] = "id";
        jctx.params["left_alias"] = "l";
        jctx.params["right_alias"] = "r";
        jctx.params["join_type"] = "inner";
        jctx.params["left_columns"] = "id,lv";
        jctx.params["right_columns"] = "id,rv";
        std::vector<std::any> j_up = {std::any{hl}, std::any{hr}};
        auto h_join = (*join_builder)(dag, j_up, jctx).main_handle;

        // GROUP BY the join key (l_id), SUM the left value (l_lv).
        clink::plugin::BuildContext actx;
        actx.params["group_keys"] = "l_id";
        actx.params["group_key_outputs"] = "l_id";
        actx.params["aggregates"] = R"([{"name":"tot","fn":"sum","input_column":"l_lv"}])";
        std::vector<std::any> a_up = {h_join};
        auto out = (*agg_builder)(dag, a_up, actx);
        auto h_agg = std::any_cast<StageHandle<Row>>(out.main_handle);

        auto sink = std::make_shared<CollectingSink<Row>>();
        dag.add_sink<Row>(h_agg, sink);
        JobConfig cfg;
        cfg.state_backend = std::move(backend);
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();

        std::map<std::int64_t, std::int64_t> final_by_key;
        for (const auto& rec : sink->collected_records()) {
            const Row& r = rec.value();
            final_by_key[static_cast<std::int64_t>(r.values.at("l_id").as_number())] =
                static_cast<std::int64_t>(r.values.at("tot").as_number());
        }
        return final_by_key;
    };

    const auto baseline = run_on(std::make_shared<InMemoryStateBackend>());
    // id=1: left lv {10,11} each join right {100,101} -> 4 rows, all l_id=1,
    // l_lv {10,10,11,11} -> SUM = 42.
    const std::map<std::int64_t, std::int64_t> want = {{1, 42}};
    EXPECT_EQ(baseline, want) << "in-memory baseline";

    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    const auto async_res = run_on(rrb);
    EXPECT_EQ(async_res, baseline)
        << "whole-job async output must match the in-memory baseline (correctness)";
    EXPECT_GT(rrb->remote_loads(), 0u)
        << "no stateful op rode the deferring backend - the one-switch whole-job async claim fails";
}

namespace {

// Build an equi-join of `join_type` via the registered Dag builder over
// `backend`, run it cluster-free, and return every emitted record (changelog,
// incl. __row_kind for outer Insert/Delete).
std::vector<Record<Row>> run_equi_join(const std::string& join_type,
                                       const std::vector<Record<Row>>& left,
                                       const std::vector<Record<Row>>& right,
                                       std::shared_ptr<StateBackend> backend) {
    const auto* builder = cluster::DagBuilderRegistry::default_instance().find("equi_join_row");
    Dag dag;
    auto hl = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(left));
    auto hr = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(right));
    clink::plugin::BuildContext ctx;
    ctx.params["left_key_column"] = "id";
    ctx.params["right_key_column"] = "id";
    ctx.params["left_alias"] = "l";
    ctx.params["right_alias"] = "r";
    ctx.params["join_type"] = join_type;
    ctx.params["left_columns"] = "id,lv";
    ctx.params["right_columns"] = "id,rv";
    std::vector<std::any> upstream = {std::any{hl}, std::any{hr}};
    auto out = (*builder)(dag, upstream, ctx);
    auto h_join = std::any_cast<StageHandle<Row>>(out.main_handle);
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(h_join, sink);
    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return sink->collected_records();
}

// Collapse an outer-join changelog into its order-INDEPENDENT final relation:
// per row identity (the row minus its __row_kind), net = +1 per insert-like and
// -1 per delete-like; identities with net > 0 are live. The emission SEQUENCE of
// an outer join depends on left/right arrival interleaving (which differs between
// the sync and async runners), but the final relation does not - so this is the
// right invariant to prove the async path matches the sync path.
std::vector<std::string> join_live_set(const std::vector<Record<Row>>& emissions) {
    std::map<std::string, int> net;
    for (const auto& rec : emissions) {
        Row r = rec.value();
        const std::string kind = row_kind_of(r);
        r.values.erase(std::string{kRowKindField});
        const std::string id =
            clink::config::JsonValue{clink::config::JsonObject{r.values}}.serialize(0);
        net[id] += is_delete_like(kind) ? -1 : 1;
    }
    std::vector<std::string> live;
    for (const auto& [id, n] : net) {
        if (n > 0) {
            live.push_back(id);
        }
    }
    std::sort(live.begin(), live.end());
    return live;
}

std::vector<Record<Row>> outer_join_rows(
    const std::vector<std::pair<std::int64_t, std::int64_t>>& kv, const char* col) {
    std::vector<Record<Row>> rows;
    for (const auto& [id, v] : kv) {
        Row r;
        r.values["id"] = clink::config::JsonValue{static_cast<double>(id)};
        r.values[col] = clink::config::JsonValue{static_cast<double>(v)};
        rows.push_back(Record<Row>{std::move(r)});
    }
    return rows;
}

}  // namespace

// OUTER joins ride the async/disaggregated path too (extends INNER): a match
// RETRACTS the matched row's live null-padding by mutating the OTHER side's
// entries - a read-modify-write of both sides, serialised under the per-key gate.
// Proven by comparing the COLLAPSED final relation of the async (deferring
// backend) path to the synchronous in-memory path: identical, and the async path
// went through the backend (remote_loads > 0). LEFT OUTER: l1 joins r1, l2 is
// null-padded (l1's earlier (l1,null) is retracted; l2's survives).
TEST(SqlRuntime, AsyncStateLeftOuterJoinMatchesSyncPath) {
    ensure_sql_installed_once();
    const auto left = outer_join_rows({{1, 10}, {2, 20}}, "lv");
    const auto right = outer_join_rows({{1, 100}}, "rv");

    const auto sync_live = join_live_set(
        run_equi_join("left_outer", left, right, std::make_shared<InMemoryStateBackend>()));
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    const auto async_live = join_live_set(run_equi_join("left_outer", left, right, rrb));

    EXPECT_EQ(sync_live.size(), 2u) << "l1 joined + l2 null-padded";
    EXPECT_EQ(async_live, sync_live)
        << "async LEFT OUTER must produce the same final relation as the sync path";
    EXPECT_GT(rrb->remote_loads(), 0u);
}

// FULL OUTER exercises retraction on BOTH sides: l1 joins r1; l2 null-padded (no
// right 2); r3 null-padded (no left 3).
TEST(SqlRuntime, AsyncStateFullOuterJoinMatchesSyncPath) {
    ensure_sql_installed_once();
    const auto left = outer_join_rows({{1, 10}, {2, 20}}, "lv");
    const auto right = outer_join_rows({{1, 100}, {3, 300}}, "rv");

    const auto sync_live = join_live_set(
        run_equi_join("full_outer", left, right, std::make_shared<InMemoryStateBackend>()));
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    const auto async_live = join_live_set(run_equi_join("full_outer", left, right, rrb));

    EXPECT_EQ(sync_live.size(), 3u) << "l1 joined + l2 null-padded + r3 null-padded";
    EXPECT_EQ(async_live, sync_live)
        << "async FULL OUTER must produce the same final relation as the sync path";
    EXPECT_GT(rrb->remote_loads(), 0u);
}

namespace {

// Build a tumbling-window SUM (size 10, GROUP BY k) via the registered Dag
// builder over `backend`, run it cluster-free, and return the emitted window
// rows. A deferring backend auto-enables WindowRowOp's async path: each group's
// window map lives in KeyedState (the pool, keyed by the group key) and every
// new window registers an event-time timer, so firing point-reads only the due
// group's map instead of the watermark scan the in-memory path uses.
std::vector<Record<Row>> run_tumbling_window(const std::vector<Record<Row>>& input,
                                             std::shared_ptr<StateBackend> backend) {
    const auto* builder =
        cluster::DagBuilderRegistry::default_instance().find("tumbling_window_row");
    Dag dag;
    auto h = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(input));
    clink::plugin::BuildContext ctx;
    ctx.params["time_column"] = "ts";
    ctx.params["size_ms"] = "10";
    ctx.params["group_keys"] = "k";
    ctx.params["group_key_outputs"] = "k";
    ctx.params["aggregates"] = R"([{"name":"s","fn":"sum","input_column":"amt"}])";
    std::vector<std::any> upstream = {std::any{h}};
    auto out = (*builder)(dag, upstream, ctx);
    auto h_win = std::any_cast<StageHandle<Row>>(out.main_handle);
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(h_win, sink);
    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return sink->collected_records();
}

// Build a hopping-window SUM (size 30, slide 10, GROUP BY k) via the registered
// Dag builder over `backend`. Overlapping windows mean one record lands in
// several windows, so a group can have multiple due win_ends at one watermark -
// the case the per-key fire-batching targets.
std::vector<Record<Row>> run_hopping_window(const std::vector<Record<Row>>& input,
                                            std::shared_ptr<StateBackend> backend) {
    const auto* builder =
        cluster::DagBuilderRegistry::default_instance().find("hopping_window_row");
    Dag dag;
    auto h = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(input));
    clink::plugin::BuildContext ctx;
    ctx.params["time_column"] = "ts";
    ctx.params["size_ms"] = "30";
    ctx.params["slide_ms"] = "10";
    ctx.params["group_keys"] = "k";
    ctx.params["group_key_outputs"] = "k";
    ctx.params["aggregates"] = R"([{"name":"s","fn":"sum","input_column":"amt"}])";
    std::vector<std::any> upstream = {std::any{h}};
    auto out = (*builder)(dag, upstream, ctx);
    auto h_win = std::any_cast<StageHandle<Row>>(out.main_handle);
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(h_win, sink);
    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return sink->collected_records();
}

// Build a cumulate-window SUM (size 30, step 10, GROUP BY k) via the registered
// Dag builder over `backend`. Cumulate slices SHARE a window_start and grow by
// step, so one record lands in every slice up to size - another multiple-due-
// windows-per-group case, distinct from hop in that the slices share a start.
std::vector<Record<Row>> run_cumulate_window(const std::vector<Record<Row>>& input,
                                             std::shared_ptr<StateBackend> backend) {
    const auto* builder =
        cluster::DagBuilderRegistry::default_instance().find("cumulate_window_row");
    Dag dag;
    auto h = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(input));
    clink::plugin::BuildContext ctx;
    ctx.params["time_column"] = "ts";
    ctx.params["size_ms"] = "30";
    ctx.params["step_ms"] = "10";  // cumulate carries the step in the slide slot
    ctx.params["group_keys"] = "k";
    ctx.params["group_key_outputs"] = "k";
    ctx.params["aggregates"] = R"([{"name":"s","fn":"sum","input_column":"amt"}])";
    std::vector<std::any> upstream = {std::any{h}};
    auto out = (*builder)(dag, upstream, ctx);
    auto h_win = std::any_cast<StageHandle<Row>>(out.main_handle);
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(h_win, sink);
    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return sink->collected_records();
}

Record<Row> window_input_row(std::int64_t k, std::int64_t ts, std::int64_t amt) {
    Row r;
    r.values["k"] = clink::config::JsonValue{static_cast<double>(k)};
    r.values["ts"] = clink::config::JsonValue{static_cast<double>(ts)};
    r.values["amt"] = clink::config::JsonValue{static_cast<double>(amt)};
    return Record<Row>{std::move(r)};
}

// (k, window_start) -> sum: the order-independent window relation. The async
// path fires windows from event-time timers and the sync path from a watermark
// scan, so emission order can differ; the relation must not.
std::map<std::pair<std::int64_t, std::int64_t>, std::int64_t> window_relation(
    const std::vector<Record<Row>>& rows) {
    std::map<std::pair<std::int64_t, std::int64_t>, std::int64_t> rel;
    for (const auto& rec : rows) {
        const Row& r = rec.value();
        rel[{static_cast<std::int64_t>(r.values.at("k").as_number()),
             static_cast<std::int64_t>(r.values.at("window_start").as_number())}] =
            static_cast<std::int64_t>(r.values.at("s").as_number());
    }
    return rel;
}

// Scripted source for the late-data regression: two records into window [0,10),
// a watermark at 10 that fires and PURGES that window, then a LATE record whose
// event time (2) falls in the already-fired window, then a record for window
// [10,20) and a closing watermark. A correct engine drops the late record; the
// pre-fix WindowRowOp re-created and re-fired [0,10), emitting a phantom row.
class LateDataWindowSource final : public Source<Row> {
public:
    bool produce(Emitter<Row>& out) override {
        if (this->cancelled() || done_) {
            return false;
        }
        Batch<Row> b1;
        b1.push(window_input_row(1, 1, 5));
        b1.push(window_input_row(1, 3, 7));
        out.emit_data(std::move(b1));
        out.emit_watermark(Watermark{EventTime{10}});  // fires + purges [0,10) (k=1, sum=12)
        Batch<Row> b2;
        b2.push(window_input_row(1, 2, 100));  // LATE: belongs to purged [0,10) -> must drop
        b2.push(window_input_row(1, 15, 3));   // window [10,20)
        out.emit_data(std::move(b2));
        out.emit_watermark(Watermark{EventTime{30}});  // fires [10,20) (k=1, sum=3)
        done_ = true;
        return false;
    }
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }
    std::string name() const override { return "late_data_window_src"; }

private:
    bool done_ = false;
};

std::vector<Record<Row>> run_tumbling_window_scripted(std::shared_ptr<StateBackend> backend) {
    const auto* builder =
        cluster::DagBuilderRegistry::default_instance().find("tumbling_window_row");
    Dag dag;
    auto h = dag.add_source<Row>(std::make_shared<LateDataWindowSource>());
    clink::plugin::BuildContext ctx;
    ctx.params["time_column"] = "ts";
    ctx.params["size_ms"] = "10";
    ctx.params["group_keys"] = "k";
    ctx.params["group_key_outputs"] = "k";
    ctx.params["aggregates"] = R"([{"name":"s","fn":"sum","input_column":"amt"}])";
    std::vector<std::any> upstream = {std::any{h}};
    auto out = (*builder)(dag, upstream, ctx);
    auto h_win = std::any_cast<StageHandle<Row>>(out.main_handle);
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(h_win, sink);
    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return sink->collected_records();
}

// Session analogue of the late-data scenario (gap 5): a session [1,3] fires and
// is purged at watermark 10, then a LATE record at ts=2 (which would open a
// fresh, already-purged session [2,2]) must be dropped, while a record at ts=20
// opens a new live session. A late record that instead MERGED into a live
// session would legitimately extend it; this one opens a fresh purged session.
class LateDataSessionSource final : public Source<Row> {
public:
    bool produce(Emitter<Row>& out) override {
        if (this->cancelled() || done_) {
            return false;
        }
        Batch<Row> b1;
        b1.push(window_input_row(1, 1, 5));
        b1.push(window_input_row(1, 3, 7));
        out.emit_data(std::move(b1));
        out.emit_watermark(Watermark{EventTime{10}});  // fires + purges session [1,3]
        Batch<Row> b2;
        b2.push(window_input_row(1, 2, 100));  // LATE: fresh session [2,2] already purged -> drop
        b2.push(window_input_row(1, 20, 3));   // new live session [20,20]
        out.emit_data(std::move(b2));
        out.emit_watermark(Watermark{EventTime{40}});  // fires session [20,20]
        done_ = true;
        return false;
    }
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }
    std::string name() const override { return "late_data_session_src"; }

private:
    bool done_ = false;
};

std::vector<Record<Row>> run_session_window_scripted(std::shared_ptr<StateBackend> backend) {
    const auto* builder =
        cluster::DagBuilderRegistry::default_instance().find("session_window_row");
    Dag dag;
    auto h = dag.add_source<Row>(std::make_shared<LateDataSessionSource>());
    clink::plugin::BuildContext ctx;
    ctx.params["time_column"] = "ts";
    ctx.params["gap_ms"] = "5";
    ctx.params["group_keys"] = "k";
    ctx.params["group_key_outputs"] = "k";
    ctx.params["aggregates"] = R"([{"name":"s","fn":"sum","input_column":"amt"}])";
    std::vector<std::any> upstream = {std::any{h}};
    auto out = (*builder)(dag, upstream, ctx);
    auto h_win = std::any_cast<StageHandle<Row>>(out.main_handle);
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(h_win, sink);
    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return sink->collected_records();
}

// A DLQ that just records every bad record it is handed, for tests that assert on
// the late side output. Thread-safe (one instance is shared across subtasks).
class CapturingDeadLetterQueue final : public DeadLetterQueue {
public:
    void report(const BadRecord& rec) override {
        std::lock_guard<std::mutex> lk(mu_);
        records_.push_back(rec);
    }
    std::vector<BadRecord> records() const {
        std::lock_guard<std::mutex> lk(mu_);
        return records_;
    }

private:
    mutable std::mutex mu_;
    std::vector<BadRecord> records_;
};

// allowed_lateness scenario for a tumbling window (size 10, grace band 5):
//   two on-time records into [0,10) (sum 12); a watermark at 10 that with a 5ms
//   grace band HOLDS [0,10) open (it fires at >= 15) rather than purging it; a
//   late-but-within-band record at ts=2 (amt 100) folded into the held window; a
//   watermark at 15 firing [0,10) ONCE (sum 112); a fully-late record at ts=4
//   (amt 9) past the band -> dropped (routed to the DLQ when opted in); a closing
//   watermark. With lateness 0 the window fires at 10 with sum 12 and both later
//   records drop, so sum 112 proves the grace band absorbed the within-band one.
class AllowedLatenessWindowSource final : public Source<Row> {
public:
    bool produce(Emitter<Row>& out) override {
        if (this->cancelled() || done_) {
            return false;
        }
        Batch<Row> b1;
        b1.push(window_input_row(1, 1, 5));
        b1.push(window_input_row(1, 3, 7));
        out.emit_data(std::move(b1));
        out.emit_watermark(Watermark{EventTime{10}});  // grace band holds [0,10) open
        Batch<Row> b2;
        b2.push(window_input_row(1, 2, 100));  // late but within band -> folded in
        out.emit_data(std::move(b2));
        out.emit_watermark(Watermark{EventTime{15}});  // fires [0,10) once, sum 112
        Batch<Row> b3;
        b3.push(window_input_row(1, 4, 9));  // fully late -> dropped (DLQ if opted in)
        out.emit_data(std::move(b3));
        out.emit_watermark(Watermark{EventTime{30}});
        done_ = true;
        return false;
    }
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }
    std::string name() const override { return "allowed_lateness_window_src"; }

private:
    bool done_ = false;
};

// allowed_lateness scenario for a session window (gap 5, grace band 5): a session
// [1,3] (sum 12) is held past its end+gap=8 by the 5ms band (fires at >= 13); a
// late record at ts=2 (amt 100) merges into the still-held session (sum 112); a
// watermark at 13 fires it once; then a fully-late record at ts=2 (amt 9) would
// open a fresh, already-purged session -> dropped (DLQ if opted in).
class AllowedLatenessSessionSource final : public Source<Row> {
public:
    bool produce(Emitter<Row>& out) override {
        if (this->cancelled() || done_) {
            return false;
        }
        Batch<Row> b1;
        b1.push(window_input_row(1, 1, 5));
        b1.push(window_input_row(1, 3, 7));
        out.emit_data(std::move(b1));
        out.emit_watermark(Watermark{EventTime{10}});  // grace band holds session [1,3] open
        Batch<Row> b2;
        b2.push(window_input_row(1, 2, 100));  // late, merges into held session
        out.emit_data(std::move(b2));
        out.emit_watermark(Watermark{EventTime{13}});  // fires session [1,3] once, sum 112
        Batch<Row> b3;
        b3.push(window_input_row(1, 2, 9));  // fully late: fresh purged session -> dropped
        out.emit_data(std::move(b3));
        out.emit_watermark(Watermark{EventTime{40}});
        done_ = true;
        return false;
    }
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }
    std::string name() const override { return "allowed_lateness_session_src"; }

private:
    bool done_ = false;
};

// Multi-key session scenario for the async-vs-sync proof (gap 5): key 1 has a
// burst [1,3] (sum 12) plus a separate later session [20,20] (sum 3); key 2 has
// [2,4] (sum 30). One closing watermark past every end+gap fires them all once.
class AsyncSessionScenarioSource final : public Source<Row> {
public:
    bool produce(Emitter<Row>& out) override {
        if (this->cancelled() || done_) {
            return false;
        }
        Batch<Row> b;
        b.push(window_input_row(1, 1, 5));
        b.push(window_input_row(1, 3, 7));
        b.push(window_input_row(2, 2, 10));
        b.push(window_input_row(2, 4, 20));
        b.push(window_input_row(1, 20, 3));
        out.emit_data(std::move(b));
        out.emit_watermark(Watermark{EventTime{100}});  // fires every session once
        done_ = true;
        return false;
    }
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }
    std::string name() const override { return "async_session_scenario_src"; }

private:
    bool done_ = false;
};

// Generic tumbling-window scripted run: feeds `source` through tumbling_window_row
// (size 10, sum over amt, keyed by k) with `extra` params merged in and an
// optional DLQ wired, returning the emitted rows.
std::vector<Record<Row>> run_tumbling_window_with(std::shared_ptr<Source<Row>> source,
                                                  std::shared_ptr<StateBackend> backend,
                                                  const std::map<std::string, std::string>& extra,
                                                  DeadLetterQueue* dlq = nullptr) {
    const auto* builder =
        cluster::DagBuilderRegistry::default_instance().find("tumbling_window_row");
    Dag dag;
    auto h = dag.add_source<Row>(std::move(source));
    clink::plugin::BuildContext ctx;
    ctx.params["time_column"] = "ts";
    ctx.params["size_ms"] = "10";
    ctx.params["group_keys"] = "k";
    ctx.params["group_key_outputs"] = "k";
    ctx.params["aggregates"] = R"([{"name":"s","fn":"sum","input_column":"amt"}])";
    for (const auto& [k, v] : extra) {
        ctx.params[k] = v;
    }
    std::vector<std::any> upstream = {std::any{h}};
    auto out = (*builder)(dag, upstream, ctx);
    auto h_win = std::any_cast<StageHandle<Row>>(out.main_handle);
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(h_win, sink);
    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    cfg.dead_letter_queue = dlq;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return sink->collected_records();
}

// Generic session-window scripted run (gap 5), mirror of run_tumbling_window_with.
std::vector<Record<Row>> run_session_window_with(std::shared_ptr<Source<Row>> source,
                                                 std::shared_ptr<StateBackend> backend,
                                                 const std::map<std::string, std::string>& extra,
                                                 DeadLetterQueue* dlq = nullptr) {
    const auto* builder =
        cluster::DagBuilderRegistry::default_instance().find("session_window_row");
    Dag dag;
    auto h = dag.add_source<Row>(std::move(source));
    clink::plugin::BuildContext ctx;
    ctx.params["time_column"] = "ts";
    ctx.params["gap_ms"] = "5";
    ctx.params["group_keys"] = "k";
    ctx.params["group_key_outputs"] = "k";
    ctx.params["aggregates"] = R"([{"name":"s","fn":"sum","input_column":"amt"}])";
    for (const auto& [k, v] : extra) {
        ctx.params[k] = v;
    }
    std::vector<std::any> upstream = {std::any{h}};
    auto out = (*builder)(dag, upstream, ctx);
    auto h_win = std::any_cast<StageHandle<Row>>(out.main_handle);
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(h_win, sink);
    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    cfg.dead_letter_queue = dlq;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return sink->collected_records();
}

}  // namespace

// Window aggregation rides the async/disaggregated path: with a deferring
// backend, WindowRowOp keeps each group's window map in KeyedState (the pool)
// and fires per-window event-time timers, so a fire point-reads only the due
// group rather than scanning every group at the watermark. Proven by comparing
// the window relation to the synchronous in-memory path (identical) and showing
// the async path actually went through the backend (remote_loads > 0).
TEST(SqlRuntime, AsyncStateTumblingWindowMatchesSyncPath) {
    ensure_sql_installed_once();
    const std::vector<Record<Row>> input = {window_input_row(1, 1, 5),
                                            window_input_row(1, 3, 7),
                                            window_input_row(1, 12, 2),
                                            window_input_row(2, 2, 100)};

    const auto sync_rel =
        window_relation(run_tumbling_window(input, std::make_shared<InMemoryStateBackend>()));
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    const auto async_rel = window_relation(run_tumbling_window(input, rrb));

    const std::map<std::pair<std::int64_t, std::int64_t>, std::int64_t> want = {
        {{1, 0}, 12}, {{1, 10}, 2}, {{2, 0}, 100}};
    EXPECT_EQ(sync_rel, want) << "sync in-memory window relation";
    EXPECT_EQ(async_rel, sync_rel)
        << "async window path must produce the same window relation as the sync path";
    EXPECT_GT(rrb->remote_loads(), 0u)
        << "window op did not route its per-group state through the deferring backend (async path)";
}

// Session windows ride the async/disaggregated path: with a deferring backend,
// SessionWindowRowOp keeps each group's session map in KeyedState and registers
// an event-time timer per fold at the session's end + gap + lateness, so firing
// point-reads only the due group (never scans all groups). Session merges make
// the timers dynamic (a session that extends leaves a stale timer; a merge
// leaves duplicates), so the fire re-checks each session's live end against the
// watermark - idempotent. Proven by comparing the session relation to the sync
// in-memory path (identical) and showing the async path went through the backend
// (remote_loads > 0).
TEST(SqlRuntime, AsyncStateSessionWindowMatchesSyncPath) {
    ensure_sql_installed_once();
    auto relation = [](const std::vector<Record<Row>>& rows) {
        std::map<std::pair<std::int64_t, std::int64_t>, std::int64_t> rel;
        for (const auto& rec : rows) {
            const Row& r = rec.value();
            rel[{static_cast<std::int64_t>(r.values.at("k").as_number()),
                 static_cast<std::int64_t>(r.values.at("window_start").as_number())}] =
                static_cast<std::int64_t>(r.values.at("s").as_number());
        }
        return rel;
    };

    const auto sync_rel =
        relation(run_session_window_with(std::make_shared<AsyncSessionScenarioSource>(),
                                         std::make_shared<InMemoryStateBackend>(),
                                         {}));
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    const auto async_rel =
        relation(run_session_window_with(std::make_shared<AsyncSessionScenarioSource>(), rrb, {}));

    const std::map<std::pair<std::int64_t, std::int64_t>, std::int64_t> want = {
        {{1, 1}, 12}, {{1, 20}, 3}, {{2, 2}, 30}};
    EXPECT_EQ(sync_rel, want) << "sync in-memory session relation";
    EXPECT_EQ(async_rel, sync_rel)
        << "async session path must produce the same session relation as the sync path";
    EXPECT_GT(rrb->remote_loads(), 0u) << "session op did not route its per-group state through "
                                          "the deferring backend (async path)";
}

// Late-data correctness: a record arriving after its tumbling window has fired
// and been purged must be DROPPED, not re-folded into a re-created window (which
// emitted a duplicate, partial row for the same window before the fix). Verified
// on both the sync in-memory path and the async/disaggregated path - the drop
// horizon (current_wm_) must be identical across them.
TEST(SqlRuntime, TumblingWindowDropsLateRecord) {
    ensure_sql_installed_once();
    auto check = [](const std::vector<Record<Row>>& rows, const char* path) {
        std::map<std::int64_t, std::vector<std::int64_t>> sums_by_start;
        for (const auto& rec : rows) {
            const Row& r = rec.value();
            sums_by_start[static_cast<std::int64_t>(r.values.at("window_start").as_number())]
                .push_back(static_cast<std::int64_t>(r.values.at("s").as_number()));
        }
        // Exactly one row for [0,10) (sum 12, late 100 dropped) and one for
        // [10,20) (sum 3). A second [0,10) row means the late record re-fired.
        ASSERT_EQ(sums_by_start[0].size(), 1u) << path << ": late record re-fired window [0,10)";
        EXPECT_EQ(sums_by_start[0][0], 12) << path;
        ASSERT_EQ(sums_by_start[10].size(), 1u) << path;
        EXPECT_EQ(sums_by_start[10][0], 3) << path;
    };
    check(run_tumbling_window_scripted(std::make_shared<InMemoryStateBackend>()), "sync");
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    check(run_tumbling_window_scripted(rrb), "async");
}

// Late-data correctness for SESSION windows: a record that would open a session
// already fired and purged is dropped, not re-created (which emitted a phantom
// single-event session row before the fix). A record merging into a live session
// still extends it; this late record opens a fresh, purged session.
TEST(SqlRuntime, SessionWindowDropsLateRecord) {
    ensure_sql_installed_once();
    const auto rows = run_session_window_scripted(std::make_shared<InMemoryStateBackend>());
    std::map<std::int64_t, std::vector<std::int64_t>> sums_by_start;
    for (const auto& rec : rows) {
        const Row& r = rec.value();
        sums_by_start[static_cast<std::int64_t>(r.values.at("window_start").as_number())].push_back(
            static_cast<std::int64_t>(r.values.at("s").as_number()));
    }
    // Session [1,3] fired once (sum 12, late 100 dropped); session [20,20] (sum
    // 3). No fresh session at start=2 from the late record.
    ASSERT_EQ(sums_by_start.count(2), 0u) << "late record opened a phantom session [2,2]";
    ASSERT_EQ(sums_by_start[1].size(), 1u) << "session [1,3] re-fired";
    EXPECT_EQ(sums_by_start[1][0], 12);
    ASSERT_EQ(sums_by_start[20].size(), 1u);
    EXPECT_EQ(sums_by_start[20][0], 3);
}

// allowed_lateness_ms holds a fired tumbling window open for a grace band so a
// late-but-within-band record still lands in it, then fires the window ONCE. With
// the band the window [0,10) emits sum 112 (the late 100 folded in); the fully-late
// record past the band still drops. Verified on the sync and async paths - the
// drop horizon (current_wm_ - allowed_lateness) must be identical across them.
TEST(SqlRuntime, TumblingWindowAllowedLatenessHoldsAndIncludesLateRecord) {
    ensure_sql_installed_once();
    auto check = [](const std::vector<Record<Row>>& rows, const char* path) {
        std::map<std::int64_t, std::vector<std::int64_t>> sums_by_start;
        for (const auto& rec : rows) {
            const Row& r = rec.value();
            sums_by_start[static_cast<std::int64_t>(r.values.at("window_start").as_number())]
                .push_back(static_cast<std::int64_t>(r.values.at("s").as_number()));
        }
        // Exactly one row for [0,10), summing 5+7+100 = 112 (the within-band late
        // record folded in). A second row would mean it re-fired; sum 12 would mean
        // the band did not hold the window open.
        ASSERT_EQ(sums_by_start[0].size(), 1u) << path << ": window [0,10) fired more than once";
        EXPECT_EQ(sums_by_start[0][0], 112) << path << ": within-band late record not included";
    };
    const std::map<std::string, std::string> params = {{"allowed_lateness_ms", "5"}};
    check(run_tumbling_window_with(std::make_shared<AllowedLatenessWindowSource>(),
                                   std::make_shared<InMemoryStateBackend>(),
                                   params),
          "sync");
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    check(run_tumbling_window_with(std::make_shared<AllowedLatenessWindowSource>(), rrb, params),
          "async");
}

// late_records_to_dlq routes a fully-late tumbling-window record (one past its
// window's grace band) to the dead-letter channel instead of dropping it silently.
// Exercised on the sync and async paths; each drops exactly the one late record.
TEST(SqlRuntime, TumblingWindowLateRecordToDeadLetterQueue) {
    ensure_sql_installed_once();
    const std::map<std::string, std::string> params = {{"late_records_to_dlq", "true"}};

    CapturingDeadLetterQueue sync_dlq;
    run_tumbling_window_with(std::make_shared<LateDataWindowSource>(),
                             std::make_shared<InMemoryStateBackend>(),
                             params,
                             &sync_dlq);
    const auto sync_recs = sync_dlq.records();
    ASSERT_EQ(sync_recs.size(), 1u) << "sync: exactly one late record reported";
    EXPECT_EQ(sync_recs[0].connector, "sql_window");
    EXPECT_EQ(sync_recs[0].direction, "operator");
    EXPECT_NE(sync_recs[0].payload.find("\"amt\""), std::string::npos)
        << "payload carries the dropped record's columns";

    CapturingDeadLetterQueue async_dlq;
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    run_tumbling_window_with(std::make_shared<LateDataWindowSource>(), rrb, params, &async_dlq);
    EXPECT_EQ(async_dlq.records().size(), 1u) << "async: exactly one late record reported";
}

// allowed_lateness_ms for session windows: the band holds session [1,3] open past
// end+gap so a late record at ts=2 merges in (sum 112) before the single fire.
TEST(SqlRuntime, SessionWindowAllowedLatenessHoldsAndIncludesLateRecord) {
    ensure_sql_installed_once();
    const std::map<std::string, std::string> params = {{"allowed_lateness_ms", "5"}};
    const auto rows = run_session_window_with(std::make_shared<AllowedLatenessSessionSource>(),
                                              std::make_shared<InMemoryStateBackend>(),
                                              params);
    std::map<std::int64_t, std::vector<std::int64_t>> sums_by_start;
    for (const auto& rec : rows) {
        const Row& r = rec.value();
        sums_by_start[static_cast<std::int64_t>(r.values.at("window_start").as_number())].push_back(
            static_cast<std::int64_t>(r.values.at("s").as_number()));
    }
    ASSERT_EQ(sums_by_start[1].size(), 1u) << "session [1,3] fired more than once";
    EXPECT_EQ(sums_by_start[1][0], 112) << "within-band late record not merged into the session";
}

// late_records_to_dlq for session windows: a record that would open an
// already-purged session is routed to the dead-letter channel.
TEST(SqlRuntime, SessionWindowLateRecordToDeadLetterQueue) {
    ensure_sql_installed_once();
    const std::map<std::string, std::string> params = {{"late_records_to_dlq", "true"}};
    CapturingDeadLetterQueue dlq;
    run_session_window_with(std::make_shared<LateDataSessionSource>(),
                            std::make_shared<InMemoryStateBackend>(),
                            params,
                            &dlq);
    const auto recs = dlq.records();
    ASSERT_EQ(recs.size(), 1u) << "exactly one late record reported";
    EXPECT_EQ(recs[0].connector, "sql_session_window");
    EXPECT_EQ(recs[0].direction, "operator");
}

// Proves the OVERLAP (not just correctness): when many groups' windows fall due
// at one watermark, WindowRowOp fires them as gate-routed async coroutines whose
// per-group reads COALESCE into batched get_many round-trips, instead of one
// blocking single-key read per group inside the watermark release. Observed at
// the backend: no per-key read reached it (get_async_calls()==0 - the coalescer
// absorbed them) and the whole run (8 groups ingested + 8 windows fired) issued
// only a small handful of batched reads, far below one per group.
TEST(SqlRuntime, AsyncStateWindowFireCoalescesReadsAcrossGroups) {
    ensure_sql_installed_once();
    constexpr int kGroups = 8;
    std::vector<Record<Row>> input;
    for (int k = 1; k <= kGroups; ++k) {
        input.push_back(window_input_row(k, /*ts=*/1, /*amt=*/k * 10));  // all in window [0,10)
    }

    auto pool = std::make_shared<InMemoryRemotePool>();
    auto rrb = std::make_shared<RemoteReadBackend>(pool, /*io_threads=*/1, /*hot_max_bytes=*/0);
    const auto rel = window_relation(run_tumbling_window(input, rrb));

    std::map<std::pair<std::int64_t, std::int64_t>, std::int64_t> want;
    for (int k = 1; k <= kGroups; ++k) {
        want[{k, 0}] = k * 10;
    }
    EXPECT_EQ(rel, want) << "all " << kGroups << " per-group windows must be correct";
    // Backend level: every per-key read was coalesced into a batched round-trip.
    EXPECT_EQ(rrb->get_async_calls(), 0u)
        << "a read bypassed the coalescer as a single-key round-trip";
    EXPECT_GT(rrb->get_many_async_calls(), 0u) << "no batched reads (async fire path inactive?)";
    EXPECT_LT(rrb->get_many_async_calls(), static_cast<std::uint64_t>(kGroups))
        << "reads were not coalesced across groups: ~one batched round-trip per group";
    // Pool level: the cold reads that reached the pool were batched too.
    EXPECT_EQ(pool->read_calls(), 0u) << "a cold read hit the pool as a single-key round-trip";
}

// Per-key fire-batching: a HOP window where one record lands in 3 overlapping
// windows (size 30, slide 10 -> [0,30) [10,40) [20,50)) gives group k=1 THREE
// due windows at the max watermark. They fire from ONE state read, not three:
// the whole run is exactly 2 batched backend reads (one ingest, one fire), so
// the group's three due windows did not serialise into three same-key reads.
TEST(SqlRuntime, AsyncStateHopWindowFiresMultiplePerGroupInOneRead) {
    ensure_sql_installed_once();
    const std::vector<Record<Row>> input = {window_input_row(1, /*ts=*/25, /*amt=*/7)};

    auto pool = std::make_shared<InMemoryRemotePool>();
    auto rrb = std::make_shared<RemoteReadBackend>(pool, /*io_threads=*/1, /*hot_max_bytes=*/0);
    const auto rel = window_relation(run_hopping_window(input, rrb));

    const std::map<std::pair<std::int64_t, std::int64_t>, std::int64_t> want = {
        {{1, 0}, 7}, {{1, 10}, 7}, {{1, 20}, 7}};
    EXPECT_EQ(rel, want) << "the record's 3 overlapping hop windows each sum 7";
    EXPECT_EQ(rrb->get_async_calls(), 0u) << "a read bypassed the coalescer as a single-key read";
    EXPECT_EQ(rrb->get_many_async_calls(), 2u)
        << "the group's 3 due windows did not batch into a single fire read (expected 1 ingest + "
           "1 fire)";
}

// Cumulate variant of the per-key fire-batching proof. Cumulate slices SHARE a
// window_start (so they're keyed here by window_end, not window_start): size 30,
// step 10 -> [0,10) [0,20) [0,30); one record at ts=5 lands in all three growing
// slices. Group k=1 has 3 due windows at the max watermark and fires them in one
// batched read (2 backend reads total: one ingest, one fire).
TEST(SqlRuntime, AsyncStateCumulateWindowFiresMultiplePerGroupInOneRead) {
    ensure_sql_installed_once();
    const std::vector<Record<Row>> input = {window_input_row(1, /*ts=*/5, /*amt=*/7)};

    auto pool = std::make_shared<InMemoryRemotePool>();
    auto rrb = std::make_shared<RemoteReadBackend>(pool, /*io_threads=*/1, /*hot_max_bytes=*/0);

    std::map<std::int64_t, std::int64_t> by_end;  // window_end -> sum (slices share start=0)
    for (const auto& rec : run_cumulate_window(input, rrb)) {
        const Row& r = rec.value();
        EXPECT_EQ(static_cast<std::int64_t>(r.values.at("window_start").as_number()), 0)
            << "cumulate slices share window_start=0";
        by_end[static_cast<std::int64_t>(r.values.at("window_end").as_number())] =
            static_cast<std::int64_t>(r.values.at("s").as_number());
    }
    const std::map<std::int64_t, std::int64_t> want = {{10, 7}, {20, 7}, {30, 7}};
    EXPECT_EQ(by_end, want) << "all 3 cumulate slices contain the record and sum 7";
    EXPECT_EQ(rrb->get_async_calls(), 0u) << "a read bypassed the coalescer as a single-key read";
    EXPECT_EQ(rrb->get_many_async_calls(), 2u)
        << "the group's 3 cumulate slices did not batch into a single fire read";
}

// OVER aggregate over the async/disaggregated path. With a deferring backend and
// no LAG ring / bounded frame, OverAggregateRowOp keeps each partition's running
// state in KeyedState and fires per-partition event-time timers, so a fire
// point-reads only the due partition. Proven by comparing the per-row relation to
// the synchronous in-memory path (identical) over a MULTI-watermark script that
// fires each partition more than once - so the running aggregate (and first_row)
// round-trips through the PartState codec ACROSS fires - plus a late record that
// both paths must drop. remote_loads > 0 proves the async path went through the
// deferring tier.
namespace {

struct OverScriptStep {
    std::vector<Record<Row>> records;   // emitted as one data batch (if non-empty)
    std::optional<std::int64_t> wm_ms;  // a watermark at this event time (if set)
};

// A source that emits a scripted sequence of data batches and watermarks in order
// (VectorSource only emits a single trailing max watermark, so it cannot fire a
// watermark-driven operator more than once). A trailing max watermark flushes
// anything still pending.
class ScriptedRowSource final : public clink::Source<Row> {
public:
    explicit ScriptedRowSource(std::vector<OverScriptStep> steps) : steps_(std::move(steps)) {}
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }
    bool produce(Emitter<Row>& out) override {
        if (done_ || this->cancelled()) {
            return false;
        }
        for (auto& s : steps_) {
            if (!s.records.empty()) {
                Batch<Row> b;
                for (auto& r : s.records) {
                    b.push(r);
                }
                out.emit_data(std::move(b));
            }
            if (s.wm_ms.has_value()) {
                out.emit_watermark(clink::Watermark{clink::EventTime::from_millis(*s.wm_ms)});
            }
        }
        out.emit_watermark(clink::Watermark::max());
        done_ = true;
        return false;
    }
    std::string name() const override { return "scripted_row_source"; }

private:
    std::vector<OverScriptStep> steps_;
    bool done_ = false;
};

Record<Row> over_row(std::int64_t id, std::int64_t p, std::int64_t ts, std::int64_t amt) {
    Row r;
    r.values["id"] = clink::config::JsonValue{static_cast<double>(id)};
    r.values["p"] = clink::config::JsonValue{static_cast<double>(p)};
    r.values["ts"] = clink::config::JsonValue{static_cast<double>(ts)};
    r.values["amt"] = clink::config::JsonValue{static_cast<double>(amt)};
    return Record<Row>{std::move(r)};
}

std::vector<Record<Row>> run_over_aggregate(std::vector<OverScriptStep> steps,
                                            std::shared_ptr<StateBackend> backend) {
    const auto* factory = cluster::OperatorRegistry::default_instance().find_operator(
        "over_aggregate_row", std::string{kChannelRow}, std::string{kChannelRow});
    cluster::OperatorBuildContext octx;
    octx.params["time_column"] = "ts";
    octx.params["partition_columns"] = "p";
    octx.params["outputs"] = R"([{"name":"sm","fn":"sum","input_column":"amt"},)"
                             R"({"name":"cnt","fn":"count","input_column":"amt"},)"
                             R"({"name":"av","fn":"avg","input_column":"amt"},)"
                             R"({"name":"mn","fn":"min","input_column":"amt"},)"
                             R"({"name":"mx","fn":"max","input_column":"amt"},)"
                             R"({"name":"fv","fn":"first_value","input_column":"amt"},)"
                             R"({"name":"lv","fn":"last_value","input_column":"amt"}])";
    auto op = std::static_pointer_cast<Operator<Row, Row>>(factory->build(octx));

    Dag dag;
    auto h_src = dag.add_source<Row>(std::make_shared<ScriptedRowSource>(std::move(steps)));
    auto h_op = dag.add_operator<Row, Row>(h_src, op);
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(h_op, sink);
    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return sink->collected_records();
}

// id -> the row's computed OVER values, serialised for an order-independent
// comparison (OVER emits one row per input; the async fire order differs from the
// sync scan, but the per-id computed values must be identical).
std::map<std::int64_t, std::string> over_relation(const std::vector<Record<Row>>& rows) {
    std::map<std::int64_t, std::string> rel;
    for (const auto& rec : rows) {
        const Row& r = rec.value();
        const auto id = static_cast<std::int64_t>(r.values.at("id").as_number());
        std::string vals;
        for (const char* col : {"sm", "cnt", "av", "mn", "mx", "fv", "lv"}) {
            auto it = r.values.find(col);
            vals += std::string(col) + "=" +
                    (it == r.values.end() ? std::string("<>") : it->second.serialize(0)) + ";";
        }
        rel[id] = vals;
    }
    return rel;
}

std::vector<OverScriptStep> over_parity_script() {
    std::vector<OverScriptStep> steps;
    // Wave 1: p1 gets id1(ts10), id3(ts15); p2 gets id2(ts12).
    steps.push_back(
        {{over_row(1, 1, 10, 5), over_row(2, 2, 12, 100), over_row(3, 1, 15, 7)}, std::nullopt});
    steps.push_back({{}, 12});  // fire: p1 id1 (ts10<=12), p2 id2 (ts12<=12); id3 stays
    // Wave 2: id4(ts20) is in time; id5(ts11) is LATE (<= the wm 12 already seen).
    steps.push_back({{over_row(4, 1, 20, 3), over_row(5, 1, 11, 99)}, std::nullopt});
    steps.push_back({{}, 25});  // fire: p1 id3 (ts15) then id4 (ts20) in order; id5 dropped
    return steps;
}

}  // namespace

TEST(SqlRuntime, OverAggregateAsyncMatchesSyncPath) {
    ensure_sql_installed_once();

    const auto sync_rel = over_relation(
        run_over_aggregate(over_parity_script(), std::make_shared<InMemoryStateBackend>()));
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    const auto async_rel = over_relation(run_over_aggregate(over_parity_script(), rrb));

    // Sanity on the sync oracle: id5 (late) is dropped, 4 rows emit, and the
    // running aggregate accumulated across the two fires of partition 1.
    EXPECT_EQ(sync_rel.size(), 4u) << "the late record id5 must be dropped";
    EXPECT_EQ(sync_rel.count(5), 0u) << "late record must not emit";
    ASSERT_EQ(sync_rel.count(1), 1u);
    ASSERT_EQ(sync_rel.count(4), 1u);
    EXPECT_NE(sync_rel.at(1).find("sm=5;"), std::string::npos) << "id1 running sum = 5";
    EXPECT_NE(sync_rel.at(4).find("sm=15;"), std::string::npos)
        << "id4 running sum = 5+7+3 = 15 (agg survived the fire at wm 12 via the codec)";
    EXPECT_NE(sync_rel.at(4).find("fv=5;"), std::string::npos)
        << "first_value persisted across fires";

    // The async/disaggregated path is byte-identical to the sync path, and it
    // genuinely routed partition state through the deferring backend.
    EXPECT_EQ(async_rel, sync_rel) << "async OVER must match the sync path exactly";
    EXPECT_GT(rrb->remote_loads(), 0u)
        << "OVER did not route its per-partition state through the deferring backend";
}

// Bounded-frame (ROWS / RANGE) and LAG OVER ride the async/disaggregated path
// too. The per-partition LAG ring (recent) and frame buffer (folded) already
// ride the PartState codec, and emit_one_ maintains them identically on both
// paths, so a deferring backend produces the same per-row OVER values as the
// sync in-memory path. Verified against the sync oracle AND with the exact
// expected values, plus remote_loads > 0 to prove the deferring tier was used.
TEST(SqlRuntime, OverBoundedFrameAndLagAsyncMatchesSyncPath) {
    ensure_sql_installed_once();
    // One partition (p=1), four rows in (ts, seq) order: id1(ts1,10),
    // id2(ts1,15), id3(ts2,20), id4(ts5,50); a closing watermark fires them all.
    auto steps = [] {
        std::vector<OverScriptStep> s;
        s.push_back({{over_row(1, 1, 1, 10),
                      over_row(2, 1, 1, 15),
                      over_row(3, 1, 2, 20),
                      over_row(4, 1, 5, 50)},
                     std::nullopt});
        s.push_back({{}, 100});  // fire every buffered row once
        return s;
    };
    auto run = [&](std::shared_ptr<StateBackend> backend) {
        const auto* factory = cluster::OperatorRegistry::default_instance().find_operator(
            "over_aggregate_row", std::string{kChannelRow}, std::string{kChannelRow});
        cluster::OperatorBuildContext octx;
        octx.params["time_column"] = "ts";
        octx.params["partition_columns"] = "p";
        octx.params["outputs"] =
            R"([{"name":"sr","fn":"sum","input_column":"amt","frame_mode":1,"frame_start":2},)"
            R"({"name":"sg","fn":"sum","input_column":"amt","frame_mode":2,"frame_start":2},)"
            R"({"name":"lg","fn":"lag","input_column":"amt","lag_offset":1}])";
        auto op = std::static_pointer_cast<Operator<Row, Row>>(factory->build(octx));
        Dag dag;
        auto h_src = dag.add_source<Row>(std::make_shared<ScriptedRowSource>(steps()));
        auto h_op = dag.add_operator<Row, Row>(h_src, op);
        auto sink = std::make_shared<CollectingSink<Row>>();
        dag.add_sink<Row>(h_op, sink);
        JobConfig cfg;
        cfg.state_backend = std::move(backend);
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();
        // id -> (ROWS sum, RANGE sum, LAG or -1 for NULL). Typed, so the
        // number formatting cannot skew the comparison.
        auto read = [](const Row& r, const char* c) -> std::int64_t {
            auto it = r.values.find(c);
            if (it == r.values.end() || it->second.is_null()) {
                return -1;
            }
            return static_cast<std::int64_t>(it->second.as_number());
        };
        std::map<std::int64_t, std::tuple<std::int64_t, std::int64_t, std::int64_t>> rel;
        for (const auto& rec : sink->collected_records()) {
            const Row& r = rec.value();
            rel[static_cast<std::int64_t>(r.values.at("id").as_number())] = {
                read(r, "sr"), read(r, "sg"), read(r, "lg")};
        }
        return rel;
    };

    const auto sync_rel = run(std::make_shared<InMemoryStateBackend>());
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    const auto async_rel = run(rrb);

    using T = std::tuple<std::int64_t, std::int64_t, std::int64_t>;
    const std::map<std::int64_t, T> want = {
        {1, T{10, 10, -1}},  // ROWS [10]; RANGE [10]; no prior row -> LAG null
        {2, T{25, 25, 10}},  // ROWS [10,15]; RANGE ts in [-1,1] {10,15}; LAG id1=10
        {3, T{45, 45, 15}},  // ROWS [10,15,20]; RANGE ts in [0,2] all; LAG id2=15
        {4, T{85, 50, 20}},  // ROWS last3 [15,20,50]=85; RANGE ts in [3,5] {50}; LAG id3=20
    };
    EXPECT_EQ(sync_rel, want) << "sync bounded-frame / LAG OVER values";
    EXPECT_EQ(async_rel, sync_rel)
        << "async bounded-frame / LAG OVER must match the sync path exactly";
    EXPECT_GT(rrb->remote_loads(), 0u)
        << "bounded-frame / LAG OVER did not route its per-partition state through the "
           "deferring backend";
}

// #59: the same unbounded GROUP BY built through the programmatic Table API
// instead of a SQL string. Proves the Table-API-built JobGraphSpec actually
// executes end-to-end (the byte-identical-IR test in test_table_api.cpp proves
// it equals the SQL spec; this proves that spec runs).
TEST(SqlRuntime, TableApiGroupBySumRunsEndToEnd) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_tapi_e2e_orders.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_tapi_e2e_totals.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":10})",
                    R"({"user_id":2,"amount":20})",
                    R"({"user_id":1,"amount":30})",
                    R"({"user_id":2,"amount":5})",
                    R"({"user_id":1,"amount":7})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    clink::api::TableEnvironment tenv{cat};
    using namespace clink::api;
    auto spec = tenv.from("orders")
                    .group_by({"user_id"})
                    .agg({key("user_id"), sum("amount") >> "total"})
                    .insert_into("out_t");

    InProcessCluster cluster("tm-tapi-e2e-groupby", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    ASSERT_FALSE(lines.empty());

    std::int64_t final_user1 = -1;
    std::int64_t final_user2 = -1;
    for (const auto& line : lines) {
        auto js = clink::config::parse(line);
        ASSERT_TRUE(js.is_object()) << "bad output line: " << line;
        auto uid = static_cast<std::int64_t>(js.at("user_id").as_number());
        auto total = static_cast<std::int64_t>(js.at("total").as_number());
        if (uid == 1)
            final_user1 = total;
        else if (uid == 2)
            final_user2 = total;
    }
    EXPECT_EQ(final_user1, 47);
    EXPECT_EQ(final_user2, 25);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// CREATE VIEW end-to-end: a query over a logical view runs by expanding the
// view inline. Proves both that the view's own filter is applied and that data
// flows through the expanded plan (the view has no storage of its own).
TEST(SqlRuntime, CreateViewExpandsAndFiltersEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_view_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_view_out.ndjson";
    for (const auto& p : {in_path, out_path}) {
        std::filesystem::remove(p);
    }
    write_lines(in_path,
                {
                    R"({"a":1,"b":5})",
                    R"({"a":2,"b":50})",
                    R"({"a":3,"b":7})",
                    R"({"a":4,"b":99})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE src (a BIGINT, b BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (a BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    // A logical view that keeps only b > 10. Querying it must apply that filter.
    register_view(
        cat,
        std::move(std::get<ast::CreateViewStmt>(
            parse("CREATE VIEW big AS SELECT a, b FROM src WHERE b > 10").statements[0])));

    auto spec = compile(cat, "INSERT INTO out_t SELECT a FROM big");

    InProcessCluster cluster("tm-sql-view-e2e", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    std::multiset<std::int64_t> got;
    for (const auto& line : read_lines(out_path)) {
        auto js = clink::config::parse(line);
        got.insert(static_cast<std::int64_t>(js.at("a").as_number()));
    }
    // Only the b>10 rows (a=2 b=50, a=4 b=99) survive the view's filter.
    EXPECT_EQ(got, (std::multiset<std::int64_t>{2, 4}));

    for (const auto& p : {in_path, out_path}) {
        std::filesystem::remove(p);
    }
}

// CREATE VIEW column-alias end-to-end: the alias list renames the view's output
// columns, and the aliased names are what a referencing query selects and filters
// on. Proves the renaming projection flows data under the aliased names.
TEST(SqlRuntime, CreateViewColumnAliasFlowsUnderAliasedNamesEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_valias_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_valias_out.ndjson";
    for (const auto& p : {in_path, out_path}) {
        std::filesystem::remove(p);
    }
    write_lines(in_path, {R"({"a":1,"b":5})", R"({"a":2,"b":50})", R"({"a":3,"b":99})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE src (a BIGINT, b BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (id BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    // The view renames a -> id, b -> score; the query uses those aliased names.
    register_view(cat,
                  std::move(std::get<ast::CreateViewStmt>(
                      parse("CREATE VIEW v (id, score) AS SELECT a, b FROM src").statements[0])));

    auto spec = compile(cat, "INSERT INTO out_t SELECT id FROM v WHERE score > 10");

    InProcessCluster cluster("tm-sql-valias-e2e", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    std::multiset<std::int64_t> got;
    for (const auto& line : read_lines(out_path)) {
        got.insert(static_cast<std::int64_t>(clink::config::parse(line).at("id").as_number()));
    }
    // score>10 keeps a=2 (b=50) and a=3 (b=99); their ids are 2 and 3.
    EXPECT_EQ(got, (std::multiset<std::int64_t>{2, 3}));

    for (const auto& p : {in_path, out_path}) {
        std::filesystem::remove(p);
    }
}

// ALTER TABLE ADD COLUMN end-to-end: a column not in the original declaration is
// unusable, and becomes queryable after the ALTER (the catalog change takes
// effect for subsequent compilations), with its values flowing through.
TEST(SqlRuntime, AlterTableAddColumnMakesItQueryableEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_alter_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_alter_out.ndjson";
    for (const auto& p : {in_path, out_path}) {
        std::filesystem::remove(p);
    }
    write_lines(in_path, {R"({"a":1,"b":10})", R"({"a":2,"b":20})"});

    Catalog cat;
    // Declare ONLY column `a` to begin with.
    auto ddl = parse(std::string{"CREATE TABLE t (a BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (a BIGINT, b BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    // `b` is not declared yet, so a query referencing it cannot bind.
    EXPECT_ANY_THROW((void)compile(cat, "INSERT INTO out_t SELECT a, b FROM t"));

    // Declare it via ALTER, then the same query binds, runs, and carries b.
    cat.alter_table(
        std::get<ast::AlterTableStmt>(parse("ALTER TABLE t ADD COLUMN b BIGINT").statements[0]));
    auto spec = compile(cat, "INSERT INTO out_t SELECT a, b FROM t");

    InProcessCluster cluster("tm-sql-alter-e2e", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    std::multiset<std::pair<std::int64_t, std::int64_t>> got;
    for (const auto& line : read_lines(out_path)) {
        auto js = clink::config::parse(line);
        got.emplace(static_cast<std::int64_t>(js.at("a").as_number()),
                    static_cast<std::int64_t>(js.at("b").as_number()));
    }
    EXPECT_EQ(got, (std::multiset<std::pair<std::int64_t, std::int64_t>>{{1, 10}, {2, 20}}));

    for (const auto& p : {in_path, out_path}) {
        std::filesystem::remove(p);
    }
}

// ALTER TABLE SET (...) end-to-end: SET rewrites a WITH-option, and the next
// compile binds the source from the new value. Here SET re-points the file
// source's `path`, so the query reads the second file's rows, not the first.
TEST(SqlRuntime, AlterTableSetOptionRedirectsSourceEndToEnd) {
    ensure_sql_installed_once();
    const auto in_a = std::filesystem::temp_directory_path() / "clink_sql_set_a.ndjson";
    const auto in_b = std::filesystem::temp_directory_path() / "clink_sql_set_b.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_set_out.ndjson";
    for (const auto& p : {in_a, in_b, out_path}) {
        std::filesystem::remove(p);
    }
    write_lines(in_a, {R"({"a":1})", R"({"a":2})"});
    write_lines(in_b, {R"({"a":100})", R"({"a":200})"});

    Catalog cat;
    // Table declared against the first file.
    auto ddl = parse(std::string{"CREATE TABLE t (a BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_a.string() +
                     "');"
                     "CREATE TABLE out_t (a BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    // Re-point the source to the second file; the next compile binds from it.
    cat.alter_table(std::get<ast::AlterTableStmt>(
        parse(std::string{"ALTER TABLE t SET (path='"} + in_b.string() + "')").statements[0]));
    auto spec = compile(cat, "INSERT INTO out_t SELECT a FROM t");

    InProcessCluster cluster("tm-sql-set-e2e", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    std::multiset<std::int64_t> got;
    for (const auto& line : read_lines(out_path)) {
        got.insert(static_cast<std::int64_t>(clink::config::parse(line).at("a").as_number()));
    }
    // Rows come from the second file, proving SET took effect at compile time.
    EXPECT_EQ(got, (std::multiset<std::int64_t>{100, 200}));

    for (const auto& p : {in_a, in_b, out_path}) {
        std::filesystem::remove(p);
    }
}

// ALTER TABLE RENAME COLUMN end-to-end: renaming a column to match the source's
// actual field name makes it queryable + decode under the new name (and the old
// name stops binding). Realistic use: fixing a declared column to match upstream.
TEST(SqlRuntime, AlterTableRenameColumnTakesEffectEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_rename_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_rename_out.ndjson";
    for (const auto& p : {in_path, out_path}) {
        std::filesystem::remove(p);
    }
    // The source's field is named `amount`.
    write_lines(in_path, {R"({"amount":10})", R"({"amount":20})"});

    Catalog cat;
    // Declared (wrongly) as `amt`; the source has `amount`, so it would decode null.
    auto ddl = parse(std::string{"CREATE TABLE t (amt BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (amount BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    // `amount` is not a declared column yet, so a query on it cannot bind.
    EXPECT_ANY_THROW((void)compile(cat, "INSERT INTO out_t SELECT amount FROM t"));

    // Rename the column to match the source field; now it binds, runs, decodes.
    cat.rename(std::get<ast::RenameStmt>(
        parse("ALTER TABLE t RENAME COLUMN amt TO amount").statements[0]));
    auto spec = compile(cat, "INSERT INTO out_t SELECT amount FROM t");

    InProcessCluster cluster("tm-sql-rename-e2e", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    std::multiset<std::int64_t> got;
    for (const auto& line : read_lines(out_path)) {
        got.insert(static_cast<std::int64_t>(clink::config::parse(line).at("amount").as_number()));
    }
    EXPECT_EQ(got, (std::multiset<std::int64_t>{10, 20}));

    for (const auto& p : {in_path, out_path}) {
        std::filesystem::remove(p);
    }
}

// #61 phase 2: end-to-end MATCH_RECOGNIZE. Compiles a real query to get the
// actual match_recognize_row params, builds the op via the registry factory,
// and drives it through a cluster-free Dag + LocalExecutor (VectorSource ->
// op -> CollectingSink) so the runtime matching is exercised without the
// InProcessCluster (whose TaskManager heartbeat is flaky under Docker).
TEST(SqlRuntime, MatchRecognizeMatchesEndToEnd) {
    ensure_sql_installed_once();

    Catalog cat;
    auto ddl = parse(
        std::string{"CREATE TABLE events (user_id BIGINT, ts BIGINT, price BIGINT) "
                    "WITH (connector='file', format='json', path='/tmp/mr_ev.ndjson');"
                    "CREATE TABLE out_mr (user_id BIGINT, start_price BIGINT, final_price BIGINT) "
                    "WITH (connector='file', format='json', path='/tmp/mr_out.ndjson')"});
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO out_mr SELECT * FROM events MATCH_RECOGNIZE ("
                        "  PARTITION BY user_id ORDER BY ts"
                        "  MEASURES FIRST(a.price) AS start_price, LAST(b.price) AS final_price"
                        "  PATTERN (a b)"
                        "  DEFINE a AS price > 100, b AS price > 200)");

    std::map<std::string, std::string> params;
    for (const auto& op : spec.ops) {
        if (op.type == "match_recognize_row") {
            params = op.params;
        }
    }
    ASSERT_FALSE(params.empty()) << "no match_recognize_row op in the compiled spec";

    const auto* factory = cluster::OperatorRegistry::default_instance().find_operator(
        "match_recognize_row", std::string{kChannelRow}, std::string{kChannelRow});
    ASSERT_NE(factory, nullptr);
    cluster::OperatorBuildContext octx;
    octx.params = params;
    auto op = std::static_pointer_cast<Operator<Row, Row>>(factory->build(octx));
    ASSERT_NE(op, nullptr);

    auto mk = [](std::int64_t uid, std::int64_t ts, std::int64_t price) {
        Row r;
        r.values["user_id"] = clink::config::JsonValue{static_cast<double>(uid)};
        r.values["ts"] = clink::config::JsonValue{static_cast<double>(ts)};
        r.values["price"] = clink::config::JsonValue{static_cast<double>(price)};
        return Record<Row>{std::move(r)};
    };

    Dag dag;
    auto src =
        std::make_shared<VectorSource<Row>>(std::vector<Record<Row>>{mk(1, 1, 150), mk(1, 2, 250)});
    auto h_src = dag.add_source<Row>(src);
    auto h_op = dag.add_operator<Row, Row>(h_src, op);
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(h_op, sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    auto out = sink->collected();
    ASSERT_EQ(out.size(), 1u) << "pattern (a b) over price 150,250 should yield one match";
    const auto& r = out.front();
    EXPECT_EQ(static_cast<std::int64_t>(r.values.at("user_id").as_number()), 1);
    EXPECT_EQ(static_cast<std::int64_t>(r.values.at("start_price").as_number()), 150);
    EXPECT_EQ(static_cast<std::int64_t>(r.values.at("final_price").as_number()), 250);
}

// --- retracting aggregate ------------------------------

TEST(SqlRuntime, AggregateRetractsOnDeleteTaggedInput) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_retract_in.ndjson";
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_retract_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // user=1: +10, +20, -20 (retracted) -> total 10
    // user=2: +50 -> total 50
    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":10})",
                    R"({"user_id":1,"amount":20})",
                    R"({"user_id":1,"amount":20,"__row_kind":"delete"})",
                    R"({"user_id":2,"amount":50})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE per_user (user_id BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='user_id')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO per_user SELECT user_id, SUM(amount) AS total FROM orders "
                        "GROUP BY user_id");

    InProcessCluster cluster("tm-sql-e2e-retract", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    std::map<std::int64_t, std::int64_t> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("user_id").as_number())] =
            static_cast<std::int64_t>(js.at("total").as_number());
    }
    EXPECT_EQ(got[1], 10);
    EXPECT_EQ(got[2], 50);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, MinMaxRecomputeWhenTheExtremeIsRetracted) {
    ensure_sql_installed_once();

    const auto in_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_minmax_retract_in.ndjson";
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_minmax_retract_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // user=1: +10,+30,+20, then delete 10 (the MIN) -> {30,20} -> lo=20, hi=30
    // user=2: +50,+40, then delete 50 (the MAX)     -> {40}    -> lo=40, hi=40
    // user=3: +7,+7, then delete one 7 (duplicate)  -> {7}     -> lo=7,  hi=7
    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":10})",
                    R"({"user_id":1,"amount":30})",
                    R"({"user_id":1,"amount":20})",
                    R"({"user_id":1,"amount":10,"__row_kind":"delete"})",
                    R"({"user_id":2,"amount":50})",
                    R"({"user_id":2,"amount":40})",
                    R"({"user_id":2,"amount":50,"__row_kind":"delete"})",
                    R"({"user_id":3,"amount":7})",
                    R"({"user_id":3,"amount":7})",
                    R"({"user_id":3,"amount":7,"__row_kind":"delete"})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE per_user (user_id BIGINT, lo BIGINT, hi BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='user_id')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO per_user SELECT user_id, MIN(amount) AS lo, MAX(amount) AS hi "
                        "FROM orders GROUP BY user_id");

    InProcessCluster cluster("tm-sql-e2e-minmax-retract", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    std::map<std::int64_t, std::pair<std::int64_t, std::int64_t>> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("user_id").as_number())] = {
            static_cast<std::int64_t>(js.at("lo").as_number()),
            static_cast<std::int64_t>(js.at("hi").as_number())};
    }
    EXPECT_EQ(got[1], (std::pair<std::int64_t, std::int64_t>{20, 30}));
    EXPECT_EQ(got[2], (std::pair<std::int64_t, std::int64_t>{40, 40}));
    EXPECT_EQ(got[3], (std::pair<std::int64_t, std::int64_t>{7, 7}));

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, CastToBooleanRunsEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_cast_bool_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_cast_bool_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"id":1,"amount":0})", R"({"id":2,"amount":5})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE flags (id BIGINT, flag BOOLEAN) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec =
        compile(cat, "INSERT INTO flags SELECT id, CAST(amount AS BOOLEAN) AS flag FROM orders");

    InProcessCluster cluster("tm-sql-cast-bool", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    std::map<std::int64_t, bool> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("id").as_number())] = js.at("flag").as_bool();
    }
    EXPECT_EQ(got[1], false);  // CAST(0 AS BOOLEAN)
    EXPECT_EQ(got[2], true);   // CAST(5 AS BOOLEAN)
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, NestedCteBodyRunsEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_nested_cte_in.ndjson";
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_sql_nested_cte_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"id":1,"amount":10})", R"({"id":2,"amount":3})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE kept (id BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    // A CTE (a) whose body itself carries a WITH (b). The inner scope's
    // names must not leak into / wipe the outer scope.
    auto spec = compile(cat,
                        "INSERT INTO kept "
                        "WITH a AS (WITH b AS (SELECT id, amount FROM orders WHERE amount > 5) "
                        "           SELECT id FROM b) "
                        "SELECT id FROM a");

    InProcessCluster cluster("tm-sql-nested-cte", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    std::vector<std::int64_t> ids;
    for (const auto& l : read_lines(out_path)) {
        ids.push_back(static_cast<std::int64_t>(clink::config::parse(l).at("id").as_number()));
    }
    EXPECT_EQ(ids, (std::vector<std::int64_t>{1}));  // only amount>5 survives
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, FloatLiteralInArithmeticRunsEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_flit_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_flit_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"id":1,"amount":10})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE scaled (id BIGINT, s DOUBLE) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    // 1.5 is a fractional literal - previously rejected at parse time.
    auto spec = compile(cat, "INSERT INTO scaled SELECT id, amount * 1.5 AS s FROM orders");

    InProcessCluster cluster("tm-sql-flit", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1u);
    EXPECT_DOUBLE_EQ(clink::config::parse(lines[0]).at("s").as_number(), 15.0);
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, PercentileMedianRunsEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_pct_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_pct_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path,
                {R"({"g":1,"amount":10})",
                 R"({"g":1,"amount":20})",
                 R"({"g":1,"amount":30})",
                 R"({"g":1,"amount":40})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (g BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE pcts (g BIGINT, p25 DOUBLE, p50 DOUBLE, p100 DOUBLE) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='g')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat,
                        "INSERT INTO pcts SELECT g, PERCENTILE(amount, 0.25) AS p25, "
                        "PERCENTILE(amount, 0.5) AS p50, PERCENTILE(amount, 1.0) AS p100 "
                        "FROM orders GROUP BY g");

    InProcessCluster cluster("tm-sql-pct", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    // Final upsert snapshot for g=1 over [10,20,30,40]:
    //   p25 = 17.5 (linear interp), p50 = 25 (median), p100 = 40.
    auto lines = read_lines(out_path);
    ASSERT_FALSE(lines.empty());
    auto js = clink::config::parse(lines.back());
    EXPECT_DOUBLE_EQ(js.at("p25").as_number(), 17.5);
    EXPECT_DOUBLE_EQ(js.at("p50").as_number(), 25.0);
    EXPECT_DOUBLE_EQ(js.at("p100").as_number(), 40.0);
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// --- Wave 5: complex types (ARRAY) --------------------------------

TEST(SqlRuntime, ArrayLiteralAndSubscriptRunEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_arr1_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_arr1_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"id":1,"amount":50})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE picks (id BIGINT, second BIGINT, third BIGINT, oob BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    // second  = ARRAY[10,20,30][2]      -> 20  (1-based subscript)
    // third   = ARRAY[amount,99,7][3]   -> 7   (array over a column ref)
    // oob     = ARRAY[1,2][9]           -> NULL (out of range, no error)
    auto spec = compile(cat,
                        "INSERT INTO picks SELECT id, (ARRAY[10,20,30])[2] AS second, "
                        "(ARRAY[amount,99,7])[3] AS third, (ARRAY[1,2])[9] AS oob FROM orders");

    InProcessCluster cluster("tm-sql-arr1", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1u);
    auto js = clink::config::parse(lines[0]);
    EXPECT_DOUBLE_EQ(js.at("second").as_number(), 20.0);
    EXPECT_DOUBLE_EQ(js.at("third").as_number(), 7.0);
    EXPECT_TRUE(js.at("oob").is_null());
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, ArrayLiteralRoundTripsToArrayColumn) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_arr2_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_arr2_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"id":1,"amount":50})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE bagged (id BIGINT, vals BIGINT ARRAY) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    // ARRAY[id, amount, 99] -> list<int64>, matching the BIGINT ARRAY sink.
    auto spec =
        compile(cat, "INSERT INTO bagged SELECT id, ARRAY[id, amount, 99] AS vals FROM orders");

    InProcessCluster cluster("tm-sql-arr2", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1u);
    auto js = clink::config::parse(lines[0]);
    ASSERT_TRUE(js.at("vals").is_array());
    const auto& vals = js.at("vals").as_array();
    ASSERT_EQ(vals.size(), 3u);
    EXPECT_DOUBLE_EQ(vals[0].as_number(), 1.0);
    EXPECT_DOUBLE_EQ(vals[1].as_number(), 50.0);
    EXPECT_DOUBLE_EQ(vals[2].as_number(), 99.0);
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, ArrayColumnIndexRunsEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_arr3_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_arr3_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    // tags arrives as a JSON array on the row wire and rides through as-is.
    write_lines(in_path, {R"({"id":1,"tags":[5,6,7]})", R"({"id":2,"tags":[8]})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE t (id BIGINT, tags INT ARRAY) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE firsts (id BIGINT, first_tag INTEGER, missing INTEGER) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    // tags[1] -> first element (1-based); tags[5] -> NULL for the short array.
    auto spec = compile(
        cat, "INSERT INTO firsts SELECT id, tags[1] AS first_tag, tags[5] AS missing FROM t");

    InProcessCluster cluster("tm-sql-arr3", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    std::map<double, clink::config::JsonValue> by_id;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        by_id[js.at("id").as_number()] = js;
    }
    ASSERT_EQ(by_id.size(), 2u);
    EXPECT_DOUBLE_EQ(by_id[1].at("first_tag").as_number(), 5.0);
    EXPECT_TRUE(by_id[1].at("missing").is_null());
    EXPECT_DOUBLE_EQ(by_id[2].at("first_tag").as_number(), 8.0);
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, ArrayElementAccessEdgeCasesYieldNull) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_arr4_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_arr4_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"id":1,"amount":50,"txt":"hello","maybe":null})"});

    Catalog cat;
    auto ddl =
        parse(std::string{"CREATE TABLE t (id BIGINT, amount BIGINT, txt TEXT, maybe BIGINT) "
                          "WITH (connector='file', format='json', path='"} +
              in_path.string() +
              "');"
              "CREATE TABLE edges (id BIGINT, zero_idx BIGINT, frac_idx BIGINT, "
              "nan_idx BIGINT, nonarray_idx TEXT, null_idx BIGINT, neg_idx BIGINT) "
              "WITH (connector='file', format='json', path='" +
              out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    // zero_idx     [0]    -> NULL (1-based; 0 < 1)
    // frac_idx     [2.7]  -> 20   (finite fractional truncates toward zero)
    // nan_idx      [1/0]  -> NULL (NaN index: guarded before the int cast, no UB)
    // nonarray_idx (txt)[1] -> NULL (non-array base)
    // null_idx     [maybe] -> NULL (NULL index, maybe is null)
    // neg_idx      [0-1]  -> NULL (negative computed index)
    auto spec = compile(cat,
                        "INSERT INTO edges SELECT id, "
                        "(ARRAY[10,20,30])[0] AS zero_idx, "
                        "(ARRAY[10,20,30])[2.7] AS frac_idx, "
                        "(ARRAY[10,20,30])[1/0] AS nan_idx, "
                        "(txt)[1] AS nonarray_idx, "
                        "(ARRAY[10,20,30])[maybe] AS null_idx, "
                        "(ARRAY[10,20,30])[0-1] AS neg_idx FROM t");

    InProcessCluster cluster("tm-sql-arr4", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1u);
    auto js = clink::config::parse(lines[0]);
    EXPECT_TRUE(js.at("zero_idx").is_null());
    EXPECT_DOUBLE_EQ(js.at("frac_idx").as_number(), 20.0);
    EXPECT_TRUE(js.at("nan_idx").is_null());
    EXPECT_TRUE(js.at("nonarray_idx").is_null());
    EXPECT_TRUE(js.at("null_idx").is_null());
    EXPECT_TRUE(js.at("neg_idx").is_null());
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// --- Wave 6a: ARRAY_AGG -------------------------------------------

namespace {
// Collect the final (last-write-wins) array for each key from an upsert
// changelog output file, as a vector of doubles.
std::map<std::int64_t, std::vector<double>> final_arrays(const std::vector<std::string>& lines,
                                                         const char* key_col,
                                                         const char* arr_col) {
    std::map<std::int64_t, std::vector<double>> by_key;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        std::vector<double> vals;
        if (js.at(arr_col).is_array()) {
            for (const auto& e : js.at(arr_col).as_array()) {
                vals.push_back(e.as_number());
            }
        }
        by_key[static_cast<std::int64_t>(js.at(key_col).as_number())] = std::move(vals);
    }
    return by_key;
}
}  // namespace

TEST(SqlRuntime, RowConstructAndFieldAccessRunEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_row1_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_row1_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"id":1,"amount":50})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE picked (id BIGINT, amt BIGINT, miss VARCHAR) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    // (ROW(amount, id)).amount -> 50 (named field, derived from the column);
    // (ROW(amount)).nope -> NULL (missing field of a known struct infers the
    // utf8 fallback type, hence the VARCHAR sink column, and yields NULL).
    auto spec = compile(cat,
                        "INSERT INTO picked SELECT id, (ROW(amount, id)).amount AS amt, "
                        "(ROW(amount)).nope AS miss FROM orders");

    InProcessCluster cluster("tm-sql-row1", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1u);
    auto js = clink::config::parse(lines[0]);
    EXPECT_DOUBLE_EQ(js.at("amt").as_number(), 50.0);
    EXPECT_TRUE(js.at("miss").is_null());
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, FieldAccessOnNestedSourceColumnRunsEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_row2_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_row2_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    // profile arrives as a nested JSON object on the row wire. The dominant
    // streaming use of ROW/struct is deconstructing such source columns.
    write_lines(
        in_path,
        {R"({"id":1,"profile":{"city":"London","age":30}})", R"({"id":2,"profile":{"age":41}})"});

    Catalog cat;
    // profile has no declarable struct DDL type; it rides as a VARCHAR-typed
    // column carrying a JSON object, and (profile).field deconstructs it at
    // runtime (field type falls back to utf8, matching the VARCHAR sink).
    auto ddl = parse(std::string{"CREATE TABLE events (id BIGINT, profile VARCHAR) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE cities (id BIGINT, city VARCHAR) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat, "INSERT INTO cities SELECT id, (profile).city AS city FROM events");

    InProcessCluster cluster("tm-sql-row2", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    std::map<double, clink::config::JsonValue> by_id;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        by_id[js.at("id").as_number()] = js;
    }
    ASSERT_EQ(by_id.size(), 2u);
    EXPECT_EQ(by_id[1].at("city").as_string(), "London");
    EXPECT_TRUE(by_id[2].at("city").is_null());  // absent field -> NULL
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, RowValueToSinkColumnRejected) {
    ensure_sql_installed_once();
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE orders (id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/clink_row_rej_in.ndjson');"
        "CREATE TABLE wrong (id BIGINT, r BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/clink_row_rej_out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    // A whole ROW value cannot be written to a sink column (no ROW column
    // type is declarable); the binder rejects it with an actionable error.
    EXPECT_THROW(compile(cat, "INSERT INTO wrong SELECT id, ROW(amount, id) AS r FROM orders"),
                 TranslationError);
}

TEST(SqlRuntime, MapConstructAndAccessRunEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_map1_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_map1_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"id":1,"code":"US"})", R"({"id":2,"code":"XX"})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE codes (id BIGINT, code VARCHAR) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE named (id BIGINT, country VARCHAR) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    // Inline lookup map keyed by a dynamic column: present -> value, absent
    // key -> NULL. The MAP(...) base is statically typed, so [code] is
    // dispatched to map_get.
    auto spec = compile(cat,
                        "INSERT INTO named SELECT id, "
                        "(MAP('US', 'United States', 'GB', 'United Kingdom'))[code] AS country "
                        "FROM codes");

    InProcessCluster cluster("tm-sql-map1", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    std::map<double, clink::config::JsonValue> by_id;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        by_id[js.at("id").as_number()] = js;
    }
    ASSERT_EQ(by_id.size(), 2u);
    EXPECT_EQ(by_id[1].at("country").as_string(), "United States");
    EXPECT_TRUE(by_id[2].at("country").is_null());  // absent key -> NULL
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, MapEdgeCasesRunEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_map2_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_map2_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"id":1,"nullc":null})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE t (id BIGINT, nullc VARCHAR) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE r (id BIGINT, dup VARCHAR, nk VARCHAR) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    // dup: a duplicate key is last-wins -> 'b'.
    // nk:  a NULL key makes the whole map NULL, so any lookup is NULL.
    auto spec = compile(cat,
                        "INSERT INTO r SELECT id, (MAP('k', 'a', 'k', 'b'))['k'] AS dup, "
                        "(MAP(nullc, 'v'))['k'] AS nk FROM t");

    InProcessCluster cluster("tm-sql-map2", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1u);
    auto js = clink::config::parse(lines[0]);
    EXPECT_EQ(js.at("dup").as_string(), "b");
    EXPECT_TRUE(js.at("nk").is_null());
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// A whole MAP value lands in a typed MAP<...> sink column and survives to
// the sink as a nested JSON object (not flattened or stringified). This is
// the runtime counterpart to the bind-only SqlPreparse.MatchingCompositeSinkBinds:
// #61 added MAP<k,v> / ROW<...> DDL column types so a composite value can
// reach a typed sink, and this proves it executes end-to-end.
TEST(SqlRuntime, MapValueToTypedSinkRunsEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_mapsink_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_mapsink_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"id":1,"k":"US","v":1})", R"({"id":2,"k":"GB","v":44})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE src (id BIGINT, k TEXT, v BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out (id BIGINT, m MAP<TEXT,BIGINT>) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat, "INSERT INTO out SELECT id, MAP(k, v) AS m FROM src");

    InProcessCluster cluster("tm-sql-mapsink", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    std::map<double, clink::config::JsonValue> by_id;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        by_id[js.at("id").as_number()] = js;
    }
    ASSERT_EQ(by_id.size(), 2u);
    // The whole map survives as a nested JSON object, not a scalar or string.
    ASSERT_TRUE(by_id[1].at("m").is_object());
    EXPECT_DOUBLE_EQ(by_id[1].at("m").at("US").as_number(), 1.0);
    ASSERT_TRUE(by_id[2].at("m").is_object());
    EXPECT_DOUBLE_EQ(by_id[2].at("m").at("GB").as_number(), 44.0);
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// A whole ROW value lands in a typed ROW<...> sink column and survives as a
// nested JSON object keyed by field name. The untyped counterpart (a ROW into
// a scalar sink column with no declarable ROW type) stays rejected - see
// RowValueToSinkColumnRejected.
TEST(SqlRuntime, RowValueToTypedSinkRunsEndToEnd) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_rowsink_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_rowsink_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"id":1,"amount":50})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE src (id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out (rid BIGINT, r ROW<id BIGINT, amount BIGINT>) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat, "INSERT INTO out SELECT id AS rid, ROW(id, amount) AS r FROM src");

    InProcessCluster cluster("tm-sql-rowsink", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1u);
    auto js = clink::config::parse(lines[0]);
    ASSERT_TRUE(js.at("r").is_object());
    EXPECT_DOUBLE_EQ(js.at("r").at("id").as_number(), 1.0);
    EXPECT_DOUBLE_EQ(js.at("r").at("amount").as_number(), 50.0);
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, ArrayAggCollectsPerGroup) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_aagg1_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_aagg1_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path,
                {R"({"g":1,"amount":10})", R"({"g":1,"amount":20})", R"({"g":2,"amount":30})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (g BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE grouped (g BIGINT, xs BIGINT ARRAY) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='g')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(
        cat, "INSERT INTO grouped SELECT g, array_agg(amount) AS xs FROM orders GROUP BY g");

    InProcessCluster cluster("tm-sql-aagg1", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto got = final_arrays(read_lines(out_path), "g", "xs");
    EXPECT_EQ(got[1], (std::vector<double>{10.0, 20.0}));
    EXPECT_EQ(got[2], (std::vector<double>{30.0}));
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, ArrayAggRetractsDeletedValue) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_aagg2_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_aagg2_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    // g=1: +10, +20, -20 (delete) -> [10]; g=2: +50 -> [50]
    write_lines(in_path,
                {R"({"g":1,"amount":10})",
                 R"({"g":1,"amount":20})",
                 R"({"g":1,"amount":20,"__row_kind":"delete"})",
                 R"({"g":2,"amount":50})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (g BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE grouped (g BIGINT, xs BIGINT ARRAY) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='g')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(
        cat, "INSERT INTO grouped SELECT g, array_agg(amount) AS xs FROM orders GROUP BY g");

    InProcessCluster cluster("tm-sql-aagg2", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto got = final_arrays(read_lines(out_path), "g", "xs");
    EXPECT_EQ(got[1], (std::vector<double>{10.0}));  // the deleted 20 is gone
    EXPECT_EQ(got[2], (std::vector<double>{50.0}));
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, ArrayAggDistinctDedups) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_aagg3_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_aagg3_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path,
                {R"({"g":1,"amount":10})", R"({"g":1,"amount":20})", R"({"g":1,"amount":10})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (g BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE grouped (g BIGINT, xs BIGINT ARRAY) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='g')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(
        cat,
        "INSERT INTO grouped SELECT g, array_agg(DISTINCT amount) AS xs FROM orders GROUP BY g");

    InProcessCluster cluster("tm-sql-aagg3", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto got = final_arrays(read_lines(out_path), "g", "xs");
    // DISTINCT keeps first occurrence order: 10 then 20 (the second 10 drops).
    EXPECT_EQ(got[1], (std::vector<double>{10.0, 20.0}));
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// ARRAY_AGG over an all-NULL column yields SQL NULL (NULLs ignored, then the
// empty collection finalizes to NULL - matching PostgreSQL).
TEST(SqlRuntime, ArrayAggAllNullReturnsNull) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_aanull_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_aanull_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"g":1,"amount":null})", R"({"g":1,"amount":null})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (g BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE grouped (g BIGINT, xs BIGINT ARRAY) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='g')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(
        cat, "INSERT INTO grouped SELECT g, array_agg(amount) AS xs FROM orders GROUP BY g");

    InProcessCluster cluster("tm-sql-aanull", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    ASSERT_FALSE(lines.empty());
    auto js = clink::config::parse(lines.back());
    EXPECT_TRUE(js.at("xs").is_null());
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Regression for the review bug: a SESSION window merge must keep every
// session's ARRAY_AGG values (merge_into only merged sum/avg/count before).
// Arrival order 100, 1000, 550 forces two sessions to merge into one.
TEST(SqlRuntime, ArrayAggSessionMergeKeepsAllValues) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_aasess_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_aasess_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    // ts=100 -> session [100,600]; ts=1000 -> separate [1000,1500];
    // ts=550 joins the first ([100,1050]) and now overlaps the second -> merge.
    write_lines(in_path,
                {R"({"u":1,"ts":100,"amount":10})",
                 R"({"u":1,"ts":1000,"amount":100})",
                 R"({"u":1,"ts":550,"amount":55})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE evt (u BIGINT, ts BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE out_t (u BIGINT, amounts BIGINT ARRAY) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT u, array_agg(amount) AS amounts FROM evt "
                        "GROUP BY SESSION(ts, 500), u");

    InProcessCluster cluster("tm-sql-aasess", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1u) << "the three rows form one merged session";
    auto js = clink::config::parse(lines[0]);
    ASSERT_TRUE(js.at("amounts").is_array());
    std::vector<double> got;
    for (const auto& e : js.at("amounts").as_array())
        got.push_back(e.as_number());
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<double>{10.0, 55.0, 100.0}));  // none dropped by the merge
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// --- file 2PC sink e2e ---------------------------------

TEST(SqlRuntime, FileExactlyOnceSinkProducesCommittedRecords) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_eo_in.ndjson";
    const auto out_dir = std::filesystem::temp_directory_path() / "clink_sql_e2e_eo_dir";
    std::filesystem::remove(in_path);
    std::filesystem::remove_all(out_dir);

    write_lines(in_path,
                {
                    R"({"k":1,"v":10})",
                    R"({"k":2,"v":20})",
                    R"({"k":3,"v":30})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE src_t (k BIGINT, v BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE eo (k BIGINT, v BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_dir.string() + "', delivery_guarantee='exactly_once')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat, "INSERT INTO eo SELECT k, v FROM src_t");

    InProcessCluster cluster("tm-sql-e2e-eo", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    // The 2PC sink stages records under <out_dir>/staging/. Without
    // an externally-driven checkpoint completing during this short
    // job, the records sit in staging/ as pending or as a
    // checkpoint-tagged .dat. Either way, they should be findable
    // somewhere under out_dir (not in a plain <out_dir> output file,
    // which would mean the sink bypassed the staging step).
    ASSERT_TRUE(std::filesystem::exists(out_dir));
    std::size_t staged_lines = 0;
    for (auto& entry : std::filesystem::recursive_directory_iterator(out_dir)) {
        if (!entry.is_regular_file())
            continue;
        std::ifstream in(entry.path());
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty())
                ++staged_lines;
        }
    }
    EXPECT_EQ(staged_lines, 3u) << "expected 3 staged rows total, got " << staged_lines;

    std::filesystem::remove(in_path);
    std::filesystem::remove_all(out_dir);
}

// --- TOP-N-per-key e2e ---------------------------------

TEST(SqlRuntime, TopNPerKeyEmitsChangelog) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_tnpk_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_tnpk_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // Two users, ranked by amount DESC, take top 2 per user.
    // user=1: 10, 50, 30, 20  -> top 2 by amount: {50, 30}
    // user=2:  5, 40           -> top 2 by amount: {40, 5}
    // Sink receives insert/delete events as the top-2 shifts.
    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":10})",
                    R"({"user_id":2,"amount":5})",
                    R"({"user_id":1,"amount":50})",
                    R"({"user_id":1,"amount":30})",
                    R"({"user_id":2,"amount":40})",
                    R"({"user_id":1,"amount":20})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE topk (user_id BIGINT, amount BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='user_id,amount')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO topk SELECT * FROM "
                        "(SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC)"
                        " AS rn FROM orders) sub WHERE rn <= 2");

    InProcessCluster cluster("tm-sql-e2e-tnpk", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Upsert sink materialised the final top-2 set; the file is
    // exactly the surviving (user, amount) pairs - no insert/delete
    // trail, no __row_kind tag.
    auto lines = read_lines(out_path);
    std::set<std::pair<std::int64_t, std::int64_t>> survivors;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        EXPECT_FALSE(js.contains("__row_kind")) << l;
        survivors.emplace(static_cast<std::int64_t>(js.at("user_id").as_number()),
                          static_cast<std::int64_t>(js.at("amount").as_number()));
    }
    EXPECT_EQ(survivors,
              (std::set<std::pair<std::int64_t, std::int64_t>>{{1, 50}, {1, 30}, {2, 40}, {2, 5}}));

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// RANK / DENSE_RANK Top-N tie semantics, end to end. ROW_NUMBER keeps
// exactly N rows; RANK keeps tied rows together with a gap after the
// tie; DENSE_RANK keeps tied rows together with no gap. Each row has a
// unique `id` so the upsert sink (keyed by id) preserves tied rows
// separately and the survivor count reflects the ranking semantics.
TEST(SqlRuntime, RankAndDenseRankTopNSemantics) {
    ensure_sql_installed_once();

    auto run_topn = [](const std::string& tag,
                       const std::vector<std::string>& in_lines,
                       const std::string& rank_fn,
                       int n) -> std::set<std::int64_t> {
        const auto in_path =
            std::filesystem::temp_directory_path() / ("clink_sql_e2e_rank_in_" + tag + ".ndjson");
        const auto out_path =
            std::filesystem::temp_directory_path() / ("clink_sql_e2e_rank_out_" + tag + ".ndjson");
        std::filesystem::remove(in_path);
        std::filesystem::remove(out_path);
        write_lines(in_path, in_lines);

        Catalog cat;
        auto ddl =
            parse(std::string{"CREATE TABLE orders (id BIGINT, user_id BIGINT, amount BIGINT) "
                              "WITH (connector='file', format='json', path='"} +
                  in_path.string() +
                  "');"
                  "CREATE TABLE topk (id BIGINT, user_id BIGINT, amount BIGINT) "
                  "WITH (connector='file', format='json', path='" +
                  out_path.string() + "', mode='upsert', primary_key='id')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

        const std::string sql =
            "INSERT INTO topk SELECT id, user_id, amount FROM "
            "(SELECT *, " +
            rank_fn +
            "() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rn "
            "FROM orders) sub WHERE rn <= " +
            std::to_string(n);
        auto spec = compile(cat, sql.c_str());

        InProcessCluster cluster("tm-sql-e2e-rank-" + tag, 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto result = submitter.submit(spec.to_json(), {}, opts);
        EXPECT_TRUE(result.completed) << "reject: " << result.reject_message;
        EXPECT_TRUE(result.ok) << "errors: "
                               << (result.errors.empty() ? "(none)" : result.errors[0]);

        std::set<std::int64_t> ids;
        for (const auto& l : read_lines(out_path)) {
            auto js = clink::config::parse(l);
            ids.insert(static_cast<std::int64_t>(js.at("id").as_number()));
        }
        std::filesystem::remove(in_path);
        std::filesystem::remove(out_path);
        return ids;
    };

    // Dataset A: one partition, amounts 10,9,9,8 (ids 1..4), rn <= 2.
    // ROW_NUMBER keeps exactly 2 rows; RANK / DENSE_RANK keep the tie
    // group (both 9s) so 3 rows survive.
    const std::vector<std::string> ds_a = {
        R"({"id":1,"user_id":1,"amount":10})",
        R"({"id":2,"user_id":1,"amount":9})",
        R"({"id":3,"user_id":1,"amount":9})",
        R"({"id":4,"user_id":1,"amount":8})",
    };
    EXPECT_EQ(run_topn("rn_a", ds_a, "ROW_NUMBER", 2).size(), 2u);
    EXPECT_EQ(run_topn("rk_a", ds_a, "RANK", 2).size(), 3u);
    EXPECT_EQ(run_topn("dr_a", ds_a, "DENSE_RANK", 2).size(), 3u);

    // Dataset B: amounts 10,10,9 (ids 1..3), rn <= 2. RANK ranks the 9
    // at 3 (gap after the tie) so it is excluded; DENSE_RANK ranks it
    // at 2 (no gap) so it survives.
    const std::vector<std::string> ds_b = {
        R"({"id":1,"user_id":1,"amount":10})",
        R"({"id":2,"user_id":1,"amount":10})",
        R"({"id":3,"user_id":1,"amount":9})",
    };
    EXPECT_EQ(run_topn("rk_b", ds_b, "RANK", 2), (std::set<std::int64_t>{1, 2}));
    EXPECT_EQ(run_topn("dr_b", ds_b, "DENSE_RANK", 2), (std::set<std::int64_t>{1, 2, 3}));
}

// --- FROM-derived tables e2e ----------------------------

TEST(SqlRuntime, DerivedTableFeedsOuterAggregate) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_dt_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_dt_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":5})",
                    R"({"user_id":1,"amount":15})",
                    R"({"user_id":2,"amount":3})",
                    R"({"user_id":2,"amount":20})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE per_user (user_id BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    // Derived table filters to amount > 10; outer SUMs per user.
    // user=1 -> 15, user=2 -> 20.
    auto spec = compile(cat,
                        "INSERT INTO per_user SELECT user_id, SUM(amount) AS total FROM "
                        "(SELECT user_id, amount FROM orders WHERE amount > 10) AS big "
                        "GROUP BY user_id");

    InProcessCluster cluster("tm-sql-e2e-dt", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    std::map<std::int64_t, std::int64_t> latest;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        latest[static_cast<std::int64_t>(js.at("user_id").as_number())] =
            static_cast<std::int64_t>(js.at("total").as_number());
    }
    EXPECT_EQ(latest[1], 15);
    EXPECT_EQ(latest[2], 20);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// --- INSERT column list e2e ----------------------------

TEST(SqlRuntime, InsertColumnListReordersToSinkSchema) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_ic_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_ic_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"url":"home"})",
                    R"({"user_id":2,"url":"docs"})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE clicks (user_id BIGINT, url TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (a TEXT, b BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    // SELECT projects (user_id, url); the column list says these go to
    // (b, a). The reordered output is (a, b) = (url, user_id), which
    // matches the sink's declared order.
    auto spec = compile(cat, "INSERT INTO out_t (b, a) SELECT user_id, url FROM clicks");

    InProcessCluster cluster("tm-sql-e2e-ic", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    EXPECT_EQ(lines.size(), 2u);
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        ASSERT_TRUE(js.contains("a")) << l;
        ASSERT_TRUE(js.contains("b")) << l;
        EXPECT_TRUE(js.at("a").is_string());
        EXPECT_TRUE(js.at("b").is_number());
    }

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// --- HAVING with direct aggregate e2e ------------------

TEST(SqlRuntime, HavingDirectAggregateFiltersGroups) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_h_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_h_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":5})",
                    R"({"user_id":1,"amount":20})",
                    R"({"user_id":2,"amount":3})",
                    R"({"user_id":3,"amount":50})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE per_user (user_id BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    // HAVING SUM(amount) > 20: user=1 (25), user=3 (50) pass; user=2 (3)
    // is filtered. No alias on the SUM in SELECT.
    auto spec = compile(cat,
                        "INSERT INTO per_user SELECT user_id, SUM(amount) AS total FROM orders "
                        "GROUP BY user_id HAVING SUM(amount) > 20");

    InProcessCluster cluster("tm-sql-e2e-having", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    // Each input record emits one aggregate row (upsert semantics);
    // collect the final total per user.
    std::map<std::int64_t, std::int64_t> latest;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        latest[static_cast<std::int64_t>(js.at("user_id").as_number())] =
            static_cast<std::int64_t>(js.at("total").as_number());
    }
    EXPECT_EQ(latest.count(2), 0u) << "user=2 sum=3 should be filtered out";
    EXPECT_EQ(latest[1], 25);
    EXPECT_EQ(latest[3], 50);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// --- OFFSET e2e ----------------------------------------

TEST(SqlRuntime, LimitOffsetSkipsThenForwards) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_off_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_off_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"id":1})",
                    R"({"id":2})",
                    R"({"id":3})",
                    R"({"id":4})",
                    R"({"id":5})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE t (id BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (id BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    // OFFSET 2 LIMIT 2: skip ids 1,2; emit ids 3,4.
    auto spec = compile(cat, "INSERT INTO out_t SELECT id FROM t LIMIT 2 OFFSET 2");

    InProcessCluster cluster("tm-sql-e2e-offset", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    auto lines = read_lines(out_path);
    EXPECT_EQ(lines.size(), 2u);
    std::set<std::int64_t> got;
    for (const auto& l : lines)
        got.insert(static_cast<std::int64_t>(clink::config::parse(l).at("id").as_number()));
    EXPECT_EQ(got, (std::set<std::int64_t>{3, 4}));

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// --- IN literal-list e2e -------------------------------

TEST(SqlRuntime, InListFiltersUsers) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_in_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_in_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"url":"a"})",
                    R"({"user_id":2,"url":"b"})",
                    R"({"user_id":3,"url":"c"})",
                    R"({"user_id":4,"url":"d"})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE clicks (user_id BIGINT, url TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, url TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT user_id, url FROM clicks "
                        "WHERE user_id IN (2, 4)");

    InProcessCluster cluster("tm-sql-e2e-in", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    EXPECT_EQ(lines.size(), 2u);
    std::set<std::int64_t> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got.insert(static_cast<std::int64_t>(js.at("user_id").as_number()));
    }
    EXPECT_EQ(got, (std::set<std::int64_t>{2, 4}));

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// --- stream-stream equi-join e2e ------------------------

TEST(SqlRuntime, EquiJoinMatchesAllPairsByKey) {
    ensure_sql_installed_once();

    const auto a_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_eqj_a.ndjson";
    const auto b_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_eqj_b.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_eqj_out.ndjson";
    std::filesystem::remove(a_path);
    std::filesystem::remove(b_path);
    std::filesystem::remove(out_path);

    // clicks: user=1 (home), user=2 (about), user=1 (docs)
    // orders: user=1 (x), user=3 (y), user=1 (z)
    // expect 2 x 2 = 4 matches for user=1, 0 for user=2 (no order)
    // and 0 for user=3 (no click).
    write_lines(a_path,
                {
                    R"({"user_id":1,"page":"home"})",
                    R"({"user_id":2,"page":"about"})",
                    R"({"user_id":1,"page":"docs"})",
                });
    write_lines(b_path,
                {
                    R"({"user_id":1,"sku":"x"})",
                    R"({"user_id":3,"sku":"y"})",
                    R"({"user_id":1,"sku":"z"})",
                });

    Catalog cat;
    auto ddl =
        parse(std::string{"CREATE TABLE clicks (user_id BIGINT, page TEXT) "
                          "WITH (connector='file', format='json', path='"} +
              a_path.string() +
              "');"
              "CREATE TABLE orders (user_id BIGINT, sku TEXT) "
              "WITH (connector='file', format='json', path='" +
              b_path.string() +
              "');"
              "CREATE TABLE joined (c_user_id BIGINT, c_page TEXT, o_user_id BIGINT, o_sku TEXT) "
              "WITH (connector='file', format='json', path='" +
              out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[2]));

    auto spec = compile(cat,
                        "INSERT INTO joined SELECT * FROM clicks c JOIN orders o "
                        "ON c.user_id = o.user_id");

    InProcessCluster cluster("tm-sql-e2e-eqj", 12);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 4u);
    std::set<std::string> pairs;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        EXPECT_EQ(static_cast<std::int64_t>(js.at("c_user_id").as_number()), 1);
        EXPECT_EQ(static_cast<std::int64_t>(js.at("o_user_id").as_number()), 1);
        pairs.insert(js.at("c_page").as_string() + "|" + js.at("o_sku").as_string());
    }
    EXPECT_EQ(pairs, (std::set<std::string>{"home|x", "home|z", "docs|x", "docs|z"}));

    std::filesystem::remove(a_path);
    std::filesystem::remove(b_path);
    std::filesystem::remove(out_path);
}

// SQLOPT-4 (projection through joins): a narrow projection over an equi-join of
// two OVER-WIDE sources narrows BOTH scans (keys + referenced columns kept, junk
// dropped) and still produces the correct join result.
TEST(SqlRuntime, EquiJoinNarrowsWideSources) {
    ensure_sql_installed_once();
    const auto a_path = std::filesystem::temp_directory_path() / "clink_sql_ejnw_a.ndjson";
    const auto b_path = std::filesystem::temp_directory_path() / "clink_sql_ejnw_b.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_ejnw_out.ndjson";
    for (const auto& p : {a_path, b_path, out_path}) {
        std::filesystem::remove(p);
    }
    // Each row carries an extra unreferenced column (note / score).
    write_lines(a_path,
                {R"({"user_id":1,"page":"home","note":"x"})",
                 R"({"user_id":1,"page":"docs","note":"y"})",
                 R"({"user_id":2,"page":"about","note":"z"})"});
    write_lines(b_path,
                {R"({"user_id":1,"sku":"A","score":9})",
                 R"({"user_id":1,"sku":"B","score":8})",
                 R"({"user_id":3,"sku":"C","score":7})"});
    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE clicks (user_id BIGINT, page TEXT, note TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     a_path.string() +
                     "');"
                     "CREATE TABLE orders (user_id BIGINT, sku TEXT, score BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     b_path.string() +
                     "');"
                     "CREATE TABLE out_t (c_page TEXT, o_sku TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (const auto& st : ddl.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    }
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT c_page, o_sku FROM clicks c JOIN orders o "
                        "ON c.user_id = o.user_id");

    InProcessCluster cluster("tm-sql-ejnw", 12);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // user 1 matches (home,docs) x (A,B) = 4 pairs; user 2 / user 3 unmatched.
    std::multiset<std::string> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        got.insert(js.at("c_page").as_string() + "|" + js.at("o_sku").as_string());
    }
    EXPECT_EQ(got, (std::multiset<std::string>{"home|A", "home|B", "docs|A", "docs|B"}));

    for (const auto& p : {a_path, b_path, out_path}) {
        std::filesystem::remove(p);
    }
}

// A top-level join now honours an outer WHERE + projection (previously both
// were silently dropped and only `SELECT * FROM a JOIN b` worked). Join columns
// are the flat "<alias>_<col>" names.
TEST(SqlRuntime, JoinWithWhereAndProjection) {
    ensure_sql_installed_once();
    const auto a_path = std::filesystem::temp_directory_path() / "clink_sql_jwp_a.ndjson";
    const auto b_path = std::filesystem::temp_directory_path() / "clink_sql_jwp_b.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_jwp_out.ndjson";
    for (const auto& p : {a_path, b_path, out_path}) {
        std::filesystem::remove(p);
    }
    write_lines(a_path, {R"({"user_id":1,"page":"home"})", R"({"user_id":1,"page":"docs"})"});
    write_lines(b_path, {R"({"user_id":1,"amount":100})", R"({"user_id":1,"amount":30})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE clicks (user_id BIGINT, page TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     a_path.string() +
                     "');"
                     "CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     b_path.string() +
                     "');"
                     "CREATE TABLE out_t (c_page TEXT, o_amount BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (const auto& st : ddl.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    }
    // 4 join pairs; WHERE keeps only amount > 50 -> (home,100),(docs,100).
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT c_page, o_amount FROM clicks c JOIN orders o "
                        "ON c.user_id = o.user_id WHERE o_amount > 50");

    InProcessCluster cluster("tm-sql-jwp", 12);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 2u);
    std::set<std::string> pages;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        EXPECT_EQ(static_cast<std::int64_t>(js.at("o_amount").as_number()), 100);
        pages.insert(js.at("c_page").as_string());
    }
    EXPECT_EQ(pages, (std::set<std::string>{"home", "docs"}));
    for (const auto& p : {a_path, b_path, out_path}) {
        std::filesystem::remove(p);
    }
}

// A GROUP BY aggregate over a join (previously silently dropped). Proves the
// join output feeds the aggregate path like any source.
TEST(SqlRuntime, JoinFeedsGroupByAggregate) {
    ensure_sql_installed_once();
    const auto a_path = std::filesystem::temp_directory_path() / "clink_sql_jga_a.ndjson";
    const auto b_path = std::filesystem::temp_directory_path() / "clink_sql_jga_b.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_jga_out.ndjson";
    for (const auto& p : {a_path, b_path, out_path}) {
        std::filesystem::remove(p);
    }
    write_lines(a_path, {R"({"user_id":1,"region":"eu"})", R"({"user_id":2,"region":"us"})"});
    write_lines(b_path,
                {R"({"user_id":1,"amount":10})",
                 R"({"user_id":1,"amount":30})",
                 R"({"user_id":2,"amount":5})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE clicks (user_id BIGINT, region TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     a_path.string() +
                     "');"
                     "CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     b_path.string() +
                     "');"
                     "CREATE TABLE out_t (c_region TEXT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (const auto& st : ddl.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    }
    // Join: (eu,10),(eu,30),(us,5) -> GROUP BY c_region: eu=40, us=5.
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT c_region, SUM(o_amount) AS total "
                        "FROM clicks c JOIN orders o ON c.user_id = o.user_id GROUP BY c_region");

    InProcessCluster cluster("tm-sql-jga", 12);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    std::map<std::string, std::int64_t> totals;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        totals[js.at("c_region").as_string()] =
            static_cast<std::int64_t>(js.at("total").as_number());
    }
    EXPECT_EQ(totals["eu"], 40);
    EXPECT_EQ(totals["us"], 5);
    for (const auto& p : {a_path, b_path, out_path}) {
        std::filesystem::remove(p);
    }
}

// Parity wave: LEFT / RIGHT / FULL outer equi-joins. The op emits a
// changelog (null-pad unmatched, retract on later match); we replay it
// to the final logical result, which converges regardless of the
// nondeterministic interleaving of the two sources.
TEST(SqlRuntime, OuterEquiJoins) {
    ensure_sql_installed_once();

    auto run = [](const std::string& tag,
                  const std::string& join_kw) -> std::multiset<std::string> {
        const auto c_path =
            std::filesystem::temp_directory_path() / ("clink_sql_e2e_oj_c_" + tag + ".ndjson");
        const auto o_path =
            std::filesystem::temp_directory_path() / ("clink_sql_e2e_oj_o_" + tag + ".ndjson");
        const auto out_path =
            std::filesystem::temp_directory_path() / ("clink_sql_e2e_oj_out_" + tag + ".ndjson");
        for (const auto& p : {c_path, o_path, out_path})
            std::filesystem::remove(p);
        // customer 2 has no order (unmatched left); order for cust 4 has
        // no customer (unmatched right).
        write_lines(c_path,
                    {R"({"id":1,"name":"a"})", R"({"id":2,"name":"b"})", R"({"id":3,"name":"c"})"});
        write_lines(o_path,
                    {R"({"cust":1,"amt":10})", R"({"cust":3,"amt":30})", R"({"cust":4,"amt":40})"});

        Catalog cat;
        auto ddl =
            parse(std::string{"CREATE TABLE customers (id BIGINT, name TEXT) "
                              "WITH (connector='file', format='json', path='"} +
                  c_path.string() +
                  "');"
                  "CREATE TABLE orders (cust BIGINT, amt BIGINT) "
                  "WITH (connector='file', format='json', path='" +
                  o_path.string() +
                  "');"
                  "CREATE TABLE out_t (c_id BIGINT, c_name TEXT, o_cust BIGINT, o_amt BIGINT) "
                  "WITH (connector='file', format='json', path='" +
                  out_path.string() + "')");
        for (const auto& st : ddl.statements)
            cat.register_table(std::get<ast::CreateTableStmt>(st));
        auto spec = compile(cat,
                            ("INSERT INTO out_t SELECT * FROM customers c " + join_kw +
                             " JOIN orders o ON c.id = o.cust")
                                .c_str());

        InProcessCluster cluster("tm-sql-e2e-oj-" + tag, 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto result = submitter.submit(spec.to_json(), {}, opts);
        EXPECT_TRUE(result.completed) << "reject: " << result.reject_message;
        EXPECT_TRUE(result.ok) << "errors: "
                               << (result.errors.empty() ? "(none)" : result.errors[0]);

        auto key_of = [](const clink::config::JsonValue& js) {
            auto f = [&](const char* k) -> std::string {
                if (!js.contains(k) || js.at(k).is_null())
                    return "NULL";
                const auto& v = js.at(k);
                return v.is_string() ? v.as_string()
                                     : std::to_string(static_cast<std::int64_t>(v.as_number()));
            };
            return "c_id=" + f("c_id") + ";c_name=" + f("c_name") + ";o_cust=" + f("o_cust") +
                   ";o_amt=" + f("o_amt");
        };
        std::multiset<std::string> live;
        for (const auto& l : read_lines(out_path)) {
            auto js = clink::config::parse(l);
            const bool del =
                js.contains("__row_kind") && js.at("__row_kind").as_string() == "delete";
            auto key = key_of(js);
            if (del) {
                auto it = live.find(key);
                if (it != live.end())
                    live.erase(it);
            } else {
                live.insert(std::move(key));
            }
        }
        for (const auto& p : {c_path, o_path, out_path})
            std::filesystem::remove(p);
        return live;
    };

    EXPECT_EQ(run("left", "LEFT"),
              (std::multiset<std::string>{"c_id=1;c_name=a;o_cust=1;o_amt=10",
                                          "c_id=2;c_name=b;o_cust=NULL;o_amt=NULL",
                                          "c_id=3;c_name=c;o_cust=3;o_amt=30"}));
    EXPECT_EQ(run("right", "RIGHT"),
              (std::multiset<std::string>{"c_id=1;c_name=a;o_cust=1;o_amt=10",
                                          "c_id=3;c_name=c;o_cust=3;o_amt=30",
                                          "c_id=NULL;c_name=NULL;o_cust=4;o_amt=40"}));
    EXPECT_EQ(run("full", "FULL"),
              (std::multiset<std::string>{"c_id=1;c_name=a;o_cust=1;o_amt=10",
                                          "c_id=2;c_name=b;o_cust=NULL;o_amt=NULL",
                                          "c_id=3;c_name=c;o_cust=3;o_amt=30",
                                          "c_id=NULL;c_name=NULL;o_cust=4;o_amt=40"}));
}

// --- window + interval-join e2e -------------------------

TEST(SqlRuntime, TumbleWindowAggregatesByUserPerSecond) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_tumble_in.ndjson";
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_tumble_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // Two 1-second windows per user. user=1 lands 3 events in window
    // [0,1000), 2 events in [1000,2000); user=2 lands 1 event in each.
    write_lines(in_path,
                {
                    R"({"user_id":1,"ts":100,"amount":10})",
                    R"({"user_id":1,"ts":200,"amount":20})",
                    R"({"user_id":1,"ts":300,"amount":30})",
                    R"({"user_id":2,"ts":400,"amount":5})",
                    R"({"user_id":1,"ts":1100,"amount":100})",
                    R"({"user_id":1,"ts":1200,"amount":200})",
                    R"({"user_id":2,"ts":1500,"amount":50})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE events (user_id BIGINT, ts BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE per_window (user_id BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO per_window "
                        "SELECT user_id, SUM(amount) AS total FROM events "
                        "GROUP BY TUMBLE(ts, 1000), user_id");

    InProcessCluster cluster("tm-sql-e2e-tumble", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Expect 4 window rows: user=1 (60, 300), user=2 (5, 50).
    auto lines = read_lines(out_path);
    std::multimap<std::int64_t, std::int64_t> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("user_id").as_number()),
                    static_cast<std::int64_t>(js.at("total").as_number()));
    }
    auto contains = [&](std::int64_t uid, std::int64_t total) {
        auto range = got.equal_range(uid);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second == total)
                return true;
        }
        return false;
    };
    EXPECT_TRUE(contains(1, 60)) << "user=1 window=[0,1000) total=60 missing";
    EXPECT_TRUE(contains(1, 300)) << "user=1 window=[1000,2000) total=300 missing";
    EXPECT_TRUE(contains(2, 5)) << "user=2 window=[0,1000) total=5 missing";
    EXPECT_TRUE(contains(2, 50)) << "user=2 window=[1000,2000) total=50 missing";

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// A windowed GROUP BY can project the synthetic window bounds (window_start /
// window_end) into the output, honouring a SELECT alias and landing the bounds
// at the right sink position alongside keys and aggregates.
TEST(SqlRuntime, TumbleWindowSelectsWindowBounds) {
    ensure_sql_installed_once();

    const auto in_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_winbounds_in.ndjson";
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_winbounds_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // user=1: 3 events in [0,1000) sum=60, 2 events in [1000,2000) sum=300.
    // user=2: 1 event in each (5, 50).
    write_lines(in_path,
                {
                    R"({"user_id":1,"ts":100,"amount":10})",
                    R"({"user_id":1,"ts":200,"amount":20})",
                    R"({"user_id":1,"ts":300,"amount":30})",
                    R"({"user_id":2,"ts":400,"amount":5})",
                    R"({"user_id":1,"ts":1100,"amount":100})",
                    R"({"user_id":1,"ts":1200,"amount":200})",
                    R"({"user_id":2,"ts":1500,"amount":50})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE events (user_id BIGINT, ts BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE per_window (user_id BIGINT, total BIGINT, wstart BIGINT, "
                     "wend BIGINT) WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    // window_start/window_end are aliased and interleaved with the aggregate.
    auto spec = compile(cat,
                        "INSERT INTO per_window "
                        "SELECT user_id, SUM(amount) AS total, window_start AS wstart, "
                        "window_end AS wend FROM events GROUP BY TUMBLE(ts, 1000), user_id");

    InProcessCluster cluster("tm-sql-e2e-winbounds", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Each output row carries (user_id, total, wstart, wend) under the aliases.
    auto lines = read_lines(out_path);
    struct Row {
        std::int64_t total, ws, we;
    };
    std::multimap<std::int64_t, Row> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("user_id").as_number()),
                    Row{static_cast<std::int64_t>(js.at("total").as_number()),
                        static_cast<std::int64_t>(js.at("wstart").as_number()),
                        static_cast<std::int64_t>(js.at("wend").as_number())});
    }
    auto contains = [&](std::int64_t uid, std::int64_t total, std::int64_t ws, std::int64_t we) {
        auto range = got.equal_range(uid);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second.total == total && it->second.ws == ws && it->second.we == we)
                return true;
        }
        return false;
    };
    EXPECT_TRUE(contains(1, 60, 0, 1000)) << "user=1 [0,1000) total=60 missing";
    EXPECT_TRUE(contains(1, 300, 1000, 2000)) << "user=1 [1000,2000) total=300 missing";
    EXPECT_TRUE(contains(2, 5, 0, 1000)) << "user=2 [0,1000) total=5 missing";
    EXPECT_TRUE(contains(2, 50, 1000, 2000)) << "user=2 [1000,2000) total=50 missing";

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// A WHERE predicate can compare two columns of the same row (not just a column
// to a literal).
TEST(SqlRuntime, WhereComparesTwoColumns) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_colcmp_in.ndjson";
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_colcmp_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // Keep rows where a >= b: (5,3) and (4,4); drop (1,2) and (2,8).
    write_lines(in_path,
                {
                    R"({"a":1,"b":2})",
                    R"({"a":5,"b":3})",
                    R"({"a":4,"b":4})",
                    R"({"a":2,"b":8})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE pairs (a BIGINT, b BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE kept (a BIGINT, b BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat, "INSERT INTO kept SELECT a, b FROM pairs WHERE a >= b");

    InProcessCluster cluster("tm-sql-e2e-colcmp", 4);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::set<std::pair<std::int64_t, std::int64_t>> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("a").as_number()),
                    static_cast<std::int64_t>(js.at("b").as_number()));
    }
    EXPECT_EQ(got.size(), 2u);
    EXPECT_TRUE(got.count({5, 3}) == 1) << "(5,3) a>=b missing";
    EXPECT_TRUE(got.count({4, 4}) == 1) << "(4,4) a>=b (eq) missing";
    EXPECT_TRUE(got.count({1, 2}) == 0) << "(1,2) a<b wrongly kept";

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Nexmark-q7 shape: the highest-priced bid(s) per window. A windowed MAX is a
// join side (equi on price), and a column-vs-column range residual keeps only the
// bid whose time falls in that window - rejecting cross-window false matches
// (two windows share max price 70; each bid must bind to its OWN window).
TEST(SqlRuntime, HighestBidPerWindowJoinWithRangeResidual) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q7_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q7_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // win [0,1000): max price 70 (bidder 6 @ ts 400). win [1000,2000): max 70 (bidder 4 @ ts 1100).
    write_lines(in_path,
                {
                    R"({"price":10,"bidder":1,"ts":100})",
                    R"({"price":50,"bidder":2,"ts":200})",
                    R"({"price":70,"bidder":6,"ts":400})",
                    R"({"price":70,"bidder":4,"ts":1100})",
                    R"({"price":20,"bidder":5,"ts":1200})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE bids (price BIGINT, bidder BIGINT, ts BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE hot (price BIGINT, bidder BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(
        cat,
        "INSERT INTO hot SELECT b_price AS price, b_bidder AS bidder FROM bids AS B "
        "JOIN (SELECT MAX(price) AS maxprice, window_start AS ws, window_end AS we FROM bids "
        "GROUP BY TUMBLE(ts, 1000)) AS M ON B.price = M.maxprice "
        "WHERE b_ts >= m_ws AND b_ts < m_we");

    InProcessCluster cluster("tm-sql-e2e-q7", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 20s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::multiset<std::pair<std::int64_t, std::int64_t>> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("price").as_number()),
                    static_cast<std::int64_t>(js.at("bidder").as_number()));
    }
    // Exactly the two per-window winners; the range residual rejects the 2
    // cross-window false matches that the equi-join alone would produce.
    EXPECT_EQ(got.size(), 2u);
    EXPECT_EQ(got.count({70, 6}), 1u) << "window [0,1000) winner bidder 6 missing";
    EXPECT_EQ(got.count({70, 4}), 1u) << "window [1000,2000) winner bidder 4 missing";

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// A non-windowed GROUP BY feeding a changelog-netting sink emits update_before/
// update_after on each group change; the sink nets them to the final aggregate
// per group. Distinguishes changelog from append: without retraction the
// intermediate (k=1,total=10) row would survive (3 rows), with it only the final
// (k=1,total=30) does (2 rows).
TEST(SqlRuntime, NonWindowedAggregateEmitsChangelogToNettingSink) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_clog_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_clog_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path, {R"({"k":1,"v":10})", R"({"k":1,"v":20})", R"({"k":2,"v":5})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE events (k BIGINT, v BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE agg (k BIGINT, total BIGINT) "
                     "WITH (connector='changelog', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat, "INSERT INTO agg SELECT k, SUM(v) AS total FROM events GROUP BY k");

    InProcessCluster cluster("tm-sql-e2e-clog", 4);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::map<std::int64_t, std::int64_t> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("k").as_number())] =
            static_cast<std::int64_t>(js.at("total").as_number());
    }
    EXPECT_EQ(lines.size(), 2u) << "intermediate aggregate not retracted (changelog off?)";
    EXPECT_EQ(got[1], 30) << "k=1 final total";
    EXPECT_EQ(got[2], 5) << "k=2 final total";

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Nexmark-q19 shape: per-auction top-N bids by price. ROW_NUMBER() OVER
// (PARTITION BY auction ORDER BY price DESC) with rn <= N is a TOP-N-per-key
// changelog; the netting sink resolves it to the final top-N set per auction.
TEST(SqlRuntime, TopNBidsPerAuctionToNettingSink) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q19_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q19_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // auction 1 prices {30,10,20} -> top-2 {30,20}; auction 2 {5,15} -> {15,5}.
    write_lines(in_path,
                {
                    R"({"auction":1,"price":30})",
                    R"({"auction":1,"price":10})",
                    R"({"auction":1,"price":20})",
                    R"({"auction":2,"price":5})",
                    R"({"auction":2,"price":15})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE bids (auction BIGINT, price BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE topn (auction BIGINT, price BIGINT) "
                     "WITH (connector='changelog', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO topn SELECT auction, price FROM "
                        "(SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) "
                        "AS rn FROM bids) AS t WHERE rn <= 2");

    InProcessCluster cluster("tm-sql-e2e-q19", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::set<std::pair<std::int64_t, std::int64_t>> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("auction").as_number()),
                    static_cast<std::int64_t>(js.at("price").as_number()));
    }
    EXPECT_EQ(got.size(), 4u) << "expected top-2 per auction (4 rows), the rest retracted";
    EXPECT_TRUE(got.count({1, 30}) && got.count({1, 20})) << "auction 1 top-2 {30,20}";
    EXPECT_TRUE(got.count({2, 15}) && got.count({2, 5})) << "auction 2 top-2 {15,5}";
    EXPECT_FALSE(got.count({1, 10})) << "auction 1 price 10 should be retracted (not top-2)";

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Nexmark-q15 shape: per-day bidding stats. DATE_TRUNC('day', <ms>) buckets the
// event time; the non-windowed GROUP BY computes COUNT(*) + COUNT(DISTINCT) per
// day; an upsert sink keyed by day keeps the final aggregate per day.
TEST(SqlRuntime, PerDayBiddingStatsWithDistinctCounts) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q15_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q15_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // day 0 (ts < 86400000): 3 bids, bidders {1,2,2}, auctions {1,1,2}.
    // day 1 (ts >= 86400000): 1 bid.
    write_lines(in_path,
                {
                    R"({"auction":1,"bidder":1,"datetime":100})",
                    R"({"auction":1,"bidder":2,"datetime":200})",
                    R"({"auction":2,"bidder":2,"datetime":300})",
                    R"({"auction":1,"bidder":1,"datetime":86400100})",
                });

    Catalog cat;
    auto ddl =
        parse(std::string{"CREATE TABLE bids (auction BIGINT, bidder BIGINT, datetime BIGINT) "
                          "WITH (connector='file', format='json', path='"} +
              in_path.string() +
              "');"
              "CREATE TABLE stats (day BIGINT, total BIGINT, bidders BIGINT, auctions BIGINT) "
              "WITH (connector='file', format='json', mode='upsert', primary_key='day', "
              "path='" +
              out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO stats SELECT day, COUNT(*) AS total, "
                        "COUNT(DISTINCT bidder) AS bidders, COUNT(DISTINCT auction) AS auctions "
                        "FROM (SELECT DATE_TRUNC('day', datetime) AS day, bidder, auction "
                        "FROM bids) AS t GROUP BY day");

    InProcessCluster cluster("tm-sql-e2e-q15", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    struct S {
        std::int64_t total, bidders, auctions;
    };
    std::map<std::int64_t, S> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("day").as_number())] =
            S{static_cast<std::int64_t>(js.at("total").as_number()),
              static_cast<std::int64_t>(js.at("bidders").as_number()),
              static_cast<std::int64_t>(js.at("auctions").as_number())};
    }
    ASSERT_TRUE(got.count(0)) << "day 0 bucket missing";
    EXPECT_EQ(got[0].total, 3);
    EXPECT_EQ(got[0].bidders, 2) << "distinct bidders {1,2}";
    EXPECT_EQ(got[0].auctions, 2) << "distinct auctions {1,2}";
    ASSERT_TRUE(got.count(86400000)) << "day 1 bucket missing";
    EXPECT_EQ(got[86400000].total, 1);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Nexmark-q16 shape: per-(channel,day) stats - COUNT, COUNT(DISTINCT) bidders/
// auctions, MIN/MAX bid time, and price-range bucket counts (SUM over CASE
// buckets computed in a derived table) into an upsert sink.
TEST(SqlRuntime, ChannelStatsPerDayWithBuckets) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q16_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q16_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // channel A, day 0: prices {5000, 50000, 2000000}, bidders {1,2,1}, auctions {1,1,2}.
    //   -> total 3, distinct bidders 2, distinct auctions 2, min/max bid time 100/300,
    //      lt10k 1 (5000), bet 1 (50000), gt1m 1 (2000000). channel B day 0: one bid price 8000.
    write_lines(in_path,
                {
                    R"({"channel":"A","bidder":1,"auction":1,"price":5000,"datetime":100})",
                    R"({"channel":"A","bidder":2,"auction":1,"price":50000,"datetime":200})",
                    R"({"channel":"A","bidder":1,"auction":2,"price":2000000,"datetime":300})",
                    R"({"channel":"B","bidder":3,"auction":3,"price":8000,"datetime":400})",
                });

    Catalog cat;
    auto ddl = parse(
        std::string{
            "CREATE TABLE bid (channel VARCHAR, bidder BIGINT, auction BIGINT, price BIGINT, "
            "datetime BIGINT) WITH (connector='file', format='json', path='"} +
        in_path.string() +
        "');"
        "CREATE TABLE chstats (channel VARCHAR, day BIGINT, total BIGINT, bidders BIGINT, "
        "auctions BIGINT, minbid BIGINT, maxbid BIGINT, lt10k BIGINT, bet BIGINT, gt1m BIGINT) "
        "WITH (connector='file', format='json', mode='upsert', primary_key='channel,day', path='" +
        out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(
        cat,
        "INSERT INTO chstats SELECT channel, day, COUNT(*) AS total, "
        "COUNT(DISTINCT bidder) AS bidders, COUNT(DISTINCT auction) AS auctions, MIN(dt) AS "
        "minbid, "
        "MAX(dt) AS maxbid, SUM(r1) AS lt10k, SUM(r2) AS bet, SUM(r3) AS gt1m FROM "
        "(SELECT channel, DATE_TRUNC('day', datetime) AS day, bidder, auction, datetime AS dt, "
        "CASE WHEN price < 10000 THEN 1 ELSE 0 END AS r1, "
        "CASE WHEN price >= 10000 AND price <= 1000000 THEN 1 ELSE 0 END AS r2, "
        "CASE WHEN price > 1000000 THEN 1 ELSE 0 END AS r3 FROM bid) AS t GROUP BY channel, day");

    InProcessCluster cluster("tm-sql-e2e-q16", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    struct S {
        std::int64_t total, bidders, auctions, minbid, maxbid, lt10k, bet, gt1m;
    };
    std::map<std::string, S> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[js.at("channel").as_string()] =
            S{static_cast<std::int64_t>(js.at("total").as_number()),
              static_cast<std::int64_t>(js.at("bidders").as_number()),
              static_cast<std::int64_t>(js.at("auctions").as_number()),
              static_cast<std::int64_t>(js.at("minbid").as_number()),
              static_cast<std::int64_t>(js.at("maxbid").as_number()),
              static_cast<std::int64_t>(js.at("lt10k").as_number()),
              static_cast<std::int64_t>(js.at("bet").as_number()),
              static_cast<std::int64_t>(js.at("gt1m").as_number())};
    }
    ASSERT_TRUE(got.count("A"));
    EXPECT_EQ(got["A"].total, 3);
    EXPECT_EQ(got["A"].bidders, 2);
    EXPECT_EQ(got["A"].auctions, 2);
    EXPECT_EQ(got["A"].minbid, 100);
    EXPECT_EQ(got["A"].maxbid, 300);
    EXPECT_EQ(got["A"].lt10k, 1);
    EXPECT_EQ(got["A"].bet, 1);
    EXPECT_EQ(got["A"].gt1m, 1);
    ASSERT_TRUE(got.count("B"));
    EXPECT_EQ(got["B"].total, 1);
    EXPECT_EQ(got["B"].lt10k, 1);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Nexmark q21/q22 shape: regexp_extract + split_index scalar functions in a
// stateless projection (q21 extracts a channel id; q22 splits a url path).
TEST(SqlRuntime, RegexpExtractAndSplitIndexInProjection) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_strfn_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_strfn_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"id":1,"url":"https://site.com/foo/42"})",
                    R"({"id":2,"url":"plain"})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE src (id BIGINT, url VARCHAR) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (id BIGINT, num VARCHAR, seg0 VARCHAR, seg2 VARCHAR) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT id, regexp_extract(url, '([0-9]+)', 1) AS num, "
                        "split_index(url, '/', 0) AS seg0, split_index(url, '/', 2) AS seg2 "
                        "FROM src");

    InProcessCluster cluster("tm-sql-e2e-strfn", 4);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::map<std::int64_t, clink::config::JsonValue> rows;
    for (const auto& l : lines)
        rows[static_cast<std::int64_t>(clink::config::parse(l).at("id").as_number())] =
            clink::config::parse(l);
    ASSERT_TRUE(rows.count(1) && rows.count(2));
    // id 1: url 'https://site.com/foo/42' -> num 42, seg0 'https:', seg2 'site.com'.
    EXPECT_EQ(rows[1].at("num").as_string(), "42");
    EXPECT_EQ(rows[1].at("seg0").as_string(), "https:");
    EXPECT_EQ(rows[1].at("seg2").as_string(), "site.com");
    // id 2: 'plain' -> no digits (num null), seg0 whole string, seg2 out of range (null).
    EXPECT_TRUE(rows[2].at("num").is_null());
    EXPECT_EQ(rows[2].at("seg0").as_string(), "plain");
    EXPECT_TRUE(rows[2].at("seg2").is_null());

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Nexmark-q10 shape: log to partitioned storage - write the stream to one file
// per distinct partition-key value (connector='file' + partition_by). Verifies
// records land in the right partition file.
TEST(SqlRuntime, PartitionedFileSinkByColumn) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q10_in.ndjson";
    const auto base = (std::filesystem::temp_directory_path() / "clink_sql_e2e_q10_out").string();
    std::filesystem::remove(in_path);
    std::filesystem::remove(base + ".2024");
    std::filesystem::remove(base + ".2025");

    write_lines(in_path,
                {
                    R"({"id":1,"day":"2024","v":10})",
                    R"({"id":2,"day":"2024","v":20})",
                    R"({"id":3,"day":"2025","v":30})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE src (id BIGINT, day VARCHAR, v BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE part (id BIGINT, day VARCHAR, v BIGINT) "
                     "WITH (connector='file', format='json', partition_by='day', path='" +
                     base + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat, "INSERT INTO part SELECT id, day, v FROM src");

    InProcessCluster cluster("tm-sql-e2e-q10", 4);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // One file per partition value; each carries only its partition's rows.
    auto ids_in = [](const std::filesystem::path& p) {
        std::set<std::int64_t> ids;
        for (const auto& l : read_lines(p))
            ids.insert(static_cast<std::int64_t>(clink::config::parse(l).at("id").as_number()));
        return ids;
    };
    ASSERT_TRUE(std::filesystem::exists(base + ".2024")) << "partition 2024 file missing";
    ASSERT_TRUE(std::filesystem::exists(base + ".2025")) << "partition 2025 file missing";
    EXPECT_EQ(ids_in(base + ".2024"), (std::set<std::int64_t>{1, 2}));
    EXPECT_EQ(ids_in(base + ".2025"), (std::set<std::int64_t>{3}));

    std::filesystem::remove(in_path);
    std::filesystem::remove(base + ".2024");
    std::filesystem::remove(base + ".2025");
}

// q10 partition sink, path-safety + collision-freedom: partition values that
// carry filename-unsafe bytes ('/', '.') must (a) not escape the base directory
// and (b) never alias onto one another. The two values "a/b" and "a.b" would
// both collapse to "a_b" under a lossy map-to-underscore scheme - silent data
// loss - so this asserts they land in DISTINCT percent-encoded files, each with
// only its own rows. "a/b" must also not create a real subdirectory.
TEST(SqlRuntime, PartitionedFileSinkPercentEncodesUnsafeKeys) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_q10enc_in.ndjson";
    const auto base = (std::filesystem::temp_directory_path() / "clink_sql_q10enc_out").string();
    // "a/b" -> "%2F" ('/' = 0x2F), "a.b" -> "%2E" ('.' = 0x2E).
    const std::string f_slash = base + ".a%2Fb";
    const std::string f_dot = base + ".a%2Eb";
    std::filesystem::remove(in_path);
    std::filesystem::remove(f_slash);
    std::filesystem::remove(f_dot);

    write_lines(in_path,
                {
                    R"({"id":1,"k":"a/b","v":10})",
                    R"({"id":2,"k":"a.b","v":20})",
                    R"({"id":3,"k":"a/b","v":30})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE src (id BIGINT, k VARCHAR, v BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE part (id BIGINT, k VARCHAR, v BIGINT) "
                     "WITH (connector='file', format='json', partition_by='k', path='" +
                     base + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat, "INSERT INTO part SELECT id, k, v FROM src");

    InProcessCluster cluster("tm-sql-q10enc", 4);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto ids_in = [](const std::filesystem::path& p) {
        std::set<std::int64_t> ids;
        for (const auto& l : read_lines(p))
            ids.insert(static_cast<std::int64_t>(clink::config::parse(l).at("id").as_number()));
        return ids;
    };
    // Distinct files for the two distinct keys - no collision, no data loss.
    ASSERT_TRUE(std::filesystem::exists(f_slash)) << "a/b partition file missing";
    ASSERT_TRUE(std::filesystem::exists(f_dot)) << "a.b partition file missing";
    EXPECT_EQ(ids_in(f_slash), (std::set<std::int64_t>{1, 3}));
    EXPECT_EQ(ids_in(f_dot), (std::set<std::int64_t>{2}));
    // The '/' was encoded, not honoured: no "a" subdirectory under the temp dir.
    EXPECT_FALSE(std::filesystem::is_directory(base + ".a"));

    std::filesystem::remove(in_path);
    std::filesystem::remove(f_slash);
    std::filesystem::remove(f_dot);
}

// q10 partition sink rejects append-only-incompatible modes at bind: a
// partition_by sink writes one append-only file per key, so combining it with
// mode='upsert' must be rejected rather than silently dropping the mode.
TEST(SqlRuntime, PartitionedFileSinkRejectsUpsertMode) {
    ensure_sql_installed_once();

    Catalog cat;
    auto ddl = parse(std::string{
        "CREATE TABLE src (id BIGINT, k VARCHAR) "
        "WITH (connector='file', format='json', path='/tmp/clink_sql_q10reject_in.ndjson');"
        "CREATE TABLE part (id BIGINT, k VARCHAR) "
        "WITH (connector='file', format='json', partition_by='k', mode='upsert', "
        "key='id', path='/tmp/clink_sql_q10reject_out')"});
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    bool threw = false;
    try {
        // Bind/compile should reject the partition_by + upsert combination.
        auto spec = compile(cat, "INSERT INTO part SELECT id, k FROM src");
    } catch (const std::exception&) {
        threw = true;
    }
    EXPECT_TRUE(threw) << "partition_by + mode='upsert' should be rejected at bind";
}

// Nexmark-q14 shape: a calculation with user-defined scalar functions - one in
// the projection (a custom fee) and one as a predicate (kept-bid test). clink
// has no CREATE FUNCTION DDL, so the UDFs are registered programmatically via
// ScalarFunctionRegistry; the predicate UDF runs via the derived-table pattern
// (WHERE compares a column - here the UDF-computed flag - to a literal).
TEST(SqlRuntime, ScalarUdfInProjectionAndPredicate) {
    ensure_sql_installed_once();

    ScalarFunctionRegistry::global().register_function(
        "bid_fee",
        arrow::int64(),
        [](const std::vector<clink::config::JsonValue>& args) -> clink::config::JsonValue {
            if (args.empty() || args[0].is_null())
                return clink::config::JsonValue{nullptr};
            return clink::config::JsonValue{static_cast<std::int64_t>(args[0].as_number()) + 100};
        });
    ScalarFunctionRegistry::global().register_function(
        "bid_keep",
        arrow::boolean(),
        [](const std::vector<clink::config::JsonValue>& args) -> clink::config::JsonValue {
            if (args.empty() || args[0].is_null())
                return clink::config::JsonValue{false};
            return clink::config::JsonValue{args[0].as_number() >= 50.0};
        });

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q14_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q14_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path,
                {R"({"id":1,"price":30})", R"({"id":2,"price":80})", R"({"id":3,"price":100})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE bid (id BIGINT, price BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (id BIGINT, fee BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    // fee = bid_fee(price); keep rows where bid_keep(price) (price >= 50).
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT id, fee FROM "
                        "(SELECT id, bid_fee(price) AS fee, bid_keep(price) AS k FROM bid) AS t "
                        "WHERE k = true");

    InProcessCluster cluster("tm-sql-e2e-q14", 4);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::map<std::int64_t, std::int64_t> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("id").as_number())] =
            static_cast<std::int64_t>(js.at("fee").as_number());
    }
    EXPECT_EQ(got.size(), 2u) << "id 1 (price 30 < 50) filtered out by the predicate UDF";
    EXPECT_EQ(got[2], 180) << "bid_fee(80) = 180";
    EXPECT_EQ(got[3], 200) << "bid_fee(100) = 200";
    EXPECT_FALSE(got.count(1));

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Nexmark-q13 shape: bounded side-input join - enrich each bid with a label
// from a static side table keyed by (auction mod N). Expressed via clink's
// lookup join (connector='lookup' + a registered side function), the engine's
// equivalent of Flink's FOR SYSTEM_TIME side-input join.
TEST(SqlRuntime, BoundedSideInputLookupJoin) {
    ensure_sql_installed_once();

    // Side input: label = "L" + (auction mod 3). Bounded, deterministic.
    AsyncFunctionRegistry::global().register_function(
        "side_lookup", [](const Row& probe) -> clink::async::Task<Row> {
            Row dim;
            if (auto s = probe.get_string("auction")) {
                const std::int64_t k = std::stoll(*s);
                dim.values["key"] = clink::config::JsonValue{static_cast<double>(k)};
                dim.values["label"] =
                    clink::config::JsonValue{std::string{"L"} + std::to_string(k % 3)};
            }
            co_return dim;
        });

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q13_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q13_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path,
                {R"({"auction":1,"bidder":10})",
                 R"({"auction":2,"bidder":20})",
                 R"({"auction":5,"bidder":50})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE bid (auction BIGINT, bidder BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE side (key BIGINT, label TEXT) "
                     "WITH (connector='lookup', function='side_lookup');"
                     "CREATE TABLE out_t (auction BIGINT, bidder BIGINT, label TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));

    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT B_auction AS auction, B_bidder AS bidder, "
                        "S_label AS label FROM bid AS B JOIN side AS S ON B.auction = S.key");

    InProcessCluster cluster("tm-sql-e2e-q13", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::map<std::int64_t, std::string> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("auction").as_number())] = js.at("label").as_string();
    }
    EXPECT_EQ(got.size(), 3u);
    EXPECT_EQ(got[1], "L1");  // 1 % 3
    EXPECT_EQ(got[2], "L2");  // 2 % 3
    EXPECT_EQ(got[5], "L2");  // 5 % 3

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Nexmark-q9 shape: winning bid per auction = the MAX-price bid during the
// auction's open period [datetime, expires]. bid INNER JOIN auction on
// auction=id + a column-vs-column interval residual, then ROW_NUMBER top-1 by
// price (a TOP-N-per-key changelog) into a netting sink. The residual must
// EXCLUDE an out-of-window higher bid.
TEST(SqlRuntime, WinningBidPerAuctionIntervalJoinTopN) {
    ensure_sql_installed_once();

    const auto au_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q9_au.ndjson";
    const auto bd_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q9_bd.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q9_out.ndjson";
    for (const auto& f : {au_path, bd_path, out_path})
        std::filesystem::remove(f);

    write_lines(
        au_path,
        {R"({"id":1,"datetime":0,"expires":1000})", R"({"id":2,"datetime":0,"expires":1000})"});
    // auction 1: 50@100, 80@200 (both in window), 90@2000 (OUT of window -> excluded).
    // auction 2: 30@300. Winners: a1 -> bidder 11 price 80; a2 -> bidder 20 price 30.
    write_lines(bd_path,
                {
                    R"({"auction":1,"bidder":10,"price":50,"datetime":100})",
                    R"({"auction":1,"bidder":11,"price":80,"datetime":200})",
                    R"({"auction":1,"bidder":12,"price":90,"datetime":2000})",
                    R"({"auction":2,"bidder":20,"price":30,"datetime":300})",
                });

    Catalog cat;
    auto ddl =
        parse(std::string{"CREATE TABLE auction (id BIGINT, datetime BIGINT, expires BIGINT) "
                          "WITH (connector='file', format='json', path='"} +
              au_path.string() +
              "');"
              "CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, datetime BIGINT) "
              "WITH (connector='file', format='json', path='" +
              bd_path.string() +
              "');"
              "CREATE TABLE winners (auction BIGINT, bidder BIGINT, price BIGINT) "
              "WITH (connector='changelog', format='json', path='" +
              out_path.string() + "')");
    for (int i = 0; i < 3; ++i)
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[i]));

    auto spec = compile(
        cat,
        "INSERT INTO winners SELECT b_auction AS auction, b_bidder AS bidder, b_price AS price "
        "FROM "
        "(SELECT *, ROW_NUMBER() OVER (PARTITION BY b_auction ORDER BY b_price DESC) AS rn FROM "
        "(SELECT b_auction, b_bidder, b_price FROM bid AS B JOIN auction AS A ON B.auction = A.id "
        "WHERE b_datetime >= a_datetime AND b_datetime <= a_expires) AS j) AS r WHERE rn <= 1");

    InProcessCluster cluster("tm-sql-e2e-q9", 16);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 25s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::map<std::int64_t, std::pair<std::int64_t, std::int64_t>>
        got;  // auction -> (bidder, price)
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("auction").as_number())] = {
            static_cast<std::int64_t>(js.at("bidder").as_number()),
            static_cast<std::int64_t>(js.at("price").as_number())};
    }
    EXPECT_EQ(got.size(), 2u) << "one winner per auction";
    EXPECT_EQ(got[1].first, 11)
        << "auction 1 winner is the in-window max (80), not the 90 out of window";
    EXPECT_EQ(got[1].second, 80);
    EXPECT_EQ(got[2].first, 20);
    EXPECT_EQ(got[2].second, 30);

    for (const auto& f : {au_path, bd_path, out_path})
        std::filesystem::remove(f);
}

// Nexmark-q4 shape: average winning price per category. Winning price = MAX bid
// per auction during its open period (the q9 interval join + per-auction MAX);
// then AVG over categories - a STACKED aggregate (AVG over a per-auction MAX),
// which needs the inner MAX to emit a changelog the outer AVG retracts.
TEST(SqlRuntime, AvgWinningPriceByCategoryStackedAggregate) {
    ensure_sql_installed_once();

    const auto au_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q4_au.ndjson";
    const auto bd_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q4_bd.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q4_out.ndjson";
    for (const auto& f : {au_path, bd_path, out_path})
        std::filesystem::remove(f);

    write_lines(au_path,
                {
                    R"({"id":1,"category":10,"datetime":0,"expires":1000})",
                    R"({"id":2,"category":10,"datetime":0,"expires":1000})",
                    R"({"id":3,"category":20,"datetime":0,"expires":1000})",
                });
    // a1 (cat10) max 80, a2 (cat10) max 40, a3 (cat20) max 100. (All in window.)
    // AVG by category: cat10 = (80+40)/2 = 60; cat20 = 100.
    write_lines(bd_path,
                {
                    R"({"auction":1,"price":50,"datetime":100})",
                    R"({"auction":1,"price":80,"datetime":200})",
                    R"({"auction":2,"price":40,"datetime":150})",
                    R"({"auction":3,"price":100,"datetime":250})",
                });

    Catalog cat;
    auto ddl = parse(
        std::string{"CREATE TABLE auction (id BIGINT, category BIGINT, datetime BIGINT, "
                    "expires BIGINT) WITH (connector='file', format='json', path='"} +
        au_path.string() +
        "');"
        "CREATE TABLE bid (auction BIGINT, price BIGINT, datetime BIGINT) "
        "WITH (connector='file', format='json', path='" +
        bd_path.string() +
        "');"
        "CREATE TABLE avg_by_cat (category BIGINT, avgp DOUBLE) "
        "WITH (connector='file', format='json', mode='upsert', primary_key='category', path='" +
        out_path.string() + "')");
    for (int i = 0; i < 3; ++i)
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[i]));

    auto spec = compile(cat,
                        "INSERT INTO avg_by_cat SELECT category, AVG(maxp) AS avgp FROM "
                        "(SELECT a_category AS category, MAX(b_price) AS maxp FROM "
                        "(SELECT b_auction, b_price, a_category FROM bid AS B JOIN auction AS A ON "
                        "B.auction = A.id "
                        "WHERE b_datetime >= a_datetime AND b_datetime <= a_expires) AS j "
                        "GROUP BY b_auction, a_category) AS wins GROUP BY category");

    InProcessCluster cluster("tm-sql-e2e-q4", 16);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 25s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::map<std::int64_t, double> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("category").as_number())] = js.at("avgp").as_number();
    }
    ASSERT_TRUE(got.count(10) && got.count(20));
    EXPECT_NEAR(got[10], 60.0, 1e-9) << "cat10 = avg(80,40); inner MAX must not double-count";
    EXPECT_NEAR(got[20], 100.0, 1e-9);

    for (const auto& f : {au_path, bd_path, out_path})
        std::filesystem::remove(f);
}

// last_n_agg: a rolling aggregate over the most-recent N elements per key. A
// bounded ROWS frame over a source with NO declared event_time_column lowers to
// the changelog-emitting last_n_agg operator (the engine primitive behind
// "metric over the last N events per key"). Here AVG over the last 2 per key:
// for k=1 the values 10,20,60 arrive and the final last-2 average is
// avg(20,60)=40 (the 10 has slid out; avg-of-all would be 30), proving the
// window slides. Materialised via an upsert sink keyed on k.
TEST(SqlRuntime, LastNAggRollingAverageSlidesPerKey) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_lastn_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_lastn_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path,
                {
                    R"({"k":1,"t":1,"v":10})",
                    R"({"k":1,"t":2,"v":20})",
                    R"({"k":1,"t":3,"v":60})",
                    R"({"k":2,"t":1,"v":100})",
                });

    Catalog cat;
    auto ddl =
        parse(std::string{"CREATE TABLE src (k BIGINT, t BIGINT, v BIGINT) "
                          "WITH (connector='file', format='json', path='"} +
              in_path.string() +
              "');"
              "CREATE TABLE rollavg (k BIGINT, avgv DOUBLE) "
              "WITH (connector='file', format='json', mode='upsert', primary_key='k', path='" +
              out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO rollavg SELECT k, AVG(v) OVER (PARTITION BY k ORDER BY t "
                        "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avgv FROM src");

    InProcessCluster cluster("tm-sql-lastn", 4);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 20s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::map<std::int64_t, double> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("k").as_number())] = js.at("avgv").as_number();
    }
    ASSERT_TRUE(got.count(1) && got.count(2));
    EXPECT_NEAR(got[1], 40.0, 1e-9) << "last-2 avg(20,60)=40; the 10 slid out of the window";
    EXPECT_NEAR(got[2], 100.0, 1e-9);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Nexmark-q6: average winning price per seller over that seller's last-N closed
// auctions. Winning price = MAX in-window bid per auction (the q9 TOP-1 changelog
// producer); then a last-N-per-seller AVG ordered by close time (a_expires).
// Two engine paths are exercised at once:
//   - SLIDING eviction: seller 1 closes 3 auctions, so with N=2 the oldest
//     (a1) slides out of the average.
//   - RETRACTION CONSUMPTION: auction a3's winning bid changes (60 -> 70) within
//     its window, so the upstream TOP-1 emits delete(a3,60)+insert(a3,70); the
//     last_n_agg must drop the old 60 (not fold it as a second insert), else
//     seller 1's last-2 would be avg(60,70)=65 with a2 evicted, not avg(30,70)=50.
TEST(SqlRuntime, WinningPriceAvgOverSellerLastNClosedAuctions) {
    ensure_sql_installed_once();

    const auto au_path = std::filesystem::temp_directory_path() / "clink_sql_q6_au.ndjson";
    const auto bd_path = std::filesystem::temp_directory_path() / "clink_sql_q6_bd.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_q6_out.ndjson";
    for (const auto& f : {au_path, bd_path, out_path})
        std::filesystem::remove(f);

    // seller 1: a1 (close 100), a2 (close 200), a3 (close 300). seller 2: a4.
    write_lines(au_path,
                {
                    R"({"id":1,"seller":1,"datetime":0,"expires":100})",
                    R"({"id":2,"seller":1,"datetime":0,"expires":200})",
                    R"({"id":3,"seller":1,"datetime":0,"expires":300})",
                    R"({"id":4,"seller":2,"datetime":0,"expires":100})",
                });
    // Winning prices: a1=50, a2=30, a3=70 (60 then a higher 70 within window), a4=99.
    write_lines(bd_path,
                {
                    R"({"auction":1,"price":50,"datetime":10})",
                    R"({"auction":2,"price":30,"datetime":20})",
                    R"({"auction":3,"price":60,"datetime":30})",
                    R"({"auction":3,"price":70,"datetime":40})",
                    R"({"auction":4,"price":99,"datetime":10})",
                });

    Catalog cat;
    auto ddl =
        parse(std::string{"CREATE TABLE auction (id BIGINT, seller BIGINT, datetime BIGINT, "
                          "expires BIGINT) WITH (connector='file', format='json', path='"} +
              au_path.string() +
              "');"
              "CREATE TABLE bid (auction BIGINT, price BIGINT, datetime BIGINT) "
              "WITH (connector='file', format='json', path='" +
              bd_path.string() +
              "');"
              "CREATE TABLE q6 (seller BIGINT, avgp DOUBLE) "
              "WITH (connector='file', format='json', mode='upsert', primary_key='seller', path='" +
              out_path.string() + "')");
    for (int i = 0; i < 3; ++i)
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[i]));

    auto spec = compile(
        cat,
        "INSERT INTO q6 SELECT seller, AVG(price) OVER (PARTITION BY seller ORDER BY close_dt "
        "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avgp FROM "
        "(SELECT a_seller AS seller, b_price AS price, a_expires AS close_dt FROM "
        "(SELECT *, ROW_NUMBER() OVER (PARTITION BY b_auction ORDER BY b_price DESC) AS rn FROM "
        "(SELECT b_auction, b_price, a_seller, a_expires FROM bid AS B JOIN auction AS A ON "
        "B.auction = A.id WHERE b_datetime >= a_datetime AND b_datetime <= a_expires) AS j) AS r "
        "WHERE rn <= 1) AS wins");

    InProcessCluster cluster("tm-sql-q6", 16);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 25s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::map<std::int64_t, double> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("seller").as_number())] = js.at("avgp").as_number();
    }
    ASSERT_TRUE(got.count(1) && got.count(2));
    EXPECT_NEAR(got[1], 50.0, 1e-9)
        << "seller 1 last-2 closed = avg(a2=30, a3=70); a1 slid out and a3's old 60 was retracted";
    EXPECT_NEAR(got[2], 99.0, 1e-9);

    for (const auto& f : {au_path, bd_path, out_path})
        std::filesystem::remove(f);
}

// last_n_agg must not emit a fabricated/stale row when a key's window is empty.
// k=1: insert then delete the only element -> the window empties, so the key
//   must be RETRACTED (a delete), not left as a stale {k:1, null} row.
// k=2: a leading delete for a never-seen key -> emit nothing (no key fabricated
//   from a bare delete).
// k=3: a plain insert -> survives. Final upsert table: only k=3.
TEST(SqlRuntime, LastNAggEmptyWindowRetractsKey) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_lastn_empty_in.ndjson";
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_sql_lastn_empty_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path,
                {
                    R"({"k":1,"t":1,"v":10})",
                    R"({"k":1,"t":1,"v":10,"__row_kind":"delete"})",
                    R"({"k":2,"t":1,"v":5,"__row_kind":"delete"})",
                    R"({"k":3,"t":1,"v":7})",
                });

    Catalog cat;
    auto ddl =
        parse(std::string{"CREATE TABLE src (k BIGINT, t BIGINT, v BIGINT) "
                          "WITH (connector='file', format='json', path='"} +
              in_path.string() +
              "');"
              "CREATE TABLE rollavg (k BIGINT, avgv DOUBLE) "
              "WITH (connector='file', format='json', mode='upsert', primary_key='k', path='" +
              out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO rollavg SELECT k, AVG(v) OVER (PARTITION BY k ORDER BY t "
                        "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avgv FROM src");

    InProcessCluster cluster("tm-sql-lastn-empty", 4);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 20s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::map<std::int64_t, double> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("k").as_number())] = js.at("avgv").as_number();
    }
    EXPECT_EQ(got.count(1), 0u) << "k=1 emptied -> key retracted, no stale null row";
    EXPECT_EQ(got.count(2), 0u) << "k=2 leading delete -> no fabricated key";
    ASSERT_TRUE(got.count(3));
    EXPECT_NEAR(got[3], 7.0, 1e-9);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// The last-N window gate accepts only the running aggregates it recomputes
// correctly (SUM/COUNT/AVG/MIN/MAX). STRING_AGG (no separator plumbed on this
// path) must be rejected at bind rather than silently producing a run-together
// string.
TEST(SqlRuntime, LastNAggRejectsNonRunningAggregate) {
    ensure_sql_installed_once();

    Catalog cat;
    auto ddl = parse(std::string{
        "CREATE TABLE src (k BIGINT, t BIGINT, v VARCHAR) "
        "WITH (connector='file', format='json', path='/tmp/clink_sql_lastn_reject_in.ndjson');"
        "CREATE TABLE out_t (k BIGINT, s VARCHAR) "
        "WITH (connector='file', format='json', mode='upsert', primary_key='k', "
        "path='/tmp/clink_sql_lastn_reject_out')"});
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    bool threw = false;
    try {
        auto spec = compile(cat,
                            "INSERT INTO out_t SELECT k, STRING_AGG(v) OVER (PARTITION BY k "
                            "ORDER BY t ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM src");
    } catch (const std::exception&) {
        threw = true;
    }
    EXPECT_TRUE(threw) << "STRING_AGG over a last-N window must be rejected at bind";
}

// The bound output schema must follow SELECT-list order (like GROUP BY), so the
// positional INSERT type-check lines up when the aggregate is projected BEFORE
// the partition key. Previously the schema was always partition-cols-first,
// which falsely rejected this shape against a sink declared (avgv, k).
TEST(SqlRuntime, LastNAggSchemaFollowsSelectOrder) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_lastn_ord_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_lastn_ord_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"k":1,"t":1,"v":10})", R"({"k":1,"t":2,"v":20})"});

    Catalog cat;
    auto ddl =
        parse(std::string{"CREATE TABLE src (k BIGINT, t BIGINT, v BIGINT) "
                          "WITH (connector='file', format='json', path='"} +
              in_path.string() +
              "');"
              "CREATE TABLE out_t (avgv DOUBLE, k BIGINT) "
              "WITH (connector='file', format='json', mode='upsert', primary_key='k', path='" +
              out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    // Aggregate projected first, then the partition key.
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT AVG(v) OVER (PARTITION BY k ORDER BY t "
                        "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avgv, k FROM src");

    InProcessCluster cluster("tm-sql-lastn-ord", 4);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 20s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::map<std::int64_t, double> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("k").as_number())] = js.at("avgv").as_number();
    }
    ASSERT_TRUE(got.count(1));
    EXPECT_NEAR(got[1], 15.0, 1e-9) << "last-2 avg(10,20)=15";

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Nexmark-q18 shape: latest bid per (auction, bidder) - a MULTI-COLUMN
// PARTITION BY TOP-N-per-key (rn <= 1, ORDER BY time DESC) into a netting sink.
TEST(SqlRuntime, LatestBidPerAuctionBidderMultiColPartition) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q18_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q18_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // (1,2): bids at ts 100 (price 10) then 200 (price 20) -> latest 20.
    // (1,3): ts 150 price 5. (2,2): ts 120 price 7.
    write_lines(in_path,
                {
                    R"({"auction":1,"bidder":2,"price":10,"ts":100})",
                    R"({"auction":1,"bidder":2,"price":20,"ts":200})",
                    R"({"auction":1,"bidder":3,"price":5,"ts":150})",
                    R"({"auction":2,"bidder":2,"price":7,"ts":120})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE bids (auction BIGINT, bidder BIGINT, price BIGINT, "
                                 "ts BIGINT) WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE latest (auction BIGINT, bidder BIGINT, price BIGINT) "
                     "WITH (connector='changelog', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO latest SELECT auction, bidder, price FROM "
                        "(SELECT *, ROW_NUMBER() OVER (PARTITION BY auction, bidder ORDER BY ts "
                        "DESC) AS rn FROM bids) AS t WHERE rn <= 1");

    InProcessCluster cluster("tm-sql-e2e-q18", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::map<std::pair<std::int64_t, std::int64_t>, std::int64_t> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[{static_cast<std::int64_t>(js.at("auction").as_number()),
             static_cast<std::int64_t>(js.at("bidder").as_number())}] =
            static_cast<std::int64_t>(js.at("price").as_number());
    }
    EXPECT_EQ(got.size(), 3u) << "one latest bid per (auction,bidder) pair";
    EXPECT_EQ((got[{1, 2}]), 20) << "(1,2) latest is price 20 (ts 200), not 10 (retracted)";
    EXPECT_EQ((got[{1, 3}]), 5);
    EXPECT_EQ((got[{2, 2}]), 7);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Nexmark-q17 shape: per-(auction,day) bid stats - a MULTI-KEY GROUP BY mixing
// COUNT, COUNT(DISTINCT), MIN, MAX, AVG, SUM into an upsert sink keyed by the
// composite (auction, day).
TEST(SqlRuntime, PerAuctionDayStatsMultiKeyMixedAggregates) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q17_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q17_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // auction 1, day 0: prices {10,30,20}, bidders {1,2,2}. auction 2, day 0: {5}, bidder 1.
    write_lines(in_path,
                {
                    R"({"auction":1,"bidder":1,"price":10,"datetime":100})",
                    R"({"auction":1,"bidder":2,"price":30,"datetime":200})",
                    R"({"auction":1,"bidder":2,"price":20,"datetime":300})",
                    R"({"auction":2,"bidder":1,"price":5,"datetime":400})",
                });

    Catalog cat;
    auto ddl =
        parse(std::string{"CREATE TABLE bids (auction BIGINT, bidder BIGINT, price BIGINT, "
                          "datetime BIGINT) WITH (connector='file', format='json', path='"} +
              in_path.string() +
              "');"
              "CREATE TABLE stats (auction BIGINT, day BIGINT, total BIGINT, bidders BIGINT, "
              "minp BIGINT, maxp BIGINT, avgp DOUBLE, sump BIGINT) "
              "WITH (connector='file', format='json', mode='upsert', primary_key='auction,day', "
              "path='" +
              out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec =
        compile(cat,
                "INSERT INTO stats SELECT auction, day, COUNT(*) AS total, "
                "COUNT(DISTINCT bidder) AS bidders, MIN(price) AS minp, MAX(price) AS maxp, "
                "AVG(price) AS avgp, SUM(price) AS sump FROM (SELECT auction, "
                "DATE_TRUNC('day', datetime) AS day, bidder, price FROM bids) AS t "
                "GROUP BY auction, day");

    InProcessCluster cluster("tm-sql-e2e-q17", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    struct S {
        std::int64_t total, bidders, minp, maxp, sump;
        double avgp;
    };
    std::map<std::int64_t, S> got;  // keyed by auction (both rows are day 0)
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("auction").as_number())] =
            S{static_cast<std::int64_t>(js.at("total").as_number()),
              static_cast<std::int64_t>(js.at("bidders").as_number()),
              static_cast<std::int64_t>(js.at("minp").as_number()),
              static_cast<std::int64_t>(js.at("maxp").as_number()),
              static_cast<std::int64_t>(js.at("sump").as_number()),
              js.at("avgp").as_number()};
    }
    ASSERT_TRUE(got.count(1));
    EXPECT_EQ(got[1].total, 3);
    EXPECT_EQ(got[1].bidders, 2) << "distinct bidders {1,2}";
    EXPECT_EQ(got[1].minp, 10);
    EXPECT_EQ(got[1].maxp, 30);
    EXPECT_EQ(got[1].sump, 60);
    EXPECT_NEAR(got[1].avgp, 20.0, 1e-9);
    ASSERT_TRUE(got.count(2));
    EXPECT_EQ(got[2].total, 1);
    EXPECT_EQ(got[2].minp, 5);
    EXPECT_EQ(got[2].maxp, 5);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Nexmark-q5 shape: hot items - the auction(s) with the most bids per window.
// D = per-(auction,window) bid count; M = per-window MAX of those counts (an
// aggregate OVER a derived windowed aggregate); join on the window + a
// column-vs-column residual keeping rows whose count equals the window max.
//
// DISABLED: this needs RETRACTION/CHANGELOG streams, which clink does not have.
// M is a non-windowed GROUP BY (grouping by the window bounds as columns), so it
// runs in upsert mode and emits an updated max row per input record; the
// append-only INNER join then multiplies against each emission (each winner is
// produced once per auction in its window). The per-window MAX is a non-windowed
// GROUP BY, so it runs in changelog mode (update_before/update_after); the join
// consumes those retractions (retracting the prior joined pairs) and the
// changelog-netting sink resolves the +/- to the final hot items per window.
TEST(SqlRuntime, HotItemsPerWindowMaxOverWindowedAggregate) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q5_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q5_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // win[0,1000): auction 1 has 3 bids (max), auction 2 has 1.
    // win[1000,2000): auction 2 has 2 bids (max), auction 1 has 1.
    write_lines(in_path,
                {
                    R"({"auction":1,"ts":100})",
                    R"({"auction":1,"ts":200})",
                    R"({"auction":1,"ts":300})",
                    R"({"auction":2,"ts":400})",
                    R"({"auction":2,"ts":1100})",
                    R"({"auction":2,"ts":1200})",
                    R"({"auction":1,"ts":1300})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE bids (auction BIGINT, ts BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE hot (auction BIGINT, num BIGINT) "
                     "WITH (connector='changelog', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(
        cat,
        "INSERT INTO hot SELECT D_auction AS auction, D_num AS num FROM "
        "(SELECT auction, COUNT(*) AS num, window_start AS ws, window_end AS we FROM bids "
        "GROUP BY TUMBLE(ts, 1000), auction) AS D "
        "JOIN (SELECT window_start AS ws, window_end AS we, MAX(num) AS maxnum FROM "
        "(SELECT auction, COUNT(*) AS num, window_start AS window_start, window_end AS window_end "
        "FROM bids GROUP BY TUMBLE(ts, 1000), auction) AS d2 GROUP BY window_start, window_end) AS "
        "M "
        "ON D.ws = M.ws WHERE D_we = M_we AND D_num = M_maxnum");

    InProcessCluster cluster("tm-sql-e2e-q5", 16);  // nested windowed-agg + join: many operators
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 25s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::multiset<std::pair<std::int64_t, std::int64_t>> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("auction").as_number()),
                    static_cast<std::int64_t>(js.at("num").as_number()));
    }
    // One hot auction per window: auction 1 (3 bids) in win1, auction 2 (2) in win2.
    EXPECT_EQ(got.count({1, 3}), 1u) << "win[0,1000) hot auction 1 (num=3) missing";
    EXPECT_EQ(got.count({2, 2}), 1u) << "win[1000,2000) hot auction 2 (num=2) missing";
    EXPECT_EQ(got.size(), 2u) << "expected exactly one hot auction per window";

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Nexmark-q8 shape: persons + auctions created in the SAME tumbling window,
// joined on seller=id. TWO windowed-aggregate join sides, equi on the key plus a
// column-vs-column window-equality residual that matches same-window pairs and
// rejects cross-window ones.
TEST(SqlRuntime, NewUsersTwoWindowedAggregateJoinSides) {
    ensure_sql_installed_once();

    const auto p_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q8_p.ndjson";
    const auto a_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q8_a.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_q8_out.ndjson";
    for (const auto& f : {p_path, a_path, out_path})
        std::filesystem::remove(f);

    // person 1 created in win[0,1000); person 2 in win[1000,2000).
    write_lines(p_path, {R"({"id":1,"ts":100})", R"({"id":2,"ts":1100})"});
    // seller 1 has auctions in BOTH windows; seller 2's auction is in win[0,1000)
    // (a different window than person 2). So only person 1 matches (same window).
    write_lines(
        a_path,
        {R"({"seller":1,"ts":200})", R"({"seller":1,"ts":1200})", R"({"seller":2,"ts":300})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE persons (id BIGINT, ts BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     p_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE auctions (seller BIGINT, ts BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     a_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE newusers (id BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (int i = 0; i < 3; ++i)
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[i]));

    auto spec = compile(
        cat,
        "INSERT INTO newusers SELECT P_id AS id FROM "
        "(SELECT id, COUNT(*) AS pc, window_start AS ps, window_end AS pe FROM persons "
        "GROUP BY TUMBLE(ts, 1000), id) AS P "
        "JOIN (SELECT seller, COUNT(*) AS ac, window_start AS as_, window_end AS ae FROM auctions "
        "GROUP BY TUMBLE(ts, 1000), seller) AS A ON P.id = A.seller "
        "WHERE P_ps = A_as_ AND P_pe = A_ae");

    InProcessCluster cluster("tm-sql-e2e-q8", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 20s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::multiset<std::int64_t> got;
    for (const auto& l : lines)
        got.insert(static_cast<std::int64_t>(clink::config::parse(l).at("id").as_number()));
    // Only person 1 created an auction in their OWN window; person 2 did not.
    EXPECT_EQ(got.count(1), 1u) << "person 1 (same-window auction) missing";
    EXPECT_EQ(got.count(2), 0u) << "person 2 (cross-window only) wrongly present";

    for (const auto& f : {p_path, a_path, out_path})
        std::filesystem::remove(f);
}

// A derived table (here a windowed aggregate) can be a JOIN side: enrich a base
// stream with a per-key windowed total. Proves the binder accepts a subquery as a
// multi-way-join input and the runtime joins it like a base table.
TEST(SqlRuntime, WindowedAggregateAsJoinSide) {
    ensure_sql_installed_once();

    const auto ev_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_wjoin_ev.ndjson";
    const auto pr_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_wjoin_pr.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_wjoin_out.ndjson";
    for (const auto& p : {ev_path, pr_path, out_path})
        std::filesystem::remove(p);

    // events -> one TUMBLE(1000) window: k=1 total=30, k=2 total=5.
    write_lines(ev_path,
                {
                    R"({"k":1,"v":10,"ts":100})",
                    R"({"k":1,"v":20,"ts":200})",
                    R"({"k":2,"v":5,"ts":300})",
                });
    // probe rows to enrich with the per-key windowed total.
    write_lines(pr_path,
                {
                    R"({"k":1,"label":100})",
                    R"({"k":2,"label":200})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE events (k BIGINT, v BIGINT, ts BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     ev_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE probe (k BIGINT, label BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     pr_path.string() +
                     "');"
                     "CREATE TABLE enriched (label BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (int i = 0; i < 3; ++i)
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[i]));

    // The windowed aggregate M is a JOIN side, joined to base table probe on key.
    auto spec = compile(cat,
                        "INSERT INTO enriched SELECT P_label AS label, M_total AS total "
                        "FROM probe AS P JOIN (SELECT k AS mk, SUM(v) AS total FROM events "
                        "GROUP BY TUMBLE(ts, 1000), k) AS M ON P.k = M.mk");

    InProcessCluster cluster("tm-sql-e2e-wjoin", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 20s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::multimap<std::int64_t, std::int64_t> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("label").as_number()),
                    static_cast<std::int64_t>(js.at("total").as_number()));
    }
    auto contains = [&](std::int64_t label, std::int64_t total) {
        auto range = got.equal_range(label);
        for (auto it = range.first; it != range.second; ++it)
            if (it->second == total)
                return true;
        return false;
    };
    EXPECT_TRUE(contains(100, 30)) << "label=100 enriched with k=1 windowed total 30 missing";
    EXPECT_TRUE(contains(200, 5)) << "label=200 enriched with k=2 windowed total 5 missing";

    for (const auto& p : {ev_path, pr_path, out_path})
        std::filesystem::remove(p);
}

// SESSION window bounds: a session that grows by merging buckets must emit the
// earliest start and (end + gap) as window_start / window_end.
TEST(SqlRuntime, SessionWindowSelectsMergedBounds) {
    ensure_sql_installed_once();

    const auto in_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_sessbounds_in.ndjson";
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_sessbounds_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // gap=500. user=1: ts 100, 550, 1000 all chain (each within 500 of the
    // prior) into ONE session [100, 1000]; emitted window_end = 1000 + 500.
    // user=2: a lone event at ts 100 -> session [100, 100], end = 600.
    write_lines(in_path,
                {
                    R"({"user_id":1,"ts":100,"amount":1})",
                    R"({"user_id":1,"ts":550,"amount":1})",
                    R"({"user_id":1,"ts":1000,"amount":1})",
                    R"({"user_id":2,"ts":100,"amount":1})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE events (user_id BIGINT, ts BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE per_session (user_id BIGINT, cnt BIGINT, wstart BIGINT, "
                     "wend BIGINT) WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO per_session "
                        "SELECT user_id, COUNT(*) AS cnt, window_start AS wstart, "
                        "window_end AS wend FROM events GROUP BY SESSION(ts, 500), user_id");

    InProcessCluster cluster("tm-sql-e2e-sessbounds", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    struct Row {
        std::int64_t cnt, ws, we;
    };
    std::multimap<std::int64_t, Row> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("user_id").as_number()),
                    Row{static_cast<std::int64_t>(js.at("cnt").as_number()),
                        static_cast<std::int64_t>(js.at("wstart").as_number()),
                        static_cast<std::int64_t>(js.at("wend").as_number())});
    }
    auto contains = [&](std::int64_t uid, std::int64_t cnt, std::int64_t ws, std::int64_t we) {
        auto range = got.equal_range(uid);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second.cnt == cnt && it->second.ws == ws && it->second.we == we)
                return true;
        }
        return false;
    };
    // Merged session keeps the earliest start (100) across the 3 merges.
    EXPECT_TRUE(contains(1, 3, 100, 1500)) << "user=1 merged session [100,1000]+gap missing";
    EXPECT_TRUE(contains(2, 1, 100, 600)) << "user=2 lone session missing";

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// SQLOPT-4 (arm the projection hint): the same TUMBLE query over an OVER-WIDE
// source (extra columns the query never references) must produce identical
// windows. This proves the source actually drops the unreferenced columns
// (projection pushdown is now live) AND that the event-time column survives the
// narrowing so watermarks still fire (the Fix A guarantee, verified at runtime
// since a dropped ts would silently stall watermarks, not error).
TEST(SqlRuntime, ProjectionPushdownNarrowsWideEventTimeWindow) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_ppw_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_ppw_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    // Each row carries two extra columns (note, score) the query never reads.
    write_lines(in_path,
                {
                    R"({"user_id":1,"ts":100,"amount":10,"note":"a","score":999})",
                    R"({"user_id":1,"ts":200,"amount":20,"note":"b","score":888})",
                    R"({"user_id":1,"ts":300,"amount":30,"note":"c","score":777})",
                    R"({"user_id":2,"ts":400,"amount":5,"note":"d","score":666})",
                    R"({"user_id":1,"ts":1100,"amount":100,"note":"e","score":555})",
                    R"({"user_id":1,"ts":1200,"amount":200,"note":"f","score":444})",
                    R"({"user_id":2,"ts":1500,"amount":50,"note":"g","score":333})",
                });
    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE events (user_id BIGINT, ts BIGINT, amount BIGINT, "
                                 "note TEXT, score BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE per_window (user_id BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat,
                        "INSERT INTO per_window SELECT user_id, SUM(amount) AS total FROM events "
                        "GROUP BY TUMBLE(ts, 1000), user_id");

    InProcessCluster cluster("tm-sql-ppw", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    std::multimap<std::int64_t, std::int64_t> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("user_id").as_number()),
                    static_cast<std::int64_t>(js.at("total").as_number()));
    }
    auto contains = [&](std::int64_t uid, std::int64_t total) {
        auto range = got.equal_range(uid);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second == total)
                return true;
        }
        return false;
    };
    // Identical windows to the narrow-source test: the extra columns are dropped
    // and ts survives, so the event-time windows fire exactly the same.
    EXPECT_TRUE(contains(1, 60)) << "window [0,1000) user=1";
    EXPECT_TRUE(contains(1, 300)) << "window [1000,2000) user=1";
    EXPECT_TRUE(contains(2, 5)) << "window [0,1000) user=2";
    EXPECT_TRUE(contains(2, 50)) << "window [1000,2000) user=2";

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// CUMULATE produces nested slices that share a start and grow by step
// up to size, end to end. The final slice must equal a TUMBLE(size)
// over the same data.
TEST(SqlRuntime, CumulateWindowEmitsGrowingSlices) {
    ensure_sql_installed_once();

    auto run = [](const std::string& tag,
                  const std::string& group_by) -> std::multiset<std::int64_t> {
        const auto in_path =
            std::filesystem::temp_directory_path() / ("clink_sql_e2e_cumul_in_" + tag + ".ndjson");
        const auto out_path =
            std::filesystem::temp_directory_path() / ("clink_sql_e2e_cumul_out_" + tag + ".ndjson");
        std::filesystem::remove(in_path);
        std::filesystem::remove(out_path);
        // Single user; events at ts 0, 500, 1500, 2500 (all within the
        // size-3000 anchor at 0).
        write_lines(in_path,
                    {
                        R"({"user_id":1,"ts":0,"amount":10})",
                        R"({"user_id":1,"ts":500,"amount":20})",
                        R"({"user_id":1,"ts":1500,"amount":30})",
                        R"({"user_id":1,"ts":2500,"amount":40})",
                    });
        Catalog cat;
        auto ddl =
            parse(std::string{"CREATE TABLE events (user_id BIGINT, ts BIGINT, amount BIGINT) "
                              "WITH (connector='file', format='json', path='"} +
                  in_path.string() +
                  "', event_time_column='ts');"
                  "CREATE TABLE per_window (user_id BIGINT, total BIGINT) "
                  "WITH (connector='file', format='json', path='" +
                  out_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        const std::string sql =
            "INSERT INTO per_window SELECT user_id, SUM(amount) AS total "
            "FROM events GROUP BY " +
            group_by + ", user_id";
        auto spec = compile(cat, sql.c_str());
        InProcessCluster cluster("tm-sql-e2e-cumul-" + tag, 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto result = submitter.submit(spec.to_json(), {}, opts);
        EXPECT_TRUE(result.completed) << "reject: " << result.reject_message;
        EXPECT_TRUE(result.ok) << "errors: "
                               << (result.errors.empty() ? "(none)" : result.errors[0]);
        std::multiset<std::int64_t> totals;
        for (const auto& l : read_lines(out_path)) {
            auto js = clink::config::parse(l);
            totals.insert(static_cast<std::int64_t>(js.at("total").as_number()));
        }
        std::filesystem::remove(in_path);
        std::filesystem::remove(out_path);
        return totals;
    };

    // CUMULATE(ts, step=1000, size=3000): three nested slices grow as
    // [0,1000)=10+20=30, [0,2000)=+30=60, [0,3000)=+40=100.
    auto cumulate = run("c", "CUMULATE(ts, 1000, 3000)");
    EXPECT_EQ(cumulate, (std::multiset<std::int64_t>{30, 60, 100}));

    // The final (full-size) slice equals TUMBLE(3000) over the same data.
    auto tumble = run("t", "TUMBLE(ts, 3000)");
    ASSERT_EQ(tumble.size(), 1u);
    EXPECT_EQ(*tumble.rbegin(), 100);
    EXPECT_EQ(*cumulate.rbegin(), *tumble.rbegin());
}

// OVER running aggregates end to end: one append-only row per input
// with the running SUM/COUNT, LAG, and FIRST_VALUE up to that row. A
// null amount exercises 3-valued logic (ignored by SUM/COUNT).
TEST(SqlRuntime, OverRunningAggregatesAppendOnly) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_over_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_over_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"ts":1,"amount":10})",
                    R"({"user_id":1,"ts":2,"amount":20})",
                    R"({"user_id":1,"ts":3,"amount":null})",
                    R"({"user_id":1,"ts":4,"amount":40})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE evt (user_id BIGINT, ts BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE out_t (user_id BIGINT, ts BIGINT, amount BIGINT, rsum BIGINT, "
                     "rcnt BIGINT, prev BIGINT, fv BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT *, "
                        "SUM(amount) OVER (PARTITION BY user_id ORDER BY ts) AS rsum, "
                        "COUNT(amount) OVER (PARTITION BY user_id ORDER BY ts) AS rcnt, "
                        "LAG(amount, 1) OVER (PARTITION BY user_id ORDER BY ts) AS prev, "
                        "FIRST_VALUE(amount) OVER (PARTITION BY user_id ORDER BY ts) AS fv "
                        "FROM evt");

    InProcessCluster cluster("tm-sql-e2e-over", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    struct Got {
        std::int64_t rsum = 0;
        std::int64_t rcnt = 0;
        std::int64_t fv = 0;
        bool prev_null = true;
        std::int64_t prev = 0;
    };
    std::map<std::int64_t, Got> by_ts;
    auto lines = read_lines(out_path);
    EXPECT_EQ(lines.size(), 4u);  // append-only: one row per input
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        Got g;
        g.rsum = static_cast<std::int64_t>(js.at("rsum").as_number());
        g.rcnt = static_cast<std::int64_t>(js.at("rcnt").as_number());
        g.fv = static_cast<std::int64_t>(js.at("fv").as_number());
        g.prev_null = !js.contains("prev") || js.at("prev").is_null();
        g.prev = g.prev_null ? 0 : static_cast<std::int64_t>(js.at("prev").as_number());
        by_ts[static_cast<std::int64_t>(js.at("ts").as_number())] = g;
    }
    ASSERT_EQ(by_ts.size(), 4u);
    // ts=1: sum=10 cnt=1 prev=NULL fv=10
    EXPECT_EQ(by_ts[1].rsum, 10);
    EXPECT_EQ(by_ts[1].rcnt, 1);
    EXPECT_TRUE(by_ts[1].prev_null);
    EXPECT_EQ(by_ts[1].fv, 10);
    // ts=2: sum=30 cnt=2 prev=10
    EXPECT_EQ(by_ts[2].rsum, 30);
    EXPECT_EQ(by_ts[2].rcnt, 2);
    EXPECT_FALSE(by_ts[2].prev_null);
    EXPECT_EQ(by_ts[2].prev, 10);
    // ts=3 amount=NULL: sum stays 30, cnt stays 2 (null ignored); prev=20
    EXPECT_EQ(by_ts[3].rsum, 30);
    EXPECT_EQ(by_ts[3].rcnt, 2);
    EXPECT_FALSE(by_ts[3].prev_null);
    EXPECT_EQ(by_ts[3].prev, 20);
    // ts=4: sum=70 cnt=3; LAG of the null row is NULL; fv still 10
    EXPECT_EQ(by_ts[4].rsum, 70);
    EXPECT_EQ(by_ts[4].rcnt, 3);
    EXPECT_TRUE(by_ts[4].prev_null);
    EXPECT_EQ(by_ts[4].fv, 10);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Wave 7: bounded OVER frames. ROWS counts physical rows; RANGE bounds by
// the event-time value. A ts tie + gap separates the two.
TEST(SqlRuntime, OverBoundedFrameRowsVsRange) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_obf_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_obf_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    // amounts are unique, so we can key the output by amount.
    write_lines(in_path,
                {R"({"u":1,"ts":1,"amount":10})",
                 R"({"u":1,"ts":1,"amount":15})",
                 R"({"u":1,"ts":2,"amount":20})",
                 R"({"u":1,"ts":5,"amount":50})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE evt (u BIGINT, ts BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE out_t (u BIGINT, ts BIGINT, amount BIGINT, "
                     "sum_rows BIGINT, sum_range BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(
        cat,
        "INSERT INTO out_t SELECT *, "
        "SUM(amount) OVER (PARTITION BY u ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) "
        "AS sum_rows, "
        "SUM(amount) OVER (PARTITION BY u ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) "
        "AS sum_range FROM evt");

    InProcessCluster cluster("tm-sql-obf", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    std::map<std::int64_t, std::pair<std::int64_t, std::int64_t>>
        by_amt;  // amount -> (rows, range)
    auto lines = read_lines(out_path);
    EXPECT_EQ(lines.size(), 4u);
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        by_amt[static_cast<std::int64_t>(js.at("amount").as_number())] = {
            static_cast<std::int64_t>(js.at("sum_rows").as_number()),
            static_cast<std::int64_t>(js.at("sum_range").as_number())};
    }
    // ROWS 2 PRECEDING (last 3 by (ts,seq)) vs RANGE 2 PRECEDING (ts in [t-2,t]):
    EXPECT_EQ(by_amt[10], (std::pair<std::int64_t, std::int64_t>{10, 10}));
    EXPECT_EQ(by_amt[15], (std::pair<std::int64_t, std::int64_t>{25, 25}));
    EXPECT_EQ(by_amt[20], (std::pair<std::int64_t, std::int64_t>{45, 45}));
    // ts=5: ROWS=15+20+50=85; RANGE [3,5] = {50} = 50 (the gap excludes earlier rows).
    EXPECT_EQ(by_amt[50], (std::pair<std::int64_t, std::int64_t>{85, 50}));
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Regression for the review bug: ARRAY_AGG in a running OVER aggregate must
// emit growing arrays (it was silently dropped because is_running_agg_ omitted
// it). Append-only, one row per input.
TEST(SqlRuntime, ArrayAggOverRunningEmitsGrowingArrays) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_aaov_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_aaov_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path,
                {R"({"u":1,"ts":1,"amount":10})",
                 R"({"u":1,"ts":2,"amount":20})",
                 R"({"u":1,"ts":3,"amount":30})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE evt (u BIGINT, ts BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE out_t (u BIGINT, ts BIGINT, amount BIGINT, hist BIGINT ARRAY) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT *, "
                        "ARRAY_AGG(amount) OVER (PARTITION BY u ORDER BY ts) AS hist FROM evt");

    InProcessCluster cluster("tm-sql-aaov", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    std::map<std::int64_t, std::vector<double>> by_ts;
    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 3u);
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        std::vector<double> hist;
        ASSERT_TRUE(js.at("hist").is_array()) << l;
        for (const auto& e : js.at("hist").as_array())
            hist.push_back(e.as_number());
        by_ts[static_cast<std::int64_t>(js.at("ts").as_number())] = std::move(hist);
    }
    EXPECT_EQ(by_ts[1], (std::vector<double>{10}));
    EXPECT_EQ(by_ts[2], (std::vector<double>{10, 20}));
    EXPECT_EQ(by_ts[3], (std::vector<double>{10, 20, 30}));
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// IN-subquery -> semi join: keep clicks whose user is a VIP.
TEST(SqlRuntime, InSubquerySemiJoin) {
    ensure_sql_installed_once();
    const auto clicks = std::filesystem::temp_directory_path() / "clink_sql_e2e_in_clicks.ndjson";
    const auto vips = std::filesystem::temp_directory_path() / "clink_sql_e2e_in_vips.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_in_out.ndjson";
    for (const auto& p : {clicks, vips, out_path})
        std::filesystem::remove(p);
    write_lines(
        clicks,
        {R"({"user_id":1,"url":"a"})", R"({"user_id":2,"url":"b"})", R"({"user_id":3,"url":"c"})"});
    write_lines(vips, {R"({"user_id":1})", R"({"user_id":3})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE clicks (user_id BIGINT, url TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     clicks.string() +
                     "');"
                     "CREATE TABLE vips (user_id BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     vips.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, url TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='user_id')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT * FROM clicks "
                        "WHERE user_id IN (SELECT user_id FROM vips)");
    InProcessCluster cluster("tm-sql-e2e-in", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    std::set<std::int64_t> survivors;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        survivors.insert(static_cast<std::int64_t>(js.at("user_id").as_number()));
    }
    EXPECT_EQ(survivors, (std::set<std::int64_t>{1, 3}));
    for (const auto& p : {clicks, vips, out_path})
        std::filesystem::remove(p);
}

// Wave 3: multi-column IN -> composite-key semi join. Both columns of the
// tuple must match; a partial match (one column equal) does NOT qualify.
TEST(SqlRuntime, MultiColumnInSemiJoin) {
    ensure_sql_installed_once();
    const auto t_path = std::filesystem::temp_directory_path() / "clink_sql_mcin_t.ndjson";
    const auto s_path = std::filesystem::temp_directory_path() / "clink_sql_mcin_s.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_mcin_out.ndjson";
    for (const auto& p : {t_path, s_path, out_path})
        std::filesystem::remove(p);
    // (1,10) and (2,20) match s; (1,20) matches neither (partial) -> dropped.
    write_lines(t_path,
                {R"({"a":1,"b":10,"tag":100})",
                 R"({"a":2,"b":20,"tag":200})",
                 R"({"a":1,"b":20,"tag":300})"});
    write_lines(s_path, {R"({"x":1,"y":10})", R"({"x":2,"y":20})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE t (a BIGINT, b BIGINT, tag BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     t_path.string() +
                     "');"
                     "CREATE TABLE s (x BIGINT, y BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     s_path.string() +
                     "');"
                     "CREATE TABLE out_t (a BIGINT, b BIGINT, tag BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='tag')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec =
        compile(cat, "INSERT INTO out_t SELECT * FROM t WHERE (a, b) IN (SELECT x, y FROM s)");
    InProcessCluster cluster("tm-sql-mcin", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    std::set<std::int64_t> tags;
    for (const auto& l : read_lines(out_path))
        tags.insert(static_cast<std::int64_t>(clink::config::parse(l).at("tag").as_number()));
    EXPECT_EQ(tags, (std::set<std::int64_t>{100, 200}));  // not 300 (partial match)
    for (const auto& p : {t_path, s_path, out_path})
        std::filesystem::remove(p);
}

// Wave 3: multi-equality correlated EXISTS decorrelates to a composite-key
// semi join on (c.a = s.x AND c.b = s.y).
TEST(SqlRuntime, MultiEqualityExistsSemiJoin) {
    ensure_sql_installed_once();
    const auto t_path = std::filesystem::temp_directory_path() / "clink_sql_mcex_t.ndjson";
    const auto s_path = std::filesystem::temp_directory_path() / "clink_sql_mcex_s.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_mcex_out.ndjson";
    for (const auto& p : {t_path, s_path, out_path})
        std::filesystem::remove(p);
    write_lines(t_path,
                {R"({"a":1,"b":10,"tag":100})",
                 R"({"a":2,"b":20,"tag":200})",
                 R"({"a":1,"b":20,"tag":300})"});
    write_lines(s_path, {R"({"x":1,"y":10})", R"({"x":2,"y":20})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE t (a BIGINT, b BIGINT, tag BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     t_path.string() +
                     "');"
                     "CREATE TABLE s (x BIGINT, y BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     s_path.string() +
                     "');"
                     "CREATE TABLE out_t (a BIGINT, b BIGINT, tag BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='tag')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT * FROM t c WHERE EXISTS "
                        "(SELECT 1 FROM s WHERE s.x = c.a AND s.y = c.b)");
    InProcessCluster cluster("tm-sql-mcex", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    std::set<std::int64_t> tags;
    for (const auto& l : read_lines(out_path))
        tags.insert(static_cast<std::int64_t>(clink::config::parse(l).at("tag").as_number()));
    EXPECT_EQ(tags, (std::set<std::int64_t>{100, 200}));
    for (const auto& p : {t_path, s_path, out_path})
        std::filesystem::remove(p);
}

// Wave 3 (anti): multi-equality NOT EXISTS -> composite plain anti. Unlike
// NOT IN, NOT EXISTS is NULL-insensitive: a NULL correlation key (probe or
// inner) simply does not match, so the probe is INCLUDED - no poison.
TEST(SqlRuntime, MultiEqualityNotExistsAntiJoin) {
    ensure_sql_installed_once();
    const auto t_path = std::filesystem::temp_directory_path() / "clink_sql_mcne_t.ndjson";
    const auto s_path = std::filesystem::temp_directory_path() / "clink_sql_mcne_s.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_mcne_out.ndjson";
    for (const auto& p : {t_path, s_path, out_path})
        std::filesystem::remove(p);
    write_lines(t_path,
                {R"({"a":1,"b":10,"tag":100})",       // (1,10) in s -> excluded
                 R"({"a":2,"b":20,"tag":200})",       // no (2,20) in s -> included
                 R"({"a":5,"b":50,"tag":500})",       // s has (NULL,50): NULL x never matches
                 R"({"a":null,"b":10,"tag":700})"});  // NULL probe never matches -> included
    write_lines(s_path, {R"({"x":1,"y":10})", R"({"x":2,"y":99})", R"({"x":null,"y":50})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE t (a BIGINT, b BIGINT, tag BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     t_path.string() +
                     "');"
                     "CREATE TABLE s (x BIGINT, y BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     s_path.string() +
                     "');"
                     "CREATE TABLE out_t (a BIGINT, b BIGINT, tag BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='tag')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT * FROM t WHERE NOT EXISTS "
                        "(SELECT 1 FROM s WHERE s.x = t.a AND s.y = t.b)");
    InProcessCluster cluster("tm-sql-mcne", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    std::set<std::int64_t> live;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        const auto tag = static_cast<std::int64_t>(js.at("tag").as_number());
        if (js.contains("__row_kind") && js.at("__row_kind").as_string() == "delete")
            live.erase(tag);
        else
            live.insert(tag);
    }
    // 100 excluded (match); 200/500/700 included (no match; NULLs don't poison).
    EXPECT_EQ(live, (std::set<std::int64_t>{200, 500, 700}));
    for (const auto& p : {t_path, s_path, out_path})
        std::filesystem::remove(p);
}

// NOT IN with a NULL on the right -> SQL-standard UNKNOWN -> zero rows.
// This is the SQL-standard NULL-semantics correctness case.
TEST(SqlRuntime, NotInAntiJoinWithNull) {
    ensure_sql_installed_once();
    const auto clicks = std::filesystem::temp_directory_path() / "clink_sql_e2e_ni_clicks.ndjson";
    const auto vips = std::filesystem::temp_directory_path() / "clink_sql_e2e_ni_vips.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_ni_out.ndjson";
    for (const auto& p : {clicks, vips, out_path})
        std::filesystem::remove(p);
    write_lines(
        clicks,
        {R"({"user_id":1,"url":"a"})", R"({"user_id":2,"url":"b"})", R"({"user_id":3,"url":"c"})"});
    // A NULL in the NOT IN set makes every probe UNKNOWN.
    write_lines(vips, {R"({"user_id":1})", R"({"user_id":null})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE clicks (user_id BIGINT, url TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     clicks.string() +
                     "');"
                     "CREATE TABLE vips (user_id BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     vips.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, url TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='user_id')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT * FROM clicks "
                        "WHERE user_id NOT IN (SELECT user_id FROM vips)");
    InProcessCluster cluster("tm-sql-e2e-ni", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    EXPECT_TRUE(read_lines(out_path).empty()) << "NOT IN with a NULL must yield zero rows";
    for (const auto& p : {clicks, vips, out_path})
        std::filesystem::remove(p);
}

// #49 helper: run `INSERT INTO out_t SELECT * FROM t WHERE (a,b) NOT IN
// (SELECT x,y FROM s)` over the given NDJSON rows and return the surviving
// `tag` set. t has (a,b,tag); s has (x,y); both may carry JSON nulls. The
// upsert sink (primary_key=tag) reconciles the anti join's insert/delete
// changelog so the final file is the surviving set.
namespace {
std::set<std::int64_t> run_multicol_not_in(const std::string& cluster_tag,
                                           const std::vector<std::string>& t_rows,
                                           const std::vector<std::string>& s_rows) {
    const auto dir = std::filesystem::temp_directory_path();
    const auto t_path = dir / (cluster_tag + "_t.ndjson");
    const auto s_path = dir / (cluster_tag + "_s.ndjson");
    const auto out_path = dir / (cluster_tag + "_out.ndjson");
    for (const auto& p : {t_path, s_path, out_path})
        std::filesystem::remove(p);
    write_lines(t_path, t_rows);
    write_lines(s_path, s_rows);

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE t (a BIGINT, b BIGINT, tag BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     t_path.string() +
                     "');"
                     "CREATE TABLE s (x BIGINT, y BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     s_path.string() +
                     "');"
                     "CREATE TABLE out_t (a BIGINT, b BIGINT, tag BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='tag')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT * FROM t WHERE (a, b) NOT IN "
                        "(SELECT x, y FROM s)");
    InProcessCluster cluster("tm-" + cluster_tag, 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    EXPECT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    std::set<std::int64_t> tags;
    for (const auto& l : read_lines(out_path))
        tags.insert(static_cast<std::int64_t>(clink::config::parse(l).at("tag").as_number()));
    for (const auto& p : {t_path, s_path, out_path})
        std::filesystem::remove(p);
    return tags;
}
}  // namespace

// #49 baseline: multi-column NOT IN with no NULLs anywhere. Exercises the
// unchanged exact-hash fast path; only definite matches are excluded.
TEST(SqlRuntime, MultiColumnNotInNoNullBaseline) {
    ensure_sql_installed_once();
    auto tags = run_multicol_not_in(
        "sql-mcni-base",
        {R"({"a":1,"b":2,"tag":100})", R"({"a":3,"b":4,"tag":200})", R"({"a":5,"b":6,"tag":300})"},
        {R"({"x":1,"y":2})", R"({"x":5,"y":6})"});
    EXPECT_EQ(tags, (std::set<std::int64_t>{200}));  // 100, 300 are definite matches
}

// #49 headline: a NULL in one right position only poisons probes
// that agree on the OTHER position. (3,5) is UNKNOWN against (NULL,5) -> out;
// (3,7) is FALSE against (NULL,5) (pos2 differs) -> qualifies.
TEST(SqlRuntime, MultiColumnNotInNullDefeatedByMismatch) {
    ensure_sql_installed_once();
    auto tags = run_multicol_not_in("sql-mcni-mismatch",
                                    {R"({"a":3,"b":5,"tag":100})", R"({"a":3,"b":7,"tag":200})"},
                                    {R"({"x":null,"y":5})", R"({"x":1,"y":2})"});
    EXPECT_EQ(tags, (std::set<std::int64_t>{200}));
}

// #49 null-bearing PROBE vs no-null right (exercises the exact-right partial
// scan). (NULL,2) is UNKNOWN against (5,2) -> out; (NULL,9) is FALSE -> in.
TEST(SqlRuntime, MultiColumnNotInNullProbePartialMatch) {
    ensure_sql_installed_once();
    auto tags =
        run_multicol_not_in("sql-mcni-nullprobe",
                            {R"({"a":null,"b":2,"tag":400})", R"({"a":null,"b":9,"tag":500})"},
                            {R"({"x":5,"y":2})"});
    EXPECT_EQ(tags, (std::set<std::int64_t>{500}));
}

// #49 empty right set: NOT IN over an empty subquery is TRUE for EVERY probe,
// including a null-component probe (NULL NOT IN () = TRUE). Fixes the corner
// the old code got wrong by dropping null probes unconditionally.
TEST(SqlRuntime, MultiColumnNotInEmptyRightAllQualify) {
    ensure_sql_installed_once();
    auto tags = run_multicol_not_in(
        "sql-mcni-empty", {R"({"a":1,"b":2,"tag":100})", R"({"a":null,"b":2,"tag":200})"}, {});
    EXPECT_EQ(tags, (std::set<std::int64_t>{100, 200}));
}

// #49 all-NULL right tuple is a wildcard that poisons every probe (matches the
// old global-poison behaviour for this one case); exercises the retraction of
// already-emitted probes when the poisoning tuple arrives.
TEST(SqlRuntime, MultiColumnNotInAllNullRightPoisonsAll) {
    ensure_sql_installed_once();
    auto tags = run_multicol_not_in("sql-mcni-allnull",
                                    {R"({"a":1,"b":2,"tag":100})", R"({"a":3,"b":4,"tag":200})"},
                                    {R"({"x":null,"y":null})"});
    EXPECT_TRUE(tags.empty());
}

// #49 single-column NOT IN over an empty subquery: the null probe now
// qualifies (NULL NOT IN () = TRUE), the one assertion that moves from the
// old behaviour. Confirms the per-position model fixes k=1 too.
TEST(SqlRuntime, SingleColumnNotInEmptyRightNullProbe) {
    ensure_sql_installed_once();
    const auto dir = std::filesystem::temp_directory_path();
    const auto clicks = dir / "clink_sql_scni_clicks.ndjson";
    const auto vips = dir / "clink_sql_scni_vips.ndjson";
    const auto out_path = dir / "clink_sql_scni_out.ndjson";
    for (const auto& p : {clicks, vips, out_path})
        std::filesystem::remove(p);
    write_lines(clicks, {R"({"user_id":1,"tag":100})", R"({"user_id":null,"tag":200})"});
    write_lines(vips, {});  // empty NOT IN set

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE clicks (user_id BIGINT, tag BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     clicks.string() +
                     "');"
                     "CREATE TABLE vips (user_id BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     vips.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, tag BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='tag')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT * FROM clicks "
                        "WHERE user_id NOT IN (SELECT user_id FROM vips)");
    InProcessCluster cluster("tm-sql-scni", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    std::set<std::int64_t> tags;
    for (const auto& l : read_lines(out_path))
        tags.insert(static_cast<std::int64_t>(clink::config::parse(l).at("tag").as_number()));
    EXPECT_EQ(tags, (std::set<std::int64_t>{100, 200}));  // null probe qualifies over empty R
    for (const auto& p : {clicks, vips, out_path})
        std::filesystem::remove(p);
}

// #49 one probe poisoned by BOTH an exact (no-null) right and a null-bearing
// right: it must be retracted exactly once (the `emitted` guard). (5,NULL) is
// poisoned by exact (5,2) and by null-bearing (NULL,2); (9,9) matches neither.
TEST(SqlRuntime, MultiColumnNotInMixedExactAndNullPoison) {
    ensure_sql_installed_once();
    auto tags = run_multicol_not_in("sql-mcni-mixed",
                                    {R"({"a":5,"b":null,"tag":600})", R"({"a":9,"b":9,"tag":700})"},
                                    {R"({"x":5,"y":2})", R"({"x":null,"y":2})"});
    EXPECT_EQ(tags, (std::set<std::int64_t>{700}));
}

// #49 float key components: locks in the serialize->parse->serialize round-trip
// idempotence the masked-key comparison relies on (a null-bearing probe is
// compared against a no-null right via masked_key_from_exact_). (NULL,0.1) is
// poisoned by (5,0.1); (NULL,0.2) is not.
TEST(SqlRuntime, MultiColumnNotInFloatComponentMatch) {
    ensure_sql_installed_once();
    const auto dir = std::filesystem::temp_directory_path();
    const auto t_path = dir / "clink_sql_mcnif_t.ndjson";
    const auto s_path = dir / "clink_sql_mcnif_s.ndjson";
    const auto out_path = dir / "clink_sql_mcnif_out.ndjson";
    for (const auto& p : {t_path, s_path, out_path})
        std::filesystem::remove(p);
    write_lines(t_path, {R"({"a":null,"b":0.1,"tag":800})", R"({"a":null,"b":0.2,"tag":900})"});
    write_lines(s_path, {R"({"x":5,"y":0.1})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE t (a DOUBLE, b DOUBLE, tag BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     t_path.string() +
                     "');"
                     "CREATE TABLE s (x DOUBLE, y DOUBLE) "
                     "WITH (connector='file', format='json', path='" +
                     s_path.string() +
                     "');"
                     "CREATE TABLE out_t (a DOUBLE, b DOUBLE, tag BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='tag')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT * FROM t WHERE (a, b) NOT IN "
                        "(SELECT x, y FROM s)");
    InProcessCluster cluster("tm-sql-mcnif", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    std::set<std::int64_t> tags;
    for (const auto& l : read_lines(out_path))
        tags.insert(static_cast<std::int64_t>(clink::config::parse(l).at("tag").as_number()));
    EXPECT_EQ(tags, (std::set<std::int64_t>{900}));  // 800=(NULL,0.1) poisoned by (5,0.1)
    for (const auto& p : {t_path, s_path, out_path})
        std::filesystem::remove(p);
}

// --- #56: exact DECIMAL arithmetic end-to-end -------------------------------

// Headline: SUM over a DECIMAL file column, exact across a GROUP BY shuffle.
// Proves (1) schema-aware source ingestion (price born exact), (2) the dec
// value survives the keyBy wire shuffle, (3) exact decimal SUM (0.10+0.20=0.30,
// not 0.30000000000000004), (4) clean sink output with no sentinel byte.
TEST(SqlRuntime, DecimalGroupBySumExactAndClean) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_decsum_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_decsum_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path,
                {R"({"k":1,"price":0.10})",
                 R"({"k":1,"price":0.20})",
                 R"({"k":2,"price":10.01})",
                 R"({"k":2,"price":0.02})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (k BIGINT, price DECIMAL(18,2)) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (k BIGINT, total DECIMAL(18,2)) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='k')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec =
        compile(cat, "INSERT INTO out_t SELECT k, SUM(price) AS total FROM orders GROUP BY k");
    InProcessCluster cluster("tm-sql-decsum", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    std::map<std::int64_t, std::string> raw_total;  // exact textual rendering per key
    for (const auto& l : read_lines(out_path)) {
        EXPECT_EQ(l.find('\x01'), std::string::npos)
            << "decimal sentinel leaked into output: " << l;
        auto js = clink::config::parse(l);
        // Re-extract the exact text by scanning the raw line (parse->double
        // would drop the trailing zero); assert the canonical decimal text.
        auto k = static_cast<std::int64_t>(js.at("k").as_number());
        raw_total[k] = l;
    }
    ASSERT_EQ(raw_total.size(), 2U);
    EXPECT_NE(raw_total[1].find("\"total\":0.30"), std::string::npos) << raw_total[1];
    EXPECT_EQ(raw_total[1].find("0.300"), std::string::npos) << "over-precision: " << raw_total[1];
    EXPECT_NE(raw_total[2].find("\"total\":10.03"), std::string::npos) << raw_total[2];
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Exact decimal arithmetic in projection: 0.1 + 0.2 = 0.3 (not 0.30000...4),
// and an exact money multiply.
TEST(SqlRuntime, DecimalArithmeticExactInProjection) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_decar_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_decar_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"id":1,"qty":3})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE t (id BIGINT, qty BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (a DECIMAL(10,2), b DECIMAL(10,2)) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    // 0.1 + 0.2 -> exactly 0.3; qty(3) * 1.50 -> exactly 4.50.
    auto spec = compile(cat, "INSERT INTO out_t SELECT 0.1 + 0.2 AS a, qty * 1.50 AS b FROM t");
    InProcessCluster cluster("tm-sql-decar", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1U);
    EXPECT_EQ(lines[0].find('\x01'), std::string::npos) << lines[0];
    EXPECT_NE(lines[0].find("\"a\":0.3"), std::string::npos) << lines[0];
    EXPECT_EQ(lines[0].find("0.30000"), std::string::npos) << "not exact: " << lines[0];
    EXPECT_NE(lines[0].find("\"b\":4.50"), std::string::npos) << lines[0];
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// CAST to DECIMAL with HALF_UP division rounding, and overflow past the
// declared precision yielding NULL.
TEST(SqlRuntime, DecimalDivisionAndOverflow) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_decdiv_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_decdiv_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"id":1,"amount":10})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE t (id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (q DECIMAL(10,2), o DECIMAL(5,2)) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    // 10 / 3 at scale 2 (clink div floor 6, then cast to (10,2)) -> 3.33;
    // CAST(12345.67 AS DECIMAL(5,2)) overflows 5 digits -> NULL.
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT CAST(amount AS DECIMAL(10,2)) / 3 AS q, "
                        "CAST(12345.67 AS DECIMAL(5,2)) AS o FROM t");
    InProcessCluster cluster("tm-sql-decdiv", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1U);
    auto js = clink::config::parse(lines[0]);
    EXPECT_DOUBLE_EQ(js.at("q").as_number(), 3.33);  // 3.3333.. HALF_UP at scale 2
    EXPECT_TRUE(js.at("o").is_null()) << "overflow past DECIMAL(5,2) must be NULL: " << lines[0];
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// SESSION-window MIN/MAX over DECIMAL across a session MERGE: a bridging event
// joins two sessions whose decimal extremes order differently lexically vs
// numerically (9.00 / 10.00). The merge must use decimal-value order, not a
// lexical sentinel-string compare.
TEST(SqlRuntime, DecimalSessionMinMaxMergeIsNumeric) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_decsess_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_decsess_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    // gap=500. (ts=100) and (ts=900) form two sessions; (ts=500) bridges them
    // (within 500 of both) -> merge into one session {9.00, 9.50, 10.00}.
    write_lines(in_path,
                {R"({"k":1,"ts":100,"amount":9.00})",
                 R"({"k":1,"ts":900,"amount":10.00})",
                 R"({"k":1,"ts":500,"amount":9.50})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE events (k BIGINT, ts BIGINT, amount DECIMAL(6,2)) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts', watermark_lag_ms='0');"
                     "CREATE TABLE out_t (k BIGINT, lo DECIMAL(6,2), hi DECIMAL(6,2)) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT k, MIN(amount) AS lo, MAX(amount) AS hi "
                        "FROM events GROUP BY SESSION(ts, 500), k");
    InProcessCluster cluster("tm-sql-decsess", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1U);
    auto js = clink::config::parse(lines[0]);
    EXPECT_DOUBLE_EQ(js.at("lo").as_number(), 9.0);   // numeric min, not lexical "10.00"
    EXPECT_DOUBLE_EQ(js.at("hi").as_number(), 10.0);  // numeric max, not lexical "9.50"
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Correlated EXISTS -> semi join (decorrelated on the equality).
TEST(SqlRuntime, ExistsCorrelatedSemiJoin) {
    ensure_sql_installed_once();
    const auto clicks = std::filesystem::temp_directory_path() / "clink_sql_e2e_ex_clicks.ndjson";
    const auto vips = std::filesystem::temp_directory_path() / "clink_sql_e2e_ex_vips.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_ex_out.ndjson";
    for (const auto& p : {clicks, vips, out_path})
        std::filesystem::remove(p);
    write_lines(
        clicks,
        {R"({"user_id":1,"url":"a"})", R"({"user_id":2,"url":"b"})", R"({"user_id":3,"url":"c"})"});
    write_lines(vips, {R"({"user_id":2})", R"({"user_id":3})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE clicks (user_id BIGINT, url TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     clicks.string() +
                     "');"
                     "CREATE TABLE vips (user_id BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     vips.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, url TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='user_id')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT * FROM clicks c "
                        "WHERE EXISTS (SELECT 1 FROM vips v WHERE v.user_id = c.user_id)");
    InProcessCluster cluster("tm-sql-e2e-ex", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    std::set<std::int64_t> survivors;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        survivors.insert(static_cast<std::int64_t>(js.at("user_id").as_number()));
    }
    EXPECT_EQ(survivors, (std::set<std::int64_t>{2, 3}));
    for (const auto& p : {clicks, vips, out_path})
        std::filesystem::remove(p);
}

// Uncorrelated scalar subquery -> broadcast filter against the settled
// aggregate: keep orders above the overall average.
TEST(SqlRuntime, ScalarSubqueryGreaterThanAvg) {
    ensure_sql_installed_once();
    const auto orders = std::filesystem::temp_directory_path() / "clink_sql_e2e_sc_orders.ndjson";
    const auto refs = std::filesystem::temp_directory_path() / "clink_sql_e2e_sc_refs.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_sc_out.ndjson";
    for (const auto& p : {orders, refs, out_path})
        std::filesystem::remove(p);
    write_lines(orders,
                {R"({"id":1,"amount":10})",
                 R"({"id":2,"amount":20})",
                 R"({"id":3,"amount":30})",
                 R"({"id":4,"amount":40})"});
    write_lines(refs,
                {R"({"amount":10})",
                 R"({"amount":20})",
                 R"({"amount":30})",
                 R"({"amount":40})"});  // avg = 25

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     orders.string() +
                     "');"
                     "CREATE TABLE refs (amount BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     refs.string() +
                     "');"
                     "CREATE TABLE out_t (id BIGINT, amount BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT * FROM orders "
                        "WHERE amount > (SELECT avg(amount) FROM refs)");
    InProcessCluster cluster("tm-sql-e2e-sc", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    std::set<std::int64_t> survivors;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        survivors.insert(static_cast<std::int64_t>(js.at("id").as_number()));
    }
    EXPECT_EQ(survivors, (std::set<std::int64_t>{3, 4}));  // amounts 30, 40 > avg 25
    for (const auto& p : {orders, refs, out_path})
        std::filesystem::remove(p);
}

// #55: scalar subquery as a SELECT item -> its single value is appended
// to every outer row (append-only against the final scalar).
TEST(SqlRuntime, ScalarSubqueryInSelectAppendsToEveryRow) {
    ensure_sql_installed_once();
    const auto orders = std::filesystem::temp_directory_path() / "clink_sql_e2e_scp_orders.ndjson";
    const auto refs = std::filesystem::temp_directory_path() / "clink_sql_e2e_scp_refs.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_scp_out.ndjson";
    for (const auto& p : {orders, refs, out_path})
        std::filesystem::remove(p);
    write_lines(orders,
                {R"({"id":1,"amount":10})",
                 R"({"id":2,"amount":20})",
                 R"({"id":3,"amount":30})",
                 R"({"id":4,"amount":40})"});
    write_lines(refs,
                {R"({"amount":10})",
                 R"({"amount":20})",
                 R"({"amount":30})",
                 R"({"amount":40})"});  // max = 40

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     orders.string() +
                     "');"
                     "CREATE TABLE refs (amount BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     refs.string() +
                     "');"
                     "CREATE TABLE out_t (id BIGINT, m BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(
        cat, "INSERT INTO out_t SELECT id, (SELECT max(amount) FROM refs) AS m FROM orders");
    InProcessCluster cluster("tm-sql-e2e-scp", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    std::map<std::int64_t, std::int64_t> by_id;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        by_id[static_cast<std::int64_t>(js.at("id").as_number())] =
            static_cast<std::int64_t>(js.at("m").as_number());
    }
    // Every outer row carries the scalar value (max over refs = 40).
    EXPECT_EQ(by_id, (std::map<std::int64_t, std::int64_t>{{1, 40}, {2, 40}, {3, 40}, {4, 40}}));
    for (const auto& p : {orders, refs, out_path})
        std::filesystem::remove(p);
}

// #55: an empty scalar subquery yields a NULL appended column on every
// outer row (no insert-like scalar value ever arrives).
TEST(SqlRuntime, ScalarSubqueryInSelectEmptyYieldsNull) {
    ensure_sql_installed_once();
    const auto orders = std::filesystem::temp_directory_path() / "clink_sql_e2e_scpn_orders.ndjson";
    const auto refs = std::filesystem::temp_directory_path() / "clink_sql_e2e_scpn_refs.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_scpn_out.ndjson";
    for (const auto& p : {orders, refs, out_path})
        std::filesystem::remove(p);
    write_lines(orders, {R"({"id":1,"amount":10})", R"({"id":2,"amount":20})"});
    write_lines(refs, {});  // empty -> max over no rows is NULL

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     orders.string() +
                     "');"
                     "CREATE TABLE refs (amount BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     refs.string() +
                     "');"
                     "CREATE TABLE out_t (id BIGINT, m BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(
        cat, "INSERT INTO out_t SELECT id, (SELECT max(amount) FROM refs) AS m FROM orders");
    InProcessCluster cluster("tm-sql-e2e-scpn", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    std::set<std::int64_t> ids;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        ids.insert(static_cast<std::int64_t>(js.at("id").as_number()));
        EXPECT_TRUE(!js.contains("m") || js.at("m").is_null());  // empty subquery -> NULL
    }
    EXPECT_EQ(ids, (std::set<std::int64_t>{1, 2}));  // every row passes through
    for (const auto& p : {orders, refs, out_path})
        std::filesystem::remove(p);
}

TEST(SqlRuntime, SessionWindowCollapsesBurstsPerUser) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_session_in.ndjson";
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_session_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // user=1: two sessions (gap > 500ms apart): {100,200,300} and {1000}.
    // user=2: one session: {400,500}.
    write_lines(in_path,
                {
                    R"({"user_id":1,"ts":100})",
                    R"({"user_id":1,"ts":200})",
                    R"({"user_id":1,"ts":300})",
                    R"({"user_id":2,"ts":400})",
                    R"({"user_id":2,"ts":500})",
                    R"({"user_id":1,"ts":1000})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE events (user_id BIGINT, ts BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE per_session (user_id BIGINT, hits BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO per_session "
                        "SELECT user_id, COUNT(*) AS hits FROM events "
                        "GROUP BY SESSION(ts, 500), user_id");

    InProcessCluster cluster("tm-sql-e2e-session", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    std::multimap<std::int64_t, std::int64_t> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("user_id").as_number()),
                    static_cast<std::int64_t>(js.at("hits").as_number()));
    }
    // user=1: sessions of size 3 and 1. user=2: session of size 2.
    auto contains = [&](std::int64_t uid, std::int64_t hits) {
        auto range = got.equal_range(uid);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second == hits)
                return true;
        }
        return false;
    };
    EXPECT_TRUE(contains(1, 3)) << "user=1 burst session of 3 missing";
    EXPECT_TRUE(contains(1, 1)) << "user=1 late single-event session missing";
    EXPECT_TRUE(contains(2, 2)) << "user=2 session of 2 missing";

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, IntervalJoinMatchesWithinWindow) {
    ensure_sql_installed_once();

    const auto a_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_ij_a.ndjson";
    const auto b_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_ij_b.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_ij_out.ndjson";
    std::filesystem::remove(a_path);
    std::filesystem::remove(b_path);
    std::filesystem::remove(out_path);

    // clicks: user=1 at ts=100, user=2 at ts=2000
    // orders: user=1 at ts=150 (within +200 of click) -> match
    //         user=2 at ts=4000 (too late) -> no match
    write_lines(a_path,
                {
                    R"({"user_id":1,"click_ts":100,"page":"home"})",
                    R"({"user_id":2,"click_ts":2000,"page":"docs"})",
                });
    write_lines(b_path,
                {
                    R"({"user_id":1,"order_ts":150,"sku":"x"})",
                    R"({"user_id":2,"order_ts":4000,"sku":"y"})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE clicks (user_id BIGINT, click_ts BIGINT, page TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     a_path.string() +
                     "', event_time_column='click_ts');"
                     "CREATE TABLE orders (user_id BIGINT, order_ts BIGINT, sku TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     b_path.string() +
                     "', event_time_column='order_ts');"
                     "CREATE TABLE joined (c_user_id BIGINT, c_click_ts BIGINT, c_page TEXT, "
                     "                     o_user_id BIGINT, o_order_ts BIGINT, o_sku TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[2]));

    auto spec = compile(cat,
                        "INSERT INTO joined "
                        "SELECT * FROM clicks c JOIN orders o "
                        "ON c.user_id = o.user_id AND "
                        "   c.click_ts BETWEEN o.order_ts - 50 AND o.order_ts + 200");

    InProcessCluster cluster("tm-sql-e2e-ij", 12);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1u) << "expected exactly one match";
    auto js = clink::config::parse(lines[0]);
    EXPECT_EQ(static_cast<std::int64_t>(js.at("c_user_id").as_number()), 1);
    EXPECT_EQ(static_cast<std::int64_t>(js.at("o_user_id").as_number()), 1);
    EXPECT_EQ(js.at("c_page").as_string(), "home");
    EXPECT_EQ(js.at("o_sku").as_string(), "x");

    std::filesystem::remove(a_path);
    std::filesystem::remove(b_path);
    std::filesystem::remove(out_path);
}

// An INNER interval join rides the checkpointed KeyedState path and auto-activates
// the async/disaggregated path on a deferring backend. Each side's per-key buffer
// was an in-memory map a restore would lose; both now live in per-join-key
// KeyedState slots (ijL / ijR), and a left arrival reads the right buffer + emits
// matches + appends to the left buffer (and symmetrically) - the EquiJoin RMW
// shape plus the time predicate. OUTER interval joins keep the sync path (their
// eviction-time null-padding needs per-key timers, a follow-on). Driven
// cluster-free through the interval_join_row Dag builder (INNER matches emit on
// arrival, so no watermark is needed) on a RemoteReadBackend so remote_loads()
// proves the per-key state went through the deferring tier. clicks(u1@100,
// u2@2000) JOIN orders(u1@150, u2@4000) ON u AND click BETWEEN order-50 AND
// order+200: only (u1 click, u1 order) matches.
TEST(SqlRuntime, IntervalJoinInnerRidesRemoteReadPath) {
    ensure_sql_installed_once();
    const auto* builder = cluster::DagBuilderRegistry::default_instance().find("interval_join_row");
    ASSERT_NE(builder, nullptr) << "interval_join_row Dag builder not registered";

    auto click = [](std::int64_t u, std::int64_t ts, const char* page) {
        Row r;
        r.values["user_id"] = clink::config::JsonValue{static_cast<double>(u)};
        r.values["click_ts"] = clink::config::JsonValue{static_cast<double>(ts)};
        r.values["page"] = clink::config::JsonValue{std::string{page}};
        return Record<Row>{std::move(r)};
    };
    auto order = [](std::int64_t u, std::int64_t ts, const char* sku) {
        Row r;
        r.values["user_id"] = clink::config::JsonValue{static_cast<double>(u)};
        r.values["order_ts"] = clink::config::JsonValue{static_cast<double>(ts)};
        r.values["sku"] = clink::config::JsonValue{std::string{sku}};
        return Record<Row>{std::move(r)};
    };
    const std::vector<Record<Row>> left = {click(1, 100, "home"), click(2, 2000, "docs")};
    const std::vector<Record<Row>> right = {order(1, 150, "x"), order(2, 4000, "y")};

    auto run = [&](std::shared_ptr<StateBackend> backend) {
        Dag dag;
        auto hl = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(left));
        auto hr = dag.add_source<Row>(std::make_shared<VectorSource<Row>>(right));
        clink::plugin::BuildContext ctx;
        ctx.params["left_key_column"] = "user_id";
        ctx.params["right_key_column"] = "user_id";
        ctx.params["left_ts_column"] = "click_ts";
        ctx.params["right_ts_column"] = "order_ts";
        ctx.params["left_alias"] = "c";
        ctx.params["right_alias"] = "o";
        ctx.params["lower_offset_ms"] = "-50";
        ctx.params["upper_offset_ms"] = "200";
        ctx.params["join_type"] = "inner";
        ctx.params["left_columns"] = "user_id,click_ts,page";
        ctx.params["right_columns"] = "user_id,order_ts,sku";
        std::vector<std::any> upstream = {std::any{hl}, std::any{hr}};
        auto built = (*builder)(dag, upstream, ctx);
        auto h_op = std::any_cast<StageHandle<Row>>(built.main_handle);
        auto sink = std::make_shared<CollectingSink<Row>>();
        dag.add_sink<Row>(h_op, sink);
        JobConfig cfg;
        cfg.state_backend = std::move(backend);
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();
        std::set<std::pair<std::int64_t, std::string>> pairs;  // (c_user_id, o_sku)
        for (const auto& rec : sink->collected_records()) {
            const Row& r = rec.value();
            pairs.emplace(static_cast<std::int64_t>(r.values.at("c_user_id").as_number()),
                          r.values.at("o_sku").as_string());
        }
        return pairs;
    };

    const auto sync_pairs = run(std::make_shared<InMemoryStateBackend>());
    auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                   /*io_threads=*/1,
                                                   /*hot_max_bytes=*/0);
    const auto async_pairs = run(rrb);

    const std::set<std::pair<std::int64_t, std::string>> want = {{1, "x"}};
    EXPECT_EQ(sync_pairs, want) << "sync interval join matches";
    EXPECT_EQ(async_pairs, sync_pairs)
        << "async interval join must produce the same matches as the sync path";
    EXPECT_GT(rrb->remote_loads(), 0u)
        << "interval join did not route its per-key state through the deferring backend";
}

// Emits a fixed row vector as one batch then a closing watermark, so a
// cluster-free co-op test can drive watermark-triggered eviction / null-padding.
class TsWatermarkSource final : public Source<Row> {
public:
    TsWatermarkSource(std::vector<Record<Row>> rows, std::int64_t closing_wm)
        : rows_(std::move(rows)), closing_wm_(closing_wm) {}
    bool produce(Emitter<Row>& out) override {
        if (this->cancelled() || done_) {
            return false;
        }
        Batch<Row> b;
        for (const auto& r : rows_) {
            b.push(r);
        }
        out.emit_data(std::move(b));
        out.emit_watermark(Watermark{EventTime{closing_wm_}});
        done_ = true;
        return false;
    }
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }
    std::string name() const override { return "ts_wm_src"; }

private:
    std::vector<Record<Row>> rows_;
    std::int64_t closing_wm_;
    bool done_ = false;
};

// OUTER interval joins ride the async/disaggregated path too: matched rows join,
// and an unmatched kept-side row is null-padded when its window is EVICTED at the
// watermark, via a per-key eviction timer (the async twin of the sync watermark
// scan). u1 click joins u1 order; u2 click is unmatched (kept by LEFT/FULL); u3
// order is unmatched (kept only by FULL). Verified for LEFT and FULL OUTER
// against the sync in-memory path, with the exact expected rows, and
// remote_loads > 0 to prove the per-key state rode the deferring tier.
TEST(SqlRuntime, OuterIntervalJoinAsyncMatchesSyncPath) {
    ensure_sql_installed_once();
    const auto* builder = cluster::DagBuilderRegistry::default_instance().find("interval_join_row");
    ASSERT_NE(builder, nullptr) << "interval_join_row Dag builder not registered";

    auto click = [](std::int64_t u, std::int64_t ts, const char* page) {
        Row r;
        r.values["user_id"] = clink::config::JsonValue{static_cast<double>(u)};
        r.values["click_ts"] = clink::config::JsonValue{static_cast<double>(ts)};
        r.values["page"] = clink::config::JsonValue{std::string{page}};
        return Record<Row>{std::move(r)};
    };
    auto order = [](std::int64_t u, std::int64_t ts, const char* sku) {
        Row r;
        r.values["user_id"] = clink::config::JsonValue{static_cast<double>(u)};
        r.values["order_ts"] = clink::config::JsonValue{static_cast<double>(ts)};
        r.values["sku"] = clink::config::JsonValue{std::string{sku}};
        return Record<Row>{std::move(r)};
    };
    const std::vector<Record<Row>> left = {click(1, 100, "home"), click(2, 2000, "docs")};
    const std::vector<Record<Row>> right = {order(1, 150, "x"), order(3, 5000, "z")};

    auto run = [&](const char* join_type, std::shared_ptr<StateBackend> backend) {
        Dag dag;
        auto hl = dag.add_source<Row>(std::make_shared<TsWatermarkSource>(left, 1000000));
        auto hr = dag.add_source<Row>(std::make_shared<TsWatermarkSource>(right, 1000000));
        clink::plugin::BuildContext ctx;
        ctx.params["left_key_column"] = "user_id";
        ctx.params["right_key_column"] = "user_id";
        ctx.params["left_ts_column"] = "click_ts";
        ctx.params["right_ts_column"] = "order_ts";
        ctx.params["left_alias"] = "c";
        ctx.params["right_alias"] = "o";
        ctx.params["lower_offset_ms"] = "-50";
        ctx.params["upper_offset_ms"] = "200";
        ctx.params["join_type"] = join_type;
        ctx.params["left_columns"] = "user_id,click_ts,page";
        ctx.params["right_columns"] = "user_id,order_ts,sku";
        std::vector<std::any> upstream = {std::any{hl}, std::any{hr}};
        auto built = (*builder)(dag, upstream, ctx);
        auto h_op = std::any_cast<StageHandle<Row>>(built.main_handle);
        auto sink = std::make_shared<CollectingSink<Row>>();
        dag.add_sink<Row>(h_op, sink);
        JobConfig cfg;
        cfg.state_backend = std::move(backend);
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();
        // Each output row as (c_user_id or -1, o_user_id or -1); -1 = null/absent.
        auto rd = [](const Row& r, const char* c) -> std::int64_t {
            auto it = r.values.find(c);
            if (it == r.values.end() || it->second.is_null()) {
                return -1;
            }
            return static_cast<std::int64_t>(it->second.as_number());
        };
        std::set<std::pair<std::int64_t, std::int64_t>> rows;
        for (const auto& rec : sink->collected_records()) {
            rows.emplace(rd(rec.value(), "c_user_id"), rd(rec.value(), "o_user_id"));
        }
        return rows;
    };

    using P = std::pair<std::int64_t, std::int64_t>;
    // LEFT OUTER: u1 join + u2 left null-pad (u3 order unmatched but RIGHT not kept).
    {
        const auto sync_rows = run("left_outer", std::make_shared<InMemoryStateBackend>());
        auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                       /*io_threads=*/1,
                                                       /*hot_max_bytes=*/0);
        const auto async_rows = run("left_outer", rrb);
        const std::set<P> want = {P{1, 1}, P{2, -1}};
        EXPECT_EQ(sync_rows, want) << "sync LEFT OUTER interval join";
        EXPECT_EQ(async_rows, sync_rows) << "async LEFT OUTER must match the sync path";
        EXPECT_GT(rrb->remote_loads(), 0u) << "LEFT OUTER did not ride the deferring backend";
    }
    // FULL OUTER: u1 join + u2 left null-pad + u3 right null-pad.
    {
        const auto sync_rows = run("full_outer", std::make_shared<InMemoryStateBackend>());
        auto rrb = std::make_shared<RemoteReadBackend>(std::make_shared<InMemoryRemotePool>(),
                                                       /*io_threads=*/1,
                                                       /*hot_max_bytes=*/0);
        const auto async_rows = run("full_outer", rrb);
        const std::set<P> want = {P{1, 1}, P{2, -1}, P{-1, 3}};
        EXPECT_EQ(sync_rows, want) << "sync FULL OUTER interval join";
        EXPECT_EQ(async_rows, sync_rows) << "async FULL OUTER must match the sync path";
        EXPECT_GT(rrb->remote_loads(), 0u) << "FULL OUTER did not ride the deferring backend";
    }
}

// SQLOPT-1: LEFT OUTER interval join. An unmatched left (click) row is
// null-padded at watermark eviction; matched rows are unaffected.
TEST(SqlRuntime, LeftOuterIntervalJoinNullPadsUnmatchedLeft) {
    ensure_sql_installed_once();

    const auto a_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_lij_a.ndjson";
    const auto b_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_lij_b.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_lij_out.ndjson";
    std::filesystem::remove(a_path);
    std::filesystem::remove(b_path);
    std::filesystem::remove(out_path);

    // click user=1 @100 matches order user=1 @150; click user=2 @2000 has no
    // order in [1800,2050] so it stays unmatched -> LEFT join null-pads it.
    write_lines(a_path,
                {
                    R"({"user_id":1,"click_ts":100,"page":"home"})",
                    R"({"user_id":2,"click_ts":2000,"page":"docs"})",
                });
    write_lines(b_path,
                {
                    R"({"user_id":1,"order_ts":150,"sku":"x"})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE clicks (user_id BIGINT, click_ts BIGINT, page TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     a_path.string() +
                     "', event_time_column='click_ts');"
                     "CREATE TABLE orders (user_id BIGINT, order_ts BIGINT, sku TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     b_path.string() +
                     "', event_time_column='order_ts');"
                     "CREATE TABLE joined (c_user_id BIGINT, c_click_ts BIGINT, c_page TEXT, "
                     "                     o_user_id BIGINT, o_order_ts BIGINT, o_sku TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[2]));

    auto spec = compile(cat,
                        "INSERT INTO joined "
                        "SELECT * FROM clicks c LEFT JOIN orders o "
                        "ON c.user_id = o.user_id AND "
                        "   c.click_ts BETWEEN o.order_ts - 50 AND o.order_ts + 200");

    InProcessCluster cluster("tm-sql-e2e-lij", 12);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 2u) << "expected one match + one null-padded left row";
    bool saw_match = false;
    bool saw_null_pad = false;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        const auto cu = static_cast<std::int64_t>(js.at("c_user_id").as_number());
        if (cu == 1) {
            saw_match = true;
            EXPECT_EQ(static_cast<std::int64_t>(js.at("o_user_id").as_number()), 1);
            EXPECT_EQ(js.at("o_sku").as_string(), "x");
        } else if (cu == 2) {
            saw_null_pad = true;
            EXPECT_EQ(js.at("c_page").as_string(), "docs");
            EXPECT_TRUE(js.at("o_user_id").is_null()) << "unmatched right must be null";
            EXPECT_TRUE(js.at("o_sku").is_null());
        }
    }
    EXPECT_TRUE(saw_match) << "matched user=1 row missing";
    EXPECT_TRUE(saw_null_pad) << "null-padded user=2 row missing";

    std::filesystem::remove(a_path);
    std::filesystem::remove(b_path);
    std::filesystem::remove(out_path);
}

// SQLOPT-1: FULL OUTER interval join emits the matched pair plus a null-padded
// row for the unmatched left AND the unmatched right.
TEST(SqlRuntime, FullOuterIntervalJoinNullPadsBothSides) {
    ensure_sql_installed_once();

    const auto a_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_fij_a.ndjson";
    const auto b_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_fij_b.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_fij_out.ndjson";
    std::filesystem::remove(a_path);
    std::filesystem::remove(b_path);
    std::filesystem::remove(out_path);

    // user=1 click@100 + order@150 match; user=2 click@2000 unmatched (left);
    // user=2 order@4000 unmatched (right).
    write_lines(a_path,
                {
                    R"({"user_id":1,"click_ts":100,"page":"home"})",
                    R"({"user_id":2,"click_ts":2000,"page":"docs"})",
                });
    write_lines(b_path,
                {
                    R"({"user_id":1,"order_ts":150,"sku":"x"})",
                    R"({"user_id":2,"order_ts":4000,"sku":"y"})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE clicks (user_id BIGINT, click_ts BIGINT, page TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     a_path.string() +
                     "', event_time_column='click_ts');"
                     "CREATE TABLE orders (user_id BIGINT, order_ts BIGINT, sku TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     b_path.string() +
                     "', event_time_column='order_ts');"
                     "CREATE TABLE joined (c_user_id BIGINT, c_click_ts BIGINT, c_page TEXT, "
                     "                     o_user_id BIGINT, o_order_ts BIGINT, o_sku TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[2]));

    auto spec = compile(cat,
                        "INSERT INTO joined "
                        "SELECT * FROM clicks c FULL OUTER JOIN orders o "
                        "ON c.user_id = o.user_id AND "
                        "   c.click_ts BETWEEN o.order_ts - 50 AND o.order_ts + 200");

    InProcessCluster cluster("tm-sql-e2e-fij", 12);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 3u) << "expected match + unmatched-left + unmatched-right";
    bool matched = false, left_pad = false, right_pad = false;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        const bool c_null = js.at("c_user_id").is_null();
        const bool o_null = js.at("o_user_id").is_null();
        if (!c_null && !o_null) {
            matched = true;
            EXPECT_EQ(static_cast<std::int64_t>(js.at("c_user_id").as_number()), 1);
        } else if (!c_null && o_null) {
            left_pad = true;
            EXPECT_EQ(static_cast<std::int64_t>(js.at("c_user_id").as_number()), 2);
        } else if (c_null && !o_null) {
            right_pad = true;
            EXPECT_EQ(static_cast<std::int64_t>(js.at("o_user_id").as_number()), 2);
            EXPECT_EQ(js.at("o_sku").as_string(), "y");
        }
    }
    EXPECT_TRUE(matched) << "matched pair missing";
    EXPECT_TRUE(left_pad) << "null-padded unmatched-left missing";
    EXPECT_TRUE(right_pad) << "null-padded unmatched-right missing";

    std::filesystem::remove(a_path);
    std::filesystem::remove(b_path);
    std::filesystem::remove(out_path);
}

// SQLOPT-3: a C++-registered scalar UDF is callable from SQL by name and types
// as its declared return type (so it feeds a typed sink).
TEST(SqlRuntime, ScalarUdfCallableFromSql) {
    ensure_sql_installed_once();

    // Register a scalar UDF: bump(x) = x + 100, declared BIGINT.
    clink::ScalarFunctionRegistry::global().register_function(
        "bump",
        arrow::int64(),
        [](const std::vector<clink::config::JsonValue>& args) -> clink::config::JsonValue {
            if (args.empty() || !args[0].is_number()) {
                return clink::config::JsonValue{nullptr};
            }
            return clink::config::JsonValue{
                static_cast<double>(static_cast<std::int64_t>(args[0].as_number()) + 100)};
        });

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_udf_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_udf_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":5})",
                    R"({"user_id":2,"amount":40})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE evt (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, bumped BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat, "INSERT INTO out_t SELECT user_id, bump(amount) AS bumped FROM evt");

    InProcessCluster cluster("tm-sql-e2e-udf", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 2u);
    std::map<std::int64_t, std::int64_t> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("user_id").as_number())] =
            static_cast<std::int64_t>(js.at("bumped").as_number());
    }
    EXPECT_EQ(got[1], 105);
    EXPECT_EQ(got[2], 140);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

namespace {
// Register the test UDAFs once into the process-global registry (idempotent;
// overwrite is harmless). "t_sum" is fully featured (retract + merge); "t_prod"
// is append-only (no retract, no merge) to exercise the clear-error guards.
void register_test_udafs() {
    using clink::config::JsonValue;
    auto as_i64 = [](const JsonValue& v) { return static_cast<std::int64_t>(v.as_number()); };
    clink::AggFunctionRegistry::global().register_function(
        "t_sum",
        arrow::int64(),
        []() { return JsonValue{static_cast<std::int64_t>(0)}; },
        [as_i64](JsonValue acc, const std::vector<JsonValue>& a) {
            return JsonValue{as_i64(acc) + as_i64(a[0])};
        },
        [as_i64](const JsonValue& acc) { return JsonValue{as_i64(acc)}; },
        [as_i64](JsonValue acc, const std::vector<JsonValue>& a) {
            return JsonValue{as_i64(acc) - as_i64(a[0])};
        },
        [as_i64](JsonValue x, JsonValue y) { return JsonValue{as_i64(x) + as_i64(y)}; });
    clink::AggFunctionRegistry::global().register_function(
        "t_prod",
        arrow::int64(),
        []() { return JsonValue{static_cast<std::int64_t>(1)}; },
        [as_i64](JsonValue acc, const std::vector<JsonValue>& a) {
            return JsonValue{as_i64(acc) * as_i64(a[0])};
        },
        [as_i64](const JsonValue& acc) { return JsonValue{as_i64(acc)}; });
}
}  // namespace

// SQLOPT-3 UDAF: a C++-registered aggregate UDF works in an append-only GROUP BY
// and types as its declared return type.
TEST(SqlRuntime, UdafGroupBySum) {
    ensure_sql_installed_once();
    register_test_udafs();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_udaf_gb_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_udaf_gb_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"k":1,"v":10})", R"({"k":1,"v":30})", R"({"k":2,"v":5})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE evt (k BIGINT, v BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (k BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat, "INSERT INTO out_t SELECT k, t_sum(v) AS total FROM evt GROUP BY k");

    InProcessCluster cluster("tm-sql-udaf-gb", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    std::map<std::int64_t, std::int64_t> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("k").as_number())] =
            static_cast<std::int64_t>(js.at("total").as_number());
    }
    EXPECT_EQ(got[1], 40);
    EXPECT_EQ(got[2], 5);
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// A UDAF with only init/accumulate/result works in a TUMBLE window (append-only;
// never retracts or merges).
TEST(SqlRuntime, UdafTumbleProduct) {
    ensure_sql_installed_once();
    register_test_udafs();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_udaf_tw_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_udaf_tw_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    // window [0,1000): 2*3; window [1000,2000): 5
    write_lines(
        in_path,
        {R"({"k":1,"ts":100,"v":2})", R"({"k":1,"ts":200,"v":3})", R"({"k":1,"ts":1100,"v":5})"});
    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE evt (k BIGINT, ts BIGINT, v BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE out_t (k BIGINT, p BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(
        cat, "INSERT INTO out_t SELECT k, t_prod(v) AS p FROM evt GROUP BY TUMBLE(ts, 1000), k");

    InProcessCluster cluster("tm-sql-udaf-tw", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    std::multiset<std::int64_t> products;
    for (const auto& l : read_lines(out_path)) {
        products.insert(static_cast<std::int64_t>(clink::config::parse(l).at("p").as_number()));
    }
    EXPECT_TRUE(products.count(6) == 1) << "window [0,1000) product 2*3=6 missing";
    EXPECT_TRUE(products.count(5) == 1) << "window [1000,2000) product 5 missing";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// A group whose input column is all-NULL never accumulates; finalize returns
// result(init()) - here t_sum -> 0 - with no crash.
TEST(SqlRuntime, UdafAllNullGroupReturnsInit) {
    ensure_sql_installed_once();
    register_test_udafs();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_udaf_nil_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_udaf_nil_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"k":1,"v":null})", R"({"k":1,"v":null})"});
    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE evt (k BIGINT, v BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (k BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat, "INSERT INTO out_t SELECT k, t_sum(v) AS total FROM evt GROUP BY k");
    InProcessCluster cluster("tm-sql-udaf-nil", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    auto lines = read_lines(out_path);
    ASSERT_FALSE(lines.empty());
    EXPECT_EQ(static_cast<std::int64_t>(clink::config::parse(lines.back()).at("total").as_number()),
              0);
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// A UDAF with a merge closure is correct across a SESSION-window merge: the two
// pre-merge partial sums combine. (Arrival 100,1000,550 forces a merge.)
TEST(SqlRuntime, UdafSessionMerge) {
    ensure_sql_installed_once();
    register_test_udafs();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_udaf_se_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_udaf_se_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path,
                {R"({"k":1,"ts":100,"v":10})",
                 R"({"k":1,"ts":1000,"v":100})",
                 R"({"k":1,"ts":550,"v":55})"});
    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE evt (k BIGINT, ts BIGINT, v BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE out_t (k BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(
        cat, "INSERT INTO out_t SELECT k, t_sum(v) AS total FROM evt GROUP BY SESSION(ts, 500), k");
    InProcessCluster cluster("tm-sql-udaf-se", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1u) << "the three rows form one merged session";
    EXPECT_EQ(static_cast<std::int64_t>(clink::config::parse(lines[0]).at("total").as_number()),
              165);  // 10+100+55, merge combined the two partial sums
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// A merge-less UDAF under a SESSION window that actually merges must NOT
// silently emit a (wrong) combined result. The op raises a clear error at merge
// time. A merge-less UDAF (t_prod) in a SESSION window is rejected at bind time
// (sessions can merge, and the UDAF has no merge closure) - a clear error, not a
// silent wrong result. The matching success test UdafSessionMerge shows the
// with-merge path (t_sum) emits 165.
TEST(SqlRuntime, UdafSessionWithoutMergeRejectedAtBind) {
    ensure_sql_installed_once();
    register_test_udafs();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_udaf_sex_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_udaf_sex_out.ndjson";
    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE evt (k BIGINT, ts BIGINT, v BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE out_t (k BIGINT, p BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    EXPECT_ANY_THROW(compile(
        cat, "INSERT INTO out_t SELECT k, t_prod(v) AS p FROM evt GROUP BY SESSION(ts, 500), k"));
}

// SQLOPT-3: a UDAF in an OVER window is rejected at bind time (the OVER runtime
// operator cannot resolve UDAFs and would otherwise emit a silent NULL column).
TEST(SqlRuntime, UdafInOverRejectedAtBind) {
    ensure_sql_installed_once();
    register_test_udafs();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_udaf_ov_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_udaf_ov_out.ndjson";
    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE evt (k BIGINT, ts BIGINT, v BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "', event_time_column='ts');"
                     "CREATE TABLE out_t (k BIGINT, r BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    EXPECT_ANY_THROW(compile(
        cat,
        "INSERT INTO out_t SELECT k, t_sum(v) OVER (PARTITION BY k ORDER BY ts) AS r FROM evt"));
}

// A retractable UDAF handles a changelog delete exactly: +10,+30, delete 10 ->
// 30 (the retract closure inverts the deleted row).
TEST(SqlRuntime, UdafRetractionApplied) {
    ensure_sql_installed_once();
    register_test_udafs();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_udaf_rt_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_udaf_rt_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(
        in_path,
        {R"({"k":1,"v":10})", R"({"k":1,"v":30})", R"({"k":1,"v":10,"__row_kind":"delete"})"});
    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE evt (k BIGINT, v BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (k BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat, "INSERT INTO out_t SELECT k, t_sum(v) AS total FROM evt GROUP BY k");
    InProcessCluster cluster("tm-sql-udaf-rt", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);
    auto lines = read_lines(out_path);
    ASSERT_FALSE(lines.empty());
    EXPECT_EQ(static_cast<std::int64_t>(clink::config::parse(lines.back()).at("total").as_number()),
              30);
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

namespace {
// SQLOPT PTF: a stateful per-key running-total KeyedProcessFunction registered
// into the process-global PtfRegistry. Emits one row per input carrying the
// partition key and the running sum of `amount`, exercising per-key state
// isolation across the registry -> preparse -> binder -> physical -> install
// bridge. State is read/written under the function's current_key(), which the
// adapter sets from the PARTITION BY key extractor.
class RunningTotalPtf final : public clink::KeyedProcessFunction<std::string, Row, Row> {
public:
    void open(clink::RuntimeContext& ctx) override { rt_ = &ctx; }

    void process_element(const Row& in,
                         clink::ProcessFunctionContext<Row>& /*pctx*/,
                         clink::Collector<Row>& out) override {
        auto state = rt_->keyed_state<std::string, std::int64_t>(
            "ptf_running_total", clink::string_codec(), clink::int64_codec());
        std::int64_t total = state.get(this->current_key()).value_or(0);
        if (auto it = in.values.find("amount"); it != in.values.end() && it->second.is_number()) {
            total += static_cast<std::int64_t>(it->second.as_number());
        }
        state.put(this->current_key(), total);

        Row r;
        if (auto it = in.values.find("user_id"); it != in.values.end())
            r.values["user_id"] = it->second;
        r.values["running"] = clink::config::JsonValue{total};
        out.collect(std::move(r));
    }

    std::string name() const override { return "running_total_ptf"; }

private:
    clink::RuntimeContext* rt_{nullptr};
};

void register_test_ptf() {
    clink::sql::PtfRegistry::global().register_function(
        "running_total",
        {ColumnSpec{"user_id", arrow::int64()}, ColumnSpec{"running", arrow::int64()}},
        []() -> std::shared_ptr<clink::KeyedProcessFunction<std::string, Row, Row>> {
            return std::make_shared<RunningTotalPtf>();
        });
}
}  // namespace

// SQLOPT PTF: a C++-registered process-table-function is callable from SQL as
// `fn(TABLE t PARTITION BY k)`. The PARTITION BY routes each key to its own
// state slot, so two users accumulate independent running totals end-to-end.
TEST(SqlRuntime, ProcessTableFunctionPartitionedRunningTotal) {
    ensure_sql_installed_once();
    register_test_ptf();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_ptf_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_ptf_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":10})",
                    R"({"user_id":2,"amount":20})",
                    R"({"user_id":1,"amount":30})",
                    R"({"user_id":2,"amount":5})",
                    R"({"user_id":1,"amount":7})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE events (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, running BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO out_t "
                        "SELECT user_id, running FROM running_total(TABLE events PARTITION BY "
                        "user_id)");

    InProcessCluster cluster("tm-sql-ptf", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // One output per input; partitioned running totals isolated per key:
    //   user 1: 10, 40, 47   user 2: 20, 25
    std::map<std::int64_t, std::vector<std::int64_t>> by_user;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        by_user[static_cast<std::int64_t>(js.at("user_id").as_number())].push_back(
            static_cast<std::int64_t>(js.at("running").as_number()));
    }
    ASSERT_EQ(by_user[1].size(), 3u);
    ASSERT_EQ(by_user[2].size(), 2u);
    // Within a key the source order is preserved, so the totals are monotone and
    // the final value is the full per-key sum (proves state did not leak keys).
    EXPECT_EQ(by_user[1].back(), 47);
    EXPECT_EQ(by_user[2].back(), 25);
    EXPECT_EQ(by_user[1].front(), 10);
    EXPECT_EQ(by_user[2].front(), 20);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// SQLOPT PTF: the PTF output feeds an outer WHERE + projection. Only rows whose
// running total exceeds 20 survive, proving the function island is a first-class
// relation the rest of the query plans over.
TEST(SqlRuntime, ProcessTableFunctionFeedsOuterFilter) {
    ensure_sql_installed_once();
    register_test_ptf();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_ptf2_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_ptf2_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":10})",
                    R"({"user_id":1,"amount":30})",
                    R"({"user_id":1,"amount":7})",
                    R"({"user_id":2,"amount":20})",
                    R"({"user_id":2,"amount":5})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE events (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (running BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO out_t "
                        "SELECT running FROM running_total(TABLE events PARTITION BY user_id) "
                        "WHERE running > 20");

    InProcessCluster cluster("tm-sql-ptf2", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // user 1: 10,40,47 -> {40,47}; user 2: 20,25 -> {25}.
    std::multiset<std::int64_t> got;
    for (const auto& l : read_lines(out_path)) {
        got.insert(static_cast<std::int64_t>(clink::config::parse(l).at("running").as_number()));
    }
    EXPECT_EQ(got, (std::multiset<std::int64_t>{25, 40, 47}));

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// SQLOPT PTF: calling an unregistered function as a table function is a bind
// error (the binder consults PtfRegistry for the output schema and fails clean
// rather than producing an unresolved island).
TEST(SqlRuntime, ProcessTableFunctionUnregisteredRejectedAtBind) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_ptf3_in.ndjson";
    Catalog cat;
    auto ddl =
        parse(std::string{"CREATE TABLE events (user_id BIGINT, amount BIGINT) "
                          "WITH (connector='file', format='json', path='"} +
              in_path.string() +
              "');"
              "CREATE TABLE out_t (user_id BIGINT, running BIGINT) "
              "WITH (connector='file', format='json', path='/tmp/clink_sql_ptf3_out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    EXPECT_ANY_THROW(compile(cat,
                             "INSERT INTO out_t SELECT user_id, running FROM "
                             "no_such_ptf(TABLE events PARTITION BY user_id)"));
}

namespace {
// Compile an already-bound LogicalSink plan (e.g. the materialized-view
// maintenance plan) into a JobGraphSpec, mirroring the SQL `compile` helper but
// for a plan we built programmatically rather than from a SQL string.
cluster::JobGraphSpec compile_plan(std::unique_ptr<LogicalPlan> plan) {
    plan = optimize(std::move(plan));
    PhysicalPlanner pp;
    return pp.compile(static_cast<const LogicalSink&>(*plan));
}

ast::CreateMaterializedViewStmt parse_mv(const std::string& sql) {
    return std::move(std::get<ast::CreateMaterializedViewStmt>(parse(sql).statements[0]));
}
}  // namespace

// MATTBL: CREATE MATERIALIZED VIEW desugars into a backing table + a continuous
// maintenance job. The maintenance job populates the (upsert) backing store
// with the current aggregate per key; a separate referencing query then reads
// the committed materialised rows. This proves the whole MV = backing-store +
// maintenance-INSERT + plain-scan model end-to-end.
TEST(SqlRuntime, MaterializedViewKeyedAggregationRunsEndToEnd) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_mv_events.ndjson";
    const auto mv_path = std::filesystem::temp_directory_path() / "clink_sql_mv_backing.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_mv_out.ndjson";
    for (const auto& p : {in_path, mv_path, out_path}) {
        std::filesystem::remove(p);
    }

    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":10})",
                    R"({"user_id":2,"amount":20})",
                    R"({"user_id":1,"amount":30})",
                    R"({"user_id":2,"amount":5})",
                    R"({"user_id":1,"amount":7})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE events (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    // Desugar the MV: registers the backing table `mv` (upsert, pk=user_id) and
    // returns the maintenance INSERT plan.
    auto mvplan = plan_materialized_view(
        parse_mv(std::string{"CREATE MATERIALIZED VIEW mv "
                             "WITH (freshness='0', connector='file', format='json', path='"} +
                 mv_path.string() +
                 "') AS SELECT user_id, SUM(amount) AS total FROM events GROUP BY user_id"),
        cat);
    const auto* mv_def = cat.get_table("mv");
    ASSERT_NE(mv_def, nullptr);
    EXPECT_TRUE(mv_def->is_materialized_view());
    EXPECT_TRUE(mv_def->is_upsert());

    auto maintenance_spec = compile_plan(std::move(mvplan.maintenance));
    // The referencing query scans the backing table like any source.
    auto read_spec = compile(cat, "INSERT INTO out_t SELECT user_id, total FROM mv");

    InProcessCluster cluster("tm-sql-mv", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;

    // Run the maintenance job to completion (the file source is bounded), then
    // read the materialised backing store.
    auto maint = submitter.submit(maintenance_spec.to_json(), {}, opts);
    ASSERT_TRUE(maint.completed) << "reject: " << maint.reject_message;
    EXPECT_TRUE(maint.ok) << "errors: " << (maint.errors.empty() ? "(none)" : maint.errors[0]);

    auto read = submitter.submit(read_spec.to_json(), {}, opts);
    ASSERT_TRUE(read.completed) << "reject: " << read.reject_message;
    EXPECT_TRUE(read.ok) << "errors: " << (read.errors.empty() ? "(none)" : read.errors[0]);

    // The upsert backing holds one current row per key; the referencing read
    // sees the final aggregates: user 1 = 10+30+7 = 47, user 2 = 20+5 = 25.
    std::map<std::int64_t, std::int64_t> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("user_id").as_number())] =
            static_cast<std::int64_t>(js.at("total").as_number());
    }
    EXPECT_EQ(got[1], 47);
    EXPECT_EQ(got[2], 25);

    for (const auto& p : {in_path, mv_path, out_path}) {
        std::filesystem::remove(p);
    }
}

TEST(SqlRuntime, UnionAllMergesTwoFileSources) {
    ensure_sql_installed_once();

    const auto a_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_union_a.ndjson";
    const auto b_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_union_b.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_union_out.ndjson";
    std::filesystem::remove(a_path);
    std::filesystem::remove(b_path);
    std::filesystem::remove(out_path);

    write_lines(a_path,
                {
                    R"({"id":1,"url":"a1"})",
                    R"({"id":2,"url":"a2"})",
                });
    write_lines(b_path,
                {
                    R"({"id":10,"url":"b1"})",
                    R"({"id":20,"url":"b2"})",
                    R"({"id":30,"url":"b3"})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE a (id BIGINT, url TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     a_path.string() +
                     "');"
                     "CREATE TABLE b (id BIGINT, url TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     b_path.string() +
                     "');"
                     "CREATE TABLE out_t (id BIGINT, url TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[2]));

    auto spec = compile(cat,
                        "INSERT INTO out_t "
                        "SELECT id, url FROM a UNION ALL SELECT id, url FROM b");

    InProcessCluster cluster("tm-sql-e2e-union", 12);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    EXPECT_EQ(lines.size(), 5u);

    std::set<std::int64_t> got_ids;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got_ids.insert(static_cast<std::int64_t>(js.at("id").as_number()));
    }
    EXPECT_EQ(got_ids, (std::set<std::int64_t>{1, 2, 10, 20, 30}));

    std::filesystem::remove(a_path);
    std::filesystem::remove(b_path);
    std::filesystem::remove(out_path);
}

// Set operations UNION (distinct) / INTERSECT / EXCEPT. Duplicates
// within a side are eliminated; EXCEPT is a changelog (a left row is
// retracted when the right side later produces it), so we replay the
// __row_kind stream to the converged set, which is order-independent.
TEST(SqlRuntime, SetOperations) {
    ensure_sql_installed_once();

    auto run = [](const std::string& tag, const std::string& op) -> std::set<std::int64_t> {
        const auto a_path =
            std::filesystem::temp_directory_path() / ("clink_sql_e2e_setop_a_" + tag + ".ndjson");
        const auto b_path =
            std::filesystem::temp_directory_path() / ("clink_sql_e2e_setop_b_" + tag + ".ndjson");
        const auto out_path =
            std::filesystem::temp_directory_path() / ("clink_sql_e2e_setop_out_" + tag + ".ndjson");
        for (const auto& p : {a_path, b_path, out_path})
            std::filesystem::remove(p);
        // Intra-side duplicates (3 in a, 4 in b) exercise distinct.
        write_lines(a_path, {R"({"v":1})", R"({"v":2})", R"({"v":3})", R"({"v":3})"});
        write_lines(b_path, {R"({"v":2})", R"({"v":3})", R"({"v":4})", R"({"v":4})"});

        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE a (v BIGINT) "
                                     "WITH (connector='file', format='json', path='"} +
                         a_path.string() +
                         "');"
                         "CREATE TABLE b (v BIGINT) "
                         "WITH (connector='file', format='json', path='" +
                         b_path.string() +
                         "');"
                         "CREATE TABLE out_t (v BIGINT) "
                         "WITH (connector='file', format='json', path='" +
                         out_path.string() + "')");
        for (const auto& st : ddl.statements)
            cat.register_table(std::get<ast::CreateTableStmt>(st));
        auto spec =
            compile(cat, ("INSERT INTO out_t SELECT v FROM a " + op + " SELECT v FROM b").c_str());

        InProcessCluster cluster("tm-sql-e2e-setop-" + tag, 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto result = submitter.submit(spec.to_json(), {}, opts);
        EXPECT_TRUE(result.completed) << "reject: " << result.reject_message;
        EXPECT_TRUE(result.ok) << "errors: "
                               << (result.errors.empty() ? "(none)" : result.errors[0]);

        std::set<std::int64_t> live;
        for (const auto& l : read_lines(out_path)) {
            auto js = clink::config::parse(l);
            const auto v = static_cast<std::int64_t>(js.at("v").as_number());
            const bool del =
                js.contains("__row_kind") && js.at("__row_kind").as_string() == "delete";
            if (del)
                live.erase(v);
            else
                live.insert(v);
        }
        for (const auto& p : {a_path, b_path, out_path})
            std::filesystem::remove(p);
        return live;
    };

    EXPECT_EQ(run("union", "UNION"), (std::set<std::int64_t>{1, 2, 3, 4}));
    EXPECT_EQ(run("intersect", "INTERSECT"), (std::set<std::int64_t>{2, 3}));
    EXPECT_EQ(run("except", "EXCEPT"), (std::set<std::int64_t>{1}));
}

// Multiset INTERSECT ALL / EXCEPT ALL: output multiplicity is min(L, R)
// and max(L - R, 0) respectively. The output is a changelog (append file
// sink), so we reconstruct the live multiset by netting inserts/deletes.
TEST(SqlRuntime, SetOperationsAll) {
    ensure_sql_installed_once();

    auto run = [](const std::string& tag, const std::string& op) -> std::map<std::int64_t, int> {
        const auto a_path =
            std::filesystem::temp_directory_path() / ("clink_sql_setall_a_" + tag + ".ndjson");
        const auto b_path =
            std::filesystem::temp_directory_path() / ("clink_sql_setall_b_" + tag + ".ndjson");
        const auto out_path =
            std::filesystem::temp_directory_path() / ("clink_sql_setall_out_" + tag + ".ndjson");
        for (const auto& p : {a_path, b_path, out_path})
            std::filesystem::remove(p);
        // a multiset: 1x1, 1x2, 3x3 ; b multiset: 1x2, 2x3, 1x4
        write_lines(a_path, {R"({"v":1})", R"({"v":2})", R"({"v":3})", R"({"v":3})", R"({"v":3})"});
        write_lines(b_path, {R"({"v":2})", R"({"v":3})", R"({"v":3})", R"({"v":4})"});

        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE a (v BIGINT) "
                                     "WITH (connector='file', format='json', path='"} +
                         a_path.string() +
                         "');"
                         "CREATE TABLE b (v BIGINT) "
                         "WITH (connector='file', format='json', path='" +
                         b_path.string() +
                         "');"
                         "CREATE TABLE out_t (v BIGINT) "
                         "WITH (connector='file', format='json', path='" +
                         out_path.string() + "')");
        for (const auto& st : ddl.statements)
            cat.register_table(std::get<ast::CreateTableStmt>(st));
        auto spec =
            compile(cat, ("INSERT INTO out_t SELECT v FROM a " + op + " SELECT v FROM b").c_str());

        InProcessCluster cluster("tm-sql-setall-" + tag, 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto result = submitter.submit(spec.to_json(), {}, opts);
        EXPECT_TRUE(result.completed) << "reject: " << result.reject_message;

        std::map<std::int64_t, int> live;
        for (const auto& l : read_lines(out_path)) {
            auto js = clink::config::parse(l);
            const auto v = static_cast<std::int64_t>(js.at("v").as_number());
            const bool del =
                js.contains("__row_kind") && js.at("__row_kind").as_string() == "delete";
            live[v] += del ? -1 : 1;
            if (live[v] == 0)
                live.erase(v);
        }
        for (const auto& p : {a_path, b_path, out_path})
            std::filesystem::remove(p);
        return live;
    };

    // INTERSECT ALL: 2 -> min(1,1)=1, 3 -> min(3,2)=2.
    EXPECT_EQ(run("intersectall", "INTERSECT ALL"), (std::map<std::int64_t, int>{{2, 1}, {3, 2}}));
    // EXCEPT ALL: 1 -> max(1-0,0)=1, 3 -> max(3-2,0)=1.
    EXPECT_EQ(run("exceptall", "EXCEPT ALL"), (std::map<std::int64_t, int>{{1, 1}, {3, 1}}));
}

// Regression for the review bug: an EXCEPT over a changelog left input must
// retract a left-only row when the left deletes it (the representative row
// was being erased before the owed delete could be emitted, leaking a
// phantom row). The delete-tagged input row drives the left count to zero.
TEST(SqlRuntime, SetOpExceptRetractsDeletedLeftRow) {
    ensure_sql_installed_once();
    const auto a_path = std::filesystem::temp_directory_path() / "clink_sql_setdel_a.ndjson";
    const auto b_path = std::filesystem::temp_directory_path() / "clink_sql_setdel_b.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_setdel_out.ndjson";
    for (const auto& p : {a_path, b_path, out_path})
        std::filesystem::remove(p);
    // left: v=1 inserted then deleted (changelog), v=2 stays. right: v=9.
    write_lines(a_path, {R"({"v":1})", R"({"v":2})", R"({"v":1,"__row_kind":"delete"})"});
    write_lines(b_path, {R"({"v":9})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE a (v BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     a_path.string() +
                     "');"
                     "CREATE TABLE b (v BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     b_path.string() +
                     "');"
                     "CREATE TABLE out_t (v BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(cat, "INSERT INTO out_t SELECT v FROM a EXCEPT SELECT v FROM b");

    InProcessCluster cluster("tm-sql-setdel", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    std::set<std::int64_t> live;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        const auto v = static_cast<std::int64_t>(js.at("v").as_number());
        if (js.contains("__row_kind") && js.at("__row_kind").as_string() == "delete")
            live.erase(v);
        else
            live.insert(v);
    }
    // v=1 was retracted on the left, so only v=2 survives (no phantom v=1).
    EXPECT_EQ(live, (std::set<std::int64_t>{2}));
    for (const auto& p : {a_path, b_path, out_path})
        std::filesystem::remove(p);
}

// --- ORDER BY + LIMIT TOP-N e2e -------------------------

TEST(SqlRuntime, TopNReturnsHighestThreeByAmount) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_topn_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_topn_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"id":1,"amount":50})",
                    R"({"id":2,"amount":10})",
                    R"({"id":3,"amount":80})",
                    R"({"id":4,"amount":30})",
                    R"({"id":5,"amount":70})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE top_orders (id BIGINT, amount BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO top_orders "
                        "SELECT id, amount FROM orders ORDER BY amount DESC LIMIT 3");

    InProcessCluster cluster("tm-sql-e2e-topn", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 3u);
    std::vector<std::int64_t> got_amounts;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got_amounts.push_back(static_cast<std::int64_t>(js.at("amount").as_number()));
    }
    EXPECT_EQ(got_amounts, (std::vector<std::int64_t>{80, 70, 50}));

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// --- CTEs (WITH clause) e2e -----------------------------

TEST(SqlRuntime, CteFiltersFeedingUnboundedAggregate) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_cte_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_cte_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":5})",
                    R"({"user_id":1,"amount":15})",
                    R"({"user_id":2,"amount":3})",
                    R"({"user_id":2,"amount":20})",
                    R"({"user_id":3,"amount":50})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE per_user (user_id BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    // CTE 'big_orders' filters to amount > 10; outer aggregates
    // running SUM per user. user=1 -> 15, user=2 -> 20, user=3 -> 50.
    auto spec = compile(cat,
                        "INSERT INTO per_user "
                        "WITH big_orders AS (SELECT user_id, amount FROM orders WHERE amount > 10) "
                        "SELECT user_id, SUM(amount) AS total FROM big_orders GROUP BY user_id");

    InProcessCluster cluster("tm-sql-e2e-cte", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Take the last seen total per user (upsert-style aggregate).
    auto lines = read_lines(out_path);
    std::map<std::int64_t, std::int64_t> latest;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        latest[static_cast<std::int64_t>(js.at("user_id").as_number())] =
            static_cast<std::int64_t>(js.at("total").as_number());
    }
    EXPECT_EQ(latest[1], 15);
    EXPECT_EQ(latest[2], 20);
    EXPECT_EQ(latest[3], 50);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// --- built-in scalar functions e2e ----------------------

TEST(SqlRuntime, ScalarBuiltinsAppliedPerRow) {
    ensure_sql_installed_once();

    const auto in_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_builtins_in.ndjson";
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_builtins_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"url":"  alpha  ","score":3.14159})",
                    R"({"user_id":2,"url":"  beta  ","score":-2.5})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE in_t (user_id BIGINT, url TEXT, score DOUBLE) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, trimmed TEXT, prefix TEXT, "
                     "                    abs_score DOUBLE, rounded DOUBLE, "
                     "                    pos BIGINT, zeroed BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    // Exercises TRIM, SUBSTRING, ABS, ROUND, POSITION, NULLIF in one
    // pipeline.
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT user_id, "
                        "  TRIM(url) AS trimmed, "
                        "  SUBSTRING(TRIM(url), 1, 3) AS prefix, "
                        "  ABS(score) AS abs_score, "
                        "  ROUND(score, 2) AS rounded, "
                        "  POSITION('e' IN TRIM(url)) AS pos, "
                        "  NULLIF(user_id, 1) AS zeroed "
                        "FROM in_t");

    InProcessCluster cluster("tm-sql-e2e-builtins", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 2u);
    std::map<std::int64_t, clink::config::JsonValue> by_user;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        by_user.emplace(static_cast<std::int64_t>(js.at("user_id").as_number()), js);
    }
    {
        const auto& u1 = by_user.at(1);
        EXPECT_EQ(u1.at("trimmed").as_string(), "alpha");
        EXPECT_EQ(u1.at("prefix").as_string(), "alp");
        EXPECT_DOUBLE_EQ(u1.at("abs_score").as_number(), 3.14159);
        EXPECT_DOUBLE_EQ(u1.at("rounded").as_number(), 3.14);
        EXPECT_EQ(static_cast<std::int64_t>(u1.at("pos").as_number()), 0);
        EXPECT_TRUE(u1.at("zeroed").is_null());
    }
    {
        const auto& u2 = by_user.at(2);
        EXPECT_EQ(u2.at("trimmed").as_string(), "beta");
        EXPECT_EQ(u2.at("prefix").as_string(), "bet");
        EXPECT_DOUBLE_EQ(u2.at("abs_score").as_number(), 2.5);
        EXPECT_DOUBLE_EQ(u2.at("rounded").as_number(), -2.5);
        EXPECT_EQ(static_cast<std::int64_t>(u2.at("pos").as_number()), 2);
        EXPECT_EQ(static_cast<std::int64_t>(u2.at("zeroed").as_number()), 2);
    }

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Parity wave: extended scalar builtins (string + math) end to end.
TEST(SqlRuntime, ExtendedScalarBuiltinsAppliedPerRow) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_ext_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_ext_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"user_id":4,"url":"a,b,c"})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE in_t (user_id BIGINT, url TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, cl BIGINT, rev TEXT, part TEXT, "
                     "                    sq DOUBLE, sw BOOLEAN) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT user_id, "
                        "  CHAR_LENGTH(url) AS cl, "
                        "  REVERSE(url) AS rev, "
                        "  SPLIT_PART(url, ',', 2) AS part, "
                        "  SQRT(user_id) AS sq, "
                        "  STARTS_WITH(url, 'a') AS sw "
                        "FROM in_t");

    InProcessCluster cluster("tm-sql-e2e-ext", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1u);
    auto js = clink::config::parse(lines[0]);
    EXPECT_EQ(static_cast<std::int64_t>(js.at("cl").as_number()), 5);
    EXPECT_EQ(js.at("rev").as_string(), "c,b,a");
    EXPECT_EQ(js.at("part").as_string(), "b");
    EXPECT_DOUBLE_EQ(js.at("sq").as_number(), 2.0);
    EXPECT_TRUE(js.at("sw").as_bool());

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Parity wave: stddev / variance aggregates over GROUP BY.
TEST(SqlRuntime, StddevVarianceAggregates) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_stddev_in.ndjson";
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_stddev_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    // user 1: [2, 6] -> var_pop=4, var_samp=8, stddev_pop=2.
    // user 2: [10, 10, 10] -> all zero.
    write_lines(in_path,
                {R"({"user_id":1,"amount":2})",
                 R"({"user_id":1,"amount":6})",
                 R"({"user_id":2,"amount":10})",
                 R"({"user_id":2,"amount":10})",
                 R"({"user_id":2,"amount":10})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, sd DOUBLE, vp DOUBLE, vs DOUBLE) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='user_id')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec =
        compile(cat,
                "INSERT INTO out_t SELECT user_id, STDDEV_POP(amount) AS sd, "
                "VAR_POP(amount) AS vp, VAR_SAMP(amount) AS vs FROM orders GROUP BY user_id");

    InProcessCluster cluster("tm-sql-e2e-stddev", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    std::map<std::int64_t, clink::config::JsonValue> by_user;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        by_user.emplace(static_cast<std::int64_t>(js.at("user_id").as_number()), js);
    }
    ASSERT_EQ(by_user.size(), 2u);
    EXPECT_DOUBLE_EQ(by_user.at(1).at("sd").as_number(), 2.0);
    EXPECT_DOUBLE_EQ(by_user.at(1).at("vp").as_number(), 4.0);
    EXPECT_DOUBLE_EQ(by_user.at(1).at("vs").as_number(), 8.0);
    EXPECT_DOUBLE_EQ(by_user.at(2).at("sd").as_number(), 0.0);
    EXPECT_DOUBLE_EQ(by_user.at(2).at("vp").as_number(), 0.0);
    EXPECT_DOUBLE_EQ(by_user.at(2).at("vs").as_number(), 0.0);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, CaseWhenAssignsTierPerRow) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_case_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_case_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"score":5})",
                    R"({"user_id":2,"score":55})",
                    R"({"user_id":3,"score":120})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE scores (user_id BIGINT, score BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE tiers (user_id BIGINT, tier TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO tiers SELECT user_id, "
                        "CASE WHEN score > 100 THEN 'top' "
                        "     WHEN score > 50  THEN 'mid' "
                        "     ELSE 'low' END AS tier "
                        "FROM scores");

    InProcessCluster cluster("tm-sql-e2e-case", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 3u);
    std::map<std::int64_t, std::string> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got[static_cast<std::int64_t>(js.at("user_id").as_number())] = js.at("tier").as_string();
    }
    EXPECT_EQ(got[1], "low");
    EXPECT_EQ(got[2], "mid");
    EXPECT_EQ(got[3], "top");

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, LimitCapsOutputRowCount) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_limit_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_limit_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"url":"a"})",
                    R"({"user_id":2,"url":"b"})",
                    R"({"user_id":3,"url":"c"})",
                    R"({"user_id":4,"url":"d"})",
                    R"({"user_id":5,"url":"e"})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE clicks (user_id BIGINT, url TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (user_id BIGINT, url TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat, "INSERT INTO out_t SELECT user_id, url FROM clicks LIMIT 2");

    InProcessCluster cluster("tm-sql-e2e-limit", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    EXPECT_EQ(lines.size(), 2u) << "expected at most 2 lines, got " << lines.size();

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(SqlRuntime, FilterAndProjectRunsEndToEnd) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_clicks.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_urls.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"url":"home"})",
                    R"({"user_id":2,"url":"about"})",
                    R"({"user_id":3,"url":"home"})",
                    R"({"user_id":4,"url":"docs"})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE clicks (user_id BIGINT, url TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (url TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat, "INSERT INTO out_t SELECT url FROM clicks WHERE url = 'home'");

    InProcessCluster cluster("tm-sql-e2e-filter", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    EXPECT_EQ(lines.size(), 2u);
    for (const auto& l : lines) {
        EXPECT_NE(l.find("\"url\":\"home\""), std::string::npos) << l;
    }

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// --- full changelog pair model -----------------------
//
// The delete-tagged retraction test above exercises insert + delete.
// This one exercises the explicit update_before / update_after pair
// shape that the changelog model adds on top: an aggregate must treat
// update_before as a retraction of the old value and update_after as
// an insertion of the new value, so consecutive update pairs net to
// the latest value per key. Guards the pair path that delete-then-
// insert decomposition would otherwise hide.
TEST(SqlRuntime, AggregateNetsUpdateBeforeAfterPairs) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_updpair_in.ndjson";
    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_updpair_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    // user=1: +10, then (update_before 10 / update_after 30),
    //               then (update_before 30 / update_after 100) -> 100
    // user=2: +50 -> 50
    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":10})",
                    R"({"user_id":1,"amount":10,"__row_kind":"update_before"})",
                    R"({"user_id":1,"amount":30,"__row_kind":"update_after"})",
                    R"({"user_id":1,"amount":30,"__row_kind":"update_before"})",
                    R"({"user_id":1,"amount":100,"__row_kind":"update_after"})",
                    R"({"user_id":2,"amount":50})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE per_user (user_id BIGINT, total BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='user_id')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat,
                        "INSERT INTO per_user SELECT user_id, SUM(amount) AS total FROM orders "
                        "GROUP BY user_id");

    InProcessCluster cluster("tm-sql-e2e-updpair", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;

    // Upsert sink keys on user_id; take the last emitted total per key
    // (the running changelog trail collapses to the final value).
    auto lines = read_lines(out_path);
    ASSERT_FALSE(lines.empty());
    std::map<std::int64_t, std::int64_t> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        // Skip update_before rows: they carry the pre-image, not the
        // current value. The final per-key state is an insert-like row.
        if (js.is_object() && js.contains("__row_kind")) {
            if (is_delete_like(js.at("__row_kind").as_string())) {
                continue;
            }
        }
        got[static_cast<std::int64_t>(js.at("user_id").as_number())] =
            static_cast<std::int64_t>(js.at("total").as_number());
    }
    EXPECT_EQ(got[1], 100) << "two update pairs should net to the latest value";
    EXPECT_EQ(got[2], 50);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// --- 2PC sink driven from SQL, commit verified ----------
//
// FileExactlyOnceSinkProducesCommittedRecords above only proves
// records are STAGED (no checkpoint completes in that run). This test
// enables checkpointing so the bounded source's end-of-stream terminal
// barrier gives the 2PC sink a state backend + local commit signal,
// then asserts every record lands in committed/ exactly once with
// staging/ drained - the commit half of 2PC, driven entirely from the
// SQL DDL `delivery_guarantee='exactly_once'`.
TEST(SqlRuntime, ExactlyOnceSinkViaSqlCommitsRecordsExactlyOnce) {
    ensure_sql_installed_once();

    const auto in_path =
        std::filesystem::temp_directory_path() / "clink_sql_e2e_eocommit_in.ndjson";
    const auto out_dir = std::filesystem::temp_directory_path() / "clink_sql_e2e_eocommit_dir";
    const auto ckpt_dir = std::filesystem::temp_directory_path() / "clink_sql_e2e_eocommit_ckpt";
    std::filesystem::remove(in_path);
    std::filesystem::remove_all(out_dir);
    std::filesystem::remove_all(ckpt_dir);

    write_lines(in_path,
                {
                    R"({"k":1,"v":10})",
                    R"({"k":2,"v":20})",
                    R"({"k":3,"v":30})",
                    R"({"k":4,"v":40})",
                    R"({"k":5,"v":50})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE src_t (k BIGINT, v BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE eo (k BIGINT, v BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_dir.string() + "', delivery_guarantee='exactly_once')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    auto spec = compile(cat, "INSERT INTO eo SELECT k, v FROM src_t");

    InProcessCluster cluster("tm-sql-e2e-eocommit", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 20s;
    // Enable checkpointing: gives the job a state backend so the
    // bounded source emits a terminal barrier at EOS and the 2PC sink
    // commits its staged records locally.
    opts.checkpoint.checkpoint_dir = ckpt_dir.string();
    opts.checkpoint.interval_ms = 200;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Walk out_dir; classify files by whether they sit under a
    // committed/ or staging/ path (robust to per-subtask subdirs).
    ASSERT_TRUE(std::filesystem::exists(out_dir));
    std::multiset<std::string> committed_rows;
    std::size_t staging_files = 0;
    for (const auto& e : std::filesystem::recursive_directory_iterator(out_dir)) {
        if (!e.is_regular_file()) {
            continue;
        }
        const auto path = e.path().string();
        if (path.find("committed") != std::string::npos) {
            std::ifstream in(e.path());
            std::string line;
            while (std::getline(in, line)) {
                if (!line.empty()) {
                    committed_rows.insert(line);
                }
            }
        } else if (path.find("staging") != std::string::npos) {
            ++staging_files;
        }
    }
    const std::set<std::string> unique_rows(committed_rows.begin(), committed_rows.end());
    EXPECT_EQ(committed_rows.size(), 5u)
        << "expected 5 committed rows, got " << committed_rows.size();
    EXPECT_EQ(unique_rows.size(), committed_rows.size())
        << "duplicate rows in committed/ - not exactly-once";
    EXPECT_EQ(staging_files, 0u) << "staging/ should be drained after commit";

    std::filesystem::remove(in_path);
    std::filesystem::remove_all(out_dir);
    std::filesystem::remove_all(ckpt_dir);
}

// --- async lookup via SQL ----------------------
//
// `INSERT INTO enriched SELECT enrich_double(*) FROM events`, with
// `enrich_double` registered in AsyncFunctionRegistry, lowers through
// the binder (LogicalAsyncMap) and physical planner (async_lookup_row)
// to a runtime async map that drives the registered Row -> Task<Row>
// coroutine per row. This proves the SQL frontend reaches the async
// runtime end to end - the gap that was "planned" before this pass.
TEST(SqlRuntime, AsyncLookupViaSqlEnrichesRows) {
    ensure_sql_installed_once();

    // Async enrichment: copy the row, add doubled = 2 * amount. The
    // coroutine completes synchronously (co_return) - real I/O would
    // co_await inside the body; the operator drives it either way.
    AsyncFunctionRegistry::global().register_function(
        "enrich_double", [](const Row& in) -> clink::async::Task<Row> {
            Row out = in;
            std::int64_t amt = 0;
            if (auto s = in.get_string("amount")) {
                amt = std::stoll(*s);
            }
            out.values["doubled"] = clink::config::JsonValue{static_cast<double>(amt * 2)};
            co_return out;
        });

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_async_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_async_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);

    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":10})",
                    R"({"user_id":2,"amount":25})",
                    R"({"user_id":3,"amount":0})",
                });

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE events (user_id BIGINT, amount BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE enriched (user_id BIGINT, amount BIGINT, doubled BIGINT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));

    // The async function name in the projection is what the binder
    // recognizes and lowers to async_lookup_row.
    auto spec = compile(cat, "INSERT INTO enriched SELECT enrich_double(*) FROM events");

    InProcessCluster cluster("tm-sql-e2e-async", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out_path);
    ASSERT_FALSE(lines.empty());
    std::map<std::int64_t, std::int64_t> doubled_by_user;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        ASSERT_TRUE(js.is_object()) << l;
        const auto uid = static_cast<std::int64_t>(js.at("user_id").as_number());
        ASSERT_TRUE(js.contains("doubled")) << "async enrichment column missing in: " << l;
        doubled_by_user[uid] = static_cast<std::int64_t>(js.at("doubled").as_number());
    }
    EXPECT_EQ(doubled_by_user[1], 20);
    EXPECT_EQ(doubled_by_user[2], 50);
    EXPECT_EQ(doubled_by_user[3], 0);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// Lookup (enrichment) join: a probe stream JOINed against a
// connector='lookup' dim table whose registered async function returns
// the dim columns for the probe key. INNER drops probe rows with no dim
// match (the physical plan appends an IS NOT NULL filter on the dim
// key); LEFT keeps them with null-padded dim columns.
TEST(SqlRuntime, LookupJoinEnrichesProbeStream) {
    ensure_sql_installed_once();

    // cust -> (id, name) for ids {1, 3}; cust 2 has no match. The fn
    // receives the probe row and returns the dim columns (empty on miss).
    AsyncFunctionRegistry::global().register_function(
        "cust_lookup", [](const Row& probe) -> clink::async::Task<Row> {
            Row dim;
            if (auto s = probe.get_string("cust")) {
                const std::int64_t k = std::stoll(*s);
                if (k == 1 || k == 3) {
                    dim.values["id"] = clink::config::JsonValue{static_cast<double>(k)};
                    dim.values["name"] =
                        clink::config::JsonValue{std::string{k == 1 ? "ann" : "cat"}};
                }
            }
            co_return dim;
        });

    auto run = [](const std::string& tag, const std::string& outer_kw) -> std::set<std::string> {
        const auto o_path =
            std::filesystem::temp_directory_path() / ("clink_sql_e2e_lj_o_" + tag + ".ndjson");
        const auto out_path =
            std::filesystem::temp_directory_path() / ("clink_sql_e2e_lj_out_" + tag + ".ndjson");
        std::filesystem::remove(o_path);
        std::filesystem::remove(out_path);
        write_lines(o_path,
                    {R"({"cust":1,"amt":10})", R"({"cust":2,"amt":20})", R"({"cust":3,"amt":30})"});

        Catalog cat;
        auto ddl =
            parse(std::string{"CREATE TABLE orders (cust BIGINT, amt BIGINT) "
                              "WITH (connector='file', format='json', path='"} +
                  o_path.string() +
                  "');"
                  "CREATE TABLE customers (id BIGINT, name TEXT) "
                  "WITH (connector='lookup', function='cust_lookup');"
                  "CREATE TABLE out_t (o_cust BIGINT, o_amt BIGINT, c_id BIGINT, c_name TEXT) "
                  "WITH (connector='file', format='json', path='" +
                  out_path.string() + "')");
        for (const auto& st : ddl.statements)
            cat.register_table(std::get<ast::CreateTableStmt>(st));
        auto spec = compile(cat,
                            ("INSERT INTO out_t SELECT * FROM orders o " + outer_kw +
                             " JOIN customers c ON o.cust = c.id")
                                .c_str());

        InProcessCluster cluster("tm-sql-e2e-lj-" + tag, 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto result = submitter.submit(spec.to_json(), {}, opts);
        EXPECT_TRUE(result.completed) << "reject: " << result.reject_message;
        EXPECT_TRUE(result.ok) << "errors: "
                               << (result.errors.empty() ? "(none)" : result.errors[0]);

        std::set<std::string> rows;
        for (const auto& l : read_lines(out_path)) {
            auto js = clink::config::parse(l);
            const auto cust = static_cast<std::int64_t>(js.at("o_cust").as_number());
            const auto amt = static_cast<std::int64_t>(js.at("o_amt").as_number());
            std::string name = "NULL";
            if (js.contains("c_name") && !js.at("c_name").is_null())
                name = js.at("c_name").as_string();
            rows.insert(std::to_string(cust) + "|" + std::to_string(amt) + "|" + name);
        }
        std::filesystem::remove(o_path);
        std::filesystem::remove(out_path);
        return rows;
    };

    // INNER: cust 2 has no dim match and is dropped.
    EXPECT_EQ(run("inner", ""), (std::set<std::string>{"1|10|ann", "3|30|cat"}));
    // LEFT: cust 2 survives with null dim columns.
    EXPECT_EQ(run("left", "LEFT"), (std::set<std::string>{"1|10|ann", "2|20|NULL", "3|30|cat"}));
}

// Date/time scalar functions per row over an epoch-millis column.
// Anchored instants: ts=1000000000000 -> 2001-09-09 01:46:40 UTC;
// ts=0 -> 1970-01-01 00:00:00. The round-trip column proves
// TO_TIMESTAMP(DATE_FORMAT(ts)) == ts.
TEST(SqlRuntime, DateTimeFunctionsPerRow) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_dt_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_dt_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    write_lines(in_path, {R"({"id":1,"ts":1000000000000})", R"({"id":2,"ts":0})"});

    Catalog cat;
    auto ddl =
        parse(std::string{"CREATE TABLE events (id BIGINT, ts BIGINT) "
                          "WITH (connector='file', format='json', path='"} +
              in_path.string() +
              "');"
              "CREATE TABLE out_t (id BIGINT, yr BIGINT, day_ms BIGINT, fmt TEXT, rt BIGINT) "
              "WITH (connector='file', format='json', path='" +
              out_path.string() + "')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT id, EXTRACT(YEAR FROM ts) AS yr, "
                        "DATE_TRUNC('day', ts) AS day_ms, "
                        "DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm:ss') AS fmt, "
                        "TO_TIMESTAMP(DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm:ss')) AS rt FROM events");

    InProcessCluster cluster("tm-sql-e2e-dt", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    struct Got {
        std::int64_t yr;
        std::int64_t day_ms;
        std::int64_t rt;
        std::string fmt;
    };
    std::map<std::int64_t, Got> by_id;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        const auto id = static_cast<std::int64_t>(js.at("id").as_number());
        by_id[id] = Got{static_cast<std::int64_t>(js.at("yr").as_number()),
                        static_cast<std::int64_t>(js.at("day_ms").as_number()),
                        static_cast<std::int64_t>(js.at("rt").as_number()),
                        js.at("fmt").as_string()};
    }
    ASSERT_EQ(by_id.size(), 2u);
    EXPECT_EQ(by_id[1].yr, 2001);
    EXPECT_EQ(by_id[1].day_ms, 999993600000);
    EXPECT_EQ(by_id[1].fmt, "2001-09-09 01:46:40");
    EXPECT_EQ(by_id[1].rt, 1000000000000);  // round-trip
    EXPECT_EQ(by_id[2].yr, 1970);
    EXPECT_EQ(by_id[2].day_ms, 0);
    EXPECT_EQ(by_id[2].fmt, "1970-01-01 00:00:00");
    EXPECT_EQ(by_id[2].rt, 0);

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// JSON-path functions over a JSON-text payload column. Extraction
// (JSON_VALUE / JSON_QUERY) plus construction (JSON_OBJECT). Row 2 is
// missing fields, exercising the null path.
TEST(SqlRuntime, JsonFunctionsExtractAndConstruct) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_json_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_json_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    // payload is a string column whose value is JSON text.
    write_lines(
        in_path,
        {R"({"id":1,"payload":"{\"user\":{\"name\":\"ann\",\"age\":30},\"tags\":[\"x\",\"y\"]}"})",
         R"({"id":2,"payload":"{\"user\":{\"name\":\"bob\"}}"})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE events (id BIGINT, payload TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE out_t (id BIGINT, nm TEXT, age TEXT, tag1 TEXT, usr TEXT, "
                     "obj TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT id, "
                        "JSON_VALUE(payload, '$.user.name') AS nm, "
                        "JSON_VALUE(payload, '$.user.age') AS age, "
                        "JSON_VALUE(payload, '$.tags[1]') AS tag1, "
                        "JSON_QUERY(payload, '$.user') AS usr, "
                        "JSON_OBJECT('who', JSON_VALUE(payload, '$.user.name')) AS obj "
                        "FROM events");

    InProcessCluster cluster("tm-sql-e2e-json", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto field = [](const clink::config::JsonValue& js, const char* k) -> std::string {
        if (!js.contains(k) || js.at(k).is_null())
            return "NULL";
        return js.at(k).as_string();
    };
    std::map<std::int64_t, std::map<std::string, std::string>> by_id;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        const auto id = static_cast<std::int64_t>(js.at("id").as_number());
        by_id[id] = {{"nm", field(js, "nm")},
                     {"age", field(js, "age")},
                     {"tag1", field(js, "tag1")},
                     {"usr", field(js, "usr")},
                     {"obj", field(js, "obj")}};
    }
    ASSERT_EQ(by_id.size(), 2u);
    EXPECT_EQ(by_id[1]["nm"], "ann");
    EXPECT_EQ(by_id[1]["age"], "30");
    EXPECT_EQ(by_id[1]["tag1"], "y");
    EXPECT_EQ(by_id[1]["usr"], R"({"age":30,"name":"ann"})");
    EXPECT_EQ(by_id[1]["obj"], R"({"who":"ann"})");
    EXPECT_EQ(by_id[2]["nm"], "bob");
    EXPECT_EQ(by_id[2]["age"], "NULL");   // missing field
    EXPECT_EQ(by_id[2]["tag1"], "NULL");  // missing array
    EXPECT_EQ(by_id[2]["usr"], R"({"name":"bob"})");
    EXPECT_EQ(by_id[2]["obj"], R"({"who":"bob"})");

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// COUNT(DISTINCT) and STRING_AGG in a GROUP BY. The aggregate emits a
// changelog and the upsert sink keeps the latest row per key, so the
// file holds the converged aggregate. STRING_AGG output is sorted
// (deterministic) and keeps duplicates.
TEST(SqlRuntime, CountDistinctAndStringAggGroupBy) {
    ensure_sql_installed_once();

    const auto in_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_aggd_in.ndjson";
    const auto out_path = std::filesystem::temp_directory_path() / "clink_sql_e2e_aggd_out.ndjson";
    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
    // k=1: v {10,10,20} -> 2 distinct; s {a,b,a} -> "a|a|b" (sorted).
    // k=2: v {30} -> 1 distinct; s {c} -> "c".
    write_lines(in_path,
                {R"({"k":1,"v":10,"s":"a"})",
                 R"({"k":1,"v":10,"s":"b"})",
                 R"({"k":1,"v":20,"s":"a"})",
                 R"({"k":2,"v":30,"s":"c"})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE t (k BIGINT, v BIGINT, s TEXT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in_path.string() +
                     "');"
                     "CREATE TABLE per_k (k BIGINT, cd BIGINT, sa TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out_path.string() + "', mode='upsert', primary_key='k')");
    for (const auto& st : ddl.statements)
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    auto spec = compile(cat,
                        "INSERT INTO per_k SELECT k, COUNT(DISTINCT v) AS cd, "
                        "STRING_AGG(s, '|') AS sa FROM t GROUP BY k");

    InProcessCluster cluster("tm-sql-e2e-aggd", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Upsert sink: last row per key wins (the converged aggregate).
    std::map<std::int64_t, std::pair<std::int64_t, std::string>> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        const auto k = static_cast<std::int64_t>(js.at("k").as_number());
        got[k] = {static_cast<std::int64_t>(js.at("cd").as_number()), js.at("sa").as_string()};
    }
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[1].first, 2);
    EXPECT_EQ(got[1].second, "a|a|b");
    EXPECT_EQ(got[2].first, 1);
    EXPECT_EQ(got[2].second, "c");

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

// CREATE TABLE ... WITH (connector='parquet') over a multi-column table:
// NDJSON -> typed-columnar Parquet -> NDJSON. Proves the SQL DDL path
// writes externally-typed Parquet (one Arrow column per SQL column) and
// reads it back losslessly.
TEST(SqlRuntime, ParquetTypedColumnarRoundTripEndToEnd) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto in_path = tmp / "clink_sql_pq_in.ndjson";
    const auto pq_path = tmp / "clink_sql_pq_data.parquet";
    const auto out_path = tmp / "clink_sql_pq_out.ndjson";
    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);

    write_lines(in_path,
                {
                    R"({"id":1,"name":"AAPL","px":191.25,"active":true})",
                    R"({"id":2,"name":"MSFT","px":410.10,"active":false})",
                    R"({"id":3,"name":"NVDA","px":1203.99,"active":true})",
                });

    const std::string cols = "(id BIGINT, name VARCHAR, px DOUBLE, active BOOLEAN)";

    // Job 1: NDJSON source -> typed-columnar Parquet sink.
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE ev "} + cols +
                         " WITH (connector='file', format='json', path='" + in_path.string() +
                         "');"
                         "CREATE TABLE pq " +
                         cols + " WITH (connector='parquet', path='" + pq_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat, "INSERT INTO pq SELECT id, name, px, active FROM ev");

        InProcessCluster cluster("tm-sql-pq-write", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto result = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
        EXPECT_TRUE(result.ok) << "errors: "
                               << (result.errors.empty() ? "(none)" : result.errors[0]);
    }

    // The Parquet file is externally typed: one Arrow column per SQL column.
    ASSERT_TRUE(std::filesystem::exists(pq_path)) << pq_path.string();
    {
        auto in = arrow::io::ReadableFile::Open(pq_path.string());
        ASSERT_TRUE(in.ok());
        auto rr = parquet::arrow::OpenFile(*in, arrow::default_memory_pool());
        ASSERT_TRUE(rr.ok());
        std::shared_ptr<arrow::Schema> schema;
        ASSERT_TRUE((*rr)->GetSchema(&schema).ok());
        ASSERT_NE(schema->GetFieldByName("id"), nullptr);
        EXPECT_EQ(schema->GetFieldByName("id")->type()->id(), arrow::Type::INT64);
        EXPECT_EQ(schema->GetFieldByName("name")->type()->id(), arrow::Type::STRING);
        EXPECT_EQ(schema->GetFieldByName("px")->type()->id(), arrow::Type::DOUBLE);
        EXPECT_EQ(schema->GetFieldByName("active")->type()->id(), arrow::Type::BOOL);
    }

    // Job 2: read the Parquet back via a parquet source -> NDJSON sink.
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE pq_in "} + cols +
                         " WITH (connector='parquet', path='" + pq_path.string() +
                         "');"
                         "CREATE TABLE final_t " +
                         cols + " WITH (connector='file', format='json', path='" +
                         out_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat, "INSERT INTO final_t SELECT id, name, px, active FROM pq_in");

        InProcessCluster cluster("tm-sql-pq-read", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto result = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
        EXPECT_TRUE(result.ok) << "errors: "
                               << (result.errors.empty() ? "(none)" : result.errors[0]);
    }

    // The values round-trip through Parquet unchanged.
    auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 3u);
    struct RowOut {
        std::string name;
        double px;
        bool active;
    };
    std::map<std::int64_t, RowOut> got;
    for (const auto& line : lines) {
        auto js = clink::config::parse(line);
        ASSERT_TRUE(js.is_object()) << "bad output line: " << line;
        const auto id = static_cast<std::int64_t>(js.at("id").as_number());
        got[id] = {js.at("name").as_string(), js.at("px").as_number(), js.at("active").as_bool()};
    }
    ASSERT_EQ(got.size(), 3u);
    EXPECT_EQ(got[1].name, "AAPL");
    EXPECT_NEAR(got[1].px, 191.25, 1e-6);
    EXPECT_TRUE(got[1].active);
    EXPECT_EQ(got[2].name, "MSFT");
    EXPECT_NEAR(got[2].px, 410.10, 1e-6);
    EXPECT_FALSE(got[2].active);
    EXPECT_EQ(got[3].name, "NVDA");
    EXPECT_NEAR(got[3].px, 1203.99, 1e-6);
    EXPECT_TRUE(got[3].active);

    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);
}

// Columnar keyed aggregation end-to-end: a typed-columnar Parquet source feeds
// a GROUP BY, driving the columnar SQL path - parquet_row_source emits columnar
// Batch<Row>, the columnar row_compute_key appends __key to the Arrow sidecar,
// and the aggregate's columnar ingest reads the group key + agg inputs straight
// from the buffers. The final per-group aggregates must be exactly correct,
// proving the columnar keyed path is byte-identical to the row path.
TEST(SqlRuntime, ColumnarParquetGroupByEndToEnd) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto in_path = tmp / "clink_sql_cgb_in.ndjson";
    const auto pq_path = tmp / "clink_sql_cgb.parquet";
    const auto out_path = tmp / "clink_sql_cgb_out.ndjson";
    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);

    write_lines(in_path,
                {
                    R"({"region":"eu","amount":10})",
                    R"({"region":"us","amount":60})",
                    R"({"region":"eu","amount":50})",
                    R"({"region":"us","amount":40})",
                    R"({"region":"eu","amount":5})",
                });
    const std::string cols = "(region VARCHAR, amount BIGINT)";

    // Job 1: NDJSON -> typed-columnar Parquet.
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE ev "} + cols +
                         " WITH (connector='file', format='json', path='" + in_path.string() +
                         "');"
                         "CREATE TABLE pq " +
                         cols + " WITH (connector='parquet', path='" + pq_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat, "INSERT INTO pq SELECT region, amount FROM ev");
        InProcessCluster cluster("tm-cgb-write", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }

    // Job 2: columnar Parquet source -> GROUP BY region -> NDJSON.
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE pq_in "} + cols +
                         " WITH (connector='parquet', path='" + pq_path.string() +
                         "');"
                         "CREATE TABLE agg_out (region VARCHAR, total BIGINT, n BIGINT)"
                         " WITH (connector='file', format='json', path='" +
                         out_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat,
                            "INSERT INTO agg_out SELECT region, SUM(amount) AS total, "
                            "COUNT(*) AS n FROM pq_in GROUP BY region");
        InProcessCluster cluster("tm-cgb-agg", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }

    // GROUP BY emits a running upsert per key; the last emission per region is
    // the final aggregate.
    auto lines = read_lines(out_path);
    ASSERT_FALSE(lines.empty());
    std::map<std::string, std::pair<std::int64_t, std::int64_t>> final_agg;
    for (const auto& line : lines) {
        auto js = clink::config::parse(line);
        ASSERT_TRUE(js.is_object()) << "bad output line: " << line;
        final_agg[js.at("region").as_string()] = {
            static_cast<std::int64_t>(js.at("total").as_number()),
            static_cast<std::int64_t>(js.at("n").as_number())};
    }
    ASSERT_EQ(final_agg.size(), 2u);
    EXPECT_EQ(final_agg["eu"].first, 65);  // 10 + 50 + 5
    EXPECT_EQ(final_agg["eu"].second, 3);
    EXPECT_EQ(final_agg["us"].first, 100);  // 60 + 40
    EXPECT_EQ(final_agg["us"].second, 2);

    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);
}

// WS3 within-batch group-by: a columnar Parquet source feeds a GROUP BY whose
// aggregates are ALL batch-foldable (SUM / COUNT / AVG), so each distinct
// group's rows fold in a single pass behind ONE state probe - the
// AggregateRowOp::process_columnar within-batch path. (MIN/MAX are deliberately
// excluded: the SQL GROUP BY factory marks every aggregate retractable, so
// is_batch_foldable rejects them and they take the per-row path - including
// MIN/MAX here would disable the batch fold for the whole op.) The final
// aggregates must be exactly correct: parity with the per-row fold, which
// reuses the same update_agg/finalize_agg.
TEST(SqlRuntime, ColumnarParquetGroupByMultiAggregateBatchFold) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto in_path = tmp / "clink_sql_ws3_in.ndjson";
    const auto pq_path = tmp / "clink_sql_ws3.parquet";
    const auto out_path = tmp / "clink_sql_ws3_out.ndjson";
    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);

    write_lines(in_path,
                {
                    R"({"region":"eu","amount":10})",
                    R"({"region":"us","amount":60})",
                    R"({"region":"eu","amount":50})",
                    R"({"region":"us","amount":40})",
                    R"({"region":"eu","amount":5})",
                });
    const std::string cols = "(region VARCHAR, amount BIGINT)";

    // Job 1: NDJSON -> typed-columnar Parquet.
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE ev "} + cols +
                         " WITH (connector='file', format='json', path='" + in_path.string() +
                         "');"
                         "CREATE TABLE pq " +
                         cols + " WITH (connector='parquet', path='" + pq_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat, "INSERT INTO pq SELECT region, amount FROM ev");
        InProcessCluster cluster("tm-ws3-write", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }

    // Job 2: columnar Parquet source -> GROUP BY with three batch-foldable
    // aggregates (all fold within the batch behind one probe per group).
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE pq_in "} + cols +
                         " WITH (connector='parquet', path='" + pq_path.string() +
                         "');"
                         "CREATE TABLE agg_out (region VARCHAR, total BIGINT, n BIGINT, av DOUBLE)"
                         " WITH (connector='file', format='json', path='" +
                         out_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat,
                            "INSERT INTO agg_out SELECT region, SUM(amount) AS total, "
                            "COUNT(*) AS n, AVG(amount) AS av FROM pq_in GROUP BY region");
        InProcessCluster cluster("tm-ws3-agg", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }

    auto lines = read_lines(out_path);
    ASSERT_FALSE(lines.empty());
    struct Agg {
        std::int64_t total, n;
        double av;
    };
    std::map<std::string, Agg> fa;
    for (const auto& line : lines) {
        auto js = clink::config::parse(line);
        ASSERT_TRUE(js.is_object()) << "bad output line: " << line;
        fa[js.at("region").as_string()] = {static_cast<std::int64_t>(js.at("total").as_number()),
                                           static_cast<std::int64_t>(js.at("n").as_number()),
                                           js.at("av").as_number()};
    }
    ASSERT_EQ(fa.size(), 2u);
    // eu: 10,50,5 -> sum 65, n 3, avg 65/3 (3 rows folded behind one probe).
    EXPECT_EQ(fa["eu"].total, 65);
    EXPECT_EQ(fa["eu"].n, 3);
    EXPECT_NEAR(fa["eu"].av, 65.0 / 3.0, 1e-3);  // JSON round-trip rounds the double
    // us: 60,40 -> sum 100, n 2, avg 50.
    EXPECT_EQ(fa["us"].total, 100);
    EXPECT_EQ(fa["us"].n, 2);
    EXPECT_NEAR(fa["us"].av, 50.0, 1e-3);

    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);
}

// WS6 increment 2: a columnar Parquet source feeds a GROUP BY whose aggregates
// are SUM / AVG / VAR_POP / STDDEV_POP over a DOUBLE column plus COUNT(*) - all
// vectorisable, so AggregateRowOp::process_columnar takes the column-at-a-time
// fold (no per-record Row). Parity oracle for the double + variance kernels: the
// values must equal the row path's, AND batch_materialize_counter must not move
// (proving the vectorised fold ran, not a row-decoding fallback).
TEST(SqlRuntime, ColumnarParquetDoubleVarianceBatchFold) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto in_path = tmp / "clink_sql_ws6v_in.ndjson";
    const auto pq_path = tmp / "clink_sql_ws6v.parquet";
    const auto out_path = tmp / "clink_sql_ws6v_out.ndjson";
    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);

    // eu: [2.5, 6.5] -> sum 9, avg 4.5, var_pop 4, stddev_pop 2, n 2.
    // us: [10, 10, 10] -> sum 30, avg 10, var_pop 0, stddev_pop 0, n 3.
    write_lines(in_path,
                {
                    R"({"region":"eu","amount":2.5})",
                    R"({"region":"us","amount":10})",
                    R"({"region":"eu","amount":6.5})",
                    R"({"region":"us","amount":10})",
                    R"({"region":"us","amount":10})",
                });
    const std::string cols = "(region VARCHAR, amount DOUBLE)";

    // Job 1: NDJSON -> typed-columnar Parquet (amount as a DOUBLE column).
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE ev "} + cols +
                         " WITH (connector='file', format='json', path='" + in_path.string() +
                         "');"
                         "CREATE TABLE pq " +
                         cols + " WITH (connector='parquet', path='" + pq_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat, "INSERT INTO pq SELECT region, amount FROM ev");
        InProcessCluster cluster("tm-ws6v-write", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }

    // Job 2: columnar Parquet source -> GROUP BY region with double SUM/AVG +
    // VAR_POP + STDDEV_POP + COUNT(*) (all vectorisable -> column-at-a-time fold).
    const std::uint64_t mat_before =
        clink::detail::batch_materialize_counter().load(std::memory_order_relaxed);
    {
        Catalog cat;
        auto ddl =
            parse(std::string{"CREATE TABLE pq_in "} + cols + " WITH (connector='parquet', path='" +
                  pq_path.string() +
                  "');"
                  "CREATE TABLE agg_out (region VARCHAR, total DOUBLE, av DOUBLE, vp DOUBLE, "
                  "sd DOUBLE, n BIGINT) WITH (connector='file', format='json', path='" +
                  out_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat,
                            "INSERT INTO agg_out SELECT region, SUM(amount) AS total, "
                            "AVG(amount) AS av, VAR_POP(amount) AS vp, STDDEV_POP(amount) AS sd, "
                            "COUNT(*) AS n FROM pq_in GROUP BY region");
        InProcessCluster cluster("tm-ws6v-agg", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }
    // Zero row decode: the vectorised fold reads Arrow cells directly, so the
    // columnar batch is never materialised to Rows.
    EXPECT_EQ(clink::detail::batch_materialize_counter().load(std::memory_order_relaxed),
              mat_before);

    struct Agg {
        double total, av, vp, sd;
        std::int64_t n;
    };
    std::map<std::string, Agg> fa;
    for (const auto& line : read_lines(out_path)) {
        auto js = clink::config::parse(line);
        ASSERT_TRUE(js.is_object()) << "bad output line: " << line;
        fa[js.at("region").as_string()] = {js.at("total").as_number(),
                                           js.at("av").as_number(),
                                           js.at("vp").as_number(),
                                           js.at("sd").as_number(),
                                           static_cast<std::int64_t>(js.at("n").as_number())};
    }
    ASSERT_EQ(fa.size(), 2u);
    EXPECT_NEAR(fa["eu"].total, 9.0, 1e-9);
    EXPECT_NEAR(fa["eu"].av, 4.5, 1e-9);
    EXPECT_NEAR(fa["eu"].vp, 4.0, 1e-9);
    EXPECT_NEAR(fa["eu"].sd, 2.0, 1e-9);
    EXPECT_EQ(fa["eu"].n, 2);
    EXPECT_NEAR(fa["us"].total, 30.0, 1e-9);
    EXPECT_NEAR(fa["us"].av, 10.0, 1e-9);
    EXPECT_NEAR(fa["us"].vp, 0.0, 1e-9);
    EXPECT_NEAR(fa["us"].sd, 0.0, 1e-9);
    EXPECT_EQ(fa["us"].n, 3);

    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);
}

// WS6 increment 3: a columnar Parquet source feeds a GROUP BY with SUM over a
// DECIMAL(18,2) column. The vectorised fold reads the Decimal128 directly and
// reproduces update_agg's EXACT running_sum_dec accumulation (no dec-string
// round-trip), so the output must be the exact, clean decimal the row path
// produces (DecimalGroupBySumExactAndClean is the row-path twin), with no
// sentinel leak, AND batch_materialize_counter must not move.
TEST(SqlRuntime, ColumnarParquetDecimalSumExact) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto in_path = tmp / "clink_sql_ws6d_in.ndjson";
    const auto pq_path = tmp / "clink_sql_ws6d.parquet";
    const auto out_path = tmp / "clink_sql_ws6d_out.ndjson";
    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);

    // eu: 0.10 + 0.20 -> 0.30 (not 0.300...). us: 10.01 + 0.02 -> 10.03.
    write_lines(in_path,
                {
                    R"({"region":"eu","amount":0.10})",
                    R"({"region":"us","amount":10.01})",
                    R"({"region":"eu","amount":0.20})",
                    R"({"region":"us","amount":0.02})",
                });
    const std::string cols = "(region VARCHAR, amount DECIMAL(18,2))";

    // Job 1: NDJSON -> typed-columnar Parquet (amount as a DECIMAL128(18,2) column).
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE ev "} + cols +
                         " WITH (connector='file', format='json', path='" + in_path.string() +
                         "');"
                         "CREATE TABLE pq " +
                         cols + " WITH (connector='parquet', path='" + pq_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat, "INSERT INTO pq SELECT region, amount FROM ev");
        InProcessCluster cluster("tm-ws6d-write", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }

    // Job 2: columnar Parquet source -> GROUP BY region, SUM(amount) (append).
    const std::uint64_t mat_before =
        clink::detail::batch_materialize_counter().load(std::memory_order_relaxed);
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE pq_in "} + cols +
                         " WITH (connector='parquet', path='" + pq_path.string() +
                         "');"
                         "CREATE TABLE agg_out (region VARCHAR, total DECIMAL(18,2))"
                         " WITH (connector='file', format='json', path='" +
                         out_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat,
                            "INSERT INTO agg_out SELECT region, SUM(amount) AS total "
                            "FROM pq_in GROUP BY region");
        InProcessCluster cluster("tm-ws6d-agg", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }
    EXPECT_EQ(clink::detail::batch_materialize_counter().load(std::memory_order_relaxed),
              mat_before);

    // Append mode emits a running snapshot per group per batch; keep the LAST
    // raw line per region (the final aggregate) and assert the exact text.
    std::map<std::string, std::string> raw;
    for (const auto& l : read_lines(out_path)) {
        EXPECT_EQ(l.find('\x01'), std::string::npos) << "decimal sentinel leaked: " << l;
        auto js = clink::config::parse(l);
        ASSERT_TRUE(js.is_object()) << "bad output line: " << l;
        raw[js.at("region").as_string()] = l;
    }
    ASSERT_EQ(raw.size(), 2u);
    EXPECT_NE(raw["eu"].find("\"total\":0.30"), std::string::npos) << raw["eu"];
    EXPECT_EQ(raw["eu"].find("0.300"), std::string::npos) << "over-precision: " << raw["eu"];
    EXPECT_NE(raw["us"].find("\"total\":10.03"), std::string::npos) << raw["us"];

    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);
}

// Columnar tumbling window end-to-end: a typed-columnar Parquet source feeds a
// TUMBLE window aggregate, exercising the columnar window ingest (the window op
// reads its time / group / agg-input columns straight from the Arrow sidecar).
// The per-window totals must be exactly correct.
TEST(SqlRuntime, ColumnarParquetTumbleWindowEndToEnd) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto in_path = tmp / "clink_sql_ctw_in.ndjson";
    const auto pq_path = tmp / "clink_sql_ctw.parquet";
    const auto out_path = tmp / "clink_sql_ctw_out.ndjson";
    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);

    write_lines(in_path,
                {
                    R"({"user_id":1,"ts":100,"amount":10})",
                    R"({"user_id":1,"ts":200,"amount":20})",
                    R"({"user_id":1,"ts":300,"amount":30})",
                    R"({"user_id":2,"ts":400,"amount":5})",
                    R"({"user_id":1,"ts":1100,"amount":100})",
                    R"({"user_id":1,"ts":1200,"amount":200})",
                    R"({"user_id":2,"ts":1500,"amount":50})",
                });
    const std::string cols = "(user_id BIGINT, ts BIGINT, amount BIGINT)";

    // Job 1: NDJSON -> typed-columnar Parquet.
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE ev "} + cols +
                         " WITH (connector='file', format='json', path='" + in_path.string() +
                         "');"
                         "CREATE TABLE pq " +
                         cols + " WITH (connector='parquet', path='" + pq_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat, "INSERT INTO pq SELECT user_id, ts, amount FROM ev");
        InProcessCluster cluster("tm-ctw-write", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }

    // Job 2: columnar Parquet source (event-time on ts) -> TUMBLE(1000) window.
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE pq_in "} + cols +
                         " WITH (connector='parquet', path='" + pq_path.string() +
                         "', event_time_column='ts');"
                         "CREATE TABLE per_window (user_id BIGINT, total BIGINT) "
                         "WITH (connector='file', format='json', path='" +
                         out_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat,
                            "INSERT INTO per_window SELECT user_id, SUM(amount) AS total "
                            "FROM pq_in GROUP BY TUMBLE(ts, 1000), user_id");
        InProcessCluster cluster("tm-ctw-agg", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }

    auto lines = read_lines(out_path);
    std::multimap<std::int64_t, std::int64_t> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("user_id").as_number()),
                    static_cast<std::int64_t>(js.at("total").as_number()));
    }
    auto contains = [&](std::int64_t uid, std::int64_t total) {
        auto range = got.equal_range(uid);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second == total)
                return true;
        }
        return false;
    };
    EXPECT_TRUE(contains(1, 60)) << "user=1 window=[0,1000) total=60 (10+20+30)";
    EXPECT_TRUE(contains(1, 300)) << "user=1 window=[1000,2000) total=300 (100+200)";
    EXPECT_TRUE(contains(2, 5)) << "user=2 window=[0,1000) total=5";
    EXPECT_TRUE(contains(2, 50)) << "user=2 window=[1000,2000) total=50";

    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);
}

// WS6 window family (end-to-end, with the columnar event-time assigner): a
// Parquet source feeds a HOP window (each record fans into multiple overlapping
// windows) with SUM + COUNT. assign_timestamps_row now forwards the columnar
// batch through (reading event time from the sidecar), so WindowRowOp takes the
// column-at-a-time fold. Exercises the multi-window slicing that distinguishes
// the window fold from the GROUP BY one. Per-window (total, count) must be
// exactly the row path's, AND batch_materialize_counter must NOT move - proving
// the WHOLE pipeline (source -> assigner -> window) stays columnar, zero decode.
TEST(SqlRuntime, ColumnarParquetHopWindowVectorised) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto in_path = tmp / "clink_sql_chw_in.ndjson";
    const auto pq_path = tmp / "clink_sql_chw.parquet";
    const auto out_path = tmp / "clink_sql_chw_out.ndjson";
    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);

    // HOP(ts, size=2000, slide=1000): ts 500 -> [0,2000); 1500 -> [0,2000)+[1000,3000);
    // 2500 -> [1000,3000)+[2000,4000). So windows (by end): 2000 {10+20=30, n2},
    // 3000 {20+30=50, n2}, 4000 {30, n1}.
    write_lines(in_path,
                {
                    R"({"user_id":1,"ts":500,"amount":10})",
                    R"({"user_id":1,"ts":1500,"amount":20})",
                    R"({"user_id":1,"ts":2500,"amount":30})",
                });
    const std::string cols = "(user_id BIGINT, ts BIGINT, amount BIGINT)";

    // Job 1: NDJSON -> typed-columnar Parquet.
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE ev "} + cols +
                         " WITH (connector='file', format='json', path='" + in_path.string() +
                         "');"
                         "CREATE TABLE pq " +
                         cols + " WITH (connector='parquet', path='" + pq_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat, "INSERT INTO pq SELECT user_id, ts, amount FROM ev");
        InProcessCluster cluster("tm-chw-write", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }

    // Job 2: columnar Parquet source -> HOP(2000,1000) window, SUM + COUNT.
    const std::uint64_t mat_before =
        clink::detail::batch_materialize_counter().load(std::memory_order_relaxed);
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE pq_in "} + cols +
                         " WITH (connector='parquet', path='" + pq_path.string() +
                         "', event_time_column='ts');"
                         "CREATE TABLE per_window (user_id BIGINT, total BIGINT, cnt BIGINT) "
                         "WITH (connector='file', format='json', path='" +
                         out_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat,
                            "INSERT INTO per_window SELECT user_id, SUM(amount) AS total, "
                            "COUNT(*) AS cnt FROM pq_in GROUP BY HOP(ts, 2000, 1000), user_id");
        InProcessCluster cluster("tm-chw-agg", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }
    // Whole pipeline stays columnar: the assigner forwards the sidecar and the
    // window op folds it column-at-a-time - no row decode anywhere.
    EXPECT_EQ(clink::detail::batch_materialize_counter().load(std::memory_order_relaxed),
              mat_before);

    // Collect (total, count) pairs; the three overlapping windows are
    // {30,2}, {50,2}, {30,1} (count distinguishes the two total=30 windows).
    std::multiset<std::pair<std::int64_t, std::int64_t>> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("total").as_number()),
                    static_cast<std::int64_t>(js.at("cnt").as_number()));
    }
    const std::multiset<std::pair<std::int64_t, std::int64_t>> want{{30, 2}, {50, 2}, {30, 1}};
    EXPECT_EQ(got, want);

    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);
}

// Wave 2: columnar JSON decode FIRES ON THE PRODUCTION (Kafka) PATH end-to-end.
// We compile the real planner-emitted chain for a kafka columnar_decode='true'
// windowed GROUP BY (q12 shape), then swap the kafka source op for a
// file_text_source reading an NDJSON file - the SAME string channel feeding the
// SAME json_string_to_row_columnar bridge - so the test needs no broker. The
// whole chain (bridge -> assign_timestamps_row -> row_compute_key -> windowed
// aggregate) must stay columnar: batch_materialize_counter must NOT move (zero
// row decode anywhere), and the windowed counts must be correct. This is the
// "fires-on-production" proof: before Wave 2 the Kafka path was row-form and the
// columnar agg fold was dormant.
TEST(SqlRuntime, ColumnarKafkaDecodeFiresOnProductionPath) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto in_path = tmp / "clink_sql_ckd_in.ndjson";
    const auto out_path = tmp / "clink_sql_ckd_out.ndjson";
    for (const auto& p : {in_path, out_path})
        std::filesystem::remove(p);

    // bid-shaped records (q12 subset). Two 10s tumbling windows by datetime(ms):
    // [0,10000): bidder1 x2, bidder2 x1 ; [10000,20000): bidder1 x1, bidder2 x1.
    write_lines(in_path,
                {
                    R"({"bidder":1,"datetime":1000})",
                    R"({"bidder":1,"datetime":2000})",
                    R"({"bidder":2,"datetime":3000})",
                    R"({"bidder":1,"datetime":11000})",
                    R"({"bidder":2,"datetime":12000})",
                });

    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE bid (bidder BIGINT, datetime BIGINT) "
        "WITH (connector='kafka', format='json', brokers='localhost:9092', topic='nx-bid', "
        "group_id='g', auto_offset_reset='earliest', event_time_column='datetime', "
        "columnar_decode='true');"
        "CREATE TABLE out_q (bidder BIGINT, cnt BIGINT) WITH (connector='file', format='json', "
        "path='" +
        out_path.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(cat,
                        "INSERT INTO out_q SELECT bidder, COUNT(*) AS cnt FROM bid "
                        "GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), bidder");

    // Sanity: columnar_decode='true' routed the kafka source through the
    // columnar bridge (not the plain row-form json_string_to_row).
    bool has_columnar_bridge = false;
    for (const auto& op : spec.ops) {
        if (op.type == "json_string_to_row_columnar")
            has_columnar_bridge = true;
        EXPECT_NE(op.type, "json_string_to_row") << "row-form bridge emitted, not columnar";
    }
    ASSERT_TRUE(has_columnar_bridge) << "columnar_decode did not emit the columnar bridge";

    // Swap the kafka source for a file_text_source over the NDJSON file (same
    // string channel, same bridge, same id/inputs) so no broker is needed.
    bool swapped = false;
    for (auto& op : spec.ops) {
        if (op.type == "kafka_source_string") {
            op.type = "file_text_source";
            op.params.clear();
            op.params["path"] = in_path.string();
            swapped = true;
        }
    }
    ASSERT_TRUE(swapped) << "no kafka_source_string op to swap";

    const std::uint64_t mat_before =
        clink::detail::batch_materialize_counter().load(std::memory_order_relaxed);
    {
        InProcessCluster cluster("tm-ckd", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }

    // FIRES ON PRODUCTION: the columnar bridge emitted a sidecar and the whole
    // chain (assigner -> row_compute_key -> windowed aggregate) folded it
    // column-at-a-time - zero row materialisation anywhere.
    EXPECT_EQ(clink::detail::batch_materialize_counter().load(std::memory_order_relaxed),
              mat_before);

    // ...and the windowed counts are correct: (bidder, cnt) per fired window.
    std::multiset<std::pair<std::int64_t, std::int64_t>> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("bidder").as_number()),
                    static_cast<std::int64_t>(js.at("cnt").as_number()));
    }
    const std::multiset<std::pair<std::int64_t, std::int64_t>> want{{1, 2}, {2, 1}, {1, 1}, {2, 1}};
    EXPECT_EQ(got, want);

    for (const auto& p : {in_path, out_path})
        std::filesystem::remove(p);
}

// WS6 increment 5: windowed MAX/MIN over a numeric columnar source vectorises.
// Window aggregates are non-retractable (only GROUP BY marks MIN/MAX retractable),
// so MAX(price)/MIN(price) per window now fold column-at-a-time (no per-record Row)
// instead of the gated per-record-Row ingest. Per-window extremes must be exactly
// the row path's AND batch_materialize_counter must not move.
TEST(SqlRuntime, ColumnarParquetWindowMinMaxVectorised) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto in_path = tmp / "clink_sql_wmm_in.ndjson";
    const auto pq_path = tmp / "clink_sql_wmm.parquet";
    const auto out_path = tmp / "clink_sql_wmm_out.ndjson";
    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);

    // user 1, window [0,1000): price 10,30,20 -> max 30, min 10.
    //         window [1000,2000): price 5,50 -> max 50, min 5.
    write_lines(in_path,
                {
                    R"({"user_id":1,"ts":100,"price":10})",
                    R"({"user_id":1,"ts":200,"price":30})",
                    R"({"user_id":1,"ts":300,"price":20})",
                    R"({"user_id":1,"ts":1100,"price":5})",
                    R"({"user_id":1,"ts":1200,"price":50})",
                });
    const std::string cols = "(user_id BIGINT, ts BIGINT, price BIGINT)";

    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE ev "} + cols +
                         " WITH (connector='file', format='json', path='" + in_path.string() +
                         "');"
                         "CREATE TABLE pq " +
                         cols + " WITH (connector='parquet', path='" + pq_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat, "INSERT INTO pq SELECT user_id, ts, price FROM ev");
        InProcessCluster cluster("tm-wmm-write", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }
    const std::uint64_t mat_before =
        clink::detail::batch_materialize_counter().load(std::memory_order_relaxed);
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE pq_in "} + cols +
                         " WITH (connector='parquet', path='" + pq_path.string() +
                         "', event_time_column='ts');"
                         "CREATE TABLE per_window (user_id BIGINT, mx BIGINT, mn BIGINT) "
                         "WITH (connector='file', format='json', path='" +
                         out_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat,
                            "INSERT INTO per_window SELECT user_id, MAX(price) AS mx, "
                            "MIN(price) AS mn FROM pq_in GROUP BY TUMBLE(ts, 1000), user_id");
        InProcessCluster cluster("tm-wmm-agg", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }
    EXPECT_EQ(clink::detail::batch_materialize_counter().load(std::memory_order_relaxed),
              mat_before);

    std::multiset<std::pair<std::int64_t, std::int64_t>> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("mx").as_number()),
                    static_cast<std::int64_t>(js.at("mn").as_number()));
    }
    const std::multiset<std::pair<std::int64_t, std::int64_t>> want{{30, 10}, {50, 5}};
    EXPECT_EQ(got, want);

    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);
}

// WS6 regression guard: a windowed COUNT(DISTINCT) over a columnar source must
// fall back to the row path (DISTINCT is not column-foldable) and return the true
// per-window distinct cardinality - NOT 0. Guards aggs_vectorisable rejecting
// DISTINCT: the window op gates its vectorised fold ONLY on aggs_vectorisable, so
// without the guard COUNT(DISTINCT) would fold via running_count and finalize to 0.
TEST(SqlRuntime, ColumnarParquetWindowedCountDistinctRowFallback) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto in_path = tmp / "clink_sql_wcd_in.ndjson";
    const auto pq_path = tmp / "clink_sql_wcd.parquet";
    const auto out_path = tmp / "clink_sql_wcd_out.ndjson";
    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);

    // user 1, window [0,1000): bidders {5,5,7} -> distinct 2.
    //         window [1000,2000): bidders {9,9} -> distinct 1.
    write_lines(in_path,
                {
                    R"({"user_id":1,"ts":100,"bidder":5})",
                    R"({"user_id":1,"ts":200,"bidder":5})",
                    R"({"user_id":1,"ts":300,"bidder":7})",
                    R"({"user_id":1,"ts":1100,"bidder":9})",
                    R"({"user_id":1,"ts":1200,"bidder":9})",
                });
    const std::string cols = "(user_id BIGINT, ts BIGINT, bidder BIGINT)";

    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE ev "} + cols +
                         " WITH (connector='file', format='json', path='" + in_path.string() +
                         "');"
                         "CREATE TABLE pq " +
                         cols + " WITH (connector='parquet', path='" + pq_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat, "INSERT INTO pq SELECT user_id, ts, bidder FROM ev");
        InProcessCluster cluster("tm-wcd-write", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE pq_in "} + cols +
                         " WITH (connector='parquet', path='" + pq_path.string() +
                         "', event_time_column='ts');"
                         "CREATE TABLE per_window (user_id BIGINT, d BIGINT) "
                         "WITH (connector='file', format='json', path='" +
                         out_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat,
                            "INSERT INTO per_window SELECT user_id, COUNT(DISTINCT bidder) AS d "
                            "FROM pq_in GROUP BY TUMBLE(ts, 1000), user_id");
        InProcessCluster cluster("tm-wcd-agg", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }
    std::multiset<std::int64_t> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        got.insert(static_cast<std::int64_t>(js.at("d").as_number()));
    }
    const std::multiset<std::int64_t> want{2, 1};  // would be {0,0} if DISTINCT slipped through
    EXPECT_EQ(got, want);

    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);
}

// WS3 within-batch group-by on the SESSION window: a columnar Parquet source
// feeds a SESSION window, so each group's events fold into its session map
// behind ONE state_ probe (SessionWindowRowOp::process_columnar). Session merges
// are order-sensitive; within-group arrival order is preserved, so the merged
// sessions are exactly correct - parity with the per-record path's fold_session_.
TEST(SqlRuntime, ColumnarParquetSessionWindowEndToEnd) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto in_path = tmp / "clink_sql_csw_in.ndjson";
    const auto pq_path = tmp / "clink_sql_csw.parquet";
    const auto out_path = tmp / "clink_sql_csw_out.ndjson";
    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);

    // user=1: {100,200,300} one session + {1000} a separate session (gap > 500).
    // user=2: {400,500} one session.
    write_lines(in_path,
                {
                    R"({"user_id":1,"ts":100})",
                    R"({"user_id":1,"ts":200})",
                    R"({"user_id":1,"ts":300})",
                    R"({"user_id":2,"ts":400})",
                    R"({"user_id":2,"ts":500})",
                    R"({"user_id":1,"ts":1000})",
                });
    const std::string cols = "(user_id BIGINT, ts BIGINT)";

    // Job 1: NDJSON -> typed-columnar Parquet.
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE ev "} + cols +
                         " WITH (connector='file', format='json', path='" + in_path.string() +
                         "');"
                         "CREATE TABLE pq " +
                         cols + " WITH (connector='parquet', path='" + pq_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat, "INSERT INTO pq SELECT user_id, ts FROM ev");
        InProcessCluster cluster("tm-csw-write", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }

    // Job 2: columnar Parquet source (event-time on ts) -> SESSION(500) window.
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE pq_in "} + cols +
                         " WITH (connector='parquet', path='" + pq_path.string() +
                         "', event_time_column='ts');"
                         "CREATE TABLE per_session (user_id BIGINT, hits BIGINT) "
                         "WITH (connector='file', format='json', path='" +
                         out_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat,
                            "INSERT INTO per_session SELECT user_id, COUNT(*) AS hits "
                            "FROM pq_in GROUP BY SESSION(ts, 500), user_id");
        InProcessCluster cluster("tm-csw-agg", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }

    auto lines = read_lines(out_path);
    std::multimap<std::int64_t, std::int64_t> got;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("user_id").as_number()),
                    static_cast<std::int64_t>(js.at("hits").as_number()));
    }
    auto contains = [&](std::int64_t uid, std::int64_t hits) {
        auto range = got.equal_range(uid);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second == hits)
                return true;
        }
        return false;
    };
    EXPECT_TRUE(contains(1, 3)) << "user=1 burst session of 3";
    EXPECT_TRUE(contains(1, 1)) << "user=1 late single-event session";
    EXPECT_TRUE(contains(2, 2)) << "user=2 session of 2";

    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);
}

// WS6 columnar session fold: a Parquet source feeds a SESSION window with
// COUNT + SUM (both vectorisable). Now that assign_timestamps_row forwards the
// columnar batch, SessionWindowRowOp takes the cell-based fold_session_columnar_
// (no per-record Row). Sessions are sequential (merge-on-gap); this asserts the
// merged per-session aggregates are exactly the row path's AND
// batch_materialize_counter does not move (whole source->assigner->session
// pipeline columnar, zero decode).
TEST(SqlRuntime, ColumnarParquetSessionSumVectorised) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto in_path = tmp / "clink_sql_css_in.ndjson";
    const auto pq_path = tmp / "clink_sql_css.parquet";
    const auto out_path = tmp / "clink_sql_css_out.ndjson";
    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);

    // gap 500. user 1: ts 100(10), 300(20) -> one session [100,300] (gap 200<=500):
    // count 2, sum 30. ts 1500(5) -> 1500-300=1200>500 -> new session: count 1, sum 5.
    write_lines(in_path,
                {
                    R"({"user_id":1,"ts":100,"amount":10})",
                    R"({"user_id":1,"ts":300,"amount":20})",
                    R"({"user_id":1,"ts":1500,"amount":5})",
                });
    const std::string cols = "(user_id BIGINT, ts BIGINT, amount BIGINT)";

    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE ev "} + cols +
                         " WITH (connector='file', format='json', path='" + in_path.string() +
                         "');"
                         "CREATE TABLE pq " +
                         cols + " WITH (connector='parquet', path='" + pq_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat, "INSERT INTO pq SELECT user_id, ts, amount FROM ev");
        InProcessCluster cluster("tm-css-write", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }
    const std::uint64_t mat_before =
        clink::detail::batch_materialize_counter().load(std::memory_order_relaxed);
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE pq_in "} + cols +
                         " WITH (connector='parquet', path='" + pq_path.string() +
                         "', event_time_column='ts');"
                         "CREATE TABLE per_session (user_id BIGINT, hits BIGINT, total BIGINT) "
                         "WITH (connector='file', format='json', path='" +
                         out_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat,
                            "INSERT INTO per_session SELECT user_id, COUNT(*) AS hits, "
                            "SUM(amount) AS total FROM pq_in GROUP BY SESSION(ts, 500), user_id");
        InProcessCluster cluster("tm-css-agg", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }
    EXPECT_EQ(clink::detail::batch_materialize_counter().load(std::memory_order_relaxed),
              mat_before);

    std::multiset<std::pair<std::int64_t, std::int64_t>> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        got.emplace(static_cast<std::int64_t>(js.at("hits").as_number()),
                    static_cast<std::int64_t>(js.at("total").as_number()));
    }
    const std::multiset<std::pair<std::int64_t, std::int64_t>> want{{2, 30}, {1, 5}};
    EXPECT_EQ(got, want);

    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);
}

// WS6 increment 5: vectorised session MIN/MAX across a 3-way merge. An
// out-of-order arrival bridges two sessions; the merged session's MIN/MAX must
// combine all three values via merge_into (which the kernel-built running_min/max
// feed identically to the row path). Exercises the vectorised-fold + merge_into
// interaction for non-additive aggregates, with zero row decode.
TEST(SqlRuntime, ColumnarParquetSessionMinMaxMergeVectorised) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto in_path = tmp / "clink_sql_smm_in.ndjson";
    const auto pq_path = tmp / "clink_sql_smm.parquet";
    const auto out_path = tmp / "clink_sql_smm_out.ndjson";
    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);

    // gap 500. Arrivals (in file order): ts 100(price 10), 1000(price 50),
    // 550(price 5). 100 -> session A; 1000 -> session B (gap 900>500); 550 bridges
    // A and B (within gap of both) -> ONE merged session [100,1000] over {10,50,5}:
    // max 50, min 5.
    write_lines(in_path,
                {
                    R"({"user_id":1,"ts":100,"price":10})",
                    R"({"user_id":1,"ts":1000,"price":50})",
                    R"({"user_id":1,"ts":550,"price":5})",
                });
    const std::string cols = "(user_id BIGINT, ts BIGINT, price BIGINT)";

    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE ev "} + cols +
                         " WITH (connector='file', format='json', path='" + in_path.string() +
                         "');"
                         "CREATE TABLE pq " +
                         cols + " WITH (connector='parquet', path='" + pq_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat, "INSERT INTO pq SELECT user_id, ts, price FROM ev");
        InProcessCluster cluster("tm-smm-write", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }
    const std::uint64_t mat_before =
        clink::detail::batch_materialize_counter().load(std::memory_order_relaxed);
    {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE pq_in "} + cols +
                         " WITH (connector='parquet', path='" + pq_path.string() +
                         "', event_time_column='ts');"
                         "CREATE TABLE per_session (user_id BIGINT, mx BIGINT, mn BIGINT) "
                         "WITH (connector='file', format='json', path='" +
                         out_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat,
                            "INSERT INTO per_session SELECT user_id, MAX(price) AS mx, "
                            "MIN(price) AS mn FROM pq_in GROUP BY SESSION(ts, 500), user_id");
        InProcessCluster cluster("tm-smm-agg", 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto r = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
        EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);
    }
    EXPECT_EQ(clink::detail::batch_materialize_counter().load(std::memory_order_relaxed),
              mat_before);

    auto lines = read_lines(out_path);
    // One merged session for user 1: max 50, min 5 (last emission is final).
    ASSERT_FALSE(lines.empty());
    std::pair<std::int64_t, std::int64_t> last{-1, -1};
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        last = {static_cast<std::int64_t>(js.at("mx").as_number()),
                static_cast<std::int64_t>(js.at("mn").as_number())};
    }
    EXPECT_EQ(last.first, 50);
    EXPECT_EQ(last.second, 5);

    for (const auto& p : {in_path, pq_path, out_path})
        std::filesystem::remove(p);
}

// Multi-way (3-table) INNER equi-join: a JOIN b JOIN c binds to a left-deep
// nested EquiJoin tree (the inner join's flat columns pass through the outer
// join unprefixed) and executes end-to-end. The joined output uses the flat
// <alias>_<col> names; aliased here to the sink's columns.
TEST(SqlRuntime, ThreeWayInnerJoinEndToEnd) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto a_path = tmp / "clink_sql_3wj_a.ndjson";
    const auto b_path = tmp / "clink_sql_3wj_b.ndjson";
    const auto c_path = tmp / "clink_sql_3wj_c.ndjson";
    const auto out_path = tmp / "clink_sql_3wj_out.ndjson";
    for (const auto& p : {a_path, b_path, c_path, out_path})
        std::filesystem::remove(p);

    write_lines(a_path, {R"({"id":1,"av":10})", R"({"id":2,"av":20})"});
    write_lines(b_path, {R"({"id":1,"bv":100})", R"({"id":2,"bv":200})"});
    write_lines(c_path, {R"({"id":1,"cv":1000})", R"({"id":2,"cv":2000})"});

    Catalog cat;
    auto mk = [](const std::string& name,
                 const std::string& cols,
                 const std::string& path,
                 const std::string& stats) {
        return "CREATE TABLE " + name + " " + cols +
               " WITH (connector='file', format='json', path='" + path + "', " + stats + ");";
    };
    // Skewed declared stats so cost-based reordering DOES fire (drive with the
    // smallest, c): syntactic order is a,b,c. The actual data is tiny and the
    // reorder must still produce identical results - this is the equivalence
    // proof for the reordered nested-join tree.
    auto ddl = parse(
        mk("a",
           "(id BIGINT, av BIGINT)",
           a_path.string(),
           "row_count='1000000', ndv_id='1000000'") +
        mk("b", "(id BIGINT, bv BIGINT)", b_path.string(), "row_count='1000', ndv_id='1000'") +
        mk("c", "(id BIGINT, cv BIGINT)", c_path.string(), "row_count='10', ndv_id='10'") +
        "CREATE TABLE out_t (av BIGINT, bv BIGINT, cv BIGINT) "
        "WITH (connector='file', format='json', path='" +
        out_path.string() + "')");
    for (int i = 0; i < 4; ++i)
        cat.register_table(
            std::get<ast::CreateTableStmt>(ddl.statements[static_cast<std::size_t>(i)]));

    // a JOIN b ON a.id=b.id JOIN c ON b.id=c.id; alias the flat join columns to
    // the sink's declared column names.
    auto spec = compile(cat,
                        "INSERT INTO out_t SELECT a_av AS av, b_bv AS bv, c_cv AS cv FROM a "
                        "JOIN b ON a.id = b.id JOIN c ON b.id = c.id");

    InProcessCluster cluster("tm-3wj", 12);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto r = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(r.completed) << "reject: " << r.reject_message;
    EXPECT_TRUE(r.ok) << "errors: " << (r.errors.empty() ? "(none)" : r.errors[0]);

    std::map<std::int64_t, std::pair<std::int64_t, std::int64_t>> got;  // av -> (bv, cv)
    for (const auto& line : read_lines(out_path)) {
        auto js = clink::config::parse(line);
        ASSERT_TRUE(js.is_object()) << line;
        got[static_cast<std::int64_t>(js.at("av").as_number())] = {
            static_cast<std::int64_t>(js.at("bv").as_number()),
            static_cast<std::int64_t>(js.at("cv").as_number())};
    }
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[10], (std::pair<std::int64_t, std::int64_t>{100, 1000}));
    EXPECT_EQ(got[20], (std::pair<std::int64_t, std::int64_t>{200, 2000}));

    for (const auto& p : {a_path, b_path, c_path, out_path})
        std::filesystem::remove(p);
}

// Async-state SQL GROUP BY: with the planner's async-state switch on, the
// aggregate holds per-group state in KeyedState (serialising every AggBucket
// through the codec on each record) instead of the in-memory map. Running the
// same GROUP BY both ways must yield identical final aggregates - which proves
// the AggState/AggBucket codec round-trips every accumulator field. Covers
// SUM (+exact-decimal), COUNT, MIN/MAX (multiset), AVG, COUNT(DISTINCT).
TEST(SqlRuntime, AsyncStateGroupByMatchesInMemory) {
    ensure_sql_installed_once();

    const auto tmp = std::filesystem::temp_directory_path();
    const auto in_path = tmp / "clink_sql_aggasync_in.ndjson";
    const auto sync_out = tmp / "clink_sql_aggasync_sync.ndjson";
    const auto async_out = tmp / "clink_sql_aggasync_async.ndjson";
    for (const auto& p : {in_path, sync_out, async_out})
        std::filesystem::remove(p);

    write_lines(in_path,
                {
                    R"({"user_id":1,"amount":10})",
                    R"({"user_id":2,"amount":20})",
                    R"({"user_id":1,"amount":30})",
                    R"({"user_id":2,"amount":5})",
                    R"({"user_id":1,"amount":10})",  // repeat -> COUNT(DISTINCT) < COUNT
                    R"({"user_id":1,"amount":7})",
                    R"({"user_id":2,"amount":20})",  // repeat
                });

    const std::string select =
        "SELECT user_id, SUM(amount) AS s, COUNT(*) AS c, MIN(amount) AS mn, "
        "MAX(amount) AS mx, AVG(amount) AS av, COUNT(DISTINCT amount) AS cd "
        "FROM orders GROUP BY user_id";

    auto run = [&](const std::filesystem::path& out_path, bool async_agg, const char* tm_id) {
        Catalog cat;
        auto ddl = parse(std::string{"CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                                     "WITH (connector='file', format='json', path='"} +
                         in_path.string() +
                         "');"
                         "CREATE TABLE dst (user_id BIGINT, s BIGINT, c BIGINT, mn BIGINT, "
                         "mx BIGINT, av DOUBLE, cd BIGINT) "
                         "WITH (connector='file', format='json', path='" +
                         out_path.string() + "')");
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
        auto spec = compile(cat, ("INSERT INTO dst " + select).c_str(), async_agg);
        InProcessCluster cluster(tm_id, 8);
        application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
        application::SubmitOptions opts;
        opts.wait_timeout = 15s;
        auto result = submitter.submit(spec.to_json(), {}, opts);
        ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
        ASSERT_TRUE(result.ok) << "errors: "
                               << (result.errors.empty() ? "(none)" : result.errors[0]);
    };

    run(sync_out, /*async_agg=*/false, "tm-agg-sync");
    run(async_out, /*async_agg=*/true, "tm-agg-async");

    // Last emission per group is its final aggregate (upsert stream). Order
    // across keys is not guaranteed, so compare per-key finals, not raw lines.
    auto finals = [](const std::vector<std::string>& lines) {
        std::map<std::int64_t, clink::config::JsonValue> m;
        for (const auto& l : lines) {
            auto j = clink::config::parse(l);
            if (j.is_object())
                m[static_cast<std::int64_t>(j.at("user_id").as_number())] = j;
        }
        return m;
    };
    auto sync_finals = finals(read_lines(sync_out));
    auto async_finals = finals(read_lines(async_out));

    // Expected in-memory values (sanity, so we are not just comparing two wrongs):
    //   user 1: amounts 10,30,10,7 -> s=57 c=4 mn=7 mx=30 av=14.25 cd=3
    //   user 2: amounts 20,5,20    -> s=45 c=3 mn=5 mx=20 av=15.0  cd=2
    ASSERT_EQ(sync_finals.size(), 2u);
    EXPECT_EQ(sync_finals[1].at("s").as_number(), 57);
    EXPECT_EQ(sync_finals[1].at("c").as_number(), 4);
    EXPECT_EQ(sync_finals[1].at("mn").as_number(), 7);
    EXPECT_EQ(sync_finals[1].at("mx").as_number(), 30);
    EXPECT_NEAR(sync_finals[1].at("av").as_number(), 14.25, 1e-9);
    EXPECT_EQ(sync_finals[1].at("cd").as_number(), 3);
    EXPECT_EQ(sync_finals[2].at("s").as_number(), 45);
    EXPECT_EQ(sync_finals[2].at("cd").as_number(), 2);

    // The codec proof: async-state (KeyedState) finals match in-memory finals.
    ASSERT_EQ(async_finals.size(), sync_finals.size());
    for (const auto& [k, jrow] : sync_finals) {
        ASSERT_TRUE(async_finals.count(k)) << "missing group " << k;
        const auto& jb = async_finals.at(k);
        for (const char* f : {"s", "c", "mn", "mx", "cd"}) {
            EXPECT_EQ(jrow.at(f).as_number(), jb.at(f).as_number())
                << "group " << k << " field " << f;
        }
        EXPECT_NEAR(jrow.at("av").as_number(), jb.at("av").as_number(), 1e-9) << "group " << k;
    }

    for (const auto& p : {in_path, sync_out, async_out})
        std::filesystem::remove(p);
}

// ANALYZE parses (both the PG `ANALYZE t` and the Flink-style `ANALYZE TABLE t`,
// the latter via the pre-parser strip), carrying the table + optional columns.
TEST(SqlAnalyzeStmt, ParsesTableAndColumns) {
    auto s1 = parse("ANALYZE TABLE orders");
    ASSERT_EQ(s1.statements.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<ast::AnalyzeStmt>(s1.statements[0]));
    EXPECT_EQ(std::get<ast::AnalyzeStmt>(s1.statements[0]).table, "orders");
    EXPECT_TRUE(std::get<ast::AnalyzeStmt>(s1.statements[0]).columns.empty());

    auto s2 = parse("ANALYZE orders (amount)");  // PG spelling + a column list
    ASSERT_TRUE(std::holds_alternative<ast::AnalyzeStmt>(s2.statements[0]));
    const auto& a = std::get<ast::AnalyzeStmt>(s2.statements[0]);
    EXPECT_EQ(a.table, "orders");
    ASSERT_EQ(a.columns.size(), 1u);
    EXPECT_EQ(a.columns[0], "amount");
}

// End to end: ANALYZE scans a bounded file/json table, writes exact statistics
// into the catalog, and those stats drive the optimizer's selectivity estimate.
TEST(SqlAnalyzeStmt, ScansBoundedTableAndStatsDriveSelectivity) {
    ensure_sql_installed_once();
    const auto in_path = std::filesystem::temp_directory_path() / "clink_analyze_orders.ndjson";
    std::vector<std::string> rows;
    for (int i = 0; i < 8; ++i) {  // status skewed: 8x200, 2x404
        rows.push_back(R"({"status":200,"amount":)" + std::to_string(i) + "}");
    }
    for (int i = 8; i < 10; ++i) {
        rows.push_back(R"({"status":404,"amount":)" + std::to_string(i) + "}");
    }
    write_lines(in_path, rows);

    Catalog cat;
    cat.register_table(std::get<ast::CreateTableStmt>(
        parse(std::string("CREATE TABLE orders (status BIGINT, amount BIGINT) "
                          "WITH (connector='file', format='json', path='") +
              in_path.string() + "')")
            .statements[0]));

    EXPECT_FALSE(cat.get_table("orders")->properties.count("row_count"))
        << "no stats before ANALYZE";

    analyze_table(cat, "orders");

    const auto& props = cat.get_table("orders")->properties;
    EXPECT_EQ(props.at("row_count"), "10");
    EXPECT_EQ(props.at("ndv_status"), "2");
    EXPECT_NE(props.at("mcv_status").find("200:0.8"), std::string::npos)
        << "got mcv_status=" << props.at("mcv_status");
    EXPECT_FALSE(props.at("hist_amount").empty());

    // The analyzed stats drive selectivity: status=200 -> MCV 0.8 -> 10*0.8 = 8.
    Binder b(cat);
    auto plan = b.bind_select(
        std::get<ast::SelectStmt>(parse("SELECT * FROM orders WHERE status = 200").statements[0]));
    EXPECT_DOUBLE_EQ(estimate_rows(*plan), 8.0);

    std::filesystem::remove(in_path);
}

// ANALYZE rejects an unbounded source (a Kafka string stream needs a bridge and
// never terminates) rather than hanging on the scan.
TEST(SqlAnalyzeStmt, RejectsUnboundedSource) {
    ensure_sql_installed_once();
    Catalog cat;
    cat.register_table(std::get<ast::CreateTableStmt>(
        parse("CREATE TABLE s (k BIGINT) WITH (connector='kafka', format='json', topic='t')")
            .statements[0]));
    EXPECT_THROW(analyze_table(cat, "s"), TranslationError);
    EXPECT_THROW(analyze_table(cat, "no_such_table"), TranslationError);
}

#ifdef CLINK_TESTS_HAVE_VECTOR_SEARCH
// SQL-native AI end to end: VECTOR_SEARCH loads a bounded corpus at open(), then
// emits each query row joined to its top-k nearest corpus rows + a score. The
// operator runs on the in-process cluster; the corpus file source is loaded via the
// same registry the SQL Row sources register into.
TEST(SqlRuntime, VectorSearchTopKEndToEnd) {
    ensure_sql_installed_once();
    const auto docs = std::filesystem::temp_directory_path() / "clink_vs_docs.ndjson";
    const auto queries = std::filesystem::temp_directory_path() / "clink_vs_queries.ndjson";
    const auto out = std::filesystem::temp_directory_path() / "clink_vs_out.ndjson";
    std::filesystem::remove(docs);
    std::filesystem::remove(queries);
    std::filesystem::remove(out);
    write_lines(docs,
                {R"({"doc_id":1,"vec":[1.0,0.0],"title":"east"})",
                 R"({"doc_id":2,"vec":[0.0,1.0],"title":"north"})",
                 R"({"doc_id":3,"vec":[1.0,1.0],"title":"ne"})"});
    write_lines(queries, {R"({"qid":1,"emb":[1.0,0.1]})"});

    Catalog cat;
    auto ddl = parse(std::string{"CREATE TABLE q (qid BIGINT, emb DOUBLE PRECISION ARRAY) "
                                 "WITH (connector='file', format='json', path='"} +
                     queries.string() +
                     "');"
                     "CREATE TABLE docs (doc_id BIGINT, vec DOUBLE PRECISION ARRAY, title TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     docs.string() +
                     "');"
                     "CREATE TABLE vout (qid BIGINT, emb DOUBLE PRECISION ARRAY, doc_id BIGINT, "
                     "vec DOUBLE PRECISION ARRAY, title TEXT, score DOUBLE PRECISION) "
                     "WITH (connector='file', format='json', path='" +
                     out.string() + "')");
    for (int i = 0; i < 3; ++i) {
        cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[i]));
    }
    auto spec = compile(cat,
                        "INSERT INTO vout SELECT * FROM VECTOR_SEARCH("
                        "TABLE q, emb, docs, DESCRIPTOR(vec), 1, metric='cosine')");

    InProcessCluster cluster("tm-sql-vs", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out);
    ASSERT_EQ(lines.size(), 1U) << "expected one top-1 hit for the single query";
    auto js = clink::config::parse(lines[0]);
    ASSERT_TRUE(js.is_object());
    EXPECT_EQ(static_cast<std::int64_t>(js.at("qid").as_number()), 1);
    EXPECT_EQ(static_cast<std::int64_t>(js.at("doc_id").as_number()), 1);  // [1,0] is nearest
    EXPECT_EQ(js.at("title").as_string(), "east");
    EXPECT_GT(js.at("score").as_number(), 0.9);  // cosine similarity ~0.995
    std::filesystem::remove(docs);
    std::filesystem::remove(queries);
    std::filesystem::remove(out);
}
#endif  // CLINK_TESTS_HAVE_VECTOR_SEARCH

// SQL-native AI end to end: ML_PREDICT applies a registered model per row and appends
// its OUTPUT columns. Uses the in-process "test_double" closure provider (no network),
// so it exercises the full CREATE MODEL -> bind -> ml_predict_row -> provider path.
TEST(SqlRuntime, MlPredictAppendsModelOutputEndToEnd) {
    ensure_sql_installed_once();
    const auto in = std::filesystem::temp_directory_path() / "clink_mlp_in.ndjson";
    const auto out = std::filesystem::temp_directory_path() / "clink_mlp_out.ndjson";
    std::filesystem::remove(in);
    std::filesystem::remove(out);
    write_lines(in, {R"({"id":1,"n":21})", R"({"id":2,"n":10})"});

    Catalog cat;
    // The model is a catalog object; ML_PREDICT reads its OUTPUT columns + provider at
    // bind time. provider='test_double' resolves to the in-process closure provider.
    auto model_ddl = parse(
        "CREATE MODEL doubler INPUT (n BIGINT) OUTPUT (doubled DOUBLE PRECISION, tag TEXT) "
        "WITH (provider='test_double')");
    cat.register_model(std::get<ast::CreateModelStmt>(model_ddl.statements[0]));
    auto ddl = parse(std::string{"CREATE TABLE nums (id BIGINT, n BIGINT) "
                                 "WITH (connector='file', format='json', path='"} +
                     in.string() +
                     "');"
                     "CREATE TABLE mlout (id BIGINT, n BIGINT, doubled DOUBLE PRECISION, tag TEXT) "
                     "WITH (connector='file', format='json', path='" +
                     out.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[1]));
    auto spec = compile(
        cat,
        "INSERT INTO mlout SELECT * FROM ML_PREDICT(TABLE nums, MODEL doubler, DESCRIPTOR(n))");

    InProcessCluster cluster("tm-sql-mlp", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto result = submitter.submit(spec.to_json(), {}, opts);
    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    auto lines = read_lines(out);
    ASSERT_EQ(lines.size(), 2U);
    std::map<std::int64_t, double> doubled_by_id;
    std::map<std::int64_t, std::string> tag_by_id;
    for (const auto& l : lines) {
        auto js = clink::config::parse(l);
        ASSERT_TRUE(js.is_object());
        const auto id = static_cast<std::int64_t>(js.at("id").as_number());
        doubled_by_id[id] = js.at("doubled").as_number();
        tag_by_id[id] = js.at("tag").as_string();
    }
    EXPECT_EQ(doubled_by_id[1], 42);  // 21 * 2
    EXPECT_EQ(doubled_by_id[2], 20);  // 10 * 2
    EXPECT_EQ(tag_by_id[1], "ok");
    EXPECT_EQ(tag_by_id[2], "ok");
    std::filesystem::remove(in);
    std::filesystem::remove(out);
}

// Materialized tables (full-refresh arm): a FRESHNESS > 0 view recomputes its whole
// result and atomically overwrites the backing. The initial CREATE population and a
// later REFRESH both run as bounded jobs; the overwrite sink publishes on completion,
// so the backing file reflects exactly the current source each time.
TEST(SqlRuntime, MaterializedViewFullRefreshOverwritesAtomically) {
    ensure_sql_installed_once();
    const auto src = std::filesystem::temp_directory_path() / "clink_mv_src.ndjson";
    const auto out = std::filesystem::temp_directory_path() / "clink_mv_out.ndjson";
    std::filesystem::remove(src);
    std::filesystem::remove(out);
    std::filesystem::remove(std::filesystem::path(out.string() + ".staging"));
    write_lines(src, {R"({"id":1,"val":5})", R"({"id":2,"val":20})", R"({"id":3,"val":30})"});

    Catalog cat;
    auto src_ddl = parse(std::string{"CREATE TABLE src (id BIGINT, val BIGINT) "
                                     "WITH (connector='file', format='json', path='"} +
                         src.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(src_ddl.statements[0]));

    const std::string mv_sql =
        std::string{
            "CREATE MATERIALIZED VIEW mv "
            "WITH (connector='file', format='json', freshness='1h', path='"} +
        out.string() + "') AS SELECT id, val FROM src WHERE val > 10";
    auto mv_script = parse(mv_sql);
    auto mvplan = plan_materialized_view(
        std::get<ast::CreateMaterializedViewStmt>(std::move(mv_script.statements[0])), cat, mv_sql);
    EXPECT_EQ(mvplan.arm, RefreshArm::Full);
    EXPECT_EQ(cat.get_table("mv")->properties.at("write_mode"), "overwrite");

    InProcessCluster cluster("tm-sql-mv", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;

    auto submit_plan = [&](std::unique_ptr<LogicalPlan> plan) {
        auto optimised = optimize(std::move(plan));
        PhysicalPlanner pp;
        auto spec = pp.compile(static_cast<const LogicalSink&>(*optimised));
        auto result = submitter.submit(spec.to_json(), {}, opts);
        EXPECT_TRUE(result.completed) << "reject: " << result.reject_message;
        EXPECT_TRUE(result.ok) << "errors: "
                               << (result.errors.empty() ? "(none)" : result.errors[0]);
    };
    auto ids_in_out = [&]() {
        std::set<std::int64_t> ids;
        for (const auto& l : read_lines(out)) {
            ids.insert(static_cast<std::int64_t>(clink::config::parse(l).at("id").as_number()));
        }
        return ids;
    };

    // Initial population: only val > 10 (ids 2, 3).
    submit_plan(std::move(mvplan.maintenance));
    EXPECT_EQ(ids_in_out(), (std::set<std::int64_t>{2, 3}));

    // Change the source, then REFRESH: the backing is atomically overwritten to
    // reflect the new source (only id 4 has val > 10).
    write_lines(src, {R"({"id":4,"val":50})", R"({"id":5,"val":1})"});
    submit_plan(plan_materialized_view_refresh("mv", cat));
    EXPECT_EQ(ids_in_out(), (std::set<std::int64_t>{4}));

    std::filesystem::remove(src);
    std::filesystem::remove(out);
}

// The RefreshScheduler drives the recompute automatically at the freshness cadence:
// with a short freshness, changing the source and waiting (no manual REFRESH) is
// enough for the backing to be atomically overwritten to the new result.
TEST(SqlRuntime, MaterializedViewSchedulerAutoRefreshes) {
    ensure_sql_installed_once();
    const auto src = std::filesystem::temp_directory_path() / "clink_mvsched_src.ndjson";
    const auto out = std::filesystem::temp_directory_path() / "clink_mvsched_out.ndjson";
    std::filesystem::remove(src);
    std::filesystem::remove(out);
    write_lines(src, {R"({"id":1,"val":5})", R"({"id":2,"val":20})"});

    Catalog cat;
    auto src_ddl = parse(std::string{"CREATE TABLE ssrc (id BIGINT, val BIGINT) "
                                     "WITH (connector='file', format='json', path='"} +
                         src.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(src_ddl.statements[0]));
    const std::string mv_sql =
        std::string{
            "CREATE MATERIALIZED VIEW smv "
            "WITH (connector='file', format='json', freshness='300ms', path='"} +
        out.string() + "') AS SELECT id, val FROM ssrc WHERE val > 10";
    auto mv_script = parse(mv_sql);
    auto mvplan = plan_materialized_view(
        std::get<ast::CreateMaterializedViewStmt>(std::move(mv_script.statements[0])), cat, mv_sql);
    ASSERT_EQ(mvplan.arm, RefreshArm::Full);

    InProcessCluster cluster("tm-sql-mvsched", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto ids_in_out = [&]() {
        std::set<std::int64_t> ids;
        for (const auto& l : read_lines(out)) {
            ids.insert(static_cast<std::int64_t>(clink::config::parse(l).at("id").as_number()));
        }
        return ids;
    };

    // Initial population.
    {
        auto plan = optimize(std::move(mvplan.maintenance));
        PhysicalPlanner pp;
        auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
        ASSERT_TRUE(submitter.submit(spec.to_json(), {}, opts).completed);
    }
    EXPECT_EQ(ids_in_out(), (std::set<std::int64_t>{2}));

    // Register the view with a scheduler whose callback recompiles + submits + waits.
    clink::cluster::RefreshSchedulerConfig scfg;
    scfg.tick_period = 100ms;
    clink::cluster::RefreshScheduler sched(scfg);
    sched.register_view("smv", 300ms, [&]() {
        auto plan = optimize(plan_materialized_view_refresh("smv", cat));
        PhysicalPlanner pp;
        auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
        (void)submitter.submit(spec.to_json(), {}, opts);
    });

    // Change the source; the scheduler should refresh the backing to {4} on its own.
    write_lines(src, {R"({"id":4,"val":50})", R"({"id":5,"val":1})"});
    sched.start();
    for (int i = 0; i < 60 && ids_in_out() != std::set<std::int64_t>{4}; ++i) {
        std::this_thread::sleep_for(100ms);
    }
    sched.stop();
    EXPECT_EQ(ids_in_out(), (std::set<std::int64_t>{4}));
    EXPECT_GE(sched.refreshes(), 1U);

    std::filesystem::remove(src);
    std::filesystem::remove(out);
}

// Materialized tables with PARTITIONED BY: a full-refresh view whose backing carries a
// partition_by writes one file per distinct partition value into a directory and swaps
// the whole partitioned set atomically on completion. Here the view partitions by
// `region`, so the published directory holds one file per region, each containing only
// that region's rows.
TEST(SqlRuntime, MaterializedViewPartitionedFullRefresh) {
    ensure_sql_installed_once();
    const auto src = std::filesystem::temp_directory_path() / "clink_mvpart_src.ndjson";
    const auto out = std::filesystem::temp_directory_path() / "clink_mvpart_out";
    std::filesystem::remove(src);
    std::filesystem::remove_all(out);
    std::filesystem::remove_all(std::filesystem::path(out.string() + ".staging"));
    write_lines(src,
                {R"({"region":"eu","amt":10})",
                 R"({"region":"us","amt":20})",
                 R"({"region":"eu","amt":5})"});

    Catalog cat;
    auto src_ddl = parse(std::string{"CREATE TABLE psrc (region VARCHAR, amt BIGINT) "
                                     "WITH (connector='file', format='json', path='"} +
                         src.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(src_ddl.statements[0]));

    const std::string mv_sql =
        std::string{
            "CREATE MATERIALIZED VIEW pmv "
            "WITH (connector='file', format='json', freshness='1h', partition_by='region', "
            "path='"} +
        out.string() + "') AS SELECT region, amt FROM psrc";
    auto mv_script = parse(mv_sql);
    auto mvplan = plan_materialized_view(
        std::get<ast::CreateMaterializedViewStmt>(std::move(mv_script.statements[0])), cat, mv_sql);
    EXPECT_EQ(mvplan.arm, RefreshArm::Full);
    EXPECT_EQ(cat.get_table("pmv")->properties.at("write_mode"), "overwrite");
    EXPECT_EQ(cat.get_table("pmv")->properties.at("partition_by"), "region");

    InProcessCluster cluster("tm-sql-mvpart", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;

    {
        auto plan = optimize(std::move(mvplan.maintenance));
        PhysicalPlanner pp;
        auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
        auto result = submitter.submit(spec.to_json(), {}, opts);
        EXPECT_TRUE(result.completed) << "reject: " << result.reject_message;
        EXPECT_TRUE(result.ok) << "errors: "
                               << (result.errors.empty() ? "(none)" : result.errors[0]);
    }

    // The published directory holds one file per region; each file carries only that
    // region's rows (eu has two, us has one).
    ASSERT_TRUE(std::filesystem::is_directory(out)) << "expected partitioned dir " << out;
    EXPECT_TRUE(std::filesystem::exists(out / "eu"));
    EXPECT_TRUE(std::filesystem::exists(out / "us"));
    auto amts_in = [&](const std::string& region) {
        std::multiset<std::int64_t> amts;
        for (const auto& l : read_lines(out / region)) {
            amts.insert(static_cast<std::int64_t>(clink::config::parse(l).at("amt").as_number()));
        }
        return amts;
    };
    EXPECT_EQ(amts_in("eu"), (std::multiset<std::int64_t>{5, 10}));
    EXPECT_EQ(amts_in("us"), (std::multiset<std::int64_t>{20}));

    std::filesystem::remove(src);
    std::filesystem::remove_all(out);
}

// Materialized tables (full-refresh arm) over an AGGREGATING defining query. A keyed
// GROUP BY auto-derives an upsert backing; the bounded recompute's changelog is netted
// by primary key and the final aggregate per key is written atomically on each refresh.
TEST(SqlRuntime, MaterializedViewFullRefreshAggregateNetsByKey) {
    ensure_sql_installed_once();
    const auto src = std::filesystem::temp_directory_path() / "clink_mvagg_src.ndjson";
    const auto out = std::filesystem::temp_directory_path() / "clink_mvagg_out.ndjson";
    std::filesystem::remove(src);
    std::filesystem::remove(out);
    std::filesystem::remove(std::filesystem::path(out.string() + ".tmp"));
    write_lines(src,
                {R"({"region":"eu","amt":10})",
                 R"({"region":"us","amt":20})",
                 R"({"region":"eu","amt":5})"});

    Catalog cat;
    auto src_ddl = parse(std::string{"CREATE TABLE asrc (region VARCHAR, amt BIGINT) "
                                     "WITH (connector='file', format='json', path='"} +
                         src.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(src_ddl.statements[0]));

    const std::string mv_sql =
        std::string{
            "CREATE MATERIALIZED VIEW amv "
            "WITH (connector='file', format='json', freshness='1h', path='"} +
        out.string() + "') AS SELECT region, COUNT(*) AS cnt FROM asrc GROUP BY region";
    auto mv_script = parse(mv_sql);
    auto mvplan = plan_materialized_view(
        std::get<ast::CreateMaterializedViewStmt>(std::move(mv_script.statements[0])), cat, mv_sql);
    EXPECT_EQ(mvplan.arm, RefreshArm::Full);
    EXPECT_EQ(cat.get_table("amv")->properties.at("mode"), "upsert");
    EXPECT_EQ(cat.get_table("amv")->properties.at("primary_key"), "region");

    InProcessCluster cluster("tm-sql-mvagg", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto submit_plan = [&](std::unique_ptr<LogicalPlan> plan) {
        auto optimised = optimize(std::move(plan));
        PhysicalPlanner pp;
        auto spec = pp.compile(static_cast<const LogicalSink&>(*optimised));
        auto result = submitter.submit(spec.to_json(), {}, opts);
        EXPECT_TRUE(result.completed) << "reject: " << result.reject_message;
        EXPECT_TRUE(result.ok) << "errors: "
                               << (result.errors.empty() ? "(none)" : result.errors[0]);
    };
    auto counts_in_out = [&]() {
        std::map<std::string, std::int64_t> by_region;
        for (const auto& l : read_lines(out)) {
            auto row = clink::config::parse(l);
            by_region[row.at("region").as_string()] =
                static_cast<std::int64_t>(row.at("cnt").as_number());
        }
        return by_region;
    };

    // Initial population: eu has 2 rows, us has 1.
    submit_plan(std::move(mvplan.maintenance));
    EXPECT_EQ(counts_in_out(), (std::map<std::string, std::int64_t>{{"eu", 2}, {"us", 1}}));

    // Change the source, then REFRESH: the backing is atomically overwritten to the new
    // aggregate (eu 1, ap 2). The stale us key is gone - a full recompute, not a merge.
    write_lines(
        src,
        {R"({"region":"eu","amt":1})", R"({"region":"ap","amt":2})", R"({"region":"ap","amt":3})"});
    submit_plan(plan_materialized_view_refresh("amv", cat));
    EXPECT_EQ(counts_in_out(), (std::map<std::string, std::int64_t>{{"eu", 1}, {"ap", 2}}));

    std::filesystem::remove(src);
    std::filesystem::remove(out);
}

// A table over a DIRECTORY reads every file under it: connector='file' with a directory
// path auto-detects and reads the whole set, so a partitioned materialized-view backing
// (one file per partition, as partition_overwrite_sink writes) can be read straight back
// into a downstream query.
TEST(SqlRuntime, FileSourceReadsBackPartitionedDirectory) {
    ensure_sql_installed_once();
    const auto dir = std::filesystem::temp_directory_path() / "clink_readback_dir";
    const auto out = std::filesystem::temp_directory_path() / "clink_readback_out.ndjson";
    std::filesystem::remove_all(dir);
    std::filesystem::remove(out);
    std::filesystem::create_directories(dir);
    // A partitioned backing: one file per region, as partition_overwrite_sink writes.
    write_lines(dir / "eu", {R"({"region":"eu","amt":10})", R"({"region":"eu","amt":5})"});
    write_lines(dir / "us", {R"({"region":"us","amt":20})"});

    Catalog cat;
    auto rb_ddl = parse(std::string{"CREATE TABLE rb (region VARCHAR, amt BIGINT) "
                                    "WITH (connector='file', format='json', path='"} +
                        dir.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(rb_ddl.statements[0]));
    auto out_ddl = parse(std::string{"CREATE TABLE out_t (region VARCHAR, amt BIGINT) "
                                     "WITH (connector='file', format='json', path='"} +
                         out.string() + "')");
    cat.register_table(std::get<ast::CreateTableStmt>(out_ddl.statements[0]));

    InProcessCluster cluster("tm-sql-readback", 8);
    application::JobSubmitter submitter("127.0.0.1", cluster.jm_port);
    application::SubmitOptions opts;
    opts.wait_timeout = 15s;
    auto spec = compile(cat, "INSERT INTO out_t SELECT region, amt FROM rb");
    auto result = submitter.submit(spec.to_json(), {}, opts);
    EXPECT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Every row from every partition file is read back.
    std::multiset<std::pair<std::string, std::int64_t>> got;
    for (const auto& l : read_lines(out)) {
        auto row = clink::config::parse(l);
        got.emplace(row.at("region").as_string(),
                    static_cast<std::int64_t>(row.at("amt").as_number()));
    }
    EXPECT_EQ(
        got,
        (std::multiset<std::pair<std::string, std::int64_t>>{{"eu", 10}, {"eu", 5}, {"us", 20}}));

    std::filesystem::remove_all(dir);
    std::filesystem::remove(out);
}

}  // namespace clink::sql
