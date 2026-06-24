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
#include <map>
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
#include "clink/cluster/task_manager.hpp"
#include "clink/config/json.hpp"
#include "clink/operators/agg_function_registry.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/operators/scalar_function_registry.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/sql/analyze.hpp"
#include "clink/sql/async_function_registry.hpp"
#include "clink/sql/binder.hpp"
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

// --- Phase 24b: retracting aggregate ------------------------------

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

// --- Phase 23b: file 2PC sink e2e ---------------------------------

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

// --- Phase 21c: TOP-N-per-key e2e ---------------------------------

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

// --- Phase 20: FROM-derived tables e2e ----------------------------

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

// --- Phase 19d: INSERT column list e2e ----------------------------

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

// --- Phase 19c: HAVING with direct aggregate e2e ------------------

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

// --- Phase 19b: OFFSET e2e ----------------------------------------

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

// --- Phase 19a: IN literal-list e2e -------------------------------

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

// --- Phase 18: stream-stream equi-join e2e ------------------------

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

// --- Phase 14: window + interval-join e2e -------------------------

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

// --- Phase 17: ORDER BY + LIMIT TOP-N e2e -------------------------

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

// --- Phase 16: CTEs (WITH clause) e2e -----------------------------

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

// --- Phase 15: built-in scalar functions e2e ----------------------

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

// --- Phase 24a/b: full changelog pair model -----------------------
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

// --- Phase 23: 2PC sink driven from SQL, commit verified ----------
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

// --- Phase 28c-frontend: async lookup via SQL ----------------------
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

}  // namespace clink::sql
