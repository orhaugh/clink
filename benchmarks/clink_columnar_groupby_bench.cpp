// Columnar windowed-GROUP-BY benchmark - isolates the columnar-execution wave
// (WS3 within-batch group-by + WS4 open-addressing state map) on a path that
// actually fires it.
//
// WHY THIS EXISTS: the Kafka/JSON Nexmark harness sources rows, and the
// inter-operator wire only PRESERVES columnar (it never manufactures it from
// rows), so a JSON-sourced query takes the row path and process_columnar never
// runs. Columnar is only born from a columnar-native source. This bench feeds
// the windowed aggregate from a typed Parquet file so WindowRowOp::process_columnar
// reads the Arrow sidecar directly, then measures the win two ways:
//
//   1. wall-clock of the measured GROUP BY job (A/B vs the row path), and
//   2. detail::batch_materialize_counter() delta - the zero-row-decode proof:
//      ~0 on the columnar path (cells read straight from the sidecar), one per
//      batch on the row path (each columnar batch materialised to Rows).
//
// Two orthogonal levers isolate the two wave workstreams:
//   - CLINK_DISABLE_COLUMNAR=1 (env)  -> forces the row path (WS3 off).
//   - build with -DCLINK_USE_FLAT_HASH_MAP=ON -> ankerl state map (WS4 on).
// Run the 2x2 from build/ and build-flat/ with/without the env var.
//
//   clink_columnar_groupby_bench --events 5000000 --repeat 5 \
//       --parquet /tmp/clink_cgb_bid.parquet --reuse
//
// Pipeline: Nexmark bid source -> Parquet(bidder,datetime)  [unmeasured, once]
//           then Parquet source -> COUNT(*) GROUP BY TUMBLE(datetime,10s),bidder
//           -> blackhole  [measured, --repeat times].
// A clink-only number; not comparable across engines.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "clink/application/job_submitter.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/worker.hpp"
#include "clink/core/record.hpp"
#include "clink/nexmark/register.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/optimizer.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/physical_plan.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// Compile one INSERT against a catalog built from `ddl` (one or more CREATE
// TABLE statements) into a runnable job graph.
cluster::JobGraphSpec compile_job(const std::string& ddl, const std::string& insert_sql) {
    sql::Catalog cat;
    for (auto& stmt : sql::parse(ddl).statements) {
        cat.register_table(std::get<sql::ast::CreateTableStmt>(stmt));
    }
    sql::Binder b(cat);
    auto plan = b.bind_insert(std::get<sql::ast::InsertStmt>(sql::parse(insert_sql).statements[0]));
    plan = sql::optimize(std::move(plan));
    sql::PhysicalPlanner pp;
    return pp.compile(static_cast<const sql::LogicalSink&>(*plan));
}

double submit_wall_ms(application::JobSubmitter& submitter, const cluster::JobGraphSpec& spec) {
    application::SubmitOptions opts;
    opts.wait_timeout = 120s;
    const auto t0 = std::chrono::steady_clock::now();
    auto r = submitter.submit(spec.to_json(), {}, opts);
    const auto dt = std::chrono::steady_clock::now() - t0;
    if (!r.completed || !r.ok) {
        std::cerr << "job did not complete cleanly: "
                  << (r.completed ? (r.errors.empty() ? "(error)" : r.errors[0]) : r.reject_message)
                  << "\n";
        return -1.0;
    }
    return std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(dt).count();
}

}  // namespace

int main(int argc, char** argv) {
    std::int64_t events = 5'000'000;
    std::int64_t tps = 1'000'000;
    std::size_t slots = 8;
    int repeat = 5;
    std::string pq_path = "/tmp/clink_cgb_bid.parquet";
    bool reuse = false;
    // --wide: source the FULL 6-column bid row (incl two VARCHAR columns) so the
    // query reads only 2 of 6 columns. This exercises columnar projection
    // pushdown - the row path materialises all 6 columns per record (two string
    // allocations), the columnar path reads just the 2 needed int64 columns from
    // the sidecar. --narrow (default) writes only (bidder, datetime), so both
    // paths touch the same 2 columns and only the row-vs-columnar fold differs.
    bool wide = false;
    // --agg: non-windowed GROUP BY (AggregateRowOp, the WS6 Increment 1 target)
    // instead of the default windowed q12 (WindowRowOp). COUNT(*) + SUM(datetime),
    // both int-foldable, grouped by bidder.
    bool agg_mode = false;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() { return i + 1 < argc ? std::string(argv[++i]) : std::string(); };
        if (a == "--events")
            events = std::stoll(next());
        else if (a == "--tps")
            tps = std::stoll(next());
        else if (a == "--slots")
            slots = static_cast<std::size_t>(std::stoul(next()));
        else if (a == "--repeat")
            repeat = std::stoi(next());
        else if (a == "--wide")
            wide = true;
        else if (a == "--agg")
            agg_mode = true;
        else if (a == "--parquet")
            pq_path = next();
        else if (a == "--reuse")
            reuse = true;
    }

    const bool columnar_disabled = [] {
        const char* e = std::getenv("CLINK_DISABLE_COLUMNAR");
        return e != nullptr && e[0] == '1';
    }();
    const bool flat_hash =
#if defined(CLINK_USE_FLAT_HASH_MAP)
        true;
#else
        false;
#endif

    // Register SQL ops + Nexmark source + blackhole sink into the host registry
    // (default_instance) so the in-process worker can build them.
    cluster::ensure_built_ins_registered();
    plugin::PluginRegistry reg;
    sql::install(reg);
    auto marks = std::make_shared<nexmark::SteadyMarks>();
    nexmark::register_nexmark_factories(reg, marks);

    cluster::Coordinator coordinator;
    const std::uint16_t coordinator_port = coordinator.start();
    coordinator.expect_workers({"worker-cgb"});
    cluster::Worker::Config tcfg;
    tcfg.slot_count = slots;
    cluster::Worker worker("worker-cgb", "127.0.0.1", tcfg);
    worker.connect_to_coordinator("127.0.0.1", coordinator_port);
    std::this_thread::sleep_for(150ms);

    application::JobSubmitter submitter("127.0.0.1", coordinator_port);

    // Parquet schema + the projection into it. --wide keeps all 6 bid columns
    // (two of them VARCHAR), --narrow keeps only the 2 the query reads.
    const std::string pq_cols =
        wide ? "(auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, "
               "datetime BIGINT)"
             : "(bidder BIGINT, datetime BIGINT)";
    const std::string pq_select =
        wide ? "SELECT auction, bidder, price, channel, url, datetime FROM bid"
             : "SELECT bidder, datetime FROM bid";

    // Job 1 (unmeasured): generate a typed-columnar Parquet from the Nexmark bid
    // stream. Skipped if --reuse and the file already exists.
    if (!reuse || !std::filesystem::exists(pq_path)) {
        const std::string src_w =
            " connector='nexmark', format='json', event_time_column='datetime', "
            "watermark_lag_ms='4000', events_num='" +
            std::to_string(events) + "', tps='" + std::to_string(tps) + "', nexmark_type='bid'";
        const std::string ddl =
            "CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, "
            "url VARCHAR, datetime BIGINT) WITH (" +
            src_w + ");" + "CREATE TABLE pq " + pq_cols + " WITH (connector='parquet', path='" +
            pq_path + "')";
        try {
            auto spec = compile_job(ddl, "INSERT INTO pq " + pq_select);
            const double w = submit_wall_ms(submitter, spec);
            if (w < 0) {
                worker.stop();
                coordinator.stop();
                return 1;
            }
            std::cerr << "wrote parquet " << pq_path << " (" << events << " bids, " << (int)w
                      << " ms)\n";
        } catch (const std::exception& e) {
            std::cerr << "parquet-write compile/run failed: " << e.what() << "\n";
            worker.stop();
            coordinator.stop();
            return 1;
        }
    } else {
        std::cerr << "reusing parquet " << pq_path << "\n";
    }

    // Job 2 (measured, --repeat times): columnar Parquet source -> per-bidder
    // aggregate -> blackhole. Default fires WindowRowOp (windowed q12); --agg
    // fires AggregateRowOp (non-windowed GROUP BY, the WS6 Increment 1 target).
    // CLINK_DISABLE_COLUMNAR forces the row path on either.
    // Windowed mode declares event_time_column so the planner inserts the
    // (now columnar-preserving) assign_timestamps_row op - the realistic
    // event-time windowed pipeline. --agg (non-windowed) needs no event time.
    const std::string pq_with =
        agg_mode ? "" : ", event_time_column='datetime', watermark_lag_ms='0'";
    const std::string agg_ddl =
        "CREATE TABLE pq_in " + pq_cols + " WITH (connector='parquet', path='" + pq_path + "'" +
        pq_with + ");" +
        (agg_mode ? "CREATE TABLE sink_agg (bidder BIGINT, c BIGINT, s BIGINT) WITH "
                    "(connector='blackhole', format='json')"
                  : "CREATE TABLE sink_q12 (bidder BIGINT, bid_count BIGINT) WITH "
                    "(connector='blackhole', format='json')");
    const std::string agg_sql =
        agg_mode ? "INSERT INTO sink_agg SELECT bidder, COUNT(*) AS c, SUM(datetime) AS s "
                   "FROM pq_in GROUP BY bidder"
                 : "INSERT INTO sink_q12 SELECT bidder, COUNT(*) AS bid_count FROM pq_in "
                   "GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), bidder";

    cluster::JobGraphSpec agg_spec;
    try {
        agg_spec = compile_job(agg_ddl, agg_sql);
    } catch (const std::exception& e) {
        std::cerr << "groupby compile failed: " << e.what() << "\n";
        worker.stop();
        coordinator.stop();
        return 1;
    }

    std::vector<double> walls;
    std::uint64_t mat_delta_total = 0;
    for (int r = 0; r < repeat; ++r) {
        const std::uint64_t mat_before =
            detail::batch_materialize_counter().load(std::memory_order_relaxed);
        const double w = submit_wall_ms(submitter, agg_spec);
        const std::uint64_t mat_after =
            detail::batch_materialize_counter().load(std::memory_order_relaxed);
        if (w < 0) {
            worker.stop();
            coordinator.stop();
            return 1;
        }
        walls.push_back(w);
        mat_delta_total += (mat_after - mat_before);
    }

    worker.stop();
    coordinator.stop();

    std::sort(walls.begin(), walls.end());
    const double best = walls.front();
    const double median = walls[walls.size() / 2];
    const double rate = best > 0 ? static_cast<double>(events) / (best / 1000.0) : 0.0;
    const std::uint64_t mat_per_run =
        repeat > 0 ? mat_delta_total / static_cast<std::uint64_t>(repeat) : 0;

    std::cout << "{\"bench\":\"columnar_groupby_q12\",\"events\":" << events
              << ",\"slots\":" << slots << ",\"repeat\":" << repeat
              << ",\"columnar\":" << (columnar_disabled ? "false" : "true")
              << ",\"flat_hash\":" << (flat_hash ? "true" : "false")
              << ",\"wide\":" << (wide ? "true" : "false")
              << ",\"agg\":" << (agg_mode ? "true" : "false")
              << ",\"best_ms\":" << static_cast<std::int64_t>(best)
              << ",\"median_ms\":" << static_cast<std::int64_t>(median)
              << ",\"rows_per_sec_best\":" << static_cast<std::int64_t>(rate)
              << ",\"materialize_per_run\":" << mat_per_run << "}\n";
    return 0;
}
