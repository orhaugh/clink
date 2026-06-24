// Nexmark-on-clink benchmark harness.
//
// Generates a Nexmark event stream (connector='nexmark') and runs a Nexmark
// query as a clink SQL job on an in-process JobManager+TaskManager cluster,
// discarding output through a blackhole sink, and reports input-event
// throughput. Mirrors the SQL runtime tests' InProcessCluster + the
// failover/cold-start bench shape.
//
//   clink_nexmark_bench --query q0 --events 5000000 --tps 1000000 --slots 8
//
// Output: one JSON line {query, events, slots, wall_ms, events_per_sec,
// events_per_sec_per_core}. wall_ms includes job deploy/round-trip overhead, so
// use a large --events; a steady-state (warm-up-subtracted) mode is a follow-on.

#include <chrono>
#include <cstdint>
#include <iostream>
#include <map>
#include <string>
#include <thread>

#include <arrow/type.h>

#include "clink/application/job_submitter.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/job_manager.hpp"
#include "clink/cluster/task_manager.hpp"
#include "clink/nexmark/register.hpp"
#include "clink/operators/scalar_function_registry.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/async_function_registry.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/optimizer.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/physical_plan.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// Per-type base tables, each a nexmark_source filtered to one event type. clink
// joins require base tables; running the same seeded generator in each instance
// (advancing over every event, emitting one type) keeps foreign keys consistent
// across the three. datetime is the event-time column; the planner's
// assign_timestamps_row generates watermarks from it (watermark_lag_ms).
std::string base_tables_ddl(std::int64_t events, std::int64_t tps) {
    // Column names are lower-case: the SQL parser lower-cases identifiers, so a
    // camelCase column would not match a lower-cased reference (and the post-join
    // residual in q7/q8 would silently see a null column).
    const std::string w =
        " connector='nexmark', format='json', event_time_column='datetime', "
        "watermark_lag_ms='4000', events_num='" +
        std::to_string(events) + "', tps='" + std::to_string(tps) + "', ";
    return "CREATE TABLE person (id BIGINT, name VARCHAR, emailaddress VARCHAR, city VARCHAR, "
           "state VARCHAR, datetime BIGINT) WITH (" +
           w + "nexmark_type='person');" +
           "CREATE TABLE auction (id BIGINT, itemname VARCHAR, initialbid BIGINT, reserve BIGINT, "
           "expires BIGINT, seller BIGINT, category BIGINT, datetime BIGINT) WITH (" +
           w + "nexmark_type='auction');" +
           "CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, "
           "url VARCHAR, datetime BIGINT) WITH (" +
           w + "nexmark_type='bid');";
}

struct Query {
    std::string sink_ddl;
    std::string insert_sql;
};

const std::map<std::string, Query>& queries() {
    static const std::map<std::string, Query> q = {
        // q0: pass-through (projection only).
        {"q0",
         {"CREATE TABLE sink_q0 (auction BIGINT, bidder BIGINT, price BIGINT, datetime BIGINT) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q0 SELECT auction, bidder, price, datetime FROM bid"}},
        // q1: currency conversion (stateless arithmetic).
        {"q1",
         {"CREATE TABLE sink_q1 (auction BIGINT, bidder BIGINT, price DECIMAL(18,3), datetime "
          "BIGINT) WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q1 SELECT auction, bidder, price * 0.908 AS price, datetime FROM bid"}},
        // q2: selection on a MOD predicate. clink's WHERE compares a column to a
        // literal, so mod() is computed as a projected column in a derived table
        // and filtered in the outer WHERE.
        {"q2",
         {"CREATE TABLE sink_q2 (auction BIGINT, price BIGINT) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q2 SELECT auction, price FROM ("
          "SELECT auction, price, mod(auction, 123) AS m FROM bid) AS t WHERE m = 0"}},
        // q3: local item suggestion - INNER join auction/person on seller=id.
        // Post-join columns are flat <alias>_<col>; the outer SELECT/WHERE use
        // them (qualified alias.col in a join projection is rejected).
        {"q3",
         {"CREATE TABLE sink_q3 (id BIGINT, name VARCHAR, city VARCHAR, state VARCHAR) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q3 SELECT A_id AS id, P_name AS name, P_city AS city, P_state AS state "
          "FROM auction AS A JOIN person AS P ON A.seller = P.id "
          "WHERE A_category = 10 AND P_state IN ('OR', 'ID', 'CA')"}},
        // q20: expand bid with its auction - INNER join bid/auction on auction=id.
        {"q20",
         {"CREATE TABLE sink_q20 (auction BIGINT, bidder BIGINT, price BIGINT, category BIGINT) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q20 SELECT B_auction AS auction, B_bidder AS bidder, B_price AS price, "
          "A_category AS category FROM bid AS B JOIN auction AS A ON B.auction = A.id "
          "WHERE A_category = 10"}},
        // q11: user sessions - per-bidder COUNT over a 10s session-gap window,
        // emitting the session start/end (window_start/window_end are now
        // projectable from a windowed GROUP BY).
        {"q11",
         {"CREATE TABLE sink_q11 (bidder BIGINT, bid_count BIGINT, starttime BIGINT, "
          "endtime BIGINT) WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q11 SELECT bidder, COUNT(*) AS bid_count, window_start AS starttime, "
          "window_end AS endtime FROM bid GROUP BY SESSION(datetime, INTERVAL '10' SECOND), "
          "bidder"}},
        // q12: per-bidder bid count per 10s window. Nexmark uses PROCTIME; clink's
        // window TVFs are event-time, so this is the event-time analogue (same
        // count-per-user-per-10s shape).
        {"q12",
         {"CREATE TABLE sink_q12 (bidder BIGINT, bid_count BIGINT) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q12 SELECT bidder, COUNT(*) AS bid_count FROM bid "
          "GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), bidder"}},
        // q7: highest bid per window. A per-window MAX(price) windowed aggregate
        // is a join side (equi on price); the column-vs-column range residual
        // binds each bid to its OWN window (rejecting cross-window false matches).
        {"q7",
         {"CREATE TABLE sink_q7 (auction BIGINT, price BIGINT, bidder BIGINT) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q7 SELECT B_auction AS auction, B_price AS price, B_bidder AS bidder "
          "FROM bid AS B JOIN (SELECT MAX(price) AS maxprice, window_start AS ws, window_end AS we "
          "FROM bid GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND)) AS M ON B.price = M.maxprice "
          "WHERE B_datetime >= M_ws AND B_datetime < M_we"}},
        // q8: new users. Persons and auctions created in the SAME tumbling window,
        // joined on seller=id - two windowed-aggregate join sides, equi on the key
        // plus a column-vs-column window-equality residual. The per-window grouping
        // carries a COUNT(*) (unused) because clink requires an aggregate in a
        // GROUP BY SELECT; the grouping/dedup semantics are identical.
        {"q8",
         {"CREATE TABLE sink_q8 (id BIGINT, name VARCHAR, starttime BIGINT) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q8 SELECT P_id AS id, P_name AS name, P_starttime AS starttime "
          "FROM (SELECT id, name, COUNT(*) AS pc, window_start AS starttime, window_end AS endtime "
          "FROM person GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), id, name) AS P "
          "JOIN (SELECT seller, COUNT(*) AS ac, window_start AS astart, window_end AS aend "
          "FROM auction GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), seller) AS A "
          "ON P.id = A.seller WHERE P_starttime = A_astart AND P_endtime = A_aend"}},
        // q5: hot items - per-window auction(s) with the most bids. D is the
        // per-(auction,window) bid count; M is the per-window MAX of those counts
        // (a non-windowed GROUP BY -> changelog). Join on the window + a
        // column-vs-column residual (count == window max). The retracting M and
        // the retraction-consuming join keep only the current hot items. Heavy
        // topology (two windowed counts + a join): run with --slots 16.
        {"q5",
         {"CREATE TABLE sink_q5 (auction BIGINT, num BIGINT) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q5 SELECT D_auction AS auction, D_num AS num FROM "
          "(SELECT auction, COUNT(*) AS num, window_start AS ws, window_end AS we FROM bid "
          "GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), auction) AS D "
          "JOIN (SELECT window_start AS ws, window_end AS we, MAX(num) AS maxnum FROM "
          "(SELECT auction, COUNT(*) AS num, window_start AS window_start, window_end AS "
          "window_end "
          "FROM bid GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), auction) AS d2 "
          "GROUP BY window_start, window_end) AS M ON D.ws = M.ws "
          "WHERE D_we = M_we AND D_num = M_maxnum"}},
        // q19: per-auction top-10 bids by price (TOP-N-per-key changelog).
        {"q19",
         {"CREATE TABLE sink_q19 (auction BIGINT, bidder BIGINT, price BIGINT) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q19 SELECT auction, bidder, price FROM "
          "(SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rn FROM bid) "
          "AS t WHERE rn <= 10"}},
        // q18: latest bid per (auction, bidder) (TOP-N-per-key, rn=1).
        {"q18",
         {"CREATE TABLE sink_q18 (auction BIGINT, bidder BIGINT, price BIGINT) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q18 SELECT auction, bidder, price FROM "
          "(SELECT *, ROW_NUMBER() OVER (PARTITION BY auction, bidder ORDER BY datetime DESC) AS "
          "rn "
          "FROM bid) AS t WHERE rn <= 1"}},
        // q15: per-day bidding stats (count + distinct bidders/auctions).
        {"q15",
         {"CREATE TABLE sink_q15 (day BIGINT, total BIGINT, bidders BIGINT, auctions BIGINT) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q15 SELECT day, COUNT(*) AS total, COUNT(DISTINCT bidder) AS bidders, "
          "COUNT(DISTINCT auction) AS auctions FROM (SELECT DATE_TRUNC('day', datetime) AS day, "
          "bidder, auction FROM bid) AS t GROUP BY day"}},
        // q17: per-(auction,day) bid stats (count, distinct bidders, min/max/avg/sum price).
        {"q17",
         {"CREATE TABLE sink_q17 (auction BIGINT, day BIGINT, total BIGINT, bidders BIGINT, "
          "minp BIGINT, maxp BIGINT, avgp DOUBLE, sump BIGINT) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q17 SELECT auction, day, COUNT(*) AS total, COUNT(DISTINCT bidder) AS "
          "bidders, MIN(price) AS minp, MAX(price) AS maxp, AVG(price) AS avgp, SUM(price) AS sump "
          "FROM (SELECT auction, DATE_TRUNC('day', datetime) AS day, bidder, price FROM bid) AS t "
          "GROUP BY auction, day"}},
        // q16: per-(channel,day) bid stats - count, distinct bidders/auctions, first/last
        // bid time, and price-range bucket counts (SUM over CASE buckets computed in a
        // derived table, since clink WHERE/aggregates take a column not an expression).
        {"q16",
         {"CREATE TABLE sink_q16 (channel VARCHAR, day BIGINT, total BIGINT, bidders BIGINT, "
          "auctions BIGINT, minbid BIGINT, maxbid BIGINT, lt10k BIGINT, bet BIGINT, gt1m BIGINT) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q16 SELECT channel, day, COUNT(*) AS total, "
          "COUNT(DISTINCT bidder) AS bidders, COUNT(DISTINCT auction) AS auctions, "
          "MIN(dt) AS minbid, MAX(dt) AS maxbid, SUM(r1) AS lt10k, SUM(r2) AS bet, "
          "SUM(r3) AS gt1m FROM (SELECT channel, DATE_TRUNC('day', datetime) AS day, bidder, "
          "auction, datetime AS dt, CASE WHEN price < 10000 THEN 1 ELSE 0 END AS r1, "
          "CASE WHEN price >= 10000 AND price <= 1000000 THEN 1 ELSE 0 END AS r2, "
          "CASE WHEN price > 1000000 THEN 1 ELSE 0 END AS r3 FROM bid) AS t "
          "GROUP BY channel, day"}},
        // q21: add channel_id - extract the numeric id from the bid url with
        // regexp_extract (a new built-in scalar function).
        {"q21",
         {"CREATE TABLE sink_q21 (auction BIGINT, bidder BIGINT, chan_id VARCHAR) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q21 SELECT auction, bidder, regexp_extract(url, '([0-9]+)', 1) AS "
          "chan_id FROM bid"}},
        // q22: get url directories - split the url path into segments with
        // split_index (a new built-in scalar function, 0-based like Flink).
        {"q22",
         {"CREATE TABLE sink_q22 (auction BIGINT, dir2 VARCHAR, dir3 VARCHAR) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q22 SELECT auction, split_index(url, '/', 2) AS dir2, "
          "split_index(url, '/', 3) AS dir3 FROM bid"}},
        // q9: winning bids - for each auction the bid with MAX price during the
        // auction's open period [datetime, expires]. bid INNER JOIN auction (equi
        // on auction=id) + a column-vs-column interval residual, then ROW_NUMBER
        // top-1 per auction by price (a TOP-N-per-key changelog).
        {"q9",
         {"CREATE TABLE sink_q9 (auction BIGINT, bidder BIGINT, price BIGINT) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q9 SELECT b_auction AS auction, b_bidder AS bidder, b_price AS price "
          "FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY b_auction ORDER BY b_price DESC) AS rn "
          "FROM (SELECT b_auction, b_bidder, b_price FROM bid AS B JOIN auction AS A "
          "ON B.auction = A.id WHERE b_datetime >= a_datetime AND b_datetime <= a_expires) AS j) "
          "AS r WHERE rn <= 1"}},
        // q4: average winning price per category. Winning price = MAX bid per
        // auction during its open period (the q9 interval join + per-auction MAX),
        // then AVG over categories (a second, stacked GROUP BY).
        {"q4",
         {"CREATE TABLE sink_q4 (category BIGINT, avgp DOUBLE) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q4 SELECT category, AVG(maxp) AS avgp FROM (SELECT a_category AS "
          "category, MAX(b_price) AS maxp FROM (SELECT b_auction, b_price, a_category FROM bid AS "
          "B "
          "JOIN auction AS A ON B.auction = A.id WHERE b_datetime >= a_datetime AND "
          "b_datetime <= a_expires) AS j GROUP BY b_auction, a_category) AS wins GROUP BY "
          "category"}},
        // q13: bounded side-input join - enrich each bid with a label from a
        // static side table keyed by (auction mod N), via clink's lookup join
        // (connector='lookup' + the side_lookup function registered in main).
        {"q13",
         {"CREATE TABLE side (key BIGINT, label VARCHAR) "
          "WITH (connector='lookup', function='side_lookup');"
          "CREATE TABLE sink_q13 (auction BIGINT, bidder BIGINT, label VARCHAR) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q13 SELECT B_auction AS auction, B_bidder AS bidder, S_label AS label "
          "FROM bid AS B JOIN side AS S ON B.auction = S.key"}},
        // q14: calculation with user-defined scalar functions - bid_fee in the
        // projection + bid_keep as a predicate (via the derived-table pattern,
        // since WHERE compares a column to a literal). UDFs registered in main.
        {"q14",
         {"CREATE TABLE sink_q14 (auction BIGINT, bidder BIGINT, fee BIGINT) "
          "WITH (connector='blackhole', format='json')",
          "INSERT INTO sink_q14 SELECT auction, bidder, fee FROM (SELECT auction, bidder, "
          "bid_fee(price) AS fee, bid_keep(price) AS k FROM bid) AS t WHERE k = true"}},
    };
    return q;
}

cluster::JobGraphSpec build_spec(std::int64_t events, std::int64_t tps, const Query& q) {
    sql::Catalog cat;
    for (auto& stmt : sql::parse(base_tables_ddl(events, tps)).statements) {
        cat.register_table(std::get<sql::ast::CreateTableStmt>(stmt));
    }
    // sink_ddl may declare more than one table (e.g. q13's lookup side table
    // plus the sink), so register every CREATE TABLE it contains.
    for (auto& stmt : sql::parse(q.sink_ddl).statements) {
        cat.register_table(std::get<sql::ast::CreateTableStmt>(stmt));
    }
    sql::Binder b(cat);
    auto plan =
        b.bind_insert(std::get<sql::ast::InsertStmt>(sql::parse(q.insert_sql).statements[0]));
    plan = sql::optimize(std::move(plan));
    sql::PhysicalPlanner pp;
    return pp.compile(static_cast<const sql::LogicalSink&>(*plan));
}

}  // namespace

int main(int argc, char** argv) {
    std::string query_id = "q0";
    std::int64_t events = 5'000'000;
    std::int64_t tps = 1'000'000;
    std::size_t slots = 8;  // joins (q3/q20) need >= 7
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() { return i + 1 < argc ? std::string(argv[++i]) : std::string(); };
        if (a == "--query")
            query_id = next();
        else if (a == "--events")
            events = std::stoll(next());
        else if (a == "--tps")
            tps = std::stoll(next());
        else if (a == "--slots")
            slots = static_cast<std::size_t>(std::stoul(next()));
    }
    auto it = queries().find(query_id);
    if (it == queries().end()) {
        std::cerr << "unknown query '" << query_id << "'\n";
        return 1;
    }

    // Install SQL ops + the Nexmark source/blackhole-sink factories into the host
    // registry (default_instance), so the in-process TM can build them.
    cluster::ensure_built_ins_registered();
    plugin::PluginRegistry reg;
    sql::install(reg);
    nexmark::register_nexmark_factories(reg);
    // q13's bounded side input: a lookup function mapping a bid to a label by
    // (auction mod 4). Registered globally so the lookup-join op can resolve it.
    sql::AsyncFunctionRegistry::global().register_function(
        "side_lookup", [](const sql::Row& probe) -> async::Task<sql::Row> {
            sql::Row dim;
            if (auto s = probe.get_string("auction")) {
                const std::int64_t k = std::stoll(*s);
                dim.values["key"] = config::JsonValue{static_cast<double>(k)};
                dim.values["label"] = config::JsonValue{std::string{"L"} + std::to_string(k % 4)};
            }
            co_return dim;
        });
    // q14's user-defined scalar functions (clink has no CREATE FUNCTION DDL, so
    // they are registered programmatically): a fee transform + a kept-bid test.
    ScalarFunctionRegistry::global().register_function(
        "bid_fee",
        arrow::int64(),
        [](const std::vector<config::JsonValue>& a) -> config::JsonValue {
            if (a.empty() || a[0].is_null())
                return config::JsonValue{nullptr};
            return config::JsonValue{static_cast<std::int64_t>(a[0].as_number()) + 100};
        });
    ScalarFunctionRegistry::global().register_function(
        "bid_keep",
        arrow::boolean(),
        [](const std::vector<config::JsonValue>& a) -> config::JsonValue {
            return config::JsonValue{!a.empty() && !a[0].is_null() && a[0].as_number() >= 50.0};
        });

    cluster::JobManager jm;
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"tm-nexmark"});
    cluster::TaskManager::Config tcfg;
    tcfg.slot_count = slots;
    cluster::TaskManager tm("tm-nexmark", "127.0.0.1", tcfg);
    tm.connect_to_jm("127.0.0.1", jm_port);
    std::this_thread::sleep_for(150ms);

    cluster::JobGraphSpec spec;
    try {
        spec = build_spec(events, tps, it->second);
    } catch (const std::exception& e) {
        std::cerr << "compile failed for " << query_id << ": " << e.what() << "\n";
        tm.stop();
        jm.stop();
        return 1;
    }

    application::JobSubmitter submitter("127.0.0.1", jm_port);
    application::SubmitOptions opts;
    const auto t0 = std::chrono::steady_clock::now();
    auto result = submitter.submit(spec.to_json(), {}, opts);
    const auto wall = std::chrono::steady_clock::now() - t0;

    tm.stop();
    jm.stop();

    if (!result.completed || !result.ok) {
        std::cerr << "job did not complete cleanly: "
                  << (result.completed ? (result.errors.empty() ? "(error)" : result.errors[0])
                                       : result.reject_message)
                  << "\n";
        return 1;
    }

    const double wall_ms =
        std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(wall).count();
    const double eps = wall_ms > 0 ? (static_cast<double>(events) / (wall_ms / 1000.0)) : 0.0;
    std::cout << "{\"query\":\"" << query_id << "\",\"events\":" << events << ",\"slots\":" << slots
              << ",\"wall_ms\":" << static_cast<std::int64_t>(wall_ms)
              << ",\"events_per_sec\":" << static_cast<std::int64_t>(eps)
              << ",\"events_per_sec_per_core\":"
              << static_cast<std::int64_t>(eps / static_cast<double>(slots)) << "}\n";
    return 0;
}
