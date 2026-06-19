// Integration test: PostgresSource → interval_join → reduce → join with file
//                   source → enriched output.
//
// Spins up a real Postgres instance via Docker, populates two related
// tables (users, orders), runs the clink pipeline, and verifies the set
// of enriched sessions exactly matches the closed-form expected output.
//
// Skipped when Docker is not available locally.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/file_source.hpp"
#include "clink/connectors/postgres_source.hpp"
#include "clink/connectors/text_format.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/reduce_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

#include "tests/integration/docker_postgres.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

struct UserRow {
    int user_id;
    std::string name;
};
struct OrderRow {
    int user_id;
    double amount;
};
struct JoinedRow {
    int user_id;
    std::string name;
    double amount;
};
struct ReduceValue {
    std::string name;
    double amount;
};
struct UserAggregate {
    std::string name;
    double total{0};
    int count{0};
};
struct SessionRow {
    int user_id;
    std::string page;
};
struct EnrichedSession {
    int user_id;
    std::string page;
    UserAggregate agg;
};

class DelayedFileSource final : public Source<SessionRow> {
public:
    DelayedFileSource(std::vector<SessionRow> sessions, std::chrono::milliseconds delay)
        : sessions_(std::move(sessions)), delay_(delay) {}

    bool produce(Emitter<SessionRow>& out) override {
        if (this->cancelled() || done_) {
            return false;
        }
        std::this_thread::sleep_for(delay_);
        Batch<SessionRow> b;
        for (auto& s : sessions_) {
            b.emplace(std::move(s));
        }
        out.emit_data(std::move(b));
        out.emit_watermark(Watermark::max());
        done_ = true;
        return false;
    }

    std::string name() const override { return "delayed_sessions"; }

private:
    std::vector<SessionRow> sessions_;
    std::chrono::milliseconds delay_;
    bool done_{false};
};

}  // namespace

TEST(PostgresIntegration, ReadJoinReduceEnrichWithFileStream) {
    if (!test::DockerPostgres::docker_available()) {
        GTEST_SKIP() << "Docker not available; skipping Postgres integration test";
    }

    test::DockerPostgres pg;

    // ----- Schema and seed data ------------------------------------------
    pg.exec(R"(
        CREATE TABLE users  (id INT PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE orders (id INT PRIMARY KEY, user_id INT NOT NULL, amount NUMERIC(10,2) NOT NULL);

        INSERT INTO users  VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
        INSERT INTO orders VALUES
            (1, 1, 12.50), (2, 1, 7.25),
            (3, 2, 100.00), (4, 2, 50.00), (5, 2, 25.00);
        -- carol intentionally has no orders
    )");

    // ----- Pipeline build ------------------------------------------------
    Dag dag;

    auto users_pg = std::make_shared<PostgresSource>(
        PostgresSource::Options{.conninfo = pg.conninfo(), .query = "SELECT id, name FROM users"});
    auto orders_pg = std::make_shared<PostgresSource>(PostgresSource::Options{
        .conninfo = pg.conninfo(), .query = "SELECT user_id, amount FROM orders"});

    auto h_users_raw = dag.add_source<PostgresRow>(users_pg);
    auto h_orders_raw = dag.add_source<PostgresRow>(orders_pg);

    // PostgresRow → typed structs
    auto parse_user = std::make_shared<MapOperator<PostgresRow, UserRow>>(
        [](const PostgresRow& r) { return UserRow{std::stoi(r.at("id")), r.at("name")}; },
        "parse_user");
    auto parse_order = std::make_shared<MapOperator<PostgresRow, OrderRow>>(
        [](const PostgresRow& r) {
            return OrderRow{std::stoi(r.at("user_id")), std::stod(r.at("amount"))};
        },
        "parse_order");

    auto h_users = dag.add_operator<PostgresRow, UserRow>(h_users_raw, parse_user);
    auto h_orders = dag.add_operator<PostgresRow, OrderRow>(h_orders_raw, parse_order);

    // Step 1: Join users × orders on user_id. Wide window (1 hour) and
    // event_time = 0 on all records means everything is in-window.
    auto h_user_order = dag.interval_join<UserRow, OrderRow, int, JoinedRow>(
        h_users,
        h_orders,
        [](const UserRow& u) { return u.user_id; },
        [](const OrderRow& o) { return o.user_id; },
        std::chrono::hours{1},
        std::chrono::hours{1},
        [](const std::optional<UserRow>& u, const std::optional<OrderRow>& o) -> JoinedRow {
            return JoinedRow{u->user_id, u->name, o->amount};
        });

    // Step 2: Project to (key, value) for reduce.
    auto to_reduce_input = std::make_shared<MapOperator<JoinedRow, std::pair<int, ReduceValue>>>(
        [](const JoinedRow& j) { return std::make_pair(j.user_id, ReduceValue{j.name, j.amount}); },
        "to_reduce_input");
    auto h_reduce_input =
        dag.add_operator<JoinedRow, std::pair<int, ReduceValue>>(h_user_order, to_reduce_input);

    // Step 3: Reduce per user_id, emit one final aggregate per user on
    // flush. This is the "in-memory data structure" - when the upstream
    // PG sources close, this operator emits one (user_id, UserAggregate)
    // per user.
    auto reducer = std::make_shared<ReduceOperator<int, ReduceValue, UserAggregate>>(
        []() { return UserAggregate{}; },
        [](const UserAggregate& acc, const ReduceValue& v) {
            return UserAggregate{v.name, acc.total + v.amount, acc.count + 1};
        },
        "user_aggregator",
        ReduceEmitMode::OnFlush);
    auto h_aggregates =
        dag.add_operator<std::pair<int, ReduceValue>, std::pair<int, UserAggregate>>(h_reduce_input,
                                                                                     reducer);

    // Step 4: Sessions from a synthesised "file" source. The DelayedFileSource
    // sleeps briefly before emitting so the reduce-on-flush has already
    // produced its aggregates by the time sessions reach the next join.
    auto sessions_source = std::make_shared<DelayedFileSource>(
        std::vector<SessionRow>{
            SessionRow{1, "/home"},
            SessionRow{1, "/shoe"},
            SessionRow{2, "/cart"},
            SessionRow{4, "/missing"},  // user 4 doesn't exist; should not match
        },
        std::chrono::milliseconds{200});
    auto h_sessions = dag.add_source<SessionRow>(sessions_source);

    // Step 5: Inner join the aggregates with the sessions on user_id.
    // Wide window again - semantics are "look up the matching aggregate".
    auto h_enriched =
        dag.interval_join<std::pair<int, UserAggregate>, SessionRow, int, EnrichedSession>(
            h_aggregates,
            h_sessions,
            [](const std::pair<int, UserAggregate>& a) { return a.first; },
            [](const SessionRow& s) { return s.user_id; },
            std::chrono::hours{1},
            std::chrono::hours{1},
            [](const std::optional<std::pair<int, UserAggregate>>& a,
               const std::optional<SessionRow>& s) -> EnrichedSession {
                return EnrichedSession{s->user_id, s->page, a->second};
            },
            Dag::JoinType::Inner,
            "session_enrich");

    auto sink = std::make_shared<CollectingSink<EnrichedSession>>();
    dag.add_sink<EnrichedSession>(h_enriched, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    // ----- Assertions ----------------------------------------------------
    auto results = sink->collected();
    ASSERT_EQ(results.size(), 3u) << "expected 3 enriched sessions (alice/alice/bob)";

    // Sort for deterministic comparison.
    std::sort(
        results.begin(), results.end(), [](const EnrichedSession& a, const EnrichedSession& b) {
            if (a.user_id != b.user_id) {
                return a.user_id < b.user_id;
            }
            return a.page < b.page;
        });

    EXPECT_EQ(results[0].user_id, 1);
    EXPECT_EQ(results[0].page, "/home");
    EXPECT_EQ(results[0].agg.name, "alice");
    EXPECT_EQ(results[0].agg.count, 2);
    EXPECT_NEAR(results[0].agg.total, 19.75, 1e-6);

    EXPECT_EQ(results[1].user_id, 1);
    EXPECT_EQ(results[1].page, "/shoe");
    EXPECT_EQ(results[1].agg.name, "alice");

    EXPECT_EQ(results[2].user_id, 2);
    EXPECT_EQ(results[2].page, "/cart");
    EXPECT_EQ(results[2].agg.name, "bob");
    EXPECT_EQ(results[2].agg.count, 3);
    EXPECT_NEAR(results[2].agg.total, 175.00, 1e-6);
}
