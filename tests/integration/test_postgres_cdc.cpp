// Integration test: PostgresCdcSource streams logical-replication changes.
//
// Spins up Postgres with `wal_level=logical`, creates a table, starts a
// clink pipeline subscribed to a logical replication slot, then
// concurrently performs INSERT/UPDATE/DELETE from a separate libpq
// connection. Verifies the source emits the expected event sequence and
// shuts down cleanly.

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/cdc_event.hpp"
#include "clink/connectors/postgres_cdc_source.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

#include "tests/integration/docker_postgres.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

class CdcEventCollector final : public Sink<CdcEvent> {
public:
    void on_data(const Batch<CdcEvent>& batch) override {
        std::lock_guard lock(mu_);
        for (const auto& r : batch) {
            events_.push_back(r.value());
        }
    }

    std::vector<CdcEvent> snapshot() const {
        std::lock_guard lock(mu_);
        return events_;
    }

    std::size_t size() const {
        std::lock_guard lock(mu_);
        return events_.size();
    }

    std::string name() const override { return "cdc_collector"; }

private:
    mutable std::mutex mu_;
    std::vector<CdcEvent> events_;
};

}  // namespace

TEST(PostgresCdcIntegration, StreamsInsertUpdateDeleteThroughPipeline) {
    if (!test::DockerPostgres::docker_available()) {
        GTEST_SKIP() << "Docker not available; skipping Postgres CDC test";
    }

    test::DockerPostgres::Options pg_opts;
    pg_opts.postgres_args = {
        "-c",
        "wal_level=logical",
        "-c",
        "max_wal_senders=4",
        "-c",
        "max_replication_slots=4",
    };
    test::DockerPostgres pg(pg_opts);

    pg.exec(R"(
        CREATE TABLE widgets (id INT PRIMARY KEY, name TEXT NOT NULL);
    )");

    Dag dag;
    auto cdc = std::make_shared<PostgresCdcSource>(PostgresCdcSource::Options{
        .conninfo = pg.conninfo(),
        .slot_name = "clink_cdc_test",
    });
    auto sink = std::make_shared<CdcEventCollector>();
    auto h_src = dag.add_source<CdcEvent>(cdc);
    dag.add_sink<CdcEvent>(h_src, sink);

    LocalExecutor exec(std::move(dag));
    exec.start();

    // Give the source a moment to issue START_REPLICATION before we
    // generate WAL traffic.
    std::this_thread::sleep_for(500ms);

    // Drive INSERT/UPDATE/DELETE from a parallel connection. Each
    // statement is its own implicit transaction → 1 BEGIN + 1 row event +
    // 1 COMMIT per statement, so 9 events total.
    pg.exec("INSERT INTO widgets VALUES (1, 'first')");
    pg.exec("UPDATE widgets SET name = 'updated' WHERE id = 1");
    pg.exec("DELETE FROM widgets WHERE id = 1");

    // Poll for events to arrive (with a timeout - slot reads through the
    // WAL with some latency).
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < deadline && sink->size() < 9) {
        std::this_thread::sleep_for(50ms);
    }

    cdc->cancel();
    exec.cancel();
    exec.await_termination();

    auto events = sink->snapshot();
    ASSERT_GE(events.size(), 9u) << "expected at least 9 CDC events (3 transactions × 3 rows)";

    // Verify the canonical (BEGIN, INSERT, COMMIT, BEGIN, UPDATE, COMMIT,
    // BEGIN, DELETE, COMMIT) sequence appears in order. We tolerate extra
    // trailing keepalive-free transactions but require this prefix.
    std::size_t idx = 0;
    auto expect_begin = [&](std::size_t i) {
        EXPECT_EQ(events[i].op, CdcEvent::Op::Begin) << "at index " << i;
    };
    auto expect_commit = [&](std::size_t i) {
        EXPECT_EQ(events[i].op, CdcEvent::Op::Commit) << "at index " << i;
    };

    expect_begin(idx);
    EXPECT_EQ(events[idx + 1].op, CdcEvent::Op::Insert);
    EXPECT_EQ(events[idx + 1].table, "public.widgets");
    EXPECT_FALSE(events[idx + 1].values.empty());
    {
        bool saw_id = false;
        bool saw_name = false;
        for (const auto& f : events[idx + 1].values) {
            if (f.name == "id" && f.value == "1")
                saw_id = true;
            if (f.name == "name" && f.value == "first")
                saw_name = true;
        }
        EXPECT_TRUE(saw_id);
        EXPECT_TRUE(saw_name);
    }
    expect_commit(idx + 2);

    idx = 3;
    expect_begin(idx);
    EXPECT_EQ(events[idx + 1].op, CdcEvent::Op::Update);
    EXPECT_EQ(events[idx + 1].table, "public.widgets");
    expect_commit(idx + 2);

    idx = 6;
    expect_begin(idx);
    EXPECT_EQ(events[idx + 1].op, CdcEvent::Op::Delete);
    EXPECT_EQ(events[idx + 1].table, "public.widgets");
    expect_commit(idx + 2);

    // No errors should have been recorded on the executor.
    EXPECT_TRUE(exec.operator_errors().empty());
}
