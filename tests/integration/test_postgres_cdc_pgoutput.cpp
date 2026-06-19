// Integration test: full production CDC path.
//
//   * pgoutput plugin (binary protocol, the production-grade native one)
//   * Initial snapshot of pre-existing rows bound to the slot's exported
//     snapshot, transitioning seamlessly into streaming
//   * Periodic Standby Status Update keepalives at 1s cadence
//
// Verifies the canonical event sequence:
//   2× Insert (snapshot) → Begin/Insert/Commit (live INSERT) →
//   Begin/Update/Commit (live UPDATE) → Begin/Delete/Commit (live DELETE).

#include <chrono>
#include <libpq-fe.h>
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

std::string field_value(const CdcEvent& ev, std::string_view key) {
    for (const auto& f : ev.values) {
        if (f.name == key) {
            return f.value;
        }
    }
    return {};
}

const CdcField* find_field(const CdcEvent& ev, std::string_view key) {
    for (const auto& f : ev.values) {
        if (f.name == key) {
            return &f;
        }
    }
    return nullptr;
}

}  // namespace

TEST(PostgresCdcIntegration, PgOutputPlusSnapshotPlusKeepalive) {
    if (!test::DockerPostgres::docker_available()) {
        GTEST_SKIP() << "Docker not available; skipping pgoutput CDC test";
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

    // Schema + pre-existing rows that the snapshot phase should pick up.
    pg.exec(R"(
        CREATE TABLE widgets (id INT PRIMARY KEY, name TEXT NOT NULL);
        INSERT INTO widgets VALUES (1, 'one'), (2, 'two');
        CREATE PUBLICATION clink_pub FOR TABLE widgets;
    )");

    Dag dag;
    auto cdc = std::make_shared<PostgresCdcSource>(PostgresCdcSource::Options{
        .conninfo = pg.conninfo(),
        .slot_name = "clink_pgoutput_test",
        .plugin = "pgoutput",
        .create_slot = true,
        .poll_interval = 50ms,
        .publication_names = "clink_pub",
        .proto_version = 1,
        .standby_status_interval = 1s,
        .enable_initial_snapshot = true,
        .initial_snapshot =
            {
                PostgresCdcSnapshotQuery{.table = "public.widgets",
                                         .query = "SELECT id, name FROM widgets ORDER BY id"},
            },
        .drop_slot_on_close = true,
    });
    auto sink = std::make_shared<CdcEventCollector>();
    auto h_src = dag.add_source<CdcEvent>(cdc);
    dag.add_sink<CdcEvent>(h_src, sink);

    LocalExecutor exec(std::move(dag));
    exec.start();

    // Wait for the snapshot phase to flush both pre-existing rows.
    auto deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < deadline && sink->size() < 2) {
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_GE(sink->size(), 2u) << "snapshot rows did not arrive in time";

    // Now drive live changes - should produce 12 streaming events:
    // 3× (BEGIN/INSERT/COMMIT) + 1× (BEGIN/TRUNCATE/COMMIT) = 12 events.
    pg.exec("INSERT INTO widgets VALUES (3, 'three')");
    pg.exec("UPDATE widgets SET name = 'THREE' WHERE id = 3");
    pg.exec("DELETE FROM widgets WHERE id = 3");
    pg.exec("TRUNCATE widgets");

    deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < deadline && sink->size() < 14) {
        std::this_thread::sleep_for(50ms);
    }

    cdc->cancel();
    exec.cancel();
    exec.await_termination();

    auto events = sink->snapshot();
    ASSERT_GE(events.size(), 14u)
        << "expected at least 14 events (2 snapshot + 9 row-change stream + 3 truncate)";

    // First two events are the snapshot Inserts, in id order. Type info
    // populated from PQftype + the built-in OID lookup.
    EXPECT_EQ(events[0].op, CdcEvent::Op::Insert);
    EXPECT_EQ(events[0].table, "public.widgets");
    EXPECT_EQ(field_value(events[0], "id"), "1");
    EXPECT_EQ(field_value(events[0], "name"), "one");
    {
        const auto* id_field = find_field(events[0], "id");
        const auto* name_field = find_field(events[0], "name");
        ASSERT_NE(id_field, nullptr);
        ASSERT_NE(name_field, nullptr);
        EXPECT_EQ(id_field->type, "int4");
        EXPECT_EQ(name_field->type, "text");
        EXPECT_FALSE(id_field->is_null);
    }

    EXPECT_EQ(events[1].op, CdcEvent::Op::Insert);
    EXPECT_EQ(events[1].table, "public.widgets");
    EXPECT_EQ(field_value(events[1], "id"), "2");
    EXPECT_EQ(field_value(events[1], "name"), "two");

    // Streaming events: BEGIN/INSERT/COMMIT, BEGIN/UPDATE/COMMIT,
    // BEGIN/DELETE/COMMIT.
    EXPECT_EQ(events[2].op, CdcEvent::Op::Begin);
    EXPECT_EQ(events[3].op, CdcEvent::Op::Insert);
    EXPECT_EQ(events[3].table, "public.widgets");
    EXPECT_EQ(field_value(events[3], "id"), "3");
    EXPECT_EQ(field_value(events[3], "name"), "three");
    EXPECT_EQ(events[4].op, CdcEvent::Op::Commit);

    EXPECT_EQ(events[5].op, CdcEvent::Op::Begin);
    EXPECT_EQ(events[6].op, CdcEvent::Op::Update);
    EXPECT_EQ(events[6].table, "public.widgets");
    EXPECT_EQ(field_value(events[6], "id"), "3");
    EXPECT_EQ(field_value(events[6], "name"), "THREE");
    EXPECT_EQ(events[7].op, CdcEvent::Op::Commit);

    EXPECT_EQ(events[8].op, CdcEvent::Op::Begin);
    EXPECT_EQ(events[9].op, CdcEvent::Op::Delete);
    EXPECT_EQ(events[9].table, "public.widgets");
    // With default REPLICA IDENTITY (the PK), DELETE only carries the key
    // columns. id=3 should be present; name comes back as a not-null but
    // unchanged-TOAST sentinel (empty value, is_null false).
    EXPECT_EQ(field_value(events[9], "id"), "3");
    EXPECT_EQ(events[10].op, CdcEvent::Op::Commit);

    // TRUNCATE produces its own BEGIN/TRUNCATE/COMMIT envelope.
    EXPECT_EQ(events[11].op, CdcEvent::Op::Begin);
    EXPECT_EQ(events[12].op, CdcEvent::Op::Truncate);
    EXPECT_EQ(events[12].table, "public.widgets");
    EXPECT_EQ(events[13].op, CdcEvent::Op::Commit);

    EXPECT_TRUE(exec.operator_errors().empty());

    // The slot was set to drop on close. Confirm via a fresh admin
    // connection that pg_replication_slots no longer lists it.
    {
        PGconn* admin = PQconnectdb(pg.conninfo().c_str());
        ASSERT_EQ(PQstatus(admin), CONNECTION_OK);
        PGresult* r = PQexec(admin,
                             "SELECT count(*) FROM pg_replication_slots WHERE slot_name = "
                             "'clink_pgoutput_test'");
        ASSERT_EQ(PQresultStatus(r), PGRES_TUPLES_OK);
        EXPECT_EQ(std::string{PQgetvalue(r, 0, 0)}, "0")
            << "drop_slot_on_close should have removed the slot";
        PQclear(r);
        PQfinish(admin);
    }
}
