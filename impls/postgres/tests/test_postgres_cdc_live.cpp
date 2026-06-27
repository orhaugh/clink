// Postgres CDC LIVE integration test. SKIPPED unless CLINK_POSTGRES_CDC_TEST_DSN
// is set (e.g. "host=localhost port=5432 user=postgres password=postgres
// dbname=postgres" against docker/integration-services.yml, a postgres started
// with wal_level=logical). Proves, against real Postgres via pgoutput: an
// INSERT/UPDATE/DELETE stream is decoded into ordered CdcEvents with LSNs and
// the source metrics fire; and an LSN checkpoint resumes a fresh reader on the
// remainder with no gap or replay (the already-built exactly-once-ish path).

#include <chrono>
#include <cstdlib>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#ifdef CLINK_HAS_POSTGRES
#include <libpq-fe.h>
#endif

#include "clink/config/json.hpp"
#include "clink/connectors/cdc_event.hpp"
#include "clink/connectors/cdc_json.hpp"
#include "clink/connectors/postgres_cdc_source.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using clink::CdcEvent;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::PostgresCdcSource;
using clink::StreamElement;

namespace {

bool pg_configured() {
    return std::getenv("CLINK_POSTGRES_CDC_TEST_DSN") != nullptr;
}
std::string pg_dsn() {
    return std::getenv("CLINK_POSTGRES_CDC_TEST_DSN");
}

std::string uniq() {
    return std::to_string(static_cast<long>(::getpid()));
}

#ifdef CLINK_HAS_POSTGRES
// Run one SQL statement on a plain (non-replication) connection; fail the test
// on a connection or command error.
void run_sql(const std::string& dsn, const std::string& sql) {
    PGconn* c = PQconnectdb(dsn.c_str());
    ASSERT_EQ(PQstatus(c), CONNECTION_OK) << PQerrorMessage(c);
    PGresult* r = PQexec(c, sql.c_str());
    const auto st = PQresultStatus(r);
    const bool ok = st == PGRES_COMMAND_OK || st == PGRES_TUPLES_OK;
    EXPECT_TRUE(ok) << sql << " -> " << PQerrorMessage(c);
    PQclear(r);
    PQfinish(c);
}
#endif

struct Captured {
    std::vector<CdcEvent> events;
};
Emitter<CdcEvent> capturing(Captured& sink) {
    return Emitter<CdcEvent>{[&sink](StreamElement<CdcEvent> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                sink.events.push_back(r.value());
            }
        }
        return true;
    }};
}

// Count data-change events (Insert/Update/Delete) collected so far.
std::size_t changes(const Captured& cap) {
    std::size_t n = 0;
    for (const auto& e : cap.events) {
        if (e.op == CdcEvent::Op::Insert || e.op == CdcEvent::Op::Update ||
            e.op == CdcEvent::Op::Delete) {
            ++n;
        }
    }
    return n;
}

void drain_until(PostgresCdcSource& src, Captured& cap, std::size_t want_changes, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (changes(cap) < want_changes && std::chrono::steady_clock::now() < deadline) {
        auto em = capturing(cap);
        src.produce(em);
    }
}

PostgresCdcSource::Options cdc_opts(const std::string& slot, const std::string& pub) {
    PostgresCdcSource::Options o;
    o.conninfo = pg_dsn();
    o.slot_name = slot;
    o.plugin = "pgoutput";
    o.publication_names = pub;
    o.create_slot = true;
    o.standby_status_interval = std::chrono::milliseconds{0};  // deterministic in-test
    return o;
}

std::int64_t id_of(const CdcEvent& e) {
    for (const auto& f : e.values) {
        if (f.name == "id" && !f.is_null) {
            return std::stol(f.value);
        }
    }
    return -1;
}

std::uint64_t counter_value(const std::string& name) {
    for (const auto& [k, v] : clink::MetricsRegistry::global().snapshot().counters) {
        if (k == name) {
            return v;
        }
    }
    return 0;
}

}  // namespace

#ifdef CLINK_HAS_POSTGRES

TEST(PostgresCdcLive, PgoutputDecodesInsertUpdateDelete) {
    if (!pg_configured()) {
        GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN (docker/integration-services.yml)";
    }
    const std::string dsn = pg_dsn();
    const std::string tbl = "cdc_live_" + uniq();
    const std::string pub = "pub_" + uniq();
    const std::string slot = "slot_" + uniq();
    run_sql(dsn, "DROP TABLE IF EXISTS " + tbl);
    run_sql(dsn, "CREATE TABLE " + tbl + " (id int primary key, val text)");
    run_sql(dsn, "DROP PUBLICATION IF EXISTS " + pub);
    run_sql(dsn, "CREATE PUBLICATION " + pub + " FOR TABLE " + tbl);

    const std::string recs =
        R"(clink_connector_records_total{connector="postgres_cdc",direction="source"})";
    const std::string bytes =
        R"(clink_connector_bytes_total{connector="postgres_cdc",direction="source"})";
    const auto recs0 = counter_value(recs);
    const auto bytes0 = counter_value(bytes);

    auto o = cdc_opts(slot, pub);
    o.drop_slot_on_close = true;
    PostgresCdcSource src(o);
    src.open();  // creates the slot at the current LSN, BEFORE the DML below

    run_sql(dsn, "INSERT INTO " + tbl + " VALUES (1, 'a')");
    run_sql(dsn, "UPDATE " + tbl + " SET val = 'b' WHERE id = 1");
    run_sql(dsn, "DELETE FROM " + tbl + " WHERE id = 1");

    Captured cap;
    drain_until(src, cap, /*want_changes=*/3, /*timeout_ms=*/30000);
    src.close();
    run_sql(dsn, "DROP TABLE IF EXISTS " + tbl);
    run_sql(dsn, "DROP PUBLICATION IF EXISTS " + pub);

    // The three row changes are present, in order, for our table, with LSNs.
    std::vector<CdcEvent::Op> ops;
    for (const auto& e : cap.events) {
        if (e.op == CdcEvent::Op::Insert || e.op == CdcEvent::Op::Update ||
            e.op == CdcEvent::Op::Delete) {
            ops.push_back(e.op);
            EXPECT_EQ(e.table, "public." + tbl) << "unexpected table";
            EXPECT_FALSE(e.lsn.empty()) << "change event missing LSN";
        }
    }
    ASSERT_EQ(ops.size(), 3u);
    EXPECT_EQ(ops[0], CdcEvent::Op::Insert);
    EXPECT_EQ(ops[1], CdcEvent::Op::Update);
    EXPECT_EQ(ops[2], CdcEvent::Op::Delete);

    // Metrics fired (delta-asserted against the shared registry).
    EXPECT_GT(counter_value(recs) - recs0, 0u);
    EXPECT_GT(counter_value(bytes) - bytes0, 0u);
}

// M5: the CdcEvent -> flat-JSON-row transform (postgres_cdc_source's Row path)
// applied to a REAL pgoutput stream. Proves the composed path: data columns flat,
// __op/__table metadata, a SQL NULL column -> JSON null, markers -> nullopt.
TEST(PostgresCdcLive, CdcEventToJsonRowOnLiveStream) {
    if (!pg_configured()) {
        GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN";
    }
    const std::string dsn = pg_dsn();
    const std::string tbl = "cdc_json_" + uniq();
    const std::string pub = "pubj_" + uniq();
    const std::string slot = "slotj_" + uniq();
    run_sql(dsn, "DROP TABLE IF EXISTS " + tbl);
    run_sql(dsn, "CREATE TABLE " + tbl + " (id int primary key, val text)");
    run_sql(dsn, "DROP PUBLICATION IF EXISTS " + pub);
    run_sql(dsn, "CREATE PUBLICATION " + pub + " FOR TABLE " + tbl);

    auto o = cdc_opts(slot, pub);
    o.drop_slot_on_close = true;
    PostgresCdcSource src(o);
    src.open();
    run_sql(dsn, "INSERT INTO " + tbl + " VALUES (1, NULL)");  // NULL column

    Captured cap;
    drain_until(src, cap, /*want_changes=*/1, /*timeout_ms=*/30000);
    src.close();
    run_sql(dsn, "DROP TABLE IF EXISTS " + tbl);
    run_sql(dsn, "DROP PUBLICATION IF EXISTS " + pub);

    bool saw_insert = false;
    for (const auto& e : cap.events) {
        auto row = clink::pgcdc::cdc_event_to_json_row(e);
        if (e.op == CdcEvent::Op::Insert) {
            ASSERT_TRUE(row.has_value());
            auto j = clink::config::parse(*row);
            const auto& obj = j.as_object();
            EXPECT_EQ(obj.at("__op").as_string(), "insert");
            EXPECT_EQ(obj.at("__table").as_string(), "public." + tbl);
            EXPECT_EQ(obj.at("id").as_string(), "1");
            EXPECT_TRUE(obj.at("val").is_null()) << "a SQL NULL column must emit JSON null";
            saw_insert = true;
        } else if (e.op == CdcEvent::Op::Begin || e.op == CdcEvent::Op::Commit) {
            EXPECT_FALSE(row.has_value()) << "transaction markers must be dropped";
        }
    }
    EXPECT_TRUE(saw_insert) << "should have decoded the insert";
}

TEST(PostgresCdcLive, LsnCheckpointResumesWithoutGap) {
    if (!pg_configured()) {
        GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN";
    }
    const std::string dsn = pg_dsn();
    const std::string tbl = "cdc_replay_" + uniq();
    const std::string pub = "pubr_" + uniq();
    const std::string slot = "slotr_" + uniq();
    run_sql(dsn, "DROP TABLE IF EXISTS " + tbl);
    run_sql(dsn, "CREATE TABLE " + tbl + " (id int primary key, val text)");
    run_sql(dsn, "DROP PUBLICATION IF EXISTS " + pub);
    run_sql(dsn, "CREATE PUBLICATION " + pub + " FOR TABLE " + tbl);

    InMemoryStateBackend backend;
    const OperatorId op_id{1};
    std::set<std::int64_t> first;
    std::set<std::int64_t> second;

    // First reader: consume rows 1..5, then checkpoint the LSN. Keep the slot.
    {
        PostgresCdcSource s1(cdc_opts(slot, pub));  // drop_slot_on_close defaults false
        s1.open();
        for (int i = 1; i <= 5; ++i) {
            run_sql(dsn, "INSERT INTO " + tbl + " VALUES (" + std::to_string(i) + ", 'x')");
        }
        Captured cap;
        drain_until(s1, cap, 5, 30000);
        s1.snapshot_offset(backend, op_id, clink::CheckpointId{1});
        s1.close();
        for (const auto& e : cap.events) {
            if (e.op == CdcEvent::Op::Insert) {
                first.insert(id_of(e));
            }
        }
        ASSERT_EQ(first.size(), 5u);
    }
    // Resumed reader: restore the LSN, reuse the slot, read rows 6..10 only.
    {
        // cdc_opts keeps drop_slot_on_close=false (the Options default), so s1
        // left the slot in place; s2 reuses it and tears it down via drop_slot().
        PostgresCdcSource s2(cdc_opts(slot, pub));
        ASSERT_TRUE(s2.restore_offset(backend, op_id));
        s2.open();
        // Negative control (review H3): prove the RESTORED checkpoint LSN, not
        // the slot default, drove START_REPLICATION. s2 reuses the existing slot
        // (create_slot hits "already exists" -> slot_was_created=false), so if
        // restore_offset had been a no-op the start position would be "0/0" (the
        // slot default). A concrete restored LSN here means the disjointness
        // assertion below is attributable to the restore, not to the slot's
        // server-side confirmed_flush position (which is non-deterministic here).
        EXPECT_NE(s2.start_position(), "0/0") << "resume used the slot default, not the checkpoint";
        EXPECT_FALSE(s2.start_position().empty());
        for (int i = 6; i <= 10; ++i) {
            run_sql(dsn, "INSERT INTO " + tbl + " VALUES (" + std::to_string(i) + ", 'y')");
        }
        Captured cap;
        drain_until(s2, cap, 5, 30000);
        s2.close();
        s2.drop_slot();  // teardown the persisted slot
        for (const auto& e : cap.events) {
            if (e.op == CdcEvent::Op::Insert) {
                second.insert(id_of(e));
            }
        }
    }
    run_sql(dsn, "DROP TABLE IF EXISTS " + tbl);
    run_sql(dsn, "DROP PUBLICATION IF EXISTS " + pub);

    // The resumed reader saw the NEW rows only (no replay of 1..5), covering all
    // of 6..10 (no gap).
    for (std::int64_t id : second) {
        EXPECT_EQ(first.count(id), 0u) << "resumed reader re-read checkpointed row " << id;
    }
    for (std::int64_t id = 6; id <= 10; ++id) {
        EXPECT_EQ(second.count(id), 1u) << "resumed reader missed row " << id;
    }
}

#endif  // CLINK_HAS_POSTGRES
