// The PostgreSQL CDC source against a replication slot that has lost WAL its
// checkpoint needs. Its own server, so that capping slot WAL cannot invalidate
// another test's slot: max_slot_wal_keep_size is set low, the job's slot is
// left behind while the server writes and checkpoints past the cap, and the
// server invalidates it (wal_status = 'lost'). The restore must refuse rather
// than stream on from wherever the server could still start, which would skip
// the changes the lost WAL held.

#include <chrono>
#include <libpq-fe.h>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include "clink/connectors/cdc_event.hpp"
#include "clink/connectors/postgres_cdc_source.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#include "tests/integration/docker_postgres.hpp"

namespace {

using namespace std::chrono_literals;
using clink::CdcEvent;
using clink::Emitter;
using clink::PostgresCdcSource;
using clink::StreamElement;

std::string run(const std::string& conninfo, const std::string& sql) {
    PGconn* c = PQconnectdb(conninfo.c_str());
    if (PQstatus(c) != CONNECTION_OK) {
        const std::string err = PQerrorMessage(c);
        PQfinish(c);
        throw std::runtime_error("connect: " + err);
    }
    PGresult* r = PQexec(c, sql.c_str());
    const auto st = PQresultStatus(r);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(c);
        PQclear(r);
        PQfinish(c);
        throw std::runtime_error(sql + " -> " + err);
    }
    std::string out;
    if (st == PGRES_TUPLES_OK && PQntuples(r) > 0 && PQgetisnull(r, 0, 0) == 0) {
        out = PQgetvalue(r, 0, 0);
    }
    PQclear(r);
    PQfinish(c);
    return out;
}

PostgresCdcSource::Options options(const std::string& conninfo) {
    PostgresCdcSource::Options o;
    o.conninfo = conninfo;
    o.slot_name = "lost_wal_slot";
    o.plugin = "pgoutput";
    o.publication_names = "lost_wal_pub";
    o.table = "public.lost_wal";
    o.key_columns = {"id"};
    o.strict = true;
    o.decode_error_policy = PostgresCdcSource::DecodeErrorPolicy::Fail;
    o.standby_status_interval = 0ms;
    return o;
}

}  // namespace

TEST(PostgresCdcSlot, ASlotThatLostItsWalIsRefused) {
    if (!clink::test::DockerPostgres::docker_available()) {
        GTEST_SKIP() << "Docker not available; skipping the lost-WAL refusal";
    }
    clink::test::DockerPostgresOptions po;
    po.postgres_args = {"-c",
                        "wal_level=logical",
                        "-c",
                        "max_slot_wal_keep_size=1MB",
                        "-c",
                        "max_wal_size=32MB",
                        "-c",
                        "min_wal_size=32MB"};
    clink::test::DockerPostgres pg(po);
    const std::string conn = pg.conninfo();
    run(conn, "CREATE TABLE lost_wal (id bigint PRIMARY KEY, v text)");
    run(conn, "CREATE PUBLICATION lost_wal_pub FOR TABLE lost_wal");

    clink::InMemoryStateBackend backend;
    const clink::OperatorId op{1};
    {
        PostgresCdcSource src(options(conn));
        src.open();
        run(conn, "INSERT INTO lost_wal VALUES (1, 'a'), (2, 'b')");
        std::size_t changes = 0;
        Emitter<CdcEvent> em{Emitter<CdcEvent>::Forward{[&](StreamElement<CdcEvent> e) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    if (r.value().op == CdcEvent::Op::Insert) {
                        ++changes;
                    }
                }
            }
            return true;
        }}};
        const auto deadline = std::chrono::steady_clock::now() + 30s;
        while (changes < 2 && std::chrono::steady_clock::now() < deadline) {
            ASSERT_TRUE(src.produce(em));
        }
        ASSERT_EQ(changes, 2U);
        src.snapshot_offset(backend, op, clink::CheckpointId{1});
        src.close();
    }

    // Write and switch WAL segments past the cap, checkpointing, until the
    // server invalidates the idle slot.
    std::string status;
    for (int i = 0; i < 40 && status != "lost"; ++i) {
        run(conn,
            "INSERT INTO lost_wal SELECT g, repeat('x', 1000) FROM generate_series(" +
                std::to_string(1000 + (i * 2000)) + ", " + std::to_string(2999 + (i * 2000)) +
                ") g");
        run(conn, "SELECT pg_switch_wal()");
        run(conn, "CHECKPOINT");
        status = run(
            conn, "SELECT wal_status FROM pg_replication_slots WHERE slot_name = 'lost_wal_slot'");
    }
    ASSERT_EQ(status, "lost") << "the server never invalidated the slot";

    PostgresCdcSource restored(options(conn));
    ASSERT_TRUE(restored.restore_offset(backend, op));
    try {
        restored.open();
        FAIL() << "a restore onto a slot that lost its WAL carried on";
    } catch (const std::exception& e) {
        EXPECT_NE(std::string{e.what()}.find("lost WAL"), std::string::npos) << e.what();
    }
}
