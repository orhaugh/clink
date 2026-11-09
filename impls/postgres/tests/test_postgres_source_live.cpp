// The PostgreSQL SELECT source against a live server. SKIPPED unless
// CLINK_POSTGRES_CDC_TEST_DSN names one; scripts/postgres-live.sh runs it and
// fails when any case is skipped.
//
// Each case drives the source as a job does at the operator level: one source
// reads part of the result and is checkpointed into a state backend, the table
// changes or not, and a second source restores from that checkpoint, runs the
// query again and reads to the end. What the two emitted between them is the
// verdict: rows repeated are allowed (at least once), a row skipped is not.

#include <cstdint>
#include <cstdlib>
#include <libpq-fe.h>
#include <map>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/postgres_source.hpp"
#include "clink/core/record.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using clink::CheckpointId;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::PostgresRow;
using clink::PostgresSource;
using clink::StreamElement;

namespace {

bool server_configured() {
    return std::getenv("CLINK_POSTGRES_CDC_TEST_DSN") != nullptr;
}
std::string dsn() {
    return std::getenv("CLINK_POSTGRES_CDC_TEST_DSN");
}

#define REQUIRE_SERVER()                                                                       \
    do {                                                                                       \
        if (!server_configured()) {                                                            \
            GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN (scripts/postgres-live.sh does)"; \
        }                                                                                      \
    } while (0)

// The test's own connection, for the table the source reads.
class Pg {
public:
    Pg() : c_(PQconnectdb(dsn().c_str())) {
        if (PQstatus(c_) != CONNECTION_OK) {
            throw std::runtime_error(std::string("the test cannot connect: ") + PQerrorMessage(c_));
        }
    }
    ~Pg() { PQfinish(c_); }
    Pg(const Pg&) = delete;
    Pg& operator=(const Pg&) = delete;
    void exec(const std::string& sql) {
        PGresult* r = PQexec(c_, sql.c_str());
        const auto st = PQresultStatus(r);
        const std::string err = PQerrorMessage(c_);
        PQclear(r);
        if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
            throw std::runtime_error("the test's statement failed: " + sql + ": " + err);
        }
    }

private:
    PGconn* c_;
};

std::string table_name() {
    static int n = 0;
    return "clink_select_src_" + std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(++n);
}

// A table of ids 1..n, each with a value naming it.
std::string make_table(Pg& pg, int n) {
    const auto t = table_name();
    pg.exec("DROP TABLE IF EXISTS " + t);
    pg.exec("CREATE TABLE " + t + " (id BIGINT PRIMARY KEY, v TEXT)");
    pg.exec("INSERT INTO " + t + " SELECT g, 'v' || g FROM generate_series(1, " +
            std::to_string(n) + ") AS g");
    return t;
}

PostgresSource::Options options(const std::string& query, std::size_t batch) {
    PostgresSource::Options o;
    o.conninfo = dsn();
    o.query = query;
    o.batch_size = batch;
    return o;
}

// Every row's first column, from one produce() call or all of them.
std::vector<std::string> read(PostgresSource& src, bool to_end) {
    std::vector<std::string> ids;
    Emitter<PostgresRow> em(Emitter<PostgresRow>::Forward([&ids](StreamElement<PostgresRow> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                ids.push_back(r.value().values()[0]);
            }
        }
        return true;
    }));
    while (src.produce(em) && to_end) {
    }
    return ids;
}

std::vector<std::string> ids(int from, int to) {
    std::vector<std::string> out;
    for (int i = from; i <= to; ++i) {
        out.push_back(std::to_string(i));
    }
    return out;
}

const OperatorId kOp{4242};

}  // namespace

TEST(PostgresSourceLive, EveryRowArrivesWithItsValuesAndNulls) {
    REQUIRE_SERVER();
    Pg pg;
    const auto t = table_name();
    pg.exec("DROP TABLE IF EXISTS " + t);
    pg.exec("CREATE TABLE " + t + " (id BIGINT PRIMARY KEY, v TEXT, n NUMERIC)");
    pg.exec("INSERT INTO " + t +
            " VALUES (1, 'plain', 12345678901234567890.123456789), (2, '', NULL), (3, NULL, 0)");
    PostgresSource src(options("SELECT id, v, n FROM " + t + " ORDER BY id", 256));
    src.open();
    std::vector<PostgresRow> rows;
    Emitter<PostgresRow> em(Emitter<PostgresRow>::Forward([&rows](StreamElement<PostgresRow> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                rows.push_back(r.value());
            }
        }
        return true;
    }));
    while (src.produce(em)) {
    }
    src.close();
    ASSERT_EQ(rows.size(), 3U);
    EXPECT_EQ(rows[0].values()[1], "plain");
    EXPECT_EQ(rows[0].values()[2], "12345678901234567890.123456789");
    EXPECT_FALSE(rows[1].is_null(1));
    EXPECT_EQ(rows[1].values()[1], "");
    EXPECT_TRUE(rows[1].is_null(2));
    EXPECT_TRUE(rows[2].is_null(1));
    pg.exec("DROP TABLE " + t);
}

// Over the same rows in the same order, the restored source resumes after
// those the checkpoint covered: none repeated, none skipped.
TEST(PostgresSourceLive, ARestoreOverTheSameRowsResumesAfterThem) {
    REQUIRE_SERVER();
    Pg pg;
    const auto t = make_table(pg, 10);
    const auto q = "SELECT id, v FROM " + t + " ORDER BY id";
    InMemoryStateBackend backend;
    PostgresSource first(options(q, 4));
    first.open();
    EXPECT_EQ(read(first, false), ids(1, 4));
    first.snapshot_offset(backend, kOp, CheckpointId{1});
    first.close();

    PostgresSource second(options(q, 4));
    ASSERT_TRUE(second.restore_offset(backend, kOp));
    second.open();
    EXPECT_EQ(read(second, true), ids(5, 10));
    second.close();
    pg.exec("DROP TABLE " + t);
}

// A row deleted among those the checkpoint covered moves every later row up
// one place. Resumed by position, the source skipped the first row after the
// checkpoint (id 5, now at the fourth place). It now sees the rows differ and
// emits every row again.
TEST(PostgresSourceLive, ARestoreAfterAnEarlierRowWasDeletedEmitsEveryRowAgain) {
    REQUIRE_SERVER();
    Pg pg;
    const auto t = make_table(pg, 10);
    const auto q = "SELECT id, v FROM " + t + " ORDER BY id";
    InMemoryStateBackend backend;
    PostgresSource first(options(q, 4));
    first.open();
    EXPECT_EQ(read(first, false), ids(1, 4));
    first.snapshot_offset(backend, kOp, CheckpointId{1});
    first.close();

    pg.exec("DELETE FROM " + t + " WHERE id = 2");
    PostgresSource second(options(q, 4));
    ASSERT_TRUE(second.restore_offset(backend, kOp));
    second.open();
    auto want = ids(1, 10);
    want.erase(want.begin() + 1);
    EXPECT_EQ(read(second, true), want);
    second.close();
    pg.exec("DROP TABLE " + t);
}

// A row changed among those the checkpoint covered: the same count, other
// rows. Every row is emitted again.
TEST(PostgresSourceLive, ARestoreAfterAnEarlierRowChangedEmitsEveryRowAgain) {
    REQUIRE_SERVER();
    Pg pg;
    const auto t = make_table(pg, 6);
    const auto q = "SELECT id, v FROM " + t + " ORDER BY id";
    InMemoryStateBackend backend;
    PostgresSource first(options(q, 3));
    first.open();
    EXPECT_EQ(read(first, false), ids(1, 3));
    first.snapshot_offset(backend, kOp, CheckpointId{1});
    first.close();

    pg.exec("UPDATE " + t + " SET v = 'changed' WHERE id = 3");
    PostgresSource second(options(q, 3));
    ASSERT_TRUE(second.restore_offset(backend, kOp));
    second.open();
    EXPECT_EQ(read(second, true), ids(1, 6));
    second.close();
    pg.exec("DROP TABLE " + t);
}

// Rows added after those the checkpoint covered leave them in place: the
// restored source resumes, and reads the new rows.
TEST(PostgresSourceLive, ARestoreAfterRowsWereAddedAtTheEndResumes) {
    REQUIRE_SERVER();
    Pg pg;
    const auto t = make_table(pg, 6);
    const auto q = "SELECT id, v FROM " + t + " ORDER BY id";
    InMemoryStateBackend backend;
    PostgresSource first(options(q, 3));
    first.open();
    EXPECT_EQ(read(first, false), ids(1, 3));
    first.snapshot_offset(backend, kOp, CheckpointId{1});
    first.close();

    pg.exec("INSERT INTO " + t + " VALUES (7, 'v7'), (8, 'v8')");
    PostgresSource second(options(q, 3));
    ASSERT_TRUE(second.restore_offset(backend, kOp));
    second.open();
    EXPECT_EQ(read(second, true), ids(4, 8));
    second.close();
    pg.exec("DROP TABLE " + t);
}

// A position from an earlier release holds no hash to compare: every row is
// emitted again rather than resumed on trust.
TEST(PostgresSourceLive, AnEarlierReleasesPositionEmitsEveryRowAgain) {
    REQUIRE_SERVER();
    Pg pg;
    const auto t = make_table(pg, 5);
    InMemoryStateBackend backend;
    const std::string key = "__postgres_source_row__";
    std::string legacy(8, '\0');
    legacy[0] = 3;
    backend.put_operator_state(kOp,
                               clink::StateBackend::KeyView{key.data(), key.size()},
                               clink::StateBackend::ValueView{legacy.data(), legacy.size()});
    PostgresSource src(options("SELECT id, v FROM " + t + " ORDER BY id", 256));
    ASSERT_TRUE(src.restore_offset(backend, kOp));
    src.open();
    EXPECT_EQ(read(src, true), ids(1, 5));
    src.close();
    pg.exec("DROP TABLE " + t);
}
