// CassandraUpsertSink tests - the changelog-aware upsert sink (mode='upsert').
//
// Construction validation runs anywhere. The LIVE integration tests are SKIPPED
// unless CLINK_CASSANDRA_TEST_CONTACT_POINTS is set. They prove a changelog
// stream maintains the table by primary key: insert/update_after upserts (CQL
// INSERT), delete/update_before DELETEs by key, netted within a flush, replay-
// idempotent.

#include <cassandra.h>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cassandra/cassandra_upsert_sink.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::cassandra::CassandraUpsertSink;

namespace {

// --- construction validation (no server) --------------------------------

TEST(CassandraUpsertSink, RejectsEmptyKeyColumns) {
    CassandraUpsertSink::Options o;
    o.keyspace = "ks";
    o.table = "t";
    EXPECT_THROW(CassandraUpsertSink{std::move(o)}, std::runtime_error);
}

// --- live integration ---------------------------------------------------

bool cass_configured() {
    return std::getenv("CLINK_CASSANDRA_TEST_CONTACT_POINTS") != nullptr;
}
std::string cass_contact_points() {
    return std::getenv("CLINK_CASSANDRA_TEST_CONTACT_POINTS");
}

CassSession* connect_raw(CassCluster** out_cluster) {
    CassCluster* cluster = cass_cluster_new();
    cass_cluster_set_contact_points(cluster, cass_contact_points().c_str());
    cass_cluster_set_connect_timeout(cluster, 10000);
    CassSession* session = cass_session_new();
    CassFuture* cf = cass_session_connect(session, cluster);
    cass_future_wait(cf);
    const bool ok = cass_future_error_code(cf) == CASS_OK;
    cass_future_free(cf);
    if (!ok) {
        cass_session_free(session);
        cass_cluster_free(cluster);
        return nullptr;
    }
    *out_cluster = cluster;
    return session;
}

bool run_cql(CassSession* s, const std::string& cql) {
    CassStatement* st = cass_statement_new(cql.c_str(), 0);
    CassFuture* f = cass_session_execute(s, st);
    cass_future_wait(f);
    const bool ok = cass_future_error_code(f) == CASS_OK;
    cass_future_free(f);
    cass_statement_free(st);
    return ok;
}

// SELECT COUNT(*) ... -> row count.
long count_rows(CassSession* s, const std::string& cql) {
    CassStatement* st = cass_statement_new(cql.c_str(), 0);
    CassFuture* f = cass_session_execute(s, st);
    cass_future_wait(f);
    long out = -1;
    if (cass_future_error_code(f) == CASS_OK) {
        const CassResult* res = cass_future_get_result(f);
        const CassRow* row = cass_result_first_row(res);
        if (row != nullptr) {
            cass_int64_t v = 0;
            cass_value_get_int64(cass_row_get_column(row, 0), &v);
            out = static_cast<long>(v);
        }
        cass_result_free(res);
    }
    cass_future_free(f);
    cass_statement_free(st);
    return out;
}

// SELECT v ... -> the text value (empty if no row).
std::string text_of(CassSession* s, const std::string& cql) {
    CassStatement* st = cass_statement_new(cql.c_str(), 0);
    CassFuture* f = cass_session_execute(s, st);
    cass_future_wait(f);
    std::string out;
    if (cass_future_error_code(f) == CASS_OK) {
        const CassResult* res = cass_future_get_result(f);
        const CassRow* row = cass_result_first_row(res);
        if (row != nullptr) {
            const char* str = nullptr;
            std::size_t len = 0;
            if (cass_value_get_string(cass_row_get_column(row, 0), &str, &len) == CASS_OK) {
                out.assign(str, len);
            }
        }
        cass_result_free(res);
    }
    cass_future_free(f);
    cass_statement_free(st);
    return out;
}

CassandraUpsertSink::Options opts_for(const std::string& ks, const std::string& table) {
    CassandraUpsertSink::Options o;
    o.conn.contact_points = cass_contact_points();
    o.keyspace = ks;
    o.table = table;
    o.key_columns = {"id"};
    return o;
}

void apply_changelog(const std::string& ks,
                     const std::string& table,
                     const std::vector<std::string>& rows) {
    CassandraUpsertSink sink(opts_for(ks, table));
    sink.open();
    Batch<std::string> b;
    for (const auto& r : rows) {
        b.emplace(std::string(r));
    }
    sink.on_data(b);
    sink.on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink.close();
}

#define REQUIRE_LIVE_CASS()                                            \
    do {                                                               \
        if (!cass_configured())                                        \
            GTEST_SKIP() << "set CLINK_CASSANDRA_TEST_CONTACT_POINTS"; \
    } while (0)

struct LiveTable {
    CassSession* session{nullptr};
    CassCluster* cluster{nullptr};
    std::string ks{"clink_upsert_it"};
    std::string table;
    bool ok{false};

    LiveTable() {
        table = "t_" + std::to_string(static_cast<long>(::getpid()));
        session = connect_raw(&cluster);
        if (session == nullptr) {
            return;
        }
        ok = run_cql(session,
                     "CREATE KEYSPACE IF NOT EXISTS " + ks +
                         " WITH replication = {'class':'SimpleStrategy','replication_factor':1}") &&
             run_cql(session, "DROP TABLE IF EXISTS " + ks + "." + table) &&
             run_cql(session,
                     "CREATE TABLE " + ks + "." + table + " (id int PRIMARY KEY, val text)");
    }
    ~LiveTable() {
        if (session != nullptr) {
            run_cql(session, "DROP TABLE IF EXISTS " + ks + "." + table);
            CassFuture* cf = cass_session_close(session);
            cass_future_wait(cf);
            cass_future_free(cf);
            cass_session_free(session);
        }
        if (cluster != nullptr) {
            cass_cluster_free(cluster);
        }
    }
    std::string fqn() const { return ks + "." + table; }
};

TEST(CassandraUpsertSinkLive, InsertThenUpdateByKey) {
    REQUIRE_LIVE_CASS();
    LiveTable t;
    ASSERT_NE(t.session, nullptr) << "could not connect to Cassandra";
    ASSERT_TRUE(t.ok) << "schema setup failed";

    apply_changelog(t.ks, t.table, {R"({"id":1,"val":"a"})", R"({"id":2,"val":"b"})"});
    EXPECT_EQ(count_rows(t.session, "SELECT COUNT(*) FROM " + t.fqn()), 2);

    apply_changelog(t.ks, t.table, {R"({"id":1,"val":"a2","__row_kind":"update_after"})"});
    EXPECT_EQ(count_rows(t.session, "SELECT COUNT(*) FROM " + t.fqn()), 2);
    EXPECT_EQ(text_of(t.session, "SELECT val FROM " + t.fqn() + " WHERE id=1"), "a2");
}

TEST(CassandraUpsertSinkLive, DeleteRemovesByKey) {
    REQUIRE_LIVE_CASS();
    LiveTable t;
    ASSERT_NE(t.session, nullptr) << "could not connect to Cassandra";
    ASSERT_TRUE(t.ok) << "schema setup failed";

    apply_changelog(t.ks, t.table, {R"({"id":1,"val":"a"})", R"({"id":2,"val":"b"})"});
    apply_changelog(t.ks, t.table, {R"({"id":1,"__row_kind":"delete"})"});

    EXPECT_EQ(count_rows(t.session, "SELECT COUNT(*) FROM " + t.fqn()), 1);
    EXPECT_EQ(text_of(t.session, "SELECT val FROM " + t.fqn() + " WHERE id=2"), "b");
}

TEST(CassandraUpsertSinkLive, ReplayIsIdempotent) {
    REQUIRE_LIVE_CASS();
    LiveTable t;
    ASSERT_NE(t.session, nullptr) << "could not connect to Cassandra";
    ASSERT_TRUE(t.ok) << "schema setup failed";

    const std::vector<std::string> batch = {
        R"({"id":1,"val":"a"})", R"({"id":2,"val":"b"})", R"({"id":1,"__row_kind":"delete"})"};
    apply_changelog(t.ks, t.table, batch);
    apply_changelog(t.ks, t.table, batch);

    EXPECT_EQ(count_rows(t.session, "SELECT COUNT(*) FROM " + t.fqn()), 1);
    EXPECT_EQ(text_of(t.session, "SELECT val FROM " + t.fqn() + " WHERE id=2"), "b");
}

}  // namespace
