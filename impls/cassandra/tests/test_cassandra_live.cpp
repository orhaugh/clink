// Cassandra LIVE integration test. SKIPPED unless CLINK_CASSANDRA_TEST_CONTACT_POINTS is set
// (e.g. 127.0.0.1). Proves against a real cluster: the sink JSON-inserts rows that read back with
// the right values. The test provisions the keyspace + table via the C API (the sink does not).

#include <algorithm>
#include <cassandra.h>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cassandra/cassandra_sink.hpp"
#include "clink/core/record.hpp"

using clink::Batch;
using clink::cassandra::CassandraSink;

namespace {

bool cass_configured() {
    return std::getenv("CLINK_CASSANDRA_TEST_CONTACT_POINTS") != nullptr;
}
std::string cass_contact_points() {
    return std::getenv("CLINK_CASSANDRA_TEST_CONTACT_POINTS");
}

// Minimal raw-session helper for schema setup + read-back. Returns nullptr on connect failure.
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

}  // namespace

TEST(CassandraLive, JsonInsertThenReadBack) {
    if (!cass_configured()) {
        GTEST_SKIP() << "set CLINK_CASSANDRA_TEST_CONTACT_POINTS (e.g. 127.0.0.1)";
    }
    CassCluster* cluster = nullptr;
    CassSession* session = connect_raw(&cluster);
    ASSERT_NE(session, nullptr) << "could not connect to Cassandra";

    const std::string ks = "clink_it";
    const std::string table = "t_" + std::to_string(static_cast<long>(::getpid()));
    ASSERT_TRUE(run_cql(session,
                        "CREATE KEYSPACE IF NOT EXISTS " + ks +
                            " WITH replication = {'class':'SimpleStrategy',"
                            "'replication_factor':1}"));
    ASSERT_TRUE(run_cql(
        session,
        "CREATE TABLE IF NOT EXISTS " + ks + "." + table + " (id int PRIMARY KEY, v text)"));

    {
        CassandraSink::Options o;
        o.conn.contact_points = cass_contact_points();
        o.keyspace = ks;
        o.table = table;
        CassandraSink sink(std::move(o));
        sink.open();
        Batch<std::string> b;
        b.emplace(R"({"id":1,"v":"a"})");
        b.emplace(R"({"id":2,"v":"b"})");
        b.emplace(R"({"id":3,"v":"c"})");
        sink.on_data(b);
        sink.flush();  // throws on any insert failure
        sink.close();
    }

    // Read back the v values and assert the set {a,b,c}.
    std::vector<std::string> vs;
    CassStatement* sel = cass_statement_new(("SELECT v FROM " + ks + "." + table).c_str(), 0);
    CassFuture* f = cass_session_execute(session, sel);
    cass_future_wait(f);
    ASSERT_EQ(cass_future_error_code(f), CASS_OK);
    const CassResult* res = cass_future_get_result(f);
    CassIterator* it = cass_iterator_from_result(res);
    while (cass_iterator_next(it) == cass_true) {
        const CassRow* row = cass_iterator_get_row(it);
        const char* str = nullptr;
        std::size_t len = 0;
        cass_value_get_string(cass_row_get_column(row, 0), &str, &len);
        vs.emplace_back(str, len);
    }
    cass_iterator_free(it);
    cass_result_free(res);
    cass_future_free(f);
    cass_statement_free(sel);

    EXPECT_EQ(vs.size(), 3u);
    EXPECT_NE(std::find(vs.begin(), vs.end(), "a"), vs.end());
    EXPECT_NE(std::find(vs.begin(), vs.end(), "b"), vs.end());
    EXPECT_NE(std::find(vs.begin(), vs.end(), "c"), vs.end());

    run_cql(session, "DROP TABLE " + ks + "." + table);  // best-effort cleanup
    cass_session_free(session);
    cass_cluster_free(cluster);
}
