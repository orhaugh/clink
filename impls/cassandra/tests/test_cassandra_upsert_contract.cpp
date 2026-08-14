// Live instantiation of the public upsert contract suite against a real
// Cassandra: idempotency-key collapse is the storage model itself - an
// INSERT is an upsert by primary key. Self-skips without
// CLINK_CASSANDRA_TEST_CONTACT_POINTS, same convention as the other live
// tests in this directory.

#include <cassandra.h>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cassandra/cassandra_upsert_sink.hpp"
#include "clink/test/upsert_contract.hpp"

namespace {

using clink::cassandra::CassandraUpsertSink;
using clink::test::UpsertContractFixture;
using clink::test::UpsertState;

bool cass_configured() {
    return std::getenv("CLINK_CASSANDRA_TEST_CONTACT_POINTS") != nullptr;
}
std::string cass_contact_points() {
    const char* v = std::getenv("CLINK_CASSANDRA_TEST_CONTACT_POINTS");
    return v == nullptr ? std::string{} : std::string{v};
}

CassSession* connect_raw(CassCluster** out_cluster) {
    CassCluster* cluster = cass_cluster_new();
    cass_cluster_set_contact_points(cluster, cass_contact_points().c_str());
    CassSession* session = cass_session_new();
    CassFuture* cf = cass_session_connect(session, cluster);
    cass_future_wait(cf);
    const bool ok = cass_future_error_code(cf) == CASS_OK;
    cass_future_free(cf);
    if (!ok) {
        cass_session_free(session);
        cass_cluster_free(cluster);
        *out_cluster = nullptr;
        return nullptr;
    }
    *out_cluster = cluster;
    return session;
}

// Run one statement (and drop the session) - the fixture setup path.
bool run_cql(const std::string& cql) {
    CassCluster* cluster = nullptr;
    CassSession* session = connect_raw(&cluster);
    if (session == nullptr) {
        return false;
    }
    CassStatement* st = cass_statement_new(cql.c_str(), 0);
    CassFuture* f = cass_session_execute(session, st);
    cass_future_wait(f);
    const bool ok = cass_future_error_code(f) == CASS_OK;
    cass_future_free(f);
    cass_statement_free(st);
    CassFuture* cf = cass_session_close(session);
    cass_future_wait(cf);
    cass_future_free(cf);
    cass_session_free(session);
    cass_cluster_free(cluster);
    return ok;
}

// All (id, val) rows of ks.table, any order.
UpsertState rows_of(const std::string& ks, const std::string& table) {
    UpsertState out;
    CassCluster* cluster = nullptr;
    CassSession* session = connect_raw(&cluster);
    if (session == nullptr) {
        return out;
    }
    CassStatement* st = cass_statement_new(("SELECT id, val FROM " + ks + "." + table).c_str(), 0);
    CassFuture* f = cass_session_execute(session, st);
    cass_future_wait(f);
    if (cass_future_error_code(f) == CASS_OK) {
        const CassResult* res = cass_future_get_result(f);
        CassIterator* it = cass_iterator_from_result(res);
        while (cass_iterator_next(it) == cass_true) {
            const CassRow* row = cass_iterator_get_row(it);
            cass_int32_t id = 0;
            cass_value_get_int32(cass_row_get_column(row, 0), &id);
            const char* str = nullptr;
            std::size_t len = 0;
            std::string val;
            if (cass_value_get_string(cass_row_get_column(row, 1), &str, &len) == CASS_OK) {
                val.assign(str, len);
            }
            out.emplace_back(std::to_string(id), val);
        }
        cass_iterator_free(it);
        cass_result_free(res);
    }
    cass_future_free(f);
    cass_statement_free(st);
    CassFuture* cf = cass_session_close(session);
    cass_future_wait(cf);
    cass_future_free(cf);
    cass_session_free(session);
    cass_cluster_free(cluster);
    return out;
}

std::string row_json(int id, const std::string& val) {
    return "{\"id\":" + std::to_string(id) + ",\"val\":\"" + val + "\"}";
}

std::string table_for(const std::filesystem::path& dir) {
    // CQL unquoted identifiers fold to lowercase; fold here so the fixture
    // and the sink name the same table.
    std::string name = "t_" + dir.filename().string();
    for (auto& ch : name) {
        if (ch >= 'A' && ch <= 'Z') {
            ch = static_cast<char>(ch - 'A' + 'a');
        } else if ((ch < 'a' || ch > 'z') && (ch < '0' || ch > '9')) {
            ch = '_';
        }
    }
    if (name.size() > 40) {
        name.resize(40);  // Cassandra caps table names at 48
    }
    return name;
}

constexpr const char* kContractKeyspace = "clink_contract_it";

struct CassandraUpsertContract {
    using Value = std::string;
    static constexpr std::string_view kCapabilityName = "cassandra_upsert";

    static bool available() { return cass_configured(); }

    static UpsertContractFixture<std::string> make(const std::filesystem::path& dir) {
        const auto table = table_for(dir);
        run_cql(std::string("CREATE KEYSPACE IF NOT EXISTS ") + kContractKeyspace +
                " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
        run_cql("DROP TABLE IF EXISTS " + std::string(kContractKeyspace) + "." + table);
        run_cql("CREATE TABLE " + std::string(kContractKeyspace) + "." + table +
                " (id int PRIMARY KEY, val text)");

        UpsertContractFixture<std::string> fx;
        fx.records = {row_json(1, "a"), row_json(2, "b"), row_json(3, "c")};
        fx.updated_records = {row_json(1, "a2"), row_json(2, "b2"), row_json(3, "c2")};
        fx.expected = {{"1", "a"}, {"2", "b"}, {"3", "c"}};
        fx.expected_updated = {{"1", "a2"}, {"2", "b2"}, {"3", "c2"}};
        fx.fresh = [table] {
            CassandraUpsertSink::Options o;
            o.conn.contact_points = cass_contact_points();
            o.keyspace = kContractKeyspace;
            o.table = table;
            o.key_columns = {"id"};
            return std::make_shared<CassandraUpsertSink>(o);
        };
        fx.state = [table] { return rows_of(kContractKeyspace, table); };
        return fx;
    }
};

}  // namespace

namespace clink::test {

INSTANTIATE_TYPED_TEST_SUITE_P(CassandraUpsert, UpsertContractSuite, CassandraUpsertContract);

}  // namespace clink::test
