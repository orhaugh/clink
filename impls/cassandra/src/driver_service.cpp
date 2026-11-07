// The Cassandra sinks' calls over the DataStax C/C++ driver (cassandra.h): the
// session, the schema metadata, the prepared insert and delete, and one future
// per write. Every driver object is owned by a handle that frees it, so no
// failure path leaks one.

#include <cassandra.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "clink/cassandra/cassandra_service.hpp"

namespace clink::cassandra {

namespace {

struct ClusterFree {
    void operator()(CassCluster* p) const noexcept { cass_cluster_free(p); }
};
struct SessionFree {
    void operator()(CassSession* p) const noexcept { cass_session_free(p); }
};
struct FutureFree {
    void operator()(CassFuture* p) const noexcept { cass_future_free(p); }
};
struct PreparedFree {
    void operator()(const CassPrepared* p) const noexcept { cass_prepared_free(p); }
};
struct StatementFree {
    void operator()(CassStatement* p) const noexcept { cass_statement_free(p); }
};
struct SchemaFree {
    void operator()(const CassSchemaMeta* p) const noexcept { cass_schema_meta_free(p); }
};

using ClusterPtr = std::unique_ptr<CassCluster, ClusterFree>;
using SessionPtr = std::unique_ptr<CassSession, SessionFree>;
using FuturePtr = std::unique_ptr<CassFuture, FutureFree>;
using PreparedPtr = std::unique_ptr<const CassPrepared, PreparedFree>;
using StatementPtr = std::unique_ptr<CassStatement, StatementFree>;
using SchemaPtr = std::unique_ptr<const CassSchemaMeta, SchemaFree>;

// How often a blocking wait looks at the cancel check.
constexpr std::uint64_t kWaitTickUs = 50'000;

std::string text_of(const char* s, std::size_t n) {
    return s == nullptr ? std::string{} : std::string(s, n);
}

CassandraFault fault_of(CassError e) {
    switch (e) {
        case CASS_OK:
            return CassandraFault::None;
        case CASS_ERROR_SERVER_UNAVAILABLE:
        case CASS_ERROR_SERVER_OVERLOADED:
        case CASS_ERROR_SERVER_IS_BOOTSTRAPPING:
        case CASS_ERROR_LIB_REQUEST_QUEUE_FULL:
        case CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD:
        case CASS_ERROR_LIB_UNABLE_TO_CONNECT:
        case CASS_ERROR_LIB_HOST_RESOLUTION:
        // Set only once every host in the query plan was tried without the
        // write reaching one (no connection), or refused it before applying it
        // (Unavailable, which the default retry policy takes to the next host,
        // or a bootstrapping node). A write that reached a host and then failed
        // returns that failure instead: the driver does not resend a statement
        // not marked idempotent, and the sinks mark none.
        case CASS_ERROR_LIB_NO_HOSTS_AVAILABLE:
            return CassandraFault::NotApplied;
        case CASS_ERROR_SERVER_SYNTAX_ERROR:
        case CASS_ERROR_SERVER_INVALID_QUERY:
        case CASS_ERROR_SERVER_UNAUTHORIZED:
        case CASS_ERROR_SERVER_BAD_CREDENTIALS:
        case CASS_ERROR_SERVER_CONFIG_ERROR:
        case CASS_ERROR_SERVER_ALREADY_EXISTS:
        case CASS_ERROR_SERVER_PROTOCOL_ERROR:
        case CASS_ERROR_SERVER_FUNCTION_FAILURE:
        case CASS_ERROR_LIB_BAD_PARAMS:
        case CASS_ERROR_LIB_MESSAGE_ENCODE:
        case CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS:
        case CASS_ERROR_LIB_INVALID_ITEM_COUNT:
        case CASS_ERROR_LIB_INVALID_VALUE_TYPE:
        case CASS_ERROR_LIB_NAME_DOES_NOT_EXIST:
        case CASS_ERROR_LIB_NULL_VALUE:
        case CASS_ERROR_LIB_PARAMETER_UNSET:
        case CASS_ERROR_LIB_INVALID_STATEMENT_TYPE:
            return CassandraFault::Permanent;
        default:
            // A write timeout or failure from the coordinator, the driver's own
            // request timeout, a write error on a connection, no host left to
            // try after one was tried: any of them may follow a write that some
            // replica applied.
            return CassandraFault::Ambiguous;
    }
}

CassandraOutcome outcome_of(CassFuture* f) {
    const CassError e = cass_future_error_code(f);
    CassandraOutcome o;
    o.fault = fault_of(e);
    if (e != CASS_OK) {
        o.error = cass_error_desc(e);
        const char* msg = nullptr;
        std::size_t len = 0;
        cass_future_error_message(f, &msg, &len);
        o.message = text_of(msg, len);
    }
    return o;
}

CassandraOutcome failed(CassandraFault fault, std::string error, std::string message = {}) {
    CassandraOutcome o;
    o.fault = fault;
    o.error = std::move(error);
    o.message = std::move(message);
    return o;
}

// Waits for `f` in ticks, asking `cancelled` between them. False when cancelled.
bool wait_for(CassFuture* f, const CassandraWriteService::CancelCheck& cancelled) {
    while (cass_future_wait_timed(f, kWaitTickUs) == cass_false) {
        if (cancelled && cancelled()) {
            return false;
        }
    }
    return true;
}

CassConsistency driver_consistency(CassandraConsistency c) {
    switch (c) {
        case CassandraConsistency::One:
            return CASS_CONSISTENCY_ONE;
        case CassandraConsistency::Two:
            return CASS_CONSISTENCY_TWO;
        case CassandraConsistency::Three:
            return CASS_CONSISTENCY_THREE;
        case CassandraConsistency::Quorum:
            return CASS_CONSISTENCY_QUORUM;
        case CassandraConsistency::All:
            return CASS_CONSISTENCY_ALL;
        case CassandraConsistency::LocalQuorum:
            return CASS_CONSISTENCY_LOCAL_QUORUM;
        case CassandraConsistency::EachQuorum:
            return CASS_CONSISTENCY_EACH_QUORUM;
        case CassandraConsistency::LocalOne:
            return CASS_CONSISTENCY_LOCAL_ONE;
    }
    return CASS_CONSISTENCY_LOCAL_ONE;
}

std::string type_name(const CassDataType* dt) {
    if (dt == nullptr) {
        return "custom";
    }
    switch (cass_data_type_type(dt)) {
        case CASS_VALUE_TYPE_ASCII:
            return "ascii";
        case CASS_VALUE_TYPE_BIGINT:
            return "bigint";
        case CASS_VALUE_TYPE_BLOB:
            return "blob";
        case CASS_VALUE_TYPE_BOOLEAN:
            return "boolean";
        case CASS_VALUE_TYPE_COUNTER:
            return "counter";
        case CASS_VALUE_TYPE_DECIMAL:
            return "decimal";
        case CASS_VALUE_TYPE_DOUBLE:
            return "double";
        case CASS_VALUE_TYPE_FLOAT:
            return "float";
        case CASS_VALUE_TYPE_INT:
            return "int";
        case CASS_VALUE_TYPE_TEXT:
            return "text";
        case CASS_VALUE_TYPE_TIMESTAMP:
            return "timestamp";
        case CASS_VALUE_TYPE_UUID:
            return "uuid";
        case CASS_VALUE_TYPE_VARCHAR:
            return "varchar";
        case CASS_VALUE_TYPE_VARINT:
            return "varint";
        case CASS_VALUE_TYPE_TIMEUUID:
            return "timeuuid";
        case CASS_VALUE_TYPE_INET:
            return "inet";
        case CASS_VALUE_TYPE_DATE:
            return "date";
        case CASS_VALUE_TYPE_TIME:
            return "time";
        case CASS_VALUE_TYPE_SMALL_INT:
            return "smallint";
        case CASS_VALUE_TYPE_TINY_INT:
            return "tinyint";
        case CASS_VALUE_TYPE_DURATION:
            return "duration";
        case CASS_VALUE_TYPE_LIST:
            return "list";
        case CASS_VALUE_TYPE_MAP:
            return "map";
        case CASS_VALUE_TYPE_SET:
            return "set";
        case CASS_VALUE_TYPE_UDT:
            return "udt";
        case CASS_VALUE_TYPE_TUPLE:
            return "tuple";
        default:
            return "custom";
    }
}

CassandraColumnMeta column_of(const CassColumnMeta* c) {
    CassandraColumnMeta out;
    const char* name = nullptr;
    std::size_t len = 0;
    cass_column_meta_name(c, &name, &len);
    out.name = text_of(name, len);
    out.type = type_name(cass_column_meta_data_type(c));
    return out;
}

class DriverService final : public CassandraWriteService {
public:
    DriverService() = default;
    ~DriverService() override { close(); }
    DriverService(const DriverService&) = delete;
    DriverService& operator=(const DriverService&) = delete;
    DriverService(DriverService&&) = delete;
    DriverService& operator=(DriverService&&) = delete;

    CassandraOutcome connect(const CassandraConnParams& conn,
                             const CancelCheck& cancelled) override {
        close();
        ClusterPtr cluster{cass_cluster_new()};
        if (const CassError e =
                cass_cluster_set_contact_points(cluster.get(), conn.contact_points.c_str());
            e != CASS_OK) {
            return failed(CassandraFault::Permanent, cass_error_desc(e), "contact_points");
        }
        if (const CassError e = cass_cluster_set_port(cluster.get(), conn.port); e != CASS_OK) {
            return failed(CassandraFault::Permanent, cass_error_desc(e), "port");
        }
        cass_cluster_set_connect_timeout(cluster.get(),
                                         static_cast<unsigned>(conn.connect_timeout_ms));
        cass_cluster_set_request_timeout(cluster.get(),
                                         static_cast<unsigned>(conn.request_timeout_ms));
        cass_cluster_set_consistency(cluster.get(), driver_consistency(conn.consistency));
        if (!conn.username.empty()) {
            cass_cluster_set_credentials(
                cluster.get(), conn.username.c_str(), conn.password.c_str());
        }
        // The driver's defaults, set so that they hold whatever the driver
        // version: schema metadata on (the sink reads the table's key), a
        // pending-request queue of 8192 (the sink's in-flight bound stays at or
        // below it, so a write never meets a full queue).
        cass_cluster_set_use_schema(cluster.get(), cass_true);
        cass_cluster_set_queue_size_io(cluster.get(), 8192);

        SessionPtr session{cass_session_new()};
        FuturePtr f{cass_session_connect(session.get(), cluster.get())};
        if (!wait_for(f.get(), cancelled)) {
            // The session's destructor waits for the connect attempt to end,
            // within the connect timeout.
            return failed(CassandraFault::Cancelled, "cancelled while connecting");
        }
        CassandraOutcome o = outcome_of(f.get());
        if (!o.ok()) {
            return o;
        }
        cluster_ = std::move(cluster);
        session_ = std::move(session);
        return {};
    }

    CassandraTableMeta describe_table(const std::string& keyspace,
                                      const std::string& table) override {
        CassandraTableMeta out;
        if (!session_) {
            out.error = failed(CassandraFault::Permanent, "not connected");
            return out;
        }
        SchemaPtr schema{cass_session_get_schema_meta(session_.get())};
        const CassKeyspaceMeta* ks =
            cass_schema_meta_keyspace_by_name_n(schema.get(), keyspace.data(), keyspace.size());
        if (ks == nullptr) {
            return out;
        }
        out.keyspace_found = true;
        const CassTableMeta* t = cass_keyspace_meta_table_by_name_n(ks, table.data(), table.size());
        if (t == nullptr) {
            return out;
        }
        out.table_found = true;
        for (std::size_t i = 0; i < cass_table_meta_partition_key_count(t); ++i) {
            out.partition_key.push_back(column_of(cass_table_meta_partition_key(t, i)));
        }
        for (std::size_t i = 0; i < cass_table_meta_clustering_key_count(t); ++i) {
            out.clustering_key.push_back(column_of(cass_table_meta_clustering_key(t, i)));
        }
        for (std::size_t i = 0; i < cass_table_meta_column_count(t); ++i) {
            out.columns.push_back(column_of(cass_table_meta_column(t, i)));
        }
        return out;
    }

    CassandraOutcome prepare(const std::string& insert_cql,
                             const std::string& delete_cql,
                             const CancelCheck& cancelled) override {
        if (!session_) {
            return failed(CassandraFault::Permanent, "not connected");
        }
        PreparedPtr insert;
        PreparedPtr del;
        for (const auto* cql : {&insert_cql, &delete_cql}) {
            if (cql->empty()) {
                continue;
            }
            FuturePtr f{cass_session_prepare_n(session_.get(), cql->data(), cql->size())};
            if (!wait_for(f.get(), cancelled)) {
                return failed(CassandraFault::Cancelled, "cancelled while preparing");
            }
            CassandraOutcome o = outcome_of(f.get());
            if (!o.ok()) {
                return o;
            }
            (cql == &insert_cql ? insert : del).reset(cass_future_get_prepared(f.get()));
        }
        insert_ = std::move(insert);
        delete_ = std::move(del);
        return {};
    }

    std::uint64_t submit(const CassandraWrite& write) override {
        const std::uint64_t ticket = next_ticket_++;
        Pending p;
        const CassPrepared* prepared =
            write.kind == CassandraWrite::Kind::Insert ? insert_.get() : delete_.get();
        if (!session_ || prepared == nullptr) {
            p.early = failed(CassandraFault::Permanent, "the statement is not prepared");
            pending_.emplace(ticket, std::move(p));
            return ticket;
        }
        StatementPtr st{cass_prepared_bind(prepared)};
        CassError be = CASS_OK;
        if (write.kind == CassandraWrite::Kind::Insert) {
            be = cass_statement_bind_string_n(st.get(), 0, write.json.data(), write.json.size());
        } else {
            for (std::size_t i = 0; i < write.key_json.size() && be == CASS_OK; ++i) {
                be = cass_statement_bind_string_n(
                    st.get(), i, write.key_json[i].data(), write.key_json[i].size());
            }
        }
        if (be == CASS_OK) {
            be = cass_statement_set_timestamp(st.get(), write.timestamp_us);
        }
        if (be != CASS_OK) {
            p.early = failed(CassandraFault::Permanent, cass_error_desc(be), "binding the write");
            pending_.emplace(ticket, std::move(p));
            return ticket;
        }
        // The future owns the request from here; the statement can go.
        p.future.reset(cass_session_execute(session_.get(), st.get()));
        pending_.emplace(ticket, std::move(p));
        return ticket;
    }

    std::optional<CassandraOutcome> poll(std::uint64_t ticket,
                                         std::chrono::microseconds wait) override {
        const auto it = pending_.find(ticket);
        if (it == pending_.end()) {
            return failed(CassandraFault::Permanent, "unknown write");
        }
        if (it->second.early) {
            return it->second.early;
        }
        const auto us = wait.count() < 0 ? 0 : static_cast<cass_duration_t>(wait.count());
        if (cass_future_wait_timed(it->second.future.get(), us) == cass_false) {
            return std::nullopt;
        }
        return outcome_of(it->second.future.get());
    }

    void release(std::uint64_t ticket) noexcept override { pending_.erase(ticket); }

    void close() noexcept override {
        pending_.clear();
        insert_.reset();
        delete_.reset();
        if (session_) {
            FuturePtr f{cass_session_close(session_.get())};
            cass_future_wait(f.get());
        }
        session_.reset();
        cluster_.reset();
    }

private:
    struct Pending {
        FuturePtr future;
        std::optional<CassandraOutcome> early;  // failed before it reached the driver
    };

    ClusterPtr cluster_;
    SessionPtr session_;
    PreparedPtr insert_;
    PreparedPtr delete_;
    std::map<std::uint64_t, Pending> pending_;
    std::uint64_t next_ticket_{1};
};

}  // namespace

std::shared_ptr<CassandraWriteService> make_cassandra_driver_service() {
    return std::make_shared<DriverService>();
}

}  // namespace clink::cassandra
