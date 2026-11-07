#pragma once

// The calls the Cassandra and ScyllaDB sinks make, behind an interface so that
// tests can substitute a model of the server. make_cassandra_driver_service() is
// the production implementation, over the DataStax C/C++ driver; its types stay
// in the .cpp.

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "clink/cassandra/connection_params.hpp"

namespace clink::cassandra {

// What a failed call means for the data, as far as the sink can tell.
enum class CassandraFault : std::uint8_t {
    None,
    // Refused before any replica wrote it: Unavailable, Overloaded, a
    // bootstrapping node, a full request queue in the driver.
    NotApplied,
    // May or may not have been applied: a write timeout or write failure from
    // the coordinator, the driver's own request timeout, a lost connection.
    Ambiguous,
    // The request itself is wrong and will fail again: an invalid query, a
    // missing table or column, refused credentials or permissions.
    Permanent,
    // The task was cancelled before the call finished.
    Cancelled,
};

struct CassandraOutcome {
    CassandraFault fault{CassandraFault::None};
    std::string error;    // the driver's name for the failure, e.g. "Write timeout"
    std::string message;  // the server's or driver's message, as received
    [[nodiscard]] bool ok() const noexcept { return fault == CassandraFault::None; }
};

// A column as the driver's schema metadata describes it. `type` is the CQL type
// name for a native type ("int", "text", "timeuuid", ...), and "list", "set",
// "map", "tuple", "udt" or "custom" for the rest.
struct CassandraColumnMeta {
    std::string name;
    std::string type;
};

struct CassandraTableMeta {
    CassandraOutcome error;  // not ok when the schema could not be read
    bool keyspace_found{false};
    bool table_found{false};
    std::vector<CassandraColumnMeta> partition_key;   // in key order
    std::vector<CassandraColumnMeta> clustering_key;  // in key order
    std::vector<CassandraColumnMeta> columns;         // every column
};

// One write a sink hands to the service. The timestamp is the write's CQL
// timestamp, in microseconds since the epoch, sent with the statement; the
// server orders writes to a row by it, never by arrival.
struct CassandraWrite {
    enum class Kind : std::uint8_t { Insert, Delete };
    Kind kind{Kind::Insert};
    std::string json;                   // Insert: the row as one JSON object
    std::vector<std::string> key_json;  // Delete: each key column's value as JSON text
    std::int64_t timestamp_us{0};
};

class CassandraWriteService {
public:
    // Asked while a blocking call waits; true ends the wait as Cancelled.
    using CancelCheck = std::function<bool()>;

    virtual ~CassandraWriteService() = default;

    // Connects to the cluster with `conn`'s settings, the consistency level and
    // request timeout included. Bounded by the connect timeout.
    virtual CassandraOutcome connect(const CassandraConnParams& conn,
                                     const CancelCheck& cancelled) = 0;
    // The table's columns and key, from the schema metadata read at connect.
    // `keyspace` and `table` are the folded names.
    virtual CassandraTableMeta describe_table(const std::string& keyspace,
                                              const std::string& table) = 0;
    // Prepares the insert statement (one bind marker: the row's JSON) and, when
    // `delete_cql` is not empty, the delete (one bind marker per key column, in
    // the key's order).
    virtual CassandraOutcome prepare(const std::string& insert_cql,
                                     const std::string& delete_cql,
                                     const CancelCheck& cancelled) = 0;
    // Hands one write to the driver and returns its ticket. Never waits for the
    // server. A write the driver cannot even build (a bind error) is still given
    // a ticket, which polls as a failed outcome.
    virtual std::uint64_t submit(const CassandraWrite& write) = 0;
    // Waits up to `wait` for the write's outcome; nullopt while it is unresolved.
    virtual std::optional<CassandraOutcome> poll(std::uint64_t ticket,
                                                 std::chrono::microseconds wait) = 0;
    // Forgets a ticket, resolved or not. An unresolved write still runs to its
    // end inside the driver, within the request timeout; nobody learns how.
    virtual void release(std::uint64_t ticket) noexcept = 0;
    // Releases every ticket and the prepared statements, and closes the session.
    // The driver lets writes still in flight finish first, so this returns
    // within about the request timeout.
    virtual void close() noexcept = 0;
};

[[nodiscard]] std::shared_ptr<CassandraWriteService> make_cassandra_driver_service();

}  // namespace clink::cassandra
