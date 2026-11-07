#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "clink/cassandra/cassandra_service.hpp"
#include "clink/cassandra/connection_params.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::cassandra {

// Cassandra / ScyllaDB sink. Each input record is a JSON object string (e.g. SQL
// row_to_json_string); the sink writes it, as it is, with a prepared
// `INSERT INTO "<keyspace>"."<table>" JSON ?` statement, so the server maps the
// object's fields to columns by name and coerces the types. Columns the record
// leaves out are written as null.
//
// Writes are asynchronous: at most max_in_flight of them, holding at most
// max_in_flight_bytes of record text, are outstanding at once (a record larger
// than that goes alone), and a record that would exceed either waits for the
// oldest write to be acknowledged. Every write carries a timestamp from the
// sink's own clock, strictly increasing within the instance, so the server keeps
// the later of two writes to a row whatever order they arrive in. A barrier
// returns once every write before it is acknowledged at the configured
// consistency level. A write that is not (refused, timed out, its connection
// lost, the task cancelled) fails the sink: it gives up the writes in flight and
// refuses every later record, flush and barrier until open() starts afresh, so
// a checkpoint never covers a write whose outcome is unknown.
//
// Delivery is at-least-once: a restart replays the records after the last
// completed checkpoint, and each replayed insert overwrites its row. The
// ordering and final-state contract is in docs/connectors/cassandra.md.
class CassandraSink final : public Sink<std::string> {
public:
    struct Options {
        CassandraConnParams conn;
        std::string keyspace;  // required: a CQL unquoted identifier
        std::string table;     // required: a CQL unquoted identifier
        // Bounds on the writes outstanding at once: their number (1 to 8192) and
        // their records' bytes (1024 to 1 GiB). Not a bound on the process's
        // memory: the driver, the batch being written and the runtime use more.
        std::size_t max_in_flight{1024};
        std::size_t max_in_flight_bytes{64U << 20U};
        std::string name{"cassandra_sink"};
        // The calls the sink makes; empty means the DataStax driver.
        std::shared_ptr<CassandraWriteService> service;
    };

    // Checks the options; touches no server.
    explicit CassandraSink(Options opts);
    ~CassandraSink() override;

    CassandraSink(const CassandraSink&) = delete;
    CassandraSink& operator=(const CassandraSink&) = delete;
    CassandraSink(CassandraSink&&) = delete;
    CassandraSink& operator=(CassandraSink&&) = delete;

    // Connects, checks that the table exists and prepares the insert.
    void open() override;
    void on_data(const Batch<std::string>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    // Its barrier hook writes out everything it received before the barrier,
    // which is its guarantee only while it owns its chain's checkpoint, so
    // Dag::add_sink refuses another sink beside it.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    // Waits for every write in flight to be acknowledged.
    void flush() override;
    // Writes nothing: a clean end of input has flushed already, and a failed or
    // cancelled task must not write. Writes still in flight are not awaited.
    void close() override;
    [[nodiscard]] std::string name() const override;

    // True once a write that was not acknowledged, or a cancellation, stopped
    // this instance.
    [[nodiscard]] bool failed() const;
    // Writes handed to the server and not yet acknowledged.
    [[nodiscard]] std::size_t in_flight() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::cassandra
