#pragma once

// CassandraUpsertSink - changelog-aware Cassandra / ScyllaDB sink for mode='upsert'.
//
// Consumes the clink changelog (a "__row_kind" field on each JSON row) and
// maintains a table by its PRIMARY KEY:
//   insert / update_after  -> INSERT INTO "<ks>"."<t>" JSON ?   (a CQL INSERT is an upsert)
//   delete / update_before -> DELETE FROM "<ks>"."<t>" WHERE "<k1>" = fromJson(?) AND ...
//   (a row with no __row_kind is an insert; any other __row_kind is refused)
//
// When it opens, the sink reads the table's schema and refuses to run unless
// key_columns names exactly the table's partition key and clustering columns,
// each of a type it accepts as a key (cassandra_rules.hpp). Every record is
// checked before it is buffered: a missing, null or ill-typed key column, a
// field named twice, or an unknown __row_kind fails the sink.
//
// Buffered rows are netted by key (the latest record for a row wins) up to
// max_buffered_records rows or max_buffered_bytes; reaching either writes the
// buffer out early, as a barrier does. Rows are written in the order of the
// records that produced them, each with a timestamp from the sink's clock,
// strictly increasing within the instance, so an older record's write never wins
// over a newer one's for the same row. The writes in flight are bounded as in
// CassandraSink. A barrier returns once every row before it is acknowledged; a
// write that is not fails the sink, which then refuses every later barrier.
//
// Not two-phase commit. Final-state convergence under the assumptions in
// docs/connectors/cassandra.md: a replay converges the table for a stable
// primary key and a deterministic defining query, with each key written by one
// sink subtask and no other writer.

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "clink/cassandra/cassandra_service.hpp"
#include "clink/cassandra/connection_params.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::cassandra {

class CassandraUpsertSink final : public Sink<std::string> {
public:
    struct Options {
        CassandraConnParams conn;
        std::string keyspace;                  // required
        std::string table;                     // required
        std::vector<std::string> key_columns;  // required: the table's whole primary key
        // Rows buffered (netted by key) before an early write-out: their number
        // (1 to 1,000,000) and their bytes (1024 to 1 GiB). A record larger than
        // max_buffered_bytes is buffered alone and written at once.
        std::size_t max_buffered_records{10000};
        std::size_t max_buffered_bytes{64U << 20U};
        // As CassandraSink::Options.
        std::size_t max_in_flight{1024};
        std::size_t max_in_flight_bytes{64U << 20U};
        std::string name{"cassandra_upsert_sink"};
        std::shared_ptr<CassandraWriteService> service;
    };

    // Checks the options; touches no server.
    explicit CassandraUpsertSink(Options opts);
    ~CassandraUpsertSink() override;

    CassandraUpsertSink(const CassandraUpsertSink&) = delete;
    CassandraUpsertSink& operator=(const CassandraUpsertSink&) = delete;
    CassandraUpsertSink(CassandraUpsertSink&&) = delete;
    CassandraUpsertSink& operator=(CassandraUpsertSink&&) = delete;

    // Connects, checks key_columns against the table's primary key and prepares
    // the insert and the delete.
    void open() override;
    // Checks every record of the batch before buffering any: a batch with a
    // record the sink refuses adds nothing, and the sink fails.
    void on_data(const Batch<std::string>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    // Its barrier hook writes out everything it received before the barrier,
    // which is its guarantee only while it owns its chain's checkpoint, so
    // Dag::add_sink refuses another sink beside it.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    // Writes the buffer out and waits for every write to be acknowledged.
    void flush() override;
    // Writes nothing; drops the buffer and does not await writes in flight.
    void close() override;
    [[nodiscard]] std::string name() const override;

    [[nodiscard]] bool failed() const;
    // Distinct rows buffered and not yet handed to the server.
    [[nodiscard]] std::size_t buffered_rows() const;
    [[nodiscard]] std::size_t in_flight() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::cassandra
