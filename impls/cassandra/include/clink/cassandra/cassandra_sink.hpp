#pragma once

#include <memory>
#include <string>

#include "clink/cassandra/connection_params.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::cassandra {

// Cassandra / ScyllaDB sink. Each input record is a JSON object string (e.g. SQL
// row_to_json_string); the sink writes it with a prepared `INSERT INTO <keyspace>.<table> JSON ?`
// statement, so Cassandra maps the JSON object's fields to table columns by name and coerces
// types. Delivery is AT-LEAST-ONCE: inserts are executed asynchronously for throughput, and
// flush()/on_barrier() waits for every pending insert and THROWS on any failure so the job
// replays from the last checkpoint. A CQL INSERT is an upsert keyed by the primary key, so
// re-delivery on replay overwrites rather than duplicates - effectively-once for a stable PK.
//
// This is a SINK only: Cassandra is a serving/OLAP store, not a streaming log, so there is no
// natural unbounded source. driver types do not appear here (cassandra.h is confined to .cpp).
class CassandraSink final : public Sink<std::string> {
public:
    struct Options {
        CassandraConnParams conn;
        std::string keyspace;  // required
        std::string table;     // required
        std::string name{"cassandra_sink"};
    };

    explicit CassandraSink(Options opts);
    ~CassandraSink() override;

    CassandraSink(const CassandraSink&) = delete;
    CassandraSink& operator=(const CassandraSink&) = delete;

    void open() override;
    void on_data(const Batch<std::string>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    void flush() override;
    void close() override;
    [[nodiscard]] std::string name() const override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::cassandra
