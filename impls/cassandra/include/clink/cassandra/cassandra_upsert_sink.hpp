#pragma once

// CassandraUpsertSink - changelog-aware Cassandra / ScyllaDB sink for mode='upsert'.
//
// Consumes the clink changelog (a "__row_kind" field on each JSON row) and
// maintains a table by PRIMARY KEY:
//   insert / update_after  -> INSERT INTO <ks>.<t> JSON ?   (a CQL INSERT is an upsert by PK)
//   delete / update_before -> DELETE FROM <ks>.<t> WHERE <pk1>=<lit> AND ...
//   (a row with no __row_kind is an implicit insert)
//
// Within a flush the changelog is netted by primary key (last op wins), then the
// surviving upserts + deletes are executed asynchronously and awaited. EFFECTIVELY-
// ONCE on the sink table for a stable primary key and a deterministic defining
// query: a CQL INSERT and a keyed DELETE are both idempotent, so a replay
// converges the table to the same final state. NOT two-phase commit.
//
// Lets a retracting SQL query (GROUP BY, TOP-N, outer join) maintain a Cassandra
// table; the append-only cassandra_sink has no delete/tombstone handling.
//
// CQL has no multi-row IN over a composite key, so deletes are per-row
// (DELETE ... WHERE k1=? AND k2=?). driver types are confined to the .cpp.

#include <memory>
#include <string>
#include <vector>

#include "clink/cassandra/connection_params.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::cassandra {

class CassandraUpsertSink final : public Sink<std::string> {
public:
    struct Options {
        CassandraConnParams conn;
        std::string keyspace;                  // required
        std::string table;                     // required
        std::vector<std::string> key_columns;  // PRIMARY KEY - the DELETE key (required)
        std::string name{"cassandra_upsert_sink"};
    };

    explicit CassandraUpsertSink(Options opts);
    ~CassandraUpsertSink() override;

    CassandraUpsertSink(const CassandraUpsertSink&) = delete;
    CassandraUpsertSink& operator=(const CassandraUpsertSink&) = delete;

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
