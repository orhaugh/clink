#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "clink/connectors/clickhouse_row.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

// ClickHouseSource emits rows from a SELECT against a ClickHouse server.
//
// MVP behaviour: snapshot mode. open() opens a connection and starts the
// SELECT; produce() drains blocks from clickhouse-cpp's Select callback
// into Batch<ClickHouseRow> deliveries; once the server has shipped its
// final block, produce() returns false and the source closes.
//
// The same shape as PostgresSource: cell values are stringified to keep
// the source schema-agnostic. Downstream operators address columns by
// index (`row[0]`) or by name (`row.at("event_time")`) and parse to
// their actual types via a MapOperator.
//
// Implementation lives in src/clickhouse_source.cpp behind
// `CLINK_HAS_CLICKHOUSE`. When CMake doesn't find clickhouse-cpp,
// construction throws - same shape as the other impls.
class ClickHouseSource final : public Source<ClickHouseRow> {
public:
    struct Options {
        std::string host{"localhost"};
        std::uint16_t port{9000};
        std::string database{"default"};
        std::string user{"default"};
        std::string password;

        // SELECT statement to execute. Single statement, no trailing
        // semicolon. Result rows arrive in arbitrary order unless the
        // query has an ORDER BY clause.
        std::string query;

        // Rows emitted per produce() call. Smaller batches give more
        // responsive backpressure; larger batches reduce per-batch
        // overhead. A single clickhouse-cpp block may straddle multiple
        // produce() calls.
        std::size_t batch_size{1024};
    };

    explicit ClickHouseSource(Options opts);
    ~ClickHouseSource() override;

    ClickHouseSource(const ClickHouseSource&) = delete;
    ClickHouseSource& operator=(const ClickHouseSource&) = delete;
    ClickHouseSource(ClickHouseSource&&) = delete;
    ClickHouseSource& operator=(ClickHouseSource&&) = delete;

    void open() override;
    bool produce(Emitter<ClickHouseRow>& out) override;
    void close() override;

    // A SELECT materialises a finite result set (BATCH-1).
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    // #60: source replay. The cursor is the row index into the snapshot the
    // SELECT materialises in open(); persisting it lets a restart resume mid
    // result-set rather than from the top. Exactly-once at the source boundary
    // holds only for a deterministically-ordered query (ORDER BY) over data
    // unchanged between runs - row index N is "the same row" only then. open()
    // clamps a restored cursor to the re-materialised row count.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt) override;
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;

    std::string name() const override { return "clickhouse_source"; }

    static bool is_real_implementation();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
