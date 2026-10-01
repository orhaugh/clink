#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "clink/operators/operator_base.hpp"

namespace clink {

// ClickHouseSink inserts rows into a ClickHouse table.
//
// Each input record (std::string) is one row, as a line of TSV or a JSON object
// (JSONEachRow) depending on `Options::format`. The buffered rows go to the
// server as one INSERT ... FORMAT statement per flush, which carries its rows
// inline. A flush happens when `batch_rows` rows are buffered, when a batch
// arrives at least `batch_interval` after the last flush, at every checkpoint
// barrier and at close.
//
// Every INSERT names its settings rather than inheriting them from the user's
// profile: it is synchronous (async_insert=0), so the flush returns only once
// the rows are in the table, and it carries a deduplication token of its own,
// so two separate batches with identical rows are never mistaken for a resend
// of each other.
//
// Backed by clickhouse-cpp when CMake finds it; throws on construction
// otherwise. Not final: the barrier-to-flush dispatch, the INSERT text and the
// metrics are observed in tests through a subclass, which needs no server.
class ClickHouseSink : public Sink<std::string> {
public:
    enum class Format : std::uint8_t {
        // Treat each record as one row in TSV (tab-separated values).
        TSV,
        // Treat each record as a JSON object (JSONEachRow).
        JSONEachRow,
    };

    struct Options {
        std::string host{"localhost"};
        std::uint16_t port{9000};
        std::string database{"default"};
        std::string table;
        std::string user{"default"};
        std::string password{};
        Format format{Format::TSV};
        std::size_t batch_rows{1000};
        std::chrono::milliseconds batch_interval{std::chrono::seconds{1}};
        // Socket timeouts for the client. The client's own default for send
        // and receive is none at all, so a server that stops answering would
        // hold a flush, and with it the checkpoint, for ever.
        std::chrono::milliseconds connect_timeout{5000};
        std::chrono::milliseconds send_timeout{30000};
        std::chrono::milliseconds receive_timeout{30000};
        // A `format` value the factory did not recognise. The sink writes TSV
        // in its place, as it always has, and names the value in a warning at
        // open. Empty when the value was recognised or `format` was set
        // directly.
        std::string unrecognised_format{};
    };

    explicit ClickHouseSink(Options opts);
    ~ClickHouseSink() override;

    ClickHouseSink(const ClickHouseSink&) = delete;
    ClickHouseSink& operator=(const ClickHouseSink&) = delete;
    ClickHouseSink(ClickHouseSink&&) = delete;
    ClickHouseSink& operator=(ClickHouseSink&&) = delete;

    void open() override;
    void on_data(const Batch<std::string>& batch) override;
    // A checkpoint barrier flushes whatever is buffered. The runner snapshots
    // and acks the checkpoint right after this returns, and a recovery resumes
    // the source past the records already consumed - so a row still sitting in
    // the buffer at the barrier would be lost by the next crash, and the
    // connector's declared at-least-once delivery would be false across a
    // restart. A flush that fails throws, which fails the checkpoint instead of
    // completing it over rows that never reached ClickHouse. Same shape as the
    // Postgres JSON sink.
    void on_barrier(CheckpointBarrier /*barrier*/) override { flush(); }
    void flush() override;
    void close() override;

    std::string name() const override { return "clickhouse_sink"; }

    [[nodiscard]] const Options& options() const noexcept;

    static bool is_real_implementation();

protected:
    // The two server round trips. open() calls connect() last, once the
    // options are checked; a flush hands send_insert() one complete INSERT
    // statement, its rows included, and counts the rows only once it returns.
    // Virtual so that a test can stand in for the server.
    virtual void connect();
    virtual void send_insert(const std::string& statement);

private:
    // Every flush trigger (row count, interval, barrier, close) goes through
    // here, so each one records the same latency, error and output metrics.
    void flush_with_metrics();
    void warn(const std::string& message) const;

    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
