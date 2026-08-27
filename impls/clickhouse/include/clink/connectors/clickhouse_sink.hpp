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
// Each input record (std::string) is interpreted as one column value (default)
// or one row in TSV/JSONEachRow format depending on `Options::format`.
// Multi-column inserts will follow once we have a typed Row<...> seam.
//
// Backed by clickhouse-cpp when CMake finds it; throws on construction
// otherwise. Not final: a test observes the barrier-to-flush dispatch through
// a subclass, which needs no server.
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

    static bool is_real_implementation();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
