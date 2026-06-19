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
// otherwise.
class ClickHouseSink final : public Sink<std::string> {
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
    void flush() override;
    void close() override;

    std::string name() const override { return "clickhouse_sink"; }

    static bool is_real_implementation();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
