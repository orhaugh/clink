#pragma once

#include <memory>
#include <string>

#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"

#include "native/insert_transport.hpp"
#include "native/sink_options.hpp"

namespace clink::clickhouse::native {

class NativeSink final : public Sink<sql::Row> {
public:
    NativeSink(SinkOptions options, TransportFactory factory);
    ~NativeSink() override;
    NativeSink(const NativeSink&) = delete;
    NativeSink& operator=(const NativeSink&) = delete;

    void open() override;
    void on_data(const Batch<sql::Row>& batch) override;
    void on_data(Batch<sql::Row>&& batch) override;
    void on_barrier(CheckpointBarrier barrier) override;
    void flush() override;
    void close() override;
    void close_cancelled() override;
    // supports_columnar() keeps the default false: a columnar batch is read
    // through its row accessors.
    [[nodiscard]] std::string name() const override { return "clickhouse_native_sink"; }

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::clickhouse::native
