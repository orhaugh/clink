#pragma once

// ClinkFlightSqlServer - an Arrow Flight SQL endpoint over the embedded
// engine, so any Flight SQL / ADBC / JDBC client (DBeaver, pandas via
// adbc_driver_flightsql, dbt adapters) can talk to clink over one wire
// protocol:
//
//   clink::embed::EmbeddedEngine engine{opts};
//   clink::embed::ClinkFlightSqlServer server{engine};
//   arrow::flight::FlightServerOptions fopts{
//       arrow::flight::Location::ForGrpcTcp("127.0.0.1", 32010).ValueOrDie()};
//   server.Init(fopts);
//   server.Serve();
//
// Statement QUERIES compile a bare SELECT into a synthesised collect
// table and stream its typed Arrow batches back (changelog-aware: a
// retracting plan's stream carries a leading row_kind column).
// Statement UPDATES run DDL / INSERT scripts synchronously (submitted
// jobs complete before the call returns - streaming pipelines belong in
// queries). GetTables serves the engine catalog, with real per-table
// Arrow schemas when the client asks for them.
//
// v1 scope: no authentication (bind loopback or front with a proxy),
// no prepared statements, no transactions - each call stands alone.

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <arrow/flight/sql/server.h>

#include "clink/embed/embedded_engine.hpp"

namespace clink::embed {

class ClinkFlightSqlServer : public arrow::flight::sql::FlightSqlServerBase {
public:
    explicit ClinkFlightSqlServer(EmbeddedEngine& engine) : engine_(engine) {}

    arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoStatement(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::sql::StatementQuery& command,
        const arrow::flight::FlightDescriptor& descriptor) override;

    arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetStatement(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::sql::StatementQueryTicket& command) override;

    arrow::Result<std::int64_t> DoPutCommandStatementUpdate(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::sql::StatementUpdate& command) override;

    arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoTables(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::sql::GetTables& command,
        const arrow::flight::FlightDescriptor& descriptor) override;

    arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetTables(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::sql::GetTables& command) override;

private:
    EmbeddedEngine& engine_;
    // Serialises engine access (gRPC handler threads) and guards pending_.
    std::mutex m_;
    // Statement handle -> the claimed collect reader, staged by
    // GetFlightInfoStatement and consumed by DoGetStatement.
    std::unordered_map<std::string, std::shared_ptr<arrow::RecordBatchReader>> pending_;
    std::atomic<std::uint64_t> seq_{0};
};

}  // namespace clink::embed
