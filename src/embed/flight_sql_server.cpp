// The Flight SQL endpoint over the embedded engine (see the header).

#include "clink/embed/flight_sql_server.hpp"

#include <utility>
#include <vector>

#include <arrow/array/builder_binary.h>
#include <arrow/flight/server.h>
#include <arrow/flight/sql/types.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>

namespace clink::embed {

namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;

arrow::Result<std::unique_ptr<flight::FlightInfo>> ClinkFlightSqlServer::GetFlightInfoStatement(
    const flight::ServerCallContext& /*context*/,
    const flightsql::StatementQuery& command,
    const flight::FlightDescriptor& descriptor) {
    std::lock_guard lock(m_);
    // Compile + submit the SELECT into a fresh collect table and claim its
    // reader now: the FlightInfo needs the result schema, and the ticket
    // hands the reader to DoGetStatement.
    ARROW_ASSIGN_OR_RAISE(const auto table, engine_.submit_select_to_collect(command.query));
    ARROW_ASSIGN_OR_RAISE(auto reader, engine_.collect_reader(table));
    const std::string handle = "stmt-" + std::to_string(seq_.fetch_add(1));
    pending_[handle] = reader;
    ARROW_ASSIGN_OR_RAISE(const auto ticket_str, flightsql::CreateStatementQueryTicket(handle));
    std::vector<flight::FlightEndpoint> endpoints;
    endpoints.push_back(flight::FlightEndpoint{flight::Ticket{ticket_str}, {}, {}, {}});
    ARROW_ASSIGN_OR_RAISE(
        auto info,
        flight::FlightInfo::Make(
            reader->schema(), descriptor, endpoints, /*total_records=*/-1, /*total_bytes=*/-1));
    return std::make_unique<flight::FlightInfo>(std::move(info));
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>> ClinkFlightSqlServer::DoGetStatement(
    const flight::ServerCallContext& /*context*/, const flightsql::StatementQueryTicket& command) {
    std::shared_ptr<arrow::RecordBatchReader> reader;
    {
        std::lock_guard lock(m_);
        auto it = pending_.find(command.statement_handle);
        if (it == pending_.end()) {
            return arrow::Status::KeyError("unknown statement handle '",
                                           command.statement_handle,
                                           "' (tickets are one-shot; call GetFlightInfo again)");
        }
        reader = std::move(it->second);
        pending_.erase(it);
    }
    return std::make_unique<flight::RecordBatchStream>(reader);
}

arrow::Result<std::int64_t> ClinkFlightSqlServer::DoPutCommandStatementUpdate(
    const flight::ServerCallContext& /*context*/, const flightsql::StatementUpdate& command) {
    std::lock_guard lock(m_);
    ARROW_RETURN_NOT_OK(engine_.execute_update(command.query));
    return -1;  // affected-record count unknown (streaming inserts)
}

arrow::Result<std::unique_ptr<flight::FlightInfo>> ClinkFlightSqlServer::GetFlightInfoTables(
    const flight::ServerCallContext& /*context*/,
    const flightsql::GetTables& command,
    const flight::FlightDescriptor& descriptor) {
    const auto& schema = command.include_schema
                             ? flightsql::SqlSchema::GetTablesSchemaWithIncludedSchema()
                             : flightsql::SqlSchema::GetTablesSchema();
    std::vector<flight::FlightEndpoint> endpoints;
    endpoints.push_back(flight::FlightEndpoint{flight::Ticket{descriptor.cmd}, {}, {}, {}});
    ARROW_ASSIGN_OR_RAISE(auto info,
                          flight::FlightInfo::Make(schema,
                                                   descriptor,
                                                   endpoints,
                                                   /*total_records=*/-1,
                                                   /*total_bytes=*/-1));
    return std::make_unique<flight::FlightInfo>(std::move(info));
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>> ClinkFlightSqlServer::DoGetTables(
    const flight::ServerCallContext& /*context*/, const flightsql::GetTables& command) {
    std::vector<std::string> names;
    {
        std::lock_guard lock(m_);
        for (const auto& name : engine_.catalog().list_tables()) {
            names.push_back(name);
        }
    }

    arrow::StringBuilder catalog_b;
    arrow::StringBuilder db_schema_b;
    arrow::StringBuilder name_b;
    arrow::StringBuilder type_b;
    arrow::BinaryBuilder schema_b;
    for (const auto& name : names) {
        ARROW_RETURN_NOT_OK(catalog_b.AppendNull());
        ARROW_RETURN_NOT_OK(db_schema_b.AppendNull());
        ARROW_RETURN_NOT_OK(name_b.Append(name));
        ARROW_RETURN_NOT_OK(type_b.Append("TABLE"));
        if (command.include_schema) {
            // The declared columns as a serialised Arrow schema (the
            // catalog stores Arrow types verbatim), so clients browse real
            // column metadata.
            std::shared_ptr<arrow::Schema> table_schema;
            {
                std::lock_guard lock(m_);
                const auto* def = engine_.catalog().get_table(name);
                if (def != nullptr) {
                    arrow::FieldVector fields;
                    fields.reserve(def->columns.size());
                    for (const auto& c : def->columns) {
                        fields.push_back(arrow::field(c.name, c.type));
                    }
                    table_schema = arrow::schema(std::move(fields));
                }
            }
            if (!table_schema) {
                table_schema = arrow::schema({});
            }
            ARROW_ASSIGN_OR_RAISE(auto buf, arrow::ipc::SerializeSchema(*table_schema));
            ARROW_RETURN_NOT_OK(
                schema_b.Append(buf->data(), static_cast<std::int32_t>(buf->size())));
        }
    }

    arrow::ArrayVector columns;
    ARROW_ASSIGN_OR_RAISE(auto catalog_a, catalog_b.Finish());
    ARROW_ASSIGN_OR_RAISE(auto db_schema_a, db_schema_b.Finish());
    ARROW_ASSIGN_OR_RAISE(auto name_a, name_b.Finish());
    ARROW_ASSIGN_OR_RAISE(auto type_a, type_b.Finish());
    columns = {catalog_a, db_schema_a, name_a, type_a};
    auto schema = flightsql::SqlSchema::GetTablesSchema();
    if (command.include_schema) {
        ARROW_ASSIGN_OR_RAISE(auto schema_a, schema_b.Finish());
        columns.push_back(schema_a);
        schema = flightsql::SqlSchema::GetTablesSchemaWithIncludedSchema();
    }
    auto batch = arrow::RecordBatch::Make(schema, static_cast<std::int64_t>(names.size()), columns);
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::RecordBatchReader::Make({std::move(batch)}, schema));
    return std::make_unique<flight::RecordBatchStream>(reader);
}

}  // namespace clink::embed
