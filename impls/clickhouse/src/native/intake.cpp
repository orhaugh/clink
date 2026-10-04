#include "native/intake.hpp"

#include <string>
#include <unordered_map>

#include "clink/sql/row_columnar_batcher.hpp"

namespace clink::clickhouse::native {

namespace {

// The value types rows_from_record_batch reads with read_cell; any other
// makes it refuse the whole batch.
bool carried(const arrow::DataType& type) {
    switch (type.id()) {
        case arrow::Type::INT64:
        case arrow::Type::INT32:
        case arrow::Type::DOUBLE:
        case arrow::Type::FLOAT:
        case arrow::Type::BOOL:
        case arrow::Type::DECIMAL128:
        case arrow::Type::STRING:
            return true;
        case arrow::Type::LIST:
            return clink::sql::row_columnar_detail::is_list_float32(type);
        default:
            return false;
    }
}

}  // namespace

const char* to_string(IntakeDecline reason) noexcept {
    switch (reason) {
        case IntakeDecline::EventTime:
            return "event_time";
        case IntakeDecline::UnsupportedType:
            return "unsupported_type";
        case IntakeDecline::DuplicateName:
            return "duplicate_name";
    }
    return "unknown";
}

IntakeResult compile_intake(const arrow::Schema& schema, const std::vector<SqlColumn>& columns) {
    if (schema.num_fields() < 1 || schema.field(0)->type()->id() != arrow::Type::INT64) {
        return IntakeDecline::EventTime;
    }
    // Value column name -> its index, or -2 once a second column carries it.
    std::unordered_map<std::string, int> by_name;
    for (int i = 1; i < schema.num_fields(); ++i) {
        const auto& f = schema.field(i);
        // The engine's partition column is watermark metadata; the reader
        // drops it before it looks at the type.
        if (f->name() == clink::sql::kSourcePartitionColumn) {
            continue;
        }
        if (!carried(*f->type())) {
            return IntakeDecline::UnsupportedType;
        }
        const auto [it, fresh] = by_name.emplace(f->name(), i);
        if (!fresh) {
            it->second = -2;
        }
    }
    IntakePlan plan;
    plan.source.reserve(columns.size());
    for (const auto& c : columns) {
        const auto it = by_name.find(c.name);
        if (it == by_name.end()) {
            plan.source.push_back(-1);
        } else if (it->second < 0) {
            return IntakeDecline::DuplicateName;
        } else {
            plan.source.push_back(it->second);
        }
    }
    return plan;
}

}  // namespace clink::clickhouse::native
