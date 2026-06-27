#include "clink/connectors/clickhouse_source.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <utility>

#include "clink/metrics/connector_metrics.hpp"

#ifdef CLINK_HAS_CLICKHOUSE
#include <clickhouse/client.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/types/types.h>
#endif

namespace clink {

#ifdef CLINK_HAS_CLICKHOUSE

namespace {

// Stringify one cell of a clickhouse-cpp Column. Handles the common
// types directly and falls back to "<unsupported:<type-name>>" so the
// row still has the right arity for downstream consumers.
//
// Strategy mirrors PostgresSource: emit canonical text (ints in decimal,
// strings verbatim, Date/DateTime as the underlying integer). Downstream
// MapOperators parse to typed values as needed.
std::string stringify_cell(const clickhouse::ColumnRef& col, std::size_t row) {
    using clickhouse::Type;

    // Nullable<T> is a wrapper: detect first, peel the null bit, recurse
    // into the nested column for the value.
    if (col->Type()->GetCode() == Type::Nullable) {
        const auto nullable = col->As<clickhouse::ColumnNullable>();
        if (nullable->IsNull(row)) {
            return {};
        }
        return stringify_cell(nullable->Nested(), row);
    }

    switch (col->Type()->GetCode()) {
        case Type::Int8:
            return std::to_string(col->As<clickhouse::ColumnInt8>()->At(row));
        case Type::Int16:
            return std::to_string(col->As<clickhouse::ColumnInt16>()->At(row));
        case Type::Int32:
            return std::to_string(col->As<clickhouse::ColumnInt32>()->At(row));
        case Type::Int64:
            return std::to_string(col->As<clickhouse::ColumnInt64>()->At(row));
        case Type::UInt8:
            return std::to_string(col->As<clickhouse::ColumnUInt8>()->At(row));
        case Type::UInt16:
            return std::to_string(col->As<clickhouse::ColumnUInt16>()->At(row));
        case Type::UInt32:
            return std::to_string(col->As<clickhouse::ColumnUInt32>()->At(row));
        case Type::UInt64:
            return std::to_string(col->As<clickhouse::ColumnUInt64>()->At(row));
        case Type::Float32:
            return std::to_string(col->As<clickhouse::ColumnFloat32>()->At(row));
        case Type::Float64:
            return std::to_string(col->As<clickhouse::ColumnFloat64>()->At(row));
        case Type::String: {
            const auto sv = col->As<clickhouse::ColumnString>()->At(row);
            return std::string{sv};
        }
        case Type::FixedString: {
            const auto sv = col->As<clickhouse::ColumnFixedString>()->At(row);
            return std::string{sv};
        }
        case Type::Date:
            return std::to_string(col->As<clickhouse::ColumnDate>()->At(row));
        case Type::DateTime:
            return std::to_string(col->As<clickhouse::ColumnDateTime>()->At(row));
        case Type::DateTime64:
            return std::to_string(col->As<clickhouse::ColumnDateTime64>()->At(row));
        default:
            return "<unsupported:" + col->Type()->GetName() + ">";
    }
}

// Whether a cell is SQL NULL (only a Nullable column can be). Companion to
// stringify_cell, which returns "" for a NULL and so cannot itself report it.
bool cell_is_null(const clickhouse::ColumnRef& col, std::size_t row) {
    if (col->Type()->GetCode() == clickhouse::Type::Nullable) {
        return col->As<clickhouse::ColumnNullable>()->IsNull(row);
    }
    return false;
}

}  // namespace

struct ClickHouseSource::Impl {
    Options opts;
    std::unique_ptr<clickhouse::Client> client;

    // Snapshot mode: open() runs the SELECT to completion, accumulating
    // every row into a flat vector. produce() then slices `batch_size`
    // rows per call until exhausted. Same shape as PostgresSource;
    // streaming mode (chunked Select callbacks producing partial batches
    // with backpressure) is a v2.
    std::vector<ClickHouseRow> rows;
    std::size_t next_row{0};

    std::shared_ptr<const std::vector<std::string>> column_names;
    std::shared_ptr<const std::vector<std::string>> column_types;
};

bool ClickHouseSource::is_real_implementation() {
    return true;
}

ClickHouseSource::ClickHouseSource(Options opts) : impl_(std::make_unique<Impl>()) {
    impl_->opts = std::move(opts);
}

ClickHouseSource::~ClickHouseSource() = default;

void ClickHouseSource::open() {
    if (impl_->opts.query.empty()) {
        throw std::runtime_error("ClickHouseSource::open: 'query' is required");
    }

    clickhouse::ClientOptions co;
    co.SetHost(impl_->opts.host)
        .SetPort(impl_->opts.port)
        .SetDefaultDatabase(impl_->opts.database)
        .SetUser(impl_->opts.user)
        .SetPassword(impl_->opts.password);
    impl_->client = std::make_unique<clickhouse::Client>(co);

    // Schema captured from the first non-empty block. Every subsequent
    // block must match - clickhouse-cpp guarantees this for a single
    // SELECT, so we don't re-verify.
    auto names = std::make_shared<std::vector<std::string>>();
    auto types = std::make_shared<std::vector<std::string>>();
    bool schema_set = false;

    impl_->client->Select(impl_->opts.query, [&](const clickhouse::Block& block) {
        const std::size_t ncols = block.GetColumnCount();
        const std::size_t nrows = block.GetRowCount();
        if (nrows == 0) {
            return;
        }

        if (!schema_set) {
            names->reserve(ncols);
            types->reserve(ncols);
            for (std::size_t c = 0; c < ncols; ++c) {
                names->emplace_back(block.GetColumnName(c));
                types->emplace_back(block[c]->Type()->GetName());
            }
            impl_->column_names = names;
            impl_->column_types = types;
            schema_set = true;
        }

        for (std::size_t r = 0; r < nrows; ++r) {
            std::vector<std::string> values;
            std::vector<char> nulls;
            values.reserve(ncols);
            nulls.reserve(ncols);
            for (std::size_t c = 0; c < ncols; ++c) {
                values.emplace_back(stringify_cell(block[c], r));
                nulls.push_back(cell_is_null(block[c], r) ? 1 : 0);
            }
            impl_->rows.emplace_back(ClickHouseRow{
                impl_->column_names, impl_->column_types, std::move(values), std::move(nulls)});
        }
    });

    // #60: a cursor restored by restore_offset() (which runs before open())
    // may exceed this run's row count if the query result shrank between runs.
    // Clamp so produce() simply finds nothing left to emit rather than reading
    // out of bounds.
    if (impl_->next_row > impl_->rows.size()) {
        impl_->next_row = impl_->rows.size();
    }
}

bool ClickHouseSource::produce(Emitter<ClickHouseRow>& out) {
    if (this->cancelled() || !impl_->client) {
        return false;
    }
    if (impl_->next_row >= impl_->rows.size()) {
        return false;
    }

    const std::size_t end = std::min(impl_->next_row + impl_->opts.batch_size, impl_->rows.size());

    Batch<ClickHouseRow> batch;
    for (std::size_t r = impl_->next_row; r < end; ++r) {
        batch.emplace(std::move(impl_->rows[r]));
    }
    const auto batch_size = batch.size();
    out.emit_data(std::move(batch));
    clink::metrics::connector::records_in_inc("clickhouse", batch_size);

    impl_->next_row = end;
    return impl_->next_row < impl_->rows.size();
}

void ClickHouseSource::close() {
    if (impl_) {
        impl_->client.reset();
        impl_->rows.clear();
        impl_->next_row = 0;
    }
}

namespace {
constexpr const char* kClickHouseOffsetKey = "__clickhouse_source_row__";
}  // namespace

void ClickHouseSource::snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId) {
    const auto cursor = static_cast<std::uint64_t>(impl_->next_row);
    std::array<std::byte, 8> bytes{};
    for (int i = 0; i < 8; ++i) {
        bytes[static_cast<std::size_t>(i)] = static_cast<std::byte>((cursor >> (i * 8)) & 0xFF);
    }
    backend.put_operator_state(
        op_id,
        StateBackend::KeyView{kClickHouseOffsetKey, std::strlen(kClickHouseOffsetKey)},
        StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
}

bool ClickHouseSource::restore_offset(StateBackend& backend, OperatorId op_id) {
    auto v = backend.get_operator_state(
        op_id, StateBackend::KeyView{kClickHouseOffsetKey, std::strlen(kClickHouseOffsetKey)});
    if (!v.has_value() || v->size() < 8) {
        return false;
    }
    std::uint64_t restored = 0;
    for (int i = 0; i < 8; ++i) {
        restored |= static_cast<std::uint64_t>(static_cast<std::uint8_t>((*v)[i])) << (i * 8);
    }
    impl_->next_row = static_cast<std::size_t>(restored);
    return true;
}

#else  // !CLINK_HAS_CLICKHOUSE

struct ClickHouseSource::Impl {};

bool ClickHouseSource::is_real_implementation() {
    return false;
}

ClickHouseSource::ClickHouseSource(Options /*opts*/) {
    throw std::runtime_error(
        "ClickHouseSource: built without clickhouse-cpp. Install it and "
        "reconfigure cmake - find_package(clickhouse-cpp) must succeed.");
}

ClickHouseSource::~ClickHouseSource() = default;
void ClickHouseSource::open() {}
bool ClickHouseSource::produce(Emitter<ClickHouseRow>& /*out*/) {
    return false;
}
void ClickHouseSource::close() {}
void ClickHouseSource::snapshot_offset(StateBackend&, OperatorId, CheckpointId) {}
bool ClickHouseSource::restore_offset(StateBackend&, OperatorId) {
    return false;
}

#endif

}  // namespace clink
