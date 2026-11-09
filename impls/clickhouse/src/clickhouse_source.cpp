#include "clink/connectors/clickhouse_source.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"

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

namespace {

// FNV-1a over a row's cells, each as its null flag, its length and its text,
// so rows that differ anywhere hash apart.
constexpr std::uint64_t kFnvOffset = 14695981039346656037ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

std::uint64_t mix(std::uint64_t h, const char* data, std::size_t n) {
    for (std::size_t i = 0; i < n; ++i) {
        h ^= static_cast<std::uint8_t>(data[i]);
        h *= kFnvPrime;
    }
    return h;
}

std::uint64_t mix_row(std::uint64_t h, const ClickHouseRow& row) {
    const auto& values = row.values();
    for (std::size_t c = 0; c < values.size(); ++c) {
        const char null = row.is_null(c) ? 1 : 0;
        h = mix(h, &null, 1);
        const auto len = static_cast<std::uint64_t>(values[c].size());
        std::array<char, 8> le{};
        for (int i = 0; i < 8; ++i) {
            le[static_cast<std::size_t>(i)] = static_cast<char>((len >> (i * 8)) & 0xFF);
        }
        h = mix(h, le.data(), le.size());
        h = mix(h, values[c].data(), values[c].size());
    }
    return h;
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
    // The rows emitted so far, as a hash, checkpointed with next_row.
    std::uint64_t emitted_hash{kFnvOffset};
    // What restore_offset() read: where to resume, and, when the checkpoint
    // holds one, the hash of the rows before that position.
    bool restored{false};
    std::size_t restored_row{0};
    std::optional<std::uint64_t> restored_hash;

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

    // A restore resumes by position, which holds only while the query returns
    // the rows before it again, in the same order: without ORDER BY ClickHouse
    // returns rows in whatever order its parallel reads finish, and a row
    // deleted, added or changed among them moves the rest. Resumed after such
    // a change, the source skipped rows. The rows before the position are
    // hashed and compared with the hash the checkpoint holds; on a difference,
    // or with no hash (an earlier release's checkpoint), every row is emitted
    // again: repeated, which at-least-once allows, never skipped.
    impl_->next_row = 0;
    impl_->emitted_hash = kFnvOffset;
    if (impl_->restored && impl_->restored_row > 0) {
        bool same = false;
        if (impl_->restored_hash && impl_->restored_row <= impl_->rows.size()) {
            std::uint64_t h = kFnvOffset;
            for (std::size_t r = 0; r < impl_->restored_row; ++r) {
                h = mix_row(h, impl_->rows[r]);
            }
            same = h == *impl_->restored_hash;
        }
        if (same) {
            impl_->next_row = impl_->restored_row;
            impl_->emitted_hash = *impl_->restored_hash;
        } else {
            clink::log::warn("source.clickhouse",
                             "clickhouse_source: the checkpoint covers the query's first " +
                                 std::to_string(impl_->restored_row) +
                                 " rows, and the query no longer returns those rows first, so "
                                 "every row is emitted again rather than resumed by position");
        }
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
        impl_->emitted_hash = mix_row(impl_->emitted_hash, impl_->rows[r]);
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
// The checkpoint's form: a version byte, the next row's index and the hash of
// the rows before it, each little-endian. An earlier release wrote the index
// alone, in 8 bytes.
constexpr std::uint8_t kOffsetVersion = 2;
constexpr std::size_t kOffsetBytes = 17;

std::uint64_t read_le64(const std::byte* p) {
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(p[i])) << (i * 8);
    }
    return v;
}
}  // namespace

void ClickHouseSource::snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId) {
    std::array<char, kOffsetBytes> bytes{};
    bytes[0] = static_cast<char>(kOffsetVersion);
    const auto cursor = static_cast<std::uint64_t>(impl_->next_row);
    for (int i = 0; i < 8; ++i) {
        bytes[static_cast<std::size_t>(1 + i)] = static_cast<char>((cursor >> (i * 8)) & 0xFF);
        bytes[static_cast<std::size_t>(9 + i)] =
            static_cast<char>((impl_->emitted_hash >> (i * 8)) & 0xFF);
    }
    backend.put_operator_state(
        op_id,
        StateBackend::KeyView{kClickHouseOffsetKey, std::strlen(kClickHouseOffsetKey)},
        StateBackend::ValueView{bytes.data(), bytes.size()});
}

bool ClickHouseSource::restore_offset(StateBackend& backend, OperatorId op_id) {
    auto v = backend.get_operator_state(
        op_id, StateBackend::KeyView{kClickHouseOffsetKey, std::strlen(kClickHouseOffsetKey)});
    if (!v.has_value()) {
        return false;
    }
    std::uint64_t restored = 0;
    std::optional<std::uint64_t> hash;
    if (v->size() == kOffsetBytes && std::to_integer<std::uint8_t>((*v)[0]) == kOffsetVersion) {
        restored = read_le64(v->data() + 1);
        hash = read_le64(v->data() + 9);
    } else if (v->size() == 8) {
        restored = read_le64(v->data());
    } else {
        throw std::runtime_error(
            "clickhouse_source: the checkpoint's position is in a form this release does not "
            "read (" +
            std::to_string(v->size()) + " bytes)");
    }
    impl_->restored = true;
    impl_->restored_row = static_cast<std::size_t>(restored);
    impl_->restored_hash = hash;
    // Until open() compares the rows, the position stands as restored, so a
    // checkpoint taken before then holds it again.
    impl_->next_row = impl_->restored_row;
    impl_->emitted_hash = hash.value_or(kFnvOffset);
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
