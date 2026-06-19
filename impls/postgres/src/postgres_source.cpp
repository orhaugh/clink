#include "clink/connectors/postgres_source.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

#include "clink/metrics/connector_metrics.hpp"

#ifdef CLINK_HAS_POSTGRES
#include <libpq-fe.h>
#endif

namespace clink {

#ifdef CLINK_HAS_POSTGRES

struct PostgresSource::Impl {
    Options opts;
    PGconn* conn{nullptr};
    PGresult* result{nullptr};
    int total_rows{0};
    int next_row{0};
    std::shared_ptr<const std::vector<std::string>> column_names;
};

bool PostgresSource::is_real_implementation() {
    return true;
}

PostgresSource::PostgresSource(Options opts) : impl_(std::make_unique<Impl>()) {
    impl_->opts = std::move(opts);
}

PostgresSource::~PostgresSource() {
    if (impl_) {
        if (impl_->result != nullptr) {
            PQclear(impl_->result);
        }
        if (impl_->conn != nullptr) {
            PQfinish(impl_->conn);
        }
    }
}

void PostgresSource::open() {
    impl_->conn = PQconnectdb(impl_->opts.conninfo.c_str());
    if (PQstatus(impl_->conn) != CONNECTION_OK) {
        const std::string err = PQerrorMessage(impl_->conn);
        PQfinish(impl_->conn);
        impl_->conn = nullptr;
        throw std::runtime_error("PostgresSource::open failed: " + err);
    }

    impl_->result = PQexec(impl_->conn, impl_->opts.query.c_str());
    if (PQresultStatus(impl_->result) != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(impl_->conn);
        PQclear(impl_->result);
        impl_->result = nullptr;
        throw std::runtime_error("PostgresSource::open query failed: " + err);
    }

    impl_->total_rows = PQntuples(impl_->result);

    // Capture column names once; every emitted row shares the pointer.
    auto names = std::make_shared<std::vector<std::string>>();
    const int ncols = PQnfields(impl_->result);
    names->reserve(static_cast<std::size_t>(ncols));
    for (int c = 0; c < ncols; ++c) {
        names->emplace_back(PQfname(impl_->result, c));
    }
    impl_->column_names = std::move(names);

    // #60: a cursor restored by restore_offset() (which runs before open()) may
    // exceed this run's row count if the result set shrank between runs. Clamp
    // so produce() finds nothing left rather than indexing out of bounds.
    if (impl_->next_row > impl_->total_rows) {
        impl_->next_row = impl_->total_rows;
    }
}

bool PostgresSource::produce(Emitter<PostgresRow>& out) {
    if (this->cancelled() || impl_->result == nullptr) {
        return false;
    }
    if (impl_->next_row >= impl_->total_rows) {
        return false;
    }

    const int ncols = PQnfields(impl_->result);
    const int end_row =
        std::min(impl_->next_row + static_cast<int>(impl_->opts.batch_size), impl_->total_rows);

    Batch<PostgresRow> batch;
    for (int r = impl_->next_row; r < end_row; ++r) {
        std::vector<std::string> values;
        values.reserve(static_cast<std::size_t>(ncols));
        for (int c = 0; c < ncols; ++c) {
            if (PQgetisnull(impl_->result, r, c) != 0) {
                values.emplace_back();
            } else {
                values.emplace_back(PQgetvalue(impl_->result, r, c),
                                    static_cast<std::size_t>(PQgetlength(impl_->result, r, c)));
            }
        }
        batch.emplace(PostgresRow{impl_->column_names, std::move(values)});
    }
    if (!batch.empty()) {
        clink::metrics::connector::records_in_inc("postgres", batch.size());
        out.emit_data(std::move(batch));
    }
    impl_->next_row = end_row;
    return impl_->next_row < impl_->total_rows;
}

void PostgresSource::close() {
    if (impl_) {
        if (impl_->result != nullptr) {
            PQclear(impl_->result);
            impl_->result = nullptr;
        }
        if (impl_->conn != nullptr) {
            PQfinish(impl_->conn);
            impl_->conn = nullptr;
        }
    }
}

namespace {
constexpr const char* kPostgresOffsetKey = "__postgres_source_row__";
}  // namespace

void PostgresSource::snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId) {
    const auto cursor = static_cast<std::uint64_t>(impl_->next_row);
    std::array<std::byte, 8> bytes{};
    for (int i = 0; i < 8; ++i) {
        bytes[static_cast<std::size_t>(i)] = static_cast<std::byte>((cursor >> (i * 8)) & 0xFF);
    }
    backend.put_operator_state(
        op_id,
        StateBackend::KeyView{kPostgresOffsetKey, std::strlen(kPostgresOffsetKey)},
        StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
}

bool PostgresSource::restore_offset(StateBackend& backend, OperatorId op_id) {
    auto v = backend.get_operator_state(
        op_id, StateBackend::KeyView{kPostgresOffsetKey, std::strlen(kPostgresOffsetKey)});
    if (!v.has_value() || v->size() < 8) {
        return false;
    }
    std::uint64_t restored = 0;
    for (int i = 0; i < 8; ++i) {
        restored |= static_cast<std::uint64_t>(static_cast<std::uint8_t>((*v)[i])) << (i * 8);
    }
    // next_row is an int (PQntuples returns int, so the result set can't exceed
    // INT_MAX rows). A persisted cursor beyond that is corrupt state, not a real
    // position - treat it as no restore rather than truncating into a negative.
    if (restored > static_cast<std::uint64_t>(std::numeric_limits<int>::max())) {
        return false;
    }
    impl_->next_row = static_cast<int>(restored);
    return true;
}

#else  // !CLINK_HAS_POSTGRES

struct PostgresSource::Impl {};

bool PostgresSource::is_real_implementation() {
    return false;
}

PostgresSource::PostgresSource(Options /*opts*/) {
    throw std::runtime_error(
        "PostgresSource: built without libpq. Install postgresql or libpq "
        "(e.g. `brew install libpq`) and reconfigure with CLINK_WITH_POSTGRES=ON.");
}

PostgresSource::~PostgresSource() = default;
void PostgresSource::open() {}
bool PostgresSource::produce(Emitter<PostgresRow>& /*out*/) {
    return false;
}
void PostgresSource::close() {}
void PostgresSource::snapshot_offset(StateBackend&, OperatorId, CheckpointId) {}
bool PostgresSource::restore_offset(StateBackend&, OperatorId) {
    return false;
}

#endif

}  // namespace clink
