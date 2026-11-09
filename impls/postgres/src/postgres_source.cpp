#include "clink/connectors/postgres_source.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/connectors/postgres_tls.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"

#ifdef CLINK_HAS_POSTGRES
#include <libpq-fe.h>
#endif

namespace clink {

#ifdef CLINK_HAS_POSTGRES

namespace {

constexpr const char* kLog = "source.postgres";

// FNV-1a over a row's cells, each as its null flag, its length and its bytes,
// so rows that differ anywhere, or split their bytes differently across cells,
// hash apart.
constexpr std::uint64_t kFnvOffset = 14695981039346656037ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

std::uint64_t mix(std::uint64_t h, const char* data, std::size_t n) {
    for (std::size_t i = 0; i < n; ++i) {
        h ^= static_cast<std::uint8_t>(data[i]);
        h *= kFnvPrime;
    }
    return h;
}

std::uint64_t mix_row(std::uint64_t h, const PGresult* result, int row, int ncols) {
    for (int c = 0; c < ncols; ++c) {
        const char null = PQgetisnull(result, row, c) != 0 ? 1 : 0;
        h = mix(h, &null, 1);
        const auto len = static_cast<std::uint64_t>(null != 0 ? 0 : PQgetlength(result, row, c));
        std::array<char, 8> le{};
        for (int i = 0; i < 8; ++i) {
            le[static_cast<std::size_t>(i)] = static_cast<char>((len >> (i * 8)) & 0xFF);
        }
        h = mix(h, le.data(), le.size());
        if (null == 0) {
            h = mix(h, PQgetvalue(result, row, c), static_cast<std::size_t>(len));
        }
    }
    return h;
}

}  // namespace

struct PostgresSource::Impl {
    Options opts;
    PGconn* conn{nullptr};
    PGresult* result{nullptr};
    int total_rows{0};
    int next_row{0};
    // The rows emitted so far, as a hash, checkpointed with next_row.
    std::uint64_t emitted_hash{kFnvOffset};
    // What restore_offset() read: where to resume, and, when the checkpoint
    // holds one, the hash of the rows before that position.
    bool restored{false};
    int restored_row{0};
    std::optional<std::uint64_t> restored_hash;
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
    clink::connectors::pg::assert_no_silent_downgrade(
        impl_->conn, impl_->opts.conninfo, "postgres_source");

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

    // A restore resumes by position, which holds only while the query returns
    // the rows before that position again, in the same order: a query without
    // ORDER BY may return them in another (a parallel or synchronized scan), and
    // a row deleted, added or changed among them moves the rest. Resumed after
    // such a change, the source skipped rows. The rows before the position are
    // hashed and compared with the hash the checkpoint holds; on any difference,
    // or with no hash to compare (a checkpoint from an earlier release), every
    // row is emitted again: repeated, which at-least-once allows, never skipped.
    impl_->next_row = 0;
    impl_->emitted_hash = kFnvOffset;
    if (impl_->restored && impl_->restored_row > 0) {
        bool same = false;
        if (impl_->restored_hash && impl_->restored_row <= impl_->total_rows) {
            std::uint64_t h = kFnvOffset;
            for (int r = 0; r < impl_->restored_row; ++r) {
                h = mix_row(h, impl_->result, r, ncols);
            }
            same = h == *impl_->restored_hash;
        }
        if (same) {
            impl_->next_row = impl_->restored_row;
            impl_->emitted_hash = *impl_->restored_hash;
        } else {
            clink::log::warn(
                kLog,
                "postgres_source: the checkpoint covers the query's first " +
                    std::to_string(impl_->restored_row) + " rows, and " +
                    (impl_->restored_hash
                         ? std::string{impl_->restored_row > impl_->total_rows
                                           ? "the query now returns only " +
                                                 std::to_string(impl_->total_rows)
                                           : "the query's first rows are not those rows"}
                         : std::string{"the checkpoint is from an earlier release and holds "
                                       "nothing to compare them with"}) +
                    ", so every row is emitted again rather than resumed by position");
        }
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
        std::vector<char> nulls;
        values.reserve(static_cast<std::size_t>(ncols));
        nulls.reserve(static_cast<std::size_t>(ncols));
        for (int c = 0; c < ncols; ++c) {
            if (PQgetisnull(impl_->result, r, c) != 0) {
                values.emplace_back();
                nulls.push_back(1);  // SQL NULL (distinct from an empty-string value)
            } else {
                values.emplace_back(PQgetvalue(impl_->result, r, c),
                                    static_cast<std::size_t>(PQgetlength(impl_->result, r, c)));
                nulls.push_back(0);
            }
        }
        batch.emplace(PostgresRow{impl_->column_names, std::move(values), std::move(nulls)});
        impl_->emitted_hash = mix_row(impl_->emitted_hash, impl_->result, r, ncols);
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
// The checkpoint's form: a version byte, the next row's index and the hash of
// the rows before it, each little-endian. An earlier release wrote the index
// alone, in 8 bytes.
constexpr std::uint8_t kOffsetVersion = 2;
constexpr std::size_t kOffsetBytes = 17;
constexpr std::size_t kLegacyOffsetBytes = 8;

std::uint64_t read_le64(const std::byte* p) {
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(p[i])) << (i * 8);
    }
    return v;
}
}  // namespace

void PostgresSource::snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId) {
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
        StateBackend::KeyView{kPostgresOffsetKey, std::strlen(kPostgresOffsetKey)},
        StateBackend::ValueView{bytes.data(), bytes.size()});
}

bool PostgresSource::restore_offset(StateBackend& backend, OperatorId op_id) {
    auto v = backend.get_operator_state(
        op_id, StateBackend::KeyView{kPostgresOffsetKey, std::strlen(kPostgresOffsetKey)});
    if (!v.has_value()) {
        return false;
    }
    std::uint64_t restored = 0;
    std::optional<std::uint64_t> hash;
    if (v->size() == kOffsetBytes && static_cast<std::uint8_t>((*v)[0]) == kOffsetVersion) {
        restored = read_le64(v->data() + 1);
        hash = read_le64(v->data() + 9);
    } else if (v->size() == kLegacyOffsetBytes) {
        restored = read_le64(v->data());
    } else {
        throw std::runtime_error(
            "postgres_source: the checkpoint's position is in a form this release does not "
            "read (" +
            std::to_string(v->size()) + " bytes)");
    }
    // next_row is an int (PQntuples returns int, so the result set can't exceed
    // INT_MAX rows). A persisted cursor beyond that is corrupt state, not a real
    // position - treat it as no restore rather than truncating into a negative.
    if (restored > static_cast<std::uint64_t>(std::numeric_limits<int>::max())) {
        return false;
    }
    impl_->restored = true;
    impl_->restored_row = static_cast<int>(restored);
    impl_->restored_hash = hash;
    // Until open() compares the rows, the position stands as restored, so a
    // checkpoint taken before then holds it again.
    impl_->next_row = impl_->restored_row;
    impl_->emitted_hash = hash.value_or(kFnvOffset);
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
