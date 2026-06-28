// Apache Iceberg sink implementation, built on iceberg-cpp. Reuses the SQL Row
// columnar Arrow batcher for typed Parquet data files (handed to iceberg-cpp's
// Parquet writer via the Arrow C Data Interface), and commits one Iceberg snapshot
// (FastAppend) per checkpoint interval through a SQLite SQL catalog. See
// iceberg_row_sink.hpp for the delivery contract.

#include "clink/iceberg/iceberg_row_sink.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/c/bridge.h>  // ExportRecordBatch (defines the shared ArrowArray first)

#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/state_backend.hpp"

#include "iceberg/catalog/sql/sql_catalog.h"
#include "iceberg/file_format.h"
#include "iceberg/file_io.h"
#include "iceberg/file_writer.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type.h"
#include "iceberg/update/fast_append.h"

// iceberg-cpp's Arrow / Parquet / Avro write backend lives in libiceberg_bundle. These
// entry points are exported there but their headers (iceberg/arrow/arrow_io_util.h,
// iceberg/parquet/parquet_register.h, iceberg/avro/avro_register.h) are not installed,
// so forward-declare the symbols we need. RegisterAll() force-pulls + runs the writer
// factory registration (it is explicit, not static-init that survives a static-lib link).
namespace iceberg::arrow {
std::unique_ptr<::iceberg::FileIO> MakeLocalFileIO();
// MakeS3FileIO self-initialises Arrow's S3 subsystem (EnsureS3Initialized) on first
// use; clink never calls arrow::fs::InitializeS3 elsewhere, so there is no double-init.
std::unique_ptr<::iceberg::FileIO> MakeS3FileIO(
    const std::unordered_map<std::string, std::string>& properties);
}  // namespace iceberg::arrow
namespace iceberg::parquet {
void RegisterAll();
}  // namespace iceberg::parquet
namespace iceberg::avro {
void RegisterAll();
}  // namespace iceberg::avro

namespace clink::iceberg {

namespace ice = ::iceberg;

namespace {

constexpr const char* kLabel = "iceberg";

void ensure_registered() {
    static std::once_flag once;
    std::call_once(once, [] {
        ice::parquet::RegisterAll();
        ice::avro::RegisterAll();
    });
}

template <typename T>
T unwrap(ice::Result<T> r, const std::string& what) {
    if (!r.has_value()) {
        throw std::runtime_error("iceberg: " + what + ": " + r.error().message);
    }
    return std::move(r).value();
}

void ensure_ok(const ice::Status& s, const std::string& what) {
    if (!s.has_value()) {
        throw std::runtime_error("iceberg: " + what + ": " + s.error().message);
    }
}

std::shared_ptr<ice::Type> arrow_to_ice_type(const arrow::DataType& t) {
    switch (t.id()) {
        case arrow::Type::INT64:
            return ice::int64();
        case arrow::Type::INT32:
            return ice::int32();
        case arrow::Type::DOUBLE:
            return ice::float64();
        case arrow::Type::FLOAT:
            return ice::float32();
        case arrow::Type::BOOL:
            return ice::boolean();
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
            return ice::string();
        case arrow::Type::DATE32:
            return ice::date();
        case arrow::Type::TIMESTAMP:
            return ice::timestamp();
        case arrow::Type::DECIMAL128: {
            const auto& d = static_cast<const arrow::Decimal128Type&>(t);
            return ice::decimal(d.precision(), d.scale());
        }
        default:
            throw std::runtime_error(std::string("iceberg: unsupported Arrow type ") +
                                     t.ToString());
    }
}

// Build the iceberg Schema from the batcher's Arrow schema, matching each field's
// nullability so the iceberg->Arrow round-trip the writer does agrees with the data.
std::shared_ptr<ice::Schema> arrow_schema_to_ice(const arrow::Schema& s) {
    std::vector<ice::SchemaField> fields;
    fields.reserve(static_cast<std::size_t>(s.num_fields()));
    int32_t id = 1;
    for (const auto& f : s.fields()) {
        auto ty = arrow_to_ice_type(*f->type());
        fields.push_back(f->nullable() ? ice::SchemaField::MakeOptional(id, f->name(), ty)
                                       : ice::SchemaField::MakeRequired(id, f->name(), ty));
        ++id;
    }
    return std::make_shared<ice::Schema>(std::move(fields));
}

std::string make_uuid() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<std::uint64_t> dist;
    std::uint64_t hi = (dist(gen) & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL;
    std::uint64_t lo = (dist(gen) & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;
    static const char* h = "0123456789abcdef";
    std::string b(32, '0');
    for (int i = 0; i < 16; ++i) {
        std::uint64_t byte = (i < 8) ? (hi >> (8 * (7 - i))) : (lo >> (8 * (15 - i)));
        b[2 * i] = h[(byte >> 4) & 0xF];
        b[2 * i + 1] = h[byte & 0xF];
    }
    return b.substr(0, 8) + "-" + b.substr(8, 4) + "-" + b.substr(12, 4) + "-" + b.substr(16, 4) +
           "-" + b.substr(20, 12);
}

}  // namespace

class IcebergRowSink final : public Sink<clink::sql::Row> {
public:
    explicit IcebergRowSink(IcebergRowSinkOptions opts)
        : opts_(std::move(opts)), dormant_(opts_.subtask_idx != 0) {
        if (opts_.warehouse.empty()) {
            throw std::runtime_error(opts_.name + ": 'warehouse' is required");
        }
        if (opts_.table.empty()) {
            throw std::runtime_error(opts_.name + ": 'table' is required");
        }
        if (!opts_.batcher.schema || !opts_.batcher.build) {
            throw std::runtime_error(opts_.name + ": batcher must have schema + build");
        }
    }

    void open() override {
        if (dormant_) {
            return;
        }
        // Hold mu_ across the whole of open() (incl. recovery) so an early CommitCheckpoint
        // on the TM thread cannot race the catalog/table build or the recovery commit.
        std::lock_guard<std::mutex> lk(mu_);
        ensure_registered();
        schema_ = opts_.batcher.schema();
        ice_schema_ = arrow_schema_to_ice(*schema_);

        // FileIO by warehouse scheme: S3 for an s3:// warehouse (data + table metadata go
        // to the object store), else local filesystem. Both factories return unique_ptr;
        // the catalog + writer want shared_ptr.
        const bool s3 = opts_.warehouse.rfind("s3://", 0) == 0;
        if (s3) {
            // The SQLite catalog file cannot live on S3, so an s3:// warehouse needs an
            // explicit LOCAL catalog_uri. (Full-S3 deployments use the REST catalog.)
            if (opts_.catalog_uri.empty()) {
                throw std::runtime_error(
                    opts_.name +
                    ": an s3:// warehouse requires an explicit local catalog_uri (the SQLite "
                    "catalog cannot live on S3; or use a REST catalog)");
            }
            file_io_ = std::shared_ptr<ice::FileIO>(
                ice::arrow::MakeS3FileIO(opts_.file_io_props).release());
        } else {
            file_io_ = std::shared_ptr<ice::FileIO>(ice::arrow::MakeLocalFileIO().release());
        }

        ice::sql::SqlCatalogConfig cfg;
        cfg.name = "clink";
        cfg.warehouse_location = opts_.warehouse;
        cfg.uri = opts_.catalog_uri.empty() ? (opts_.warehouse + "/catalog.db") : opts_.catalog_uri;
        catalog_ =
            unwrap(ice::sql::SqlCatalog::MakeSqliteCatalog(cfg, file_io_), "make sqlite catalog");

        ice::Namespace ns;
        ns.levels = opts_.namespace_levels;
        if (auto ex = catalog_->NamespaceExists(ns); ex.has_value() && !ex.value()) {
            (void)catalog_->CreateNamespace(ns, {});  // best-effort
        }
        id_.ns = ns;
        id_.name = opts_.table;

        // Drive the table location explicitly (warehouse/<ns levels>/<table>).
        std::string location = opts_.warehouse;
        for (const auto& level : opts_.namespace_levels) {
            location += "/" + level;
        }
        location += "/" + opts_.table;

        // Pre-create the metadata/ + data/ dirs for a LOCAL warehouse: iceberg-cpp's local
        // Arrow FileIO writes through OpenOutputStream, which does NOT create parent
        // directories, so the first metadata write in CreateTable would fail on a fresh
        // warehouse. On S3 directories are implicit (key prefixes), so this is skipped.
        std::error_code ec;
        if (!s3) {
            std::filesystem::create_directories(location + "/metadata", ec);
            std::filesystem::create_directories(location + "/data", ec);
        }

        if (auto ex = catalog_->TableExists(id_); ex.has_value() && !ex.value()) {
            (void)unwrap(catalog_->CreateTable(id_,
                                               ice_schema_,
                                               ice::PartitionSpec::Unpartitioned(),
                                               ice::SortOrder::Unsorted(),
                                               location,
                                               {}),
                         "create table");
        }
        table_ = unwrap(catalog_->LoadTable(id_), "load table");

        // For an already-existing LOCAL table the loaded location may differ from ours;
        // make sure its data/ + metadata/ dirs exist before the writer + snapshot commits.
        if (!s3) {
            std::filesystem::create_directories(std::string(table_->location()) + "/metadata", ec);
            std::filesystem::create_directories(std::string(table_->location()) + "/data", ec);
        }

        // Exactly-once recovery: commit any data files staged before a crash whose
        // checkpoint completed globally (idempotent - skips checkpoints already snapshotted).
        recover_pending_();
    }

    void on_data(const Batch<clink::sql::Row>& batch) override {
        if (dormant_ || batch.empty()) {
            return;
        }
        // mu_ serialises the runner thread (on_data/on_barrier) against the TM reader thread
        // that delivers on_commit/on_abort - both touch table_/catalog_/file_io_/writer_.
        std::lock_guard<std::mutex> lk(mu_);
        if (table_ == nullptr) {
            return;  // closed concurrently
        }
        if (!writer_) {
            open_writer_();
        }
        auto rb = opts_.batcher.build(batch);
        if (!rb) {
            throw std::runtime_error(opts_.name + ": batcher.build returned null");
        }
        ArrowArray c_arr{};
        if (auto st = arrow::ExportRecordBatch(*rb, &c_arr); !st.ok()) {
            throw std::runtime_error(opts_.name + ": ExportRecordBatch: " + st.ToString());
        }
        // Writer::Write does ImportRecordBatch, which on success MOVES c_arr (nulling its
        // release). On failure it may NOT consume it, so the C Data Interface leaves us, the
        // consumer, owning it. Release iff still owned: skips on success (no double-free),
        // frees on failure (no leak). Do this BEFORE ensure_ok throws on a bad status.
        auto wst = writer_->Write(&c_arr);
        if (c_arr.release != nullptr) {
            c_arr.release(&c_arr);
        }
        ensure_ok(wst, "writer Write");
        cur_records_ += static_cast<std::int64_t>(batch.size());
    }

    // Barrier = 2PC phase 1 (PRE-COMMIT). With a state backend the interval's data file is
    // closed + recorded in state but NOT yet snapshotted - it becomes visible only when the
    // engine confirms the checkpoint is globally durable (on_commit). Without a state
    // backend (standalone use, no JM) there is no second phase to wait for, so commit
    // immediately = at-least-once, preserving the simple-sink behaviour.
    void on_barrier(CheckpointBarrier b) override {
        if (dormant_) {
            return;
        }
        std::lock_guard<std::mutex> lk(mu_);
        if (table_ == nullptr) {
            return;  // closed concurrently
        }
        const std::uint64_t ckpt = b.id().value();
        if (two_phase_()) {
            stage_current_(ckpt);
        } else {
            commit_current_immediate_(ckpt);
        }
    }

    // 2PC phase 2 (COMMIT): the checkpoint is globally durable, so snapshot the data file
    // staged for it. Idempotent (skips a checkpoint already snapshotted), so a redelivered
    // commit or a recovery replay never double-commits.
    void on_commit(std::uint64_t checkpoint_id) override {
        if (dormant_) {
            return;
        }
        // Runs on the TM reader thread; serialise against close()/on_barrier and bail if the
        // sink was torn down concurrently (a late CommitCheckpoint racing shutdown).
        std::lock_guard<std::mutex> lk(mu_);
        auto* state = state_backend_();
        if (state == nullptr || table_ == nullptr || catalog_ == nullptr) {
            return;
        }
        auto stored = state->get(this->id(), state_key_(checkpoint_id));
        if (!stored.has_value()) {
            return;  // nothing staged for this checkpoint (empty interval / already committed)
        }
        Staged st = parse_staged_(*stored);
        commit_staged_(checkpoint_id, st, /*delete_data_on_commit_failure=*/false);
        state->erase(this->id(), state_key_(checkpoint_id));
    }

    // 2PC abort: the checkpoint did NOT complete globally, so drop the staged (unreferenced)
    // data file. The source replays the interval, which writes a fresh data file.
    void on_abort(std::uint64_t checkpoint_id) override {
        if (dormant_) {
            return;
        }
        std::lock_guard<std::mutex> lk(mu_);
        auto* state = state_backend_();
        if (state == nullptr || file_io_ == nullptr) {
            return;  // closed concurrently
        }
        auto stored = state->get(this->id(), state_key_(checkpoint_id));
        if (!stored.has_value()) {
            return;
        }
        Staged st = parse_staged_(*stored);
        (void)file_io_->DeleteFile(st.path);  // best-effort orphan cleanup
        state->erase(this->id(), state_key_(checkpoint_id));
    }

    void close() override {
        // Hold mu_ across teardown so a late on_commit/on_abort on the TM thread either runs
        // fully before close (mutex) or sees the reset table_/catalog_/file_io_ and bails.
        std::lock_guard<std::mutex> lk(mu_);
        if (!dormant_ && table_ != nullptr) {
            if (two_phase_()) {
                // The interval since the last barrier was never staged/committed; drop its
                // partial data file (the source replays it). Committed + staged-pending data
                // is durable in the catalog / state, untouched here.
                abandon_current_();
            } else {
                commit_current_immediate_(0);  // flush the tail (at-least-once standalone)
            }
        }
        writer_.reset();
        table_.reset();
        catalog_.reset();
        file_io_.reset();
    }

    std::string name() const override { return opts_.name; }

private:
    struct Staged {
        std::string path;
        std::int64_t records{0};
        std::int64_t size{0};
    };

    bool two_phase_() const noexcept { return state_backend_() != nullptr; }

    StateBackend* state_backend_() const noexcept {
        return this->runtime() != nullptr ? this->runtime()->state_backend() : nullptr;
    }

    // Single-writer table (only subtask 0 active), but keep the subtask in the key so the
    // scheme is uniform with the other 2PC sinks.
    std::string state_key_(std::uint64_t ckpt) const {
        return "_2pc_pending_sub" + std::to_string(opts_.subtask_idx) + "_" + std::to_string(ckpt);
    }

    void open_writer_() {
        cur_data_path_ = std::string(table_->location()) + "/data/" + make_uuid() + ".parquet";
        ice::WriterOptions wopts;
        wopts.path = cur_data_path_;
        wopts.schema = ice_schema_;
        wopts.io = file_io_;
        writer_ = unwrap(ice::WriterFactoryRegistry::Open(ice::FileFormatType::kParquet, wopts),
                         "open parquet writer");
        cur_records_ = 0;
    }

    // Close the interval's writer (producing a complete Parquet data file) and return its
    // DataFile facts. Returns false if there was no data this interval.
    bool finish_writer_(Staged& out) {
        if (!writer_) {
            return false;
        }
        ensure_ok(writer_->Close(), "writer Close");
        out.path = cur_data_path_;
        out.records = cur_records_;
        out.size = unwrap(writer_->length(), "writer length");
        writer_.reset();
        cur_data_path_.clear();
        cur_records_ = 0;
        return true;
    }

    // Phase 1: close the data file + record it in state, keyed by checkpoint. No snapshot.
    void stage_current_(std::uint64_t ckpt) {
        Staged st;
        if (!finish_writer_(st)) {
            return;  // empty interval -> nothing to stage, no snapshot
        }
        std::string blob =
            std::to_string(st.records) + "\t" + std::to_string(st.size) + "\t" + st.path;
        state_backend_()->put(
            this->id(), state_key_(ckpt), std::string_view{blob.data(), blob.size()});
    }

    // Standalone (no 2PC): close + snapshot immediately.
    void commit_current_immediate_(std::uint64_t ckpt) {
        Staged st;
        if (!finish_writer_(st)) {
            return;
        }
        commit_staged_(ckpt, st, /*delete_data_on_commit_failure=*/true);
    }

    // Snapshot one staged data file via a FastAppend, tagging the snapshot with the
    // checkpoint id so the commit is idempotent. The in-memory table is stale after a
    // commit, so reload it for the next append.
    void commit_staged_(std::uint64_t ckpt, const Staged& st, bool delete_data_on_commit_failure) {
        if (already_committed_(ckpt)) {
            return;  // idempotent: this checkpoint's snapshot already exists
        }
        auto df = std::make_shared<ice::DataFile>();
        df->content = ice::DataFile::Content::kData;
        df->file_path = st.path;
        df->file_format = ice::FileFormatType::kParquet;
        df->record_count = st.records;
        df->file_size_in_bytes = st.size;
        df->partition_spec_id = 0;  // the table is created unpartitioned (spec id 0)

        auto fa = unwrap(table_->NewFastAppend(), "new fast append");
        fa->AppendFile(df);
        fa->Set(kCheckpointProp, std::to_string(ckpt));  // idempotency marker on the snapshot
        if (auto cst = fa->Commit(); !cst.has_value()) {
            if (delete_data_on_commit_failure) {
                // Standalone: no state to retry from, so do not strand the orphan data file.
                // (In 2PC, leave it: state still references it and recovery retries this commit.)
                (void)file_io_->DeleteFile(st.path);
            }
            throw std::runtime_error("iceberg: commit snapshot: " + cst.error().message);
        }
        table_ = unwrap(catalog_->LoadTable(id_), "reload table after commit");

        clink::metrics::connector::records_out_inc(kLabel, static_cast<std::uint64_t>(st.records));
        clink::metrics::connector::bytes_out_inc(kLabel, static_cast<std::uint64_t>(st.size));
    }

    // Has a snapshot tagged with this checkpoint id already been committed?
    bool already_committed_(std::uint64_t ckpt) const {
        const std::string want = std::to_string(ckpt);
        for (const auto& snap : table_->snapshots()) {
            auto it = snap->summary.find(kCheckpointProp);
            if (it != snap->summary.end() && it->second == want) {
                return true;
            }
        }
        return false;
    }

    // Drop the in-progress (un-staged) interval: delete its partial data file.
    void abandon_current_() {
        if (writer_) {
            (void)writer_->Close();
            writer_.reset();
        }
        if (!cur_data_path_.empty()) {
            (void)file_io_->DeleteFile(cur_data_path_);
            cur_data_path_.clear();
        }
        cur_records_ = 0;
    }

    // Blob layout = "<records>\t<size>\t<path>". The path is the tail (everything after the
    // 2nd tab) so it may contain any character; stoll failures surface as a clear error
    // (not a raw std::invalid_argument escaping recovery's catch).
    Staged parse_staged_(const std::vector<std::byte>& blob) const {
        std::string s(reinterpret_cast<const char*>(blob.data()), blob.size());
        const auto t1 = s.find('\t');
        const auto t2 = (t1 == std::string::npos) ? std::string::npos : s.find('\t', t1 + 1);
        if (t1 == std::string::npos || t2 == std::string::npos) {
            throw std::runtime_error(opts_.name + ": corrupt staged-commit state: " + s);
        }
        Staged st;
        try {
            st.records = std::stoll(s.substr(0, t1));
            st.size = std::stoll(s.substr(t1 + 1, t2 - t1 - 1));
        } catch (const std::exception& e) {
            throw std::runtime_error(opts_.name + ": corrupt staged-commit state (" + e.what() +
                                     "): " + s);
        }
        st.path = s.substr(t2 + 1);
        return st;
    }

    // Recovery: snapshot any data files staged before a crash but not yet committed. Routed
    // through the idempotent commit, so a file whose checkpoint already snapshotted is skipped
    // (its state key is then cleared). A still-pending key whose checkpoint never completed is
    // left for the engine's abort, or harmlessly re-committed on the next global checkpoint.
    void recover_pending_() {
        auto* state = state_backend_();
        if (state == nullptr) {
            return;
        }
        const std::string prefix = "_2pc_pending_sub" + std::to_string(opts_.subtask_idx) + "_";
        std::vector<std::pair<std::string, std::vector<std::byte>>> pending;
        state->scan(this->id(), [&](StateBackend::KeyView k, StateBackend::ValueView v) {
            const std::string key{k};
            if (key.rfind(prefix, 0) == 0) {
                const auto* p = reinterpret_cast<const std::byte*>(v.data());
                pending.emplace_back(key, std::vector<std::byte>{p, p + v.size()});
            }
        });
        for (const auto& [key, blob] : pending) {
            std::uint64_t ckpt = 0;
            try {
                ckpt = std::stoull(key.substr(prefix.size()));
            } catch (...) {
                continue;
            }
            Staged st = parse_staged_(blob);
            commit_staged_(ckpt, st, /*delete_data_on_commit_failure=*/false);
            state->erase(this->id(), key);
        }
    }

    static constexpr const char* kCheckpointProp = "clink.checkpoint-id";

    IcebergRowSinkOptions opts_;
    bool dormant_{false};
    // Serialises the runner thread (open/on_data/on_barrier/close) against the TM reader
    // thread that delivers on_commit/on_abort. Single-writer table, so contention is rare.
    mutable std::mutex mu_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<ice::Schema> ice_schema_;
    std::shared_ptr<ice::FileIO> file_io_;
    std::shared_ptr<ice::sql::SqlCatalog> catalog_;
    ice::TableIdentifier id_;
    std::shared_ptr<ice::Table> table_;
    std::unique_ptr<ice::Writer> writer_;
    std::string cur_data_path_;
    std::int64_t cur_records_{0};
};

std::shared_ptr<Sink<clink::sql::Row>> make_iceberg_row_sink(IcebergRowSinkOptions opts) {
    // The sink is stateful under 2PC (it stages pending commits in the state backend keyed
    // by its OperatorId, which derives from the uid). Default a stable uid from the target
    // table so the staged-commit state survives a restart even if the caller did not pin one
    // explicitly; an explicit .uid() from the planner still overrides this.
    std::string uid = "iceberg:";
    for (const auto& level : opts.namespace_levels) {
        uid += level + ".";
    }
    uid += opts.table;
    auto sink = std::make_shared<IcebergRowSink>(std::move(opts));
    sink->set_uid(uid);
    return sink;
}

}  // namespace clink::iceberg
