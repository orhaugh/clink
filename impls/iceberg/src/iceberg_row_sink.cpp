// Apache Iceberg sink implementation, built on iceberg-cpp. Reuses the SQL Row
// columnar Arrow batcher for typed Parquet data files (handed to iceberg-cpp's
// Parquet writer via the Arrow C Data Interface), and commits Iceberg snapshots
// (FastAppend) through a SQLite SQL catalog. See iceberg_row_sink.hpp for the
// delivery contract (exactly-once under 2PC), S3 + partitioning support.

#include "clink/iceberg/iceberg_row_sink.hpp"

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/c/bridge.h>  // ExportRecordBatch (defines the shared ArrowArray first)
#include <arrow/c/bridge.h>

#include "clink/config/json.hpp"
#include "clink/connectors/arrow_s3_lifecycle.hpp"
#include "clink/iceberg/iceberg_row_source.hpp"
#include "clink/iceberg/state_export.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/row_columnar_batcher.hpp"
#include "clink/sql/row_kind.hpp"
#include "clink/state/state_backend.hpp"

#include "iceberg/arrow_c_data.h"
#include "iceberg/catalog.h"
#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/rest_catalog.h"
#include "iceberg/catalog/sql/sql_catalog.h"
#include "iceberg/data/equality_delete_writer.h"
#include "iceberg/data/writer.h"
#include "iceberg/expression/literal.h"
#include "iceberg/file_format.h"
#include "iceberg/file_io.h"
#include "iceberg/file_reader.h"
#include "iceberg/file_writer.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_field.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_scan.h"
#include "iceberg/transaction.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/merging_snapshot_update.h"

// iceberg-cpp's Arrow / Parquet / Avro write backend lives in libiceberg_bundle. These
// entry points are exported there but their headers (iceberg/arrow/arrow_io_util.h,
// iceberg/parquet/parquet_register.h, iceberg/avro/avro_register.h) are not installed,
// so forward-declare the symbols we need. RegisterAll() force-pulls + runs the writer
// factory registration (it is explicit, not static-init that survives a static-lib link).
namespace iceberg::arrow {
std::unique_ptr<::iceberg::FileIO> MakeLocalFileIO();
// MakeS3FileIO self-initialises Arrow's S3 subsystem (EnsureS3Initialized) on first use.
// We init through clink's single S3 lifecycle owner just before calling it, so this lazy
// init no-ops and there is exactly one InitializeS3 / one atexit FinalizeS3 per process.
// NOTE: unlike MakeLocalFileIO, this returns Result<unique_ptr<FileIO>> (see
// iceberg/arrow/arrow_io_util.h). The return type is not part of the mangled name, so
// declaring it as a plain unique_ptr links fine but reads a garbage pointer back (the
// Result is returned via a hidden sret) - a SIGSEGV when that FileIO is first used.
::iceberg::Result<std::unique_ptr<::iceberg::FileIO>> MakeS3FileIO(
    const std::unordered_map<std::string, std::string>& properties);
// Registers the arrow-fs-local + arrow-fs-s3 FileIO factories in the FileIORegistry. The
// SQLite-catalog path builds FileIO directly, but the REST catalog resolves it by name via
// the registry, so this must run before a REST catalog is constructed.
void RegisterAll();
}  // namespace iceberg::arrow
namespace iceberg::parquet {
void RegisterAll();
}  // namespace iceberg::parquet
namespace iceberg::avro {
void RegisterAll();
}  // namespace iceberg::avro

namespace clink::iceberg {

namespace ice = ::iceberg;
namespace cfg = clink::config;

namespace {

constexpr const char* kLabel = "iceberg";

void ensure_registered() {
    static std::once_flag once;
    std::call_once(once, [] {
        ice::parquet::RegisterAll();
        ice::avro::RegisterAll();
        ice::arrow::RegisterAll();  // arrow-fs-local + arrow-fs-s3 FileIO factories (REST catalog)
        // The iceberg S3 FileIO (explicit s3:// warehouse) and an S3-backed REST catalog both
        // lazily EnsureS3Initialized via Arrow's AWS CRT under the hood, bringing up system
        // OpenSSL. Suppress OpenSSL's heap-corrupting atexit before that can happen; harmless on
        // the local-only path. S3 finalisation (joining the CRT threads) is the host process's
        // job at end of main - clink_node does it; see arrow_s3_lifecycle.hpp.
        clink::connectors::suppress_openssl_atexit();
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
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:
            return ice::binary();
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
// Field ids are assigned 1..N in column order (so the partition source ids below match).
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

// Concrete MergingSnapshotUpdate so the upsert path can commit data files AND v2 equality-
// delete files in one snapshot (iceberg-cpp 0.3.0 ships no public RowDelta; the base's
// Catalog + table open, shared by the streaming sink and the state export.
// Selects the catalog by catalog_uri scheme (http(s):// = REST, otherwise
// SQLite SQL catalog with a local or S3 FileIO), best-effort-creates the
// namespace, creates the table when missing (with an explicit location for
// SQL catalogs - see the comment inside) and loads it.
struct CatalogTargetSpec {
    std::string warehouse;
    std::string table;
    std::string catalog_uri;
    std::string rest_auth_token;
    std::string name;  // error-message prefix
    std::vector<std::string> namespace_levels;
    std::unordered_map<std::string, std::string> file_io_props;
};

struct OpenedCatalogTable {
    std::shared_ptr<ice::FileIO> file_io;
    std::shared_ptr<ice::Catalog> catalog;
    ice::TableIdentifier id;
    std::shared_ptr<ice::Table> table;
};

OpenedCatalogTable open_catalog_table(const CatalogTargetSpec& t,
                                      const std::shared_ptr<ice::Schema>& ice_schema,
                                      const std::shared_ptr<ice::PartitionSpec>& spec) {
    OpenedCatalogTable out;
    // Catalog selection by catalog_uri scheme:
    //   http(s)://  -> REST catalog (resolves its own FileIO, e.g. S3, from /config +
    //                  the io props; the table location is server-assigned).
    //   otherwise   -> SQLite SQL catalog + an explicit local/S3 FileIO.
    const bool rest =
        t.catalog_uri.rfind("http://", 0) == 0 || t.catalog_uri.rfind("https://", 0) == 0;
    const bool s3 = t.warehouse.rfind("s3://", 0) == 0;

    if (rest) {
        std::unordered_map<std::string, std::string> props;
        props["uri"] = t.catalog_uri;
        props["name"] = "clink";
        if (!t.warehouse.empty()) {
            props["warehouse"] = t.warehouse;
        }
        // Pass FileIO props (s3.endpoint/region/credentials/path-style) through so the
        // catalog-resolved FileIO can reach the object store.
        for (const auto& [k, v] : t.file_io_props) {
            props[k] = v;
        }
        if (!t.rest_auth_token.empty()) {
            props["header.Authorization"] = "Bearer " + t.rest_auth_token;
        }
        // RestCatalog::Make does a synchronous GET /config here; a bad/unreachable
        // endpoint surfaces as a clear error (open() is otherwise local-only).
        out.catalog = unwrap(ice::rest::RestCatalog::Make(
                                 ice::rest::RestCatalogProperties::FromMap(std::move(props))),
                             "make rest catalog");
    } else if (s3) {
        // The SQLite catalog file cannot live on S3, so an s3:// warehouse needs an
        // explicit LOCAL catalog_uri (or use a REST catalog for full-S3).
        if (t.catalog_uri.empty()) {
            throw std::runtime_error(
                t.name +
                ": an s3:// warehouse requires an explicit local catalog_uri (the SQLite "
                "catalog cannot live on S3; or use a REST catalog)");
        }
        // Let iceberg-cpp's MakeS3FileIO own the Arrow S3 bring-up (it guards
        // InitializeS3 to once per process). Do NOT pre-init via clink's owner here:
        // 0.3.0's MakeS3FileIO calls arrow::fs::InitializeS3, which RETURNS AN ERROR if
        // S3 was already initialized (unlike EnsureS3Initialized) - so a clink pre-init
        // makes it fail "S3 was already initialized". ensure_registered() already
        // suppressed OpenSSL's atexit before any S3 bring-up, and the process finalises S3
        // once at exit (finalize_arrow_s3 / the s3-finalizing test main). This mirrors the
        // REST-catalog path, which likewise lets iceberg resolve + initialise its own S3 IO.
        out.file_io = std::shared_ptr<ice::FileIO>(
            unwrap(ice::arrow::MakeS3FileIO(t.file_io_props), "make s3 file io").release());
    } else {
        out.file_io = std::shared_ptr<ice::FileIO>(ice::arrow::MakeLocalFileIO().release());
    }

    if (!rest) {
        ice::sql::SqlCatalogConfig cfg_;
        cfg_.name = "clink";
        cfg_.warehouse_location = t.warehouse;
        cfg_.uri = t.catalog_uri.empty() ? (t.warehouse + "/catalog.db") : t.catalog_uri;
        out.catalog = unwrap(ice::sql::SqlCatalog::MakeSqliteCatalog(cfg_, out.file_io),
                             "make sqlite catalog");
    }

    ice::Namespace ns;
    ns.levels = t.namespace_levels;
    // A null ice_schema selects LOAD-ONLY mode (the source path): never
    // create the namespace or the table - a missing table is the user's
    // error, not a fresh empty table.
    const bool load_only = ice_schema == nullptr;
    if (!load_only) {
        if (auto ex = out.catalog->NamespaceExists(ns); ex.has_value() && !ex.value()) {
            (void)out.catalog->CreateNamespace(ns, {});  // best-effort
        }
    }
    out.id.ns = ns;
    out.id.name = t.table;
    if (load_only) {
        if (auto ex = out.catalog->TableExists(out.id); ex.has_value() && !ex.value()) {
            throw std::runtime_error(t.name + ": iceberg table '" + t.table +
                                     "' does not exist in this catalog");
        }
        out.table = unwrap(out.catalog->LoadTable(out.id), "load table");
        if (rest) {
            out.file_io = out.table->io();
        }
        return out;
    }

    // Table location (warehouse/<ns>/<table>). A REST catalog lets the server assign it
    // (pass empty). A SQL catalog - local-FS OR S3 - must be given an EXPLICIT location:
    // iceberg-cpp's SqlCatalog::CreateTable does not derive one from the warehouse, so an
    // empty location makes it write the initial metadata to an empty path, dereferencing a
    // null output stream (SIGSEGV in TableMetadataUtil::Write). Only a local-FS warehouse
    // needs the dirs pre-created (Arrow's local FileIO does not mkdir parents; S3 has none).
    const bool local_fs = !rest && !s3;
    std::string location;
    if (!rest) {
        location = t.warehouse;
        for (const auto& level : t.namespace_levels) {
            location += "/" + level;
        }
        location += "/" + t.table;
        if (local_fs) {
            std::error_code ec;
            std::filesystem::create_directories(location + "/metadata", ec);
            std::filesystem::create_directories(location + "/data", ec);
        }
    }

    if (auto ex = out.catalog->TableExists(out.id); ex.has_value() && !ex.value()) {
        (void)unwrap(out.catalog->CreateTable(
                         out.id, ice_schema, spec, ice::SortOrder::Unsorted(), location, {}),
                     "create table");
    }
    out.table = unwrap(out.catalog->LoadTable(out.id), "load table");

    // REST: the table carries the catalog-resolved FileIO (e.g. S3) - adopt it for the
    // data-file writer. For an existing local table, ensure its data/metadata dirs exist.
    if (rest) {
        out.file_io = out.table->io();
    } else if (local_fs) {
        std::error_code ec;
        std::filesystem::create_directories(std::string(out.table->location()) + "/metadata", ec);
        std::filesystem::create_directories(std::string(out.table->location()) + "/data", ec);
    }
    return out;
}

// AddDataFile/AddDeleteFile are protected, so a thin subclass exposes them - the pattern
// the in-tree merging_snapshot_update_test uses). operation() = overwrite.
class RowDelta : public ice::MergingSnapshotUpdate {
public:
    static ice::Result<std::unique_ptr<RowDelta>> Make(std::string table_name,
                                                       std::shared_ptr<ice::Table> table) {
        auto ctx = ice::TransactionContext::Make(std::move(table), ice::TransactionKind::kUpdate);
        if (!ctx.has_value()) {
            return std::unexpected(ctx.error());
        }
        return std::unique_ptr<RowDelta>(
            new RowDelta(std::move(table_name), std::move(ctx.value())));
    }
    std::string operation() override { return "overwrite"; }
    ice::Status AddData(std::shared_ptr<ice::DataFile> f) { return AddDataFile(std::move(f)); }
    ice::Status AddDelete(std::shared_ptr<ice::DataFile> f) { return AddDeleteFile(std::move(f)); }

private:
    RowDelta(std::string table_name, std::shared_ptr<ice::TransactionContext> ctx)
        : ice::MergingSnapshotUpdate(std::move(table_name), std::move(ctx)) {}
};

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
        auto spec = build_partition_spec_();  // also fills part_cols_

        upsert_ = !opts_.equality_key.empty();
        if (upsert_) {
            if (!opts_.partition_by.empty()) {
                throw std::runtime_error(opts_.name +
                                         ": upsert (equality_key) + partition_by is not "
                                         "supported in v1");
            }
            eq_cols_ = resolve_cols_(opts_.equality_key, "equality_key");
            // The equality-delete files carry only the key columns, identified by field id.
            std::vector<ice::SchemaField> kf;
            for (const auto& kc : eq_cols_) {
                kf.push_back(ice::SchemaField::MakeRequired(
                    kc.source_id,
                    kc.name,
                    arrow_to_ice_type(*schema_->field(kc.source_id - 1)->type())));
            }
            key_ice_schema_ = std::make_shared<ice::Schema>(std::move(kf));
        }

        // Catalog + table open, shared with the state export (see
        // open_catalog_table above): REST / S3-SQLite / local-SQLite
        // selection, namespace + table create-if-missing, LoadTable.
        CatalogTargetSpec target;
        target.warehouse = opts_.warehouse;
        target.table = opts_.table;
        target.catalog_uri = opts_.catalog_uri;
        target.rest_auth_token = opts_.rest_auth_token;
        target.name = opts_.name;
        target.namespace_levels = opts_.namespace_levels;
        target.file_io_props = opts_.file_io_props;
        auto opened = open_catalog_table(target, ice_schema_, spec);
        file_io_ = std::move(opened.file_io);
        catalog_ = std::move(opened.catalog);
        id_ = std::move(opened.id);
        table_ = std::move(opened.table);

        // Exactly-once recovery: commit any data files staged before a crash whose
        // checkpoint completed globally (idempotent - skips checkpoints already snapshotted).
        recover_pending_();
    }

    void on_data(const Batch<clink::sql::Row>& batch) override {
        if (dormant_ || batch.empty()) {
            return;
        }
        // mu_ serialises the runner thread (on_data/on_barrier) against the TM reader thread
        // that delivers on_commit/on_abort - both touch table_/catalog_/file_io_/open_.
        std::lock_guard<std::mutex> lk(mu_);
        if (table_ == nullptr) {
            return;  // closed concurrently
        }
        if (upsert_) {
            // Changelog: net per key in memory for this interval (last op wins). The actual
            // data + equality-delete files are written at the barrier (flush_upsert_), because
            // netting needs the whole interval (a delete then re-insert of a key must NOT also
            // delete the re-insert - they share a snapshot sequence number).
            for (std::size_t i = 0; i < batch.size(); ++i) {
                const auto& row = batch[i].value();
                std::string key;
                for (const auto& kc : eq_cols_) {
                    std::string v = part_value_string_(row, kc);
                    key += std::to_string(v.size());
                    key.push_back(':');
                    key.append(v);
                }
                NetEntry e;
                e.is_delete = clink::sql::is_delete_like(clink::sql::row_kind_of(row));
                e.row = row;
                e.event_time = batch[i].event_time();
                net_[std::move(key)] = std::move(e);  // last op for the key wins
            }
            return;
        }
        if (part_cols_.empty()) {
            write_group_("", {}, batch);  // unpartitioned: one data file per interval
            return;
        }
        // Partitioned: fan the batch out by its identity-partition tuple, one data file per
        // (partition, interval). Group row indices first, then build a sub-batch per group.
        std::map<std::string, std::vector<std::size_t>> groups;
        std::map<std::string, std::vector<std::string>> group_pv;
        for (std::size_t i = 0; i < batch.size(); ++i) {
            const auto& row = batch[i].value();
            std::vector<std::string> pv;
            pv.reserve(part_cols_.size());
            std::string key;
            for (const auto& pc : part_cols_) {
                std::string v = part_value_string_(row, pc);
                // Length-prefix each component so distinct tuples cannot alias on a
                // separator even when a string partition value contains control bytes.
                key += std::to_string(v.size());
                key.push_back(':');
                key.append(v);
                pv.push_back(std::move(v));
            }
            groups[key].push_back(i);
            group_pv.emplace(key, std::move(pv));
        }
        for (const auto& [key, idxs] : groups) {
            Batch<clink::sql::Row> sub;
            for (std::size_t i : idxs) {
                const auto& rec = batch[i];
                // Preserve the engine-prepended event_time column (single-arg emplace would
                // write null), so a partitioned table keeps the same event_time an
                // unpartitioned one would.
                if (rec.event_time()) {
                    sub.emplace(rec.value(), *rec.event_time());
                } else {
                    sub.emplace(rec.value());
                }
            }
            write_group_(key, group_pv[key], sub);
        }
    }

    // Barrier = 2PC phase 1 (PRE-COMMIT). With a state backend the interval's data files are
    // closed + recorded in state but NOT yet snapshotted - they become visible only when the
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

    // 2PC phase 2 (COMMIT): the checkpoint is globally durable, so snapshot the data files
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
        auto files = parse_staged_(*stored);
        commit_staged_(checkpoint_id, files, /*delete_data_on_commit_failure=*/false);
        state->erase(this->id(), state_key_(checkpoint_id));
    }

    // 2PC abort: the checkpoint did NOT complete globally, so drop the staged (unreferenced)
    // data files. The source replays the interval, which writes fresh data files.
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
        for (const auto& sf : parse_staged_(*stored)) {
            (void)file_io_->DeleteFile(sf.path);  // best-effort orphan cleanup
        }
        state->erase(this->id(), state_key_(checkpoint_id));
    }

    void close() override {
        // Hold mu_ across teardown so a late on_commit/on_abort on the TM thread either runs
        // fully before close (mutex) or sees the reset table_/catalog_/file_io_ and bails.
        std::lock_guard<std::mutex> lk(mu_);
        if (!dormant_ && table_ != nullptr) {
            if (two_phase_()) {
                // The interval since the last barrier was never staged/committed; drop its
                // partial data files (the source replays them). Committed + staged-pending
                // data is durable in the catalog / state, untouched here.
                abandon_current_();
            } else {
                commit_current_immediate_(0);  // flush the tail (at-least-once standalone)
            }
        }
        open_.clear();
        table_.reset();
        catalog_.reset();
        file_io_.reset();
    }

    std::string name() const override { return opts_.name; }

private:
    // An identity-partition source column: its name, its iceberg-schema field id, and the
    // Arrow type used to extract a value from a Row + rebuild the partition Literal.
    struct PartCol {
        std::string name;
        int32_t source_id{0};
        arrow::Type::type atype{arrow::Type::NA};
    };

    // An open per-partition writer for the current interval.
    struct OpenPart {
        std::unique_ptr<ice::Writer> writer;
        std::string data_path;
        std::int64_t records{0};
        std::vector<std::string> partition;  // string-encoded partition values (1 per PartCol)
    };

    // A closed, staged file awaiting (or undergoing) commit: a data file, or (upsert) an
    // equality-delete file.
    struct StagedFile {
        std::string path;
        std::int64_t records{0};
        std::int64_t size{0};
        std::vector<std::string> partition;      // string-encoded partition values (data files)
        bool is_delete{false};                   // true = equality-delete file (upsert)
        std::vector<std::int32_t> equality_ids;  // delete files: the PK field ids
    };

    // The netted last-known op for one key in the current interval (upsert mode).
    struct NetEntry {
        bool is_delete{false};
        clink::sql::Row row;                  // the row (insert) or the key-carrying row (delete)
        std::optional<EventTime> event_time;  // last op's event time (for the data file)
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

    // Resolve a list of column names to {name, iceberg source field id, arrow type}. ids in
    // ice_schema_ are assigned 1..N in column order (arrow_schema_to_ice), so the source field
    // id for the column at arrow index idx is idx + 1. Used for both partition + equality key.
    std::vector<PartCol> resolve_cols_(const std::vector<std::string>& names, const char* label) {
        std::vector<PartCol> out;
        for (const auto& col : names) {
            int idx = -1;
            for (int i = 0; i < schema_->num_fields(); ++i) {
                if (schema_->field(i)->name() == col) {
                    idx = i;
                    break;
                }
            }
            if (idx < 0) {
                throw std::runtime_error(opts_.name + ": " + label + " column '" + col +
                                         "' is not in the sink schema");
            }
            const auto atype = schema_->field(idx)->type()->id();
            if (!partition_type_supported_(atype)) {
                throw std::runtime_error(opts_.name + ": " + label + " column '" + col +
                                         "' has an unsupported type (int/long/bool/string only)");
            }
            out.push_back(PartCol{col, idx + 1, atype});
        }
        return out;
    }

    // Build the table's PartitionSpec from opts_.partition_by (identity transforms) and fill
    // part_cols_ for value extraction. Empty partition_by -> Unpartitioned. v1 supports only
    // IDENTITY (the partition value == the column value, so no transform-result-type concern).
    std::shared_ptr<ice::PartitionSpec> build_partition_spec_() {
        part_cols_ = resolve_cols_(opts_.partition_by, "partition_by");
        if (part_cols_.empty()) {
            return ice::PartitionSpec::Unpartitioned();
        }
        std::vector<ice::PartitionField> fields;
        int32_t field_id = ice::PartitionSpec::kLegacyPartitionDataIdStart;  // 1000
        for (const auto& pc : part_cols_) {
            fields.emplace_back(pc.source_id, field_id++, pc.name, ice::Transform::Identity());
        }
        return std::shared_ptr<ice::PartitionSpec>(
            unwrap(ice::PartitionSpec::Make(*ice_schema_,
                                            ice::PartitionSpec::kInitialSpecId,
                                            std::move(fields),
                                            /*allow_missing_fields=*/false),
                   "make partition spec")
                .release());
    }

    static bool partition_type_supported_(arrow::Type::type t) {
        switch (t) {
            // Exact, lossless string round-trip only. FLOAT/DOUBLE are excluded: their
            // textual form would be lossy, so the recorded partition value could diverge
            // from the data (and two near-equal doubles could collide into one file),
            // violating Iceberg's "all rows in a file share the partition value" contract.
            // Partitioning on a float is unusual anyway.
            case arrow::Type::INT64:
            case arrow::Type::INT32:
            case arrow::Type::BOOL:
            case arrow::Type::STRING:
            case arrow::Type::LARGE_STRING:
                return true;
            default:
                return false;
        }
    }

    // Extract a Row's partition value for one column as a canonical string (round-trips
    // through literal_from_string_). v1 requires the value present + non-null.
    std::string part_value_string_(const clink::sql::Row& row, const PartCol& pc) const {
        auto it = row.values.find(pc.name);
        if (it == row.values.end() || it->second.is_null()) {
            throw std::runtime_error(opts_.name + ": partition column '" + pc.name +
                                     "' is null/missing (v1 requires non-null identity "
                                     "partition values)");
        }
        const auto& jv = it->second;
        // The JsonValue accessors throw std::bad_variant_access on a type mismatch (e.g. a
        // string value in an int64 key column); rethrow as a clear, sink-scoped error.
        try {
            switch (pc.atype) {
                case arrow::Type::INT64:
                    return std::to_string(static_cast<std::int64_t>(jv.as_number()));
                case arrow::Type::INT32:
                    return std::to_string(static_cast<std::int32_t>(jv.as_number()));
                case arrow::Type::DOUBLE:
                    return std::to_string(jv.as_number());
                case arrow::Type::FLOAT:
                    return std::to_string(static_cast<float>(jv.as_number()));
                case arrow::Type::BOOL:
                    return jv.as_bool() ? "true" : "false";
                default:
                    return jv.as_string();  // STRING / LARGE_STRING
            }
        } catch (const std::exception&) {
            throw std::runtime_error(opts_.name + ": column '" + pc.name +
                                     "' value does not match its declared type");
        }
    }

    ice::Literal literal_from_string_(const std::string& s, const PartCol& pc) const {
        switch (pc.atype) {
            case arrow::Type::INT64:
                return ice::Literal::Long(std::stoll(s));
            case arrow::Type::INT32:
                return ice::Literal::Int(static_cast<std::int32_t>(std::stoll(s)));
            case arrow::Type::DOUBLE:
                return ice::Literal::Double(std::stod(s));
            case arrow::Type::FLOAT:
                return ice::Literal::Float(static_cast<float>(std::stod(s)));
            case arrow::Type::BOOL:
                return ice::Literal::Boolean(s == "true");
            default:
                return ice::Literal::String(s);  // STRING / LARGE_STRING
        }
    }

    // Write one partition group (or the whole batch when unpartitioned) into its open writer,
    // opening the writer lazily under the table's data/ dir.
    void write_group_(const std::string& key,
                      const std::vector<std::string>& partition,
                      const Batch<clink::sql::Row>& b) {
        if (b.empty()) {
            return;
        }
        OpenPart& op = open_[key];
        if (!op.writer) {
            op.data_path = std::string(table_->location()) + "/data/" + make_uuid() + ".parquet";
            op.partition = partition;
            op.records = 0;
            ice::WriterOptions wopts;
            wopts.path = op.data_path;
            wopts.schema = ice_schema_;
            wopts.io = file_io_;
            op.writer =
                unwrap(ice::WriterFactoryRegistry::Open(ice::FileFormatType::kParquet, wopts),
                       "open parquet writer");
        }
        auto rb = opts_.batcher.build(b);
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
        auto wst = op.writer->Write(&c_arr);
        if (c_arr.release != nullptr) {
            c_arr.release(&c_arr);
        }
        ensure_ok(wst, "writer Write");
        op.records += static_cast<std::int64_t>(b.size());
    }

    // Close all open per-partition writers, producing complete Parquet data files. In upsert
    // mode the interval was buffered in net_, so flush that instead.
    std::vector<StagedFile> finish_all_() {
        if (upsert_) {
            return flush_upsert_();
        }
        std::vector<StagedFile> out;
        for (auto& [key, op] : open_) {
            if (!op.writer) {
                continue;
            }
            ensure_ok(op.writer->Close(), "writer Close");
            StagedFile sf;
            sf.path = op.data_path;
            sf.records = op.records;
            sf.size = unwrap(op.writer->length(), "writer length");
            sf.partition = op.partition;
            out.push_back(std::move(sf));
        }
        open_.clear();
        return out;
    }

    // Upsert flush: from the netted interval, write ONE data file (the surviving inserts) and
    // ONE equality-delete file keyed on EVERY touched key. The equality delete (snapshot seq S)
    // removes any prior row for the key (seq < S); the new insert is in the same snapshot (seq
    // S, not deleted). So an insert upserts (delete-old + write-new) and a delete removes.
    std::vector<StagedFile> flush_upsert_() {
        std::vector<StagedFile> out;
        if (net_.empty()) {
            return out;
        }
        Batch<clink::sql::Row> inserts;
        std::vector<const clink::sql::Row*> key_rows;  // every touched key -> equality-delete
        for (auto& [k, e] : net_) {
            key_rows.push_back(&e.row);
            if (!e.is_delete) {
                if (e.event_time.has_value()) {
                    inserts.emplace(e.row, *e.event_time);
                } else {
                    inserts.emplace(e.row);
                }
            }
        }
        if (!inserts.empty()) {
            out.push_back(write_one_data_file_(inserts));
        }
        out.push_back(write_eq_delete_file_(key_rows));
        net_.clear();
        return out;
    }

    // Write a complete Parquet data file from a batch (one-shot, unpartitioned upsert).
    StagedFile write_one_data_file_(const Batch<clink::sql::Row>& b) {
        const std::string path =
            std::string(table_->location()) + "/data/" + make_uuid() + ".parquet";
        ice::WriterOptions wopts;
        wopts.path = path;
        wopts.schema = ice_schema_;
        wopts.io = file_io_;
        auto w = unwrap(ice::WriterFactoryRegistry::Open(ice::FileFormatType::kParquet, wopts),
                        "open parquet writer");
        auto rb = opts_.batcher.build(b);
        if (!rb) {
            throw std::runtime_error(opts_.name + ": batcher.build returned null");
        }
        ArrowArray c_arr{};
        if (auto st = arrow::ExportRecordBatch(*rb, &c_arr); !st.ok()) {
            throw std::runtime_error(opts_.name + ": ExportRecordBatch: " + st.ToString());
        }
        auto wst = w->Write(&c_arr);
        if (c_arr.release != nullptr) {
            c_arr.release(&c_arr);
        }
        ensure_ok(wst, "writer Write");
        ensure_ok(w->Close(), "writer Close");
        StagedFile sf;
        sf.path = path;
        sf.records = static_cast<std::int64_t>(b.size());
        sf.size = unwrap(w->length(), "writer length");
        return sf;
    }

    // Write an Iceberg v2 equality-delete file holding the key columns of every touched key.
    StagedFile write_eq_delete_file_(const std::vector<const clink::sql::Row*>& key_rows) {
        const std::string path =
            std::string(table_->location()) + "/data/" + make_uuid() + "-del.parquet";
        std::vector<std::int32_t> eq_ids;
        for (const auto& kc : eq_cols_) {
            eq_ids.push_back(kc.source_id);
        }
        ice::EqualityDeleteWriterOptions eo;
        eo.path = path;
        eo.schema = key_ice_schema_;
        eo.spec = ice::PartitionSpec::Unpartitioned();
        eo.io = file_io_;
        eo.equality_field_ids = eq_ids;
        auto w = unwrap(ice::EqualityDeleteWriter::Make(eo), "make equality-delete writer");
        auto rb = build_key_record_batch_(key_rows);
        ArrowArray c_arr{};
        if (auto st = arrow::ExportRecordBatch(*rb, &c_arr); !st.ok()) {
            throw std::runtime_error(opts_.name + ": ExportRecordBatch(delete): " + st.ToString());
        }
        auto wst = w->Write(&c_arr);
        if (c_arr.release != nullptr) {
            c_arr.release(&c_arr);
        }
        ensure_ok(wst, "eq-delete Write");
        ensure_ok(w->Close(), "eq-delete Close");
        StagedFile sf;
        sf.path = path;
        sf.records = static_cast<std::int64_t>(key_rows.size());
        sf.size = unwrap(w->Length(), "eq-delete length");
        sf.is_delete = true;
        sf.equality_ids = std::move(eq_ids);
        return sf;
    }

    // Build an Arrow RecordBatch with just the equality-key columns from the given rows. NO
    // event_time (the delete file contains only the key columns the equality_field_ids name).
    std::shared_ptr<arrow::RecordBatch> build_key_record_batch_(
        const std::vector<const clink::sql::Row*>& rows) {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (const auto& kc : eq_cols_) {
            const auto* afield = schema_->field(kc.source_id - 1).get();  // source_id = idx + 1
            fields.push_back(arrow::field(kc.name, afield->type(), /*nullable=*/false));
            arrays.push_back(build_key_array_(rows, kc));
        }
        return arrow::RecordBatch::Make(
            arrow::schema(fields), static_cast<std::int64_t>(rows.size()), arrays);
    }

    std::shared_ptr<arrow::Array> build_key_array_(const std::vector<const clink::sql::Row*>& rows,
                                                   const PartCol& kc) {
        auto must_finish = [&](arrow::ArrayBuilder& bld) {
            std::shared_ptr<arrow::Array> a;
            if (auto st = bld.Finish(&a); !st.ok()) {
                throw std::runtime_error(opts_.name + ": key array Finish: " + st.ToString());
            }
            return a;
        };
        auto val = [&](const clink::sql::Row* r) -> const cfg::JsonValue& {
            return r->values.at(kc.name);
        };
        switch (kc.atype) {
            case arrow::Type::INT64: {
                arrow::Int64Builder b;
                for (auto* r : rows)
                    (void)b.Append(static_cast<std::int64_t>(val(r).as_number()));
                return must_finish(b);
            }
            case arrow::Type::INT32: {
                arrow::Int32Builder b;
                for (auto* r : rows)
                    (void)b.Append(static_cast<std::int32_t>(val(r).as_number()));
                return must_finish(b);
            }
            case arrow::Type::BOOL: {
                arrow::BooleanBuilder b;
                for (auto* r : rows)
                    (void)b.Append(val(r).as_bool());
                return must_finish(b);
            }
            default: {  // STRING / LARGE_STRING
                arrow::StringBuilder b;
                for (auto* r : rows)
                    (void)b.Append(val(r).as_string());
                return must_finish(b);
            }
        }
    }

    // Close the data files + record them in state, keyed by checkpoint. No snapshot.
    void stage_current_(std::uint64_t ckpt) {
        auto files = finish_all_();
        if (files.empty()) {
            return;  // empty interval -> nothing to stage, no snapshot
        }
        std::string blob = serialize_staged_(files);
        state_backend_()->put(
            this->id(), state_key_(ckpt), std::string_view{blob.data(), blob.size()});
    }

    // Standalone (no 2PC): close + snapshot immediately.
    void commit_current_immediate_(std::uint64_t ckpt) {
        auto files = finish_all_();
        if (files.empty()) {
            return;
        }
        commit_staged_(ckpt, files, /*delete_data_on_commit_failure=*/true);
    }

    // Snapshot all staged data files for a checkpoint via one FastAppend, tagging the snapshot
    // with the checkpoint id so the commit is idempotent. The in-memory table is stale after a
    // commit, so reload it for the next append.
    void commit_staged_(std::uint64_t ckpt,
                        const std::vector<StagedFile>& files,
                        bool delete_data_on_commit_failure) {
        if (files.empty() || already_committed_(ckpt)) {
            return;  // idempotent: this checkpoint's snapshot already exists
        }
        // Build DataFiles, splitting data vs equality-delete (upsert).
        std::vector<std::shared_ptr<ice::DataFile>> data_files;
        std::vector<std::shared_ptr<ice::DataFile>> delete_files;
        std::int64_t total_records = 0;
        std::int64_t total_size = 0;
        for (const auto& sf : files) {
            auto df = std::make_shared<ice::DataFile>();
            df->file_path = sf.path;
            df->file_format = ice::FileFormatType::kParquet;
            df->record_count = sf.records;
            df->file_size_in_bytes = sf.size;
            df->partition_spec_id = ice::PartitionSpec::kInitialSpecId;  // 0
            if (sf.is_delete) {
                df->content = ice::DataFile::Content::kEqualityDeletes;
                df->equality_ids = sf.equality_ids;
                delete_files.push_back(std::move(df));
            } else {
                df->content = ice::DataFile::Content::kData;
                if (!part_cols_.empty()) {
                    std::vector<ice::Literal> lits;
                    lits.reserve(sf.partition.size());
                    for (std::size_t i = 0; i < part_cols_.size() && i < sf.partition.size(); ++i) {
                        lits.push_back(literal_from_string_(sf.partition[i], part_cols_[i]));
                    }
                    df->partition = ice::PartitionValues(std::move(lits));
                }
                data_files.push_back(std::move(df));
            }
            total_records += sf.records;
            total_size += sf.size;
        }

        // With equality-delete files (upsert) commit via a RowDelta so data + deletes land in
        // ONE snapshot; otherwise a plain FastAppend. Both tag the snapshot for idempotency.
        ice::Status cst = ice::Status{};
        if (!delete_files.empty()) {
            auto rd = unwrap(RowDelta::Make(opts_.table, table_), "make row delta");
            for (auto& d : data_files) {
                ensure_ok(rd->AddData(d), "RowDelta AddData");
            }
            for (auto& d : delete_files) {
                ensure_ok(rd->AddDelete(d), "RowDelta AddDelete");
            }
            rd->Set(kCheckpointProp, std::to_string(ckpt));
            cst = rd->Commit();
        } else {
            auto fa = unwrap(table_->NewFastAppend(), "new fast append");
            for (auto& d : data_files) {
                fa->AppendFile(d);
            }
            fa->Set(kCheckpointProp, std::to_string(ckpt));
            cst = fa->Commit();
        }
        if (!cst.has_value()) {
            if (delete_data_on_commit_failure) {
                // Standalone: no state to retry from, so do not strand the orphan data files.
                // (In 2PC, leave them: state still references them and recovery retries.)
                for (const auto& sf : files) {
                    (void)file_io_->DeleteFile(sf.path);
                }
            }
            throw std::runtime_error("iceberg: commit snapshot: " + cst.error().message);
        }
        table_ = unwrap(catalog_->LoadTable(id_), "reload table after commit");

        clink::metrics::connector::records_out_inc(kLabel,
                                                   static_cast<std::uint64_t>(total_records));
        clink::metrics::connector::bytes_out_inc(kLabel, static_cast<std::uint64_t>(total_size));
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

    // Drop all in-progress (un-staged) work: the upsert net buffer (nothing written yet) and
    // any open per-partition writers' partial data files.
    void abandon_current_() {
        net_.clear();
        for (auto& [key, op] : open_) {
            if (op.writer) {
                (void)op.writer->Close();
                op.writer.reset();
            }
            if (!op.data_path.empty() && file_io_ != nullptr) {
                (void)file_io_->DeleteFile(op.data_path);
            }
        }
        open_.clear();
    }

    // Staged state = a JSON array of {path, records, size, partition:[str,...], is_delete,
    // equality_ids:[int,...]}. JSON so an arbitrary string partition value (or a path) cannot
    // break a delimiter scheme.
    std::string serialize_staged_(const std::vector<StagedFile>& files) const {
        cfg::JsonArray arr;
        for (const auto& sf : files) {
            cfg::JsonObject o;
            o["path"] = cfg::JsonValue{sf.path};
            o["records"] = cfg::JsonValue{sf.records};
            o["size"] = cfg::JsonValue{sf.size};
            cfg::JsonArray pv;
            for (const auto& s : sf.partition) {
                pv.push_back(cfg::JsonValue{s});
            }
            o["partition"] = cfg::JsonValue{std::move(pv)};
            if (sf.is_delete) {
                o["is_delete"] = cfg::JsonValue{true};
                cfg::JsonArray eq;
                for (auto fid : sf.equality_ids) {
                    eq.push_back(cfg::JsonValue{static_cast<std::int64_t>(fid)});
                }
                o["equality_ids"] = cfg::JsonValue{std::move(eq)};
            }
            arr.push_back(cfg::JsonValue{std::move(o)});
        }
        return cfg::JsonValue{std::move(arr)}.serialize(0);
    }

    std::vector<StagedFile> parse_staged_(const std::vector<std::byte>& blob) const {
        std::string s(reinterpret_cast<const char*>(blob.data()), blob.size());
        std::vector<StagedFile> out;
        try {
            auto v = cfg::parse(s);
            for (const auto& e : v.as_array()) {
                const auto& o = e.as_object();
                StagedFile sf;
                sf.path = o.at("path").as_string();
                sf.records = static_cast<std::int64_t>(o.at("records").as_number());
                sf.size = static_cast<std::int64_t>(o.at("size").as_number());
                if (auto pit = o.find("partition"); pit != o.end()) {
                    for (const auto& pv : pit->second.as_array()) {
                        sf.partition.push_back(pv.as_string());
                    }
                }
                if (auto dit = o.find("is_delete"); dit != o.end()) {
                    sf.is_delete = dit->second.as_bool();
                }
                if (auto eit = o.find("equality_ids"); eit != o.end()) {
                    for (const auto& fid : eit->second.as_array()) {
                        sf.equality_ids.push_back(static_cast<std::int32_t>(fid.as_number()));
                    }
                }
                out.push_back(std::move(sf));
            }
        } catch (const std::exception& e) {
            throw std::runtime_error(opts_.name + ": corrupt staged-commit state (" + e.what() +
                                     "): " + s);
        }
        return out;
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
            auto files = parse_staged_(blob);
            commit_staged_(ckpt, files, /*delete_data_on_commit_failure=*/false);
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
    std::vector<PartCol> part_cols_;  // empty = unpartitioned
    std::shared_ptr<ice::FileIO> file_io_;
    std::shared_ptr<ice::Catalog> catalog_;  // SqlCatalog (sqlite) or RestCatalog
    ice::TableIdentifier id_;
    std::shared_ptr<ice::Table> table_;
    std::map<std::string, OpenPart> open_;  // open per-partition writers this interval (append)
    // Upsert mode (equality_key non-empty): changelog netted by key, flushed at the barrier.
    bool upsert_{false};
    std::vector<PartCol> eq_cols_;                 // the equality-key columns
    std::shared_ptr<ice::Schema> key_ice_schema_;  // key-columns-only schema for delete files
    std::map<std::string, NetEntry> net_;          // netted ops for the current interval
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

IcebergStateExportResult export_state_iceberg(const clink::state_processor::StateEntries& entries,
                                              IcebergStateExportOptions options) {
    if (options.warehouse.empty()) {
        throw std::runtime_error("iceberg state export: 'warehouse' is required");
    }
    if (options.table.empty()) {
        throw std::runtime_error("iceberg state export: 'table' is required");
    }
    ensure_registered();

    // The decoded entry schema (see state_export.hpp). op_id is bit-cast
    // to signed long: Iceberg has no unsigned types, and the cast is
    // exact and reversible.
    auto arrow_schema = arrow::schema({
        arrow::field("op_id", arrow::int64(), /*nullable=*/false),
        arrow::field("key_group", arrow::int32(), /*nullable=*/false),
        arrow::field("slot", arrow::utf8(), /*nullable=*/false),
        arrow::field("user_key", arrow::binary(), /*nullable=*/false),
        arrow::field("value_bytes", arrow::binary(), /*nullable=*/false),
    });
    auto ice_schema = arrow_schema_to_ice(*arrow_schema);

    CatalogTargetSpec target;
    target.warehouse = options.warehouse;
    target.table = options.table;
    target.catalog_uri = options.catalog_uri;
    target.rest_auth_token = options.rest_auth_token;
    target.name = "iceberg state export";
    target.namespace_levels = options.namespace_levels;
    target.file_io_props = options.file_io_props;
    auto opened = open_catalog_table(target, ice_schema, ice::PartitionSpec::Unpartitioned());

    // Appending to an existing table: its column names must match ours,
    // else refuse rather than commit a file the table schema disagrees with.
    {
        const auto existing = unwrap(opened.table->schema(), "table schema");
        const auto& fields = existing->fields();
        bool ok = fields.size() == 5;
        static constexpr const char* kNames[5] = {
            "op_id", "key_group", "slot", "user_key", "value_bytes"};
        for (std::size_t i = 0; ok && i < fields.size(); ++i) {
            ok = fields[i].name() == kNames[i];
        }
        if (!ok) {
            throw std::runtime_error("iceberg state export: table '" + options.table +
                                     "' exists with a different schema; export into a fresh table");
        }
    }

    // One Parquet data file, written in bounded chunks.
    const std::string data_path =
        std::string(opened.table->location()) + "/data/" + make_uuid() + ".parquet";
    ice::WriterOptions wopts;
    wopts.path = data_path;
    wopts.schema = ice_schema;
    wopts.io = opened.file_io;
    auto writer = unwrap(ice::WriterFactoryRegistry::Open(ice::FileFormatType::kParquet, wopts),
                         "open parquet writer");

    constexpr std::size_t kChunkRows = 64 * 1024;
    std::int64_t total_rows = 0;
    arrow::Int64Builder op_b;
    arrow::Int32Builder kg_b;
    arrow::StringBuilder slot_b;
    arrow::BinaryBuilder key_b;
    arrow::BinaryBuilder val_b;
    std::size_t pending = 0;

    auto flush_chunk = [&]() {
        if (pending == 0) {
            return;
        }
        std::shared_ptr<arrow::Array> op_arr, kg_arr, slot_arr, key_arr, val_arr;
        auto fin = [&](auto& b, std::shared_ptr<arrow::Array>& out, const char* what) {
            if (auto st = b.Finish(&out); !st.ok()) {
                throw std::runtime_error(std::string("iceberg state export: ") + what + ": " +
                                         st.ToString());
            }
        };
        fin(op_b, op_arr, "finish op_id");
        fin(kg_b, kg_arr, "finish key_group");
        fin(slot_b, slot_arr, "finish slot");
        fin(key_b, key_arr, "finish user_key");
        fin(val_b, val_arr, "finish value_bytes");
        auto rb = arrow::RecordBatch::Make(arrow_schema,
                                           static_cast<std::int64_t>(pending),
                                           {op_arr, kg_arr, slot_arr, key_arr, val_arr});
        ArrowArray c_arr{};
        if (auto st = arrow::ExportRecordBatch(*rb, &c_arr); !st.ok()) {
            throw std::runtime_error("iceberg state export: ExportRecordBatch: " + st.ToString());
        }
        // Writer::Write MOVES c_arr on success; on failure ownership stays
        // with us (see write_group_ above for the full contract).
        auto wst = writer->Write(&c_arr);
        if (c_arr.release != nullptr) {
            c_arr.release(&c_arr);
        }
        ensure_ok(wst, "writer Write");
        total_rows += static_cast<std::int64_t>(pending);
        pending = 0;
    };

    for (const auto& [op, slots] : entries) {
        for (const auto& [slot, slot_entries] : slots) {
            for (const auto& [user_key, entry] : slot_entries) {
                (void)op_b.Append(static_cast<std::int64_t>(op.value()));
                (void)kg_b.Append(static_cast<std::int32_t>(entry.key_group));
                (void)slot_b.Append(slot);
                (void)key_b.Append(reinterpret_cast<const uint8_t*>(user_key.data()),
                                   static_cast<int32_t>(user_key.size()));
                (void)val_b.Append(reinterpret_cast<const uint8_t*>(entry.value.data()),
                                   static_cast<int32_t>(entry.value.size()));
                if (++pending >= kChunkRows) {
                    flush_chunk();
                }
            }
        }
    }
    flush_chunk();
    ensure_ok(writer->Close(), "writer Close");
    const auto file_size = unwrap(writer->length(), "writer length");

    // One snapshot for the whole export, tagged for provenance.
    auto df = std::make_shared<ice::DataFile>();
    df->file_path = data_path;
    df->file_format = ice::FileFormatType::kParquet;
    df->record_count = total_rows;
    df->file_size_in_bytes = file_size;
    df->partition_spec_id = ice::PartitionSpec::kInitialSpecId;
    df->content = ice::DataFile::Content::kData;
    auto fa = unwrap(opened.table->NewFastAppend(), "new fast append");
    fa->AppendFile(df);
    fa->Set("clink.state-export", "true");
    auto cst = fa->Commit();
    if (!cst.has_value()) {
        (void)opened.file_io->DeleteFile(data_path);  // do not strand the orphan
        throw std::runtime_error("iceberg state export: commit snapshot: " + cst.error().message);
    }

    IcebergStateExportResult result;
    result.table_location = std::string(opened.table->location());
    result.rows = total_rows;
    return result;
}

// ---------------------------------------------------------------------------
// IcebergRowSource - the bounded snapshot-scan source (iceberg_row_source.hpp)
// ---------------------------------------------------------------------------

namespace {

class IcebergRowSource final : public Source<clink::sql::Row> {
public:
    explicit IcebergRowSource(IcebergRowSourceOptions o) : o_(std::move(o)) {
        if (o_.warehouse.empty()) {
            throw std::runtime_error(o_.name + ": 'warehouse' (or path) is required");
        }
        if (o_.table.empty()) {
            throw std::runtime_error(o_.name + ": 'table' is required");
        }
        if (o_.columns.empty()) {
            throw std::runtime_error(o_.name + ": declared columns are required");
        }
        batcher_ = clink::sql::make_row_columnar_arrow_batcher(o_.columns);
    }

    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    void open() override {
        ensure_registered();
        CatalogTargetSpec t;
        t.warehouse = o_.warehouse;
        t.catalog_uri = o_.catalog_uri;
        t.rest_auth_token = o_.rest_auth_token;
        t.name = o_.name;
        t.namespace_levels = o_.namespace_levels;
        t.table = o_.table;
        t.file_io_props = o_.file_io_props;
        opened_ = open_catalog_table(t, /*ice_schema=*/nullptr, /*spec=*/nullptr);

        // Pin the snapshot: a fresh run pins the CURRENT snapshot; a restore
        // re-pins the checkpointed one, so replay plans the identical task
        // list even if the table gained snapshots meanwhile.
        if (snapshot_id_ == 0) {
            auto snap = opened_.table->current_snapshot();
            if (!snap.has_value() || !snap.value()) {
                tasks_.clear();  // empty table (no snapshot yet): nothing to read
                return;
            }
            snapshot_id_ = snap.value()->snapshot_id;
        }

        std::vector<std::string> names;
        names.reserve(o_.columns.size());
        for (const auto& c : o_.columns) {
            names.push_back(c.name);
        }
        auto builder = unwrap(opened_.table->NewScan(), "new scan");
        builder->Select(names);
        builder->UseSnapshot(snapshot_id_);
        auto scan = unwrap(builder->Build(), "build scan");
        tasks_ = unwrap(scan->PlanFiles(), "plan files");
        for (const auto& task : tasks_) {
            if (!task->delete_files().empty()) {
                throw std::runtime_error(
                    o_.name + ": table '" + o_.table +
                    "' has delete files in the scanned snapshot; v1 reads append-only "
                    "snapshots (compact the table first)");
            }
        }

        // The projection schema for the data-file readers: the TABLE's own
        // fields (real field ids - Parquet projection resolves by id)
        // filtered to the selected names.
        auto full = unwrap(opened_.table->schema(), "table schema");
        std::vector<ice::SchemaField> proj_fields;
        proj_fields.reserve(names.size());
        for (const auto& name : names) {
            bool found = false;
            for (const auto& f : full->fields()) {
                if (f.name() == name) {
                    proj_fields.push_back(f);
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw std::runtime_error(o_.name + ": table '" + o_.table + "' has no column '" +
                                         name + "'");
            }
        }
        proj_schema_ = std::make_shared<ice::Schema>(std::move(proj_fields), full->schema_id());

        // Resume: skip whole tasks, then skip batches inside the first open
        // task (open_reader_ consumes skip_batches_).
        skip_batches_ = batch_i_;
        batch_i_ = 0;
    }

    bool produce(Emitter<clink::sql::Row>& out) override {
        while (true) {
            if (this->cancelled()) {
                return false;
            }
            if (!reader_) {
                if (task_i_ >= tasks_.size()) {
                    return false;  // scan exhausted
                }
                open_reader_();
                continue;
            }
            auto next = unwrap(reader_->Next(), "read data file");
            if (!next.has_value()) {
                (void)reader_->Close();
                reader_.reset();
                ++task_i_;
                batch_i_ = 0;
                continue;
            }
            ArrowArray arr = std::move(*next);
            auto rb_r = arrow::ImportRecordBatch(&arr, reader_schema_);
            if (!rb_r.ok()) {
                throw std::runtime_error(o_.name + ": import batch: " + rb_r.status().ToString());
            }
            ++batch_i_;
            auto batch = emit_shape_(*rb_r);
            auto parsed = batcher_.parse(*batch);
            if (!parsed.has_value()) {
                throw std::runtime_error(o_.name + ": row batcher rejected a data-file batch");
            }
            if (!parsed->empty()) {
                out.emit_data(std::move(*parsed));
            }
            return true;
        }
    }

    void close() override {
        if (reader_) {
            (void)reader_->Close();
            reader_.reset();
        }
        opened_ = {};
    }

    // Exactly-once replay position: (snapshot id, task index, batch index).
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId /*ckpt*/) override {
        std::array<std::byte, 24> bytes{};
        auto put = [&](std::size_t at, std::uint64_t v) {
            for (int i = 0; i < 8; ++i) {
                bytes[at + static_cast<std::size_t>(i)] =
                    static_cast<std::byte>((v >> (i * 8)) & 0xFF);
            }
        };
        put(0, static_cast<std::uint64_t>(snapshot_id_));
        put(8, task_i_);
        put(16, batch_i_);
        backend.put_operator_state(
            op_id,
            StateBackend::KeyView{kOffsetKey_, std::strlen(kOffsetKey_)},
            StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
    }

    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        auto v = backend.get_operator_state(
            op_id, StateBackend::KeyView{kOffsetKey_, std::strlen(kOffsetKey_)});
        if (!v.has_value() || v->size() < 24) {
            return false;
        }
        auto get = [&](std::size_t at) {
            std::uint64_t u = 0;
            for (int i = 0; i < 8; ++i) {
                u |= static_cast<std::uint64_t>(static_cast<std::uint8_t>((*v)[at + i])) << (i * 8);
            }
            return u;
        };
        snapshot_id_ = static_cast<std::int64_t>(get(0));
        task_i_ = get(8);
        batch_i_ = get(16);
        return true;
    }

    std::string name() const override { return o_.name; }

private:
    void open_reader_() {
        const auto& task = *tasks_[task_i_];
        const auto& df = *task.data_file();
        ice::ReaderOptions ro;
        ro.path = df.file_path;
        ro.io = opened_.file_io;
        ro.projection = proj_schema_;
        reader_ = unwrap(ice::ReaderFactoryRegistry::Open(df.file_format, ro), "open data file");
        auto cschema = unwrap(reader_->Schema(), "data-file schema");
        auto imported = arrow::ImportSchema(&cschema);
        if (!imported.ok()) {
            throw std::runtime_error(o_.name + ": import schema: " + imported.status().ToString());
        }
        reader_schema_ = *imported;
        // Per-task column mapping (name-resolved) + type check against the
        // declared columns, so a mismatch errors clearly at the first file.
        col_map_.clear();
        col_map_.reserve(o_.columns.size());
        for (const auto& c : o_.columns) {
            const int idx = reader_schema_->GetFieldIndex(c.name);
            if (idx < 0) {
                throw std::runtime_error(o_.name + ": data file " + df.file_path +
                                         " has no column '" + c.name + "'");
            }
            if (!reader_schema_->field(idx)->type()->Equals(*c.type)) {
                throw std::runtime_error(o_.name + ": column '" + c.name + "' is " +
                                         reader_schema_->field(idx)->type()->ToString() +
                                         " in the table but " + c.type->ToString() +
                                         " in the declared schema");
            }
            col_map_.push_back(idx);
        }
        // Resume mid-task: burn the already-emitted batches.
        for (std::uint64_t skipped = 0; skipped < skip_batches_; ++skipped) {
            auto next = unwrap(reader_->Next(), "replay skip");
            if (!next.has_value()) {
                break;  // file shorter than the restored offset
            }
            ArrowArray arr = std::move(*next);
            arr.release(&arr);  // discard without importing
        }
        batch_i_ = skip_batches_;
        skip_batches_ = 0;
    }

    // Shape a data-file batch for the row batcher: prepend the engine's
    // null event-time column and order the value columns as declared.
    std::shared_ptr<arrow::RecordBatch> emit_shape_(
        const std::shared_ptr<arrow::RecordBatch>& rb) const {
        arrow::ArrayVector columns;
        columns.reserve(o_.columns.size() + 1);
        auto evt = arrow::MakeArrayOfNull(arrow::int64(), rb->num_rows());
        if (!evt.ok()) {
            throw std::runtime_error(o_.name + ": event-time column: " + evt.status().ToString());
        }
        columns.push_back(*evt);
        for (const int idx : col_map_) {
            columns.push_back(rb->column(idx));
        }
        return arrow::RecordBatch::Make(batcher_.schema(), rb->num_rows(), std::move(columns));
    }

    static constexpr const char* kOffsetKey_ = "iceberg_source.cursor";

    IcebergRowSourceOptions o_;
    ArrowBatcher<clink::sql::Row> batcher_;
    OpenedCatalogTable opened_;
    std::shared_ptr<ice::Schema> proj_schema_;
    std::vector<std::shared_ptr<ice::FileScanTask>> tasks_;
    std::unique_ptr<ice::Reader> reader_;
    std::shared_ptr<arrow::Schema> reader_schema_;
    std::vector<int> col_map_;
    std::int64_t snapshot_id_{0};
    std::uint64_t task_i_{0};
    std::uint64_t batch_i_{0};
    std::uint64_t skip_batches_{0};
};

}  // namespace

std::shared_ptr<Source<clink::sql::Row>> make_iceberg_row_source(IcebergRowSourceOptions opts) {
    return std::make_shared<IcebergRowSource>(std::move(opts));
}

}  // namespace clink::iceberg
