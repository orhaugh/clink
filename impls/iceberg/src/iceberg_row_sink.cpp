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
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/c/bridge.h>  // ExportRecordBatch (defines the shared ArrowArray first)

#include "clink/metrics/connector_metrics.hpp"

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
        ensure_registered();
        schema_ = opts_.batcher.schema();
        ice_schema_ = arrow_schema_to_ice(*schema_);

        // Local FileIO for v1 (S3 FileIO is a follow-on). MakeLocalFileIO returns a
        // unique_ptr; the catalog + writer want a shared_ptr.
        file_io_ = std::shared_ptr<ice::FileIO>(ice::arrow::MakeLocalFileIO().release());

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

        // Drive the table location explicitly (warehouse/<ns levels>/<table>) so we can
        // pre-create the metadata/ + data/ dirs: iceberg-cpp's local Arrow FileIO writes
        // through OpenOutputStream, which does NOT create parent directories, so the very
        // first metadata write in CreateTable would fail on a fresh warehouse.
        std::string location = opts_.warehouse;
        for (const auto& level : opts_.namespace_levels) {
            location += "/" + level;
        }
        location += "/" + opts_.table;
        std::error_code ec;
        std::filesystem::create_directories(location + "/metadata", ec);
        std::filesystem::create_directories(location + "/data", ec);

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

        // For an already-existing table the loaded location may differ from ours; make
        // sure its data/ + metadata/ dirs exist before the writer + snapshot commits.
        std::filesystem::create_directories(std::string(table_->location()) + "/metadata", ec);
        std::filesystem::create_directories(std::string(table_->location()) + "/data", ec);
    }

    void on_data(const Batch<clink::sql::Row>& batch) override {
        if (dormant_ || batch.empty()) {
            return;
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

    void on_barrier(CheckpointBarrier /*b*/) override { commit_current_(); }

    void close() override {
        if (!dormant_) {
            commit_current_();
        }
        writer_.reset();
        table_.reset();
        catalog_.reset();
        file_io_.reset();
    }

    std::string name() const override { return opts_.name; }

private:
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

    void commit_current_() {
        if (!writer_) {
            return;  // no rows this interval
        }
        ensure_ok(writer_->Close(), "writer Close");
        const std::int64_t size = unwrap(writer_->length(), "writer length");
        writer_.reset();

        auto df = std::make_shared<ice::DataFile>();
        df->content = ice::DataFile::Content::kData;
        df->file_path = cur_data_path_;
        df->file_format = ice::FileFormatType::kParquet;
        df->record_count = cur_records_;
        df->file_size_in_bytes = size;
        df->partition_spec_id = 0;  // the table is created unpartitioned (spec id 0)

        // FastAppend is created off the (current) table, then committed; the in-memory
        // table is stale after commit, so reload it from the catalog for the next append.
        auto fa = unwrap(table_->NewFastAppend(), "new fast append");
        fa->AppendFile(df);
        if (auto cst = fa->Commit(); !cst.has_value()) {
            // The data file is written but no snapshot references it. Best-effort delete so
            // a repeatedly-failing commit does not strand orphan files in the warehouse (a
            // replay writes a fresh uuid file, so the orphan would otherwise never be GC'd).
            (void)file_io_->DeleteFile(cur_data_path_);
            throw std::runtime_error("iceberg: commit snapshot: " + cst.error().message);
        }
        table_ = unwrap(catalog_->LoadTable(id_), "reload table after commit");

        clink::metrics::connector::records_out_inc(kLabel,
                                                   static_cast<std::uint64_t>(cur_records_));
        clink::metrics::connector::bytes_out_inc(kLabel, static_cast<std::uint64_t>(size));
        cur_data_path_.clear();
        cur_records_ = 0;
    }

    IcebergRowSinkOptions opts_;
    bool dormant_{false};
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
    return std::make_shared<IcebergRowSink>(std::move(opts));
}

}  // namespace clink::iceberg
