#pragma once

// Delta Lake SINK for the SQL Row channel. Reuses the schema-driven row columnar
// Arrow batcher (make_row_columnar_arrow_batcher) to write typed Parquet DATA
// FILES - exactly like parquet_row_sink - and adds the Delta transaction-log layer
// (clink::delta, delta_log.hpp) so the result is a real Delta table that delta-rs /
// DuckDB / Spark can read. Data files + the log are written through an
// arrow::fs::FileSystem resolved from the table-root URI, so a local path and an
// s3:// URI both work. (S3 region + credentials come from the URI / the AWS env
// chain; there is no endpoint_override yet, so MinIO / localstack targeting is a v2
// item - real S3 works today.)
//
// CADENCE: one Delta commit per checkpoint interval. During the interval the sink
// writes the buffered Rows into one Parquet data file (opened lazily on the first
// record, written one row group per on_data); on the checkpoint barrier it closes
// that file and appends a new log version (_delta_log/<N>.json) whose `add` action
// references it. The next version number is read by listing _delta_log on open()
// (so the sink appends to an existing table).
//
// DELIVERY = AT-LEAST-ONCE, append-only, SINGLE-WRITER. The commit writes
// _delta_log/<N>.json with N = (max existing version)+1; correct because the sink
// is the sole writer of the table (only subtask 0 is active, others dormant). A
// crash between the data-file write and the global checkpoint replays the interval
// (the rows are re-written as a new version - duplicate rows, absorbed by a
// downstream dedup or a later compaction). NOT supported in v1: UPDATE / DELETE /
// MERGE / compaction (need delete vectors / the kernel), partitioning, multi-writer
// commits (would need S3 If-None-Match conditional PUT), and exactly-once. The
// table schema includes the leading event_time column the row batcher emits.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include "clink/connectors/arrow_s3_lifecycle.hpp"
#include "clink/connectors/delta_log.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"

namespace clink::sql {

struct DeltaRowSinkOptions {
    std::string table_root;        // local path or s3:// URI of the table
    ArrowBatcher<Row> batcher;     // schema-driven typed Arrow batcher (row columnar)
    std::uint32_t subtask_idx{0};  // only subtask 0 writes (single-writer table)
    std::string name{"delta_row_sink"};
};

class DeltaRowSink final : public Sink<Row> {
public:
    explicit DeltaRowSink(DeltaRowSinkOptions opts)
        : opts_(std::move(opts)), dormant_(opts_.subtask_idx != 0) {
        if (opts_.table_root.empty()) {
            throw std::runtime_error(opts_.name + ": 'table_root' is required");
        }
        if (!opts_.batcher.schema || !opts_.batcher.build) {
            throw std::runtime_error(opts_.name + ": batcher must have schema + build");
        }
    }

    void open() override {
        if (dormant_) {
            return;
        }
        schema_ = opts_.batcher.schema();
        resolve_filesystem_();
        // Local FS needs the dirs to exist before OpenOutputStream; an object store
        // (S3) has no real directories (prefixes are implicit, created by the object
        // writes), and CreateDir there may even attempt bucket creation the
        // principal cannot do - so make it best-effort on object stores.
        create_dir_(base_path_, "create table dir");
        create_dir_(log_dir_(), "create _delta_log");
        version_ = next_version_();
        table_id_ = make_uuid_();
    }

    void on_data(const Batch<Row>& batch) override {
        if (dormant_ || batch.empty()) {
            return;
        }
        if (!writer_) {
            open_data_file_();
        }
        auto rb = opts_.batcher.build(batch);
        if (!rb) {
            throw std::runtime_error(opts_.name + ": batcher.build returned null");
        }
        if (auto s = writer_->WriteRecordBatch(*rb); !s.ok()) {
            throw std::runtime_error(opts_.name + ": WriteRecordBatch: " + s.ToString());
        }
        cur_records_ += static_cast<std::int64_t>(batch.size());
    }

    void on_barrier(CheckpointBarrier /*b*/) override { commit_current_(); }

    void close() override {
        if (!dormant_) {
            commit_current_();  // flush the tail interval
        }
        fs_.reset();
    }

    std::string name() const override { return opts_.name; }

private:
    std::string log_dir_() const { return base_path_ + "/_delta_log"; }

    static std::int64_t now_ms_() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    template <typename T>
    static T value_or_throw_(arrow::Result<T> r, const std::string& what) {
        if (!r.ok()) {
            throw std::runtime_error("delta: " + what + ": " + r.status().ToString());
        }
        return std::move(r).ValueUnsafe();
    }
    static void ensure_ok_(const arrow::Status& s, const std::string& what) {
        if (!s.ok()) {
            throw std::runtime_error("delta: " + what + ": " + s.ToString());
        }
    }

    void resolve_filesystem_() {
        if (opts_.table_root.find("://") != std::string::npos) {
            object_store_ = true;
            if (opts_.table_root.rfind("s3://", 0) == 0) {
                clink::connectors::ensure_arrow_s3_initialised();
            }
            fs_ = value_or_throw_(arrow::fs::FileSystemFromUri(opts_.table_root, &base_path_),
                                  "FileSystemFromUri");
        } else {
            fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
            base_path_ = std::filesystem::absolute(opts_.table_root).string();
        }
    }

    // Create a directory: strict on a local FS (the writes need it), best-effort on
    // an object store (prefixes are implicit; a bucket-create attempt may be denied).
    void create_dir_(const std::string& dir, const std::string& what) {
        auto s = fs_->CreateDir(dir, /*recursive=*/true);
        if (!object_store_) {
            ensure_ok_(s, what);
        }
    }

    // Next version = (max existing NNNNN.json in _delta_log) + 1, or 0 for a new table.
    std::int64_t next_version_() {
        arrow::fs::FileSelector sel;
        sel.base_dir = log_dir_();
        sel.allow_not_found = true;
        sel.recursive = false;
        auto infos = fs_->GetFileInfo(sel);
        std::int64_t maxv = -1;
        if (infos.ok()) {
            for (const auto& info : *infos) {
                if (info.type() != arrow::fs::FileType::File) {
                    continue;
                }
                const std::string fn = info.base_name();
                if (fn.size() == 25 && fn.compare(20, 5, ".json") == 0) {  // 20 digits + ".json"
                    bool all_digits = true;
                    for (std::size_t i = 0; i < 20; ++i) {
                        if (fn[i] < '0' || fn[i] > '9') {
                            all_digits = false;
                            break;
                        }
                    }
                    if (all_digits) {
                        maxv =
                            std::max(maxv, static_cast<std::int64_t>(std::stoll(fn.substr(0, 20))));
                    }
                }
            }
        }
        return maxv + 1;
    }

    void open_data_file_() {
        std::string fn = "part-" + clink::delta::version_filename(version_);
        fn = fn.substr(0, fn.size() - 5);  // strip ".json" -> "part-<20digits>"
        fn += "-" + std::to_string(opts_.subtask_idx) + "-" + std::to_string(file_counter_++) +
              ".parquet";
        cur_data_rel_ = fn;
        const std::string full = base_path_ + "/" + fn;
        out_ = value_or_throw_(fs_->OpenOutputStream(full), "open data file");
        auto props =
            parquet::WriterProperties::Builder().compression(parquet::Compression::ZSTD)->build();
        auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
        auto w = parquet::arrow::FileWriter::Open(
            *schema_, arrow::default_memory_pool(), out_, props, arrow_props);
        if (!w.ok()) {
            throw std::runtime_error(opts_.name +
                                     ": open parquet writer: " + w.status().ToString());
        }
        writer_ = std::move(*w);
        cur_records_ = 0;
    }

    void commit_current_() {
        if (!writer_) {
            return;  // no rows this interval: nothing to commit
        }
        ensure_ok_(writer_->Close(), "close parquet writer");
        writer_.reset();
        ensure_ok_(out_->Close(), "close data stream");
        out_.reset();

        const std::string full = base_path_ + "/" + cur_data_rel_;
        const auto info = value_or_throw_(fs_->GetFileInfo(full), "stat data file");
        const std::int64_t size = info.size();
        const std::int64_t ts = now_ms_();

        std::string body;
        if (version_ == 0) {
            body += clink::delta::protocol_action_json() + "\n";
            body += clink::delta::metadata_action_json(
                        table_id_, clink::delta::arrow_schema_to_delta_schema_json(*schema_), ts) +
                    "\n";
        }
        body += clink::delta::commit_info_action_json(ts, "WRITE") + "\n";
        body += clink::delta::add_action_json(cur_data_rel_, size, ts, cur_records_) + "\n";

        const std::string log_path = log_dir_() + "/" + clink::delta::version_filename(version_);
        auto log_out = value_or_throw_(fs_->OpenOutputStream(log_path), "open log file");
        ensure_ok_(log_out->Write(body.data(), static_cast<std::int64_t>(body.size())),
                   "write log");
        ensure_ok_(log_out->Close(), "close log");

        ++version_;
        cur_data_rel_.clear();
        cur_records_ = 0;
    }

    static std::string make_uuid_() {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<std::uint64_t> dist;
        std::uint64_t hi = dist(gen);
        std::uint64_t lo = dist(gen);
        hi = (hi & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL;  // version 4
        lo = (lo & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;  // variant
        static const char* h = "0123456789abcdef";
        std::string b(32, '0');
        for (int i = 0; i < 16; ++i) {
            std::uint64_t byte = (i < 8) ? (hi >> (8 * (7 - i))) : (lo >> (8 * (15 - i)));
            b[2 * i] = h[(byte >> 4) & 0xF];
            b[2 * i + 1] = h[byte & 0xF];
        }
        return b.substr(0, 8) + "-" + b.substr(8, 4) + "-" + b.substr(12, 4) + "-" +
               b.substr(16, 4) + "-" + b.substr(20, 12);
    }

    DeltaRowSinkOptions opts_;
    bool dormant_{false};
    bool object_store_{false};  // table_root is an object-store URI (s3://, ...)
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<arrow::fs::FileSystem> fs_;
    std::string base_path_;  // table root path within fs_
    std::int64_t version_{0};
    std::string table_id_;
    int file_counter_{0};

    std::shared_ptr<arrow::io::OutputStream> out_;
    std::unique_ptr<parquet::arrow::FileWriter> writer_;
    std::string cur_data_rel_;
    std::int64_t cur_records_{0};
};

}  // namespace clink::sql
