#pragma once

// BlockingExchangeOperator<T> - the blocking-edge primitive for batch execution
// (BATCH-2).
//
// A blocking exchange is a stage boundary that fully materialises its input
// before any record crosses to the consuming side: "materialise-then-consume".
// This is the batch counterpart to the pipelined BoundedChannel edge, and it is
// what lets a batch scheduler (BATCH-3) launch the consumer stage only after the
// producer stage has completed, decoupling their scheduling.
//
// Realisation on the thread-per-operator runtime: the operator buffers every
// incoming StreamElement in arrival order and emits NOTHING from process(). The
// single-input operator runner calls flush() once, at clean end-of-input (the
// input channel closed and drained). flush() then replays the whole buffer
// downstream in order. So no data reaches the consumer until the entire
// producing side has finished - the blocking-edge contract - while reusing the
// existing flush() seam rather than a new channel type.
//
// Spill: data batches are serialised to Arrow IPC on arrival. Serialised bytes
// accumulate in memory until they cross spill_threshold_bytes, after which
// further batches are written to a single Arrow IPC stream file under spill_dir
// (overflow-to-disk, so a large exchange does not have to fit in RAM). Control
// elements (watermark / barrier / drain) are tiny and rare, so they stay in the
// in-memory entry list; preserving them in arrival order keeps event-time and
// rescale semantics intact across the boundary (e.g. the BATCH-1 end-of-input
// max watermark still reaches downstream windows). On flush() spilled batches
// are read back from the file in the same order.
//
// Scope: intended for bounded batch jobs run to completion (no periodic
// checkpointing). Barriers are buffered and replayed in order as a courtesy but
// the exchange is not integrated with the checkpoint coordinator; a job that
// checkpoints across a blocking exchange is out of scope for BATCH-2.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#ifndef CLINK_HAS_ARROW
#error "BlockingExchangeOperator<T> requires CLINK_BUILD_ARROW=ON (spill uses Arrow IPC)."
#endif

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

struct BlockingExchangeOptions {
    // In-memory budget for buffered (Arrow-IPC-serialised) data before overflow
    // batches spill to disk, measured against each batch's IPC payload size.
    // Default 64 MiB.
    std::size_t spill_threshold_bytes{64ull * 1024 * 1024};

    // Directory for spill files. Empty disables spill entirely: all data stays
    // in memory regardless of the threshold (useful for small exchanges and
    // tests). The directory must exist; the operator creates one file in it.
    std::string spill_dir;
};

template <typename T>
class BlockingExchangeOperator final : public Operator<T, T> {
public:
    explicit BlockingExchangeOperator(ArrowBatcher<T> batcher,
                                      BlockingExchangeOptions opts = {},
                                      std::string name = "blocking_exchange")
        : batcher_(std::move(batcher)), opts_(std::move(opts)), name_(std::move(name)) {
        if (!batcher_.schema || !batcher_.build || !batcher_.parse) {
            throw std::invalid_argument(
                "BlockingExchangeOperator: ArrowBatcher must have schema, build and parse set");
        }
    }

    ~BlockingExchangeOperator() override { cleanup_spill_file_(); }

    BlockingExchangeOperator(const BlockingExchangeOperator&) = delete;
    BlockingExchangeOperator& operator=(const BlockingExchangeOperator&) = delete;
    BlockingExchangeOperator(BlockingExchangeOperator&&) = delete;
    BlockingExchangeOperator& operator=(BlockingExchangeOperator&&) = delete;

    // Buffer everything; emit nothing. The blocking-edge contract: no element
    // crosses the boundary until the producing side has completed and flush()
    // replays the buffer.
    void process(const StreamElement<T>& element, Emitter<T>& /*out*/) override {
        if (element.is_data()) {
            buffer_data_(element.as_data());
        } else if (element.is_watermark()) {
            Entry e;
            e.kind = EntryKind::Watermark;
            e.watermark = element.as_watermark();
            entries_.push_back(std::move(e));
        } else if (element.is_barrier()) {
            Entry e;
            e.kind = EntryKind::Barrier;
            e.barrier = element.as_barrier();
            entries_.push_back(std::move(e));
        } else {  // drain
            Entry e;
            e.kind = EntryKind::Drain;
            e.drain = element.as_drain();
            entries_.push_back(std::move(e));
        }
    }

    // Seal the spill file then replay the whole buffer downstream in arrival
    // order. Called once by the runner at clean end-of-input.
    void flush(Emitter<T>& out) override {
        seal_spill_writer_();
        std::shared_ptr<arrow::ipc::RecordBatchStreamReader> spill_reader;
        if (spilled_batches_ > 0) {
            spill_reader = open_spill_reader_();
        }
        for (const auto& e : entries_) {
            switch (e.kind) {
                case EntryKind::DataInMemory:
                    out.emit_data(deserialize_bytes_(e.bytes));
                    break;
                case EntryKind::DataSpilled:
                    out.emit_data(read_next_spilled_(*spill_reader));
                    break;
                case EntryKind::Watermark:
                    out.emit_watermark(*e.watermark);
                    break;
                case EntryKind::Barrier:
                    out.emit_barrier(*e.barrier);
                    break;
                case EntryKind::Drain:
                    out.emit_drain(*e.drain);
                    break;
            }
        }
        cleanup_spill_file_();
    }

    std::string name() const override { return name_; }

    // ---- Observability (tests / metrics) -------------------------------
    [[nodiscard]] std::size_t spilled_batch_count() const noexcept { return spilled_batches_; }
    [[nodiscard]] std::size_t buffered_in_memory_bytes() const noexcept { return in_memory_bytes_; }
    [[nodiscard]] std::size_t buffered_entry_count() const noexcept { return entries_.size(); }

private:
    enum class EntryKind { DataInMemory, DataSpilled, Watermark, Barrier, Drain };

    struct Entry {
        EntryKind kind{EntryKind::DataInMemory};
        std::string bytes;  // DataInMemory: serialised Arrow IPC stream
        std::optional<Watermark> watermark;
        std::optional<CheckpointBarrier> barrier;
        std::optional<DrainMarker> drain;
    };

    void buffer_data_(const Batch<T>& batch) {
        auto rb = batcher_.build(batch);
        if (!rb) {
            throw std::runtime_error("BlockingExchangeOperator: ArrowBatcher::build returned null");
        }
        std::int64_t payload_size = 0;
        if (auto s = arrow::ipc::GetRecordBatchSize(*rb, &payload_size); !s.ok()) {
            throw std::runtime_error("BlockingExchangeOperator: GetRecordBatchSize: " +
                                     s.ToString());
        }
        const bool spill_enabled = !opts_.spill_dir.empty();
        // Spill this batch if the in-memory pool would cross the threshold, OR
        // if we have already started spilling. The latch matters: spilled
        // batches do not grow in_memory_bytes_, so without it a later small
        // batch could slip back into memory after overflow began - which would
        // both interleave RAM growth with disk and break the "once overflowing,
        // stays on disk" memory bound. Latching keeps resident memory monotone
        // after the first spill.
        if (spill_enabled &&
            (spill_writer_ != nullptr || in_memory_bytes_ + static_cast<std::size_t>(payload_size) >
                                             opts_.spill_threshold_bytes)) {
            // Overflow: append this batch to the on-disk spill stream.
            ensure_spill_writer_open_();
            if (auto s = spill_writer_->WriteRecordBatch(*rb); !s.ok()) {
                throw std::runtime_error("BlockingExchangeOperator: spill WriteRecordBatch: " +
                                         s.ToString());
            }
            Entry e;
            e.kind = EntryKind::DataSpilled;
            entries_.push_back(std::move(e));
            ++spilled_batches_;
        } else {
            // Keep in memory as a serialised IPC blob.
            Entry e;
            e.kind = EntryKind::DataInMemory;
            e.bytes = serialize_record_batch_(*rb);
            in_memory_bytes_ += e.bytes.size();
            entries_.push_back(std::move(e));
        }
    }

    std::string serialize_record_batch_(const arrow::RecordBatch& rb) const {
        auto sink_res = arrow::io::BufferOutputStream::Create();
        if (!sink_res.ok()) {
            throw std::runtime_error("BlockingExchangeOperator: BufferOutputStream::Create: " +
                                     sink_res.status().ToString());
        }
        auto sink = *sink_res;
        auto writer_res = arrow::ipc::MakeStreamWriter(sink, batcher_.schema());
        if (!writer_res.ok()) {
            throw std::runtime_error("BlockingExchangeOperator: MakeStreamWriter: " +
                                     writer_res.status().ToString());
        }
        auto writer = *writer_res;
        if (auto s = writer->WriteRecordBatch(rb); !s.ok()) {
            throw std::runtime_error("BlockingExchangeOperator: WriteRecordBatch: " + s.ToString());
        }
        if (auto s = writer->Close(); !s.ok()) {
            throw std::runtime_error("BlockingExchangeOperator: writer Close: " + s.ToString());
        }
        auto buf_res = sink->Finish();
        if (!buf_res.ok()) {
            throw std::runtime_error("BlockingExchangeOperator: sink Finish: " +
                                     buf_res.status().ToString());
        }
        auto buf = *buf_res;
        return std::string(reinterpret_cast<const char*>(buf->data()),
                           static_cast<std::size_t>(buf->size()));
    }

    Batch<T> deserialize_bytes_(const std::string& bytes) const {
        auto buffer =
            std::make_shared<arrow::Buffer>(reinterpret_cast<const std::uint8_t*>(bytes.data()),
                                            static_cast<std::int64_t>(bytes.size()));
        auto input = std::make_shared<arrow::io::BufferReader>(buffer);
        auto reader_res = arrow::ipc::RecordBatchStreamReader::Open(input);
        if (!reader_res.ok()) {
            throw std::runtime_error("BlockingExchangeOperator: in-memory reader Open: " +
                                     reader_res.status().ToString());
        }
        auto reader = *reader_res;
        std::shared_ptr<arrow::RecordBatch> rb;
        if (auto s = reader->ReadNext(&rb); !s.ok()) {
            throw std::runtime_error("BlockingExchangeOperator: in-memory ReadNext: " +
                                     s.ToString());
        }
        return parse_or_throw_(rb);
    }

    Batch<T> read_next_spilled_(arrow::ipc::RecordBatchStreamReader& reader) const {
        std::shared_ptr<arrow::RecordBatch> rb;
        if (auto s = reader.ReadNext(&rb); !s.ok()) {
            throw std::runtime_error("BlockingExchangeOperator: spill ReadNext: " + s.ToString());
        }
        return parse_or_throw_(rb);
    }

    Batch<T> parse_or_throw_(const std::shared_ptr<arrow::RecordBatch>& rb) const {
        if (!rb) {
            throw std::runtime_error(
                "BlockingExchangeOperator: replay read past end of buffer (spill file shorter "
                "than the recorded spilled-batch count)");
        }
        auto parsed = batcher_.parse(*rb);
        if (!parsed.has_value()) {
            throw std::runtime_error("BlockingExchangeOperator: ArrowBatcher::parse failed");
        }
        return std::move(*parsed);
    }

    void ensure_spill_writer_open_() {
        if (spill_writer_) {
            return;
        }
        spill_path_ = make_spill_path_();
        auto file_res = arrow::io::FileOutputStream::Open(spill_path_.string());
        if (!file_res.ok()) {
            throw std::runtime_error("BlockingExchangeOperator: open spill file " +
                                     spill_path_.string() + ": " + file_res.status().ToString());
        }
        spill_file_ = *file_res;
        auto writer_res = arrow::ipc::MakeStreamWriter(spill_file_, batcher_.schema());
        if (!writer_res.ok()) {
            throw std::runtime_error("BlockingExchangeOperator: spill MakeStreamWriter: " +
                                     writer_res.status().ToString());
        }
        spill_writer_ = *writer_res;
    }

    void seal_spill_writer_() {
        if (spill_writer_) {
            if (auto s = spill_writer_->Close(); !s.ok()) {
                throw std::runtime_error("BlockingExchangeOperator: spill writer Close: " +
                                         s.ToString());
            }
            spill_writer_.reset();
        }
        if (spill_file_) {
            if (auto s = spill_file_->Close(); !s.ok()) {
                throw std::runtime_error("BlockingExchangeOperator: spill file Close: " +
                                         s.ToString());
            }
            spill_file_.reset();
        }
    }

    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> open_spill_reader_() const {
        auto file_res = arrow::io::ReadableFile::Open(spill_path_.string());
        if (!file_res.ok()) {
            throw std::runtime_error("BlockingExchangeOperator: reopen spill file " +
                                     spill_path_.string() + ": " + file_res.status().ToString());
        }
        auto reader_res = arrow::ipc::RecordBatchStreamReader::Open(*file_res);
        if (!reader_res.ok()) {
            throw std::runtime_error("BlockingExchangeOperator: spill reader Open: " +
                                     reader_res.status().ToString());
        }
        return *reader_res;
    }

    std::filesystem::path make_spill_path_() const {
        // Process-unique within spill_dir: operator id plus a global counter so
        // two exchanges (or two runs) sharing a dir never collide. The in-process
        // executor is single-process; the cluster path gives each subtask a
        // private working dir, so this is sufficient.
        static std::atomic<std::uint64_t> seq{0};
        const auto n = seq.fetch_add(1, std::memory_order_relaxed);
        return std::filesystem::path(opts_.spill_dir) /
               ("blocking_exchange_" + std::to_string(this->id().value()) + "_" +
                std::to_string(n) + ".arrow");
    }

    void cleanup_spill_file_() {
        // Best-effort: seal any open handles, then remove the spill file. The
        // exchange owns the file for the lifetime of one run; once replayed it
        // is no longer needed.
        if (spill_writer_) {
            (void)spill_writer_->Close();
            spill_writer_.reset();
        }
        if (spill_file_) {
            (void)spill_file_->Close();
            spill_file_.reset();
        }
        if (!spill_path_.empty()) {
            std::error_code ec;
            std::filesystem::remove(spill_path_, ec);
            spill_path_.clear();
        }
    }

    ArrowBatcher<T> batcher_;
    BlockingExchangeOptions opts_;
    std::string name_;

    std::vector<Entry> entries_;
    std::size_t in_memory_bytes_{0};
    std::size_t spilled_batches_{0};

    std::filesystem::path spill_path_;
    std::shared_ptr<arrow::io::FileOutputStream> spill_file_;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> spill_writer_;
};

}  // namespace clink
