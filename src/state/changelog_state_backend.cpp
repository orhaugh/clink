#include "clink/state/changelog_state_backend.hpp"

#ifndef CLINK_HAS_ARROW
#error \
    "clink requires CLINK_BUILD_ARROW=ON. ChangelogStateBackend::snapshot/restore are Arrow-IPC-only."
#endif

#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

#include "clink/metrics/state_metrics.hpp"
#include "clink/state/durable_file_write.hpp"

namespace clink {

namespace {

constexpr std::uint8_t kRowMaterialization = 0;
constexpr std::uint8_t kRowPut = 1;
constexpr std::uint8_t kRowErase = 2;
// External materialization: value_bytes carries an opaque handle
// (typically a UTF-8 path/URI string), not the materialization
// payload. Restore looks the handle up in the configured store.
constexpr std::uint8_t kRowMaterializationHandle = 3;

// Multi-blob framing (scale-down). When one new subtask inherits several
// parents, frame_blobs() packs their blobs as:
//   ['C','L','M','B'] [u32 count] { [u32 len][blob bytes] } x count   (LE)
// A real changelog blob is an Arrow IPC stream starting with 0xFFFFFFFF, so
// it never collides with this magic. A single blob is stored raw (no
// framing), so same-parallelism / scale-up restore is byte-for-byte unchanged.
constexpr std::array<std::uint8_t, 4> kMultiBlobMagic{'C', 'L', 'M', 'B'};

std::shared_ptr<arrow::Schema> changelog_schema() {
    static const auto schema = arrow::schema({
        arrow::field("row_kind", arrow::uint8(), /*nullable=*/false),
        arrow::field("op_id", arrow::uint64(), /*nullable=*/false),
        arrow::field("key_bytes", arrow::binary(), /*nullable=*/false),
        arrow::field("value_bytes", arrow::binary(), /*nullable=*/false),
    });
    return schema;
}

[[noreturn]] void throw_arrow(const std::string& where, const arrow::Status& s) {
    throw std::runtime_error("ChangelogStateBackend " + where + ": " + s.ToString());
}

}  // namespace

CaptureHandle ChangelogStateBackend::capture(CheckpointId id) {
    const auto t0 = std::chrono::steady_clock::now();
    std::lock_guard lock(log_mu_);
    if (log_bytes_estimate_ >= materialization_threshold_bytes_) {
        materialize_locked_(id);
    }

    const auto schema = changelog_schema();

    const std::size_t total_rows = 1 + log_.size();  // +1 for materialization row
    arrow::UInt8Builder kind_b;
    arrow::UInt64Builder op_b;
    arrow::BinaryBuilder key_b;
    arrow::BinaryBuilder val_b;
    if (auto s = kind_b.Reserve(static_cast<int64_t>(total_rows)); !s.ok()) {
        throw_arrow("snapshot (reserve kind)", s);
    }
    if (auto s = op_b.Reserve(static_cast<int64_t>(total_rows)); !s.ok()) {
        throw_arrow("snapshot (reserve op)", s);
    }
    if (auto s = key_b.Reserve(static_cast<int64_t>(total_rows)); !s.ok()) {
        throw_arrow("snapshot (reserve key)", s);
    }
    if (auto s = val_b.Reserve(static_cast<int64_t>(total_rows)); !s.ok()) {
        throw_arrow("snapshot (reserve val)", s);
    }

    // Row 0 - materialization. Two flavours:
    //   * kRowMaterialization (in-blob): value_bytes carries the
    //     inner backend's opaque snapshot bytes.
    //   * kRowMaterializationHandle (external store): value_bytes
    //     carries a UTF-8 handle string the store can resolve to
    //     the materialization payload.
    // op_id and key_bytes are unused in both cases.
    const bool external_mat = ext_store_ != nullptr;
    if (auto s = kind_b.Append(external_mat ? kRowMaterializationHandle : kRowMaterialization);
        !s.ok()) {
        throw_arrow("snapshot (append kind/mat)", s);
    }
    if (auto s = op_b.Append(0); !s.ok())
        throw_arrow("snapshot (append op/mat)", s);
    if (auto s = key_b.Append(reinterpret_cast<const uint8_t*>(""), 0); !s.ok()) {
        throw_arrow("snapshot (append key/mat)", s);
    }
    if (external_mat) {
        // If there's been no materialization yet (no log entries have
        // ever crossed the threshold), the handle is empty - emit a
        // zero-length value. Restore tolerates this by leaving the
        // inner untouched (same as the in-blob no-materialization
        // case which emits empty bytes).
        const auto& h = last_materialization_handle_;
        if (auto s = val_b.Append(reinterpret_cast<const uint8_t*>(h.data()),
                                  static_cast<int32_t>(h.size()));
            !s.ok()) {
            throw_arrow("snapshot (append mat handle)", s);
        }
    } else {
        if (auto s =
                val_b.Append(reinterpret_cast<const uint8_t*>(last_materialization_.bytes.data()),
                             static_cast<int32_t>(last_materialization_.bytes.size()));
            !s.ok()) {
            throw_arrow("snapshot (append mat bytes)", s);
        }
    }

    // Subsequent rows - log in insertion order.
    for (const auto& e : log_) {
        if (auto s = kind_b.Append(e.is_erase ? kRowErase : kRowPut); !s.ok()) {
            throw_arrow("snapshot (append kind/log)", s);
        }
        if (auto s = op_b.Append(e.op.value()); !s.ok()) {
            throw_arrow("snapshot (append op/log)", s);
        }
        if (auto s = key_b.Append(reinterpret_cast<const uint8_t*>(e.key.data()),
                                  static_cast<int32_t>(e.key.size()));
            !s.ok()) {
            throw_arrow("snapshot (append key/log)", s);
        }
        if (e.is_erase) {
            if (auto s = val_b.Append(reinterpret_cast<const uint8_t*>(""), 0); !s.ok()) {
                throw_arrow("snapshot (append val/erase)", s);
            }
        } else {
            if (auto s = val_b.Append(reinterpret_cast<const uint8_t*>(e.value.data()),
                                      static_cast<int32_t>(e.value.size()));
                !s.ok()) {
                throw_arrow("snapshot (append val/put)", s);
            }
        }
    }

    std::shared_ptr<arrow::Array> kind_arr, op_arr, key_arr, val_arr;
    if (auto s = kind_b.Finish(&kind_arr); !s.ok())
        throw_arrow("snapshot (finish kind)", s);
    if (auto s = op_b.Finish(&op_arr); !s.ok())
        throw_arrow("snapshot (finish op)", s);
    if (auto s = key_b.Finish(&key_arr); !s.ok())
        throw_arrow("snapshot (finish key)", s);
    if (auto s = val_b.Finish(&val_arr); !s.ok())
        throw_arrow("snapshot (finish val)", s);

    auto batch = arrow::RecordBatch::Make(
        schema, static_cast<int64_t>(total_rows), {kind_arr, op_arr, key_arr, val_arr});

    auto sink_result = arrow::io::BufferOutputStream::Create();
    if (!sink_result.ok())
        throw_arrow("snapshot (create sink)", sink_result.status());
    auto sink = *sink_result;
    auto writer_result = arrow::ipc::MakeStreamWriter(sink, schema);
    if (!writer_result.ok())
        throw_arrow("snapshot (make writer)", writer_result.status());
    auto writer = *writer_result;
    if (auto s = writer->WriteRecordBatch(*batch); !s.ok()) {
        throw_arrow("snapshot (write batch)", s);
    }
    if (auto s = writer->Close(); !s.ok())
        throw_arrow("snapshot (close writer)", s);
    auto buf_result = sink->Finish();
    if (!buf_result.ok())
        throw_arrow("snapshot (finish sink)", buf_result.status());
    auto buf = *buf_result;
    std::vector<std::byte> bytes(static_cast<std::size_t>(buf->size()));
    if (buf->size() > 0) {
        std::memcpy(bytes.data(), buf->data(), static_cast<std::size_t>(buf->size()));
    }
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::state::snapshot_completed(
        "changelog", bytes.size(), static_cast<std::uint64_t>(dt));
    // The on-thread half ends here: the framing blob is fully serialised and
    // detached from the live log/materialization, so persist() can write it
    // off the operator thread without holding the log lock.
    return CaptureHandle{.checkpoint_id = id, .bytes = std::move(bytes)};
}

Snapshot ChangelogStateBackend::persist(CaptureHandle handle) {
    // Self-persist the framing blob so a fresh process can restore it (the
    // runtime discards the returned Snapshot). Only when a working dir is
    // configured; the in-RAM `changelog://` scheme returns the blob only.
    //
    // Crash-safe + durable: write-temp, fsync, atomic rename, fsync dir, so
    // the blob is on stable storage before the checkpoint is ack'd (same
    // hardening as FileBacked). For disk-backed changelog this runs on the
    // snapshot worker (off the operator thread). A unique per-write temp
    // lets two workers persisting the same id on a shared backend coexist.
    if (!snapshot_dir_.empty()) {
        std::error_code ec;
        std::filesystem::create_directories(snapshot_dir_, ec);
        const auto path = blob_path_(handle.checkpoint_id);
        const auto tmp = path.string() + ".part." +
                         std::to_string(part_seq_.fetch_add(1, std::memory_order_relaxed));
        clink::state::detail::write_fsync_rename(
            path, tmp, handle.bytes.data(), handle.bytes.size());
    }
    return Snapshot{.checkpoint_id = handle.checkpoint_id, .bytes = std::move(handle.bytes)};
}

Snapshot ChangelogStateBackend::snapshot(CheckpointId id) {
    // Synchronous = capture (serialise under the log lock) + persist (durable
    // write). For disk-backed changelog the runner drives the two halves
    // separately via the snapshot worker so the write lands off-thread; this
    // fused form keeps every direct snapshot() caller unchanged.
    return persist(capture(id));
}

std::vector<std::byte> ChangelogStateBackend::frame_blobs(
    std::span<const std::vector<std::byte>> blobs) {
    if (blobs.empty()) {
        return {};
    }
    if (blobs.size() == 1) {
        return blobs.front();  // single blob: raw, no framing (back-compatible)
    }
    std::vector<std::byte> out;
    auto put_u32 = [&out](std::uint32_t v) {
        for (int i = 0; i < 4; ++i) {
            out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
        }
    };
    for (const auto b : kMultiBlobMagic) {
        out.push_back(static_cast<std::byte>(b));
    }
    put_u32(static_cast<std::uint32_t>(blobs.size()));
    for (const auto& blob : blobs) {
        put_u32(static_cast<std::uint32_t>(blob.size()));
        out.insert(out.end(), blob.begin(), blob.end());
    }
    return out;
}

void ChangelogStateBackend::restore(const Snapshot& snap, const KeyGroupRange& kg_filter) {
    const auto t0 = std::chrono::steady_clock::now();
    std::lock_guard lock(log_mu_);
    log_.clear();
    log_bytes_estimate_ = 0;

    if (snap.bytes.empty()) {
        // Nothing to restore. Leave inner_ untouched so callers can
        // distinguish "empty snapshot" from "no snapshot".
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::state::restore_completed("changelog", static_cast<std::uint64_t>(dt));
        return;
    }

    // Split into one or more framing blobs. A multi-blob payload (scale-down,
    // produced by frame_blobs) is magic-framed; otherwise the whole snapshot
    // is a single raw blob (same-parallelism / scale-up).
    const auto* base = reinterpret_cast<const std::uint8_t*>(snap.bytes.data());
    const std::size_t total = snap.bytes.size();
    bool framed = total >= kMultiBlobMagic.size();
    for (std::size_t i = 0; framed && i < kMultiBlobMagic.size(); ++i) {
        framed = base[i] == kMultiBlobMagic[i];
    }
    std::vector<std::pair<const std::uint8_t*, std::size_t>> blob_spans;
    if (framed) {
        std::size_t off = kMultiBlobMagic.size();
        auto read_u32 = [&](std::uint32_t& v) {
            if (off + 4 > total) {
                throw std::runtime_error(
                    "ChangelogStateBackend restore: truncated multi-blob frame");
            }
            v = static_cast<std::uint32_t>(base[off]) |
                (static_cast<std::uint32_t>(base[off + 1]) << 8) |
                (static_cast<std::uint32_t>(base[off + 2]) << 16) |
                (static_cast<std::uint32_t>(base[off + 3]) << 24);
            off += 4;
        };
        std::uint32_t count = 0;
        read_u32(count);
        for (std::uint32_t b = 0; b < count; ++b) {
            std::uint32_t blen = 0;
            read_u32(blen);
            if (off + blen > total) {
                throw std::runtime_error(
                    "ChangelogStateBackend restore: truncated multi-blob payload");
            }
            blob_spans.emplace_back(base + off, static_cast<std::size_t>(blen));
            off += blen;
        }
    } else {
        blob_spans.emplace_back(base, total);
    }

    // Parse each blob into its materialization Snapshot (if any) and its log
    // rows. Materializations from all parents are combined and restored once
    // (the inner backend folds them); then all log deltas replay. key_in_range_
    // narrows keyed rows to this subtask's range and keeps operator-state rows.
    struct PendingLog {
        std::uint8_t kind;
        OperatorId op;
        std::string key;
        std::string value;
    };
    std::vector<Snapshot> mats;
    std::vector<PendingLog> pending;

    auto parse_blob = [&](const std::uint8_t* data, std::size_t len) {
        auto buffer = std::make_shared<arrow::Buffer>(data, static_cast<int64_t>(len));
        auto input = std::make_shared<arrow::io::BufferReader>(buffer);
        auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
        if (!reader_result.ok()) {
            throw_arrow("restore (open reader)", reader_result.status());
        }
        auto reader = *reader_result;
        if (!reader->schema()->Equals(*changelog_schema(), /*check_metadata=*/false)) {
            throw std::runtime_error(
                "ChangelogStateBackend restore: snapshot schema mismatch - got " +
                reader->schema()->ToString());
        }
        bool mat_row_seen = false;
        std::shared_ptr<arrow::RecordBatch> batch;
        while (true) {
            if (auto s = reader->ReadNext(&batch); !s.ok()) {
                throw_arrow("restore (read batch)", s);
            }
            if (!batch) {
                break;
            }
            const auto* kind_arr = static_cast<const arrow::UInt8Array*>(batch->column(0).get());
            const auto* op_arr = static_cast<const arrow::UInt64Array*>(batch->column(1).get());
            const auto* key_arr = static_cast<const arrow::BinaryArray*>(batch->column(2).get());
            const auto* val_arr = static_cast<const arrow::BinaryArray*>(batch->column(3).get());
            const auto rows = batch->num_rows();
            for (int64_t i = 0; i < rows; ++i) {
                const auto kind = kind_arr->Value(i);
                int32_t klen = 0;
                const uint8_t* kptr = key_arr->GetValue(i, &klen);
                int32_t vlen = 0;
                const uint8_t* vptr = val_arr->GetValue(i, &vlen);

                if (kind == kRowMaterialization) {
                    if (ext_store_) {
                        throw std::runtime_error(
                            "ChangelogStateBackend restore: snapshot is in-blob (row_kind=0) "
                            "but this backend has an external materialization store; modes "
                            "must match the producing backend.");
                    }
                    mat_row_seen = true;
                    if (vlen > 0) {
                        Snapshot m;
                        m.checkpoint_id = snap.checkpoint_id;
                        m.bytes.resize(static_cast<std::size_t>(vlen));
                        std::memcpy(m.bytes.data(), vptr, static_cast<std::size_t>(vlen));
                        mats.push_back(std::move(m));
                    }
                    continue;
                }
                if (kind == kRowMaterializationHandle) {
                    if (!ext_store_) {
                        throw std::runtime_error(
                            "ChangelogStateBackend restore: snapshot carries an external handle "
                            "(row_kind=3) but no ExternalMaterializationStore is configured.");
                    }
                    mat_row_seen = true;
                    if (vlen > 0) {
                        std::string handle(reinterpret_cast<const char*>(vptr),
                                           static_cast<std::size_t>(vlen));
                        Snapshot m;
                        m.checkpoint_id = snap.checkpoint_id;
                        m.bytes = ext_store_->read(handle);
                        last_materialization_handle_ = std::move(handle);
                        mats.push_back(std::move(m));
                    }
                    // vlen == 0 = no materialization yet; the log rows below
                    // hydrate whatever was mutated before the first one.
                    continue;
                }

                // Log row: collect now, replay after the inner is restored.
                pending.push_back(PendingLog{kind,
                                             OperatorId{op_arr->Value(i)},
                                             std::string(reinterpret_cast<const char*>(kptr),
                                                         static_cast<std::size_t>(klen)),
                                             std::string(reinterpret_cast<const char*>(vptr),
                                                         static_cast<std::size_t>(vlen))});
            }
        }
        if (!mat_row_seen) {
            throw std::runtime_error(
                "ChangelogStateBackend restore: blob contained no materialization row "
                "(row_kind 0/3). Snapshot is malformed.");
        }
    };

    for (const auto& [data, len] : blob_spans) {
        parse_blob(data, len);
    }

    // Restore the inner from the combined materialization(s) (the inner folds
    // multiple parents - InMemory merges the IPC streams, RocksDB the
    // checkpoint dirs), then replay the log deltas.
    if (!mats.empty()) {
        auto combined = inner_->combine_snapshots(std::move(mats));
        inner_->restore(combined, kg_filter);
    }
    for (const auto& lg : pending) {
        if (!key_in_range_(kg_filter, lg.key)) {
            continue;
        }
        if (lg.kind == kRowErase) {
            inner_->erase(lg.op, lg.key);
        } else {  // kRowPut
            inner_->put(lg.op, lg.key, lg.value);
        }
    }

    // After replay, snapshot the inner so subsequent snapshots don't
    // redundantly include the just-applied log entries. When using an
    // external store, also push the fresh materialization to it (the
    // OLD handle now points to bytes that no longer match - they
    // were the pre-replay materialization).
    last_materialization_ = inner_->snapshot(snap.checkpoint_id);
    if (ext_store_) {
        last_materialization_handle_ =
            ext_store_->write(snap.checkpoint_id,
                              std::span<const std::byte>{last_materialization_.bytes.data(),
                                                         last_materialization_.bytes.size()});
    }
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::state::restore_completed("changelog", static_cast<std::uint64_t>(dt));
}

}  // namespace clink
