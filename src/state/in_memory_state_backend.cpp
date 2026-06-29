#include "clink/state/in_memory_state_backend.hpp"

#include <chrono>

#include "clink/metrics/state_metrics.hpp"

#ifndef CLINK_HAS_ARROW
#error \
    "clink requires CLINK_BUILD_ARROW=ON. InMemoryStateBackend::snapshot/restore are Arrow-IPC-only."
#endif

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

namespace clink {

namespace {

// Canonical snapshot schema. Three columns, one row per (operator, key)
// entry. op_id is uint64 (matches OperatorId::value()). Keys and values
// are opaque binary blobs - the backend does not care about their
// internal structure; the kg-filter byte is just the leading byte of
// key_bytes when applied.
//
// Field structure is identical across all snapshots. The schema's
// arrow::KeyValueMetadata carries optional state-version annotations:
//   - "clink.state_versions": packed StateVersionMap. Absent on
//     v1-format snapshots; restore must tolerate missing metadata.
std::shared_ptr<arrow::Schema> snapshot_schema() {
    static const auto schema = arrow::schema({
        arrow::field("op_id", arrow::uint64(), /*nullable=*/false),
        arrow::field("key_bytes", arrow::binary(), /*nullable=*/false),
        arrow::field("value_bytes", arrow::binary(), /*nullable=*/false),
    });
    return schema;
}

constexpr const char kStateVersionsMetadataKey[] = "clink.state_versions";

std::shared_ptr<arrow::Schema> snapshot_schema_with_versions(const StateVersionMap& versions) {
    auto schema = snapshot_schema();
    if (versions.empty()) {
        return schema;
    }
    auto packed = versions.pack();
    auto meta = std::make_shared<arrow::KeyValueMetadata>();
    meta->Append(kStateVersionsMetadataKey, packed);
    return schema->WithMetadata(meta);
}

[[noreturn]] void throw_arrow(const std::string& where, const arrow::Status& s) {
    throw std::runtime_error("InMemoryStateBackend " + where + ": " + s.ToString());
}

// Decode an exactly-8-byte little-endian value as an int64 (the wire form of
// a source offset). Returns nullopt for any other width - e.g. broadcast
// state, whose values are arbitrary and must not be compared as offsets.
std::optional<std::int64_t> as_offset_i64(const StateBackend::Value& v) {
    if (v.size() != 8) {
        return std::nullopt;
    }
    std::uint64_t u = 0;
    for (int i = 0; i < 8; ++i) {
        u |= static_cast<std::uint64_t>(
                 std::to_integer<std::uint8_t>(v[static_cast<std::size_t>(i)]))
             << (i * 8);
    }
    return static_cast<std::int64_t>(u);
}

}  // namespace

Snapshot InMemoryStateBackend::snapshot(CheckpointId id) {
    const auto t0 = std::chrono::steady_clock::now();
    std::lock_guard lock(mu_);

    // Count rows so we can reserve exactly once on each builder.
    std::size_t total_rows = 0;
    for (const auto& [op, kv] : state_) {
        total_rows += kv.size();
    }

    arrow::UInt64Builder op_b;
    arrow::BinaryBuilder key_b;
    arrow::BinaryBuilder val_b;
    if (auto s = op_b.Reserve(static_cast<int64_t>(total_rows)); !s.ok()) {
        throw_arrow("snapshot (reserve op)", s);
    }
    if (auto s = key_b.Reserve(static_cast<int64_t>(total_rows)); !s.ok()) {
        throw_arrow("snapshot (reserve key)", s);
    }
    if (auto s = val_b.Reserve(static_cast<int64_t>(total_rows)); !s.ok()) {
        throw_arrow("snapshot (reserve val)", s);
    }

    for (const auto& [op, kv] : state_) {
        const auto op_id_val = op.value();
        for (const auto& [k, v] : kv) {
            if (auto s = op_b.Append(op_id_val); !s.ok()) {
                throw_arrow("snapshot (append op)", s);
            }
            if (auto s = key_b.Append(reinterpret_cast<const uint8_t*>(k.data()),
                                      static_cast<int32_t>(k.size()));
                !s.ok()) {
                throw_arrow("snapshot (append key)", s);
            }
            if (auto s = val_b.Append(reinterpret_cast<const uint8_t*>(v.data()),
                                      static_cast<int32_t>(v.size()));
                !s.ok()) {
                throw_arrow("snapshot (append val)", s);
            }
        }
    }

    std::shared_ptr<arrow::Array> op_arr, key_arr, val_arr;
    if (auto s = op_b.Finish(&op_arr); !s.ok())
        throw_arrow("snapshot (finish op)", s);
    if (auto s = key_b.Finish(&key_arr); !s.ok())
        throw_arrow("snapshot (finish key)", s);
    if (auto s = val_b.Finish(&val_arr); !s.ok())
        throw_arrow("snapshot (finish val)", s);

    // If the operator pipeline registered version stamps before this
    // snapshot fired, embed them in the Arrow schema metadata so that
    // (a) any pyarrow/duckdb reader sees them, and (b) the restore
    // path can reload them into state_versions_ for the control plane.
    const auto schema = snapshot_schema_with_versions(state_versions_);
    auto batch = arrow::RecordBatch::Make(
        schema, static_cast<int64_t>(total_rows), {op_arr, key_arr, val_arr});

    // Write a complete Arrow IPC stream (schema + record-batch + EOS).
    // Stream format is the canonical Arrow blob: any standard Arrow
    // consumer (pyarrow, duckdb, polars, ...) can open it directly.
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
    std::size_t live_keys = 0;
    for (const auto& [_, kv] : state_) {
        live_keys += kv.size();
    }
    clink::metrics::state::snapshot_completed(
        "in_memory", bytes.size(), static_cast<std::uint64_t>(dt));
    clink::metrics::state::keyed_keys_set("in_memory", static_cast<std::int64_t>(live_keys));
    return Snapshot{.checkpoint_id = id, .bytes = std::move(bytes)};
}

void InMemoryStateBackend::restore(const Snapshot& snap, const KeyGroupRange& kg_filter) {
    const auto t0 = std::chrono::steady_clock::now();
    std::lock_guard lock(mu_);
    state_.clear();
    state_versions_.clear();

    if (snap.bytes.empty()) {
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::state::restore_completed("in_memory", static_cast<std::uint64_t>(dt));
        return;
    }

    auto buffer =
        std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(snap.bytes.data()),
                                        static_cast<int64_t>(snap.bytes.size()));
    auto input = std::make_shared<arrow::io::BufferReader>(buffer);
    auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
    if (!reader_result.ok())
        throw_arrow("restore (open reader)", reader_result.status());
    auto reader = *reader_result;

    // Validate schema matches what we wrote. The stream reader gives us
    // the schema for free; a mismatch means someone tried to restore
    // from a snapshot produced by an incompatible writer.
    if (!reader->schema()->Equals(*snapshot_schema(), /*check_metadata=*/false)) {
        throw std::runtime_error(
            "InMemoryStateBackend restore: snapshot schema mismatch - expected "
            "[op_id:uint64, key_bytes:binary, value_bytes:binary], got " +
            reader->schema()->ToString());
    }

    // Recover the state version map if present. Absence is
    // expected for older snapshots; treat that as "no stamps recorded"
    // (control plane will then assume version 1 per the trait default).
    if (const auto& metadata = reader->schema()->metadata(); metadata) {
        if (auto idx = metadata->FindKey(kStateVersionsMetadataKey); idx != -1) {
            state_versions_ = StateVersionMap::unpack(metadata->value(idx));
        }
    }

    const bool apply_filter = !kg_filter.covers_all();
    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
        if (auto s = reader->ReadNext(&batch); !s.ok()) {
            throw_arrow("restore (read batch)", s);
        }
        if (!batch)
            break;  // end of stream

        const auto* op_arr = static_cast<const arrow::UInt64Array*>(batch->column(0).get());
        const auto* key_arr = static_cast<const arrow::BinaryArray*>(batch->column(1).get());
        const auto* val_arr = static_cast<const arrow::BinaryArray*>(batch->column(2).get());

        const auto rows = batch->num_rows();
        for (int64_t i = 0; i < rows; ++i) {
            const auto op_id_val = op_arr->Value(i);

            int32_t klen = 0;
            const uint8_t* kptr = key_arr->GetValue(i, &klen);
            int32_t vlen = 0;
            const uint8_t* vptr = val_arr->GetValue(i, &vlen);

            // Narrow by key group only for KEYED rows, whose first byte is
            // their key group (< kNumKeyGroups). Operator-state rows carry a
            // reserved prefix byte >= kNumKeyGroups (kOperatorStateKeyPrefix)
            // and are exempt - every subtask restores them whole. Without
            // this gate the prefix-less legacy/operator keys were silently
            // dropped on rescale (their first byte aliased a real key group).
            if (apply_filter && klen > 0 && static_cast<KeyGroup>(kptr[0]) < kNumKeyGroups) {
                const auto kg = static_cast<KeyGroup>(kptr[0]);
                if (!kg_filter.contains(kg)) {
                    continue;
                }
            }

            std::string key(reinterpret_cast<const char*>(kptr), static_cast<std::size_t>(klen));
            Value val(static_cast<std::size_t>(vlen));
            if (vlen > 0) {
                std::memcpy(val.data(), vptr, static_cast<std::size_t>(vlen));
            }
            auto& slot = state_[OperatorId{op_id_val}];
            // Operator-state rows (first byte >= kNumKeyGroups) can appear in
            // more than one merged parent's snapshot for the same key - a Kafka
            // partition that migrated between subtasks via a consumer-group
            // rebalance, or a stale row a non-owning subtask re-persisted. For
            // fixed-width i64-LE values (source offsets, which are monotonic)
            // keep the GREATER value so the union never rewinds, independent of
            // merge/iteration order. Other widths (broadcast state, identical
            // across subtasks) keep last-writer-wins. Keyed rows never collide
            // (disjoint key-group prefixes), so this only affects operator state.
            if (klen > 0 && static_cast<KeyGroup>(kptr[0]) >= kNumKeyGroups) {
                auto existing = slot.find(key);
                if (existing != slot.end()) {
                    auto cur = as_offset_i64(existing->second);
                    auto inc = as_offset_i64(val);
                    if (cur && inc && *cur >= *inc) {
                        continue;  // existing offset is at least as advanced
                    }
                }
            }
            slot[std::move(key)] = std::move(val);
        }
    }
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::state::restore_completed("in_memory", static_cast<std::uint64_t>(dt));
}

Snapshot InMemoryStateBackend::combine_snapshots(std::vector<Snapshot> parts) const {
    if (parts.empty()) {
        return Snapshot{};
    }
    if (parts.size() == 1) {
        return std::move(parts.front());
    }
    const auto id = parts.front().checkpoint_id;
    std::vector<std::vector<std::byte>> byte_parts;
    byte_parts.reserve(parts.size());
    for (auto& p : parts) {
        byte_parts.push_back(std::move(p.bytes));
    }
    return Snapshot{.checkpoint_id = id, .bytes = merge_snapshot_bytes(byte_parts)};
}

std::vector<std::byte> InMemoryStateBackend::merge_snapshot_bytes(
    std::span<const std::vector<std::byte>> parts) {
    const auto schema = snapshot_schema();

    // Output schema = the FIRST non-empty input's schema, so its metadata
    // (the packed StateVersionMap under "clink.state_versions") survives the
    // merge. Writing batches under it is safe: every input shares the
    // canonical field layout (validated below with check_metadata=false) and
    // the IPC writer matches batches to the stream schema by fields, not
    // metadata. Falls back to the bare canonical schema when all inputs are
    // empty. Without this, merge dropped state versions (a latent scale-down
    // bug) and broke the sharded backend's snapshot round-trip.
    //
    // Invariant: callers pass parts that share one job-level version map - the
    // scale-down parents of a single global checkpoint, or one backend's own
    // shards. The first part's metadata therefore represents them all; this
    // does NOT reconcile divergent version maps (no reachable path produces
    // them).
    std::shared_ptr<arrow::Schema> out_schema = schema;
    for (const auto& part : parts) {
        if (part.empty()) {
            continue;
        }
        auto buf = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(part.data()),
                                                   static_cast<int64_t>(part.size()));
        auto in = std::make_shared<arrow::io::BufferReader>(buf);
        auto rr = arrow::ipc::RecordBatchStreamReader::Open(in);
        if (!rr.ok()) {
            throw_arrow("merge (open reader for schema)", rr.status());
        }
        out_schema = (*rr)->schema();
        break;
    }

    auto sink_result = arrow::io::BufferOutputStream::Create();
    if (!sink_result.ok())
        throw_arrow("merge (create sink)", sink_result.status());
    auto sink = *sink_result;
    auto writer_result = arrow::ipc::MakeStreamWriter(sink, out_schema);
    if (!writer_result.ok())
        throw_arrow("merge (make writer)", writer_result.status());
    auto writer = *writer_result;

    for (const auto& part : parts) {
        if (part.empty())
            continue;
        auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(part.data()),
                                                      static_cast<int64_t>(part.size()));
        auto input = std::make_shared<arrow::io::BufferReader>(buffer);
        auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
        if (!reader_result.ok())
            throw_arrow("merge (open reader)", reader_result.status());
        auto reader = *reader_result;
        if (!reader->schema()->Equals(*schema, /*check_metadata=*/false)) {
            throw std::runtime_error(
                "InMemoryStateBackend merge_snapshot_bytes: input schema mismatch - got " +
                reader->schema()->ToString());
        }
        std::shared_ptr<arrow::RecordBatch> batch;
        while (true) {
            if (auto s = reader->ReadNext(&batch); !s.ok()) {
                throw_arrow("merge (read batch)", s);
            }
            if (!batch)
                break;
            if (auto s = writer->WriteRecordBatch(*batch); !s.ok()) {
                throw_arrow("merge (write batch)", s);
            }
        }
    }

    if (auto s = writer->Close(); !s.ok())
        throw_arrow("merge (close writer)", s);
    auto buf_result = sink->Finish();
    if (!buf_result.ok())
        throw_arrow("merge (finish sink)", buf_result.status());
    auto buf = *buf_result;
    std::vector<std::byte> bytes(static_cast<std::size_t>(buf->size()));
    if (buf->size() > 0) {
        std::memcpy(bytes.data(), buf->data(), static_cast<std::size_t>(buf->size()));
    }
    return bytes;
}

std::vector<std::byte> InMemoryStateBackend::extract_operator_state_bytes(
    std::span<const std::byte> snapshot_bytes) {
    const auto schema = snapshot_schema();

    arrow::UInt64Builder op_b;
    arrow::BinaryBuilder key_b;
    arrow::BinaryBuilder val_b;

    if (!snapshot_bytes.empty()) {
        auto buffer =
            std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(snapshot_bytes.data()),
                                            static_cast<int64_t>(snapshot_bytes.size()));
        auto input = std::make_shared<arrow::io::BufferReader>(buffer);
        auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
        if (!reader_result.ok())
            throw_arrow("extract_operator_state (open reader)", reader_result.status());
        auto reader = *reader_result;
        if (!reader->schema()->Equals(*schema, /*check_metadata=*/false)) {
            throw std::runtime_error(
                "InMemoryStateBackend extract_operator_state_bytes: input schema mismatch");
        }
        std::shared_ptr<arrow::RecordBatch> batch;
        while (true) {
            if (auto s = reader->ReadNext(&batch); !s.ok())
                throw_arrow("extract_operator_state (read batch)", s);
            if (!batch)
                break;
            const auto* op_arr = static_cast<const arrow::UInt64Array*>(batch->column(0).get());
            const auto* key_arr = static_cast<const arrow::BinaryArray*>(batch->column(1).get());
            const auto* val_arr = static_cast<const arrow::BinaryArray*>(batch->column(2).get());
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                int32_t klen = 0;
                const uint8_t* kptr = key_arr->GetValue(i, &klen);
                // Keep only operator-state rows: leading byte >= kNumKeyGroups
                // (the reserved kOperatorStateKeyPrefix). Keyed rows are dropped.
                if (klen == 0 || static_cast<KeyGroup>(kptr[0]) < kNumKeyGroups)
                    continue;
                int32_t vlen = 0;
                const uint8_t* vptr = val_arr->GetValue(i, &vlen);
                if (auto s = op_b.Append(op_arr->Value(i)); !s.ok())
                    throw_arrow("extract_operator_state (append op)", s);
                if (auto s = key_b.Append(kptr, klen); !s.ok())
                    throw_arrow("extract_operator_state (append key)", s);
                if (auto s = val_b.Append(vptr, vlen); !s.ok())
                    throw_arrow("extract_operator_state (append val)", s);
            }
        }
    }

    std::shared_ptr<arrow::Array> op_arr, key_arr, val_arr;
    if (auto s = op_b.Finish(&op_arr); !s.ok())
        throw_arrow("extract_operator_state (finish op)", s);
    if (auto s = key_b.Finish(&key_arr); !s.ok())
        throw_arrow("extract_operator_state (finish key)", s);
    if (auto s = val_b.Finish(&val_arr); !s.ok())
        throw_arrow("extract_operator_state (finish val)", s);

    auto batch = arrow::RecordBatch::Make(schema, op_arr->length(), {op_arr, key_arr, val_arr});
    auto sink_result = arrow::io::BufferOutputStream::Create();
    if (!sink_result.ok())
        throw_arrow("extract_operator_state (create sink)", sink_result.status());
    auto sink = *sink_result;
    auto writer_result = arrow::ipc::MakeStreamWriter(sink, schema);
    if (!writer_result.ok())
        throw_arrow("extract_operator_state (make writer)", writer_result.status());
    auto writer = *writer_result;
    if (auto s = writer->WriteRecordBatch(*batch); !s.ok())
        throw_arrow("extract_operator_state (write batch)", s);
    if (auto s = writer->Close(); !s.ok())
        throw_arrow("extract_operator_state (close writer)", s);
    auto buf_result = sink->Finish();
    if (!buf_result.ok())
        throw_arrow("extract_operator_state (finish sink)", buf_result.status());
    auto buf = *buf_result;
    std::vector<std::byte> bytes(static_cast<std::size_t>(buf->size()));
    if (buf->size() > 0) {
        std::memcpy(bytes.data(), buf->data(), static_cast<std::size_t>(buf->size()));
    }
    return bytes;
}

}  // namespace clink
