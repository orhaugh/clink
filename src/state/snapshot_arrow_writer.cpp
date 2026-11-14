#include "clink/state/snapshot_arrow_writer.hpp"

#include "clink/runtime/arrow_memory_pool.hpp"
#include "clink/runtime/key_groups.hpp"

#ifndef CLINK_HAS_ARROW
#error "clink requires CLINK_BUILD_ARROW=ON. The state-snapshot format is Arrow-IPC-only."
#endif

#include <cstdio>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

namespace clink {

namespace {

[[noreturn]] void throw_arrow(const std::string& where, const arrow::Status& s) {
    throw std::runtime_error("SnapshotArrowWriter " + where + ": " + s.ToString());
}

std::shared_ptr<arrow::Schema> canonical_schema() {
    static const auto schema = arrow::schema({
        arrow::field("op_id", arrow::uint64(), /*nullable=*/false),
        arrow::field("key_bytes", arrow::binary(), /*nullable=*/false),
        arrow::field("value_bytes", arrow::binary(), /*nullable=*/false),
    });
    return schema;
}

std::string json_string(std::string_view in) {
    std::string out = "\"";
    for (const char c : in) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    out += '"';
    return out;
}

// The clink.operators entry: every operator id with rows, and every operator
// named by an identity, in id order.
std::string operators_json(
    const std::map<std::uint64_t, std::pair<std::uint64_t, std::uint64_t>>& per_op,
    const OperatorIdentities& identities) {
    std::map<std::uint64_t, std::pair<std::uint64_t, std::uint64_t>> all = per_op;
    for (const auto& [op, _] : identities) {
        all.try_emplace(op.value());
    }
    std::string out = "[";
    bool first = true;
    for (const auto& [op, counts] : all) {
        out += first ? "" : ",";
        first = false;
        const auto it = identities.find(OperatorId{op});
        out += "{\"op_id\":\"" + std::to_string(op) + "\",\"uid\":" +
               json_string(it == identities.end() ? std::string_view{} : it->second.uid) +
               ",\"node\":" +
               json_string(it == identities.end() ? std::string_view{} : it->second.node) +
               ",\"keyed_rows\":" + std::to_string(counts.first) +
               ",\"operator_rows\":" + std::to_string(counts.second) + "}";
    }
    out += "]";
    return out;
}

}  // namespace

struct SnapshotArrowWriter::Impl {
    explicit Impl(std::shared_ptr<MemoryBudget> owner)
        : budget(std::move(owner)),
          pool(budget, MemoryCategory::Checkpoint),
          op_b(&pool),
          key_b(&pool),
          val_b(&pool) {}
    std::shared_ptr<MemoryBudget> budget;
    BudgetArrowMemoryPool pool;
    arrow::UInt64Builder op_b;
    arrow::BinaryBuilder key_b;
    arrow::BinaryBuilder val_b;
    std::int64_t rows{0};
    // Keyed and operator-state rows appended per operator id, for the
    // clink.operators entry.
    std::map<std::uint64_t, std::pair<std::uint64_t, std::uint64_t>> per_op;
};

SnapshotArrowWriter::SnapshotArrowWriter(std::size_t reserve_rows,
                                         std::shared_ptr<MemoryBudget> budget)
    : impl_(std::make_unique<Impl>(std::move(budget))) {
    if (reserve_rows > 0) {
        const auto n = static_cast<std::int64_t>(reserve_rows);
        if (auto s = impl_->op_b.Reserve(n); !s.ok()) {
            throw_arrow("ctor (reserve op)", s);
        }
        if (auto s = impl_->key_b.Reserve(n); !s.ok()) {
            throw_arrow("ctor (reserve key)", s);
        }
        if (auto s = impl_->val_b.Reserve(n); !s.ok()) {
            throw_arrow("ctor (reserve val)", s);
        }
    }
}

SnapshotArrowWriter::~SnapshotArrowWriter() = default;

void SnapshotArrowWriter::append(std::uint64_t op_id,
                                 std::string_view key_bytes,
                                 std::string_view value_bytes) {
    if (auto s = impl_->op_b.Append(op_id); !s.ok()) {
        throw_arrow("append (op)", s);
    }
    if (auto s = impl_->key_b.Append(reinterpret_cast<const uint8_t*>(key_bytes.data()),
                                     static_cast<int32_t>(key_bytes.size()));
        !s.ok()) {
        throw_arrow("append (key)", s);
    }
    if (auto s = impl_->val_b.Append(reinterpret_cast<const uint8_t*>(value_bytes.data()),
                                     static_cast<int32_t>(value_bytes.size()));
        !s.ok()) {
        throw_arrow("append (value)", s);
    }
    ++impl_->rows;
    auto& counts = impl_->per_op[op_id];
    const bool keyed =
        key_bytes.empty() ||
        static_cast<KeyGroup>(static_cast<std::uint8_t>(key_bytes.front())) < kNumKeyGroups;
    ++(keyed ? counts.first : counts.second);
}

std::vector<std::byte> SnapshotArrowWriter::finish(const StateVersionMap& versions,
                                                   const StateFingerprintMap& fingerprints,
                                                   OperatorIdScheme scheme,
                                                   const OperatorIdentities& identities) {
    std::shared_ptr<arrow::Array> op_arr, key_arr, val_arr;
    if (auto s = impl_->op_b.Finish(&op_arr); !s.ok()) {
        throw_arrow("finish (op)", s);
    }
    if (auto s = impl_->key_b.Finish(&key_arr); !s.ok()) {
        throw_arrow("finish (key)", s);
    }
    if (auto s = impl_->val_b.Finish(&val_arr); !s.ok()) {
        throw_arrow("finish (value)", s);
    }

    auto schema = canonical_schema();
    {
        auto meta = std::make_shared<arrow::KeyValueMetadata>();
        if (scheme == OperatorIdScheme::Stable) {
            meta->Append(kSnapshotFormatVersionKey, kStableIdSnapshotFormatVersion);
            meta->Append(kOperatorIdSchemeMetadataKey, kStableOperatorIdSchemeName);
        } else {
            meta->Append(kSnapshotFormatVersionKey, kSnapshotFormatVersion);
        }
        if (!impl_->per_op.empty() || !identities.empty()) {
            meta->Append(kOperatorsMetadataKey, operators_json(impl_->per_op, identities));
        }
        if (!versions.empty()) {
            meta->Append(kStateVersionsMetadataKey, versions.pack());
        }
        if (!fingerprints.empty()) {
            meta->Append(kStateFingerprintsMetadataKey, fingerprints.pack());
        }
        schema = schema->WithMetadata(meta);
    }
    auto batch = arrow::RecordBatch::Make(schema, impl_->rows, {op_arr, key_arr, val_arr});

    auto sink_result = arrow::io::BufferOutputStream::Create(4096, &impl_->pool);
    if (!sink_result.ok()) {
        throw_arrow("finish (create sink)", sink_result.status());
    }
    auto sink = *sink_result;
    auto writer_result = arrow::ipc::MakeStreamWriter(sink, schema);
    if (!writer_result.ok()) {
        throw_arrow("finish (make writer)", writer_result.status());
    }
    auto writer = *writer_result;
    if (auto s = writer->WriteRecordBatch(*batch); !s.ok()) {
        throw_arrow("finish (write batch)", s);
    }
    if (auto s = writer->Close(); !s.ok()) {
        throw_arrow("finish (close writer)", s);
    }
    auto buf_result = sink->Finish();
    if (!buf_result.ok()) {
        throw_arrow("finish (finish sink)", buf_result.status());
    }
    auto buf = *buf_result;
    MemoryReservation output_copy(
        impl_->budget, MemoryCategory::Checkpoint, static_cast<std::size_t>(buf->size()));
    std::vector<std::byte> bytes(static_cast<std::size_t>(buf->size()));
    if (buf->size() > 0) {
        std::memcpy(bytes.data(), buf->data(), static_cast<std::size_t>(buf->size()));
    }
    return bytes;
}

}  // namespace clink
