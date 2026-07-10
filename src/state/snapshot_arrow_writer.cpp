#include "clink/state/snapshot_arrow_writer.hpp"

#ifndef CLINK_HAS_ARROW
#error "clink requires CLINK_BUILD_ARROW=ON. The state-snapshot format is Arrow-IPC-only."
#endif

#include <cstring>
#include <stdexcept>
#include <string>

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

}  // namespace

struct SnapshotArrowWriter::Impl {
    arrow::UInt64Builder op_b;
    arrow::BinaryBuilder key_b;
    arrow::BinaryBuilder val_b;
    std::int64_t rows{0};
};

SnapshotArrowWriter::SnapshotArrowWriter(std::size_t reserve_rows)
    : impl_(std::make_unique<Impl>()) {
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
}

std::vector<std::byte> SnapshotArrowWriter::finish(const StateVersionMap& versions) {
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
    if (!versions.empty()) {
        auto meta = std::make_shared<arrow::KeyValueMetadata>();
        meta->Append(kStateVersionsMetadataKey, versions.pack());
        schema = schema->WithMetadata(meta);
    }
    auto batch = arrow::RecordBatch::Make(schema, impl_->rows, {op_arr, key_arr, val_arr});

    auto sink_result = arrow::io::BufferOutputStream::Create();
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
    std::vector<std::byte> bytes(static_cast<std::size_t>(buf->size()));
    if (buf->size() > 0) {
        std::memcpy(bytes.data(), buf->data(), static_cast<std::size_t>(buf->size()));
    }
    return bytes;
}

}  // namespace clink
