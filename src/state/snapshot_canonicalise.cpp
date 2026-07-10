#include "clink/state/snapshot_canonicalise.hpp"

#ifndef CLINK_HAS_ARROW
#error "clink requires CLINK_BUILD_ARROW=ON. The state-snapshot format is Arrow-IPC-only."
#endif

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

#include "clink/state/changelog_state_backend.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace clink {

std::vector<std::byte> canonicalise_state_snapshot(
    std::vector<std::byte> bytes, std::shared_ptr<ExternalMaterializationStore> store) {
    if (bytes.empty()) {
        return bytes;  // an empty snapshot is already (vacuously) canonical
    }
    // Sniff the stream's schema. Opening the reader parses only the schema
    // message, so this is cheap.
    auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(bytes.data()),
                                                  static_cast<int64_t>(bytes.size()));
    auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(
        std::make_shared<arrow::io::BufferReader>(buffer));
    if (!reader_result.ok()) {
        throw std::runtime_error("canonicalise_state_snapshot: not an Arrow IPC stream: " +
                                 reader_result.status().ToString());
    }
    const auto schema = (*reader_result)->schema();

    const bool canonical = schema->num_fields() == 3 && schema->field(0)->name() == "op_id";
    if (canonical) {
        return bytes;
    }
    const bool changelog = schema->num_fields() == 4 && schema->field(0)->name() == "row_kind";
    if (!changelog) {
        throw std::runtime_error("canonicalise_state_snapshot: unrecognised snapshot schema: " +
                                 schema->ToString());
    }

    // Changelog variant: replay through the backend that owns the format
    // (materialisation - resolved via `store` when external - then the
    // log), and re-export the inner's contents canonically.
    auto inner = std::make_shared<InMemoryStateBackend>();
    ChangelogStateBackend replay(inner, std::move(store));
    Snapshot snap;
    snap.bytes = std::move(bytes);
    replay.restore(snap);
    return inner->export_arrow_snapshot();
}

}  // namespace clink
