#include "clink/runtime/memory_size.hpp"

#include <unordered_set>

#include <arrow/api.h>

namespace clink {
std::size_t arrow_retained_bytes(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch)
        return 0;
    std::unordered_set<const arrow::Buffer*> seen;
    std::size_t bytes = 0;
    const auto visit = [&](const auto& self,
                           const std::shared_ptr<arrow::ArrayData>& data) -> void {
        for (auto buffer : data->buffers) {
            if (!buffer)
                continue;
            while (buffer->parent())
                buffer = buffer->parent();
            if (seen.insert(buffer.get()).second)
                bytes += static_cast<std::size_t>(buffer->capacity());
        }
        for (const auto& child : data->child_data)
            self(self, child);
        if (data->dictionary)
            self(self, data->dictionary);
    };
    for (const auto& column : batch->columns())
        visit(visit, column->data());
    return bytes;
}
}  // namespace clink
