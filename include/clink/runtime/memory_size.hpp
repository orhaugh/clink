#pragma once
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>

#include "clink/core/stream_element.hpp"
#include "clink/runtime/memory_budget.hpp"

namespace clink {
// Referenced Arrow buffers are counted in full, including a slice's parent.
// Multiple queued references are deliberately charged separately.
std::size_t arrow_retained_bytes(const std::shared_ptr<arrow::RecordBatch>& batch);

template <class T>
std::size_t retained_bytes(const T& value) {
    if constexpr (requires { value.retained_bytes(); })
        return value.retained_bytes();
    else
        return sizeof(T);
}
inline std::size_t retained_bytes(const std::string& value) {
    return sizeof(value) + value.capacity() + 1;
}
template <class T>
std::size_t stream_element_retained_bytes(const StreamElement<T>& element) {
    std::size_t bytes = sizeof(element) + sizeof(MemoryReservation);
    if (!element.is_data())
        return bytes;
    const auto& batch = element.as_data();
    const auto& records = batch.materialized_records();
    bytes += records.capacity() * sizeof(Record<T>);
    for (const auto& record : records) {
        const auto retained = retained_bytes(record.value());
        if (retained < sizeof(T))
            throw std::invalid_argument("retained_bytes must include inline size");
        bytes = checked_memory_sum(bytes, retained - sizeof(T));
    }
    if (batch.is_columnar())
        bytes = checked_memory_sum(bytes, arrow_retained_bytes(batch.arrow()));
    return bytes;
}
}  // namespace clink
