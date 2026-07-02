#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "clink/config/json.hpp"
#include "clink/sql/row.hpp"

// SQL-native AI: extracting an embedding vector from a Row column as a contiguous
// float32 buffer, ready for a distance kernel.
//
// vector_from_row reads from the JSON Row array (each element is a JsonValue number):
// correct and simple, and it is the path the vector_search operator takes today because
// it materialises rows. Honest precision caveat: a float32 embedding rides the JSON Row
// as a double (float32 -> double -> ... -> double), narrowed back to float32 here.
//
// vector_from_list_cell is the columnar counterpart: it copies float32 straight out of an
// Arrow list<float32> ListArray with no JSON round-trip. The Row columnar batcher now
// carries a list<float32> column as a contiguous Arrow list across the wire (rather than
// a stringified JSON array), so this decode is exact and cheap for a columnar-native
// corpus. Wiring the vector_search operator to read the sidecar directly via this helper
// (a columnar process path, skipping row materialisation) is the remaining follow-on;
// until then the operator uses vector_from_row and the list column is materialised back
// to a JSON array first.

namespace clink::sql {

// The result of extracting a vector cell. A missing / null / non-array column is not
// an error here (present=false); the caller decides whether to skip the row or fail.
struct VectorCell {
    std::vector<float> data;
    bool present = false;  // the column existed and held a non-null array
    bool dim_ok = true;    // false when expected_dim > 0 and data.size() != expected_dim
};

// Read a Row array column as float32. expected_dim == 0 disables the dimension check.
// A non-numeric array element marks the cell present-but-invalid (dim_ok=false, data
// cleared) rather than throwing, so one malformed row does not abort a whole scan.
inline VectorCell vector_from_row(const Row& r,
                                  const std::string& column,
                                  std::size_t expected_dim = 0) {
    VectorCell cell;
    const auto it = r.values.find(column);
    if (it == r.values.end() || it->second.is_null() || !it->second.is_array()) {
        return cell;  // present=false
    }
    const auto& arr = it->second.as_array();
    cell.present = true;
    cell.data.reserve(arr.size());
    for (const auto& e : arr) {
        if (!e.is_number()) {
            cell.data.clear();
            cell.dim_ok = false;
            return cell;
        }
        cell.data.push_back(static_cast<float>(e.as_number()));
    }
    cell.dim_ok = (expected_dim == 0 || cell.data.size() == expected_dim);
    return cell;
}

// Read an embedding directly from an Arrow list<float32> column (the columnar path),
// with no JSON round-trip: the float32 values are copied straight out of the ListArray's
// value buffer. This is the efficient counterpart to vector_from_row for a corpus/query
// that arrives columnar (e.g. a Parquet source with a list<float32> embedding column);
// the row_columnar_batcher carries such a column as a contiguous Arrow list rather than a
// stringified JSON array. A null cell is present=false (the caller skips or fails).
inline VectorCell vector_from_list_cell(const arrow::ListArray& list,
                                        std::int64_t row_idx,
                                        std::size_t expected_dim = 0) {
    VectorCell cell;
    if (row_idx < 0 || row_idx >= list.length() || list.IsNull(row_idx)) {
        return cell;  // present=false
    }
    const auto& values = static_cast<const arrow::FloatArray&>(*list.values());
    const std::int32_t start = list.value_offset(row_idx);
    const std::int32_t end = list.value_offset(row_idx + 1);
    cell.present = true;
    cell.data.reserve(static_cast<std::size_t>(end - start));
    for (std::int32_t j = start; j < end; ++j) {
        cell.data.push_back(values.Value(j));  // float32 read directly, no narrowing
    }
    cell.dim_ok = (expected_dim == 0 || cell.data.size() == expected_dim);
    return cell;
}

}  // namespace clink::sql
