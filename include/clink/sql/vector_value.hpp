#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/sql/row.hpp"

// SQL-native AI: extracting an embedding vector from a Row column as a contiguous
// float32 buffer, ready for a distance kernel.
//
// v1 reads from the JSON Row array (each element is a JsonValue number). This is
// correct and simple, and it is the only path today because the columnar Row batcher
// does not yet decode arrow::list columns. Honest precision caveat: a float32
// embedding stored in a source rides the JSON wire as a double (float32 -> double ->
// text -> double), then is narrowed back to float32 here; the last narrowing is
// exact but any upstream text round-trip already happened. For KNN ranking this is
// within tolerance, but it is a stated v1 boundary. A future list<float32> sidecar
// path (vector_from_list_cell) avoids the text round-trip entirely.

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

}  // namespace clink::sql
