#pragma once

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace clink {

// ClickHouseRow is the unit of data emitted by ClickHouseSource. Mirrors
// PostgresRow: cell values are stringified by the source (using the
// canonical ClickHouse representation for each column type), and every
// row from the same query shares one column-name + column-type list via
// shared_ptr so the per-row overhead is two pointers.
//
// Decoding into typed structs is the consumer's job - a downstream
// MapOperator parses the cells it cares about. The connector is
// deliberately schema-agnostic so the same code services any SELECT.
class ClickHouseRow {
public:
    using Names = std::shared_ptr<const std::vector<std::string>>;
    using Types = std::shared_ptr<const std::vector<std::string>>;

    ClickHouseRow() = default;
    ClickHouseRow(Names names, Types types, std::vector<std::string> values)
        : names_(std::move(names)), types_(std::move(types)), values_(std::move(values)) {}
    // Overload carrying a per-cell null mask (1 = NULL). Empty => all non-null
    // (a Nullable NULL stringifies to "" otherwise, indistinguishable - M5).
    ClickHouseRow(Names names,
                  Types types,
                  std::vector<std::string> values,
                  std::vector<char> nulls)
        : names_(std::move(names)),
          types_(std::move(types)),
          values_(std::move(values)),
          nulls_(std::move(nulls)) {}

    std::size_t size() const noexcept { return values_.size(); }
    bool empty() const noexcept { return values_.empty(); }

    const std::string& at(std::size_t i) const { return values_.at(i); }
    const std::string& operator[](std::size_t i) const { return values_[i]; }

    bool is_null(std::size_t i) const noexcept { return i < nulls_.size() && nulls_[i] != 0; }
    const std::vector<char>& nulls() const noexcept { return nulls_; }

    const std::string& at(std::string_view name) const {
        if (!names_) {
            throw std::runtime_error("ClickHouseRow: column names not available");
        }
        for (std::size_t i = 0; i < names_->size(); ++i) {
            if ((*names_)[i] == name) {
                return values_[i];
            }
        }
        throw std::runtime_error("ClickHouseRow: column not found: " + std::string{name});
    }

    const std::vector<std::string>& values() const noexcept { return values_; }
    Names column_names() const noexcept { return names_; }
    Types column_types() const noexcept { return types_; }

private:
    Names names_;
    Types types_;
    std::vector<std::string> values_;
    std::vector<char> nulls_;  // per-cell NULL mask; empty => all non-null
};

}  // namespace clink
