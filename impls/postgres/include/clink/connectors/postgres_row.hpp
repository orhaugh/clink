#pragma once

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace clink {

// PostgresRow is the unit of data emitted by PostgresSource. A row carries
// its column values as text plus a shared pointer to the column-name list
// (every row from the same query shares the same list, so the cost of
// carrying names is one shared_ptr per row).
//
// Decoding into typed structs is left to a downstream MapOperator - the
// connector is intentionally schema-agnostic.
class PostgresRow {
public:
    using Names = std::shared_ptr<const std::vector<std::string>>;

    PostgresRow() = default;
    PostgresRow(Names names, std::vector<std::string> values)
        : names_(std::move(names)), values_(std::move(values)) {}
    // Overload carrying a per-cell null mask (1 = SQL NULL). Empty mask => every
    // cell non-null (back-compat: the text protocol renders a NULL cell as "",
    // indistinguishable without this bit - M5).
    PostgresRow(Names names, std::vector<std::string> values, std::vector<char> nulls)
        : names_(std::move(names)), values_(std::move(values)), nulls_(std::move(nulls)) {}

    std::size_t size() const noexcept { return values_.size(); }
    bool empty() const noexcept { return values_.empty(); }

    const std::string& at(std::size_t i) const { return values_.at(i); }
    const std::string& operator[](std::size_t i) const { return values_[i]; }

    // True iff cell i is SQL NULL (vs an empty-string value).
    bool is_null(std::size_t i) const noexcept { return i < nulls_.size() && nulls_[i] != 0; }
    const std::vector<char>& nulls() const noexcept { return nulls_; }

    const std::string& at(std::string_view name) const {
        if (!names_) {
            throw std::runtime_error("PostgresRow: column names not available");
        }
        for (std::size_t i = 0; i < names_->size(); ++i) {
            if ((*names_)[i] == name) {
                return values_[i];
            }
        }
        throw std::runtime_error("PostgresRow: column not found: " + std::string{name});
    }

    const std::vector<std::string>& values() const noexcept { return values_; }
    Names column_names() const noexcept { return names_; }

private:
    Names names_;
    std::vector<std::string> values_;
    std::vector<char> nulls_;  // per-cell SQL-NULL mask; empty => all non-null
};

}  // namespace clink
