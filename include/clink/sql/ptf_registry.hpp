#pragma once

// PtfRegistry - process-table-function registry (SQLOPT PTF).
//
// A PTF is a keyed stateful Row->Rows function (a KeyedProcessFunction) that a
// job registers from C++ by name and calls from SQL as a polymorphic table
// function in FROM: `SELECT * FROM my_ptf(TABLE events PARTITION BY user_id)`.
// It is the stateful-operator analogue of the scalar/aggregate UDF registries.
//
// Unlike a scalar/aggregate UDF, a PTF must declare its OUTPUT COLUMNS at
// registration: a PTF call in FROM produces a derived table that the outer
// SELECT binds against, so the binder needs the column list at clause-bind time
// (it cannot borrow it from a sink). The factory mints a fresh function per
// build so a PTF instance never carries cross-job mutable state - per-key state
// lives in the function's own KeyedState slots, indexed by current_key().
//
// Registration is one-shot at job/plugin load (like the UDF registries), so a
// lookup may copy the entry out. Resolve through global() ONLY - RTLD_LOCAL +
// static clink_core gives each plugin .so a private singleton, so a default-
// constructed local registry would not see host registrations.

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>

#include "clink/operators/process_function.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/row.hpp"

namespace clink::sql {

class PtfRegistry {
public:
    // Mints a fresh keyed Row->Rows process function for one operator build. K
    // is fixed to std::string in v1 (the engine-wide SQL Row key convention: the
    // \x1f-joined serialised partition-column values).
    using Factory =
        std::function<std::shared_ptr<clink::KeyedProcessFunction<std::string, Row, Row>>()>;

    struct Entry {
        Factory factory;                         // required, non-null
        std::vector<ColumnSpec> output_columns;  // required, non-empty
    };

    void register_function(std::string name,
                           std::vector<ColumnSpec> output_columns,
                           Factory factory) {
        if (name.empty()) {
            throw std::runtime_error("PtfRegistry: empty function name");
        }
        if (output_columns.empty()) {
            throw std::runtime_error("PtfRegistry: '" + name +
                                     "' must declare at least one output column");
        }
        if (!factory) {
            throw std::runtime_error("PtfRegistry: null factory for '" + name + "'");
        }
        std::lock_guard<std::mutex> lk(mu_);
        fns_[std::move(name)] = Entry{std::move(factory), std::move(output_columns)};
    }

    [[nodiscard]] bool contains(const std::string& name) const {
        std::lock_guard<std::mutex> lk(mu_);
        return fns_.find(name) != fns_.end();
    }

    [[nodiscard]] std::optional<Entry> lookup(const std::string& name) const {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = fns_.find(name);
        if (it == fns_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    // The PTF's declared output schema, or nullptr if not registered. Built from
    // the registered column list; used by the binder to register the synthetic
    // derived table before the outer SELECT binds.
    [[nodiscard]] std::shared_ptr<arrow::Schema> output_schema(const std::string& name) const {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = fns_.find(name);
        if (it == fns_.end()) {
            return nullptr;
        }
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(it->second.output_columns.size());
        for (const auto& c : it->second.output_columns) {
            fields.push_back(arrow::field(c.name, c.type));
        }
        return arrow::schema(std::move(fields));
    }

    [[nodiscard]] std::vector<std::string> names() const {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<std::string> out;
        out.reserve(fns_.size());
        for (const auto& [n, _] : fns_) {
            out.push_back(n);
        }
        return out;
    }

    [[nodiscard]] std::size_t size() const {
        std::lock_guard<std::mutex> lk(mu_);
        return fns_.size();
    }

    static PtfRegistry& global() {
        static PtfRegistry instance;
        return instance;
    }

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, Entry> fns_;
};

}  // namespace clink::sql
