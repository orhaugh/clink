#pragma once

// ScalarFunctionRegistry - user-defined scalar functions (SQLOPT-3).
//
// A scalar UDF is a deterministic native closure over JSON values plus a
// declared SQL return type. It is registered from C++ (exactly like the async
// lookup functions in AsyncFunctionRegistry) and referenced from SQL by name -
// there is no function body to interpret, so the model is fully decision-free
// and safe. Two consumers read this registry:
//   * the binder, to type a call to an otherwise-unknown function (instead of
//     defaulting to text), so a UDF result feeds a typed sink correctly;
//   * the row expression evaluator (json_value_expr.hpp), to invoke the closure
//     when it meets an op that is not a built-in.
//
// Registration happens at job/plugin load and is never undone, so a lookup may
// safely copy the entry out. Thread-safe: a mutex guards the map.
//
// (CREATE FUNCTION DDL - the SQL-level declaration of the same thing - is a thin
// follow-on; this is the registry + binding substance. UDAF / aggregate UDFs
// are a separate, larger increment over the aggregate machinery.)

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "clink/config/json.hpp"

// Forward-declared so this header (pulled into the hot json_value_expr path)
// does not drag in the full Arrow type headers; only the binder, which includes
// Arrow anyway, dereferences the return type.
namespace arrow {
class DataType;
}

namespace clink {

class ScalarFunctionRegistry {
public:
    using Fn = std::function<config::JsonValue(const std::vector<config::JsonValue>&)>;

    struct Entry {
        std::shared_ptr<arrow::DataType> return_type;  // declared SQL return type
        Fn fn;                                         // native implementation
    };

    void register_function(std::string name, std::shared_ptr<arrow::DataType> return_type, Fn fn) {
        if (!fn) {
            throw std::runtime_error("ScalarFunctionRegistry: null function for '" + name + "'");
        }
        std::lock_guard<std::mutex> lk(mu_);
        fns_[std::move(name)] = Entry{std::move(return_type), std::move(fn)};
    }

    [[nodiscard]] bool contains(const std::string& name) const {
        std::lock_guard<std::mutex> lk(mu_);
        return fns_.find(name) != fns_.end();
    }

    // Copy the entry out (registration is one-shot at load, so this is race
    // free against concurrent reads on the query path). nullopt if absent.
    [[nodiscard]] std::optional<Entry> lookup(const std::string& name) const {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = fns_.find(name);
        if (it == fns_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    // Declared return type, or nullptr if the function is not registered.
    [[nodiscard]] std::shared_ptr<arrow::DataType> return_type(const std::string& name) const {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = fns_.find(name);
        return it == fns_.end() ? nullptr : it->second.return_type;
    }

    static ScalarFunctionRegistry& global() {
        static ScalarFunctionRegistry instance;
        return instance;
    }

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, Entry> fns_;
};

}  // namespace clink
