#pragma once

// AggFunctionRegistry - user-defined aggregate functions (UDAFs, SQLOPT-3).
//
// The aggregate counterpart of ScalarFunctionRegistry: a UDAF is a set of
// deterministic native closures forming an accumulator, registered from C++ and
// referenced from SQL by name like SUM/COUNT/AVG. There is no function body to
// interpret, so the model is decision-free and safe.
//
// Accumulator contract (all values are config::JsonValue, so the accumulator
// serialises on the Row wire and at parity with the built-in aggregate state -
// the SQL aggregate operators keep per-group state in-process):
//   init()              -> Acc                 (fresh accumulator for a group)
//   accumulate(acc,args)-> Acc                 (fold one row's args into acc)
//   result(acc)         -> JsonValue           (finalise to the output value)
//   retract(acc,args)   -> Acc   (OPTIONAL)    (invert one row; needed only for
//                                               changelog/retracting input)
//   merge(a,b)          -> Acc   (OPTIONAL)    (combine two accumulators; needed
//                                               only for SESSION-window merge)
// The accumulator is taken by value and returned (a functional fold), so a UDAF
// closure can never retain a reference into the engine's aggregate state.
//
// Consumers: the binder recognises a registered name as an aggregate and types
// it from the declared return type; the aggregate operator (install.cpp)
// dispatches accumulate/retract/finalize/merge to the closures. A UDAF without
// retract is rejected with a clear error on retracting input; without merge it
// is rejected under a SESSION window - never silently miscomputed.
//
// Registration is one-shot at job/plugin load (like AsyncFunctionRegistry /
// ScalarFunctionRegistry), so a lookup may safely copy the entry out.

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "clink/config/json.hpp"

// Forward-declared so this header (pulled into the aggregate-operator path)
// does not drag in the full Arrow type headers; only the binder, which includes
// Arrow anyway, dereferences the return type.
namespace arrow {
class DataType;
}

namespace clink {

class AggFunctionRegistry {
public:
    using Acc = config::JsonValue;
    using InitFn = std::function<Acc()>;
    using AccumFn = std::function<Acc(Acc, const std::vector<config::JsonValue>&)>;
    using ResultFn = std::function<config::JsonValue(const Acc&)>;
    using RetractFn = std::function<Acc(Acc, const std::vector<config::JsonValue>&)>;  // optional
    using MergeFn = std::function<Acc(Acc, Acc)>;                                      // optional

    struct Entry {
        std::shared_ptr<arrow::DataType> return_type;  // declared SQL return type
        InitFn init;
        AccumFn accumulate;
        ResultFn result;
        RetractFn retract;  // null if the UDAF does not support retraction
        MergeFn merge;      // null if the UDAF does not support session merge

        [[nodiscard]] bool has_retract() const { return static_cast<bool>(retract); }
        [[nodiscard]] bool has_merge() const { return static_cast<bool>(merge); }
    };

    void register_function(std::string name,
                           std::shared_ptr<arrow::DataType> return_type,
                           InitFn init,
                           AccumFn accumulate,
                           ResultFn result,
                           RetractFn retract = nullptr,
                           MergeFn merge = nullptr) {
        if (!return_type) {
            throw std::runtime_error("AggFunctionRegistry: null return_type for '" + name + "'");
        }
        if (!init || !accumulate || !result) {
            throw std::runtime_error(
                "AggFunctionRegistry: init, accumulate and result are required for '" + name + "'");
        }
        std::lock_guard<std::mutex> lk(mu_);
        fns_[std::move(name)] = Entry{std::move(return_type),
                                      std::move(init),
                                      std::move(accumulate),
                                      std::move(result),
                                      std::move(retract),
                                      std::move(merge)};
    }

    [[nodiscard]] bool contains(const std::string& name) const {
        std::lock_guard<std::mutex> lk(mu_);
        return fns_.find(name) != fns_.end();
    }

    // Copy the entry out (registration is one-shot at load, so this is race free
    // against concurrent reads on the query path). nullopt if absent.
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

    static AggFunctionRegistry& global() {
        static AggFunctionRegistry instance;
        return instance;
    }

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, Entry> fns_;
};

}  // namespace clink
