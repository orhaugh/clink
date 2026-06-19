#pragma once

// Phase 28c (runtime slice): user-registered async functions keyed by
// name. The runtime factory for `async_lookup_row` reads
// `function_name` from its OperatorSpec params and looks up the
// registered closure here. SQL-frontend wiring (binder lowering of a
// FunctionCall whose name is in this registry to a LogicalAsyncMap)
// is a follow-on; today users construct the JobGraphSpec directly
// with `async_lookup_row` operator entries.
//
// Scope:
// - Process-global singleton + optional non-singleton instances for
//   tests / job-bundle isolation.
// - Thread-safe register / lookup.
// - The registered closure takes a `Row` and returns
//   `async::Task<Row>` (the lookup body is a coroutine). Users
//   embed their I/O contract (HTTP, JDBC, redis, ...) inside the
//   coroutine body; AsyncLookupOperator drives the resulting Tasks.

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "clink/async/task.hpp"
#include "clink/sql/row.hpp"

namespace clink::sql {

// Signature of a user-provided async lookup. Input is a Row from the
// upstream operator's output schema; the body co_returns the same
// Row possibly augmented with extra columns (or a transformed shape).
// The function name in the registry is the only identifier the
// planner needs to wire this into a deployed OperatorSpec.
using AsyncLookupFn = std::function<async::Task<Row>(const Row&)>;

class AsyncFunctionRegistry {
public:
    // Register or replace the lookup keyed by `name`. Thread-safe
    // against concurrent register / lookup. Empty `fn` throws.
    void register_function(std::string name, AsyncLookupFn fn);

    // Returns the registered lookup or an empty function if absent.
    // Callers should check `static_cast<bool>(...)` before invoking.
    AsyncLookupFn lookup(const std::string& name) const;

    // True iff a function with this name is registered.
    [[nodiscard]] bool contains(const std::string& name) const;

    // Snapshot of the registered names. Stable order (sorted).
    [[nodiscard]] std::vector<std::string> names() const;

    [[nodiscard]] std::size_t size() const;

    // Process-global registry. Sole instance shared across operators
    // and SQL deployments. Tests construct local instances to avoid
    // cross-test bleed.
    static AsyncFunctionRegistry& global();

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, AsyncLookupFn> fns_;
};

}  // namespace clink::sql
