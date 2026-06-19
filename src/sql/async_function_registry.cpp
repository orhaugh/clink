#include "clink/sql/async_function_registry.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace clink::sql {

void AsyncFunctionRegistry::register_function(std::string name, AsyncLookupFn fn) {
    if (!fn) {
        throw std::invalid_argument(
            "AsyncFunctionRegistry::register_function: null function for '" + name + "'");
    }
    std::lock_guard lock(mu_);
    fns_[std::move(name)] = std::move(fn);
}

AsyncLookupFn AsyncFunctionRegistry::lookup(const std::string& name) const {
    std::lock_guard lock(mu_);
    auto it = fns_.find(name);
    if (it == fns_.end())
        return {};
    return it->second;
}

bool AsyncFunctionRegistry::contains(const std::string& name) const {
    std::lock_guard lock(mu_);
    return fns_.find(name) != fns_.end();
}

std::vector<std::string> AsyncFunctionRegistry::names() const {
    std::lock_guard lock(mu_);
    std::vector<std::string> out;
    out.reserve(fns_.size());
    for (const auto& [k, _] : fns_) {
        out.push_back(k);
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::size_t AsyncFunctionRegistry::size() const {
    std::lock_guard lock(mu_);
    return fns_.size();
}

AsyncFunctionRegistry& AsyncFunctionRegistry::global() {
    static AsyncFunctionRegistry instance;
    return instance;
}

}  // namespace clink::sql
