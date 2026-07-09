#pragma once

// UdfLanguageRegistry - pluggable loaders behind CREATE FUNCTION ... LANGUAGE.
//
// ScalarFunctionRegistry (the sibling header) holds runnable scalar UDFs;
// this registry holds the LOADERS that turn a CREATE FUNCTION statement's
// definition into one. Each language (e.g. 'wasm') registers a loader at
// install time; the SQL script runner resolves the statement's LANGUAGE
// through here and hands the loader the declared signature plus the AS
// definition strings. The loader validates, builds the closure, and
// registers it into ScalarFunctionRegistry under the function's name.
//
// Kept next to ScalarFunctionRegistry in the operators layer (not sql/) so
// language impls (impls/wasm) can install a loader without linking the SQL
// frontend, exactly like connectors register factories.

#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace arrow {
class DataType;
}

namespace clink {

class UdfLanguageRegistry {
public:
    // What the SQL layer hands a loader: the declared signature plus the
    // CREATE FUNCTION ... AS 'definition'[, 'definition2'] strings verbatim
    // (for LANGUAGE wasm: the module path, and optionally the export name -
    // defaulting to the function name).
    struct FunctionDecl {
        std::string name;
        std::vector<std::shared_ptr<arrow::DataType>> arg_types;
        std::shared_ptr<arrow::DataType> return_type;
        std::vector<std::string> definitions;
    };

    // Loads the declared function and registers it (into
    // ScalarFunctionRegistry) under decl.name. Throws with a clear message
    // on a bad definition (missing module, unknown export, bad signature).
    using Loader = std::function<void(const FunctionDecl& decl)>;

    void register_language(std::string language, Loader loader) {
        if (!loader) {
            throw std::runtime_error("UdfLanguageRegistry: null loader for '" + language + "'");
        }
        std::lock_guard<std::mutex> lk(mu_);
        loaders_[std::move(language)] = std::move(loader);
    }

    [[nodiscard]] bool contains(const std::string& language) const {
        std::lock_guard<std::mutex> lk(mu_);
        return loaders_.find(language) != loaders_.end();
    }

    // Resolve and invoke the language's loader for `decl`. Throws when the
    // language has no registered loader (e.g. the impl was not built).
    void load(const std::string& language, const FunctionDecl& decl) const {
        Loader loader;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = loaders_.find(language);
            if (it == loaders_.end()) {
                throw std::runtime_error(
                    "CREATE FUNCTION: no loader registered for LANGUAGE '" + language +
                    "' (is the impl built? e.g. wasm needs -DCLINK_WITH_WASM=ON)");
            }
            loader = it->second;
        }
        loader(decl);
    }

    static UdfLanguageRegistry& global() {
        static UdfLanguageRegistry instance;
        return instance;
    }

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, Loader> loaders_;
};

}  // namespace clink
