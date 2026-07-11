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

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/type_fwd.h>

namespace clink {

class UdfLanguageRegistry {
public:
    // What the SQL layer hands a loader: the declared signature plus the
    // CREATE FUNCTION ... AS 'definition'[, 'definition2'] strings verbatim
    // (for LANGUAGE wasm: the module path, and optionally the export name -
    // defaulting to the function name).
    struct FunctionDecl {
        std::string name;
        // Parameter names (parallel to arg_types; may hold empty entries for
        // unnamed parameters). LANGUAGE sql bodies reference parameters by
        // name; wasm resolves by position and ignores these.
        std::vector<std::string> arg_names;
        std::vector<std::shared_ptr<arrow::DataType>> arg_types;
        std::shared_ptr<arrow::DataType> return_type;
        std::vector<std::string> definitions;
        // Shipped payload (e.g. the wasm module bytes), filled when the
        // declaration arrives via a job deploy rather than a local file.
        // When non-empty a loader must prefer it over any path definition:
        // the path is only meaningful on the machine that ran the CREATE.
        std::vector<std::uint8_t> module_bytes;
        // CREATE AGGREGATE (UDAF) vs scalar: an aggregate loader registers
        // an accumulator (init/accumulate/result [, retract][, merge]) into
        // AggFunctionRegistry instead of a scalar closure.
        bool is_aggregate{false};
    };

    // Loads the declared function and registers it (into
    // ScalarFunctionRegistry) under decl.name. Throws with a clear message
    // on a bad definition (missing module, unknown export, bad signature).
    using Loader = std::function<void(const FunctionDecl& decl)>;

    // Produces the shippable payload for a locally-declared function (for
    // LANGUAGE wasm: read the module file). Called by the SQL layer after a
    // successful load so the declaration can travel with a submitted job to
    // TaskManagers that cannot read the local path. Optional per language:
    // a language without one ships nothing (cluster execution then requires
    // out-of-band registration in every process).
    using Packager = std::function<std::vector<std::uint8_t>(const FunctionDecl& decl)>;

    void register_language(std::string language, Loader loader, Packager packager = {}) {
        if (!loader) {
            throw std::runtime_error("UdfLanguageRegistry: null loader for '" + language + "'");
        }
        std::lock_guard<std::mutex> lk(mu_);
        auto& entry = languages_[std::move(language)];
        entry.loader = std::move(loader);
        entry.packager = std::move(packager);
    }

    [[nodiscard]] bool contains(const std::string& language) const {
        std::lock_guard<std::mutex> lk(mu_);
        return languages_.find(language) != languages_.end();
    }

    // Resolve and invoke the language's loader for `decl`. Throws when the
    // language has no registered loader (e.g. the impl was not built).
    void load(const std::string& language, const FunctionDecl& decl) const {
        Loader loader;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = languages_.find(language);
            if (it == languages_.end()) {
                throw std::runtime_error(
                    "CREATE FUNCTION: no loader registered for LANGUAGE '" + language +
                    "' (is the impl built? e.g. wasm needs -DCLINK_WITH_WASM=ON)");
            }
            loader = it->second.loader;
        }
        loader(decl);
    }

    // The shippable payload for a locally-declared function, or empty when
    // the language registered no packager. Throws what the packager throws
    // (e.g. unreadable module file).
    [[nodiscard]] std::vector<std::uint8_t> package(const std::string& language,
                                                    const FunctionDecl& decl) const {
        Packager packager;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = languages_.find(language);
            if (it == languages_.end() || !it->second.packager) {
                return {};
            }
            packager = it->second.packager;
        }
        return packager(decl);
    }

    static UdfLanguageRegistry& global() {
        static UdfLanguageRegistry instance;
        return instance;
    }

private:
    struct LanguageEntry {
        Loader loader;
        Packager packager;
    };
    mutable std::mutex mu_;
    std::unordered_map<std::string, LanguageEntry> languages_;
};

// The wire type-name bridge for shipped declarations. A JobGraphSpec carries
// each UDF argument/return type as Arrow's ToString() name so a TaskManager
// can rebuild the FunctionDecl without linking the SQL frontend's type
// bridge. Deliberately minimal: only the value models UDF loaders accept
// (plus utf8 for future string support); anything else is a clear error at
// deploy rather than a silent misregistration.
inline std::shared_ptr<arrow::DataType> udf_type_from_wire_name(const std::string& name) {
    if (name == "int64") {
        return arrow::int64();
    }
    if (name == "int32") {
        return arrow::int32();
    }
    if (name == "double") {
        return arrow::float64();
    }
    if (name == "float") {
        return arrow::float32();
    }
    if (name == "string" || name == "utf8") {
        return arrow::utf8();
    }
    throw std::runtime_error("UDF deploy: unsupported wire type name '" + name + "'");
}

}  // namespace clink
