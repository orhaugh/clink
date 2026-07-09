// WebAssembly scalar UDFs over the wasmtime C API.
//
// Model: one shared wasmtime engine per process (thread-safe, owns the JIT);
// one module+store+instance per registered UDF. A wasmtime store is
// single-threaded, so calls serialise on a per-UDF mutex - correct at any
// parallelism, contended only when several subtasks hammer the SAME
// function concurrently (a per-thread instance pool is the recorded
// follow-on). Sandboxing: modules get no WASI, no imports, and a per-call
// fuel budget, so a hostile or buggy UDF cannot reach the host or hang the
// operator thread - fuel exhaustion fails that call with a clear error.
//
// v1 value model: numeric scalars. SQL BIGINT<->i64, INTEGER<->i32,
// DOUBLE<->f64, REAL<->f32, validated against the export's actual
// signature at CREATE FUNCTION time (not per call). NULL handling is
// SQL-style: any NULL argument short-circuits to a NULL result without
// entering the module. Strings/BYTEA need the guest-memory protocol and
// are the recorded next increment.

#include <cstring>
#include <fstream>
#include <iterator>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <wasm.h>
#include <wasmtime.h>

#include <arrow/api.h>

#include "clink/config/json.hpp"
#include "clink/operators/scalar_function_registry.hpp"
#include "clink/operators/udf_language_registry.hpp"
#include "clink/wasm/install.hpp"

namespace clink::wasm {

namespace {

constexpr std::uint64_t kDefaultFuelPerCall = 100'000'000;  // ~instructions

[[noreturn]] void throw_wasmtime(const char* what, wasmtime_error_t* error, wasm_trap_t* trap) {
    wasm_byte_vec_t msg{};
    if (error != nullptr) {
        wasmtime_error_message(error, &msg);
        wasmtime_error_delete(error);
    } else if (trap != nullptr) {
        wasm_trap_message(trap, &msg);
        wasm_trap_delete(trap);
    }
    std::string text{what};
    if (msg.data != nullptr) {
        text += ": ";
        text.append(msg.data, msg.size);
        wasm_byte_vec_delete(&msg);
    }
    throw std::runtime_error(text);
}

// The process-wide engine: JIT-compiles modules, thread-safe, fuel enabled.
wasm_engine_t* shared_engine() {
    static wasm_engine_t* engine = [] {
        wasm_config_t* config = wasm_config_new();
        wasmtime_config_consume_fuel_set(config, true);
        return wasm_engine_new_with_config(config);  // takes ownership of config
    }();
    return engine;
}

// SQL type -> wasm value kind. Only the numeric scalar set in v1.
wasm_valkind_t wasm_kind_for(const std::shared_ptr<arrow::DataType>& t) {
    if (t == nullptr) {
        throw std::runtime_error("wasm UDF: null type in declaration");
    }
    switch (t->id()) {
        case arrow::Type::INT64:
            return WASM_I64;
        case arrow::Type::INT32:
            return WASM_I32;
        case arrow::Type::DOUBLE:
            return WASM_F64;
        case arrow::Type::FLOAT:
            return WASM_F32;
        default:
            throw std::runtime_error(
                "wasm UDF: unsupported type '" + t->ToString() +
                "' (v1 supports BIGINT/INTEGER/DOUBLE/REAL; strings are a follow-on)");
    }
}

const char* kind_name(wasm_valkind_t k) {
    switch (k) {
        case WASM_I32:
            return "i32";
        case WASM_I64:
            return "i64";
        case WASM_F32:
            return "f32";
        case WASM_F64:
            return "f64";
        default:
            return "?";
    }
}

// One loaded UDF: store + instance + resolved export, serialised by mu_.
class WasmScalarFunction {
public:
    WasmScalarFunction(const std::string& module_path,
                       const std::string& export_name,
                       std::vector<wasm_valkind_t> param_kinds,
                       wasm_valkind_t result_kind,
                       std::uint64_t fuel_limit)
        : param_kinds_(std::move(param_kinds)),
          result_kind_(result_kind),
          fuel_limit_(fuel_limit == 0 ? kDefaultFuelPerCall : fuel_limit) {
        // Read the module bytes.
        std::ifstream in(module_path, std::ios::binary);
        if (!in) {
            throw std::runtime_error("wasm UDF: cannot open module " + module_path);
        }
        std::vector<std::uint8_t> bytes{std::istreambuf_iterator<char>{in},
                                        std::istreambuf_iterator<char>{}};

        store_ = wasmtime_store_new(shared_engine(), nullptr, nullptr);
        context_ = wasmtime_store_context(store_);

        wasmtime_module_t* module = nullptr;
        if (auto* err = wasmtime_module_new(shared_engine(), bytes.data(), bytes.size(), &module);
            err != nullptr) {
            throw_wasmtime(("wasm UDF: compile failed for " + module_path).c_str(), err, nullptr);
        }
        // No imports: a UDF module must be self-contained (no WASI, no host
        // functions) - that is the sandbox contract.
        wasm_trap_t* trap = nullptr;
        if (auto* err = wasmtime_instance_new(context_, module, nullptr, 0, &instance_, &trap);
            err != nullptr || trap != nullptr) {
            wasmtime_module_delete(module);
            throw_wasmtime(("wasm UDF: instantiate failed for " + module_path +
                            " (imports are not supported - the module must be self-contained)")
                               .c_str(),
                           err,
                           trap);
        }
        wasmtime_module_delete(module);

        wasmtime_extern_t item{};
        if (!wasmtime_instance_export_get(
                context_, &instance_, export_name.data(), export_name.size(), &item) ||
            item.kind != WASMTIME_EXTERN_FUNC) {
            throw std::runtime_error("wasm UDF: module " + module_path +
                                     " has no exported function '" + export_name + "'");
        }
        func_ = item.of.func;

        validate_signature_(module_path, export_name);
    }

    ~WasmScalarFunction() {
        if (store_ != nullptr) {
            wasmtime_store_delete(store_);
        }
    }

    WasmScalarFunction(const WasmScalarFunction&) = delete;
    WasmScalarFunction& operator=(const WasmScalarFunction&) = delete;

    // The ScalarFunctionRegistry closure body: JSON values in/out.
    config::JsonValue call(const std::vector<config::JsonValue>& args) {
        if (args.size() != param_kinds_.size()) {
            throw std::runtime_error("wasm UDF: expected " + std::to_string(param_kinds_.size()) +
                                     " argument(s), got " + std::to_string(args.size()));
        }
        // SQL null semantics: any null in -> null out, no call.
        for (const auto& a : args) {
            if (a.is_null()) {
                return config::JsonValue{nullptr};
            }
        }
        std::vector<wasmtime_val_t> params(param_kinds_.size());
        for (std::size_t i = 0; i < param_kinds_.size(); ++i) {
            if (!args[i].is_number()) {
                throw std::runtime_error("wasm UDF: argument " + std::to_string(i + 1) +
                                         " is not numeric");
            }
            const double v = args[i].as_number();
            switch (param_kinds_[i]) {
                case WASM_I64:
                    params[i].kind = WASMTIME_I64;
                    params[i].of.i64 = static_cast<std::int64_t>(v);
                    break;
                case WASM_I32:
                    params[i].kind = WASMTIME_I32;
                    params[i].of.i32 = static_cast<std::int32_t>(v);
                    break;
                case WASM_F64:
                    params[i].kind = WASMTIME_F64;
                    params[i].of.f64 = v;
                    break;
                default:
                    params[i].kind = WASMTIME_F32;
                    params[i].of.f32 = static_cast<float>(v);
                    break;
            }
        }

        wasmtime_val_t result{};
        {
            std::lock_guard lk(mu_);
            // Refill the fuel budget for THIS call; exhaustion traps the
            // call, not the process.
            if (auto* err = wasmtime_context_set_fuel(context_, fuel_limit_); err != nullptr) {
                throw_wasmtime("wasm UDF: set_fuel failed", err, nullptr);
            }
            wasm_trap_t* trap = nullptr;
            if (auto* err = wasmtime_func_call(
                    context_, &func_, params.data(), params.size(), &result, 1, &trap);
                err != nullptr || trap != nullptr) {
                throw_wasmtime("wasm UDF: call failed (fuel exhausted or trapped)", err, trap);
            }
        }

        switch (result.kind) {
            case WASMTIME_I64:
                return config::JsonValue{static_cast<double>(result.of.i64)};
            case WASMTIME_I32:
                return config::JsonValue{static_cast<double>(result.of.i32)};
            case WASMTIME_F64:
                return config::JsonValue{result.of.f64};
            case WASMTIME_F32:
                return config::JsonValue{static_cast<double>(result.of.f32)};
            default:
                throw std::runtime_error("wasm UDF: unsupported result kind");
        }
    }

private:
    // Compare the export's actual (params -> result) against the declared
    // SQL signature, once at load. Mismatches fail CREATE FUNCTION, not rows.
    void validate_signature_(const std::string& module_path, const std::string& export_name) {
        wasm_functype_t* ft = wasmtime_func_type(context_, &func_);
        const wasm_valtype_vec_t* params = wasm_functype_params(ft);
        const wasm_valtype_vec_t* results = wasm_functype_results(ft);
        auto fail = [&](const std::string& detail) {
            wasm_functype_delete(ft);
            throw std::runtime_error("wasm UDF: export '" + export_name + "' of " + module_path +
                                     " does not match the declared signature: " + detail);
        };
        if (params->size != param_kinds_.size()) {
            fail("takes " + std::to_string(params->size) + " parameter(s), declared " +
                 std::to_string(param_kinds_.size()));
        }
        for (std::size_t i = 0; i < params->size; ++i) {
            const auto actual = wasm_valtype_kind(params->data[i]);
            if (actual != param_kinds_[i]) {
                fail("parameter " + std::to_string(i + 1) + " is " +
                     kind_name(static_cast<wasm_valkind_t>(actual)) + ", declared " +
                     kind_name(param_kinds_[i]));
            }
        }
        if (results->size != 1) {
            fail("returns " + std::to_string(results->size) + " value(s), expected 1");
        }
        if (const auto actual = wasm_valtype_kind(results->data[0]); actual != result_kind_) {
            fail(std::string{"returns "} + kind_name(static_cast<wasm_valkind_t>(actual)) +
                 ", declared " + kind_name(result_kind_));
        }
        wasm_functype_delete(ft);
    }

    std::vector<wasm_valkind_t> param_kinds_;
    wasm_valkind_t result_kind_;
    std::uint64_t fuel_limit_;
    wasmtime_store_t* store_{nullptr};
    wasmtime_context_t* context_{nullptr};
    wasmtime_instance_t instance_{};
    wasmtime_func_t func_{};
    std::mutex mu_;
};

}  // namespace

std::vector<std::uint8_t> wat_to_wasm(const std::string& wat) {
    wasm_byte_vec_t out{};
    if (auto* err = wasmtime_wat2wasm(wat.data(), wat.size(), &out); err != nullptr) {
        throw_wasmtime("wat2wasm failed", err, nullptr);
    }
    std::vector<std::uint8_t> bytes(out.size);
    std::memcpy(bytes.data(), out.data, out.size);
    wasm_byte_vec_delete(&out);
    return bytes;
}

void register_wasm_udf(const std::string& name,
                       const std::vector<std::shared_ptr<arrow::DataType>>& arg_types,
                       const std::shared_ptr<arrow::DataType>& return_type,
                       const std::string& module_path,
                       const std::string& export_name,
                       std::uint64_t fuel_limit) {
    std::vector<wasm_valkind_t> param_kinds;
    param_kinds.reserve(arg_types.size());
    for (const auto& t : arg_types) {
        param_kinds.push_back(wasm_kind_for(t));
    }
    const auto result_kind = wasm_kind_for(return_type);
    // Construction validates everything (module, export, signature); the
    // shared_ptr keeps the instance alive inside the registry closure.
    auto fn = std::make_shared<WasmScalarFunction>(
        module_path, export_name, std::move(param_kinds), result_kind, fuel_limit);
    ScalarFunctionRegistry::global().register_function(
        name, return_type, [fn](const std::vector<config::JsonValue>& args) {
            return fn->call(args);
        });
}

void install(clink::plugin::PluginRegistry& /*reg*/) {
    // CREATE FUNCTION ... LANGUAGE wasm AS '<module.wasm>'[, '<export>'].
    // The export name defaults to the function name.
    UdfLanguageRegistry::global().register_language(
        "wasm", [](const UdfLanguageRegistry::FunctionDecl& decl) {
            if (decl.definitions.empty()) {
                throw std::runtime_error("LANGUAGE wasm requires AS '<module.wasm>'");
            }
            const std::string& module_path = decl.definitions[0];
            const std::string export_name =
                decl.definitions.size() > 1 ? decl.definitions[1] : decl.name;
            register_wasm_udf(
                decl.name, decl.arg_types, decl.return_type, module_path, export_name);
        });
}

}  // namespace clink::wasm
