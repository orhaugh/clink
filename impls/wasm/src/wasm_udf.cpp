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
// Value model: SQL BIGINT<->i64, INTEGER<->i32, DOUBLE<->f64, REAL<->f32,
// and TEXT via guest linear memory (argument = i32 ptr + i32 len pair,
// result = one i64 packing (ptr << 32) | len; the module exports `memory`
// + `alloc`, optionally `dealloc` - see ValueSpec below). Everything is
// validated against the export's actual signature at CREATE FUNCTION time
// (not per call). NULL handling is SQL-style: any NULL argument
// short-circuits to a NULL result without entering the module.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iterator>
#include <memory>
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

// One declared SQL value in the wasm ABI. Numerics map 1:1 to a wasm value
// kind. TEXT/VARCHAR has no wasm scalar type, so it rides guest linear
// memory: an ARGUMENT expands to an (i32 ptr, i32 len) pair and a RESULT is
// one i64 packing (ptr << 32) | len. A string-bearing module must export
// its linear memory as `memory` and an `alloc(i32 size) -> i32 ptr` the
// host uses to place argument bytes; `dealloc(i32 ptr, i32 size)` is
// optional and, when exported, is called for every host-allocated argument
// buffer and for the returned buffer once copied out (never for
// zero-length buffers). Bytes are UTF-8 verbatim, no transcoding, and
// every guest pointer is bounds-checked host-side.
struct ValueSpec {
    bool is_string{false};
    wasm_valkind_t kind{WASM_I64};  // meaningful when !is_string
};

ValueSpec value_spec_for(const std::shared_ptr<arrow::DataType>& t) {
    if (t == nullptr) {
        throw std::runtime_error("wasm UDF: null type in declaration");
    }
    switch (t->id()) {
        case arrow::Type::INT64:
            return {false, WASM_I64};
        case arrow::Type::INT32:
            return {false, WASM_I32};
        case arrow::Type::DOUBLE:
            return {false, WASM_F64};
        case arrow::Type::FLOAT:
            return {false, WASM_F32};
        case arrow::Type::STRING:
            return {true, WASM_I64};
        default:
            throw std::runtime_error("wasm UDF: unsupported type '" + t->ToString() +
                                     "' (v1 supports BIGINT/INTEGER/DOUBLE/REAL/TEXT)");
    }
}

// The declared signature expanded to raw wasm value kinds: a string
// argument contributes its ptr+len pair; numerics pass through.
std::vector<wasm_valkind_t> expand_param_kinds(const std::vector<ValueSpec>& params) {
    std::vector<wasm_valkind_t> out;
    out.reserve(params.size() * 2);
    for (const auto& p : params) {
        if (p.is_string) {
            out.push_back(WASM_I32);
            out.push_back(WASM_I32);
        } else {
            out.push_back(p.kind);
        }
    }
    return out;
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

// Read a module file into bytes. No size cap here: loading a large module
// locally is legitimate; only SHIPPING one with a job is capped (see the
// packager in install()).
std::vector<std::uint8_t> read_module_file(const std::string& module_path) {
    std::ifstream in(module_path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("wasm UDF: cannot open module " + module_path);
    }
    return std::vector<std::uint8_t>{std::istreambuf_iterator<char>{in},
                                     std::istreambuf_iterator<char>{}};
}

// One loaded UDF: a compiled module plus a pool of instances. wasmtime
// stores are single-threaded, so concurrency comes from INSTANCES: a call
// borrows an idle instance (or instantiates a fresh one from the compiled
// module - the JIT work happened once, at compile), runs the whole guest
// sequence on it exclusively, and returns it to the pool. A call that
// fails discards its instance instead of returning it, so a trapped or
// fuel-exhausted module (whose allocator state is now suspect) can never
// poison later calls. The pool never shrinks; its size is bounded by the
// peak number of concurrent callers. Instances are interchangeable by
// contract: a UDF is a pure function and must not rely on cross-call
// global state.
// `origin` is only for error messages: the module path locally, or a
// shipped-module marker when the bytes arrived with a job deploy.
class WasmScalarFunction {
public:
    WasmScalarFunction(const std::vector<std::uint8_t>& bytes,
                       const std::string& origin,
                       const std::string& export_name,
                       std::vector<ValueSpec> param_specs,
                       ValueSpec result_spec,
                       std::uint64_t fuel_limit)
        : origin_(origin),
          export_name_(export_name),
          param_specs_(std::move(param_specs)),
          expanded_param_kinds_(expand_param_kinds(param_specs_)),
          result_spec_(result_spec),
          uses_strings_(result_spec_.is_string ||
                        std::any_of(param_specs_.begin(),
                                    param_specs_.end(),
                                    [](const ValueSpec& p) { return p.is_string; })),
          fuel_limit_(fuel_limit == 0 ? kDefaultFuelPerCall : fuel_limit) {
        if (auto* err = wasmtime_module_new(shared_engine(), bytes.data(), bytes.size(), &module_);
            err != nullptr) {
            throw_wasmtime(("wasm UDF: compile failed for " + origin_).c_str(), err, nullptr);
        }
        // The first instance validates everything at load time: exports,
        // string ABI, signature. Later instances skip validation - the
        // compiled module cannot change.
        auto first = make_instance_();
        validate_signature_(*first);
        idle_.push_back(std::move(first));
    }

    ~WasmScalarFunction() {
        idle_.clear();  // instances first, then their module
        if (module_ != nullptr) {
            wasmtime_module_delete(module_);
        }
    }

    WasmScalarFunction(const WasmScalarFunction&) = delete;
    WasmScalarFunction& operator=(const WasmScalarFunction&) = delete;

    // The ScalarFunctionRegistry closure body: JSON values in/out.
    config::JsonValue call(const std::vector<config::JsonValue>& args) {
        if (args.size() != param_specs_.size()) {
            throw std::runtime_error("wasm UDF: expected " + std::to_string(param_specs_.size()) +
                                     " argument(s), got " + std::to_string(args.size()));
        }
        // SQL null semantics: any null in -> null out, no call.
        for (const auto& a : args) {
            if (a.is_null()) {
                return config::JsonValue{nullptr};
            }
        }
        // Type-check before touching any guest.
        for (std::size_t i = 0; i < param_specs_.size(); ++i) {
            if (param_specs_[i].is_string) {
                if (!args[i].is_string()) {
                    throw std::runtime_error("wasm UDF: argument " + std::to_string(i + 1) +
                                             " is not a string");
                }
                if (args[i].as_string().size() > 0xFFFFFFFFull) {
                    throw std::runtime_error("wasm UDF: argument " + std::to_string(i + 1) +
                                             " exceeds the wasm32 length limit");
                }
            } else if (!args[i].is_number()) {
                throw std::runtime_error("wasm UDF: argument " + std::to_string(i + 1) +
                                         " is not numeric");
            }
        }
        auto inst = borrow_();
        // A throw from run_ lets `inst` die here: a failed call's instance
        // is discarded, never returned to the pool.
        auto out = run_(*inst, args);
        give_back_(std::move(inst));
        return out;
    }

private:
    // The per-store state one borrowed call runs against. Export handles
    // are per store, so each instance resolves its own.
    struct Instance {
        wasmtime_store_t* store{nullptr};
        wasmtime_context_t* context{nullptr};
        wasmtime_instance_t instance{};
        wasmtime_func_t func{};
        wasmtime_memory_t memory{};
        wasmtime_func_t alloc{};
        wasmtime_func_t dealloc{};
        bool has_dealloc{false};

        Instance() = default;
        Instance(const Instance&) = delete;
        Instance& operator=(const Instance&) = delete;
        ~Instance() {
            if (store != nullptr) {
                wasmtime_store_delete(store);
            }
        }
    };

    std::unique_ptr<Instance> borrow_() {
        {
            std::lock_guard lk(pool_mu_);
            if (!idle_.empty()) {
                auto inst = std::move(idle_.back());
                idle_.pop_back();
                return inst;
            }
        }
        return make_instance_();
    }

    void give_back_(std::unique_ptr<Instance> inst) {
        std::lock_guard lk(pool_mu_);
        idle_.push_back(std::move(inst));
    }

    // Instantiate from the compiled module and resolve the exports this
    // UDF uses.
    std::unique_ptr<Instance> make_instance_() {
        auto inst = std::make_unique<Instance>();
        inst->store = wasmtime_store_new(shared_engine(), nullptr, nullptr);
        inst->context = wasmtime_store_context(inst->store);
        // No imports: a UDF module must be self-contained (no WASI, no host
        // functions) - that is the sandbox contract.
        wasm_trap_t* trap = nullptr;
        if (auto* err =
                wasmtime_instance_new(inst->context, module_, nullptr, 0, &inst->instance, &trap);
            err != nullptr || trap != nullptr) {
            throw_wasmtime(("wasm UDF: instantiate failed for " + origin_ +
                            " (imports are not supported - the module must be self-contained)")
                               .c_str(),
                           err,
                           trap);
        }
        wasmtime_extern_t item{};
        if (!wasmtime_instance_export_get(
                inst->context, &inst->instance, export_name_.data(), export_name_.size(), &item) ||
            item.kind != WASMTIME_EXTERN_FUNC) {
            throw std::runtime_error("wasm UDF: module " + origin_ + " has no exported function '" +
                                     export_name_ + "'");
        }
        inst->func = item.of.func;
        if (uses_strings_) {
            resolve_string_abi_(*inst);
        }
        return inst;
    }

    // Resolve the exports the string ABI needs: the linear memory, the
    // required alloc, and the optional dealloc. Runs per instance (handles
    // are store-local); a module missing them fails the first instance, at
    // CREATE FUNCTION time, with an actionable message.
    void resolve_string_abi_(Instance& inst) {
        wasmtime_extern_t item{};
        if (!wasmtime_instance_export_get(inst.context, &inst.instance, "memory", 6, &item) ||
            item.kind != WASMTIME_EXTERN_MEMORY) {
            throw std::runtime_error("wasm UDF: " + origin_ +
                                     " declares TEXT but exports no linear memory named "
                                     "'memory' (add (memory (export \"memory\") 1))");
        }
        inst.memory = item.of.memory;
        if (!wasmtime_instance_export_get(inst.context, &inst.instance, "alloc", 5, &item) ||
            item.kind != WASMTIME_EXTERN_FUNC) {
            throw std::runtime_error("wasm UDF: " + origin_ +
                                     " declares TEXT but exports no 'alloc' function; the "
                                     "host needs alloc(i32 size) -> i32 ptr to place "
                                     "argument bytes in guest memory");
        }
        inst.alloc = item.of.func;
        {
            wasm_functype_t* ft = wasmtime_func_type(inst.context, &inst.alloc);
            const auto* ps = wasm_functype_params(ft);
            const auto* rs = wasm_functype_results(ft);
            const bool ok = ps->size == 1 && wasm_valtype_kind(ps->data[0]) == WASM_I32 &&
                            rs->size == 1 && wasm_valtype_kind(rs->data[0]) == WASM_I32;
            wasm_functype_delete(ft);
            if (!ok) {
                throw std::runtime_error("wasm UDF: " + origin_ +
                                         " exports 'alloc' but not as (i32 size) -> i32 ptr");
            }
        }
        if (wasmtime_instance_export_get(inst.context, &inst.instance, "dealloc", 7, &item) &&
            item.kind == WASMTIME_EXTERN_FUNC) {
            wasm_functype_t* ft = wasmtime_func_type(inst.context, &item.of.func);
            const auto* ps = wasm_functype_params(ft);
            const auto* rs = wasm_functype_results(ft);
            const bool ok = ps->size == 2 && wasm_valtype_kind(ps->data[0]) == WASM_I32 &&
                            wasm_valtype_kind(ps->data[1]) == WASM_I32 && rs->size == 0;
            wasm_functype_delete(ft);
            if (!ok) {
                throw std::runtime_error("wasm UDF: " + origin_ +
                                         " exports 'dealloc' but not as (i32 ptr, i32 size) -> ()");
            }
            inst.dealloc = item.of.func;
            inst.has_dealloc = true;
        }
    }

    // Guest calls used by the string ABI. The instance is exclusively
    // borrowed; both draw from the fuel budget set at the start of run_.
    static std::uint32_t call_alloc_(Instance& inst, std::uint32_t size) {
        wasmtime_val_t p{};
        p.kind = WASMTIME_I32;
        p.of.i32 = static_cast<std::int32_t>(size);
        wasmtime_val_t r{};
        wasm_trap_t* trap = nullptr;
        if (auto* err = wasmtime_func_call(inst.context, &inst.alloc, &p, 1, &r, 1, &trap);
            err != nullptr || trap != nullptr) {
            throw_wasmtime("wasm UDF: guest alloc failed", err, trap);
        }
        return static_cast<std::uint32_t>(r.of.i32);
    }

    static void call_dealloc_(Instance& inst, std::uint32_t ptr, std::uint32_t size) {
        wasmtime_val_t p[2]{};
        p[0].kind = WASMTIME_I32;
        p[0].of.i32 = static_cast<std::int32_t>(ptr);
        p[1].kind = WASMTIME_I32;
        p[1].of.i32 = static_cast<std::int32_t>(size);
        wasm_trap_t* trap = nullptr;
        if (auto* err = wasmtime_func_call(inst.context, &inst.dealloc, p, 2, nullptr, 0, &trap);
            err != nullptr || trap != nullptr) {
            throw_wasmtime("wasm UDF: guest dealloc failed", err, trap);
        }
    }

    // The whole guest sequence for one call, against an exclusively
    // borrowed instance: fuel, argument allocs + writes, the call, result
    // decode, deallocs - one fuel budget covers it all, so a hostile alloc
    // cannot spin any more than the function itself can.
    config::JsonValue run_(Instance& inst, const std::vector<config::JsonValue>& args) {
        if (auto* err = wasmtime_context_set_fuel(inst.context, fuel_limit_); err != nullptr) {
            throw_wasmtime("wasm UDF: set_fuel failed", err, nullptr);
        }

        // 1. Allocate a guest buffer per (non-empty) string argument. All
        //    allocations happen before any write: alloc may grow the linear
        //    memory, which invalidates previously-taken data pointers.
        struct GuestBuf {
            std::uint32_t ptr{0};
            std::uint32_t len{0};
        };
        std::vector<GuestBuf> arg_bufs(param_specs_.size());
        bool any_string_arg = false;
        for (std::size_t i = 0; i < param_specs_.size(); ++i) {
            if (!param_specs_[i].is_string) {
                continue;
            }
            any_string_arg = true;
            const auto len = static_cast<std::uint32_t>(args[i].as_string().size());
            if (len != 0) {
                arg_bufs[i] = GuestBuf{call_alloc_(inst, len), len};
            }
        }
        // 2. Write the argument bytes through one post-alloc data pointer.
        if (any_string_arg) {
            auto* base = wasmtime_memory_data(inst.context, &inst.memory);
            const auto size = wasmtime_memory_data_size(inst.context, &inst.memory);
            for (std::size_t i = 0; i < param_specs_.size(); ++i) {
                const auto [ptr, len] = arg_bufs[i];
                if (len == 0) {
                    continue;
                }
                if (static_cast<std::uint64_t>(ptr) + len > size) {
                    throw std::runtime_error(
                        "wasm UDF: guest alloc returned an out-of-bounds buffer");
                }
                std::memcpy(base + ptr, args[i].as_string().data(), len);
            }
        }
        // 3. Build the expanded wasm parameter list.
        std::vector<wasmtime_val_t> params;
        params.reserve(expanded_param_kinds_.size());
        for (std::size_t i = 0; i < param_specs_.size(); ++i) {
            if (param_specs_[i].is_string) {
                wasmtime_val_t p{};
                p.kind = WASMTIME_I32;
                p.of.i32 = static_cast<std::int32_t>(arg_bufs[i].ptr);
                params.push_back(p);
                p.of.i32 = static_cast<std::int32_t>(arg_bufs[i].len);
                params.push_back(p);
                continue;
            }
            const double v = args[i].as_number();
            wasmtime_val_t p{};
            switch (param_specs_[i].kind) {
                case WASM_I64:
                    p.kind = WASMTIME_I64;
                    p.of.i64 = static_cast<std::int64_t>(v);
                    break;
                case WASM_I32:
                    p.kind = WASMTIME_I32;
                    p.of.i32 = static_cast<std::int32_t>(v);
                    break;
                case WASM_F64:
                    p.kind = WASMTIME_F64;
                    p.of.f64 = v;
                    break;
                default:
                    p.kind = WASMTIME_F32;
                    p.of.f32 = static_cast<float>(v);
                    break;
            }
            params.push_back(p);
        }

        // 4. The call itself.
        wasmtime_val_t result{};
        {
            wasm_trap_t* trap = nullptr;
            if (auto* err = wasmtime_func_call(
                    inst.context, &inst.func, params.data(), params.size(), &result, 1, &trap);
                err != nullptr || trap != nullptr) {
                throw_wasmtime("wasm UDF: call failed (fuel exhausted or trapped)", err, trap);
            }
        }

        // 5. Decode the result. A string result is copied out BEFORE any
        //    dealloc so a freeing allocator cannot scribble over it.
        config::JsonValue out{nullptr};
        if (result_spec_.is_string) {
            if (result.kind != WASMTIME_I64) {
                throw std::runtime_error(
                    "wasm UDF: TEXT result must be a packed i64 ((ptr << 32) | len)");
            }
            const auto packed = static_cast<std::uint64_t>(result.of.i64);
            const auto ptr = static_cast<std::uint32_t>(packed >> 32);
            const auto len = static_cast<std::uint32_t>(packed & 0xFFFFFFFFull);
            // Re-fetch: the call may have grown the memory.
            const auto* base = wasmtime_memory_data(inst.context, &inst.memory);
            const auto size = wasmtime_memory_data_size(inst.context, &inst.memory);
            if (static_cast<std::uint64_t>(ptr) + len > size) {
                throw std::runtime_error("wasm UDF: result (ptr " + std::to_string(ptr) + ", len " +
                                         std::to_string(len) +
                                         ") is out of the guest memory's bounds");
            }
            out = config::JsonValue{std::string(reinterpret_cast<const char*>(base) + ptr, len)};
            if (inst.has_dealloc && len != 0) {
                call_dealloc_(inst, ptr, len);
            }
        } else {
            switch (result.kind) {
                case WASMTIME_I64:
                    out = config::JsonValue{static_cast<double>(result.of.i64)};
                    break;
                case WASMTIME_I32:
                    out = config::JsonValue{static_cast<double>(result.of.i32)};
                    break;
                case WASMTIME_F64:
                    out = config::JsonValue{result.of.f64};
                    break;
                case WASMTIME_F32:
                    out = config::JsonValue{static_cast<double>(result.of.f32)};
                    break;
                default:
                    throw std::runtime_error("wasm UDF: unsupported result kind");
            }
        }
        // 6. Return the argument buffers to the guest allocator.
        if (inst.has_dealloc) {
            for (const auto& [ptr, len] : arg_bufs) {
                if (len != 0) {
                    call_dealloc_(inst, ptr, len);
                }
            }
        }
        return out;
    }

    // Compare the export's actual (params -> result) against the declared
    // SQL signature (string expansion applied), once at load. Mismatches
    // fail CREATE FUNCTION, not rows.
    void validate_signature_(Instance& inst) {
        wasm_functype_t* ft = wasmtime_func_type(inst.context, &inst.func);
        const wasm_valtype_vec_t* params = wasm_functype_params(ft);
        const wasm_valtype_vec_t* results = wasm_functype_results(ft);
        auto fail = [&](const std::string& detail) {
            wasm_functype_delete(ft);
            throw std::runtime_error("wasm UDF: export '" + export_name_ + "' of " + origin_ +
                                     " does not match the declared signature: " + detail);
        };
        if (params->size != expanded_param_kinds_.size()) {
            fail("takes " + std::to_string(params->size) +
                 " parameter(s), the declared signature expands to " +
                 std::to_string(expanded_param_kinds_.size()) +
                 " (a TEXT argument is an i32 ptr + i32 len pair)");
        }
        for (std::size_t i = 0; i < params->size; ++i) {
            const auto actual = wasm_valtype_kind(params->data[i]);
            if (actual != expanded_param_kinds_[i]) {
                fail("expanded parameter " + std::to_string(i + 1) + " is " +
                     kind_name(static_cast<wasm_valkind_t>(actual)) + ", declared " +
                     kind_name(expanded_param_kinds_[i]));
            }
        }
        if (results->size != 1) {
            fail("returns " + std::to_string(results->size) + " value(s), expected 1");
        }
        const auto expected_result = result_spec_.is_string ? WASM_I64 : result_spec_.kind;
        if (const auto actual = wasm_valtype_kind(results->data[0]); actual != expected_result) {
            fail(std::string{"returns "} + kind_name(static_cast<wasm_valkind_t>(actual)) +
                 (result_spec_.is_string
                      ? ", declared TEXT (expected one i64 packing (ptr << 32) | len)"
                      : ", declared " + std::string{kind_name(result_spec_.kind)}));
        }
        wasm_functype_delete(ft);
    }

    std::string origin_;
    std::string export_name_;
    std::vector<ValueSpec> param_specs_;
    std::vector<wasm_valkind_t> expanded_param_kinds_;
    ValueSpec result_spec_;
    bool uses_strings_{false};
    std::uint64_t fuel_limit_;
    wasmtime_module_t* module_{nullptr};
    std::mutex pool_mu_;
    std::vector<std::unique_ptr<Instance>> idle_;
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

namespace {

// Shared registration body: compile + instantiate + validate from bytes,
// then publish the closure. `origin` labels errors (path or shipped marker).
void register_wasm_udf_bytes(const std::string& name,
                             const std::vector<std::shared_ptr<arrow::DataType>>& arg_types,
                             const std::shared_ptr<arrow::DataType>& return_type,
                             const std::vector<std::uint8_t>& bytes,
                             const std::string& origin,
                             const std::string& export_name,
                             std::uint64_t fuel_limit) {
    std::vector<ValueSpec> param_specs;
    param_specs.reserve(arg_types.size());
    for (const auto& t : arg_types) {
        param_specs.push_back(value_spec_for(t));
    }
    const auto result_spec = value_spec_for(return_type);
    // Construction validates everything (module, export, signature, string
    // ABI exports); the shared_ptr keeps the instance alive inside the
    // registry closure.
    auto fn = std::make_shared<WasmScalarFunction>(
        bytes, origin, export_name, std::move(param_specs), result_spec, fuel_limit);
    ScalarFunctionRegistry::global().register_function(
        name, return_type, [fn](const std::vector<config::JsonValue>& args) {
            return fn->call(args);
        });
}

// Cap on the payload a CREATE FUNCTION ships with a job. Protects the
// control plane (the module travels base64 inside the spec JSON and every
// DeployMsg); real UDF modules are a few KB to a few MB.
constexpr std::size_t kMaxShippedModuleBytes = 32ull * 1024 * 1024;

}  // namespace

void register_wasm_udf(const std::string& name,
                       const std::vector<std::shared_ptr<arrow::DataType>>& arg_types,
                       const std::shared_ptr<arrow::DataType>& return_type,
                       const std::string& module_path,
                       const std::string& export_name,
                       std::uint64_t fuel_limit) {
    register_wasm_udf_bytes(name,
                            arg_types,
                            return_type,
                            read_module_file(module_path),
                            module_path,
                            export_name,
                            fuel_limit);
}

void install(clink::plugin::PluginRegistry& /*reg*/) {
    // CREATE FUNCTION ... LANGUAGE wasm AS '<module.wasm>'[, '<export>'].
    // The export name defaults to the function name. A declaration that
    // arrived with a job deploy carries the module bytes instead of a
    // readable path - prefer them (the path only exists where the CREATE
    // ran). The packager is the other half: it turns a locally-declared
    // function into those shippable bytes.
    UdfLanguageRegistry::global().register_language(
        "wasm",
        [](const UdfLanguageRegistry::FunctionDecl& decl) {
            if (decl.definitions.empty()) {
                throw std::runtime_error("LANGUAGE wasm requires AS '<module.wasm>'");
            }
            const std::string& module_path = decl.definitions[0];
            const std::string export_name =
                decl.definitions.size() > 1 ? decl.definitions[1] : decl.name;
            if (!decl.module_bytes.empty()) {
                register_wasm_udf_bytes(decl.name,
                                        decl.arg_types,
                                        decl.return_type,
                                        decl.module_bytes,
                                        "shipped module '" + module_path + "'",
                                        export_name,
                                        /*fuel_limit=*/0);
                return;
            }
            register_wasm_udf(
                decl.name, decl.arg_types, decl.return_type, module_path, export_name);
        },
        [](const UdfLanguageRegistry::FunctionDecl& decl) -> std::vector<std::uint8_t> {
            if (decl.definitions.empty()) {
                return {};
            }
            auto bytes = read_module_file(decl.definitions[0]);
            if (bytes.size() > kMaxShippedModuleBytes) {
                throw std::runtime_error(
                    "wasm UDF: module " + decl.definitions[0] + " is " +
                    std::to_string(bytes.size()) +
                    " bytes; modules over 32 MiB cannot ship with a job (keep the module "
                    "lean, or register it out of band on every node)");
            }
            return bytes;
        });
}

}  // namespace clink::wasm
