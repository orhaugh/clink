// WebAssembly scalar UDFs: WAT fixtures compiled in-process (wat_to_wasm),
// registered through register_wasm_udf, and invoked through the
// ScalarFunctionRegistry closure - the exact path a SQL expression takes.
// Covers the i64/f64 value model, SQL null semantics, signature validation
// at load, the self-contained (no imports) sandbox contract, and the
// per-call fuel budget stopping a runaway loop.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/operators/scalar_function_registry.hpp"
#include "clink/operators/udf_language_registry.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/wasm/install.hpp"

using clink::config::JsonValue;
namespace fs = std::filesystem;

namespace {

// Write a WAT module to a temp .wasm file; returns its path.
fs::path write_module(const std::string& name, const std::string& wat) {
    const auto bytes = clink::wasm::wat_to_wasm(wat);
    const auto path = fs::temp_directory_path() / name;
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()),
              static_cast<std::streamsize>(bytes.size()));
    return path;
}

JsonValue call(const std::string& fn_name, std::vector<JsonValue> args) {
    auto entry = clink::ScalarFunctionRegistry::global().lookup(fn_name);
    EXPECT_TRUE(entry.has_value()) << fn_name << " not registered";
    return entry->fn(args);
}

}  // namespace

TEST(WasmUdf, I64AddCallsThroughRegistry) {
    const auto mod = write_module("clink_wasm_add.wasm", R"((module
        (func (export "add100") (param i64) (result i64)
            local.get 0
            i64.const 100
            i64.add)))");
    clink::wasm::register_wasm_udf(
        "wasm_add100", {arrow::int64()}, arrow::int64(), mod.string(), "add100");

    auto r = call("wasm_add100", {JsonValue{7.0}});
    ASSERT_TRUE(r.is_number());
    EXPECT_EQ(static_cast<std::int64_t>(r.as_number()), 107);
    // Declared return type reaches the binder's view.
    EXPECT_TRUE(
        clink::ScalarFunctionRegistry::global().return_type("wasm_add100")->Equals(arrow::int64()));
    fs::remove(mod);
}

TEST(WasmUdf, F64AndMultipleArgs) {
    const auto mod = write_module("clink_wasm_hyp.wasm", R"((module
        (func (export "hyp2") (param f64 f64) (result f64)
            local.get 0
            local.get 0
            f64.mul
            local.get 1
            local.get 1
            f64.mul
            f64.add)))");
    clink::wasm::register_wasm_udf(
        "wasm_hyp2", {arrow::float64(), arrow::float64()}, arrow::float64(), mod.string(), "hyp2");
    auto r = call("wasm_hyp2", {JsonValue{3.0}, JsonValue{4.0}});
    ASSERT_TRUE(r.is_number());
    EXPECT_DOUBLE_EQ(r.as_number(), 25.0);
    fs::remove(mod);
}

TEST(WasmUdf, NullInNullOutWithoutCalling) {
    const auto mod = write_module("clink_wasm_null.wasm", R"((module
        (func (export "id") (param i64) (result i64) local.get 0)))");
    clink::wasm::register_wasm_udf("wasm_id", {arrow::int64()}, arrow::int64(), mod.string(), "id");
    auto r = call("wasm_id", {JsonValue{nullptr}});
    EXPECT_TRUE(r.is_null());
    fs::remove(mod);
}

TEST(WasmUdf, SignatureMismatchFailsAtLoad) {
    const auto mod = write_module("clink_wasm_sig.wasm", R"((module
        (func (export "f") (param i64) (result i64) local.get 0)))");
    // Declared DOUBLE but the export takes/returns i64 -> load-time error.
    EXPECT_THROW(clink::wasm::register_wasm_udf(
                     "wasm_bad_sig", {arrow::float64()}, arrow::float64(), mod.string(), "f"),
                 std::runtime_error);
    // Unknown export name -> load-time error.
    EXPECT_THROW(clink::wasm::register_wasm_udf(
                     "wasm_bad_export", {arrow::int64()}, arrow::int64(), mod.string(), "nope"),
                 std::runtime_error);
    fs::remove(mod);
}

TEST(WasmUdf, ImportingModuleIsRejected) {
    // The sandbox contract: modules must be self-contained. A module that
    // imports a host function cannot be instantiated.
    const auto mod = write_module("clink_wasm_import.wasm", R"((module
        (import "env" "host" (func (param i64) (result i64)))
        (func (export "f") (param i64) (result i64) local.get 0)))");
    EXPECT_THROW(clink::wasm::register_wasm_udf(
                     "wasm_importer", {arrow::int64()}, arrow::int64(), mod.string(), "f"),
                 std::runtime_error);
    fs::remove(mod);
}

TEST(WasmUdf, FuelBudgetStopsRunawayLoop) {
    const auto mod = write_module("clink_wasm_loop.wasm", R"((module
        (func (export "spin") (param i64) (result i64)
            (loop $l br $l)
            i64.const 0)))");
    clink::wasm::register_wasm_udf("wasm_spin",
                                   {arrow::int64()},
                                   arrow::int64(),
                                   mod.string(),
                                   "spin",
                                   /*fuel_limit=*/10'000);
    // The infinite loop burns its fuel and the CALL fails - the thread
    // returns instead of hanging.
    EXPECT_THROW(call("wasm_spin", {JsonValue{1.0}}), std::runtime_error);
    fs::remove(mod);
}

TEST(WasmUdf, LanguageLoaderRegistersViaCreateFunctionDecl) {
    // The path CREATE FUNCTION takes: install() registers the 'wasm'
    // language; a FunctionDecl loads and registers the UDF; the export
    // name defaults to the function name when AS has one definition.
    clink::plugin::PluginRegistry reg;
    clink::wasm::install(reg);
    ASSERT_TRUE(clink::UdfLanguageRegistry::global().contains("wasm"));

    const auto mod = write_module("clink_wasm_decl.wasm", R"((module
        (func (export "triple") (param i64) (result i64)
            local.get 0
            i64.const 3
            i64.mul)))");
    clink::UdfLanguageRegistry::FunctionDecl decl;
    decl.name = "triple";
    decl.arg_types = {arrow::int64()};
    decl.return_type = arrow::int64();
    decl.definitions = {mod.string()};
    clink::UdfLanguageRegistry::global().load("wasm", decl);

    auto r = call("triple", {JsonValue{14.0}});
    EXPECT_EQ(static_cast<std::int64_t>(r.as_number()), 42);
    fs::remove(mod);
}
