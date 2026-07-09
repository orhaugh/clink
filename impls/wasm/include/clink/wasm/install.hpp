// clink::wasm::install - register the 'wasm' UDF language loader
// (CREATE FUNCTION ... LANGUAGE wasm) with the process-wide
// UdfLanguageRegistry, and the WASM scalar-UDF machinery behind it.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "clink/plugin/plugin.hpp"

namespace arrow {
class DataType;
}

namespace clink::wasm {

void install(clink::plugin::PluginRegistry& reg);

// Load a WebAssembly module and register `export_name` as the scalar UDF
// `name` in ScalarFunctionRegistry. Exposed for direct (non-SQL) use and
// tests; CREATE FUNCTION reaches it through the language loader. Throws on
// a missing module, unknown export, or a signature that does not match the
// declared types (BIGINT<->i64, INTEGER<->i32, DOUBLE<->f64, REAL<->f32).
// `fuel_limit` bounds per-call execution (0 = the built-in default); a
// call that exhausts its fuel fails that row's evaluation rather than
// hanging the operator thread.
void register_wasm_udf(const std::string& name,
                       const std::vector<std::shared_ptr<arrow::DataType>>& arg_types,
                       const std::shared_ptr<arrow::DataType>& return_type,
                       const std::string& module_path,
                       const std::string& export_name,
                       std::uint64_t fuel_limit = 0);

// Compile WebAssembly text format to binary (a thin wrapper over
// wasmtime's wat2wasm). For tests and tooling. Throws on invalid WAT.
std::vector<std::uint8_t> wat_to_wasm(const std::string& wat);

}  // namespace clink::wasm
