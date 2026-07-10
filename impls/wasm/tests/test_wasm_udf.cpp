// WebAssembly scalar UDFs: WAT fixtures compiled in-process (wat_to_wasm),
// registered through register_wasm_udf, and invoked through the
// ScalarFunctionRegistry closure - the exact path a SQL expression takes.
// Covers the i64/f64 value model, SQL null semantics, signature validation
// at load, the self-contained (no imports) sandbox contract, and the
// per-call fuel budget stopping a runaway loop.

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/operators/agg_function_registry.hpp"
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

// Shared WAT prelude for string fixtures: an exported linear memory plus a
// trivial bump allocator (no dealloc; freeing variants add their own).
namespace {
constexpr const char* kStringPrelude = R"(
    (memory (export "memory") 1)
    (global $heap (mut i32) (i32.const 1024))
    (func $alloc (export "alloc") (param $size i32) (result i32)
        (local $p i32)
        global.get $heap
        local.set $p
        global.get $heap
        local.get $size
        i32.add
        global.set $heap
        local.get $p))";
}  // namespace

TEST(WasmUdfString, UpperRoundTripsAndEmptyStringWorks) {
    // TEXT -> TEXT: the host places the argument via the exported alloc,
    // the function writes an upper-cased copy and returns (ptr << 32) | len.
    const auto mod = write_module("clink_wasm_upper.wasm",
                                  std::string{"(module"} + kStringPrelude +
                                      R"(
        (func (export "upper") (param $ptr i32) (param $len i32) (result i64)
            (local $i i32) (local $c i32) (local $out i32)
            local.get $len
            call $alloc
            local.set $out
            block $done
                loop $l
                    local.get $i
                    local.get $len
                    i32.ge_u
                    br_if $done
                    local.get $ptr
                    local.get $i
                    i32.add
                    i32.load8_u
                    local.set $c
                    local.get $c
                    i32.const 97
                    i32.ge_u
                    local.get $c
                    i32.const 122
                    i32.le_u
                    i32.and
                    if
                        local.get $c
                        i32.const 32
                        i32.sub
                        local.set $c
                    end
                    local.get $out
                    local.get $i
                    i32.add
                    local.get $c
                    i32.store8
                    local.get $i
                    i32.const 1
                    i32.add
                    local.set $i
                    br $l
                end
            end
            local.get $out
            i64.extend_i32_u
            i64.const 32
            i64.shl
            local.get $len
            i64.extend_i32_u
            i64.or)))");
    clink::wasm::register_wasm_udf(
        "wasm_upper", {arrow::utf8()}, arrow::utf8(), mod.string(), "upper");
    auto r = call("wasm_upper", {JsonValue{std::string{"hello, clink 42!"}}});
    ASSERT_TRUE(r.is_string());
    EXPECT_EQ(r.as_string(), "HELLO, CLINK 42!");
    // Empty in, empty out (no guest allocation happens for a 0-length arg).
    auto e = call("wasm_upper", {JsonValue{std::string{}}});
    ASSERT_TRUE(e.is_string());
    EXPECT_EQ(e.as_string(), "");
    // NULL in, NULL out, without entering the module.
    EXPECT_TRUE(call("wasm_upper", {JsonValue{nullptr}}).is_null());
    // Declared return type reaches the binder's view.
    EXPECT_TRUE(
        clink::ScalarFunctionRegistry::global().return_type("wasm_upper")->Equals(arrow::utf8()));
    fs::remove(mod);
}

TEST(WasmUdfString, StringAndNumericArgsMix) {
    // (TEXT, BIGINT) -> BIGINT expands to wasm (i32, i32, i64) -> i64.
    const auto mod =
        write_module("clink_wasm_lenplus.wasm", std::string{"(module"} + kStringPrelude + R"(
        (func (export "len_plus") (param $ptr i32) (param $len i32) (param $n i64) (result i64)
            local.get $len
            i64.extend_i32_u
            local.get $n
            i64.add)))");
    clink::wasm::register_wasm_udf(
        "wasm_len_plus", {arrow::utf8(), arrow::int64()}, arrow::int64(), mod.string(), "len_plus");
    auto r = call("wasm_len_plus", {JsonValue{std::string{"clink"}}, JsonValue{37.0}});
    ASSERT_TRUE(r.is_number());
    EXPECT_EQ(static_cast<std::int64_t>(r.as_number()), 42);
    fs::remove(mod);
}

TEST(WasmUdfString, OutOfBoundsResultIsRejectedNotRead) {
    // A hostile function returns a (ptr, len) far outside its memory; the
    // host must fail the call cleanly instead of reading out of bounds.
    const auto mod = write_module("clink_wasm_oob.wasm",
                                  std::string{"(module"} + kStringPrelude +
                                      R"(
        (func (export "evil") (param $ptr i32) (param $len i32) (result i64)
            i64.const 0x7FFFFF0000001000)))");
    clink::wasm::register_wasm_udf(
        "wasm_oob", {arrow::utf8()}, arrow::utf8(), mod.string(), "evil");
    try {
        call("wasm_oob", {JsonValue{std::string{"x"}}});
        FAIL() << "out-of-bounds result must throw";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string{e.what()}.find("out of the guest memory's bounds"), std::string::npos)
            << e.what();
    }
    fs::remove(mod);
}

TEST(WasmUdfString, MissingAllocOrMemoryFailsAtLoad) {
    // TEXT declared but no alloc export.
    const auto no_alloc = write_module("clink_wasm_noalloc.wasm", R"((module
        (memory (export "memory") 1)
        (func (export "f") (param i32) (param i32) (result i64)
            i64.const 0)))");
    try {
        clink::wasm::register_wasm_udf(
            "wasm_no_alloc", {arrow::utf8()}, arrow::int64(), no_alloc.string(), "f");
        FAIL() << "missing alloc must fail the load";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string{e.what()}.find("alloc"), std::string::npos) << e.what();
    }
    // TEXT declared but no exported linear memory.
    const auto no_mem = write_module("clink_wasm_nomem.wasm", R"((module
        (func (export "alloc") (param i32) (result i32)
            i32.const 0)
        (func (export "f") (param i32) (param i32) (result i64)
            i64.const 0)))");
    try {
        clink::wasm::register_wasm_udf(
            "wasm_no_mem", {arrow::utf8()}, arrow::int64(), no_mem.string(), "f");
        FAIL() << "missing memory must fail the load";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string{e.what()}.find("memory"), std::string::npos) << e.what();
    }
    fs::remove(no_alloc);
    fs::remove(no_mem);
}

TEST(WasmUdfString, DeallocIsCalledForArgAndResultBuffers) {
    // The module counts dealloc calls and returns the count as a digit.
    // Per call the host frees the argument buffer AND the copied-out result
    // buffer, so the counter advances by 2 between calls: "0" then "2".
    const auto mod =
        write_module("clink_wasm_dealloc.wasm", std::string{"(module"} + kStringPrelude + R"(
        (global $dcount (mut i32) (i32.const 0))
        (func (export "dealloc") (param i32) (param i32)
            global.get $dcount
            i32.const 1
            i32.add
            global.set $dcount)
        (func (export "dcount_str") (param $ptr i32) (param $len i32) (result i64)
            (local $out i32)
            i32.const 1
            call $alloc
            local.set $out
            local.get $out
            global.get $dcount
            i32.const 48
            i32.add
            i32.store8
            local.get $out
            i64.extend_i32_u
            i64.const 32
            i64.shl
            i64.const 1
            i64.or)))");
    clink::wasm::register_wasm_udf(
        "wasm_dcount", {arrow::utf8()}, arrow::utf8(), mod.string(), "dcount_str");
    EXPECT_EQ(call("wasm_dcount", {JsonValue{std::string{"a"}}}).as_string(), "0");
    EXPECT_EQ(call("wasm_dcount", {JsonValue{std::string{"b"}}}).as_string(), "2");
    fs::remove(mod);
}

TEST(WasmUdfString, TextWireTypeNameRoundTrips) {
    // The ship path carries UDF types as Arrow ToString() names; the TM
    // maps them back without the SQL frontend. Pin the TEXT agreement.
    EXPECT_TRUE(clink::udf_type_from_wire_name(arrow::utf8()->ToString())->Equals(arrow::utf8()));
}

TEST(WasmUdf, ConcurrentCallsFromManyThreads) {
    // The instance pool: 8 threads hammer ONE registered UDF. Every call
    // borrows an instance exclusively, so results must all be correct
    // (before the pool, a per-UDF mutex serialised these; now they run in
    // parallel). Also mixes a string UDF into half the threads so the
    // guest-memory path is exercised concurrently too.
    const auto add_mod = write_module("clink_wasm_conc_add.wasm", R"((module
        (func (export "add7") (param i64) (result i64)
            local.get 0
            i64.const 7
            i64.add)))");
    clink::wasm::register_wasm_udf(
        "wasm_conc_add", {arrow::int64()}, arrow::int64(), add_mod.string(), "add7");
    const auto echo_mod = write_module("clink_wasm_conc_echo.wasm", R"((module
        (memory (export "memory") 1)
        (global $heap (mut i32) (i32.const 1024))
        (func $alloc (export "alloc") (param $size i32) (result i32)
            (local $p i32)
            global.get $heap
            local.set $p
            global.get $heap
            local.get $size
            i32.add
            global.set $heap
            local.get $p)
        (func (export "echo") (param $ptr i32) (param $len i32) (result i64)
            local.get $ptr
            i64.extend_i32_u
            i64.const 32
            i64.shl
            local.get $len
            i64.extend_i32_u
            i64.or)))");
    clink::wasm::register_wasm_udf(
        "wasm_conc_echo", {arrow::utf8()}, arrow::utf8(), echo_mod.string(), "echo");

    constexpr int kThreads = 8;
    constexpr int kCalls = 200;
    std::atomic<int> wrong{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([t, &wrong] {
            for (int i = 0; i < kCalls; ++i) {
                if (t % 2 == 0) {
                    auto r = call("wasm_conc_add", {JsonValue{static_cast<double>(i)}});
                    if (!r.is_number() || static_cast<int>(r.as_number()) != i + 7) {
                        ++wrong;
                    }
                } else {
                    const std::string s = "t" + std::to_string(t) + "i" + std::to_string(i);
                    auto r = call("wasm_conc_echo", {JsonValue{s}});
                    if (!r.is_string() || r.as_string() != s) {
                        ++wrong;
                    }
                }
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(wrong.load(), 0);
    fs::remove(add_mod);
    fs::remove(echo_mod);
}

// A WAT sum UDAF over the guest-bytes accumulator ABI: acc = 8 bytes
// (little-endian i64). Shared by the registry-level tests below.
namespace {
constexpr const char* kWsumWat = R"((module
    (memory (export "memory") 1)
    (global $heap (mut i32) (i32.const 1024))
    (func $alloc (export "alloc") (param $size i32) (result i32)
        (local $p i32)
        global.get $heap
        local.set $p
        global.get $heap
        local.get $size
        i32.add
        global.set $heap
        local.get $p)
    (func $pack (param $p i32) (param $l i32) (result i64)
        local.get $p
        i64.extend_i32_u
        i64.const 32
        i64.shl
        local.get $l
        i64.extend_i32_u
        i64.or)
    (func $boxed (param $v i64) (result i64)
        (local $out i32)
        i32.const 8
        call $alloc
        local.set $out
        local.get $out
        local.get $v
        i64.store
        local.get $out
        i32.const 8
        call $pack)
    (func (export "wsum_init") (result i64)
        i64.const 0
        call $boxed)
    (func (export "wsum_accumulate") (param $ap i32) (param $al i32) (param $x i64) (result i64)
        local.get $ap
        i64.load
        local.get $x
        i64.add
        call $boxed)
    (func (export "wsum_retract") (param $ap i32) (param $al i32) (param $x i64) (result i64)
        local.get $ap
        i64.load
        local.get $x
        i64.sub
        call $boxed)
    (func (export "wsum_merge") (param $ap i32) (param $al i32) (param $bp i32) (param $bl i32) (result i64)
        local.get $ap
        i64.load
        local.get $bp
        i64.load
        i64.add
        call $boxed)
    (func (export "wsum_result") (param $ap i32) (param $al i32) (result i64)
        local.get $ap
        i64.load)))";
}  // namespace

TEST(WasmUdaf, SumAccumulatesRetractsMergesThroughRegistry) {
    const auto mod = write_module("clink_wasm_udaf_sum.wasm", kWsumWat);
    clink::wasm::register_wasm_udaf(
        "wasm_wsum", {arrow::int64()}, arrow::int64(), mod.string(), "wsum");

    auto entry = clink::AggFunctionRegistry::global().lookup("wasm_wsum");
    ASSERT_TRUE(entry.has_value());
    EXPECT_TRUE(entry->has_retract());
    EXPECT_TRUE(entry->has_merge());
    EXPECT_TRUE(entry->return_type->Equals(arrow::int64()));

    auto acc = entry->init();
    ASSERT_TRUE(acc.is_string());  // opaque packed state
    acc = entry->accumulate(std::move(acc), {JsonValue{10.0}});
    acc = entry->accumulate(std::move(acc), {JsonValue{20.0}});
    acc = entry->accumulate(std::move(acc), {JsonValue{12.0}});
    EXPECT_EQ(static_cast<std::int64_t>(entry->result(acc).as_number()), 42);

    // Retraction inverts a row (changelog input).
    acc = entry->retract(std::move(acc), {JsonValue{12.0}});
    EXPECT_EQ(static_cast<std::int64_t>(entry->result(acc).as_number()), 30);

    // A NULL argument leaves the accumulator unchanged (SQL null row).
    const auto before = acc.as_string();
    acc = entry->accumulate(std::move(acc), {JsonValue{nullptr}});
    EXPECT_EQ(acc.as_string(), before);

    // Merge combines two accumulators (SESSION-window merge).
    auto other = entry->init();
    other = entry->accumulate(std::move(other), {JsonValue{5.0}});
    const auto merged = entry->merge(std::move(acc), std::move(other));
    EXPECT_EQ(static_cast<std::int64_t>(entry->result(merged).as_number()), 35);
    fs::remove(mod);
}

TEST(WasmUdaf, OptionalExportsAndMissingRequiredOnes) {
    // Without _retract/_merge the registry entry reports no such
    // capability (the aggregate operator's checks then reject retracting /
    // SESSION input up front instead of miscomputing).
    const auto minimal = write_module("clink_wasm_udaf_min.wasm", R"((module
        (memory (export "memory") 1)
        (global $heap (mut i32) (i32.const 1024))
        (func $alloc (export "alloc") (param $size i32) (result i32)
            (local $p i32)
            global.get $heap
            local.set $p
            global.get $heap
            local.get $size
            i32.add
            global.set $heap
            local.get $p)
        (func $boxed (param $v i64) (result i64)
            (local $out i32)
            i32.const 8
            call $alloc
            local.set $out
            local.get $out
            local.get $v
            i64.store
            local.get $out
            i64.extend_i32_u
            i64.const 32
            i64.shl
            i64.const 8
            i64.or)
        (func (export "cnt_init") (result i64)
            i64.const 0
            call $boxed)
        (func (export "cnt_accumulate") (param $ap i32) (param $al i32) (param $x i64) (result i64)
            local.get $ap
            i64.load
            i64.const 1
            i64.add
            call $boxed)
        (func (export "cnt_result") (param $ap i32) (param $al i32) (result i64)
            local.get $ap
            i64.load)))");
    clink::wasm::register_wasm_udaf(
        "wasm_cnt", {arrow::int64()}, arrow::int64(), minimal.string(), "cnt");
    auto entry = clink::AggFunctionRegistry::global().lookup("wasm_cnt");
    ASSERT_TRUE(entry.has_value());
    EXPECT_FALSE(entry->has_retract());
    EXPECT_FALSE(entry->has_merge());
    auto acc = entry->init();
    acc = entry->accumulate(std::move(acc), {JsonValue{99.0}});
    acc = entry->accumulate(std::move(acc), {JsonValue{1.0}});
    EXPECT_EQ(static_cast<std::int64_t>(entry->result(acc).as_number()), 2);

    // A module missing a REQUIRED export fails at registration, not rows.
    try {
        clink::wasm::register_wasm_udaf(
            "wasm_bad_agg", {arrow::int64()}, arrow::int64(), minimal.string(), "nope");
        FAIL() << "missing _init export must fail the load";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string{e.what()}.find("nope_init"), std::string::npos) << e.what();
    }
    fs::remove(minimal);
}
