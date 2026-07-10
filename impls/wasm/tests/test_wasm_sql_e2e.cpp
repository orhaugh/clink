// CREATE FUNCTION ... LANGUAGE wasm, end to end through the embedded
// engine: compile a WAT module in-process, declare it in SQL, use it in a
// SELECT expression, and assert the pipeline output - the exact path
// `clink run` drives. Also covers CREATE OR REPLACE, the duplicate-name
// rejection, and cluster module shipping (the compiled spec carries the
// declaration + module bytes; a TaskManager that never saw the CREATE
// registers it at deploy).

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/job_manager.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/task_manager.hpp"
#include "clink/config/json.hpp"
#include "clink/embed/embedded_engine.hpp"
#include "clink/operators/agg_function_registry.hpp"
#include "clink/operators/scalar_function_registry.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/script_runner.hpp"
#include "clink/wasm/install.hpp"

namespace fs = std::filesystem;

namespace {

fs::path write_wasm(const std::string& name, const std::string& wat) {
    const auto bytes = clink::wasm::wat_to_wasm(wat);
    const auto path = fs::temp_directory_path() / name;
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()),
              static_cast<std::streamsize>(bytes.size()));
    return path;
}

std::vector<std::string> read_lines(const fs::path& path) {
    std::vector<std::string> lines;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) {
            lines.push_back(line);
        }
    }
    return lines;
}

}  // namespace

TEST(WasmSqlE2E, CreateFunctionLanguageWasmRunsInPipeline) {
    const auto mod = write_wasm("clink_wasm_e2e_double.wasm", R"((module
        (func (export "double_it") (param i64) (result i64)
            local.get 0
            i64.const 2
            i64.mul)))");
    const auto in_path = fs::temp_directory_path() / "clink_wasm_e2e_in.ndjson";
    const auto out_path = fs::temp_directory_path() / "clink_wasm_e2e_out.ndjson";
    fs::remove(in_path);
    fs::remove(out_path);
    {
        std::ofstream out(in_path, std::ios::trunc);
        out << R"({"user_id":1,"amount":21})" << "\n"
            << R"({"user_id":2,"amount":5})" << "\n";
    }

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    std::ostringstream engine_out;
    opts.out = &engine_out;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    const std::string script =
        "CREATE FUNCTION double_it(x BIGINT) RETURNS BIGINT LANGUAGE wasm AS '" + mod.string() +
        "';"
        "CREATE TABLE evt (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='" +
        in_path.string() +
        "');"
        "CREATE TABLE out_t (user_id BIGINT, doubled BIGINT) "
        "WITH (connector='file', format='json', path='" +
        out_path.string() +
        "');"
        "INSERT INTO out_t SELECT user_id, double_it(amount) AS doubled FROM evt";
    ASSERT_EQ(engine.execute_script(script), 0) << err.str();
    ASSERT_TRUE(engine.await_all()) << err.str();
    EXPECT_NE(engine_out.str().find("created function double_it"), std::string::npos);

    std::map<std::int64_t, std::int64_t> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        ASSERT_TRUE(js.is_object()) << l;
        got[static_cast<std::int64_t>(js.at("user_id").as_number())] =
            static_cast<std::int64_t>(js.at("doubled").as_number());
    }
    EXPECT_EQ(got[1], 42);
    EXPECT_EQ(got[2], 10);
    fs::remove(in_path);
    fs::remove(out_path);
    fs::remove(mod);
}

TEST(WasmSqlE2E, DuplicateNameRejectedUnlessOrReplace) {
    const auto mod = write_wasm("clink_wasm_e2e_dup.wasm", R"((module
        (func (export "dupfn") (param i64) (result i64) local.get 0)))");

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    std::ostringstream out;
    opts.out = &out;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    const std::string create =
        "CREATE FUNCTION dupfn(x BIGINT) RETURNS BIGINT LANGUAGE wasm AS '" + mod.string() + "'";
    ASSERT_EQ(engine.execute_script(create), 0) << err.str();
    // Same name again without OR REPLACE: rejected with a clear message.
    EXPECT_NE(engine.execute_script(create), 0);
    EXPECT_NE(err.str().find("already exists"), std::string::npos) << err.str();
    // OR REPLACE succeeds.
    const std::string replace =
        "CREATE OR REPLACE FUNCTION dupfn(x BIGINT) RETURNS BIGINT LANGUAGE wasm AS '" +
        mod.string() + "'";
    EXPECT_EQ(engine.execute_script(replace), 0) << err.str();
    fs::remove(mod);
}

TEST(WasmSqlE2E, UnknownLanguageErrorsClearly) {
    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    EXPECT_NE(engine.execute_script("CREATE FUNCTION f(x BIGINT) RETURNS BIGINT "
                                    "LANGUAGE cobol AS '/tmp/nope'"),
              0);
    EXPECT_NE(err.str().find("no loader registered for LANGUAGE 'cobol'"), std::string::npos)
        << err.str();
}

TEST(WasmSqlE2E, CreateFunctionShipsModuleWithTheJob) {
    // The cluster path. CREATE FUNCTION registers where the script runs,
    // but the job's expressions evaluate on TaskManagers that never saw
    // the statement (and cannot read the module path). The compiled spec
    // must carry the declaration + module bytes, and the TM must register
    // them at deploy. Proven by REMOVING the local registration between
    // compile and submit: only deploy-time registration can make the job
    // pass. A stripped-spec control shows the test would catch a broken
    // ship (that job fails with the evaluator's unknown-op error).
    clink::cluster::ensure_built_ins_registered();
    clink::plugin::PluginRegistry reg;
    clink::sql::install(reg);
    clink::wasm::install(reg);

    const auto mod = write_wasm("clink_wasm_e2e_ship.wasm", R"((module
        (func (export "ship_dbl") (param i64) (result i64)
            local.get 0
            i64.const 2
            i64.mul)))");
    const auto in_path = fs::temp_directory_path() / "clink_wasm_ship_in.ndjson";
    const auto out_path = fs::temp_directory_path() / "clink_wasm_ship_out.ndjson";
    fs::remove(in_path);
    fs::remove(out_path);
    {
        std::ofstream out(in_path, std::ios::trunc);
        out << R"({"user_id":1,"amount":21})" << "\n"
            << R"({"user_id":2,"amount":5})" << "\n";
    }

    // Compile the script, capturing the spec instead of running it.
    clink::sql::Catalog catalog;
    std::ostringstream out, err;
    clink::sql::ScriptIO io{&out, &err};
    std::vector<clink::cluster::JobGraphSpec> captured;
    auto capture = [&](const clink::cluster::JobGraphSpec& spec, const std::string&) -> int {
        captured.push_back(spec);
        return 0;
    };
    const std::string script =
        "CREATE FUNCTION ship_dbl(x BIGINT) RETURNS BIGINT LANGUAGE wasm AS '" + mod.string() +
        "';"
        "CREATE TABLE evt (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='" +
        in_path.string() +
        "');"
        "CREATE TABLE out_t (user_id BIGINT, doubled BIGINT) "
        "WITH (connector='file', format='json', path='" +
        out_path.string() +
        "');"
        "INSERT INTO out_t SELECT user_id, ship_dbl(amount) AS doubled FROM evt";
    ASSERT_EQ(clink::sql::run_script(script, catalog, {}, io, capture), 0) << err.str();
    ASSERT_EQ(captured.size(), 1u);
    auto spec = captured[0];
    ASSERT_EQ(spec.udfs.size(), 1u);
    EXPECT_EQ(spec.udfs[0].name, "ship_dbl");
    EXPECT_EQ(spec.udfs[0].language, "wasm");
    EXPECT_EQ(spec.udfs[0].return_type, "int64");
    EXPECT_FALSE(spec.udfs[0].module_b64.empty());

    // Simulate the remote cluster: this process no longer knows the
    // function; only the spec payload can bring it back.
    clink::ScalarFunctionRegistry::global().remove("ship_dbl");
    ASSERT_FALSE(clink::ScalarFunctionRegistry::global().contains("ship_dbl"));

    // In-process JM + TM over loopback: the Deploy travels the real wire.
    clink::cluster::JobManager jm;
    const auto port = jm.start();
    jm.expect_tms({"wasm-ship-tm"});
    clink::cluster::TaskManager::Config cfg;
    cfg.slot_count = 8;
    clink::cluster::TaskManager tm("wasm-ship-tm", "127.0.0.1", cfg);
    tm.connect_to_jm("127.0.0.1", port);
    ASSERT_TRUE(jm.await_registrations(std::chrono::milliseconds{10'000}));

    // Control: the declaration stripped -> the job must FAIL with the
    // evaluator's unknown-op error, proving the positive leg below cannot
    // pass by accident.
    {
        auto stripped = spec;
        stripped.udfs.clear();
        const auto id = jm.submit_job(stripped,
                                      clink::cluster::OperatorRegistry::default_instance(),
                                      {},
                                      clink::cluster::CheckpointConfig{},
                                      nullptr);
        ASSERT_TRUE(jm.await_job_completion(id, std::chrono::milliseconds{20'000}));
        const auto errors = jm.job_errors(id);
        ASSERT_FALSE(errors.empty());
        EXPECT_NE(errors[0].find("unknown op"), std::string::npos) << errors[0];
        ASSERT_FALSE(clink::ScalarFunctionRegistry::global().contains("ship_dbl"));
    }
    fs::remove(out_path);

    // The real submit: the TM registers ship_dbl from the shipped payload
    // and the pipeline produces the doubled values.
    {
        const auto id = jm.submit_job(spec,
                                      clink::cluster::OperatorRegistry::default_instance(),
                                      {},
                                      clink::cluster::CheckpointConfig{},
                                      nullptr);
        ASSERT_TRUE(jm.await_job_completion(id, std::chrono::milliseconds{20'000}));
        const auto errors = jm.job_errors(id);
        EXPECT_TRUE(errors.empty()) << errors[0];
    }
    EXPECT_TRUE(clink::ScalarFunctionRegistry::global().contains("ship_dbl"));
    std::map<std::int64_t, std::int64_t> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        ASSERT_TRUE(js.is_object()) << l;
        got[static_cast<std::int64_t>(js.at("user_id").as_number())] =
            static_cast<std::int64_t>(js.at("doubled").as_number());
    }
    EXPECT_EQ(got[1], 42);
    EXPECT_EQ(got[2], 10);

    tm.stop();
    jm.stop();
    fs::remove(in_path);
    fs::remove(out_path);
    fs::remove(mod);
}

TEST(WasmSqlE2E, TextUdfRunsInPipeline) {
    // TEXT in SQL rides the guest-memory ABI: CREATE FUNCTION shout(s TEXT)
    // RETURNS TEXT, applied to a string column, lands upper-cased in the
    // sink. Covers the string path binder-to-evaluator, not just the
    // registry unit tests.
    const auto mod = write_wasm("clink_wasm_e2e_shout.wasm", R"((module
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
        (func (export "shout") (param $ptr i32) (param $len i32) (result i64)
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
    const auto in_path = fs::temp_directory_path() / "clink_wasm_shout_in.ndjson";
    const auto out_path = fs::temp_directory_path() / "clink_wasm_shout_out.ndjson";
    fs::remove(in_path);
    fs::remove(out_path);
    {
        std::ofstream out(in_path, std::ios::trunc);
        out << R"({"user_id":1,"word":"hello"})" << "\n"
            << R"({"user_id":2,"word":"clink"})" << "\n";
    }

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    const std::string script = "CREATE FUNCTION shout(s TEXT) RETURNS TEXT LANGUAGE wasm AS '" +
                               mod.string() +
                               "';"
                               "CREATE TABLE evt (user_id BIGINT, word TEXT) "
                               "WITH (connector='file', format='json', path='" +
                               in_path.string() +
                               "');"
                               "CREATE TABLE out_t (user_id BIGINT, loud TEXT) "
                               "WITH (connector='file', format='json', path='" +
                               out_path.string() +
                               "');"
                               "INSERT INTO out_t SELECT user_id, shout(word) AS loud FROM evt";
    ASSERT_EQ(engine.execute_script(script), 0) << err.str();
    ASSERT_TRUE(engine.await_all()) << err.str();

    std::map<std::int64_t, std::string> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        ASSERT_TRUE(js.is_object()) << l;
        got[static_cast<std::int64_t>(js.at("user_id").as_number())] = js.at("loud").as_string();
    }
    EXPECT_EQ(got[1], "HELLO");
    EXPECT_EQ(got[2], "CLINK");
    fs::remove(in_path);
    fs::remove(out_path);
    fs::remove(mod);
}

TEST(WasmSqlE2E, FunctionPersistsAcrossEngineRestart) {
    // CREATE FUNCTION in a catalog-backed engine survives a restart: the
    // declaration (module payload included) persists to functions/ and a
    // fresh engine re-registers it at script start. The module FILE is
    // deleted in between, so only the persisted payload can explain the
    // reload working.
    const auto cat_dir = fs::temp_directory_path() / "clink_wasm_cat_persist";
    fs::remove_all(cat_dir);
    fs::create_directories(cat_dir);
    const auto mod = write_wasm("clink_wasm_persist.wasm", R"((module
        (func (export "persist_dbl") (param i64) (result i64)
            local.get 0
            i64.const 2
            i64.mul)))");
    const auto in_path = fs::temp_directory_path() / "clink_wasm_persist_in.ndjson";
    const auto out_path = fs::temp_directory_path() / "clink_wasm_persist_out.ndjson";
    fs::remove(in_path);
    fs::remove(out_path);
    {
        std::ofstream out(in_path, std::ios::trunc);
        out << R"({"user_id":1,"amount":21})" << "\n";
    }

    {
        clink::embed::EngineOptions opts;
        opts.catalog_dir = cat_dir.string();
        std::ostringstream err;
        opts.err = &err;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        const std::string script =
            "CREATE FUNCTION persist_dbl(x BIGINT) RETURNS BIGINT LANGUAGE wasm AS '" +
            mod.string() +
            "';"
            "CREATE TABLE pevt (user_id BIGINT, amount BIGINT) "
            "WITH (connector='file', format='json', path='" +
            in_path.string() +
            "');"
            "CREATE TABLE pout (user_id BIGINT, doubled BIGINT) "
            "WITH (connector='file', format='json', path='" +
            out_path.string() + "')";
        ASSERT_EQ(engine.execute_script(script), 0) << err.str();
    }
    EXPECT_TRUE(fs::exists(cat_dir / "functions" / "persist_dbl.json"));

    // Simulate a fresh process: the module file is gone and this process
    // forgot the registration.
    fs::remove(mod);
    clink::ScalarFunctionRegistry::global().remove("persist_dbl");
    ASSERT_FALSE(clink::ScalarFunctionRegistry::global().contains("persist_dbl"));

    {
        clink::embed::EngineOptions opts;
        opts.catalog_dir = cat_dir.string();
        std::ostringstream err;
        opts.err = &err;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        // No re-declare: table and function both come from the catalog.
        ASSERT_EQ(engine.execute_script(
                      "INSERT INTO pout SELECT user_id, persist_dbl(amount) AS doubled FROM pevt"),
                  0)
            << err.str();
        ASSERT_TRUE(engine.await_all()) << err.str();
    }
    const auto lines = read_lines(out_path);
    ASSERT_EQ(lines.size(), 1u);
    auto js = clink::config::parse(lines[0]);
    EXPECT_EQ(static_cast<std::int64_t>(js.at("doubled").as_number()), 42);
    fs::remove(in_path);
    fs::remove(out_path);
    fs::remove_all(cat_dir);
}

TEST(WasmSqlE2E, DropFunctionRemovesRegistrationAndPersistence) {
    const auto cat_dir = fs::temp_directory_path() / "clink_wasm_cat_drop";
    fs::remove_all(cat_dir);
    fs::create_directories(cat_dir);
    const auto mod = write_wasm("clink_wasm_dropfn.wasm", R"((module
        (func (export "dropme") (param i64) (result i64) local.get 0)))");

    clink::embed::EngineOptions opts;
    opts.catalog_dir = cat_dir.string();
    std::ostringstream err;
    opts.err = &err;
    std::ostringstream out;
    opts.out = &out;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    ASSERT_EQ(
        engine.execute_script("CREATE FUNCTION dropme(x BIGINT) RETURNS BIGINT LANGUAGE wasm AS '" +
                              mod.string() + "'"),
        0)
        << err.str();
    ASSERT_TRUE(clink::ScalarFunctionRegistry::global().contains("dropme"));
    ASSERT_TRUE(fs::exists(cat_dir / "functions" / "dropme.json"));

    // DROP removes the registration and the persisted file.
    ASSERT_EQ(engine.execute_script("DROP FUNCTION dropme"), 0) << err.str();
    EXPECT_NE(out.str().find("dropped function dropme"), std::string::npos);
    EXPECT_FALSE(clink::ScalarFunctionRegistry::global().contains("dropme"));
    EXPECT_FALSE(fs::exists(cat_dir / "functions" / "dropme.json"));

    // A second DROP errors; IF EXISTS is silent; re-CREATE works again.
    EXPECT_NE(engine.execute_script("DROP FUNCTION dropme"), 0);
    EXPECT_NE(err.str().find("does not exist"), std::string::npos) << err.str();
    EXPECT_EQ(engine.execute_script("DROP FUNCTION IF EXISTS dropme"), 0) << err.str();
    EXPECT_EQ(
        engine.execute_script("CREATE FUNCTION dropme(x BIGINT) RETURNS BIGINT LANGUAGE wasm AS '" +
                              mod.string() + "'"),
        0)
        << err.str();
    fs::remove(mod);
    fs::remove_all(cat_dir);
}

TEST(WasmSqlE2E, CreateAggregateRunsGroupByAndPersists) {
    // CREATE AGGREGATE ... (language='wasm', ...) end to end: declared in
    // one catalog-backed engine, used from a SECOND engine (the module
    // file deleted in between, so the persisted payload and the aggregate
    // kind flag both prove themselves), driving an unbounded GROUP BY into
    // an upsert file sink.
    const auto cat_dir = fs::temp_directory_path() / "clink_wasm_cat_agg";
    fs::remove_all(cat_dir);
    fs::create_directories(cat_dir);
    const auto mod = write_wasm("clink_wasm_agg_e2e.wasm", R"((module
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
        (func (export "wsum_result") (param $ap i32) (param $al i32) (result i64)
            local.get $ap
            i64.load)))");
    const auto in_path = fs::temp_directory_path() / "clink_wasm_agg_in.ndjson";
    const auto out_path = fs::temp_directory_path() / "clink_wasm_agg_out.ndjson";
    fs::remove(in_path);
    fs::remove(out_path);
    {
        std::ofstream out(in_path, std::ios::trunc);
        out << R"({"usr":"a","amount":10})" << "\n"
            << R"({"usr":"b","amount":5})" << "\n"
            << R"({"usr":"a","amount":20})" << "\n"
            << R"({"usr":"a","amount":12})" << "\n";
    }

    {
        clink::embed::EngineOptions opts;
        opts.catalog_dir = cat_dir.string();
        std::ostringstream err, out;
        opts.err = &err;
        opts.out = &out;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        ASSERT_EQ(engine.execute_script(
                      "CREATE AGGREGATE wsum_e2e(BIGINT) (language = 'wasm', module = '" +
                      mod.string() + "', result_type = 'BIGINT', export = 'wsum')"),
                  0)
            << err.str();
        EXPECT_NE(out.str().find("created aggregate wsum_e2e"), std::string::npos);
        ASSERT_EQ(engine.execute_script("CREATE TABLE aevt (usr TEXT, amount BIGINT) "
                                        "WITH (connector='file', format='json', path='" +
                                        in_path.string() +
                                        "');"
                                        "CREATE TABLE aout (usr TEXT, total BIGINT) "
                                        "WITH (connector='file', format='json', path='" +
                                        out_path.string() + "', mode='upsert', primary_key='usr')"),
                  0)
            << err.str();
    }
    ASSERT_TRUE(fs::exists(cat_dir / "functions" / "wsum_e2e.json"));

    // A fresh process: no module file, no registration - only the catalog.
    fs::remove(mod);
    clink::AggFunctionRegistry::global().remove("wsum_e2e");
    ASSERT_FALSE(clink::AggFunctionRegistry::global().contains("wsum_e2e"));

    {
        clink::embed::EngineOptions opts;
        opts.catalog_dir = cat_dir.string();
        std::ostringstream err;
        opts.err = &err;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        ASSERT_EQ(engine.execute_script(
                      "INSERT INTO aout SELECT usr, wsum_e2e(amount) AS total FROM aevt "
                      "GROUP BY usr"),
                  0)
            << err.str();
        ASSERT_TRUE(engine.await_all()) << err.str();
    }
    std::map<std::string, std::int64_t> got;
    for (const auto& l : read_lines(out_path)) {
        auto js = clink::config::parse(l);
        ASSERT_TRUE(js.is_object()) << l;
        got[js.at("usr").as_string()] = static_cast<std::int64_t>(js.at("total").as_number());
    }
    EXPECT_EQ(got["a"], 42);
    EXPECT_EQ(got["b"], 5);
    fs::remove(in_path);
    fs::remove(out_path);
    fs::remove_all(cat_dir);
}
