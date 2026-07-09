// CREATE FUNCTION ... LANGUAGE wasm, end to end through the embedded
// engine: compile a WAT module in-process, declare it in SQL, use it in a
// SELECT expression, and assert the pipeline output - the exact path
// `clink run` drives. Also covers CREATE OR REPLACE and the
// duplicate-name rejection.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/embed/embedded_engine.hpp"
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
