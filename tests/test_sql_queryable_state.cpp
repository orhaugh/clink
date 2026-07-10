// Queryable state as a serving surface, end to end: a plain SQL GROUP BY
// job on a real JM + TM (loopback, HTTP enabled, mirroring clink_node's
// wiring) exposes its live aggregate state with ZERO user code. The test
// discovers the slot via the TM's slot list, then reads a key mid-run
// through the JM's one-call JSON route (fan-out proxy), and finally
// checks the slot unbinds when the operator closes.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/job_manager.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/task_manager.hpp"
#include "clink/config/json.hpp"
#include "clink/http/http_client.hpp"
#include "clink/http/http_server.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/queryable_state/jm_routes.hpp"
#include "clink/queryable_state/registry.hpp"
#include "clink/queryable_state/server.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/script_runner.hpp"

namespace fs = std::filesystem;

TEST(SqlQueryableState, LiveGroupByStateServedOverHttp) {
    clink::cluster::ensure_built_ins_registered();
    clink::plugin::PluginRegistry reg;
    clink::sql::install(reg);

    // Input: enough rows that the job is still running while we query.
    const auto in_path = fs::temp_directory_path() / "clink_qs_in.ndjson";
    const auto out_path = fs::temp_directory_path() / "clink_qs_out.ndjson";
    fs::remove(in_path);
    fs::remove(out_path);
    constexpr int kAliceRows = 900'000;
    constexpr int kBobRows = 600'000;
    {
        std::ofstream out(in_path, std::ios::trunc);
        for (int i = 0; i < kAliceRows; ++i) {
            out << R"({"usr":"alice","amount":1})" << "\n";
        }
        for (int i = 0; i < kBobRows; ++i) {
            out << R"({"usr":"bob","amount":2})" << "\n";
        }
    }

    // Compile the GROUP BY pipeline to a spec (capture, do not run).
    clink::sql::Catalog catalog;
    std::ostringstream out_s, err_s;
    clink::sql::ScriptIO io{&out_s, &err_s};
    std::vector<clink::cluster::JobGraphSpec> captured;
    auto capture = [&](const clink::cluster::JobGraphSpec& spec, const std::string&) -> int {
        captured.push_back(spec);
        return 0;
    };
    const std::string script =
        "CREATE TABLE qevt (usr TEXT, amount BIGINT) "
        "WITH (connector='file', format='json', path='" +
        in_path.string() +
        "');"
        "CREATE TABLE qout (usr TEXT, total BIGINT) "
        "WITH (connector='file', format='json', path='" +
        out_path.string() +
        "', mode='upsert', primary_key='usr');"
        "INSERT INTO qout SELECT usr, SUM(amount) AS total FROM qevt GROUP BY usr";
    ASSERT_EQ(clink::sql::run_script(script, catalog, {}, io, capture), 0) << err_s.str();
    ASSERT_EQ(captured.size(), 1u);

    // JM + TM with HTTP, wired exactly as clink_node wires them.
    clink::cluster::JobManager jm;
    const auto rpc_port = jm.start();
    clink::http::HttpServer jm_http;
    clink::queryable_state::register_jm_routes(jm_http, jm);
    const auto jm_http_port = jm_http.start("127.0.0.1", 0);
    jm.expect_tms({"qs-tm"});
    clink::http::HttpServer tm_http;
    clink::queryable_state::register_routes(tm_http, clink::queryable_state::Registry::global());
    const auto tm_http_port = tm_http.start("127.0.0.1", 0);
    clink::cluster::TaskManager::Config cfg;
    cfg.slot_count = 8;
    clink::cluster::TaskManager tm("qs-tm", "127.0.0.1", cfg);
    tm.set_advertised_http_port(tm_http_port);
    tm.connect_to_jm("127.0.0.1", rpc_port);
    ASSERT_TRUE(jm.await_registrations(std::chrono::milliseconds{10'000}));

    const auto id = jm.submit_job(captured[0],
                                  clink::cluster::OperatorRegistry::default_instance(),
                                  {},
                                  clink::cluster::CheckpointConfig{},
                                  nullptr);

    // Discover the aggregate's slot from the TM's list (role:subtask:agg),
    // then hit the JM's one-call JSON route with the bare key string.
    clink::http::HttpClient tm_client("127.0.0.1", tm_http_port);
    clink::http::HttpClient jm_client("127.0.0.1", jm_http_port);
    std::string role;
    std::string served_body;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{20};
    while (std::chrono::steady_clock::now() < deadline) {
        if (role.empty()) {
            auto list = tm_client.get("/api/v1/queryable_state");
            if (list.status == 200) {
                auto js = clink::config::parse(list.body);
                for (const auto& s : js.at("json_slots").as_array()) {
                    const auto& composed = s.as_string();
                    const auto sep = composed.find(':');
                    if (sep != std::string::npos && composed.rfind(":agg") == composed.size() - 4) {
                        role = composed.substr(0, sep);
                        break;
                    }
                }
            }
        }
        if (!role.empty()) {
            auto resp = jm_client.get("/api/v1/queryable_state/job/" + std::to_string(id) + "/op/" +
                                      role + "/json/agg?key=alice");
            if (resp.status == 200) {
                served_body = resp.body;
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    ASSERT_FALSE(served_body.empty()) << "never served a live value for 'alice' "
                                      << "(job may have finished before the first poll)";
    auto js = clink::config::parse(served_body);
    EXPECT_EQ(js.at("key").as_string(), "alice");
    const auto& value = js.at("value");
    ASSERT_TRUE(value.is_object()) << served_body;
    EXPECT_EQ(value.at("usr").as_string(), "alice");
    const auto live_total = static_cast<std::int64_t>(value.at("total").as_number());
    EXPECT_GE(live_total, 1);
    EXPECT_LE(live_total, kAliceRows);

    // A key that never existed: 404 through the same route.
    auto missing = jm_client.get("/api/v1/queryable_state/job/" + std::to_string(id) + "/op/" +
                                 role + "/json/agg?key=nobody");
    EXPECT_EQ(missing.status, 404);

    // Scan route: the state-as-table transport. Entries for the live keys
    // come back with the truncation contract honoured.
    auto scan = jm_client.get("/api/v1/queryable_state/job/" + std::to_string(id) + "/op/" + role +
                              "/json/agg/scan?limit=10");
    ASSERT_EQ(scan.status, 200) << scan.body;
    std::size_t live_keys = 0;
    {
        auto sj = clink::config::parse(scan.body);
        ASSERT_TRUE(sj.at("entries").is_array()) << scan.body;
        live_keys = sj.at("entries").as_array().size();
        EXPECT_GE(live_keys, 1u);
        EXPECT_FALSE(sj.at("truncated").as_bool());
        const auto& first_entry = sj.at("entries").as_array()[0];
        EXPECT_TRUE(first_entry.at("value").is_object()) << scan.body;
    }
    if (live_keys >= 2) {
        // With more keys than the limit, the scan says so.
        auto lim = jm_client.get("/api/v1/queryable_state/job/" + std::to_string(id) + "/op/" +
                                 role + "/json/agg/scan?limit=1");
        ASSERT_EQ(lim.status, 200) << lim.body;
        auto lj = clink::config::parse(lim.body);
        EXPECT_EQ(lj.at("entries").as_array().size(), 1u);
        EXPECT_TRUE(lj.at("truncated").as_bool());
    }

    // State-as-table: job B SELECTs job A's LIVE aggregate state through
    // connector='queryable_state' and lands the snapshot in a file - one
    // job's state is another job's table, no sink round-trip.
    const auto b_out = fs::temp_directory_path() / "clink_qs_b_out.ndjson";
    fs::remove(b_out);
    {
        clink::sql::Catalog catalog_b;
        std::ostringstream out_b, err_b;
        clink::sql::ScriptIO io_b{&out_b, &err_b};
        std::vector<clink::cluster::JobGraphSpec> captured_b;
        auto capture_b = [&](const clink::cluster::JobGraphSpec& spec, const std::string&) -> int {
            captured_b.push_back(spec);
            return 0;
        };
        const std::string script_b =
            "CREATE TABLE live_totals (usr TEXT, total BIGINT) "
            "WITH (connector='queryable_state', format='json', jm_host='127.0.0.1', jm_port='" +
            std::to_string(jm_http_port) + "', job_id='" + std::to_string(id) +
            "');"
            "CREATE TABLE snap (usr TEXT, total BIGINT) "
            "WITH (connector='file', format='json', path='" +
            b_out.string() +
            "');"
            "INSERT INTO snap SELECT usr, total FROM live_totals";
        ASSERT_EQ(clink::sql::run_script(script_b, catalog_b, {}, io_b, capture_b), 0)
            << err_b.str();
        ASSERT_EQ(captured_b.size(), 1u);
        const auto id_b = jm.submit_job(captured_b[0],
                                        clink::cluster::OperatorRegistry::default_instance(),
                                        {},
                                        clink::cluster::CheckpointConfig{},
                                        nullptr);
        ASSERT_TRUE(jm.await_job_completion(id_b, std::chrono::milliseconds{20'000}));
        const auto errors_b = jm.job_errors(id_b);
        EXPECT_TRUE(errors_b.empty()) << errors_b[0];
    }
    {
        std::ifstream in(b_out);
        std::string line;
        bool saw_alice = false;
        while (std::getline(in, line)) {
            if (line.empty()) {
                continue;
            }
            auto rj = clink::config::parse(line);
            if (rj.at("usr").as_string() == "alice") {
                saw_alice = true;
                EXPECT_GE(static_cast<std::int64_t>(rj.at("total").as_number()), 1);
            }
        }
        EXPECT_TRUE(saw_alice) << "job B's snapshot has no alice row";
    }
    fs::remove(b_out);

    ASSERT_TRUE(jm.await_job_completion(id, std::chrono::milliseconds{60'000}));
    EXPECT_TRUE(jm.job_errors(id).empty());

    // The operator unbound its slot at close: the registry no longer
    // serves it (fresh lookups 404 at the TM).
    const auto composed = role + ":0:agg";
    EXPECT_FALSE(clink::queryable_state::Registry::global().has_json_slot(composed));

    tm.stop();
    jm.stop();
    tm_http.stop();
    jm_http.stop();
    fs::remove(in_path);
    fs::remove(out_path);
}
