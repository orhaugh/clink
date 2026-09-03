// Stable tier: embedding the engine in a C++ process.
// Compile-only; frozen (see README.md). Additions only.

#include <chrono>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "clink/embed/embedded_engine.hpp"

namespace {

using namespace std::chrono_literals;

[[maybe_unused]] void embedded_engine() {
    clink::embed::EngineOptions opts;
    opts.parallelism = 2;
    opts.slots = 16;
    opts.state_backend_uri = "rocksdb:///tmp/state";
    opts.checkpoint_dir = "/tmp/checkpoints";
    opts.checkpoint_interval_ms = 5'000;
    opts.capture_dir = "";
    opts.capture_records = 0;
    opts.catalog_dir = "";
    opts.job_name = "conformance";
    opts.explain = false;
    opts.bare_select_to_print = true;
    std::ostringstream captured;
    opts.out = &std::cout;
    opts.err = &captured;

    clink::embed::EmbeddedEngine engine{std::move(opts)};
    const int rc = engine.execute_script("CREATE TABLE t (a BIGINT) WITH (connector='blackhole')");
    (void)rc;
    const bool ok = engine.await_all([]() { return false; });
    (void)ok;
    (void)engine.await_all();
    engine.cancel_all();
    const std::vector<clink::cluster::JobId> ids = engine.job_ids();
    for (const auto id : ids) {
        (void)engine.await_job(id, 100ms);
        (void)engine.job_errors(id);
        engine.cancel_job(id);
    }
    (void)engine.errors();
    (void)engine.job_count();
    (void)engine.user_cancelled();
    (void)engine.catalog();
    (void)engine.collect_reader("results");
    (void)engine.submit_select_to_collect("SELECT 1");
    (void)engine.execute_update("CREATE TABLE u (a BIGINT) WITH (connector='blackhole')");
}

}  // namespace
