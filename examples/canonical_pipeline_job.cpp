// Canonical map/filter/window/aggregate pipeline packaged as a job .so.
//
// A single build function that uses the fluent StreamExecutionEnvironment
// API to describe a pipeline. The CLINK_REGISTER_JOB macro wraps it as
// a plugin .so the submitter and TM processes both dlopen.
// Cross-process, the inline lambdas inside .map() / .filter() /
// .key_by() / .aggregate() fire under std::call_once on each TM, so
// the operator-types resolve the same way on every node.
//
// Pipeline:
//
//   from_elements([1, 2, 3, 4, 5])
//     .map(v -> v * 10)
//     .filter(v -> v > 20)            // drops 10, 20 -> 30, 40, 50
//     .assign_timestamps_monotonic(v -> EventTime{v})
//     .key_by(v -> (v / 10) % 2)      // bucket by undivided parity
//     .sliding_window(60ms, 60ms)     // single window spanning all events
//     .aggregate<int64_t>(() -> 0, (acc, v) -> acc + v)
//     .sink(FileInt64Sink to /tmp/canonical_pipeline_out.txt)
//
// Expected window output (single window per key, fires on EOS flush):
//   key 0 (even bucket: 40):        40
//   key 1 (odd bucket: 30, 50):     30 + 50 = 80
//
// Output path is set via the env var CLINK_CANONICAL_OUT_PATH so the
// integration test can control where the sink writes.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/stream_execution_environment.hpp"
#include "clink/job/register_job.hpp"
#include "clink/time/event_time.hpp"

namespace {

std::string output_path() {
    if (const char* p = std::getenv("CLINK_CANONICAL_OUT_PATH"); p != nullptr && *p != '\0') {
        return p;
    }
    return "/tmp/canonical_pipeline_out.txt";
}

void define_job(clink::api::StreamExecutionEnvironment& env) {
    using namespace std::chrono_literals;

    env.from_elements<std::int64_t>({1, 2, 3, 4, 5})
        .map<std::int64_t>([](const std::int64_t& v) { return v * 10; })
        .filter([](const std::int64_t& v) { return v > 20; })
        .assign_timestamps_monotonic([](const std::int64_t& v) { return clink::EventTime{v}; })
        .key_by([](const std::int64_t& v) { return (v / 10) % 2; })
        .sliding_window(60ms, 60ms)
        .aggregate<std::int64_t>(
            []() -> std::int64_t { return 0; },
            [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; })
        .sink(clink::api::FileInt64Sink::builder().path(output_path()).build());
}

}  // namespace

CLINK_REGISTER_JOB("canonical-pipeline",
                   "1.0",
                   "canonical pipeline (map/filter/key_by/window/aggregate)",
                   define_job);
