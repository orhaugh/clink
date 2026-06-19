// 08 - Pipeline packaged as a job plugin (.so) for cluster submission.
//
// All previous examples ran in-process via LocalExecutor. This one uses
// the fluent StreamExecutionEnvironment + CLINK_REGISTER_JOB macro. The
// resulting .so is the the equivalent of a user JAR, submitted
// to a running cluster via the `clink` CLI:
//
//     # In one terminal - start a JobManager:
//     clink_node --role=jm --rpc-port=6123
//
//     # In another - start at least one TaskManager:
//     clink_node --role=tm --jm-host=127.0.0.1 --jm-port=6123
//
//     # Submit the job. The .so path comes from this CMakeLists when
//     # CLINK_BUILD_PLUGIN_EXAMPLE=ON:
//     clink run --jobmanager 127.0.0.1:6123 ./build/08_cluster_job_plugin.so
//
// Pipeline:
//   from_elements([1..5]) -> map(*10) -> filter(>20)
//     -> key_by(v % 2) -> sliding_window(60ms, 60ms) -> aggregate(sum)
//     -> FileInt64Sink

#include <chrono>
#include <cstdint>

#include <clink/api/builtin_connectors.hpp>
#include <clink/api/stream_execution_environment.hpp>
#include <clink/job/register_job.hpp>
#include <clink/time/event_time.hpp>

namespace {

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
        .sink(clink::api::FileInt64Sink::builder().path("/tmp/clink_consumer_08_out.txt").build());
}

}  // namespace

CLINK_REGISTER_JOB("consumer-example-08",
                   "1.0",
                   "sliding-window aggregate; submit via `clink run`",
                   define_job);
