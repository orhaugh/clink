// bench_pipeline_job - minimal CLINK_REGISTER_JOB target the
// cross-process throughput benchmark submits via clink_submit_job.
//
// Same shape as the in-process pipeline clink_bench runs in single
// process: VectorSource<int64_t>(N) -> map(*2) -> map(+1) -> sink.
// The sink discards records (file_int64_sink writes the entire run
// to disk in cross-process mode, which would swamp the measurement
// at high record counts - we just need the "sink received them all"
// signal). The bench harness measures wall time from submit start
// to job-completion ack.
//
// Record count is read from CLINK_BENCH_RECORDS at build_fn time
// so the test can dial it down on slow runners without rebuilding
// the .so. Default 10000 keeps the test under a second.

#include <cstdint>
#include <cstdlib>
#include <string>
#include <vector>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/job/register_job.hpp"

namespace {

std::int64_t record_count() {
    if (const char* p = std::getenv("CLINK_BENCH_RECORDS"); p != nullptr && *p != '\0') {
        try {
            return static_cast<std::int64_t>(std::stoll(p));
        } catch (...) {
            // Fall through to default on parse error.
        }
    }
    return 10'000;
}

std::string output_path() {
    if (const char* p = std::getenv("CLINK_BENCH_OUT"); p != nullptr && *p != '\0') {
        return p;
    }
    return "/tmp/clink_bench_pipeline_out";
}

void define_job(clink::api::Pipeline& pipeline) {
    const auto n = record_count();
    std::vector<std::int64_t> input;
    input.reserve(static_cast<std::size_t>(n));
    for (std::int64_t i = 0; i < n; ++i) {
        input.push_back(i);
    }

    pipeline.from_elements<std::int64_t>(std::move(input))
        .map<std::int64_t>([](const std::int64_t& v) { return v * 2; })
        .map<std::int64_t>([](const std::int64_t& v) { return v + 1; })
        .sink(clink::api::FileInt64Sink::builder().path(output_path()).build());
}

}  // namespace

CLINK_REGISTER_JOB("bench-pipeline",
                   "1.0",
                   "throughput benchmark: VectorSource -> 2x map -> file sink",
                   define_job);
