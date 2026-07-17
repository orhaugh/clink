// k8s_smoke_job - a tiny bounded, stateless pipeline packaged as a job plugin
// (.so), used by deploy/helm/clink/kind-smoke.sh to prove a job SUBMITTED to a
// Kubernetes-deployed clink cluster actually runs end-to-end across the
// Workers.
//
//   from_elements([1..5]) -> map(*10) -> filter(>20) -> FileInt64Sink
//
// Bounded source so the job runs to completion; stateless (no key_by / window /
// state) so it needs no state backend; the sink file is the work-done proof:
// after a successful run it contains 30, 40, 50 (10*[3,4,5], filtered > 20) on
// whichever Worker pod ran the sink subtask.
//
// Submit (the .so is baked into the runtime image at /opt/clink/jobs):
//   curl -F job_so=@/opt/clink/jobs/k8s_smoke_job.so \
//        -F job_name=k8s-smoke http://<coordinator>:8081/api/v1/jobs

#include <cstdint>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/job/register_job.hpp"

namespace {

void define_job(clink::api::Pipeline& pipeline) {
    pipeline.from_elements<std::int64_t>({1, 2, 3, 4, 5})
        .map<std::int64_t>([](const std::int64_t& v) { return v * 10; })
        .filter([](const std::int64_t& v) { return v > 20; })
        .sink(clink::api::FileInt64Sink::builder().path("/tmp/clink_k8s_smoke_out.txt").build());
}

}  // namespace

CLINK_REGISTER_JOB("k8s-smoke",
                   "1.0",
                   "bounded stateless pipeline for the kind submission smoke",
                   define_job);
