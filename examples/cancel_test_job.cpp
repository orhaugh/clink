// cancel_test_job - long-running CLINK_REGISTER_JOB target used by
// the cancel-job integration test. The pipeline keeps emitting one
// int64 every CLINK_CANCEL_TICK_MS milliseconds forever; only
// CancelJob can stop it. Sink discards records by counting.
//
// We register a custom SlowInt64Source inline so the .so doesn't
// depend on a built-in "tick" connector. The source's produce()
// honours Source::cancelled() (inherited from Operator base) which
// is what gives LocalExecutor's cancel-token plumbing something to
// flip when the TaskManager handles CancelJob.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <thread>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/stream_execution_environment.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/job/register_job.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace cancel_test {

class SlowInt64Source final : public clink::Source<std::int64_t> {
public:
    explicit SlowInt64Source(std::chrono::milliseconds tick) : tick_(tick) {}

    bool produce(clink::Emitter<std::int64_t>& out) override {
        if (this->cancelled()) {
            return false;
        }
        clink::Batch<std::int64_t> b;
        b.emplace(counter_++);
        if (!out.emit_data(std::move(b))) {
            return false;
        }
        std::this_thread::sleep_for(tick_);
        return true;  // never naturally exhausts
    }

    std::string name() const override { return "slow_int64_source"; }

private:
    std::chrono::milliseconds tick_;
    std::int64_t counter_{0};
};

std::chrono::milliseconds tick_from_env() {
    if (const char* p = std::getenv("CLINK_CANCEL_TICK_MS"); p != nullptr && *p != '\0') {
        try {
            return std::chrono::milliseconds{std::stoll(p)};
        } catch (...) {
        }
    }
    return std::chrono::milliseconds{20};
}

void define_job(clink::api::StreamExecutionEnvironment& env) {
    const auto tick = tick_from_env();

    // Manually-registered sources via env.registry().register_source<T>
    // need the built-in channel types in TypeRegistry first - the
    // template body looks up channel_for_typeid(typeid(T).name()),
    // which is empty until ensure_built_ins_registered has populated
    // "int64" and "string". The fluent shortcuts (from_elements, map,
    // etc.) call this internally; manual registration paths must too.
    clink::cluster::ensure_built_ins_registered();

    // Register the slow source factory in this job's bundle. The .so
    // dlopened on the JM and each TM re-runs this build_fn and
    // re-registers - the per-job-bundle scoping (Phase 2) keeps the
    // registrations isolated from other concurrent jobs.
    env.registry().register_source<std::int64_t>("cancel_test.slow_source",
                                                 [tick](const clink::plugin::BuildContext&) {
                                                     return std::make_shared<SlowInt64Source>(tick);
                                                 });

    clink::api::SourceDescriptor src;
    src.op_type = "cancel_test.slow_source";
    src.channel_type = "int64";

    env.source<std::int64_t>(src)
        .map<std::int64_t>([](const std::int64_t& v) { return v + 1; })
        .sink(clink::api::FileInt64Sink::builder().path("/tmp/clink_cancel_test_sink").build());
}

}  // namespace cancel_test

CLINK_REGISTER_JOB("cancel-test",
                   "1.0",
                   "long-running tick source for the cancel-job e2e test",
                   cancel_test::define_job);
