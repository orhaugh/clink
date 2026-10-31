// cancel_test_job - long-running CLINK_REGISTER_JOB target used by
// the cancel-job integration test. The pipeline keeps emitting one
// int64 every CLINK_CANCEL_TICK_MS milliseconds forever; only
// CancelJob can stop it. Sink discards records by counting.
//
// We register a custom SlowInt64Source inline so the .so doesn't
// depend on a built-in "tick" connector. The source's produce()
// honours Source::cancelled() (inherited from Operator base) which
// is what gives LocalExecutor's cancel-token plumbing something to
// flip when the Worker handles CancelJob.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/pipeline.hpp"
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

// Where the sink writes. A test that runs beside the ones watching the default
// path names its own, so its job's output cannot stand in for theirs.
std::string sink_path_from_env() {
    if (const char* p = std::getenv("CLINK_CANCEL_SINK_PATH"); p != nullptr && *p != '\0') {
        return p;
    }
    return "/tmp/clink_cancel_test_sink";
}

void define_job(clink::api::Pipeline& pipeline) {
    const auto tick = tick_from_env();

    // Registered on the built-in int64 channel type ahead of any fluent call,
    // and without registering the built-ins first: CLINK_REGISTER_JOB does
    // that before define_job runs. This job is the check that it does
    // (PluginLoader.AJobModuleRegistersOnTheBuiltInTypesAheadOfItsFluentCalls).
    //
    // Register the slow source factory in this job's bundle. The .so
    // dlopened on the coordinator and each worker re-runs this build_fn and
    // re-registers - the per-job-bundle scoping keeps the
    // registrations isolated from other concurrent jobs.
    pipeline.registry().register_source<std::int64_t>(
        "cancel_test.slow_source", [tick](const clink::plugin::BuildContext&) {
            return std::make_shared<SlowInt64Source>(tick);
        });

    clink::api::SourceDescriptor src;
    src.op_type = "cancel_test.slow_source";
    src.channel_type = "int64";

    pipeline.source<std::int64_t>(src)
        .map<std::int64_t>([](const std::int64_t& v) { return v + 1; })
        .sink(clink::api::FileInt64Sink::builder().path(sink_path_from_env()).build());
}

}  // namespace cancel_test

CLINK_REGISTER_JOB("cancel-test",
                   "1.0",
                   "long-running tick source for the cancel-job e2e test",
                   cancel_test::define_job);
