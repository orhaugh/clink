// two_phase_commit_job - bounded slow source piped to file_2pc_sink_string.
//
// Used by the 2PC integration test. Emits exactly N strings ("record-0"
// through "record-(N-1)"), one every ~50ms, then returns false (natural
// completion). Output dir from CLINK_2PC_OUT_DIR pipeline var (defaulted
// for safety); checkpoint dir set by the submitter's CheckpointConfig.
// The source (and its offset checkpointing, which the exactly-once claim
// rides on) is shared with the connector 2PC jobs: bounded_slow_source.hpp.

#include <cstdlib>
#include <memory>
#include <string>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/job/register_job.hpp"
#include "clink/plugin/plugin.hpp"

#include "bounded_slow_source.hpp"

namespace twopc_test {

std::string out_dir_from_env() {
    if (const char* p = std::getenv("CLINK_2PC_OUT_DIR"); p != nullptr && *p != '\0') {
        return std::string{p};
    }
    return "/tmp/clink_2pc_default";
}

void define_job(clink::api::Pipeline& pipeline) {
    clink::cluster::ensure_built_ins_registered();
    const auto total = clink_examples::total_from_env();
    const auto tick = clink_examples::tick_from_env();
    const auto out_dir = out_dir_from_env();

    pipeline.registry().register_source<std::string>(
        "twopc_test.bounded_slow_source", [total, tick](const clink::plugin::BuildContext&) {
            return std::make_shared<clink_examples::BoundedSlowStringSource>(total, tick);
        });

    clink::api::SourceDescriptor src;
    src.op_type = "twopc_test.bounded_slow_source";
    src.channel_type = "string";

    clink::api::SinkDescriptor sink;
    sink.op_type = "file_2pc_sink_string";
    sink.channel_type = "string";
    sink.params["dir"] = out_dir;

    pipeline.source<std::string>(src).sink(sink);
}

}  // namespace twopc_test

CLINK_REGISTER_JOB("two-phase-commit-test",
                   "1.0",
                   "bounded slow source piped to 2PC file sink",
                   twopc_test::define_job);
