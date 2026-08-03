// two_sink_commit_group_job - one source fanned out to TWO 2PC sinks that
// share a commit_group.
//
// Exists to test cross-sink commit ATOMICITY under failure, which
// docs/production-hardening-plan.md W22 records as analysed but never
// exercised: sinks sharing a commit_group are supposed to commit as a unit,
// gated on the group's collective ack, and nothing verified that a failure
// between their two commits cannot leave one published and the other not.
//
// The property a test can check from outside is per-checkpoint agreement:
// for every checkpoint id, either BOTH sinks have a committed file or
// NEITHER does. One without the other is the split the group exists to
// prevent, and it is invisible to any test that only counts records.
//
// Both sinks receive the same records, so each output directory
// independently owes the full "record-0".."record-(N-1)" multiset. That
// makes the usual exactly-once check applicable per sink as well.
//
// Environment (shared with two_phase_commit_job where it overlaps):
//   CLINK_2SINK_OUT_A / _B   output directories, one per sink
//   CLINK_2SINK_TOTAL        records to emit
//   CLINK_2SINK_TICK_MS      pause between records
//   CLINK_2SINK_GROUP        commit_group name; empty means NO group, which
//                            is how a test contrasts grouped against
//                            independent commit

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/job/register_job.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace two_sink_test {

// Same shape as two_phase_commit_job's source, including the checkpointed
// offset - without it a restart replays from zero and the duplicates drown
// out the atomicity signal this job exists to expose.
class BoundedSlowStringSource final : public clink::Source<std::string> {
public:
    BoundedSlowStringSource(std::int64_t total, std::chrono::milliseconds tick)
        : total_(total), tick_(tick) {}

    bool produce(clink::Emitter<std::string>& out) override {
        if (this->cancelled() || counter_ >= total_) {
            return false;
        }
        clink::Batch<std::string> b;
        b.emplace("record-" + std::to_string(counter_));
        ++counter_;
        if (!out.emit_data(std::move(b))) {
            return false;
        }
        std::this_thread::sleep_for(tick_);
        return counter_ < total_;
    }

    void snapshot_offset(clink::StateBackend& backend,
                         clink::OperatorId op_id,
                         clink::CheckpointId /*ckpt*/) override {
        std::array<std::byte, 8> bytes{};
        const auto u = static_cast<std::uint64_t>(counter_);
        for (int i = 0; i < 8; ++i) {
            bytes[static_cast<std::size_t>(i)] = static_cast<std::byte>((u >> (i * 8)) & 0xFF);
        }
        backend.put_operator_state(op_id,
                                   clink::StateBackend::KeyView{kKey_, std::strlen(kKey_)},
                                   clink::StateBackend::ValueView{
                                       reinterpret_cast<const char*>(bytes.data()), bytes.size()});
    }

    bool restore_offset(clink::StateBackend& backend, clink::OperatorId op_id) override {
        auto v = backend.get_operator_state(
            op_id, clink::StateBackend::KeyView{kKey_, std::strlen(kKey_)});
        if (!v.has_value() || v->size() < 8) {
            return false;
        }
        std::uint64_t u = 0;
        for (int i = 0; i < 8; ++i) {
            u |= static_cast<std::uint64_t>(
                     static_cast<std::uint8_t>((*v)[static_cast<std::size_t>(i)]))
                 << (i * 8);
        }
        counter_ = static_cast<std::int64_t>(u);
        return true;
    }

    std::string name() const override { return "two_sink_bounded_source"; }

private:
    static constexpr const char* kKey_ = "__two_sink_source_offset__";
    std::int64_t total_;
    std::chrono::milliseconds tick_;
    std::int64_t counter_{0};
};

std::string env_or(const char* name, const char* fallback) {
    if (const char* p = std::getenv(name); p != nullptr && *p != '\0') {
        return std::string{p};
    }
    return std::string{fallback};
}

std::int64_t env_int(const char* name, std::int64_t fallback) {
    if (const char* p = std::getenv(name); p != nullptr && *p != '\0') {
        try {
            return std::stoll(p);
        } catch (...) {
        }
    }
    return fallback;
}

void define_job(clink::api::Pipeline& pipeline) {
    clink::cluster::ensure_built_ins_registered();
    const auto total = env_int("CLINK_2SINK_TOTAL", 40);
    const auto tick = std::chrono::milliseconds{env_int("CLINK_2SINK_TICK_MS", 50)};
    const auto dir_a = env_or("CLINK_2SINK_OUT_A", "/tmp/clink_2sink_a");
    const auto dir_b = env_or("CLINK_2SINK_OUT_B", "/tmp/clink_2sink_b");
    // Empty means no group: the two sinks then commit independently, which
    // is the contrast case.
    const auto group = env_or("CLINK_2SINK_GROUP", "");

    pipeline.registry().register_source<std::string>(
        "two_sink_test.bounded_source", [total, tick](const clink::plugin::BuildContext&) {
            return std::make_shared<BoundedSlowStringSource>(total, tick);
        });

    clink::api::SourceDescriptor src;
    src.op_type = "two_sink_test.bounded_source";
    src.channel_type = "string";

    auto stream = pipeline.source<std::string>(src);

    // Two sinks off the SAME stream. sink() appends an op whose inputs are
    // the upstream id, so calling it twice is a fan-out rather than a chain.
    for (const auto& [id, dir] :
         {std::pair{std::string{"sink_a"}, dir_a}, std::pair{std::string{"sink_b"}, dir_b}}) {
        clink::api::SinkDescriptor sink;
        sink.op_type = "file_2pc_sink_string";
        sink.channel_type = "string";
        sink.params["dir"] = dir;
        if (!group.empty()) {
            // The coordinator builds commit groups from OP PARAMS, so this
            // is what makes the two sinks a unit. (The sink's own
            // set_commit_group is informational - nothing in the engine
            // reads it.)
            sink.params["commit_group"] = group;
        }
        stream.sink(sink, id);
    }
}

}  // namespace two_sink_test

CLINK_REGISTER_JOB("two-sink-commit-group-test",
                   "1.0",
                   "one source fanned out to two 2PC sinks sharing a commit_group",
                   two_sink_test::define_job);
