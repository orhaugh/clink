// two_phase_commit_job - bounded slow source piped to file_2pc_sink_string.
//
// Used by the 2PC integration test. Emits exactly N strings ("record-0"
// through "record-(N-1)"), one every ~50ms, then returns false (natural
// completion). Output dir from CLINK_2PC_OUT_DIR pipeline var (defaulted
// for safety); checkpoint dir set by the submitter's CheckpointConfig.

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

namespace twopc_test {

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

    // Checkpoint the next-record index as operator state so a restart (worker
    // crash / rescale redeploy) resumes from where the source left off
    // instead of replaying from 0. Combined with the 2PC sink's commit-on-
    // checkpoint, that gives exactly-once across a recovery: no record is
    // re-emitted (and so re-committed) and none is lost.
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

    std::string name() const override { return "bounded_slow_string_source"; }

private:
    static constexpr const char* kKey_ = "__twopc_source_offset__";
    std::int64_t total_;
    std::chrono::milliseconds tick_;
    std::int64_t counter_{0};
};

std::int64_t total_from_env() {
    if (const char* p = std::getenv("CLINK_2PC_TOTAL"); p != nullptr && *p != '\0') {
        try {
            return std::stoll(p);
        } catch (...) {
        }
    }
    return 30;
}

std::chrono::milliseconds tick_from_env() {
    if (const char* p = std::getenv("CLINK_2PC_TICK_MS"); p != nullptr && *p != '\0') {
        try {
            return std::chrono::milliseconds{std::stoll(p)};
        } catch (...) {
        }
    }
    return std::chrono::milliseconds{50};
}

std::string out_dir_from_env() {
    if (const char* p = std::getenv("CLINK_2PC_OUT_DIR"); p != nullptr && *p != '\0') {
        return std::string{p};
    }
    return "/tmp/clink_2pc_default";
}

void define_job(clink::api::Pipeline& pipeline) {
    clink::cluster::ensure_built_ins_registered();
    const auto total = total_from_env();
    const auto tick = tick_from_env();
    const auto out_dir = out_dir_from_env();

    pipeline.registry().register_source<std::string>(
        "twopc_test.bounded_slow_source", [total, tick](const clink::plugin::BuildContext&) {
            return std::make_shared<BoundedSlowStringSource>(total, tick);
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
