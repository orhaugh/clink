// side_output_recovery_job - a job whose SIDE OUTPUT is held to the same
// exactly-once standard as its main branch, across a worker failure.
//
// Follow-up item 14. Side outputs were exercised on healthy runs only, and that
// branch has already proven it can break in ways nothing noticed: F39 found side
// outputs silently failing to ATTACH on Linux, so the records simply went nowhere
// and every liveness assertion still passed. A branch of the graph with no failure
// coverage is where the next defect is.
//
// The shape, and why each piece is needed to make the assertion mean anything:
//
//   replayable source ──▶ fan-out operator ──main──▶ file_2pc_sink (main/)
//                                          └─side──▶ file_2pc_sink (side/)
//
//   * The source checkpoints its offset, so a restart resumes instead of
//     replaying from zero. Without it a recovery re-emits everything and the
//     output check fails for a reason that has nothing to do with side outputs.
//   * BOTH sinks are 2PC. A plain file sink only gives at-least-once after a
//     kill, so a duplicate on the side branch would be indistinguishable from
//     correct behaviour - the test could not tell a defect from the contract.
//   * The operator emits exactly one main record and one side record per input,
//     so both branches are 1:1 with the source and comparable as multisets
//     against the same expected set.
//
// The side record is "side-<N>" for input "record-<N>", so a record that reached
// one branch but not the other is attributable to an index rather than showing up
// as a count that is merely wrong.
//
// Env:
//   CLINK_SOR_TOTAL     records to emit (default 60)
//   CLINK_SOR_TICK_MS   delay between records (default 40)
//   CLINK_SOR_OUT_DIR   parent dir; main/ and side/ are created beneath it

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
#include "clink/operators/operator_base.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace side_output_recovery {

// Same replayable source as the 2PC job: the offset rides operator state so a
// restart resumes rather than replaying from zero.
class ReplayableSource final : public clink::Source<std::string> {
public:
    ReplayableSource(std::int64_t total, std::chrono::milliseconds tick)
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

    std::string name() const override { return "side_output_recovery.source"; }

private:
    static constexpr const char* kKey_ = "__side_output_recovery_offset__";
    std::int64_t total_;
    std::chrono::milliseconds tick_;
    std::int64_t counter_{0};
};

}  // namespace side_output_recovery

namespace side_output_recovery {

inline constexpr const char* kSideTag = "side_output_recovery.side";

// One main record and one side record per input. Deliberately stateless: this
// job is about whether the side BRANCH survives a failure, and per-operator keyed
// state would add a second thing that could break.
class FanOutOperator final : public clink::Operator<std::string, std::string> {
public:
    void process(const clink::StreamElement<std::string>& el,
                 clink::Emitter<std::string>& out) override {
        if (!el.is_data()) {
            if (el.is_watermark()) {
                this->on_watermark(el.as_watermark(), out);
            } else {
                this->on_barrier(el.as_barrier(), out);
            }
            return;
        }
        clink::Batch<std::string> main_batch;
        clink::Batch<std::string> side_batch;
        for (const auto& rec : el.as_data()) {
            const auto& value = rec.value();
            main_batch.emplace(value);
            // "record-N" -> "side-N", so a record that reached one branch and not
            // the other is attributable to an index rather than showing up as a
            // count that is merely wrong.
            if (value.rfind("record-", 0) == 0) {
                side_batch.emplace("side-" + value.substr(std::strlen("record-")));
            } else {
                side_batch.emplace("side-UNPARSED-" + value);
            }
        }
        if (this->runtime() != nullptr && !side_batch.empty()) {
            auto side = this->runtime()->template side_output<std::string>(
                clink::OutputTag<std::string>{kSideTag});
            side.emit_data(std::move(side_batch));
        }
        out.emit_data(std::move(main_batch));
    }

    std::string name() const override { return "side_output_recovery.fanout"; }
};

std::int64_t total_from_env() {
    if (const char* p = std::getenv("CLINK_SOR_TOTAL"); p != nullptr && *p != '\0') {
        try {
            return std::stoll(p);
        } catch (...) {
        }
    }
    return 60;
}

std::chrono::milliseconds tick_from_env() {
    if (const char* p = std::getenv("CLINK_SOR_TICK_MS"); p != nullptr && *p != '\0') {
        try {
            return std::chrono::milliseconds{std::stoll(p)};
        } catch (...) {
        }
    }
    return std::chrono::milliseconds{40};
}

std::string out_dir_from_env() {
    if (const char* p = std::getenv("CLINK_SOR_OUT_DIR"); p != nullptr && *p != '\0') {
        return std::string{p};
    }
    return "/tmp/clink_side_output_recovery";
}

void define_job(clink::api::Pipeline& pipeline) {
    clink::cluster::ensure_built_ins_registered();
    const auto total = total_from_env();
    const auto tick = tick_from_env();
    const auto out_dir = out_dir_from_env();

    auto& reg = pipeline.registry();
    reg.register_source<std::string>("side_output_recovery.source",
                                     [total, tick](const clink::plugin::BuildContext&) {
                                         return std::make_shared<ReplayableSource>(total, tick);
                                     });
    reg.register_operator<std::string, std::string>(
        "side_output_recovery.fanout",
        [](const clink::plugin::BuildContext&) { return std::make_shared<FanOutOperator>(); });

    clink::api::SourceDescriptor src;
    src.op_type = "side_output_recovery.source";
    src.channel_type = "string";

    auto fanned = pipeline.source<std::string>(src).transform<std::string>(
        "side_output_recovery.fanout", {}, "fanout");
    pipeline.declare_side_output_on("fanout", kSideTag, "string");

    clink::api::SinkDescriptor main_sink;
    main_sink.op_type = "file_2pc_sink_string";
    main_sink.channel_type = "string";
    main_sink.params["dir"] = out_dir + "/main";
    fanned.sink(main_sink, "main_sink");

    // The side branch reaches its sink through the "<op id>::<tag>" input syntax -
    // the wiring F39 found silently failing to attach.
    //
    // Built as a raw OperatorSpec because that syntax has NO fluent surface: a
    // DataStream's sink() always consumes its own upstream id, and nothing on
    // Pipeline returns a stream for a declared side output. Same shape as F35/F36 -
    // a capability reachable only by hand-building a spec - and recorded as such
    // rather than worked around silently.
    clink::cluster::OperatorSpec side_sink;
    side_sink.id = "side_sink";
    side_sink.type = "file_2pc_sink_string";
    side_sink.out_channel = "string";
    side_sink.inputs = {std::string{"fanout::"} + kSideTag};
    side_sink.parallelism = 1;
    side_sink.params = {{"dir", out_dir + "/side"}};
    (void)pipeline.append_op(std::move(side_sink));
}

}  // namespace side_output_recovery

CLINK_REGISTER_JOB("side-output-recovery-test",
                   "1.0",
                   "replayable source fanned to a main and a side 2PC sink",
                   side_output_recovery::define_job);
