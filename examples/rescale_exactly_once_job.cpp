// rescale_exactly_once_job - a job whose output can be checked for
// exactly-once across a rescale.
//
// Written for follow-up item 2, and it established that the scenario cannot be
// run: neither rescale path can change a running job's parallelism. Per-operator
// rescale accepted every request and executed none, because every step after the
// accept addresses an operator's subtasks by a role the planner never assigns;
// whole-job rescale, asked to resize the one shared role, redeployed a
// multi-operator job as clones of a single chain and killed it. The coordinator
// now refuses both, and this job drives the tests that pin those refusals
// (tests/integration/test_rescale_exactly_once.cpp).
//
// It is kept as written rather than trimmed to the refusal tests, because the
// moment per-operator rescale becomes executable this is the job that asserts
// exactly-once across it. The premise test - 240 records, 12 keys, no rescale,
// full output equality - is meaningful on its own in the meantime.
//
// Three things have to be true at once for the assertion to mean anything, and
// each is why a piece of this job looks the way it does:
//
//   1. The source must REPLAY correctly. It checkpoints its emitted-count, so
//      a restart resumes rather than re-emitting from zero. Without that a
//      restore double-counts and every output check fails for a reason that
//      has nothing to do with rescale.
//   2. The middle operator must be KEYED and stateful, so a rescale actually
//      moves key-group state between subtasks. A stateless job would rescale
//      cleanly while proving nothing about state.
//   3. Output must be 1:1 with input, so committed records can be compared as
//      a multiset. That rules out an aggregate in the middle - hence a keyed
//      operator that keeps per-key state but emits each record unchanged.
//
// The operator also CHECKS its own state rather than only keeping it. For
// record-N with K keys, the number of records seen for that key before this
// one is exactly N / K, so the operator knows what its counter should read.
// When the stored value disagrees it emits `STATE-MISMATCH-...` instead of the
// record. Those strings are not in the expected multiset, so the test's
// existing verifier reports them as unexpected output and names the key -
// state loss becomes a visible, attributable record rather than a silent
// difference in a count nobody printed.
//
// Environment:
//   CLINK_RXO_OUT        2PC sink output directory
//   CLINK_RXO_TOTAL      records to emit (default 60)
//   CLINK_RXO_TICK_MS    pause between records (default 40)
//   CLINK_RXO_KEYS       distinct keys (default 4)
//   CLINK_RXO_MAX_PAR    its max_parallelism bound (default 4)

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/core/codec.hpp"
#include "clink/job/register_job.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/state/keyed_state.hpp"

namespace rescale_xo {

constexpr const char* kOffsetKey = "__rescale_xo_source_offset__";

// Bounded source with a CHECKPOINTED offset, so a restart resumes instead of
// replaying from zero. Same shape as the 2PC test job's source; without the
// offset a rescale-restore re-emits every record and the output check fails
// for a reason unrelated to rescale.
class ReplayableStringSource final : public clink::Source<std::string> {
public:
    ReplayableStringSource(std::int64_t total, std::chrono::milliseconds tick)
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
        backend.put_operator_state(
            op_id,
            clink::StateBackend::KeyView{kOffsetKey, std::strlen(kOffsetKey)},
            clink::StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()),
                                           bytes.size()});
    }

    bool restore_offset(clink::StateBackend& backend, clink::OperatorId op_id) override {
        auto v = backend.get_operator_state(
            op_id, clink::StateBackend::KeyView{kOffsetKey, std::strlen(kOffsetKey)});
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

    std::string name() const override { return "rescale_xo.replayable_source"; }

private:
    std::int64_t total_;
    std::chrono::milliseconds tick_;
    std::int64_t counter_{0};
};

// Parse the N out of "record-N". Returns -1 on anything else, which the
// operator turns into a visible bad record rather than a silent skip.
inline std::int64_t record_index(const std::string& s) {
    const auto dash = s.rfind('-');
    if (dash == std::string::npos) {
        return -1;
    }
    try {
        return std::stoll(s.substr(dash + 1));
    } catch (const std::exception&) {
        return -1;
    }
}

// Keyed, stateful, and self-checking. Emits each record unchanged so the
// committed output stays comparable as a multiset, and replaces it with a
// STATE-MISMATCH marker when its per-key counter disagrees with what the
// record's own index implies. A rescale that lost or duplicated key-group
// state therefore shows up as an unexpected record naming the key, instead of
// as a number nobody printed.
class KeyedCountCheckOperator final : public clink::Operator<std::string, std::string> {
public:
    explicit KeyedCountCheckOperator(std::int64_t keys) : keys_(keys < 1 ? 1 : keys) {}

    void open() override {
        if (this->runtime() == nullptr || !this->runtime()->has_state_backend()) {
            throw std::runtime_error(
                "rescale_xo.KeyedCountCheck: no RuntimeContext / state backend at open(); the "
                "whole point of this operator is per-key state that must survive a rescale");
        }
        state_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "rescale_xo_counts", clink::int64_codec(), clink::int64_codec()));
    }

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
        clink::Batch<std::string> batch;
        for (const auto& rec : el.as_data()) {
            const auto& value = rec.value();
            const auto idx = record_index(value);
            if (idx < 0) {
                batch.emplace("BAD-RECORD-" + value);
                continue;
            }
            const std::int64_t key = idx % keys_;
            // What the counter must already read for this key: one per
            // earlier record of the same key, and the source emits them in
            // ascending index order.
            const std::int64_t expected_before = idx / keys_;
            const auto stored = state_->get(key).value_or(0);
            if (stored != expected_before) {
                // State was lost (stored too low) or duplicated (too high).
                // Emitted rather than logged so the test's output comparison
                // reports it, with the key and both numbers.
                batch.emplace("STATE-MISMATCH-key" + std::to_string(key) + "-at" +
                              std::to_string(idx) + "-got" + std::to_string(stored) + "-want" +
                              std::to_string(expected_before));
                state_->put(key, expected_before + 1);
                continue;
            }
            state_->put(key, stored + 1);
            batch.emplace(value);
        }
        out.emit_data(std::move(batch));
    }

    std::string name() const override { return "rescale_xo.KeyedCountCheck"; }

private:
    std::int64_t keys_;
    std::optional<clink::KeyedState<std::int64_t, std::int64_t>> state_;
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
    const auto total = env_int("CLINK_RXO_TOTAL", 60);
    const auto tick = std::chrono::milliseconds{env_int("CLINK_RXO_TICK_MS", 40)};
    const auto keys = env_int("CLINK_RXO_KEYS", 4);
    const auto max_par = static_cast<std::uint32_t>(env_int("CLINK_RXO_MAX_PAR", 4));
    const auto out_dir = env_or("CLINK_RXO_OUT", "/tmp/clink_rescale_xo_out");

    auto& reg = pipeline.registry();
    reg.register_source<std::string>(
        "rescale_xo.replayable_source", [total, tick](const clink::plugin::BuildContext&) {
            return std::make_shared<ReplayableStringSource>(total, tick);
        });
    reg.register_operator<std::string, std::string>(
        "rescale_xo.keyed_count_check", [keys](const clink::plugin::BuildContext&) {
            return std::make_shared<KeyedCountCheckOperator>(keys);
        });
    // The extractor the keyed edge hash-partitions on. Same derivation the
    // operator uses, so a record's key and its expected count agree.
    reg.register_key_extractor<std::string>("rescale_xo.by_index_mod",
                                            [keys](const std::string& s) -> std::int64_t {
                                                const auto idx = record_index(s);
                                                return idx < 0 ? 0 : idx % (keys < 1 ? 1 : keys);
                                            });

    clink::api::SourceDescriptor src;
    src.op_type = "rescale_xo.replayable_source";
    src.channel_type = "string";
    // The source stays at parallelism 1 deliberately: it has no subtask
    // awareness, so two instances would each emit the whole range and every
    // record would be a duplicate before a rescale was even requested.
    auto stream = pipeline.source<std::string>(src);

    // key_by(name) resolves the registered extractor and returns a keyed
    // stream; KeyedDataStream::process(op_type, ...) is the attach point that
    // carries key_by onto the OperatorSpec, which OperatorDescriptor cannot
    // express.
    auto counted = stream.key_by("rescale_xo.by_index_mod")
                       .process<std::string>("rescale_xo.keyed_count_check", {}, "counter");

    // `.rescalable()` is what makes `clink rescale-op` accept a request for
    // this operator at all: without bounds the RescaleCoordinator registers it
    // 0/0 and refuses as "not scalable" (follow-up item 1b).
    //
    // It starts at 1 and may go to max_par, so the test scales UP - the
    // direction where key groups have to be redistributed across subtasks that
    // did not exist when the state was written.
    counted.uid("rescale-xo-counter").rescalable(1, max_par);

    clink::api::SinkDescriptor sink;
    sink.op_type = "file_2pc_sink_string";
    sink.channel_type = "string";
    sink.params["dir"] = out_dir;
    counted.sink(sink, "sink");
}

}  // namespace rescale_xo

CLINK_REGISTER_JOB("rescale-exactly-once-test",
                   "1.0",
                   "replayable source -> keyed self-checking counter (rescalable) -> 2PC sink",
                   rescale_xo::define_job);
