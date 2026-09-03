// Stable tier: the public testing framework (clink::test).
// Compile-only; frozen (see README.md). Additions only.

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "clink/operators/map_operator.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/test/assertions.hpp"
#include "clink/test/failure_injection.hpp"
#include "clink/test/keyed_harness.hpp"
#include "clink/test/local_environment.hpp"
#include "clink/test/one_input_harness.hpp"
#include "clink/test/output_capture.hpp"
#include "clink/test/sequence.hpp"
#include "clink/test/side_output_capture.hpp"
#include "clink/test/sink_contract.hpp"
#include "clink/test/source_contract.hpp"
#include "clink/test/sources_and_sinks.hpp"
#include "clink/test/test_cluster.hpp"
#include "clink/test/two_input_harness.hpp"
#include "clink/test/upsert_contract.hpp"

namespace {

using namespace std::chrono_literals;

class CountPerKey final
    : public clink::KeyedProcessFunction<std::string, std::string, std::int64_t> {
public:
    void open(clink::RuntimeContext& ctx) override {
        state_ = std::make_unique<clink::KeyedState<std::string, std::int64_t>>(
            ctx.keyed_state<std::string, std::int64_t>(
                "count", clink::string_codec(), clink::int64_codec()));
    }
    void process_element(const std::string&,
                         clink::ProcessFunctionContext<std::int64_t>&,
                         clink::Collector<std::int64_t>& out) override {
        const auto next = state_->get(current_key()).value_or(0) + 1;
        state_->put(current_key(), next);
        out.collect(next);
    }

private:
    std::unique_ptr<clink::KeyedState<std::string, std::int64_t>> state_;
};

class Echo final : public clink::ProcessFunction<std::int64_t, std::int64_t> {
public:
    void process_element(const std::int64_t& v,
                         clink::ProcessFunctionContext<std::int64_t>&,
                         clink::Collector<std::int64_t>& out) override {
        out.collect(v);
    }
};

class Zip final : public clink::CoOperator<std::int64_t, std::string, std::string> {
public:
    void process_element1(const clink::StreamElement<std::int64_t>&,
                          clink::Emitter<std::string>&) override {}
    void process_element2(const clink::StreamElement<std::string>&,
                          clink::Emitter<std::string>&) override {}
};

[[maybe_unused]] void capture_and_one_input_harness() {
    clink::test::OutputCapture<std::int64_t> cap;
    clink::Emitter<std::int64_t>& emitter = cap.emitter();
    (void)emitter;
    (void)cap.values();
    (void)cap.records();
    (void)cap.watermarks();
    (void)cap.barriers();
    (void)cap.value_count();
    (void)cap.any_value([](const std::int64_t& v) { return v > 0; });
    (void)cap.count_values([](const std::int64_t& v) { return v > 0; });
    (void)cap.take_events();

    clink::test::OneInputOperatorHarness<std::int64_t, std::int64_t>::Options options;
    options.operator_name = "under-test";
    options.operator_id = 7;
    options.initial_processing_time_ms = 0;
    options.state_backend = std::make_shared<clink::InMemoryStateBackend>();

    auto h = clink::test::OneInputOperatorHarness<std::int64_t, std::int64_t>::create(
        clink::MapOperator<std::int64_t, std::int64_t>([](const std::int64_t& v) { return v; }),
        options);
    h.open();
    h.process_element(1);
    h.process_element(2, /*event_time_ms=*/100);
    h.process_batch(clink::Batch<std::int64_t>{});
    h.process(clink::StreamElement<std::int64_t>::watermark(clink::Watermark::max()));
    h.process_watermark(200);
    h.process_watermark(clink::Watermark::max());
    h.process_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
    h.set_processing_time(1'000);
    h.advance_processing_time_to(2'000);
    h.advance_processing_time_by(500);
    clink::OutputTag<std::int64_t> tag{"side"};
    h.register_side_output(tag);
    (void)h.side_output_values(tag);
    (void)h.current_watermark_ms();
    (void)h.processing_time_timers();
    (void)h.event_time_timers();
    (void)h.output().values();
    h.failures().fail_once(clink::test::FailurePoint::BeforeProcessElement);
    h.failures().fail_on_nth(clink::test::FailurePoint::DuringSnapshot, 2);
    h.failures().fail_when(clink::test::FailurePoint::OnEventTimeTimer,
                           [](std::uint64_t n) { return n == 1; });
    clink::test::HarnessSnapshot snap = h.snapshot(1);
    h.flush();
    h.close();

    auto restored = clink::test::OneInputOperatorHarness<std::int64_t, std::int64_t>::restore(
        clink::MapOperator<std::int64_t, std::int64_t>([](const std::int64_t& v) { return v; }),
        snap,
        options);
    restored.restore_from(snap);
    (void)clink::test::to_string(clink::test::FailurePoint::AfterProcessElement);
}

[[maybe_unused]] void process_function_harnesses() {
    auto plain = clink::test::make_process_function_harness(Echo{});
    plain.open();
    plain.process_element(1);

    auto keyed = clink::test::make_keyed_process_function_harness(
        CountPerKey{}, [](const std::string& s) { return s; });
    keyed.open();
    keyed.process_element("alice");
    (void)keyed.state_value<std::int64_t>("alice", "count");
    (void)keyed.state_value<std::int64_t>(
        "alice", "count", clink::string_codec(), clink::int64_codec());
    keyed.seed_state<std::int64_t>("bob", "count", 4);
    keyed.clear_state("bob", "count");
    (void)keyed.known_keys<std::int64_t>("count");
    (void)keyed.has_event_time_timer(1'000, "alice");
    (void)keyed.has_processing_time_timer(1'000, "alice");
    (void)keyed.event_time_timers_for("alice");
    (void)keyed.processing_time_timers_for("alice");

    (void)clink::test::default_codec<std::string>::get();
    (void)clink::test::default_codec<std::int64_t>::get();
}

[[maybe_unused]] void two_input_harness() {
    auto h =
        clink::test::TwoInputOperatorHarness<std::int64_t, std::string, std::string>::create(Zip{});
    h.open();
    h.process_left(1);
    h.process_left(2, 100);
    h.process_left_batch(clink::Batch<std::int64_t>{});
    h.process_right("a");
    h.process_right("b", 100);
    h.process_right_batch(clink::Batch<std::string>{});
    (void)h.process_left_watermark(100);
    (void)h.process_right_watermark(100);
    (void)h.mark_left_idle();
    (void)h.mark_right_idle();
    h.advance_processing_time_to(1'000);
    h.advance_processing_time_by(10);
    (void)h.current_watermark_ms();
    clink::test::HarnessSnapshot snap = h.snapshot(1);
    h.flush();
    h.close();
    auto restored =
        clink::test::TwoInputOperatorHarness<std::int64_t, std::string, std::string>::restore(Zip{},
                                                                                              snap);
    (void)restored;
}

[[maybe_unused]] void sources_sinks_and_environments() {
    auto src =
        std::make_shared<clink::test::TestSource<std::int64_t>>(std::vector<std::int64_t>{1, 2, 3});
    src->emit(4).emit(5, /*event_time_ms=*/500).watermark(600);
    auto sink = std::make_shared<clink::test::CollectSink<std::int64_t>>();
    (void)sink->values();
    (void)sink->records();
    (void)sink->watermarks();
    (void)sink->value_count();
    auto failing = std::make_shared<clink::test::FailingSink<std::int64_t>>(/*pass_first=*/2);
    (void)failing->failed();
    auto txn = std::make_shared<clink::test::TransactionalTestSink<std::int64_t>>();
    (void)txn->committed_values();
    (void)txn->uncommitted_values();
    (void)txn->pending_checkpoints();
    (void)txn->commits();
    (void)txn->aborts();

    clink::test::LocalTestEnvironment::Options opts;
    opts.state_backend = std::make_shared<clink::InMemoryStateBackend>();
    opts.execution_mode = clink::JobConfig::ExecutionMode::Streaming;
    opts.restore_from = std::nullopt;
    clink::test::LocalTestEnvironment env{opts};
    auto h0 = env.dag().add_source<std::int64_t>(src);
    env.dag().add_sink<std::int64_t>(h0, sink);
    (void)env.state_backend();
    try {
        env.execute();
    } catch (const clink::test::PipelineFailure& failure) {
        (void)failure.what();
    }
    (void)env.execute_collecting_errors();

    clink::test::TestCluster cluster({.workers = 2, .slots_per_worker = 4});
    clink::cluster::JobGraphSpec spec;
    const clink::cluster::JobId id = cluster.submit(spec);
    (void)cluster.await_completion(id, 5s);
    (void)cluster.errors(id);
    (void)cluster.execute(spec, 5s);
}

[[maybe_unused]] void assertions_and_sequences() {
    clink::test::OutputCapture<std::int64_t> cap;
    clink::test::CheckResult r1 = clink::test::values_are(cap, {1, 2, 3});
    clink::test::CheckResult r2 = clink::test::values_are_unordered(cap, {3, 2, 1});
    clink::test::CheckResult r3 = clink::test::contains_value(cap, std::int64_t{2});
    clink::test::CheckResult r4 = clink::test::watermarks_are_monotonic(cap);
    (void)r1.ok;
    (void)r2.message;
    (void)r3;
    (void)r4;
    (void)clink::test::CheckResult::success();
    (void)clink::test::CheckResult::failure("why");

    clink::test::TestSequence<std::int64_t> seq;
    seq.element(1).element(2, 100).watermark(200).advance_processing_time_to(1'000).flush();
    auto h = clink::test::make_process_function_harness(Echo{});
    h.open();
    seq.replay(h);
    (void)clink::test::deterministic_shuffle(std::vector<std::int64_t>{1, 2, 3}, /*seed=*/42);

    clink::InMemoryStateBackend backend;
    clink::RuntimeContext ctx(clink::OperatorId{1}, "side", &backend, nullptr);
    clink::OutputTag<std::string> tag{"side"};
    clink::test::register_side_output_channel(ctx, tag);
    (void)clink::test::drain_side_output(ctx, tag);
}

[[maybe_unused]] void connector_contract_fixtures() {
    clink::test::SourceContractFixture<std::string> source{
        .fresh = []() { return std::unique_ptr<clink::Source<std::string>>{}; },
        .expected = {"a", "b"},
    };
    (void)source;
    (void)clink::test::MalformedInputPolicy::Skip;
    clink::test::UpsertContractFixture<std::string> upsert;
    (void)upsert.fresh;
    (void)upsert.state;
}

}  // namespace
