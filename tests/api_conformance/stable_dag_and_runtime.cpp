// Stable tier: the Dag, the local executor and what an operator is handed.
// Compile-only; frozen (see README.md). Additions only.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/dead_letter.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/runtime/timer_service.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace {

using namespace std::chrono_literals;  // NOLINT

struct Left {
    std::string key;
};
struct Right {
    std::string key;
};
struct Joined {
    std::string key;
};

class Pair final : public clink::CoOperator<Left, Right, Joined> {
public:
    void process_element1(const clink::StreamElement<Left>&, clink::Emitter<Joined>&) override {}
    void process_element2(const clink::StreamElement<Right>&, clink::Emitter<Joined>&) override {}
};

[[maybe_unused]] void build_and_run() {
    clink::Dag dag;

    auto src = std::make_shared<clink::VectorSource<std::int64_t>>(
        std::vector<clink::Record<std::int64_t>>{});
    clink::StageHandle<std::int64_t> h0 = dag.add_source<std::int64_t>(src);

    auto doubler = std::make_shared<clink::MapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v * 2; });
    clink::StageHandle<std::int64_t> h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, doubler);

    std::vector<clink::StageHandle<std::int64_t>> forked = dag.fork<std::int64_t>(h1, 2);
    std::vector<clink::StageHandle<std::int64_t>> split = dag.add_split<std::int64_t>(
        forked[0], [](const std::int64_t& v) { return static_cast<int>(v % 2); }, 2);
    clink::StageHandle<std::int64_t> merged =
        dag.union_streams<std::int64_t>({split[0], split[1], forked[1]});

    clink::OutputTag<std::int64_t> late{"late"};
    clink::StageHandle<std::int64_t> side =
        dag.side_output<std::int64_t, std::int64_t>(merged, late);
    (void)side;

    auto lefts = dag.add_source<Left>(
        std::make_shared<clink::VectorSource<Left>>(std::vector<clink::Record<Left>>{}));
    auto rights = dag.add_source<Right>(
        std::make_shared<clink::VectorSource<Right>>(std::vector<clink::Record<Right>>{}));
    clink::StageHandle<Joined> joined = dag.interval_join<Left, Right, std::string, Joined>(
        lefts,
        rights,
        [](const Left& l) { return l.key; },
        [](const Right& r) { return r.key; },
        50ms,
        200ms,
        [](const std::optional<Left>& l, const std::optional<Right>& r) {
            return Joined{l ? l->key : r->key};
        },
        clink::Dag::JoinType::LeftOuter);
    (void)joined;

    clink::StageHandle<Joined> paired =
        dag.add_co_operator<Left, Right, Joined>(lefts, rights, std::make_shared<Pair>());
    (void)paired;

    auto rules = dag.add_source<std::string>(std::make_shared<clink::VectorSource<std::string>>(
        std::vector<clink::Record<std::string>>{}));
    clink::StageHandle<std::int64_t> enriched =
        dag.broadcast_connect<std::int64_t, std::string, std::int64_t, std::string>(
            merged,
            rules,
            [](const std::string& rule, clink::BroadcastState<std::string>& state) {
                state.put(rule);
            },
            [](const std::int64_t& v,
               clink::BroadcastState<std::string>& state) -> std::optional<std::int64_t> {
                return state.get().has_value() ? std::optional<std::int64_t>{v} : std::nullopt;
            },
            clink::string_codec());

    dag.add_sink<std::int64_t>(
        enriched, std::make_shared<clink::FunctionSink<std::int64_t>>([](const std::int64_t&) {}));

    clink::JobConfig cfg;
    cfg.state_backend = std::make_shared<clink::InMemoryStateBackend>();
    cfg.execution_mode = clink::JobConfig::ExecutionMode::Streaming;
    cfg.restore_from = std::nullopt;
    cfg.capture_dir = "";
    cfg.dead_letter_queue = nullptr;

    clink::LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.start();
    exec.await_termination();
    exec.cancel();
    exec.run();
    exec.run_to_completion();
    clink::Snapshot savepoint = exec.take_savepoint(clink::CheckpointId{1});
    (void)savepoint.checkpoint_id;
    (void)savepoint.bytes;
    for (const clink::LocalExecutor::OperatorError& e : exec.operator_errors()) {
        (void)e;
    }
}

[[maybe_unused]] void runtime_context() {
    clink::InMemoryStateBackend backend;
    clink::RuntimeContext ctx(clink::OperatorId{1}, "conformance", &backend, /*metrics=*/nullptr);
    (void)ctx.operator_id();
    (void)ctx.operator_name();
    (void)ctx.state_backend();
    (void)ctx.has_state_backend();
    (void)ctx.metrics();

    clink::TimerService* timers = ctx.timer_service();
    timers->register_processing_time_timer(1'000, "k");
    timers->delete_processing_time_timer(1'000, "k");
    timers->register_event_time_timer(2'000, "k");
    timers->delete_event_time_timer(2'000, "k");
    (void)timers->next_timestamp();
    (void)timers->next_event_timestamp();
    (void)timers->now_ms();

    clink::Accumulator acc = ctx.accumulator("records_seen");
    acc.add();
    acc.add(-1);

    ctx.log(clink::LogSeverity::Info, "hello");
    ctx.log_debug("d");
    ctx.log_info("i");
    ctx.log_warn("w");
    ctx.log_error("e");

    ctx.report_bad_record(clink::BadRecord{.payload = "x",
                                           .error = "bad",
                                           .connector = "conformance",
                                           .direction = "source",
                                           .location = "here"});

    clink::OutputTag<std::string> tag{"side"};
    clink::Emitter<std::string> side = ctx.side_output<std::string>(tag);
    (void)side;

    clink::NullDeadLetterQueue silent;
    clink::LoggingDeadLetterQueue logging{/*logger=*/nullptr};
    silent.report(clink::BadRecord{});
    (void)logging.logged_total();
    (void)logging.suppressed_total();

    clink::Snapshot snap = backend.snapshot(clink::CheckpointId{1});
    clink::InMemoryStateBackend restored;
    restored.restore(snap);
}

}  // namespace
