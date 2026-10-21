// Which sinks may share a chain. A Dag's checkpoint is acknowledged by its
// owner, the element whose snapshot is the checkpoint: the sink when it is the
// only one, the operator in front of the sinks when there are two or more
// (Dag::add_sink). An owning operator snapshots, forwards the barrier and
// acknowledges before the sinks have even taken the barrier, so a sink whose
// guarantee rests on its own barrier hook preceding the acknowledgement must
// be the only sink on its chain:
//   * a sink that gates the acknowledgement (Sink::gates_checkpoint_ack), an
//     at-least-once writer that writes out at the barrier what it holds;
//   * a sink that stages state at the barrier (Sink::stages_state_at_barrier),
//     a two-phase-commit sink that prepares its transaction there and records
//     the handle in operator state, which the checkpoint has to capture.
// These cases run a CommittingSink in real Dags under a LocalExecutor with an
// in-memory backend, recording each acknowledgement the worker would forward to
// the coordinator, and hold the sink's prepare until the case lets it go, so
// the order is observed rather than inferred from timing.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/connectors/committing_sink.hpp"
#include "clink/core/types.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace {

using namespace std::chrono_literals;

// One step of a scripted source: a batch of values, or a checkpoint barrier.
struct OwnershipStep {
    bool barrier{false};
    int from{0};
    int to{0};
    std::uint64_t checkpoint{0};
};

OwnershipStep ownership_rows(int from, int to) {
    return OwnershipStep{.from = from, .to = to};
}
OwnershipStep ownership_barrier(std::uint64_t checkpoint) {
    return OwnershipStep{.barrier = true, .checkpoint = checkpoint};
}

// Plays its script, then ends.
class OwnershipScriptSource final : public clink::Source<int> {
public:
    explicit OwnershipScriptSource(std::vector<OwnershipStep> steps) : steps_(std::move(steps)) {}

    bool produce(clink::Emitter<int>& out) override {
        if (this->cancelled() || next_ >= steps_.size()) {
            return false;
        }
        const OwnershipStep& step = steps_[next_++];
        if (step.barrier) {
            return out.emit_barrier(clink::CheckpointBarrier{clink::CheckpointId{step.checkpoint}});
        }
        clink::Batch<int> batch;
        for (int v = step.from; v < step.to; ++v) {
            batch.emplace(v);
        }
        return out.emit_data(std::move(batch));
    }

    [[nodiscard]] bool is_bounded() const noexcept override { return true; }
    [[nodiscard]] std::string name() const override { return "sink_chain_ownership.source"; }

private:
    std::vector<OwnershipStep> steps_;
    std::size_t next_{0};
};

class OwnershipPlainSink final : public clink::Sink<int> {
public:
    void on_data(const clink::Batch<int>& /*batch*/) override {}
    [[nodiscard]] std::string name() const override { return "sink_chain_ownership.plain"; }
};

// A sink that declares it gates the checkpoint's acknowledgement, as an
// at-least-once sink that writes out at the barrier does.
class OwnershipGatingSink final : public clink::Sink<int> {
public:
    void on_data(const clink::Batch<int>& /*batch*/) override {}
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    [[nodiscard]] std::string name() const override { return "sink_chain_ownership.gating"; }
};

// A two-phase-commit sink whose prepare can be held: prepare_commit() records
// that it has started, then waits until release(). Its handle is the
// checkpoint's id as text.
class HeldPrepareSink final : public clink::CommittingSink<int, std::string> {
public:
    explicit HeldPrepareSink(bool hold) : hold_(hold) {}

    [[nodiscard]] std::string name() const override { return "held_prepare_sink"; }

    [[nodiscard]] std::uint64_t prepared() const { return prepared_.load(); }

    bool await_preparing(std::chrono::milliseconds limit) {
        std::unique_lock lock(mu_);
        return cv_.wait_for(lock, limit, [this] { return preparing_; });
    }

    void release() {
        {
            const std::lock_guard lock(mu_);
            released_ = true;
        }
        cv_.notify_all();
    }

protected:
    void write(const clink::Batch<int>& /*batch*/) override {}

    std::optional<std::string> prepare_commit(std::uint64_t checkpoint_id) override {
        if (hold_) {
            std::unique_lock lock(mu_);
            preparing_ = true;
            cv_.notify_all();
            cv_.wait(lock, [this] { return released_; });
        }
        prepared_.store(checkpoint_id);
        return std::to_string(checkpoint_id);
    }

    bool commit(const std::string& /*committable*/) override { return true; }
    std::string serialize(const std::string& committable) const override { return committable; }
    std::string deserialize(std::string_view bytes) const override { return std::string{bytes}; }

private:
    const bool hold_;
    std::atomic<std::uint64_t> prepared_{0};
    std::mutex mu_;
    std::condition_variable cv_;
    bool preparing_{false};
    bool released_{false};
};

// One acknowledgement, and what the sink had prepared when it was sent.
struct OwnershipAck {
    std::uint64_t checkpoint{0};
    bool ok{false};
    std::uint64_t prepared{0};
};

// A job around one HeldPrepareSink, recording every acknowledgement.
class OwnershipJob {
public:
    explicit OwnershipJob(std::shared_ptr<HeldPrepareSink> sink)
        : sink_(std::move(sink)), backend_(std::make_shared<clink::InMemoryStateBackend>()) {
        config_.state_backend = backend_;
        config_.on_checkpoint_ack = [this](clink::CheckpointId id, bool ok, const std::string&) {
            {
                const std::lock_guard lock(mu_);
                acks_.push_back({id.value(), ok, sink_->prepared()});
            }
            cv_.notify_all();
        };
    }

    OwnershipJob(const OwnershipJob&) = delete;
    OwnershipJob& operator=(const OwnershipJob&) = delete;
    OwnershipJob(OwnershipJob&&) = delete;
    OwnershipJob& operator=(OwnershipJob&&) = delete;

    ~OwnershipJob() {
        sink_->release();
        if (exec_) {
            exec_->await_termination();
        }
    }

    void start(clink::Dag dag) {
        exec_ = std::make_unique<clink::LocalExecutor>(std::move(dag), config_);
        exec_->start();
    }

    void await_termination() { exec_->await_termination(); }

    [[nodiscard]] std::vector<clink::LocalExecutor::OperatorError> errors() const {
        return exec_->operator_errors();
    }

    bool await_ack(std::uint64_t checkpoint, std::chrono::milliseconds limit) {
        std::unique_lock lock(mu_);
        return cv_.wait_for(lock, limit, [&] {
            for (const auto& a : acks_) {
                if (a.checkpoint == checkpoint) {
                    return true;
                }
            }
            return false;
        });
    }

    [[nodiscard]] std::vector<OwnershipAck> acks() const {
        const std::lock_guard lock(mu_);
        return acks_;
    }

    [[nodiscard]] clink::InMemoryStateBackend& backend() { return *backend_; }

private:
    std::shared_ptr<HeldPrepareSink> sink_;
    std::shared_ptr<clink::InMemoryStateBackend> backend_;
    clink::JobConfig config_;
    std::unique_ptr<clink::LocalExecutor> exec_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::vector<OwnershipAck> acks_;
};

std::shared_ptr<clink::MapOperator<int, int>> ownership_identity() {
    return std::make_shared<clink::MapOperator<int, int>>([](const int& v) { return v; },
                                                          "sink_chain_ownership.identity");
}

constexpr const char* kTransactionalRule = "held_prepare_sink must be the only sink on its chain";

}  // namespace

// A two-phase-commit sink beside another sink is refused as the Dag is built,
// whichever is added first and with or without an operator in front, and the
// message names it and says where the other sink belongs.
TEST(SinkChainOwnership, ATransactionalSinkBesideAnotherSinkIsRefusedWhicheverIsAddedFirst) {
    for (const bool with_operator : {true, false}) {
        for (const bool transactional_first : {true, false}) {
            SCOPED_TRACE(std::string{with_operator ? "behind an operator" : "on the source"} +
                         (transactional_first ? ", transactional sink first"
                                              : ", transactional sink second"));
            clink::Dag dag;
            auto src = dag.add_source<int>(
                std::make_shared<OwnershipScriptSource>(std::vector<OwnershipStep>{}));
            auto tail = with_operator ? dag.add_operator<int, int>(src, ownership_identity()) : src;
            auto transactional = std::make_shared<HeldPrepareSink>(/*hold=*/false);
            auto plain = std::make_shared<OwnershipPlainSink>();
            try {
                if (transactional_first) {
                    dag.add_sink<int>(tail, transactional);
                    dag.add_sink<int>(tail, plain);
                } else {
                    dag.add_sink<int>(tail, plain);
                    dag.add_sink<int>(tail, transactional);
                }
                ADD_FAILURE() << "a second sink joined the transactional sink's chain";
            } catch (const std::logic_error& e) {
                const std::string what = e.what();
                EXPECT_NE(what.find(kTransactionalRule), std::string::npos) << what;
                EXPECT_NE(what.find("its own subtask"), std::string::npos) << what;
            }
        }
    }
}

// The regression. Beside a second sink, a two-phase-commit sink no longer owns
// its chain's checkpoint: the operator in front snapshots and acknowledges as
// the barrier passes, so checkpoint 1 was acknowledged before the sink had
// prepared its transaction, and the snapshot it names holds no handle for it.
// A crash then would roll the open transaction back, and a restore from
// checkpoint 1 would resume past the records it held. The prepare is held here
// until the case has seen whether an acknowledgement arrives.
//
// The arrangement is now refused as the Dag is built (the case above); were
// it ever built again, the acknowledgement would have to wait for the prepare.
TEST(SinkChainOwnership, NoCheckpointIsAcknowledgedBeforeATransactionalSinkHasPrepared) {
    for (const bool transactional_first : {true, false}) {
        SCOPED_TRACE(transactional_first ? "transactional sink first"
                                         : "transactional sink second");
        auto transactional = std::make_shared<HeldPrepareSink>(/*hold=*/true);
        OwnershipJob job(transactional);
        clink::Dag dag;
        auto src = dag.add_source<int>(std::make_shared<OwnershipScriptSource>(
            std::vector<OwnershipStep>{ownership_rows(0, 10), ownership_barrier(1)}));
        auto op = dag.add_operator<int, int>(src, ownership_identity());
        try {
            if (transactional_first) {
                dag.add_sink<int>(op, transactional);
                dag.add_sink<int>(op, std::make_shared<OwnershipPlainSink>());
            } else {
                dag.add_sink<int>(op, std::make_shared<OwnershipPlainSink>());
                dag.add_sink<int>(op, transactional);
            }
        } catch (const std::logic_error& e) {
            EXPECT_NE(std::string{e.what()}.find(kTransactionalRule), std::string::npos)
                << e.what();
            continue;
        }
        job.start(std::move(dag));
        const bool acked_before_prepare = job.await_ack(1, 5s);
        transactional->release();
        job.await_termination();
        EXPECT_FALSE(acked_before_prepare)
            << "checkpoint 1 was acknowledged while the transactional sink had not prepared";
        for (const auto& ack : job.acks()) {
            if (ack.ok && ack.checkpoint == 1) {
                EXPECT_EQ(ack.prepared, 1U)
                    << "checkpoint 1 was acknowledged before its transaction was prepared";
            }
        }
    }
}

// As the only sink, the two-phase-commit sink owns the checkpoint: no
// acknowledgement while its prepare is held, and once it is acknowledged the
// handle is in operator state, where the snapshot found it.
TEST(SinkChainOwnership, ASoleTransactionalSinksCheckpointWaitsForItsPrepare) {
    auto transactional = std::make_shared<HeldPrepareSink>(/*hold=*/true);
    OwnershipJob job(transactional);
    clink::Dag dag;
    auto src = dag.add_source<int>(std::make_shared<OwnershipScriptSource>(
        std::vector<OwnershipStep>{ownership_rows(0, 10), ownership_barrier(1)}));
    auto op = dag.add_operator<int, int>(src, ownership_identity());
    dag.add_sink<int>(op, transactional);
    job.start(std::move(dag));

    ASSERT_TRUE(transactional->await_preparing(10s)) << "the sink never began its prepare";
    EXPECT_TRUE(job.acks().empty()) << "a checkpoint was acknowledged during the prepare";
    transactional->release();
    // What the worker does once the coordinator completes the checkpoint: the
    // end of the stream waits for the handle to be finalised.
    ASSERT_TRUE(job.await_ack(1, 10s)) << "checkpoint 1 was never acknowledged";
    transactional->on_commit(1);
    job.await_termination();

    EXPECT_TRUE(job.errors().empty());
    const auto acks = job.acks();
    ASSERT_FALSE(acks.empty());
    EXPECT_EQ(acks.front().checkpoint, 1U);
    EXPECT_TRUE(acks.front().ok);
    EXPECT_EQ(acks.front().prepared, 1U);
}

// A sink stage built with add_parallel_sink, as a local run of a pipeline above
// parallelism 1 builds one, gets the terminal barrier each source emits at the
// end of the input. A two-phase-commit sink prepares its last interval there,
// and the single-subtask runner then commits it locally; the parallel runner
// did not, so the last interval stayed prepared and was never published.
namespace {

class CountingCommitSink final : public clink::CommittingSink<int, std::string> {
public:
    [[nodiscard]] std::string name() const override { return "counting_commit_sink"; }
    [[nodiscard]] int commits() const { return commits_.load(); }
    [[nodiscard]] int written() const { return written_.load(); }

protected:
    void write(const clink::Batch<int>& batch) override {
        written_ += static_cast<int>(batch.size());
    }
    std::optional<std::string> prepare_commit(std::uint64_t checkpoint_id) override {
        return std::to_string(checkpoint_id);
    }
    bool commit(const std::string& /*committable*/) override {
        ++commits_;
        return true;
    }
    std::string serialize(const std::string& committable) const override { return committable; }
    std::string deserialize(std::string_view bytes) const override { return std::string{bytes}; }

private:
    std::atomic<int> commits_{0};
    std::atomic<int> written_{0};
};

}  // namespace

TEST(SinkChainOwnership, AParallelSinkStageCommitsItsLastIntervalAtTheEndOfTheInput) {
    std::vector<std::shared_ptr<CountingCommitSink>> sinks{std::make_shared<CountingCommitSink>(),
                                                           std::make_shared<CountingCommitSink>()};
    clink::Dag dag;
    auto src = dag.add_parallel_source<int>(
        [](std::size_t subtask) -> std::shared_ptr<clink::Source<int>> {
            const int base = static_cast<int>(subtask) * 100;
            return std::make_shared<OwnershipScriptSource>(
                std::vector<OwnershipStep>{ownership_rows(base, base + 10)});
        },
        2);
    dag.add_parallel_sink<int>(
        src,
        [&sinks](std::size_t subtask) -> std::shared_ptr<clink::Sink<int>> {
            return sinks.at(subtask);
        },
        2);
    clink::JobConfig config;
    config.state_backend = std::make_shared<clink::InMemoryStateBackend>();
    clink::LocalExecutor exec(std::move(dag), config);
    exec.start();
    exec.await_termination();

    EXPECT_TRUE(exec.operator_errors().empty());
    for (std::size_t i = 0; i < sinks.size(); ++i) {
        EXPECT_EQ(sinks[i]->written(), 10) << "subtask " << i;
        EXPECT_EQ(sinks[i]->commits(), 1)
            << "subtask " << i << " left its last interval prepared and never published it";
    }
}

// Parallel stages take no part in a checkpoint: their operator and sink
// subtasks snapshot nothing and acknowledge nothing. LocalExecutor ran such a
// Dag with an acknowledgement callback all the same, so its checkpoints never
// completed, or completed under a rule that counted the other runners'
// acknowledgements over state nothing had captured. It now refuses the
// callback, for a parallel sink and for a parallel operator, and still runs
// the same Dags without one.
TEST(SinkChainOwnership, ADagWithParallelStagesIsRefusedAnAcknowledgementCallback) {
    const auto source = [](std::size_t subtask) -> std::shared_ptr<clink::Source<int>> {
        const int base = static_cast<int>(subtask) * 100;
        return std::make_shared<OwnershipScriptSource>(
            std::vector<OwnershipStep>{ownership_rows(base, base + 10)});
    };
    const auto sink = [](std::size_t) -> std::shared_ptr<clink::Sink<int>> {
        return std::make_shared<OwnershipPlainSink>();
    };
    const std::vector<std::pair<std::string, std::function<clink::Dag()>>> dags{
        {"a parallel sink",
         [&] {
             clink::Dag dag;
             dag.add_parallel_sink<int>(dag.add_parallel_source<int>(source, 2), sink, 1);
             return dag;
         }},
        {"a parallel operator",
         [&] {
             clink::Dag dag;
             auto op = dag.add_parallel_operator<int, int>(
                 dag.add_parallel_source<int>(source, 2),
                 [](std::size_t) -> std::shared_ptr<clink::Operator<int, int>> {
                     return ownership_identity();
                 },
                 2);
             dag.add_parallel_sink<int>(op, sink, 2);
             return dag;
         }},
    };
    for (const auto& [what, build] : dags) {
        clink::JobConfig acknowledged;
        acknowledged.state_backend = std::make_shared<clink::InMemoryStateBackend>();
        acknowledged.on_checkpoint_ack = [](clink::CheckpointId, bool, std::string) {};
        try {
            clink::LocalExecutor refused(build(), acknowledged);
            ADD_FAILURE() << "ran a Dag with " << what << " with an acknowledgement callback";
        } catch (const std::logic_error& e) {
            EXPECT_NE(std::string{e.what()}.find("parallel stages"), std::string::npos) << e.what();
        }
        clink::JobConfig quiet;
        quiet.state_backend = std::make_shared<clink::InMemoryStateBackend>();
        clink::LocalExecutor runs(build(), quiet);
        runs.run();
        EXPECT_TRUE(runs.operator_errors().empty()) << what;
    }
}

// A two-phase-commit sink refuses a run without a state backend before it
// takes a record. It used to open and take every record: with no backend its
// prepared handles had nowhere to go and no end-of-input checkpoint came, so
// nothing was published, and the run still finished without an error.
TEST(SinkChainOwnership, ATwoPhaseCommitSinkRefusesARunWithoutAStateBackend) {
    clink::Dag dag;
    const auto source = dag.add_source<int>(
        std::make_shared<OwnershipScriptSource>(std::vector<OwnershipStep>{ownership_rows(0, 10)}));
    auto sink = std::make_shared<HeldPrepareSink>(/*hold=*/false);
    dag.add_sink<int>(source, sink);
    clink::LocalExecutor exec(std::move(dag), clink::JobConfig{});
    exec.run();
    bool refused = false;
    for (const auto& [op, message] : exec.operator_errors()) {
        refused = refused ||
                  message.find("[held_prepare_sink] needs a state backend") != std::string::npos;
    }
    EXPECT_TRUE(refused) << "the sink ran without a state backend";
    EXPECT_EQ(sink->prepared(), 0u);
}

// A Dag declared to run where no checkpoint completes may hold sinks that must
// own their chain's checkpoint side by side: its only barrier is the
// end-of-input one, which each sink commits itself. LocalExecutor refuses such
// a Dag with an acknowledgement callback, under which a checkpoint could
// complete before either sink had prepared.
TEST(SinkChainOwnership, ADagThatCompletesNoCheckpointMayShareItsChainButNotWithAnAckCallback) {
    const auto build = [] {
        clink::Dag dag;
        dag.run_without_checkpoint_completion();
        const auto source = dag.add_source<int>(std::make_shared<OwnershipScriptSource>(
            std::vector<OwnershipStep>{ownership_rows(0, 10)}));
        dag.add_sink<int>(source, std::make_shared<HeldPrepareSink>(/*hold=*/false));
        dag.add_sink<int>(source, std::make_shared<OwnershipGatingSink>());
        return dag;
    };
    clink::JobConfig quiet;
    quiet.state_backend = std::make_shared<clink::InMemoryStateBackend>();
    clink::LocalExecutor runs(build(), quiet);
    runs.run();
    EXPECT_TRUE(runs.operator_errors().empty());

    clink::JobConfig acknowledged;
    acknowledged.state_backend = std::make_shared<clink::InMemoryStateBackend>();
    acknowledged.on_checkpoint_ack = [](clink::CheckpointId, bool, std::string) {};
    try {
        clink::LocalExecutor refused(build(), acknowledged);
        ADD_FAILURE() << "ran a no-completion Dag with an acknowledgement callback";
    } catch (const std::logic_error& e) {
        EXPECT_NE(std::string{e.what()}.find("run_without_checkpoint_completion"),
                  std::string::npos)
            << e.what();
    }
}

// Sinks that need no ownership still share a chain: refusing every multi-sink
// chain would break fan-out that is safe.
TEST(SinkChainOwnership, SinksThatNeedNoOwnershipStillShareAChain) {
    clink::Dag dag;
    auto src =
        dag.add_source<int>(std::make_shared<OwnershipScriptSource>(std::vector<OwnershipStep>{}));
    auto op = dag.add_operator<int, int>(src, ownership_identity());
    EXPECT_NO_THROW({
        dag.add_sink<int>(op, std::make_shared<OwnershipPlainSink>());
        dag.add_sink<int>(op, std::make_shared<OwnershipPlainSink>());
    });
}
