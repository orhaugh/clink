// The text sink in a running job. Its at-least-once guarantee rests on the
// checkpoint being acknowledged only after the sink's barrier flush, which
// holds only while the sink owns its chain's checkpoint: while it is the only
// sink in its Dag. A second sink hands the checkpoint back to the chain's
// last operator, which snapshots and acknowledges as the barrier passes it,
// before either sink has written what it holds.
//
// These cases run the sink in real Dags under a LocalExecutor with an
// in-memory state backend, with a stand-in for the server that counts the
// rows it takes and can hold an INSERT until the case lets it go. What each
// case observes is how many rows the server had taken when each checkpoint was
// acknowledged, so no ordering is inferred from timing.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/connectors/clickhouse_sink.hpp"
#include "clink/core/types.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace {

using namespace std::chrono_literals;
using clink::ClickHouseSink;

// One step of a scripted source: a batch of rows, or a checkpoint barrier.
struct ChainScriptStep {
    bool barrier{false};
    std::vector<std::string> rows;
    std::uint64_t checkpoint{0};
};

ChainScriptStep chain_rows(int from, int to) {
    ChainScriptStep s;
    for (int i = from; i < to; ++i) {
        s.rows.push_back(R"({"id":)" + std::to_string(i) + "}");
    }
    return s;
}

ChainScriptStep chain_barrier(std::uint64_t checkpoint) {
    ChainScriptStep s;
    s.barrier = true;
    s.checkpoint = checkpoint;
    return s;
}

// Plays its script, then ends.
class ChainScriptSource final : public clink::Source<std::string> {
public:
    explicit ChainScriptSource(std::vector<ChainScriptStep> steps) : steps_(std::move(steps)) {}

    bool produce(clink::Emitter<std::string>& out) override {
        if (this->cancelled() || next_ >= steps_.size()) {
            return false;
        }
        const ChainScriptStep& step = steps_[next_++];
        if (step.barrier) {
            return out.emit_barrier(clink::CheckpointBarrier{clink::CheckpointId{step.checkpoint}});
        }
        clink::Batch<std::string> batch;
        for (const auto& r : step.rows) {
            batch.emplace(r);
        }
        return out.emit_data(std::move(batch));
    }

    [[nodiscard]] bool is_bounded() const noexcept override { return true; }
    [[nodiscard]] std::string name() const override { return "clickhouse_sink_chain.source"; }

private:
    std::vector<ChainScriptStep> steps_;
    std::size_t next_{0};
};

// Stands in for the server: counts the rows of every INSERT it takes. With
// `hold` set, each INSERT waits inside send_insert() until release(); with
// `refuse` set, each one is refused, as a server that rejected it would.
class GatedChainClickHouseSink final : public ClickHouseSink {
public:
    explicit GatedChainClickHouseSink(Options opts, bool hold = false, bool refuse = false)
        : ClickHouseSink(std::move(opts)), hold_(hold), refuse_(refuse) {}

    [[nodiscard]] std::uint64_t rows_written() const { return rows_written_.load(); }

    // Waits until an INSERT is being held, up to `limit`.
    bool await_held(std::chrono::milliseconds limit) {
        std::unique_lock lock(mu_);
        return cv_.wait_for(lock, limit, [this] { return held_; });
    }

    void release() {
        {
            const std::lock_guard lock(mu_);
            released_ = true;
        }
        cv_.notify_all();
    }

protected:
    void connect() override {}

    void send_insert(const std::string& statement) override {
        if (refuse_) {
            throw std::runtime_error("the stand-in server refused the INSERT");
        }
        if (hold_) {
            std::unique_lock lock(mu_);
            held_ = true;
            cv_.notify_all();
            cv_.wait(lock, [this] { return released_; });
        }
        // The rows follow the statement's first line, one per line.
        const auto body = statement.find('\n');
        std::uint64_t rows = 0;
        for (std::size_t i = body; i != std::string::npos && i + 1 < statement.size();
             i = statement.find('\n', i + 1)) {
            if (statement[i + 1] != '\n') {
                ++rows;
            }
        }
        rows_written_ += rows;
    }

private:
    const bool hold_;
    const bool refuse_;
    std::atomic<std::uint64_t> rows_written_{0};
    std::mutex mu_;
    std::condition_variable cv_;
    bool held_{false};
    bool released_{false};
};

ClickHouseSink::Options chain_sink_options() {
    ClickHouseSink::Options opts;
    opts.table = "events";
    opts.format = ClickHouseSink::Format::JSONEachRow;
    // Only the barrier and the end of the stream flush.
    opts.batch_rows = 1'000'000;
    opts.batch_interval = std::chrono::hours{1};
    return opts;
}

class ChainDiscardSink final : public clink::Sink<std::string> {
public:
    void on_data(const clink::Batch<std::string>& /*batch*/) override {}
    [[nodiscard]] std::string name() const override { return "clickhouse_sink_chain.discard"; }
};

std::shared_ptr<clink::MapOperator<std::string, std::string>> chain_identity() {
    return std::make_shared<clink::MapOperator<std::string, std::string>>(
        [](const std::string& s) { return s; }, "clickhouse_sink_chain.identity");
}

// One acknowledgement: the checkpoint, whether it succeeded, and how many rows
// the server had taken at that moment.
struct ChainAck {
    std::uint64_t checkpoint{0};
    bool ok{false};
    std::uint64_t rows_written{0};
};

// A job around one GatedChainClickHouseSink, recording every acknowledgement.
class ChainJob {
public:
    explicit ChainJob(std::shared_ptr<GatedChainClickHouseSink> sink) : sink_(std::move(sink)) {
        config_.state_backend = std::make_shared<clink::InMemoryStateBackend>();
        config_.on_checkpoint_ack = [this](clink::CheckpointId id, bool ok, const std::string&) {
            {
                const std::lock_guard lock(mu_);
                acks_.push_back({id.value(), ok, sink_->rows_written()});
            }
            cv_.notify_all();
        };
    }

    ChainJob(const ChainJob&) = delete;
    ChainJob& operator=(const ChainJob&) = delete;
    ChainJob(ChainJob&&) = delete;
    ChainJob& operator=(ChainJob&&) = delete;

    ~ChainJob() {
        // A case that failed part-way may have left an INSERT held.
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

    // Waits until `checkpoint` has been acknowledged, up to `limit`.
    bool await_ack(std::uint64_t checkpoint, std::chrono::milliseconds limit) {
        std::unique_lock lock(mu_);
        return cv_.wait_for(lock, limit, [&] { return find_(checkpoint) != nullptr; });
    }

    [[nodiscard]] std::vector<ChainAck> acks() const {
        const std::lock_guard lock(mu_);
        return acks_;
    }

private:
    const ChainAck* find_(std::uint64_t checkpoint) const {
        for (const auto& a : acks_) {
            if (a.checkpoint == checkpoint) {
                return &a;
            }
        }
        return nullptr;
    }

    std::shared_ptr<GatedChainClickHouseSink> sink_;
    clink::JobConfig config_;
    std::unique_ptr<clink::LocalExecutor> exec_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::vector<ChainAck> acks_;
};

constexpr const char* kOnlySinkRule = "clickhouse_sink must be the only sink on its chain";

}  // namespace

// A second sink on the text sink's chain is refused as the Dag is built,
// whichever of the two is added first, with or without an operator between the
// source and the sinks, and the message names the text sink and says where the
// other sink belongs.
TEST(ClickHouseSinkChain, TheSinkMustBeTheOnlySinkOnItsChainWhicheverIsAddedFirst) {
    for (const bool with_operator : {true, false}) {
        for (const bool text_sink_first : {true, false}) {
            SCOPED_TRACE(std::string{with_operator ? "behind an operator" : "on the source"} +
                         (text_sink_first ? ", text sink first" : ", text sink second"));
            clink::Dag dag;
            auto src = dag.add_source<std::string>(
                std::make_shared<ChainScriptSource>(std::vector<ChainScriptStep>{}));
            auto tail = with_operator
                            ? dag.add_operator<std::string, std::string>(src, chain_identity())
                            : src;
            auto text_sink = std::make_shared<GatedChainClickHouseSink>(chain_sink_options());
            auto other = std::make_shared<ChainDiscardSink>();
            try {
                if (text_sink_first) {
                    dag.add_sink<std::string>(tail, text_sink);
                    dag.add_sink<std::string>(tail, other);
                } else {
                    dag.add_sink<std::string>(tail, other);
                    dag.add_sink<std::string>(tail, text_sink);
                }
                ADD_FAILURE() << "a second sink joined the text sink's chain";
            } catch (const std::logic_error& e) {
                const std::string what = e.what();
                EXPECT_NE(what.find(kOnlySinkRule), std::string::npos) << what;
                EXPECT_NE(what.find("its own subtask"), std::string::npos) << what;
            }
        }
    }
}

// The regression. With a second sink on its chain the text sink no longer
// owns the checkpoint: the operator in front of the sinks snapshots and
// acknowledges it as the barrier passes, so the checkpoint completed while
// the text sink still held every row from before the barrier, and a restore
// from it would have resumed the source past rows ClickHouse never received.
// The server here holds the barrier's INSERT until the case has seen the
// acknowledgement, so the acknowledgement cannot be put down to timing.
//
// The topology is now refused as the Dag is built (the case above); were it
// ever built again, the acknowledgement would have to wait for the flush.
TEST(ClickHouseSinkChain, NoCheckpointIsAcknowledgedWhileTheSinkHoldsRowsFromBeforeIt) {
    for (const bool text_sink_first : {true, false}) {
        SCOPED_TRACE(text_sink_first ? "text sink first" : "text sink second");
        auto text_sink =
            std::make_shared<GatedChainClickHouseSink>(chain_sink_options(), /*hold=*/true);
        ChainJob job(text_sink);
        clink::Dag dag;
        auto src = dag.add_source<std::string>(std::make_shared<ChainScriptSource>(
            std::vector<ChainScriptStep>{chain_rows(0, 10), chain_barrier(1)}));
        auto op = dag.add_operator<std::string, std::string>(src, chain_identity());
        try {
            if (text_sink_first) {
                dag.add_sink<std::string>(op, text_sink);
                dag.add_sink<std::string>(op, std::make_shared<ChainDiscardSink>());
            } else {
                dag.add_sink<std::string>(op, std::make_shared<ChainDiscardSink>());
                dag.add_sink<std::string>(op, text_sink);
            }
        } catch (const std::logic_error& e) {
            EXPECT_NE(std::string{e.what()}.find(kOnlySinkRule), std::string::npos) << e.what();
            continue;
        }
        job.start(std::move(dag));
        // Either the acknowledgement arrives while the INSERT is held (the
        // defect), or nothing acknowledges the checkpoint before the flush.
        const bool acked_while_held = job.await_ack(1, 5s);
        text_sink->release();
        job.await_termination();
        EXPECT_FALSE(acked_while_held)
            << "checkpoint 1 was acknowledged while the text sink held its 10 rows";
        for (const auto& ack : job.acks()) {
            if (ack.ok && ack.checkpoint == 1) {
                EXPECT_EQ(ack.rows_written, 10U)
                    << "checkpoint 1 was acknowledged over rows ClickHouse had not taken";
            }
        }
    }
}

// As the only sink on its chain, the text sink owns the checkpoint: it is not
// acknowledged while the barrier's INSERT is still in flight, and once it is,
// every row from before the barrier has been taken. Behind an operator and
// straight off the source alike.
TEST(ClickHouseSinkChain, AsTheOnlySinkItsCheckpointWaitsForItsBarrierFlush) {
    for (const bool with_operator : {true, false}) {
        SCOPED_TRACE(with_operator ? "behind an operator" : "on the source");
        auto text_sink =
            std::make_shared<GatedChainClickHouseSink>(chain_sink_options(), /*hold=*/true);
        ChainJob job(text_sink);
        clink::Dag dag;
        auto src = dag.add_source<std::string>(
            std::make_shared<ChainScriptSource>(std::vector<ChainScriptStep>{
                chain_rows(0, 10), chain_barrier(1), chain_rows(10, 15), chain_barrier(2)}));
        auto tail =
            with_operator ? dag.add_operator<std::string, std::string>(src, chain_identity()) : src;
        dag.add_sink<std::string>(tail, text_sink);
        job.start(std::move(dag));

        ASSERT_TRUE(text_sink->await_held(10s)) << "the barrier's INSERT never reached the server";
        // The sink acknowledges on its own thread once the INSERT returns, and
        // that thread is inside the INSERT now.
        EXPECT_TRUE(job.acks().empty()) << "a checkpoint was acknowledged during the barrier flush";
        text_sink->release();
        job.await_termination();

        EXPECT_TRUE(job.errors().empty());
        const auto acks = job.acks();
        ASSERT_EQ(acks.size(), 2U);
        EXPECT_EQ(acks[0].checkpoint, 1U);
        EXPECT_TRUE(acks[0].ok);
        EXPECT_EQ(acks[0].rows_written, 10U);
        EXPECT_EQ(acks[1].checkpoint, 2U);
        EXPECT_TRUE(acks[1].ok);
        EXPECT_EQ(acks[1].rows_written, 15U);
    }
}

// A barrier flush the server refuses throws out of the barrier hook: the job
// fails with the server's reason, and the checkpoint is never acknowledged as
// complete.
TEST(ClickHouseSinkChain, AFailedBarrierFlushAcknowledgesNoCheckpoint) {
    auto text_sink = std::make_shared<GatedChainClickHouseSink>(
        chain_sink_options(), /*hold=*/false, /*refuse=*/true);
    ChainJob job(text_sink);
    clink::Dag dag;
    auto src = dag.add_source<std::string>(std::make_shared<ChainScriptSource>(
        std::vector<ChainScriptStep>{chain_rows(0, 10), chain_barrier(1)}));
    auto op = dag.add_operator<std::string, std::string>(src, chain_identity());
    dag.add_sink<std::string>(op, text_sink);
    job.start(std::move(dag));
    job.await_termination();

    for (const auto& ack : job.acks()) {
        EXPECT_FALSE(ack.ok) << "checkpoint " << ack.checkpoint
                             << " was acknowledged as complete over a refused flush";
    }
    const auto errors = job.errors();
    ASSERT_FALSE(errors.empty()) << "the refused flush did not fail the job";
    bool named = false;
    for (const auto& [op, message] : errors) {
        named = named || message.find("refused the INSERT") != std::string::npos;
    }
    EXPECT_TRUE(named) << "the job failed for another reason: " << errors.front().second;
    EXPECT_EQ(text_sink->rows_written(), 0U);
}
