#include <atomic>
#include <future>
#include <latch>
#include <thread>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/runtime/arrow_memory_pool.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/memory_size.hpp"
#include "clink/runtime/snapshot_worker.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/snapshot_arrow_writer.hpp"

using namespace clink;

TEST(MemoryBudget, ConcurrentReservationsCannotOversubscribe) {
    auto budget = std::make_shared<MemoryBudget>(100);
    std::atomic<int> holders{0};
    std::atomic<bool> release{false};
    std::latch attempted(16);
    std::vector<std::thread> threads;
    for (int i = 0; i < 16; ++i)
        threads.emplace_back([&] {
            try {
                MemoryReservation reservation(budget, MemoryCategory::State, 30);
                ++holders;
                attempted.count_down();
                while (!release.load())
                    std::this_thread::yield();
            } catch (const MemoryLimitExceeded&) {
                attempted.count_down();
            }
        });
    attempted.wait();
    EXPECT_EQ(holders.load(), 3);
    EXPECT_EQ(budget->usage().used, 90u);
    release = true;
    for (auto& thread : threads)
        thread.join();
    EXPECT_EQ(budget->usage().used, 0u);
    EXPECT_EQ(budget->usage().peak, 90u);
}

TEST(MemoryBudget, ChildRefusalAndFailedGrowthPreserveReservations) {
    auto root = std::make_shared<MemoryBudget>(100);
    auto child = std::make_shared<MemoryBudget>(70, "aggregate", root);
    MemoryReservation retained(child, MemoryCategory::State, 60);
    EXPECT_THROW(retained.resize(80), MemoryLimitExceeded);
    EXPECT_EQ(root->usage().used, 60u);
    MemoryReservation queue(root, MemoryCategory::Queue, 35);
    EXPECT_THROW(retained.resize(70), MemoryLimitExceeded);
    EXPECT_EQ(child->usage().used, 60u);
    retained.resize(20);
    EXPECT_EQ(root->usage().used, 55u);
    auto moved = std::move(retained);
    EXPECT_EQ(retained.size(), 0u);
    EXPECT_EQ(moved.size(), 20u);
}

TEST(MemoryBudget, AllocatorKeepsOwnerAcrossThreads) {
    auto budget = std::make_shared<MemoryBudget>(4096);
    std::vector<int, BudgetAllocator<int>> data{BudgetAllocator<int>{budget}};
    data.resize(128);
    EXPECT_GE(budget->usage().used, 512u);
    std::thread consumer([owned = std::move(data)]() mutable { owned.clear(); });
    consumer.join();
    EXPECT_EQ(budget->usage().used, 0u);
}

TEST(MemoryBudget, ArrowReallocationRefusalPreservesBufferAndCharge) {
    auto budget = std::make_shared<MemoryBudget>(256);
    BudgetArrowMemoryPool pool(budget);
    uint8_t* bytes = nullptr;
    ASSERT_TRUE(pool.Allocate(128, &bytes).ok());
    bytes[0] = 42;
    EXPECT_FALSE(pool.Reallocate(128, 256, &bytes).ok());
    EXPECT_EQ(bytes[0], 42);
    EXPECT_EQ(budget->usage().used, 128u);
    ASSERT_TRUE(pool.Reallocate(128, 64, &bytes).ok());
    EXPECT_EQ(budget->usage().used, 64u);
    pool.Free(bytes, 64);
    EXPECT_EQ(budget->usage().used, 0u);
}

TEST(MemoryBudget, QueueFailureDoesNotPublishAndCancellationRetainsQueuedCharge) {
    auto budget = std::make_shared<MemoryBudget>(100);
    {
        BoundedChannel<std::string> queue(10);
        queue.set_memory_budget(budget, [](const std::string& s) { return s.size(); });
        ASSERT_TRUE(queue.push(std::string(80, 'x')));
        EXPECT_THROW(queue.try_push(std::string(30, 'y')), MemoryLimitExceeded);
        EXPECT_EQ(queue.size(), 1u);
        EXPECT_EQ(budget->usage().used, 80u);
        queue.close(ChannelCloseReason::Cancelled);
        EXPECT_EQ(budget->usage().used, 80u);
        EXPECT_EQ(queue.pop()->size(), 80u);
        EXPECT_EQ(budget->usage().used, 0u);
    }
    EXPECT_EQ(budget->usage().used, 0u);
}

TEST(MemoryBudget, ColumnarQueueEstimateDoesNotMaterialiseAndCountsParentBuffers) {
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.AppendValues({1, 2, 3, 4}).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto full =
        arrow::RecordBatch::Make(arrow::schema({arrow::field("v", arrow::int64())}), 4, {array});
    bool materialised = false;
    Batch<int> batch(full->Slice(1, 1), 1, [&](const arrow::RecordBatch&) {
        materialised = true;
        return std::vector<Record<int>>{};
    });
    auto element = StreamElement<int>::data(std::move(batch));
    EXPECT_GE(stream_element_retained_bytes(element), sizeof(element) + 4 * sizeof(int64_t));
    EXPECT_FALSE(materialised);
}

TEST(MemoryBudget, BackendRejectsGrowthWithoutChangingValueAndReleasesOnErase) {
    auto budget = std::make_shared<MemoryBudget>(4096);
    InMemoryStateBackend backend;
    backend.set_memory_budget(budget);
    backend.put(OperatorId{1}, "key", "original");
    const auto before = budget->usage().used;
    EXPECT_THROW(backend.put(OperatorId{1}, "key", std::string(8192, 'x')), MemoryLimitExceeded);
    EXPECT_EQ(budget->usage().used, before);
    ASSERT_TRUE(backend.get(OperatorId{1}, "key"));
    EXPECT_EQ(backend.get(OperatorId{1}, "key")->size(), 8u);
    backend.put(OperatorId{1}, "key", "x");
    EXPECT_EQ(budget->usage().used, before) << "shrinking a value retains its vector capacity";
    backend.erase(OperatorId{1}, "key");
    EXPECT_EQ(budget->usage().used, 0u);
}

TEST(MemoryBudget, SnapshotAllocationFailureReleasesTemporaryBuffers) {
    auto budget = std::make_shared<MemoryBudget>(128);
    EXPECT_THROW((SnapshotArrowWriter{1000, budget}), std::runtime_error);
    EXPECT_EQ(budget->usage().used, 0u);
    EXPECT_GT(budget->usage().refused, 0u);
}

TEST(MemoryBudget, StagedRowsStayChargedUntilSnapshotConsumesThem) {
    auto budget = std::make_shared<MemoryBudget>(1024 * 1024);
    InMemoryStateBackend backend;
    backend.set_memory_budget(budget);
    backend.put(OperatorId{1}, "key", std::string(1000, 'x'));
    const auto live = budget->usage().used;
    backend.stage_operator_rows(OperatorId{1}, CheckpointId{1});
    EXPECT_GT(budget->usage().used, live);
    backend.snapshot(CheckpointId{1});
    EXPECT_EQ(budget->usage().used, live);
    backend.clear();
    EXPECT_EQ(budget->usage().used, 0u);
}

TEST(MemoryBudget, InvalidConfigurationCannotSilentlyDisableLimit) {
    for (const auto* value : {"", "-1", "+1", " 1", "1GB", "184467440737095516160"})
        EXPECT_THROW(parse_memory_limit_bytes(value), std::invalid_argument);
    EXPECT_EQ(parse_memory_limit_bytes("1234"), 1234u);
    EXPECT_EQ(parse_memory_limit_bytes("0"), 0u);
}

namespace {
class BlockingPersistBackend final : public StateBackend {
public:
    std::promise<void> entered;
    std::shared_future<void> release;
    void put(OperatorId, KeyView, ValueView) override {}
    std::optional<Value> get(OperatorId, KeyView) const override { return std::nullopt; }
    void erase(OperatorId, KeyView) override {}
    void scan(OperatorId, const ScanVisitor&) const override {}
    void restore(const Snapshot&, const KeyGroupRange&) override {}
    std::string description() const override { return "budget-test"; }
    Snapshot snapshot(CheckpointId id) override { return Snapshot{.checkpoint_id = id}; }
    Snapshot persist(CaptureHandle handle) override {
        entered.set_value();
        release.wait();
        return Snapshot{.checkpoint_id = handle.checkpoint_id};
    }
};

class BudgetTestSource final : public Source<int> {
public:
    BudgetTestSource(bool fail, std::shared_future<void> ready, std::promise<void>* signal)
        : fail_(fail), ready_(std::move(ready)), signal_(signal) {}
    bool produce(Emitter<int>& out) override {
        if (fail_) {
            ready_.wait();
            MemoryReservation allocation(
                this->runtime()->memory_budget(), MemoryCategory::State, 8192);
            return false;
        }
        if (signal_) {
            signal_->set_value();
            signal_ = nullptr;
        }
        Batch<int> batch;
        batch.emplace(1);
        return out.emit_data(std::move(batch));
    }

private:
    bool fail_;
    std::shared_future<void> ready_;
    std::promise<void>* signal_;
};
}  // namespace

TEST(MemoryBudget, AsyncPersistRetainsChargeAndRefusedCaptureIsNeverAcknowledged) {
    auto budget = std::make_shared<MemoryBudget>(300);
    BlockingPersistBackend backend;
    std::promise<void> release;
    backend.release = release.get_future().share();
    auto entered = backend.entered.get_future();
    std::atomic<int> acknowledgements{0};
    SnapshotWorker worker(1, budget);
    worker.start();
    auto job = [&](int id) {
        return SnapshotWorker::Job{
            .handle = CaptureHandle{.checkpoint_id = CheckpointId{static_cast<std::uint64_t>(id)},
                                    .bytes = std::vector<std::byte>(200)},
            .backend = &backend,
            .ack = [&](CheckpointId, bool ok, std::string) {
                if (ok)
                    ++acknowledgements;
            }};
    };
    ASSERT_TRUE(worker.enqueue(job(1)));
    const auto status = entered.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_EQ(budget->usage().used, 200u);
    EXPECT_THROW(worker.enqueue(job(2)), MemoryLimitExceeded);
    EXPECT_EQ(acknowledgements.load(), 0);
    release.set_value();
    worker.drain_and_join();
    EXPECT_EQ(acknowledgements.load(), 1);
    EXPECT_EQ(budget->usage().used, 0u);
}

TEST(MemoryBudget, FailureWakesBlockedUnrelatedBranch) {
    auto budget = std::make_shared<MemoryBudget>(4096);
    std::promise<void> started;
    auto ready = started.get_future().share();
    Dag dag(1);
    dag.add_source<int>(std::make_shared<BudgetTestSource>(false, ready, &started));
    dag.add_source<int>(std::make_shared<BudgetTestSource>(true, ready, nullptr));
    LocalExecutor executor(std::move(dag), JobConfig{.memory_budget = budget});
    executor.start();
    auto completed = std::async(std::launch::async, [&] { executor.await_termination(); });
    const auto status = completed.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(status, std::future_status::ready);
    // Ensure the test itself can terminate if the wake-up regresses.
    if (status != std::future_status::ready)
        executor.cancel();
    completed.get();
    ASSERT_FALSE(executor.operator_errors().empty());
    EXPECT_NE(executor.operator_errors().front().second.find("MEMORY_LIMIT_EXCEEDED"),
              std::string::npos);
}

namespace {
class BudgetProbeOperator final : public Operator<int, int> {
public:
    std::shared_ptr<MemoryBudget> observed;
    void open() override { observed = this->runtime()->memory_budget(); }
    void process(const StreamElement<int>& value, Emitter<int>& out) override { out.emit(value); }
};
}  // namespace

TEST(MemoryBudget, ChainedOperatorsInheritSharedDomain) {
    auto budget = std::make_shared<MemoryBudget>(1024);
    RuntimeContext context(OperatorId{1}, "chain", nullptr, nullptr);
    context.set_memory_budget(budget);
    auto first = std::make_shared<BudgetProbeOperator>();
    auto second = std::make_shared<BudgetProbeOperator>();
    ChainedOperator<int, int, int> chain(first, second);
    chain.attach_runtime(&context);
    chain.open();
    EXPECT_EQ(first->observed, budget);
    EXPECT_EQ(second->observed, budget);
    chain.close();
    chain.attach_runtime(nullptr);
}

TEST(MemoryBudget, UnknownOperatorQuotaFailsBeforeExecution) {
    JobConfig config;
    config.operator_memory_limits[OperatorId{999}] = 100;
    LocalExecutor executor(Dag{}, config);
    EXPECT_THROW(executor.start(), std::invalid_argument);
    EXPECT_FALSE(executor.running());
}
