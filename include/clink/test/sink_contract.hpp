#pragma once

// clink::test sink contract suite - the two-phase-commit obligations,
// derived from the connector's own capability record.
//
// The companion of source_contract.hpp. A transactional sink's guarantee
// lives entirely in its crash windows: nothing may be visible before
// commit, a commit delivered twice must land once, an abort must leave no
// trace, and a prepared-but-uncommitted transaction must survive the death
// of the process that prepared it and be finalised by a fresh instance -
// exactly once, however many times recovery runs. Every case here drives
// the REAL CommittingSink choreography (open -> on_data -> on_barrier ->
// on_commit / on_abort, with the prepared handle persisted in a
// StateBackend at the barrier), because that choreography - not the
// connector's write path - is what the guarantee analyser believes when a
// record says ExactlyOnceTwoPhaseCommit.
//
// The record is the gate, strictly: this suite is FOR transactional
// exactly-once sinks, so an adapter naming a record that claims anything
// weaker FAILS rather than skipping - either the record under-claims (fix
// the record; the source suite has already caught one of those) or the
// sink does not belong here.
//
// Part of the public clink testing API
// (docs/internals/testing-framework.md); in-tree instantiations are
// tests/test_sink_contract.cpp.
//
// Adapter shape (duck-typed; every member required):
//
//   struct MySinkContract {
//       using Value = <element type T>;
//       static constexpr std::string_view kCapabilityName = "my_2pc";
//       // A fixture over a scratch destination under `dir`:
//       //   fresh     - a new, identically-configured sink over the SAME
//       //               destination (the crash cases construct several).
//       //   committed - the records currently visible as COMMITTED in the
//       //               destination. Committed means what the record
//       //               promises an external reader: staged/pending output
//       //               must not appear here.
//       //   records   - distinct sample records to write.
//       static SinkContractFixture<Value> make(const std::filesystem::path& dir);
//   };
//
//   namespace clink::test {
//   INSTANTIATE_TYPED_TEST_SUITE_P(My2pc, SinkContractSuite, MySinkContract);
//   }

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/capability.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace clink::test {

template <typename T>
struct SinkContractFixture {
    std::function<std::shared_ptr<Sink<T>>()> fresh;
    std::function<std::vector<T>()> committed;
    std::vector<T> records;
};

namespace sink_contract_detail {

template <typename T>
std::vector<T> sorted(std::vector<T> v) {
    std::sort(v.begin(), v.end());
    return v;
}

template <typename T>
Batch<T> batch_of(const std::vector<T>& xs) {
    Batch<T> b;
    for (const auto& x : xs) {
        b.emplace(x);
    }
    return b;
}

}  // namespace sink_contract_detail

template <typename Adapter>
class SinkContractSuite : public ::testing::Test {
protected:
    using T = typename Adapter::Value;

    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() /
               ("clink_sink_contract_" + std::to_string(::getpid()) + "_" +
                ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
        backend_ = std::make_unique<clink::InMemoryStateBackend>();
        rctx_ = std::make_unique<RuntimeContext>(
            clink::OperatorId{42}, "contract_sink", backend_.get(), /*metrics=*/nullptr);
    }
    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    // A fresh sink wired to the SHARED backend - the crash cases depend on
    // instance N+1 seeing the handles instance N persisted, which is
    // exactly what the worker gives a restarted subtask.
    std::shared_ptr<Sink<T>> wired(const SinkContractFixture<T>& fx) {
        auto sink = fx.fresh();
        sink->set_id(clink::OperatorId{42});
        sink->attach_runtime(rctx_.get());
        return sink;
    }

    static const connectors::ConnectorCapabilities* record() {
        return connectors::CapabilityRegistry::instance().find(
            std::string{Adapter::kCapabilityName});
    }

    std::filesystem::path dir_;
    std::unique_ptr<clink::InMemoryStateBackend> backend_;
    std::unique_ptr<RuntimeContext> rctx_;
};

TYPED_TEST_SUITE_P(SinkContractSuite);

// The gate, strict on purpose: everything below holds the two-phase
// contract, so a record claiming anything weaker means either the record
// under-claims or this is the wrong suite for the connector. Both are
// findings, not skips.
TYPED_TEST_P(SinkContractSuite, TheCapabilityRecordClaimsTwoPhaseCommit) {
    const auto* rec = this->record();
    ASSERT_NE(rec, nullptr) << "no capability record named '" << TypeParam::kCapabilityName << "'";
    EXPECT_TRUE(rec->is_sink);
    EXPECT_TRUE(rec->transactional) << "this suite holds the transactional contract";
    EXPECT_TRUE(rec->checkpoint_integrated)
        << "2PC finalisation rides checkpoint completion; a record that opts out of "
           "checkpointing cannot mean two-phase commit";
    EXPECT_TRUE(rec->delivery == connectors::DeliveryGuarantee::ExactlyOnceTwoPhaseCommit ||
                rec->delivery == connectors::DeliveryGuarantee::ExactlyOnceAtomicPublish)
        << "record claims " << to_string(rec->delivery);
}

// The first half of the guarantee: written and even PREPARED output is not
// committed output. An external reader between barrier and commit must see
// nothing.
TYPED_TEST_P(SinkContractSuite, NothingIsVisibleBeforeCommit) {
    const auto fx = TypeParam::make(this->dir_);
    auto sink = this->wired(fx);
    sink->open();
    sink->on_data(sink_contract_detail::batch_of(fx.records));
    EXPECT_TRUE(fx.committed().empty()) << "records visible before any barrier";
    sink->on_barrier(CheckpointBarrier{clink::CheckpointId{1}});
    EXPECT_TRUE(fx.committed().empty())
        << "PREPARED output is visible as committed - the window between prepare and "
           "commit is exactly where a crash must not have published anything";
    sink->close();
}

TYPED_TEST_P(SinkContractSuite, CommitPublishesExactlyTheWrittenRecords) {
    const auto fx = TypeParam::make(this->dir_);
    auto sink = this->wired(fx);
    sink->open();
    sink->on_data(sink_contract_detail::batch_of(fx.records));
    sink->on_barrier(CheckpointBarrier{clink::CheckpointId{1}});
    sink->on_commit(1);
    EXPECT_EQ(sink_contract_detail::sorted(fx.committed()),
              sink_contract_detail::sorted(fx.records));
    sink->close();
}

// A commit broadcast can be re-delivered (coordinator retry, recovery
// replay). The second delivery must change nothing - on the same instance
// AND on a fresh one that never saw the first.
TYPED_TEST_P(SinkContractSuite, RedeliveredCommitIsIdempotent) {
    const auto fx = TypeParam::make(this->dir_);
    auto sink = this->wired(fx);
    sink->open();
    sink->on_data(sink_contract_detail::batch_of(fx.records));
    sink->on_barrier(CheckpointBarrier{clink::CheckpointId{1}});
    sink->on_commit(1);
    const auto after_first = sink_contract_detail::sorted(fx.committed());
    sink->on_commit(1);
    EXPECT_EQ(sink_contract_detail::sorted(fx.committed()), after_first)
        << "a re-delivered commit changed the committed output";
    sink->close();

    auto again = this->wired(fx);
    again->open();
    again->on_commit(1);
    EXPECT_EQ(sink_contract_detail::sorted(fx.committed()), after_first)
        << "a commit re-delivered to a fresh instance changed the committed output";
    again->close();
}

TYPED_TEST_P(SinkContractSuite, AbortLeavesNothingVisibleAndIsIdempotent) {
    const auto fx = TypeParam::make(this->dir_);
    auto sink = this->wired(fx);
    sink->open();
    sink->on_data(sink_contract_detail::batch_of(fx.records));
    sink->on_barrier(CheckpointBarrier{clink::CheckpointId{1}});
    sink->on_abort(1);
    EXPECT_TRUE(fx.committed().empty()) << "aborted output is visible as committed";
    sink->on_abort(1);
    EXPECT_TRUE(fx.committed().empty());
    sink->close();
}

// The window that defines 2PC: prepared, handle persisted, then the
// process dies before the commit broadcast arrives. A fresh instance's
// open() must finalise the orphan - once, however many fresh instances
// run recovery.
TYPED_TEST_P(SinkContractSuite, CrashAfterPrepareRecoversExactlyOnce) {
    const auto fx = TypeParam::make(this->dir_);
    {
        auto sink = this->wired(fx);
        sink->open();
        sink->on_data(sink_contract_detail::batch_of(fx.records));
        sink->on_barrier(CheckpointBarrier{clink::CheckpointId{1}});
        // Destroyed without on_commit/on_abort/close: the crash.
    }
    ASSERT_TRUE(fx.committed().empty()) << "output visible before any recovery ran";

    auto recovered = this->wired(fx);
    recovered->open();
    EXPECT_EQ(sink_contract_detail::sorted(fx.committed()),
              sink_contract_detail::sorted(fx.records))
        << "a prepared-but-uncommitted transaction was not finalised by recovery - "
           "every record acked into it is lost";
    recovered->close();

    auto second_recovery = this->wired(fx);
    second_recovery->open();
    EXPECT_EQ(sink_contract_detail::sorted(fx.committed()),
              sink_contract_detail::sorted(fx.records))
        << "a second recovery duplicated the recovered output";
    second_recovery->close();
}

// Dying BEFORE prepare must leave no external side effect: no barrier, no
// handle, so a fresh instance's recovery has nothing to finalise and the
// written-but-unprepared records simply never existed downstream.
TYPED_TEST_P(SinkContractSuite, CrashBeforePrepareLeavesNothingVisible) {
    const auto fx = TypeParam::make(this->dir_);
    {
        auto sink = this->wired(fx);
        sink->open();
        sink->on_data(sink_contract_detail::batch_of(fx.records));
        // Destroyed without a barrier: nothing was ever prepared.
    }
    auto recovered = this->wired(fx);
    recovered->open();
    EXPECT_TRUE(fx.committed().empty())
        << "records that never reached a checkpoint barrier were published";
    recovered->close();
}

// Checkpoints are independent transactions: committing N publishes N's
// records and nothing staged for N+1.
TYPED_TEST_P(SinkContractSuite, CheckpointsCommitIndependently) {
    using T = typename TypeParam::Value;
    const auto fx = TypeParam::make(this->dir_);
    ASSERT_GE(fx.records.size(), 2u) << "adapter must supply at least two records";
    const std::vector<T> first(fx.records.begin(),
                               fx.records.begin() + static_cast<std::ptrdiff_t>(1));
    const std::vector<T> rest(fx.records.begin() + 1, fx.records.end());

    auto sink = this->wired(fx);
    sink->open();
    sink->on_data(sink_contract_detail::batch_of(first));
    sink->on_barrier(CheckpointBarrier{clink::CheckpointId{1}});
    sink->on_data(sink_contract_detail::batch_of(rest));
    sink->on_barrier(CheckpointBarrier{clink::CheckpointId{2}});

    sink->on_commit(1);
    EXPECT_EQ(sink_contract_detail::sorted(fx.committed()), sink_contract_detail::sorted(first))
        << "committing checkpoint 1 published records staged for checkpoint 2";
    sink->on_commit(2);
    EXPECT_EQ(sink_contract_detail::sorted(fx.committed()),
              sink_contract_detail::sorted(fx.records));
    sink->close();
}

REGISTER_TYPED_TEST_SUITE_P(SinkContractSuite,
                            TheCapabilityRecordClaimsTwoPhaseCommit,
                            NothingIsVisibleBeforeCommit,
                            CommitPublishesExactlyTheWrittenRecords,
                            RedeliveredCommitIsIdempotent,
                            AbortLeavesNothingVisibleAndIsIdempotent,
                            CrashAfterPrepareRecoversExactlyOnce,
                            CrashBeforePrepareLeavesNothingVisible,
                            CheckpointsCommitIndependently);

}  // namespace clink::test
