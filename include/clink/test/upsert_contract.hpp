#pragma once

// clink::test upsert contract suite - idempotency-key collapse as an
// executable obligation.
//
// The third member of the contract family (source_contract.hpp,
// sink_contract.hpp). An upsert sink's guarantee is NOT two-phase commit:
// it is EffectivelyOnceIdempotent - duplicates reach the external system
// and COLLAPSE there, keyed by the connector's declared idempotency key.
// That claim has a precise executable form: replaying the same batch (the
// at-least-once redelivery every restart produces) must converge the
// external state to the same rows, whether the replay comes from the same
// sink instance or from a fresh one that never saw the first write; and a
// newer write for a key must win over the older one, never sit beside it.
//
// The capability gate is strict, mirroring the sink suite: the record must
// claim EffectivelyOnceIdempotent AND name the option carrying the key
// (idempotency_key_option) - the analyser's warning about that key being
// the user's responsibility is only honest when a suite holds the collapse
// behaviour behind it.
//
// Part of the public clink testing API
// (docs/internals/testing-framework.md).
//
// Adapter shape (duck-typed; every member required):
//
//   struct MyUpsertContract {
//       using Value = <element type T>;
//       static constexpr std::string_view kCapabilityName = "my_upsert";
//       static bool available();          // live systems self-skip
//       static UpsertContractFixture<Value> make(const std::filesystem::path& dir);
//   };
//
// The fixture supplies records over DISTINCT keys, a second set over the
// SAME keys with different values, the expected external state after each,
// and a probe reading the external state as canonical (key, value) pairs.

#include <algorithm>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/capability.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::test {

using UpsertState = std::vector<std::pair<std::string, std::string>>;

template <typename T>
struct UpsertContractFixture {
    std::function<std::shared_ptr<Sink<T>>()> fresh;
    // The external system's current rows as (key, value), any order.
    std::function<UpsertState()> state;
    std::vector<T> records;          // one record per distinct key
    std::vector<T> updated_records;  // the SAME keys, different values
    UpsertState expected;            // state after `records`
    UpsertState expected_updated;    // state after `updated_records`
};

namespace upsert_contract_detail {

inline UpsertState sorted(UpsertState v) {
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

// Write one batch through the real delivery seam: on_data stages, the
// barrier flushes - the same cadence a checkpointing job gives the sink.
template <typename T>
void deliver(Sink<T>& sink, const std::vector<T>& records, std::uint64_t ckpt) {
    sink.on_data(batch_of(records));
    sink.on_barrier(CheckpointBarrier{clink::CheckpointId{ckpt}});
}

}  // namespace upsert_contract_detail

template <typename Adapter>
class UpsertContractSuite : public ::testing::Test {
protected:
    using T = typename Adapter::Value;

    void SetUp() override {
        if (!Adapter::available()) {
            GTEST_SKIP() << "external system for '" << Adapter::kCapabilityName
                         << "' is not reachable (live instantiations self-skip)";
        }
        dir_ = std::filesystem::temp_directory_path() /
               ("clink_upsert_contract_" + std::to_string(::getpid()) + "_" +
                ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
    }
    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    static const connectors::ConnectorCapabilities* record() {
        return connectors::CapabilityRegistry::instance().find(
            std::string{Adapter::kCapabilityName});
    }

    std::filesystem::path dir_;
};

TYPED_TEST_SUITE_P(UpsertContractSuite);

// The gate: effectively-once-by-key is only an honest claim when the record
// names the key option and this suite holds the collapse behaviour.
TYPED_TEST_P(UpsertContractSuite, TheCapabilityRecordClaimsIdempotentUpsert) {
    const auto* rec = this->record();
    ASSERT_NE(rec, nullptr) << "no capability record named '" << TypeParam::kCapabilityName << "'";
    EXPECT_TRUE(rec->is_sink);
    EXPECT_EQ(rec->delivery, connectors::DeliveryGuarantee::EffectivelyOnceIdempotent)
        << "record claims " << to_string(rec->delivery)
        << " - this suite holds the idempotent-upsert contract";
    EXPECT_FALSE(rec->idempotency_key_option.empty())
        << "an idempotent claim must name the option carrying the key";
}

TYPED_TEST_P(UpsertContractSuite, AWriteLandsOneRowPerKey) {
    const auto fx = TypeParam::make(this->dir_);
    auto sink = fx.fresh();
    sink->open();
    upsert_contract_detail::deliver(*sink, fx.records, 1);
    EXPECT_EQ(upsert_contract_detail::sorted(fx.state()),
              upsert_contract_detail::sorted(fx.expected));
    sink->close();
}

// The redelivery every restart produces: the same batch again, same
// instance. Convergence, not accumulation.
TYPED_TEST_P(UpsertContractSuite, AReplayedBatchConverges) {
    const auto fx = TypeParam::make(this->dir_);
    auto sink = fx.fresh();
    sink->open();
    upsert_contract_detail::deliver(*sink, fx.records, 1);
    upsert_contract_detail::deliver(*sink, fx.records, 2);
    EXPECT_EQ(upsert_contract_detail::sorted(fx.state()),
              upsert_contract_detail::sorted(fx.expected))
        << "replaying the identical batch changed the external state - the "
           "idempotency key is not collapsing duplicates";
    sink->close();
}

// The redelivery a CRASH produces: a fresh instance that never saw the
// first write replays the batch. This is the exact shape behind the
// analyser's effectively-once level, so it is the case that must hold.
TYPED_TEST_P(UpsertContractSuite, AFreshInstanceReplayConverges) {
    const auto fx = TypeParam::make(this->dir_);
    {
        auto sink = fx.fresh();
        sink->open();
        upsert_contract_detail::deliver(*sink, fx.records, 1);
        // Destroyed without close: the crash. The barrier already flushed.
    }
    auto replay = fx.fresh();
    replay->open();
    upsert_contract_detail::deliver(*replay, fx.records, 1);
    replay->close();
    EXPECT_EQ(upsert_contract_detail::sorted(fx.state()),
              upsert_contract_detail::sorted(fx.expected))
        << "a restarted writer replaying its batch duplicated or diverged the "
           "external state";
}

TYPED_TEST_P(UpsertContractSuite, TheLatestWritePerKeyWins) {
    const auto fx = TypeParam::make(this->dir_);
    auto sink = fx.fresh();
    sink->open();
    upsert_contract_detail::deliver(*sink, fx.records, 1);
    upsert_contract_detail::deliver(*sink, fx.updated_records, 2);
    EXPECT_EQ(upsert_contract_detail::sorted(fx.state()),
              upsert_contract_detail::sorted(fx.expected_updated))
        << "an update for an existing key must replace its row, not sit beside it";
    sink->close();
}

REGISTER_TYPED_TEST_SUITE_P(UpsertContractSuite,
                            TheCapabilityRecordClaimsIdempotentUpsert,
                            AWriteLandsOneRowPerKey,
                            AReplayedBatchConverges,
                            AFreshInstanceReplayConverges,
                            TheLatestWritePerKeyWins);

}  // namespace clink::test
