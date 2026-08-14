#pragma once

// clink::test source contract suite - a capability claim is a test
// obligation.
//
// Every source connector answers the same questions: does a restore from a
// snapshotted offset replay exactly once, does cancellation actually stop
// it, what happens to a record it cannot parse, and what happens to one far
// larger than anything it was tuned for. Before this suite each connector
// answered them (or did not) in its own hand-written tests, and nothing
// tied the answers to what the connector's ConnectorCapabilities record
// CLAIMS. Here the record drives the obligation: a source whose record says
// `replayable` + `checkpoint_integrated` has the replay cases RUN against
// it and must pass; one that honestly claims neither has them skipped with
// the record's own words as the reason. A claim without a passing test and
// a test without a claim both fail.
//
// Cut semantics: snapshots are taken BETWEEN produce() calls, because that
// is where the runtime takes them - snapshot_offset and restore_offset run
// on the source runner thread, never concurrently with produce (see
// add_source in dag.hpp). The exactly-once case therefore cuts at every
// produce-call boundary the input offers, not at record granularity.
//
// Part of the public clink testing API
// (docs/internals/testing-framework.md): consumers instantiate it against
// their own Source<T> implementations exactly as the in-tree connectors do
// in tests/test_source_contract.cpp.
//
// Adapter shape (duck-typed; every member below is required):
//
//   struct MySourceContract {
//       using Value = <element type T>;
//       // CapabilityRegistry name, or "" for a source with no record
//       // (the manifest gate decides which impls may say "").
//       static constexpr std::string_view kCapabilityName = "my_conn";
//       // Declared policy for an unparseable input record.
//       static constexpr MalformedInputPolicy kMalformedPolicy = ...;
//       // A fixture whose input carries `count` distinct records, plus a
//       // factory for fresh, identically-configured instances over it.
//       static SourceContractFixture<Value> make(const std::filesystem::path& dir,
//                                                std::size_t count);
//       // Same, with exactly one malformed record among well-formed ones.
//       // nullopt = malformed input cannot exist for this connector.
//       static std::optional<SourceContractFixture<Value>> make_with_malformed(
//           const std::filesystem::path& dir);
//       // Same, with one record of at least a few megabytes. nullopt =
//       // not applicable.
//       static std::optional<SourceContractFixture<Value>> make_oversized(
//           const std::filesystem::path& dir);
//   };
//
//   using MyContract = ::clink::test::SourceContract<MySourceContract>;
//   INSTANTIATE_TYPED_TEST_SUITE_P(MyConn, SourceContractSuite, MySourceContract);

#include <cstddef>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/capability.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/test/output_capture.hpp"

namespace clink::test {

enum class MalformedInputPolicy {
    // The decoder treats an unparseable record as a filter miss and drops
    // it; every well-formed record still arrives. The file family's
    // documented TextFormat::decode semantics.
    Skip,
    // The source refuses the input loudly (open() or produce() throws).
    // A structured format whose framing is broken has no safe "rest of
    // the file" to continue with; Parquet behaves this way.
    Refuse,
};

template <typename T>
struct SourceContractFixture {
    // Fresh, identically-configured instance reading the same input.
    std::function<std::unique_ptr<Source<T>>()> fresh;
    // Exactly the well-formed records the input carries, in emission order.
    std::vector<T> expected;
};

namespace detail {

// Drive produce() to exhaustion, appending data records to `out`. Bounded:
// a source that never finishes is a contract violation, not a hang.
template <typename T>
bool drain_source(Source<T>& src, std::vector<T>& out, std::size_t max_calls = 10000) {
    OutputCapture<T> cap;
    std::size_t calls = 0;
    while (src.produce(cap.emitter())) {
        if (++calls > max_calls) {
            return false;
        }
    }
    for (auto& v : cap.values()) {
        out.push_back(std::move(v));
    }
    return true;
}

}  // namespace detail

template <typename Adapter>
class SourceContractSuite : public ::testing::Test {
protected:
    using T = typename Adapter::Value;

    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() /
               ("clink_src_contract_" + std::to_string(::getpid()) + "_" +
                ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
    }
    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    // The connector's capability record, or nullptr for the (allowed,
    // named) recordless case.
    static const connectors::ConnectorCapabilities* record() {
        if (Adapter::kCapabilityName.empty()) {
            return nullptr;
        }
        return connectors::CapabilityRegistry::instance().find(
            std::string{Adapter::kCapabilityName});
    }

    static bool claims_replay() {
        const auto* rec = record();
        return rec != nullptr && rec->replayable && rec->checkpoint_integrated;
    }

    std::filesystem::path dir_;
};

TYPED_TEST_SUITE_P(SourceContractSuite);

// The adapter's naming must resolve: an adapter pointing at a name with no
// record would silently skip every capability-gated case below, which is
// the green-over-nothing failure mode.
TYPED_TEST_P(SourceContractSuite, TheCapabilityRecordResolves) {
    if (TypeParam::kCapabilityName.empty()) {
        GTEST_SKIP() << "adapter declares no capability record";
    }
    ASSERT_NE(this->record(), nullptr)
        << "adapter names capability '" << TypeParam::kCapabilityName
        << "' but the registry has no such record - the replay obligations "
           "below would all silently skip";
}

// The replay obligation. Cut between every pair of produce() calls: the
// prefix collected before the cut plus a fresh instance's post-restore
// remainder must equal the whole input, in order, exactly once. This is
// the executable form of `replayable` + `checkpoint_integrated`.
TYPED_TEST_P(SourceContractSuite, EveryProduceBoundaryReplaysExactlyOnce) {
    if (!this->claims_replay()) {
        GTEST_SKIP() << "capability record does not claim replayable + "
                        "checkpoint-integrated, so there is no offset contract to hold";
    }
    using T = typename TypeParam::Value;
    const auto fx = TypeParam::make(this->dir_, 8);
    const clink::OperatorId op{7};

    // First learn how many produce() calls exhaust this input, so every
    // boundary gets a cut - including 0 (restore before anything was read)
    // and the final one (restore at end-of-input).
    std::size_t total_calls = 0;
    {
        auto probe = fx.fresh();
        probe->open();
        OutputCapture<T> cap;
        while (probe->produce(cap.emitter())) {
            ASSERT_LT(++total_calls, 10000u) << "source never exhausted";
        }
        probe->close();
    }

    for (std::size_t cut = 0; cut <= total_calls; ++cut) {
        clink::InMemoryStateBackend backend;
        std::vector<T> got;

        auto first = fx.fresh();
        first->open();
        OutputCapture<T> cap;
        for (std::size_t i = 0; i < cut; ++i) {
            (void)first->produce(cap.emitter());
        }
        for (auto& v : cap.values()) {
            got.push_back(std::move(v));
        }
        first->snapshot_offset(backend, op, clink::CheckpointId{1});
        first->close();

        auto second = fx.fresh();
        const bool restored = second->restore_offset(backend, op);
        EXPECT_TRUE(restored) << "cut " << cut
                              << ": a snapshotted offset was not restored - the fresh "
                                 "instance will replay from the top";
        second->open();
        ASSERT_TRUE(detail::drain_source(*second, got)) << "cut " << cut;
        second->close();

        EXPECT_EQ(got, fx.expected)
            << "cut after produce() call " << cut << " of " << total_calls
            << ": prefix + post-restore remainder must be the whole input exactly once "
               "(shorter = loss, longer = replayed duplicates)";
    }
}

// Restoring at end-of-input must produce nothing: "I had finished" is an
// offset like any other, and forgetting it turns every post-completion
// restart into a full duplicate of the stream.
TYPED_TEST_P(SourceContractSuite, RestoreAfterExhaustionProducesNothing) {
    if (!this->claims_replay()) {
        GTEST_SKIP() << "no replay claim in the capability record";
    }
    using T = typename TypeParam::Value;
    const auto fx = TypeParam::make(this->dir_, 5);
    const clink::OperatorId op{7};
    clink::InMemoryStateBackend backend;

    auto first = fx.fresh();
    first->open();
    std::vector<T> all;
    ASSERT_TRUE(detail::drain_source(*first, all));
    ASSERT_EQ(all, fx.expected);
    first->snapshot_offset(backend, op, clink::CheckpointId{1});
    first->close();

    auto second = fx.fresh();
    ASSERT_TRUE(second->restore_offset(backend, op));
    second->open();
    std::vector<T> replayed;
    ASSERT_TRUE(detail::drain_source(*second, replayed));
    EXPECT_TRUE(replayed.empty()) << replayed.size()
                                  << " record(s) replayed after a snapshot taken at "
                                     "end-of-input - every completed run would duplicate "
                                     "on restart";
}

// Cancellation is a stop, not a suggestion: after cancel(), produce() must
// decline promptly and emit no further data.
TYPED_TEST_P(SourceContractSuite, CancelStopsProduceWithoutFurtherData) {
    using T = typename TypeParam::Value;
    const auto fx = TypeParam::make(this->dir_, 8);
    auto src = fx.fresh();
    src->open();
    src->cancel();
    OutputCapture<T> cap;
    EXPECT_FALSE(src->produce(cap.emitter()))
        << "produce() after cancel() claimed there is more to read";
    EXPECT_TRUE(cap.values().empty()) << "a cancelled source emitted data";
    src->close();
}

// Malformed input does whatever the adapter DECLARED - and only that.
// Either policy is legitimate; drifting between them silently is not.
TYPED_TEST_P(SourceContractSuite, MalformedInputFollowsTheDeclaredPolicy) {
    using T = typename TypeParam::Value;
    const auto fx = TypeParam::make_with_malformed(this->dir_);
    if (!fx.has_value()) {
        GTEST_SKIP() << "malformed input cannot exist for this connector";
    }
    auto src = fx->fresh();
    if (TypeParam::kMalformedPolicy == MalformedInputPolicy::Skip) {
        src->open();
        std::vector<T> got;
        ASSERT_TRUE(detail::drain_source(*src, got));
        EXPECT_EQ(got, fx->expected)
            << "declared policy is Skip: every well-formed record arrives and "
               "the malformed one is absent";
        src->close();
    } else {
        EXPECT_THROW(
            {
                src->open();
                std::vector<T> got;
                (void)detail::drain_source(*src, got);
            },
            std::exception)
            << "declared policy is Refuse: the malformed input must fail loudly, "
               "not decode into something";
    }
}

// A record far past the tuning point either arrives intact or is refused
// loudly. Truncation - a partial record delivered as if whole - is the one
// outcome that is never acceptable, and the equality check catches it.
TYPED_TEST_P(SourceContractSuite, OversizedRecordArrivesIntactOrRefusesLoudly) {
    using T = typename TypeParam::Value;
    const auto fx = TypeParam::make_oversized(this->dir_);
    if (!fx.has_value()) {
        GTEST_SKIP() << "an oversized record cannot exist for this connector";
    }
    auto src = fx->fresh();
    std::vector<T> got;
    try {
        src->open();
        ASSERT_TRUE(detail::drain_source(*src, got));
    } catch (const std::exception&) {
        return;  // loud refusal is a valid answer
    }
    EXPECT_EQ(got, fx->expected)
        << "the oversized record was neither delivered intact nor refused - "
           "silent truncation or loss";
    src->close();
}

REGISTER_TYPED_TEST_SUITE_P(SourceContractSuite,
                            TheCapabilityRecordResolves,
                            EveryProduceBoundaryReplaysExactlyOnce,
                            RestoreAfterExhaustionProducesNothing,
                            CancelStopsProduceWithoutFurtherData,
                            MalformedInputFollowsTheDeclaredPolicy,
                            OversizedRecordArrivesIntactOrRefusesLoudly);

}  // namespace clink::test
