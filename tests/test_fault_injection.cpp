// Fault-injection framework contract.
//
// These tests pin the properties every fault-driven test downstream of
// here relies on: determinism keyed on (name, ordinal), a fast path that
// does nothing when disarmed, loud rejection of a malformed schedule, and
// a reset that cannot leave a rule or a parked thread behind.

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "clink/fault/fault_injection.hpp"

#ifdef CLINK_FAULT_INJECTION

namespace {

using clink::fault::Action;
using clink::fault::InjectedFault;
using clink::fault::Registry;
using clink::fault::Rule;

class FaultInjectionTest : public ::testing::Test {
protected:
    void SetUp() override { Registry::instance().reset(); }
    void TearDown() override { Registry::instance().reset(); }
};

TEST_F(FaultInjectionTest, DisarmedPointIsInert) {
    for (int i = 0; i < 100; ++i) {
        const auto outcome = CLINK_FAULT_POINT("test.inert");
        EXPECT_FALSE(outcome.fired);
    }
    // Nothing armed means nothing counted - the fast path never enters the
    // registry at all. This is the property that keeps an unarmed fault
    // point free on the hot path.
    EXPECT_EQ(Registry::instance().hits("test.inert"), 0u);
}

TEST_F(FaultInjectionTest, ObserveCountsWithoutPerturbing) {
    Registry::instance().arm(Rule{.point = "test.observe", .action = Action::Observe});
    for (int i = 0; i < 5; ++i) {
        const auto outcome = CLINK_FAULT_POINT("test.observe");
        EXPECT_FALSE(outcome.fired);
    }
    EXPECT_EQ(Registry::instance().hits("test.observe"), 5u);
}

TEST_F(FaultInjectionTest, ThrowFiresOnEveryOccurrenceWhenOrdinalIsZero) {
    Registry::instance().arm(Rule{.point = "test.throw", .action = Action::Throw});
    EXPECT_THROW(CLINK_FAULT_POINT("test.throw"), InjectedFault);
    EXPECT_THROW(CLINK_FAULT_POINT("test.throw"), InjectedFault);
    EXPECT_EQ(Registry::instance().hits("test.throw"), 2u);
}

TEST_F(FaultInjectionTest, OrdinalSelectsExactlyOneOccurrence) {
    Registry::instance().arm(Rule{.point = "test.nth", .ordinal = 3, .action = Action::Throw});
    EXPECT_NO_THROW(CLINK_FAULT_POINT("test.nth"));
    EXPECT_NO_THROW(CLINK_FAULT_POINT("test.nth"));
    EXPECT_THROW(CLINK_FAULT_POINT("test.nth"), InjectedFault);
    EXPECT_NO_THROW(CLINK_FAULT_POINT("test.nth"));
    EXPECT_EQ(Registry::instance().hits("test.nth"), 4u);
}

TEST_F(FaultInjectionTest, RuleIsScopedToItsOwnPointName) {
    Registry::instance().arm(Rule{.point = "test.a", .action = Action::Throw});
    EXPECT_NO_THROW(CLINK_FAULT_POINT("test.b"));
    EXPECT_THROW(CLINK_FAULT_POINT("test.a"), InjectedFault);
}

TEST_F(FaultInjectionTest, ErrorAndTruncateAreObservedNotThrown) {
    Registry::instance().arm(Rule{.point = "test.err", .action = Action::Error});
    Registry::instance().arm(Rule{.point = "test.trunc", .action = Action::Truncate, .arg = 4});

    const auto err = CLINK_FAULT_POINT("test.err");
    EXPECT_TRUE(err.is_error());
    EXPECT_FALSE(err.is_truncate());

    const auto trunc = CLINK_FAULT_POINT("test.trunc");
    EXPECT_TRUE(trunc.is_truncate());
    EXPECT_EQ(trunc.truncate_to(10), 4u);
    // A truncate longer than the payload cannot lengthen it.
    EXPECT_EQ(trunc.truncate_to(2), 2u);
    // An unfired outcome leaves the size alone.
    EXPECT_EQ(clink::fault::Outcome{}.truncate_to(10), 10u);
}

TEST_F(FaultInjectionTest, FirstMatchingRuleWins) {
    Registry::instance().arm(Rule{.point = "test.order", .ordinal = 1, .action = Action::Error});
    Registry::instance().arm(Rule{.point = "test.order", .ordinal = 1, .action = Action::Throw});
    // Arm order decides, so the Error rule shadows the Throw on hit 1.
    const auto outcome = CLINK_FAULT_POINT("test.order");
    EXPECT_TRUE(outcome.is_error());
}

TEST_F(FaultInjectionTest, ScheduleOfSeveralOrdinalsIsDeterministic) {
    // The "explicit fault schedule" the harness contract asks for: rules
    // pinned to occurrences 2 and 5, replayed twice with identical results.
    for (int run = 0; run < 2; ++run) {
        Registry::instance().reset();
        Registry::instance().arm_from_spec("test.sched=error@2, test.sched=truncate:7@5");
        std::string fired;
        for (int i = 1; i <= 6; ++i) {
            const auto o = CLINK_FAULT_POINT("test.sched");
            fired += o.fired ? std::string(clink::fault::to_string(o.action))[0] : '.';
        }
        EXPECT_EQ(fired, ".e..t.") << "run " << run;
    }
}

TEST_F(FaultInjectionTest, SpecParsesActionArgumentAndOrdinal) {
    const auto n =
        Registry::instance().arm_from_spec("  a.b = exit:3 @ 2 , c.d=block:50, e.f=throw ");
    EXPECT_EQ(n, 3u);
    EXPECT_TRUE(Registry::instance().any_armed());
}

TEST_F(FaultInjectionTest, MalformedSpecIsRejectedLoudly) {
    // A typo in a fault schedule must fail the test rather than silently
    // disarm it and let an empty exercise pass.
    EXPECT_THROW(Registry::instance().arm_from_spec("no-equals-sign"), std::invalid_argument);
    EXPECT_THROW(Registry::instance().arm_from_spec("a.b=nosuchaction"), std::invalid_argument);
    EXPECT_THROW(Registry::instance().arm_from_spec("a.b=throw@notanumber"), std::invalid_argument);
    EXPECT_THROW(Registry::instance().arm_from_spec("a.b=delay:"), std::invalid_argument);
    EXPECT_THROW(Registry::instance().arm_from_spec("=throw"), std::invalid_argument);
}

TEST_F(FaultInjectionTest, ResetDisarmsAndClearsCounters) {
    Registry::instance().arm(Rule{.point = "test.reset", .action = Action::Observe});
    CLINK_FAULT_POINT("test.reset");
    EXPECT_EQ(Registry::instance().hits("test.reset"), 1u);
    Registry::instance().reset();
    EXPECT_FALSE(Registry::instance().any_armed());
    EXPECT_EQ(Registry::instance().hits("test.reset"), 0u);
    EXPECT_NO_THROW(CLINK_FAULT_POINT("test.reset"));
}

TEST_F(FaultInjectionTest, BlockParksUntilReleased) {
    Registry::instance().arm(Rule{.point = "test.block", .action = Action::Block});
    std::atomic<bool> past{false};
    std::thread worker([&] {
        CLINK_FAULT_POINT("test.block");
        past.store(true, std::memory_order_release);
    });

    // State-driven, not timing-driven: spin until the registry reports the
    // thread parked, then assert it has NOT passed the point.
    while (Registry::instance().hits("test.block") == 0) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(past.load(std::memory_order_acquire));

    Registry::instance().release("test.block");
    worker.join();
    EXPECT_TRUE(past.load(std::memory_order_acquire));
}

TEST_F(FaultInjectionTest, ResetReleasesAParkedThread) {
    // A test that arms Block and then dies on an assertion must not wedge
    // the whole binary: reset() is the unconditional escape hatch.
    Registry::instance().arm(Rule{.point = "test.block2", .action = Action::Block});
    std::atomic<bool> past{false};
    std::thread worker([&] {
        CLINK_FAULT_POINT("test.block2");
        past.store(true, std::memory_order_release);
    });
    while (Registry::instance().hits("test.block2") == 0) {
        std::this_thread::yield();
    }
    Registry::instance().reset();
    worker.join();
    EXPECT_TRUE(past.load(std::memory_order_acquire));
}

TEST_F(FaultInjectionTest, ScopedFaultResetsOnScopeExit) {
    {
        const clink::fault::ScopedFault guard("test.scoped=throw");
        EXPECT_THROW(CLINK_FAULT_POINT("test.scoped"), InjectedFault);
    }
    EXPECT_FALSE(Registry::instance().any_armed());
    EXPECT_NO_THROW(CLINK_FAULT_POINT("test.scoped"));
}

TEST_F(FaultInjectionTest, ConcurrentReachesShareOneOrdinalSequence) {
    // Ordinals are per-point across the whole process, so N threads
    // reaching the same point produce exactly N hits and exactly one
    // firing for a single-ordinal rule - no lost or double counts.
    Registry::instance().arm(Rule{.point = "test.mt", .ordinal = 7, .action = Action::Error});
    constexpr int kThreads = 8;
    constexpr int kPerThread = 25;
    std::atomic<int> fired{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < kPerThread; ++i) {
                if (CLINK_FAULT_POINT("test.mt").fired) {
                    fired.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(Registry::instance().hits("test.mt"), kThreads * kPerThread);
    EXPECT_EQ(fired.load(), 1);
}

TEST_F(FaultInjectionTest, ActionNamesRoundTrip) {
    for (const auto a : {Action::Throw,
                         Action::Exit,
                         Action::Abort,
                         Action::Block,
                         Action::Delay,
                         Action::Error,
                         Action::Truncate,
                         Action::Observe}) {
        const auto parsed = clink::fault::action_from_string(clink::fault::to_string(a));
        ASSERT_TRUE(parsed.has_value()) << clink::fault::to_string(a);
        EXPECT_EQ(*parsed, a);
    }
    EXPECT_FALSE(clink::fault::action_from_string("explode").has_value());
}

TEST_F(FaultInjectionTest, AvailabilityIsReportedTrueInThisBuild) {
    EXPECT_TRUE(clink::fault::available());
}

TEST_F(FaultInjectionTest, EnvironmentSeedingRunsAtStaticInit) {
    // Regression guard. The inline reach() checks g_any_armed and returns
    // before touching Registry::instance(), so on a process that arms
    // nothing programmatically the constructor - and therefore the
    // CLINK_FAULT_INJECT read - would never run. Every fault point stayed
    // inert while an operator believed a fault was armed, which made the
    // whole cross-process story dead code. A static initialiser forces the
    // construction; this asserts it still happens.
    EXPECT_TRUE(clink::fault::Registry::env_seeding_ran())
        << "CLINK_FAULT_INJECT is not being read: the static initialiser that constructs "
           "the registry has been dropped, and every env-armed fault is silently inert";
}

}  // namespace

#else  // !CLINK_FAULT_INJECTION

TEST(FaultInjectionCompiledOut, PointsAreInertAndAvailabilityIsFalse) {
    EXPECT_FALSE(clink::fault::available());
    EXPECT_FALSE(CLINK_FAULT_POINT("test.compiled.out").fired);
}

#endif  // CLINK_FAULT_INJECTION
