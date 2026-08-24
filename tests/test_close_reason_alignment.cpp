// The channel close-reason contract (followups item 79).
//
// Closed inputs contribute end-of-time to the watermark min so a FINISHED
// input never holds survivors back - and before this contract existed, an
// all-closed input set ALWAYS read as end-of-time, whatever closed it.
// Cancel teardown closes channels too, so a window subtask whose upstreams
// happened to close first advanced event time to end-of-time and fired
// every open window into a still-live sink: QUAL-07 measured a cancelled
// job appending 11,532 correct-valued, premature cumulate panes, a
// nondeterministic subset of the open ones. The contract: only a set of
// closes that all FINISHED is end-of-input; any cancelled close in an
// all-closed set holds the watermark where it was.

#include <gtest/gtest.h>

#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/multi_input_alignment.hpp"

using namespace clink;

TEST(CloseReasonAlignment, AllInputsFinishedIsEndOfInput) {
    MultiInputAlignment align(2);
    (void)align.on_watermark(0, Watermark{EventTime{100}});
    (void)align.on_watermark(1, Watermark{EventTime{200}});
    (void)align.on_input_closed(0, /*cancelled=*/false);
    (void)align.refresh_watermark();
    (void)align.on_input_closed(1, /*cancelled=*/false);
    const auto adv = align.refresh_watermark();
    ASSERT_TRUE(adv.forward) << "a fully-FINISHED input set is genuine end-of-input";
    EXPECT_EQ(adv.watermark.timestamp(), Watermark::max().timestamp())
        << "end-of-input advances event time to end-of-time so open windows fire";
}

TEST(CloseReasonAlignment, ACancelledCloseInAnAllClosedSetIsNotEndOfInput) {
    MultiInputAlignment align(2);
    (void)align.on_watermark(0, Watermark{EventTime{100}});
    (void)align.on_watermark(1, Watermark{EventTime{200}});
    (void)align.on_input_closed(0, /*cancelled=*/false);
    (void)align.refresh_watermark();
    (void)align.on_input_closed(1, /*cancelled=*/true);
    const auto adv = align.refresh_watermark();
    EXPECT_FALSE(adv.forward)
        << "teardown wreckage read as end-of-time: this is what fired every open "
           "window into a still-live sink during cancel (item 79)";
    // Input 0 finishing legitimately advanced the min to input 1's
    // watermark (200); the cancelled close must hold it THERE, not lift
    // it to end-of-time.
    EXPECT_EQ(align.current_watermark().timestamp(), EventTime{200})
        << "the emitted watermark must hold where it was";
}

TEST(CloseReasonAlignment, ACancelledCloseAmongSurvivorsDoesNotHoldTimeBack) {
    // The reason gates only the all-closed terminal emission. A cancelled
    // input among ALIVE ones stops constraining the min exactly like a
    // finished one: its job fragment is gone either way, and the survivors'
    // clock must not be pinned to a corpse.
    MultiInputAlignment align(2);
    (void)align.on_watermark(0, Watermark{EventTime{100}});
    (void)align.on_watermark(1, Watermark{EventTime{50}});
    (void)align.on_input_closed(1, /*cancelled=*/true);
    (void)align.refresh_watermark();
    const auto adv = align.on_watermark(0, Watermark{EventTime{300}});
    ASSERT_TRUE(adv.forward);
    EXPECT_EQ(adv.watermark.timestamp(), EventTime{300})
        << "the surviving input alone should drive the min";
}

TEST(CloseReasonAlignment, BoundedChannelFirstCloseReasonWins) {
    // The cancel lambda and the runner's exit tail can both close the same
    // channel; whichever lands first decides, so a racing Finished close
    // cannot launder a teardown into end-of-input.
    BoundedChannel<int> ch(4);
    ch.close(ChannelCloseReason::Cancelled);
    ch.close(ChannelCloseReason::Finished);
    EXPECT_TRUE(ch.close_cancelled());

    BoundedChannel<int> ch2(4);
    ch2.close();  // default: Finished
    ch2.close(ChannelCloseReason::Cancelled);
    EXPECT_FALSE(ch2.close_cancelled());
}
