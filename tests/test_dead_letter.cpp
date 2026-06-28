// Engine-level dead-letter queue: the DeadLetterQueue implementations, the
// RuntimeContext::report_bad_record seam (delegate / null-safe), and the
// LoggingDeadLetterQueue rate limit + window roll.

#include <chrono>
#include <cstddef>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/types.hpp"
#include "clink/runtime/dead_letter.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace {

using clink::BadRecord;
using clink::LoggingDeadLetterQueue;
using clink::NullDeadLetterQueue;
using clink::OperatorId;
using clink::RuntimeContext;

// Test double: records every reported BadRecord.
class CapturingDeadLetterQueue final : public clink::DeadLetterQueue {
public:
    void report(const BadRecord& rec) override {
        std::lock_guard<std::mutex> lk(m_);
        records.push_back(rec);
    }
    std::vector<BadRecord> records;

private:
    std::mutex m_;
};

BadRecord make_rec(std::string payload) {
    return BadRecord{.payload = std::move(payload),
                     .error = "boom",
                     .connector = "test",
                     .direction = "sink",
                     .location = "loc"};
}

RuntimeContext make_ctx() {
    return RuntimeContext{OperatorId{1}, "test", nullptr, nullptr};
}

}  // namespace

TEST(DeadLetter, NullQueueDropsSilently) {
    NullDeadLetterQueue dlq;
    EXPECT_NO_THROW(dlq.report(make_rec("x")));
}

TEST(DeadLetter, ReportBadRecordDelegatesToConfiguredQueue) {
    CapturingDeadLetterQueue cap;
    RuntimeContext ctx = make_ctx();
    ctx.set_dead_letter_queue(&cap);
    ctx.report_bad_record(make_rec("payload-1"));
    ASSERT_EQ(cap.records.size(), 1u);
    EXPECT_EQ(cap.records[0].payload, "payload-1");
    EXPECT_EQ(cap.records[0].connector, "test");
    EXPECT_EQ(cap.records[0].direction, "sink");
}

TEST(DeadLetter, ReportBadRecordIsNoOpWhenNoQueueWired) {
    RuntimeContext ctx = make_ctx();  // no DLQ set
    EXPECT_EQ(ctx.dead_letter_queue(), nullptr);
    EXPECT_NO_THROW(ctx.report_bad_record(make_rec("x")));  // best-effort: silent no-op
}

TEST(DeadLetter, LoggingQueueRateLimitsWithinAWindow) {
    // A long window so all reports land in one window; cap at 3.
    LoggingDeadLetterQueue dlq(/*logger=*/nullptr,
                               /*max_per_window=*/3,
                               /*window=*/std::chrono::seconds{30});
    for (int i = 0; i < 10; ++i) {
        dlq.report(make_rec("r" + std::to_string(i)));
    }
    EXPECT_EQ(dlq.logged_total(), 3u) << "only max_per_window records are logged";
    EXPECT_EQ(dlq.suppressed_total(), 7u) << "the rest are suppressed (and counted)";
}

TEST(DeadLetter, LoggingQueueAllowsMoreAfterWindowRolls) {
    LoggingDeadLetterQueue dlq(nullptr,
                               /*max_per_window=*/2,
                               /*window=*/std::chrono::milliseconds{5});
    dlq.report(make_rec("a"));
    dlq.report(make_rec("b"));
    dlq.report(make_rec("c"));  // suppressed (over the cap this window)
    EXPECT_EQ(dlq.logged_total(), 2u);
    std::this_thread::sleep_for(std::chrono::milliseconds{20});  // let the window roll
    dlq.report(make_rec("d"));                                   // new window: logged again
    EXPECT_EQ(dlq.logged_total(), 3u);
}

TEST(DeadLetter, LoggingQueueToleratesLargeBinaryPayload) {
    LoggingDeadLetterQueue dlq(nullptr, 100, std::chrono::seconds{1}, /*max_payload_preview=*/16);
    std::string big(10000, '\xAB');              // larger than the preview cap
    EXPECT_NO_THROW(dlq.report(make_rec(big)));  // truncation path must not over-read
    EXPECT_EQ(dlq.logged_total(), 1u);
}
