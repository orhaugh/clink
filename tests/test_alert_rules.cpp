// The shipped Prometheus alert rules reference metrics that exist.
//
// deploy/prometheus/clink-alerts.yaml is the answer to a gap recorded in
// docs/production-hardening-plan.md W19: the metrics supported the obvious
// alerts and no rule file shipped, so every operator derived the same
// queries independently and got the subtleties wrong independently too.
//
// Shipping rules creates a worse failure than not shipping them. A rule
// whose metric has been renamed does not error - Prometheus evaluates it
// against no series, the expression is never true, and the alert silently
// never fires. The operator believes they are covered. That is strictly
// worse than having written no rule, and nothing about it is visible
// without a test.
//
// So: every `clink_*` token in the file, including the ones inside
// annotation prose, is checked against the metric names the code actually
// defines. The check is by CONSTANT, not by a copy of the string, so
// renaming a metric fails here in one of two ways - a changed value fails
// the assertion, a changed identifier fails the compile - and both are
// louder than an alert that quietly stops working.

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/metrics/checkpoint_metrics.hpp"
#include "clink/metrics/orchestration_metrics.hpp"
#include "clink/metrics/process_metrics.hpp"
#include "clink/metrics/system_metrics.hpp"

namespace {

std::filesystem::path rules_path() {
#ifdef CLINK_ALERT_RULES_PATH
    return std::filesystem::path{CLINK_ALERT_RULES_PATH};
#else
    return {};
#endif
}

// Names a clink binary can export. Deliberately built from the constants
// rather than from a snapshot of a live registry: several of these are
// registered lazily on first use, so a registry snapshot in a test process
// that never ran a coordinator would be missing exactly the names the
// cluster alerts depend on, and the test would fail for the wrong reason.
std::set<std::string> exportable_metric_names() {
    using namespace clink::metrics;
    return {
        kCheckpointTriggered,
        kCheckpointCompleted,
        kCheckpointFailed,
        kCheckpointLastCompletedUnixSeconds,
        kCheckpointDurationMs,
        kSubtaskSnapshotAck,
        kSubtaskSnapshotFailure,
        kRestoreFromSavepointNs,
        kJobRestarts,
        kProtocolMismatches,
        kMalformedFrames,
        kCoordinatorWorkersLostTotal,
        kCoordinatorSlotsCapacity,
        kCoordinatorSlotsInUse,
        kWorkerSubtasksFailedTotal,
        kWorkerFencedFramesTotal,
        kWorkerSlotsCapacity,
        kWorkerSlotsInUse,
        kDiskTotalBytes,
        kDiskFreeBytes,
        // state_metric_name() builds this one per backend, so the exported
        // series is clink_state_restore_keys_dropped_total{backend="..."}
        // and the bare name is what the rule matches on.
        "clink_state_restore_keys_dropped_total",
    };
}

// Prometheus renders a histogram under three derived names. A rule referring
// to `clink_ckpt_duration_ms_bucket` is referring to the `clink_ckpt_duration_ms`
// histogram, so strip the suffix before looking it up - otherwise the test
// would demand a metric constant that by construction cannot exist.
std::string strip_histogram_suffix(const std::string& name) {
    for (const std::string suffix : {"_bucket", "_sum", "_count"}) {
        if (name.size() > suffix.size() && name.ends_with(suffix)) {
            const auto base = name.substr(0, name.size() - suffix.size());
            // Only strip when the base is itself a known histogram; plain
            // counters legitimately end in _count-like words and must not be
            // mangled into something that then "passes".
            if (base == clink::metrics::kCheckpointDurationMs ||
                base == clink::metrics::kRestoreFromSavepointNs) {
                return base;
            }
        }
    }
    return name;
}

std::string read_rules() {
    std::ifstream in(rules_path());
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

// Every clink_-prefixed identifier in the file, wherever it appears.
std::vector<std::string> referenced_metrics(const std::string& text) {
    std::vector<std::string> out;
    const std::string prefix = "clink_";
    std::size_t pos = 0;
    while ((pos = text.find(prefix, pos)) != std::string::npos) {
        std::size_t end = pos;
        while (end < text.size() &&
               (std::isalnum(static_cast<unsigned char>(text[end])) != 0 || text[end] == '_')) {
            ++end;
        }
        out.push_back(text.substr(pos, end - pos));
        pos = end;
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

class AlertRulesTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::filesystem::exists(rules_path())) {
            GTEST_SKIP() << "alert rules file not found at " << rules_path();
        }
    }
};

}  // namespace

TEST_F(AlertRulesTest, EveryMetricTheRulesReferenceIsOneClinkExports) {
    const auto text = read_rules();
    ASSERT_FALSE(text.empty()) << "rules file is empty";

    const auto known = exportable_metric_names();
    const auto referenced = referenced_metrics(text);
    ASSERT_FALSE(referenced.empty()) << "no clink_ metrics referenced; the extractor is broken, so "
                                        "this test would pass against any file at all";

    for (const auto& raw : referenced) {
        const auto name = strip_histogram_suffix(raw);
        EXPECT_TRUE(known.count(name) == 1)
            << "the alert rules reference '" << raw
            << "', which no clink metric exports. Prometheus does not error on this - the "
               "expression matches no series, never fires, and the operator believes they are "
               "covered.";
    }
}

TEST_F(AlertRulesTest, TheStalenessAlertUsesTheTimestampGaugeNotTheCompletionCounter) {
    // The specific mistake this file exists to stop an operator making. A
    // completion counter that has stopped moving is indistinguishable from an
    // idle job over any short window, so `rate(clink_ckpt_completed_total)`
    // reads as a stall on a job that is simply quiet - and, worse, reads as
    // healthy on a stalled job for as long as the rate window is wide.
    // clink exports the completion TIMESTAMP for exactly this.
    const auto text = read_rules();
    const auto stalled = text.find("ClinkCheckpointsStalled");
    ASSERT_NE(stalled, std::string::npos) << "the checkpoint staleness alert is missing";

    // The expression sits between this alert's name and the next alert.
    const auto next = text.find("- alert:", stalled);
    const auto body = text.substr(stalled, next - stalled);
    EXPECT_NE(body.find(clink::metrics::kCheckpointLastCompletedUnixSeconds), std::string::npos)
        << "the staleness alert does not use the completion timestamp gauge: " << body;
    EXPECT_EQ(body.find("rate(clink_ckpt_completed_total"), std::string::npos)
        << "the staleness alert uses the completion counter, which cannot distinguish a stalled "
           "job from an idle one: "
        << body;
}

TEST_F(AlertRulesTest, TheDurationAlertUsesBucketsBecauseAMeanWouldHideTheTail) {
    // Guards the reason checkpoint duration is a histogram at all. A rule
    // written against _sum/_count computes a mean, and the mean of a
    // checkpoint duration hides the case worth alerting on.
    const auto text = read_rules();
    EXPECT_NE(text.find("histogram_quantile"), std::string::npos)
        << "no quantile is alerted on, so the duration histogram's buckets are unused";
    EXPECT_NE(text.find(std::string{clink::metrics::kCheckpointDurationMs} + "_bucket"),
              std::string::npos);
}
