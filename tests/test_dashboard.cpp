// The shipped Grafana dashboard references metrics that exist.
//
// deploy/grafana/clink-dashboard.json closes the dashboard half of
// production-hardening followups item 23 (the runbook half shipped first).
// It inherits the alert-rules problem exactly: a panel whose metric has
// been renamed does not error - the query matches no series, the panel
// draws an empty graph, and the operator staring at it mid-incident
// concludes the thing it measures is not happening. So the same gate
// applies: every `clink_*` token in the file is checked against the metric
// constants the code exports, by CONSTANT rather than by string copy, so a
// rename fails the compile or the assertion instead of blanking a panel.

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

std::filesystem::path dashboard_path() {
#ifdef CLINK_DASHBOARD_PATH
    return std::filesystem::path{CLINK_DASHBOARD_PATH};
#else
    return {};
#endif
}

// Names a clink binary can export, built from the constants for the same
// reason as the alert-rules test: several register lazily, so a live
// registry snapshot in a test process would be missing exactly the names
// the cluster panels depend on.
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
        kCoordinatorWorkersRegistered,
        kCoordinatorWorkersLostTotal,
        kCoordinatorJobsRunning,
        kCoordinatorSlotsCapacity,
        kCoordinatorSlotsInUse,
        kWorkerSubtasksFailedTotal,
        kWorkerFencedFramesTotal,
        kDiskTotalBytes,
        kDiskFreeBytes,
        // Built per backend by state_metric_name(); the exported series is
        // clink_state_restore_keys_dropped_total{backend="..."}.
        "clink_state_restore_keys_dropped_total",
    };
}

// Histogram-derived suffixes resolve to their base metric, and only for
// bases that really are histograms - see the alert-rules test for why
// plain counters must not be mangled into something that then "passes".
std::string strip_histogram_suffix(const std::string& name) {
    for (const std::string suffix : {"_bucket", "_sum", "_count"}) {
        if (name.size() > suffix.size() && name.ends_with(suffix)) {
            const auto base = name.substr(0, name.size() - suffix.size());
            if (base == clink::metrics::kCheckpointDurationMs ||
                base == clink::metrics::kRestoreFromSavepointNs) {
                return base;
            }
        }
    }
    return name;
}

std::string read_dashboard() {
    std::ifstream in(dashboard_path());
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

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

class DashboardTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::filesystem::exists(dashboard_path())) {
            GTEST_SKIP() << "dashboard file not found at " << dashboard_path();
        }
    }
};

}  // namespace

TEST_F(DashboardTest, EveryMetricThePanelsReferenceIsOneClinkExports) {
    const auto text = read_dashboard();
    ASSERT_FALSE(text.empty()) << "dashboard file is empty";

    const auto known = exportable_metric_names();
    auto referenced = referenced_metrics(text);
    // The dashboard's uid and comment mention "clink" bare; the extractor
    // only collects clink_-prefixed identifiers, but "clink_" followed by
    // nothing alphanumeric would arrive as the bare prefix - drop it.
    std::erase(referenced, "clink_");
    ASSERT_FALSE(referenced.empty())
        << "no clink_ metrics referenced; the extractor is broken, so this test would pass "
           "against any file at all";

    for (const auto& raw : referenced) {
        const auto name = strip_histogram_suffix(raw);
        EXPECT_TRUE(known.count(name) == 1)
            << "the dashboard references '" << raw
            << "', which no clink metric exports. Grafana does not error on this - the query "
               "matches no series and the panel draws empty, which reads as 'not happening' "
               "to the operator relying on it.";
    }
}

TEST_F(DashboardTest, TheStalenessPanelUsesTheTimestampGaugeNotTheCompletionCounter) {
    // The same mistake the alert rules test pins, because the dashboard is
    // where an operator would otherwise re-derive it: a completion counter
    // that stops moving cannot distinguish a stalled job from an idle one.
    const auto text = read_dashboard();
    EXPECT_NE(
        text.find(std::string{"time() - "} + clink::metrics::kCheckpointLastCompletedUnixSeconds),
        std::string::npos)
        << "the staleness panel does not chart time() minus the completion timestamp gauge";
    EXPECT_EQ(text.find("rate(clink_ckpt_completed_total[5m]) == 0"), std::string::npos)
        << "a zero-completion-rate expression cannot distinguish stalled from idle";
}

TEST_F(DashboardTest, TheDashboardIsStructurallyAGrafanaDashboard) {
    // Not a JSON parse (the repo deliberately has no general JSON parser in
    // tests); structural greps that catch the truncated-file and
    // wrong-file-committed failure modes.
    const auto text = read_dashboard();
    EXPECT_NE(text.find("\"panels\""), std::string::npos);
    EXPECT_NE(text.find("\"targets\""), std::string::npos);
    EXPECT_NE(text.find("\"uid\": \"clink-ops\""), std::string::npos);
    // Balanced braces is the cheapest truncation tripwire available
    // without a parser.
    const auto opens = std::count(text.begin(), text.end(), '{');
    const auto closes = std::count(text.begin(), text.end(), '}');
    EXPECT_EQ(opens, closes) << "unbalanced braces - the file is truncated or hand-edited broken";
}
