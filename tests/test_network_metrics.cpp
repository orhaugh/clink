// Network bridge metric helpers.

#include <string>

#include <gtest/gtest.h>

#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/network_metrics.hpp"

using namespace clink;

namespace {

std::uint64_t counter_value(const std::string& name) {
    auto snap = MetricsRegistry::global().snapshot();
    for (const auto& [n, v] : snap.counters) {
        if (n == name) {
            return v;
        }
    }
    return 0;
}

}  // namespace

TEST(NetworkMetrics, RecordsAndBytesByDirection) {
    using namespace clink::metrics;
    const auto snk_records = net_metric_name("records_total", "sink");
    const auto src_records = net_metric_name("records_total", "source");
    const auto snk_bytes = net_metric_name("bytes_total", "sink");
    const auto src_bytes = net_metric_name("bytes_total", "source");

    const auto r0 = counter_value(snk_records);
    const auto r1 = counter_value(src_records);
    const auto b0 = counter_value(snk_bytes);
    const auto b1 = counter_value(src_bytes);

    net::records_sent_inc(4);
    net::records_received_inc(5);
    net::bytes_sent_inc(256);
    net::bytes_received_inc(512);

    EXPECT_EQ(counter_value(snk_records) - r0, 4u);
    EXPECT_EQ(counter_value(src_records) - r1, 5u);
    EXPECT_EQ(counter_value(snk_bytes) - b0, 256u);
    EXPECT_EQ(counter_value(src_bytes) - b1, 512u);
}

TEST(NetworkMetrics, ConnectReconnectAndErrors) {
    using namespace clink::metrics;
    const auto conn = net_metric_name("connect_attempts_total", "sink");
    const auto re = net_metric_name("reconnects_total", "sink");
    const auto se = net_metric_name("send_errors_total", "sink");
    const auto re2 = net_metric_name("recv_errors_total", "source");
    const auto cls = net_metric_name("close_send_total", "sink");
    const auto cred = net_metric_name("credit_exhaustion_total", "sink");

    const auto c0 = counter_value(conn);
    const auto r0 = counter_value(re);
    const auto se0 = counter_value(se);
    const auto re20 = counter_value(re2);
    const auto cls0 = counter_value(cls);
    const auto cred0 = counter_value(cred);

    net::connect_attempt("sink");
    net::reconnect("sink");
    net::send_error();
    net::recv_error();
    net::close_send();
    net::credit_exhaustion();

    EXPECT_EQ(counter_value(conn) - c0, 1u);
    EXPECT_EQ(counter_value(re) - r0, 1u);
    EXPECT_EQ(counter_value(se) - se0, 1u);
    EXPECT_EQ(counter_value(re2) - re20, 1u);
    EXPECT_EQ(counter_value(cls) - cls0, 1u);
    EXPECT_EQ(counter_value(cred) - cred0, 1u);
}
