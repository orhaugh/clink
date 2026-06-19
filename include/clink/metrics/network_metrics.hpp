#pragma once

// Network bridge + socket observability.
//
// Coverage:
//   - clink_net_bridge_records_total{direction}  : records pushed
//                                                   (sink) / popped
//                                                   (source)
//   - clink_net_bridge_bytes_total{direction}    : serialised bytes
//                                                   on the wire
//   - clink_net_bridge_connect_attempts_total
//   - clink_net_bridge_reconnects_total
//   - clink_net_bridge_send_errors_total
//   - clink_net_bridge_recv_errors_total
//   - clink_net_bridge_close_send_total
//   - clink_net_bridge_credit_exhaustion_total   : per-channel
//                                                   credit gate
//                                                   stalled the
//                                                   producer
//
// All metrics live under the `clink_net_` prefix. Bridge counters take
// a direction tag so a process running both sides of a network bridge
// (e.g. a subtask that emits AND receives) shows up as two distinct
// entries without collision.

#include <cstdint>
#include <string>

#include "clink/metrics/metrics_registry.hpp"

namespace clink::metrics {

inline std::string net_metric_name(const char* metric, const char* direction) {
    std::string out = "clink_net_bridge_";
    out += metric;
    out += "{direction=\"";
    out += direction;
    out += "\"}";
    return out;
}

namespace net {

inline void records_sent_inc(std::uint64_t n = 1) {
    MetricsRegistry::global().counter(net_metric_name("records_total", "sink")).increment(n);
}
inline void records_received_inc(std::uint64_t n = 1) {
    MetricsRegistry::global().counter(net_metric_name("records_total", "source")).increment(n);
}
inline void bytes_sent_inc(std::uint64_t n) {
    MetricsRegistry::global().counter(net_metric_name("bytes_total", "sink")).increment(n);
}
inline void bytes_received_inc(std::uint64_t n) {
    MetricsRegistry::global().counter(net_metric_name("bytes_total", "source")).increment(n);
}
inline void connect_attempt(const char* direction = "sink") {
    MetricsRegistry::global()
        .counter(net_metric_name("connect_attempts_total", direction))
        .increment();
}
inline void reconnect(const char* direction = "sink") {
    MetricsRegistry::global().counter(net_metric_name("reconnects_total", direction)).increment();
}
inline void send_error() {
    MetricsRegistry::global().counter(net_metric_name("send_errors_total", "sink")).increment();
}
inline void recv_error() {
    MetricsRegistry::global().counter(net_metric_name("recv_errors_total", "source")).increment();
}
inline void close_send() {
    MetricsRegistry::global().counter(net_metric_name("close_send_total", "sink")).increment();
}
inline void credit_exhaustion() {
    MetricsRegistry::global()
        .counter(net_metric_name("credit_exhaustion_total", "sink"))
        .increment();
}

}  // namespace net

}  // namespace clink::metrics
