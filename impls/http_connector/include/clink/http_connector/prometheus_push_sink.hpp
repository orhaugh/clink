#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "clink/http_connector/batched_http_bulk_sink.hpp"
#include "clink/http_connector/cancel_watch.hpp"
#include "clink/http_connector/http_bulk_post.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::http_connector {

// Which series each push carries. The Pushgateway's POST replaces every series
// of the pushed metric in the push's group with the series the push carries,
// so this decides which series a scrape of the gateway shows.
enum class PromPushSeries : std::uint8_t {
    // The series of the records since the previous push: each push replaces the
    // series of the one before it, whichever trigger made it.
    Batch,
    // Every series the sink holds: the latest value of each series it was
    // given and, on a run restored from a checkpoint, the series the group held
    // for the metric when the run began. Each push carries them all.
    All,
};

// Options for the Prometheus Pushgateway sink. One sink pushes ONE gauge metric
// into ONE group (job plus grouping): each record is one sample, value_field its
// value and every other field a label. The sink must be the group's only writer
// of the metric.
struct PromPushOptions {
    std::string url;                         // gateway base scheme://host[:port] (required)
    std::string job;                         // the group's job label (required, not empty)
    std::string grouping;                    // the group's other labels "k=v,k2=v2"
    std::string metric_name{"clink_value"};  // the gauge's name
    std::string value_field{"value"};        // the record field holding the sample value
    std::string help;                        // optional # HELP text
    std::string headers;                     // optional "K: V; ..." (e.g. basic auth)
    bool verify_tls{true};
    PromPushSeries push_series{PromPushSeries::Batch};
    std::size_t batch_records{500};  // Batch: push once this many series are buffered
    std::size_t max_series{10000};   // All: the most series the sink holds
    // The most bytes one push may carry. Batch: the sink pushes before its
    // buffer passes it. All: a record that would take the push past it fails
    // the sink, which cannot push part of the series it holds.
    std::size_t batch_bytes{4 * 1024 * 1024};
    std::size_t max_sample_bytes{64 * 1024};  // the longest sample line; a longer record is refused
    int max_retries{4};                       // attempts beyond the first, 0 to 20
    std::chrono::milliseconds max_age{0};     // linger: push updates this old (0 = off)
    int connect_timeout_ms{5000};
    int request_timeout_ms{30000};  // the longest one push or read may take
    DlqPolicy dlq_policy{DlqPolicy::Fail};
    std::string name{"prometheus_sink"};
};

// The rules a record must meet to become a sample, and the push path and
// preamble of the group. Built once per sink; the constructor checks every
// option that shapes a sample or the path and throws std::invalid_argument
// naming the option.
//
// A record is one JSON object. value_field must hold a JSON number, a boolean
// (1 or 0), a string holding a decimal number, or one of the strings NaN,
// Infinity, -Infinity, +Inf and -Inf; an integer must be within 2^53, which a
// gauge holds exactly. Every other field is a label: its name must be a
// Prometheus label name ([a-zA-Z_][a-zA-Z0-9_]*, not starting with __) and its
// value a string (valid UTF-8), a number (its JSON text exactly as written), a
// boolean (true or false) or null. An empty string and null both mean the label
// is absent, as Prometheus treats an empty label value. A field named after a
// grouping label (job included) must be absent or equal to the group's value:
// the gateway would otherwise replace it with the group's value without a word.
class PromSampleRenderer {
public:
    explicit PromSampleRenderer(const PromPushOptions& o);

    struct Sample {
        std::string labels;  // the series' label block, "" or {a="x",b="y"}: its identity
        double value{0};
    };

    // The record's sample. Throws UnrenderableRecord, naming the rule and the
    // field but never a value.
    [[nodiscard]] Sample render(const std::string& record) const;

    // One exposition line, LF-terminated, and its length.
    [[nodiscard]] std::string line(const std::string& labels, double value) const;
    [[nodiscard]] std::size_t line_size(const std::string& labels, double value) const;

    // The label block of a series the gateway holds in this group, from the
    // labels it reports (the grouping labels and the empty instance label the
    // gateway adds left out). Throws std::runtime_error for a series the sink
    // could not have pushed.
    [[nodiscard]] std::string labels_of(const std::map<std::string, std::string>& reported) const;

    [[nodiscard]] const std::string& path() const noexcept { return path_; }
    [[nodiscard]] const std::string& preamble() const noexcept { return preamble_; }
    [[nodiscard]] const std::string& metric() const noexcept { return metric_; }
    // The group's labels, job included.
    [[nodiscard]] const std::map<std::string, std::string>& group() const noexcept {
        return group_;
    }

private:
    [[noreturn]] void refuse(const std::string& why) const;

    std::string name_;
    std::string metric_;
    std::string value_field_;
    std::size_t max_sample_bytes_;
    std::map<std::string, std::string> group_;
    std::string path_;
    std::string preamble_;
};

// What a push's response means.
struct PromPushVerdict {
    enum class Kind : std::uint8_t {
        Pushed,   // 200, or 202 from a gateway that skips its consistency check
        Refused,  // the gateway refused the push whole (400): nothing of it applied
        Retry,    // not confirmed: send it again, then fail
    };
    Kind kind{Kind::Retry};
    std::string detail;  // what the response said, without any value it quoted
};
[[nodiscard]] PromPushVerdict pushgateway_push_result(const BoundedResponse& r);

// The first line of a gateway message with every quoted or braced span
// replaced by "..." or {...}: the gateway quotes the label values of the
// series it refused.
[[nodiscard]] std::string pushgateway_redact(const std::string& body);

// Exposition text of a sample value: NaN, +Inf, -Inf, or the shortest form
// that reads back as the same double.
[[nodiscard]] std::string pushgateway_value_text(double v);

// Prometheus Pushgateway sink. Records are validated and rendered whole before
// anything is buffered; a series appears once per push with its latest value.
//
// Pushes are POSTs to the group's path, each replacing every series of the
// metric in the group with the series it carries (push_series says which).
// They happen at every checkpoint barrier, at end of input, after linger_ms,
// and in Batch mode when batch_records series or batch_bytes are buffered.
// A barrier returns only once a push carrying everything received before it
// was confirmed (200, or 202), so no checkpoint covers a sample no confirmed
// push carried. A failed push keeps what it carried: the next push sends it
// again. A refused record under dlq=fail, and a record past the bounds in All
// mode, are thrown away and the sink refuses every later record, flush and
// barrier until open(). A cancelled task stops at once, a push in flight
// interrupted. Fault points "<name>.before_write" (a push about to be sent)
// and "<name>.after_write" (a push confirmed, the call not yet returned) let
// recovery tests stop a task at either side of a push.
class PrometheusPushSink : public Sink<std::string> {
public:
    explicit PrometheusPushSink(PromPushOptions o);

    void open() override;
    void close() override;
    void on_data(const Batch<std::string>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    // Its barrier returns once everything received before it was pushed,
    // which is its guarantee only while it owns its chain's checkpoint, so
    // Dag::add_sink refuses another sink beside it.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    void flush() override;
    std::string name() const override { return opts_.name; }

private:
    void ingest_(const std::string& rec);
    void hold_(const PromSampleRenderer::Sample& s);
    void push_();
    void adopt_group_();
    BoundedResponse exchange_(const char* what,
                              const std::string& path,
                              const std::string* body,
                              std::size_t max_response_bytes);
    void wait_before_retry_(int attempt, const std::string& why) const;
    void check_usable_() const;
    [[nodiscard]] bool linger_elapsed_() const;
    [[nodiscard]] std::string where_() const;

    PromPushOptions opts_;
    PromSampleRenderer renderer_;
    std::string before_write_point_;
    std::string after_write_point_;
    std::unique_ptr<HttpRequest> client_;
    // Interrupts a request in flight when the task is cancelled or a request
    // runs past request_timeout_ms. Declared after client_, so it is joined
    // before the client goes.
    std::unique_ptr<CancelWatch> watch_;
    std::map<std::string, double> series_;  // label block -> latest value
    std::size_t body_bytes_{0};             // the push body these series make
    std::size_t unpushed_records_{0};       // records since the last confirmed push
    std::chrono::steady_clock::time_point first_unpushed_at_{};
    bool adopt_pending_{false};  // All mode, restored run: read the group first
    std::string unusable_;       // why the sink refuses to go on; empty while usable
};

inline std::shared_ptr<Sink<std::string>> make_prometheus_pushgateway_sink(PromPushOptions o) {
    return std::make_shared<PrometheusPushSink>(std::move(o));
}

}  // namespace clink::http_connector
