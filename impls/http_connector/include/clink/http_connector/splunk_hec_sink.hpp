#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/http_connector/batched_http_bulk_sink.hpp"
#include "clink/http_connector/cancel_watch.hpp"
#include "clink/http_connector/http_bulk_post.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::http_connector {

// Where a confirmed request's guarantee ends.
enum class SplunkHecAck : std::uint8_t {
    // HEC accepted the request (HTTP 200, code 0, 24 or 25): its events were
    // taken into Splunk's pipeline, which is not yet confirmation that they
    // were indexed, and less that a search finds them.
    None,
    // Indexer acknowledgement: every request carries the run's channel, each
    // accepted request returns an ackId, and a barrier returns only once HEC
    // reported every ackId of the requests before it true.
    Indexer,
};

// Options for the Splunk HTTP Event Collector sink. Each record is one JSON
// object, written as the "event" of one envelope; the metadata options apply
// to every event.
struct SplunkHecOptions {
    std::string url;    // scheme://host[:port] (required)
    std::string token;  // HEC token (required), sent as "Authorization: Splunk <token>"
    // The event endpoint: /services/collector/event, /services/collector/event/1.0
    // or /services/collector, behind any prefix a proxy adds.
    std::string path{"/services/collector/event"};
    std::string sourcetype;  // optional metadata, the same for every event
    std::string source;
    std::string host;
    std::string index;    // must be one the token may write to
    std::string headers;  // extra "K: V; ..."; never Authorization or the channel
    bool verify_tls{true};
    std::size_t batch_records{500};            // the most events one request carries
    std::size_t batch_bytes{4 * 1024 * 1024};  // the most bytes one request carries
    int max_retries{4};                        // attempts beyond the first, 0 to 20
    std::chrono::milliseconds max_age{0};      // linger: send events this old (0 = off)
    int connect_timeout_ms{5000};
    int request_timeout_ms{30000};  // the longest one request may take
    SplunkHecAck ack{SplunkHecAck::None};
    // Indexer only: the most accepted requests awaiting acknowledgement, the
    // pause between acknowledgement queries, and how long one request may wait
    // for its acknowledgement before the sink fails.
    std::size_t max_pending_acks{100};
    std::chrono::milliseconds ack_poll_interval{1000};
    std::chrono::milliseconds ack_timeout{120000};
    DlqPolicy dlq_policy{DlqPolicy::Fail};
    std::string name{"splunk_hec_sink"};
};

// The rule a record must meet to become an event, and the envelope it is
// written in. Built once per sink; the constructor checks the metadata options
// and throws std::invalid_argument naming the option.
//
// A record is exactly one JSON object (RFC 8259, strict), with nothing but
// whitespace around it: its strings valid UTF-8 with no raw control
// character and no unpaired surrogate escape, no key repeated within an
// object (compared after unescaping), and at most 100 levels of nesting. It
// becomes {"event":<record>,<metadata>} on one line: the record without the
// whitespace between its tokens and otherwise exactly as written, so its
// numbers keep their digits, its strings their escapes and its keys their
// order. Keys inside the record named like metadata (time, host, source,
// sourcetype, index, fields) stay event content; they never set metadata. An
// envelope longer than batch_bytes is refused, since no request may carry it.
class HecEventRenderer {
public:
    explicit HecEventRenderer(const SplunkHecOptions& o);

    // Appends `record`'s envelope to `out`. Throws UnrenderableRecord, naming
    // the rule and a byte offset but no content of the record, and leaves
    // `out` as it was.
    void render(std::string& out, std::string_view record) const;

    // The metadata members every envelope ends with, ',"host":"..."...', or "".
    [[nodiscard]] const std::string& metadata() const noexcept { return metadata_; }

private:
    std::string name_;
    std::string metadata_;
    std::size_t max_envelope_bytes_;
};

// What a reply to an event request means.
struct HecEventVerdict {
    enum class Kind : std::uint8_t {
        // 200 with code 0, 24 or 25 (the last two warn that HEC is nearing a
        // capacity limit): HEC accepted every event of the request.
        Accepted,
        // 400 with code 6, 12 or 13 and an invalid-event-number naming an event
        // of the request: HEC would not read that event. The events before it
        // may have been accepted and those after it were not read.
        RefusedEvent,
        // Not confirmed, and possibly transient (a lost reply, throttling, a
        // busy or failing server): the request is sent again, then the sink
        // fails. HEC may have accepted part or all of it.
        Retry,
        // Not confirmed, and repeating it would not help (a refused token, a
        // missing index, a reply that is not HEC's or contradicts itself): the
        // sink fails at once, keeping the events.
        Failed,
    };
    Kind kind{Kind::Failed};
    std::string detail;                   // what the reply said; never the token or an event
    std::size_t event{0};                 // RefusedEvent: the position HEC named
    std::optional<std::uint64_t> ack_id;  // Accepted under SplunkHecAck::Indexer
};
[[nodiscard]] HecEventVerdict splunk_hec_event_result(const BoundedResponse& r,
                                                      std::size_t events,
                                                      SplunkHecAck ack);

// What a reply to an acknowledgement query means.
struct HecAckVerdict {
    enum class Kind : std::uint8_t {
        Read,    // 200 with {"acks":{...}}: `indexed` holds the asked ackIds reported true
        Retry,   // not read, possibly transient: ask again while the requests may wait
        Failed,  // not read and not worth asking again
    };
    Kind kind{Kind::Failed};
    std::string detail;
    std::vector<std::uint64_t> indexed;
};
[[nodiscard]] HecAckVerdict splunk_hec_ack_result(const BoundedResponse& r,
                                                  const std::vector<std::uint64_t>& asked);

// A reply as a message may show it: the HTTP status and, for a reply that is
// HEC's, its code and text; otherwise its content type and length, never its
// body.
[[nodiscard]] std::string splunk_hec_describe(const BoundedResponse& r);

// Splunk HTTP Event Collector sink. Each record is checked and rendered whole
// before it is buffered (HecEventRenderer); a refused record goes to the
// dead-letter queue under dlq=drop, and under dlq=fail the sink throws and
// refuses every later record, flush and barrier until open().
//
// A request carries at most batch_records events and batch_bytes bytes, one
// envelope per line. Requests are sent at every checkpoint barrier, at end of
// input, after linger_ms, and when the next event would pass either bound.
// splunk_hec_event_result() judges each reply. A request HEC accepted is done;
// one it did not confirm is sent again whole, up to max_retries times, so an
// event HEC accepted before the reply was lost, or before a busy server gave
// up on the rest, is sent again and can be indexed twice. A refused event is
// dead-lettered under dlq=drop and the rest sent again; under dlq=fail, and
// for every Failed reply, the sink throws with the events kept, so a later
// barrier sends them again rather than passing over them.
//
// Under SplunkHecAck::Indexer the sink chooses a channel GUID at open() and
// sends it with every request. A barrier, and flush(), return only once HEC
// reported true for the ackId of every request accepted before them; until
// then the sink queries /services/collector/ack every ack_poll_interval. A
// request whose acknowledgement has not arrived after ack_timeout, or a reply
// that makes acknowledgements unreadable, fails the sink, which then refuses
// every later record, flush and barrier until open(): it keeps the ackIds but
// not the events, so only the job's replay from its last checkpoint can send
// them again. At most max_pending_acks requests wait at once; the next send
// waits for an acknowledgement first.
//
// A cancelled task stops at once: a request in flight is interrupted and a
// pause cut short, and the sink throws with its events kept. Fault points
// "<name>.before_write" (a request about to be sent) and "<name>.after_write"
// (a request accepted, the call not yet returned) let recovery tests stop a
// task at either side of a write.
class SplunkHecSink : public Sink<std::string> {
public:
    explicit SplunkHecSink(SplunkHecOptions o);

    void open() override;
    void close() override;
    void on_data(const Batch<std::string>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    // Its barrier returns once everything received before it was accepted
    // (and, under Indexer, acknowledged), which is its guarantee only while
    // it owns its chain's checkpoint, so Dag::add_sink refuses another sink
    // beside it.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    void flush() override;
    std::string name() const override { return opts_.name; }

    // The channel this run sends on under Indexer, chosen at open(); empty
    // under None.
    [[nodiscard]] const std::string& channel() const noexcept { return channel_; }

private:
    // An accepted request awaiting its acknowledgement.
    struct Unacked {
        std::uint64_t ack_id{0};
        std::size_t events{0};
        std::chrono::steady_clock::time_point sent{};
    };

    void append_(const std::string& rec);
    void recount_();
    void send_();
    void await_acks_(std::size_t at_most);
    BoundedResponse exchange_(const std::string& path, const std::string& body);
    void pause_(std::chrono::milliseconds d, const std::string& doing) const;
    void check_usable_() const;
    [[noreturn]] void fail_(const std::string& why) const;
    [[nodiscard]] bool linger_elapsed_() const;

    SplunkHecOptions opts_;
    HecEventRenderer renderer_;
    std::string ack_path_;
    std::string before_write_point_;
    std::string after_write_point_;
    std::unique_ptr<HttpRequest> client_;
    // Interrupts a request in flight when the task is cancelled or a request
    // runs past request_timeout_ms. Declared after client_, so it is joined
    // before the client goes.
    std::unique_ptr<CancelWatch> watch_;
    std::vector<std::string> events_;  // rendered envelopes HEC has not accepted
    std::size_t pending_bytes_{0};     // the request body events_ make
    std::chrono::steady_clock::time_point first_buffered_at_{};
    std::string channel_;
    std::vector<Unacked> unacked_;
    std::string unusable_;  // why the sink refuses to go on; empty while usable
};

inline std::shared_ptr<Sink<std::string>> make_splunk_hec_sink(SplunkHecOptions o) {
    return std::make_shared<SplunkHecSink>(std::move(o));
}

}  // namespace clink::http_connector
