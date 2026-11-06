#pragma once

// Amazon Data Firehose sink: string records written with PutRecordBatch.
//
// Each record is opaque bytes, sent as one Firehose record's data with the
// delimiter appended (a newline by default), so a destination that concatenates
// records can split them again. The sink never parses a record.
//
// Limits, from the service documentation: a record holds at most 1,000 KiB
// (1,024,000 bytes) before base64 encoding, its delimiter included, and a
// request at most 500 records and 4 MiB. The documentation does not say whether
// the 4 MiB counts the base64 encoding, so the sink counts a request as the SDK
// sends it, base64 and JSON included, which keeps it within either reading. A
// record over its limit is refused when it arrives, by its position and size; the
// buffer is flushed before a record that would push the next request past its
// count or byte limit is added, so no request is ever built over a limit.
//
// A response counts only when it accounts for every record: one entry per
// record, each holding a RecordId or an ErrorCode, never both or neither, and a
// FailedPutCount equal to the entries that failed. The records whose entries
// failed, and only those, are sent again. Any other response is ambiguous and
// fails the sink. Throttling and transient errors are retried with a backoff
// bounded in attempts and in time, which a cancellation cuts short; permanent
// errors fail at once. A barrier returns only once every record before it has
// been accepted, and a sink that has failed refuses every later barrier.
//
// Accepted means accepted by Firehose: delivery to the destination is the
// service's, after its own buffering and retries. Delivery into Firehose is
// at-least-once: a record resent after a reply that never arrived, or replayed
// after a restart, can reach the destination twice, and Firehose keeps no order
// and no producer identity. See docs/connectors/aws.md ("The Firehose sink").

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "clink/aws/aws_client.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::aws {

// ---- Firehose's limits ----------------------------------------------------------------

inline constexpr std::size_t kFirehoseMaxRecordBytes = 1000 * 1024;  // before base64
inline constexpr std::size_t kFirehoseMaxBatchRecords = 500;
inline constexpr std::size_t kFirehoseMaxRequestBytes = 4 * 1024 * 1024;

// What a record of `data_bytes` (delimiter included) adds to a PutRecordBatch
// request as the AWS SDK sends it: its data base64-encoded, inside its indented
// JSON object, and the separator before the next one.
[[nodiscard]] std::size_t firehose_record_request_bytes(std::size_t data_bytes);
// The request's JSON around its records, for a stream name of `name_bytes`.
[[nodiscard]] std::size_t firehose_request_envelope_bytes(std::size_t name_bytes);
// The smallest max_bytes that still lets the largest record go alone.
[[nodiscard]] std::size_t firehose_min_request_bytes(std::size_t name_bytes);

// ---- the calls the sink makes -------------------------------------------------------

enum class FirehoseFault : std::uint8_t {
    None,
    Throttled,  // nothing was accepted: back off and send again
    Transient,  // the request may or may not have been accepted: back off and send again
    Permanent,  // configuration, access, a missing stream or a refused request: fail now
    Cancelled,  // the task was cancelled while the call was in flight
};

struct FirehoseCallError {
    FirehoseFault fault{FirehoseFault::None};
    std::string code;     // the service's error code, e.g. ServiceUnavailableException
    std::string message;  // the service's message
    int http_status{0};   // 0 when no response arrived
    [[nodiscard]] bool ok() const noexcept { return fault == FirehoseFault::None; }
};

// One entry of a response's RequestResponses, as the service sent it.
struct FirehoseResponseEntry {
    std::string record_id;
    std::string error_code;
    std::string error_message;
};

struct FirehoseBatchResult {
    FirehoseCallError error;
    // A response the SDK read without FailedPutCount reports 0 here.
    std::int64_t failed_put_count{0};
    std::vector<FirehoseResponseEntry> entries;
};

// The verdict on a response: when `valid`, the positions (in the request) of the
// records whose entries failed; otherwise why the response cannot be trusted.
struct FirehoseVerdict {
    bool valid{false};
    std::vector<std::size_t> failed;
    std::string why;
};

// Judges a response to a request of `sent` records: one entry per record, each
// a RecordId or an ErrorCode and not both, and a FailedPutCount equal to the
// failed entries. Never reads past the shorter of the two.
[[nodiscard]] FirehoseVerdict judge_firehose_response(std::size_t sent,
                                                      const FirehoseBatchResult& result);

// The calls the sink makes. make_firehose_sdk_service is the production
// implementation; tests substitute a model of the service.
class FirehoseWriteService {
public:
    virtual ~FirehoseWriteService() = default;
    // One PutRecordBatch of these records, in order, to `stream`.
    virtual FirehoseBatchResult put_record_batch(
        const std::string& stream, const std::vector<const std::string*>& records) = 0;
    // From another thread: end the call in flight, and every later one, at once.
    virtual void abort() noexcept {}
};

// The AWS SDK's Firehose client with its own retries off, so every failure
// reaches the sink, which classifies, paces and bounds them.
[[nodiscard]] std::shared_ptr<FirehoseWriteService> make_firehose_sdk_service(
    const AwsClientOptions& client);

// How the sink treats an error, from its code, HTTP status (0 when no response
// arrived) and whether the SDK would have retried it.
[[nodiscard]] FirehoseFault classify_firehose_error(std::string_view code,
                                                    int http_status,
                                                    bool sdk_retryable);

// ---- the sink -----------------------------------------------------------------------

struct FirehoseSinkOptions {
    std::string delivery_stream;  // required: 1 to 64 of a-z, A-Z, 0-9, '_', '.', '-'
    // Appended to each record's data, and counted in its size; empty appends nothing.
    std::string delimiter{"\n"};
    AwsClientOptions client;
    std::size_t batch_records{500};  // records per PutRecordBatch, 1 to 500
    // The most one request may carry as the SDK sends it, from
    // firehose_min_request_bytes (the largest record alone) to 4 MiB.
    std::size_t max_bytes{kFirehoseMaxRequestBytes};
    // Retries in a row that accept nothing (a failed request, or failed entries
    // that did not shrink) before a flush fails, 0 to kMaxRetries: 0 fails at the
    // first such attempt.
    int max_retries{8};
    std::chrono::milliseconds retry_base_backoff{100};
    std::chrono::milliseconds max_backoff{10000};
    // The longest one request's records may take with every retry and wait.
    std::chrono::milliseconds retry_timeout{60000};
    std::chrono::milliseconds max_age{0};  // linger: flush a partial batch this old (0 = off)
    std::string name{"firehose_sink"};
    // The calls the sink makes; empty means the AWS SDK.
    std::shared_ptr<FirehoseWriteService> service;
};

class FirehoseSink : public Sink<std::string> {
public:
    static constexpr int kMaxRetries = 20;

    // Checks the options; touches no service.
    explicit FirehoseSink(FirehoseSinkOptions opts);
    ~FirehoseSink() override;
    FirehoseSink(const FirehoseSink&) = delete;
    FirehoseSink& operator=(const FirehoseSink&) = delete;
    FirehoseSink(FirehoseSink&&) = delete;
    FirehoseSink& operator=(FirehoseSink&&) = delete;

    // Starts afresh: an empty buffer and no failure, also after an earlier one.
    void open() override;
    // Writes nothing: a clean end of input has flushed, and a failed or
    // cancelled task must not send what it holds.
    void close() override;
    // Checks every record of the batch before buffering any: a batch with a
    // record over the limit adds nothing, and the sink fails.
    void on_data(const Batch<std::string>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    // Its barrier hook writes out everything it received before the barrier,
    // which is its guarantee only while it owns its chain's checkpoint, so
    // Dag::add_sink refuses another sink beside it.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    void flush() override;
    std::string name() const override;

    // Records buffered and not yet accepted.
    [[nodiscard]] std::size_t pending_records() const;
    // True once a refused record or a failed send has stopped this instance; it
    // then refuses every call that would send or acknowledge a barrier, until
    // open() starts it afresh.
    [[nodiscard]] bool failed() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::aws
