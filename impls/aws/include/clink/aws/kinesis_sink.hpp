#pragma once

// Kinesis Data Streams sink: string records written with PutRecords.
//
// Each record's bytes are sent as one Kinesis record's data, untouched. Its
// partition key comes from one of two modes, chosen by partition_key:
//
//   - set: the record must be one JSON object holding that top-level member,
//     and the member's value is the key. A string is used as it is, a number as
//     its exact value in plain decimal (9007199254740993 stays itself, 1.50 and
//     15E-1 are both "1.5", -0 is "0"), and true or false as that word. A record
//     whose member is missing, null, an object or an array, appears twice, or
//     would make a key Kinesis refuses is refused: the sink never routes a record
//     by anything but its own key.
//   - unset: the sink numbers records 0, 1, 2 and so on per instance, so records
//     spread across shards. A key is fixed when the record is buffered, so every
//     retry of it carries the same key; a restarted instance numbers afresh, so a
//     replayed record can land on another shard than its first copy.
//
// Limits, from the service's documentation: a partition key holds 1 to 256
// characters (counted here as UTF-16 code units, so a character outside the
// Basic Multilingual Plane counts as two, the stricter reading); a record's
// data plus its key's UTF-8 bytes must not exceed the stream's maximum record
// size (1 MiB unless the stream was raised, at most 10 MiB); a request holds at
// most 500 records and 10 MiB of data and keys. A record over a limit is refused
// before it is buffered; the buffer is sent before a record that would take the
// next request past its count or byte limit is added, so no request is ever
// built over a limit. A refused record fails the sink, or under dlq='drop' goes
// to the job's dead-letter path while the rest of the batch is written.
//
// A response counts only when it accounts for every record: one entry per
// record, each either accepted (a well-formed SequenceNumber and ShardId) or
// failed (an ErrorCode), never both or neither, no sequence number twice, and a
// FailedRecordCount equal to the entries that failed. The records whose entries
// failed, and only those, are sent again with the keys they were sent with. Any
// other response is ambiguous and fails the sink. Throttling and transient
// errors are retried with a backoff bounded in attempts and in time, which a
// cancellation cuts short; permanent errors (access, credentials, a missing
// stream, KMS, a refused request) fail at once and are never dead-lettered. A
// barrier returns only once every record before it has been accepted, and a sink
// that has failed refuses every later barrier until it is opened again.
//
// Delivery is at-least-once: a record resent after a reply that never arrived,
// or replayed after a restart, can be stored twice, and Kinesis keeps no
// producer identity. PutRecords keeps no order within a request and the sink
// resends failed entries after later ones were stored, so records sharing a
// partition key are not promised to arrive in order. See
// docs/connectors/aws.md ("The Kinesis sink").

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

// ---- Kinesis's limits -----------------------------------------------------------------

inline constexpr std::size_t kKinesisMaxBatchRecords = 500;
// A stream's maximum record size unless raised (MaxRecordSizeInKiB 1024).
inline constexpr std::size_t kKinesisDefaultMaxRecordBytes = 1024 * 1024;
// The most a stream's maximum record size can be raised to (MaxRecordSizeInKiB 10240).
inline constexpr std::size_t kKinesisMaxRecordBytes = 10 * 1024 * 1024;
// A PutRecords request: every record's data and partition key.
inline constexpr std::size_t kKinesisMaxRequestBytes = 10 * 1024 * 1024;
// The sink's default request bound: the documented limit before records over
// 1 MiB were introduced, within the current one.
inline constexpr std::size_t kKinesisDefaultRequestBytes = 5 * 1024 * 1024;
inline constexpr std::size_t kKinesisMaxPartitionKeyChars = 256;

// What a sink does with a record it refuses before sending it: a missing or
// unusable partition key, or a record over the maximum record size. Fail (the
// default) stops the sink, and the job's restart replays the record; Drop sends
// it to the job's dead-letter path and writes the rest. Errors from the service
// are never dropped.
enum class DlqPolicy { Fail, Drop };

// ---- partition keys -----------------------------------------------------------------

// A record's partition key under partition_key=`field`, or why there is none.
struct KinesisKeyResult {
    bool ok{false};
    std::string key;  // valid when ok
    std::string why;  // the refusal, when not ok
};

// The key `record` routes by: the value of its top-level member `field`, read
// as the header describes. The record must be one JSON object (RFC 8259, UTF-8);
// anything else is refused with the reason. Pure.
[[nodiscard]] KinesisKeyResult kinesis_record_key(std::string_view record, std::string_view field);

// Why `key` cannot be a partition key, or empty when it can: 1 to 256 UTF-16
// code units of valid UTF-8 with no U+0000 (the AWS SDK for C++ cuts a string
// at its first NUL when it writes the request). Pure.
[[nodiscard]] std::string kinesis_key_problem(std::string_view key);

// The partition key's length as the service's limit counts it: UTF-16 code
// units of valid UTF-8. Pure.
[[nodiscard]] std::size_t kinesis_key_chars(std::string_view key);

// ---- the calls the sink makes -------------------------------------------------------

enum class KinesisPutFault : std::uint8_t {
    None,
    Throttled,  // nothing was stored: back off and send again
    Transient,  // the request may or may not have been stored: back off and send again
    Permanent,  // configuration, access, a missing stream or a refused request: fail now
    Cancelled,  // the task was cancelled while the call was in flight
};

struct KinesisPutError {
    KinesisPutFault fault{KinesisPutFault::None};
    std::string code;     // the service's error code, e.g. ProvisionedThroughputExceededException
    std::string message;  // the service's message
    int http_status{0};   // 0 when no response arrived
    [[nodiscard]] bool ok() const noexcept { return fault == KinesisPutFault::None; }
};

// One entry of a response's Records, as the service sent it.
struct KinesisPutResponseEntry {
    std::string sequence_number;
    std::string shard_id;
    std::string error_code;
    std::string error_message;
};

struct KinesisPutResult {
    KinesisPutError error;
    // A response the SDK read without FailedRecordCount reports 0 here.
    std::int64_t failed_record_count{0};
    std::vector<KinesisPutResponseEntry> entries;
};

// The verdict on a response: when `valid`, the positions (in the request) of the
// records whose entries failed; otherwise why the response cannot be trusted.
struct KinesisPutVerdict {
    bool valid{false};
    std::vector<std::size_t> failed;
    std::string why;
};

// Judges a response to a request of `sent` records as the header describes.
// Never reads past the shorter of the request and the response. Pure.
[[nodiscard]] KinesisPutVerdict judge_kinesis_response(std::size_t sent,
                                                       const KinesisPutResult& result);

// One record of a PutRecords request. Both views stay valid for the call.
struct KinesisPutRecord {
    std::string_view data;
    std::string_view partition_key;
};

// The calls the sink makes. make_kinesis_write_sdk_service is the production
// implementation; tests substitute a model of the service.
class KinesisWriteService {
public:
    virtual ~KinesisWriteService() = default;
    // One PutRecords of these records, in order, to `stream` (a name, or an ARN).
    virtual KinesisPutResult put_records(const std::string& stream,
                                         const std::vector<KinesisPutRecord>& records) = 0;
    // From another thread: end the call in flight, and every later one, at once.
    virtual void abort() noexcept {}
};

// The AWS SDK's Kinesis client with its own retries off, so every failure
// reaches the sink, which classifies, paces and bounds them.
[[nodiscard]] std::shared_ptr<KinesisWriteService> make_kinesis_write_sdk_service(
    const AwsClientOptions& client);

// How the sink treats a failed PutRecords, from its error code, HTTP status (0
// when no response arrived) and whether the SDK would have retried it. Pure.
[[nodiscard]] KinesisPutFault classify_kinesis_put_error(std::string_view code,
                                                         int http_status,
                                                         bool sdk_retryable);

// ---- the sink -----------------------------------------------------------------------

struct KinesisSinkOptions {
    // Required: a stream name (1 to 128 of a-z, A-Z, 0-9, '_', '.', '-') or a
    // stream ARN (arn:<partition>:kinesis:<region>:<account>:stream/<name>).
    std::string stream;
    // The record's top-level JSON member that holds its partition key; empty
    // numbers records per instance instead.
    std::string partition_key;
    AwsClientOptions client;
    std::size_t batch_records{500};  // records per PutRecords, 1 to 500
    // The stream's maximum record size, which a record's data plus its key's
    // UTF-8 bytes must not exceed: 1 to 10 MiB. Raise it only with the stream's
    // MaxRecordSizeInKiB.
    std::size_t max_record_bytes{kKinesisDefaultMaxRecordBytes};
    // The most one request may carry in data and keys, from max_record_bytes
    // (the largest record alone) to 10 MiB.
    std::size_t max_request_bytes{kKinesisDefaultRequestBytes};
    // Retries in a row that store nothing (a failed request, or failed entries
    // that did not shrink) before a flush fails, 0 to kMaxRetries: 0 fails at the
    // first such attempt.
    int max_retries{8};
    std::chrono::milliseconds retry_base_backoff{100};
    std::chrono::milliseconds max_backoff{10000};
    // The longest one request's records may take with every retry and wait.
    std::chrono::milliseconds retry_timeout{60000};
    std::chrono::milliseconds max_age{0};  // linger: flush a partial batch this old (0 = off)
    DlqPolicy dlq_policy{DlqPolicy::Fail};
    std::string name{"kinesis_sink"};
    // The calls the sink makes; empty means the AWS SDK.
    std::shared_ptr<KinesisWriteService> service;
};

class KinesisSink : public Sink<std::string> {
public:
    static constexpr int kMaxRetries = 20;

    // Checks the options; touches no service.
    explicit KinesisSink(KinesisSinkOptions opts);
    ~KinesisSink() override;
    KinesisSink(const KinesisSink&) = delete;
    KinesisSink& operator=(const KinesisSink&) = delete;
    KinesisSink(KinesisSink&&) = delete;
    KinesisSink& operator=(KinesisSink&&) = delete;

    // Starts afresh: an empty buffer and no failure, also after an earlier one.
    void open() override;
    // Writes nothing: a clean end of input has flushed, and a failed or
    // cancelled task must not send what it holds.
    void close() override;
    // Checks every record of the batch before buffering any. Under dlq='fail' a
    // batch with a refused record adds nothing, and the sink fails; under
    // dlq='drop' the refused records go to the dead-letter path and the rest are
    // buffered.
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
