#pragma once

// Kinesis Data Streams source: lists a stream's shards (ListShards), reads each
// shard it owns (GetShardIterator, GetRecords) and emits every record's Data as
// a std::string (JSON on the SQL path). At-least-once: the checkpoint holds, for
// each owned shard, the last record handed downstream, and a restore resumes
// right after it.
//
// Ownership. Shard s belongs to subtask kinesis_shard_owner(s, parallelism), a
// hash of the ShardId alone. Subtasks that list the stream at different
// moments, page through it differently or restart still agree on every owner,
// so no shard is read by two subtasks and none by nobody.
//
// Lineage. The service names a split's parent and a merge's two parents on each
// child. A child is read once every parent this subtask owns has been read to
// its end; a parent absent from the listing has left the stream. At parallelism
// 1 this subtask owns every parent, so records of one hash key are emitted in
// the order the stream accepted them, across splits and merges. Above
// parallelism 1 a parent owned by another subtask is not waited for, and the
// order holds within each shard only. Nothing orders records across shards.
//
// Discovery lists the stream every discovery_interval and soon after an owned
// shard ends, whether or not this subtask owns shards or receives records.
//
// Checkpoint. One row per owned shard (status, last emitted sequence number,
// parents, where an unread shard starts) and one row per subtask (parallelism,
// stream, start mode, start time). A restore resumes each shard AT its saved
// sequence number and requires the service to return that record first, so a
// position that left the stream fails the job instead of skipping records. A
// restore at another parallelism needs every previous subtask's rows (the
// file:// backend hands them to every subtask); one that lacks them is refused.
// See docs/connectors/aws.md for the full contract.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/aws/aws_client.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/state_backend.hpp"

namespace clink::aws {

// ---- the service seam --------------------------------------------------------

struct KinesisShard {
    std::string id;
    std::string parent;           // ParentShardId; empty for a shard with no parent
    std::string adjacent_parent;  // AdjacentParentShardId: a merge's second parent
    // SequenceNumberRange.StartingSequenceNumber: fixed for the life of the
    // shard, so a different value under a known ShardId means the stream was
    // deleted and created again.
    std::string starting_sequence;
};

struct KinesisRecord {
    std::string sequence_number;
    std::string data;
};

// Where a shard read starts. AtSequence resumes at the last record handed
// downstream: the service must return that record first, which proves the
// position is still in the stream, and the source drops it.
struct KinesisReadFrom {
    enum class Kind { TrimHorizon, AtTimestamp, AtSequence };
    Kind kind{Kind::TrimHorizon};
    std::string sequence_number;   // AtSequence
    std::int64_t timestamp_ms{0};  // AtTimestamp: Unix epoch milliseconds
};

// How the source handles a failed call.
enum class KinesisFault {
    None,
    ExpiredIterator,      // GetRecords: mint a new iterator at the shard's position
    ExpiredNextToken,     // ListShards: list again from the first page
    Throttled,            // back off and retry
    Transient,            // back off and retry within the retry budget
    PositionUnavailable,  // GetShardIterator/GetRecords: the shard or the sequence number is gone
    Permanent,            // access, configuration, a missing stream: fail now
    Cancelled,            // cancel() aborted the call
};

enum class KinesisCall { ListShards, GetShardIterator, GetRecords };

struct KinesisCallError {
    KinesisFault fault{KinesisFault::None};
    std::string code;  // the service's error code, e.g. ProvisionedThroughputExceededException
    std::string message;
    [[nodiscard]] bool ok() const noexcept { return fault == KinesisFault::None; }
};

struct KinesisShardPage {
    std::vector<KinesisShard> shards;
    std::string next_token;  // empty on the last page
    KinesisCallError error;
};

struct KinesisIterator {
    std::string iterator;
    KinesisCallError error;
};

struct KinesisRecordBatch {
    std::vector<KinesisRecord> records;
    std::string next_iterator;  // empty: the shard is closed and read to its end
    std::int64_t millis_behind{-1};
    KinesisCallError error;
};

// The calls the source makes. make_kinesis_sdk_service is the production
// implementation; the source's tests run it against a model of the service.
class KinesisShardService {
public:
    virtual ~KinesisShardService() = default;
    // One ListShards page. An empty next_token names the stream (the first page).
    virtual KinesisShardPage list_shards(const std::string& next_token) = 0;
    virtual KinesisIterator shard_iterator(const std::string& shard_id,
                                           const KinesisReadFrom& from) = 0;
    virtual KinesisRecordBatch get_records(const std::string& iterator, int limit) = 0;
    // Called from another thread by cancel(): abort calls in flight and fail
    // later ones as Cancelled.
    virtual void abort() noexcept {}
};

// The AWS SDK implementation (src/kinesis_sdk_service.cpp). The SDK's own
// retries are off: the source classifies every failure and paces its retries.
// `list_page_size` is ListShards MaxResults (0 = the service default).
std::shared_ptr<KinesisShardService> make_kinesis_sdk_service(const std::string& stream,
                                                              const AwsClientOptions& client,
                                                              int list_page_size = 0);

// Maps a service error to how the source handles it: `code` is the error code
// (namespace prefix allowed), `http_status` the response status (0 when no
// response arrived) and `retryable` the SDK's verdict for codes this does not
// know. Pure.
KinesisFault classify_kinesis_error(KinesisCall call,
                                    std::string_view code,
                                    int http_status,
                                    bool retryable);

// ---- ownership and checkpoint rows -------------------------------------------

// The subtask of `parallelism` that owns `shard_id`: FNV-1a 64 of the ShardId,
// modulo the parallelism. A function of the id alone, never of a listing, so it
// is stable across pages, listing order, restarts and the moments different
// subtasks list the stream. Pure.
inline std::uint32_t kinesis_shard_owner(std::string_view shard_id,
                                         std::uint32_t parallelism) noexcept {
    if (parallelism <= 1) {
        return 0;
    }
    std::uint64_t h = 1469598103934665603ULL;
    for (const char c : shard_id) {
        h ^= static_cast<std::uint8_t>(c);
        h *= 1099511628211ULL;
    }
    return static_cast<std::uint32_t>(h % parallelism);
}

// One owned shard's checkpoint row, under "__kinesis_shard__:<ShardId>".
struct KinesisShardCheckpoint {
    enum class Status {
        Waiting,  // a child whose parents this subtask owns are not yet read to their end
        Reading,  // eligible; last_sequence is the last record handed downstream, if any
        Drained,  // closed and read to its end
    };
    Status status{Status::Waiting};
    std::string last_sequence;
    std::string parent;
    std::string adjacent_parent;
    std::string starting_sequence;
    // Where the shard starts while last_sequence is empty: the latest-mode start
    // time, or nullopt for the shard's oldest retained record.
    std::optional<std::int64_t> start_timestamp_ms;
};

// One subtask's row, under "__kinesis_subtask__:<index>". Present in every
// checkpoint the source writes, so its presence marks a recovery and its
// parallelism tells a restore which rows it must have received.
struct KinesisSubtaskCheckpoint {
    std::uint32_t parallelism{1};
    std::string initial_position;
    std::int64_t start_ms{0};  // the first start's wall-clock time (latest mode reads from it)
    std::string stream;
};

inline constexpr std::string_view kKinesisShardRowPrefix = "__kinesis_shard__:";
inline constexpr std::string_view kKinesisSubtaskRowPrefix = "__kinesis_subtask__:";
// Earlier releases kept one row per shard holding the last sequence number.
inline constexpr std::string_view kKinesisLegacyRowPrefix = "__kinesis_seq__:";

std::string encode_kinesis_shard_checkpoint(const KinesisShardCheckpoint& row);
std::string encode_kinesis_subtask_checkpoint(const KinesisSubtaskCheckpoint& row);
// Throw std::runtime_error on a row this version cannot read (another format
// version or a malformed value): a restore never guesses at a position.
KinesisShardCheckpoint decode_kinesis_shard_checkpoint(std::string_view value);
KinesisSubtaskCheckpoint decode_kinesis_subtask_checkpoint(std::string_view value);

// ---- the source ----------------------------------------------------------------

struct KinesisSourceOptions {
    std::string stream;                            // stream name or ARN (required)
    std::string initial_position{"trim_horizon"};  // "trim_horizon" | "latest"
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};
    AwsClientOptions client;
    int max_records_per_poll{1000};  // GetRecords Limit (1..10000)
    // How long a caught-up shard waits before its next GetRecords.
    std::chrono::milliseconds poll_interval{250};
    // The least time between two GetRecords on one shard: the service allows
    // five a second per shard.
    std::chrono::milliseconds read_spacing{200};
    // ListShards cadence; an owned shard reaching its end lists sooner.
    std::chrono::milliseconds discovery_interval{10000};
    // How long one call (ListShards, or one shard's reads) may keep failing
    // with throttling or transient errors before the source fails the task.
    std::chrono::milliseconds retry_timeout{300000};
    std::chrono::milliseconds max_backoff{10000};
    int list_page_size{0};  // ListShards MaxResults; 0 = the service default
    std::string name{"kinesis_source"};
    // The service the source calls; null = the AWS SDK, built at open().
    std::shared_ptr<KinesisShardService> service;
};

// One owned shard as the source tracks it (diagnostics and tests).
struct KinesisShardStatus {
    std::string shard_id;
    KinesisShardCheckpoint::Status status{KinesisShardCheckpoint::Status::Waiting};
    std::string last_sequence;
    std::string parent;
    std::string adjacent_parent;
};

class KinesisSource : public Source<std::string> {
public:
    explicit KinesisSource(KinesisSourceOptions opts);
    ~KinesisSource() override;
    KinesisSource(const KinesisSource&) = delete;
    KinesisSource& operator=(const KinesisSource&) = delete;

    void open() override;
    bool produce(Emitter<std::string>& out) override;
    void cancel() override;
    void close() override;

    [[nodiscard]] bool is_bounded() const noexcept override { return false; }
    [[nodiscard]] std::size_t split_count() const noexcept override;
    std::string name() const override { return opts_.name; }

    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override;
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;

    // The owned shards, ordered by ShardId.
    [[nodiscard]] std::vector<KinesisShardStatus> shard_statuses() const;
    // Complete listings applied since open().
    [[nodiscard]] std::uint64_t listings() const noexcept;

private:
    struct Impl;
    KinesisSourceOptions opts_;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::aws
