#pragma once

// DynamoDB sink: JSON-object records written as items with BatchWriteItem.
//
// Each record is a JSON object (SQL's row_to_json_string writes one per row)
// mapped to an item: a string to S, a number to N, true and false to BOOL, null
// to NULL, an object to M and an array to L. A number keeps the digits it was
// written with; nothing passes through a double. The sink reads the table's key
// schema with DescribeTable when it opens and checks every record against it and
// against DynamoDB's documented limits before buffering it, so a record the
// service would refuse is refused here, by name, and never replaces a buffered
// one.
//
// Buffered records are keyed by item identity (DynamoDbItemKey): the key
// attributes' types and values, a string by its UTF-8 bytes and a number by its
// value, as DynamoDB compares them (1, 1.0 and 10E-1 name one item). A later
// record for a buffered item replaces it, so a batch never names an item twice,
// which the service refuses, and the item ends at the later record's value.
// Buffered items go out in BatchWriteItem calls of at most 25 items and 16 MiB;
// the items a call returns as UnprocessedItems, and only those, are sent again
// with exponential backoff until written or the retry budget is spent. A
// barrier returns only once everything received before it is written, so a
// checkpoint never covers a write still outstanding, and a sink that has failed
// refuses every later barrier.
//
// Delivery is at-least-once: a restart replays the records after the last
// completed checkpoint and each replayed put overwrites its item. The ordering
// and final-state contract is in docs/connectors/aws.md ("The DynamoDB sink").

#include <chrono>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/aws/aws_client.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::aws {

// ---- DynamoDB's limits ----------------------------------------------------------------
//
// From the service documentation (Constraints, item sizes and formats,
// BatchWriteItem). Sizes are binary: 1 KB = 1024 bytes.

inline constexpr std::size_t kDynamoDbMaxItemBytes = 400 * 1024;
inline constexpr std::size_t kDynamoDbMaxPartitionKeyBytes = 2048;
inline constexpr std::size_t kDynamoDbMaxSortKeyBytes = 1024;
inline constexpr std::size_t kDynamoDbMaxAttributeNameBytes = 64 * 1024;
inline constexpr std::size_t kDynamoDbMaxBatchWrites = 25;
// What one BatchWriteItem may carry over the network, its JSON included.
inline constexpr std::size_t kDynamoDbMaxRequestBytes = 16 * 1024 * 1024;
inline constexpr int kDynamoDbMaxNumberDigits = 38;
// The documented 32 levels count the item itself: a map or list may sit at most
// 31 levels below its attribute (an attribute's own value is level 1).
inline constexpr int kDynamoDbMaxContainerDepth = 31;

// ---- items --------------------------------------------------------------------------

// An attribute value as the sink writes it.
struct DynamoDbValue {
    enum class Kind : std::uint8_t { S, N, Bool, Null, M, L };
    Kind kind{Kind::Null};
    std::string text;  // S: the UTF-8 bytes; N: the canonical numeral (DynamoDbNumber::text)
    bool flag{false};  // Bool
    // M: the members in name order, each name once. L: the elements in order,
    // with empty names.
    std::vector<std::pair<std::string, DynamoDbValue>> members;

    friend bool operator==(const DynamoDbValue&, const DynamoDbValue&) = default;
};

// An item: its attributes in name order, each name once.
using DynamoDbItem = std::vector<std::pair<std::string, DynamoDbValue>>;

// A DynamoDB number: the significant digits without leading or trailing zeros,
// and the power of ten of the last one. Zero has no digits and is never
// negative. Two numerals denote the same number exactly when they parse to
// equal DynamoDbNumbers, which is how DynamoDB compares N values.
struct DynamoDbNumber {
    bool negative{false};
    std::string digits;
    std::int64_t exponent{0};

    // The numeral the sink sends: plain decimal where that is at most 40
    // characters, otherwise d.dddE+x. An integer's text is its decimal digits.
    [[nodiscard]] std::string text() const;
    // Its size in an item, without the attribute name: one byte per pair of
    // digits, pairs aligned on the decimal point, plus one. The documentation
    // gives "1 byte per two significant digits, plus 1" as an approximation;
    // the aligned count is never smaller, and is what DynamoDB Local charges.
    [[nodiscard]] std::size_t size_bytes() const;

    friend bool operator==(const DynamoDbNumber&, const DynamoDbNumber&) = default;
};

// Parses a numeral. With `json` true the text must follow JSON's number grammar,
// as a record's numbers do; with it false the leading '+', leading zeros and a
// bare '.' that DynamoDB's own numerals may carry are accepted too. Returns
// nullopt, with the reason in *why when given, for text that is not a numeral,
// a number with more than 38 significant digits, or one outside DynamoDB's
// range (a magnitude of 1E-130 to 9.9999999999999999999999999999999999999E+125).
[[nodiscard]] std::optional<DynamoDbNumber> parse_dynamodb_number(std::string_view text,
                                                                  bool json,
                                                                  std::string* why = nullptr);

// ---- keys and identity --------------------------------------------------------------

enum class DynamoDbKeyType : std::uint8_t { S, N, B };

struct DynamoDbKeyAttribute {
    std::string name;
    DynamoDbKeyType type{DynamoDbKeyType::S};
};

// A table's primary key, as DescribeTable reports it.
struct DynamoDbKeySchema {
    DynamoDbKeyAttribute partition;
    std::optional<DynamoDbKeyAttribute> sort;
};

// One key attribute's value: its type and, for S, its UTF-8 bytes or, for N, its
// canonical numeral. Compared member by member, so distinct values never share a
// representation: no byte of a string is a separator, and the string "1" and the
// number 1 differ by type.
struct DynamoDbKeyValue {
    DynamoDbKeyType type{DynamoDbKeyType::S};
    std::string bytes;

    friend auto operator<=>(const DynamoDbKeyValue&, const DynamoDbKeyValue&) = default;
};

// An item's identity: two records name the same item exactly when their keys
// compare equal.
struct DynamoDbItemKey {
    DynamoDbKeyValue partition;
    std::optional<DynamoDbKeyValue> sort;

    friend auto operator<=>(const DynamoDbItemKey&, const DynamoDbItemKey&) = default;
};

// A key for an error message: each value JSON-quoted, at most 64 bytes of it.
[[nodiscard]] std::string describe_dynamodb_key(const DynamoDbItemKey& key);

// ---- records ------------------------------------------------------------------------

class DynamoDbRecordError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

struct DynamoDbPreparedItem {
    DynamoDbItemKey key;
    DynamoDbItem item;
    std::size_t item_bytes{0};     // DynamoDB's size of the item
    std::size_t request_bytes{0};  // at most this many bytes of a BatchWriteItem's JSON
};

// Maps one record to an item and checks it against `schema` and DynamoDB's
// limits: a JSON object with valid UTF-8 and no U+0000 in any string (the AWS
// SDK for C++ cuts a string there), no attribute name empty, longer than
// 64 KB or repeated, no map or list deeper than 31 levels, every number within
// DynamoDB's precision and range, the key attributes present with the table's
// types and non-empty within their size limits, and the item within 400 KB.
// Throws DynamoDbRecordError naming the first rule the record breaks.
[[nodiscard]] DynamoDbPreparedItem prepare_dynamodb_item(std::string_view record,
                                                         const DynamoDbKeySchema& schema);

// DynamoDB's size of an item: each attribute's name bytes and value, a string by
// its bytes, a number by DynamoDbNumber::size_bytes, a boolean or null 1, a map or
// list 3 plus its elements, each element 1 more (a member also its name bytes).
[[nodiscard]] std::size_t dynamodb_item_bytes(const DynamoDbItem& item);

// An upper bound on the bytes the item adds to a BatchWriteItem request as the
// AWS SDK writes it, its PutRequest wrapper included: the SDK indents the JSON
// with a tab per level, so a nested value costs more than its text, and every
// byte of a string may be escaped.
[[nodiscard]] std::size_t dynamodb_request_bytes(const DynamoDbItem& item);

// The identity of an item the service handed back, or nullopt when a key
// attribute is missing, has the wrong type or is not a valid key value.
[[nodiscard]] std::optional<DynamoDbItemKey> dynamodb_item_key(const DynamoDbItem& item,
                                                               const DynamoDbKeySchema& schema);

// ---- the calls the sink makes -------------------------------------------------------

enum class DynamoDbFault : std::uint8_t {
    None,
    Throttled,  // nothing was written: back off and send again
    Transient,  // the request may or may not have been applied: back off and send again
    Permanent,  // configuration, access, a missing table or a refused request: fail now
    Cancelled,  // the task was cancelled while the call was in flight
};

struct DynamoDbCallError {
    DynamoDbFault fault{DynamoDbFault::None};
    std::string code;     // the service's error code, e.g. ProvisionedThroughputExceededException
    std::string message;  // the service's message
    int http_status{0};   // 0 when no response arrived
    [[nodiscard]] bool ok() const noexcept { return fault == DynamoDbFault::None; }
};

struct DynamoDbTableDescription {
    DynamoDbCallError error;
    std::string status;  // ACTIVE, CREATING, UPDATING, DELETING, ...
    DynamoDbKeySchema key;
    // The table's name and ARN: a response may name the table either way,
    // whichever the request used.
    std::string name;
    std::string arn;
};

// One entry of a response's UnprocessedItems, as the service sent it.
struct DynamoDbUnprocessed {
    enum class Kind : std::uint8_t { Put, Delete, Neither };
    std::string table;  // the RequestItems key it was listed under
    Kind kind{Kind::Put};
    DynamoDbItem item;  // Put: the item; Delete: the key
    // False when the entry carries a value of a type the sink never writes
    // (binary or a set), so it cannot be one of the sink's puts.
    bool recognised{true};
};

struct DynamoDbBatchResult {
    DynamoDbCallError error;
    std::vector<DynamoDbUnprocessed> unprocessed;
};

// The calls the sink makes. make_dynamodb_sdk_service is the production
// implementation; tests substitute a model of the service.
class DynamoDbWriteService {
public:
    virtual ~DynamoDbWriteService() = default;
    virtual DynamoDbTableDescription describe_table(const std::string& table) = 0;
    // One BatchWriteItem with a PutRequest per item, every one for `table`.
    virtual DynamoDbBatchResult batch_write(const std::string& table,
                                            const std::vector<const DynamoDbItem*>& items) = 0;
    // From another thread: end the call in flight, and every later one, at once.
    virtual void abort() noexcept {}
};

// The AWS SDK's DynamoDB client with its own retries off, so every failure
// reaches the sink, which classifies, paces and bounds them.
[[nodiscard]] std::shared_ptr<DynamoDbWriteService> make_dynamodb_sdk_service(
    const AwsClientOptions& client);

// How the sink treats an error, from its code, HTTP status (0 when no response
// arrived) and whether the SDK would have retried it.
[[nodiscard]] DynamoDbFault classify_dynamodb_error(std::string_view code,
                                                    int http_status,
                                                    bool sdk_retryable);

// ---- the sink -----------------------------------------------------------------------

struct DynamoDbSinkOptions {
    std::string table;          // required: a table name or ARN
    std::string partition_key;  // required: the table's partition key attribute
    std::string sort_key;       // the table's sort key attribute; required when it has one
    AwsClientOptions client;
    std::size_t batch_records{25};  // items per BatchWriteItem, 1 to 25
    // Retries in a row that write nothing (a failed request, or UnprocessedItems
    // that did not shrink) before a flush fails, 0 to kMaxRetries: 0 fails at the
    // first such attempt.
    int max_retries{8};
    std::chrono::milliseconds retry_base_backoff{100};
    std::chrono::milliseconds max_backoff{10000};
    // The longest one BatchWriteItem's items, or the open's DescribeTable, may
    // take with every retry and wait.
    std::chrono::milliseconds retry_timeout{60000};
    std::chrono::milliseconds max_age{0};  // linger: flush a partial batch this old (0 = off)
    std::string name{"dynamodb_sink"};
    // The calls the sink makes; empty means the AWS SDK.
    std::shared_ptr<DynamoDbWriteService> service;
};

class DynamoDbSink : public Sink<std::string> {
public:
    static constexpr int kMaxRetries = 20;

    // Checks the options; touches no service.
    explicit DynamoDbSink(DynamoDbSinkOptions opts);
    ~DynamoDbSink() override;
    DynamoDbSink(const DynamoDbSink&) = delete;
    DynamoDbSink& operator=(const DynamoDbSink&) = delete;
    DynamoDbSink(DynamoDbSink&&) = delete;
    DynamoDbSink& operator=(DynamoDbSink&&) = delete;

    // Reads the table's key schema and checks the options against it.
    void open() override;
    void close() override;
    // Checks every record of the batch before buffering any: a batch with a
    // record the sink refuses adds nothing, and the sink fails.
    void on_data(const Batch<std::string>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    // Its barrier hook writes out everything it received before the barrier,
    // which is its guarantee only while it owns its chain's checkpoint, so
    // Dag::add_sink refuses another sink beside it.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    void flush() override;
    std::string name() const override;

    // Distinct items buffered and not yet written.
    [[nodiscard]] std::size_t pending_items() const;
    // True once a refused record or a failed write has stopped this instance;
    // it then refuses every call that would write or acknowledge a barrier.
    [[nodiscard]] bool failed() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::aws
