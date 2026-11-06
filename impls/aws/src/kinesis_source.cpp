// The Kinesis source: ownership, discovery, lineage, the read loop and its
// checkpoint rows. The AWS SDK calls live in kinesis_sdk_service.cpp behind
// KinesisShardService; everything here is the production logic the tests drive
// against a model of the service.

#include "clink/aws/kinesis_source.hpp"

#include <algorithm>
#include <charconv>
#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <unordered_map>
#include <utility>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/logging.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace clink::aws {

namespace {

using Clock = std::chrono::steady_clock;
using Status = KinesisShardCheckpoint::Status;

constexpr char kLogComponent[] = "kinesis.source";
// Records handed downstream, no checkpoint covering them yet.
constexpr char kFaultAfterEmit[] = "kinesis.source_after_emit";
constexpr char kConnector[] = "kinesis";

// produce() returns to the runner at least this often, so a checkpoint barrier
// waiting between produce() calls is never held behind the source's pacing.
constexpr std::chrono::milliseconds kMaxWaitSlice{100};
constexpr std::chrono::milliseconds kBackoffBase{200};
// An owned shard read to its end lists the stream this soon, to find its children.
constexpr std::chrono::milliseconds kRelistAfterDrain{500};
constexpr std::chrono::milliseconds kMinDiscoveryInterval{100};
constexpr int kMaxConsecutiveExpiries = 5;
constexpr int kMaxNextTokenRestarts = 3;
// A drained shard is forgotten once this many complete listings in a row have
// not named it: it has left the stream and no child can still need it.
constexpr int kListingsBeforePrune = 2;

const char* status_name(Status s) {
    switch (s) {
        case Status::Waiting:
            return "waiting";
        case Status::Reading:
            return "reading";
        case Status::Drained:
            return "drained";
    }
    return "?";
}

std::optional<Status> parse_status(std::string_view s) {
    if (s == "waiting") {
        return Status::Waiting;
    }
    if (s == "reading") {
        return Status::Reading;
    }
    if (s == "drained") {
        return Status::Drained;
    }
    return std::nullopt;
}

const char* call_name(KinesisCall c) {
    switch (c) {
        case KinesisCall::ListShards:
            return "ListShards";
        case KinesisCall::GetShardIterator:
            return "GetShardIterator";
        case KinesisCall::GetRecords:
            return "GetRecords";
    }
    return "?";
}

// "com.amazonaws.kinesis.v20131202#ExpiredIteratorException:..." -> the bare code.
std::string bare_error_code(std::string_view code) {
    if (const auto hash = code.rfind('#'); hash != std::string_view::npos) {
        code.remove_prefix(hash + 1);
    }
    if (const auto colon = code.find(':'); colon != std::string_view::npos) {
        code = code.substr(0, colon);
    }
    return std::string{code};
}

// What to check for an error the source cannot retry past, by code.
std::string permanent_hint(const KinesisCallError& e) {
    const std::string code = bare_error_code(e.code);
    if (code == "ResourceNotFoundException") {
        return " Check that the stream exists in the configured region (and endpoint_override, if "
               "set).";
    }
    if (code.find("AccessDenied") != std::string::npos || code == "UnrecognizedClientException" ||
        code == "InvalidSignatureException" || code == "MissingAuthenticationToken" ||
        code == "IncompleteSignature" || code == "InvalidClientTokenId" ||
        code == "ExpiredTokenException") {
        return " Check the credentials the AWS default chain resolves and that they allow "
               "kinesis:ListShards, kinesis:GetShardIterator and kinesis:GetRecords on the stream "
               "(and kms:Decrypt for an encrypted stream).";
    }
    if (code.rfind("KMS", 0) == 0) {
        return " The stream's KMS key is not usable by these credentials.";
    }
    return {};
}

std::vector<std::string_view> split_fields(std::string_view s, char sep, std::size_t max_fields) {
    std::vector<std::string_view> out;
    while (out.size() + 1 < max_fields) {
        const auto at = s.find(sep);
        if (at == std::string_view::npos) {
            break;
        }
        out.push_back(s.substr(0, at));
        s.remove_prefix(at + 1);
    }
    out.push_back(s);
    return out;
}

template <typename Int>
std::optional<Int> parse_int(std::string_view s) {
    Int v{};
    const auto* end = s.data() + s.size();
    const auto [ptr, ec] = std::from_chars(s.data(), end, v);
    if (s.empty() || ec != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return v;
}

std::int64_t wall_clock_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool starts_with(std::string_view s, std::string_view prefix) {
    return s.size() >= prefix.size() && s.substr(0, prefix.size()) == prefix;
}

}  // namespace

// ---- error classification -------------------------------------------------

KinesisFault classify_kinesis_error(KinesisCall call,
                                    std::string_view raw_code,
                                    int http_status,
                                    bool retryable) {
    static const std::set<std::string, std::less<>> kThrottled = {
        "ProvisionedThroughputExceededException",
        "LimitExceededException",
        "ThrottlingException",
        "Throttling",
        "ThrottledException",
        "RequestLimitExceeded",
        "TooManyRequestsException",
        "KMSThrottlingException",
        "SlowDown",
    };
    static const std::set<std::string, std::less<>> kPermanent = {
        "AccessDeniedException",
        "AccessDenied",
        "UnrecognizedClientException",
        "InvalidSignatureException",
        "IncompleteSignature",
        "IncompleteSignatureException",
        "MissingAuthenticationToken",
        "MissingAuthenticationTokenException",
        "InvalidClientTokenId",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "ExpiredTokenException",
        "ExpiredToken",
        "ValidationException",
        "OptInRequired",
        "KMSAccessDeniedException",
        "KMSDisabledException",
        "KMSInvalidStateException",
        "KMSNotFoundException",
        "KMSOptInRequired",
    };
    static const std::set<std::string, std::less<>> kTransient = {
        "InternalFailure",
        "InternalFailureException",
        "InternalServerError",
        "InternalServerErrorException",
        "ServiceUnavailable",
        "ServiceUnavailableException",
        "RequestTimeout",
        "RequestTimeoutException",
        // The stream is CREATING, UPDATING (resharding) or DELETING; DELETING
        // becomes ResourceNotFound, which is permanent.
        "ResourceInUseException",
    };
    const std::string code = bare_error_code(raw_code);
    if (kThrottled.count(code) != 0 || http_status == 429) {
        return KinesisFault::Throttled;
    }
    if (code == "ExpiredIteratorException") {
        return KinesisFault::ExpiredIterator;
    }
    if (code == "ExpiredNextTokenException") {
        return KinesisFault::ExpiredNextToken;
    }
    if (code == "ResourceNotFoundException") {
        // ListShards: the stream is missing. A shard call: the shard (or, on the
        // emulator, the record at a sequence number) is gone.
        return call == KinesisCall::ListShards ? KinesisFault::Permanent
                                               : KinesisFault::PositionUnavailable;
    }
    if (code == "InvalidArgumentException") {
        switch (call) {
            case KinesisCall::GetShardIterator:
                // The source builds these arguments itself; what the service
                // rejects is the sequence number (another stream's, or no longer
                // in this one).
                return KinesisFault::PositionUnavailable;
            case KinesisCall::GetRecords:
                // An iterator the service does not accept: mint another at the
                // shard's position, as for an expired one (bounded).
                return KinesisFault::ExpiredIterator;
            case KinesisCall::ListShards:
                return KinesisFault::Permanent;
        }
    }
    if (kPermanent.count(code) != 0) {
        return KinesisFault::Permanent;
    }
    if (kTransient.count(code) != 0 || http_status == 0 || http_status >= 500) {
        return KinesisFault::Transient;
    }
    return retryable ? KinesisFault::Transient : KinesisFault::Permanent;
}

// ---- checkpoint rows --------------------------------------------------------

// Rows are text with '|' between fields, led by a format version. ShardIds,
// sequence numbers, stream names and ARNs contain no '|'; the stream, which an
// ARN makes the longest field, is last and takes the rest of the value.

std::string encode_kinesis_shard_checkpoint(const KinesisShardCheckpoint& row) {
    std::string out = "v1|";
    out += status_name(row.status);
    out += '|';
    out += row.last_sequence;
    out += '|';
    out += row.parent;
    out += '|';
    out += row.adjacent_parent;
    out += '|';
    out += row.starting_sequence;
    out += '|';
    if (row.start_timestamp_ms) {
        out += std::to_string(*row.start_timestamp_ms);
    }
    return out;
}

KinesisShardCheckpoint decode_kinesis_shard_checkpoint(std::string_view value) {
    const auto bad = [&](const std::string& why) {
        return std::runtime_error(
            "kinesis_source: cannot read a shard checkpoint row ('" + std::string{value} +
            "'): " + why +
            ". It was written by a format this version does not know; restore it with the "
            "clink release that wrote it.");
    };
    const auto f = split_fields(value, '|', 7);
    if (f.size() != 7 || f[0] != "v1") {
        throw bad("expected 7 fields led by v1");
    }
    KinesisShardCheckpoint row;
    const auto status = parse_status(f[1]);
    if (!status) {
        throw bad("unknown status '" + std::string{f[1]} + "'");
    }
    row.status = *status;
    row.last_sequence = std::string{f[2]};
    row.parent = std::string{f[3]};
    row.adjacent_parent = std::string{f[4]};
    row.starting_sequence = std::string{f[5]};
    if (!f[6].empty()) {
        const auto ts = parse_int<std::int64_t>(f[6]);
        if (!ts) {
            throw bad("start time '" + std::string{f[6]} + "' is not a number");
        }
        row.start_timestamp_ms = *ts;
    }
    return row;
}

std::string encode_kinesis_subtask_checkpoint(const KinesisSubtaskCheckpoint& row) {
    return "v1|" + std::to_string(row.parallelism) + "|" + row.initial_position + "|" +
           std::to_string(row.start_ms) + "|" + row.stream;
}

KinesisSubtaskCheckpoint decode_kinesis_subtask_checkpoint(std::string_view value) {
    const auto bad = [&](const std::string& why) {
        return std::runtime_error(
            "kinesis_source: cannot read a subtask checkpoint row ('" + std::string{value} +
            "'): " + why +
            ". It was written by a format this version does not know; restore it with the clink "
            "release that wrote it.");
    };
    const auto f = split_fields(value, '|', 5);
    if (f.size() != 5 || f[0] != "v1") {
        throw bad("expected 5 fields led by v1");
    }
    KinesisSubtaskCheckpoint row;
    const auto p = parse_int<std::uint32_t>(f[1]);
    const auto start = parse_int<std::int64_t>(f[3]);
    if (!p || *p == 0 || !start) {
        throw bad("parallelism or start time is not a number");
    }
    row.parallelism = *p;
    row.initial_position = std::string{f[2]};
    row.start_ms = *start;
    row.stream = std::string{f[4]};
    return row;
}

// ---- the source -----------------------------------------------------------

namespace {

[[noreturn]] void fail_continuity(const std::string& source_name,
                                  const std::string& shard_id,
                                  const std::string& what) {
    clink::metrics::connector::permanent_failures_inc(kConnector);
    const std::string msg = source_name + ": shard " + shard_id + ": " + what +
                            " The source cannot continue this shard without skipping records it "
                            "owes, so it stops rather than restart at a later point.";
    clink::log::error(kLogComponent, msg);
    throw std::runtime_error(msg);
}

[[noreturn]] void fail_permanent(const std::string& msg) {
    clink::metrics::connector::permanent_failures_inc(kConnector);
    clink::log::error(kLogComponent, msg);
    throw std::runtime_error(msg);
}

std::chrono::milliseconds backoff_delay(int failures, std::chrono::milliseconds cap) {
    return std::min<std::chrono::milliseconds>(cap,
                                               kBackoffBase * (1LL << std::min(failures - 1, 16)));
}

bool power_of_two(int n) {
    return n > 0 && (n & (n - 1)) == 0;
}

std::string seconds_since(Clock::time_point since, Clock::time_point now) {
    return std::to_string(std::chrono::duration_cast<std::chrono::seconds>(now - since).count());
}

}  // namespace

struct KinesisSource::Impl {
    struct Tracked {
        std::string parent;
        std::string adjacent_parent;
        std::string starting_sequence;
        Status status{Status::Waiting};
        std::string last_sequence;  // last record handed downstream
        // The next iterator starts AT last_sequence, and the read must return
        // that record first (a restore, or a re-mint after an expiry).
        bool resume_at_saved{false};
        std::optional<std::int64_t> start_timestamp_ms;  // unread start; nullopt = oldest record
        std::string iterator;
        Clock::time_point next_attempt{};
        int failures{0};
        Clock::time_point failing_since{};
        int expiries{0};
        int absent_listings{0};
    };
    using Shards = std::map<std::string, Tracked>;

    Impl(const KinesisSourceOptions& o, const KinesisSource& s) : opts(o), source(s) {}

    const KinesisSourceOptions& opts;
    const KinesisSource& source;

    // Guards `service` against cancel() on another thread, and carries the
    // cancellable waits.
    mutable std::mutex mu;
    std::condition_variable wake;
    std::shared_ptr<KinesisShardService> service;

    Shards shards;            // owned, by ShardId
    std::string last_polled;  // round-robin cursor

    std::int64_t start_ms{0};
    // Unread shards start at their oldest record whatever initial_position
    // says: the restored state carries no start time to place them.
    bool start_from_oldest{false};

    // Restored rows, applied in open().
    std::map<std::uint32_t, KinesisSubtaskCheckpoint> restored_subtasks;
    std::map<std::string, KinesisShardCheckpoint> restored_shards;
    std::map<std::string, std::string> restored_legacy;

    // Discovery. A listing interrupted by a failure resumes at its token.
    Clock::time_point next_discovery{};
    std::uint64_t listings{0};
    std::string list_token;
    std::vector<KinesisShard> list_partial;
    std::set<std::string> list_seen;
    int token_restarts{0};
    int list_failures{0};
    Clock::time_point list_failing_since{};

    [[nodiscard]] std::string who() const {
        return opts.name + " subtask " + std::to_string(opts.subtask_idx);
    }
    [[nodiscard]] bool owned(const std::string& shard_id) const {
        return kinesis_shard_owner(shard_id, opts.parallelism) == opts.subtask_idx;
    }

    void wait_until(Clock::time_point deadline) {
        std::unique_lock lock(mu);
        wake.wait_until(lock, deadline, [&] { return source.cancelled(); });
    }

    // Where an unread shard discovered now starts: the latest-mode start time,
    // or nullopt for its oldest record.
    [[nodiscard]] std::optional<std::int64_t> start_rule() const {
        if (start_from_oldest || opts.initial_position != "latest") {
            return std::nullopt;
        }
        return start_ms;
    }

    // ---- lineage ------------------------------------------------------------

    // A parent this subtask must see read to its end before the child is read.
    [[nodiscard]] bool settled(const std::string& parent) const {
        if (parent.empty() || !owned(parent)) {
            return true;  // none, or another subtask's: not awaited
        }
        const auto it = shards.find(parent);
        // Untracked: absent from every listing this subtask applied (it left the
        // stream before this source could read it), or drained and forgotten.
        return it == shards.end() || it->second.status == Status::Drained;
    }

    void promote_eligible() {
        for (auto& [id, t] : shards) {
            if (t.status != Status::Waiting || !settled(t.parent) || !settled(t.adjacent_parent)) {
                continue;
            }
            t.status = Status::Reading;
            t.next_attempt = Clock::time_point{};
            if (!t.parent.empty() && listings > 0) {
                clink::log::info(kLogComponent,
                                 who() + ": shard " + id + " is eligible (parents " + t.parent +
                                     (t.adjacent_parent.empty() ? "" : ", " + t.adjacent_parent) +
                                     " read to their end or not this subtask's)");
            }
        }
    }

    // ---- discovery ------------------------------------------------------------

    void apply_listing(const std::vector<KinesisShard>& listing) {
        std::unordered_map<std::string, const KinesisShard*> by_id;
        for (const auto& sh : listing) {
            by_id.emplace(sh.id, &sh);
        }
        for (auto it = shards.begin(); it != shards.end();) {
            auto& t = it->second;
            const auto found = by_id.find(it->first);
            if (found == by_id.end()) {
                if (t.status != Status::Drained) {
                    fail_continuity(
                        opts.name,
                        it->first,
                        std::string{"it is "} + status_name(t.status) +
                            (t.last_sequence.empty() ? " with nothing read yet"
                                                     : " after record " + t.last_sequence) +
                            ", and the stream's shard listing no longer names it: "
                            "its unread records have passed the stream's retention "
                            "period, or the stream was deleted.");
                }
                if (++t.absent_listings >= kListingsBeforePrune) {
                    clink::log::info(kLogComponent,
                                     who() + ": forgetting drained shard " + it->first +
                                         ", which has left the stream");
                    it = shards.erase(it);
                    continue;
                }
            } else {
                const auto& sh = *found->second;
                t.absent_listings = 0;
                if (!t.starting_sequence.empty() && !sh.starting_sequence.empty() &&
                    t.starting_sequence != sh.starting_sequence) {
                    fail_continuity(opts.name,
                                    it->first,
                                    "the stream lists it starting at sequence number " +
                                        sh.starting_sequence + ", and the checkpoint recorded " +
                                        t.starting_sequence +
                                        ": the stream was deleted and created again under the "
                                        "same name.");
                }
                if (t.starting_sequence.empty()) {
                    t.starting_sequence = sh.starting_sequence;
                }
                if (t.parent.empty() && t.adjacent_parent.empty()) {
                    // An earlier release's row carries no lineage.
                    t.parent = sh.parent;
                    t.adjacent_parent = sh.adjacent_parent;
                }
            }
            ++it;
        }
        for (const auto& sh : listing) {
            if (!owned(sh.id) || shards.count(sh.id) != 0) {
                continue;
            }
            Tracked t;
            t.parent = sh.parent;
            t.adjacent_parent = sh.adjacent_parent;
            t.starting_sequence = sh.starting_sequence;
            t.start_timestamp_ms = start_rule();
            shards.emplace(sh.id, std::move(t));
            if (listings > 0) {
                clink::log::info(
                    kLogComponent,
                    who() + ": discovered shard " + sh.id +
                        (sh.parent.empty()
                             ? std::string{}
                             : " (parents " + sh.parent +
                                   (sh.adjacent_parent.empty() ? "" : ", " + sh.adjacent_parent) +
                                   ")"));
            }
        }
        promote_eligible();
        if (++listings == 1) {
            std::size_t counts[3] = {0, 0, 0};
            std::string ids;
            for (const auto& [id, t] : shards) {
                ids += (ids.empty() ? "" : " ") + id;
                ++counts[static_cast<int>(t.status)];
            }
            clink::log::info(kLogComponent,
                             who() + "/" + std::to_string(opts.parallelism) + " owns " +
                                 std::to_string(shards.size()) + " of " +
                                 std::to_string(listing.size()) + " listed shards (waiting " +
                                 std::to_string(counts[0]) + ", reading " +
                                 std::to_string(counts[1]) + ", drained " +
                                 std::to_string(counts[2]) + "): " + ids);
        }
    }

    // Fetches listing pages until the listing is complete, then applies it. A
    // throttled or failed page is retried at the next turn after a backoff,
    // resuming at its token; produce() never blocks on it.
    void discover(KinesisShardService& svc) {
        for (;;) {
            auto page = svc.list_shards(list_token);
            if (!page.error.ok()) {
                on_list_error(page.error);
                return;
            }
            list_failures = 0;
            for (auto& sh : page.shards) {
                if (list_seen.insert(sh.id).second) {
                    list_partial.push_back(std::move(sh));
                }
            }
            if (page.next_token.empty()) {
                break;
            }
            list_token = std::move(page.next_token);
            if (source.cancelled()) {
                return;
            }
        }
        const auto listing = std::move(list_partial);
        list_partial.clear();
        list_seen.clear();
        list_token.clear();
        token_restarts = 0;
        apply_listing(listing);
        next_discovery = Clock::now() + opts.discovery_interval;
    }

    void on_list_error(const KinesisCallError& e) {
        const auto now = Clock::now();
        switch (e.fault) {
            case KinesisFault::Cancelled:
                return;
            case KinesisFault::ExpiredNextToken:
                clink::metrics::connector::error_inc(kConnector, "source");
                if (++token_restarts > kMaxNextTokenRestarts) {
                    fail_permanent(opts.name + ": ListShards on stream '" + opts.stream +
                                   "': the listing's NextToken expired " +
                                   std::to_string(token_restarts) +
                                   " times in a row before the listing completed");
                }
                list_token.clear();
                list_partial.clear();
                list_seen.clear();
                next_discovery = now;
                return;
            case KinesisFault::Throttled:
            case KinesisFault::Transient: {
                clink::metrics::connector::error_inc(kConnector, "source");
                if (list_failures == 0) {
                    list_failing_since = now;
                }
                ++list_failures;
                if (now - list_failing_since >= opts.retry_timeout) {
                    fail_permanent(opts.name + ": ListShards on stream '" + opts.stream +
                                   "' has failed for " + seconds_since(list_failing_since, now) +
                                   " s (" + std::to_string(list_failures) +
                                   " attempts), last with " + e.code + ": " + e.message);
                }
                const auto delay = backoff_delay(list_failures, opts.max_backoff);
                next_discovery = now + delay;
                if (power_of_two(list_failures)) {
                    clink::log::warn(
                        kLogComponent,
                        opts.name + ": ListShards " +
                            (e.fault == KinesisFault::Throttled ? "throttled" : "failed") + " (" +
                            e.code + ": " + e.message + "); attempt " +
                            std::to_string(list_failures) + ", retrying in " +
                            std::to_string(delay.count()) + " ms");
                }
                return;
            }
            case KinesisFault::ExpiredIterator:
            case KinesisFault::PositionUnavailable:
            case KinesisFault::Permanent:
            case KinesisFault::None:
                break;
        }
        clink::metrics::connector::error_inc(kConnector, "source");
        fail_permanent(opts.name + ": ListShards on stream '" + opts.stream + "' failed with " +
                       e.code + ": " + e.message + "." + permanent_hint(e));
    }

    // ---- reading ----------------------------------------------------------------

    void on_shard_error(const std::string& id,
                        Tracked& t,
                        KinesisCall call,
                        const KinesisCallError& e,
                        Clock::time_point now) {
        if (e.fault == KinesisFault::Cancelled) {
            return;
        }
        clink::metrics::connector::error_inc(kConnector, "source");
        switch (e.fault) {
            case KinesisFault::ExpiredIterator:
                // Mint again at the shard's position. With a record already read
                // that is AT the last one, which the service must return first;
                // before the first record it is the shard's start, never the tip.
                t.iterator.clear();
                if (++t.expiries > kMaxConsecutiveExpiries) {
                    fail_permanent(opts.name + ": shard " + id + ": " + std::to_string(t.expiries) +
                                   " iterators in a row were refused as expired or invalid, last "
                                   "with " +
                                   e.code + ": " + e.message);
                }
                return;
            case KinesisFault::Throttled:
            case KinesisFault::Transient: {
                if (t.failures == 0) {
                    t.failing_since = now;
                }
                ++t.failures;
                if (now - t.failing_since >= opts.retry_timeout) {
                    fail_permanent(opts.name + ": shard " + id + ": " + call_name(call) +
                                   " has failed for " + seconds_since(t.failing_since, now) +
                                   " s (" + std::to_string(t.failures) + " attempts), last with " +
                                   e.code + ": " + e.message +
                                   (e.fault == KinesisFault::Throttled
                                        ? ". The shard's read capacity is used up; another "
                                          "consumer of the stream may be reading it heavily."
                                        : "."));
                }
                const auto delay = backoff_delay(t.failures, opts.max_backoff);
                t.next_attempt = now + delay;
                if (power_of_two(t.failures)) {
                    clink::log::warn(
                        kLogComponent,
                        opts.name + ": shard " + id + ": " + call_name(call) + " " +
                            (e.fault == KinesisFault::Throttled ? "throttled" : "failed") + " (" +
                            e.code + ": " + e.message + "); attempt " + std::to_string(t.failures) +
                            ", retrying in " + std::to_string(delay.count()) + " ms");
                }
                return;
            }
            case KinesisFault::PositionUnavailable:
                fail_continuity(opts.name,
                                id,
                                std::string{call_name(call)} + " refused the shard's position (" +
                                    (t.last_sequence.empty() ? std::string{"its start"}
                                                             : "record " + t.last_sequence) +
                                    ") with " + e.code + ": " + e.message +
                                    ": the position has passed the stream's retention period, or "
                                    "the shard or the stream was deleted.");
            case KinesisFault::Cancelled:
            case KinesisFault::ExpiredNextToken:
            case KinesisFault::Permanent:
            case KinesisFault::None:
                break;
        }
        fail_permanent(opts.name + ": shard " + id + ": " + call_name(call) + " failed with " +
                       e.code + ": " + e.message + "." + permanent_hint(e));
    }

    void poll(KinesisShardService& svc,
              const std::string& id,
              Tracked& t,
              Emitter<std::string>& out,
              Clock::time_point now) {
        if (t.iterator.empty()) {
            KinesisReadFrom from;
            if (!t.last_sequence.empty()) {
                // A live read never needs this: it continues on the iterator the
                // last GetRecords returned. A restore or an expiry resumes AT the
                // last record handed downstream.
                t.resume_at_saved = true;
                from.kind = KinesisReadFrom::Kind::AtSequence;
                from.sequence_number = t.last_sequence;
            } else if (t.start_timestamp_ms) {
                from.kind = KinesisReadFrom::Kind::AtTimestamp;
                from.timestamp_ms = *t.start_timestamp_ms;
            }
            auto minted = svc.shard_iterator(id, from);
            if (!minted.error.ok()) {
                on_shard_error(id, t, KinesisCall::GetShardIterator, minted.error, now);
                return;
            }
            t.iterator = std::move(minted.iterator);
        }
        auto r = svc.get_records(t.iterator, opts.max_records_per_poll);
        if (!r.error.ok()) {
            on_shard_error(id, t, KinesisCall::GetRecords, r.error, now);
            return;
        }
        t.failures = 0;
        t.expiries = 0;
        std::size_t first = 0;
        if (t.resume_at_saved) {
            if (r.records.empty()) {
                if (r.next_iterator.empty()) {
                    fail_continuity(opts.name,
                                    id,
                                    "the shard ended without returning record " + t.last_sequence +
                                        ", the last one handed downstream: it has passed the "
                                        "stream's retention period.");
                }
                t.iterator = std::move(r.next_iterator);
                t.next_attempt = now + opts.read_spacing;
                return;
            }
            if (r.records.front().sequence_number != t.last_sequence) {
                fail_continuity(
                    opts.name,
                    id,
                    "resuming at record " + t.last_sequence +
                        ", the last one handed downstream, the stream returned record " +
                        r.records.front().sequence_number +
                        " first: the saved position has passed the stream's retention "
                        "period, and records after it may have too.");
            }
            first = 1;
            t.resume_at_saved = false;
        }
        if (r.millis_behind >= 0) {
            clink::metrics::connector::consumer_lag_set(kConnector, r.millis_behind);
        }
        Batch<std::string> batch;
        std::uint64_t bytes = 0;
        std::string last = t.last_sequence;
        for (std::size_t i = first; i < r.records.size(); ++i) {
            auto& rec = r.records[i];
            bytes += rec.data.size();
            last = std::move(rec.sequence_number);
            batch.emplace(std::move(rec.data));
        }
        const bool emitted = !batch.empty();
        if (emitted) {
            const auto n = batch.size();
            out.emit_data(std::move(batch));
            // Only now: a checkpoint never records a position past records the
            // runtime has not been handed.
            t.last_sequence = std::move(last);
            clink::metrics::connector::records_in_inc(kConnector, n);
            clink::metrics::connector::bytes_in_inc(kConnector, bytes);
            (void)CLINK_FAULT_POINT(kFaultAfterEmit);
        }
        t.iterator = std::move(r.next_iterator);
        if (t.iterator.empty()) {
            t.status = Status::Drained;
            clink::log::info(
                kLogComponent,
                who() + ": shard " + id + " read to its end" +
                    (t.last_sequence.empty() ? std::string{}
                                             : " (last record " + t.last_sequence + ")"));
            next_discovery = std::min(next_discovery,
                                      now + std::min(opts.discovery_interval, kRelistAfterDrain));
            promote_eligible();
            return;
        }
        t.next_attempt = now + (emitted ? opts.read_spacing : opts.poll_interval);
    }

    // The next readable shard after the last one polled, in ShardId order, and
    // the earliest moment anything is due when none is.
    std::pair<Shards::iterator, Clock::time_point> pick(Clock::time_point now) {
        auto chosen = shards.end();
        Clock::time_point earliest = next_discovery;
        const auto scan = [&](Shards::iterator from, Shards::iterator to) {
            for (auto it = from; it != to && chosen == shards.end(); ++it) {
                if (it->second.status != Status::Reading) {
                    continue;
                }
                if (it->second.next_attempt <= now) {
                    chosen = it;
                } else {
                    earliest = std::min(earliest, it->second.next_attempt);
                }
            }
        };
        const auto after = shards.upper_bound(last_polled);
        scan(after, shards.end());
        scan(shards.begin(), after);
        return {chosen, earliest};
    }
};

KinesisSource::KinesisSource(KinesisSourceOptions opts)
    : opts_(std::move(opts)), impl_(std::make_unique<Impl>(opts_, *this)) {
    const auto bad = [&](const std::string& why) {
        return std::runtime_error(opts_.name + ": " + why);
    };
    if (opts_.stream.empty()) {
        throw bad("'stream' is required");
    }
    // Fail fast on a bad initial_position rather than silently coercing a typo
    // (e.g. "lastest") to TRIM_HORIZON, which would replay the whole stream.
    if (opts_.initial_position != "trim_horizon" && opts_.initial_position != "latest") {
        throw bad("invalid initial_position '" + opts_.initial_position +
                  "' (expected 'trim_horizon' or 'latest')");
    }
    if (opts_.parallelism == 0 || opts_.subtask_idx >= opts_.parallelism) {
        throw bad("subtask " + std::to_string(opts_.subtask_idx) + " of parallelism " +
                  std::to_string(opts_.parallelism) +
                  " is not a valid subtask (the index must be below the parallelism)");
    }
    if (opts_.max_records_per_poll < 1 || opts_.max_records_per_poll > 10000) {
        throw bad("max_records_per_poll " + std::to_string(opts_.max_records_per_poll) +
                  " is outside the service's GetRecords limit of 1 to 10000");
    }
    if (opts_.poll_interval.count() < 1 || opts_.read_spacing.count() < 0) {
        throw bad("poll_interval_ms must be at least 1, and the read spacing not negative");
    }
    if (opts_.discovery_interval < kMinDiscoveryInterval) {
        throw bad("discovery_interval_ms must be at least " +
                  std::to_string(kMinDiscoveryInterval.count()));
    }
    if (opts_.retry_timeout.count() < 1 || opts_.max_backoff.count() < 1) {
        throw bad("retry_timeout_ms and the maximum backoff must be positive");
    }
    if (opts_.list_page_size < 0 || opts_.list_page_size > 10000) {
        throw bad("list_page_size must be between 0 (the service default) and 10000");
    }
}

KinesisSource::~KinesisSource() = default;

std::size_t KinesisSource::split_count() const noexcept {
    return impl_->shards.empty() ? 1 : impl_->shards.size();
}

std::uint64_t KinesisSource::listings() const noexcept {
    return impl_->listings;
}

std::vector<KinesisShardStatus> KinesisSource::shard_statuses() const {
    std::vector<KinesisShardStatus> out;
    out.reserve(impl_->shards.size());
    for (const auto& [id, t] : impl_->shards) {
        out.push_back(
            KinesisShardStatus{id, t.status, t.last_sequence, t.parent, t.adjacent_parent});
    }
    return out;
}

bool KinesisSource::restore_offset(StateBackend& backend, OperatorId op_id) {
    auto& im = *impl_;
    im.restored_subtasks.clear();
    im.restored_shards.clear();
    im.restored_legacy.clear();
    // Collect first, decode after: a row this version cannot read throws, and
    // the throw should not cross the backend's scan.
    std::vector<std::pair<std::string, std::string>> rows;
    backend.scan_operator_state(op_id,
                                [&](StateBackend::KeyView key, StateBackend::ValueView value) {
                                    if (starts_with(key, kKinesisShardRowPrefix) ||
                                        starts_with(key, kKinesisSubtaskRowPrefix) ||
                                        starts_with(key, kKinesisLegacyRowPrefix)) {
                                        rows.emplace_back(std::string{key}, std::string{value});
                                    }
                                });
    for (const auto& [key, value] : rows) {
        const std::string_view k{key};
        if (starts_with(k, kKinesisShardRowPrefix)) {
            im.restored_shards[std::string{k.substr(kKinesisShardRowPrefix.size())}] =
                decode_kinesis_shard_checkpoint(value);
        } else if (starts_with(k, kKinesisSubtaskRowPrefix)) {
            const auto idx = parse_int<std::uint32_t>(k.substr(kKinesisSubtaskRowPrefix.size()));
            if (!idx) {
                throw std::runtime_error(opts_.name + ": cannot read the checkpoint row '" + key +
                                         "': the subtask index is not a number");
            }
            im.restored_subtasks[*idx] = decode_kinesis_subtask_checkpoint(value);
        } else {
            im.restored_legacy[std::string{k.substr(kKinesisLegacyRowPrefix.size())}] = value;
        }
    }
    return !im.restored_subtasks.empty() || !im.restored_shards.empty() ||
           !im.restored_legacy.empty();
}

void KinesisSource::open() {
    auto& im = *impl_;
    {
        auto service = opts_.service;
        if (!service) {
            service = make_kinesis_sdk_service(opts_.stream, opts_.client, opts_.list_page_size);
        }
        std::lock_guard lock(im.mu);
        im.service = std::move(service);
    }
    im.shards.clear();
    const auto me = opts_.subtask_idx;
    const auto parallelism = opts_.parallelism;
    std::string start_note;

    if (!im.restored_subtasks.empty()) {
        // A checkpoint this format wrote. Every subtask's row names the stream and
        // the parallelism that wrote it; all of them must agree.
        std::optional<std::uint32_t> previous;
        std::int64_t start = 0;
        for (const auto& [idx, row] : im.restored_subtasks) {
            if (row.stream != opts_.stream) {
                throw std::runtime_error(
                    opts_.name + ": the checkpoint holds positions in stream '" + row.stream +
                    "', and this source reads '" + opts_.stream +
                    "'. Positions do not carry across streams; start the job afresh to read "
                    "another stream.");
            }
            if (previous && *previous != row.parallelism) {
                throw std::runtime_error(
                    opts_.name + ": the restored source state mixes rows written at parallelism " +
                    std::to_string(*previous) + " and " + std::to_string(row.parallelism) +
                    ", so it is not one checkpoint's state");
            }
            if (idx >= row.parallelism) {
                throw std::runtime_error(opts_.name + ": restored a row for subtask " +
                                         std::to_string(idx) + " of parallelism " +
                                         std::to_string(row.parallelism));
            }
            previous = row.parallelism;
            start = start == 0 ? row.start_ms : std::min(start, row.start_ms);
        }
        std::vector<std::uint32_t> missing;
        if (*previous == parallelism) {
            // Ownership is a function of the ShardId and the parallelism, so at an
            // unchanged parallelism this subtask owns what it owned, and its own
            // rows hold every position it needs.
            if (im.restored_subtasks.count(me) == 0) {
                missing.push_back(me);
            }
        } else {
            // At a new parallelism a shard's new owner can be any subtask, so
            // every subtask needs every previous subtask's rows.
            for (std::uint32_t i = 0; i < *previous; ++i) {
                if (im.restored_subtasks.count(i) == 0) {
                    missing.push_back(i);
                }
            }
        }
        if (!missing.empty()) {
            std::string list;
            for (const auto i : missing) {
                list += (list.empty() ? "" : ", ") + std::to_string(i);
            }
            fail_permanent(
                opts_.name + ": subtask " + std::to_string(me) + " of " +
                std::to_string(parallelism) + " is restoring source state written at parallelism " +
                std::to_string(*previous) + " without the rows of previous subtask(s) " + list +
                ". Each shard's position lives in the state of the subtask that read it, so "
                "restoring at another parallelism needs every previous subtask's rows at every "
                "new subtask. The file:// state backend hands them over; rocksdb:// and "
                "forst:// hand a new subtask only the previous subtasks mapped to it. Restore "
                "at parallelism " +
                std::to_string(*previous) + ", or rescale the source on file://.");
        }
        im.start_ms = start;
        for (const auto& [id, row] : im.restored_shards) {
            if (!im.owned(id)) {
                continue;  // another subtask's shard: its row is dropped at the next checkpoint
            }
            Impl::Tracked t;
            t.parent = row.parent;
            t.adjacent_parent = row.adjacent_parent;
            t.starting_sequence = row.starting_sequence;
            t.status = row.status;
            t.last_sequence = row.last_sequence;
            t.resume_at_saved = !row.last_sequence.empty();
            t.start_timestamp_ms = row.start_timestamp_ms;
            im.shards.emplace(id, std::move(t));
        }
        start_note = "recovery from a checkpoint written at parallelism " +
                     std::to_string(*previous) + " (" + std::to_string(im.shards.size()) +
                     " owned shard(s) restored)";
    } else if (!im.restored_legacy.empty()) {
        // An earlier release's rows: one last sequence number per shard, no
        // record of which subtask read which shard, no start time.
        if (parallelism > 1) {
            fail_permanent(
                opts_.name +
                ": this checkpoint was written by an earlier Kinesis source, which recorded "
                "positions without the subtask that held them. At parallelism " +
                std::to_string(parallelism) +
                " the source cannot tell whether this subtask received every position its "
                "shards need, so it refuses the restore rather than read a shard from the "
                "wrong place. Restore the checkpoint at parallelism 1, or start the job afresh.");
        }
        im.start_from_oldest = true;
        im.start_ms = wall_clock_ms();
        for (const auto& [id, seq] : im.restored_legacy) {
            Impl::Tracked t;
            t.status = Status::Reading;
            t.last_sequence = seq;
            t.resume_at_saved = !seq.empty();
            im.shards.emplace(id, std::move(t));
        }
        start_note = "recovery from an earlier release's checkpoint (" +
                     std::to_string(im.shards.size()) +
                     " position(s)); shards without one start at their oldest record";
    } else if (runtime() != nullptr && runtime()->restore_from_checkpoint_id() > 0) {
        // A restore with no source rows for this subtask: an earlier release's
        // checkpoint taken before any record, or a source new to the job. Its
        // start time is unknown, so no shard may start later than its oldest
        // record.
        im.start_from_oldest = true;
        im.start_ms = wall_clock_ms();
        start_note = "restoring checkpoint " +
                     std::to_string(runtime()->restore_from_checkpoint_id()) +
                     ", which holds no Kinesis position for this subtask: every shard starts at "
                     "its oldest record";
    } else {
        im.start_ms = wall_clock_ms();
        start_note = opts_.initial_position == "latest"
                         ? "fresh start: shards start at records arriving from " +
                               std::to_string(im.start_ms) + " ms (Unix time)"
                         : "fresh start: shards start at their oldest record";
    }
    im.restored_subtasks.clear();
    im.restored_shards.clear();
    im.restored_legacy.clear();
    im.next_discovery = Clock::time_point{};  // list at the first produce()
    im.listings = 0;

    clink::log::info(
        kLogComponent,
        im.who() + "/" + std::to_string(parallelism) + " on stream '" + opts_.stream +
            "': " + start_note +
            (parallelism > 1
                 ? "; above parallelism 1 a child shard whose parent another subtask owns is not "
                   "held back for it, so records of one key keep their order within a shard only"
                 : ""));
}

bool KinesisSource::produce(Emitter<std::string>& out) {
    auto& im = *impl_;
    if (this->cancelled()) {
        return false;
    }
    std::shared_ptr<KinesisShardService> service;
    {
        std::lock_guard lock(im.mu);
        service = im.service;
    }
    if (!service) {
        throw std::runtime_error(opts_.name + ": produce() before open()");
    }
    auto now = Clock::now();
    if (now >= im.next_discovery) {
        im.discover(*service);
        if (this->cancelled()) {
            return false;
        }
        now = Clock::now();
    }
    const auto [chosen, earliest] = im.pick(now);
    if (chosen == im.shards.end()) {
        im.wait_until(std::min(earliest, now + kMaxWaitSlice));
        return !this->cancelled();
    }
    im.last_polled = chosen->first;
    im.poll(*service, chosen->first, chosen->second, out, now);
    return !this->cancelled();
}

void KinesisSource::cancel() {
    Source<std::string>::cancel();
    std::shared_ptr<KinesisShardService> service;
    {
        std::lock_guard lock(impl_->mu);
        service = impl_->service;
    }
    impl_->wake.notify_all();
    if (service) {
        service->abort();
    }
}

void KinesisSource::close() {
    std::lock_guard lock(impl_->mu);
    impl_->service.reset();
}

void KinesisSource::snapshot_offset(StateBackend& backend,
                                    OperatorId op_id,
                                    CheckpointId /*ckpt_id*/) {
    // Runs on the source thread between produce() calls, so every position here
    // is of a record already handed downstream. One row per owned shard plus
    // this subtask's row; every other row of ours in this backend (another
    // subtask's restored by a union, an earlier format's, a forgotten shard's)
    // is erased, so a later restore cannot take a stale copy for a live one.
    auto& im = *impl_;
    std::set<std::string, std::less<>> keep;
    const auto put = [&](std::string key, const std::string& value) {
        backend.put_operator_state(op_id, key, StateBackend::ValueView{value.data(), value.size()});
        keep.insert(std::move(key));
    };
    KinesisSubtaskCheckpoint mine;
    mine.parallelism = opts_.parallelism;
    mine.initial_position = opts_.initial_position;
    mine.start_ms = im.start_ms;
    mine.stream = opts_.stream;
    put(std::string{kKinesisSubtaskRowPrefix} + std::to_string(opts_.subtask_idx),
        encode_kinesis_subtask_checkpoint(mine));
    for (const auto& [id, t] : im.shards) {
        KinesisShardCheckpoint row;
        row.status = t.status;
        row.last_sequence = t.last_sequence;
        row.parent = t.parent;
        row.adjacent_parent = t.adjacent_parent;
        row.starting_sequence = t.starting_sequence;
        row.start_timestamp_ms = t.start_timestamp_ms;
        put(std::string{kKinesisShardRowPrefix} + id, encode_kinesis_shard_checkpoint(row));
    }
    std::vector<std::string> stale;
    backend.scan_operator_state(op_id, [&](StateBackend::KeyView key, StateBackend::ValueView) {
        if ((starts_with(key, kKinesisShardRowPrefix) ||
             starts_with(key, kKinesisSubtaskRowPrefix) ||
             starts_with(key, kKinesisLegacyRowPrefix)) &&
            keep.find(key) == keep.end()) {
            stale.emplace_back(key);
        }
    });
    for (const auto& key : stale) {
        backend.erase_operator_state(op_id, StateBackend::KeyView{key.data(), key.size()});
    }
}

}  // namespace clink::aws
