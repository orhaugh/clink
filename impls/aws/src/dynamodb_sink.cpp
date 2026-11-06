// The DynamoDB sink: options, the table's key schema, buffering by item
// identity, the BatchWriteItem and UnprocessedItems loop, the retry budget, and
// the failed state that keeps a sink which dropped writes from acknowledging a
// later barrier. The service calls are behind DynamoDbWriteService.

#include "clink/aws/dynamodb_sink.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <utility>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/logging.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace clink::aws {

namespace {

constexpr char kLogComponent[] = "dynamodb.sink";
// How often a retry pause looks at the task's cancel signal.
constexpr std::chrono::milliseconds kCancelPoll{50};
// A request's JSON besides its items: {"RequestItems":{"<table>":[...]}} and
// headroom. The table name is charged separately.
constexpr std::size_t kRequestEnvelopeBytes = 64 * 1024;

// The code without the protocol prefix some services add
// ("com.amazonaws.dynamodb.v20120810#ResourceNotFoundException").
std::string bare_error_code(std::string_view code) {
    if (const auto hash = code.rfind('#'); hash != std::string_view::npos) {
        code.remove_prefix(hash + 1);
    }
    if (const auto colon = code.find(':'); colon != std::string_view::npos) {
        code = code.substr(0, colon);
    }
    return std::string{code};
}

// A service message with anything that identifies credentials removed: access
// key ids (AKIA..., ASIA...) and the values after Credential=, Signature= and
// X-Amz-Security-Token=. Error text reaches logs and job failure reports.
std::string without_credentials(std::string_view message) {
    std::string out;
    out.reserve(message.size());
    const auto upper_alnum = [](char c) {
        return (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
    };
    std::size_t i = 0;
    while (i < message.size()) {
        bool replaced = false;
        for (const std::string_view marker : {std::string_view{"Credential="},
                                              std::string_view{"Signature="},
                                              std::string_view{"X-Amz-Security-Token="}}) {
            if (message.substr(i, marker.size()) == marker) {
                out.append(marker);
                out += "<removed>";
                i += marker.size();
                while (i < message.size() && message[i] != ',' && message[i] != ' ' &&
                       message[i] != '&' && message[i] != '\'' && message[i] != '"') {
                    ++i;
                }
                replaced = true;
                break;
            }
        }
        if (replaced) {
            continue;
        }
        const auto rest = message.substr(i);
        if ((rest.substr(0, 4) == "AKIA" || rest.substr(0, 4) == "ASIA") && rest.size() >= 20 &&
            std::all_of(rest.begin() + 4, rest.begin() + 20, upper_alnum) &&
            (i == 0 || !upper_alnum(message[i - 1]))) {
            out += "<access key id>";
            i += 20;
            continue;
        }
        out.push_back(message[i++]);
    }
    return out;
}

// What to check for an error the sink does not retry, by code.
std::string permanent_hint(const std::string& code) {
    if (code == "ResourceNotFoundException") {
        return " Check that the table exists in the configured region (and at endpoint_override, "
               "if set) and is ACTIVE.";
    }
    if (code.find("AccessDenied") != std::string::npos || code == "UnrecognizedClientException" ||
        code == "InvalidSignatureException" || code == "SignatureDoesNotMatch" ||
        code.find("MissingAuthenticationToken") != std::string::npos ||
        code.find("IncompleteSignature") != std::string::npos || code == "InvalidClientTokenId" ||
        code.find("ExpiredToken") != std::string::npos) {
        return " Check the credentials the AWS SDK's default chain finds (environment, profile, "
               "container or instance role): the sink needs dynamodb:DescribeTable and "
               "dynamodb:BatchWriteItem on the table.";
    }
    if (code == "ValidationException") {
        return " The sink checks each record against DynamoDB's documented rules before sending "
               "it, so a refusal it did not foresee may be a defect in the sink; the service's "
               "message names the rule.";
    }
    return "";
}

std::string error_text(const DynamoDbCallError& e) {
    std::string out = e.code;
    if (e.http_status > 0) {
        out += " (HTTP " + std::to_string(e.http_status) + ")";
    }
    if (!e.message.empty()) {
        out += ": " + without_credentials(e.message);
    }
    return out;
}

// Ends the call in flight on a DynamoDbWriteService once the task that owns the
// sink is cancelled, so a cancelled task stops at once rather than waiting out
// the SDK's request timeout. One thread per sink, asleep unless a call is armed.
class CallWatch {
public:
    explicit CallWatch(std::shared_ptr<DynamoDbWriteService> service)
        : service_(std::move(service)), thread_([this] { run(); }) {}

    ~CallWatch() {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        thread_.join();
    }

    CallWatch(const CallWatch&) = delete;
    CallWatch& operator=(const CallWatch&) = delete;
    CallWatch(CallWatch&&) = delete;
    CallWatch& operator=(CallWatch&&) = delete;

    void arm(CancelSignal cancel) {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            armed_ = true;
            cancel_ = std::move(cancel);
        }
        cv_.notify_all();
    }

    // True when the watch aborted the call armed last.
    bool disarm() {
        const std::lock_guard<std::mutex> lock(mu_);
        armed_ = false;
        return aborted_;
    }

private:
    void run() {
        std::unique_lock<std::mutex> lock(mu_);
        while (!stop_) {
            if (!armed_ || aborted_) {
                cv_.wait(lock, [this] { return stop_ || (armed_ && !aborted_); });
                continue;
            }
            cv_.wait_for(lock, std::chrono::milliseconds{20});
            if (stop_ || !armed_ || !cancel_.requested()) {
                continue;
            }
            aborted_ = true;
            lock.unlock();
            service_->abort();
            lock.lock();
        }
    }

    std::shared_ptr<DynamoDbWriteService> service_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool stop_{false};
    bool armed_{false};
    bool aborted_{false};
    CancelSignal cancel_;
    std::thread thread_;
};

bool valid_table_name(const std::string& t) {
    if (t.rfind("arn:", 0) == 0) {
        return t.size() <= 1024;
    }
    return t.size() >= 3 && t.size() <= 255 && std::all_of(t.begin(), t.end(), [](char c) {
               return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
                      c == '_' || c == '-' || c == '.';
           });
}

// An endpoint must be an http(s) URL naming a host, with no user information:
// a URL that carried credentials would put them in every message that names
// the endpoint.
bool valid_endpoint(const std::string& e) {
    std::string_view rest{e};
    if (rest.rfind("https://", 0) == 0) {
        rest.remove_prefix(8);
    } else if (rest.rfind("http://", 0) == 0) {
        rest.remove_prefix(7);
    } else {
        return false;
    }
    const auto authority = rest.substr(0, rest.find_first_of("/?#"));
    return !authority.empty() && authority.find('@') == std::string_view::npos &&
           authority.find_first_of(" \t\r\n") == std::string_view::npos;
}

// Why one BatchWriteItem's items could not be written, and what to check;
// flush() adds what the flush had written and fails the sink.
class WriteFailure : public std::runtime_error {
public:
    explicit WriteFailure(const std::string& why, std::string hint = {})
        : std::runtime_error(why), hint_(std::move(hint)) {}
    [[nodiscard]] const std::string& hint() const noexcept { return hint_; }

private:
    std::string hint_;
};

std::chrono::milliseconds backoff_delay(int round,
                                        std::chrono::milliseconds base,
                                        std::chrono::milliseconds cap) {
    auto d = base;
    for (int i = 1; i < round && d < cap; ++i) {
        d *= 2;
    }
    return std::min(d, cap);
}

}  // namespace

DynamoDbFault classify_dynamodb_error(std::string_view raw_code,
                                      int http_status,
                                      bool sdk_retryable) {
    // From DynamoDB's error table ("OK to retry?") and the common AWS errors.
    // UnrecognizedClientException is listed as retryable there; the sink fails
    // at once instead, since retrying with the same credentials rarely helps
    // and the job's restart opens the sink again.
    static const std::set<std::string, std::less<>> kThrottled = {
        "ProvisionedThroughputExceededException",
        "ThrottlingException",
        "Throttling",
        "RequestLimitExceeded",
        "TooManyRequestsException",
        "LimitExceededException",
    };
    static const std::set<std::string, std::less<>> kPermanent = {
        "ValidationException",
        "ResourceNotFoundException",
        "ResourceInUseException",
        "ConditionalCheckFailedException",
        "SerializationException",
        "UnknownOperationException",
        "AccessDeniedException",
        "AccessDenied",
        "UnrecognizedClientException",
        "InvalidSignatureException",
        "SignatureDoesNotMatch",
        "IncompleteSignature",
        "IncompleteSignatureException",
        "MissingAuthenticationToken",
        "MissingAuthenticationTokenException",
        "InvalidClientTokenId",
        "InvalidAccessKeyId",
        "ExpiredToken",
        "ExpiredTokenException",
        "RequestExpired",
        "OptInRequired",
        "KMSAccessDeniedException",
        "KMSDisabledException",
        "KMSInvalidStateException",
        "KMSNotFoundException",
    };
    static const std::set<std::string, std::less<>> kTransient = {
        "InternalServerError",
        "InternalFailure",
        "InternalFailureException",
        "ServiceUnavailable",
        "ServiceUnavailableException",
        "ReplicatedWriteConflictException",
        "ItemCollectionSizeLimitExceededException",
        "TransactionInProgressException",
        "RequestTimeout",
        "RequestTimeoutException",
    };
    const std::string code = bare_error_code(raw_code);
    if (kThrottled.count(code) != 0 || http_status == 429) {
        return DynamoDbFault::Throttled;
    }
    if (kPermanent.count(code) != 0) {
        return DynamoDbFault::Permanent;
    }
    if (kTransient.count(code) != 0 || http_status == 0 || http_status >= 500) {
        return DynamoDbFault::Transient;
    }
    return sdk_retryable ? DynamoDbFault::Transient : DynamoDbFault::Permanent;
}

struct DynamoDbSink::Impl {
    explicit Impl(DynamoDbSinkOptions o)
        : opts(std::move(o)),
          before_write_point(opts.name + ".before_write"),
          after_write_point(opts.name + ".after_write") {}

    struct Entry {
        DynamoDbItemKey key;
        DynamoDbItem item;
        std::size_t request_bytes{0};
    };

    DynamoDbSinkOptions opts;
    std::shared_ptr<DynamoDbWriteService> service;
    std::optional<DynamoDbKeySchema> schema;
    // The table as the request names it, and as DescribeTable named it.
    std::set<std::string> table_names;
    std::vector<Entry> pending;
    std::map<DynamoDbItemKey, std::size_t> index;  // key -> position in pending
    std::chrono::steady_clock::time_point first_buffered_at{};
    std::optional<std::string> failure;  // why this instance stopped
    std::string before_write_point;
    std::string after_write_point;
    std::unique_ptr<CallWatch> watch;

    // Stops this instance: drops what is buffered, refuses every later write
    // and barrier, and throws `why` with how many buffered items were dropped
    // and, last, what to check.
    [[noreturn]] void fail_now(std::string why, std::size_t dropped, const std::string& hint = {}) {
        if (dropped > 0) {
            why += "; " + std::to_string(dropped) +
                   " buffered item(s) not yet written are dropped with this sink instance";
        }
        why +=
            ". The sink accepts nothing more, and the job's restart replays every record after "
            "the last completed checkpoint." +
            hint;
        pending.clear();
        index.clear();
        failure = why;
        clink::metrics::connector::error_inc("dynamodb", "sink");
        clink::log::error(kLogComponent, opts.name + ": " + why);
        throw std::runtime_error(opts.name + ": " + why);
    }

    void check_usable() const {
        if (failure) {
            throw std::runtime_error(opts.name + ": the sink failed earlier and dropped writes, " +
                                     "so it refuses to write or acknowledge a barrier (" +
                                     *failure + ")");
        }
        if (!schema) {
            throw std::runtime_error(opts.name + ": open() has not read the table's key schema");
        }
    }

    // The pause before a retry, cut short when the task is cancelled.
    void pause(RuntimeContext* rt, std::chrono::milliseconds delay, const std::string& doing) {
        const auto until = std::chrono::steady_clock::now() + delay;
        for (;;) {
            if (rt != nullptr && rt->cancel_requested()) {
                throw std::runtime_error("cancelled while waiting to retry " + doing);
            }
            const auto now = std::chrono::steady_clock::now();
            if (now >= until) {
                return;
            }
            std::this_thread::sleep_for(
                std::min<std::chrono::steady_clock::duration>(kCancelPoll, until - now));
        }
    }

    void arm(RuntimeContext* rt) {
        if (rt == nullptr) {
            return;
        }
        if (!watch) {
            watch = std::make_unique<CallWatch>(service);
        }
        watch->arm(rt->cancel_signal());
    }

    bool disarm() { return watch && watch->disarm(); }

    void open(RuntimeContext* rt) {
        failure.reset();
        pending.clear();
        index.clear();
        schema.reset();
        watch.reset();
        service = opts.service ? opts.service : make_dynamodb_sdk_service(opts.client);
        const auto deadline = std::chrono::steady_clock::now() + opts.retry_timeout;
        int idle = 0;
        std::string last;
        DynamoDbTableDescription d;
        for (;;) {
            if (idle > 0) {
                const auto delay = backoff_delay(idle, opts.retry_base_backoff, opts.max_backoff);
                if (idle > opts.max_retries ||
                    std::chrono::steady_clock::now() + delay > deadline) {
                    throw std::runtime_error(opts.name + ": DescribeTable on table '" + opts.table +
                                             "' did not succeed after " + std::to_string(idle) +
                                             " attempt(s) (" + last + ")");
                }
                pause(rt, delay, "DescribeTable");
            }
            arm(rt);
            d = service->describe_table(opts.table);
            const bool aborted = disarm();
            if (aborted || d.error.fault == DynamoDbFault::Cancelled) {
                throw std::runtime_error(opts.name + ": cancelled during DescribeTable");
            }
            if (!d.error.ok()) {
                if (d.error.fault == DynamoDbFault::Permanent) {
                    throw std::runtime_error(opts.name + ": DescribeTable on table '" + opts.table +
                                             "' failed: " + error_text(d.error) + "." +
                                             permanent_hint(bare_error_code(d.error.code)));
                }
                ++idle;
                last = error_text(d.error);
                continue;
            }
            if (d.status == "CREATING") {
                ++idle;
                last = "the table is CREATING";
                continue;
            }
            if (d.status != "ACTIVE" && d.status != "UPDATING") {
                throw std::runtime_error(opts.name + ": table '" + opts.table + "' is " + d.status +
                                         "; the sink writes to an ACTIVE table");
            }
            break;
        }
        check_schema(d.key);
        schema = d.key;
        table_names = {opts.table};
        for (const auto* n : {&d.name, &d.arn}) {
            if (!n->empty()) {
                table_names.insert(*n);
            }
        }
        clink::log::info(kLogComponent,
                         opts.name + ": table '" + opts.table + "' has partition key '" +
                             d.key.partition.name + "'" +
                             (d.key.sort ? " and sort key '" + d.key.sort->name + "'" : "") +
                             "; writing up to " + std::to_string(opts.batch_records) +
                             " items per BatchWriteItem");
    }

    void check_schema(const DynamoDbKeySchema& k) const {
        const auto type_of = [](DynamoDbKeyType t) {
            return t == DynamoDbKeyType::S ? "S" : t == DynamoDbKeyType::N ? "N" : "B";
        };
        const std::string table = "table '" + opts.table + "'";
        if (k.partition.name != opts.partition_key) {
            throw std::runtime_error(opts.name + ": " + table + " has partition key '" +
                                     k.partition.name + "' (" + type_of(k.partition.type) +
                                     "); the sink was given partition_key='" + opts.partition_key +
                                     "'");
        }
        if (!k.sort && !opts.sort_key.empty()) {
            throw std::runtime_error(opts.name + ": " + table +
                                     " has no sort key; the sink was given sort_key='" +
                                     opts.sort_key + "'");
        }
        if (k.sort && opts.sort_key.empty()) {
            throw std::runtime_error(opts.name + ": " + table + " has sort key '" + k.sort->name +
                                     "' (" + type_of(k.sort->type) + "); set sort_key='" +
                                     k.sort->name + "'");
        }
        if (k.sort && k.sort->name != opts.sort_key) {
            throw std::runtime_error(opts.name + ": " + table + " has sort key '" + k.sort->name +
                                     "' (" + type_of(k.sort->type) +
                                     "); the sink was given sort_key='" + opts.sort_key + "'");
        }
        for (const auto* a : {&k.partition, k.sort ? &*k.sort : nullptr}) {
            if (a != nullptr && a->type == DynamoDbKeyType::B) {
                throw std::runtime_error(opts.name + ": " + table + "'s key attribute '" + a->name +
                                         "' is binary (B); the sink maps JSON records, which carry "
                                         "no binary type, and supports S and N keys");
            }
        }
    }

    void ingest(DynamoDbPreparedItem&& p) {
        const auto it = index.find(p.key);
        if (it != index.end()) {
            // The same item again: the later record is the one DynamoDB keeps.
            auto& e = pending[it->second];
            e.item = std::move(p.item);
            e.request_bytes = p.request_bytes;
            return;
        }
        if (pending.empty()) {
            first_buffered_at = std::chrono::steady_clock::now();
        }
        index.emplace(p.key, pending.size());
        pending.push_back(Entry{std::move(p.key), std::move(p.item), p.request_bytes});
    }

    bool linger_elapsed() const {
        return opts.max_age.count() > 0 && !pending.empty() &&
               std::chrono::steady_clock::now() - first_buffered_at >= opts.max_age;
    }

    [[noreturn]] static void ambiguous(std::size_t sent, const std::string& why) {
        throw WriteFailure("BatchWriteItem's response does not match its request: it " + why +
                           ". Whether the request's " + std::to_string(sent) +
                           " item(s) were written is unknown, so the flush fails rather than count "
                           "them written");
    }

    // Matches a response's UnprocessedItems to the request. Returns the
    // positions in `pending` still to write; any entry that is not one of the
    // request's own puts makes the response ambiguous.
    std::vector<std::size_t> unprocessed_of(const std::vector<DynamoDbUnprocessed>& unprocessed,
                                            const std::vector<std::size_t>& sent) {
        std::map<DynamoDbItemKey, std::size_t> by_key;
        for (const auto i : sent) {
            by_key.emplace(pending[i].key, i);
        }
        std::set<DynamoDbItemKey> seen;
        std::vector<std::size_t> remaining;
        for (const auto& u : unprocessed) {
            if (table_names.count(u.table) == 0) {
                ambiguous(sent.size(),
                          "lists unprocessed writes for table '" + u.table +
                              "', which the request did not write");
            }
            if (u.kind == DynamoDbUnprocessed::Kind::Delete) {
                ambiguous(sent.size(), "returns a DeleteRequest, and the request sent only puts");
            }
            if (u.kind == DynamoDbUnprocessed::Kind::Neither) {
                ambiguous(sent.size(), "returns an entry with neither a put nor a delete");
            }
            if (!u.recognised) {
                ambiguous(sent.size(), "returns a value of a type the sink never writes");
            }
            const auto key = dynamodb_item_key(u.item, *schema);
            if (!key) {
                ambiguous(sent.size(), "returns an item without a valid key for the table");
            }
            const auto it = by_key.find(*key);
            if (it == by_key.end()) {
                ambiguous(sent.size(),
                          "returns item " + describe_dynamodb_key(*key) +
                              ", which the request did not carry");
            }
            if (!seen.insert(*key).second) {
                ambiguous(sent.size(), "returns item " + describe_dynamodb_key(*key) + " twice");
            }
            remaining.push_back(it->second);
        }
        std::sort(remaining.begin(), remaining.end());
        return remaining;
    }

    // Writes pending[first, last) in one BatchWriteItem and its retries.
    void write_chunk(RuntimeContext* rt, std::size_t first, std::size_t last) {
        std::vector<std::size_t> outstanding;
        for (auto i = first; i < last; ++i) {
            outstanding.push_back(i);
        }
        const auto started = std::chrono::steady_clock::now();
        const auto deadline = started + opts.retry_timeout;
        int idle = 0;  // attempts in a row that wrote nothing
        int attempts = 0;
        std::string last_problem;
        for (;;) {
            if (attempts > 0) {
                if (idle > opts.max_retries) {
                    throw WriteFailure(
                        std::to_string(outstanding.size()) + " of " + std::to_string(last - first) +
                        " item(s) of a BatchWriteItem were still unwritten after " +
                        std::to_string(idle) +
                        " attempt(s) in a row that wrote nothing (max_retries=" +
                        std::to_string(opts.max_retries) + "; last: " + last_problem + ")");
                }
                const auto delay =
                    backoff_delay(std::max(idle, 1), opts.retry_base_backoff, opts.max_backoff);
                if (std::chrono::steady_clock::now() + delay > deadline) {
                    throw WriteFailure(
                        std::to_string(outstanding.size()) + " of " + std::to_string(last - first) +
                        " item(s) of a BatchWriteItem were still unwritten when the "
                        "retry budget of " +
                        std::to_string(opts.retry_timeout.count()) + " ms ran out (" +
                        std::to_string(attempts) + " attempt(s); last: " + last_problem + ")");
                }
                if ((attempts & (attempts - 1)) == 0) {
                    clink::log::warn(kLogComponent,
                                     opts.name + ": " + std::to_string(outstanding.size()) +
                                         " item(s) unwritten (" + last_problem + "); attempt " +
                                         std::to_string(attempts + 1) + " in " +
                                         std::to_string(delay.count()) + " ms");
                }
                try {
                    pause(rt, delay, "a BatchWriteItem");
                } catch (const std::runtime_error& e) {
                    throw WriteFailure(std::string{e.what()} + "; " +
                                       std::to_string(outstanding.size()) +
                                       " item(s) of it unwritten");
                }
            }
            ++attempts;
            (void)CLINK_FAULT_POINT(before_write_point);
            std::vector<const DynamoDbItem*> items;
            items.reserve(outstanding.size());
            for (const auto i : outstanding) {
                items.push_back(&pending[i].item);
            }
            arm(rt);
            auto result = service->batch_write(opts.table, items);
            const bool aborted = disarm();
            if (aborted || result.error.fault == DynamoDbFault::Cancelled) {
                throw WriteFailure("cancelled while a BatchWriteItem of " +
                                   std::to_string(outstanding.size()) +
                                   " item(s) was in flight; whether they were written is unknown");
            }
            if (!result.error.ok()) {
                if (result.error.fault == DynamoDbFault::Permanent) {
                    throw WriteFailure("BatchWriteItem was refused: " + error_text(result.error),
                                       permanent_hint(bare_error_code(result.error.code)));
                }
                // Throttled: nothing was written. Transient: some or all of it
                // may have been; sending the same puts again rewrites the same
                // values, and nothing later for these items has been sent.
                ++idle;
                last_problem = error_text(result.error);
                continue;
            }
            auto remaining = unprocessed_of(result.unprocessed, outstanding);
            if (remaining.empty()) {
                (void)CLINK_FAULT_POINT(after_write_point);
                return;
            }
            idle = remaining.size() < outstanding.size() ? 0 : idle + 1;
            last_problem = std::to_string(remaining.size()) + " of " +
                           std::to_string(outstanding.size()) + " returned as UnprocessedItems";
            outstanding = std::move(remaining);
        }
    }

    void flush(RuntimeContext* rt) {
        check_usable();
        if (pending.empty()) {
            return;
        }
        const auto n = pending.size();
        const auto t0 = std::chrono::steady_clock::now();
        const std::size_t budget =
            kDynamoDbMaxRequestBytes - kRequestEnvelopeBytes - 6 * opts.table.size();
        std::size_t written = 0;
        std::size_t first = 0;
        while (first < n) {
            std::size_t last = first;
            std::size_t bytes = 0;
            while (last < n && last - first < opts.batch_records &&
                   (last == first || bytes + pending[last].request_bytes <= budget)) {
                bytes += pending[last].request_bytes;
                ++last;
            }
            try {
                write_chunk(rt, first, last);
            } catch (const WriteFailure& e) {
                fail_now(std::string{e.what()} + " (this flush had written " +
                             std::to_string(written) + " of its " + std::to_string(n) +
                             " buffered item(s))",
                         n - written,
                         e.hint());
            }
            written += last - first;
            first = last;
        }
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::records_out_inc("dynamodb", n);
        clink::metrics::connector::commit_latency_observe("dynamodb",
                                                          static_cast<std::uint64_t>(dt));
        pending.clear();
        index.clear();
    }
};

DynamoDbSink::DynamoDbSink(DynamoDbSinkOptions opts) {
    const auto refuse = [&](const std::string& why) {
        throw std::runtime_error(opts.name + ": " + why);
    };
    if (opts.table.empty()) {
        refuse("'table' is required");
    }
    if (!valid_table_name(opts.table)) {
        refuse(
            "'table' must be a table name of 3 to 255 characters from a-z, A-Z, 0-9, '_', '-' "
            "and '.', or a table ARN");
    }
    if (opts.partition_key.empty()) {
        refuse("'partition_key' is required");
    }
    if (!opts.sort_key.empty() && opts.sort_key == opts.partition_key) {
        refuse("'sort_key' and 'partition_key' name the same attribute");
    }
    if (opts.batch_records < 1 || opts.batch_records > kDynamoDbMaxBatchWrites) {
        refuse("'batch_records' must be 1 to 25 (BatchWriteItem takes at most 25 writes); it is " +
               std::to_string(opts.batch_records));
    }
    if (opts.max_retries < 0 || opts.max_retries > kMaxRetries) {
        refuse("'max_retries' must be 0 to " + std::to_string(kMaxRetries) + "; it is " +
               std::to_string(opts.max_retries));
    }
    if (opts.retry_base_backoff.count() <= 0 || opts.max_backoff < opts.retry_base_backoff) {
        refuse("the retry backoff must be positive, and its cap no smaller than its base");
    }
    if (opts.retry_timeout.count() <= 0) {
        refuse("'retry_timeout_ms' must be positive");
    }
    if (opts.max_age.count() < 0) {
        refuse("'linger_ms' must not be negative");
    }
    if (opts.client.endpoint_override && !valid_endpoint(*opts.client.endpoint_override)) {
        // The value is not repeated: it may hold credentials.
        refuse(
            "'endpoint_override' must be an http:// or https:// URL naming a host, without user "
            "information");
    }
    if (opts.client.region && opts.client.region->empty()) {
        refuse("'region' is empty");
    }
    if (opts.client.connect_timeout_ms <= 0 || opts.client.connect_timeout_ms > 600000 ||
        opts.client.request_timeout_ms <= 0 || opts.client.request_timeout_ms > 600000) {
        refuse("'connect_timeout_ms' and 'request_timeout_ms' must be 1 to 600000");
    }
    impl_ = std::make_unique<Impl>(std::move(opts));
}

DynamoDbSink::~DynamoDbSink() = default;

void DynamoDbSink::open() {
    impl_->open(runtime());
}

void DynamoDbSink::close() {
    // Nothing is written here: a clean end of input has already flushed, and a
    // failed or cancelled task must not write what it was handed.
    impl_->watch.reset();
}

void DynamoDbSink::on_data(const Batch<std::string>& batch) {
    impl_->check_usable();
    auto* rt = runtime();
    std::vector<DynamoDbPreparedItem> prepared;
    prepared.reserve(batch.size());
    std::size_t i = 0;
    for (const auto& rec : batch) {
        try {
            prepared.push_back(prepare_dynamodb_item(rec.value(), *impl_->schema));
        } catch (const DynamoDbRecordError& e) {
            impl_->fail_now("record " + std::to_string(i + 1) + " of a batch of " +
                                std::to_string(batch.size()) + " is refused: " + e.what() +
                                ". Nothing of that batch was buffered or written",
                            impl_->pending.size());
        }
        ++i;
    }
    for (auto& p : prepared) {
        impl_->ingest(std::move(p));
        if (impl_->pending.size() >= impl_->opts.batch_records || impl_->linger_elapsed()) {
            impl_->flush(rt);
        }
    }
}

void DynamoDbSink::on_barrier(CheckpointBarrier /*b*/) {
    impl_->flush(runtime());
}

void DynamoDbSink::flush() {
    impl_->flush(runtime());
}

std::string DynamoDbSink::name() const {
    return impl_->opts.name;
}

std::size_t DynamoDbSink::pending_items() const {
    return impl_->pending.size();
}

bool DynamoDbSink::failed() const {
    return impl_->failure.has_value();
}

}  // namespace clink::aws
