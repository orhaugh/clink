// The Firehose sink: options, the record and request limits, the buffer, the
// PutRecordBatch loop with its response judge and retry budget, and the failed
// state that keeps a sink which dropped records from acknowledging a later
// barrier. The service calls are behind FirehoseWriteService.

#include "clink/aws/firehose_sink.hpp"

#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <optional>
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

constexpr char kLogComponent[] = "firehose.sink";
// How often a retry pause looks at the task's cancel signal.
constexpr std::chrono::milliseconds kCancelPoll{50};

// The code without the protocol prefix some services add.
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
        return " Check that the delivery stream exists in the configured region (and at "
               "endpoint_override, if set) and takes direct PUTs.";
    }
    if (code.find("AccessDenied") != std::string::npos || code == "UnrecognizedClientException" ||
        code == "InvalidSignatureException" || code == "SignatureDoesNotMatch" ||
        code.find("MissingAuthenticationToken") != std::string::npos ||
        code.find("IncompleteSignature") != std::string::npos || code == "InvalidClientTokenId" ||
        code.find("ExpiredToken") != std::string::npos) {
        return " Check the credentials the AWS SDK's default chain finds (environment, profile, "
               "container or instance role): the sink needs firehose:PutRecordBatch on the "
               "stream.";
    }
    if (code == "InvalidKMSResourceException") {
        return " The stream's server-side encryption key is not usable (disabled, missing or not "
               "accessible).";
    }
    if (code == "InvalidArgumentException") {
        return " The sink checks every record and request against Firehose's documented limits "
               "before sending it, so a refusal it did not foresee may be a defect in the sink; "
               "the service's message names the rule.";
    }
    return "";
}

std::string error_text(const FirehoseCallError& e) {
    std::string out = e.code;
    if (e.http_status > 0) {
        out += " (HTTP " + std::to_string(e.http_status) + ")";
    }
    if (!e.message.empty()) {
        out += ": " + without_credentials(e.message);
    }
    return out;
}

// Ends the call in flight on a FirehoseWriteService once the task that owns the
// sink is cancelled, so a cancelled task stops at once rather than waiting out
// the SDK's request timeout. One thread per sink, asleep unless a call is armed.
class CallWatch {
public:
    explicit CallWatch(std::shared_ptr<FirehoseWriteService> service)
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

    std::shared_ptr<FirehoseWriteService> service_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool stop_{false};
    bool armed_{false};
    bool aborted_{false};
    CancelSignal cancel_;
    std::thread thread_;
};

// Why one request's records could not be accepted, and what to check; flush()
// adds what it had sent and fails the sink.
class SendFailure : public std::runtime_error {
public:
    explicit SendFailure(const std::string& why, std::string hint = {})
        : std::runtime_error(why), hint_(std::move(hint)) {}
    [[nodiscard]] const std::string& hint() const noexcept { return hint_; }

private:
    std::string hint_;
};

bool valid_stream_name(const std::string& s) {
    return !s.empty() && s.size() <= 64 && std::all_of(s.begin(), s.end(), [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
               c == '_' || c == '.' || c == '-';
    });
}

// An endpoint must be an http(s) URL naming a host, with no user information:
// a URL that carried credentials would put them in every message naming it.
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

std::chrono::milliseconds backoff_delay(int round,
                                        std::chrono::milliseconds base,
                                        std::chrono::milliseconds cap) {
    auto d = base;
    for (int i = 1; i < round && d < cap; ++i) {
        d *= 2;
    }
    return std::min(d, cap);
}

std::string with_thousands(std::size_t n) {
    std::string digits = std::to_string(n);
    for (auto i = static_cast<std::ptrdiff_t>(digits.size()) - 3; i > 0; i -= 3) {
        digits.insert(static_cast<std::size_t>(i), ",");
    }
    return digits;
}

}  // namespace

// ---- limits -------------------------------------------------------------------------

std::size_t firehose_record_request_bytes(std::size_t data_bytes) {
    // The SDK writes a request's records indented, each as
    //   {\n\t\t\t"Data":\t"<base64>"\n\t\t}
    // with ", " before the next: 4 base64 characters per 3 bytes, rounded up,
    // and 32 bytes of JSON and whitespace, a few more than it uses.
    return 4 * ((data_bytes + 2) / 3) + 32;
}

std::size_t firehose_request_envelope_bytes(std::size_t name_bytes) {
    // {\n\t"DeliveryStreamName":\t"<name>",\n\t"Records":\t[...]\n}, with room.
    return 64 + name_bytes;
}

std::size_t firehose_min_request_bytes(std::size_t name_bytes) {
    return firehose_request_envelope_bytes(name_bytes) +
           firehose_record_request_bytes(kFirehoseMaxRecordBytes);
}

// ---- responses ----------------------------------------------------------------------

FirehoseVerdict judge_firehose_response(std::size_t sent, const FirehoseBatchResult& result) {
    FirehoseVerdict v;
    if (result.entries.size() != sent) {
        v.why = "returned " + std::to_string(result.entries.size()) + " entries for " +
                std::to_string(sent) + " records";
        return v;
    }
    for (std::size_t i = 0; i < sent; ++i) {
        const auto& e = result.entries[i];
        const bool accepted = !e.record_id.empty();
        const bool failed = !e.error_code.empty();
        if (accepted == failed) {
            v.why = "entry " + std::to_string(i) + " holds " +
                    (accepted ? std::string{"both a RecordId and an ErrorCode"}
                              : std::string{"neither a RecordId nor an ErrorCode"});
            v.failed.clear();
            return v;
        }
        if (failed) {
            v.failed.push_back(i);
        }
    }
    if (result.failed_put_count < 0 ||
        static_cast<std::uint64_t>(result.failed_put_count) != v.failed.size()) {
        v.why = "reports FailedPutCount " + std::to_string(result.failed_put_count) + " where " +
                std::to_string(v.failed.size()) + " entries failed";
        v.failed.clear();
        return v;
    }
    v.valid = true;
    return v;
}

FirehoseFault classify_firehose_error(std::string_view raw_code,
                                      int http_status,
                                      bool sdk_retryable) {
    // ServiceUnavailableException is Firehose's throttling answer ("back off and
    // retry"); InternalFailure and a missing reply leave the request's outcome
    // unknown. UnrecognizedClientException and ExpiredToken fail at once, as
    // for the other AWS connectors: the job's restart opens the sink again.
    static const std::set<std::string, std::less<>> kThrottled = {
        "ServiceUnavailableException",
        "ThrottlingException",
        "Throttling",
        "LimitExceededException",
        "TooManyRequestsException",
        "RequestLimitExceeded",
        "ProvisionedThroughputExceededException",
    };
    static const std::set<std::string, std::less<>> kPermanent = {
        "InvalidArgumentException",
        "InvalidKMSResourceException",
        "InvalidSourceException",
        "ResourceNotFoundException",
        "ValidationException",
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
    };
    static const std::set<std::string, std::less<>> kTransient = {
        "InternalFailure",
        "InternalFailureException",
        "InternalServerError",
        "ServiceUnavailable",
        "RequestTimeout",
        "RequestTimeoutException",
        "ResourceInUseException",
    };
    const std::string code = bare_error_code(raw_code);
    if (kThrottled.count(code) != 0 || http_status == 429) {
        return FirehoseFault::Throttled;
    }
    if (kPermanent.count(code) != 0) {
        return FirehoseFault::Permanent;
    }
    if (kTransient.count(code) != 0 || http_status == 0 || http_status >= 500) {
        return FirehoseFault::Transient;
    }
    return sdk_retryable ? FirehoseFault::Transient : FirehoseFault::Permanent;
}

// ---- the sink -----------------------------------------------------------------------

struct FirehoseSink::Impl {
    explicit Impl(FirehoseSinkOptions o)
        : opts(std::move(o)),
          before_write_point(opts.name + ".before_write"),
          after_write_point(opts.name + ".after_write") {}

    FirehoseSinkOptions opts;
    std::shared_ptr<FirehoseWriteService> service;
    bool opened{false};
    std::vector<std::string> pending;  // each record's data, delimiter appended
    std::size_t pending_request_bytes{0};
    std::size_t pending_data_bytes{0};
    std::chrono::steady_clock::time_point first_buffered_at{};
    std::optional<std::string> failure;  // why this instance stopped
    std::string before_write_point;
    std::string after_write_point;
    std::unique_ptr<CallWatch> watch;

    // Stops this instance: drops what is buffered, refuses every later record,
    // flush and barrier until open(), and throws `why` with how many records were
    // dropped and, last, what to check.
    [[noreturn]] void fail_now(std::string why, std::size_t dropped, const std::string& hint = {}) {
        if (dropped > 0) {
            why += "; " + std::to_string(dropped) +
                   " buffered record(s) not yet accepted are dropped with this sink instance";
        }
        why +=
            ". The sink accepts nothing more, and the job's restart replays every record after "
            "the last completed checkpoint." +
            hint;
        pending.clear();
        pending_request_bytes = 0;
        pending_data_bytes = 0;
        failure = why;
        clink::metrics::connector::error_inc("firehose", "sink");
        clink::log::error(kLogComponent, opts.name + ": " + why);
        throw std::runtime_error(opts.name + ": " + why);
    }

    void check_usable() const {
        if (failure) {
            throw std::runtime_error(opts.name + ": the sink failed earlier and dropped records, " +
                                     "so it refuses to send or acknowledge a barrier (" + *failure +
                                     ")");
        }
        if (!opened) {
            throw std::runtime_error(opts.name + ": open() has not been called");
        }
    }

    [[nodiscard]] std::size_t request_budget() const {
        return opts.max_bytes - firehose_request_envelope_bytes(opts.delivery_stream.size());
    }

    // The pause before a retry, cut short when the task is cancelled.
    void pause(RuntimeContext* rt, std::chrono::milliseconds delay) {
        const auto until = std::chrono::steady_clock::now() + delay;
        for (;;) {
            if (rt != nullptr && rt->cancel_requested()) {
                throw std::runtime_error("cancelled while waiting to retry a PutRecordBatch");
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

    void open() {
        failure.reset();
        pending.clear();
        pending_request_bytes = 0;
        pending_data_bytes = 0;
        watch.reset();
        service = opts.service ? opts.service : make_firehose_sdk_service(opts.client);
        opened = true;
        clink::log::info(kLogComponent,
                         opts.name + ": writing to delivery stream '" + opts.delivery_stream +
                             "', up to " + std::to_string(opts.batch_records) + " records and " +
                             std::to_string(opts.max_bytes) + " bytes per PutRecordBatch");
    }

    void buffer(const std::string& record) {
        std::string data = record;
        data += opts.delimiter;
        const auto need = firehose_record_request_bytes(data.size());
        if (pending.empty()) {
            first_buffered_at = std::chrono::steady_clock::now();
        }
        pending_request_bytes += need;
        pending_data_bytes += data.size();
        pending.push_back(std::move(data));
    }

    // Sends the buffered records in one PutRecordBatch and its retries.
    void send(RuntimeContext* rt) {
        std::vector<std::size_t> outstanding(pending.size());
        for (std::size_t i = 0; i < outstanding.size(); ++i) {
            outstanding[i] = i;
        }
        const auto deadline = std::chrono::steady_clock::now() + opts.retry_timeout;
        int idle = 0;  // attempts in a row that accepted nothing
        int attempts = 0;
        std::string last_problem;
        for (;;) {
            if (attempts > 0) {
                if (idle > opts.max_retries) {
                    throw SendFailure(std::to_string(outstanding.size()) + " of " +
                                      std::to_string(pending.size()) +
                                      " record(s) of a PutRecordBatch were still not accepted "
                                      "after " +
                                      std::to_string(idle) +
                                      " attempt(s) in a row that accepted nothing (max_retries=" +
                                      std::to_string(opts.max_retries) + "; last: " + last_problem +
                                      ")");
                }
                const auto delay =
                    backoff_delay(std::max(idle, 1), opts.retry_base_backoff, opts.max_backoff);
                if (std::chrono::steady_clock::now() + delay > deadline) {
                    throw SendFailure(std::to_string(outstanding.size()) + " of " +
                                      std::to_string(pending.size()) +
                                      " record(s) of a PutRecordBatch were still not accepted when "
                                      "the retry budget of " +
                                      std::to_string(opts.retry_timeout.count()) + " ms ran out (" +
                                      std::to_string(attempts) +
                                      " attempt(s); last: " + last_problem + ")");
                }
                if ((attempts & (attempts - 1)) == 0) {
                    clink::log::warn(kLogComponent,
                                     opts.name + ": " + std::to_string(outstanding.size()) +
                                         " record(s) not accepted (" + last_problem +
                                         "); attempt " + std::to_string(attempts + 1) + " in " +
                                         std::to_string(delay.count()) + " ms");
                }
                try {
                    pause(rt, delay);
                } catch (const std::runtime_error& e) {
                    throw SendFailure(std::string{e.what()} + "; " +
                                      std::to_string(outstanding.size()) +
                                      " record(s) of it not accepted");
                }
            }
            ++attempts;
            (void)CLINK_FAULT_POINT(before_write_point);
            std::vector<const std::string*> records;
            records.reserve(outstanding.size());
            for (const auto i : outstanding) {
                records.push_back(&pending[i]);
            }
            arm(rt);
            auto result = service->put_record_batch(opts.delivery_stream, records);
            const bool aborted = disarm();
            if (aborted || result.error.fault == FirehoseFault::Cancelled) {
                throw SendFailure(
                    "cancelled while a PutRecordBatch of " + std::to_string(outstanding.size()) +
                    " record(s) was in flight; whether they were accepted is unknown, "
                    "so a replay may send them twice");
            }
            if (!result.error.ok()) {
                if (result.error.fault == FirehoseFault::Permanent) {
                    throw SendFailure("PutRecordBatch was refused: " + error_text(result.error),
                                      permanent_hint(bare_error_code(result.error.code)));
                }
                // Throttled: nothing was accepted. Transient: some or all of it
                // may have been; sending it again can deliver those twice.
                ++idle;
                last_problem = error_text(result.error);
                continue;
            }
            const auto verdict = judge_firehose_response(outstanding.size(), result);
            if (!verdict.valid) {
                throw SendFailure(
                    "PutRecordBatch's response does not account for its request: it " +
                    verdict.why + ". Which of the " + std::to_string(outstanding.size()) +
                    " record(s) Firehose accepted is unknown, so the flush fails "
                    "rather than count them accepted, and a replay may send some "
                    "twice");
            }
            if (verdict.failed.empty()) {
                (void)CLINK_FAULT_POINT(after_write_point);
                return;
            }
            std::vector<std::size_t> again;
            again.reserve(verdict.failed.size());
            for (const auto pos : verdict.failed) {
                again.push_back(outstanding[pos]);
            }
            const auto& first_failed = result.entries[verdict.failed.front()];
            idle = again.size() < outstanding.size() ? 0 : idle + 1;
            last_problem = std::to_string(again.size()) + " of " +
                           std::to_string(outstanding.size()) + " failed, first with " +
                           first_failed.error_code +
                           (first_failed.error_message.empty()
                                ? std::string{}
                                : ": " + without_credentials(first_failed.error_message));
            outstanding = std::move(again);
        }
    }

    void flush(RuntimeContext* rt) {
        check_usable();
        if (pending.empty()) {
            return;
        }
        const auto n = pending.size();
        const auto bytes = pending_data_bytes;
        const auto t0 = std::chrono::steady_clock::now();
        try {
            send(rt);
        } catch (const SendFailure& e) {
            fail_now(std::string{e.what()} + " (in a flush of " + std::to_string(n) + " record(s))",
                     n,
                     e.hint());
        }
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::records_out_inc("firehose", n);
        clink::metrics::connector::bytes_out_inc("firehose", bytes);
        clink::metrics::connector::commit_latency_observe("firehose",
                                                          static_cast<std::uint64_t>(dt));
        pending.clear();
        pending_request_bytes = 0;
        pending_data_bytes = 0;
    }

    bool linger_elapsed() const {
        return opts.max_age.count() > 0 && !pending.empty() &&
               std::chrono::steady_clock::now() - first_buffered_at >= opts.max_age;
    }
};

FirehoseSink::FirehoseSink(FirehoseSinkOptions opts) {
    const auto refuse = [&](const std::string& why) {
        throw std::runtime_error(opts.name + ": " + why);
    };
    if (opts.delivery_stream.empty()) {
        refuse("'delivery_stream' is required");
    }
    if (!valid_stream_name(opts.delivery_stream)) {
        refuse("'delivery_stream' must be 1 to 64 characters from a-z, A-Z, 0-9, '_', '.' and '-'");
    }
    if (opts.delimiter.size() > kFirehoseMaxRecordBytes) {
        refuse("'delimiter' is longer than a whole Firehose record (1,024,000 bytes)");
    }
    if (opts.batch_records < 1 || opts.batch_records > kFirehoseMaxBatchRecords) {
        refuse(
            "'batch_records' must be 1 to 500 (PutRecordBatch takes at most 500 records); it is " +
            std::to_string(opts.batch_records));
    }
    const auto min_bytes = firehose_min_request_bytes(opts.delivery_stream.size());
    if (opts.max_bytes < min_bytes || opts.max_bytes > kFirehoseMaxRequestBytes) {
        refuse("'max_bytes' must be " + std::to_string(min_bytes) +
               " (the largest record alone) to " + std::to_string(kFirehoseMaxRequestBytes) +
               " (4 MiB); it is " + std::to_string(opts.max_bytes));
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

FirehoseSink::~FirehoseSink() = default;

void FirehoseSink::open() {
    impl_->open();
}

void FirehoseSink::close() {
    impl_->watch.reset();
}

void FirehoseSink::on_data(const Batch<std::string>& batch) {
    impl_->check_usable();
    auto* rt = runtime();
    const auto& delim = impl_->opts.delimiter;
    std::size_t i = 0;
    for (const auto& rec : batch) {
        const auto bytes = rec.value().size() + delim.size();
        if (bytes > kFirehoseMaxRecordBytes) {
            impl_->fail_now("record " + std::to_string(i + 1) + " of a batch of " +
                                std::to_string(batch.size()) + " is " + with_thousands(bytes) +
                                " bytes with its " + std::to_string(delim.size()) +
                                "-byte delimiter; Firehose takes at most 1,024,000 bytes (1,000 "
                                "KiB) per record before base64 encoding, so split or compress it "
                                "before the sink. Nothing of that batch was buffered or sent",
                            impl_->pending.size());
        }
        ++i;
    }
    const auto budget = impl_->request_budget();
    for (const auto& rec : batch) {
        const auto need = firehose_record_request_bytes(rec.value().size() + delim.size());
        // Send what is buffered first when this record would push the request
        // past its count or byte limit.
        if (!impl_->pending.empty() && (impl_->pending.size() >= impl_->opts.batch_records ||
                                        impl_->pending_request_bytes + need > budget)) {
            impl_->flush(rt);
        }
        impl_->buffer(rec.value());
        if (impl_->pending.size() >= impl_->opts.batch_records || impl_->linger_elapsed()) {
            impl_->flush(rt);
        }
    }
}

void FirehoseSink::on_barrier(CheckpointBarrier /*b*/) {
    impl_->flush(runtime());
}

void FirehoseSink::flush() {
    impl_->flush(runtime());
}

std::string FirehoseSink::name() const {
    return impl_->opts.name;
}

std::size_t FirehoseSink::pending_records() const {
    return impl_->pending.size();
}

bool FirehoseSink::failed() const {
    return impl_->failure.has_value();
}

}  // namespace clink::aws
