#pragma once

// Kinesis Data Streams sink (PutRecords). SDK-dependent; compiled only with the
// AWS SDK.
//
// Each input record is a string (e.g. SQL row_to_json_string JSON) sent as one
// Kinesis record's Data. The PartitionKey is the configured field's value when
// present, else a rotating counter (Kinesis hashes the key to pick a shard, so a
// counter spreads load). Delivery is AT-LEAST-ONCE: Kinesis has no producer
// dedup key, so a retry of throttled records can duplicate. PutRecords is
// partial-success - on FailedRecordCount > 0 ONLY the failed entries (by
// response index) are resent, never the whole batch (which would duplicate the
// already-committed records).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/KinesisErrors.h>
#include <aws/kinesis/model/PutRecordsRequest.h>
#include <aws/kinesis/model/PutRecordsRequestEntry.h>
#include <aws/kinesis/model/PutRecordsResultEntry.h>

#include "clink/aws/aws_client.hpp"
#include "clink/config/json.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::aws {

// Whether an AWS failure is worth retrying (transient outage / throttle) or is a
// permanent request error (a poison batch that will fail identically on replay).
enum class FailureClass { Transient, Permanent };

// What a sink does with a permanently-poison record (one that fails identically
// on replay, e.g. a Kinesis record over the 1 MiB per-record limit). Fail
// (default) = let it through / throw so the job replays (crash-loops on true
// poison); Drop = route to the dead-letter path (count + metric) and continue.
enum class DlqPolicy { Fail, Drop };

// Classify a WHOLE-CALL Kinesis error (outcome.GetError()). Throttle /
// service-unavailable / internal / slow-down are transient (retry); validation /
// bad-argument / access / not-found are permanent (a malformed request, e.g. an
// oversized record or a bad stream -> DLQ rather than crash-loop). Unknown
// defaults to Transient: replay is the safe direction (never silently drop).
inline FailureClass classify_kinesis_error(Aws::Kinesis::KinesisErrors err) {
    using E = Aws::Kinesis::KinesisErrors;
    switch (err) {
        case E::VALIDATION:
        case E::INVALID_ARGUMENT:
        case E::ACCESS_DENIED:
        case E::RESOURCE_NOT_FOUND:
        case E::RESOURCE_IN_USE:
            return FailureClass::Permanent;
        default:
            return FailureClass::Transient;
    }
}

// Indices of the response entries that FAILED (non-empty ErrorCode). The
// PutRecords response is index-aligned with the request, so these map straight
// back to the request entries that must be resent. Pure: unit-testable without
// a live stream.
inline std::vector<std::size_t> kinesis_failed_indices(
    const Aws::Vector<Aws::Kinesis::Model::PutRecordsResultEntry>& results) {
    std::vector<std::size_t> failed;
    for (std::size_t i = 0; i < results.size(); ++i) {
        if (!results[i].GetErrorCode().empty()) {
            failed.push_back(i);
        }
    }
    return failed;
}

struct KinesisSinkOptions {
    std::string stream;         // stream name or ARN (required)
    std::string partition_key;  // record field used as the Kinesis PartitionKey (optional)
    AwsClientOptions client;
    std::size_t batch_records{500};                        // PutRecords hard max is 500
    std::size_t max_bytes{4u * 1024 * 1024 + 512 * 1024};  // flush under the 5 MiB request cap
    std::size_t max_record_bytes{1024 * 1024};             // Kinesis per-record limit (1 MiB)
    int max_retries{8};                                    // failed-subset resend attempts
    std::chrono::milliseconds retry_base_backoff{100};
    std::chrono::milliseconds max_age{0};   // linger: flush a partial batch this old (0 = off)
    DlqPolicy dlq_policy{DlqPolicy::Fail};  // oversized record: throw (Fail) vs drop
    std::string name{"kinesis_sink"};
};

class KinesisSink : public Sink<std::string> {
public:
    static constexpr int kMaxRetries = 20;  // bound the backoff shift + attempt loop

    explicit KinesisSink(KinesisSinkOptions opts) : opts_(std::move(opts)) {
        if (opts_.stream.empty()) {
            throw std::runtime_error(opts_.name + ": 'stream' is required");
        }
        if (opts_.batch_records == 0 || opts_.batch_records > 500) {
            opts_.batch_records = 500;  // PutRecords ceiling
        }
        if (opts_.max_retries < 0) {
            opts_.max_retries = 0;
        } else if (opts_.max_retries > kMaxRetries) {
            opts_.max_retries = kMaxRetries;  // else 1u<<(attempt-1) overflows
        }
    }

    void open() override {
        ensure_aws_initialized();
        client_ = std::make_unique<Aws::Kinesis::KinesisClient>(make_client_config(opts_.client));
        pending_.clear();
    }

    void on_data(const Batch<std::string>& batch) override {
        for (const auto& rec : batch) {
            // A single record over the Kinesis per-record limit (1 MiB) poisons
            // the WHOLE PutRecords batch (ValidationException on every retry -> a
            // wedged job). Under DlqPolicy::Drop, route just that record to the
            // dead-letter path so the rest of the batch still flows. Under Fail,
            // let it through and PutRecords rejects the batch loudly.
            if (rec.value().size() > opts_.max_record_bytes &&
                opts_.dlq_policy == DlqPolicy::Drop) {
                clink::metrics::connector::dropped_records_inc("kinesis", 1);
                clink::metrics::connector::permanent_failures_inc("kinesis");
                continue;
            }
            if (pending_.empty()) {
                first_buffered_at_ = std::chrono::steady_clock::now();  // linger age clock
            }
            pending_.push_back(make_entry_(rec.value()));
            // ~partition key (<=256) + framing overhead; flush on count OR bytes
            // so a batch of large records never exceeds the 5 MiB request cap
            // (which would be rejected on every retry -> a wedged job).
            pending_bytes_ += rec.value().size() + 320;
            if (pending_.size() >= opts_.batch_records || pending_bytes_ >= opts_.max_bytes ||
                linger_elapsed_()) {
                flush();
            }
        }
    }

    void on_barrier(CheckpointBarrier /*b*/) override { flush(); }

    void flush() override {
        if (pending_.empty()) {
            return;
        }
        const std::size_t n = pending_.size();
        const std::size_t bytes = pending_bytes_;
        const auto t0 = std::chrono::steady_clock::now();
        try {
            put_with_retry_(std::move(pending_));
        } catch (...) {
            clink::metrics::connector::error_inc("kinesis", "sink");
            pending_.clear();
            pending_bytes_ = 0;
            throw;
        }
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::records_out_inc("kinesis", n);
        clink::metrics::connector::bytes_out_inc("kinesis", bytes);
        clink::metrics::connector::commit_latency_observe("kinesis",
                                                          static_cast<std::uint64_t>(dt));
        pending_.clear();
        pending_bytes_ = 0;
    }

    std::string name() const override { return opts_.name; }

private:
    Aws::Kinesis::Model::PutRecordsRequestEntry make_entry_(const std::string& rec) {
        Aws::Kinesis::Model::PutRecordsRequestEntry e;
        e.SetData(
            Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(rec.data()), rec.size()));
        e.SetPartitionKey(partition_key_for_(rec));
        return e;
    }

    // The Kinesis PartitionKey: the configured field's scalar value when present
    // and non-empty, else a rotating counter (keeps writes spread across shards
    // without forcing the user to nominate a key). Max 256 chars per the API; a
    // field value over that is truncated.
    std::string partition_key_for_(const std::string& rec) {
        if (!opts_.partition_key.empty()) {
            try {
                auto j = clink::config::parse(rec);
                if (j.is_object()) {
                    const auto& obj = j.as_object();
                    if (auto it = obj.find(opts_.partition_key);
                        it != obj.end() && !it->second.is_null()) {
                        std::string pk = scalar_text_(it->second);
                        if (!pk.empty()) {
                            return pk.size() > 256 ? pk.substr(0, 256) : pk;
                        }
                    }
                }
            } catch (...) {
                // fall through to the counter
            }
        }
        return std::to_string(counter_++);
    }

    static std::string scalar_text_(const clink::config::JsonValue& v) {
        if (v.is_string()) {
            return v.as_string();
        }
        if (v.is_bool()) {
            return v.as_bool() ? "true" : "false";
        }
        if (v.is_number()) {
            const double d = v.as_number();
            constexpr double kInt64Lo = -9223372036854775808.0;
            constexpr double kInt64HiExclusive = 9223372036854775808.0;
            if (d >= kInt64Lo && d < kInt64HiExclusive &&
                d == static_cast<double>(static_cast<std::int64_t>(d))) {
                return std::to_string(static_cast<std::int64_t>(d));
            }
            return std::to_string(d);
        }
        return {};  // object/array/null: no usable scalar key -> caller falls back
    }

    void set_stream_(Aws::Kinesis::Model::PutRecordsRequest& req) const {
        if (opts_.stream.rfind("arn:", 0) == 0) {
            req.SetStreamARN(opts_.stream);
        } else {
            req.SetStreamName(opts_.stream);
        }
    }

    // PutRecords + failed-subset resend. On partial failure resend ONLY the
    // entries whose response index carries an ErrorCode; the successful ones are
    // already committed, so resending them would duplicate. Throw on exhaustion
    // (job replays from the last checkpoint - at-least-once, no silent drop).
    void put_with_retry_(std::vector<Aws::Kinesis::Model::PutRecordsRequestEntry> entries) {
        for (int attempt = 0; attempt <= opts_.max_retries; ++attempt) {
            if (attempt > 0) {
                sleep_backoff_(attempt);
            }
            Aws::Kinesis::Model::PutRecordsRequest req;
            set_stream_(req);
            req.SetRecords(Aws::Vector<Aws::Kinesis::Model::PutRecordsRequestEntry>(entries));
            auto outcome = client_->PutRecords(req);
            if (!outcome.IsSuccess()) {
                // A permanent whole-call error (validation / access / bad stream)
                // fails identically on retry - surface it immediately rather than
                // burning the retry budget. Transient errors retry the whole
                // batch (none can be assumed written) until exhausted.
                const bool permanent = classify_kinesis_error(outcome.GetError().GetErrorType()) ==
                                       FailureClass::Permanent;
                if (permanent || attempt == opts_.max_retries) {
                    throw std::runtime_error(opts_.name + ": PutRecords failed: " +
                                             std::string(outcome.GetError().GetMessage().c_str()));
                }
                continue;
            }
            if (outcome.GetResult().GetFailedRecordCount() == 0) {
                return;  // all records written
            }
            const auto failed = kinesis_failed_indices(outcome.GetResult().GetRecords());
            if (!failed.empty()) {
                std::vector<Aws::Kinesis::Model::PutRecordsRequestEntry> retry;
                retry.reserve(failed.size());
                for (std::size_t idx : failed) {
                    retry.push_back(entries[idx]);
                }
                entries = std::move(retry);
            }
            // else: FailedRecordCount>0 but no per-entry ErrorCode (anomalous /
            // short response). Do NOT treat as done - retry the WHOLE batch
            // (entries unchanged) rather than silently dropping records.
        }
        throw std::runtime_error(opts_.name + ": PutRecords left records unwritten after " +
                                 std::to_string(opts_.max_retries + 1) + " attempts");
    }

    void sleep_backoff_(int attempt) {
        auto backoff = opts_.retry_base_backoff * (1u << (attempt - 1));
        constexpr std::chrono::milliseconds kMaxBackoff{30000};
        if (backoff > kMaxBackoff) {
            backoff = kMaxBackoff;
        }
        std::this_thread::sleep_for(backoff);
    }

    bool linger_elapsed_() const {
        return opts_.max_age.count() > 0 && !pending_.empty() &&
               std::chrono::steady_clock::now() - first_buffered_at_ >= opts_.max_age;
    }

    KinesisSinkOptions opts_;
    std::unique_ptr<Aws::Kinesis::KinesisClient> client_;
    std::vector<Aws::Kinesis::Model::PutRecordsRequestEntry> pending_;
    std::size_t pending_bytes_{0};
    std::uint64_t counter_{0};
    std::chrono::steady_clock::time_point first_buffered_at_{};
};

}  // namespace clink::aws
