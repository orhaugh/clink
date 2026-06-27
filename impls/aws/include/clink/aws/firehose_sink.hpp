#pragma once

// Amazon Data Firehose sink (PutRecordBatch). SDK-dependent; compiled only with
// the AWS SDK.
//
// Each input record is a string (e.g. SQL row_to_json_string JSON) sent as one
// Firehose record's Data, with a delimiter (newline by default) appended so the
// destination can split records (Firehose concatenates record bytes and adds no
// boundary itself). Firehose has NO partition key and NO ordering guarantee.
// Delivery is AT-LEAST-ONCE: PutRecordBatch is partial-success, so on
// FailedPutCount > 0 ONLY the failed entries (by response index) are resent.

#include <chrono>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <aws/firehose/FirehoseClient.h>
#include <aws/firehose/model/PutRecordBatchRequest.h>
#include <aws/firehose/model/PutRecordBatchResponseEntry.h>
#include <aws/firehose/model/Record.h>

#include "clink/aws/aws_client.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::aws {

// Indices of the response entries that FAILED (non-empty ErrorCode). The
// PutRecordBatch response is index-aligned with the request. Pure: unit-testable
// without a live delivery stream.
inline std::vector<std::size_t> firehose_failed_indices(
    const Aws::Vector<Aws::Firehose::Model::PutRecordBatchResponseEntry>& results) {
    std::vector<std::size_t> failed;
    for (std::size_t i = 0; i < results.size(); ++i) {
        if (!results[i].GetErrorCode().empty()) {
            failed.push_back(i);
        }
    }
    return failed;
}

struct FirehoseSinkOptions {
    std::string delivery_stream;  // required
    std::string delimiter{"\n"};  // appended to each record's Data (empty = none)
    AwsClientOptions client;
    std::size_t batch_records{500};                        // PutRecordBatch hard max is 500
    std::size_t max_bytes{3u * 1024 * 1024 + 512 * 1024};  // flush under the 4 MiB request cap
    int max_retries{8};                                    // failed-subset resend attempts
    std::chrono::milliseconds retry_base_backoff{100};
    std::string name{"firehose_sink"};
};

class FirehoseSink : public Sink<std::string> {
public:
    static constexpr int kMaxRetries = 20;  // bound the backoff shift + attempt loop

    explicit FirehoseSink(FirehoseSinkOptions opts) : opts_(std::move(opts)) {
        if (opts_.delivery_stream.empty()) {
            throw std::runtime_error(opts_.name + ": 'delivery_stream' is required");
        }
        if (opts_.batch_records == 0 || opts_.batch_records > 500) {
            opts_.batch_records = 500;  // PutRecordBatch ceiling
        }
        if (opts_.max_retries < 0) {
            opts_.max_retries = 0;
        } else if (opts_.max_retries > kMaxRetries) {
            opts_.max_retries = kMaxRetries;  // else 1u<<(attempt-1) overflows
        }
    }

    void open() override {
        ensure_aws_initialized();
        client_ = std::make_unique<Aws::Firehose::FirehoseClient>(make_client_config(opts_.client));
        pending_.clear();
    }

    void on_data(const Batch<std::string>& batch) override {
        for (const auto& rec : batch) {
            pending_.push_back(make_record_(rec.value()));
            // Flush on count OR bytes so a batch of large records never exceeds
            // the 4 MiB request cap (rejected on every retry -> a wedged job).
            pending_bytes_ += rec.value().size() + opts_.delimiter.size() + 16;
            if (pending_.size() >= opts_.batch_records || pending_bytes_ >= opts_.max_bytes) {
                flush();
            }
        }
    }

    void on_barrier(CheckpointBarrier /*b*/) override { flush(); }

    void flush() override {
        if (pending_.empty()) {
            return;
        }
        put_with_retry_(std::move(pending_));
        pending_.clear();
        pending_bytes_ = 0;
    }

    std::string name() const override { return opts_.name; }

private:
    Aws::Firehose::Model::Record make_record_(const std::string& rec) {
        // Append the delimiter to the RAW bytes (the SDK base64-encodes on the
        // wire; never pre-encode or concatenate base64).
        std::string data = rec;
        data += opts_.delimiter;
        Aws::Firehose::Model::Record r;
        r.SetData(Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(data.data()),
                                         data.size()));
        return r;
    }

    // PutRecordBatch + failed-subset resend. On partial failure resend ONLY the
    // entries whose response index carries an ErrorCode; the rest are delivered,
    // so resending them would duplicate. Throw on exhaustion.
    void put_with_retry_(std::vector<Aws::Firehose::Model::Record> records) {
        for (int attempt = 0; attempt <= opts_.max_retries; ++attempt) {
            if (attempt > 0) {
                sleep_backoff_(attempt);
            }
            Aws::Firehose::Model::PutRecordBatchRequest req;
            req.SetDeliveryStreamName(opts_.delivery_stream);
            req.SetRecords(Aws::Vector<Aws::Firehose::Model::Record>(records));
            auto outcome = client_->PutRecordBatch(req);
            if (!outcome.IsSuccess()) {
                if (attempt == opts_.max_retries) {
                    throw std::runtime_error(opts_.name + ": PutRecordBatch failed: " +
                                             std::string(outcome.GetError().GetMessage().c_str()));
                }
                continue;
            }
            if (outcome.GetResult().GetFailedPutCount() == 0) {
                return;  // all records delivered
            }
            const auto failed = firehose_failed_indices(outcome.GetResult().GetRequestResponses());
            if (!failed.empty()) {
                std::vector<Aws::Firehose::Model::Record> retry;
                retry.reserve(failed.size());
                for (std::size_t idx : failed) {
                    retry.push_back(records[idx]);
                }
                records = std::move(retry);
            }
            // else: FailedPutCount>0 but no per-entry ErrorCode (anomalous /
            // short response). Retry the WHOLE batch (records unchanged) rather
            // than silently dropping records.
        }
        throw std::runtime_error(opts_.name + ": PutRecordBatch left records undelivered after " +
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

    FirehoseSinkOptions opts_;
    std::unique_ptr<Aws::Firehose::FirehoseClient> client_;
    std::vector<Aws::Firehose::Model::Record> pending_;
    std::size_t pending_bytes_{0};
};

}  // namespace clink::aws
