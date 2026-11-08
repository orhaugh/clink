// The Kinesis sink's call on the AWS SDK. The SDK's own retries are off, so
// every failure reaches the sink, which classifies it
// (classify_kinesis_put_error), paces its retries and bounds them.

#include <algorithm>
#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/PutRecordsRequest.h>
#include <aws/kinesis/model/PutRecordsRequestEntry.h>

#include "clink/aws/aws_client.hpp"
#include "clink/aws/kinesis_sink.hpp"

namespace clink::aws {

namespace {

// The SDK's Kinesis client, with a way to end its calls from another thread.
// Reaching DisableRequestProcessing from a subclass works whether a release
// makes it public or protected.
class AbortableKinesisClient final : public Aws::Kinesis::KinesisClient {
public:
    using Aws::Kinesis::KinesisClient::KinesisClient;
    void end_requests() { DisableRequestProcessing(); }
};

class KinesisWriteSdkService final : public KinesisWriteService {
public:
    explicit KinesisWriteSdkService(const AwsClientOptions& options) {
        ensure_aws_initialized();
        auto cfg = make_client_config(options);
        cfg.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(0);
        client_ = std::make_unique<AbortableKinesisClient>(cfg);
    }

    KinesisPutResult put_records(const std::string& stream,
                                 const std::vector<KinesisPutRecord>& records) override {
        KinesisPutResult out;
        Aws::Kinesis::Model::PutRecordsRequest req;
        if (stream.rfind("arn:", 0) == 0) {
            req.SetStreamARN(stream);
        } else {
            req.SetStreamName(stream);
        }
        Aws::Vector<Aws::Kinesis::Model::PutRecordsRequestEntry> entries;
        entries.reserve(records.size());
        for (const auto& r : records) {
            // The SDK base64-encodes the raw bytes on the wire.
            Aws::Kinesis::Model::PutRecordsRequestEntry e;
            e.SetData(Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(r.data.data()),
                                             r.data.size()));
            e.SetPartitionKey(Aws::String(r.partition_key.data(), r.partition_key.size()));
            entries.push_back(std::move(e));
        }
        req.SetRecords(std::move(entries));
        auto outcome = client_->PutRecords(req);
        if (!outcome.IsSuccess()) {
            out.error = error_of(outcome.GetError());
            return out;
        }
        const auto& result = outcome.GetResult();
        out.failed_record_count = result.GetFailedRecordCount();
        out.entries.reserve(result.GetRecords().size());
        for (const auto& e : result.GetRecords()) {
            out.entries.push_back(KinesisPutResponseEntry{
                e.GetSequenceNumber(), e.GetShardId(), e.GetErrorCode(), e.GetErrorMessage()});
        }
        return out;
    }

    void abort() noexcept override {
        aborted_.store(true, std::memory_order_relaxed);
        // Fails the request in flight and every later one at once.
        client_->end_requests();
    }

private:
    template <typename Error>
    KinesisPutError error_of(const Error& e) const {
        KinesisPutError out;
        if (aborted_.load(std::memory_order_relaxed)) {
            out.fault = KinesisPutFault::Cancelled;
            out.code = "Cancelled";
            out.message = "the task was cancelled";
            return out;
        }
        out.code = e.GetExceptionName().c_str();
        out.message = e.GetMessage().c_str();
        // REQUEST_NOT_MADE (-1) and the like: no response arrived. A 1xx is
        // the interim 100 Continue the SDK asks for, left behind when the
        // final reply never came.
        out.http_status = static_cast<int>(e.GetResponseCode());
        if (out.http_status < 200) {
            out.http_status = 0;
        }
        if (out.code.empty()) {
            out.code =
                out.http_status == 0 ? "NoResponse" : "HTTP" + std::to_string(out.http_status);
        }
        out.fault = classify_kinesis_put_error(out.code, out.http_status, e.ShouldRetry());
        return out;
    }

    std::unique_ptr<AbortableKinesisClient> client_;
    std::atomic<bool> aborted_{false};
};

}  // namespace

std::shared_ptr<KinesisWriteService> make_kinesis_write_sdk_service(
    const AwsClientOptions& client) {
    return std::make_shared<KinesisWriteSdkService>(client);
}

}  // namespace clink::aws
