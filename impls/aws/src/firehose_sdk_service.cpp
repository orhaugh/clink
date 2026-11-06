// The Firehose sink's calls on the AWS SDK. The SDK's own retries are off, so
// every failure reaches the sink, which classifies it (classify_firehose_error),
// paces its retries and bounds them.

#include <algorithm>
#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/firehose/FirehoseClient.h>
#include <aws/firehose/model/PutRecordBatchRequest.h>
#include <aws/firehose/model/Record.h>

#include "clink/aws/aws_client.hpp"
#include "clink/aws/firehose_sink.hpp"

namespace clink::aws {

namespace {

// The SDK's Firehose client, with a way to end its calls from another thread.
// DisableRequestProcessing is public on the client this SDK release builds and
// protected on some later, smithy-based ones; reaching it from a subclass works
// with both.
class AbortableFirehoseClient final : public Aws::Firehose::FirehoseClient {
public:
    using Aws::Firehose::FirehoseClient::FirehoseClient;
    void end_requests() { DisableRequestProcessing(); }
};

class FirehoseSdkService final : public FirehoseWriteService {
public:
    explicit FirehoseSdkService(const AwsClientOptions& options) {
        ensure_aws_initialized();
        auto cfg = make_client_config(options);
        cfg.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(0);
        client_ = std::make_unique<AbortableFirehoseClient>(cfg);
    }

    FirehoseBatchResult put_record_batch(const std::string& stream,
                                         const std::vector<const std::string*>& records) override {
        FirehoseBatchResult out;
        Aws::Firehose::Model::PutRecordBatchRequest req;
        req.SetDeliveryStreamName(stream);
        Aws::Vector<Aws::Firehose::Model::Record> batch;
        batch.reserve(records.size());
        for (const auto* data : records) {
            // The SDK base64-encodes the raw bytes on the wire.
            Aws::Firehose::Model::Record r;
            r.SetData(Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(data->data()),
                                             data->size()));
            batch.push_back(std::move(r));
        }
        req.SetRecords(std::move(batch));
        auto outcome = client_->PutRecordBatch(req);
        if (!outcome.IsSuccess()) {
            out.error = error_of(outcome.GetError());
            return out;
        }
        const auto& result = outcome.GetResult();
        out.failed_put_count = result.GetFailedPutCount();
        out.entries.reserve(result.GetRequestResponses().size());
        for (const auto& e : result.GetRequestResponses()) {
            out.entries.push_back(
                FirehoseResponseEntry{e.GetRecordId(), e.GetErrorCode(), e.GetErrorMessage()});
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
    FirehoseCallError error_of(const Error& e) const {
        FirehoseCallError out;
        if (aborted_.load(std::memory_order_relaxed)) {
            out.fault = FirehoseFault::Cancelled;
            out.code = "Cancelled";
            out.message = "the task was cancelled";
            return out;
        }
        out.code = e.GetExceptionName().c_str();
        out.message = e.GetMessage().c_str();
        // REQUEST_NOT_MADE (-1) and the like: no response arrived.
        out.http_status = std::max(0, static_cast<int>(e.GetResponseCode()));
        if (out.code.empty()) {
            out.code =
                out.http_status == 0 ? "NoResponse" : "HTTP" + std::to_string(out.http_status);
        }
        out.fault = classify_firehose_error(out.code, out.http_status, e.ShouldRetry());
        return out;
    }

    std::unique_ptr<AbortableFirehoseClient> client_;
    std::atomic<bool> aborted_{false};
};

}  // namespace

std::shared_ptr<FirehoseWriteService> make_firehose_sdk_service(const AwsClientOptions& client) {
    return std::make_shared<FirehoseSdkService>(client);
}

}  // namespace clink::aws
