// The Kinesis source's calls on the AWS SDK. Error codes are classified by
// classify_kinesis_error; the SDK's own retries are off so every failure
// reaches the source, which paces its retries and bounds them.

#include <atomic>
#include <memory>
#include <string>

#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/utils/DateTime.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/ListShardsRequest.h>
#include <aws/kinesis/model/ShardIteratorType.h>

#include "clink/aws/aws_client.hpp"
#include "clink/aws/kinesis_source.hpp"

namespace clink::aws {

namespace {

class KinesisSdkService final : public KinesisShardService {
public:
    KinesisSdkService(std::string stream, const AwsClientOptions& options, int list_page_size)
        : stream_(std::move(stream)), list_page_size_(list_page_size) {
        ensure_aws_initialized();
        auto cfg = make_client_config(options);
        cfg.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(0);
        client_ = std::make_unique<Aws::Kinesis::KinesisClient>(cfg);
    }

    KinesisShardPage list_shards(const std::string& next_token) override {
        Aws::Kinesis::Model::ListShardsRequest req;
        if (next_token.empty()) {
            if (is_arn()) {
                req.SetStreamARN(stream_);
            } else {
                req.SetStreamName(stream_);
            }
        } else {
            req.SetNextToken(next_token);  // must not also name the stream
        }
        if (list_page_size_ > 0) {
            req.SetMaxResults(list_page_size_);
        }
        KinesisShardPage page;
        auto outcome = client_->ListShards(req);
        if (!outcome.IsSuccess()) {
            page.error = error_of(KinesisCall::ListShards, outcome.GetError());
            return page;
        }
        const auto& result = outcome.GetResult();
        for (const auto& shard : result.GetShards()) {
            KinesisShard s;
            s.id = shard.GetShardId();
            s.parent = shard.GetParentShardId();
            s.adjacent_parent = shard.GetAdjacentParentShardId();
            s.starting_sequence = shard.GetSequenceNumberRange().GetStartingSequenceNumber();
            page.shards.push_back(std::move(s));
        }
        page.next_token = result.GetNextToken();
        return page;
    }

    KinesisIterator shard_iterator(const std::string& shard_id,
                                   const KinesisReadFrom& from) override {
        using Type = Aws::Kinesis::Model::ShardIteratorType;
        Aws::Kinesis::Model::GetShardIteratorRequest req;
        if (is_arn()) {
            req.SetStreamARN(stream_);
        } else {
            req.SetStreamName(stream_);
        }
        req.SetShardId(shard_id);
        switch (from.kind) {
            case KinesisReadFrom::Kind::TrimHorizon:
                req.SetShardIteratorType(Type::TRIM_HORIZON);
                break;
            case KinesisReadFrom::Kind::AtTimestamp:
                req.SetShardIteratorType(Type::AT_TIMESTAMP);
                req.SetTimestamp(Aws::Utils::DateTime(static_cast<int64_t>(from.timestamp_ms)));
                break;
            case KinesisReadFrom::Kind::AtSequence:
                req.SetShardIteratorType(Type::AT_SEQUENCE_NUMBER);
                req.SetStartingSequenceNumber(from.sequence_number);
                break;
        }
        KinesisIterator it;
        auto outcome = client_->GetShardIterator(req);
        if (!outcome.IsSuccess()) {
            it.error = error_of(KinesisCall::GetShardIterator, outcome.GetError());
            return it;
        }
        it.iterator = outcome.GetResult().GetShardIterator();
        return it;
    }

    KinesisRecordBatch get_records(const std::string& iterator, int limit) override {
        Aws::Kinesis::Model::GetRecordsRequest req;
        req.SetShardIterator(iterator);
        req.SetLimit(limit);
        KinesisRecordBatch batch;
        auto outcome = client_->GetRecords(req);
        if (!outcome.IsSuccess()) {
            batch.error = error_of(KinesisCall::GetRecords, outcome.GetError());
            return batch;
        }
        auto& result = outcome.GetResult();
        batch.records.reserve(result.GetRecords().size());
        for (const auto& rec : result.GetRecords()) {
            const auto& data = rec.GetData();  // Aws::Utils::ByteBuffer, not NUL-terminated
            batch.records.push_back(
                KinesisRecord{rec.GetSequenceNumber(),
                              std::string(reinterpret_cast<const char*>(data.GetUnderlyingData()),
                                          data.GetLength())});
        }
        batch.next_iterator = result.GetNextShardIterator();
        batch.millis_behind = static_cast<std::int64_t>(result.GetMillisBehindLatest());
        return batch;
    }

    void abort() noexcept override {
        aborted_.store(true, std::memory_order_relaxed);
        // Fails the request in flight and every later one at once.
        client_->DisableRequestProcessing();
    }

private:
    [[nodiscard]] bool is_arn() const { return stream_.rfind("arn:", 0) == 0; }

    template <typename Error>
    KinesisCallError error_of(KinesisCall call, const Error& e) const {
        KinesisCallError out;
        if (aborted_.load(std::memory_order_relaxed)) {
            out.fault = KinesisFault::Cancelled;
            out.code = "Cancelled";
            out.message = "the source was cancelled";
            return out;
        }
        out.code = e.GetExceptionName().c_str();
        out.message = e.GetMessage().c_str();
        // REQUEST_NOT_MADE (-1) and the like: no response arrived.
        const int status = std::max(0, static_cast<int>(e.GetResponseCode()));
        if (out.code.empty()) {
            out.code = status == 0 ? "NoResponse" : "HTTP" + std::to_string(status);
        }
        out.fault = classify_kinesis_error(call, out.code, status, e.ShouldRetry());
        return out;
    }

    std::string stream_;
    int list_page_size_{0};
    std::unique_ptr<Aws::Kinesis::KinesisClient> client_;
    std::atomic<bool> aborted_{false};
};

}  // namespace

std::shared_ptr<KinesisShardService> make_kinesis_sdk_service(const std::string& stream,
                                                              const AwsClientOptions& client,
                                                              int list_page_size) {
    return std::make_shared<KinesisSdkService>(stream, client, list_page_size);
}

}  // namespace clink::aws
