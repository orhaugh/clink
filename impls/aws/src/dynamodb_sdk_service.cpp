// The DynamoDB sink's calls on the AWS SDK. The SDK's own retries are off, so
// every failure reaches the sink, which classifies it (classify_dynamodb_error),
// paces its retries and bounds them.

#include <algorithm>
#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/AttributeValue.h>
#include <aws/dynamodb/model/BatchWriteItemRequest.h>
#include <aws/dynamodb/model/DescribeTableRequest.h>
#include <aws/dynamodb/model/PutRequest.h>
#include <aws/dynamodb/model/TableStatus.h>
#include <aws/dynamodb/model/WriteRequest.h>

#include "clink/aws/aws_client.hpp"
#include "clink/aws/dynamodb_sink.hpp"

namespace clink::aws {

namespace {

using Aws::DynamoDB::Model::AttributeValue;
using Aws::DynamoDB::Model::ValueType;

AttributeValue to_sdk(const DynamoDbValue& v) {
    AttributeValue av;
    switch (v.kind) {
        case DynamoDbValue::Kind::S:
            av.SetS(v.text);
            break;
        case DynamoDbValue::Kind::N:
            av.SetN(v.text);
            break;
        case DynamoDbValue::Kind::Bool:
            av.SetBool(v.flag);
            break;
        case DynamoDbValue::Kind::Null:
            av.SetNull(true);
            break;
        case DynamoDbValue::Kind::M:
            for (const auto& [name, member] : v.members) {
                av.AddMEntry(name, Aws::MakeShared<AttributeValue>("clink-ddb", to_sdk(member)));
            }
            if (v.members.empty()) {
                av.SetM(Aws::Map<Aws::String, const std::shared_ptr<AttributeValue>>{});
            }
            break;
        case DynamoDbValue::Kind::L:
            for (const auto& [name, member] : v.members) {
                (void)name;
                av.AddLItem(Aws::MakeShared<AttributeValue>("clink-ddb", to_sdk(member)));
            }
            if (v.members.empty()) {
                av.SetL(Aws::Vector<std::shared_ptr<AttributeValue>>{});
            }
            break;
    }
    return av;
}

// A value the service returned. `recognised` turns false on a type the sink
// never writes: binary, the sets, or none at all.
DynamoDbValue from_sdk(const AttributeValue& av, bool& recognised) {
    DynamoDbValue v;
    switch (av.GetType()) {
        case ValueType::STRING:
            v.kind = DynamoDbValue::Kind::S;
            v.text = av.GetS();
            break;
        case ValueType::NUMBER:
            v.kind = DynamoDbValue::Kind::N;
            v.text = av.GetN();
            break;
        case ValueType::BOOL:
            v.kind = DynamoDbValue::Kind::Bool;
            v.flag = av.GetBool();
            break;
        case ValueType::NULLVALUE:
            v.kind = DynamoDbValue::Kind::Null;
            break;
        case ValueType::ATTRIBUTE_MAP:
            v.kind = DynamoDbValue::Kind::M;
            for (const auto& [name, member] : av.GetM()) {
                v.members.emplace_back(name, from_sdk(*member, recognised));
            }
            std::sort(v.members.begin(), v.members.end(), [](const auto& a, const auto& b) {
                return a.first < b.first;
            });
            break;
        case ValueType::ATTRIBUTE_LIST:
            v.kind = DynamoDbValue::Kind::L;
            for (const auto& member : av.GetL()) {
                v.members.emplace_back(std::string{}, from_sdk(*member, recognised));
            }
            break;
        default:
            recognised = false;
            break;
    }
    return v;
}

DynamoDbItem item_from_sdk(const Aws::Map<Aws::String, AttributeValue>& attributes,
                           bool& recognised) {
    DynamoDbItem item;
    for (const auto& [name, value] : attributes) {
        item.emplace_back(name, from_sdk(value, recognised));
    }
    std::sort(
        item.begin(), item.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    return item;
}

// The SDK's DynamoDB client, with a way to end its calls from another thread.
// DisableRequestProcessing is protected on the smithy-based client in some SDK
// releases (1.11.795, the toolchain image's) and public in others (1.11.840).
class AbortableDynamoDbClient final : public Aws::DynamoDB::DynamoDBClient {
public:
    using Aws::DynamoDB::DynamoDBClient::DynamoDBClient;
    void end_requests() { DisableRequestProcessing(); }
};

class DynamoDbSdkService final : public DynamoDbWriteService {
public:
    explicit DynamoDbSdkService(const AwsClientOptions& options) {
        ensure_aws_initialized();
        auto cfg = make_client_config(options);
        cfg.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(0);
        client_ = std::make_unique<AbortableDynamoDbClient>(cfg);
    }

    DynamoDbTableDescription describe_table(const std::string& table) override {
        DynamoDbTableDescription out;
        Aws::DynamoDB::Model::DescribeTableRequest req;
        req.SetTableName(table);
        auto outcome = client_->DescribeTable(req);
        if (!outcome.IsSuccess()) {
            out.error = error_of(outcome.GetError());
            return out;
        }
        const auto& t = outcome.GetResult().GetTable();
        out.status =
            Aws::DynamoDB::Model::TableStatusMapper::GetNameForTableStatus(t.GetTableStatus());
        out.name = t.GetTableName();
        out.arn = t.GetTableArn();
        const auto type_of = [&](const Aws::String& name) {
            for (const auto& def : t.GetAttributeDefinitions()) {
                if (def.GetAttributeName() == name) {
                    switch (def.GetAttributeType()) {
                        case Aws::DynamoDB::Model::ScalarAttributeType::N:
                            return DynamoDbKeyType::N;
                        case Aws::DynamoDB::Model::ScalarAttributeType::B:
                            return DynamoDbKeyType::B;
                        default:
                            return DynamoDbKeyType::S;
                    }
                }
            }
            return DynamoDbKeyType::S;
        };
        for (const auto& k : t.GetKeySchema()) {
            DynamoDbKeyAttribute attr{k.GetAttributeName(), type_of(k.GetAttributeName())};
            if (k.GetKeyType() == Aws::DynamoDB::Model::KeyType::RANGE) {
                out.key.sort = std::move(attr);
            } else {
                out.key.partition = std::move(attr);
            }
        }
        return out;
    }

    DynamoDbBatchResult batch_write(const std::string& table,
                                    const std::vector<const DynamoDbItem*>& items) override {
        DynamoDbBatchResult out;
        Aws::Vector<Aws::DynamoDB::Model::WriteRequest> writes;
        writes.reserve(items.size());
        for (const auto* item : items) {
            Aws::DynamoDB::Model::PutRequest put;
            for (const auto& [name, value] : *item) {
                put.AddItem(name, to_sdk(value));
            }
            Aws::DynamoDB::Model::WriteRequest w;
            w.SetPutRequest(std::move(put));
            writes.push_back(std::move(w));
        }
        Aws::DynamoDB::Model::BatchWriteItemRequest req;
        req.AddRequestItems(table, std::move(writes));
        auto outcome = client_->BatchWriteItem(req);
        if (!outcome.IsSuccess()) {
            out.error = error_of(outcome.GetError());
            return out;
        }
        for (const auto& [name, list] : outcome.GetResult().GetUnprocessedItems()) {
            for (const auto& w : list) {
                DynamoDbUnprocessed u;
                u.table = name;
                if (w.PutRequestHasBeenSet() && !w.DeleteRequestHasBeenSet()) {
                    u.kind = DynamoDbUnprocessed::Kind::Put;
                    u.item = item_from_sdk(w.GetPutRequest().GetItem(), u.recognised);
                } else if (w.DeleteRequestHasBeenSet() && !w.PutRequestHasBeenSet()) {
                    u.kind = DynamoDbUnprocessed::Kind::Delete;
                    u.item = item_from_sdk(w.GetDeleteRequest().GetKey(), u.recognised);
                } else {
                    u.kind = DynamoDbUnprocessed::Kind::Neither;
                }
                out.unprocessed.push_back(std::move(u));
            }
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
    DynamoDbCallError error_of(const Error& e) const {
        DynamoDbCallError out;
        if (aborted_.load(std::memory_order_relaxed)) {
            out.fault = DynamoDbFault::Cancelled;
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
        out.fault = classify_dynamodb_error(out.code, out.http_status, e.ShouldRetry());
        return out;
    }

    std::unique_ptr<AbortableDynamoDbClient> client_;
    std::atomic<bool> aborted_{false};
};

}  // namespace

std::shared_ptr<DynamoDbWriteService> make_dynamodb_sdk_service(const AwsClientOptions& client) {
    return std::make_shared<DynamoDbSdkService>(client);
}

}  // namespace clink::aws
