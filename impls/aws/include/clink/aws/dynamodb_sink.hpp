#pragma once

// DynamoDB sink (BatchWriteItem). SDK-dependent; compiled only with the AWS SDK.
//
// Each input record is a JSON object string (e.g. SQL row_to_json_string) mapped
// to a DynamoDB item (PutRequest). Delivery is AT-LEAST-ONCE but EFFECTIVELY-ONCE
// when the primary key is stable and deterministic from the record: PutItem is an
// UPSERT, so re-delivery on replay overwrites the same item rather than
// duplicating. The batch is deduped by primary key (DynamoDB rejects a batch with
// two items sharing a key) last-write-wins, which matches the upsert model.

#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/AttributeValue.h>
#include <aws/dynamodb/model/BatchWriteItemRequest.h>
#include <aws/dynamodb/model/PutRequest.h>
#include <aws/dynamodb/model/WriteRequest.h>

#include "clink/aws/aws_client.hpp"
#include "clink/config/json.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::aws {

// A JSON number -> DynamoDB N (carried as a string). Integral and within int64
// range renders as a plain integer (the range guard avoids the float->int64 UB
// and the INT64_MAX saturation collapse); otherwise the shortest round-trip
// double text. (DynamoDB N cannot be NaN/Inf, which valid JSON never produces.)
inline std::string ddb_number_text(double d) {
    constexpr double kInt64Lo = -9223372036854775808.0;          // -2^63
    constexpr double kInt64HiExclusive = 9223372036854775808.0;  //  2^63
    if (std::isfinite(d) && d >= kInt64Lo && d < kInt64HiExclusive &&
        d == static_cast<double>(static_cast<std::int64_t>(d))) {
        return std::to_string(static_cast<std::int64_t>(d));
    }
    char buf[64];
    auto res = std::to_chars(buf, buf + sizeof(buf), d);
    return std::string(buf, res.ptr);
}

// Map a JSON value to a DynamoDB AttributeValue (S/N/BOOL/NULL/M/L). Empty
// strings are kept (DynamoDB allows them on NON-key attributes since 2020; an
// empty KEY value is rejected by DynamoDB with a clear error).
inline Aws::DynamoDB::Model::AttributeValue json_to_attribute_value(
    const clink::config::JsonValue& v) {
    using AV = Aws::DynamoDB::Model::AttributeValue;
    AV av;
    if (v.is_null()) {
        av.SetNull(true);
    } else if (v.is_bool()) {
        av.SetBool(v.as_bool());
    } else if (v.is_number()) {
        av.SetN(ddb_number_text(v.as_number()));
    } else if (v.is_string()) {
        av.SetS(v.as_string());
    } else if (v.is_array()) {
        // L (list): preserves heterogeneity and order. Built via shared_ptr
        // entries, which is the SDK's AddLItem signature.
        for (const auto& e : v.as_array()) {
            av.AddLItem(Aws::MakeShared<AV>("clink-ddb", json_to_attribute_value(e)));
        }
        // An empty array still needs the L type set, not left as an unset AV.
        if (v.as_array().empty()) {
            av.SetL(Aws::Vector<std::shared_ptr<AV>>{});
        }
    } else if (v.is_object()) {
        for (const auto& [k, val] : v.as_object()) {
            av.AddMEntry(k, Aws::MakeShared<AV>("clink-ddb", json_to_attribute_value(val)));
        }
        if (v.as_object().empty()) {
            av.SetM(Aws::Map<Aws::String, const std::shared_ptr<AV>>{});
        }
    }
    return av;
}

// Build the DynamoDB item map from a parsed JSON object.
inline Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> json_object_to_item(
    const clink::config::JsonObject& obj) {
    Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> item;
    for (const auto& [k, v] : obj) {
        item.emplace(k, json_to_attribute_value(v));
    }
    return item;
}

struct DynamoDbSinkOptions {
    std::string table;          // required
    std::string partition_key;  // required - attribute name present in every record
    std::string sort_key;       // optional - composite-key tables
    AwsClientOptions client;
    std::size_t batch_records{25};  // BatchWriteItem hard max is 25
    int max_retries{8};             // UnprocessedItems resubmit attempts
    std::chrono::milliseconds retry_base_backoff{100};
    std::string name{"dynamodb_sink"};
};

class DynamoDbSink : public Sink<std::string> {
public:
    explicit DynamoDbSink(DynamoDbSinkOptions opts) : opts_(std::move(opts)) {
        if (opts_.table.empty()) {
            throw std::runtime_error(opts_.name + ": 'table' is required");
        }
        if (opts_.partition_key.empty()) {
            throw std::runtime_error(opts_.name + ": 'partition_key' is required");
        }
        if (opts_.batch_records == 0 || opts_.batch_records > 25) {
            opts_.batch_records = 25;  // DynamoDB BatchWriteItem ceiling
        }
    }

    void open() override {
        ensure_aws_initialized();
        client_ = std::make_unique<Aws::DynamoDB::DynamoDBClient>(make_client_config(opts_.client));
        pending_.clear();
    }

    void on_data(const Batch<std::string>& batch) override {
        for (const auto& rec : batch) {
            ingest_(rec.value());
            if (pending_.size() >= opts_.batch_records) {
                flush();
            }
        }
    }

    void on_barrier(CheckpointBarrier /*b*/) override { flush(); }

    void flush() override {
        if (pending_.empty()) {
            return;
        }
        // Emit in chunks of <= batch_records; pending_ is already key-deduped so
        // no chunk carries two items with the same primary key.
        std::vector<Aws::DynamoDB::Model::WriteRequest> chunk;
        chunk.reserve(opts_.batch_records);
        for (auto& [key, item] : pending_) {
            Aws::DynamoDB::Model::PutRequest put;
            put.SetItem(std::move(item));
            Aws::DynamoDB::Model::WriteRequest wr;
            wr.SetPutRequest(std::move(put));
            chunk.push_back(std::move(wr));
            if (chunk.size() >= opts_.batch_records) {
                write_with_retry_(std::move(chunk));
                chunk.clear();
                chunk.reserve(opts_.batch_records);
            }
        }
        if (!chunk.empty()) {
            write_with_retry_(std::move(chunk));
        }
        pending_.clear();
    }

    std::string name() const override { return opts_.name; }

private:
    void ingest_(const std::string& rec) {
        clink::config::JsonValue j;
        try {
            j = clink::config::parse(rec);
        } catch (...) {
            throw std::runtime_error(opts_.name + ": record is not valid JSON");
        }
        if (!j.is_object()) {
            throw std::runtime_error(opts_.name + ": record must be a JSON object");
        }
        const auto& obj = j.as_object();
        std::string key = scalar_key_(obj, opts_.partition_key);
        if (!opts_.sort_key.empty()) {
            key.push_back('\x00');
            key += scalar_key_(obj, opts_.sort_key);
        }
        pending_[key] = json_object_to_item(obj);  // dedup: last write wins
    }

    // Extract a record field's scalar value as a dedup-key string. The key
    // attribute must be present and non-empty (DynamoDB rejects an empty/missing
    // key value); fail fast with a clear message rather than an opaque
    // ValidationException from the service.
    std::string scalar_key_(const clink::config::JsonObject& obj, const std::string& field) {
        auto it = obj.find(field);
        if (it == obj.end() || it->second.is_null()) {
            throw std::runtime_error(opts_.name + ": key attribute '" + field +
                                     "' is missing or null in the record");
        }
        const auto& v = it->second;
        std::string s;
        if (v.is_string()) {
            s = v.as_string();
        } else if (v.is_number()) {
            s = ddb_number_text(v.as_number());
        } else if (v.is_bool()) {
            s = v.as_bool() ? "true" : "false";
        } else {
            throw std::runtime_error(opts_.name + ": key attribute '" + field +
                                     "' must be a scalar (string/number/bool)");
        }
        if (s.empty()) {
            throw std::runtime_error(opts_.name + ": key attribute '" + field + "' is empty");
        }
        return s;
    }

    // BatchWriteItem + the documented UnprocessedItems loop: resubmit only the
    // items the service did not process, with exponential backoff, until the map
    // is empty. Throws on a whole-request failure or when attempts are exhausted
    // so the job replays from the last checkpoint (at-least-once, no silent drop).
    void write_with_retry_(std::vector<Aws::DynamoDB::Model::WriteRequest> writes) {
        Aws::Map<Aws::String, Aws::Vector<Aws::DynamoDB::Model::WriteRequest>> request_items;
        request_items[opts_.table] =
            Aws::Vector<Aws::DynamoDB::Model::WriteRequest>(std::move(writes));

        for (int attempt = 0; attempt <= opts_.max_retries; ++attempt) {
            if (attempt > 0) {
                auto backoff = opts_.retry_base_backoff * (1u << (attempt - 1));
                constexpr std::chrono::milliseconds kMaxBackoff{30000};
                if (backoff > kMaxBackoff) {
                    backoff = kMaxBackoff;
                }
                std::this_thread::sleep_for(backoff);
            }
            Aws::DynamoDB::Model::BatchWriteItemRequest req;
            req.SetRequestItems(request_items);
            auto outcome = client_->BatchWriteItem(req);
            if (!outcome.IsSuccess()) {
                // The SDK's retry strategy already retried transient throttles;
                // a surfaced failure is either non-retryable (validation / bad
                // table) or persistent. Fail loudly.
                throw std::runtime_error(opts_.name + ": BatchWriteItem failed: " +
                                         std::string(outcome.GetError().GetMessage().c_str()));
            }
            request_items = outcome.GetResult().GetUnprocessedItems();
            if (request_items.empty()) {
                return;  // all items written
            }
        }
        throw std::runtime_error(opts_.name + ": BatchWriteItem left items unprocessed after " +
                                 std::to_string(opts_.max_retries + 1) + " attempts");
    }

    DynamoDbSinkOptions opts_;
    std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client_;
    // primary-key string -> item; dedups a batch (DynamoDB rejects duplicate
    // keys in one BatchWriteItem) last-write-wins.
    std::map<std::string, Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue>> pending_;
};

}  // namespace clink::aws
