#pragma once

// A model of PutRecords for the Kinesis sink's deterministic tests. It keeps
// every record it stores, with its partition key, in order and duplicates
// included, as what a reader of the stream would see, and refuses what the
// service refuses for a request's shape: no records or more than 500, a
// partition key of no characters or more than 256 (counted in UTF-16 code
// units, by its own count), a record whose data and key exceed the stream's
// maximum record size, more than 10 MiB of data and keys. A plan scripts each
// call: which records to store and which to fail, an error to answer with
// (before or after storing, the second being a reply lost after the service
// took the request), a hand-written response, or a call that hangs until
// abort().

#include <condition_variable>
#include <cstddef>
#include <cstdio>
#include <functional>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "clink/aws/kinesis_sink.hpp"

namespace clink::aws::test {

class KinesisSinkModel final : public KinesisWriteService {
public:
    struct Stored {
        std::string data;
        std::string key;
        std::string shard;
        std::string sequence;
    };
    struct Plan {
        // Store none of the call's records.
        bool store_none{false};
        // When not empty: which of the call's records the service stores.
        std::vector<bool> store;
        // The ErrorCode of an entry that failed.
        std::string failure_code{"ProvisionedThroughputExceededException"};
        // An error to answer with instead of a response.
        std::optional<KinesisPutError> error;
        // A response to answer with instead of the one the storing gives.
        std::optional<KinesisPutResult> response;
        // Block until abort(), then answer Cancelled; nothing is stored.
        bool hang{false};
    };
    using Planner = std::function<Plan(std::size_t call, const std::vector<KinesisPutRecord>&)>;
    using Sent = std::vector<std::pair<std::string, std::string>>;  // (data, key) per record

    explicit KinesisSinkModel(std::string stream,
                              std::size_t max_record_bytes = 1024 * 1024,
                              std::size_t shards = 4)
        : stream_(std::move(stream)), max_record_bytes_(max_record_bytes), shards_(shards) {}

    void set_planner(Planner p) {
        const std::lock_guard<std::mutex> lock(mu_);
        planner_ = std::move(p);
    }

    // Store every record but those at `failing`, which fail with `code`.
    static Plan failing(std::size_t n,
                        const std::set<std::size_t>& failing,
                        std::string code = "ProvisionedThroughputExceededException") {
        Plan p;
        p.store.assign(n, true);
        for (const auto i : failing) {
            p.store[i] = false;
        }
        p.failure_code = std::move(code);
        return p;
    }
    // An error answer; `stored` says whether the records were stored first (a
    // reply lost after the service took the request).
    static Plan error(std::string code,
                      int http_status,
                      KinesisPutFault fault,
                      bool stored = false,
                      std::string message = "scripted") {
        Plan p;
        p.error = KinesisPutError{fault, std::move(code), std::move(message), http_status};
        p.store_none = !stored;
        return p;
    }

    // What each PutRecords carried, in call order.
    std::vector<Sent> calls() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return calls_;
    }
    // Every record stored, in order, duplicates included.
    std::vector<Stored> stored() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return stored_;
    }
    // The stored records' (data, key) pairs.
    Sent stored_pairs() const {
        Sent out;
        for (const auto& s : stored()) {
            out.emplace_back(s.data, s.key);
        }
        return out;
    }
    bool aborted() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return aborted_;
    }
    bool hanging() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return hanging_;
    }

    // UTF-16 code units of a UTF-8 string, by the model's own count: one per
    // code point, two for one past U+FFFF (a four-byte sequence).
    static std::size_t utf16_units(const std::string& s) {
        std::size_t units = 0;
        for (const char c : s) {
            const auto b = static_cast<unsigned char>(c);
            if ((b & 0xC0) != 0x80) {
                units += b >= 0xF0 ? 2 : 1;
            }
        }
        return units;
    }

    KinesisPutResult put_records(const std::string& stream,
                                 const std::vector<KinesisPutRecord>& records) override {
        std::unique_lock<std::mutex> lock(mu_);
        const auto call = calls_.size();
        Sent copy;
        for (const auto& r : records) {
            copy.emplace_back(std::string{r.data}, std::string{r.partition_key});
        }
        calls_.push_back(copy);
        KinesisPutResult out;
        if (aborted_) {
            out.error = {KinesisPutFault::Cancelled, "Cancelled", "aborted", 0};
            return out;
        }
        if (stream != stream_) {
            out.error = {KinesisPutFault::Permanent,
                         "ResourceNotFoundException",
                         "Stream " + stream + " under account 111111111111 not found.",
                         400};
            return out;
        }
        if (records.empty() || records.size() > 500) {
            out.error = {KinesisPutFault::Permanent,
                         "InvalidArgumentException",
                         "1 validation error detected: Records must have 1 to 500 members",
                         400};
            return out;
        }
        std::size_t total = 0;
        for (std::size_t i = 0; i < copy.size(); ++i) {
            const auto& [data, key] = copy[i];
            const auto units = utf16_units(key);
            if (units < 1 || units > 256) {
                out.error = {KinesisPutFault::Permanent,
                             "ValidationException",
                             "1 validation error detected: Value at 'records." +
                                 std::to_string(i + 1) +
                                 ".member.partitionKey' failed to satisfy constraint: Member must "
                                 "have length between 1 and 256",
                             400};
                return out;
            }
            if (data.size() + key.size() > max_record_bytes_) {
                out.error = {KinesisPutFault::Permanent,
                             "InvalidArgumentException",
                             "Record size exceeds the stream's maximum record size",
                             400};
                return out;
            }
            total += data.size() + key.size();
        }
        if (total > 10 * 1024 * 1024) {
            out.error = {KinesisPutFault::Permanent,
                         "InvalidArgumentException",
                         "Records size exceeds 10 MB limit",
                         400};
            return out;
        }
        Plan plan = planner_ ? planner_(call, records) : Plan{};
        if (plan.hang) {
            hanging_ = true;
            cv_.wait(lock, [this] { return aborted_; });
            hanging_ = false;
            out.error = {KinesisPutFault::Cancelled, "Cancelled", "aborted", 0};
            return out;
        }
        for (std::size_t i = 0; i < copy.size(); ++i) {
            const bool store = !plan.store_none &&
                               (plan.store.empty() || (i < plan.store.size() && plan.store[i]));
            if (store) {
                const auto shard = shard_of(copy[i].second);
                const auto sequence = next_sequence();
                stored_.push_back(Stored{copy[i].first, copy[i].second, shard, sequence});
                out.entries.push_back({sequence, shard, "", ""});
            } else {
                out.entries.push_back({"",
                                       "",
                                       plan.failure_code,
                                       plan.failure_code == "InternalFailure"
                                           ? "Internal service failure."
                                           : "Rate exceeded for shard " + shard_of(copy[i].second) +
                                                 " in stream " + stream_ +
                                                 " under account 111111111111."});
                ++out.failed_record_count;
            }
        }
        if (plan.error) {
            KinesisPutResult err;
            err.error = *plan.error;
            return err;
        }
        if (plan.response) {
            return *plan.response;
        }
        return out;
    }

    void abort() noexcept override {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            aborted_ = true;
        }
        cv_.notify_all();
    }

private:
    std::string shard_of(const std::string& key) const {
        char id[32];
        std::snprintf(id, sizeof id, "shardId-%012zu", std::hash<std::string>{}(key) % shards_);
        return id;
    }

    std::string next_sequence() {
        char seq[48];
        std::snprintf(seq, sizeof seq, "4960%020zu", next_sequence_++);
        return seq;
    }

    std::string stream_;
    std::size_t max_record_bytes_;
    std::size_t shards_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    Planner planner_;
    std::vector<Sent> calls_;
    std::vector<Stored> stored_;
    std::size_t next_sequence_{1};
    bool aborted_{false};
    bool hanging_{false};
};

}  // namespace clink::aws::test
