#pragma once

// A model of the two DynamoDB calls the sink makes, DescribeTable and
// BatchWriteItem, for the sink's deterministic tests. The table keeps items by
// key and refuses what the service refuses for a request's shape: more than 25
// writes, two writes to one item, a key attribute missing, empty or of the wrong
// type, a number DynamoDB cannot hold. Its key equality is its own (numbers
// normalised here with a regular expression, keys encoded with their type and
// length), not the sink's, so a fault in the sink's identity shows up as items
// the model refuses or keeps apart.
//
// A plan scripts each BatchWriteItem: which of its items to apply, and what to
// answer (UnprocessedItems computed from the items not applied, an error, a
// hand-written response), or to hang until abort().

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <regex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "clink/aws/dynamodb_sink.hpp"

namespace clink::aws::test {

class DynamoDbModel final : public DynamoDbWriteService {
public:
    struct Plan {
        // Apply none of the call's items.
        bool apply_none{false};
        // When not empty: which of the call's items the service applies.
        std::vector<bool> apply;
        // An error to answer with instead of a response.
        std::optional<DynamoDbCallError> error;
        // A response's UnprocessedItems to answer with instead of the items not
        // applied (for malformed and inconsistent responses).
        std::optional<std::vector<DynamoDbUnprocessed>> unprocessed;
        // Block until abort(), then answer Cancelled; nothing is applied.
        bool hang{false};
    };
    using Planner = std::function<Plan(std::size_t call, const std::vector<const DynamoDbItem*>&)>;

    DynamoDbModel(std::string table, DynamoDbKeySchema key)
        : table_(std::move(table)), key_(std::move(key)) {}

    // ---- scripting ----

    void set_planner(Planner p) {
        const std::lock_guard<std::mutex> lock(mu_);
        planner_ = std::move(p);
    }
    void set_status(std::string s) {
        const std::lock_guard<std::mutex> lock(mu_);
        status_ = std::move(s);
    }
    // DescribeTable answers these errors in turn, then succeeds.
    void fail_describe(std::vector<DynamoDbCallError> errors) {
        const std::lock_guard<std::mutex> lock(mu_);
        describe_errors_ = std::move(errors);
    }

    static Plan unprocessed(std::size_t n, std::set<std::size_t> held) {
        Plan p;
        p.apply.assign(n, true);
        for (const auto i : held) {
            p.apply[i] = false;
        }
        return p;
    }
    // An error answer; `applied` says whether the items were written first (a
    // reply lost after the service applied the request).
    static Plan error(std::string code,
                      int http_status,
                      DynamoDbFault fault,
                      bool applied = false,
                      std::string message = "scripted") {
        Plan p;
        p.error = DynamoDbCallError{fault, std::move(code), std::move(message), http_status};
        p.apply_none = !applied;
        return p;
    }

    // ---- inspection ----

    // What each BatchWriteItem carried, in call order.
    std::vector<std::vector<DynamoDbItem>> calls() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return calls_;
    }
    std::size_t describe_calls() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return describe_calls_;
    }
    // The table's items in key order.
    std::vector<DynamoDbItem> items() const {
        const std::lock_guard<std::mutex> lock(mu_);
        std::vector<DynamoDbItem> out;
        for (const auto& [k, item] : table_items_) {
            (void)k;
            out.push_back(item);
        }
        return out;
    }
    std::optional<DynamoDbItem> item(
        const DynamoDbValue& partition,
        const std::optional<DynamoDbValue>& sort = std::nullopt) const {
        const std::lock_guard<std::mutex> lock(mu_);
        std::string why;
        const auto k = encode_key(partition, sort, why);
        if (!k) {
            return std::nullopt;
        }
        const auto it = table_items_.find(*k);
        return it == table_items_.end() ? std::nullopt : std::optional<DynamoDbItem>{it->second};
    }
    bool aborted() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return aborted_;
    }
    bool hanging() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return hanging_;
    }

    std::string arn() const { return "arn:aws:dynamodb:us-east-1:123456789012:table/" + table_; }

    // ---- the service ----

    DynamoDbTableDescription describe_table(const std::string& table) override {
        const std::lock_guard<std::mutex> lock(mu_);
        ++describe_calls_;
        DynamoDbTableDescription d;
        if (!describe_errors_.empty()) {
            d.error = describe_errors_.front();
            describe_errors_.erase(describe_errors_.begin());
            return d;
        }
        if (table != table_ && table != arn()) {
            d.error = {DynamoDbFault::Permanent,
                       "ResourceNotFoundException",
                       "Requested resource not found: Table: " + table + " not found",
                       400};
            return d;
        }
        d.status = status_;
        d.key = key_;
        d.name = table_;
        d.arn = arn();
        return d;
    }

    DynamoDbBatchResult batch_write(const std::string& table,
                                    const std::vector<const DynamoDbItem*>& items) override {
        std::unique_lock<std::mutex> lock(mu_);
        const auto call = calls_.size();
        std::vector<DynamoDbItem> copy;
        for (const auto* i : items) {
            copy.push_back(*i);
        }
        calls_.push_back(copy);
        DynamoDbBatchResult out;
        if (aborted_) {
            out.error = {DynamoDbFault::Cancelled, "Cancelled", "aborted", 0};
            return out;
        }
        if (table != table_ && table != arn()) {
            out.error = {DynamoDbFault::Permanent,
                         "ResourceNotFoundException",
                         "Requested resource not found",
                         400};
            return out;
        }
        // The service's own refusals, before any plan.
        if (items.empty() || items.size() > 25) {
            out.error = {DynamoDbFault::Permanent,
                         "ValidationException",
                         "Too many items requested for the BatchWriteItem call",
                         400};
            return out;
        }
        std::vector<std::string> keys;
        for (const auto* i : items) {
            std::string why;
            auto k = key_of(*i, why);
            if (!k) {
                out.error = {DynamoDbFault::Permanent, "ValidationException", why, 400};
                return out;
            }
            for (const auto& [name, value] : *i) {
                (void)name;
                if (!numbers_valid(value)) {
                    out.error = {DynamoDbFault::Permanent,
                                 "ValidationException",
                                 "a number is not one DynamoDB can hold",
                                 400};
                    return out;
                }
            }
            keys.push_back(std::move(*k));
        }
        if (std::set<std::string>(keys.begin(), keys.end()).size() != keys.size()) {
            out.error = {DynamoDbFault::Permanent,
                         "ValidationException",
                         "Provided list of item keys contains duplicates",
                         400};
            return out;
        }
        Plan plan = planner_ ? planner_(call, items) : Plan{};
        if (plan.hang) {
            hanging_ = true;
            cv_.wait(lock, [this] { return aborted_; });
            hanging_ = false;
            out.error = {DynamoDbFault::Cancelled, "Cancelled", "aborted", 0};
            return out;
        }
        std::vector<DynamoDbUnprocessed> not_applied;
        for (std::size_t i = 0; i < items.size(); ++i) {
            const bool apply = !plan.apply_none &&
                               (plan.apply.empty() || (i < plan.apply.size() && plan.apply[i]));
            if (apply) {
                table_items_[keys[i]] = *items[i];
            } else {
                not_applied.push_back(
                    DynamoDbUnprocessed{table, DynamoDbUnprocessed::Kind::Put, *items[i], true});
            }
        }
        if (plan.error) {
            out.error = *plan.error;
            return out;
        }
        out.unprocessed = plan.unprocessed ? *plan.unprocessed : std::move(not_applied);
        return out;
    }

    void abort() noexcept override {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            aborted_ = true;
        }
        cv_.notify_all();
    }

    // The model's own number normalisation: sign, digits without leading or
    // trailing zeros, and the exponent of the last digit, as one string.
    static std::optional<std::string> normal_number(const std::string& text) {
        static const std::regex re(R"(^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$)");
        std::smatch m;
        if (!std::regex_match(text, m, re) || (m[2].length() + m[3].length()) == 0) {
            return std::nullopt;
        }
        const std::string digits = m[2].str() + m[3].str();
        long long exp = m[4].matched ? std::stoll(m[4].str()) : 0;
        exp -= static_cast<long long>(m[3].length());
        const auto lo = digits.find_first_not_of('0');
        if (lo == std::string::npos) {
            return std::string{"0"};
        }
        const auto hi = digits.find_last_not_of('0');
        exp += static_cast<long long>(digits.size() - 1 - hi);
        const std::string sig = digits.substr(lo, hi - lo + 1);
        if (sig.size() > 38) {
            return std::nullopt;
        }
        const long long sci = exp + static_cast<long long>(sig.size()) - 1;
        if (sci < -130 || sci > 125) {
            return std::nullopt;
        }
        return (m[1].str() == "-" ? std::string{"-"} : std::string{}) + sig + "e" +
               std::to_string(exp);
    }

private:
    static bool numbers_valid(const DynamoDbValue& v) {
        if (v.kind == DynamoDbValue::Kind::N) {
            return normal_number(v.text).has_value();
        }
        for (const auto& [name, m] : v.members) {
            (void)name;
            if (!numbers_valid(m)) {
                return false;
            }
        }
        return true;
    }

    std::optional<std::string> encode_one(const DynamoDbKeyAttribute& attr,
                                          const DynamoDbValue& v,
                                          std::size_t max,
                                          std::string& why) const {
        if (attr.type == DynamoDbKeyType::S) {
            if (v.kind != DynamoDbValue::Kind::S) {
                why = "One or more parameter values were invalid: Type mismatch for key";
                return std::nullopt;
            }
            if (v.text.empty() || v.text.size() > max) {
                why = "One or more parameter values are not valid: key length";
                return std::nullopt;
            }
            return "S" + std::to_string(v.text.size()) + ":" + v.text;
        }
        if (v.kind != DynamoDbValue::Kind::N) {
            why = "One or more parameter values were invalid: Type mismatch for key";
            return std::nullopt;
        }
        const auto n = normal_number(v.text);
        if (!n) {
            why = "a key number is not one DynamoDB can hold";
            return std::nullopt;
        }
        return "N" + std::to_string(n->size()) + ":" + *n;
    }

    std::optional<std::string> encode_key(const DynamoDbValue& partition,
                                          const std::optional<DynamoDbValue>& sort,
                                          std::string& why) const {
        auto p = encode_one(key_.partition, partition, 2048, why);
        if (!p) {
            return std::nullopt;
        }
        if (!key_.sort) {
            return p;
        }
        if (!sort) {
            why = "One of the required keys was not given a value";
            return std::nullopt;
        }
        auto s = encode_one(*key_.sort, *sort, 1024, why);
        if (!s) {
            return std::nullopt;
        }
        return *p + "|" + *s;
    }

    std::optional<std::string> key_of(const DynamoDbItem& item, std::string& why) const {
        const auto find = [&](const std::string& name) -> const DynamoDbValue* {
            for (const auto& [n, v] : item) {
                if (n == name) {
                    return &v;
                }
            }
            return nullptr;
        };
        const auto* p = find(key_.partition.name);
        if (p == nullptr) {
            why = "One of the required keys was not given a value";
            return std::nullopt;
        }
        std::optional<DynamoDbValue> s;
        if (key_.sort) {
            const auto* sv = find(key_.sort->name);
            if (sv == nullptr) {
                why = "One of the required keys was not given a value";
                return std::nullopt;
            }
            s = *sv;
        }
        return encode_key(*p, s, why);
    }

    std::string table_;
    DynamoDbKeySchema key_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::string status_{"ACTIVE"};
    std::vector<DynamoDbCallError> describe_errors_;
    std::size_t describe_calls_{0};
    Planner planner_;
    std::vector<std::vector<DynamoDbItem>> calls_;
    std::map<std::string, DynamoDbItem> table_items_;
    bool aborted_{false};
    bool hanging_{false};
};

}  // namespace clink::aws::test
