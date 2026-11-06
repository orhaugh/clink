#pragma once

// A model of PutRecordBatch for the Firehose sink's deterministic tests. It keeps
// every record it accepts, in order and duplicates included, as what the delivery
// stream would pass on, and refuses what the service refuses for a request's
// shape: no records or more than 500, a record over 1,024,000 bytes, more than
// 4 MiB of record data. A plan scripts each call: which records to accept and
// which to fail, an error to answer with (before or after accepting, the second
// being a reply lost after acceptance), a hand-written response, or a call that
// hangs until abort().

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "clink/aws/firehose_sink.hpp"

namespace clink::aws::test {

class FirehoseModel final : public FirehoseWriteService {
public:
    struct Plan {
        // Accept none of the call's records.
        bool accept_none{false};
        // When not empty: which of the call's records the service accepts.
        std::vector<bool> accept;
        // An error to answer with instead of a response.
        std::optional<FirehoseCallError> error;
        // A response to answer with instead of the one the acceptance gives.
        std::optional<FirehoseBatchResult> response;
        // Block until abort(), then answer Cancelled; nothing is accepted.
        bool hang{false};
    };
    using Planner = std::function<Plan(std::size_t call, const std::vector<const std::string*>&)>;

    explicit FirehoseModel(std::string stream) : stream_(std::move(stream)) {}

    void set_planner(Planner p) {
        const std::lock_guard<std::mutex> lock(mu_);
        planner_ = std::move(p);
    }

    // Accept every record but those at `failing`, which fail with
    // ServiceUnavailableException.
    static Plan failing(std::size_t n, std::set<std::size_t> failing) {
        Plan p;
        p.accept.assign(n, true);
        for (const auto i : failing) {
            p.accept[i] = false;
        }
        return p;
    }
    // An error answer; `accepted` says whether the records were accepted first (a
    // reply lost after the service took the request).
    static Plan error(std::string code,
                      int http_status,
                      FirehoseFault fault,
                      bool accepted = false,
                      std::string message = "scripted") {
        Plan p;
        p.error = FirehoseCallError{fault, std::move(code), std::move(message), http_status};
        p.accept_none = !accepted;
        return p;
    }

    // What each PutRecordBatch carried, in call order.
    std::vector<std::vector<std::string>> calls() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return calls_;
    }
    // Every record accepted, in order, duplicates included.
    std::vector<std::string> accepted() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return accepted_;
    }
    bool aborted() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return aborted_;
    }
    bool hanging() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return hanging_;
    }

    FirehoseBatchResult put_record_batch(const std::string& stream,
                                         const std::vector<const std::string*>& records) override {
        std::unique_lock<std::mutex> lock(mu_);
        const auto call = calls_.size();
        std::vector<std::string> copy;
        for (const auto* r : records) {
            copy.push_back(*r);
        }
        calls_.push_back(copy);
        FirehoseBatchResult out;
        if (aborted_) {
            out.error = {FirehoseFault::Cancelled, "Cancelled", "aborted", 0};
            return out;
        }
        if (stream != stream_) {
            out.error = {FirehoseFault::Permanent,
                         "ResourceNotFoundException",
                         "Firehose " + stream + " under account 123456789012 not found.",
                         400};
            return out;
        }
        std::size_t data = 0;
        for (const auto* r : records) {
            if (r->size() > 1024000) {
                out.error = {FirehoseFault::Permanent,
                             "InvalidArgumentException",
                             "Record size exceeds the 1,000 KiB limit",
                             400};
                return out;
            }
            data += r->size();
        }
        if (records.empty() || records.size() > 500 || data > 4 * 1024 * 1024) {
            out.error = {FirehoseFault::Permanent,
                         "InvalidArgumentException",
                         "1 validation error detected: Records",
                         400};
            return out;
        }
        Plan plan = planner_ ? planner_(call, records) : Plan{};
        if (plan.hang) {
            hanging_ = true;
            cv_.wait(lock, [this] { return aborted_; });
            hanging_ = false;
            out.error = {FirehoseFault::Cancelled, "Cancelled", "aborted", 0};
            return out;
        }
        for (std::size_t i = 0; i < records.size(); ++i) {
            const bool accept = !plan.accept_none &&
                                (plan.accept.empty() || (i < plan.accept.size() && plan.accept[i]));
            if (accept) {
                accepted_.push_back(*records[i]);
                out.entries.push_back({"rid-" + std::to_string(next_id_++), "", ""});
            } else {
                out.entries.push_back({"", "ServiceUnavailableException", "Slow down."});
                ++out.failed_put_count;
            }
        }
        if (plan.error) {
            FirehoseBatchResult err;
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
    std::string stream_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    Planner planner_;
    std::vector<std::vector<std::string>> calls_;
    std::vector<std::string> accepted_;
    std::size_t next_id_{0};
    bool aborted_{false};
    bool hanging_{false};
};

}  // namespace clink::aws::test
