#pragma once

// A model of the Kinesis Data Streams calls the source makes (ListShards,
// GetShardIterator, GetRecords), for the source's deterministic tests. It keeps
// a topology with the service's parent and child links, per-shard records with
// arrival times, retention (trimmed records, shards leaving the listing),
// iterators that can be expired, paginated and reordered listings, and a hook
// that fails any call. The source under test is the production KinesisSource;
// only the service is modelled.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "clink/aws/kinesis_source.hpp"

namespace clink::aws::test {

inline KinesisCallError service_error(KinesisCall call, const std::string& code, int status = 400) {
    KinesisCallError e;
    e.code = code;
    e.message = "modelled " + code;
    e.fault = classify_kinesis_error(call, code, status, status >= 500);
    return e;
}

inline std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// "shardId-000000000042"
inline std::string shard_name(int n) {
    char buf[32];
    std::snprintf(buf, sizeof buf, "shardId-%012d", n);
    return buf;
}

// The first shard number at or after `from` whose ShardId the given subtask
// owns at the given parallelism.
inline int shard_number_owned_by(std::uint32_t subtask, std::uint32_t parallelism, int from) {
    for (int n = from;; ++n) {
        if (kinesis_shard_owner(shard_name(n), parallelism) == subtask) {
            return n;
        }
    }
}

class KinesisModel final : public KinesisShardService {
public:
    struct Rec {
        std::string seq;
        std::string data;
        std::int64_t arrival_ms{0};
    };
    struct Shard {
        std::string parent;
        std::string adjacent_parent;
        std::string starting_sequence;
        std::vector<Rec> records;
        std::size_t trimmed{0};  // records below this index have left the stream
        bool closed{false};
        bool listed{true};  // false once the shard has left the stream
    };

    // ---- topology --------------------------------------------------------------

    std::string create(const std::string& id,
                       const std::string& parent = {},
                       const std::string& adjacent_parent = {}) {
        std::lock_guard lock(mu_);
        Shard s;
        s.parent = parent;
        s.adjacent_parent = adjacent_parent;
        s.starting_sequence = "start-" + id + "-" + std::to_string(incarnation_);
        shards_[id] = std::move(s);
        return id;
    }

    std::string put(const std::string& id,
                    const std::string& data,
                    std::optional<std::int64_t> arrival_ms = std::nullopt) {
        std::lock_guard lock(mu_);
        auto& s = shards_.at(id);
        if (s.closed) {
            throw std::logic_error("put to closed shard " + id);
        }
        char seq[48];
        std::snprintf(seq, sizeof seq, "%020llu", static_cast<unsigned long long>(++next_seq_));
        s.records.push_back(Rec{seq, data, arrival_ms.value_or(now_ms())});
        return seq;
    }

    void close(const std::string& id) {
        std::lock_guard lock(mu_);
        shards_.at(id).closed = true;
    }

    // A split: the parent closes and two children name it.
    void split(const std::string& parent, const std::string& left, const std::string& right) {
        close(parent);
        create(left, parent);
        create(right, parent);
    }

    // A merge: both parents close and the child names both.
    void merge(const std::string& parent, const std::string& adjacent, const std::string& child) {
        close(parent);
        close(adjacent);
        create(child, parent, adjacent);
    }

    // The shard has passed the retention period: its records are gone and the
    // listing no longer names it.
    void expire(const std::string& id) {
        std::lock_guard lock(mu_);
        auto& s = shards_.at(id);
        s.listed = false;
        s.trimmed = s.records.size();
    }

    // The first `n` records of the shard have passed the retention period.
    void trim(const std::string& id, std::size_t n) {
        std::lock_guard lock(mu_);
        auto& s = shards_.at(id);
        s.trimmed = std::min(n, s.records.size());
    }

    // The stream was deleted and created again: same ShardIds, new shards.
    void recreate() {
        std::lock_guard lock(mu_);
        ++incarnation_;
        for (auto& [id, s] : shards_) {
            s = Shard{};
            s.starting_sequence = "start-" + id + "-" + std::to_string(incarnation_);
        }
    }

    // Every iterator handed out so far is refused as expired.
    void expire_iterators() {
        std::lock_guard lock(mu_);
        ++iterator_epoch_;
    }

    // ---- behaviour ----------------------------------------------------------------

    std::size_t page_size{0};  // 0: one page
    // Permutes each listing before it is paged, as the service promises no order.
    std::function<void(std::vector<KinesisShard>&)> reorder;
    // A non-ok error fails the call. The string is the shard (empty for ListShards).
    std::function<KinesisCallError(KinesisCall, const std::string&)> fault;
    // How AtSequence answers a sequence number that has left the stream.
    enum class MissingSequence { Refuse, FromTrimHorizon };
    MissingSequence missing_sequence{MissingSequence::Refuse};
    // GetRecords blocks until abort().
    std::atomic<bool> hang_get_records{false};

    // ---- observations ----------------------------------------------------------

    struct Mint {
        std::string shard;
        KinesisReadFrom from;
    };
    std::vector<Mint> mints() const {
        std::lock_guard lock(mu_);
        return mints_;
    }
    std::size_t list_calls() const {
        std::lock_guard lock(mu_);
        return list_calls_;
    }
    std::size_t get_records_calls(const std::string& id) const {
        std::lock_guard lock(mu_);
        const auto it = get_records_calls_.find(id);
        return it == get_records_calls_.end() ? 0 : it->second;
    }
    std::vector<std::string> data_of(const std::string& id) const {
        std::lock_guard lock(mu_);
        std::vector<std::string> out;
        for (const auto& r : shards_.at(id).records) {
            out.push_back(r.data);
        }
        return out;
    }
    std::vector<std::string> all_data() const {
        std::lock_guard lock(mu_);
        std::vector<std::string> out;
        for (const auto& [id, s] : shards_) {
            for (const auto& r : s.records) {
                out.push_back(r.data);
            }
        }
        return out;
    }

    // ---- KinesisShardService ------------------------------------------------------

    KinesisShardPage list_shards(const std::string& next_token) override {
        std::lock_guard lock(mu_);
        KinesisShardPage page;
        if (aborted_) {
            page.error = cancelled();
            return page;
        }
        ++list_calls_;
        if (fault) {
            if (auto e = fault(KinesisCall::ListShards, {}); !e.ok()) {
                page.error = std::move(e);
                return page;
            }
        }
        std::size_t offset = 0;
        if (next_token.empty()) {
            listing_.clear();
            for (const auto& [id, s] : shards_) {
                if (s.listed) {
                    listing_.push_back(
                        KinesisShard{id, s.parent, s.adjacent_parent, s.starting_sequence});
                }
            }
            if (reorder) {
                reorder(listing_);
            }
            ++listing_generation_;
        } else {
            const auto sep = next_token.find(':');
            if (next_token.substr(0, sep) != std::to_string(listing_generation_)) {
                page.error = service_error(KinesisCall::ListShards, "ExpiredNextTokenException");
                return page;
            }
            offset = std::stoul(next_token.substr(sep + 1));
        }
        const std::size_t n = page_size == 0 ? listing_.size() : page_size;
        const std::size_t end = std::min(listing_.size(), offset + n);
        page.shards.assign(listing_.begin() + static_cast<std::ptrdiff_t>(offset),
                           listing_.begin() + static_cast<std::ptrdiff_t>(end));
        if (end < listing_.size()) {
            page.next_token = std::to_string(listing_generation_) + ":" + std::to_string(end);
        }
        return page;
    }

    KinesisIterator shard_iterator(const std::string& id, const KinesisReadFrom& from) override {
        std::lock_guard lock(mu_);
        KinesisIterator it;
        if (aborted_) {
            it.error = cancelled();
            return it;
        }
        mints_.push_back(Mint{id, from});
        if (fault) {
            if (auto e = fault(KinesisCall::GetShardIterator, id); !e.ok()) {
                it.error = std::move(e);
                return it;
            }
        }
        const auto found = shards_.find(id);
        if (found == shards_.end() || !found->second.listed) {
            it.error = service_error(KinesisCall::GetShardIterator, "ResourceNotFoundException");
            return it;
        }
        const auto& s = found->second;
        std::size_t index = s.trimmed;
        switch (from.kind) {
            case KinesisReadFrom::Kind::TrimHorizon:
                break;
            case KinesisReadFrom::Kind::AtTimestamp:
                while (index < s.records.size() &&
                       s.records[index].arrival_ms < from.timestamp_ms) {
                    ++index;
                }
                break;
            case KinesisReadFrom::Kind::AtSequence: {
                const auto at = std::find_if(s.records.begin(), s.records.end(), [&](const Rec& r) {
                    return r.seq == from.sequence_number;
                });
                const auto pos = static_cast<std::size_t>(at - s.records.begin());
                if (at == s.records.end() || pos < s.trimmed) {
                    if (missing_sequence == MissingSequence::Refuse) {
                        it.error = service_error(KinesisCall::GetShardIterator,
                                                 "InvalidArgumentException");
                        return it;
                    }
                    break;  // FromTrimHorizon
                }
                index = pos;
                break;
            }
        }
        it.iterator = mint_locked(id, index);
        return it;
    }

    KinesisRecordBatch get_records(const std::string& iterator, int limit) override {
        std::unique_lock lock(mu_);
        KinesisRecordBatch batch;
        if (hang_get_records) {
            abort_cv_.wait(lock, [&] { return aborted_.load(); });
        }
        if (aborted_) {
            batch.error = cancelled();
            return batch;
        }
        const auto found = iterators_.find(iterator);
        if (found == iterators_.end() || found->second.epoch != iterator_epoch_) {
            batch.error = service_error(KinesisCall::GetRecords, "ExpiredIteratorException");
            return batch;
        }
        const auto [id, index] = std::pair{found->second.shard, found->second.index};
        ++get_records_calls_[id];
        if (fault) {
            if (auto e = fault(KinesisCall::GetRecords, id); !e.ok()) {
                batch.error = std::move(e);
                return batch;
            }
        }
        auto& s = shards_.at(id);
        if (!s.listed) {
            batch.error = service_error(KinesisCall::GetRecords, "ResourceNotFoundException");
            return batch;
        }
        std::size_t at = std::max(index, s.trimmed);
        const std::size_t end = std::min(s.records.size(), at + static_cast<std::size_t>(limit));
        for (; at < end; ++at) {
            batch.records.push_back(KinesisRecord{s.records[at].seq, s.records[at].data});
        }
        batch.millis_behind = at < s.records.size() ? 1000 : 0;
        if (!(s.closed && at == s.records.size())) {
            batch.next_iterator = mint_locked(id, at);
        }
        return batch;
    }

    void abort() noexcept override {
        aborted_ = true;
        abort_cv_.notify_all();
    }

private:
    struct Iter {
        std::string shard;
        std::size_t index{0};
        int epoch{0};
    };

    std::string mint_locked(const std::string& id, std::size_t index) {
        const std::string name = "it-" + std::to_string(++next_iterator_);
        iterators_[name] = Iter{id, index, iterator_epoch_};
        return name;
    }

    static KinesisCallError cancelled() {
        KinesisCallError e;
        e.fault = KinesisFault::Cancelled;
        e.code = "Cancelled";
        return e;
    }

    mutable std::mutex mu_;
    std::condition_variable abort_cv_;
    std::atomic<bool> aborted_{false};
    std::map<std::string, Shard> shards_;
    std::vector<KinesisShard> listing_;
    std::uint64_t listing_generation_{0};
    std::map<std::string, Iter> iterators_;
    std::uint64_t next_iterator_{0};
    int iterator_epoch_{0};
    std::uint64_t next_seq_{0};
    int incarnation_{1};
    std::vector<Mint> mints_;
    std::size_t list_calls_{0};
    std::map<std::string, std::size_t> get_records_calls_;
};

}  // namespace clink::aws::test
