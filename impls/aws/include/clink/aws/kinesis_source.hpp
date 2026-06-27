#pragma once

// Kinesis Data Streams source (ListShards + GetShardIterator + GetRecords).
// SDK-dependent; compiled only with the AWS SDK.
//
// Unbounded. Each subtask owns a static modulo-slice of the stream's shards
// (shard_index % parallelism == subtask_idx) and polls each in round-robin,
// emitting every record's Data as a std::string (JSON on the SQL path).
// Per-shard checkpoint = the last SequenceNumber, persisted as operator-state
// keyed by ShardId (so it survives a rescale); resume re-mints the iterator with
// AFTER_SEQUENCE_NUMBER -> AT-LEAST-ONCE. Iterator expiry (5 min) is recovered
// by re-minting from the last sequence number.
//
// BASELINE CAVEATS: shard assignment is fixed at open(); a reshard is picked up
// by re-ListShards when an owned shard closes, but a parent on ANOTHER subtask
// is not coordinated, so during an active reshard a child may be read before a
// cross-subtask parent fully drains (records out of sequence-number order across
// the split). Steady-state (no active resharding) is unaffected.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/KinesisErrors.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/ListShardsRequest.h>
#include <aws/kinesis/model/ShardIteratorType.h>

#include "clink/aws/aws_client.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/state_backend.hpp"

namespace clink::aws {

// Static shard-to-subtask assignment: shard at `shard_index` is owned by subtask
// `subtask_idx` of `parallelism`. Pure: unit-testable.
inline bool kinesis_shard_assigned(std::size_t shard_index,
                                   std::uint32_t subtask_idx,
                                   std::uint32_t parallelism) {
    if (parallelism <= 1) {
        return true;
    }
    return shard_index % parallelism == subtask_idx;
}

struct KinesisSourceOptions {
    std::string stream;                            // stream name or ARN (required)
    std::string initial_position{"trim_horizon"};  // "trim_horizon" | "latest"
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};
    AwsClientOptions client;
    int max_records_per_poll{1000};                // GetRecords Limit (1..10000)
    std::chrono::milliseconds poll_interval{250};  // sleep after a fully-idle round
    std::string name{"kinesis_source"};
};

class KinesisSource : public Source<std::string> {
public:
    explicit KinesisSource(KinesisSourceOptions opts) : opts_(std::move(opts)) {
        if (opts_.stream.empty()) {
            throw std::runtime_error(opts_.name + ": 'stream' is required");
        }
        if (opts_.max_records_per_poll < 1 || opts_.max_records_per_poll > 10000) {
            opts_.max_records_per_poll = 1000;
        }
    }

    void open() override {
        ensure_aws_initialized();
        client_ = std::make_unique<Aws::Kinesis::KinesisClient>(make_client_config(opts_.client));
        shards_.clear();
        discover_assigned_shards_();
    }

    bool produce(Emitter<std::string>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (shards_.empty()) {
            // No shards assigned to this subtask (yet); idle without spinning.
            std::this_thread::sleep_for(opts_.poll_interval);
            return !this->cancelled();
        }
        // Poll ONE shard per call, round-robin. Sleep only once a full pass has
        // produced nothing (so a busy shard is never throttled by idle siblings).
        ShardState& s = shards_[rr_ % shards_.size()];
        ++rr_;
        bool emitted = false;
        if (!s.finished) {
            emitted = poll_shard_(s, out);
        }
        if (emitted) {
            empty_streak_ = 0;
        } else if (++empty_streak_ >= shards_.size()) {
            empty_streak_ = 0;
            std::this_thread::sleep_for(opts_.poll_interval);
            maybe_refresh_shards_();  // pick up children after a reshard
        }
        return !this->cancelled();
    }

    void cancel() override { Source<std::string>::cancel(); }

    void close() override { client_.reset(); }

    [[nodiscard]] bool is_bounded() const noexcept override { return false; }
    [[nodiscard]] std::size_t split_count() const noexcept override {
        return shards_.empty() ? 1 : shards_.size();
    }

    std::string name() const override { return opts_.name; }

    void snapshot_offset(StateBackend& backend,
                         OperatorId op_id,
                         CheckpointId /*ckpt_id*/) override {
        // One operator-state row per owned shard, keyed by ShardId so the
        // checkpoint survives a rescale. Value = the last SequenceNumber string
        // (or the restored resume point if the shard has not been read yet).
        for (const auto& s : shards_) {
            const std::string& seq = !s.last_seq.empty() ? s.last_seq : restored_seq_(s.shard_id);
            if (seq.empty()) {
                continue;
            }
            const std::string key = kSeqPrefix + s.shard_id;
            backend.put_operator_state(op_id, key, StateBackend::ValueView{seq.data(), seq.size()});
        }
    }

    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        restored_seqs_.clear();
        const std::string_view prefix{kSeqPrefix};
        backend.scan_operator_state(
            op_id, [&](StateBackend::KeyView key, StateBackend::ValueView value) {
                if (key.size() <= prefix.size() || key.substr(0, prefix.size()) != prefix) {
                    return;
                }
                restored_seqs_.emplace(std::string{key.substr(prefix.size())}, std::string{value});
            });
        return !restored_seqs_.empty();
    }

private:
    static constexpr const char* kSeqPrefix = "__kinesis_seq__:";

    struct ShardState {
        std::string shard_id;
        std::string iterator;  // empty => needs (re-)minting
        std::string last_seq;  // last sequence number read (checkpoint token)
        bool finished{false};  // shard closed and fully drained
    };

    bool is_arn_() const { return opts_.stream.rfind("arn:", 0) == 0; }

    std::string restored_seq_(const std::string& shard_id) const {
        auto it = restored_seqs_.find(shard_id);
        return it == restored_seqs_.end() ? std::string{} : it->second;
    }

    // ListShards (paginated) -> keep the shards this subtask owns by modulo.
    void discover_assigned_shards_() {
        std::unordered_set<std::string> have;
        for (const auto& s : shards_) {
            have.insert(s.shard_id);
        }
        std::string next_token;
        std::size_t global_index = 0;
        do {
            Aws::Kinesis::Model::ListShardsRequest req;
            if (next_token.empty()) {
                if (is_arn_()) {
                    req.SetStreamARN(opts_.stream);
                } else {
                    req.SetStreamName(opts_.stream);
                }
            } else {
                req.SetNextToken(next_token);  // must NOT also set stream name
            }
            auto outcome = client_->ListShards(req);
            if (!outcome.IsSuccess()) {
                // A transient ListShards failure at open() is fatal (we cannot
                // know our shards); surface it so the job restarts.
                throw std::runtime_error(opts_.name + ": ListShards failed: " +
                                         std::string(outcome.GetError().GetMessage().c_str()));
            }
            for (const auto& shard : outcome.GetResult().GetShards()) {
                const std::size_t idx = global_index++;
                if (!kinesis_shard_assigned(idx, opts_.subtask_idx, opts_.parallelism)) {
                    continue;
                }
                const std::string id = shard.GetShardId();
                if (have.count(id)) {
                    continue;  // already tracking
                }
                ShardState st;
                st.shard_id = id;
                st.last_seq = restored_seq_(id);
                shards_.push_back(std::move(st));
                have.insert(id);
            }
            next_token = outcome.GetResult().GetNextToken();
        } while (!next_token.empty());
    }

    // Re-ListShards to pick up child shards after a reshard. Cheap-guarded: only
    // runs when at least one owned shard has finished (the reshard signal).
    void maybe_refresh_shards_() {
        bool any_finished = false;
        for (const auto& s : shards_) {
            if (s.finished) {
                any_finished = true;
                break;
            }
        }
        if (any_finished) {
            discover_assigned_shards_();  // adds newly-owned shards, keeps existing
        }
    }

    std::string mint_iterator_(ShardState& s) {
        Aws::Kinesis::Model::GetShardIteratorRequest req;
        if (is_arn_()) {
            req.SetStreamARN(opts_.stream);
        } else {
            req.SetStreamName(opts_.stream);
        }
        req.SetShardId(s.shard_id);
        const std::string seq = !s.last_seq.empty() ? s.last_seq : restored_seq_(s.shard_id);
        if (!seq.empty()) {
            req.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::AFTER_SEQUENCE_NUMBER);
            req.SetStartingSequenceNumber(seq);
        } else if (opts_.initial_position == "latest") {
            req.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::LATEST);
        } else {
            req.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);
        }
        auto outcome = client_->GetShardIterator(req);
        if (!outcome.IsSuccess()) {
            return {};  // leave iterator empty; retried next round
        }
        return outcome.GetResult().GetShardIterator();
    }

    // Poll one shard once. Returns true iff records were emitted.
    bool poll_shard_(ShardState& s, Emitter<std::string>& out) {
        if (s.iterator.empty()) {
            s.iterator = mint_iterator_(s);
            if (s.iterator.empty()) {
                return false;  // mint failed (or shard gone); retry next round
            }
        }
        Aws::Kinesis::Model::GetRecordsRequest req;
        req.SetShardIterator(s.iterator);
        req.SetLimit(opts_.max_records_per_poll);
        auto outcome = client_->GetRecords(req);
        if (!outcome.IsSuccess()) {
            const auto err = outcome.GetError().GetErrorType();
            if (err == Aws::Kinesis::KinesisErrors::EXPIRED_ITERATOR) {
                s.iterator.clear();  // re-mint from last_seq next round
            } else if (err == Aws::Kinesis::KinesisErrors::PROVISIONED_THROUGHPUT_EXCEEDED) {
                // Returned no data; keep the iterator and back off.
                std::this_thread::sleep_for(opts_.poll_interval);
            } else {
                clink::metrics::connector::error_inc("kinesis", "source");
                s.iterator.clear();  // re-mint defensively
            }
            return false;
        }
        const auto& result = outcome.GetResult();
        Batch<std::string> batch;
        std::uint64_t bytes = 0;
        for (const auto& rec : result.GetRecords()) {
            const auto& data = rec.GetData();  // Aws::Utils::ByteBuffer (not NUL-terminated)
            batch.emplace(std::string(reinterpret_cast<const char*>(data.GetUnderlyingData()),
                                      data.GetLength()));
            bytes += data.GetLength();
            s.last_seq = rec.GetSequenceNumber();
        }
        // NextShardIterator empty/null => shard ended (closed and fully drained).
        s.iterator = result.GetNextShardIterator();
        if (s.iterator.empty()) {
            s.finished = true;
        }
        if (batch.empty()) {
            return false;
        }
        const auto n = batch.size();
        clink::metrics::connector::records_in_inc("kinesis", n);
        clink::metrics::connector::bytes_in_inc("kinesis", bytes);
        out.emit_data(std::move(batch));
        return true;
    }

    KinesisSourceOptions opts_;
    std::unique_ptr<Aws::Kinesis::KinesisClient> client_;
    std::vector<ShardState> shards_;
    std::unordered_map<std::string, std::string> restored_seqs_;  // shard_id -> resume seq
    std::size_t rr_{0};
    std::size_t empty_streak_{0};
};

}  // namespace clink::aws
