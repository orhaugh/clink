// Redis keyed sink. See redis_upsert_sink.hpp.

#include "clink/redis/redis_upsert_sink.hpp"

#include <stdexcept>
#include <utility>

#include "clink/fault/fault_injection.hpp"
#include "clink/redis/redis_sink_rules.hpp"

#include "fault_points.hpp"

namespace clink::redis {

namespace {

RedisUpsertSinkOptions checked(RedisUpsertSinkOptions o) {
    if (o.key_columns.empty()) {
        throw std::runtime_error(o.name + ": 'key_columns' (the primary key) is required");
    }
    if (o.batch_records == 0) {
        o.batch_records = 1;
    }
    if (o.max_bytes == 0) {
        o.max_bytes = RedisUpsertSink::kDefaultMaxBytes;
    }
    if (o.max_record_bytes == 0) {
        o.max_record_bytes = RedisUpsertSink::kDefaultMaxRecordBytes;
    }
    return o;
}

RedisSinkCoreOptions core_options(const RedisUpsertSinkOptions& o) {
    RedisSinkCoreOptions c;
    c.conn = o.conn;
    c.name = o.name;
    c.target = o.key_prefix.empty() ? std::string{"the keyspace"}
                                    : "the keys under prefix " + o.key_prefix;
    c.server_factory = o.server_factory;
    return c;
}

}  // namespace

RedisUpsertSink::RedisUpsertSink(RedisUpsertSinkOptions opts)
    : opts_(checked(std::move(opts))), core_(core_options(opts_)) {}

void RedisUpsertSink::open() {
    pending_.clear();
    pending_bytes_ = 0;
    core_.open(runtime());
}

void RedisUpsertSink::on_data(const Batch<std::string>& batch) {
    core_.require_usable("a record");
    for (const auto& rec : batch) {
        const auto& text = rec.value();
        if (text.size() > opts_.max_record_bytes) {
            core_.fail("a record is " + std::to_string(text.size()) +
                       " bytes, more than max_record_bytes (" +
                       std::to_string(opts_.max_record_bytes) + ")");
        }
        JsonMembers m;
        if (auto why = read_json_object(text, m); !why.empty()) {
            core_.fail("a record cannot be written: " + why);
        }
        std::string key;
        Change change;
        try {
            change.is_delete = change_kind_of(m) == ChangeKind::Delete;
            key = redis_key(m, opts_.key_columns, opts_.key_prefix);
            if (!change.is_delete) {
                change.value = remove_member(text, kRowKindMember);
            }
        } catch (const RecordRefused& e) {
            core_.fail(std::string{"a record cannot be written: "} + e.what());
        }
        add_(std::move(key), std::move(change));
        if (pending_.size() >= opts_.batch_records || pending_bytes_ >= opts_.max_bytes) {
            flush();
        }
    }
}

void RedisUpsertSink::add_(std::string key, Change change) {
    const auto weight = key.size() + change.value.size();
    if (const auto it = pending_.find(key); it != pending_.end()) {
        pending_bytes_ -= it->first.size() + it->second.value.size();
        it->second = std::move(change);
    } else {
        pending_.emplace(std::move(key), std::move(change));
    }
    pending_bytes_ += weight;
}

void RedisUpsertSink::on_barrier(CheckpointBarrier /*b*/) {
    core_.require_usable("a checkpoint barrier");
    const bool writes = !pending_.empty();
    flush();
    if (writes) {
        (void)CLINK_FAULT_POINT(points::kSinkAfterFlush);
    }
}

void RedisUpsertSink::flush() {
    // A failed sink refuses even an empty flush, so a clean end of input cannot
    // pass over its failure; one never opened has nothing to flush.
    core_.require_not_failed("a flush");
    if (pending_.empty()) {
        return;
    }
    const auto records = pending_.size();
    const auto bytes = pending_bytes_;
    std::vector<std::vector<std::string>> commands;
    commands.reserve(records);
    for (auto& [key, change] : pending_) {
        if (change.is_delete) {
            commands.push_back({"DEL", key});
        } else {
            commands.push_back({"SET", key, std::move(change.value)});
        }
    }
    pending_.clear();
    pending_bytes_ = 0;
    core_.write(commands, records, bytes);
}

void RedisUpsertSink::close() {
    pending_.clear();
    pending_bytes_ = 0;
    core_.close();
}

}  // namespace clink::redis
