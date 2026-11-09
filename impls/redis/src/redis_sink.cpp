// Redis Streams sink. See redis_sink.hpp.

#include "clink/redis/redis_sink.hpp"

#include <stdexcept>
#include <utility>

#include "clink/fault/fault_injection.hpp"

#include "fault_points.hpp"

namespace clink::redis {

namespace {

RedisSinkOptions checked(RedisSinkOptions o) {
    if (o.stream.empty()) {
        throw std::runtime_error(o.name + ": 'stream' is required");
    }
    if (o.field.empty()) {
        throw std::runtime_error(o.name + ": 'field' must not be empty");
    }
    if (o.batch_records == 0) {
        o.batch_records = 1;
    }
    if (o.max_bytes == 0) {
        o.max_bytes = RedisSink::kDefaultMaxBytes;
    }
    if (o.max_record_bytes == 0) {
        o.max_record_bytes = RedisSink::kDefaultMaxRecordBytes;
    }
    return o;
}

RedisSinkCoreOptions core_options(const RedisSinkOptions& o) {
    RedisSinkCoreOptions c;
    c.conn = o.conn;
    c.name = o.name;
    c.target = "stream " + o.stream;
    c.server_factory = o.server_factory;
    return c;
}

}  // namespace

RedisSink::RedisSink(RedisSinkOptions opts)
    : opts_(checked(std::move(opts))), core_(core_options(opts_)) {}

void RedisSink::open() {
    pending_.clear();
    pending_bytes_ = 0;
    core_.open(runtime());
}

void RedisSink::on_data(const Batch<std::string>& batch) {
    core_.require_usable("a record");
    for (const auto& rec : batch) {
        const auto& payload = rec.value();
        if (payload.size() > opts_.max_record_bytes) {
            core_.fail("a record is " + std::to_string(payload.size()) +
                       " bytes, more than max_record_bytes (" +
                       std::to_string(opts_.max_record_bytes) + ")");
        }
        if (pending_.empty()) {
            first_buffered_at_ = std::chrono::steady_clock::now();
        }
        pending_bytes_ += payload.size();
        pending_.push_back(payload);
        if (pending_.size() >= opts_.batch_records || pending_bytes_ >= opts_.max_bytes ||
            linger_elapsed_()) {
            flush();
        }
    }
}

void RedisSink::on_barrier(CheckpointBarrier /*b*/) {
    core_.require_usable("a checkpoint barrier");
    const bool writes = !pending_.empty();
    flush();
    if (writes) {
        (void)CLINK_FAULT_POINT(points::kSinkAfterFlush);
    }
}

void RedisSink::flush() {
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
    for (auto& p : pending_) {
        commands.push_back(xadd_args_(std::move(p)));
    }
    pending_.clear();
    pending_bytes_ = 0;
    core_.write(commands, records, bytes);
}

void RedisSink::close() {
    pending_.clear();
    pending_bytes_ = 0;
    core_.close();
}

std::vector<std::string> RedisSink::xadd_args_(std::string payload) const {
    std::vector<std::string> args;
    args.reserve(8);
    args.emplace_back("XADD");
    args.push_back(opts_.stream);
    if (opts_.maxlen > 0) {
        args.emplace_back("MAXLEN");
        args.emplace_back(opts_.approx_maxlen ? "~" : "=");
        args.push_back(std::to_string(opts_.maxlen));
    }
    args.emplace_back("*");  // server-assigned id
    args.push_back(opts_.field);
    args.push_back(std::move(payload));
    return args;
}

bool RedisSink::linger_elapsed_() const {
    return opts_.max_age.count() > 0 && !pending_.empty() &&
           std::chrono::steady_clock::now() - first_buffered_at_ >= opts_.max_age;
}

}  // namespace clink::redis
