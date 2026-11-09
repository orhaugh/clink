#pragma once

// Redis Streams sink: each record is appended to a stream with XADD, as the
// value of one field (default "v"), so redis_source reads it back as written.
// On the SQL path a record is a row_to_json_string object; with
// changelog='true' it keeps its __row_kind marker and the reader nets.
//
// Delivery is AT-LEAST-ONCE. XADD has no producer key, so the records a
// restored job writes again (those its last checkpoint did not cover) are
// appended again. Records are written at the count, byte and linger thresholds
// and at every checkpoint barrier, so a completed checkpoint covers only
// records the stream holds. MAXLEN trims the oldest entries as the stream
// grows, whether or not a consumer group has read them.
//
// Writes, failures and the command timeout are the shared core's
// (clink/redis/redis_sink_core.hpp). A record longer than max_record_bytes
// fails the sink before anything of its flush is sent.

#include <chrono>
#include <cstddef>
#include <string>
#include <vector>

#include "clink/operators/operator_base.hpp"
#include "clink/redis/redis_client.hpp"
#include "clink/redis/redis_sink_core.hpp"

namespace clink::redis {

struct RedisSinkOptions {
    ConnectOptions conn;                   // a command_timeout of 0 means 30 s
    std::string stream;                    // target stream key (required)
    std::string field{"v"};                // field name holding each record's payload
    std::size_t maxlen{0};                 // XADD MAXLEN cap (0 = unbounded)
    bool approx_maxlen{true};              // MAXLEN ~ (approx, cheap) vs = (exact)
    std::size_t batch_records{1000};       // flush after this many buffered records
    std::size_t max_bytes{0};              // flush after this many buffered bytes (0 = 16 MiB)
    std::size_t max_record_bytes{0};       // refuse a longer record (0 = 16 MiB)
    std::chrono::milliseconds max_age{0};  // linger: flush a partial batch this old (0 = off)
    std::string name{"redis_sink"};
    RedisSinkServerFactory server_factory;  // unset: a real server
};

class RedisSink : public Sink<std::string> {
public:
    static constexpr std::size_t kDefaultMaxBytes = std::size_t{16} << 20;
    static constexpr std::size_t kDefaultMaxRecordBytes = std::size_t{16} << 20;

    explicit RedisSink(RedisSinkOptions opts);

    void open() override;
    void on_data(const Batch<std::string>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    // Its barrier hook writes out everything it received before the barrier,
    // which is its guarantee only while it owns its chain's checkpoint, so
    // Dag::add_sink refuses another sink beside it.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    void flush() override;
    // Discards what is buffered and releases the connection; never throws.
    void close() override;
    std::string name() const override { return opts_.name; }

private:
    [[nodiscard]] std::vector<std::string> xadd_args_(std::string payload) const;
    [[nodiscard]] bool linger_elapsed_() const;

    RedisSinkOptions opts_;
    RedisSinkCore core_;
    std::vector<std::string> pending_;
    std::size_t pending_bytes_{0};
    std::chrono::steady_clock::time_point first_buffered_at_{};
};

}  // namespace clink::redis
