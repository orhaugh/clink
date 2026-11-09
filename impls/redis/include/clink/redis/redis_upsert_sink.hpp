#pragma once

// Redis keyed sink for mode='upsert': maintains one Redis string per primary
// key from a changelog.
//   insert / update_after (or no __row_kind)  -> SET <key> <value>
//   delete / update_before                    -> DEL <key>
// The key is key_prefix and the primary key as text (redis_sink_rules.hpp): a
// string as its characters, a number as its exact plain decimal, the columns of
// a composite key joined by U+001F. The value is the record as written, without
// its __row_kind member.
//
// A flush nets the changelog by key (a key's last change wins) and pipelines
// the survivors. Delivery is EFFECTIVELY-ONCE on the keyspace for a stable key
// and a deterministic query: SET and DEL by key converge on the same final
// state when a restored job writes its records again. It is not two-phase
// commit: a reader can see a flush partly applied, and a key can show an
// earlier value while a restored job catches up.
//
// Records are read strictly (one JSON object, numbers kept as written). A
// record that is not one, whose __row_kind is unknown, or whose key member is
// missing, null, an object or an array fails the sink, as does a composite key
// member holding U+001F. Writes, failures and the command timeout are the
// shared core's (clink/redis/redis_sink_core.hpp).

#include <cstddef>
#include <map>
#include <string>
#include <vector>

#include "clink/operators/operator_base.hpp"
#include "clink/redis/redis_client.hpp"
#include "clink/redis/redis_sink_core.hpp"

namespace clink::redis {

struct RedisUpsertSinkOptions {
    ConnectOptions conn;                   // a command_timeout of 0 means 30 s
    std::vector<std::string> key_columns;  // the primary key (required)
    std::string key_prefix;                // prefix of every Redis key (optional)
    std::size_t batch_records{1000};       // flush after this many buffered keys
    std::size_t max_bytes{0};              // flush after this many buffered bytes (0 = 16 MiB)
    std::size_t max_record_bytes{0};       // refuse a longer record (0 = 16 MiB)
    std::string name{"redis_upsert_sink"};
    RedisSinkServerFactory server_factory;  // unset: a real server
};

class RedisUpsertSink : public Sink<std::string> {
public:
    static constexpr std::size_t kDefaultMaxBytes = std::size_t{16} << 20;
    static constexpr std::size_t kDefaultMaxRecordBytes = std::size_t{16} << 20;

    explicit RedisUpsertSink(RedisUpsertSinkOptions opts);

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
    struct Change {
        bool is_delete{false};
        std::string value;  // the record without __row_kind; empty for a delete
    };

    void add_(std::string key, Change change);

    RedisUpsertSinkOptions opts_;
    RedisSinkCore core_;
    std::map<std::string, Change> pending_;  // by Redis key
    std::size_t pending_bytes_{0};
};

}  // namespace clink::redis
