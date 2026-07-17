// Typed ClickHouse sink helper. The existing `clickhouse_sink` factory
// takes string-channel JSONEachRow lines; this helper composes a
// user-supplied per-T encoder map step ahead of it so call sites that
// have a typed record stream don't have to write the `.map<string>(
// to_jsoneachrow)` hop by hand.
//
// Free function (not a fluent Builder) so it can wire the encoder map
// and the sink descriptor in one call.
//
// Usage:
//   clink::clickhouse::sink<MyRecord>(
//       my_stream,
//       clink::clickhouse::SinkOptions{
//           .host = "localhost",
//           .port = 9000,
//           .database = "events",
//           .table = "my_table",
//           .format = "JSONEachRow",
//       },
//       [](const MyRecord& r) { return to_jsoneachrow(r); });

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <utility>

#include "clink/api/clickhouse_builders.hpp"
#include "clink/api/descriptors.hpp"
#include "clink/api/pipeline.hpp"

namespace clink::clickhouse {

struct SinkOptions {
    std::string host = "localhost";
    std::uint16_t port = 9000;
    std::string database;
    std::string table;
    std::string user;
    std::string password;
    std::string format = "JSONEachRow";
    std::int64_t batch_rows = 0;
    std::int64_t batch_interval_ms = 0;
};

inline clink::api::SinkDescriptor make_sink_descriptor(const SinkOptions& opts) {
    auto b =
        clink::api::ClickHouseSink::builder().host(opts.host).port(opts.port).table(opts.table);
    if (!opts.database.empty()) {
        b.database(opts.database);
    }
    if (!opts.user.empty()) {
        b.user(opts.user);
    }
    if (!opts.password.empty()) {
        b.password(opts.password);
    }
    if (!opts.format.empty()) {
        b.format(opts.format);
    }
    if (opts.batch_rows > 0) {
        b.batch_rows(opts.batch_rows);
    }
    if (opts.batch_interval_ms > 0) {
        b.batch_interval_ms(opts.batch_interval_ms);
    }
    return b.build();
}

template <typename T>
inline void sink(clink::api::DataStream<T> stream,
                 const SinkOptions& opts,
                 std::function<std::string(const T&)> encode_fn,
                 std::string id = {}) {
    stream.template map<std::string>(std::move(encode_fn))
        .sink(make_sink_descriptor(opts), std::move(id));
}

inline void string_sink(clink::api::DataStream<std::string> stream,
                        const SinkOptions& opts,
                        std::string id = {}) {
    stream.sink(make_sink_descriptor(opts), std::move(id));
}

}  // namespace clink::clickhouse
