// clink_serde_bench - compares wire-serialization cost between the
// current per-record codec path and Arrow's columnar RecordBatch IPC
// path.
//
// Motivation: clink today encodes each Record<T> one at a time via a
// Codec<T>::encode lambda (variable-length, length-prefixed). That's
// ~50-200ns per record overhead for non-trivial types, which caps
// single-stream throughput around 1-5M rec/sec for narrow payloads.
// Arrow's columnar RecordBatch + IPC format batches encoding into
// one columnar layout per batch, sharing per-record overhead - the
// theory we want to measure.
//
// Setup:
//   * Sender + receiver communicate over a loopback TCP socket pair
//     (same shape NetworkChannel uses in production).
//   * For each payload type (int64_t and a small struct) we run two
//     paths back-to-back:
//       - "current": NetworkChannelSink<T>::push(StreamElement::data(batch))
//         then NetworkChannelSource<T>::pop().
//       - "arrow": same wire framing, but the payload is one
//         arrow::ipc::SerializeRecordBatch(...) blob per batch (Arrow
//         path is gated on CLINK_HAS_ARROW).
//   * Records flow N records / batch, total RECORDS records. The
//     bench times only the inter-process producer-to-consumer cost;
//     allocation of input data happens up-front.
//
// Output: human-readable comparison + optional CSV for tracking
// over time. Format:
//
//   payload=int64,    records=1000000, current_rps=8.42M, current_ns=119, ...
//   payload=int64,    records=1000000, arrow_rps=23.1M,   arrow_ns=43, ...
//
// Usage:
//   clink_serde_bench [--records=1000000] [--batch-size=1024]
//                     [--payload=int64|click|all] [--csv=path.csv]

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/runtime/network/network_channel.hpp"
#include "clink/runtime/network/network_socket.hpp"

#ifdef CLINK_HAS_ARROW
#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#endif

namespace {

using namespace clink;

// A small struct that's representative of stream-processing payloads:
// a numeric column + a small variable-length column.
struct Click {
    std::int64_t user_id{0};
    std::int64_t timestamp_ms{0};
    std::string url;  // ~32 chars typical
};

// Codec<Click> for the current path.
clink::Codec<Click> click_codec() {
    return clink::Codec<Click>{
        .encode =
            [](const Click& c) {
                clink::Codec<Click>::Bytes out;
                // 8 + 8 + 4 + url.size()
                out.reserve(8 + 8 + 4 + c.url.size());
                const auto put_i64 = [&](std::int64_t v) {
                    auto u = static_cast<std::uint64_t>(v);
                    for (int i = 0; i < 8; ++i) {
                        out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
                    }
                };
                const auto put_u32 = [&](std::uint32_t v) {
                    for (int i = 0; i < 4; ++i) {
                        out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
                    }
                };
                put_i64(c.user_id);
                put_i64(c.timestamp_ms);
                put_u32(static_cast<std::uint32_t>(c.url.size()));
                for (char ch : c.url) {
                    out.push_back(static_cast<std::byte>(ch));
                }
                return out;
            },
        .decode = [](clink::Codec<Click>::BytesView b) -> std::optional<Click> {
            if (b.size() < 20) return std::nullopt;
            const auto read_i64 = [&](std::size_t off) {
                std::uint64_t v = 0;
                for (int i = 0; i < 8; ++i) {
                    v |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(b[off + i]))
                         << (i * 8);
                }
                return static_cast<std::int64_t>(v);
            };
            const auto read_u32 = [&](std::size_t off) {
                std::uint32_t v = 0;
                for (int i = 0; i < 4; ++i) {
                    v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(b[off + i]))
                         << (i * 8);
                }
                return v;
            };
            Click c;
            c.user_id = read_i64(0);
            c.timestamp_ms = read_i64(8);
            const auto url_len = read_u32(16);
            if (b.size() < 20 + url_len) return std::nullopt;
            c.url.assign(reinterpret_cast<const char*>(b.data() + 20), url_len);
            return c;
        }};
}

// Wall-clock measurement.
struct Measurement {
    std::string payload;
    std::string path;
    std::size_t records{0};
    std::size_t bytes_wire{0};
    double wall_ms{0.0};

    [[nodiscard]] double records_per_sec() const {
        return wall_ms > 0 ? (static_cast<double>(records) / (wall_ms / 1000.0)) : 0.0;
    }
    [[nodiscard]] double bytes_per_sec() const {
        return wall_ms > 0 ? (static_cast<double>(bytes_wire) / (wall_ms / 1000.0)) : 0.0;
    }
    [[nodiscard]] double ns_per_record() const {
        return records > 0 ? (wall_ms * 1'000'000.0 / static_cast<double>(records)) : 0.0;
    }
};

// CLI parse.
std::string get_arg(int argc,
                    char** argv,
                    std::string_view flag,
                    std::string_view default_value = {}) {
    const std::string prefix = "--" + std::string{flag} + "=";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a.rfind(prefix, 0) == 0) {
            return a.substr(prefix.size());
        }
    }
    return std::string{default_value};
}

// ---------------------------------------------------------------------------
// Current path: NetworkChannel<T> with Codec<T>.
// ---------------------------------------------------------------------------
template <typename T>
Measurement run_current_path(const std::string& payload_name,
                             std::vector<T> records,
                             std::size_t batch_size,
                             clink::Codec<T> codec) {
    using namespace clink::network;
    const std::size_t total = records.size();

    NetworkChannelSource<T> source(0, codec, "127.0.0.1");
    const auto port = source.listen();

    NetworkChannelSink<T> sink("127.0.0.1", port, codec);

    std::thread accept_thread([&] { source.accept(); });
    sink.connect();
    accept_thread.join();

    // Consumer thread: drain everything that the sender pushes.
    std::size_t bytes_received = 0;
    std::size_t records_received = 0;
    std::thread consumer([&] {
        while (records_received < total) {
            auto el = source.pop();
            if (!el.has_value()) break;
            if (el->is_data()) {
                records_received += el->as_data().size();
            }
        }
    });

    const auto t0 = std::chrono::steady_clock::now();
    // Push in batches.
    for (std::size_t i = 0; i < total; i += batch_size) {
        const auto n = std::min(batch_size, total - i);
        clink::Batch<T> b;
        for (std::size_t k = 0; k < n; ++k) {
            b.emplace(records[i + k]);
        }
        const auto el = clink::StreamElement<T>::data(std::move(b));
        sink.push(el);
        // Approximate bytes-on-wire: for current path it's roughly
        // (1 + 4) + n * (1 + 8 + 4 + sizeof(value)) for trivial T,
        // or sum of encoded sizes for variable types.
        // For accurate accounting we'd intercept the socket; here we
        // sample one encoding to estimate.
        if (i == 0) {
            for (std::size_t k = 0; k < n; ++k) {
                bytes_received += 1 + 8 + 4 + codec.encode(records[i + k]).size();
            }
            bytes_received *= (total / batch_size + 1);
        }
    }
    sink.close_send();
    consumer.join();
    const auto t1 = std::chrono::steady_clock::now();

    Measurement m;
    m.payload = payload_name;
    m.path = "current";
    m.records = records_received;
    m.bytes_wire = bytes_received;
    m.wall_ms =
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count() / 1'000'000.0;
    return m;
}

#ifdef CLINK_HAS_ARROW

// ---------------------------------------------------------------------------
// Arrow path: per-batch arrow::RecordBatch + arrow::ipc::SerializeRecordBatch.
// We use a raw TCP socket pair (mirroring NetworkChannel's framing) with
// the IPC bytes as the payload.
// ---------------------------------------------------------------------------

// Forward declaration for the type-specific Arrow builder/reader; one
// specialization per payload type.
template <typename T>
std::shared_ptr<arrow::RecordBatch> records_to_recordbatch(const std::vector<T>& records,
                                                            std::size_t offset,
                                                            std::size_t count);

template <>
std::shared_ptr<arrow::RecordBatch> records_to_recordbatch<std::int64_t>(
    const std::vector<std::int64_t>& records, std::size_t offset, std::size_t count) {
    arrow::Int64Builder builder;
    auto status = builder.AppendValues(records.data() + offset, count);
    (void)status;
    std::shared_ptr<arrow::Array> arr;
    status = builder.Finish(&arr);
    (void)status;
    auto schema = arrow::schema({arrow::field("value", arrow::int64())});
    return arrow::RecordBatch::Make(schema, static_cast<int64_t>(count), {arr});
}

template <>
std::shared_ptr<arrow::RecordBatch> records_to_recordbatch<Click>(const std::vector<Click>& records,
                                                                   std::size_t offset,
                                                                   std::size_t count) {
    arrow::Int64Builder user_id_b;
    arrow::Int64Builder ts_b;
    arrow::StringBuilder url_b;
    auto s1 = user_id_b.Reserve(static_cast<int64_t>(count));
    (void)s1;
    auto s2 = ts_b.Reserve(static_cast<int64_t>(count));
    (void)s2;
    auto s3 = url_b.Reserve(static_cast<int64_t>(count));
    (void)s3;
    for (std::size_t k = 0; k < count; ++k) {
        const auto& r = records[offset + k];
        auto su = user_id_b.Append(r.user_id);
        (void)su;
        auto st = ts_b.Append(r.timestamp_ms);
        (void)st;
        auto sl = url_b.Append(r.url);
        (void)sl;
    }
    std::shared_ptr<arrow::Array> user_id_arr, ts_arr, url_arr;
    auto f1 = user_id_b.Finish(&user_id_arr);
    (void)f1;
    auto f2 = ts_b.Finish(&ts_arr);
    (void)f2;
    auto f3 = url_b.Finish(&url_arr);
    (void)f3;
    auto schema = arrow::schema({arrow::field("user_id", arrow::int64()),
                                  arrow::field("timestamp_ms", arrow::int64()),
                                  arrow::field("url", arrow::utf8())});
    return arrow::RecordBatch::Make(
        schema, static_cast<int64_t>(count), {user_id_arr, ts_arr, url_arr});
}

// Send an Arrow RecordBatch as IPC bytes over a TCP socket. Framing:
// [u32 BE length][IPC payload]. Matches the existing NetworkChannel
// framing prefix so the wire shape is comparable.
bool send_ipc(int fd, const std::shared_ptr<arrow::RecordBatch>& batch) {
    auto buffer_result = arrow::ipc::SerializeRecordBatch(*batch,
                                                           arrow::ipc::IpcWriteOptions::Defaults());
    if (!buffer_result.ok()) {
        std::cerr << "SerializeRecordBatch failed: " << buffer_result.status().ToString() << "\n";
        return false;
    }
    auto buffer = *buffer_result;
    const auto len = static_cast<std::uint32_t>(buffer->size());
    std::array<std::byte, 4> header{};
    header[0] = static_cast<std::byte>((len >> 24) & 0xFF);
    header[1] = static_cast<std::byte>((len >> 16) & 0xFF);
    header[2] = static_cast<std::byte>((len >> 8) & 0xFF);
    header[3] = static_cast<std::byte>(len & 0xFF);
    if (!clink::network::NetworkSocket::send_all(fd, header.data(), header.size())) return false;
    return clink::network::NetworkSocket::send_all(
        fd, reinterpret_cast<const std::byte*>(buffer->data()), buffer->size());
}

// Receive one IPC frame; deserialize into a RecordBatch.
std::shared_ptr<arrow::RecordBatch> recv_ipc(int fd, const std::shared_ptr<arrow::Schema>& schema) {
    std::array<std::byte, 4> header{};
    if (!clink::network::NetworkSocket::recv_all(fd, header.data(), header.size())) return nullptr;
    const std::uint32_t len = (static_cast<std::uint32_t>(header[0]) << 24) |
                               (static_cast<std::uint32_t>(header[1]) << 16) |
                               (static_cast<std::uint32_t>(header[2]) << 8) |
                               static_cast<std::uint32_t>(header[3]);
    if (len == 0) return nullptr;
    std::vector<uint8_t> body(len);
    if (!clink::network::NetworkSocket::recv_all(
            fd, reinterpret_cast<std::byte*>(body.data()), body.size()))
        return nullptr;
    auto reader_buffer = std::make_shared<arrow::Buffer>(body.data(), body.size());
    auto reader_input = std::make_shared<arrow::io::BufferReader>(reader_buffer);
    arrow::ipc::DictionaryMemo memo;
    auto batch_result = arrow::ipc::ReadRecordBatch(
        schema, &memo, arrow::ipc::IpcReadOptions::Defaults(), reader_input.get());
    if (!batch_result.ok()) {
        std::cerr << "ReadRecordBatch failed: " << batch_result.status().ToString() << "\n";
        return nullptr;
    }
    return *batch_result;
}

template <typename T>
Measurement run_arrow_path(const std::string& payload_name,
                            std::vector<T> records,
                            std::size_t batch_size) {
    using namespace clink::network;
    const std::size_t total = records.size();

    // Set up listener socket via NetworkSocket helpers (same as the
    // current path so latency profile is comparable).
    std::uint16_t port = 0;
    const int listener_fd = NetworkSocket::listen_on(port, "127.0.0.1");
    if (listener_fd < 0) throw std::runtime_error("listen failed");

    int sender_fd = -1;
    int receiver_fd = -1;
    std::thread accept_thread([&] { receiver_fd = NetworkSocket::accept_one(listener_fd); });
    sender_fd = NetworkSocket::connect_to("127.0.0.1", port);
    accept_thread.join();
    NetworkSocket::close(listener_fd);
    if (sender_fd < 0 || receiver_fd < 0) throw std::runtime_error("socket pair failed");

    auto schema = records_to_recordbatch<T>(records, 0, 1)->schema();
    std::size_t bytes_received = 0;
    std::size_t records_received = 0;

    std::thread consumer([&] {
        while (records_received < total) {
            auto batch = recv_ipc(receiver_fd, schema);
            if (!batch) break;
            records_received += static_cast<std::size_t>(batch->num_rows());
        }
    });

    const auto t0 = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < total; i += batch_size) {
        const auto n = std::min(batch_size, total - i);
        auto batch = records_to_recordbatch<T>(records, i, n);
        // Track bytes per batch for the first batch only and extrapolate.
        if (i == 0) {
            auto buf = arrow::ipc::SerializeRecordBatch(*batch,
                                                         arrow::ipc::IpcWriteOptions::Defaults());
            if (buf.ok()) {
                bytes_received = (*buf)->size() * (total / batch_size + 1);
            }
        }
        send_ipc(sender_fd, batch);
    }
    // Send zero-length frame as EOS.
    std::array<std::byte, 4> eos{};
    NetworkSocket::send_all(sender_fd, eos.data(), eos.size());
    consumer.join();
    const auto t1 = std::chrono::steady_clock::now();

    NetworkSocket::close(sender_fd);
    NetworkSocket::close(receiver_fd);

    Measurement m;
    m.payload = payload_name;
    m.path = "arrow";
    m.records = records_received;
    m.bytes_wire = bytes_received;
    m.wall_ms =
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count() / 1'000'000.0;
    return m;
}

#endif  // CLINK_HAS_ARROW

void print_human(const Measurement& m) {
    const auto rps = m.records_per_sec();
    const auto mbs = m.bytes_per_sec() / (1024.0 * 1024.0);
    std::printf("  %-7s payload=%-6s records=%zu wall=%.2fms rps=%.2fM ns/rec=%.1f wire=%.1f MiB/s\n",
                m.path.c_str(),
                m.payload.c_str(),
                m.records,
                m.wall_ms,
                rps / 1'000'000.0,
                m.ns_per_record(),
                mbs);
}

void write_csv_row(std::ofstream& out, const Measurement& m) {
    out << m.payload << "," << m.path << "," << m.records << "," << m.wall_ms << ","
        << m.records_per_sec() << "," << m.bytes_per_sec() << "," << m.ns_per_record() << "\n";
}

}  // namespace

int main(int argc, char** argv) {
    const std::size_t records = std::stoull(get_arg(argc, argv, "records", "1000000"));
    const std::size_t batch_size = std::stoull(get_arg(argc, argv, "batch-size", "1024"));
    const std::string payload = get_arg(argc, argv, "payload", "all");
    const std::string csv_path = get_arg(argc, argv, "csv", "");

    std::optional<std::ofstream> csv;
    if (!csv_path.empty()) {
        csv.emplace(csv_path);
        *csv << "payload,path,records,wall_ms,records_per_sec,bytes_per_sec,ns_per_record\n";
    }

    std::printf("clink_serde_bench: records=%zu batch_size=%zu payload=%s arrow=%s\n",
                records,
                batch_size,
                payload.c_str(),
                "enabled (always-on)"
    );

    std::vector<Measurement> results;

    // Payload: int64
    if (payload == "int64" || payload == "all") {
        std::vector<std::int64_t> data;
        data.reserve(records);
        for (std::size_t i = 0; i < records; ++i) data.push_back(static_cast<std::int64_t>(i));
        results.push_back(
            run_current_path<std::int64_t>("int64", data, batch_size, clink::int64_codec()));
#ifdef CLINK_HAS_ARROW
        results.push_back(run_arrow_path<std::int64_t>("int64", std::move(data), batch_size));
#endif
    }

    // Payload: click (struct with numeric + string)
    if (payload == "click" || payload == "all") {
        std::vector<Click> data;
        data.reserve(records);
        for (std::size_t i = 0; i < records; ++i) {
            data.push_back(Click{
                .user_id = static_cast<std::int64_t>(i),
                .timestamp_ms = static_cast<std::int64_t>(i * 100),
                .url = "https://example.com/page/" + std::to_string(i % 1000),
            });
        }
        results.push_back(run_current_path<Click>("click", data, batch_size, click_codec()));
#ifdef CLINK_HAS_ARROW
        results.push_back(run_arrow_path<Click>("click", std::move(data), batch_size));
#endif
    }

    std::printf("\nResults:\n");
    for (const auto& m : results) print_human(m);
    if (csv) {
        for (const auto& m : results) write_csv_row(*csv, m);
    }
    return 0;
}
