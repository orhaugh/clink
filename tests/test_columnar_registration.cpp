// End-to-end: register a custom struct with the generated columnar
// batcher and prove it travels columnar through the REAL registration +
// network-bridge path, not just the batcher in isolation.
//
//   1. register_columnar_typed<T> stores TypeOps whose bridges carry the
//      columnar batcher; the registered outbound bridge puts a typed
//      Arrow IPC frame on the wire (raw-socket byte assertion).
//   2. A full loopback through the registered inbound + outbound bridges
//      round-trips a Batch<T> (values + event-time) intact.
//   3. The Codec<T> that backs state serialization round-trips.
//   4. The PluginRegistry helper registers the type too (apply-globally).

#ifndef CLINK_HAS_ARROW
#error "test_columnar_registration requires Arrow"
#endif

#include <array>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/columnar_registration.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/network/network_bridge.hpp"
#include "clink/runtime/network/network_socket.hpp"
#include "clink/runtime/network/wire.hpp"

using namespace clink;
using namespace clink::network;
using clink::cluster::register_columnar_typed;
using clink::cluster::TypeRegistry;

// A custom aggregate with a mix of field types, defined at namespace
// scope with a unique name so its CLINK_ARROW_FIELDS specialisation is
// well-formed and collision-free across test TUs.
struct ColRegTrade {
    std::int64_t id;
    std::string symbol;
    double price;
    std::int32_t qty;
    bool buy;
};

CLINK_ARROW_FIELDS(ColRegTrade, id, symbol, price, qty, buy);

namespace {

// A hand-written Codec<ColRegTrade>: little-endian fixed fields plus a
// length-prefixed symbol. This is the codec that backs state; the
// columnar batcher governs only the wire/Parquet layout.
clink::Codec<ColRegTrade> trade_codec() {
    clink::Codec<ColRegTrade> c;
    c.encode = [](const ColRegTrade& t) {
        std::vector<std::byte> out;
        auto put_u64 = [&](std::uint64_t v) {
            for (int i = 0; i < 8; ++i)
                out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xff));
        };
        auto put_u32 = [&](std::uint32_t v) {
            for (int i = 0; i < 4; ++i)
                out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xff));
        };
        put_u64(static_cast<std::uint64_t>(t.id));
        std::uint64_t pbits = 0;
        std::memcpy(&pbits, &t.price, 8);
        put_u64(pbits);
        put_u32(static_cast<std::uint32_t>(t.qty));
        out.push_back(static_cast<std::byte>(t.buy ? 1 : 0));
        put_u32(static_cast<std::uint32_t>(t.symbol.size()));
        for (char ch : t.symbol)
            out.push_back(static_cast<std::byte>(static_cast<unsigned char>(ch)));
        return out;
    };
    c.decode = [](std::span<const std::byte> buf) -> std::optional<ColRegTrade> {
        std::size_t pos = 0;
        auto need = [&](std::size_t n) { return pos + n <= buf.size(); };
        auto get_u64 = [&](std::uint64_t& v) {
            if (!need(8))
                return false;
            v = 0;
            for (int i = 0; i < 8; ++i)
                v |= static_cast<std::uint64_t>(std::to_integer<unsigned char>(buf[pos + i]))
                     << (i * 8);
            pos += 8;
            return true;
        };
        auto get_u32 = [&](std::uint32_t& v) {
            if (!need(4))
                return false;
            v = 0;
            for (int i = 0; i < 4; ++i)
                v |= static_cast<std::uint32_t>(std::to_integer<unsigned char>(buf[pos + i]))
                     << (i * 8);
            pos += 4;
            return true;
        };
        ColRegTrade t{};
        std::uint64_t idb = 0;
        std::uint64_t pbits = 0;
        std::uint32_t qty = 0;
        std::uint32_t slen = 0;
        if (!get_u64(idb) || !get_u64(pbits) || !get_u32(qty))
            return std::nullopt;
        if (!need(1))
            return std::nullopt;
        const bool buy = std::to_integer<unsigned char>(buf[pos++]) != 0;
        if (!get_u32(slen) || !need(slen))
            return std::nullopt;
        std::string sym(slen, '\0');
        for (std::uint32_t i = 0; i < slen; ++i)
            sym[i] = static_cast<char>(std::to_integer<unsigned char>(buf[pos + i]));
        pos += slen;
        t.id = static_cast<std::int64_t>(idb);
        std::memcpy(&t.price, &pbits, 8);
        t.qty = static_cast<std::int32_t>(qty);
        t.buy = buy;
        t.symbol = std::move(sym);
        return t;
    };
    return c;
}

bool trade_eq(const ColRegTrade& a, const ColRegTrade& b) {
    return a.id == b.id && a.symbol == b.symbol && a.price == b.price && a.qty == b.qty &&
           a.buy == b.buy;
}

}  // namespace

TEST(ColumnarRegistration, CodecRoundTripsForState) {
    auto codec = trade_codec();
    const ColRegTrade in{7, "GOOG", 174.5, 33, false};
    const auto bytes = codec.encode(in);
    const auto out = codec.decode(std::span<const std::byte>{bytes.data(), bytes.size()});
    ASSERT_TRUE(out.has_value());
    EXPECT_TRUE(trade_eq(*out, in));
}

TEST(ColumnarRegistration, RegisteredOutboundBridgePutsTypedColumnsOnWire) {
    TypeRegistry reg;
    register_columnar_typed<ColRegTrade>(reg, "test.col.Trade", trade_codec());
    const auto* ops = reg.find("test.col.Trade");
    ASSERT_NE(ops, nullptr);

    // Raw listener so we can read the actual frame bytes.
    std::uint16_t port = 0;
    const int listener_fd = NetworkSocket::listen_on(port, "127.0.0.1");
    ASSERT_GE(listener_fd, 0);
    ASSERT_GT(port, 0u);

    std::thread sender([&] {
        auto sink_void = ops->connect_outbound_bridge("127.0.0.1", port);
        auto sink = std::static_pointer_cast<NetworkBridgeSink<ColRegTrade>>(sink_void);
        sink->open();
        Batch<ColRegTrade> b;
        b.emplace(ColRegTrade{1, "AAPL", 191.25, 100, true}, EventTime{100});
        b.emplace(ColRegTrade{2, "MSFT", 410.10, 50, false});  // no event-time
        sink->on_data(std::move(b));
        sink->close();
    });

    const int peer_fd = NetworkSocket::accept_one(listener_fd);
    ASSERT_GE(peer_fd, 0);
    NetworkSocket::close(listener_fd);

    // The receiver normally grants initial credit on accept; reproduce it
    // so the sink's push does not block.
    {
        std::vector<std::byte> credit_payload;
        credit_payload.push_back(static_cast<std::byte>(Kind::CreditUpdate));
        put_u32_be(credit_payload, kInitialNetworkCredit);
        std::vector<std::byte> credit_header;
        put_u32_be(credit_header, static_cast<std::uint32_t>(credit_payload.size()));
        ASSERT_TRUE(NetworkSocket::send_all(peer_fd, credit_header.data(), credit_header.size()));
        ASSERT_TRUE(NetworkSocket::send_all(peer_fd, credit_payload.data(), credit_payload.size()));
    }

    std::array<std::byte, 4> hdr_buf{};
    ASSERT_TRUE(NetworkSocket::recv_all(peer_fd, hdr_buf.data(), hdr_buf.size()));
    const std::uint32_t frame_len = read_u32_be(hdr_buf.data());
    ASSERT_GT(frame_len, 0u);
    std::vector<std::byte> body(frame_len);
    ASSERT_TRUE(NetworkSocket::recv_all(peer_fd, body.data(), body.size()));

    // Columnar wire frame, typed per field - NOT the value_bytes:binary fallback.
    ASSERT_EQ(static_cast<Kind>(body[0]), Kind::ArrowBatch);
    auto rb = clink::arrow_batch_from_ipc(body.data() + 1, body.size() - 1);
    ASSERT_NE(rb, nullptr);
    ASSERT_EQ(rb->num_columns(), 6);
    EXPECT_EQ(rb->schema()->field(1)->name(), "id");
    EXPECT_EQ(rb->schema()->field(1)->type()->id(), arrow::Type::INT64);
    EXPECT_EQ(rb->schema()->field(2)->type()->id(), arrow::Type::STRING);
    EXPECT_EQ(rb->schema()->field(3)->type()->id(), arrow::Type::DOUBLE);
    EXPECT_EQ(rb->schema()->field(4)->type()->id(), arrow::Type::INT32);
    EXPECT_EQ(rb->schema()->field(5)->type()->id(), arrow::Type::BOOL);
    for (int i = 0; i < rb->num_columns(); ++i)
        EXPECT_NE(rb->schema()->field(i)->type()->id(), arrow::Type::BINARY);

    NetworkSocket::close(peer_fd);
    sender.join();
}

TEST(ColumnarRegistration, RegisteredBridgesRoundTripLoopback) {
    TypeRegistry reg;
    register_columnar_typed<ColRegTrade>(reg, "test.col.Trade", trade_codec());
    const auto* ops = reg.find("test.col.Trade");
    ASSERT_NE(ops, nullptr);

    // Inbound bridge binds the listener and returns the port.
    auto [port, src_void] = ops->bind_inbound_bridge();
    auto src = std::static_pointer_cast<NetworkBridgeSource<ColRegTrade>>(src_void);
    ASSERT_GT(port, 0u);

    const std::vector<ColRegTrade> sent = {
        {1, "AAPL", 191.25, 100, true},
        {2, "MSFT", 410.10, 50, false},
        {3, "NVDA", 1203.99, 7, true},
    };

    std::thread sender([&] {
        auto sink_void = ops->connect_outbound_bridge("127.0.0.1", port);
        auto sink = std::static_pointer_cast<NetworkBridgeSink<ColRegTrade>>(sink_void);
        sink->open();
        Batch<ColRegTrade> b;
        b.emplace(sent[0], EventTime{1000});
        b.emplace(sent[1], EventTime{1001});
        b.emplace(sent[2]);  // no event-time
        sink->on_data(std::move(b));
        sink->close();
    });

    std::vector<ColRegTrade> got;
    std::vector<bool> got_has_ts;
    std::vector<std::int64_t> got_ts;
    Emitter<ColRegTrade> em([&](StreamElement<ColRegTrade> e) -> bool {
        if (e.is_data()) {
            auto& batch = e.as_data();
            for (std::size_t i = 0; i < batch.size(); ++i) {
                got.push_back(batch[i].value());
                const auto ts = batch[i].event_time();
                got_has_ts.push_back(ts.has_value());
                got_ts.push_back(ts.has_value() ? ts->millis() : -1);
            }
        }
        return true;
    });

    src->open();  // accept
    while (src->produce(em)) {
    }
    sender.join();

    ASSERT_EQ(got.size(), sent.size());
    for (std::size_t i = 0; i < sent.size(); ++i)
        EXPECT_TRUE(trade_eq(got[i], sent[i]));
    EXPECT_TRUE(got_has_ts[0]);
    EXPECT_EQ(got_ts[0], 1000);
    EXPECT_TRUE(got_has_ts[1]);
    EXPECT_EQ(got_ts[1], 1001);
    EXPECT_FALSE(got_has_ts[2]);
}

TEST(ColumnarRegistration, PluginRegistryRegistersColumnarType) {
    TypeRegistry tr;
    clink::plugin::PluginRegistry preg(tr,
                                       clink::cluster::RunnerRegistry::default_instance(),
                                       clink::cluster::SelectorRegistry::default_instance());
    clink::plugin::register_columnar_type<ColRegTrade>(preg, "test.col.PluginTrade", trade_codec());
    EXPECT_NE(tr.find("test.col.PluginTrade"), nullptr);
}
