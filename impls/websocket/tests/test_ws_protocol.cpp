// The RFC 6455 protocol layer, pinned to the RFC's own worked examples so
// the client is never tested only against this repository's own encoder:
// the section 1.3 / 4.2.2 nonce->accept pair, and the section 5.7 masked
// and unmasked "Hello" frames, byte for byte.

#include <cstdint>
#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "clink/websocket/ws_protocol.hpp"

namespace {

using namespace clink::websocket;

std::string bytes(std::initializer_list<unsigned> xs) {
    std::string out;
    for (const unsigned x : xs) {
        out.push_back(static_cast<char>(static_cast<unsigned char>(x)));
    }
    return out;
}

// --- URL ---------------------------------------------------------------

TEST(WsUrlParse, SchemesHostsPortsPaths) {
    auto u = parse_ws_url("ws://example.test/feed?symbols=all");
    ASSERT_TRUE(u.has_value());
    EXPECT_FALSE(u->tls);
    EXPECT_EQ(u->host, "example.test");
    EXPECT_EQ(u->port, 80);
    EXPECT_EQ(u->path, "/feed?symbols=all");

    u = parse_ws_url("wss://stream.example.test:9443/ws");
    ASSERT_TRUE(u.has_value());
    EXPECT_TRUE(u->tls);
    EXPECT_EQ(u->port, 9443);

    u = parse_ws_url("wss://stream.example.test");
    ASSERT_TRUE(u.has_value());
    EXPECT_EQ(u->port, 443);
    EXPECT_EQ(u->path, "/");

    EXPECT_FALSE(parse_ws_url("http://example.test/").has_value());
    EXPECT_FALSE(parse_ws_url("ws://:9000/x").has_value());
    EXPECT_FALSE(parse_ws_url("ws://host:0/x").has_value());
    EXPECT_FALSE(parse_ws_url("ws://host:99999/x").has_value());
}

// --- handshake ----------------------------------------------------------

TEST(WsHandshake, AcceptKeyMatchesTheRfcExample) {
    // RFC 6455 sections 1.3 and 4.2.2: the sample nonce and its accept.
    EXPECT_EQ(accept_key_for("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
}

TEST(WsHandshake, ResponseParserExtractsStatusAndHeaders) {
    const std::string head =
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"
        "\r\n";
    // Incomplete head: keep asking for bytes.
    EXPECT_FALSE(parse_handshake_response(head.substr(0, head.size() - 1)).has_value());

    const auto resp = parse_handshake_response(head + "extra-frame-bytes");
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->status, 101);
    EXPECT_EQ(resp->header_bytes, head.size());
    const auto* accept = find_header(*resp, "sec-websocket-accept");
    ASSERT_NE(accept, nullptr);
    EXPECT_EQ(*accept, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
}

TEST(WsHandshake, RequestCarriesTheUpgradeHeaders) {
    const auto req =
        build_handshake_request("server.example.test", "/chat", "dGhlIHNhbXBsZSBub25jZQ==");
    EXPECT_NE(req.find("GET /chat HTTP/1.1\r\n"), std::string::npos);
    EXPECT_NE(req.find("Host: server.example.test\r\n"), std::string::npos);
    EXPECT_NE(req.find("Upgrade: websocket\r\n"), std::string::npos);
    EXPECT_NE(req.find("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"), std::string::npos);
    EXPECT_NE(req.find("Sec-WebSocket-Version: 13\r\n"), std::string::npos);
}

// --- frames --------------------------------------------------------------

TEST(WsFrames, UnmaskedHelloMatchesTheRfcExample) {
    // RFC 6455 section 5.7: a single-frame unmasked text message "Hello".
    EXPECT_EQ(encode_frame(Opcode::Text, "Hello", /*mask=*/false),
              bytes({0x81, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f}));
}

TEST(WsFrames, MaskedHelloMatchesTheRfcExample) {
    // RFC 6455 section 5.7: the same message masked with key 37 fa 21 3d.
    EXPECT_EQ(encode_frame(Opcode::Text, "Hello", /*mask=*/true, 0x37FA213Du),
              bytes({0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58}));
}

TEST(WsFrames, DecoderReadsMaskedAndUnmaskedFrames) {
    FrameDecoder dec;
    std::string err;
    const auto unmasked = bytes({0x81, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f});
    const auto masked = bytes({0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58});
    dec.append(unmasked.data(), unmasked.size());
    dec.append(masked.data(), masked.size());

    auto f = dec.next_frame(err);
    ASSERT_TRUE(f.has_value());
    EXPECT_EQ(f->opcode, Opcode::Text);
    EXPECT_TRUE(f->fin);
    EXPECT_EQ(f->payload, "Hello");

    f = dec.next_frame(err);
    ASSERT_TRUE(f.has_value());
    EXPECT_EQ(f->payload, "Hello");
    EXPECT_TRUE(err.empty());
}

TEST(WsFrames, DecoderHandlesBytewiseArrival) {
    FrameDecoder dec;
    std::string err;
    const auto frame = bytes({0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58});
    for (const char c : frame) {
        EXPECT_FALSE(dec.next_frame(err).has_value());
        dec.append(&c, 1);
    }
    const auto f = dec.next_frame(err);
    ASSERT_TRUE(f.has_value());
    EXPECT_EQ(f->payload, "Hello");
}

TEST(WsFrames, ExtendedLengthsRoundTrip) {
    // 16-bit and 64-bit length paths (RFC 6455 section 5.7's 256-byte and
    // 64 KiB examples, generalised): encode then decode, sizes preserved.
    for (const std::size_t n : {std::size_t{126}, std::size_t{300}, std::size_t{70'000}}) {
        const std::string payload(n, 'x');
        const auto wire = encode_frame(Opcode::Binary, payload, /*mask=*/false);
        FrameDecoder dec;
        std::string err;
        dec.append(wire.data(), wire.size());
        const auto f = dec.next_frame(err);
        ASSERT_TRUE(f.has_value()) << n;
        EXPECT_EQ(f->opcode, Opcode::Binary);
        EXPECT_EQ(f->payload.size(), n);
    }
}

TEST(WsFrames, OversizedFrameIsAProtocolError) {
    FrameDecoder dec(/*max_payload=*/16);
    std::string err;
    const auto wire = encode_frame(Opcode::Text, std::string(17, 'x'), /*mask=*/false);
    dec.append(wire.data(), wire.size());
    EXPECT_FALSE(dec.next_frame(err).has_value());
    EXPECT_FALSE(err.empty());
}

// --- messages -------------------------------------------------------------

TEST(WsMessages, FragmentedTextReassembles) {
    // "Hel" (fin=0, text) + "lo" (fin=1, continuation) - the RFC 5.4 shape.
    MessageReader reader;
    std::string err;
    const auto first = encode_frame(Opcode::Text, "Hel", /*mask=*/false, 0, /*fin=*/false);
    const auto rest = encode_frame(Opcode::Continuation, "lo", /*mask=*/false, 0, /*fin=*/true);
    reader.append(first.data(), first.size());
    EXPECT_FALSE(reader.next_message(err).has_value());
    reader.append(rest.data(), rest.size());
    const auto msg = reader.next_message(err);
    ASSERT_TRUE(msg.has_value());
    EXPECT_EQ(msg->opcode, Opcode::Text);
    EXPECT_EQ(msg->payload, "Hello");
}

TEST(WsMessages, ControlFramesInterleaveWithAFragmentedMessage) {
    MessageReader reader;
    std::string err;
    const auto first = encode_frame(Opcode::Text, "Hel", /*mask=*/false, 0, /*fin=*/false);
    const auto ping = encode_frame(Opcode::Ping, "k", /*mask=*/false);
    const auto rest = encode_frame(Opcode::Continuation, "lo", /*mask=*/false, 0, /*fin=*/true);
    reader.append(first.data(), first.size());
    reader.append(ping.data(), ping.size());
    reader.append(rest.data(), rest.size());

    auto msg = reader.next_message(err);
    ASSERT_TRUE(msg.has_value());
    EXPECT_EQ(msg->opcode, Opcode::Ping);
    EXPECT_EQ(msg->payload, "k");

    msg = reader.next_message(err);
    ASSERT_TRUE(msg.has_value());
    EXPECT_EQ(msg->opcode, Opcode::Text);
    EXPECT_EQ(msg->payload, "Hello");
}

TEST(WsMessages, StrayContinuationIsAProtocolError) {
    MessageReader reader;
    std::string err;
    const auto stray = encode_frame(Opcode::Continuation, "x", /*mask=*/false);
    reader.append(stray.data(), stray.size());
    EXPECT_FALSE(reader.next_message(err).has_value());
    EXPECT_FALSE(err.empty());
}

}  // namespace
