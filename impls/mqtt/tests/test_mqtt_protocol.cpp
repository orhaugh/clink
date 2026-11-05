// The MQTT 3.1.1 and 5 packet codec the source's subscriber speaks: each
// encoding byte for byte against the standards' layouts, the reader cutting a
// stream into packets, and the malformed inputs it refuses. No broker.

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#ifdef CLINK_HAS_MQTT
#include "clink/mqtt/mqtt_protocol.hpp"
#endif

#ifdef CLINK_HAS_MQTT

namespace proto = clink::mqtt::protocol;
using proto::PacketType;
using proto::ProtocolError;

namespace {

std::string bytes(std::initializer_list<int> b) {
    std::string out;
    for (const int v : b) {
        out.push_back(static_cast<char>(v));
    }
    return out;
}

}  // namespace

TEST(MqttProtocol, AnMqtt5ConnectStatesItsSessionExpiryAndReceiveMaximum) {
    proto::ConnectFields f;
    f.version = proto::kMqtt5;
    f.client_id = "c";
    f.keepalive_s = 30;
    f.session_expiry_s = proto::kSessionNeverExpires;
    f.receive_maximum = 1000;
    EXPECT_EQ(proto::encode_connect(f),
              bytes({0x10, 22,                                  // CONNECT, remaining length
                     0x00, 0x04, 'M',  'Q',  'T',  'T',  0x05,  // protocol name, level 5
                     0x00,                                      // flags: no clean start
                     0x00, 30,                                  // keepalive
                     8,    0x11, 0xFF, 0xFF, 0xFF, 0xFF, 0x21, 0x03, 0xE8,  // properties
                     0x00, 0x01, 'c'}));                                    // client id
}

TEST(MqttProtocol, AnMqtt311ConnectCarriesNoPropertiesAndItsCredentials) {
    proto::ConnectFields f;
    f.version = proto::kMqtt311;
    f.client_id = "c";
    f.keepalive_s = 60;
    f.username = "u";
    f.password = "p";
    f.session_expiry_s = proto::kSessionNeverExpires;  // not MQTT 3.1.1's to send
    f.receive_maximum = 1000;
    EXPECT_EQ(proto::encode_connect(f),
              bytes({0x10, 19, 0x00, 0x04, 'M', 'Q',  'T',  'T', 0x04,
                     0xC0,  // user name and password flags, no clean session
                     0x00, 60, 0x00, 0x01, 'c', 0x00, 0x01, 'u', 0x00, 0x01, 'p'}));
}

TEST(MqttProtocol, MqttThreeOneOneSendsNoPasswordWithoutAUserName) {
    proto::ConnectFields f;
    f.version = proto::kMqtt311;
    f.client_id = "c";
    f.password = "p";
    EXPECT_THROW(proto::encode_connect(f), ProtocolError);
    f.version = proto::kMqtt5;
    EXPECT_NO_THROW(proto::encode_connect(f));
}

TEST(MqttProtocol, ASubscribeCarriesItsQosAndUnderMqtt5ItsRetainHandling) {
    EXPECT_EQ(proto::encode_subscribe(proto::kMqtt5, 7, "a/b", 1, 1),
              bytes({0x82, 9, 0x00, 0x07, 0x00, 0x00, 0x03, 'a', '/', 'b', 0x11}));
    EXPECT_EQ(proto::encode_subscribe(proto::kMqtt311, 7, "a/b", 2, 1),
              bytes({0x82, 8, 0x00, 0x07, 0x00, 0x03, 'a', '/', 'b', 0x02}));
    EXPECT_THROW(proto::encode_subscribe(proto::kMqtt5, 0, "a", 1), ProtocolError);
    EXPECT_THROW(proto::encode_subscribe(proto::kMqtt5, 1, "a", 3), ProtocolError);
}

TEST(MqttProtocol, AcknowledgementsPingAndDisconnectAreTheirFixedEncodings) {
    EXPECT_EQ(proto::encode_ack(PacketType::Puback, 0x1234), bytes({0x40, 2, 0x12, 0x34}));
    EXPECT_EQ(proto::encode_ack(PacketType::Pubrec, 0x1234), bytes({0x50, 2, 0x12, 0x34}));
    EXPECT_EQ(proto::encode_ack(PacketType::Pubrel, 0x1234), bytes({0x62, 2, 0x12, 0x34}));
    EXPECT_EQ(proto::encode_ack(PacketType::Pubcomp, 0x1234), bytes({0x70, 2, 0x12, 0x34}));
    EXPECT_EQ(proto::encode_pingreq(), bytes({0xC0, 0}));
    EXPECT_EQ(proto::encode_disconnect(), bytes({0xE0, 0}));
}

TEST(MqttProtocol, TheReaderWaitsForAWholePacketAndCutsSeveralFromOneChunk) {
    const std::string a = proto::packet(PacketType::Pingresp, 0, {});
    std::string body;
    proto::put_string(body, "t");
    body += "payload";
    const std::string b = proto::packet(PacketType::Publish, 0, body);

    proto::PacketReader r;
    for (std::size_t i = 0; i + 1 < b.size(); ++i) {
        r.append(&b[i], 1);
        EXPECT_FALSE(r.next().has_value()) << "a packet handed over before its last byte";
    }
    r.append(&b[b.size() - 1], 1);
    auto p = r.next();
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->type, PacketType::Publish);
    EXPECT_EQ(p->body, body);

    const std::string both = a + b;
    r.append(both.data(), both.size());
    p = r.next();
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->type, PacketType::Pingresp);
    p = r.next();
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->type, PacketType::Publish);
    EXPECT_FALSE(r.next().has_value());
    EXPECT_EQ(r.buffered(), 0u);
}

TEST(MqttProtocol, TheReaderTakesAMultiByteRemainingLength) {
    const std::string body(200, 'x');  // two length bytes
    const std::string pkt = proto::packet(PacketType::Publish, 0, body);
    ASSERT_EQ(static_cast<std::uint8_t>(pkt[1]), 0xC8);
    ASSERT_EQ(static_cast<std::uint8_t>(pkt[2]), 0x01);
    proto::PacketReader r;
    r.append(pkt.data(), pkt.size());
    const auto p = r.next();
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->body.size(), 200u);
}

TEST(MqttProtocol, TheReaderRefusesMalformedLengthsAndTheReservedType) {
    {
        proto::PacketReader r;
        const auto five = bytes({0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x01});
        r.append(five.data(), five.size());
        EXPECT_THROW(r.next(), ProtocolError);
    }
    {
        proto::PacketReader r(10);
        const auto big = proto::packet(PacketType::Publish, 0, std::string(11, 'x'));
        r.append(big.data(), big.size());
        EXPECT_THROW(r.next(), ProtocolError);
    }
    {
        proto::PacketReader r;
        const auto zero = bytes({0x00, 0x00});
        r.append(zero.data(), zero.size());
        EXPECT_THROW(r.next(), ProtocolError);
    }
}

TEST(MqttProtocol, AnMqtt5PublishIsReadPastItsProperties) {
    std::string body;
    proto::put_string(body, "s/1");
    proto::put_u16(body, 5);
    std::string props;
    proto::put_u8(props, 0x02);  // Message Expiry Interval
    proto::put_u32(props, 60);
    proto::put_u8(props, 0x26);  // User Property
    proto::put_string(props, "k");
    proto::put_string(props, "v");
    proto::put_u8(props, 0x0B);  // Subscription Identifier
    proto::put_varint(props, 300);
    proto::put_varint(body, static_cast<std::uint32_t>(props.size()));
    body += props;
    body += "hello";
    const auto m = proto::decode_publish(proto::kMqtt5, 0x0B, body);  // DUP, QoS 1, retain
    EXPECT_EQ(m.topic, "s/1");
    EXPECT_EQ(m.packet_id, 5);
    EXPECT_EQ(m.qos, 1);
    EXPECT_TRUE(m.dup);
    EXPECT_TRUE(m.retain);
    EXPECT_EQ(m.payload, "hello");
}

TEST(MqttProtocol, AnMqtt311PublishHasNoProperties) {
    std::string body;
    proto::put_string(body, "t");
    proto::put_u16(body, 9);
    body += "x";
    const auto m = proto::decode_publish(proto::kMqtt311, 0x04, body);  // QoS 2
    EXPECT_EQ(m.qos, 2);
    EXPECT_EQ(m.packet_id, 9);
    EXPECT_EQ(m.payload, "x");
    EXPECT_FALSE(m.dup);

    std::string q0;
    proto::put_string(q0, "t");
    q0 += "y";
    const auto z = proto::decode_publish(proto::kMqtt311, 0x00, q0);
    EXPECT_EQ(z.qos, 0);
    EXPECT_EQ(z.packet_id, 0);
    EXPECT_EQ(z.payload, "y");
}

TEST(MqttProtocol, APublishThisClientCannotHaveIsRefused) {
    std::string alias;
    proto::put_string(alias, "t");
    proto::put_u16(alias, 1);
    proto::put_varint(alias, 3);
    proto::put_u8(alias, 0x23);  // Topic Alias, never allowed by this client
    proto::put_u16(alias, 1);
    EXPECT_THROW(proto::decode_publish(proto::kMqtt5, 0x02, alias), ProtocolError);

    std::string zero_id;
    proto::put_string(zero_id, "t");
    proto::put_u16(zero_id, 0);
    EXPECT_THROW(proto::decode_publish(proto::kMqtt311, 0x02, zero_id), ProtocolError);

    std::string any;
    proto::put_string(any, "t");
    proto::put_u16(any, 1);
    EXPECT_THROW(proto::decode_publish(proto::kMqtt311, 0x06, any), ProtocolError);  // QoS 3

    std::string empty_topic;
    proto::put_string(empty_topic, "");
    EXPECT_THROW(proto::decode_publish(proto::kMqtt311, 0x00, empty_topic), ProtocolError);

    EXPECT_THROW(proto::decode_publish(proto::kMqtt311, 0x02, bytes({0x00, 0x05, 't'})),
                 ProtocolError);  // ends inside the topic
}

TEST(MqttProtocol, AConnackSaysWhetherTheSessionResumedAndTheBrokersKeepalive) {
    const auto v5 = proto::decode_connack(proto::kMqtt5, bytes({0x01, 0x00, 3, 0x13, 0x00, 0x14}));
    EXPECT_TRUE(v5.session_present);
    EXPECT_EQ(v5.code, 0);
    ASSERT_TRUE(v5.props.server_keep_alive.has_value());
    EXPECT_EQ(*v5.props.server_keep_alive, 20);

    // A broker that does not speak MQTT 5 answers in MQTT 3.1.1.
    const auto old = proto::decode_connack(proto::kMqtt5, bytes({0x00, 0x01}));
    EXPECT_FALSE(old.session_present);
    EXPECT_EQ(old.code, 0x01);

    EXPECT_THROW(proto::decode_connack(proto::kMqtt311, bytes({0x02, 0x00})), ProtocolError);
    EXPECT_EQ(proto::reason_text(proto::kMqtt311, 0x05), "not authorized");
    EXPECT_EQ(proto::reason_text(proto::kMqtt5, 0x86), "bad user name or password");
    EXPECT_EQ(proto::reason_text(proto::kMqtt5, 0x7E), "reason code 0x7E");
}

TEST(MqttProtocol, ASubackCarriesTheGrantedQosOrTheRefusal) {
    const auto ok = proto::decode_suback(proto::kMqtt311, bytes({0x00, 0x07, 0x01}));
    EXPECT_EQ(ok.packet_id, 7);
    EXPECT_EQ(ok.codes, (std::vector<std::uint8_t>{0x01}));

    const auto refused = proto::decode_suback(proto::kMqtt5, bytes({0x00, 0x07, 0x00, 0x9E}));
    EXPECT_EQ(refused.codes, (std::vector<std::uint8_t>{0x9E}));
    EXPECT_EQ(proto::reason_text(proto::kMqtt5, 0x9E), "shared subscriptions not supported");

    EXPECT_THROW(proto::decode_suback(proto::kMqtt311, bytes({0x00, 0x07})), ProtocolError);
}

TEST(MqttProtocol, ABrokersDisconnectCarriesItsReason) {
    const auto d = proto::decode_disconnect(bytes({0x8E, 0x00}));
    EXPECT_EQ(d.reason, 0x8E);
    EXPECT_EQ(proto::reason_text(proto::kMqtt5, d.reason), "session taken over");
    EXPECT_EQ(proto::decode_disconnect({}).reason, 0x00);
}

TEST(MqttProtocol, AnUnknownOrTruncatedPropertyIsRefused) {
    const auto unknown = bytes({2, 0x7F, 0x00});
    const auto too_long = bytes({9, 0x11, 0x00});         // longer than the packet
    const auto cut_short = bytes({3, 0x11, 0x00, 0x00});  // a four-byte value cut short
    for (const auto* body : {&unknown, &too_long, &cut_short}) {
        proto::Cursor c(*body);
        EXPECT_THROW(proto::read_properties(c), ProtocolError);
    }
    EXPECT_EQ(proto::decode_packet_id(bytes({0x01, 0x02, 0x00, 0x00})), 0x0102);
    EXPECT_THROW(proto::decode_packet_id(bytes({0x00, 0x00})), ProtocolError);
}

#endif  // CLINK_HAS_MQTT
