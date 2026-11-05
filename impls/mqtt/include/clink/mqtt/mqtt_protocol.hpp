#pragma once

// MQTT control packets as the source's subscriber speaks them, for MQTT 3.1.1
// and MQTT 5.0: the packets it sends (CONNECT, SUBSCRIBE, PUBACK, PUBREC,
// PUBCOMP, PINGREQ, DISCONNECT), a reader that cuts the byte stream into
// packets, and the decoding of what a broker sends a subscriber (CONNACK,
// SUBACK, PUBLISH, PUBREL, DISCONNECT). Pure functions over bytes with no
// socket, so every encoding and every malformed input is testable offline. A
// malformed packet throws ProtocolError.

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace clink::mqtt::protocol {

// CONNECT's protocol level.
inline constexpr std::uint8_t kMqtt311 = 4;
inline constexpr std::uint8_t kMqtt5 = 5;

// The most a four-byte Remaining Length can say.
inline constexpr std::size_t kMaxRemainingLength = 268'435'455;

// MQTT 5's Session Expiry Interval for a session that never expires.
inline constexpr std::uint32_t kSessionNeverExpires = 0xFFFFFFFF;

// MQTT 5's Receive Maximum when CONNECT does not state one.
inline constexpr std::uint16_t kDefaultReceiveMaximum = 65535;

enum class PacketType : std::uint8_t {
    Connect = 1,
    Connack = 2,
    Publish = 3,
    Puback = 4,
    Pubrec = 5,
    Pubrel = 6,
    Pubcomp = 7,
    Subscribe = 8,
    Suback = 9,
    Unsubscribe = 10,
    Unsuback = 11,
    Pingreq = 12,
    Pingresp = 13,
    Disconnect = 14,
    Auth = 15,
};

class ProtocolError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// ---- encoding ----------------------------------------------------------------

inline void put_u8(std::string& out, std::uint8_t v) {
    out.push_back(static_cast<char>(v));
}

inline void put_u16(std::string& out, std::uint16_t v) {
    put_u8(out, static_cast<std::uint8_t>(v >> 8));
    put_u8(out, static_cast<std::uint8_t>(v & 0xFF));
}

inline void put_u32(std::string& out, std::uint32_t v) {
    put_u16(out, static_cast<std::uint16_t>(v >> 16));
    put_u16(out, static_cast<std::uint16_t>(v & 0xFFFF));
}

inline void put_varint(std::string& out, std::uint32_t v) {
    do {
        auto b = static_cast<std::uint8_t>(v % 128);
        v /= 128;
        if (v > 0) {
            b |= 0x80;
        }
        put_u8(out, b);
    } while (v > 0);
}

// A UTF-8 string or binary data field: a two-byte length, then the bytes.
inline void put_string(std::string& out, std::string_view s) {
    if (s.size() > 0xFFFF) {
        throw ProtocolError("mqtt: a string field longer than 65535 bytes");
    }
    put_u16(out, static_cast<std::uint16_t>(s.size()));
    out.append(s);
}

// A whole control packet: the fixed header (type, flags, Remaining Length), then `body`.
inline std::string packet(PacketType type, std::uint8_t flags, std::string_view body) {
    if (body.size() > kMaxRemainingLength) {
        throw ProtocolError("mqtt: a packet longer than MQTT allows");
    }
    std::string out;
    out.reserve(body.size() + 5);
    put_u8(out, static_cast<std::uint8_t>((static_cast<std::uint8_t>(type) << 4) | (flags & 0x0F)));
    put_varint(out, static_cast<std::uint32_t>(body.size()));
    out.append(body);
    return out;
}

struct ConnectFields {
    std::uint8_t version{kMqtt5};
    std::string client_id;
    bool clean_start{false};
    std::uint16_t keepalive_s{60};
    std::optional<std::string> username;
    std::optional<std::string> password;
    // MQTT 5 only: how long the broker keeps the session after the connection
    // ends, and how many unacknowledged QoS 1 and 2 messages it may send at once.
    std::uint32_t session_expiry_s{0};
    std::uint16_t receive_maximum{kDefaultReceiveMaximum};
};

inline std::string encode_connect(const ConnectFields& f) {
    if (f.version != kMqtt311 && f.version != kMqtt5) {
        throw ProtocolError("mqtt: protocol level " + std::to_string(f.version) +
                            " is neither MQTT 3.1.1 (4) nor MQTT 5 (5)");
    }
    if (f.version == kMqtt311 && f.password && !f.username) {
        throw ProtocolError("mqtt: MQTT 3.1.1 sends a password only with a user name");
    }
    if (f.receive_maximum == 0) {
        throw ProtocolError("mqtt: a Receive Maximum of 0");
    }
    std::string body;
    put_string(body, "MQTT");
    put_u8(body, f.version);
    std::uint8_t flags = 0;
    if (f.clean_start) {
        flags |= 0x02;
    }
    if (f.password) {
        flags |= 0x40;
    }
    if (f.username) {
        flags |= 0x80;
    }
    put_u8(body, flags);
    put_u16(body, f.keepalive_s);
    if (f.version == kMqtt5) {
        std::string props;
        if (f.session_expiry_s != 0) {
            put_u8(props, 0x11);
            put_u32(props, f.session_expiry_s);
        }
        if (f.receive_maximum != kDefaultReceiveMaximum) {
            put_u8(props, 0x21);
            put_u16(props, f.receive_maximum);
        }
        put_varint(body, static_cast<std::uint32_t>(props.size()));
        body += props;
    }
    put_string(body, f.client_id);
    if (f.username) {
        put_string(body, *f.username);
    }
    if (f.password) {
        put_string(body, *f.password);
    }
    return packet(PacketType::Connect, 0, body);
}

// SUBSCRIBE to one filter. retain_handling is MQTT 5's subscription option: 0
// sends retained messages at every subscribe, 1 only when the subscription is
// new, 2 never. MQTT 3.1.1 has no such option and always sends them.
inline std::string encode_subscribe(std::uint8_t version,
                                    std::uint16_t packet_id,
                                    std::string_view filter,
                                    std::uint8_t qos,
                                    std::uint8_t retain_handling = 0) {
    if (packet_id == 0) {
        throw ProtocolError("mqtt: SUBSCRIBE with packet identifier 0");
    }
    if (qos > 2) {
        throw ProtocolError("mqtt: SUBSCRIBE with QoS " + std::to_string(qos));
    }
    std::string body;
    put_u16(body, packet_id);
    if (version == kMqtt5) {
        put_varint(body, 0);  // no properties
    }
    put_string(body, filter);
    auto options = static_cast<std::uint8_t>(qos);
    if (version == kMqtt5) {
        options |= static_cast<std::uint8_t>((retain_handling & 0x03) << 4);
    }
    put_u8(body, options);
    return packet(PacketType::Subscribe, 0x02, body);
}

// PUBACK, PUBREC or PUBCOMP with success. MQTT 5 lets the reason code and the
// properties go when the reason is success and there are none, which leaves
// exactly MQTT 3.1.1's encoding.
inline std::string encode_ack(PacketType type, std::uint16_t packet_id) {
    std::string body;
    put_u16(body, packet_id);
    return packet(type, type == PacketType::Pubrel ? 0x02 : 0x00, body);
}

inline std::string encode_pingreq() {
    return packet(PacketType::Pingreq, 0, {});
}

// DISCONNECT, normal. MQTT 5 lets reason 0x00 with no properties be left out,
// which is MQTT 3.1.1's empty DISCONNECT.
inline std::string encode_disconnect() {
    return packet(PacketType::Disconnect, 0, {});
}

// ---- decoding ----------------------------------------------------------------

struct Packet {
    PacketType type{PacketType::Connect};
    std::uint8_t flags{0};
    std::string body;
};

// Cuts a byte stream into packets. Bytes go in as they arrive; next() hands
// back each whole packet once all of it is here.
class PacketReader {
public:
    explicit PacketReader(std::size_t max_remaining = kMaxRemainingLength)
        : max_remaining_(max_remaining) {}

    void append(const char* data, std::size_t len) { buf_.append(data, len); }

    // The next whole packet, if one has arrived. Throws on a Remaining Length
    // that is malformed or above the bound, and on the reserved type 0.
    std::optional<Packet> next() {
        if (buf_.size() - pos_ < 2) {
            return std::nullopt;
        }
        std::size_t len = 0;
        std::size_t mult = 1;
        std::size_t i = pos_ + 1;
        for (int n = 0;; ++n) {
            if (n == 4) {
                throw ProtocolError("mqtt: a Remaining Length longer than four bytes");
            }
            if (i >= buf_.size()) {
                return std::nullopt;
            }
            const auto b = static_cast<std::uint8_t>(buf_[i++]);
            len += static_cast<std::size_t>(b & 0x7F) * mult;
            mult *= 128;
            if ((b & 0x80) == 0) {
                break;
            }
        }
        if (len > max_remaining_) {
            throw ProtocolError("mqtt: a packet of " + std::to_string(len) +
                                " bytes, above the bound of " + std::to_string(max_remaining_));
        }
        if (buf_.size() - i < len) {
            return std::nullopt;
        }
        const auto head = static_cast<std::uint8_t>(buf_[pos_]);
        if ((head >> 4) == 0) {
            throw ProtocolError("mqtt: a packet of the reserved type 0");
        }
        Packet p{static_cast<PacketType>(head >> 4),
                 static_cast<std::uint8_t>(head & 0x0F),
                 buf_.substr(i, len)};
        pos_ = i + len;
        if (pos_ == buf_.size()) {
            buf_.clear();
            pos_ = 0;
        } else if (pos_ > 65536 && pos_ * 2 > buf_.size()) {
            buf_.erase(0, pos_);
            pos_ = 0;
        }
        return p;
    }

    [[nodiscard]] std::size_t buffered() const noexcept { return buf_.size() - pos_; }

private:
    std::size_t max_remaining_;
    std::string buf_;
    std::size_t pos_{0};
};

// Reads fields off a packet's body, throwing when the body ends first.
class Cursor {
public:
    explicit Cursor(std::string_view body) : b_(body) {}

    std::uint8_t u8() {
        need(1, "byte");
        return static_cast<std::uint8_t>(b_[p_++]);
    }
    std::uint16_t u16() {
        need(2, "two-byte integer");
        const auto v = static_cast<std::uint16_t>((static_cast<std::uint8_t>(b_[p_]) << 8) |
                                                  static_cast<std::uint8_t>(b_[p_ + 1]));
        p_ += 2;
        return v;
    }
    std::uint32_t u32() {
        const std::uint32_t hi = u16();
        return (hi << 16) | u16();
    }
    std::uint32_t varint() {
        std::uint32_t v = 0;
        std::uint32_t mult = 1;
        for (int n = 0; n < 4; ++n) {
            const auto b = u8();
            v += static_cast<std::uint32_t>(b & 0x7F) * mult;
            if ((b & 0x80) == 0) {
                return v;
            }
            mult *= 128;
        }
        throw ProtocolError("mqtt: a variable byte integer longer than four bytes");
    }
    std::string_view take(std::size_t n) {
        need(n, "field");
        const auto v = b_.substr(p_, n);
        p_ += n;
        return v;
    }
    std::string_view str() { return take(u16()); }
    std::string_view rest() {
        const auto v = b_.substr(p_);
        p_ = b_.size();
        return v;
    }
    [[nodiscard]] bool empty() const noexcept { return p_ == b_.size(); }
    [[nodiscard]] std::size_t left() const noexcept { return b_.size() - p_; }

private:
    void need(std::size_t n, const char* what) const {
        if (b_.size() - p_ < n) {
            throw ProtocolError(std::string("mqtt: a packet that ends inside a ") + what);
        }
    }

    std::string_view b_;
    std::size_t p_{0};
};

// The MQTT 5 properties a subscriber acts on; the rest are read past.
struct Properties {
    std::optional<std::uint32_t> session_expiry;     // 0x11
    std::optional<std::uint16_t> server_keep_alive;  // 0x13
    std::optional<std::uint16_t> receive_maximum;    // 0x21
    std::optional<std::uint16_t> topic_alias;        // 0x23
    std::optional<std::uint8_t> maximum_qos;         // 0x24
    std::string reason_string;                       // 0x1F
};

inline Properties read_properties(Cursor& c) {
    const auto len = c.varint();
    if (len > c.left()) {
        throw ProtocolError("mqtt: properties longer than their packet");
    }
    Cursor p(c.take(len));
    Properties out;
    while (!p.empty()) {
        const auto id = p.varint();
        switch (id) {
            case 0x01:  // Payload Format Indicator
            case 0x17:  // Request Problem Information
            case 0x19:  // Request Response Information
            case 0x25:  // Retain Available
            case 0x28:  // Wildcard Subscription Available
            case 0x29:  // Subscription Identifier Available
            case 0x2A:  // Shared Subscription Available
                p.u8();
                break;
            case 0x24:
                out.maximum_qos = p.u8();
                break;
            case 0x22:  // Topic Alias Maximum
                p.u16();
                break;
            case 0x21:
                out.receive_maximum = p.u16();
                break;
            case 0x13:
                out.server_keep_alive = p.u16();
                break;
            case 0x23:
                out.topic_alias = p.u16();
                break;
            case 0x02:  // Message Expiry Interval
            case 0x18:  // Will Delay Interval
            case 0x27:  // Maximum Packet Size
                p.u32();
                break;
            case 0x11:
                out.session_expiry = p.u32();
                break;
            case 0x0B:  // Subscription Identifier
                p.varint();
                break;
            case 0x03:  // Content Type
            case 0x08:  // Response Topic
            case 0x09:  // Correlation Data
            case 0x12:  // Assigned Client Identifier
            case 0x15:  // Authentication Method
            case 0x16:  // Authentication Data
            case 0x1A:  // Response Information
            case 0x1C:  // Server Reference
                p.str();
                break;
            case 0x1F:
                out.reason_string = std::string(p.str());
                break;
            case 0x26:  // User Property: a name and a value
                p.str();
                p.str();
                break;
            default:
                throw ProtocolError("mqtt: unknown property identifier " + std::to_string(id));
        }
    }
    return out;
}

struct Connack {
    bool session_present{false};
    std::uint8_t code{0};  // 0 accepts; anything else refuses
    Properties props;
};

inline Connack decode_connack(std::uint8_t version, std::string_view body) {
    Cursor c(body);
    Connack out;
    const auto flags = c.u8();
    if ((flags & 0xFE) != 0) {
        throw ProtocolError("mqtt: CONNACK with reserved flags set");
    }
    out.session_present = (flags & 0x01) != 0;
    out.code = c.u8();
    // A broker that does not speak MQTT 5 answers its CONNECT in MQTT 3.1.1, with
    // no properties.
    if (version == kMqtt5 && !c.empty()) {
        out.props = read_properties(c);
    }
    return out;
}

struct Suback {
    std::uint16_t packet_id{0};
    std::vector<std::uint8_t> codes;  // per filter: the granted QoS, or 0x80 and above refused
    std::string reason_string;
};

inline Suback decode_suback(std::uint8_t version, std::string_view body) {
    Cursor c(body);
    Suback out;
    out.packet_id = c.u16();
    if (version == kMqtt5) {
        out.reason_string = read_properties(c).reason_string;
    }
    const auto codes = c.rest();
    if (codes.empty()) {
        throw ProtocolError("mqtt: SUBACK without a return code");
    }
    out.codes.assign(codes.begin(), codes.end());
    return out;
}

struct Publish {
    std::string topic;
    std::string payload;
    std::uint16_t packet_id{0};  // QoS 1 and 2 only
    std::uint8_t qos{0};
    bool dup{false};
    bool retain{false};
};

inline Publish decode_publish(std::uint8_t version, std::uint8_t flags, std::string_view body) {
    Publish out;
    out.dup = (flags & 0x08) != 0;
    out.qos = static_cast<std::uint8_t>((flags >> 1) & 0x03);
    out.retain = (flags & 0x01) != 0;
    if (out.qos == 3) {
        throw ProtocolError("mqtt: PUBLISH with QoS 3");
    }
    Cursor c(body);
    out.topic = std::string(c.str());
    if (out.qos > 0) {
        out.packet_id = c.u16();
        if (out.packet_id == 0) {
            throw ProtocolError("mqtt: PUBLISH with packet identifier 0");
        }
    }
    if (version == kMqtt5) {
        // CONNECT states no Topic Alias Maximum, so the broker may not use one.
        if (read_properties(c).topic_alias) {
            throw ProtocolError("mqtt: PUBLISH with a topic alias this client did not allow");
        }
    }
    if (out.topic.empty()) {
        throw ProtocolError("mqtt: PUBLISH with an empty topic name");
    }
    out.payload = std::string(c.rest());
    return out;
}

// The packet identifier of a PUBACK, PUBREC, PUBREL or PUBCOMP. MQTT 5 may add
// a reason code and properties, which a subscriber does not need.
inline std::uint16_t decode_packet_id(std::string_view body) {
    Cursor c(body);
    const auto id = c.u16();
    if (id == 0) {
        throw ProtocolError("mqtt: an acknowledgement for packet identifier 0");
    }
    return id;
}

struct Disconnect {
    std::uint8_t reason{0};
    std::string reason_string;
};

// A broker's DISCONNECT, which only MQTT 5 has.
inline Disconnect decode_disconnect(std::string_view body) {
    Disconnect out;
    if (body.empty()) {
        return out;
    }
    Cursor c(body);
    out.reason = c.u8();
    if (!c.empty()) {
        out.reason_string = read_properties(c).reason_string;
    }
    return out;
}

// What a reason or return code means, for an error message.
inline std::string reason_text(std::uint8_t version, std::uint8_t code) {
    if (version == kMqtt311) {
        switch (code) {
            case 0x01:
                return "unacceptable protocol version";
            case 0x02:
                return "identifier rejected";
            case 0x03:
                return "server unavailable";
            case 0x04:
                return "bad user name or password";
            case 0x05:
                return "not authorized";
            case 0x80:
                return "failure";
            default:
                break;
        }
    } else {
        switch (code) {
            case 0x04:
                return "disconnect with will message";
            case 0x80:
                return "unspecified error";
            case 0x81:
                return "malformed packet";
            case 0x82:
                return "protocol error";
            case 0x83:
                return "implementation specific error";
            case 0x84:
                return "unsupported protocol version";
            case 0x85:
                return "client identifier not valid";
            case 0x86:
                return "bad user name or password";
            case 0x87:
                return "not authorized";
            case 0x88:
                return "server unavailable";
            case 0x89:
                return "server busy";
            case 0x8A:
                return "banned";
            case 0x8B:
                return "server shutting down";
            case 0x8C:
                return "bad authentication method";
            case 0x8D:
                return "keep alive timeout";
            case 0x8E:
                return "session taken over";
            case 0x8F:
                return "topic filter invalid";
            case 0x91:
                return "packet identifier in use";
            case 0x93:
                return "receive maximum exceeded";
            case 0x95:
                return "packet too large";
            case 0x97:
                return "quota exceeded";
            case 0x9C:
                return "use another server";
            case 0x9D:
                return "server moved";
            case 0x9E:
                return "shared subscriptions not supported";
            case 0x9F:
                return "connection rate exceeded";
            case 0xA1:
                return "subscription identifiers not supported";
            case 0xA2:
                return "wildcard subscriptions not supported";
            default:
                break;
        }
    }
    char hex[8];
    std::snprintf(hex, sizeof(hex), "0x%02X", code);
    return std::string("reason code ") + hex;
}

}  // namespace clink::mqtt::protocol
