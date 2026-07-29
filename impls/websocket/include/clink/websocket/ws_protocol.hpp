#pragma once

// RFC 6455 protocol layer for the WebSocket connector: URL parsing, the
// opening-handshake key/accept computation, frame encode, and an incremental
// frame decoder with fragmented-message reassembly.
//
// Deliberately pure: no sockets, no TLS, no engine types - every function is
// (bytes in, bytes out), which is what lets the unit tests pin this layer to
// the RFC's own worked examples (the sample nonce/accept pair of section 1.3
// and the masked "Hello" frame of section 5.7) instead of testing the client
// against itself. The self-contained SHA-1 exists because the handshake needs
// it even in a plain ws:// build with no TLS library present; it is used for
// nothing else.

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace clink::websocket {

// ---------------------------------------------------------------------------
// URL
// ---------------------------------------------------------------------------

struct WsUrl {
    bool tls{false};       // wss://
    std::string host;      // host name or address
    std::uint16_t port{};  // explicit, or the scheme default (80 / 443)
    std::string path;      // begins with '/', query string included
};

// Parse ws://host[:port][/path...] or wss://... - anything else is nullopt.
inline std::optional<WsUrl> parse_ws_url(std::string_view url) {
    WsUrl out;
    std::string_view rest;
    if (url.rfind("ws://", 0) == 0) {
        out.tls = false;
        rest = url.substr(5);
    } else if (url.rfind("wss://", 0) == 0) {
        out.tls = true;
        rest = url.substr(6);
    } else {
        return std::nullopt;
    }
    const auto slash = rest.find('/');
    std::string_view authority = slash == std::string_view::npos ? rest : rest.substr(0, slash);
    out.path = slash == std::string_view::npos ? std::string{"/"} : std::string{rest.substr(slash)};
    if (authority.empty()) {
        return std::nullopt;
    }
    const auto colon = authority.rfind(':');
    if (colon == std::string_view::npos) {
        out.host = std::string{authority};
        out.port = out.tls ? std::uint16_t{443} : std::uint16_t{80};
    } else {
        out.host = std::string{authority.substr(0, colon)};
        std::uint32_t port = 0;
        for (const char c : authority.substr(colon + 1)) {
            if (c < '0' || c > '9') {
                return std::nullopt;
            }
            port = port * 10 + static_cast<std::uint32_t>(c - '0');
            if (port > 65535) {
                return std::nullopt;
            }
        }
        if (port == 0) {
            return std::nullopt;
        }
        out.host = std::string{authority.substr(0, colon)};
        out.port = static_cast<std::uint16_t>(port);
    }
    if (out.host.empty()) {
        return std::nullopt;
    }
    return out;
}

// ---------------------------------------------------------------------------
// SHA-1 + base64 (handshake only)
// ---------------------------------------------------------------------------

namespace detail {

inline std::array<std::uint8_t, 20> sha1(std::string_view msg) {
    std::uint32_t h0 = 0x67452301u;
    std::uint32_t h1 = 0xEFCDAB89u;
    std::uint32_t h2 = 0x98BADCFEu;
    std::uint32_t h3 = 0x10325476u;
    std::uint32_t h4 = 0xC3D2E1F0u;

    // Pre-processing: append 0x80, pad to 56 mod 64, append bit length BE.
    std::string data{msg};
    const std::uint64_t bit_len = static_cast<std::uint64_t>(msg.size()) * 8u;
    data.push_back(static_cast<char>(static_cast<unsigned char>(0x80)));
    while (data.size() % 64 != 56) {
        data.push_back('\0');
    }
    for (int i = 7; i >= 0; --i) {
        data.push_back(static_cast<char>(static_cast<unsigned char>((bit_len >> (i * 8)) & 0xFF)));
    }

    const auto rol = [](std::uint32_t v, unsigned n) -> std::uint32_t {
        return (v << n) | (v >> (32u - n));
    };

    for (std::size_t off = 0; off < data.size(); off += 64) {
        std::uint32_t w[80];
        for (std::size_t i = 0; i < 16; ++i) {
            const auto b = [&](std::size_t k) {
                return static_cast<std::uint32_t>(
                    static_cast<unsigned char>(data[off + i * 4 + k]));
            };
            w[i] = (b(0) << 24) | (b(1) << 16) | (b(2) << 8) | b(3);
        }
        for (std::size_t i = 16; i < 80; ++i) {
            w[i] = rol(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
        }
        std::uint32_t a = h0;
        std::uint32_t b = h1;
        std::uint32_t c = h2;
        std::uint32_t d = h3;
        std::uint32_t e = h4;
        for (std::size_t i = 0; i < 80; ++i) {
            std::uint32_t f = 0;
            std::uint32_t k = 0;
            if (i < 20) {
                f = (b & c) | ((~b) & d);
                k = 0x5A827999u;
            } else if (i < 40) {
                f = b ^ c ^ d;
                k = 0x6ED9EBA1u;
            } else if (i < 60) {
                f = (b & c) | (b & d) | (c & d);
                k = 0x8F1BBCDCu;
            } else {
                f = b ^ c ^ d;
                k = 0xCA62C1D6u;
            }
            const std::uint32_t tmp = rol(a, 5) + f + e + k + w[i];
            e = d;
            d = c;
            c = rol(b, 30);
            b = a;
            a = tmp;
        }
        h0 += a;
        h1 += b;
        h2 += c;
        h3 += d;
        h4 += e;
    }

    std::array<std::uint8_t, 20> digest{};
    const std::uint32_t hs[5] = {h0, h1, h2, h3, h4};
    for (std::size_t i = 0; i < 5; ++i) {
        digest[i * 4 + 0] = static_cast<std::uint8_t>((hs[i] >> 24) & 0xFF);
        digest[i * 4 + 1] = static_cast<std::uint8_t>((hs[i] >> 16) & 0xFF);
        digest[i * 4 + 2] = static_cast<std::uint8_t>((hs[i] >> 8) & 0xFF);
        digest[i * 4 + 3] = static_cast<std::uint8_t>(hs[i] & 0xFF);
    }
    return digest;
}

inline std::string base64(const std::uint8_t* data, std::size_t len) {
    static constexpr char kAlphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    std::size_t i = 0;
    for (; i + 3 <= len; i += 3) {
        const std::uint32_t v = (static_cast<std::uint32_t>(data[i]) << 16) |
                                (static_cast<std::uint32_t>(data[i + 1]) << 8) |
                                static_cast<std::uint32_t>(data[i + 2]);
        out.push_back(kAlphabet[(v >> 18) & 0x3F]);
        out.push_back(kAlphabet[(v >> 12) & 0x3F]);
        out.push_back(kAlphabet[(v >> 6) & 0x3F]);
        out.push_back(kAlphabet[v & 0x3F]);
    }
    if (i + 1 == len) {
        const std::uint32_t v = static_cast<std::uint32_t>(data[i]) << 16;
        out.push_back(kAlphabet[(v >> 18) & 0x3F]);
        out.push_back(kAlphabet[(v >> 12) & 0x3F]);
        out.append("==");
    } else if (i + 2 == len) {
        const std::uint32_t v = (static_cast<std::uint32_t>(data[i]) << 16) |
                                (static_cast<std::uint32_t>(data[i + 1]) << 8);
        out.push_back(kAlphabet[(v >> 18) & 0x3F]);
        out.push_back(kAlphabet[(v >> 12) & 0x3F]);
        out.push_back(kAlphabet[(v >> 6) & 0x3F]);
        out.push_back('=');
    }
    return out;
}

}  // namespace detail

// ---------------------------------------------------------------------------
// Opening handshake
// ---------------------------------------------------------------------------

// Sec-WebSocket-Key from a caller-supplied 16-byte nonce (the caller owns
// randomness; tests pass a fixed nonce).
inline std::string client_key_from_nonce(const std::array<std::uint8_t, 16>& nonce) {
    return detail::base64(nonce.data(), nonce.size());
}

// Sec-WebSocket-Accept for a client key: base64(SHA1(key + GUID)), the GUID
// being the constant RFC 6455 section 4.2.2 defines.
inline std::string accept_key_for(std::string_view client_key) {
    static constexpr std::string_view kGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    std::string input;
    input.reserve(client_key.size() + kGuid.size());
    input.append(client_key);
    input.append(kGuid);
    const auto digest = detail::sha1(input);
    return detail::base64(digest.data(), digest.size());
}

// The client's opening request. `host_header` should carry the port when it
// is not the scheme default.
inline std::string build_handshake_request(std::string_view host_header,
                                           std::string_view path,
                                           std::string_view client_key) {
    std::string req;
    req.append("GET ").append(path).append(" HTTP/1.1\r\n");
    req.append("Host: ").append(host_header).append("\r\n");
    req.append("Upgrade: websocket\r\n");
    req.append("Connection: Upgrade\r\n");
    req.append("Sec-WebSocket-Key: ").append(client_key).append("\r\n");
    req.append("Sec-WebSocket-Version: 13\r\n");
    req.append("\r\n");
    return req;
}

struct HandshakeResponse {
    int status{0};
    std::vector<std::pair<std::string, std::string>> headers;  // names lower-cased
    std::size_t header_bytes{0};  // length of the head incl. the blank line
};

// Parse an HTTP response head out of `buf`. nullopt until the terminating
// blank line has arrived (feed more bytes and retry); a malformed status
// line parses as status 0, which the caller rejects.
inline std::optional<HandshakeResponse> parse_handshake_response(std::string_view buf) {
    const auto end = buf.find("\r\n\r\n");
    if (end == std::string_view::npos) {
        return std::nullopt;
    }
    HandshakeResponse out;
    out.header_bytes = end + 4;
    const std::string_view head = buf.substr(0, end);
    std::size_t line_start = 0;
    bool first = true;
    while (line_start <= head.size()) {
        auto line_end = head.find("\r\n", line_start);
        if (line_end == std::string_view::npos) {
            line_end = head.size();
        }
        const std::string_view line = head.substr(line_start, line_end - line_start);
        if (first) {
            // "HTTP/1.1 101 Switching Protocols"
            const auto sp = line.find(' ');
            if (sp != std::string_view::npos && sp + 4 <= line.size()) {
                int status = 0;
                for (std::size_t i = sp + 1; i < line.size() && line[i] >= '0' && line[i] <= '9';
                     ++i) {
                    status = status * 10 + (line[i] - '0');
                }
                out.status = status;
            }
            first = false;
        } else if (!line.empty()) {
            const auto colon = line.find(':');
            if (colon != std::string_view::npos) {
                std::string name{line.substr(0, colon)};
                for (auto& c : name) {
                    if (c >= 'A' && c <= 'Z') {
                        c = static_cast<char>(c - 'A' + 'a');
                    }
                }
                std::size_t v = colon + 1;
                while (v < line.size() && (line[v] == ' ' || line[v] == '\t')) {
                    ++v;
                }
                out.headers.emplace_back(std::move(name), std::string{line.substr(v)});
            }
        }
        if (line_end == head.size()) {
            break;
        }
        line_start = line_end + 2;
    }
    return out;
}

inline const std::string* find_header(const HandshakeResponse& resp, std::string_view lower_name) {
    for (const auto& [name, value] : resp.headers) {
        if (name == lower_name) {
            return &value;
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

enum class Opcode : std::uint8_t {
    Continuation = 0x0,
    Text = 0x1,
    Binary = 0x2,
    Close = 0x8,
    Ping = 0x9,
    Pong = 0xA,
};

// Encode one frame. When `mask` is set (mandatory for client-to-server
// frames), `mask_key` supplies the 4 masking bytes, big-endian: 0x37FA213D
// masks with bytes 37 FA 21 3D, matching how RFC 6455 section 5.7 prints its
// example. The caller owns key randomness; tests pass the RFC's key.
inline std::string encode_frame(
    Opcode op, std::string_view payload, bool mask, std::uint32_t mask_key = 0, bool fin = true) {
    std::string out;
    out.reserve(payload.size() + 14);
    out.push_back(static_cast<char>(
        static_cast<std::uint8_t>((fin ? 0x80u : 0x00u) | static_cast<std::uint8_t>(op))));
    const std::uint8_t mask_bit = mask ? 0x80u : 0x00u;
    const std::size_t len = payload.size();
    if (len <= 125) {
        out.push_back(static_cast<char>(static_cast<std::uint8_t>(mask_bit | len)));
    } else if (len <= 0xFFFF) {
        out.push_back(static_cast<char>(static_cast<std::uint8_t>(mask_bit | 126u)));
        out.push_back(static_cast<char>(static_cast<std::uint8_t>((len >> 8) & 0xFF)));
        out.push_back(static_cast<char>(static_cast<std::uint8_t>(len & 0xFF)));
    } else {
        out.push_back(static_cast<char>(static_cast<std::uint8_t>(mask_bit | 127u)));
        const auto len64 = static_cast<std::uint64_t>(len);
        for (int i = 7; i >= 0; --i) {
            out.push_back(static_cast<char>(static_cast<std::uint8_t>((len64 >> (i * 8)) & 0xFF)));
        }
    }
    if (mask) {
        const std::uint8_t key[4] = {static_cast<std::uint8_t>((mask_key >> 24) & 0xFF),
                                     static_cast<std::uint8_t>((mask_key >> 16) & 0xFF),
                                     static_cast<std::uint8_t>((mask_key >> 8) & 0xFF),
                                     static_cast<std::uint8_t>(mask_key & 0xFF)};
        out.append(reinterpret_cast<const char*>(key), 4);
        for (std::size_t i = 0; i < payload.size(); ++i) {
            out.push_back(static_cast<char>(
                static_cast<std::uint8_t>(static_cast<unsigned char>(payload[i]) ^ key[i % 4])));
        }
    } else {
        out.append(payload);
    }
    return out;
}

struct Frame {
    Opcode opcode{Opcode::Continuation};
    bool fin{true};
    std::string payload;
};

// Incremental frame decoder: append() raw bytes, next_frame() yields one
// complete frame at a time. Decodes masked frames too (a compliant server
// never masks, but the connector's own tests run this decoder on the server
// side, where client frames arrive masked).
class FrameDecoder {
public:
    // Guard against a corrupt or hostile length prefix claiming gigabytes.
    explicit FrameDecoder(std::size_t max_payload = 64 * 1024 * 1024) : max_payload_(max_payload) {}

    void append(const char* data, std::size_t len) { buf_.append(data, len); }

    // One decoded frame, std::nullopt when more bytes are needed. Sets
    // `error` (and always returns nullopt afterwards) on a protocol
    // violation the stream cannot recover from.
    std::optional<Frame> next_frame(std::string& error) {
        if (!error_.empty()) {
            error = error_;
            return std::nullopt;
        }
        if (buf_.size() < 2) {
            return std::nullopt;
        }
        const auto b0 = static_cast<std::uint8_t>(static_cast<unsigned char>(buf_[0]));
        const auto b1 = static_cast<std::uint8_t>(static_cast<unsigned char>(buf_[1]));
        const bool fin = (b0 & 0x80u) != 0;
        if ((b0 & 0x70u) != 0) {
            return fail_("reserved frame bits set (no extension was negotiated)", error);
        }
        const auto opcode = static_cast<Opcode>(b0 & 0x0Fu);
        const bool masked = (b1 & 0x80u) != 0;
        std::uint64_t len = b1 & 0x7Fu;
        std::size_t pos = 2;
        if (len == 126) {
            if (buf_.size() < pos + 2) {
                return std::nullopt;
            }
            len = (static_cast<std::uint64_t>(static_cast<unsigned char>(buf_[pos])) << 8) |
                  static_cast<std::uint64_t>(static_cast<unsigned char>(buf_[pos + 1]));
            pos += 2;
        } else if (len == 127) {
            if (buf_.size() < pos + 8) {
                return std::nullopt;
            }
            len = 0;
            for (std::size_t i = 0; i < 8; ++i) {
                len = (len << 8) |
                      static_cast<std::uint64_t>(static_cast<unsigned char>(buf_[pos + i]));
            }
            pos += 8;
        }
        if (len > max_payload_) {
            return fail_("frame payload exceeds the configured maximum", error);
        }
        std::uint8_t key[4] = {0, 0, 0, 0};
        if (masked) {
            if (buf_.size() < pos + 4) {
                return std::nullopt;
            }
            for (std::size_t i = 0; i < 4; ++i) {
                key[i] = static_cast<std::uint8_t>(static_cast<unsigned char>(buf_[pos + i]));
            }
            pos += 4;
        }
        if (buf_.size() < pos + static_cast<std::size_t>(len)) {
            return std::nullopt;
        }
        Frame out;
        out.opcode = opcode;
        out.fin = fin;
        out.payload.reserve(static_cast<std::size_t>(len));
        for (std::size_t i = 0; i < static_cast<std::size_t>(len); ++i) {
            const auto raw = static_cast<std::uint8_t>(static_cast<unsigned char>(buf_[pos + i]));
            out.payload.push_back(
                static_cast<char>(masked ? static_cast<std::uint8_t>(raw ^ key[i % 4]) : raw));
        }
        buf_.erase(0, pos + static_cast<std::size_t>(len));
        return out;
    }

private:
    std::optional<Frame> fail_(std::string msg, std::string& error) {
        error_ = std::move(msg);
        error = error_;
        return std::nullopt;
    }

    std::string buf_;
    std::string error_;
    std::size_t max_payload_;
};

// Message-level view over FrameDecoder: reassembles fragmented text/binary
// messages (a data frame with fin=false followed by continuations) and
// passes control frames through immediately, as RFC 6455 requires - control
// frames may interleave with a fragmented message and are never fragmented
// themselves.
class MessageReader {
public:
    explicit MessageReader(std::size_t max_message = 64 * 1024 * 1024)
        : decoder_(max_message), max_message_(max_message) {}

    void append(const char* data, std::size_t len) { decoder_.append(data, len); }

    struct Message {
        Opcode opcode{Opcode::Text};  // Text, Binary, Close, Ping or Pong
        std::string payload;
    };

    std::optional<Message> next_message(std::string& error) {
        while (true) {
            auto frame = decoder_.next_frame(error);
            if (!frame.has_value()) {
                return std::nullopt;
            }
            switch (frame->opcode) {
                case Opcode::Close:
                case Opcode::Ping:
                case Opcode::Pong:
                    if (!frame->fin || frame->payload.size() > 125) {
                        error = "malformed control frame";
                        return std::nullopt;
                    }
                    return Message{frame->opcode, std::move(frame->payload)};
                case Opcode::Text:
                case Opcode::Binary:
                    if (!assembling_.empty() || assembling_opcode_.has_value()) {
                        error = "new data frame while a fragmented message is open";
                        return std::nullopt;
                    }
                    if (frame->fin) {
                        return Message{frame->opcode, std::move(frame->payload)};
                    }
                    assembling_opcode_ = frame->opcode;
                    assembling_ = std::move(frame->payload);
                    break;
                case Opcode::Continuation:
                    if (!assembling_opcode_.has_value()) {
                        error = "continuation frame with no message open";
                        return std::nullopt;
                    }
                    if (assembling_.size() + frame->payload.size() > max_message_) {
                        error = "fragmented message exceeds the configured maximum";
                        return std::nullopt;
                    }
                    assembling_.append(frame->payload);
                    if (frame->fin) {
                        Message out{*assembling_opcode_, std::move(assembling_)};
                        assembling_.clear();
                        assembling_opcode_.reset();
                        return out;
                    }
                    break;
            }
        }
    }

private:
    FrameDecoder decoder_;
    std::string assembling_;
    std::optional<Opcode> assembling_opcode_;
    std::size_t max_message_;
};

}  // namespace clink::websocket
