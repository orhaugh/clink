#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

// Standard RFC 4648 base64 (alphabet A-Za-z0-9+/ with '=' padding).
// Header-only and dependency-free so any layer can carry binary payloads
// through text protocols (JSON specs, wire strings). Shared by the cluster
// layer (UDF module shipping) and the HTTP connectors (Pub/Sub payloads).
namespace clink {

inline std::string base64_encode(std::string_view in) {
    static constexpr char kEnc[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((in.size() + 2) / 3) * 4);
    auto uc = [&](std::size_t k) {
        return static_cast<unsigned>(static_cast<unsigned char>(in[k]));
    };
    const std::size_t n = in.size();
    std::size_t i = 0;
    for (; i + 3 <= n; i += 3) {
        const unsigned v = (uc(i) << 16) | (uc(i + 1) << 8) | uc(i + 2);
        out.push_back(kEnc[(v >> 18) & 0x3F]);
        out.push_back(kEnc[(v >> 12) & 0x3F]);
        out.push_back(kEnc[(v >> 6) & 0x3F]);
        out.push_back(kEnc[v & 0x3F]);
    }
    const std::size_t rem = n - i;
    if (rem == 1) {
        const unsigned v = uc(i) << 16;
        out.push_back(kEnc[(v >> 18) & 0x3F]);
        out.push_back(kEnc[(v >> 12) & 0x3F]);
        out.push_back('=');
        out.push_back('=');
    } else if (rem == 2) {
        const unsigned v = (uc(i) << 16) | (uc(i + 1) << 8);
        out.push_back(kEnc[(v >> 18) & 0x3F]);
        out.push_back(kEnc[(v >> 12) & 0x3F]);
        out.push_back(kEnc[(v >> 6) & 0x3F]);
        out.push_back('=');
    }
    return out;
}

// Decode standard base64. Tolerates embedded ASCII whitespace (a wrapped wire
// line) and accepts input with or without trailing '=' padding. Returns nullopt
// on an invalid character (so a malformed payload is surfaced, not silently
// mangled). Leftover sub-byte bits at end-of-input are discarded per the
// tolerant-decoder convention.
inline std::optional<std::string> base64_decode(std::string_view in) {
    auto val = [](unsigned char c) -> int {
        if (c >= 'A' && c <= 'Z')
            return c - 'A';
        if (c >= 'a' && c <= 'z')
            return c - 'a' + 26;
        if (c >= '0' && c <= '9')
            return c - '0' + 52;
        if (c == '+')
            return 62;
        if (c == '/')
            return 63;
        return -1;
    };
    std::string out;
    out.reserve((in.size() / 4) * 3 + 3);
    int acc = 0;
    int bits = 0;
    for (char ch : in) {
        const auto c = static_cast<unsigned char>(ch);
        if (c == '=') {
            break;  // padding: end of data
        }
        if (c == '\r' || c == '\n' || c == '\t' || c == ' ') {
            continue;
        }
        const int d = val(c);
        if (d < 0) {
            return std::nullopt;
        }
        acc = (acc << 6) | d;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<char>((acc >> bits) & 0xFF));
        }
    }
    return out;
}

}  // namespace clink
