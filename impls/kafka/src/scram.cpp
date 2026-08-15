#include "clink/kafka/scram.hpp"

#include <cstring>
#include <random>

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

namespace clink::kafka::scram {

namespace {

constexpr std::size_t kHashLen = 32;  // SHA-256

using Bytes = std::vector<std::byte>;

Bytes hmac_sha256(const Bytes& key, const std::string& data) {
    unsigned char out[EVP_MAX_MD_SIZE];
    unsigned int out_len = 0;
    if (HMAC(EVP_sha256(),
             key.data(),
             static_cast<int>(key.size()),
             reinterpret_cast<const unsigned char*>(data.data()),
             data.size(),
             out,
             &out_len) == nullptr) {
        return {};
    }
    Bytes b(out_len);
    std::memcpy(b.data(), out, out_len);
    return b;
}

Bytes sha256(const Bytes& in) {
    unsigned char out[kHashLen];
    unsigned int out_len = 0;
    if (EVP_Digest(in.data(), in.size(), out, &out_len, EVP_sha256(), nullptr) != 1) {
        return {};
    }
    Bytes b(out_len);
    std::memcpy(b.data(), out, out_len);
    return b;
}

// Hi(password, salt, i) = PBKDF2-HMAC-SHA-256 with dkLen = hash length.
Bytes salted_password(const std::string& password, const Bytes& salt, std::uint32_t iterations) {
    Bytes out(kHashLen);
    if (PKCS5_PBKDF2_HMAC(password.data(),
                          static_cast<int>(password.size()),
                          reinterpret_cast<const unsigned char*>(salt.data()),
                          static_cast<int>(salt.size()),
                          static_cast<int>(iterations),
                          EVP_sha256(),
                          static_cast<int>(out.size()),
                          reinterpret_cast<unsigned char*>(out.data())) != 1) {
        return {};
    }
    return out;
}

// RFC 5802 attribute escaping for the username: '=' -> "=3D", ',' -> "=2C".
std::string escape_username(const std::string& u) {
    std::string out;
    out.reserve(u.size());
    for (const char c : u) {
        if (c == '=') {
            out += "=3D";
        } else if (c == ',') {
            out += "=2C";
        } else {
            out += c;
        }
    }
    return out;
}

const char kB64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

// One attribute value out of "a=...,b=...": everything after "<key>=" up to
// the next comma. nullopt when the key is absent.
std::optional<std::string> attribute(const std::string& msg, char key) {
    const std::string needle = std::string(1, key) + "=";
    std::size_t pos = 0;
    while (pos < msg.size()) {
        if (msg.compare(pos, needle.size(), needle) == 0 && (pos == 0 || msg[pos - 1] == ',')) {
            const auto end = msg.find(',', pos);
            const auto start = pos + needle.size();
            return msg.substr(start, end == std::string::npos ? std::string::npos : end - start);
        }
        const auto comma = msg.find(',', pos);
        if (comma == std::string::npos) {
            break;
        }
        pos = comma + 1;
    }
    return std::nullopt;
}

}  // namespace

std::string base64_encode(const Bytes& in) {
    std::string out;
    out.reserve(((in.size() + 2) / 3) * 4);
    std::size_t i = 0;
    while (i + 3 <= in.size()) {
        const auto a = static_cast<std::uint8_t>(in[i]);
        const auto b = static_cast<std::uint8_t>(in[i + 1]);
        const auto c = static_cast<std::uint8_t>(in[i + 2]);
        out += kB64[a >> 2];
        out += kB64[((a & 0x03) << 4) | (b >> 4)];
        out += kB64[((b & 0x0F) << 2) | (c >> 6)];
        out += kB64[c & 0x3F];
        i += 3;
    }
    const auto rem = in.size() - i;
    if (rem == 1) {
        const auto a = static_cast<std::uint8_t>(in[i]);
        out += kB64[a >> 2];
        out += kB64[(a & 0x03) << 4];
        out += "==";
    } else if (rem == 2) {
        const auto a = static_cast<std::uint8_t>(in[i]);
        const auto b = static_cast<std::uint8_t>(in[i + 1]);
        out += kB64[a >> 2];
        out += kB64[((a & 0x03) << 4) | (b >> 4)];
        out += kB64[(b & 0x0F) << 2];
        out += '=';
    }
    return out;
}

std::optional<Bytes> base64_decode(const std::string& in) {
    if (in.size() % 4 != 0) {
        return std::nullopt;
    }
    const auto val = [](char c) -> int {
        if (c >= 'A' && c <= 'Z') {
            return c - 'A';
        }
        if (c >= 'a' && c <= 'z') {
            return c - 'a' + 26;
        }
        if (c >= '0' && c <= '9') {
            return c - '0' + 52;
        }
        if (c == '+') {
            return 62;
        }
        if (c == '/') {
            return 63;
        }
        return -1;
    };
    Bytes out;
    out.reserve((in.size() / 4) * 3);
    for (std::size_t i = 0; i < in.size(); i += 4) {
        const bool pad1 = in[i + 2] == '=';
        const bool pad2 = in[i + 3] == '=';
        if (pad1 && !pad2) {
            return std::nullopt;
        }
        const int a = val(in[i]);
        const int b = val(in[i + 1]);
        const int c = pad1 ? 0 : val(in[i + 2]);
        const int d = pad2 ? 0 : val(in[i + 3]);
        if (a < 0 || b < 0 || c < 0 || d < 0) {
            return std::nullopt;
        }
        out.push_back(static_cast<std::byte>((a << 2) | (b >> 4)));
        if (!pad1) {
            out.push_back(static_cast<std::byte>(((b & 0x0F) << 4) | (c >> 2)));
        }
        if (!pad2) {
            out.push_back(static_cast<std::byte>(((c & 0x03) << 6) | d));
        }
        if ((pad1 || pad2) && i + 4 != in.size()) {
            return std::nullopt;  // padding only at the end
        }
    }
    return out;
}

ClientFirst client_first(const std::string& username, const std::string& client_nonce) {
    ClientFirst out;
    out.nonce = client_nonce;
    out.bare = "n=" + escape_username(username) + ",r=" + client_nonce;
    out.full = "n,," + out.bare;
    return out;
}

std::optional<ServerFirst> parse_server_first(const std::string& message) {
    const auto r = attribute(message, 'r');
    const auto s = attribute(message, 's');
    const auto i = attribute(message, 'i');
    if (!r.has_value() || !s.has_value() || !i.has_value() || r->empty()) {
        return std::nullopt;
    }
    const auto salt = base64_decode(*s);
    if (!salt.has_value() || salt->empty()) {
        return std::nullopt;
    }
    ServerFirst out;
    out.nonce = *r;
    out.salt = *salt;
    try {
        const auto iters = std::stoul(*i);
        // RFC 7677 demands >= 4096; anything below is a downgrade attempt
        // (an attacker weakening the KDF), not a legitimate server.
        if (iters < 4096 || iters > 10'000'000) {
            return std::nullopt;
        }
        out.iterations = static_cast<std::uint32_t>(iters);
    } catch (const std::exception&) {
        return std::nullopt;
    }
    out.raw = message;
    return out;
}

std::optional<ClientFinal> client_final(const std::string& password,
                                        const ClientFirst& first,
                                        const ServerFirst& server) {
    // The server nonce MUST extend the client's; a reflected or foreign
    // nonce is an attack shape.
    if (server.nonce.size() <= first.nonce.size() ||
        server.nonce.compare(0, first.nonce.size(), first.nonce) != 0) {
        return std::nullopt;
    }
    const auto salted = salted_password(password, server.salt, server.iterations);
    if (salted.empty()) {
        return std::nullopt;
    }
    const auto client_key = hmac_sha256(salted, "Client Key");
    const auto stored_key = sha256(client_key);
    const auto server_key = hmac_sha256(salted, "Server Key");
    if (client_key.empty() || stored_key.empty() || server_key.empty()) {
        return std::nullopt;
    }
    // c=biws is base64("n,,") - the GS2 header echoed, no channel binding.
    const std::string without_proof = "c=biws,r=" + server.nonce;
    const std::string auth_message = first.bare + "," + server.raw + "," + without_proof;
    const auto client_signature = hmac_sha256(stored_key, auth_message);
    if (client_signature.size() != client_key.size()) {
        return std::nullopt;
    }
    Bytes proof(client_key.size());
    for (std::size_t i = 0; i < proof.size(); ++i) {
        proof[i] = client_key[i] ^ client_signature[i];
    }
    ClientFinal out;
    out.message = without_proof + ",p=" + base64_encode(proof);
    out.expected_server_signature = hmac_sha256(server_key, auth_message);
    if (out.expected_server_signature.empty()) {
        return std::nullopt;
    }
    return out;
}

std::optional<Bytes> parse_server_final_signature(const std::string& message) {
    if (attribute(message, 'e').has_value()) {
        return std::nullopt;  // an explicit server error, never a signature
    }
    const auto v = attribute(message, 'v');
    if (!v.has_value()) {
        return std::nullopt;
    }
    return base64_decode(*v);
}

std::string random_nonce() {
    std::random_device rd;
    Bytes raw(18);
    for (auto& b : raw) {
        b = static_cast<std::byte>(rd() & 0xFF);
    }
    return base64_encode(raw);
}

}  // namespace clink::kafka::scram
