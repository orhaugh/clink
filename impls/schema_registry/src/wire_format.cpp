#include "clink/schema_registry/wire_format.hpp"

#include <cstring>

namespace clink::schema_registry {

void append_zigzag_varint(std::string& out, std::int64_t v) {
    auto u = static_cast<std::uint64_t>((v << 1) ^ (v >> 63));
    while (u >= 0x80) {
        out.push_back(static_cast<char>((u & 0x7f) | 0x80));
        u >>= 7;
    }
    out.push_back(static_cast<char>(u));
}

std::optional<std::int64_t> read_zigzag_varint(std::string_view& in) {
    std::uint64_t u = 0;
    int shift = 0;
    std::size_t i = 0;
    while (i < in.size()) {
        const auto b = static_cast<std::uint8_t>(in[i++]);
        u |= static_cast<std::uint64_t>(b & 0x7f) << shift;
        if ((b & 0x80) == 0) {
            in.remove_prefix(i);
            return static_cast<std::int64_t>((u >> 1) ^ (~(u & 1) + 1));
        }
        shift += 7;
        if (shift > 63) {
            return std::nullopt;  // over-long
        }
    }
    return std::nullopt;  // truncated
}

std::optional<Frame> parse_frame(std::string_view bytes,
                                 bool with_message_indexes,
                                 std::string* error) {
    if (bytes.size() < kHeaderSize) {
        if (error != nullptr) {
            *error = "registry frame too short: " + std::to_string(bytes.size()) +
                     " byte(s), need at least " + std::to_string(kHeaderSize);
        }
        return std::nullopt;
    }
    if (static_cast<std::uint8_t>(bytes[0]) != kMagicByte) {
        if (error != nullptr) {
            *error = "not a registry-framed value: first byte is 0x" +
                     [](std::uint8_t b) {
                         static constexpr char hex[] = "0123456789abcdef";
                         return std::string{hex[b >> 4], hex[b & 0xf]};
                     }(static_cast<std::uint8_t>(bytes[0])) +
                     ", expected the magic byte 0x00 (is this topic really registry-framed?)";
        }
        return std::nullopt;
    }
    Frame f;
    std::uint32_t id = 0;
    for (std::size_t i = 1; i < kHeaderSize; ++i) {
        id = (id << 8) | static_cast<std::uint8_t>(bytes[i]);
    }
    f.schema_id = static_cast<std::int32_t>(id);
    std::string_view rest = bytes.substr(kHeaderSize);
    if (with_message_indexes) {
        const auto count = read_zigzag_varint(rest);
        if (!count.has_value() || *count < 0 || *count > 128) {
            if (error != nullptr) {
                *error = "malformed protobuf message-index array after the registry header";
            }
            return std::nullopt;
        }
        if (*count == 0) {
            f.message_indexes = {0};
        } else {
            f.message_indexes.reserve(static_cast<std::size_t>(*count));
            for (std::int64_t i = 0; i < *count; ++i) {
                const auto idx = read_zigzag_varint(rest);
                if (!idx.has_value() || *idx < 0 || *idx > 0x7fffffff) {
                    if (error != nullptr) {
                        *error = "malformed protobuf message-index array after the registry header";
                    }
                    return std::nullopt;
                }
                f.message_indexes.push_back(static_cast<std::int32_t>(*idx));
            }
        }
    }
    f.payload = rest;
    return f;
}

namespace {
void append_header(std::string& out, std::int32_t schema_id) {
    const auto id = static_cast<std::uint32_t>(schema_id);
    out.push_back(static_cast<char>(kMagicByte));
    out.push_back(static_cast<char>((id >> 24) & 0xff));
    out.push_back(static_cast<char>((id >> 16) & 0xff));
    out.push_back(static_cast<char>((id >> 8) & 0xff));
    out.push_back(static_cast<char>(id & 0xff));
}
}  // namespace

std::string frame(std::int32_t schema_id, std::string_view payload) {
    std::string out;
    out.reserve(kHeaderSize + payload.size());
    append_header(out, schema_id);
    out.append(payload);
    return out;
}

std::string frame(std::int32_t schema_id,
                  const std::vector<std::int32_t>& message_indexes,
                  std::string_view payload) {
    std::string out;
    out.reserve(kHeaderSize + 1 + message_indexes.size() * 2 + payload.size());
    append_header(out, schema_id);
    if (message_indexes.size() == 1 && message_indexes[0] == 0) {
        out.push_back('\0');
    } else {
        append_zigzag_varint(out, static_cast<std::int64_t>(message_indexes.size()));
        for (const auto idx : message_indexes) {
            append_zigzag_varint(out, idx);
        }
    }
    out.append(payload);
    return out;
}

}  // namespace clink::schema_registry
