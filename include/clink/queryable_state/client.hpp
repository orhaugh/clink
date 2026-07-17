#pragma once

// Client for Queryable State HTTP endpoint.
//
// Wraps clink::http::HttpClient with typed get<K, V>(slot, key,
// kc, vc) calls. The client hex-encodes the user key, decodes the
// hex-encoded value, and returns optional<V>. Transport errors and
// 5xx surface as std::runtime_error; 404 returns std::nullopt.

#include <cstddef>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/http/http_client.hpp"
#include "clink/queryable_state/server.hpp"  // for hex_encode / hex_decode

namespace clink::queryable_state {

class Client {
public:
    Client(std::string host, std::uint16_t port) : http_(std::move(host), port) {}

    // Typed lookup. Returns the decoded value on hit; nullopt on 404
    // (slot missing or key absent). Throws on transport / 5xx errors
    // and on malformed responses.
    template <typename K, typename V>
    [[nodiscard]] std::optional<V> get(const std::string& slot,
                                       const K& key,
                                       Codec<K> kc,
                                       Codec<V> vc) {
        return get_at_path_<K, V>("/api/v1/queryable_state/" + slot, key, kc, vc);
    }

    // Subtask-scoped lookup. Same wire shape as the bare-slot overload
    // but hits the path that namespaces the slot by (role, subtask_idx).
    // Used by RoutedClient after the coordinator /route endpoint returns the
    // owning subtask for a given key.
    template <typename K, typename V>
    [[nodiscard]] std::optional<V> get(const std::string& role,
                                       std::uint32_t subtask_idx,
                                       const std::string& slot,
                                       const K& key,
                                       Codec<K> kc,
                                       Codec<V> vc) {
        const std::string path = "/api/v1/queryable_state/op/" + role + "/subtask/" +
                                 std::to_string(subtask_idx) + "/slot/" + slot;
        return get_at_path_<K, V>(path, key, kc, vc);
    }

private:
    template <typename K, typename V>
    [[nodiscard]] std::optional<V> get_at_path_(const std::string& base_path,
                                                const K& key,
                                                Codec<K> kc,
                                                Codec<V> vc) {
        const auto key_bytes = kc.encode(key);
        const auto key_hex =
            detail::hex_encode(std::span<const std::byte>{key_bytes.data(), key_bytes.size()});
        const std::string path = base_path + "?key=" + key_hex;
        auto resp = http_.get(path);
        if (resp.status == 0) {
            throw std::runtime_error("queryable_state::Client: transport error: " + resp.error);
        }
        if (resp.status == 404) {
            return std::nullopt;
        }
        if (resp.status >= 500) {
            throw std::runtime_error("queryable_state::Client: server error (" +
                                     std::to_string(resp.status) + "): " + resp.body);
        }
        if (resp.status != 200) {
            throw std::runtime_error("queryable_state::Client: unexpected status (" +
                                     std::to_string(resp.status) + "): " + resp.body);
        }
        // Body shape: {"value_hex":"<hex>"} or {"value_hex":null}.
        // Cheap parse - we control the server format. Tolerates
        // whitespace between tokens but nothing fancier.
        const auto& body = resp.body;
        const auto null_pos = body.find("\"value_hex\":null");
        if (null_pos != std::string::npos) {
            return std::nullopt;
        }
        const auto val_pos = body.find("\"value_hex\":\"");
        if (val_pos == std::string::npos) {
            throw std::runtime_error("queryable_state::Client: malformed response body: " + body);
        }
        const auto start = val_pos + std::string{"\"value_hex\":\""}.size();
        const auto end = body.find('"', start);
        if (end == std::string::npos) {
            throw std::runtime_error("queryable_state::Client: unterminated value_hex");
        }
        const auto value_hex = body.substr(start, end - start);
        auto value_bytes = detail::hex_decode(value_hex);
        if (!value_bytes.has_value()) {
            throw std::runtime_error("queryable_state::Client: malformed value_hex");
        }
        return vc.decode(std::span<const std::byte>{value_bytes->data(), value_bytes->size()});
    }

private:
    http::HttpClient http_;
};

}  // namespace clink::queryable_state
