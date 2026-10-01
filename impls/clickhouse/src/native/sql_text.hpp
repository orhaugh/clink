#pragma once

// Quoting, INSERT tokens and the legacy sink's INSERT prefix. Includes no
// clickhouse-cpp or Arrow header, so the legacy sink builds against it on
// every client version.

#include <array>
#include <cstdint>
#include <string>
#include <string_view>

namespace clink::clickhouse::native {

// `a` with ` and \ backslash-escaped.
[[nodiscard]] std::string quote_identifier(std::string_view name);
// 'a' with ' and \ backslash-escaped.
[[nodiscard]] std::string quote_string(std::string_view value);
[[nodiscard]] std::string qualified_table(std::string_view database, std::string_view table);

struct Token {
    std::string nonce_hex;                   // 32 lower-case hex digits
    std::uint64_t seq{0};                    // 1, 2, ... per open
    [[nodiscard]] std::string text() const;  // clink1-<nonce_hex>-<seq>
};

class TokenSource {
public:
    explicit TokenSource(std::array<std::uint8_t, 16> nonce);
    [[nodiscard]] static TokenSource random();  // std::random_device, drawn at each open
    [[nodiscard]] Token next();
    [[nodiscard]] const std::string& nonce_hex() const noexcept { return nonce_hex_; }

private:
    std::string nonce_hex_;
    std::uint64_t seq_{0};
};

// Legacy sink: INSERT INTO `db`.`t` SETTINGS async_insert=0,
// wait_for_async_insert=1, insert_deduplication_token='<token>' FORMAT <fmt>
[[nodiscard]] std::string legacy_insert_prefix(std::string_view db,
                                               std::string_view table,
                                               const Token& token,
                                               std::string_view format);

}  // namespace clink::clickhouse::native
