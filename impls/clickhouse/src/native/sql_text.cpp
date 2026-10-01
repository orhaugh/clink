#include "native/sql_text.hpp"

#include <cstddef>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>

namespace clink::clickhouse::native {

namespace {

// ClickHouse reads a backslash escape inside both quote styles, so escaping
// the quote character and the backslash itself is enough to keep any value
// inside its quotes. Every other byte, newlines included, is literal there.
std::string quoted(std::string_view text, char quote) {
    std::string out;
    out.reserve(text.size() + 2);
    out.push_back(quote);
    for (const char c : text) {
        if (c == quote || c == '\\') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    out.push_back(quote);
    return out;
}

bool is_lower_hex(char c) noexcept {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
}

// A default-constructed Token would give every batch the same deduplication
// token, so separate batches with the same content would drop each other:
// the defect the token exists to remove. Refuse it rather than send it.
void require_well_formed(const Token& token, const char* caller) {
    bool ok = token.seq != 0 && token.nonce_hex.size() == 32;
    for (const char c : token.nonce_hex) {
        ok = ok && is_lower_hex(c);
    }
    if (!ok) {
        throw std::invalid_argument(std::string(caller) +
                                    ": the token is not one TokenSource issued (" + token.text() +
                                    ")");
    }
}

// The format name is spliced into the statement unquoted, so it must be a
// bare word.
void require_format_name(std::string_view format) {
    bool ok = !format.empty();
    for (const char c : format) {
        ok = ok && ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
                    c == '_');
    }
    if (!ok) {
        throw std::invalid_argument("legacy_insert_prefix: '" + std::string(format) +
                                    "' is not a format name");
    }
}

}  // namespace

std::string quote_identifier(std::string_view name) {
    return quoted(name, '`');
}

std::string quote_string(std::string_view value) {
    return quoted(value, '\'');
}

std::string qualified_table(std::string_view database, std::string_view table) {
    return quote_identifier(database) + "." + quote_identifier(table);
}

std::string Token::text() const {
    return "clink1-" + nonce_hex + "-" + std::to_string(seq);
}

TokenSource::TokenSource(std::array<std::uint8_t, 16> nonce) {
    static constexpr char kDigits[] = "0123456789abcdef";
    nonce_hex_.reserve(nonce.size() * 2);
    for (const std::uint8_t byte : nonce) {
        nonce_hex_.push_back(kDigits[byte >> 4U]);
        nonce_hex_.push_back(kDigits[byte & 0x0FU]);
    }
}

TokenSource TokenSource::random() {
    // Every bit of a std::random_device draw is usable: its range is the
    // whole of its result type.
    static_assert(std::random_device::min() == 0);
    static_assert(std::random_device::max() ==
                  std::numeric_limits<std::random_device::result_type>::max());
    std::random_device device;
    std::array<std::uint8_t, 16> nonce{};
    std::size_t filled = 0;
    while (filled < nonce.size()) {
        auto word = device();
        for (std::size_t i = 0; i < sizeof(word) && filled < nonce.size(); ++i, ++filled) {
            nonce[filled] = static_cast<std::uint8_t>(word & 0xFFU);
            word >>= 8U;
        }
    }
    return TokenSource(nonce);
}

Token TokenSource::next() {
    return Token{nonce_hex_, ++seq_};
}

std::string legacy_insert_prefix(std::string_view db,
                                 std::string_view table,
                                 const Token& token,
                                 std::string_view format) {
    require_well_formed(token, "legacy_insert_prefix");
    require_format_name(format);
    return "INSERT INTO " + qualified_table(db, table) +
           " SETTINGS async_insert=0, wait_for_async_insert=1, insert_deduplication_token=" +
           quote_string(token.text()) + " FORMAT " + std::string(format);
}

}  // namespace clink::clickhouse::native
