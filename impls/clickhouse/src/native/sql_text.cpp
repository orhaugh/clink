#include "native/sql_text.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native {

std::string quote_identifier(std::string_view /*name*/) {
    not_implemented("quote_identifier");
}

std::string quote_string(std::string_view /*value*/) {
    not_implemented("quote_string");
}

std::string qualified_table(std::string_view /*database*/, std::string_view /*table*/) {
    not_implemented("qualified_table");
}

std::string Token::text() const {
    not_implemented("Token::text");
}

TokenSource::TokenSource(std::array<std::uint8_t, 16> /*nonce*/) {
    not_implemented("TokenSource");
}

TokenSource TokenSource::random() {
    not_implemented("TokenSource::random");
}

Token TokenSource::next() {
    not_implemented("TokenSource::next");
}

std::string legacy_insert_prefix(std::string_view /*db*/,
                                 std::string_view /*table*/,
                                 const Token& /*token*/,
                                 std::string_view /*format*/) {
    not_implemented("legacy_insert_prefix");
}

}  // namespace clink::clickhouse::native
