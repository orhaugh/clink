#pragma once

#include <functional>
#include <optional>
#include <string>
#include <string_view>

namespace clink {

// TextFormat<T> is the (de)serialization seam for line-oriented text
// connectors (file, Kafka with text payloads, newline-delimited S3 objects).
//
// The two callbacks are:
//   - decode: parse a single text line into a T (return nullopt to skip)
//   - encode: serialize a T to a single text line (no trailing newline)
//
// A binary equivalent (BinaryFormat<T>) will live next to this once we add
// connectors that need it (Avro, Parquet, raw bytes). The two are kept
// separate because the framing concerns are different - text is line-
// delimited, binary needs explicit length prefixes or a schema.
template <typename T>
struct TextFormat {
    using Decoder = std::function<std::optional<T>(std::string_view)>;
    using Encoder = std::function<std::string(const T&)>;

    Decoder decode;
    Encoder encode;
};

// Identity format for std::string: every line is a record verbatim.
inline TextFormat<std::string> string_text_format() {
    return TextFormat<std::string>{
        .decode = [](std::string_view s) -> std::optional<std::string> { return std::string{s}; },
        .encode = [](const std::string& s) { return s; }};
}

}  // namespace clink
