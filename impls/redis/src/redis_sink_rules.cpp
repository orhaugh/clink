// The Redis keyed sink's pure rules. See redis_sink_rules.hpp.

#include "clink/redis/redis_sink_rules.hpp"

namespace clink::redis {

namespace {

// The longest plain decimal a key takes a number in.
constexpr std::size_t kKeyNumberChars = 4096;

}  // namespace

std::string redis_key(const JsonMembers& m,
                      const std::vector<std::string>& key_columns,
                      const std::string& prefix) {
    std::string key = prefix;
    for (std::size_t i = 0; i < key_columns.size(); ++i) {
        const auto& kc = key_columns[i];
        const auto it = m.find(kc);
        if (it == m.end() || it->second.kind == JsonMember::Kind::Null) {
            throw RecordRefused("its key member \"" + kc +
                                "\" is missing or null, so it names no key");
        }
        if (i > 0) {
            key.push_back('\x1f');
        }
        switch (it->second.kind) {
            case JsonMember::Kind::String:
                if (key_columns.size() > 1 && it->second.text.find('\x1f') != std::string::npos) {
                    throw RecordRefused("its key member \"" + kc +
                                        "\" holds U+001F, the separator between key columns, "
                                        "so two rows could name one key");
                }
                key += it->second.text;
                break;
            case JsonMember::Kind::Number: {
                const auto plain = sinkrules::plain_decimal(it->second.text, kKeyNumberChars);
                if (plain.empty()) {
                    throw RecordRefused("its key member \"" + kc +
                                        "\" is a number too long to write as a key");
                }
                key += plain;
                break;
            }
            case JsonMember::Kind::Bool:
                key += it->second.text;
                break;
            default:
                throw RecordRefused("its key member \"" + kc +
                                    "\" is an object or array, which names no key");
        }
    }
    return key;
}

}  // namespace clink::redis
