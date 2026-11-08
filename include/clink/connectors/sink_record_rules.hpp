#pragma once

// Record rules the SQL sinks share, testable without a server: the strict
// reading of a record (one JSON object, numbers kept as written), a number's
// exact plain decimal, the key a record names as the stream holds it, the
// changelog marker, the packing of rendered rows into statements under a byte
// bound, and the changelog buffer that nets a key's changes while keeping the
// order in which the survivors were last changed. Each sink renders values and
// checks its table in its own dialect.

#include <cstddef>
#include <cstdint>
#include <list>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace clink::sinkrules {

// ---- one record -----------------------------------------------------------------------

struct JsonMember {
    enum class Kind : std::uint8_t { Null, Bool, Number, String, Composite };
    Kind kind{Kind::Null};
    // Number: the token as written; String: its decoded UTF-8; Bool: "true" or
    // "false"; Composite: the object's or array's text as written.
    std::string text;
};
using JsonMembers = std::map<std::string, JsonMember, std::less<>>;

// Reads `text` as one JSON object, strictly (RFC 8259: UTF-8 throughout, no
// raw control characters in strings, no repeated member names, nothing but
// whitespace after the object). Returns why it is not one, or an empty string.
// The reason never quotes the record.
[[nodiscard]] std::string read_json_object(std::string_view text, JsonMembers& out);

// A JSON number token's exact value in plain decimal: no exponent, no leading
// zeros, no trailing zeros after a point, "0" for zero of either sign. Empty
// when that form would be longer than `max_chars`. The token has passed JSON's
// number grammar.
[[nodiscard]] std::string plain_decimal(std::string_view token, std::size_t max_chars);

// A record the sink cannot write as declared. The message names why, never the
// record's contents.
class RecordRefused : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// ---- keys ----------------------------------------------------------------------------

// A text that identifies the record's key exactly as the stream holds it: the
// key members' kinds and texts, length-prefixed.
[[nodiscard]] std::string stream_key(const JsonMembers& m,
                                     const std::vector<std::string>& key_columns);

// ---- the changelog marker -------------------------------------------------------------

enum class ChangeKind : std::uint8_t { Upsert, Delete };

inline constexpr std::string_view kRowKindMember = "__row_kind";

// The record's __row_kind: none or "insert" or "update_after" is an upsert,
// "delete" or "update_before" a delete. Anything else is refused.
[[nodiscard]] ChangeKind change_kind_of(const JsonMembers& m);

// ---- statements -------------------------------------------------------------------------

// head + items (comma-separated) + tail, as few statements as keep each within
// `max_bytes`. Items keep their order. Throws when one item alone does not fit.
[[nodiscard]] std::vector<std::string> pack_statements(const std::string& head,
                                                       const std::vector<std::string>& items,
                                                       const std::string& tail,
                                                       std::size_t max_bytes);

// ---- the changelog buffer -------------------------------------------------------------

// A flush's changes in the order they must be applied. A change to a key the
// buffer holds replaces the earlier one, and the survivor takes the new
// change's place, so the survivors stay in the order in which each key was
// last changed. Two keys the stream holds apart that the table treats as one
// row then still meet the table in the order they were changed.
class ChangelogBuffer {
public:
    struct Change {
        ChangeKind kind{ChangeKind::Upsert};
        std::string row;  // the rendered VALUES row (upserts)
        std::string key;  // the rendered key (deletes)
    };

    // Adds a change, replacing the key's earlier one.
    void add(const std::string& stream_key, Change change);
    [[nodiscard]] std::size_t size() const noexcept { return changes_.size(); }
    [[nodiscard]] std::size_t bytes() const noexcept { return bytes_; }
    [[nodiscard]] bool empty() const noexcept { return changes_.empty(); }
    void clear();

    // The surviving changes in order, as consecutive runs of one kind.
    [[nodiscard]] std::vector<std::pair<ChangeKind, std::vector<std::string>>> runs() const;

private:
    struct Entry {
        std::string stream_key;
        Change change;
    };
    static std::size_t weight(const Change& c) { return c.row.size() + c.key.size(); }

    std::list<Entry> changes_;
    std::unordered_map<std::string, std::list<Entry>::iterator> index_;
    std::size_t bytes_{0};
};

}  // namespace clink::sinkrules
