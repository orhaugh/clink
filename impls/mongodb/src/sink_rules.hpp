#pragma once

// The MongoDB sink's rules, apart from its I/O so they are testable without a
// server: which keys a replacement can match by, which write concerns the
// sink's guarantee survives, and which index makes a key match at most one
// document.

#include <chrono>
#include <optional>
#include <string>
#include <string_view>

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/write_concern.hpp>

namespace clink::mongodb::rules {

// The key types a replacement matches by: string, 32- and 64-bit integer, and
// ObjectId. Each names one value exactly. A null filter also matches every
// document that lacks the field, an array one any document whose array holds
// the value, a floating-point number compares by value across numeric types,
// an embedded document only in the same field order, and a regular expression
// by pattern; a replacement keyed by one of those can overwrite a document
// that is not the record's.
inline bool permitted_key_type(bsoncxx::type t) {
    return t == bsoncxx::type::k_string || t == bsoncxx::type::k_int32 ||
           t == bsoncxx::type::k_int64 || t == bsoncxx::type::k_oid;
}

inline std::string key_type_name(bsoncxx::type t) {
    switch (t) {
        case bsoncxx::type::k_double:
            return "a double";
        case bsoncxx::type::k_document:
            return "an embedded document";
        case bsoncxx::type::k_array:
            return "an array";
        case bsoncxx::type::k_binary:
            return "binary data";
        case bsoncxx::type::k_bool:
            return "a boolean";
        case bsoncxx::type::k_date:
            return "a date";
        case bsoncxx::type::k_null:
            return "null";
        case bsoncxx::type::k_regex:
            return "a regular expression";
        case bsoncxx::type::k_timestamp:
            return "a timestamp";
        case bsoncxx::type::k_decimal128:
            return "a decimal";
        default:
            return "a " + bsoncxx::to_string(t);
    }
}

// Why the record cannot be written as a replacement keyed by `key_field`, or
// nullopt when it can.
inline std::optional<std::string> key_problem(bsoncxx::document::view doc,
                                              std::string_view key_field) {
    const auto key = doc[key_field];
    if (!key) {
        return "it has no '" + std::string(key_field) + "' field to match a document by";
    }
    if (!permitted_key_type(key.type())) {
        return "its '" + std::string(key_field) + "' is " + key_type_name(key.type()) +
               ", which is not a key type the sink matches by (string, 32- or 64-bit integer, "
               "ObjectId)";
    }
    return std::nullopt;
}

// Why a key field name cannot be matched by, or nullopt. A replacement's filter
// names a top-level field; a dotted path or an operator name is not one.
inline std::optional<std::string> key_field_problem(std::string_view key_field) {
    if (key_field.empty()) {
        return std::string{"'key_field' is empty"};
    }
    if (key_field.find('.') != std::string_view::npos) {
        return "'key_field' is '" + std::string(key_field) +
               "', a dotted path; the key must be a top-level field";
    }
    if (key_field.front() == '$') {
        return "'key_field' is '" + std::string(key_field) + "', which names an operator";
    }
    return std::nullopt;
}

// Why the connection's write concern cannot carry the sink's guarantee, or
// nullopt. A checkpoint completes once the sink's batch is acknowledged, and
// what it acknowledged is never written again. Only a majority-acknowledged
// write survives a failover: one acknowledged by fewer members can be rolled
// back when the primary changes, after the checkpoint covering it completed.
inline std::optional<std::string> write_concern_problem(const mongocxx::write_concern& wc) {
    using level = mongocxx::write_concern::level;
    switch (wc.acknowledge_level()) {
        case level::k_default:
        case level::k_majority:
            return std::nullopt;
        case level::k_unacknowledged:
            return std::string{
                "the URI asks for w=0, which acknowledges nothing, so a batch the server never "
                "applied would count as written"};
        case level::k_tag:
            return std::string{
                "the URI asks for a tagged write concern, which the sink cannot show reaches a "
                "majority; a write acknowledged by fewer members can be rolled back in a "
                "failover after the checkpoint covering it completed"};
        case level::k_acknowledged:
        default:
            break;
    }
    const auto nodes = wc.nodes();
    return "the URI asks for w=" + std::to_string(nodes ? *nodes : 1) +
           "; a write acknowledged by fewer than a majority of members can be rolled back in a "
           "failover after the checkpoint covering it completed. Remove w from the URI or set "
           "w=majority";
}

// The write concern the sink writes with: majority, with the URI's wtimeout
// when it sets one and `timeout` otherwise, so a write that cannot reach a
// majority fails within a bound instead of waiting for ever.
inline mongocxx::write_concern effective_write_concern(const mongocxx::write_concern& from_uri,
                                                       std::chrono::milliseconds timeout) {
    mongocxx::write_concern wc;
    wc.majority(from_uri.majority() && from_uri.timeout().count() > 0 ? from_uri.timeout()
                                                                      : timeout);
    return wc;
}

// Whether `index` (one document of listIndexes) makes `field` match at most one
// document: unique, on that field alone, and with no partial filter, which
// would let a document outside it hold the same key.
inline bool unique_single_field_index(bsoncxx::document::view index, std::string_view field) {
    const auto unique = index["unique"];
    if (!unique || unique.type() != bsoncxx::type::k_bool || !unique.get_bool().value) {
        return false;
    }
    if (index["partialFilterExpression"]) {
        return false;
    }
    const auto key = index["key"];
    if (!key || key.type() != bsoncxx::type::k_document) {
        return false;
    }
    std::size_t fields = 0;
    bool names_field = false;
    for (const auto& e : key.get_document().value) {
        ++fields;
        names_field = e.key() == field;
    }
    return fields == 1 && names_field;
}

}  // namespace clink::mongodb::rules
