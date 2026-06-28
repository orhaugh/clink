#pragma once

// Pure (no mongocxx) mapping of a MongoDB change-stream event - already converted
// to clink JSON (the source does bsoncxx::to_json then clink::config::parse) - into
// the emitted record JSON. The changed document image (fullDocument, or
// documentKey for a delete) becomes the top-level object, plus reserved
// __op / __ns / __id metadata added with no-overwrite emplace (a data field
// literally named __op keeps its own value, matching the Postgres/MySQL CDC
// reserved-key precedence). Non-row events (drop / rename / invalidate /
// dropDatabase / ...) carry no document image and return std::nullopt.
//
// Pure clink-JSON, so it is unit-tested against hand-built change events without a
// live MongoDB.

#include <optional>
#include <string>

#include "clink/config/json.hpp"

namespace clink::mongodb {

inline const clink::config::JsonValue* json_find(const clink::config::JsonObject& o,
                                                 const char* key) {
    auto it = o.find(key);
    return it == o.end() ? nullptr : &it->second;
}

inline std::optional<std::string> change_event_to_json(const clink::config::JsonValue& ev) {
    if (!ev.is_object()) {
        return std::nullopt;
    }
    const auto& e = ev.as_object();

    const clink::config::JsonValue* op_v = json_find(e, "operationType");
    if (op_v == nullptr || !op_v->is_string()) {
        return std::nullopt;
    }
    const std::string op = op_v->as_string();
    const bool is_insert = op == "insert";
    const bool is_replace = op == "replace";
    const bool is_update = op == "update";
    const bool is_delete = op == "delete";
    if (!is_insert && !is_replace && !is_update && !is_delete) {
        return std::nullopt;  // not a row-level change (drop/rename/invalidate/...)
    }

    // The document image for this operation.
    clink::config::JsonObject body;
    if (is_insert || is_replace) {
        if (const auto* fd = json_find(e, "fullDocument"); fd != nullptr && fd->is_object()) {
            body = fd->as_object();
        }
    } else if (is_update) {
        if (const auto* fd = json_find(e, "fullDocument"); fd != nullptr && fd->is_object()) {
            body = fd->as_object();  // full_document=updateLookup: the post-image
        } else if (const auto* ud = json_find(e, "updateDescription");
                   ud != nullptr && ud->is_object()) {
            if (const auto* uf = json_find(ud->as_object(), "updatedFields");
                uf != nullptr && uf->is_object()) {
                body = uf->as_object();  // partial: only the changed fields
            }
        }
    } else {  // delete: only the key survives
        if (const auto* dk = json_find(e, "documentKey"); dk != nullptr && dk->is_object()) {
            body = dk->as_object();
        }
    }

    std::string ns;
    if (const auto* nsv = json_find(e, "ns"); nsv != nullptr && nsv->is_object()) {
        const auto& n = nsv->as_object();
        if (const auto* db = json_find(n, "db"); db != nullptr && db->is_string()) {
            ns = db->as_string();
        }
        if (const auto* coll = json_find(n, "coll"); coll != nullptr && coll->is_string()) {
            ns += "." + coll->as_string();
        }
    }

    clink::config::JsonValue id_val;  // null unless documentKey._id is present
    if (const auto* dk = json_find(e, "documentKey"); dk != nullptr && dk->is_object()) {
        if (const auto* idp = json_find(dk->as_object(), "_id"); idp != nullptr) {
            id_val = *idp;  // keep its JSON type verbatim (ObjectId object, int, string, ...)
        }
    }

    body.emplace("__op", clink::config::JsonValue{op});
    body.emplace("__ns", clink::config::JsonValue{ns});
    body.emplace("__id", id_val);
    return clink::config::JsonValue{std::move(body)}.serialize(0);
}

}  // namespace clink::mongodb
