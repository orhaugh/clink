#pragma once

// Pure (no mongocxx) mapping of a MongoDB change-stream event - already converted
// to clink JSON (the source does bsoncxx::to_json then clink::config::parse) - into
// the emitted record JSON: the event's document fields at the top level, plus the
// change metadata
//
//   __op     insert | update | replace | delete
//   __ns     "<db>.<collection>"
//   __id     the changed document's _id, as the event's documentKey carries it
//   __image  what the document fields are:
//              full     the whole document as the change left it: an insert, a
//                       replace, or an update read with a post-image
//                       (full_document whenAvailable or required)
//              lookup   an update read with updateLookup: the document as it was
//                       when the server looked it up, after the event, so it can
//                       already carry later changes
//              partial  an update with no document to read: only the fields the
//                       update set (updateDescription.updatedFields, top-level and
//                       dotted paths as the server names them); removed fields
//                       are not carried
//              key      a delete: the documentKey only
//
// A document field named like one of these is refused (ChangeEventError), so a
// field of the data cannot pass for change metadata.
//
// Every event that is not a row change is refused too (ChangeStreamStop):
// drop, rename, dropDatabase and invalidate remove or move documents without a
// delete event for each, or end the stream, so the source cannot continue past
// one as if the documents it emitted were still there. So is an event type the
// source does not know.
//
// Pure clink-JSON, so it is unit-tested against hand-built change events without a
// live MongoDB.

#include <array>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "clink/config/json.hpp"

namespace clink::mongodb {

// The fullDocument the source asks the change stream for, which decides what an
// update event's document fields are.
enum class FullDocument {
    Off,            // none: an update carries only the fields it set
    UpdateLookup,   // the current document, looked up after the event
    WhenAvailable,  // the post-image, where the collection keeps them; else none
    Required,       // the post-image; the stream fails where there is none
};

inline constexpr std::array<std::string_view, 4> kMetadataKeys = {
    "__op", "__ns", "__id", "__image"};

// An event the source cannot continue past.
class ChangeStreamStop : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// An event the source cannot represent as a record.
class ChangeEventError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

inline const clink::config::JsonValue* json_find(const clink::config::JsonObject& o,
                                                 const char* key) {
    auto it = o.find(key);
    return it == o.end() ? nullptr : &it->second;
}

namespace detail {

inline std::string ns_of(const clink::config::JsonValue* nsv) {
    std::string ns;
    if (nsv != nullptr && nsv->is_object()) {
        const auto& n = nsv->as_object();
        if (const auto* db = json_find(n, "db"); db != nullptr && db->is_string()) {
            ns = db->as_string();
        }
        if (const auto* coll = json_find(n, "coll"); coll != nullptr && coll->is_string()) {
            ns += "." + coll->as_string();
        }
    }
    return ns;
}

inline constexpr const char* kHowToContinue =
    " To continue, start the job without restoring this source's state; it then starts at the "
    "current time, after this event.";

}  // namespace detail

// A change event taken apart: what the metadata says and what the document
// fields are. Shared by the JSON envelope (change_event_to_json) and the typed
// SQL row (sql_row.hpp), which read the same event in relaxed and canonical
// Extended JSON respectively.
struct ChangeParts {
    std::string op;                  // insert | update | replace | delete
    std::string ns;                  // "<db>.<collection>"
    std::string image;               // full | lookup | partial | key
    clink::config::JsonValue id;     // documentKey._id, null when absent
    clink::config::JsonObject body;  // the document fields
};

// Throws ChangeStreamStop for an event the source cannot continue past and
// ChangeEventError for one it cannot represent, a document field named like the
// metadata among them.
inline ChangeParts decompose_change(const clink::config::JsonValue& ev, FullDocument mode) {
    if (!ev.is_object()) {
        throw ChangeEventError("a change event that is not a document");
    }
    const auto& e = ev.as_object();

    const clink::config::JsonValue* op_v = json_find(e, "operationType");
    if (op_v == nullptr || !op_v->is_string()) {
        throw ChangeEventError("a change event without an operationType");
    }
    ChangeParts parts;
    parts.op = op_v->as_string();
    const std::string& op = parts.op;
    parts.ns = detail::ns_of(json_find(e, "ns"));
    const std::string& ns = parts.ns;

    if (op == "drop") {
        throw ChangeStreamStop("the change stream reported that " + ns +
                               " was dropped: its documents are gone without a delete event "
                               "for each, so the source stops rather than continue as if they "
                               "were still there." +
                               detail::kHowToContinue);
    }
    if (op == "rename") {
        throw ChangeStreamStop("the change stream reported that " + ns + " was renamed to " +
                               detail::ns_of(json_find(e, "to")) +
                               ": its documents moved without an event for each, so the source "
                               "stops rather than continue as if they were where it read them." +
                               detail::kHowToContinue);
    }
    if (op == "dropDatabase") {
        throw ChangeStreamStop("the change stream reported that database " + ns +
                               " was dropped: its documents are gone without a delete event for "
                               "each, so the source stops." +
                               detail::kHowToContinue);
    }
    if (op == "invalidate") {
        throw ChangeStreamStop(
            "the change stream was invalidated (the watched collection or database was dropped "
            "or renamed) and delivers nothing more." +
            std::string(detail::kHowToContinue));
    }
    const bool is_insert = op == "insert";
    const bool is_replace = op == "replace";
    const bool is_update = op == "update";
    const bool is_delete = op == "delete";
    if (!is_insert && !is_replace && !is_update && !is_delete) {
        throw ChangeStreamStop("the change stream delivered an event of type '" + op + "' on " +
                               ns + ", which the source does not handle.");
    }

    const auto* fd = json_find(e, "fullDocument");
    const bool has_document = fd != nullptr && fd->is_object();
    if (is_insert || is_replace) {
        if (!has_document) {
            throw ChangeEventError("an " + op + " event on " + ns + " without its document");
        }
        parts.body = fd->as_object();
        parts.image = "full";
    } else if (is_update) {
        if (has_document) {
            parts.body = fd->as_object();
            parts.image = mode == FullDocument::UpdateLookup ? "lookup" : "full";
        } else {
            if (const auto* ud = json_find(e, "updateDescription");
                ud != nullptr && ud->is_object()) {
                if (const auto* uf = json_find(ud->as_object(), "updatedFields");
                    uf != nullptr && uf->is_object()) {
                    parts.body = uf->as_object();
                }
            }
            parts.image = "partial";
        }
    } else {  // delete: only the key survives
        if (const auto* dk = json_find(e, "documentKey"); dk != nullptr && dk->is_object()) {
            parts.body = dk->as_object();
        }
        parts.image = "key";
    }

    if (const auto* dk = json_find(e, "documentKey"); dk != nullptr && dk->is_object()) {
        if (const auto* idp = json_find(dk->as_object(), "_id"); idp != nullptr) {
            parts.id = *idp;  // keep its JSON type verbatim (ObjectId object, int, string, ...)
        }
    }

    for (const auto key : kMetadataKeys) {
        if (parts.body.find(std::string(key)) != parts.body.end()) {
            throw ChangeEventError("a document in " + ns + " (_id " + parts.id.serialize(0) +
                                   ") has a top-level field '" + std::string(key) +
                                   "', which the source reserves for change metadata, so it "
                                   "cannot be told apart from the metadata");
        }
    }
    return parts;
}

inline std::string change_event_to_json(const clink::config::JsonValue& ev, FullDocument mode) {
    auto parts = decompose_change(ev, mode);
    auto& body = parts.body;
    body.emplace("__op", clink::config::JsonValue{parts.op});
    body.emplace("__ns", clink::config::JsonValue{parts.ns});
    body.emplace("__id", parts.id);
    body.emplace("__image", clink::config::JsonValue{parts.image});
    return clink::config::JsonValue{std::move(body)}.serialize(0);
}

// What a server error code means for a source resuming its change stream, or
// nullopt when it is not a resume failure. The source never falls back to the
// current time: a token it cannot resume from fails the job.
inline std::optional<std::string> resume_failure(int server_code) {
    switch (server_code) {
        case 286:  // ChangeStreamHistoryLost
            return std::string{
                "the oplog no longer holds the history after the checkpointed resume token, so "
                "the changes since it are lost to the source"};
        case 260:  // InvalidResumeToken
            return std::string{
                "the checkpointed resume token is not one this change stream can resume from "
                "(a different collection or deployment, or a corrupted token)"};
        case 280:  // ChangeStreamFatalError
            return std::string{"the server cannot resume this change stream"};
        default:
            return std::nullopt;
    }
}

}  // namespace clink::mongodb
