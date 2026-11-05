// MongoDB sink implementation: buffer JSON-object records, parse to BSON, and
// bulk-write them into a collection with w=majority (insert, or upsert-by-key
// under on_duplicate='replace'). See mongo_sink.hpp for the delivery contract
// and sink_rules.hpp for the key, write concern and index rules.

#include "clink/mongodb/mongo_sink.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/exception/bulk_write_exception.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/model/insert_one.hpp>
#include <mongocxx/model/replace_one.hpp>
#include <mongocxx/options/bulk_write.hpp>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/mongodb/mongo_client.hpp"
#include "clink/runtime/log_buffer.hpp"

#include "fault_points.hpp"
#include "sink_rules.hpp"

namespace clink::mongodb {

namespace {

constexpr const char* kLabel = "mongodb";

// The document MongoDB stores for `doc`: the same fields in the same order,
// except that the server moves _id to the front on insert.
bsoncxx::document::value as_stored(bsoncxx::document::view doc) {
    using bsoncxx::builder::basic::kvp;
    bsoncxx::builder::basic::document out;
    if (const auto id = doc["_id"]; id) {
        out.append(kvp("_id", id.get_value()));
    }
    for (const auto& field : doc) {
        if (field.key() != "_id") {
            out.append(kvp(std::string(field.key()), field.get_value()));
        }
    }
    return out.extract();
}

// A numeric field of a server reply as an integer (libmongoc reports error
// codes and indexes as int32, a server may send int64 or double).
std::optional<std::int64_t> as_integer(const bsoncxx::document::element& e) {
    if (!e) {
        return std::nullopt;
    }
    switch (e.type()) {
        case bsoncxx::type::k_int32:
            return e.get_int32().value;
        case bsoncxx::type::k_int64:
            return e.get_int64().value;
        case bsoncxx::type::k_double:
            return static_cast<std::int64_t>(e.get_double().value);
        default:
            return std::nullopt;
    }
}

constexpr std::int64_t kDuplicateKey = 11000;
constexpr int kNamespaceNotFound = 26;

class MongoSink final : public Sink<std::string> {
public:
    explicit MongoSink(MongoSinkOptions opts) : opts_(std::move(opts)) {
        if (opts_.database.empty() || opts_.collection.empty()) {
            throw std::runtime_error(opts_.name + ": 'database' and 'collection' are required");
        }
        if (opts_.upsert) {
            if (const auto why = rules::key_field_problem(opts_.key_field)) {
                throw std::runtime_error(opts_.name + ": " + *why);
            }
        }
        if (opts_.batch_records == 0) {
            opts_.batch_records = 1;
        }
        if (opts_.max_bytes == 0) {
            opts_.max_bytes = 1;
        }
        if (opts_.write_timeout.count() <= 0) {
            throw std::runtime_error(opts_.name + ": the write timeout must be positive");
        }
    }

    void open() override {
        try {
            connection_.emplace(opts_.uri);
            auto& client = connection_->client();
            // Every write is majority-acknowledged; a URI asking for less is refused
            // rather than quietly overridden or quietly honoured.
            const auto from_uri = client.write_concern();
            if (const auto why = rules::write_concern_problem(from_uri)) {
                throw std::runtime_error(*why);
            }
            write_concern_ = rules::effective_write_concern(from_uri, opts_.write_timeout);
            coll_ = client[opts_.database][opts_.collection];
            if (opts_.upsert && opts_.key_field != "_id") {
                require_unique_key_index_();
            }
        } catch (const std::exception& e) {
            coll_.reset();
            connection_.reset();
            throw std::runtime_error(opts_.name + ": " + e.what());
        }
    }

    void on_data(const Batch<std::string>& batch) override {
        for (const auto& rec : batch) {
            if (pending_.empty()) {
                first_buffered_at_ = std::chrono::steady_clock::now();
            }
            pending_bytes_ += rec.value().size();
            pending_.push_back(rec.value());
            if (pending_.size() >= opts_.batch_records || pending_bytes_ >= opts_.max_bytes ||
                linger_elapsed_()) {
                flush();
            }
        }
    }

    void on_barrier(CheckpointBarrier /*b*/) override { flush(); }
    // Its barrier hook writes out everything it received before the barrier,
    // which is its guarantee only while it owns its chain's checkpoint, so
    // Dag::add_sink refuses another sink beside it.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }

    void flush() override {
        if (pending_.empty()) {
            return;
        }
        if (!coll_) {
            throw std::runtime_error(opts_.name + ": flush() before open()");
        }
        const std::size_t n = pending_.size();
        const std::size_t bytes = pending_bytes_;
        const auto t0 = std::chrono::steady_clock::now();
        try {
            // Reserve so the document/filter buffers never reallocate: the bulk-write
            // models hold views into them and must stay valid until execute().
            std::vector<bsoncxx::document::value> docs;
            std::vector<bsoncxx::document::value> filters;
            docs.reserve(n);
            filters.reserve(n);
            // An upsert batch stays ordered, so a key written twice in it ends as its
            // later record. An insert batch carries no such order; unordered, every
            // record is attempted and every error reported, so a replayed batch names
            // each record already stored rather than stopping at the first.
            mongocxx::options::bulk_write bulk_opts;
            bulk_opts.ordered(opts_.upsert);
            bulk_opts.write_concern(write_concern_);
            auto bulk = coll_->create_bulk_write(bulk_opts);
            // Every record is parsed, and under replace keyed, before anything of
            // the batch is written: a record that cannot be matched by its key
            // fails the flush, rather than being inserted beside the document it
            // should have replaced.
            for (std::size_t i = 0; i < pending_.size(); ++i) {
                try {
                    docs.push_back(bsoncxx::from_json(pending_[i]));
                } catch (const std::exception& e) {
                    throw std::runtime_error("record " + std::to_string(i + 1) + " of " +
                                             std::to_string(n) + " is not a JSON object (" +
                                             e.what() + "); nothing of the batch was written");
                }
                const auto view = docs.back().view();
                if (opts_.upsert) {
                    if (const auto why = rules::key_problem(view, opts_.key_field)) {
                        throw std::runtime_error("record " + std::to_string(i + 1) + " of " +
                                                 std::to_string(n) + " cannot replace by key: " +
                                                 *why + "; nothing of the batch was written");
                    }
                    using bsoncxx::builder::basic::kvp;
                    using bsoncxx::builder::basic::make_document;
                    filters.push_back(
                        make_document(kvp(opts_.key_field, view[opts_.key_field].get_value())));
                    bulk.append(
                        mongocxx::model::replace_one{filters.back().view(), view}.upsert(true));
                } else {
                    bulk.append(mongocxx::model::insert_one{view});
                }
            }
            (void)CLINK_FAULT_POINT(points::kSinkBeforeWrite);
            try {
                bulk.execute();
            } catch (const mongocxx::bulk_write_exception& e) {
                // A restore replays the records after the last checkpoint, and an
                // insert of one carrying its own _id meets the document this sink
                // already wrote. Those are written; anything else fails the flush.
                if (opts_.upsert) {
                    throw;
                }
                std::size_t replayed = 0;
                if (auto conflict = unwritten_records_(e, docs, replayed); conflict) {
                    throw std::runtime_error(std::string(e.what()) + "; " + *conflict);
                }
                clink::log::info("mongodb.sink",
                                 opts_.name + ": " + std::to_string(replayed) + " of " +
                                     std::to_string(n) +
                                     " records were already stored exactly as they arrive: "
                                     "writes a run made before a restore, replayed");
            }
        } catch (const std::exception& e) {
            clink::metrics::connector::error_inc(kLabel, "sink");
            pending_.clear();
            pending_bytes_ = 0;
            coll_.reset();  // drop the connection; re-open re-establishes it
            connection_.reset();
            throw std::runtime_error(opts_.name + ": bulk write failed: " + e.what());
        }
        (void)CLINK_FAULT_POINT(points::kSinkAfterWrite);
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::records_out_inc(kLabel, n);
        clink::metrics::connector::bytes_out_inc(kLabel, bytes);
        clink::metrics::connector::commit_latency_observe(kLabel, static_cast<std::uint64_t>(dt));
        pending_.clear();
        pending_bytes_ = 0;
    }

    void close() override {
        if (coll_) {
            flush();
        }
        coll_.reset();
        connection_.reset();
    }

    std::string name() const override { return opts_.name; }

private:
    // A key other than _id must match at most one document, which only a unique
    // index on that field alone guarantees: without one, a replacement can
    // overwrite either of two documents holding the key, and two subtasks
    // upserting the same new key can insert it twice.
    void require_unique_key_index_() {
        bool found = false;
        try {
            for (auto&& index : coll_->list_indexes()) {
                if (rules::unique_single_field_index(index, opts_.key_field)) {
                    found = true;
                }
            }
        } catch (const mongocxx::operation_exception& e) {
            if (e.code().value() != kNamespaceNotFound) {
                throw;
            }
        }
        if (!found) {
            throw std::runtime_error(
                "on_duplicate='replace' with key_field='" + opts_.key_field +
                "' needs a unique index on that field alone, so that a key matches at most one "
                "document, and " +
                opts_.database + "." + opts_.collection +
                " has none: create it first, as createIndex({" + opts_.key_field +
                ": 1}, {unique: true})");
        }
    }

    // Why a failed insert batch left records unwritten, or nullopt when every
    // error in it is a replay: a duplicate key on a record that carries its own
    // _id, whose document is stored exactly as the record now arrives, which is
    // the write this sink made before the restore. A record the driver gave a
    // fresh _id conflicts on some other unique index, and a different document
    // under the record's _id is a record of its own; both stay failures, as
    // does any error that is not a duplicate key.
    std::optional<std::string> unwritten_records_(const mongocxx::bulk_write_exception& e,
                                                  const std::vector<bsoncxx::document::value>& docs,
                                                  std::size_t& replayed_count) {
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;
        const auto& reply = e.raw_server_error();
        if (!reply) {
            return "no server reply to tell a replayed record from a failure";
        }
        const auto view = reply->view();
        if (const auto wce = view["writeConcernErrors"];
            wce && wce.type() == bsoncxx::type::k_array && !wce.get_array().value.empty()) {
            return "the write concern was not met";
        }
        const auto errors = view["writeErrors"];
        if (!errors || errors.type() != bsoncxx::type::k_array) {
            return "no per-record errors in the server reply";
        }
        std::vector<std::size_t> replayed;
        bsoncxx::builder::basic::array ids;
        for (const auto& err : errors.get_array().value) {
            if (err.type() != bsoncxx::type::k_document) {
                return "an unreadable write error";
            }
            const auto doc = err.get_document().value;
            const auto code = as_integer(doc["code"]);
            const auto index = as_integer(doc["index"]);
            if (code != kDuplicateKey || !index || *index < 0 ||
                static_cast<std::size_t>(*index) >= docs.size()) {
                return "a write error other than a duplicate key";
            }
            const auto i = static_cast<std::size_t>(*index);
            const auto id = docs[i].view()["_id"];
            if (!id) {
                return "a duplicate key on a unique index other than _id, for a record "
                       "without an _id of its own";
            }
            replayed.push_back(i);
            ids.append(id.get_value());
        }
        if (replayed.empty()) {
            return "no per-record errors in the server reply";
        }
        std::vector<bsoncxx::document::value> stored;
        for (auto&& doc : coll_->find(make_document(kvp("_id", make_document(kvp("$in", ids)))))) {
            stored.emplace_back(doc);
        }
        for (const auto i : replayed) {
            const auto id = docs[i].view()["_id"].get_value();
            const auto expected = as_stored(docs[i].view());
            const bsoncxx::document::value* match = nullptr;
            for (const auto& s : stored) {
                if (s.view()["_id"].get_value() == id) {
                    match = &s;
                    break;
                }
            }
            const auto id_json = bsoncxx::to_json(make_document(kvp("_id", id)));
            if (match == nullptr) {
                return "the record with " + id_json +
                       " met a duplicate key on another unique index, and nothing is stored "
                       "under its _id";
            }
            if (match->view() != expected.view()) {
                return "the record with " + id_json +
                       " differs from the document stored under that _id, so it is a record "
                       "of its own, not a replay of one this sink wrote; on_duplicate='replace' "
                       "with key_field='_id' writes the later record instead";
            }
        }
        replayed_count = replayed.size();
        return std::nullopt;
    }

    bool linger_elapsed_() const {
        return opts_.max_age.count() > 0 && !pending_.empty() &&
               std::chrono::steady_clock::now() - first_buffered_at_ >= opts_.max_age;
    }

    MongoSinkOptions opts_;
    std::optional<Connection> connection_;
    std::optional<mongocxx::collection> coll_;
    mongocxx::write_concern write_concern_;
    std::vector<std::string> pending_;
    std::size_t pending_bytes_{0};
    std::chrono::steady_clock::time_point first_buffered_at_{};
};

}  // namespace

std::shared_ptr<Sink<std::string>> make_mongo_sink(const MongoSinkOptions& opts) {
    return std::make_shared<MongoSink>(opts);
}

}  // namespace clink::mongodb
