// MongoDB sink implementation: buffer JSON-object records, parse to BSON, and
// bulk-write them into a collection (insert, or upsert-by-key under
// on_duplicate='replace'). See mongo_sink.hpp for the delivery contract.

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
#include <mongocxx/model/insert_one.hpp>
#include <mongocxx/model/replace_one.hpp>
#include <mongocxx/options/bulk_write.hpp>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/mongodb/mongo_client.hpp"

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

class MongoSink final : public Sink<std::string> {
public:
    explicit MongoSink(MongoSinkOptions opts) : opts_(std::move(opts)) {
        if (opts_.database.empty() || opts_.collection.empty()) {
            throw std::runtime_error(opts_.name + ": 'database' and 'collection' are required");
        }
        if (opts_.batch_records == 0) {
            opts_.batch_records = 1;
        }
    }

    void open() override {
        client_.emplace(make_client(opts_.uri));
        coll_ = (*client_)[opts_.database][opts_.collection];
    }

    void on_data(const Batch<std::string>& batch) override {
        for (const auto& rec : batch) {
            if (pending_.empty()) {
                first_buffered_at_ = std::chrono::steady_clock::now();
            }
            pending_bytes_ += rec.value().size();
            pending_.push_back(rec.value());
            if (pending_.size() >= opts_.batch_records || linger_elapsed_()) {
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
            auto bulk = coll_->create_bulk_write(bulk_opts);
            for (const auto& rec : pending_) {
                docs.push_back(bsoncxx::from_json(rec));  // throws on malformed JSON
                const auto view = docs.back().view();
                auto key_it = view.find(opts_.key_field);
                if (opts_.upsert && key_it != view.end()) {
                    using bsoncxx::builder::basic::kvp;
                    using bsoncxx::builder::basic::make_document;
                    filters.push_back(make_document(kvp(opts_.key_field, key_it->get_value())));
                    bulk.append(
                        mongocxx::model::replace_one{filters.back().view(), view}.upsert(true));
                } else {
                    bulk.append(mongocxx::model::insert_one{view});
                }
            }
            try {
                bulk.execute();
            } catch (const mongocxx::bulk_write_exception& e) {
                // A restore replays the records after the last checkpoint, and an
                // insert of one carrying its own _id meets the document this sink
                // already wrote. Those are written; anything else fails the flush.
                if (opts_.upsert) {
                    throw;
                }
                if (auto conflict = unwritten_records_(e, docs); conflict) {
                    throw std::runtime_error(std::string(e.what()) + "; " + *conflict);
                }
            }
        } catch (const std::exception& e) {
            clink::metrics::connector::error_inc(kLabel, "sink");
            pending_.clear();
            pending_bytes_ = 0;
            client_.reset();  // drop the connection; re-open re-establishes it
            coll_.reset();
            throw std::runtime_error(opts_.name + ": bulk write failed: " + e.what());
        }
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
        client_.reset();
    }

    std::string name() const override { return opts_.name; }

private:
    // Why a failed insert batch left records unwritten, or nullopt when every
    // error in it is a replay: a duplicate key on a record that carries its own
    // _id, whose document is stored exactly as the record now arrives, which is
    // the write this sink made before the restore. A record the driver gave a
    // fresh _id conflicts on some other unique index, and a different document
    // under the record's _id is a record of its own; both stay failures, as
    // does any error that is not a duplicate key.
    std::optional<std::string> unwritten_records_(
        const mongocxx::bulk_write_exception& e,
        const std::vector<bsoncxx::document::value>& docs) {
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
        return std::nullopt;
    }

    bool linger_elapsed_() const {
        return opts_.max_age.count() > 0 && !pending_.empty() &&
               std::chrono::steady_clock::now() - first_buffered_at_ >= opts_.max_age;
    }

    MongoSinkOptions opts_;
    std::optional<mongocxx::client> client_;
    std::optional<mongocxx::collection> coll_;
    std::vector<std::string> pending_;
    std::size_t pending_bytes_{0};
    std::chrono::steady_clock::time_point first_buffered_at_{};
};

}  // namespace

std::shared_ptr<Sink<std::string>> make_mongo_sink(const MongoSinkOptions& opts) {
    return std::make_shared<MongoSink>(opts);
}

}  // namespace clink::mongodb
