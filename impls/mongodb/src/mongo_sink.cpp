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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/model/insert_one.hpp>
#include <mongocxx/model/replace_one.hpp>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/mongodb/mongo_client.hpp"

namespace clink::mongodb {

namespace {

constexpr const char* kLabel = "mongodb";

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
            auto bulk = coll_->create_bulk_write();  // ordered: first error throws
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
            bulk.execute();
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
