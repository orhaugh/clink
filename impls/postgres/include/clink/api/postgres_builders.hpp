// Fluent builders for the Postgres text source / CDC source factories.
// Lives at include/clink/api/ during Phase 1 of the impls split; in
// Phase 2 this header moves to impls/postgres/include/clink/api/.

#pragma once

#include <cstdint>
#include <string>
#include <utility>

#include "clink/api/descriptors.hpp"

namespace clink::api {

class PostgresTextSource {
public:
    class Builder {
    public:
        Builder& conninfo(std::string c) {
            conninfo_ = std::move(c);
            return *this;
        }
        Builder& query(std::string q) {
            query_ = std::move(q);
            return *this;
        }
        Builder& delim(std::string d) {
            delim_ = std::move(d);
            return *this;
        }
        Builder& batch_size(std::int64_t n) {
            batch_size_ = n;
            return *this;
        }

        SourceDescriptor build() const {
            SourceDescriptor d;
            d.op_type = "postgres_text_source";
            d.channel_type = "string";
            d.params["conninfo"] = conninfo_;
            d.params["query"] = query_;
            if (!delim_.empty()) {
                d.params["delim"] = delim_;
            }
            d.params["batch_size"] = std::to_string(batch_size_);
            return d;
        }

    private:
        std::string conninfo_;
        std::string query_;
        std::string delim_;
        std::int64_t batch_size_{256};
    };

    static Builder builder() { return Builder{}; }
};

// Subscribes to a Postgres logical-replication slot and emits each
// CdcEvent as a JSON line.
class PostgresCdcTextSource {
public:
    class Builder {
    public:
        Builder& conninfo(std::string c) {
            conninfo_ = std::move(c);
            return *this;
        }
        Builder& slot_name(std::string s) {
            slot_name_ = std::move(s);
            return *this;
        }
        Builder& plugin(std::string p) {
            plugin_ = std::move(p);
            return *this;
        }
        Builder& publication_names(std::string p) {
            publication_names_ = std::move(p);
            return *this;
        }
        Builder& create_slot(bool v) {
            create_slot_ = v;
            return *this;
        }
        Builder& drop_slot_on_close(bool v) {
            drop_slot_on_close_ = v;
            return *this;
        }

        SourceDescriptor build() const {
            SourceDescriptor d;
            d.op_type = "postgres_cdc_text_source";
            d.channel_type = "string";
            d.params["conninfo"] = conninfo_;
            d.params["slot_name"] = slot_name_;
            if (!plugin_.empty()) {
                d.params["plugin"] = plugin_;
            }
            if (!publication_names_.empty()) {
                d.params["publication_names"] = publication_names_;
            }
            d.params["create_slot"] = create_slot_ ? "true" : "false";
            d.params["drop_slot_on_close"] = drop_slot_on_close_ ? "true" : "false";
            return d;
        }

    private:
        std::string conninfo_;
        std::string slot_name_;
        std::string plugin_;
        std::string publication_names_;
        bool create_slot_{true};
        bool drop_slot_on_close_{false};
    };

    static Builder builder() { return Builder{}; }
};

}  // namespace clink::api
