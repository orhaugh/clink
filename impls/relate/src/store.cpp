#include "store.hpp"

#include <filesystem>
#include <system_error>
#include <utility>

namespace clink::relate {

namespace {

std::string take(char* owned) {
    std::string out = owned != nullptr ? owned : "";
    relate_free_string(owned);
    return out;
}

// The key a store is shared under and the path Relate is given: absolute, with
// symbolic links resolved as far as the path exists, so two spellings of one
// store meet on one handle.
std::string resolved(const std::string& path) {
    std::error_code ec;
    const auto absolute = std::filesystem::absolute(path, ec);
    if (ec) {
        return path;
    }
    const auto canonical = std::filesystem::weakly_canonical(absolute, ec);
    return (ec ? absolute.lexically_normal() : canonical).string();
}

std::mutex& registry_mutex() {
    static std::mutex mu;
    return mu;
}

}  // namespace

struct Store::Shared {
    relate_database db{nullptr};
    std::string path;
    std::mutex open_mu;
    std::size_t users{0};
};

std::map<std::string, std::unique_ptr<Store::Shared>>& Store::registry_() {
    static std::map<std::string, std::unique_ptr<Shared>> stores;
    return stores;
}

std::unique_ptr<Store> Store::open(const std::string& path) {
    const std::string key = resolved(path);
    std::lock_guard<std::mutex> lock(registry_mutex());
    auto& stores = registry_();
    if (const auto it = stores.find(key); it != stores.end()) {
        ++it->second->users;
        return std::unique_ptr<Store>(new Store(it->second.get()));
    }
    std::error_code ec;
    const auto parent = std::filesystem::path(key).parent_path();
    if (!parent.empty() && !std::filesystem::is_directory(parent, ec)) {
        throw RelateError(code::kStoreUnavailable,
                          "relate sink: the store '" + key + "' cannot be created: '" +
                              parent.string() + "' is not a directory");
    }
    relate_database db = nullptr;
    char* err = nullptr;
    if (relate_database_open(key.c_str(), &db, &err) != RELATE_OK || db == nullptr) {
        const auto e = parse_engine_error(take(err));
        throw RelateError(
            code::kStoreUnavailable,
            "relate sink: the store '" + key + "' could not be opened: " + e.describe() +
                ". Relate reports a store it cannot lock or read only as an internal error: "
                "another process may have it open for writing (a store has one writing "
                "process), or the path may hold something that is not a Relate store. Set "
                "RELATE_FFI_LOG=1 to have Relate log its own reason to stderr");
    }
    auto shared = std::make_unique<Shared>();
    shared->db = db;
    shared->path = key;
    shared->users = 1;
    auto* raw = shared.get();
    stores.emplace(key, std::move(shared));
    return std::unique_ptr<Store>(new Store(raw));
}

Store::~Store() {
    std::lock_guard<std::mutex> lock(registry_mutex());
    if (--shared_->users > 0) {
        return;
    }
    relate_database_close(shared_->db);
    registry_().erase(shared_->path);
}

relate_database Store::handle() const noexcept {
    return shared_->db;
}

const std::string& Store::path() const noexcept {
    return shared_->path;
}

std::mutex& Store::open_mutex() noexcept {
    return shared_->open_mu;
}

std::size_t Store::users() const {
    std::lock_guard<std::mutex> lock(registry_mutex());
    return shared_->users;
}

// ---- Connection -------------------------------------------------------------

Connection::Connection(Store& store) {
    char* err = nullptr;
    if (relate_connect_namespace(store.handle(), nullptr, &conn_, &err) != RELATE_OK ||
        conn_ == nullptr) {
        throw RelateError(code::kEngineFailure,
                          "relate sink: connecting to the store '" + store.path() +
                              "' failed: " + parse_engine_error(take(err)).describe());
    }
}

Connection::~Connection() {
    if (conn_ == nullptr) {
        return;
    }
    if (in_transaction_) {
        relate_conn_rollback(conn_, nullptr);
    }
    relate_disconnect(conn_);
}

Reply Connection::run(const std::string& cypher, relate_params params) {
    relate_query_result result = nullptr;
    char* err = nullptr;
    if (relate_query_params(conn_, cypher.c_str(), params, &result, &err) != RELATE_OK) {
        throw RelateError(
            code::kEngineFailure,
            "relate sink: the connection failed: " + parse_engine_error(take(err)).describe());
    }
    Reply reply;
    if (relate_result_is_error(result) != 0) {
        reply.ok = false;
        const char* shape = relate_result_error_json(result);
        reply.error = parse_engine_error(shape != nullptr ? shape : "");
    } else {
        if (const char* type = relate_result_query_type(result); type != nullptr) {
            reply.query_type = type;
        }
        reply.columns = relate_column_count(result);
        if (const char* bookmark = relate_result_bookmark(result); bookmark != nullptr) {
            reply.bookmark = bookmark;
        }
    }
    relate_destroy_result(result);
    return reply;
}

Reply Connection::begin() {
    Reply reply;
    char* err = nullptr;
    if (relate_conn_begin(conn_, &err) != RELATE_OK) {
        reply.ok = false;
        reply.error = parse_engine_error(take(err));
        return reply;
    }
    in_transaction_ = true;
    return reply;
}

Reply Connection::commit() {
    Reply reply;
    char* bookmark = nullptr;
    char* err = nullptr;
    if (relate_conn_commit(conn_, &bookmark, &err) != RELATE_OK) {
        relate_free_string(bookmark);
        reply.ok = false;
        reply.error = parse_engine_error(take(err));
        return reply;
    }
    reply.bookmark = take(bookmark);
    in_transaction_ = false;
    return reply;
}

void Connection::rollback() noexcept {
    if (conn_ != nullptr) {
        relate_conn_rollback(conn_, nullptr);
    }
    in_transaction_ = false;
}

}  // namespace clink::relate
