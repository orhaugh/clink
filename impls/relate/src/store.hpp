#pragma once

// A Relate store open in this process, and a connection to it.

#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <relate.h>
#include <string>

#include "errors.hpp"

namespace clink::relate {

// One user's hold on a Relate store that this process has open for writing.
//
// Relate gives a store one writing process, and refuses a second handle on it
// even to the process that holds the first. So every sink on one store shares
// one handle: the first use opens it, creating the store when it does not
// exist, and the last release closes it. A handle may be used from several
// threads at once; each user takes its own Connection.
class Store {
public:
    // Throws RelateError(store_unavailable) when the store cannot be opened.
    [[nodiscard]] static std::unique_ptr<Store> open(const std::string& path);
    ~Store();
    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    [[nodiscard]] relate_database handle() const noexcept;
    // The store's absolute path, as Relate was given it.
    [[nodiscard]] const std::string& path() const noexcept;
    // Held while a sink applies its schema statements and checks its statement:
    // two sinks opening on one store would otherwise meet over one constraint.
    [[nodiscard]] std::mutex& open_mutex() noexcept;
    // How many holds this process has on the store, this one included.
    [[nodiscard]] std::size_t users() const;

private:
    struct Shared;
    explicit Store(Shared* shared) noexcept : shared_(shared) {}
    // The stores this process has open, by resolved path. Read and changed
    // only under the registry's mutex, the open and the close included, so a
    // release that closes a store and an open that would join it cannot cross.
    static std::map<std::string, std::unique_ptr<Shared>>& registry_();

    Shared* shared_;
};

// How one call ended. A statement, a begin or a commit that Relate refused is a
// Reply with ok false and Relate's own account of it.
struct Reply {
    bool ok{true};
    EngineError error;
    std::string query_type;  // "r", "w", "rw" or "s", for a statement that ran
    long long columns{0};    // the columns a statement that ran returns
    std::string bookmark;    // "relate:<n>" when the call committed a write
};

// A session on a store. Not for two threads at once: its owner makes one call
// at a time.
class Connection {
public:
    // Throws RelateError(engine_failure).
    explicit Connection(Store& store);
    // Rolls back a transaction left open, then disconnects.
    ~Connection();
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    // Runs one statement with `params` (nullptr for none). Throws
    // RelateError(engine_failure) when the connection itself failed, which
    // leaves it unusable.
    [[nodiscard]] Reply run(const std::string& cypher, relate_params params);
    [[nodiscard]] Reply begin();
    // Commits the open transaction. After a refusal the transaction is still
    // open and must be rolled back.
    [[nodiscard]] Reply commit();
    void rollback() noexcept;
    [[nodiscard]] bool in_transaction() const noexcept { return in_transaction_; }

private:
    relate_connection conn_{nullptr};
    bool in_transaction_{false};
};

}  // namespace clink::relate
