// ASYNC-1: the StateBackend async-read surface (get_async / supports_async_get).
//
// The base default makes every backend async-correct for free: get_async
// is `co_return get(op, key)`, completing in a single resume with no
// suspension. A backend whose read can genuinely block overrides it to
// suspend until the I/O completes and copies the key bytes across the
// suspension (the borrowed-view contract). These tests pin both halves:
// the inline default over InMemory, and a suspending DeferredBackend that
// only yields its value after an external release + resume.

#include <chrono>
#include <coroutine>
#include <cstring>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

std::string_view sv(const std::string& s) {
    return std::string_view{s};
}

std::string to_string(const StateBackend::Value& v) {
    std::string out(v.size(), '\0');
    if (!v.empty()) {
        std::memcpy(out.data(), v.data(), v.size());
    }
    return out;
}

TtlConfig short_ttl(bool refresh_on_read = false) {
    TtlConfig c;
    c.ttl = std::chrono::milliseconds(100);
    c.refresh_on_write = true;
    c.refresh_on_read = refresh_on_read;
    return c;
}

// Count the raw rows physically stored under `op` for a slot (does NOT
// skip TTL-expired entries, unlike KeyedState::scan), so a test can prove
// the lazy-purge erase actually fired.
int raw_rows(const StateBackend& backend, OperatorId op, std::string_view slot) {
    int n = 0;
    backend.scan(op, [&](StateBackend::KeyView k, StateBackend::ValueView) {
        if (k.find(slot) != std::string_view::npos) {
            ++n;
        }
    });
    return n;
}

// A backend that DEFERS every read: get_async suspends at a single
// suspension point and resolves the value (against a composed InMemory
// store) only after the test calls release() and the parked coroutine is
// resumed. It copies the key bytes into the coroutine frame before
// suspending, so the source key buffer may be mutated or destroyed while
// the read is outstanding - exactly the contract a real remote backend
// must honour. (InMemoryStateBackend is final, so it is composed, not
// inherited; the sync surface just forwards.)
class DeferredBackend : public StateBackend {
public:
    void put(OperatorId op, KeyView key, ValueView value) override { store_.put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return store_.get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { store_.erase(op, key); }
    void scan(OperatorId op, const ScanVisitor& visit) const override { store_.scan(op, visit); }
    Snapshot snapshot(CheckpointId id) override { return store_.snapshot(id); }
    void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) override {
        store_.restore(snap, kg_filter);
    }
    std::string description() const override { return "deferred"; }

    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }

    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        std::string owned_key(key);           // own the bytes across the suspension
        co_await SuspendOnce{this};           // parks until release()
        co_return store_.get(op, owned_key);  // resolve against the real store
    }

    // Test driver.
    [[nodiscard]] bool parked() const noexcept { return static_cast<bool>(parked_); }
    void release() {
        if (parked_) {
            auto h = parked_;
            parked_ = {};
            h.resume();
        }
    }

private:
    struct SuspendOnce {
        const DeferredBackend* self;
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) const noexcept { self->parked_ = h; }
        void await_resume() const noexcept {}
    };

    InMemoryStateBackend store_;
    mutable std::coroutine_handle<> parked_{};
};

}  // namespace

// The base default does not opt into the async-read path: the runtime must
// keep calling get() inline for backends that have not overridden it.
TEST(AsyncStateGet, BaseDefaultDoesNotSupportAsyncGet) {
    InMemoryStateBackend backend;
    EXPECT_FALSE(backend.supports_async_get());
}

// Base default over InMemory: a present key resolves to the same value as
// sync get(), completing in exactly one resume (no suspension).
TEST(AsyncStateGet, BaseDefaultPresentKeyMatchesSyncGetInOneResume) {
    InMemoryStateBackend backend;
    OperatorId op{7};
    backend.put(op, sv(std::string{"k"}), sv(std::string{"v"}));

    // get_async borrows the key as a view; its backing must outlive the first
    // resume (the lazy task does not read the key until then). A temporary
    // would dangle - keep it in a named local.
    const std::string k = "k";
    auto task = backend.get_async(op, sv(k));
    EXPECT_FALSE(task.done());  // lazy: not started until resumed
    task.resume();
    EXPECT_TRUE(task.done());  // single synchronous step

    auto got = task.get();
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(to_string(*got), "v");
}

// Base default over InMemory: an absent key resolves to nullopt, matching
// sync get().
TEST(AsyncStateGet, BaseDefaultAbsentKeyYieldsNullopt) {
    InMemoryStateBackend backend;
    OperatorId op{7};

    const std::string missing = "missing";  // keep the borrowed key alive past resume
    auto task = backend.get_async(op, sv(missing));
    task.resume();
    ASSERT_TRUE(task.done());
    EXPECT_FALSE(task.get().has_value());
}

// A genuinely-async backend: the value is available only after the
// external release + resume, never before.
TEST(AsyncStateGet, DeferredBackendYieldsValueOnlyAfterRelease) {
    DeferredBackend backend;
    EXPECT_TRUE(backend.supports_async_get());
    OperatorId op{9};
    backend.put(op, sv(std::string{"k"}), sv(std::string{"v"}));

    const std::string k = "k";  // keep the borrowed key alive past the first resume
    auto task = backend.get_async(op, sv(k));
    EXPECT_FALSE(backend.parked());  // lazy: nothing started yet

    task.resume();  // runs to the suspension point and parks
    EXPECT_FALSE(task.done());
    EXPECT_TRUE(backend.parked());

    backend.release();  // resumes the parked read to completion
    EXPECT_TRUE(task.done());

    auto got = task.get();
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(to_string(*got), "v");
}

// The borrowed-view contract: a deferring backend owns the key bytes, so
// mutating the caller's source buffer while the read is outstanding does
// not corrupt the result.
TEST(AsyncStateGet, DeferredBackendOwnsKeyAcrossSuspension) {
    DeferredBackend backend;
    OperatorId op{9};
    backend.put(op, sv(std::string{"k"}), sv(std::string{"v"}));

    std::string key = "k";
    auto task = backend.get_async(op, sv(key));
    task.resume();  // parks; key bytes already copied
    key[0] = 'X';   // corrupt the source buffer mid-flight

    backend.release();
    ASSERT_TRUE(task.done());
    auto got = task.get();
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(to_string(*got), "v");  // still resolved the original "k"
}

// ---- ASYNC-2: KeyedState::get_async typed view --------------------------

// The typed async accessor yields the same decoded optional<V> as sync
// get() for present and absent keys, completing in a single resume over
// the inline base-default backend (symmetric transfer chains the inner
// backend get_async into the same step).
TEST(KeyedStateGetAsync, MatchesSyncGetForPresentAndAbsent) {
    InMemoryStateBackend backend;
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{2}, "counts", string_codec(), int64_codec());
    kv.put("a", 42);

    auto t = kv.get_async(std::string{"a"});
    t.resume();
    ASSERT_TRUE(t.done());
    auto got = t.get();
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, 42);
    EXPECT_EQ(kv.get("a"), got);  // identical to the sync twin

    auto t2 = kv.get_async(std::string{"missing"});
    t2.resume();
    ASSERT_TRUE(t2.done());
    EXPECT_FALSE(t2.get().has_value());
}

// A TTL-expired entry resolves to nullopt AND issues the lazy-purge erase
// from the async read path (proven by the raw row vanishing, distinct from
// sync get()'s own purge).
TEST(KeyedStateGetAsync, TtlExpiredReturnsNulloptAndPurges) {
    InMemoryStateBackend backend;
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{3}, "ttl", string_codec(), int64_codec(), short_ttl());
    kv.put("alice", 1);
    std::this_thread::sleep_for(150ms);  // cross the 100ms expiry

    EXPECT_EQ(raw_rows(backend, OperatorId{3}, "ttl"), 1);  // still physically present

    auto t = kv.get_async(std::string{"alice"});
    t.resume();
    ASSERT_TRUE(t.done());
    EXPECT_FALSE(t.get().has_value());
    EXPECT_EQ(raw_rows(backend, OperatorId{3}, "ttl"), 0)
        << "get_async must lazy-purge the expired row";
}

// refresh_on_read: an async read of a live entry re-puts it with an
// advanced expiry, so it survives past the original TTL window.
TEST(KeyedStateGetAsync, RefreshOnReadAdvancesExpiry) {
    InMemoryStateBackend backend;
    KeyedState<std::string, std::int64_t> kv(backend,
                                             OperatorId{4},
                                             "ttl",
                                             string_codec(),
                                             int64_codec(),
                                             short_ttl(/*refresh_on_read=*/true));
    kv.put("k", 9);

    std::this_thread::sleep_for(70ms);  // not yet expired
    auto t = kv.get_async(std::string{"k"});
    t.resume();
    ASSERT_TRUE(t.done());
    ASSERT_TRUE(t.get().has_value());  // present, and re-put with a fresh expiry

    std::this_thread::sleep_for(70ms);  // 140ms since put, 70ms since refresh -> alive
    auto t2 = kv.get_async(std::string{"k"});
    t2.resume();
    ASSERT_TRUE(t2.done());
    EXPECT_TRUE(t2.get().has_value())
        << "refresh_on_read via get_async should have advanced the expiry";
}

// The key is taken by value, so it survives into the coroutine frame even
// when the caller passes a temporary and the read suspends before the body
// encodes it. ASan would flag a reference-capture regression here.
TEST(KeyedStateGetAsync, OwnsKeyAcrossSuspensionOverDeferredBackend) {
    DeferredBackend backend;
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{5}, "counts", string_codec(), int64_codec());
    kv.put("k", 7);

    auto t = kv.get_async(std::string{"k"});  // temporary key argument
    t.resume();                               // parks inside the backend read
    EXPECT_FALSE(t.done());
    EXPECT_TRUE(backend.parked());

    backend.release();
    ASSERT_TRUE(t.done());
    auto got = t.get();
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, 7);
}
