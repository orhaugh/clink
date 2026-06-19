// Unit tests for clink::sql::AsyncFunctionRegistry (Phase 28c runtime
// slice). The registry is the seam between the SQL frontend (which
// users programmatically populate today; binder lowering of an async
// UDF call is a follow-on) and the async_lookup_row runtime
// operator's factory.

#include <atomic>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/async/task.hpp"
#include "clink/sql/async_function_registry.hpp"

using clink::async::Task;
using clink::sql::AsyncFunctionRegistry;
using clink::sql::AsyncLookupFn;
using clink::sql::Row;

namespace {

AsyncLookupFn identity_fn() {
    return [](const Row& r) -> Task<Row> { co_return r; };
}

AsyncLookupFn stamping_fn(const std::string& key, const std::string& value) {
    return [key, value](const Row& r) -> Task<Row> {
        Row out = r;
        out.values[key] = clink::config::JsonValue{value};
        co_return out;
    };
}

}  // namespace

TEST(AsyncFunctionRegistry, EmptyByDefault) {
    AsyncFunctionRegistry reg;
    EXPECT_EQ(reg.size(), 0u);
    EXPECT_FALSE(reg.contains("anything"));
    EXPECT_FALSE(static_cast<bool>(reg.lookup("anything")));
    EXPECT_TRUE(reg.names().empty());
}

TEST(AsyncFunctionRegistry, RegisterAndLookup) {
    AsyncFunctionRegistry reg;
    reg.register_function("identity", identity_fn());
    EXPECT_TRUE(reg.contains("identity"));
    EXPECT_EQ(reg.size(), 1u);

    auto fn = reg.lookup("identity");
    ASSERT_TRUE(static_cast<bool>(fn));

    Row r;
    r.values["x"] = clink::config::JsonValue{static_cast<std::int64_t>(42)};
    auto t = fn(r);
    t.resume();
    ASSERT_TRUE(t.done());
    auto result = t.get();
    EXPECT_EQ(result.get_string("x"), std::optional<std::string>{"42"});
}

TEST(AsyncFunctionRegistry, RegisterReplacesExistingByName) {
    AsyncFunctionRegistry reg;
    reg.register_function("stamper", stamping_fn("source", "v1"));
    reg.register_function("stamper", stamping_fn("source", "v2"));
    EXPECT_EQ(reg.size(), 1u);

    auto fn = reg.lookup("stamper");
    Row r;
    auto t = fn(r);
    t.resume();
    auto out = t.get();
    EXPECT_EQ(out.get_string("source"), std::optional<std::string>{"v2"})
        << "second registration must replace the first";
}

TEST(AsyncFunctionRegistry, RejectsNullFunction) {
    AsyncFunctionRegistry reg;
    EXPECT_THROW(reg.register_function("bad", AsyncLookupFn{}), std::invalid_argument);
    EXPECT_FALSE(reg.contains("bad"));
}

TEST(AsyncFunctionRegistry, NamesReturnsSortedSnapshot) {
    AsyncFunctionRegistry reg;
    reg.register_function("zeta", identity_fn());
    reg.register_function("alpha", identity_fn());
    reg.register_function("mu", identity_fn());

    auto names = reg.names();
    EXPECT_EQ(names, (std::vector<std::string>{"alpha", "mu", "zeta"}));
}

TEST(AsyncFunctionRegistry, GlobalIsSingleton) {
    auto& a = AsyncFunctionRegistry::global();
    auto& b = AsyncFunctionRegistry::global();
    EXPECT_EQ(&a, &b);
}

TEST(AsyncFunctionRegistry, RegisteredFnRunsThroughAsyncLookupOperatorTaskAPI) {
    // The registry's stored AsyncLookupFn is shape-compatible with
    // what AsyncLookupOperator<Row, Row> consumes. Verify the
    // round-trip by simulating the operator's drive loop without
    // building a full Dag.
    AsyncFunctionRegistry reg;
    reg.register_function("greet", [](const Row& r) -> Task<Row> {
        Row out = r;
        const auto who = r.get_string("name").value_or(std::string{"world"});
        out.values["greeting"] = clink::config::JsonValue{"hello, " + who};
        co_return out;
    });

    auto fn = reg.lookup("greet");
    ASSERT_TRUE(static_cast<bool>(fn));

    Row in;
    in.values["name"] = clink::config::JsonValue{std::string{"alice"}};
    auto t = fn(in);
    EXPECT_FALSE(t.done()) << "Task is lazy until resumed";
    t.resume();
    ASSERT_TRUE(t.done());
    auto out = t.get();
    EXPECT_EQ(out.get_string("greeting"), std::optional<std::string>{"hello, alice"});
}
