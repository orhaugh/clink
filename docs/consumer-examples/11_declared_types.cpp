// 11 - One declaration per type (design record 009).
//
// A user type used to need five statements of one fact: the struct, a
// hand-written Codec<T>, a registration with a channel name, an Arrow
// field description, and a schema-version stamp. This example is the
// whole surface after the change: ONE field-list declaration, and the
// byte codec, the state path, the restore, and the shape gate all derive
// from it.
//
// Self-checking: returns non-zero on any failed expectation, so it
// registers with CTest.

#include <cstdint>
#include <iostream>
#include <optional>
#include <string>

#include <clink/core/codec.hpp>
#include <clink/core/derived_codec.hpp>
#include <clink/core/fields.hpp>
#include <clink/runtime/runtime_context.hpp>
#include <clink/state/in_memory_state_backend.hpp>

// The one declaration. At namespace scope; a namespaced type is invoked
// as CLINK_FIELDS(ns::Type, ...). The macro also asserts the list names
// EVERY member of the aggregate, so a forgotten field is a compile
// error, not a silently short wire.
struct Position {
    std::int64_t account{};
    std::string symbol;
    std::int64_t qty{};
    double avg_price{};
};
CLINK_FIELDS(Position, account, symbol, qty, avg_price);

// The same fields declared in a different order: a DIFFERENT shape. Used
// below to show the restore gate doing its job.
struct PositionReordered {
    std::string symbol;
    std::int64_t account{};
    std::int64_t qty{};
    double avg_price{};
};
CLINK_FIELDS(PositionReordered, symbol, account, qty, avg_price);

namespace {

int failures = 0;
void expect(bool ok, const char* what) {
    if (!ok) {
        std::cerr << "FAILED: " << what << '\n';
        ++failures;
    }
}

}  // namespace

int main() {
    // 1. The derived codec: encode/decode with no codec code written.
    const auto codec = clink::derived_codec<Position>();
    const Position p{.account = 7, .symbol = "CLNK", .qty = 100, .avg_price = 12.5};
    const auto bytes = codec.encode(p);
    const auto back = codec.decode(bytes);
    expect(back.has_value(), "derived codec round-trips");
    expect(back && back->symbol == "CLNK" && back->qty == 100, "decoded fields match");

    // 2. Keyed state through the same declaration, then a snapshot and a
    // restore - the shape fingerprint is stamped and carried invisibly.
    clink::InMemoryStateBackend backend;
    clink::RuntimeContext ctx(clink::OperatorId{1}, "positions", &backend, nullptr);
    {
        auto state = ctx.keyed_state<std::int64_t, Position>(
            "book", clink::int64_codec(), clink::derived_codec<Position>());
        state.put(7, p);
    }
    const auto snap = backend.snapshot(clink::CheckpointId{1});

    clink::InMemoryStateBackend restored;
    restored.restore(snap);
    clink::RuntimeContext ctx2(clink::OperatorId{1}, "positions", &restored, nullptr);
    {
        auto state = ctx2.keyed_state<std::int64_t, Position>(
            "book", clink::int64_codec(), clink::derived_codec<Position>());
        const auto got = state.get(7);
        expect(got.has_value() && got->avg_price == 12.5, "state survives snapshot + restore");
    }

    // 3. The gate: binding a REORDERED shape against the restored bytes,
    // with no declared schema-version bump, must refuse - every value
    // would otherwise be misread. The message names the remedy.
    bool refused = false;
    try {
        auto state = ctx2.keyed_state<std::int64_t, PositionReordered>(
            "book", clink::int64_codec(), clink::derived_codec<PositionReordered>());
        (void)state;
    } catch (const std::runtime_error& e) {
        refused = true;
        std::cout << "gate refused, as it must:\n  " << e.what() << '\n';
    }
    expect(refused, "an undeclared field reorder is refused at bind");

    if (failures == 0) {
        std::cout << "11_declared_types: OK\n";
        return 0;
    }
    return 1;
}
