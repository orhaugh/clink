# Declared types: fields, the derived codec, and shape fingerprints

One field-list declaration per user type, and everything the engine needs
derives from it (design record
[009](../design/009-one-declaration-per-type.md)):

```cpp
struct Trade {
    std::int64_t id{};
    std::string symbol;
    double price{};
};
CLINK_FIELDS(Trade, id, symbol, price);
```

That one line yields, with no further code:

| Derived | Where |
| --- | --- |
| Arrow schema + typed columnar batcher | `make_columnar_arrow_batcher<T>()`, auto-selected by the plain registrations ([columnar execution](columnar-execution.md)) |
| Byte codec for state values and the row wire | `derived_codec<T>()`, `include/clink/core/derived_codec.hpp` |
| Registration defaults | `TypeRegistry::register_typed<T>()` / `PluginRegistry::register_type<T>()`: channel name = the declared type name |
| Shape fingerprint gating restores | `fields_fingerprint_v<T>`, checked where `RuntimeContext::keyed_state` binds |

The declaration is the **adapter** form: it annotates an existing struct
rather than defining one, so it works on types the codebase already has.
It lives at namespace scope (a namespaced type is invoked as
`CLINK_FIELDS(ns::Trade, ...)` from the global scope, and registers under
the name `"ns::Trade"`). `CLINK_ARROW_FIELDS` is the historical spelling,
kept as a deprecated alias. The trait the macro populates
(`ArrowFields<T>::descriptors()` in `include/clink/core/fields.hpp`,
Arrow-free) is the stable seam: when C++26 static reflection is usable on
the toolchains, a reflecting frontend populates the same trait and the
macro becomes optional, with nothing downstream changing.

## The field universe

Leaves: fixed-width integers (8/16/32/64, signed and unsigned), `float`,
`double`, `bool`, `std::string`. Composites: `std::optional<E>` (the only
nullable), `std::vector<E>`, `std::map<K, V>`, and nested described
structs, at arbitrary depth. `std::vector<bool>` is refused at compile
time in both the codec and the columnar path (bit-packed proxy
container); store `std::vector<std::uint8_t>`.

## The completeness guard

A field missing from the list would be silently absent from the wire,
from every snapshot and from every Parquet file, so for aggregate types
`CLINK_FIELDS` statically asserts the list names every member (a
brace-initialisability arity probe; no reflection needed). The probe
counts a base subobject as one member, so a type with base classes may
trip it conservatively: `CLINK_FIELDS_SUBSET` is the explicit opt-out,
for that case and for a deliberate partial description.

## The derived codec is a durability contract

The layout is specified in `derived_codec.hpp`, matches the idiom the
hand-written codecs already used (little-endian fixed-width, u32-length
strings), and is pinned by a frozen-bytes fixture
(`tests/fixtures/derived-codec-v1.bin`) and an inventory row in
[protocol and format compatibility](protocol-compatibility.md). Fields
encode in DECLARED ORDER: reordering the macro's arguments rewrites the
bytes, which is exactly what the fingerprint gate below exists to catch.
Strictness is deliberate where the hand idiom was tolerant: decode
consumes its buffer exactly, a bool or presence byte above one fails
closed, an over-u32 length throws at encode instead of wrapping, and
container reserves are bounded by the bytes remaining rather than the
untrusted count.

## The shape fingerprint and the restore gate

`SchemaVersionTrait<T>` is a hand-bumped integer, so the failure mode it
cannot catch is the bump nobody made. The declaration therefore also
yields `fields_fingerprint_v<T>`: a compile-time FNV-1a64 over the
ordered (field name, wire kind) sequence, recursive into composites,
over a frozen kind-tag table (`fields.hpp`; the absolute values are
pinned by `tests/fixtures/state-fingerprints-v1.txt`).

Fingerprints ride snapshots under the `clink.state_fingerprints`
metadata key, beside `clink.state_versions` - an additive metadata key,
which the [snapshot format contract](state-snapshot-format.md) defines
as a compatible change. They are stamped when `keyed_state<K, V>` binds
a described `V`, and checked at the same place after a restore:

- stored fingerprint == live fingerprint: normal operation.
- stored differs from live: **the bind refuses**, naming the slot, both
  fingerprints, and the remedy (bump `SchemaVersionTrait<T>` and
  register a migration).
- no stored fingerprint - an older snapshot, an undescribed type, or a
  backend that does not store them: no gate, exactly the
  pre-fingerprint behaviour. Nothing existing is ever refused.

The declared-evolution path passes by construction rather than by
configuration: `migrate_restored_state` clears the migrated slots'
stamps as it rewrites their values, so after a legitimate bump +
migration the bind sees no stale stamp and re-stamps the new shape.
There is deliberately no override flag; migrations are the path.

Backends: the in-memory backend and the paths layered on it - the
file-backed, sharded, changelog and coalescing wrappers - store and
persist fingerprints (the file-backed and sharded wiring landed 2026-09;
before that the file-backed path dropped both the version and fingerprint
stamps). RocksDB persists them through the same reserved default-CF key
channel as its version stamps, so they ride every checkpoint and both
Arrow exports. ForSt inherits the base no-op hooks for now, so its
snapshots carry no fingerprints and get no gate - never a false refusal -
until its metadata channel is wired.

## Worked example

[`docs/consumer-examples/11_declared_types.cpp`](../consumer-examples/11_declared_types.cpp)
runs the whole surface: one declaration, the derived codec, keyed state
through a snapshot and restore, and the gate refusing an undeclared
field reorder. `examples/heavy_pipeline_job.cpp` is the migration
exhibit: its two hand-written codecs (48 lines) became two `CLINK_FIELDS`
lines, byte-identically (proven in `tests/test_derived_codec.cpp`).
