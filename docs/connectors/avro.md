# Apache Avro (serialization)

> An Avro serialization codec for typed records, usable with any clink source or sink. It is an encoding, not a source or sink in its own right.

## Overview
The `clink::avro` impl ships only `Codec<T>` templates: there are no built-in Avro sources or sinks. The codecs encode and decode `avrogencpp`-generated record structs (any type for which `avro::codec_traits<T>` is specialised) to and from Avro's binary and JSON text wire formats. You compose them with an existing connector, for example registering an Avro codec for a record type and then reading or writing that type through a Kafka, file, or state-backend channel. Three codecs are provided: binary, JSON text, and a keyed-record wrapper that pairs a UTF-8 partition key with an Avro-binary payload.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| Avro C++ (`avrocpp` / AvroCpp) | System package via apt (Debian, e.g. `libavrocpp-dev`) / brew (macOS, `avro-cpp`) | Not pinned by clink |

The codecs depend only on the Avro C++ library and `clink::core`. They do not use Arrow.

## Enabling it
The impl is gated by the CMake cache variable `CLINK_WITH_AVRO`, which defaults to `AUTO`.

- `AUTO`: probes for Avro C++ via `find_package(AvroCpp CONFIG)`, falling back to a direct search for the `avro/Specific.hh` header and the `avrocpp` library under `/opt/homebrew` and `/usr/local`. If found, the target is built; if not, it is skipped quietly.
- `ON`: requires Avro C++; configuration fails with `CLINK_WITH_AVRO=ON but AvroCpp not found` if it is absent.
- `OFF`: the target is not defined.

Avro C++ is not pre-installed in the base build environment, so under `AUTO` the impl is built only where the library is present (install it via apt or brew first). When built, the target defines `CLINK_HAS_AVRO` and links `AvroCpp::AvroCpp`.

```bash
cmake -S . -B build -DCLINK_WITH_AVRO=ON
```

## Factories
None. The impl registers no sources or sinks. `clink::avro::install()` exists for API parity with the other impls but is a no-op, and `clink::plugin::install_defaults` does not call it. The codecs are header-only templates used directly from C++, not looked up by factory name.

| Factory name | Direction | Record type |
| --- | --- | --- |
| (none) | n/a | n/a |

## Configuration
There is no Options struct and no `BuildContext` parameter parsing. The codecs are constructed directly in C++:

| Codec | Argument | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `binary_codec<T>()` | none | n/a | n/a | Avro binary encoder/decoder for `T`, raw bytes with no framing. |
| `json_codec<T>(schema_path)` | `schema_path` (`std::string`) | Yes | none | Avro JSON-text codec for `T`. The `.avsc` schema file is read once at construction via `compileJsonSchemaFromFile`. |
| `keyed_record_codec<T>()` | none | n/a | n/a | Codec for `KeyedRecord<T>`, a `{ std::string key; T payload; }` pair. Wire shape: 4-byte little-endian key length, key UTF-8 bytes, 4-byte little-endian payload length, Avro-binary payload. |

`T` must be an `avrogencpp`-generated struct, or any type with an `avro::codec_traits<T>` specialisation. There are no authentication options; this is a serialization layer only.

## SQL usage
Not exposed through the SQL frontend; use the programmatic API. There is no `connector='avro'` mapping in `src/sql/physical_plan.cpp`.

## Example
Based on `impls/avro/tests/test_codec.cpp`. For a record `Greeting { long id; string message; }` (an `avrogencpp`-generated struct, or a hand-written struct with an `avro::codec_traits` specialisation):

```cpp
#include "clink/avro/binary_codec.hpp"
#include "clink/avro/json_codec.hpp"
#include "clink/avro/keyed_record_codec.hpp"

// Binary codec: raw Avro bytes, no framing.
clink::Codec<Greeting> bin = clink::avro::binary_codec<Greeting>();
Greeting g{42, "hello"};
auto bytes = bin.encode(g);
std::optional<Greeting> back = bin.decode(bytes);  // decode failure -> std::nullopt

// JSON text codec: schema file read once at construction.
clink::Codec<Greeting> json = clink::avro::json_codec<Greeting>("greeting.avsc");

// Keyed-record codec: partition key + Avro-binary payload in one buffer.
clink::Codec<clink::avro::KeyedRecord<Greeting>> keyed =
    clink::avro::keyed_record_codec<Greeting>();
clink::avro::KeyedRecord<Greeting> kr{.key = "customer-42", .payload = g};
auto kbytes = keyed.encode(kr);
```

Compose any of these with a typed source or sink by registering the codec for the record type and using that connector's typed fluent helpers (for example a Kafka message source carrying `Greeting`).

## Delivery semantics
Not applicable. This impl provides no source or sink, so it carries no delivery guarantee. Any at-least-once, exactly-once, or replay behaviour comes from the connector the codec is composed with, not from the Avro layer. A failed decode (malformed bytes, schema mismatch, or a truncated `KeyedRecord` buffer) returns `std::nullopt` and increments the `avro` connector error metric rather than throwing.

## Limitations
- Codec-only: no built-in Avro source or sink. The codecs must be composed with another connector or a state backend.
- `T` must be `avrogencpp`-generated or otherwise have an `avro::codec_traits<T>` specialisation; arbitrary structs are not supported automatically.
- `binary_codec` emits raw Avro bytes with no length framing. To concatenate multiple records in one buffer, compose with a framing codec such as `vector_codec` or `pair_codec` from core.
- `json_codec` reads its schema file once at construction; the file path must be present and readable at that point. A schema/record mismatch surfaces as a decode failure (`std::nullopt`) at runtime, not at construction.
- `keyed_record_codec` keys are UTF-8 byte strings; both key and payload lengths are encoded as 4-byte little-endian, capping each at 2^32 - 1 bytes.

## Testing
There is no env-gated live or integration test, and no Docker image is required: the codecs encode in memory and need no external broker or service. The in-process round-trip suite is `impls/avro/tests/test_codec.cpp` (binary, JSON, and keyed-record round trips, plus empty-field and truncated-buffer cases), built when `CLINK_BUILD_TESTS` is on and Avro C++ is present. Run it with:

```bash
ctest --test-dir build -L avro
```
