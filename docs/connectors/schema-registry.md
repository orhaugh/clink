---
title: Schema Registry formats
description: "The Confluent Schema Registry wire format on the Kafka connector: registry-framed Avro, Protobuf and JSON Schema values decoded to and encoded from clink's JSON rows, with the registry client, the type mapping and every option."
---

# Schema Registry formats (Avro, Protobuf, JSON Schema)

> Registry-framed values on the Kafka connector. A `format='avro'`, `'protobuf'` or `'json-schema'` table reads and writes messages in the Confluent Schema Registry wire format, against Confluent Schema Registry or any registry that speaks its REST API (Redpanda, Karapace, Apicurio's compatibility endpoint). It is a value format, not a connector: `connector='kafka'` stays as it is.

## Overview

Most enterprise Kafka estates frame their values for a schema registry: a
magic byte, a four-byte schema id, then Avro binary, Protobuf binary or JSON
text. The `clink::schema_registry` library (`impls/schema_registry/`)
implements that framing, the registry REST client behind it, and the three
value formats. The Kafka connector links it and offers the formats on its
string-channel factories, the ones the SQL planner emits for every Kafka
table.

The design keeps the engine's JSON path intact. A source decodes each
message into one JSON object text, exactly what a `format='json'` table
carries, so the planner's `json_string_to_row_columnar` bridge, projection
pushdown and the columnar decode all apply unchanged. A sink encodes the
`row_to_json_string` bridge's JSON rows as the last step before the
producer. The planner's only job is to keep the Row channel and pass the
format through; everything format-specific is the connector's.

```mermaid
flowchart LR
    T["Kafka topic<br/>registry-framed values"] --> S["kafka_source_string<br/>decode to JSON text"]
    S --> B["json_string_to_row_columnar"]
    B --> Q["SQL operators"]
    Q --> R["row_to_json_string"]
    R --> K["kafka_sink_string<br/>encode + frame"]
    K --> U["Kafka topic"]
    S <-. "GET /schemas/ids/{id}" .-> REG["Schema Registry"]
    K <-. "POST /subjects/{s}/versions" .-> REG
```

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| Apache Avro C++ (`avrocpp`), for `format='avro'` | From-source pin in the Debian image (`scripts/install-connector-deps.sh`, checksum-verified) / brew (macOS, `avro-cpp`) | `1.12.1` (`AVRO_CPP_VERSION` in `scripts/versions.env`) |
| libprotobuf + libprotoc (the compiler library, which parses `.proto` text), for `format='protobuf'` | System package via apt (`libprotobuf-dev`, `libprotoc-dev`) / brew (`protobuf`) | Not pinned by clink (`3.21` in the image, current release on Homebrew; both are exercised) |
| cpp-httplib (the registry client's transport, via `clink::http_connector`) | Vendored | vendored |

`format='json-schema'` needs nothing beyond the library itself.

## Enabling it

The library is built whenever `clink::http_connector` is (`CLINK_WITH_HTTP`,
default `AUTO`, effectively always). Each format is compiled in when its
dependency is found:

- `format='avro'`: `CLINK_WITH_AVRO` (`AUTO`/`ON`/`OFF`), the same knob as the [Avro codec impl](avro.md).
- `format='protobuf'`: `CLINK_WITH_PROTOBUF` (`AUTO`/`ON`/`OFF`). `AUTO` looks for the protobuf CMake config first (Homebrew), then the CMake module (Debian), and needs `protobuf::libprotoc` as well as `protobuf::libprotobuf`.
- `format='json-schema'`: always.

The configure log says which landed: `clink::schema_registry - enabled (avro, json-schema, protobuf)`. A format a build lacks is refused by name at build (deploy) time, not at runtime, and `clink --capabilities-json` lists the compiled-in formats under the Kafka connector's `formats`. The CI image carries all three.

## Wire format

| Bytes | Content |
| --- | --- |
| 0 | magic byte, always `0x00` |
| 1 to 4 | schema id, big-endian `int32` |
| (Protobuf only) | message indexes: a zigzag-varint count, then that many zigzag-varint indexes into the schema's message list and nested-message lists; the common path `[0]` is written as the single byte `0x00` |
| rest | the encoded value: Avro binary, Protobuf binary, or JSON text |

Nested Protobuf types are counted as the descriptor lists them, so a
synthesised map-entry type occupies an index like any other nested message.

## Options

All options are `WITH (...)` options on the Kafka table (or `BuildContext`
parameters on the `kafka_source_string` / `kafka_sink_string` /
`kafka_2pc_sink_string` / `kafka_upsert_sink_string` factories). Secrets
may be given as `env://VAR` like every other connector option.

| Option | Applies to | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `format` | both | Yes | (none) | `avro`, `protobuf` or `json-schema` (`json_schema` and `jsonschema` are accepted spellings). Any other value is the plain JSON or text path. |
| `schema_registry_url` | both | Yes | (none) | `http://host:8081`, `https://registry.example.com`, or with a path prefix such as `https://host/apis/ccompat/v7`. Credentials in the URL's userinfo part (`https://key:secret@host`) are sent as basic auth. |
| `schema_registry_auth` | both | No | (none) | `user:password`, sent as `Authorization: Basic`. |
| `schema_registry_token` | both | No | (none) | A bearer token, used when `schema_registry_auth` is empty. |
| `schema_registry_verify_tls` | both | No | `true` | `false` skips server-certificate verification on an `https` registry. |
| `schema_registry_timeout_ms` | both | No | `30000` | Connect and read timeout for registry calls. |
| `decode_error` | source | No | `fail` | What the source does with a message its format cannot decode (a wrong magic byte, an id the registry does not know, a payload the schema rejects). `fail` stops the job with the topic, partition, offset and reason. `skip` drops the record, logs the first occurrence and every thousandth, and continues. |
| `schema_registry_subject` | sink | No | `<topic>-value` | The subject the sink registers under or reads from (the topic-name strategy). |
| `schema_registry_auto_register` | sink | No | `true` | Derive a schema from the table's declared columns and register it under the subject; the registry returns the existing id when it is already there, so redeploys are idempotent. `false` uses the subject's latest registered version instead and fails at build time if there is none. |
| `schema_registry_record_name` | sink | No | derived from the subject | The Avro record name or Protobuf message name of a derived schema. The default strips a `-value` or `-key` suffix from the subject and replaces every character outside `[A-Za-z0-9_]` with `_`. |
| `schema_registry_namespace` | sink | No | `clink` | The Avro namespace of a derived schema. |
| `schema_registry_message` | sink (Protobuf) | No | the first message | Which message of a registry-held Protobuf schema to write, as `Outer.Inner` for a nested one. The frame carries its index path. |

The sink talks to the registry when it is built (register or read the
subject), so a wrong URL, bad credentials or an incompatible schema fails at
deploy, before the first record. The source is lazy: ids arrive with the
data, each is fetched once and cached for the life of the operator.

## Type mapping

### Decoding (source)

Every message becomes one JSON object keyed by field name.

| Schema type | JSON |
| --- | --- |
| Avro `record`, Protobuf message | object; a non-record top-level Avro value is wrapped as `{"value": ...}` |
| `null`, `boolean`, `int`, `long`, `float`, `double`, `string`; proto3 scalars | the JSON equivalent; `int64` and `uint64` are JSON integers, not strings; `NaN` and infinities become `null` |
| Avro `union` | the value of the branch that was written; `null` for the null branch |
| Avro `enum`, Protobuf `enum` | the symbol name |
| Avro `array`, `repeated` | array |
| Avro `map`, Protobuf `map` | object |
| `bytes`, `fixed` | base64 string |
| Avro `decimal` (bytes or fixed) | a decimal string with the schema's scale, for example `"1234.56"`, which a `DECIMAL(p,s)` column ingests exactly |
| Avro `date` | `"YYYY-MM-DD"` |
| Avro `time-millis` / `time-micros` | `"HH:MM:SS.fff"` / `"HH:MM:SS.ffffff"` |
| Avro `timestamp-millis`, `-micros`, `-nanos` (and the `local-` variants); `google.protobuf.Timestamp` | integer epoch **milliseconds** (micros and nanos are scaled down), the unit clink's event-time functions and `event_time_column` take; declare the column `BIGINT` |
| Avro `uuid` | string |
| Protobuf field with presence (`optional`, message, oneof member) that is unset | omitted; a proto3 scalar without presence is always present with its default |

Protobuf field names are the names declared in the `.proto`, not the
lowerCamelCase JSON names. A schema's `references` (Protobuf imports, Avro
named types held under another subject) are resolved through the registry by
subject and version; `google.protobuf.*` well-known types need no reference.

### Encoding (sink)

The sink's input is the row JSON the `row_to_json_string` bridge produces.
With `schema_registry_auto_register` (the default) the schema is derived
from the declared columns; every derived field is nullable, so a column the
row lacks encodes as null, and the sink refuses nothing a `SELECT` can
produce. Against a registry-held schema the mapping accepts what decoding
emits, plus a few conveniences:

- Keys the schema does not name are dropped (`__row_kind` among them). A field the schema requires and the row lacks is an error that names the field.
- Timestamps take an integer (epoch milliseconds, scaled up to the schema's unit) or an ISO-8601 string (`2024-01-01T00:00:00.123Z`); dates take `YYYY-MM-DD` or a day count; times take `HH:MM:SS.fff` or an integer.
- Decimals take a number or a string and are encoded at exactly the schema's scale from the row's exact digits (the generic parse would round past 17 significant digits); a value with more fractional digits than the scale is refused rather than rounded.
- A JSON integer for an Avro union chooses `long` over `double`, a string chooses `string` over `bytes`; the first branch that accepts the value wins otherwise.
- Protobuf: `int64` fields accept numeric strings, enums accept names or numbers, `bytes` take base64, and a decimal column into a `string` field keeps every digit.

Derived schemas map the declared columns as follows.

| SQL column | Avro | Protobuf (proto3) | JSON Schema |
| --- | --- | --- | --- |
| `BIGINT` | `["null","long"]` | `int64` | `["integer","null"]` |
| `INT` | `["null","int"]` | `int32` | `["integer","null"]` |
| `DOUBLE` | `["null","double"]` | `double` | `["number","null"]` |
| `FLOAT` | `["null","float"]` | `float` | `["number","null"]` |
| `BOOLEAN` | `["null","boolean"]` | `bool` | `["boolean","null"]` |
| `VARCHAR` / `TEXT` (and any other type) | `["null","string"]` | `string` | `["string","null"]` |
| `DECIMAL(p,s)` | `["null",{"type":"bytes","logicalType":"decimal","precision":p,"scale":s}]` | `string` (proto3 has no exact decimal scalar) | `["number","null"]` |
| `FLOAT ARRAY` | `["null",{"type":"array","items":"float"}]` | `repeated float` | `["array","null"]` of `number` |

A JSON Schema sink with a derived schema re-serialises each row with only
the declared columns, in schema order, keeping decimal digits exact; with a
registry-held schema it passes the row through as written.

## SQL usage

```sql
CREATE TABLE orders (
  id      BIGINT,
  name    TEXT,
  amount  DECIMAL(18,2),
  placed  BIGINT              -- an Avro timestamp-millis arrives as epoch ms
) WITH (
  connector           = 'kafka',
  format              = 'avro',
  brokers             = 'broker:9092',
  topic               = 'orders',
  group_id            = 'analytics',
  schema_registry_url = 'https://registry.example.com',
  schema_registry_auth = 'env://SR_AUTH',   -- "key:secret"
  event_time_column   = 'placed',
  watermark_lag_ms    = '5000'
);

CREATE TABLE totals (
  name   TEXT,
  total  DECIMAL(18,2)
) WITH (
  connector           = 'kafka',
  format              = 'protobuf',
  brokers             = 'broker:9092',
  topic               = 'order-totals',
  schema_registry_url = 'https://registry.example.com',
  schema_registry_auth = 'env://SR_AUTH'
);

INSERT INTO totals SELECT name, SUM(amount) FROM orders GROUP BY name;
```

The sink registers a proto3 schema with one message, `order_totals`, under
the subject `order-totals-value` on deploy, and frames every value with the
id it got back. `mode='upsert'` and exactly-once (`kafka_2pc_sink_string`)
sinks take the same options; an upsert sink keeps its JSON-derived primary
key as the message key and frames only the value.

## Programmatic use

`impls/kafka/include/clink/kafka/string_channel.hpp` builds the same source
and sink from a `BuildContext` the way the planner's factories do; the
library's own API is `impls/schema_registry/include/clink/schema_registry/`:
`client.hpp` (the registry client), `wire_format.hpp` (frame and parse),
`formats.hpp` (`make_decoder` / `make_encoder`, `parse_format_options`) and
`schema_derivation.hpp`.

## Delivery semantics

Unchanged from the Kafka connector: the format is applied per record on the
way in and out and carries no state, so the source's offset replay, the
transactional sink's two-phase commit and the upsert sink's keying behave as
documented for [Kafka](kafka.md). A decode failure under `decode_error='fail'`
is a task failure like any other; the job restarts from its last checkpoint
and meets the same record again, which is the point: a poison message is
visible, not silently lost. `skip` is the explicit choice to lose it.

## Limitations

- A Kafka connector option only. Other string-channel connectors (Pulsar, RabbitMQ, NATS) take plain JSON.
- Message keys are not decoded or encoded: the source reads values, the upsert sink writes its JSON primary key as before.
- Avro decimals wider than 16 bytes (precision above 38) are refused; every timestamp logical type is delivered as epoch milliseconds, so sub-millisecond precision is not carried into SQL.
- Derived schemas are flat and fully nullable; there is no evolution beyond what the registry's own compatibility rules allow when a redeploy registers a changed column list. Use `schema_registry_auto_register='false'` to write against a schema you own.
- Protobuf `.proto` text is parsed at runtime with the protobuf compiler library; schemas that depend on custom options or editions features the linked libprotobuf does not know will not parse.
- The subject naming strategy is topic-name (`<topic>-value`) or an explicit subject; record-name strategies are not implemented.

## Testing

Unit suites, label `schema_registry`, with an in-process registry double
(`impls/schema_registry/tests/fake_registry.hpp`, on clink's own HTTP server):
the wire format, the client (caching, credentials, error codes), the
derivations, and each format checked against its reference implementation
(payloads produced by avro-cpp's generic API and by libprotobuf's dynamic
messages, then decoded by clink; clink's output decoded by them). The Kafka
suite (`impls/kafka/tests/test_registry_formats.cpp`) drives the string
factories end to end through librdkafka's mock broker, including the
`decode_error` policies and the build-time refusals. With Docker,
`test_registry_formats_live.cpp` runs the same round trip for all three
formats against a Redpanda broker with its built-in Schema Registry.

```bash
ctest --test-dir build -L schema_registry
ctest --test-dir build -L kafka
```
