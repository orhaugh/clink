# WebSocket connector

> A source (`websocket_source_string`) for WebSocket push feeds: connect to a `ws://` or `wss://` endpoint, optionally send a subscription message, and emit each received text message as one record.

## Overview

The WebSocket source is clink's on-ramp for push feeds - the delivery
mechanism of most market-data and event APIs (crypto exchange streams,
retail market-data providers, event webhooks with a streaming leg). It
speaks RFC 6455 directly over POSIX sockets: the opening handshake, frame
decoding with fragmented-message reassembly, mandatory ping/pong, and the
close handshake are implemented in-tree (`impls/websocket/include/clink/websocket/ws_protocol.hpp`),
with no client library dependency. Each complete text message is emitted as
one `std::string` record; in SQL, a declared `format='json'` schema bridges
to Row through the columnar JSON decode exactly as a Kafka table does.

There is deliberately no WebSocket sink in v1.

## Delivery semantics, stated plainly

A WebSocket feed is an ephemeral push stream: no offsets, no
acknowledgement, nothing to rewind to. Delivery is therefore
**at-most-once across restarts** - messages that arrive while the job is
down, or between a crash and its last checkpoint, are gone, and the source
persists no cursor because there is none. Within one connection, delivery
is in-order and complete.

The two honest patterns when stronger guarantees are needed:

- **Bridge to a durable log.** This source feeding a Kafka sink, with the
  real processing reading from the topic - recovery and replay then come
  from Kafka's offsets.
- **Pair with the flight recorder.** `clink run feed.sql
  --checkpoint-dir=ckpt --capture-dir=capture` captures what each operator
  consumed, making an unreplayable feed locally replayable and debuggable
  after the fact (`clink replay --verify`, `--emit-test`).

A dropped connection is re-dialled with exponential backoff (capped at
`reconnect_backoff_max_ms`) and the subscription message is sent again; the
gap across the drop is inherent to the transport. Venues commonly
disconnect slow consumers - the source reads only inside `produce()`, so a
stalled downstream propagates as TCP backpressure and may trigger exactly
that; the reconnect path absorbs it.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| OpenSSL (only for `wss://`) | System (Homebrew on macOS, `libssl-dev` on Debian) | Not pinned by clink |

Plain `ws://` needs no external library at all. When CMake finds OpenSSL the
impl compiles with TLS (`wss://` works, with certificate and hostname
verification); when it does not, the impl still builds and a `wss://` URL
fails at `open()` with an error saying the build lacks TLS.

## Enabling it

Gated by `CLINK_WITH_WEBSOCKET` (`AUTO` default / `ON` / `OFF`). Under
`AUTO` the impl always builds - there is no required dependency to miss.

## Factories

| Factory | Kind | Channel |
| --- | --- | --- |
| `websocket_source_string` | source | `string` |

Parallelism: one connection is one stream, so only subtask 0 connects; the
other subtasks are dormant. Run the source at parallelism 1 and fan out
after it with a keyed shuffle.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `url` | required | `ws://host[:port]/path` or `wss://...` |
| `subscribe` | empty | text frame sent after every (re)connect - the venue's subscribe message |
| `read_block_ms` | `500` | how long one `produce()` call waits for data |
| `ping_interval_ms` | `30000` | client keepalive pings; `0` disables |
| `open_timeout_ms` | `10000` | connect + TLS + handshake deadline |
| `max_messages` | `0` | stop after N messages (`0` = unbounded); a capped source is bounded, for demos and tests |
| `reconnect` | `true` | re-dial dropped connections; `false` ends the stream at the first drop or server close |
| `reconnect_backoff_max_ms` | `30000` | exponential backoff cap |
| `tls_verify` | `true` | `wss://` certificate + hostname verification (disable only for self-signed development endpoints) |

Binary messages are counted
(`clink_connector_errors_total{connector="websocket",direction="binary_skipped"}`)
and not emitted: the string channel carries text.

## SQL usage

```sql
CREATE TABLE trades (
    symbol VARCHAR,
    px     DOUBLE,
    qty    BIGINT,
    ts     BIGINT
) WITH (
    connector = 'websocket',
    format    = 'json',
    url       = 'wss://stream.example.test/ws',
    subscribe = '{"op":"subscribe","channel":"trades"}',
    event_time_column = 'ts',
    watermark_lag_ms  = '3000'
);

CREATE TABLE bars (symbol VARCHAR, window_start BIGINT, hi DOUBLE, lo DOUBLE, n BIGINT)
WITH (connector = 'file', format = 'json', path = 'out/bars.ndjson');

INSERT INTO bars
SELECT symbol, window_start, MAX(px) AS hi, MIN(px) AS lo, COUNT(*) AS n
FROM trades
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), symbol;
```

One SQL file, one process (`clink run feed.sql`), live windowed aggregates
over a real feed - no broker to stand up first. A declared multi-column
schema takes the columnar JSON bridge by default, exactly as a Kafka table
does; `columnar_decode='false'` opts a table back to the plain row bridge.

Verified against a real venue: this one-liner pulls five live trades off a
public exchange stream, through TLS, the handshake, the frame decoder and
the JSON bridge, into a file -

```bash
clink run -e "CREATE TABLE trades (e VARCHAR, s VARCHAR, p VARCHAR, q VARCHAR) \
    WITH (connector='websocket', format='json', \
          url='wss://stream.binance.com:9443/ws/btcusdt@trade', max_messages='5'); \
  CREATE TABLE o (e VARCHAR, s VARCHAR, p VARCHAR, q VARCHAR) \
    WITH (connector='file', format='json', path='/tmp/trades.ndjson'); \
  INSERT INTO o SELECT e, s, p, q FROM trades"
```

`max_messages` bounds the run so it ends; drop it for a continuously
running pipeline.

## Example (fluent API)

```cpp
#include <clink/websocket/websocket_source.hpp>

clink::websocket::WebSocketSourceOptions o;
o.url = "wss://stream.example.test/ws";
o.subscribe = R"({"op":"subscribe","channel":"trades"})";
auto source = std::make_shared<clink::websocket::WebSocketSource>(std::move(o));
```

## Testing

`clink_websocket_tests` (ctest label `websocket`) pins the protocol layer to
RFC 6455's own worked examples - the section 1.3 nonce/accept pair and the
section 5.7 masked "Hello" frame, byte for byte - and drives the source
against an in-process WebSocket server: ordered delivery, subscribe-on-
reconnect, ping/pong, fragmented reassembly, server close, binary skip. The
live test is env-gated: set `CLINK_WS_LIVE_URL` (and optionally
`CLINK_WS_LIVE_SUBSCRIBE`) to run one real receive against a feed of your
choosing.
