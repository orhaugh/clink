# clink

[![ci](https://github.com/orhaugh/clink/actions/workflows/ci.yml/badge.svg)](https://github.com/orhaugh/clink/actions/workflows/ci.yml)
[![docs](https://github.com/orhaugh/clink/actions/workflows/docs.yml/badge.svg)](https://orhaugh.github.io/clink/)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE)
[![changelog](https://img.shields.io/badge/changelog-v0.8.0-lightgrey.svg)](CHANGELOG.md)

`clink` is an embedded-first, Arrow-native stream processing engine in
modern C++ (C++23): stateful stream processing with engine-grade
semantics - SQL, event time, keyed state, exactly-once checkpoints -
that you run like a tool rather than operate like a platform.

The whole engine lives in one library, and the same pipeline runs two
ways. In-process: `clink run pipeline.sql` executes it in a single
process with no daemons and prints its first result about 155 ms after
process start; `libclink` embeds the engine in any service behind a
pure-C ABI with results as Arrow C streams; `pyclink` returns them as
pyarrow tables in a notebook. At scale: the same SQL file, unchanged,
submits to a distributed Coordinator/Worker cluster with parallelism,
failover, and rescale.

Three capabilities follow from that design:

- **State is an open dataset.** Snapshots are documented Arrow IPC:
  checkpoints and savepoints open directly in pyarrow, DuckDB or Polars,
  export to Parquet and Iceberg, and a running job's live state serves
  point lookups and Arrow scans over plain HTTP, no sink round-trip.
  See [state and backends](https://orhaugh.github.io/clink/internals/state-and-backends/).
- **Incidents replay deterministically.** A flight recorder captures
  what each operator consumed per checkpoint epoch; `clink replay`
  re-executes an operator over exactly those records, offline and
  byte-identically, and can freeze the incident into a permanent
  regression test. See [replay determinism](https://orhaugh.github.io/clink/internals/replay-determinism/).
- **Nothing to manage under it.** A single static binary with no managed
  runtime: cold start in milliseconds, one artefact to ship, and the
  same behaviour embedded, in CI, and on a cluster.

Measured, not asserted: across the 17-query nexmark suite on a five-node
cluster, clink processes an event for **1.9x to 5.3x less CPU** (median 2.45x)
than a JVM stream processor producing identical, correctness-gated output.
Method, caveats and raw per-run data:
[Benchmarks](https://orhaugh.github.io/clink/benchmarks/), priced out in
instances, dollars and modelled CO2e at
[Cost and environmental footprint](https://orhaugh.github.io/clink/efficiency/).

clink is heavily inspired by Apache Flink. Flink's model of typed operator
DAGs, event-time processing, in-band watermarks and checkpoint barriers, keyed
state, and exactly-once semantics is the conceptual foundation this engine
builds on. clink reworks that model in modern C++ around Arrow-native columnar
execution and JVM-free deployment.

## Try it in one command

Nothing to clone, install or configure:

```bash
docker run --rm ghcr.io/orhaugh/clink-runtime:latest run /opt/clink/examples/sql/hello.sql
```

The pipeline and the data it reads both ship inside the image (amd64 and
arm64). It groups a small stream of device readings by region and prints
the result:

```
{"avg_reading":21.4,"peak":21.4,"readings":1,"region":"emea"}
{"avg_reading":22.65,"peak":23.9,"readings":2,"region":"emea"}
{"avg_reading":19.2,"peak":19.2,"readings":1,"region":"amer"}
...
```

Those rows are the point. A batch engine prints one line per region at
the end; clink prints a line per *update*, because the aggregate is a
stream and you are watching it change. Run your own file the same way by
mounting it:

```bash
docker run --rm -v "$PWD:/work" ghcr.io/orhaugh/clink-runtime:latest run /work/mine.sql
```

## Build from source

clink builds against a pinned Apache Arrow toolchain in `~/.clink-deps`.
The bootstrap step downloads a prebuilt, checksum-pinned archive when one
exists for your platform (macOS arm64, Linux x86_64/arm64; about a
minute) and compiles it from source otherwise:

```bash
git clone https://github.com/orhaugh/clink && cd clink
scripts/build-arrow.sh && scripts/build-iceberg-cpp.sh   # one-time, cached
cmake -S . -B build && cmake --build build --parallel 10
ctest --test-dir build --parallel 8                      # optional
```

Then run a first pipeline - one process, no daemons:

```bash
printf '{"usr":"alice","amount":12}\n{"usr":"bob","amount":7}\n{"usr":"alice","amount":5}\n' > /tmp/orders.ndjson

./build/clink run -e "CREATE TABLE orders (usr VARCHAR, amount BIGINT) \
      WITH (connector='file', format='json', path='/tmp/orders.ndjson'); \
    SELECT usr, SUM(amount) AS total FROM orders GROUP BY usr"
```

Supported platforms: macOS (Apple Silicon is the primary development
platform) and Linux (Debian-family, exercised in CI). Windows is not
supported. `./build_and_test.sh` is the reproducible CI-matching path,
including the sanitizer matrix; [CONTRIBUTING.md](CONTRIBUTING.md) has
the details.

## One engine, two execution models

**Embedded.** `clink run pipeline.sql` runs the whole engine in one
process: SQL frontend, operators, state backends, checkpointing,
connectors. First result in about 155 ms from process start, a figure
gated by a release test so it cannot silently regress. The same engine
embeds behind a pure-C ABI ([libclink](https://orhaugh.github.io/clink/internals/embedded/)),
from Python ([pyclink](python/README.md)), and over
[Arrow Flight SQL](https://orhaugh.github.io/clink/internals/embedded/)
for any ADBC/JDBC client.

**Distributed.** The same SQL file or compiled job plugin submits to a
Coordinator/Worker cluster: parallel subtasks, hash-partitioned keyed
shuffles, exactly-once checkpoints, failover from the last completed
checkpoint, hot per-operator rescale, and rolling upgrades through
savepoints. A [Helm chart and Kubernetes operator](deploy/helm/clink)
ship in-tree.

```bash
clink_node --role=coordinator --rpc-port=6123 &
clink_node --role=worker --coordinator-host=127.0.0.1 --coordinator-port=6123 &
clink run pipeline.sql --coordinator-host=127.0.0.1 --coordinator-port=8081
```

Typed C++ pipelines (the fluent `Pipeline` / `DataStream<T>` API, keyed
process functions, windows, joins, CEP) are documented with runnable
programs under [docs/consumer-examples/](docs/consumer-examples/).

## Production qualification

Benchmarks say how fast an engine is; they say nothing about whether its
guarantees hold when processes die at the worst possible instant. clink
runs a standing qualification programme: long campaigns on disposable
multi-host rigs with faults injected into the narrowest windows of the
engine's own protocols, judged by an independent oracle, published only
when green and with the evidence retained. A few of the published
results:

| Qualified | Result |
|---|---|
| [Kafka exactly-once](https://orhaugh.github.io/clink/qualification/qual-01-kafka-exactly-once/) | 755/755 windows byte-exact after two hours of kills inside commit windows, coordinator SIGKILLs, broker outages and partitions |
| [Large keyed state](https://orhaugh.github.io/clink/qualification/qual-04-large-keyed-state/) | 29 GiB of keyed state on a disaggregated backend, every key correct under the same fault battery |
| [Wide job graphs](https://orhaugh.github.io/clink/qualification/qual-06-dag-scaling/) | 147 operators as 292 subtasks, exactly once under faults, 28-second recovery from a worker kill at that width |
| [Rolling upgrade](https://orhaugh.github.io/clink/qualification/qual-08-rolling-upgrade/) | An engine upgrade with exactly-once continuity: 2 s savepoint, 2 s restore, every event across the boundary counted once |
| [Semantic comparison](https://orhaugh.github.io/clink/qualification/qual-07-semantic-comparison/) | 19 of 19 queries content-equal with an independent reference engine under pre-declared judgement classes |

The full table, the rig, the method and every campaign's honesty-bounded
claims: [Qualification](https://orhaugh.github.io/clink/qualification/).

## Status and maturity

clink is young and pre-1.0. Its guarantees are qualified within the
published bounds above, not battle-tested by years of third-party
production deployments; public C++ APIs may still change between minor
releases, and every such change is called out in the
[CHANGELOG](CHANGELOG.md). Durable state is treated conservatively:
snapshots carry schema versions with a migrate-at-restore path, so an
upgrade does not silently invalidate checkpoints or savepoints. What the
engine can do, feature by feature with caveats stated, lives in the
[capability catalogue](https://orhaugh.github.io/clink/capabilities/).

## Documentation

Everything below is published at
[orhaugh.github.io/clink](https://orhaugh.github.io/clink/):

- [Capability catalogue](https://orhaugh.github.io/clink/capabilities/):
  the complete shipped feature surface, with caveats.
- [SQL reference](https://orhaugh.github.io/clink/sql/): the supported
  SQL surface, from DDL through windows, joins, `MATCH_RECOGNIZE`, UDFs
  and SQL-native ML.
- [Connectors](https://orhaugh.github.io/clink/connectors/): twenty-plus
  sources and sinks (Kafka, Postgres incl. CDC, ClickHouse, S3/GCS/Azure
  Parquet, Iceberg, MQTT, NATS, Pulsar, RabbitMQ, Redis, MongoDB,
  Cassandra, HTTP, Avro, WebSocket and more), each with dependencies,
  options and delivery semantics.
- [Internals](https://orhaugh.github.io/clink/internals/): every
  subsystem documented the way its code is structured, citing sources.
- [Design decisions](https://orhaugh.github.io/clink/design/): why the
  engine is built this way, trade-offs included.
- [Benchmarks](https://orhaugh.github.io/clink/benchmarks/) and
  [cost footprint](https://orhaugh.github.io/clink/efficiency/).
- [Qualification](https://orhaugh.github.io/clink/qualification/).
- [Runnable examples](docs/consumer-examples/): every core feature as a
  standalone buildable program, from hello-pipeline to the testing
  framework and state-as-data workflows.

## Installing

clink ships as a regular CMake package:

```bash
cmake -S . -B build -DCLINK_BUILD_TESTS=OFF -DCLINK_BUILD_EXAMPLES=OFF \
                    -DCMAKE_INSTALL_PREFIX=/usr/local
cmake --build build --parallel 10
sudo cmake --install build
```

Downstream projects consume it with `find_package(clink REQUIRED)` and
link `clink::clink` (or per-impl targets such as `clink::kafka`);
[docs/consumer-examples/](docs/consumer-examples/) is a complete
`find_package`-based project to copy from. The install carries the
`clink` CLI and the `clink_node` cluster daemon. Optional connectors and
backends are `CLINK_WITH_<NAME>` CMake options (default `AUTO`: used
when the dependency is found); the SQL frontend is `CLINK_BUILD_SQL=ON`
by default. `clink --capabilities` prints what any given binary was
built with.

## Getting help

- Bug reports: [GitHub issues](https://github.com/orhaugh/clink/issues).
- Questions and ideas: [GitHub discussions](https://github.com/orhaugh/clink/discussions).
- Contributing a change: [CONTRIBUTING.md](CONTRIBUTING.md).
- Reporting a security issue privately: [SECURITY.md](SECURITY.md).

## Licence and attribution

clink is licensed under the Apache License 2.0. See [`LICENSE`](LICENSE)
and [`NOTICE`](NOTICE). You are free to use clink for any purpose,
including in commercial and closed-source products. If you build on it,
a link back to this repository is appreciated; for write-ups, please
cite it via [`CITATION.cff`](CITATION.cff).
