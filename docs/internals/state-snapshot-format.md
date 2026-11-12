# The state snapshot format (stable contract)

> The on-disk and on-wire encoding of a job's keyed state - an Apache Arrow IPC stream any Arrow consumer can open - documented as a stable public contract, with its key encoding, metadata, derived projections and evolution policy.

## Overview

Every snapshot of clink keyed state is one Apache Arrow IPC stream: a schema, one RecordBatch, and an end-of-stream marker. Checkpoints and savepoints from the in-memory backend family are this format natively; RocksDB state renders to it on demand through the Arrow export; `clink state-export` writes it as a file. Because it is plain Arrow, pyarrow, DuckDB, Polars, Spark or any other Arrow reader opens it directly - no clink code required. This page is the format's contract: what a reader may rely on, what a writer must produce, and how the format is allowed to change.

The single in-tree writer is `SnapshotArrowWriter` (`include/clink/state/snapshot_arrow_writer.hpp`); every producer routes through it, so the bytes agree by construction. The reference reader is `InMemoryStateBackend::restore`.

## The stream

One Arrow IPC **stream** (not the file/Feather format): schema, exactly one RecordBatch, EOS. A zero-row batch is a valid, restorable snapshot of empty state. The stream is self-contained; there are no side files.

| Column | Arrow type | Nullable | Meaning |
| --- | --- | --- | --- |
| `op_id` | `uint64` | no | The operator id (`OperatorId::value()`) owning the entry |
| `key_bytes` | `binary` | no | The full encoded state key (layout below) |
| `value_bytes` | `binary` | no | The raw stored value bytes (ownership below) |

One row per `(operator, key)` entry. Row order is unspecified; readers must not rely on it. Writers currently emit deterministic orders (the RocksDB export sorts operators ascending with keys in byte order), but that is a convenience, not a guarantee.

## Schema metadata

The schema's `KeyValueMetadata` carries:

| Key | Value | Presence |
| --- | --- | --- |
| `clink.format_version` | `"1"` | Stamped on every stream the writer produces. A reader MUST treat absence as version 1 (streams written before the marker existed). |
| `clink.state_versions` | packed `StateVersionMap` | Present when the job registered schema-evolution stamps; absent means "no stamps recorded" (the control plane then assumes version 1 per state type). |
| `clink.state_fingerprints` | packed `StateFingerprintMap` (`<op>\|<slot>\|<fp-hex>` lines) | Shape fingerprints of described types, stamped at KeyedState bind. Absent on older snapshots and for undescribed types; absence gates nothing. A stored fingerprint differing from the live type's with no schema-version bump refuses the restore at bind. |

The `StateVersionMap` packing is line-oriented text: one `<op_id>|<state_type>|<version>` triple per line, `\n`-separated, empty string for an empty map (`StateVersionMap::pack` / `unpack`, `include/clink/state/schema_version.hpp`). Unparseable content is a corrupt snapshot, not an absent map.

Readers MUST ignore metadata keys they do not recognise; writers MAY add new keys without a format-version bump (metadata additions are compatible changes).

## key_bytes encoding

The stored key layout (produced by `KeyedState`, `include/clink/state/keyed_state.hpp`):

```
keyed state:     [ kg byte (0..127) ][ slot name ][ '|' ][ user key bytes ]
operator state:  [ 0xFF ][ slot name ][ '|' ][ user key bytes ]
```

- The leading byte of a keyed row is its **key group**: FNV-1a over the user key bytes, mod 128 (`kNumKeyGroups`). It is the rescale partitioning unit - a single-byte compare filters a snapshot by key-group range with no knowledge of codecs or slots.
- A leading byte `>= 128` (in practice `0xFF`, `kOperatorStateKeyPrefix`) marks **operator state** (source offsets, broadcast slots). These rows have no key group and are restored whole by every subtask.
- The slot name never contains `'|'` or `'\n'` (enforced at slot creation), so the first `'|'` after the prefix byte splits slot from user key unambiguously.
- A key with no `'|'` separator is legacy/raw; tooling surfaces it under the reserved slot name `<raw>`.

User key bytes are the operator's key codec output. The built-in conventions worth knowing when reading externally: an 8-byte user key is typically a little-endian int64 (the built-in int64 codec); string keys are raw UTF-8.

## value_bytes ownership

Value bytes belong to the operator's value codec; the format deliberately does not describe their internal layout. Two engine-level wrappers a reader may encounter:

- **TTL slots** prefix every value with `[ 8-byte expire-at-ms, little-endian ]` before the codec bytes.
- **Collection state** (`ListState`, `MapState`, ...) serialises the whole collection as one value per key.
- **Source offsets** (operator-state rows) are typically bare 8-byte little-endian int64s; the restore merge relies on this to keep the greater offset when subtask snapshots collide on the same key. That rule only applies between *peers*: a restoring subtask's own row is authoritative for any key it holds, and peers contribute only keys it does not have, and only for the operators it runs. Keep-the-greater is the safe direction for a partition offset, which can only move forward, and would be silent data loss for a source that keeps its position under a single fixed key shared by every subtask.

## The changelog variant

`ChangelogStateBackend` snapshots use the same encoding with one extra **leading** column:

| Column | Arrow type | Meaning |
| --- | --- | --- |
| `row_kind` | `uint8` | 0 = materialisation row, 1 = put, 2 = erase |

Restore replays the materialisation rows first, then applies the log rows in order.

## Writers and readers

| Component | Role |
| --- | --- |
| `SnapshotArrowWriter` | The one canonical writer (in-memory family snapshots, operator-state extraction, RocksDB exports) |
| `InMemoryStateBackend::restore` | Reference reader (also the sharded/file-backed/savepoint loader) |
| `InMemoryStateBackend::merge_snapshot_bytes` | Concatenates streams (scale-down form); preserves the first non-empty input's metadata |
| `RocksDBStateBackend::export_arrow_snapshot` / `rocksdb_checkpoint_to_arrow` | Render RocksDB state to this format |
| `clink state-export / state-cat / state-diff / state-query / check-savepoint` | CLI surface over the format |
| Live export HTTP routes (`queryable_state/live_export.hpp`) | Serve a RUNNING job's merged live state as one stream (worker and coordinator; per-subtask atomic, not a global cut) |
| `RemoteReadBackend::export_arrow_snapshot` + `RemotePool::scan_checkpoint` | Render the disaggregated tier (pool checkpoint + hot delta) to this format |
| `canonicalise_state_snapshot` (`include/clink/state/snapshot_canonicalise.hpp`) | Render ANY snapshot blob to the canonical form: canonical passthrough; changelog variant replayed (external materialisation handles resolved via the supplied store) |

## Derived projections (not restorable)

Three derived layouts exist for analytics; they decode `key_bytes` into columns and are NOT inputs to restore:

- **Parquet export** (`state-export --format=parquet`): `op_id: uint64, key_group: uint8, slot: utf8, user_key: binary, value_bytes: binary`, versions in the file's key-value metadata.
- **Iceberg export** (`--format=iceberg`): the same five columns with Iceberg types (`op_id` bit-cast to signed `long`).
- **Query projection** (`state-query`, `write_state_query_parquet`): adds rendered text and int64-reading columns for direct SQL filtering.

## Stability and evolution policy

Version 1 (current) guarantees:

1. The three-column shape, column names, types and non-nullability are stable.
2. The `key_bytes` layout (kg byte, slot, `'|'`, user key; `0xFF` operator-state prefix; 128 key groups) is stable.
3. Zero-row streams are valid. Row order is unspecified.
4. Metadata is open for additive extension; readers ignore unknown keys.

A change that any version-1 reader would misread (column changes, key-layout changes, container changes) bumps `clink.format_version`, and readers reject a version above the highest they know rather than guess. Absence of the marker always reads as version 1. Compatible changes (new metadata keys, new derived projections, new writers) do not bump the version.

That rejection is `verify_snapshot_format_version` (`include/clink/state/snapshot_arrow_writer.hpp`), called at every read site: the three `InMemoryStateBackend` paths, changelog restore, state migration, and canonicalisation. `kMaxReadableSnapshotFormatVersion` is the ceiling and is bumped alongside `kSnapshotFormatVersion`. The parse is strict - the value must be a plain decimal number, so a stream carrying `" 1"` or `"1.0"` is treated as corrupt rather than leniently read as version 1.

Until 2026-08-02 this paragraph described a rule that nothing implemented: the marker was written by the writer and read by nobody. The schema check present at each read site is not a substitute, because a version bump means precisely "a change the previous reader misreads" and such a change need not alter the column shape.

## Reading it externally

```python
import pyarrow.ipc as ipc
t = ipc.open_stream("checkpoint-9.snap").read_all()
print(t.schema.metadata)          # format version + packed state versions
print(t.num_rows, t.column("op_id")[0])
```

```sql
-- DuckDB: a checkpoint as a table (via the Parquet projection)
-- clink state-export --from=ckpt.snap --out=state.parquet
SELECT slot, count(*) FROM 'state.parquet' GROUP BY slot;
```

## Checkpoint datasets

A snapshot is one participant's state. A **checkpoint dataset** is one completed checkpoint of a whole job, exported as a directory that can be moved, read without clink, queried and compared, and that states what it holds and whether it is complete (format `clink-state-dataset`, version 1; library `include/clink/state_processor/checkpoint_dataset.hpp`, `src/state/checkpoint_dataset.cpp`).

```bash
clink state-export --checkpoint-dir=<dir> --job=<id> (--checkpoint=<N> | --restore-point) \
    --out=<dataset> [--ha-dir=<dir>] [--commit-confirmation=yes|no] [--allow-partial] \
    [--max-bytes=N] [--max-files=N] [--max-entries=N] [--timeout-s=N] [--json]
```

### How one checkpoint is collected

The export collects with the incident bundle collector (`clink::incident::collect`, [incident verification](incident-verification.md)) and the state only, so the rules that select and copy a checkpoint are the engine's own:

1. The checkpoint is the one named, or with `--restore-point` the one the engine's restore rule chooses now (`restore_point_on_disk`), which needs `--ha-dir` or `--commit-confirmation` to say which rule applies.
2. Its participants are the set its COMPLETED marker records, read under the state root and generation the marker names. Another subtask's file, or the same checkpoint id in another generation, is never read. A marker that records no participant set makes the dataset incomplete.
3. Every file is copied and checked unchanged afterwards. A file that changes or vanishes during the copy (a retention purge, for one), or a marker the copy took that is then removed, refuses the export with kind `changed`: nothing is published, and the export can be run again. A later checkpoint completing, or a restore recorded, while the copy runs does not refuse it: the dataset is the selected checkpoint's either way, and its notes say what appeared.
4. The collector's consistency checks run over the copy (`verify_core`, profile `consistency`). Each participant's checkpoint is decoded once, by the export itself, and its entries are written as one row group.
5. The dataset is published by renaming a completed temporary directory beside `--out`. It never writes over an existing path.

Exit codes: 0 complete, 3 published partial (`--allow-partial`), 1 refused with nothing published, 2 usage. A refusal names its kind: `incomplete`, `unsupported`, or the collector's `changed`, `missing`, `bound`, `timeout` or `other`.

### Layout

| Path | Contents |
| --- | --- |
| `manifest.json` | The bundle manifest: job, selection, participants, every file's size and SHA-256, and what is absent and why |
| `checkpoint/_jobs/<job>/` | The job's markers: `COMPLETED-<id>`, `CONFIRMED-<id>`, `RESTORED` |
| `checkpoint/_state/<job>/v<gen>/<p>/checkpoint-<id>.snap` and `.meta` | Each participant's snapshot and sidecar, byte for byte (the file backend) |
| `.../v<gen>/<p>/changelog-<id>.snap` and `materializations/` | A changelog backend's snapshot and the materialisation files its handles name |
| `.../v<gen>/<p>.cp-<id>/` | A RocksDB checkpoint directory, every file of it |
| `checkpoint/_external/<job>/...` | A state root outside the checkpoint directory, mirrored with its own layout |
| `job/manifest.json` | The job manifest with credential-like graph parameters redacted, when `--ha-dir` holds it |
| `entries.parquet` | Every stored entry, exact bytes |
| `dataset.json` | The contract below |

### entries.parquet

| Column | Type | Meaning |
| --- | --- | --- |
| `op_id` | `uint64` | The operator id the state is stored under |
| `slot` | `utf8` | The slot name, or `<raw>` |
| `scope` | `utf8` | `keyed`, `operator` or `raw` |
| `key_group` | `int16`, nullable | The key group; keyed rows only |
| `participant` | `uint32` | The job-global subtask index whose snapshot holds the row |
| `user_key` | `binary` | The user key bytes |
| `value` | `binary` | The stored value bytes |
| `stored_key` | `binary` | The key exactly as the snapshot stores it |

Scope follows the key layout above. A leading byte below 128 with a slot separator is keyed state. A leading `0xFF` is operator state: slot and user key when the key has a separator, and otherwise slot `<raw>` with the bytes after the prefix as the user key (source positions and watermark progress are stored that way). Anything else, including a slot name that is not UTF-8, is a raw row with the whole key as its user key.

There is one row group per participant holding entries, sorted by `op_id`, `slot`, `scope`, `key_group`, `user_key`, with bytes compared unsigned. An entry's identity is (operator, slot, key group, user key) for keyed state: the key group is the key's own and does not depend on the parallelism, so a rescale moves an entry to another participant without changing its identity. Operator state and raw rows belong to the subtask that wrote them, so their identity is (operator, slot, owner, user key), and two subtasks' rows are never collapsed because their encoded keys match. The owner is the subtask within its operator when the checkpoint's marker records each participant's identity and every operator's rows are in one graph operator's subtasks (`layout.operator_subtasks`), so a rescale of another operator, which moves the job-global indices, does not change it; otherwise it is the participant.

No column reads a value as a type. Nothing in this build establishes a job's codecs, and an 8-byte value is not assumed to be an int64.

### dataset.json

| Field | Contents |
| --- | --- |
| `format`, `version`, `exporter` | `clink-state-dataset`, 1, and the exporting build |
| `job` | `id`, `graph_sha256` (SHA-256 of the graph text the bundle holds), `commit_confirmation`, `rescaled` |
| `checkpoint` | `id`, `generation`, `status` (`completed`, or `confirmed` when a CONFIRMED marker exists), `state_root` |
| `selection` | `rule` (`named` or `restore-point`), `engine_choice`, and `restored`: the job's RESTORED record (`restore`, `next`), which marks the run |
| `in_doubt` | `needs_resolution` (true for a completed, unconfirmed checkpoint of a job whose sinks need commit confirmation; null when unknown), a `statement`, and `external_outcomes`, which always says they are not verified |
| `participants[]` | `index`, `backend` (`file`, `changelog`, `rocksdb`), `snapshot` (the file, or the checkpoint directory), `bytes`, `sha256`, `sidecar` verdict (or that the backend keeps none), `operator` (the graph operator and subtask the marker records it as), `format_version`, `state_versions`, `state_fingerprints`, per operator its keyed, operator and raw row counts and the key groups held as ranges, `row_group`, `rows`, `exported` and the reason when not |
| `operators[]` | `op_id` (decimal text), `graph_id` and `named_by` (`uid`, or `layout` when the marker's identities name it), the graph's `uid`, `type` and `parallelism` when it holds the graph, `hosted_by` and `subtasks` (the graph operator whose subtasks hold its rows, and how many it has), the `participants` holding its rows, and its `slots[]` with `scope`, `rows` and `schema` (`fingerprint`, `versions`) |
| `layout` | `recorded` (the marker names every participant's operator and subtask), `operator_subtasks` (operator state is identified by subtask), and a statement |
| `graph` | Whether the dataset holds the job graph, and its operators; an operator with a uid carries the `op_id` its state is stored under |
| `entries` | `path`, `sha256`, `bytes`, `rows`, `row_groups`, and the columns, order and identity in words |
| `bundle` | `manifest_sha256` |
| `verification[]` | Each consistency check's verdict and reason |
| `complete`, `incomplete[]`, `unsupported[]`, `notes[]`, `limits` | Whether the contract holds, and why not |
| `represented[]`, `not_represented[]` | What the dataset holds, and what it does not |

A dataset is **complete** only when the collection succeeded; the marker names the job, the checkpoint, its generation and its participants; every participant's snapshot and sidecar is present and verifies; every snapshot is the canonical stream at a readable format version and decodes; no participant holds an entry twice and each operator's key groups are held by one participant each; participants stamp each slot alike; nothing is unsupported; and no bound was reached. A dataset that is not complete is refused unless `--allow-partial`, and then published with `complete: false` and the reasons.

**Backends.** The state root may be any absolute path on the exporting machine, inside the checkpoint directory or outside it, plain or behind `file://`, `rocksdb://` or `changelog+file://`. The file backend's snapshot is verified against its sidecar. A changelog backend over files is replayed with its materialisation handles resolved to the dataset's copies of them. A RocksDB checkpoint is rendered by the RocksDB the `clink` CLI links, from a scratch copy so nothing is written inside the dataset; RocksDB keeps no sidecar, and its integrity is RocksDB's own block checksums, checked as every block is read. A build without RocksDB reports RocksDB checkpoints unsupported.

**Unsupported**, never complete: state not on the exporting machine's filesystem (an object store, a remote or disaggregated tier), a changelog backend whose materialisations are in RocksDB (its handles are RocksDB keys, not files), and state written before it was namespaced by job.

**Represented**: keyed state in every slot; operator state (source positions, broadcast and union slots), each participant's own rows; timers, as the state rows operators write for them; SQL operators' internal slots, as stored bytes; in-flight records an older unaligned checkpoint kept, as the rows it stored; rows whose key follows no layout, whole. **Not represented**: the job's live state after the checkpoint; anything the checkpoint does not hold, such as pending asynchronous operations and buffers between operators; state held outside the checkpoint directory; what external systems received or committed; typed readings of keys and values.

A completed checkpoint is not a statement about a sink's destination. For a job whose sinks need commit confirmation, a completed checkpoint that is not confirmed is one whose transactions a restore resolves in doubt first, and `in_doubt` says so.

Operators are named from the job graph by uid (`operator_id_from_uid`), or else from the identities the checkpoint's marker records: an operator is named as graph operator X when every participant holding its rows is a subtask of X and no other operator's rows are in X's subtasks. That names an operator without a uid, whose id the runtime derives from its place in its chain (every SQL operator), and names operators in a dataset with no job graph, such as a finished job's last checkpoint, whose manifest the HA directory no longer holds. An operator neither rule names is listed by id alone, and so is every operator of a checkpoint whose marker predates the identity lines.

### Reading a dataset

Every reader opens a dataset through `open_dataset`, which checks `dataset.json`'s form and version, that `entries.parquet` and `manifest.json` hash to what it records, that no link is on the path of either or of the dataset itself, and the bundle reader's own path rules. The snapshots are not hashed again there; `clink incident-verify <dataset>` verifies the whole bundle. A dataset damaged after export is refused with exit 2.

**The digest.** `state-export` prints the dataset's digest, the SHA-256 of `dataset.json`. Since `dataset.json` binds the entry table and the bundle manifest by hash, and the manifest binds every file, the digest identifies the whole dataset. Recorded somewhere the dataset is not (a ticket, a log), it detects any later change, a deliberate rewrite of a file together with the hash recorded for it included: `--digest=<sha256>` on `state-query` and `state-cat`, and `--a-digest`/`--b-digest` on `state-diff`, refuse a dataset whose digest differs (exit 2). Without a recorded digest, the hashes detect damage but not such a rewrite.

- `clink state-cat --dataset=<d> [--max-rows=N] [--json]` lists every slot with its scope, schema stamps and entries, each as hex with its length and the participant holding it.
- `clink state-query --dataset=<d> --sql="SELECT ..."` exposes the entries as the table `state`: `op_id TEXT`, `slot TEXT`, `scope TEXT`, `key_group BIGINT`, `participant BIGINT`, `user_key_hex TEXT`, `value_hex TEXT`, `user_key_len BIGINT`, `value_len BIGINT`, `operator TEXT` (the graph operator the dataset names it as) and `subtask BIGINT` (the participant's subtask within its operator, when recorded). It states the dataset's completeness on stderr. An aggregate re-emits each group's running result as the table is read, so `state-query` prints each group's last row; that is decidable only for one `GROUP BY` at the top of the query with every group column selected, and a `HAVING`, a `LIMIT` or an outer query over an aggregate, or an aggregate in a subquery, is refused (exit 2) rather than answered from a superseded row. Its embedded engine runs a Coordinator and Workers on loopback and opens no other connection, so offline it runs under isolation that leaves loopback (`clink_audit.isolation.LoopbackIsolation`, proven by a denied connection to 192.0.2.1).
- Both refuse a partial dataset with exit 3 unless `--allow-partial`.
- pyarrow, DuckDB or any Parquet reader reads `entries.parquet` directly.

### Comparing two datasets

`clink state-diff --a=<dataset> --b=<dataset> [--raw-bytes] [--max-samples=N] [--max-entries=N] [--timeout-s=N] [--allow-partial] [--json]` merges the two entry tables in identity order, a batch per participant at a time, and reports per (operator, slot, scope):

- `added`, `removed`, `changed` and `unchanged` entries, and `relocated`: keyed entries whose participant changed, which is a rescale's or a restore's placement and not a logical change;
- `status`: `compared`; `raw-compared`; `schema-changed` (the slot's fingerprint or version stamps differ, so its entries are not compared); `subtasks-changed` (operator state or raw rows that cannot be matched subtask to subtask: with both sides' identities recorded, the operator's own number of subtasks changed, since a rescale reassigns each subtask's rows; without them, its participant set or graph parallelism changed); or `operator-only-in-a` / `operator-only-in-b`. With identities recorded on both sides, operator state is compared subtask by subtask even when a rescale of another operator moved its job-global indices;
- `--raw-bytes` compares a schema-changed slot's stored bytes, and every line and sample of it is labelled raw; a schema change still counts as a difference;
- bounded samples: at most `--max-samples` per slot, each field as hex of at most 64 bytes with its whole length.

The report states the lineage (same job, same graph, the generations, the restore between them) and whether the result is established. It is not established when either side is partial (nothing is compared unless `--allow-partial`), a slot was not compared, or `--max-entries` or `--timeout-s` was reached. Exit 0 means both sides are complete, every slot was compared and nothing differs; 1 means differences; 3 means not established, whatever was found; 2 an error. A limit never produces exit 0. `--json` writes `clink-state-diff` version 1: `a`, `b`, `lineage`, `established`, `reasons`, `limit_reached`, `entries_read`, `differences`, `exit`, `totals`, `max_samples`, `sample_bytes` and `slots[]`.

**Cost**, measured by the example on macOS arm64 with 200,004 entries (100,000 accounts, two slots, 7.6 MiB of snapshots): an export takes about 0.3 s inside the process (0.44 to 0.47 s of wall time), of which copying and hashing the files is about 0.06 s; the rest is verifying them, decoding and sorting each participant's entries, and writing Parquet with zstd. Its peak memory is 114 to 118 MiB, since it holds one participant's decoded snapshot at a time, and the dataset is 9.0 MiB. A diff of two such datasets reads 400,008 entries in 0.21 s at 60 MiB, holding one batch per participant per side. A `GROUP BY` over one takes 0.82 s at 96 MiB.

The older forms (`state-diff --dir`, `state-export --dir/--id`, `state-query --dir`) read whatever snapshot files they find, without the marker or its participants, collapse operator state of several subtasks, and render 8-byte values as int64 by guess. They are kept for single snapshots and are not checkpoint-consistent.

The runnable example is [`examples/state-inspection/`](https://github.com/orhaugh/clink/tree/main/examples/state-inspection): a job at parallelism 2 exported while it runs, checked against the state folded from its Kafka records, through a Worker crash, a rescale, a schema change, faults, a retention race and RocksDB state, with its costs measured.

## Related

- [./state-and-backends.md](./state-and-backends.md) - the backends that produce and restore this format
- [./fault-tolerance-and-rescale.md](./fault-tolerance-and-rescale.md) - key groups, rescale filtering, and the state CLI verbs
- [./incident-verification.md](./incident-verification.md) - the bundle collector and checks a checkpoint dataset is built on
- [./checkpointing.md](./checkpointing.md) - when snapshots are taken and acknowledged

### Entry-level SQL partition state

With a memory budget and SQL spill directory configured, GROUP BY, fixed/session
windows, OVER, ranking, last-N, equi/interval joins and null-aware exact-key probes
keep individual entries on disk. Checkpoints
use `.groups` length slots and `.rows` cell slots under operator-specific prefixes.
Row cells carry the original partition's key-group byte, rather than the hash of
their composite identifier. An operator-state `.format` marker is retained in
all filtered restores. Legacy whole-partition snapshots migrate on open. GROUP BY
stores metadata at index zero and each aggregate at subsequent indexes. The
`agg.values` companion partition stores individual values for the built-in
distinct, retractable, percentile, string and array collections; recovery upgrades
both the whole-partition format and the earlier per-accumulator format. Fixed
windows store pane metadata separately and use `win.values` for their growing
built-in collections, migrating earlier whole-pane entries on recovery. Sessions
store metadata separately and use `sesswin.values`; session bridges reassign
cells to the merged start, and recovery migrates earlier whole-session entries.
OVER separates metadata and individual running accumulators from its pending and
history lists. See
[memory budgets](memory-management.md) for allocation and recovery limits.
