# 011: The public API is tiered, and 1.x promises source compatibility on the Stable tier

Status: accepted, v0.9. The tier manifest is frozen at 1.0.

## Context

The road to 1.0 asks for "stable public APIs: settle the C++, C and
SQL-facing interfaces that should carry compatibility guarantees across the
1.x line". Today the engine has three kinds of public surface and no
statement of which parts of them a consumer may rely on.

The C++ surface is the whole `include/clink/` tree, 310 headers installed
wholesale plus the per-connector headers under `impls/*/include`. A consumer
who writes a job against `api/pipeline.hpp` and one who reaches into
`cluster/protocol.hpp` get the same install and the same silence about what
will hold. The README says only that "public C++ APIs may still change
between minor releases". Design record [010](010-stable-extension-model.md)
settled the *binary* contract for compiled plugins (equality over a declared
177-header surface, no N-1 while C++ crosses the dlopen boundary), which is
a different question from what a consumer's *source* can expect to keep
compiling.

The C surface is `embed/clink.h`: fourteen functions behind
`CLINK_EMBED_ABI_VERSION 1`, exported as the only symbols of `libclink`, and
the contract `pyclink` is written against. Its one options struct is passed
by pointer with no size field, so adding a single option would change the
layout under every existing caller. An ABI that cannot grow without breaking
is not stable; it is merely frozen.

The SQL surface is the statement set in the published reference
(`docs/sql.md`). It is well tested but has no notion of which behaviours a
1.0 script may hold a later 1.x release to, no rule about how the grammar,
the option keys or the function catalogue may grow, and no frozen corpus that
would make a semantic change visible as a diff. One rule works against
stability today: a built-in function shadows a user-defined function of the
same name, so a built-in added in a later release could silently change what
an existing script computes.

Byte-level compatibility is already handled: every protocol and persistent
format has its own version, policy and frozen fixture
(`docs/internals/protocol-compatibility.md`). This record is the API
counterpart to that inventory.

## Decision

**1.x is source-compatible on a declared Stable tier, and nothing else is
promised.** Three tiers cover every public surface:

- **Stable.** Code written against it compiles and keeps its meaning on every
  1.x release. Additions are allowed at any time. A removal or a change of
  signature or semantics happens only at 2.0, and only after the member has
  carried a deprecation for at least one minor release.
- **Evolving.** Documented and supported, but may change in a minor release.
  Every such change is named in the CHANGELOG, and where a rename is
  involved the old spelling keeps working for at least one minor.
- **Internal.** Installed because the plugin authoring surface's include
  closure needs it to compile, or because a tool links it. No promise at all.

The tiers are declared in tracked, generated manifests so that a contract
change is a reviewed diff rather than a side effect, following the pattern
record 010 established for the binary surface.

### C++: header tiers plus an enumerated member surface

Tiers are assigned per header by `scripts/gen-public-api-surface.py` and
written to `scripts/public-api-surface.txt`. Every header under
`include/clink/` and `impls/*/include/clink/` must be covered; a header the
rules do not name is Internal by default, so a new header cannot be promoted
by accident. The Stable tier at adoption is the authoring surface the
consumer examples and the internals pages teach: the fluent pipeline
(`api/`), job and plugin registration (`job/`, `plugin/plugin.hpp`), the
operator bases and the standard operator library (`operators/`), the runtime
handles an operator is given (`RuntimeContext`, `TimerService`,
`OutputTag`, the dead-letter seam), the local runtime (`Dag`,
`LocalExecutor`, `JobConfig`), keyed and broadcast state with the
`StateBackend` interface and schema versioning, `Codec<T>` and the declared
types machinery of record 009, watermarks and strategies, the connector
authoring bases (`CommittingSink`, the file and Parquet connectors,
`PollingSource`, the delivery-guarantee and capability records), the CEP
pattern API, the public testing framework (`test/`), and `EmbeddedEngine`.
Evolving covers the surfaces that are real but not yet settled: the Table
API, the Flight SQL server, `JobSubmitter`, `JobGraphSpec`, `Coordinator` and
`Worker` as embeddable classes, the SQL `Catalog` and script runner, the
queryable-state clients, the lineage listener, the state-processor readers,
the metrics handles, the HTTP server and client, the columnar operator fast
path, and the typed connector classes under `impls/*/include/clink/connectors/`
(their builders and `install()` entry points are Stable; the classes behind
them follow their upstream SDKs). At adoption the manifest holds 126 Stable,
71 Evolving and 265 Internal headers.

A Stable header's include closure reaches lower tiers today
(`operator_base.hpp` reaches the bounded channel and the metrics registry,
`dag.hpp` reaches most of `runtime/`; 12 Evolving and 54 Internal headers at
adoption). The manifest records that reach in `[stable-reaches-evolving]` and
`[stable-reaches-internal]` sections: those types are *reachable but not
promised*, and the check fails when either set changes without the manifest
being regenerated, so an Internal type newly exposed through a Stable header
is a reviewed event. Inside a Stable header, `namespace detail` and
trailing-underscore identifiers are Internal by convention.

Header granularity says where the promise applies; it does not say which
members are promised. That list is `tests/api_conformance/`: compile-only
translation units that exercise every promised class, function, macro and
virtual hook the way a consumer would, with the operator hooks overridden
with `override` so a changed virtual signature fails to compile. The
directory is frozen in the same sense as the fixtures under
`tests/fixtures/`: entries are added, never edited or removed, and a change
that a build needs in order to keep compiling is by definition a Stable-tier
break. Every Stable header is also compiled on its own (`api_headers_check`),
because a header that only compiles after some other header is not a usable
contract.

The plugin ABI stays as record 010 left it. A compiled job is rebuilt per
engine release; the source it was built from keeps compiling. Those are the
two halves of the extension story, and neither substitutes for the other.

### C: an ABI that can grow

`embed/clink.h` is Stable in its entirety and is versioned separately from
the library: `CLINK_EMBED_ABI_VERSION` moves only for a break, which within
1.x is never. Three changes make additions possible without one, and are
taken now, as the one deliberate break before 1.0 (`CLINK_EMBED_ABI_VERSION`
becomes 2):

- `clink_engine_options` gains a leading `struct_size` that callers set to
  `sizeof(clink_engine_options)` (via `CLINK_ENGINE_OPTIONS_INIT`). The
  library reads a field only when the caller's declared size covers it, so a
  field appended in 1.3 is invisible to a binary compiled against 1.0 and a
  1.0 binary running against a 1.3 library gets the defaults for what it
  never knew. A zero `struct_size` is refused with a named error rather than
  guessed at.
- `clink_version()` returns the library's semantic version as a string, so
  an embedder can log the engine it actually loaded; `clink_abi_version()`
  keeps answering the compatibility question.
- The exported symbol set is a tracked manifest,
  `scripts/libclink-abi-symbols.txt`, checked two ways: against the
  declarations in the header without a build (CI and the pre-commit hook),
  and against the dynamic symbol table of the built library as a test. A
  symbol is never removed from the manifest within 1.x; a new one is
  appended.

The rules for a compatible change are written into the header itself:
append-only struct growth behind `struct_size`, new functions, new
non-zero return codes only where the header already says a range is open,
and `CLINK_DEPRECATED` on anything scheduled for 2.0. `pyclink` tracks the
header (`_ABI_VERSION`, the ctypes struct) and is Stable at the Python
level: `Engine` and its documented methods and keyword arguments,
`ClinkError`.

### SQL: an additive dialect with a frozen corpus

The statements, clauses, types, functions and `WITH` option keys documented
in the SQL reference are Stable, with these exceptions kept Evolving until
their shapes have had a release to settle: the AI table functions
(`CREATE MODEL`, `ML_PREDICT`, `VECTOR_SEARCH`) and the WebAssembly
aggregate form. The wording of diagnostics, the text of `EXPLAIN`, and the
output format of `SHOW TABLES` are not contracts.

The growth rules follow from "a 1.0 script runs unchanged, with the same
results, on every 1.x":

- The grammar only grows. libpg_query pins the PostgreSQL 16 keyword set;
  clink's own extensions arrive through the pre-parser and must not capture
  text that previously parsed as ordinary SQL.
- A `WITH` option key is never removed or given a new meaning. A renamed
  key keeps its old spelling as an alias through 1.x. Unknown keys keep
  failing at compile time, as they do today.
- A built-in function's result type and semantics never change. A
  user-defined function shadows a built-in of the same name, in place of the
  current built-in-wins order, so adding a built-in can never change what an
  existing script computes. This is the one behavioural change the record
  makes.
- Persisted catalog entries (`--catalog-dir`) are a compatibility domain in
  their own right: additive JSON with readers that ignore unknown keys, pinned
  by frozen fixtures and listed in the protocol inventory.

The mechanical half is `tests/sql_conformance/`: a corpus of scripts, each
with its input and its expected output, run through the embedded engine by
one data-driven test. It is frozen the same way the conformance translation
units are, so a case that must be *edited* to stay green is a semantic
change, reviewed as a Stable-tier break.

### Everything else, declared

The remaining surfaces are tiered here so the promise has no silent edges,
without new enforcement in this round. Stable: the CMake package (the
`clink::` target and component names, `clink_add_job_module()`), the `clink`
CLI's subcommand names and documented flags, the `/api/v1` REST routes (a
break ships as `/api/v2` alongside), the capabilities manifest under its own
`schema_version`. Evolving: human-readable CLI output, metric names (a rename
keeps the old name for one minor), configuration keys and environment
variables, the Helm chart values. Byte-level formats are governed by
`docs/internals/protocol-compatibility.md`, unchanged.

Versioning for 1.x follows: a major release is the only place a Stable
surface may break; a minor may add to any tier, change Evolving surfaces
with notice, and rotate the plugin ABI fingerprint (a rebuild, refused
loudly by the 010 gate if skipped); a patch changes no surface and avoids
rotating the fingerprint, though the gate rather than a promise is what
protects a deployment that assumes it did not.

## Consequences

A consumer can answer "may I depend on this?" by looking at one tracked
file, and the answer holds for the whole 1.x line. Contract changes are
diffs in review, on four artefacts: the header tier manifest, the
conformance translation units, the C symbol manifest and the SQL corpus.
The C ABI can gain options and functions for the life of 1.x without a
version bump. Adding a SQL built-in stops being an upgrade hazard.

Trade-offs accepted. The Stable tier is deliberately narrow and the reach
into Internal headers is large; shrinking that reach is 1.x work, done by
moving declarations, and the manifest makes each step visible. Header-level
tiers over-approximate in the other direction too: a Stable header can carry
public plumbing that the conformance suite does not name, and only what the
suite names is held. Freezing conformance sources and the SQL corpus means a
genuine bug fix that changes observed output needs its case superseded (a
new case added, the old one retired with the reason recorded) rather than
quietly edited, which is more ceremony than a test normally carries and is
the point. Demoting the AI table functions to Evolving is a judgement about
how settled their option shapes are, not about whether they work. And the
promise is source-level only: a compiled artefact still rebuilds per release,
which is the boundary record 010 drew and this record does not move.
