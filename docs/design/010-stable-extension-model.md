# 010: The extension contract is a declared surface, checked completely

Status: accepted, v0.9.

## Context

A clink job or plugin is a compiled shared library ([004](004-jobs-as-compiled-plugins.md)):
it statically links the engine core, is loaded with `dlopen(RTLD_LOCAL)`, and
exchanges work with the host through typed registrations - `std::function`
closures, operator vtables and template bodies compiled into the module. That
model is deliberate: authoring stays plain C++, and per-record dispatch pays
no marshalling cost. Its price is binary coupling, and the road to 1.0 names
the bill directly: "reduce coupling between compiled plugins and individual
clink revisions, with a clearly defined compatibility contract".

The gate this record replaces hashed the entire public header tree - all 310
`include/clink/**/*.hpp` files - plus two build options into a structural
fingerprint, and admitted a plugin only on exact equality. Measured over
v0.7.0..HEAD, 22% of commits rotated that fingerprint and invalidated every
deployed plugin binary, though a plugin translation unit can only ever reach
roughly half the tree: an edit to a `sql/`, `embed/` or `queryable_state/`
header rotated the contract for modules that could not name a single type
from those directories. At the same time the gate was incomplete. The
standard-library identity, sanitizer instrumentation, the pinned Arrow
version and several layout-changing defines (`CLINK_FAULT_INJECTION` swaps a
class declaration wholesale) all shape the ABI and none was material, so a
false accept was constructible - a test-configured build's plugin loading
into a runtime cluster with a different layout for the same class.

One option was rejected outright for this round: a pure-C authoring ABI
(opaque handles, data-only crossings) would make true cross-revision and
cross-toolchain tolerance possible, and remains the plausible post-1.0
direction. It is not this record because it re-founds the operator authoring
model - every typed registration, `Codec<T>`, and the 78 inline virtuals on
the operator bases would need type-erased C equivalents - and because the
performance and ergonomics of the current model are the reasons it exists.

## Decision

While C++ crosses the boundary, no version algebra is honest: two builds
either agree on every layout a module can touch or they do not. The contract
is therefore **equality over a declared, complete surface**, made exact in
four moves.

**The surface is declared, generated and reviewed.** The transitive include
closure of the plugin authoring entry points - everything under
`include/clink/{api,job,plugin,operators,connectors}/`, plus the exception
types thrown across the boundary and caught by type on the host
(`state/checkpoint_integrity.hpp`; their layout crosses even though authoring
code rarely includes them), and what all of those reach - is computed by
`scripts/gen-plugin-abi-surface.py` into the tracked manifest
`scripts/plugin-abi-surface.txt` (177 headers at adoption; 310 in the tree). The fingerprint hashes exactly the manifest's files. A header
entering or leaving the closure is a manifest diff in review, gated stale-free
by `--check` in CI and the pre-commit hook; the generator also scans every
first-party plugin source and fails if one includes a header outside the
declared surface, which converts the residual risk (an undeclared header
changing under a module that uses it) from silent undefined behaviour into a
red check.

**The material is complete.** Beside the header hashes, the fingerprint folds
in the conditional-compilation options the surface actually uses (computed
into the manifest's `[options]` section: `CLINK_USE_FLAT_HASH_MAP`,
`CLINK_FAULT_INJECTION`, `CLINK_HAS_ARROW`, `CLINK_HAS_PARQUET` at adoption),
the pinned Arrow version, and the manual `CLINK_ABI_VERSION`. An option the
build system cannot map fails configure rather than being guessed at. Facts
the build system cannot hash - which standard library, the libstdc++
dual-ABI choice, `_GLIBCXX_DEBUG`, ASan/TSan/MSan instrumentation - are
composed by the preprocessor into a separate toolchain-identity constant
(`kToolchain`) that each module bakes for itself and the loader compares
unconditionally. Standard-library *versions* are deliberately excluded: both
mainstream libraries hold their ABI stable across releases, and folding a
version number in would resurrect rotation without protection.

**Refusals are early and named.** Every plugin carries its per-header
manifest (`clink_plugin_abi_manifest`), so a fingerprint refusal names the
headers that differ - or, when the manifests match, blames the options
material - instead of printing two opaque hashes. The submitter reads each
plugin's identity via its own `dlopen` and advertises it inside `SubmitJob`,
and the coordinator refuses an incompatible plugin on the references-only
first exchange: zero plugin bytes ship for a module the gate would reject,
and the ack carries the cluster's manifest so the client names the diff
locally. Adverts are claims; the load-time gate remains the authority.

**Deterministic refusals are fatal, never retried.** A worker whose gate
refuses a deploy's plugin (possible only on a mixed-version cluster, i.e. mid
rolling upgrade) reports the failure as fatal: the job fails with both
identities named rather than burning its restart budget on redeploy-and-refuse
loops the scheduler has no ABI affinity to escape. HA recovery keeps refusing
persisted plugins that predate an upgrade - mismatched bytes must never run,
and parking would wedge forever - but now says so at error level with a
counter (`clink_ha_recovery_skipped_plugin_total`); the remedy is a resubmit
with a rebuilt plugin.

One build recipe carries the contract to consumers: `clink_add_job_module()`
ships with the CMake package and replaces the seventeen hand-rolled module
blocks in-tree, so out-of-tree plugins get the same linkage, naming,
split-debug and exported-handshake behaviour the tree's own fixtures are
tested with.

## Consequences

A cluster rebuild keeps loading existing plugin binaries unless the declared
surface, its options, the pinned Arrow, or the toolchain genuinely moved.
Measured on history, the surface alone cuts spurious rotation modestly
(22% to 17% of commits over v0.7.0..HEAD; windows dominated by real
operator/core work, such as v0.8.0..HEAD at 35%, rotate either way - those
changes are the contract moving). The larger gains are qualitative: the
contract is a reviewable artifact rather than a side effect of the tree
layout, false accepts across toolchains and layout options are closed, an
incompatible submit fails in one round trip with the differing headers named,
and a mixed-version deploy fails a job loudly instead of eroding its restart
budget.

Trade-offs accepted. The surface over-approximates (the closure includes
headers a minimal job never touches - `cluster/protocol.hpp` among them - so
some rotations remain that a finer-grained analysis would skip); that is the
safe direction for a load gate. Impl-connector headers under
`impls/*/include` stay outside the contract, as before: they are gated
per-target by `CLINK_HAS_<impl>` and cannot change the layout of surface
types. Strict mode (`CLINK_STRICT_PLUGIN_ABI=1`, exact commit-hash equality)
is unchanged as the paranoid override. Symbol visibility stays default:
typed exceptions cross the boundary today (the worker catches
`CheckpointIntegrityError` and `TransportOnlyFailure` by type from
plugin-compiled code, pinned by a test), and hiding typeinfo would put those
catches at the mercy of RTTI comparison internals; `clink_add_job_module`
offers `HIDDEN_VISIBILITY` as an experiment, not a default. And the ceiling
is explicit: binary tolerance across *differing* surfaces - true N-1 - waits
on a C authoring ABI, which this record leaves to the 1.x era.
