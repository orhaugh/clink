# Stable C++ API conformance

Compile-only translation units that enumerate the members of the Stable tier
(design record `docs/design/011-public-api-tiers.md`). Header tiers in
`scripts/public-api-surface.txt` say *where* the 1.x source-compatibility
promise applies; these files say *which* classes, functions, macros and
virtual hooks it covers. Nothing here runs: every use sits inside a function
that is never called, and the operator hooks are overridden with `override`
so a changed virtual signature fails to compile rather than silently
becoming a new overload.

## The rule

This directory is frozen in the same sense as `tests/fixtures/`:

- Add to it freely. A new promised member gets a new use; a new promised
  header gets a new file.
- Never edit or remove an existing use to make the build pass. A change the
  build needs here is, by definition, a break of the Stable tier, and lands
  only at a major release after the member has carried a deprecation for at
  least one minor.
- A member that is not exercised here is not promised, even if its header
  is Stable. Public plumbing that a Stable class exposes without a use here
  falls under the same "reachable but not promised" rule as the
  `[stable-reaches-internal]` headers.

`clink_api_headers_check` (next to this, in `CMakeLists.txt`) compiles every
Stable header on its own, so a header that only compiles after some other
header is caught too. Both targets build as part of the normal test build;
there is nothing to run.
