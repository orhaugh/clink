#!/usr/bin/env python3
"""Generate or verify the public API tier manifest (design record 011).

Every installed header - the whole of include/clink/ plus the per-connector
headers under impls/*/include/clink/ - carries exactly one compatibility
tier, and the tracked manifest scripts/public-api-surface.txt is the
authority a consumer reads to answer "may I depend on this?":

    stable     source-compatible for the whole 1.x line: additions only,
               removal or signature change only at 2.0 after a deprecation
    evolving   documented and supported, may change in a minor with a
               CHANGELOG entry (and an alias where a rename is involved)
    internal   installed because the plugin include closure or a tool
               needs it; no promise

Tiers are assigned by the ordered rules in TIER_RULES (first match wins);
a header no rule names is internal, so a new header cannot be promoted by
accident. Every rule must match at least one header, so a rule that points
at a renamed file fails here instead of silently demoting it.

Two computed sections make the promise honest about its edges. A Stable
header's #include closure reaches headers of lower tiers (operator_base.hpp
reaches the bounded channel; dag.hpp reaches most of runtime/), and the
types it exposes that way are REACHABLE BUT NOT PROMISED:

    [stable-reaches-evolving]   lower-tier headers a Stable header pulls in
    [stable-reaches-internal]

A header entering either set without the manifest being regenerated fails
--check, so a newly exposed internal type is a reviewed event rather than a
side effect. Shrinking these lists is 1.x work; growing them is a contract
change and should be treated as one in review.

Header tiers say WHERE the promise applies. WHICH members are promised is
enumerated by tests/api_conformance/ (compile-only, frozen: additions only).

Usage:
    scripts/gen-public-api-surface.py           regenerate the manifest
    scripts/gen-public-api-surface.py --check   verify it is current (CI,
                                                pre-commit); exit 1 on drift
"""

from __future__ import annotations

import fnmatch
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
INCLUDE_ROOT = REPO_ROOT / "include"
IMPLS_ROOT = REPO_ROOT / "impls"
MANIFEST_PATH = REPO_ROOT / "scripts" / "public-api-surface.txt"

STABLE = "stable"
EVOLVING = "evolving"
INTERNAL = "internal"
TIERS = (STABLE, EVOLVING, INTERNAL)

# Ordered (tier, glob) rules over repo-relative POSIX paths. First match wins.
# The Stable tier is the authoring surface the consumer examples and the
# internals pages teach; Evolving is real but not yet settled. See
# docs/design/011-public-api-tiers.md for the reasoning behind each group.
TIER_RULES: list[tuple[str, str]] = [
    # --- Stable: the fluent pipeline and job/plugin registration -------------
    (STABLE, "include/clink/api/*.hpp"),
    (STABLE, "include/clink/job/register_job.hpp"),
    (STABLE, "include/clink/plugin/plugin.hpp"),
    (STABLE, "include/clink/plugin/install_defaults.hpp"),
    # --- Stable: core types, codecs and the declared-types machinery (009) ---
    (STABLE, "include/clink/core/codec.hpp"),
    (STABLE, "include/clink/core/fields.hpp"),
    (STABLE, "include/clink/core/derived_codec.hpp"),
    (STABLE, "include/clink/core/types.hpp"),
    (STABLE, "include/clink/core/stream_element.hpp"),
    (STABLE, "include/clink/core/record.hpp"),
    (STABLE, "include/clink/core/arrow_batcher.hpp"),
    (STABLE, "include/clink/core/columnar_batcher.hpp"),
    (STABLE, "include/clink/core/pane_info.hpp"),
    # --- Stable: operator bases and the standard operator library ------------
    (STABLE, "include/clink/operators/operator_base.hpp"),
    (STABLE, "include/clink/operators/process_function.hpp"),
    (STABLE, "include/clink/operators/source_operator.hpp"),
    (STABLE, "include/clink/operators/sink_operator.hpp"),
    (STABLE, "include/clink/operators/map_operator.hpp"),
    (STABLE, "include/clink/operators/filter_operator.hpp"),
    (STABLE, "include/clink/operators/flat_map_operator.hpp"),
    (STABLE, "include/clink/operators/key_by_operator.hpp"),
    (STABLE, "include/clink/operators/reduce_operator.hpp"),
    (STABLE, "include/clink/operators/keyed_aggregate_operator.hpp"),
    (STABLE, "include/clink/operators/tumbling_window_operator.hpp"),
    (STABLE, "include/clink/operators/sliding_window_operator.hpp"),
    (STABLE, "include/clink/operators/session_window_operator.hpp"),
    (STABLE, "include/clink/operators/evicting_tumbling_window_operator.hpp"),
    (STABLE, "include/clink/operators/window_trigger.hpp"),
    (STABLE, "include/clink/operators/window_evictor.hpp"),
    (STABLE, "include/clink/operators/window_state.hpp"),
    (STABLE, "include/clink/operators/watermark_assigner_operator.hpp"),
    (STABLE, "include/clink/operators/async_process_function.hpp"),
    (STABLE, "include/clink/operators/async_co_process_function.hpp"),
    (STABLE, "include/clink/operators/async_map_operator.hpp"),
    (STABLE, "include/clink/operators/async_lookup_operator.hpp"),
    (STABLE, "include/clink/operators/scalar_function_registry.hpp"),
    (STABLE, "include/clink/operators/agg_function_registry.hpp"),
    (STABLE, "include/clink/operators/split_by_variant_operator.hpp"),
    (STABLE, "include/clink/operators/throttle_map.hpp"),
    # --- Stable: what an operator is handed, and the local runtime -----------
    (STABLE, "include/clink/runtime/runtime_context.hpp"),
    (STABLE, "include/clink/runtime/timer_service.hpp"),
    (STABLE, "include/clink/runtime/output_tag.hpp"),
    (STABLE, "include/clink/runtime/dead_letter.hpp"),
    (STABLE, "include/clink/runtime/dag.hpp"),
    (STABLE, "include/clink/runtime/local_executor.hpp"),
    (STABLE, "include/clink/runtime/job_config.hpp"),
    (STABLE, "include/clink/checkpoint/checkpoint_barrier.hpp"),
    (STABLE, "include/clink/async/task.hpp"),
    # --- Stable: state ---------------------------------------------------------
    (STABLE, "include/clink/state/keyed_state.hpp"),
    (STABLE, "include/clink/state/broadcast_state.hpp"),
    (STABLE, "include/clink/state/typed_state.hpp"),
    (STABLE, "include/clink/state/state_backend.hpp"),
    (STABLE, "include/clink/state/state_backend_factory.hpp"),
    (STABLE, "include/clink/state/in_memory_state_backend.hpp"),
    (STABLE, "include/clink/state/schema_version.hpp"),
    # --- Stable: time ----------------------------------------------------------
    (STABLE, "include/clink/time/watermark.hpp"),
    (STABLE, "include/clink/time/watermark_strategy.hpp"),
    (STABLE, "include/clink/time/event_time.hpp"),
    (STABLE, "include/clink/time/window_arithmetic.hpp"),
    # --- Stable: connector authoring bases and the built-in connectors -------
    (STABLE, "include/clink/connectors/committing_sink.hpp"),
    (STABLE, "include/clink/connectors/delivery_guarantee.hpp"),
    (STABLE, "include/clink/connectors/capability.hpp"),
    (STABLE, "include/clink/connectors/polling_source.hpp"),
    (STABLE, "include/clink/connectors/cdc_event.hpp"),
    (STABLE, "include/clink/connectors/text_format.hpp"),
    (STABLE, "include/clink/connectors/file_source.hpp"),
    (STABLE, "include/clink/connectors/file_sink.hpp"),
    (STABLE, "include/clink/connectors/file_2pc_sink.hpp"),
    (STABLE, "include/clink/connectors/directory_file_source.hpp"),
    (STABLE, "include/clink/connectors/parquet_source.hpp"),
    (STABLE, "include/clink/connectors/parquet_sink.hpp"),
    (STABLE, "include/clink/connectors/parquet_2pc_sink.hpp"),
    (STABLE, "include/clink/connectors/multi_object_parquet_source.hpp"),
    # --- Stable: CEP pattern API, testing framework, embedding ----------------
    (STABLE, "include/clink/cep/cep.hpp"),
    (STABLE, "include/clink/cep/pattern.hpp"),
    (STABLE, "include/clink/cep/pattern_stream.hpp"),
    (STABLE, "include/clink/test/*.hpp"),
    (STABLE, "include/clink/embed/embedded_engine.hpp"),
    (STABLE, "include/clink/embed/clink.h"),
    # --- Stable: per-connector builders and install entry points -------------
    (STABLE, "impls/*/include/clink/api/*.hpp"),
    (STABLE, "impls/*/include/clink/*/install.hpp"),
    # --- Evolving --------------------------------------------------------------
    (EVOLVING, "include/clink/sql/table_api.hpp"),
    (EVOLVING, "include/clink/sql/catalog.hpp"),
    (EVOLVING, "include/clink/sql/script_runner.hpp"),
    (EVOLVING, "include/clink/embed/flight_sql_server.hpp"),
    (EVOLVING, "include/clink/application/job_submitter.hpp"),
    (EVOLVING, "include/clink/cluster/job_graph.hpp"),
    (EVOLVING, "include/clink/cluster/coordinator.hpp"),
    (EVOLVING, "include/clink/cluster/worker.hpp"),
    (EVOLVING, "include/clink/queryable_state/client.hpp"),
    (EVOLVING, "include/clink/queryable_state/cluster_client.hpp"),
    (EVOLVING, "include/clink/lineage/*.hpp"),
    (EVOLVING, "include/clink/state_processor/*.hpp"),
    (EVOLVING, "include/clink/metrics/counter.hpp"),
    (EVOLVING, "include/clink/metrics/gauge.hpp"),
    (EVOLVING, "include/clink/metrics/histogram.hpp"),
    (EVOLVING, "include/clink/metrics/metrics_registry.hpp"),
    (EVOLVING, "include/clink/metrics/operator_metrics.hpp"),
    (EVOLVING, "include/clink/http/http_server.hpp"),
    (EVOLVING, "include/clink/http/http_client.hpp"),
    (EVOLVING, "include/clink/operators/columnar_*.hpp"),
    (EVOLVING, "include/clink/operators/deadline_*.hpp"),
    (EVOLVING, "include/clink/operators/udf_language_registry.hpp"),
    (EVOLVING, "include/clink/runtime/logging.hpp"),
    (EVOLVING, "include/clink/state/expiring_collection_state.hpp"),
    (EVOLVING, "include/clink/time/alignment_group.hpp"),
    (EVOLVING, "include/clink/async/retry_policy.hpp"),
    (EVOLVING, "include/clink/async/circuit_breaker.hpp"),
    (EVOLVING, "include/clink/connectors/sql_json_builder.hpp"),
    (EVOLVING, "impls/*/include/clink/connectors/*.hpp"),
]
DEFAULT_TIER = INTERNAL

# Generated headers resolve in the build tree; their configure templates are
# the tiered stand-ins (same convention as gen-plugin-abi-surface.py).
GENERATED_HEADERS = {
    "clink/plugin/abi_version.hpp": "include/clink/plugin/abi_version.hpp.in",
    "clink/plugin/abi_surface.hpp": "include/clink/plugin/abi_surface.hpp.in",
}

HEADER_GLOBS = ("*.hpp", "*.hpp.in", "*.h")
INCLUDE_RE = re.compile(r'^\s*#\s*include\s+["<](clink/[^">]+)[">]')

HEADER_COMMENT = """\
# Public API tiers. Generated by scripts/gen-public-api-surface.py; do not
# edit by hand - change the rules in the script and regenerate. Every
# installed header appears exactly once under [stable], [evolving] or
# [internal]; the two [stable-reaches-*] sections list the lower-tier headers a
# Stable header's #include closure exposes (reachable but not promised). A
# diff here IS a change to the 1.x compatibility contract and should be
# reviewed as one. See docs/design/011-public-api-tiers.md.
"""


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def rel(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def impl_include_roots() -> list[Path]:
    if not IMPLS_ROOT.is_dir():
        return []
    roots = []
    for impl_dir in sorted(IMPLS_ROOT.iterdir()):
        inc = impl_dir / "include"
        if inc.is_dir():
            roots.append(inc)
    return roots


def collect_headers() -> list[Path]:
    files: set[Path] = set()
    roots = [INCLUDE_ROOT, *impl_include_roots()]
    for root in roots:
        for pattern in HEADER_GLOBS:
            files.update(p for p in root.rglob(pattern) if p.is_file())
    return sorted(files)


def assign_tiers(headers: list[Path]) -> tuple[dict[Path, str], list[str]]:
    tiers: dict[Path, str] = {}
    matched_rules: set[int] = set()
    for h in headers:
        r = rel(h)
        tier = DEFAULT_TIER
        for idx, (rule_tier, pattern) in enumerate(TIER_RULES):
            if fnmatch.fnmatchcase(r, pattern):
                tier = rule_tier
                matched_rules.add(idx)
                break
        tiers[h] = tier
    errors = [
        f"tier rule matches no header (renamed or removed file?): "
        f"({rule_tier}, {pattern!r})"
        for idx, (rule_tier, pattern) in enumerate(TIER_RULES)
        if idx not in matched_rules
    ]
    return tiers, errors


def resolve_include(inc: str) -> Path | None:
    """Map a `clink/...` include path to the tracked file that stands for it."""
    if inc in GENERATED_HEADERS:
        tmpl = REPO_ROOT / GENERATED_HEADERS[inc]
        return tmpl if tmpl.is_file() else None
    candidate = INCLUDE_ROOT / inc
    if candidate.is_file():
        return candidate
    for root in impl_include_roots():
        candidate = root / inc
        if candidate.is_file():
            return candidate
    return None


def closure_of(entries: list[Path]) -> tuple[set[Path], list[str]]:
    """Transitive #include closure over the tracked headers."""
    seen: set[Path] = set(entries)
    queue = list(entries)
    errors: list[str] = []
    while queue:
        f = queue.pop()
        for line in read_text(f).splitlines():
            m = INCLUDE_RE.match(line)
            if not m:
                continue
            resolved = resolve_include(m.group(1))
            if resolved is None:
                errors.append(
                    f'{rel(f)}: unresolvable include "{m.group(1)}" '
                    "(not under include/ or impls/*/include/)"
                )
                continue
            if resolved not in seen:
                seen.add(resolved)
                queue.append(resolved)
    return seen, errors


def render_manifest(tiers: dict[Path, str], reaches: dict[str, set[Path]]) -> str:
    lines = [HEADER_COMMENT]
    for tier in TIERS:
        lines.append(f"[{tier}]")
        lines.extend(sorted(rel(h) for h, t in tiers.items() if t == tier))
        lines.append("")
    for tier in (EVOLVING, INTERNAL):
        lines.append(f"[stable-reaches-{tier}]")
        lines.extend(sorted(rel(h) for h in reaches[tier]))
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    check = "--check" in sys.argv[1:]

    headers = collect_headers()
    tiers, errors = assign_tiers(headers)
    if errors:
        for e in errors:
            print(f"gen-public-api-surface: {e}", file=sys.stderr)
        return 1

    stable_headers = sorted(h for h, t in tiers.items() if t == STABLE)
    reached, closure_errors = closure_of(stable_headers)
    if closure_errors:
        for e in closure_errors:
            print(f"gen-public-api-surface: {e}", file=sys.stderr)
        return 1
    reaches = {
        EVOLVING: {h for h in reached if tiers.get(h) == EVOLVING},
        INTERNAL: {h for h in reached if tiers.get(h) == INTERNAL},
    }

    rendered = render_manifest(tiers, reaches)

    if check:
        current = MANIFEST_PATH.read_text() if MANIFEST_PATH.is_file() else ""
        if current != rendered:
            cur_lines = set(current.splitlines())
            new_lines = set(rendered.splitlines())
            for missing in sorted(new_lines - cur_lines):
                print(f"gen-public-api-surface: missing: {missing}", file=sys.stderr)
            for stale in sorted(cur_lines - new_lines):
                print(f"gen-public-api-surface: stale:   {stale}", file=sys.stderr)
            print(
                "gen-public-api-surface: scripts/public-api-surface.txt is out of "
                "date; run scripts/gen-public-api-surface.py and review the diff - "
                "a header changing tier, or a Stable header newly reaching a "
                "lower-tier one, is a change to the 1.x compatibility contract.",
                file=sys.stderr,
            )
            return 1
        print("gen-public-api-surface: manifest is current.")
        return 0

    MANIFEST_PATH.write_text(rendered)
    counts = {tier: sum(1 for t in tiers.values() if t == tier) for tier in TIERS}
    print(
        f"gen-public-api-surface: wrote {rel(MANIFEST_PATH)} "
        f"({counts[STABLE]} stable, {counts[EVOLVING]} evolving, "
        f"{counts[INTERNAL]} internal; stable reaches "
        f"{len(reaches[EVOLVING])} evolving + {len(reaches[INTERNAL])} internal)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
