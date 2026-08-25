#!/usr/bin/env python3
"""The published page is the AUTHORITY for a campaign's status.

qualification-plan.json and docs/qualification/ were two records of the
same fact and drifted apart: ten of twelve campaigns sat at "planned" or
"in_progress" in the JSON while their pages had been published as green.
Nothing was wrong with the engine; the metadata simply contradicted the
evidence, and conflicting status is worse than none - a reader cannot
tell which record is stale, so neither can be trusted.

One source is authoritative, and it is the page, because the publication
contract already ties a page to a completed campaign:

    docs/qualification/ carries a campaign page ONLY once that campaign
    is fully fixed and empirically verified green with useful results.

That makes the invariant exact, and this gate enforces it:

  * a campaign WITH a published page must be `completed` in the plan,
    and must record the engine revision it was qualified at;
  * a campaign WITHOUT a page must NOT be `completed` - claiming a
    finished campaign with nothing published is the drift running the
    other way;
  * where the published index names the engine revision, the plan's
    result_revision must be that same revision. A campaign qualified at
    one commit and recorded against another is a false provenance
    claim, which is the one thing an evidence trail cannot survive.

The plan file stays the record of INTENT (priorities, scope, what is
still planned); the page is the record of RESULT. Nothing here judges
whether a campaign should have been published - only that the two
records agree.

Exit 0 when consistent, 1 otherwise, naming every disagreement and the
edit that resolves it.
"""
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
PLAN = os.path.join(ROOT, "qualification-plan.json")
DOCS = os.path.join(ROOT, "docs", "qualification")
INDEX = os.path.join(DOCS, "README.md")


def published_pages():
    """{QUAL-NN -> filename} for every campaign page on disk."""
    out = {}
    if not os.path.isdir(DOCS):
        return out
    for name in sorted(os.listdir(DOCS)):
        m = re.match(r"^qual-(\d+)-.*\.md$", name)
        if m:
            out[f"QUAL-{int(m.group(1)):02d}"] = name
    return out


def indexed_campaigns():
    """{QUAL-NN -> engine sha or None} from the published index table.

    A page that exists but is not linked from the index is unreachable
    for a reader, so it does not count as published.
    """
    out = {}
    if not os.path.exists(INDEX):
        return out
    with open(INDEX) as fh:
        for line in fh:
            m = re.search(r"\[QUAL-(\d+)\]\(qual-\d+-[^)]+\)", line)
            if not m:
                continue
            key = f"QUAL-{int(m.group(1)):02d}"
            sha = re.search(r"engine `([0-9a-f]{7,40})`", line)
            out[key] = sha.group(1) if sha else None
    return out


def plan_items():
    with open(PLAN) as fh:
        doc = json.load(fh)
    return {
        it["id"]: it for it in doc.get("items", []) if it.get("id", "").startswith("QUAL-")
    }


def main():
    pages = published_pages()
    indexed = indexed_campaigns()
    items = plan_items()
    problems = []

    orphan_pages = sorted(set(pages) - set(indexed))
    for cid in orphan_pages:
        problems.append(
            f"{cid}: {pages[cid]} exists but the index does not link it. "
            f"Add a row to docs/qualification/README.md or remove the page."
        )

    for cid in sorted(set(indexed) | set(items)):
        item = items.get(cid)
        is_published = cid in indexed
        if item is None:
            problems.append(
                f"{cid}: published but absent from qualification-plan.json. "
                f"Add an item for it, or the plan stops describing the programme."
            )
            continue
        status = item.get("status", "")
        rev = item.get("result_revision") or ""
        if is_published and status != "completed":
            problems.append(
                f"{cid}: page published ({pages.get(cid, 'linked in the index')}) but the plan "
                f"says status='{status}'. A page exists only for a green campaign, so set "
                f"status='completed'."
            )
        if is_published and not rev:
            problems.append(
                f"{cid}: published with no result_revision in the plan. Record the engine "
                f"revision the campaign was qualified at."
            )
        if not is_published and status == "completed":
            problems.append(
                f"{cid}: the plan says completed but nothing is published for it. Either "
                f"publish the page or correct the status - a completed campaign with no "
                f"evidence is the claim this programme exists to avoid."
            )
        want = indexed.get(cid)
        if is_published and want and rev and not (
            want.startswith(rev) or rev.startswith(want)
        ):
            problems.append(
                f"{cid}: the index says engine `{want}` and the plan records "
                f"result_revision '{rev}'. They must name the same commit."
            )

    if problems:
        sys.stderr.write(
            "check-qualification-status: the plan and the published pages disagree.\n"
            "The published page is authoritative; the plan must follow it.\n\n"
        )
        for p in problems:
            sys.stderr.write(f"  - {p}\n")
        sys.stderr.write(f"\n{len(problems)} disagreement(s).\n")
        return 1

    print(
        f"check-qualification-status: {len(indexed)} published campaign(s) agree with "
        f"qualification-plan.json."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
