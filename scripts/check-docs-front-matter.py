#!/usr/bin/env python3
"""Check that every tracked page's YAML front matter parses.

MkDocs reads the `---`-delimited block at the top of a page for its `title`
and `description`. When that block is not valid YAML, MkDocs does not fail:
it logs a warning, leaves the block in the document, and renders it as body
text. The page then opens with a literal "title: ... description: ..."
paragraph, loses its custom browser title, and falls back to the site-wide
meta description - so the page-specific description a guide was written for
never reaches search results. That is what happened to the Flink comparison
guide, and nothing caught it before publication.

The usual cause is a colon. An unquoted YAML scalar may not contain ": ",
because YAML reads it as a nested mapping:

    description: Compare A and B for stream processing: native embedding   # broken
    description: "Compare A and B for stream processing: native embedding" # fine

This check mirrors what MkDocs does (mkdocs/utils/meta.py): the same
delimiter regex, the same parser, and the same requirement that the result be
a mapping. A page whose front matter would be dropped fails here instead of
publishing silently.

Usage:
    scripts/check-docs-front-matter.py
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# The delimiter regex MkDocs itself applies (mkdocs/utils/meta.py).
YAML_RE = re.compile(r"^-{3}[ \t]*\n(.*?\n)(?:\.{3}|-{3})[ \t]*\n", re.UNICODE | re.DOTALL)


def tracked_markdown() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "*.md"], cwd=REPO_ROOT, capture_output=True, text=True, check=True
    ).stdout.split()
    return [REPO_ROOT / p for p in out]


def main() -> int:
    try:
        import yaml
    except ImportError:
        # PyYAML ships with mkdocs-material, so the docs workflow always has
        # it and always enforces this. A local checkout without it says so
        # rather than passing quietly on a check it did not run.
        print(
            "check-docs-front-matter: PyYAML is not installed, so front matter was NOT checked "
            "here. Install it (pip install pyyaml) or rely on the docs workflow, which always "
            "runs this check.",
            file=sys.stderr,
        )
        return 0

    failures: list[str] = []
    checked = 0
    for path in tracked_markdown():
        text = path.read_text(encoding="utf-8").lstrip("\n")
        if not text.startswith("---"):
            continue
        rel = path.relative_to(REPO_ROOT).as_posix()
        match = YAML_RE.match(text)
        if not match:
            failures.append(
                f"{rel}: starts with '---' but has no closing '---' delimiter, so MkDocs "
                f"renders the block as body text instead of reading it as front matter"
            )
            continue
        checked += 1
        try:
            meta = yaml.safe_load(match.group(1))
        except Exception as e:  # noqa: BLE001 - any YAML error is the finding
            reason = str(e).splitlines()[0]
            failures.append(
                f"{rel}: front matter is not valid YAML ({reason}). MkDocs would drop it and "
                f"render it as body text. A value containing ': ' must be quoted."
            )
            continue
        if not isinstance(meta, dict):
            failures.append(
                f"{rel}: front matter parses as {type(meta).__name__}, not a mapping of "
                f"key: value pairs, so MkDocs would ignore it"
            )

    if failures:
        for f in failures:
            print(f"check-docs-front-matter: {f}", file=sys.stderr)
        print(
            "check-docs-front-matter: a page whose front matter does not parse publishes with "
            "the raw block as its opening paragraph and loses its title and meta description. "
            "Fix the YAML rather than removing the front matter.",
            file=sys.stderr,
        )
        return 1

    print(f"check-docs-front-matter: {checked} page(s) with front matter parse cleanly.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
