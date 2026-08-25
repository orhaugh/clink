#!/usr/bin/env python3
"""No em dashes in tracked prose.

The repository's voice rule is a spaced hyphen or a restructured
sentence, never an em dash. That is easy to state and easy to lose: em
dashes reached the published site in page titles and the site name,
where nobody rereading body copy would look, and sat there through
several rounds of doc edits.

Scope is deliberately narrow - tracked Markdown plus mkdocs.yml - so it
gates prose a reader sees without touching test fixtures, sample data,
or anything where a literal em dash is the point.

Exit 0 when clean, 1 otherwise, naming file, line and the offending
text.
"""
import os
import subprocess
import sys

EM_DASH = "—"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def tracked_files():
    out = subprocess.run(
        ["git", "ls-files", "*.md", "mkdocs.yml"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return [f for f in out.stdout.split("\n") if f]


def main():
    hits = []
    for rel in tracked_files():
        path = os.path.join(ROOT, rel)
        try:
            with open(path, encoding="utf-8") as fh:
                for n, line in enumerate(fh, 1):
                    if EM_DASH in line:
                        hits.append((rel, n, line.strip()[:110]))
        except (OSError, UnicodeDecodeError):
            continue

    if hits:
        sys.stderr.write(
            "check-no-em-dashes: em dash found in tracked prose.\n"
            "Use a spaced hyphen ( - ) or restructure the sentence.\n\n"
        )
        for rel, n, text in hits:
            sys.stderr.write(f"  {rel}:{n}: {text}\n")
        sys.stderr.write(f"\n{len(hits)} line(s).\n")
        return 1

    print(f"check-no-em-dashes: {len(tracked_files())} tracked file(s) clean.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
