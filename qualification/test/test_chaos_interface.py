#!/usr/bin/env python3
"""Every campaign driver's chaos.py invocation must parse against the
controller that will actually run.

QUAL-02's driver carried `--coordinator` and `--out` from an earlier
chaos.py; the rewritten controller (fired-proof, coverage-first) renamed
both. argparse would have exited 2 at chaos start - about twenty-five
minutes into a paid rig, after the database, stack and pipeline were all
up. Interface drift between a campaign and the shared controller is a
class, not an incident, so it is pinned here for every campaign at once.
"""
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
QUAL = HERE.parent

failures = []

chaos_src = (QUAL / "chaos" / "chaos.py").read_text()
known = set(re.findall(r'add_argument\("(--[a-z-]+)"', chaos_src))
required = set(re.findall(r'add_argument\("(--[a-z-]+)"[^)]*required=True', chaos_src))
store_true = set(re.findall(r'add_argument\("(--[a-z-]+)"[^)]*action="store_true"', chaos_src))
if not known:
    print("FAIL: could not extract chaos.py's argument surface")
    sys.exit(1)

for campaign in sorted(QUAL.glob("qual*/campaign.sh")):
    text = campaign.read_text()
    # The chaos launch is a start_on_host line continuing over backslash
    # newlines; splice those, then take everything from `chaos.py` to the
    # closing quote of that command.
    spliced = text.replace("\\\n", " ")
    # The LAUNCH line, not the to_host copy of the script itself.
    m = re.search(r'python3 chaos\.py([^"]*)', spliced)
    if not m:
        continue  # a campaign that does not launch chaos has nothing to drift
    used = set(re.findall(r"(--[a-z-]+)", m.group(1)))
    unknown = sorted(used - known)
    if unknown:
        failures.append(f"{campaign}: uses flag(s) chaos.py does not accept: {unknown}")
    missing_required = sorted(required - used)
    if missing_required:
        failures.append(f"{campaign}: omits required chaos.py flag(s): {missing_required}")
    # Value-taking flags must not be last-with-no-value; cheap shape check:
    # every non-store_true flag must be followed by a non-flag token.
    tokens = m.group(1).split()
    for i, tok in enumerate(tokens):
        if tok in known and tok not in store_true:
            if i + 1 >= len(tokens) or tokens[i + 1].startswith("--"):
                failures.append(f"{campaign}: {tok} has no value")

if failures:
    print("chaos-interface drift:")
    for f in failures:
        print("  " + f)
    print(f"\n{len(failures)} failure(s)")
    sys.exit(1)

print(f"chaos interface: every campaign invocation parses against chaos.py "
      f"({len(known)} known flags)")
sys.exit(0)
