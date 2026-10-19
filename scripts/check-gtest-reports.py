#!/usr/bin/env python3
"""Gate a live test run on its googletest JSON reports.

A live suite skips when its service or binary is missing, and gtest counts a
skip as a pass, so a run whose server never came up reads green. This fails
the run instead:

  * a report that is missing or does not parse;
  * any test that failed or was skipped;
  * an --expect PATTERN=N with fewer than N passing tests whose "Suite.Name"
    matches PATTERN (fnmatch), so a filter that selects nothing, or a suite
    renamed out from under its gate, fails too.

  check-gtest-reports.py --expect 'PostgresCdcContractLive.*=19' report.json ...

Exit 0 when every report is clean and every expectation met, 1 otherwise.
"""

import argparse
import fnmatch
import json
import sys


def tests_of(report):
    for suite in report.get("testsuites", []):
        for case in suite.get("testsuite", []):
            yield f"{suite.get('name', '?')}.{case.get('name', '?')}", case


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("reports", nargs="+", help="googletest --gtest_output=json files")
    parser.add_argument(
        "--expect",
        action="append",
        default=[],
        metavar="PATTERN=N",
        help="at least N passing tests whose Suite.Name matches PATTERN",
    )
    args = parser.parse_args()

    problems = []
    passed = []
    total = 0
    for path in args.reports:
        try:
            with open(path, encoding="utf-8") as f:
                report = json.load(f)
        except (OSError, ValueError) as e:
            problems.append(f"{path}: no readable report ({e})")
            continue
        for name, case in tests_of(report):
            total += 1
            if case.get("result") == "SKIPPED":
                problems.append(f"{name}: skipped")
            elif case.get("failures"):
                problems.append(f"{name}: failed")
            elif case.get("result") != "COMPLETED" or case.get("status") != "RUN":
                problems.append(f"{name}: did not run ({case.get('status')}/{case.get('result')})")
            else:
                passed.append(name)

    for spec in args.expect:
        pattern, _, count = spec.rpartition("=")
        if not pattern or not count.isdigit():
            problems.append(f"--expect {spec!r}: not of the form PATTERN=N")
            continue
        matched = sum(1 for n in passed if fnmatch.fnmatchcase(n, pattern))
        if matched < int(count):
            problems.append(f"{pattern}: {matched} passing test(s), {count} required")

    print(f"check-gtest-reports: {total} test(s) in {len(args.reports)} report(s), "
          f"{len(passed)} passed")
    for p in problems:
        print(f"  {p}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
