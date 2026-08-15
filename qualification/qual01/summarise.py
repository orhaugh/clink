#!/usr/bin/env python3
"""Render a campaign's evidence directory into the standard qualification
summary. Reads only retained artefacts (verdict.json, chaos.jsonl,
progress.json, job-status.json, provenance where present) and states
plainly when a field has no evidence behind it - a summary that invents a
number is worse than one with a gap in it.
"""
import argparse
import collections
import json
import os
import sys


def load_json(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--duration-h", default="?")
    ap.add_argument("--profile", default="?")
    args = ap.parse_args()

    d = args.out_dir
    verdict = load_json(os.path.join(d, "verdict.json"), {})
    progress = load_json(os.path.join(d, "progress.json"), {})
    spec = load_json(os.path.join(d, "progress.json.spec"), {})
    inventory = load_json(os.path.join(d, "inventory.json"), {})

    faults = collections.Counter()
    chaos_path = os.path.join(d, "chaos.jsonl")
    recovery_timeouts = 0
    if os.path.exists(chaos_path):
        with open(chaos_path) as f:
            for line in f:
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                fault = entry.get("fault", "?")
                faults[fault.split(":", 1)[0]] += 1
                if fault == "healthy_checkpoint_timeout":
                    recovery_timeouts += 1

    produced = sum(int(v) for v in (progress.get("produced_high") or {}).values())

    missing = verdict.get("missing")
    duplicate = verdict.get("duplicate")
    conflicting = verdict.get("conflicting")
    incorrect = verdict.get("incorrect")
    foreign = verdict.get("foreign")
    counted = [missing, duplicate, conflicting, incorrect, foreign]
    have_verdict = verdict.get("final") and all(c is not None for c in counted)
    result = ("PASS" if have_verdict and not any(counted) and not recovery_timeouts
              else "FAIL" if have_verdict or recovery_timeouts
              else "INCONCLUSIVE (no final verdict written)")

    def num(x):
        return "no evidence" if x is None else str(x)

    print(f"# QUAL-01 Kafka Exactly-Once Campaign\n")
    print(f"Run ID: `{args.run_id}`  ")
    print(f"Duration (configured): {args.duration_h}h  ")
    print(f"Chaos profile: {args.profile}  ")
    print(f"Hosts: {len(inventory.get('hosts', []))}\n")

    print("## Workload\n")
    if spec:
        print(f"- Input events produced: {produced:,}")
        print(f"- Partitions: {spec.get('partitions')}, keys: {spec.get('keys'):,}"
              if spec.get("keys") else f"- Partitions: {spec.get('partitions')}")
        print(f"- Window: {spec.get('window_ms')}ms tumbling, "
              f"event-time jitter <= {spec.get('max_jitter_ms')}ms")
        print(f"- Generator seed: {spec.get('seed')} (the oracle is this seed, "
              f"not a recording)")
    else:
        print("- no generator spec retained")
    print(f"- Output records observed by the verifier: "
          f"{num(verdict.get('output_records'))}\n")

    print("## Faults injected\n")
    if faults:
        for name, count in sorted(faults.items(), key=lambda kv: -kv[1]):
            print(f"- {name}: {count}")
    else:
        print("- none recorded")
    print()

    print("## Correctness (independent oracle, read_committed)\n")
    print(f"- Windows judged: {num(verdict.get('evaluated_windows'))}")
    print(f"- Windows fully correct: {num(verdict.get('correct_windows'))}")
    print(f"- Missing: {num(missing)}")
    print(f"- Duplicates (same value twice): {num(duplicate)}")
    print(f"- Conflicting (same key/window, different values): {num(conflicting)}")
    print(f"- Incorrect aggregates: {num(incorrect)}")
    print(f"- Foreign results: {num(foreign)}")
    print(f"- Recovery timeouts (no checkpoint after a fault): {recovery_timeouts}\n")

    if verdict.get("defect_sample"):
        print("## Defect sample\n")
        print("```json")
        print(json.dumps(verdict["defect_sample"][:10], indent=2))
        print("```\n")

    print(f"## Result\n\n**{result}**\n")
    if not have_verdict:
        print("The verifier did not write a final verdict, so this campaign "
              "proves nothing about correctness. Treat the run as void and "
              "re-run: an incomplete campaign is not a weak pass.\n")

    print("## Caveats\n")
    print("- Correctness is asserted only for the workload, fault profile and "
          "duration above. Nothing here extends to untested connectors, "
          "larger state, or longer runs.")
    print("- The verdict counts closed windows whose full input was produced "
          "and whose grace period elapsed; windows still open at shutdown are "
          "not judged either way.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
