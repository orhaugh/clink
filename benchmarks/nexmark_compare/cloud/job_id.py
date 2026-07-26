#!/usr/bin/env python3
"""Read the job id out of clink_submit_sql's output.

clink job ids are small INTEGERS, printed inside a JSON line. The split rig first
grepped for an 8+ character hex token (Flink's id shape), found nothing, and
declared a SUCCESSFUL submit failed - leaving the job running with every slot held,
so the next submit failed for real and blamed the slots.
"""
import json
import sys

for line in sys.stdin:
    line = line.strip()
    if not line.startswith("{"):
        continue
    try:
        d = json.loads(line)
    except ValueError:
        continue
    jid = d.get("job_id")
    if jid not in (None, ""):
        print(jid)
        break
