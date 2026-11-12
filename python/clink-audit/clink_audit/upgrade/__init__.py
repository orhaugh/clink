"""Upgrade validation: can a candidate build read retained state and reproduce
checked results?

`record` runs an explicitly supplied baseline build on the one supported job
(examples/upgrade-check/upgrade_job.cpp) over a deterministic input, takes a
savepoint while windows are open and timers are pending, and keeps it as an
evidence set: the savepoint as a checkpoint dataset, the input, the output the
baseline committed before the savepoint, and an independent reference's
expected output, every file bound by its SHA-256 in `evidence.json`, whose own
digest is recorded outside the set.

`check` verifies that evidence before anything runs, then restores the
baseline and the candidate, each from its own scratch copy, through the
engine's restore path (`clink run --restore-from-dir`), takes a savepoint of
each restored job before it reads anything new, delivers the same
continuation, and reports separately:

    evidence integrity and readability
    build identities and capabilities
    schema compatibility (the candidate's own check-savepoint)
    the restore (accepted, and the state restored as the evidence holds it)
    recovery (the continuation completed)
    behaviour (the candidate's output and final state against the baseline's)
    the independent reference (the candidate's output against it)
    that the evidence is unchanged afterwards

It passes only when every required check completed and passed. What it
establishes is compatibility for this job, this evidence and this
configuration, not that every job upgrades.
"""

EVIDENCE_FORMAT = "clink-upgrade-evidence"
EVIDENCE_VERSION = 1
REPORT_FORMAT = "clink-upgrade-report"
REPORT_VERSION = 1
TEST_FORMAT = "clink-upgrade-test"
TEST_VERSION = 1

EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_USAGE = 2
EXIT_NOT_ESTABLISHED = 3
