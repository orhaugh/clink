# Runbook

What to do when one of clink's alerts fires.

The alert rules in `deploy/prometheus/clink-alerts.yaml` ship with the project; the
response did not, so every operator worked it out independently. Each section below
covers one alert: what it means, what to look at first, and what the likely causes
are in order of how often they turn out to be the answer.

Two things apply throughout.

**A red alert on a busy machine is often the machine.** Several of clink's failure
signatures - checkpoint duration, restart loops, subtask failures - move with load.
Before treating an alert as a defect, check whether something else on the host is
consuming the cores. This is not hypothetical: a full day of this project's own
hardening work went into diagnosing thirteen "failures" that were an unrelated
process holding seven of twelve cores.

**The coordinator's log is the primary source.** Every recovery decision it takes is
logged with the job id and the reason: `awaiting_restart (attempt N/M)`,
`worker lost`, `replanned job_id=...`, `refusing to rewrite active-leader.json`. If
the log is silent about a job that is misbehaving, that silence is itself the
finding - it means the coordinator did not observe the event, which is a different
class of problem from handling it badly.

---

The Grafana dashboard at `deploy/grafana/clink-dashboard.json` charts the same
metrics these procedures reference, section for section, and is gated the same
way as the alert rules: every metric a panel queries is checked against the
metric constants by `tests/test_dashboard.cpp`, so a renamed metric fails the
build rather than leaving a panel silently empty mid-incident. Import it and
point its data-source variable at the Prometheus that scrapes the cluster.

## ClinkCheckpointsStalled

`time() - clink_ckpt_last_completed_unix_seconds > 300`

No checkpoint has completed for five minutes. Until one does, the job's recovery
point is frozen: a failure now replays from wherever the last completed checkpoint
was, and every record since is reprocessed.

**Look at:** the coordinator log for `checkpoint <id> of job <id> FAILED`, which
names the subtasks that could not snapshot.

**Likely causes, in order:**

1. **One subtask cannot write.** The failure line names it. Check the disk where
   that worker's state lives (see `ClinkDiskFillingWhereStateLives`) and its
   permissions. A checkpoint completes only when every subtask acks, so one stuck
   subtask stalls the whole job.
2. **A subtask is wedged, not failing.** No FAILED line at all, just silence. The
   barrier has not reached it. Check `clink_operator_input_queue_depth` for a
   backpressured operator upstream of the quiet one.

   One cause of this used to be invisible: a source whose barrier could not be sent
   acked the checkpoint as successful anyway, so the component that knew the send had
   failed reported success and the checkpoint simply stopped. That now fails fast with
   `source could not deliver the barrier to every downstream channel` (F69). Silence
   with no such line means the barrier did leave the source.
3. **The interval is longer than you think.** `--checkpoint-interval-ms` is per job.
   A job configured at 600000 will trip a 300s alert permanently and correctly.

**Do not** restart the job to "unstick" it before reading the FAILED line. A restart
rolls back to the last completed checkpoint, which is exactly the data you are
trying not to lose.

---

## ClinkCheckpointsFailing

Checkpoints are being attempted and none are completing. Same investigation as
`ClinkCheckpointsStalled`, but the coordinator is definitely seeing attempts, so
skip straight to the FAILED lines and the named subtasks.

**If a restore fails on integrity, read the whole message.** It now names the older
checkpoint in the same directory that still verifies, or says plainly that none does.
That older one is a recovery option, not an automatic one: the failing checkpoint was
marked COMPLETED, so a sink may already have committed output for it, and restoring
further back replays everything after it. Check the sinks before taking it.

Note that `--checkpoint-num-retained` defaults to 1, which means there is usually no
older checkpoint to name. Raising it is what buys a fallback.

One specific cause worth knowing: a checkpoint whose payload and sidecar disagree is
refused at restore, not at write. If the log shows
`checkpoint-N.snap is X bytes, sidecar declares Y`, something wrote that file twice.
That was a real defect in the rescale path (F59) where a restore staged state into a
directory another subtask was reading; if it appears outside a rescale, treat it as
a storage-layer problem and check whether two processes share a state directory.

---

## ClinkCheckpointDurationP99High

p99 checkpoint duration above 20s. A warning, not an outage: checkpoints are
completing, just slowly.

**Look at:** `clink_state_snapshot_bytes` alongside the duration. If bytes are
growing steadily, this is state growth, not a storage problem.

**Likely causes:**

1. **Unbounded keyed state.** A GROUP BY or a window with no TTL accumulates
   indefinitely. The SQL planner refuses the clearest cases at submission, but a
   hand-built DAG can still do it. Set a state TTL.
2. **Slow durable storage.** Compare against `clink_state_snapshot_bytes / duration`
   for an effective write rate and check it against the device.
3. **Checkpoint interval too short for the state size.** If a snapshot takes 20s and
   the interval is 10s, checkpoints queue behind each other and the p99 climbs
   without bound.

---

## ClinkWorkerLost

`increase(clink_coordinator_workers_lost_total[10m]) > 0`

A worker missed its heartbeats and was declared lost. Its jobs roll back to their
last completed checkpoint and redeploy onto the survivors.

**Look at:** whether the job recovered. The coordinator logs
`awaiting_restart (attempt N/M)` and then a redeploy. If both appear and the job is
running, the system did its job and this alert is informational.

**If the job did NOT recover, check first whether the worker came straight back.**
A worker that dies and is restarted quickly - which is what a container
orchestrator does by default - used to leave the job hung: the coordinator retired
the previous session and never restarted the subtasks, because the watchdog cannot
declare a worker lost while it is alive and heartbeating under the same id. That is
fixed (F64), but the signature is worth recognising: the log shows
`worker=<id> re-registered; previous session retired` and then nothing.

**Capacity:** if the lost worker's slots are needed and no survivor has room, the
redeploy cannot be placed. See `ClinkNoSlotCapacity`.

---

## ClinkJobRestartLoop

`increase(clink_coordinator_job_restarts_total[30m]) > 5`

A job is restarting repeatedly. Each restart rolls back to the last completed
checkpoint, so a loop means the job is making no forward progress and reprocessing
the same records.

**Look at:** the coordinator log for the restart reason. It differs per attempt and
the pattern matters more than any single line.

**Likely causes:**

1. **A deterministic subtask failure.** The job restores, processes to the same
   record, and fails identically. The subtask error is in the log with the job id.
   A restart budget will not fix this; the job needs a code or data fix.
2. **An unstable worker.** The restarts follow worker losses rather than subtask
   errors. Correlate with `clink_coordinator_workers_lost_total` and treat it as an
   infrastructure problem.
3. **The budget is masking it.** `--max-restarts-on-worker-loss` bounds the attempts.
   A high budget turns a hard failure into a long loop; the job would be more
   diagnosable failing fast.

---

## ClinkNoSlotCapacity

Every slot in the cluster is in use. New submissions are refused and a redeploy
after a worker loss has nowhere to go, which turns a recoverable failure into a
stuck job.

**Look at:** `/api/v1/workers` for slot capacity and usage per worker.

**Note on sizing for recovery:** a cluster sized so that the job exactly fits cannot
survive a worker loss - the survivors have no room for the redeploy. Recovery needs
one worker's worth of spare capacity, not just enough to run.

---

## ClinkFencedFrames

`increase(clink_worker_fenced_frames_total[10m]) > 0`

Workers are rejecting frames from a coordinator whose epoch is below the one they
are bound to. The fencing is working - a superseded coordinator is being ignored -
but a healthy cluster does not produce these continuously.

**A burst around a failover is expected.** The old leader's in-flight frames arrive
after the new one has taken over and are correctly refused.

**A steady trickle is not.** It means a superseded coordinator is still running and
still trying to drive the cluster. Check for two live coordinators sharing an HA
directory, and confirm the loser has actually exited.

**The dangerous variant this cannot see:** if the HA directory is on a filesystem
that does not honour POSIX write locks, both coordinators believe they lead and each
announces a higher epoch - so neither is fenced and this metric stays at zero. A
coordinator now proves the lock works at start-up and refuses leadership if it does
not (F57), logging `refusing to stand for leadership`. Bind mounts, 9p/virtiofs
shares and NFS mounted `nolock` are the usual offenders. The check covers one host;
it cannot detect a mount that honours locks locally but not between hosts, which is
the NFS mode that matters most for multi-node. Use the etcd coordinator there.

---

## ClinkProtocolMismatch

A peer was rejected for an incompatible protocol version. Almost always a partial
upgrade: some binaries at the new version, some at the old.

**Look at:** the coordinator log, which names the peer and both versions.

There is no CLI that reports the negotiated version, so the log is the only source
today.

---

## ClinkRestoreDiscardedKeyedState

`increase(clink_state_restore_keys_dropped_total[15m]) > 0`

A restore loaded keyed entries whose key group falls outside the subtask's assigned
range and discarded them.

**During a rescale this is correct and expected.** When an operator resizes, each new
subtask reads a parent snapshot covering a wider key range than it owns and narrows
it. The log says as much: `during a rescale that is correct`.

**Outside a rescale it is not.** It means a subtask restored a snapshot that does not
belong to it, and the records for those keys are gone from its state. Check whether
the job was rescaled recently and restarted from a PRE-rescale checkpoint - subtask
directories are addressed by a job-global index, and that index is not stable across
a topology change. That specific hole is fixed (F63), but the class is worth
recognising: if state appears in the wrong subtask, suspect index reuse.

---

## A job refuses to start with "refusing to restore subtask N with empty state"

Not an alert - a hard refusal at deploy. The coordinator named a completed checkpoint
to restore from, and that subtask's snapshot file is not in the checkpoint directory.

**This is the engine declining to lose your state quietly.** It used to bring the
subtask up holding nothing while its peers restored fully, which resumes a job that
looks healthy with one operator's state gone.

**Look at:** the path in the message, and `clink checkpoint-verify` over the
checkpoint directory. A completed checkpoint should have a snapshot for every
participant, and the COMPLETED marker records which subtasks those were.

**Likely causes, in order:**

1. **The directory was pruned by hand.** Deleting snapshot files to reclaim disk is
   the usual way one goes missing. Drop whole checkpoints oldest first instead, and
   see `ClinkDiskFillingWhereStateLives`.
2. **Storage lost it.** A volume that was remounted, restored from a backup taken
   mid-checkpoint, or is silently dropping writes.
3. **The operator is genuinely new.** A stateful operator added to an existing job has
   no prior state. That case is legitimate and is what
   `CLINK_ALLOW_MISSING_RESTORE_STATE=1` is for.

**Do not set the override to make a restore start.** It converts the refusal back into
silent state loss for every subtask, not just the new one. Confirm which subtask is
missing and why first; if the answer is not case 3, the checkpoint is not safe to
restore from and an older complete one is the better recovery point.

## ClinkSubtaskFailures

Subtasks are failing on a worker. The job restarts if it has budget; if not, it
fails.

**Look at:** the worker log for the subtask's own error. The coordinator's line
gives the job and subtask; the cause is on the worker.

**Likely causes:** an exception escaping user code in an operator, a connector
losing its external system, or a state backend that cannot read or write. The three
look identical from the coordinator and completely different on the worker.

---

## ClinkMalformedFrames

The coordinator or a worker rejected a frame it could not parse. Frames are
length-prefixed and size-capped, so a malformed one is refused rather than
allocated for.

**Likely causes:** a non-clink client connecting to the control port, a partial
upgrade (see `ClinkProtocolMismatch`), or a network middlebox altering the stream.
Sustained malformed frames from one peer address with no upgrade in progress is
worth treating as unexpected traffic on the port.

---

## ClinkDiskFillingWhereStateLives

The filesystem holding checkpoints or state backends is filling.

**This is the one to act on early.** A checkpoint that cannot be written does not
complete, the recovery point freezes, and the failure surfaces as
`ClinkCheckpointsStalled` some minutes later - by which time the disk is fuller.

**Look at:** the checkpoint directory. Retention keeps a bounded number of
checkpoints, but a job with large state and a short interval can still outrun a
small volume between cleanups.

**Do not** delete checkpoint files by hand to buy room. A payload without its
sidecar, or a partially deleted checkpoint, reads as incomplete and can cost you
the recovery point you were trying to preserve. Use
`clink checkpoint-verify` to see what is valid, and drop whole checkpoints oldest
first.

---

## Things that are worth checking whatever the alert

**Is the coordinator the leader you think it is?** `/api/v1/config` reports the
epoch. Two coordinators serving the same cluster is a specific and serious failure -
see `ClinkFencedFrames`.

**Did the job actually recover, or did it merely restart?** A restart that keeps
failing looks like activity. `clink jobs` shows the terminal status; the coordinator
log shows the attempt count against the budget.

**A restarted coordinator shows no jobs.** That is the contract, not a loss, when the
coordinator runs without `--ha-dir`: job manifests are persisted (and recovered on
leadership) only under an HA directory, so a plain restart abandons running jobs while
their `COMPLETED-N` markers remain intact. Recover manually by resubmitting with
`--restore-from-checkpoint-id=<latest N>`, or run the coordinator with `--ha-dir` -
one node is enough - so restarts recover jobs on their own.

**What is this job actually configured to do?** `/api/v1/jobs/:id` reports the
checkpoint configuration the job is running with - directory, interval, state backend,
restore source, restart budget and alignment. Check it before assuming a job that is
not checkpointing is broken: an interval of 0, or an empty checkpoint_dir, means it was
never asked to.

**Is the recovery point moving?** `clink_ckpt_last_completed_unix_seconds` advancing
is the single best signal that a job is healthy. A job that is running, consuming and
producing, but not completing checkpoints, is accumulating unbounded replay on its
next failure.

## What this runbook does not cover

There is no Grafana dashboard in the repository. The metric names above are stable
and the alert rules encode the thresholds, but assembling a dashboard is left to the
deployment.

None of the procedures here are exercised by an automated test, unlike the alert
rules themselves (`tests/test_alert_rules.cpp`). They are written from the failure
modes this engine has actually produced, but a runbook step that has never been
walked through on a live incident should be read as informed guidance rather than a
verified procedure.
