# 013: State is namespaced by job

**Status:** accepted, implemented 2026-10-20.

## The problem

Record 007 put a subtask's state at `<base>/v<generation>/<subtask index>`, where
`<base>` is the job's `state_backend_uri`, or its `checkpoint_dir` when it names
none. Nothing in that path says which job the state belongs to. The COMPLETED
markers had been namespaced by job for some time (`_jobs/<job id>/`); the state
they describe had not.

Any two jobs given one base therefore wrote the same files. That happens more
often than it sounds:

- every INSERT statement of a SQL script becomes its own job, and the script's
  jobs share its checkpoint directory and its `--state-backend`;
- two submissions pointed at one directory;
- every job of a cluster started with `--default-state-backend`.

It showed as a script with two INSERT statements and one checkpoint directory,
in the embedded engine: both jobs' subtask 0 wrote and renamed
`<dir>/v1/0/checkpoint-1.snap`, every checkpoint of both failed with a rename
error, and both jobs restarted from nothing, indefinitely. When the timing
lets the writes pass, it is worse: a job restores whichever job wrote the file
last, and resumes from that job's source offsets.

A second defect sat on the same path. A restore read its state from the
directory the restore named, which for a job's own restart was its checkpoint
directory. A job with its own state backend keeps only its markers there, so
its restart looked for state where there was none and refused to come up.

## The change

Each job's state has a root of its own:

```
<base>/_state/<job id>/v<generation>/<subtask index>/...
```

The coordinator composes the root (`job_state_root`,
`include/clink/cluster/state_location.hpp`) and sends it to the workers in the
field that carried the base, `DeployMsg::state_backend_uri`. The job segment goes
into the URI's location, before any `?query`, and into its `local=` parameter,
which the object-store schemes use for their local working directory. A URI that
names no location (`memory://`, `changelog://`, `disagg-local://`) is left as it
is: it keeps nothing another job could share.

Each COMPLETED marker records where its checkpoint's state was written, on a new
`state=` line: `_state/<job id>`, relative to the checkpoint directory, when the
state is in the checkpoint directory itself, and the absolute root otherwise. The
relative form means a copied checkpoint directory restores from the copy.

## Restoring

A restore is resolved from the marker of its restore point
(`resolve_restore_source`), once, when the job is submitted, resumed or taken
over, and sent to the workers as `DeployMsg::restore_from_dir`:

| The restore names | It reads |
|---|---|
| a job's marker directory, `<dir>/_jobs/<job id>` | that job's marker |
| a checkpoint directory in which one job completed the checkpoint | that job's marker |
| a checkpoint directory in which several jobs completed it | nothing: the submit is refused, and the refusal names each job's marker directory |
| a directory with no marker for the checkpoint, or a state backend URI | the location as given |

The marker's `state=` line gives the state root and its `generation=` line the
generation, so a restore no longer guesses the generation from a directory
listing. The restores the coordinator decides itself never depend on what else
the directory holds: an HA takeover and an embedded resume name the job's own
marker directory, and a restart reads the job's own root, or, when it restores
the job's submitted restore point, the location the submit resolved.

## Why the coordinator composes it

The coordinator is the one component that knows all three inputs: the job id,
the base, and which job wrote the restore point. It also reads the same
locations itself, for the in-doubt resolution walk, for the checkpoint-id floor
of a recovered job and for the restore layout gate, and those have to agree with
what the workers write.

Composing the root before it leaves the coordinator also keeps the change out of
everything the extension contract freezes. `StateBackendSpec` is Stable-tier, and
`make_subtask_job_config` is compiled into every job plugin. A plugin built
against a released SDK passes the URI it is given to the host's state backend
factory, and every backend composes `v<generation>/<subtask index>` under
whatever location that is, exactly as before. The wire format does not change
either: a worker from an earlier build reads the same two fields.

## Why beside `_jobs` and not inside it

`_jobs/<job id>/` would have made a job's markers and state one directory. The
coordination store lists `_jobs/<job id>` recursively, at submit and at
recovery, to find the job's markers, and state can be large: a RocksDB
checkpoint is a directory of hard-linked SST files. Keeping state in its own
tree keeps those listings small. `_state` and `_jobs` cannot collide with a
generation directory (`v<N>`), or with a subtask directory under the older
layout (a bare integer).

## Backends

The path-based schemes (`file`, `changelog+file`, `rocksdb`, `changelog+rocksdb`,
`forst`, `changelog+forst`) needed no change: each restores from the location in
`StateBackendSpec::restore_uri`, and that location is now the right one.

Two backends restored from their own location whatever the restore named, which
stopped being enough once two jobs no longer share a location:

- `remote-read` read the checkpoint from its own prefix. It now relocates a
  checkpoint the restore URI places at another prefix of the same bucket, or in
  another generation, into its own prefix, with the mechanism a rescale already
  used (`S3RemotePool::set_restore_sources`).
- `s3sst+forst` maps local paths to object keys relative to its local root, so
  it could only restore a checkpoint written under the same prefix and root. It
  now copies a checkpoint written elsewhere into its own mapping first
  (`copy_checkpoint_between`): the metadata files locally, the objects server
  side.

Both refuse a checkpoint in another bucket, naming it, since the copy is within
one bucket.

## Compatibility

Released builds wrote the shared layout, and a savepoint taken by one release is
how the next one is reached (QUAL-08), so it has to restore.

A marker without a `state=` line was written before this change. Its checkpoint
is restored from where state lived then: under an explicit state backend, that
backend's location; otherwise the checkpoint directory itself. Its new
checkpoints go under the job's own root. Nothing is moved, and the older files
are not deleted: once every job that used a directory has completed a checkpoint
under this layout, the `v<N>` directories at its top level are no longer read.

A marker that records neither its participants nor its state was cut short, since
every marker this build writes records both. Its state is read from the job's
own root when that exists.

## What this does not fix

The object-store composite schemes (`s3+rocksdb`, `changelog+s3`,
`changelog+s3+rocksdb`, `s3+forst`, `changelog+s3+forst`, `s3sst+forst`) keep
their local working directory under the URI's `local=` root, which is now
namespaced by job. Without `local=`, they use one temporary directory per
worker, which every job on that worker shares, whatever its prefix. That was
true before this change and still is.
