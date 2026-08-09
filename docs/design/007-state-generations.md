# 007: State directories are namespaced by topology generation

**Status:** accepted, implemented 2026-08-07.

## The problem

A subtask's state lived at `<checkpoint_dir>/<job-global subtask index>`, and that
index is **reassigned whenever an operator is resized**.

The planner allocates one contiguous block of indices per operator in graph order, so
for `source -> counter -> sink`:

```
counter at parallelism 4:   source=0   counter=1,2,3,4   sink=5
counter at parallelism 1:   source=0   counter=1         sink=2
```

Directory `2` was a counter subtask and becomes the sink. Directory `1` held one
counter's slice of the key space and becomes the merged counter holding all of it.
Nothing moves or renames: the new topology simply starts writing into the old one's
directories.

Four defects came from this, each found and fixed separately, each a different
consumer of the same unstable index:

| | What went wrong |
|---|---|
| F38 | The parent-index arithmetic was computed on the global index instead of the index within the operator |
| F59 | A restore WROTE through the index, staging state into directories other subtasks were reading as parents |
| F63 | A restart in the window before the first post-rescale checkpoint skipped the translation entirely |
| F65 | The new topology keeps writing into the old topology's directories, so a restore point stops describing one moment |

The fourth is what made the pattern undeniable. The fix belongs at the addressing
layer, not at each consumer.

## The change

```
<checkpoint_dir>/v<generation>/<subtask index>/checkpoint-<id>.snap
```

`generation` is `JobState::topology_version`, which already existed: it starts at 1
on the initial deploy, increments on every replan, and was already reported in the
replan log line. It is now also sent in `DeployMsg` and carried on
`StateBackendSpec`, alongside the generation that produced the restore point.

Each generation owns its own namespace, so a generation's files are **immutable with
respect to every other generation**. That is the invariant all four defects break.

A restore reads the generation that produced the checkpoint being restored from; the
parent-index translation (F38, F63) then decides only *which parent index within that
generation*, never which directory a live subtask may write to.

## Why generation and not checkpoint id

The obvious alternative is to make the checkpoint the directory -
`<checkpoint_dir>/chk-<id>/<subtask>.snap` - which is how several mature systems lay
state out, and which has real ergonomic advantages: a checkpoint becomes one
listable, verifiable, deletable unit.

It was not chosen as the fix, for a specific reason. Checkpoint ids are monotonic and
in-flight ids are abandoned on restart (`pending_checkpoint_acks.clear()`), so the
coordinator never issues an id twice - and yet the investigation into F65 found a
restore point's file holding state from the WRONG topology, which is a write that
should not have been possible. That write was not fully attributed.

A checkpoint-first layout only helps if no two writers ever share an id. A
generation-first layout is correct even if one does, because the two writers are in
different generations and therefore different directories. When the diagnosis has an
unexplained step, the fix that does not depend on the unexplained part is the better
one.

The checkpoint-first inversion remains a reasonable follow-on for ergonomics. It is a
larger refactor - it changes what a state backend's "snapshot directory" means, and
every backend composes that path itself - and it can be done later on top of this
without disturbing the correctness property.

## What it touches

Eleven path-composition sites, all of the form `base / std::to_string(subtask_idx)`:

| Backend | Sites |
|---|---|
| `state_backend_factory.cpp` (file, changelog+file) | 2 |
| rocksdb | 2 |
| forst | 2 |
| forst-s3 | 3 |
| rocksdb-s3 | 2 |

plus `impls/s3`, which composes an object-store prefix rather than a filesystem path -
the same change against a different string.

And three pieces of plumbing:

1. `StateBackendSpec` carries `generation` and `restore_generation`.
2. `DeployMsg` carries both, so a worker knows which generation it is writing and
   which one it is restoring from. Additive tail, so the wire stays compatible.
3. Retention understands generations, so old ones are cleaned up rather than
   accumulating.

## Compatibility

None required. There are no deployments on the previous layout, so the flat layout is
simply gone rather than supported alongside. A reader that finds no `v*` directory
finds no state, which is the correct answer for a directory written by no version of
this engine.

This is worth stating explicitly because the alternative - a fallback path for the old
layout - would have been dead code from the day it was written, and dead recovery
paths are how the F59 class of defect survives unnoticed.

## What this does not fix

It makes cross-generation collision structurally impossible. It does not, on its own,
prove that follow-up 44's intermittent scale-down failure had no second cause: the
reproduction rate was roughly one run in thirty, so another contributor could hide
behind the one that was found. The way to settle that is to watch the failure rate
after this lands, not to assume.
