------------------------- MODULE M_orphan_left_to_init -------------------------
(* Mutant: a reopening Kafka sink describes its orphan and leaves an undecided
   one for init_transactions to abort, while the worker that sealed it, declared
   lost, still runs and commits it in between.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
================================================================================
