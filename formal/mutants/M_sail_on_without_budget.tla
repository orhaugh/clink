------------------- MODULE M_sail_on_without_budget --------------------
(* Mutant: with no restart budget left, a FAILED checkpoint aborted its
   interval and the job sailed on, later checkpoints completing over the gap.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
