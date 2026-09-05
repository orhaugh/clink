----------------------- MODULE M_close_aborts_prepared -----------------------
(* Mutant: qual01-20260818a: a cancelled sink aborted its barrier-sealed prepared transaction.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
