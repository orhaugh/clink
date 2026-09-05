---------------------- MODULE M_broadcast_during_drain -----------------------
(* Mutant: qual01-20260818a: the commit broadcast went into a job draining for a restart.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
