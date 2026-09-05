----------------------------- MODULE M_id_reuse ------------------------------
(* Mutant: qual01-20260817c and 20260819g: a recovered job numbered checkpoints from the restore point.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
