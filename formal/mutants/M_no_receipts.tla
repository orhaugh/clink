---------------------------- MODULE M_no_receipts ----------------------------
(* Mutant: qual01-20260818b: no commit receipts; a fenced answer for an executed commit read as not committed.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
