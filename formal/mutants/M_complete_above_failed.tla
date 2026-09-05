----------------------- MODULE M_complete_above_failed -----------------------
(* Mutant: found by this model: a checkpoint completed above a FAILED one during its rewind.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
