----------------------- MODULE M_stop_at_first_refusal -----------------------
(* Mutant: qual01-20260819f: the walk stopped at the first refused handle, leaving the rest unproven.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
