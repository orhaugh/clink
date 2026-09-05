--------------------------- MODULE M_refusal_wall ----------------------------
(* Mutant: found by this model: the walk stopped at the first refused checkpoint, leaving later ones unproven.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
