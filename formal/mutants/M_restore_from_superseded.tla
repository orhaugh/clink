---------------------- MODULE M_restore_from_superseded ----------------------
(* Mutant: found by this model: a takeover restored from a COMPLETED marker that landed after
   the job had restored from below it, its transactions gone.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
