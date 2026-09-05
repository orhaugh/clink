---------------------- MODULE M_restore_from_completed -----------------------
(* Mutant: before the commit-confirmed protocol: restores selected the newest completed checkpoint.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
