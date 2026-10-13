----------------------- MODULE M_open_skips_restore_zero -----------------------
(* Mutant: a Kafka sink reopening at restore point 0, a restart with nothing
   confirmed, takes it for a fresh start: it neither settles its unresolved
   orphan nor arms replay suppression.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, w1, w2
MCHost == (s1 :> w2)
================================================================================
