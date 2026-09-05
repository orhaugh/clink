---------------------- MODULE M_broadcast_before_marker ----------------------
(* Mutant: hardening: the commit was broadcast before the COMPLETED marker was durable.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
