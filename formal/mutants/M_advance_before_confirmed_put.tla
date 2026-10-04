------------------- MODULE M_advance_before_confirmed_put -------------------
(* Mutant: found in review: latest_confirmed moved before the CONFIRMED marker was durable.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
