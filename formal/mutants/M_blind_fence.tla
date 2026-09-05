---------------------------- MODULE M_blind_fence ----------------------------
(* Mutant: qual01 rig night: the reopened sink fenced its orphan before describing it.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
