------------------------ MODULE M_receipt_after_begin ------------------------
(* Mutant: qual01 rig night: the receipt was written after the successor transaction began.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
