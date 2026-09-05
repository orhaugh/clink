--------------------- MODULE M_no_materialised_receipts ----------------------
(* Mutant: qual01-20260819f: a commit the walk proved over the wire got no receipt.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
