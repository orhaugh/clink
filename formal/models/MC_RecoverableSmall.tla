------------------------- MODULE MC_RecoverableSmall -------------------------
(* The staged-artifact / XA family: handles re-committed at open, restores
   from the newest COMPLETED checkpoint, no receipts and no resolution walk. *)
EXTENDS ExactlyOnce

CONSTANTS s1, s2, w1, w2

MCHost == (s1 :> w1) @@ (s2 :> w2)

==============================================================================
