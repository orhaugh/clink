------------------------ MODULE M_restore_from_memory ------------------------
(* Mutant: found by this model: the restore point ran ahead of the durable COMPLETED marker.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
