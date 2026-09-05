------------------ MODULE M_no_rewind_on_failed_checkpoint -------------------
(* Mutant: correctness sweep item 4: a FAILED checkpoint aborted its interval and the job sailed on.
   TLC must refute this configuration; see formal/README.md. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
==============================================================================
