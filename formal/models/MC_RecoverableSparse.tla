---------------------------- MODULE MC_RecoverableSparse ----------------------------
(* The recoverable family with empty intervals: a sink that received nothing
   of an interval seals no transaction, persists no handle, acks, and declines
   the commit. Safety, as the family's other configurations: at these
   bounds EventuallySettled fails for a reason the bounds create (a COMPLETED
   marker landing after the job restored from below it vouches for a cut the
   run has no checkpoint id left to re-cover), with or without empty
   intervals. *)
EXTENDS ExactlyOnce

CONSTANTS s1, s2, w1, w2

MCHost == (s1 :> w1) @@ (s2 :> w2)

====================================================================================
