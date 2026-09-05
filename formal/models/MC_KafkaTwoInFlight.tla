-------------------------- MODULE MC_KafkaTwoInFlight --------------------------
(* Two checkpoints in flight at once: the barrier for the next interval can
   reach a sink while the previous commit is still outstanding, and a failed
   checkpoint can sit below a completing one. Fewer faults than the small
   model, to keep the state space within a push gate's budget. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
================================================================================
