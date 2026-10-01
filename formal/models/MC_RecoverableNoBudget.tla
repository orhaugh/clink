------------------------ MODULE MC_RecoverableNoBudget ------------------------
(* The recoverable family with no restart budget left (MaxJobRestarts = 0): a
   FAILED checkpoint cannot rewind, so the job fails rather than sail on past
   its aborted interval. Every restart spends the same budget in the engine,
   so with none left a subtask error fails the job too: no error restarts
   here. The other models size the budget so it is never spent, which keeps
   their behaviour as it was. *)
EXTENDS ExactlyOnce

CONSTANTS s1, s2, w1, w2

MCHost == (s1 :> w1) @@ (s2 :> w2)

==============================================================================
