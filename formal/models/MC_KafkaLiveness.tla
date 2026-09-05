--------------------------- MODULE MC_KafkaLiveness ---------------------------
(* The liveness configuration: smaller bounds, since checking the temporal
   property costs a strongly connected components pass over the state graph.
   Every fault the model can inject is available once; the property says the
   run still settles with every vouched-for position published exactly once. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
===============================================================================
