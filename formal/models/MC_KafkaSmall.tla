---------------------------- MODULE MC_KafkaSmall ----------------------------
(* Two Kafka-family sinks on two workers, the source beside the first sink,
   three checkpoints, one fault of each kind. Small enough to enumerate on a
   push gate; wide enough that every named fault window is reachable. *)
EXTENDS ExactlyOnce

CONSTANTS s1, s2, w1, w2

MCHost == (s1 :> w1) @@ (s2 :> w2)

==============================================================================
