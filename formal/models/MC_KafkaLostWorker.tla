-------------------------- MODULE MC_KafkaLostWorker --------------------------
(* A worker the coordinator declares lost while its process still runs, in the
   Kafka family: the old incarnation executes the commit, abort and barrier
   frames already on the wire to it, alongside the drain, the in-doubt walk,
   the redeploy and the reopening sinks. The liveness model's bounds (two
   checkpoints, one of each fault, a walk the watchdog may cancel), checked for
   safety only: the shape in which a reopening sink's orphan, committed by the
   old incarnation between a describe and a fence, was published twice. *)
EXTENDS ExactlyOnce
CONSTANTS s1, s2, w1, w2
MCHost == (s1 :> w1) @@ (s2 :> w2)
================================================================================
