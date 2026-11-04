package controller

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	clinkv1alpha1 "github.com/clink/clink-operator/api/v1alpha1"
)

// topicPartition names one partition of a topic.
type topicPartition struct {
	topic     string
	partition int32
}

// kafkaOffsets is what one poll of a wake policy reads: each partition's end
// and start offsets, and the offsets the group has committed, only where it
// has committed one.
type kafkaOffsets struct {
	end, start, committed map[topicPartition]int64
}

// kafkaOffsetReader reads a wake policy's offsets. The reconciler reads them
// from the brokers; tests give it their own.
type kafkaOffsetReader func(ctx context.Context, k *clinkv1alpha1.KafkaLagWakeSpec) (kafkaOffsets, error)

// readKafkaOffsets asks the brokers. A fresh client per poll keeps the
// operator connection-free between polls; at the 15s-class cadence the setup
// cost is irrelevant.
func readKafkaOffsets(ctx context.Context, k *clinkv1alpha1.KafkaLagWakeSpec) (kafkaOffsets, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(k.Brokers, ",")...))
	if err != nil {
		return kafkaOffsets{}, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	end, err := adm.ListEndOffsets(reqCtx, k.Topics...)
	if err != nil {
		return kafkaOffsets{}, err
	}
	start, err := adm.ListStartOffsets(reqCtx, k.Topics...)
	if err != nil {
		return kafkaOffsets{}, err
	}
	committed, err := adm.FetchOffsetsForTopics(reqCtx, k.GroupID, k.Topics...)
	if err != nil {
		return kafkaOffsets{}, err
	}
	o := kafkaOffsets{end: map[topicPartition]int64{}, start: map[topicPartition]int64{},
		committed: map[topicPartition]int64{}}
	end.Each(func(l kadm.ListedOffset) {
		if l.Err == nil && l.Offset >= 0 {
			o.end[topicPartition{l.Topic, l.Partition}] = l.Offset
		}
	})
	start.Each(func(l kadm.ListedOffset) {
		if l.Err == nil && l.Offset >= 0 {
			o.start[topicPartition{l.Topic, l.Partition}] = l.Offset
		}
	})
	committed.Each(func(c kadm.OffsetResponse) {
		if c.Err == nil && c.At >= 0 {
			o.committed[topicPartition{c.Topic, c.Partition}] = c.At
		}
	})
	return o, nil
}

// pendingRecords is what a parked job has waiting on its wake policy's topics:
// for each partition, its end offset less where the job is taken to have read
// to. That is where the partition ended when the park began, when the operator
// recorded it: the job had been idle, so it had read what was there. Without
// a baseline, a policy added after the park, it is the group's committed
// offset, or else the partition's start offset, so everything there counts. A
// clink job's Kafka source commits nothing to its group: the baseline is what
// keeps such a job from waking at its first poll for the records it read
// before it parked.
func pendingRecords(o kafkaOffsets, baseline []clinkv1alpha1.PartitionOffset) int64 {
	parkedAt := make(map[topicPartition]int64, len(baseline))
	for _, b := range baseline {
		parkedAt[topicPartition{b.Topic, b.Partition}] = b.Offset
	}
	var pending int64
	for tp, end := range o.end {
		base, ok := parkedAt[tp]
		if !ok {
			base, ok = o.committed[tp]
		}
		if !ok {
			base, ok = o.start[tp]
		}
		if ok && end > base {
			pending += end - base
		}
	}
	return pending
}

// wakeBaseline is where each partition ends, in order, for the job's status.
func wakeBaseline(o kafkaOffsets) []clinkv1alpha1.PartitionOffset {
	out := make([]clinkv1alpha1.PartitionOffset, 0, len(o.end))
	for tp, end := range o.end {
		out = append(out, clinkv1alpha1.PartitionOffset{Topic: tp.topic, Partition: tp.partition, Offset: end})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}
