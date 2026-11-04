package controller

import (
	"reflect"
	"testing"

	clinkv1alpha1 "github.com/clink/clink-operator/api/v1alpha1"
)

// A parked job's pending records count from where each partition ended when
// the park began, then from what the group committed, then from the start.
func TestAParkedJobsLagCountsFromWhereItsTopicsEndedWhenItParked(t *testing.T) {
	p0, p1, p2 := topicPartition{"cdn.edge", 0}, topicPartition{"cdn.edge", 1}, topicPartition{"cdn.edge", 2}
	at := func(p int32, off int64) clinkv1alpha1.PartitionOffset {
		return clinkv1alpha1.PartitionOffset{Topic: "cdn.edge", Partition: p, Offset: off}
	}
	cases := map[string]struct {
		end, start, committed map[topicPartition]int64
		baseline              []clinkv1alpha1.PartitionOffset
		want                  int64
	}{
		"nothing produced since the park": {
			end: map[topicPartition]int64{p0: 900, p1: 600}, baseline: []clinkv1alpha1.PartitionOffset{at(0, 900), at(1, 600)}},
		"what was produced since the park": {
			end: map[topicPartition]int64{p0: 950, p1: 610}, baseline: []clinkv1alpha1.PartitionOffset{at(0, 900), at(1, 600)}, want: 60},
		"the baseline before a stale commit": {
			end: map[topicPartition]int64{p0: 950}, committed: map[topicPartition]int64{p0: 20},
			baseline: []clinkv1alpha1.PartitionOffset{at(0, 900)}, want: 50},
		"a commit where no baseline was recorded": {
			end: map[topicPartition]int64{p0: 950}, start: map[topicPartition]int64{p0: 0},
			committed: map[topicPartition]int64{p0: 940}, want: 10},
		"everything from the start with neither": {
			end: map[topicPartition]int64{p0: 950}, start: map[topicPartition]int64{p0: 100}, want: 850},
		"a partition added after the park counts from its start": {
			end: map[topicPartition]int64{p0: 900, p2: 7}, start: map[topicPartition]int64{p2: 0},
			baseline: []clinkv1alpha1.PartitionOffset{at(0, 900)}, want: 7},
		"a topic emptied and made again counts nothing negative": {
			end: map[topicPartition]int64{p0: 5}, baseline: []clinkv1alpha1.PartitionOffset{at(0, 900)}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			o := kafkaOffsets{end: c.end, start: c.start, committed: c.committed}
			if got := pendingRecords(o, c.baseline); got != c.want {
				t.Errorf("pending %d, want %d", got, c.want)
			}
		})
	}
}

func TestTheWakeBaselineListsEveryPartitionInOrder(t *testing.T) {
	o := kafkaOffsets{end: map[topicPartition]int64{
		{"b", 1}: 3, {"a", 2}: 9, {"a", 0}: 4, {"b", 0}: 1,
	}}
	want := []clinkv1alpha1.PartitionOffset{
		{Topic: "a", Partition: 0, Offset: 4}, {Topic: "a", Partition: 2, Offset: 9},
		{Topic: "b", Partition: 0, Offset: 1}, {Topic: "b", Partition: 1, Offset: 3},
	}
	if got := wakeBaseline(o); !reflect.DeepEqual(got, want) {
		t.Errorf("baseline %+v, want %+v", got, want)
	}
}
