package controller

import (
	"context"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	clinkv1alpha1 "github.com/clink/clink-operator/api/v1alpha1"
)

// kafkaGroupLag measures the total pending records for a consumer group over
// the named topics: sum over partitions of (end offset minus the group's
// committed offset). A partition the group has never committed counts from
// its start offset - a parked job that never consumed sees everything
// produced as pending, which is exactly the wake signal we want.
//
// A fresh client per poll keeps the operator connection-free between polls;
// at the 15s-class cadence the setup cost is irrelevant.
func kafkaGroupLag(ctx context.Context, k *clinkv1alpha1.KafkaLagWakeSpec) (int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(k.Brokers, ",")...))
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	end, err := adm.ListEndOffsets(reqCtx, k.Topics...)
	if err != nil {
		return 0, err
	}
	start, err := adm.ListStartOffsets(reqCtx, k.Topics...)
	if err != nil {
		return 0, err
	}
	committed, err := adm.FetchOffsetsForTopics(reqCtx, k.GroupID, k.Topics...)
	if err != nil {
		return 0, err
	}

	var lag int64
	end.Each(func(o kadm.ListedOffset) {
		base := int64(-1)
		if co, ok := committed.Lookup(o.Topic, o.Partition); ok && co.At >= 0 {
			base = co.At
		}
		if base < 0 {
			if so, ok := start.Lookup(o.Topic, o.Partition); ok {
				base = so.Offset
			}
		}
		if base >= 0 && o.Offset > base {
			lag += o.Offset - base
		}
	})
	return lag, nil
}
