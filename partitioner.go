package juggler

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

type Partitioner struct {
	lg            log.Logger
	topic         string
	client        sarama.Client
	consumerGroup string

	admin sarama.ClusterAdmin // will be initiated from kafka.Client object

	strategy Strategy
	// TODO: maybe have some time-based local cache
}

// Evaluate evaluate
func (p *Partitioner) Evaluate(ctx context.Context) {
	limiter := rate.NewLimiter(rate.Every(time.Second), 1)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := limiter.Wait(ctx); err != nil {
			p.lg.Print(err)
			continue
		}
		consumerOffset, err := p.FetchConsumerOffset(ctx)
		if err != nil {
			p.lg.Println(err)
			continue
		}

		logEndOffset, err := p.FetchLogEndOffset(ctx)
		if err != nil {
			p.lg.Println(err)
			continue
		}

		partitions, err := p.client.Partitions(p.topic)
		if err != nil {
			p.lg.Println(err)
			continue
		}

		for _, partition := range partitions {
			co, ok1 := consumerOffset[partition]
			leo, ok2 := logEndOffset[partition]
			if !ok1 || !ok2 { // both offsets must be valid to calculate lag
				continue
			}
			lag := max(0, leo-co)
			p.strategy.ReportLag(partition, lag)
		}

	}
}

// FetchLogEndOffset return all consumer group offsets using Kafka's Admin API.
func (p *Partitioner) FetchConsumerOffset(ctx context.Context) (m map[int32]int64, err error) {
	m = map[int32]int64{}
	partitions, err := p.client.Partitions(p.topic)
	if err != nil {
		return nil, errors.Wrapf(err, "error fetching all partition id topic:%s", p.topic)
	}
	consumerOffsets, err := p.admin.ListConsumerGroupOffsets(p.consumerGroup, map[string][]int32{p.topic: partitions})
	if err != nil {
		return nil, errors.Wrapf(err, "error fetching consumer offset group:%s topic:%s", p.consumerGroup, p.topic)
	}
	for partition, data := range consumerOffsets.Blocks[p.topic] {
		m[partition] = data.Offset
	}
	return
}

// FetchLogEndOffset return all log end offsets from Kafka.
func (p *Partitioner) FetchLogEndOffset(ctx context.Context) (m map[int32]int64, err error) {
	m = map[int32]int64{}
	eg, _ := errgroup.WithContext(ctx)
	mutex := sync.Mutex{}

	generator := func(ctx context.Context, partitionID int32) func() error {
		return func() (err error) {
			offset, err := p.client.GetOffset(p.topic, partitionID, sarama.OffsetNewest)
			if err != nil {
				return errors.Wrapf(err, "error when fetching LEO partition:%d", partitionID)
			}
			mutex.Lock()
			m[partitionID] = offset
			mutex.Unlock()
			return nil
		}
	}

	partitions, err := p.client.Partitions(p.topic)
	if err != nil {
		return nil, errors.Wrapf(err, "error fetching all partition id topic:%s", p.topic)
	}
	for _, partition := range partitions {
		eg.Go(generator(ctx, partition))
	}
	if err = eg.Wait(); err != nil {
		return nil, err
	}

	return
}
