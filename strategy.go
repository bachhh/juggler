package juggler

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
)

// Strategy implement the same interface as sarama.Partitioner, but will
// receive Lag report from the partitioner
type Strategy interface {
	// Partition partition a message
	Partition(message *sarama.ProducerMessage, numPartitions int32) (partition int32, err error)
	// RequiresConsistency return true if mapping of message->partition will be consistent
	RequiresConsistency() bool
	// ReportLag report partition Lag status, taken as LEO - consumer offset
	ReportLag(partitionID int32, lag int64)
}

// CBRBPartition define a round-robin circuit breaker partition strategy.
// If partition lag exceed some threshold, they will be penalized for some period
type RBCBPartition struct {
	logEndOffset           map[int32]int64 // map partition -> LEO
	logEndOffsetLastUpdate *time.Time

	consumerOffset           map[int32]int64 // map partition -> consumerOffset
	consumerOffsetLastUpdate *time.Time

	counter atomic.Int32

	lagThreshold int64
	// partition that has non-nil entry in penalty map indicate the last time
	// this partition has lag that exceed threshold
	penaltyUntil map[int32]*time.Time
	penaltyTime  time.Duration

	allPenalized *atomic.Bool
	// when all partitions are penalized, you can choose to fallback to normal round-robin
	// or throws an error.
	// Set this flag to true to enable silent fallback, otherwise the default
	// is to throws an error
	fallbackToRoundRobin bool
}

// Partition takes a message and partition count and chooses a partition
func (p *RBCBPartition) Partition(message *sarama.ProducerMessage, numPartitions int32) (partition int32, err error) {
	partition = p.counter.Inc() % numPartitions
	if p.allPenalized.Load() {
		if p.fallbackToRoundRobin {
			// if all partitions are penalized just return a random one
			// NOTE: or maybe return an error ?
			log.Print("all partition are penalized")
			return
		}
		return 0, errors.Errorf("all partitions are penalized")
	}

	now := time.Now()
	for ; p.penaltyUntil[partition] != nil &&
		now.Before(*p.penaltyUntil[partition]); partition = p.counter.Inc() % numPartitions {
		// loop exit when partition not penalized, or penalty time expired
	}
	return
}

func (p *RBCBPartition) RequiresConsistency() bool { return false }

func (p *RBCBPartition) ReportLag(partitionID int32, lag int64) {
	if lag > p.lagThreshold {
		penaltyNow := time.Now().Add(p.penaltyTime)
		p.penaltyUntil[partitionID] = &penaltyNow
		log.Printf("partition:%d lag:%d has been penalized", partitionID, lag)
	} else {
		p.penaltyUntil[partitionID] = nil
	}

}

// NewRBCircuitBreakPartitioner constructor for circuit breaking partitioner
func NewRBCircuitBreakPartitioner(lagThreshold int64, fallbackToRoundRobin bool, penaltyTime time.Duration) (strat Strategy, err error) {
	if penaltyTime <= time.Second {
		return nil, fmt.Errorf("penalty time must be at least 1 second")
	}
	if lagThreshold <= 1 {
		return nil, fmt.Errorf("lag threshold must be greater than 1")
	}
	return &RBCBPartition{
		logEndOffset:         map[int32]int64{},
		consumerOffset:       map[int32]int64{},
		allPenalized:         atomic.NewBool(false),
		penaltyUntil:         map[int32]*time.Time{},
		lagThreshold:         lagThreshold,
		fallbackToRoundRobin: fallbackToRoundRobin,
		penaltyTime:          penaltyTime,
	}, nil
}
