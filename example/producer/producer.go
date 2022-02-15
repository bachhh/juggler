package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

var (
	tps       = flag.Uint("tps", 1, "default consuming tps")
	topic     = flag.String("topic", "partition_balancer", "topic to consume message from")
	broker    = flag.String("broker", "localhost:9092", "kafka broker address")
	threshold = flag.Int64("threshold", 1, "lag threshold")
)

func main() {
	flag.Parse()

	// os signal handling
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	brokers := strings.Split(*broker, ",")
	client, err := InitKafkaClient(ctx, brokers)
	if err != nil {
		panic(err)
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		panic(err)
	}

	limiter := rate.NewLimiter(rate.Limit(*tps), 1)

	// NOTE: sarama.ConsumerGroup.Consume must be called within a loop
	// to rejoin a cluster cotinuously in case rebalancing happens
	for i := 0; true; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err = limiter.Wait(ctx); err != nil {
			panic(err)
		}

		val := fmt.Sprintf("%s", time.Now())
		p, o, err := producer.SendMessage(
			&sarama.ProducerMessage{
				Topic: *topic,
				Value: sarama.ByteEncoder([]byte(fmt.Sprintf("%d", i))),
			})
		if err != nil {
			panic(err)
		}
		val = fmt.Sprintf("produced message p:%d offset:%d ", p, o)
		log.Printf(val)
	}

}

// InitKafkaClient Init new Kafka with configs
func InitKafkaClient(ctx context.Context, brokers []string) (client sarama.Client, err error) {

	// this adminclient will be used to make the partitioner strategy for the
	// main kafka client returned by this function
	adminClient, err := sarama.NewClient(brokers, sarama.NewConfig())
	if err != nil {
		panic(err)
	}

	partitioner, err := NewCircuitBreakPartitioner(ctx, adminClient,
		"partition_balancer",
		"partition_balancer",
		*threshold,
	)
	if err != nil {
		panic(err)
	}

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.Compression = sarama.CompressionSnappy
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Return.Errors = true
	kafkaConfig.Producer.Partitioner = partitioner

	kafkaConfig.ClientID = "smsgw"

	return sarama.NewClient(brokers, kafkaConfig)
}

// CBPartitioner implement kafka.Partitioner interface to choosing
// partition with some consideration to their Lag factor ( producer_offset - consumer_offset )
// All Partitions will be round-robin initially.
// Partition with Lag greater than certain threshold will be taken out of
// round-robin until their lag drop below the threshold again.
//
// Due to current scope limitation, circuit break partitioner will only be able
// to accomodate 1 consumer group - per 1 topic. I am aware that in kafka
// pub-sub model each consumer group will have a different lag distribution,
// but in this particular case the topic is held exclusively by
// smsgw_group_consumer. It is seen as a queue rather than a message log.
//
// The issue of multiple consumer group can thus be ignored, for now.
type CBPartitioner struct {
	topic         string
	client        sarama.Client
	consumerGroup string

	admin sarama.ClusterAdmin // will be initiated from kafka.Client object

	logEndOffset           map[int32]int64 // map partition -> LEO
	logEndOffsetLastUpdate *time.Time

	consumerOffset           map[int32]int64 // map partition -> consumerOffset
	consumerOffsetLastUpdate *time.Time

	counter atomic.Int32
	// partition that has non-nil entry in penalty map indicate the last time
	// this partition has lag that exceed threshold
	penalty      map[int32]*time.Time
	allPenalized *atomic.Bool
	lagThreshold int64
}

// NewCircuitBreakPartitioner constructor for circuit breaking partitioner
func NewCircuitBreakPartitioner(ctx context.Context, client sarama.Client, topic, consumerGroup string, lagThreshold int64) (
	constructor sarama.PartitionerConstructor, err error) {
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		panic(err)
	}

	partitioner := &CBPartitioner{client: client,
		topic:          topic,
		consumerGroup:  consumerGroup,
		logEndOffset:   map[int32]int64{},
		consumerOffset: map[int32]int64{},
		admin:          admin,
		allPenalized:   atomic.NewBool(false),
		penalty:        map[int32]*time.Time{},
		lagThreshold:   lagThreshold,
	}

	return func(topic string) sarama.Partitioner {
		go partitioner.Evaluate(ctx)
		return partitioner
	}, nil
}

// Partition takes a message and partition count and chooses a partition
func (c *CBPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (partition int32, err error) {
	partition = c.counter.Inc() % numPartitions
	if c.allPenalized.Load() {
		// if all partitions are penalized just return a random one
		// NOTE: or maybe return an error ?
		log.Print("all partition are penalized")
		return
	}

	for ; c.penalty[partition] != nil; partition = c.counter.Inc() % numPartitions {
		// TODO also consider penalty time
	}
	return
}

func (c *CBPartitioner) RequiresConsistency() bool { return false }

// Evaluate evaluate
func (c *CBPartitioner) Evaluate(ctx context.Context) {
	log.Println("start evaluator")
	limiter := rate.NewLimiter(rate.Every(time.Second), 1)
	for {
		// log.Println("evaluating")
		if err := limiter.Wait(ctx); err != nil {
			panic(err)
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		var err error
		c.consumerOffset, err = c.FetchConsumerOffset(ctx)
		if err != nil {
			panic(err)
		}
		now1 := time.Now()
		c.consumerOffsetLastUpdate = &now1

		c.logEndOffset, err = c.FetchLogEndOffset(ctx)
		if err != nil {
			panic(err)
		}

		now2 := time.Now()
		c.logEndOffsetLastUpdate = &now2

		penalizedLog := map[int32]int64{} // partition => lag
		partitions, err := c.client.Partitions(c.topic)
		if err != nil {
			panic(err)
		}

		for _, partition := range partitions {
			co, _ := c.consumerOffset[partition]
			leo, _ := c.logEndOffset[partition]
			lag := max(0, leo-co)
			// log.Printf("partition:%d lag %d", partition, lag)
			if lag > c.lagThreshold {
				penaltyNow := time.Now()
				c.penalty[partition] = &penaltyNow
				penalizedLog[partition] = lag
			} else {
				c.penalty[partition] = nil
			}
		}

		if len(penalizedLog) > 0 {
			log.Printf("some partition has been penalized %#v", penalizedLog)
		} else if len(penalizedLog) >= len(partitions) {
			c.allPenalized.Store(true)
		} else {
			c.allPenalized.Store(false)
		}

	}
}

// FetchLogEndOffset return all consumer group offsets using Kafka's Admin API.
func (c *CBPartitioner) FetchConsumerOffset(ctx context.Context) (m map[int32]int64, err error) {
	m = map[int32]int64{}
	partitions, err := c.client.Partitions(c.topic)
	if err != nil {
		panic(err)
	}
	consumerOffsets, err := c.admin.ListConsumerGroupOffsets(c.consumerGroup, map[string][]int32{c.topic: partitions})
	if err != nil {
		panic(err)
	}
	for partition, data := range consumerOffsets.Blocks[c.topic] {
		m[partition] = data.Offset
	}
	return
}

// FetchLogEndOffset return all log end offsets from Kafka.
func (c *CBPartitioner) FetchLogEndOffset(ctx context.Context) (m map[int32]int64, err error) {
	m = map[int32]int64{}
	eg, _ := errgroup.WithContext(ctx)
	mutex := sync.Mutex{}

	generator := func(ctx context.Context, partitionID int32) func() error {
		return func() (err error) {
			offset, err := c.client.GetOffset(c.topic, partitionID, sarama.OffsetNewest)
			if err != nil {
				panic(err)
			}
			mutex.Lock()
			m[partitionID] = offset
			mutex.Unlock()
			return nil
		}
	}

	partitions, err := c.client.Partitions(c.topic)
	if err != nil {
		panic(err)
	}
	for _, partition := range partitions {
		eg.Go(generator(ctx, partition))
	}
	if err = eg.Wait(); err != nil {
		panic(err)
	}

	return
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
