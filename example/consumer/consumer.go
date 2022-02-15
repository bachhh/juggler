package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/paulbellamy/ratecounter"
	"golang.org/x/time/rate"
)

const (
	rateCounterEverySec = 5
)

var (
	tps    = flag.Uint("tps", 1, "default consuming tps")
	topic  = flag.String("topic", "partition_balancer", "topic to consume message from")
	broker = flag.String("broker", "localhost:9092", "kafka broker address")
)

func main() {
	flag.Parse()
	brokers := strings.Split(*broker, ",")
	client, err := InitKafkaClient(brokers)
	if err != nil {
		panic(err)
	}

	counter := ratecounter.NewRateCounter(rateCounterEverySec * time.Second)

	consumerGroup, err := sarama.NewConsumerGroupFromClient("partition_balancer", client)
	if err != nil {
		panic(err)
	}

	// os signal handling
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	consumer := &consumer{limiter: rate.NewLimiter(rate.Limit(*tps), 1), ratecounter: counter}
	// NOTE: sarama.ConsumerGroup.Consume must be called within a loop
	// to rejoin a cluster cotinuously in case rebalancing happens

LOOP:
	for {
		select {
		case <-ctx.Done():
			log.Printf("context cancelled")
			break LOOP
		default:
		}

		if err := consumerGroup.Consume(ctx, []string{*topic}, consumer); err != nil && err != context.Canceled {
			log.Printf("%+v", err)
		}
	}

}

// InitKafkaClient Init new Kafka with configs
func InitKafkaClient(brokers []string) (client sarama.Client, err error) {

	kafkaConfig := sarama.NewConfig()

	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	kafkaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	kafkaConfig.ClientID = "smsgw"

	return sarama.NewClient(brokers, kafkaConfig)
}

type consumer struct {
	limiter     *rate.Limiter
	ratecounter *ratecounter.RateCounter
}

// extending session manager to implement sarama.ConsumerGroupHandler interface
func (c *consumer) Setup(session sarama.ConsumerGroupSession) (err error) { return }

func (s *consumer) Cleanup(session sarama.ConsumerGroupSession) (err error) { return }

func (s *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (err error) {
	ctx := session.Context()
	for msg := range claim.Messages() {
		if err = s.limiter.Wait(ctx); err != nil {
			log.Printf("%+v\n", err)
		}
		session.MarkMessage(msg, "")
		s.ratecounter.Incr(1)
		log.Printf("consumed p:%d offset%d val:%s rate:%0.2f",
			msg.Partition, msg.Offset, msg.Value, float64(s.ratecounter.Rate())/rateCounterEverySec)
	}
	return
}
