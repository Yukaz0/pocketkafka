// Command client-example demonstrates the zero-dependency client SDK
// (pkg/client) talking to the go-kafka-neu broker.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/neu/go-kafka-neu/pkg/client"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma separated broker addresses")
	topic := flag.String("topic", "demo-topic", "topic to use")
	mode := flag.String("mode", "produce", "produce | consume")
	count := flag.Int("count", 5, "number of messages to produce")
	group := flag.String("group", "demo-group", "consumer group id")
	duration := flag.Duration("time", 5*time.Second, "how long to consume")
	flag.Parse()

	kc, err := client.NewClient(splitBrokers(*brokers), "client-example")
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer kc.Close()

	switch *mode {
	case "produce":
		produce(kc, *topic, *count)
	case "consume":
		consume(kc, *topic, *group, *duration)
	default:
		fmt.Fprintln(os.Stderr, "unknown mode", *mode)
		os.Exit(1)
	}
}

func produce(kc *client.KafkaClient, topic string, count int) {
	p := client.NewProducer(kc, client.DefaultProducerConfig())
	for i := 0; i < count; i++ {
		value := fmt.Sprintf("hello from go-kafka-neu #%d", i)
		off, err := p.SendSync(context.Background(), &client.Message{Topic: topic, Value: []byte(value)})
		if err != nil {
			fmt.Fprintf(os.Stderr, "produce: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("produced -> topic=%s offset=%d value=%q\n", topic, off, value)
	}
}

func consume(kc *client.KafkaClient, topic, group string, duration time.Duration) {
	cg := client.NewConsumerGroup(kc, client.DefaultConsumerGroupConfig(), func(msg *client.ConsumedMessage) error {
		fmt.Printf("consumed <- topic=%s partition=%d offset=%d value=%q\n",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		return nil
	})
	cg.SetGroupID(group)
	cg.SetTopics([]string{topic})

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	if err := cg.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "consumer: %v\n", err)
	}
}

func splitBrokers(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}
