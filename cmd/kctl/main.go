// Command kctl is a small admin CLI for go-kafka-neu built on the bundled
// zero-dependency client SDK (pkg/client).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/neu/go-kafka-neu/pkg/client"
)

func main() {
	fs := flag.NewFlagSet("kctl", flag.ExitOnError)
	brokers := fs.String("b", "localhost:9092", "comma-separated broker addresses")
	partitions := fs.Int("p", 1, "number of partitions (create)")
	group := fs.String("g", "", "consumer group id (consume)")
	count := fs.Int("n", 10, "max messages to consume")
	key := fs.String("k", "", "message key (produce)")
	timeout := fs.Duration("timeout", 10*time.Second, "timeout for consume")
	fs.Parse(os.Args[1:])
	args := fs.Args()
	if len(args) == 0 {
		usage()
		os.Exit(1)
	}

	kc, err := client.NewClient(splitBrokers(*brokers), "kctl")
	if err != nil {
		fatal("connect", err)
	}
	defer kc.Close()

	cmd := args[0]
	switch cmd {
	case "cluster":
		cmdCluster(kc)
	case "topics", "list":
		cmdTopics(kc)
	case "create":
		if len(args) < 2 {
			fatal("usage", fmt.Errorf("kctl create <topic> [-p partitions]"))
		}
		if err := kc.CreateTopic(args[1], *partitions); err != nil {
			fatal("create", err)
		}
		fmt.Printf("created topic %q (%d partition(s))\n", args[1], *partitions)
	case "delete":
		if len(args) < 2 {
			fatal("usage", fmt.Errorf("kctl delete <topic>"))
		}
		if err := kc.DeleteTopic(args[1]); err != nil {
			fatal("delete", err)
		}
		fmt.Printf("deleted topic %q\n", args[1])
	case "produce":
		if len(args) < 3 {
			fatal("usage", fmt.Errorf("kctl produce <topic> <value> [-k key]"))
		}
		p := client.NewProducer(kc, client.DefaultProducerConfig())
		off, err := p.SendSync(context.Background(), &client.Message{Topic: args[1], Key: []byte(*key), Value: []byte(args[2])})
		if err != nil {
			fatal("produce", err)
		}
		fmt.Printf("produced -> %s offset=%d\n", args[1], off)
	case "consume":
		if len(args) < 2 {
			fatal("usage", fmt.Errorf("kctl consume <topic> [-g group] [-n count]"))
		}
		cmdConsume(kc, args[1], *group, *count, *timeout)
	case "groups":
		cmdGroups(kc)
	default:
		usage()
		os.Exit(1)
	}
}

func cmdCluster(kc *client.KafkaClient) {
	topics := kc.ListTopics()
	parts := 0
	for _, t := range topics {
		parts += len(kc.Partitions(t))
	}
	fmt.Printf("broker:      %s\n", "go-kafka-neu")
	fmt.Printf("topics:      %d\n", len(topics))
	fmt.Printf("partitions:  %d\n", parts)
	for _, t := range topics {
		fmt.Printf("  %s\n", t)
	}
}

func cmdTopics(kc *client.KafkaClient) {
	topics := kc.ListTopics()
	if len(topics) == 0 {
		fmt.Println("(no topics)")
		return
	}
	for _, t := range topics {
		for _, p := range kc.Partitions(t) {
			leo, _ := kc.QueryOffset(t, p.PartitionID, -1)
			fmt.Printf("%s\tpartition=%d\tleader=%d\tlogEndOffset=%d\n", t, p.PartitionID, p.LeaderID, leo)
		}
	}
}

func cmdConsume(kc *client.KafkaClient, topic, group string, n int, timeout time.Duration) {
	if group == "" {
		group = "kctl-" + strconv.FormatInt(time.Now().Unix(), 10)
	}
	received := 0
	cfg := client.DefaultConsumerGroupConfig()
	cfg.InitialOffset = -2 // read existing messages from the beginning
	cg := client.NewConsumerGroup(kc, cfg, func(msg *client.ConsumedMessage) error {
		fmt.Printf("%s\tpartition=%d\toffset=%d\tkey=%q\tvalue=%q\n",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		received++
		return nil
	})
	cg.SetGroupID(group)
	cg.SetTopics([]string{topic})

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	go cg.Start(ctx)

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if received >= n {
				cancel()
				return
			}
		}
	}
}

func cmdGroups(kc *client.KafkaClient) {
	// The SDK exposes groups via the metadata only; list topics as a proxy.
	fmt.Println("Use the web UI (http://localhost:8080) to inspect consumer groups.")
}

func usage() {
	fmt.Fprintln(os.Stderr, `kctl - admin CLI for go-kafka-neu

Usage:
  kctl [-b localhost:9092] cluster
  kctl [-b ...] topics
  kctl [-b ...] create <topic> [-p partitions]
  kctl [-b ...] delete <topic>
  kctl [-b ...] produce <topic> <value> [-k key]
  kctl [-b ...] consume <topic> [-g group] [-n count]
  kctl [-b ...] groups`)
}

func fatal(what string, err error) {
	fmt.Fprintf(os.Stderr, "kctl: %s: %v\n", what, err)
	os.Exit(1)
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
