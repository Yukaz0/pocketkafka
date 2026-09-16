package main

import (
	"context"
	"fmt"
	"github.com/Yukaz0/pocketkafka/pkg/client"
	"strconv"
	"time"
)

func cmdConsume(kc *client.KafkaClient, topic, group string, n int, timeout time.Duration) {
	if group == "" {
		group = "pkctl-" + strconv.FormatInt(time.Now().Unix(), 10)
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
