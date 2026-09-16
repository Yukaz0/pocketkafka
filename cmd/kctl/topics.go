package main

import (
	"fmt"
	"github.com/Yukaz0/pocketkafka/pkg/client"
)

func cmdCluster(kc *client.KafkaClient) {
	topics := kc.ListTopics()
	parts := 0
	for _, t := range topics {
		parts += len(kc.Partitions(t))
	}
	if outputFormat == "json" {
		renderTable([]string{"broker", "topics", "partitions"}, [][]any{{"pocketkafka", len(topics), parts}})
		return
	}
	fmt.Printf("broker:      %s\n", "pocketkafka")
	fmt.Printf("topics:      %d\n", len(topics))
	fmt.Printf("partitions:  %d\n", parts)
	for _, t := range topics {
		fmt.Printf("  %s\n", t)
	}
}

func cmdTopics(kc *client.KafkaClient) {
	topics := kc.ListTopics()
	if len(topics) == 0 {
		if outputFormat == "json" {
			fmt.Println("[]")
			return
		}
		fmt.Println("(no topics)")
		return
	}
	var rows [][]any
	for _, t := range topics {
		for _, p := range kc.Partitions(t) {
			leo, _ := kc.QueryOffset(t, p.PartitionID, -1)
			rows = append(rows, []any{t, p.PartitionID, p.LeaderID, leo})
		}
	}
	renderTable([]string{"topic", "partition", "leader", "log_end_offset"}, rows)
}
