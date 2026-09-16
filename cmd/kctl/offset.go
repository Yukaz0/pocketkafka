package main

import (
	"fmt"
	"github.com/Yukaz0/pocketkafka/pkg/client"
)

func cmdOffsetReset(kc *client.KafkaClient, group, topic string, toEarliest, toLatest bool, toOffset int64) {
	var target int64
	switch {
	case toEarliest:
		target = -2
	case toLatest:
		target = -1
	case toOffset >= 0:
		target = toOffset
	default:
		fatal("usage", fmt.Errorf("choose one of --to-earliest, --to-latest, --to-offset"))
	}
	for _, p := range kc.Partitions(topic) {
		off := target
		if target == -2 || target == -1 {
			var err error
			off, err = kc.QueryOffset(topic, p.PartitionID, target)
			if err != nil {
				fatal("offset reset", err)
			}
		}
		if err := kc.ResetOffset(group, topic, p.PartitionID, off); err != nil {
			fatal("offset reset", err)
		}
		fmt.Printf("reset group=%s topic=%s partition=%d -> offset=%d\n", group, topic, p.PartitionID, off)
	}
}

func cmdOffsetGet(kc *client.KafkaClient, group, topic string) {
	var rows [][]any
	for _, p := range kc.Partitions(topic) {
		off, err := kc.CommittedOffset(group, topic, p.PartitionID)
		if err != nil || off < 0 {
			off = -1
		}
		rows = append(rows, []any{group, topic, p.PartitionID, off})
	}
	renderTable([]string{"group", "topic", "partition", "current_offset"}, rows)
}
