package main

import (
	"encoding/json"
	"fmt"
	"github.com/Yukaz0/pocketkafka/pkg/client"
	"os"
	"sort"
	"text/tabwriter"
)

type groupLagRow struct {
	GroupID       string
	Topic         string
	Partition     int32
	CurrentOffset int64
	LogEndOffset  int64
	Lag           int64
}

func cmdGroups(kc *client.KafkaClient) {
	groupIDs, err := kc.ListGroups()
	if err != nil {
		fatal("groups", err)
	}
	sort.Strings(groupIDs)
	if len(groupIDs) == 0 {
		if outputFormat == "json" {
			fmt.Println("[]")
			return
		}
		fmt.Println("(no consumer groups)")
		return
	}

	var rows []groupLagRow
	for _, gid := range groupIDs {
		info, err := kc.DescribeGroup(gid)
		if err != nil {
			continue
		}
		for _, t := range kc.ListTopics() {
			for _, p := range kc.Partitions(t) {
				committed, err := kc.CommittedOffset(gid, t, p.PartitionID)
				if err != nil || committed < 0 {
					continue
				}
				leo, err := kc.QueryOffset(t, p.PartitionID, -1)
				if err != nil {
					continue
				}
				lag := leo - committed
				if lag < 0 {
					lag = 0
				}
				rows = append(rows, groupLagRow{
					GroupID:       gid,
					Topic:         t,
					Partition:     p.PartitionID,
					CurrentOffset: committed,
					LogEndOffset:  leo,
					Lag:           lag,
				})
			}
		}
		_ = info
	}

	if outputFormat == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(rows)
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "GROUP\tTOPIC\tPARTITION\tCURRENT-OFFSET\tLOG-END-OFFSET\tLAG")
	for _, r := range rows {
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%d\n",
			r.GroupID, r.Topic, r.Partition, r.CurrentOffset, r.LogEndOffset, r.Lag)
	}
	w.Flush()
}
