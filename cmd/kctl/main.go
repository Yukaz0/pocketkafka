// Command kctl is a small admin CLI for go-kafka-neu built on the bundled
// zero-dependency client SDK (pkg/client).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/neu/go-kafka-neu/pkg/client"
)

// outputFormat is bound to the global -o/--output flag.
var outputFormat string

func main() {
	fs := flag.NewFlagSet("kctl", flag.ExitOnError)
	brokers := fs.String("b", "localhost:9092", "comma-separated broker addresses")
	output := fs.String("o", "", "output format: table (default) or json")
	partitions := fs.Int("p", 1, "number of partitions (create)")
	group := fs.String("g", "", "consumer group id (consume / offset reset)")
	topic := fs.String("t", "", "topic (offset reset)")
	count := fs.Int("n", 10, "max messages to consume")
	key := fs.String("k", "", "message key (produce)")
	timeout := fs.Duration("timeout", 10*time.Second, "timeout for consume")
	toEarliest := fs.Bool("to-earliest", false, "reset offset to earliest")
	toLatest := fs.Bool("to-latest", false, "reset offset to latest")
	toOffset := fs.Int64("to-offset", -1, "reset offset to a specific value")
	schemaFile := fs.String("file", "", "schema file to register (schema register)")
	schemaURL := fs.String("url", "http://localhost:8081", "schema registry URL (schema)")
	subject := fs.String("subject", "", "schema subject (schema register)")

	// Split the argument list at the first non-flag token (the subcommand),
	// skipping the values of flags that take one.
	// Everything before it is parsed by the global FlagSet; everything after
	// it is scanned manually for subcommand flags (Go's flag package stops at
	// the first positional argument).
	valueFlags := map[string]bool{
		"b": true, "o": true, "p": true, "g": true, "t": true, "n": true,
		"k": true, "timeout": true, "file": true, "url": true, "subject": true,
	}
	var globalArgs, rest []string
	seenCmd := false
	for i := 0; i < len(os.Args[1:]); i++ {
		a := os.Args[1:][i]
		if !seenCmd && !strings.HasPrefix(a, "-") {
			seenCmd = true
			rest = append(rest, a)
			continue
		}
		if seenCmd {
			rest = append(rest, a)
			continue
		}
		globalArgs = append(globalArgs, a)
		if strings.HasPrefix(a, "-") && !strings.Contains(a, "=") {
			name := strings.TrimLeft(a, "-")
			if valueFlags[name] && i+1 < len(os.Args[1:]) {
				i++
				globalArgs = append(globalArgs, os.Args[1:][i]) // keep the flag's value
			}
		}
	}
	fs.Parse(globalArgs)
	args := fs.Args()
	if len(args) > 0 {
		// fs.Args() holds flags that came after the subcommand; prepend them.
		rest = append(args, rest...)
	}
	if len(rest) == 0 {
		usage()
		os.Exit(1)
	}
	outputFormat = *output

	// Manual subcommand flag scanning.
	subFlags := map[string]*string{
		"group":   group,
		"topic":   topic,
		"file":    schemaFile,
		"url":     schemaURL,
		"subject": subject,
	}
	for i := 1; i < len(rest); i++ {
		a := rest[i]
		name, val, hasVal := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if dst, ok := subFlags[name]; ok && hasVal {
			*dst = val
			continue
		}
		if dst, ok := subFlags[strings.TrimLeft(a, "-")]; ok && i+1 < len(rest) {
			*dst = rest[i+1]
			i++
			continue
		}
		if a == "--to-earliest" {
			*toEarliest = true
		}
		if a == "--to-latest" {
			*toLatest = true
		}
		if strings.HasPrefix(a, "--to-offset=") {
			if v, err := strconv.ParseInt(strings.TrimPrefix(a, "--to-offset="), 10, 64); err == nil {
				*toOffset = v
			}
		}
	}

	kc, err := client.NewClient(splitBrokers(*brokers), "kctl")
	if err != nil {
		fatal("connect", err)
	}
	defer kc.Close()

	cmd := rest[0]
	switch cmd {
	case "cluster":
		cmdCluster(kc)
	case "topics", "list":
		cmdTopics(kc)
	case "create":
		if len(args) < 2 {
			fatal("usage", fmt.Errorf("kctl create <topic> [-p partitions]"))
		}
		if err := kc.CreateTopic(rest[1], *partitions); err != nil {
			fatal("create", err)
		}
		fmt.Printf("created topic %q (%d partition(s))\n", rest[1], *partitions)
	case "delete":
		if len(args) < 2 {
			fatal("usage", fmt.Errorf("kctl delete <topic>"))
		}
		if err := kc.DeleteTopic(rest[1]); err != nil {
			fatal("delete", err)
		}
		fmt.Printf("deleted topic %q\n", rest[1])
	case "produce":
		if len(args) < 3 {
			fatal("usage", fmt.Errorf("kctl produce <topic> <value> [-k key]"))
		}
		p := client.NewProducer(kc, client.DefaultProducerConfig())
		off, err := p.SendSync(context.Background(), &client.Message{Topic: rest[1], Key: []byte(*key), Value: []byte(rest[2])})
		if err != nil {
			fatal("produce", err)
		}
		fmt.Printf("produced -> %s offset=%d\n", rest[1], off)
	case "consume":
		if len(args) < 2 {
			fatal("usage", fmt.Errorf("kctl consume <topic> [-g group] [-n count]"))
		}
		cmdConsume(kc, rest[1], *group, *count, *timeout)
	case "groups":
		cmdGroups(kc)
	case "schema":
		cmdSchema(*schemaURL, rest[1:], *schemaFile, *subject)
	case "offset":
		if len(rest) < 2 {
			fatal("usage", fmt.Errorf("kctl offset reset --group G --topic T [--to-earliest|--to-latest|--to-offset N]"))
		}
		switch rest[1] {
		case "reset":
			if *group == "" || *topic == "" {
				fatal("usage", fmt.Errorf("kctl offset reset requires --group and --topic"))
			}
			cmdOffsetReset(kc, *group, *topic, *toEarliest, *toLatest, *toOffset)
		case "get":
			if *group == "" || *topic == "" {
				fatal("usage", fmt.Errorf("kctl offset get requires --group and --topic"))
			}
			cmdOffsetGet(kc, *group, *topic)
		default:
			fatal("usage", fmt.Errorf("kctl offset: unknown subcommand %q", rest[1]))
		}
	default:
		usage()
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// Output formatter (Fitur 3: global -o json)
// ---------------------------------------------------------------------------

// renderTable prints a table or, with -o json, a JSON array of objects.
func renderTable(headers []string, rows [][]any) {
	if outputFormat == "json" {
		out := make([]map[string]any, 0, len(rows))
		for _, r := range rows {
			m := make(map[string]any, len(headers))
			for i, h := range headers {
				m[strings.ReplaceAll(strings.ToLower(h), " ", "_")] = r[i]
			}
			out = append(out, m)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(out)
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, strings.Join(headers, "\t"))
	for _, r := range rows {
		cells := make([]string, len(r))
		for i, v := range r {
			cells[i] = fmt.Sprintf("%v", v)
		}
		fmt.Fprintln(w, strings.Join(cells, "\t"))
	}
	w.Flush()
}

func cmdCluster(kc *client.KafkaClient) {
	topics := kc.ListTopics()
	parts := 0
	for _, t := range topics {
		parts += len(kc.Partitions(t))
	}
	if outputFormat == "json" {
		renderTable([]string{"broker", "topics", "partitions"}, [][]any{{"go-kafka-neu", len(topics), parts}})
		return
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

// ---------------------------------------------------------------------------
// groups (Fitur 1: per-partition lag calculation)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// schema (Fitur 2.1: REST client to port 8081)
// ---------------------------------------------------------------------------

func cmdSchema(baseURL string, args []string, schemaFile, subject string) {
	if len(args) == 0 {
		fatal("usage", fmt.Errorf("kctl schema list | register --subject S --file F"))
		return
	}
	switch args[0] {
	case "list":
		resp, err := http.Get(strings.TrimRight(baseURL, "/") + "/subjects")
		if err != nil {
			fatal("schema list", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			fatal("schema list", fmt.Errorf("registry returned %d: %s", resp.StatusCode, body))
		}
		var subjects []string
		if err := json.Unmarshal(body, &subjects); err != nil {
			fatal("schema list", err)
		}
		if outputFormat == "json" {
			fmt.Println(string(body))
			return
		}
		for _, s := range subjects {
			fmt.Println(s)
		}
	case "register":
		if schemaFile == "" || subject == "" {
			fatal("usage", fmt.Errorf("kctl schema register --subject S --file F"))
		}
		schemaData, err := os.ReadFile(schemaFile)
		if err != nil {
			fatal("schema register", err)
		}
		payload, _ := json.Marshal(map[string]any{"schema": string(schemaData), "schemaType": "AVRO"})
		url := strings.TrimRight(baseURL, "/") + "/subjects/" + urlEncode(subject) + "/versions"
		req, _ := http.NewRequest("POST", url, strings.NewReader(string(payload)))
		req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fatal("schema register", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			fatal("schema register", fmt.Errorf("registry returned %d: %s", resp.StatusCode, body))
		}
		fmt.Println(string(body))
	default:
		fatal("usage", fmt.Errorf("kctl schema: unknown subcommand %q", args[0]))
	}
}

// ---------------------------------------------------------------------------
// offset (Fitur 2.2: reset & inspect offsets)
// ---------------------------------------------------------------------------

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

func urlEncode(s string) string {
	return strings.ReplaceAll(s, "/", "%2F")
}

func usage() {
	fmt.Fprintln(os.Stderr, `kctl - admin CLI for go-kafka-neu

Usage:
  kctl [-b localhost:9092] [-o table|json] cluster
  kctl [-b ...] topics
  kctl [-b ...] create <topic> [-p partitions]
  kctl [-b ...] delete <topic>
  kctl [-b ...] produce <topic> <value> [-k key]
  kctl [-b ...] consume <topic> [-g group] [-n count]
  kctl [-b ...] groups
  kctl [-b ...] offset get --group G --topic T
  kctl [-b ...] offset reset --group G --topic T [--to-earliest|--to-latest|--to-offset N]
  kctl schema list [-url http://localhost:8081]
  kctl schema register --subject S --file ./schema.avsc [-url http://localhost:8081]`)
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
