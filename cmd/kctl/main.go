package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/Yukaz0/pocketkafka/pkg/client"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	fs := flag.NewFlagSet("pkctl", flag.ExitOnError)
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

	kc, err := client.NewClient(splitBrokers(*brokers), "pkctl")
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
			fatal("usage", fmt.Errorf("pkctl create <topic> [-p partitions]"))
		}
		if err := kc.CreateTopic(rest[1], *partitions); err != nil {
			fatal("create", err)
		}
		fmt.Printf("created topic %q (%d partition(s))\n", rest[1], *partitions)
	case "delete":
		if len(args) < 2 {
			fatal("usage", fmt.Errorf("pkctl delete <topic>"))
		}
		if err := kc.DeleteTopic(rest[1]); err != nil {
			fatal("delete", err)
		}
		fmt.Printf("deleted topic %q\n", rest[1])
	case "produce":
		if len(args) < 3 {
			fatal("usage", fmt.Errorf("pkctl produce <topic> <value> [-k key]"))
		}
		p := client.NewProducer(kc, client.DefaultProducerConfig())
		off, err := p.SendSync(context.Background(), &client.Message{Topic: rest[1], Key: []byte(*key), Value: []byte(rest[2])})
		if err != nil {
			fatal("produce", err)
		}
		fmt.Printf("produced -> %s offset=%d\n", rest[1], off)
	case "consume":
		if len(args) < 2 {
			fatal("usage", fmt.Errorf("pkctl consume <topic> [-g group] [-n count]"))
		}
		cmdConsume(kc, rest[1], *group, *count, *timeout)
	case "groups":
		cmdGroups(kc)
	case "schema":
		cmdSchema(*schemaURL, rest[1:], *schemaFile, *subject)
	case "offset":
		if len(rest) < 2 {
			fatal("usage", fmt.Errorf("pkctl offset reset --group G --topic T [--to-earliest|--to-latest|--to-offset N]"))
		}
		switch rest[1] {
		case "reset":
			if *group == "" || *topic == "" {
				fatal("usage", fmt.Errorf("pkctl offset reset requires --group and --topic"))
			}
			cmdOffsetReset(kc, *group, *topic, *toEarliest, *toLatest, *toOffset)
		case "get":
			if *group == "" || *topic == "" {
				fatal("usage", fmt.Errorf("pkctl offset get requires --group and --topic"))
			}
			cmdOffsetGet(kc, *group, *topic)
		default:
			fatal("usage", fmt.Errorf("pkctl offset: unknown subcommand %q", rest[1]))
		}
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `pkctl - admin CLI for pocketkafka

Usage:
  pkctl [-b localhost:9092] [-o table|json] cluster
  pkctl [-b ...] topics
  pkctl [-b ...] create <topic> [-p partitions]
  pkctl [-b ...] delete <topic>
  pkctl [-b ...] produce <topic> <value> [-k key]
  pkctl [-b ...] consume <topic> [-g group] [-n count]
  pkctl [-b ...] groups
  pkctl [-b ...] offset get --group G --topic T
  pkctl [-b ...] offset reset --group G --topic T [--to-earliest|--to-latest|--to-offset N]
  pkctl schema list [-url http://localhost:8081]
  pkctl schema register --subject S --file ./schema.avsc [-url http://localhost:8081]`)
}

func fatal(what string, err error) {
	fmt.Fprintf(os.Stderr, "pkctl: %s: %v\n", what, err)
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
