package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/Yukaz0/pocketkafka/pkg/client"
	"os"
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

	fs.StringVar(group, "group", "", "consumer group id (alias of -g)")
	fs.StringVar(topic, "topic", "", "topic (alias of -t)")

	rest, err := parseInterspersed(fs, os.Args[1:])
	if err != nil {
		os.Exit(2) // FlagSet (ExitOnError) already printed the reason
	}
	if len(rest) == 0 {
		usage()
		os.Exit(1)
	}
	outputFormat = *output

	if err := checkArity(rest); err != nil {
		fatal("usage", err)
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
		if err := kc.CreateTopic(rest[1], *partitions); err != nil {
			fatal("create", err)
		}
		fmt.Printf("created topic %q (%d partition(s))\n", rest[1], *partitions)
	case "delete":
		if err := kc.DeleteTopic(rest[1]); err != nil {
			fatal("delete", err)
		}
		fmt.Printf("deleted topic %q\n", rest[1])
	case "produce":
		p := client.NewProducer(kc, client.DefaultProducerConfig())
		off, err := p.SendSync(context.Background(), &client.Message{Topic: rest[1], Key: []byte(*key), Value: []byte(rest[2])})
		if err != nil {
			fatal("produce", err)
		}
		fmt.Printf("produced -> %s offset=%d\n", rest[1], off)
	case "consume":
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

func checkArity(rest []string) error {
	if len(rest) == 0 {
		return errors.New("missing subcommand")
	}
	switch rest[0] {
	case "create":
		if len(rest) < 2 {
			return errors.New("pkctl create <topic> [-p partitions]")
		}
	case "delete":
		if len(rest) < 2 {
			return errors.New("pkctl delete <topic>")
		}
	case "produce":
		if len(rest) < 3 {
			return errors.New("pkctl produce <topic> <value> [-k key]")
		}
	case "consume":
		if len(rest) < 2 {
			return errors.New("pkctl consume <topic> [-g group] [-n count]")
		}
	}
	return nil
}

// parseInterspersed parses flags that appear before, between or after the
// positional arguments, and returns the positionals. Unknown flags are an
// error rather than being silently dropped.
func parseInterspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}
