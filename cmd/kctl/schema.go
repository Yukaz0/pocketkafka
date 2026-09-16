package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func cmdSchema(baseURL string, args []string, schemaFile, subject string) {
	if len(args) == 0 {
		fatal("usage", fmt.Errorf("pkctl schema list | register --subject S --file F"))
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
			fatal("usage", fmt.Errorf("pkctl schema register --subject S --file F"))
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
		fatal("usage", fmt.Errorf("pkctl schema: unknown subcommand %q", args[0]))
	}
}

func urlEncode(s string) string {
	return strings.ReplaceAll(s, "/", "%2F")
}
