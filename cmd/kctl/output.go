package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
)

// outputFormat is bound to the global -o/--output flag.
var outputFormat string

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
