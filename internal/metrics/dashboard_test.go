package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every pocketkafka_* series the Grafana dashboard queries must be emitted by
// the broker; a renamed metric would otherwise leave a silently empty panel.
func TestDashboardMetricsExist(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/grafana/dashboard.json")
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(raw) {
		t.Fatal("dashboard.json is not valid JSON")
	}
	used := regexp.MustCompile(`pocketkafka_[a-z0-9_]+`).FindAllString(string(raw), -1)
	if len(used) == 0 {
		t.Fatal("dashboard references no pocketkafka_ metrics")
	}

	var src strings.Builder
	err = filepath.Walk("..", func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return err
		}
		b, err := os.ReadFile(p)
		src.Write(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range used {
		base := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(name, "_bucket"), "_sum"), "_count")
		if !strings.Contains(src.String(), base) {
			t.Errorf("dashboard queries %s but no broker source emits %s", name, base)
		}
	}
}
