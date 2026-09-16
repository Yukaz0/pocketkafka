package main

// Probe: render Metadata v8 response hex untuk pemeriksaan manual byte-level.
import (
	"encoding/hex"
	"fmt"

	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/handler"
	"github.com/Yukaz0/pocketkafka/internal/storage"
)

func main() {
	cfg := config.Default()
	store, err := storage.NewStore("/tmp/pocketkafka-probe-XXXX", cfg.Storage.SegmentMaxBytes, cfg.Storage.IndexIntervalBytes)
	if err != nil {
		panic(err)
	}
	store.EnsureTopic("GRITA_EVENTS", 1)
	offsetStore, err := coordinator.NewOffsetStore("file", "/tmp/pocketkafka-probe-offsets")
	if err != nil {
		panic(err)
	}
	gm := coordinator.NewGroupManager(offsetStore, 0, "local-kafka", 29092, 10000)
	h := handler.New(store, gm, &cfg, 0, "local-kafka", 29092)

	// Metadata v8: topics array (compact? tidak - v8 classic: int32 array),
	// null = -1, lalu bool aut creation + 2 bool authorized ops.
	body := []byte{}
	body = append(body, 0xff, 0xff, 0xff, 0xff)                                    // null topics array
	body = append(body, 0x01)                                                      // allow auto topic creation = true
	body = append(body, 0x01)                                                      // include cluster authorized ops = true
	body = append(body, 0x01)                                                      // include topic authorized ops = true
	resp, err := h.Handle(3, 8, body, handler.RequestContext{ListenerPort: 29092}) // Metadata v8
	if err != nil {
		panic(err)
	}
	fmt.Printf("len=%d\n%s\n", len(resp), hex.Dump(resp))
}
