package web

import (
	"context"
	"net"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/netguard"
)

const clusterDialTimeout = 4 * time.Second

func clusterDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return netguard.Dialer(clusterDialTimeout).DialContext(ctx, network, address)
}
