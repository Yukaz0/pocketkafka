package web

// Outbound connections made on behalf of the dashboard (cluster health
// fan-out, peer proxying) are dialled through one guarded path. The dashboard
// is the only component that connects to an address an operator typed in, so
// its dialler refuses the destinations that turn a monitoring feature into an
// SSRF primitive. The policy itself lives in internal/netguard, shared with the
// Kafka client so the two cannot drift apart.

import (
	"context"
	"net"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/netguard"
)

// clusterDialTimeout bounds one outbound connection attempt.
const clusterDialTimeout = 4 * time.Second

// clusterDialContext is the DialContext shared by every dashboard HTTP client.
func clusterDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return netguard.Dialer(clusterDialTimeout).DialContext(ctx, network, address)
}
