package web

import (
	"context"
	"errors"
	"net"
	"net/url"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/netguard"
)

const clusterDialTimeout = 4 * time.Second

func clusterDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return netguard.Dialer(clusterDialTimeout).DialContext(ctx, network, address)
}

// peerHostAllowed gates outbound peer/cluster URLs before any request is built.
// IP literals are checked against netguard.CheckAddr (link-local, cloud metadata,
// unspecified/multicast rejected). DNS names pass here; the actual connection still
// goes through clusterDialContext -> netguard dial control. RFC1918/loopback stay
// allowed: LAN peers (e.g. 192.168.x.x) are the primary deployment target.
func peerHostAllowed(u *url.URL) error {
	if u == nil {
		return errInvalidPeerHost
	}
	host := u.Hostname()
	if host == "" || u.User != nil {
		return errInvalidPeerHost
	}
	if ip := netguard.ParseIP(host); ip != nil {
		return netguard.CheckAddr(net.JoinHostPort(host, "0"))
	}
	return nil
}

var errInvalidPeerHost = errors.New("invalid peer host")
