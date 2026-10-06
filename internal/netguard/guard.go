// Package netguard holds the one predicate that decides whether an outbound
// connection may be made to a resolved address. It exists so the dashboard's
// peer/monitoring dialler and the Kafka client cannot drift apart: a destination
// that is unsafe in one of them is unsafe in both.
//
// The rule is deliberately narrow. Loopback and RFC1918 addresses are allowed —
// the broker's whole purpose here is monitoring brokers on a private network,
// and refusing them would make the feature useless. What is refused:
//
//   - link-local unicast/multicast (fe80::/10, 169.254.0.0/16), which is where
//     the cloud instance-metadata service lives;
//   - the metadata addresses themselves, including fd00:ec2::254, which is a
//     unique-local address and therefore not covered by the link-local rule;
//   - unspecified (0.0.0.0, ::) and multicast addresses.
//
// The classifier runs on the *resolved* address (net.Dialer.Control), so a
// hostname that resolves to a blocked address — including a DNS-rebinding
// attempt — is refused as well. A zone suffix ("fe80::1%eth0") is stripped
// before parsing, because net.ParseIP rejects a zoned literal and a guard that
// reads "unparseable" as "allow" is trivially bypassed.
package netguard

import (
	"fmt"
	"net"
	"strings"
	"syscall"
	"time"
)

// BlockedAddrs are the instance-metadata endpoints of the major clouds. They are
// listed explicitly because they are not all link-local.
var BlockedAddrs = map[string]bool{
	"169.254.169.254": true, // AWS/GCP/Azure/Oracle IMDS (also link-local, listed for clarity)
	"169.254.170.2":   true, // AWS ECS task metadata
	"fd00:ec2::254":   true, // AWS IMDS over IPv6 (unique-local, not link-local)
	"100.100.100.200": true, // Alibaba Cloud metadata
}

// ParseIP parses a host, stripping an IPv6 zone suffix first. It returns nil for
// anything that is not an IP literal (a hostname reaches the dialer's resolver
// and is classified there, once it has been resolved).
func ParseIP(host string) net.IP {
	if i := strings.IndexByte(host, '%'); i >= 0 {
		host = host[:i]
	}
	return net.ParseIP(host)
}

// CheckAddr reports whether a resolved "host:port" address may be dialled. A nil
// error means allowed.
func CheckAddr(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil
	}
	ip := ParseIP(host)
	if ip == nil {
		return nil
	}
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return fmt.Errorf("refusing to connect to %s: link-local addresses are not valid targets", address)
	}
	if BlockedAddrs[ip.String()] {
		return fmt.Errorf("refusing to connect to %s: it is a cloud instance-metadata address", address)
	}
	if ip.IsUnspecified() || ip.IsMulticast() {
		return fmt.Errorf("refusing to connect to %s: not a routable target", address)
	}
	return nil
}

// Control adapts CheckAddr to the net.Dialer Control hook, which is called with
// each resolved address the dialer is about to try.
func Control(_, address string, _ syscall.RawConn) error {
	return CheckAddr(address)
}

// Dialer builds a dialer that refuses the addresses above.
func Dialer(timeout time.Duration) *net.Dialer {
	return &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second, Control: Control}
}
