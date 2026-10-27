package netguard

import (
	"fmt"
	"net"
	"strings"
	"syscall"
	"time"
)

var BlockedAddrs = map[string]bool{
	"169.254.169.254": true,
	"169.254.170.2":   true,
	"fd00:ec2::254":   true,
	"100.100.100.200": true,
}

func ParseIP(host string) net.IP {
	if i := strings.IndexByte(host, '%'); i >= 0 {
		host = host[:i]
	}
	return net.ParseIP(host)
}

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

func Control(_, address string, _ syscall.RawConn) error {
	return CheckAddr(address)
}

func Dialer(timeout time.Duration) *net.Dialer {
	return &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second, Control: Control}
}
