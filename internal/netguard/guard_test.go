package netguard

import (
	"testing"
)

func TestCheckAddrBlocksTheDangerousRanges(t *testing.T) {
	blocked := []struct {
		name string
		addr string
	}{
		{"aws/gcp/azure metadata", "169.254.169.254:80"},
		{"aws ecs metadata", "169.254.170.2:80"},
		{"alibaba metadata", "100.100.100.200:80"},
		{"aws metadata over IPv6 (unique-local, not link-local)", "[fd00:ec2::254]:80"},
		{"link-local IPv4", "169.254.9.9:9092"},
		{"link-local IPv6", "[fe80::1]:9092"},
		{"link-local IPv4 in IPv6 form", "[::ffff:169.254.169.254]:80"},
		{"zoned link-local literal (zone must be stripped before parsing)", "[fe80::1%eth0]:9092"},
		{"zoned metadata literal", "[fd00:ec2::254%eth0]:80"},
		{"unspecified IPv4", "0.0.0.0:9092"},
		{"unspecified IPv6", "[::]:9092"},
		{"multicast", "224.0.0.1:9092"},
	}
	for _, tc := range blocked {
		if err := CheckAddr(tc.addr); err == nil {
			t.Errorf("%s (%s) was allowed", tc.name, tc.addr)
		}
	}
}

func TestCheckAddrAllowsPrivateAndLoopback(t *testing.T) {
	allowed := []string{
		"127.0.0.1:8080",
		"[::1]:8080",
		"10.0.0.5:9092",
		"192.168.1.26:8080",
		"172.16.4.4:9092",
		"[fd00::1]:9092",
		"broker.internal:9092",
		"",
	}
	for _, addr := range allowed {
		if err := CheckAddr(addr); err != nil {
			t.Errorf("%s was refused: %v", addr, err)
		}
	}
}

func TestParseIPStripsZone(t *testing.T) {
	if ip := ParseIP("fe80::1%eth0"); ip == nil {
		t.Fatal("a zoned IPv6 literal must still parse")
	}
	if ip := ParseIP("not-an-ip"); ip != nil {
		t.Fatalf("hostname parsed as an IP: %v", ip)
	}
	if ip := ParseIP("10.0.0.1"); ip == nil || ip.String() != "10.0.0.1" {
		t.Fatalf("plain IPv4 parsed as %v", ip)
	}
}

func TestDialerRefusesAtDialTime(t *testing.T) {

	if _, err := Dialer(0).Dial("tcp", "169.254.169.254:80"); err == nil {
		t.Fatal("the dialer connected to the metadata address")
	}
}
