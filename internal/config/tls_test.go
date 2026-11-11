package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTLSMinVersionAndSurfaces(t *testing.T) {
	var none TLS
	if none.Any() {
		t.Fatal("an all-default TLS block should report no surface")
	}
	if cfg, err := none.ServerConfig(); cfg != nil || err != nil {
		t.Fatalf("ServerConfig on an unused block = (%v, %v), want (nil, nil)", cfg, err)
	}
	if got := none.MinTLSVersion(); got != tls.VersionTLS12 {
		t.Fatalf("default min version = %d, want TLS 1.2", got)
	}
	hardened := TLS{HTTP: true, MinVersion: "1.3"}
	if !hardened.Any() {
		t.Fatal("security.tls.http should count as a TLS surface")
	}
	if got := hardened.MinTLSVersion(); got != tls.VersionTLS13 {
		t.Fatalf("min_version 1.3 = %d, want TLS 1.3", got)
	}
}

func TestServerConfigRequiresUsableMaterial(t *testing.T) {
	if _, err := (TLS{HTTP: true}).ServerConfig(); err == nil {
		t.Fatal("a TLS surface without cert/key produced no error")
	}
	_, err := (TLS{HTTP: true, CertFile: "/nonexistent/cert.pem", KeyFile: "/nonexistent/key.pem"}).ServerConfig()
	if err == nil {
		t.Fatal("an unreadable key pair produced no error")
	}
}

func TestServerConfigCipherAllowList(t *testing.T) {
	certFile, keyFile := writeSelfSigned(t)
	cfg, err := (TLS{HTTP: true, CertFile: certFile, KeyFile: keyFile}).ServerConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("MinVersion = %d, want TLS 1.2", cfg.MinVersion)
	}
	if len(cfg.CipherSuites) == 0 {
		t.Fatal("no explicit TLS 1.2 cipher allow-list")
	}
	if cfg.ClientAuth != tls.NoClientCert {
		t.Fatal("mTLS must stay off until client_ca_file is set")
	}
}

func TestClientPrincipal(t *testing.T) {
	if got := ClientPrincipal(nil); got != "" {
		t.Fatalf("nil state gave %q", got)
	}
	unverified := &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{Subject: pkix.Name{CommonName: "spoofed"}}}}
	if got := ClientPrincipal(unverified); got != "" {
		t.Fatalf("an unverified certificate yielded %q, want \"\"", got)
	}
	leaf := &x509.Certificate{Subject: pkix.Name{CommonName: "monitor"}, DNSNames: []string{"monitor.example"}}
	verified := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf},
		VerifiedChains:   [][]*x509.Certificate{{leaf}},
	}
	if got := ClientPrincipal(verified); got != "monitor" {
		t.Fatalf("principal = %q, want the CommonName", got)
	}
	leaf.Subject = pkix.Name{}
	if got := ClientPrincipal(verified); got != "monitor.example" {
		t.Fatalf("principal = %q, want the DNS name fallback", got)
	}
}

func writeSelfSigned(t *testing.T) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "pocketkafka-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}
