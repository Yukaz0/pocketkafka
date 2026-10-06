package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
)

// Any reports whether at least one surface is served over TLS.
func (t TLS) Any() bool { return t.Enabled || t.HTTP || t.MQTT }

// MinTLSVersion maps the configured min_version to a crypto/tls constant.
// The default is TLS 1.2 with an explicit AEAD-only cipher allow-list; set
// min_version: "1.3" to drop 1.2 entirely.
func (t TLS) MinTLSVersion() uint16 {
	if strings.TrimSpace(t.MinVersion) == "1.3" {
		return tls.VersionTLS13
	}
	return tls.VersionTLS12
}

// allowedCipherSuites is the TLS 1.2 allow-list: ECDHE key exchange with AEAD
// ciphers only. CBC and static-RSA suites are excluded, and Go ignores this
// list for TLS 1.3 (whose suites are already all AEAD).
var allowedCipherSuites = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
}

// ServerConfig builds the TLS configuration shared by the dashboard, the Schema
// Registry, the REST proxy and the MQTT bridge. It returns (nil, nil) when no
// surface uses TLS, so callers can treat a nil config as "plaintext".
func (t TLS) ServerConfig() (*tls.Config, error) {
	if !t.Any() {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load TLS key pair: %w", err)
	}
	cfg := &tls.Config{
		Certificates:     []tls.Certificate{cert},
		MinVersion:       t.MinTLSVersion(),
		CipherSuites:     allowedCipherSuites,
		CurvePreferences: []tls.CurveID{tls.X25519, tls.CurveP256},
	}
	if t.ClientCAFile != "" {
		pem, err := os.ReadFile(t.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read TLS client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("TLS client CA %s contains no certificates", t.ClientCAFile)
		}
		// mTLS: a client certificate signed by the CA is required, and its
		// verified subject becomes the connection's principal (see the server
		// package), so a certificate can replace SASL for machine clients.
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// ClientPrincipal returns the principal named by a verified client
// certificate: its CommonName, or the first DNS name when the CN is empty. It
// returns "" when the connection presented no verified certificate.
func ClientPrincipal(state *tls.ConnectionState) string {
	if state == nil || len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
		return ""
	}
	leaf := state.PeerCertificates[0]
	if cn := strings.TrimSpace(leaf.Subject.CommonName); cn != "" {
		return cn
	}
	if len(leaf.DNSNames) > 0 {
		return strings.TrimSpace(leaf.DNSNames[0])
	}
	return ""
}
